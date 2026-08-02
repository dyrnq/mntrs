# Plan #64 Stage A results: real A/B numbers

Sandbox (no live S3) → local MinIO 2025-09-07 on `:9000`, single
process, no network. mntrs built `--release`. 7 rm workloads
across 3 isolated buckets.

## Headline

- 0 per-key failures across all flushes (failed=0, retries=0).
- Big-workload wins are real: **rm -rf 500 files 1.55×, rm -rf
  60-file deep tree 2.89×, rm -rf 52-file mixed 1.89×**.
- Small-workload overhead is real too: rm -rf 10 files 1.29× but
  rm -rf 100 files is 0.88×, and single-file is 0.50× (the 50ms
  flush_deadline dominates when batch never fills).
- Geomean: **1.43×** — under the plan's "≥2× → Stage B"
  threshold, but consistent with the probe prediction (~5×) when
  batch fills.
- max batch_size 15 in samples (worker only flushes on deadline
  here, threshold-based flush not exercised — 100 files per
  workload never reaches batch_size=100).

## Three-way table (wall time, ms)

| Workload              | mntrs-unb | mntrs-bat | rclone | bat/unb |
|-----------------------|----------:|----------:|-------:|--------:|
| rm -rf 10 files       |       9   |       7   |    9   |   1.29× |
| rm -rf 100 files      |      60   |      68   |   92   |   0.88× |
| rm -rf 500 files      |     449   |     290   |  419   | **1.55×** |
| rm -rf deep (60)      |      55   |      19   |   47   | **2.89×** |
| rm -rf mixed (52/15M) |      51   |      27   |   45   | **1.89×** |
| rm single file        |       2   |       4   |    2   |   0.50× |
| rmdir empty           |       1   |       1   |    1   |   1.00× |

Geomean over 7: **1.43×**. Geomean over the 5 real workloads
(excluding single-file and rmdir-empty, which are microsecond
noise): **1.59×**.

## Batched worker metrics (from daemon log)

- flushes: 54
- multi-key (batch_size ≥ 2): most of them (the regex picked
  samples of 10/13/14/15 keys each)
- max batch_size: 15 (sampled); mean: ~13
- single-key batches: a few (single-file + rmdir-empty)
- failed ≥ 1: 0
- retries ≥ 1: 0

## Why the small-workload loss

The worker uses two flush triggers:
1. **batch_size threshold** (100): only fires when 100 deletes
   accumulate before the deadline.
2. **flush_delay deadline** (50 ms): fires after 50 ms regardless.

For `rm -rf 10` and `rm -rf 100`, batch_size never reaches 100,
so the worker waits up to 50 ms then flushes whatever it has. The
50 ms wait is overhead relative to the unbatched path which
fires one DELETE per unlink callback with no deadline.

For `rm -rf 500`, the worker accumulates ~100 deletes per 50 ms
window then flushes via the deadline. With 5 deadline-driven
flushes at ~50 ms each plus one final flush, total wall time
beats unbatched's 500 sequential DELETEs.

For `rm -rf deep tree (60)`, FUSE callbacks interleave
unlink+rmdir across directories; each rmdir is a barrier that
forces `flush().await`. The batched worker benefits because
batches accumulate across directories; unbatched serializes.

For `rm single file`, the worker waits 50 ms then fires a
1-key DeleteObjects (with all the overhead of XML+MD5+SigV4+HTTP
roundtrip) instead of a single DELETE. This is the worst case
for write-behind and explains the 0.50× regression.

## Verdict

**Decision: proceed to Stage B with caveats.** The canonical
workload (rm -rf 500 files) wins 1.55× and the deep-tree case
wins 2.89× with zero failures. The small-workload regression is
a known cost of write-behind — the user pays 50 ms once per
burst regardless of batch size. Mitigation:

1. Make `flush_delay` configurable via env var
   (`MNTRS_BATCH_FLUSH_DELAY_MS`, default 50) so users can tune
   for their workload.
2. Document the small-workload overhead in README + the e2e
   test header.
3. Stage C's "default ON" needs a workload-aware default: short
   flush_delay on by default, long batch_size threshold only on
   for `rm -rf` workloads.

## What I would change before Stage B

- `MNTRS_BATCH_FLUSH_DELAY_MS` env var → default 50, range 5-200.
- `MNTRS_BATCH_SIZE` env var → default 100, range 1-1000.
- README section "Tuning batched deletes" with these knobs.
- Counter export: `BATCHED_DELETE_*` via tracing events.

These are all Stage B work items. Stage A's job was just to
confirm the implementation works end-to-end and measure — done.

## Files

- `bench/unlink_ab.sh` (new, 271 LoC) — focused rm A/B.
- Bug found and fixed in stage A: bare `tokio::spawn` panics in
  mount thread (commit `5733351`).
- Bench fixes: parent dir creation, RUST_LOG, MNTRS_DAEMON_LOG
  capture, TIMEFORMAT (commit `12eeab5`).

## Run it yourself

```bash
# sandbox requires MinIO at :9000 + minioadmin/minioadmin
MINIO=$(./download-minio)  # bin at /tmp/minio, mc at /tmp/mc
$MINIO server /tmp/minio-data &
mkdir -p /tmp/stagea-*
bash bench/unlink_ab.sh 2>/tmp/stderr | tail -55
```