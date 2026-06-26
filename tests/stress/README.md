# Stress / stability test suite (issue #143)

Large-scale stress tests that exercise failure modes the unit and
integration tests don't reach. All scenarios use the `memory://`
backend — no external S3/HDFS/MinIO dep, runs anywhere.

## Scenarios

| Script | What it tests | Default size |
|---|---|---|
| `01-large-dir.sh` | 10k files in one dir: ls, find, stat, md5 | 10000 × 256 B |
| `02-large-file-io.sh` | 1 GiB sequential write + read + md5 (multipart upload path) | 1024 MiB |
| `03-cache-eviction.sh` | 2× mem-limit write + read-back: LRU eviction transparent to read path | 256 MiB mem-limit, 512 MiB data |
| `04-writeback-concurrent.sh` | N parallel writers exceed `UPLOAD_SEM=4`: all data lands, no leak | 8 writers × 8 files × 256 KiB |
| `05-crash-recovery.sh` | `SIGKILL` during writeback; remount picks up `.dirty` sidecars | 4 MiB + 2 MiB |
| `06-soak-mixed.sh` | Continuous R/W/D/stat with RSS/fd/thread leak checks | 300 s |

## Entry points

- **`run-all.sh`** — full-size scenarios; for operator soak runs and
  "I want it to actually exercise the failure modes" sessions.
- **`ci-smoke.sh`** — conservative sizes (1k files / 256 MiB / 60 s
  soak) for nightly CI; total ~5 min.

Both accept an optional scenario list:
```bash
bash tests/stress/ci-smoke.sh                    # all 6
bash tests/stress/ci-smoke.sh 01-large-dir       # one
bash tests/stress/ci-smoke.sh 02-large-file-io 06-soak-mixed
```

## Scaling a single scenario

Each script reads `STRESS_*` env vars. Override before invocation:

```bash
STRESS_FILE_MB=4096 bash tests/stress/02-large-file-io.sh
STRESS_SOAK_SECS=3600 bash tests/stress/06-soak-mixed.sh
```

## CI integration

`.github/workflows/stress-nightly.yml` runs `ci-smoke.sh` daily at 04:00 UTC.
Manual dispatch from the Actions tab accepts a space-separated scenario
list in the `scenarios` input. Failure uploads `/tmp/mntrs-stress/**` as
an artifact (7-day retention) for postmortem.

## What each scenario catches

Mapping to the issue's expected failure modes:

- **Large directory (01)** — readdir chunk-size regressions, inode leak
  under churn, stat-cache invalidation races.
- **Large file I/O (02)** — writeback buffer overflow/truncation, the
  >200 MiB multipart upload path end-to-end (issue #46), `.dirty`
  sidecar lifecycle (issue #53), read prefetch correctness (issue
  #132).
- **Cache eviction (03)** — LRU races (`mem_limiter::release_if_reserved`,
  issue #118), block-cache inconsistency after eviction (issue #55),
  mem_cache A/B parity.
- **Writeback concurrent (04)** — concurrency bugs in `writeback::spawn`
  (issue #53 cap math, `MAX_REENQUEUE_CYCLES` race, Semaphore
  accounting), lost writes under non-FIFO upload order, `PENDING_COUNT`
  accounting leaks.
- **Crash recovery (05)** — `.dirty` sidecar recovery across crashes,
  cache-dir state corruption under abrupt exit, FUSE session leak when
  killed before cleanup.
- **Soak mixed (06)** — RSS/fd/thread leaks under sustained churn,
  unbounded growth of `DelayQueue`, tokio task join leaks.

## Output

Each script writes to `$STRSCRATCH/<name>-<pid>/`:

- `mount.log` — mntrs daemon output (incl. mem_cache trace events)
- `metrics.txt` — RSS/fd/thread samples (one line per sample)
- `summary.txt` — pass/fail + timings (single-line summary)

Override scratch dir with `STRSCRATCH=/some/where bash ...`.