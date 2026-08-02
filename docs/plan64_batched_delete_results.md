# Plan #64 results: Batched S3 Deletes for FUSE unlink burst

**Status:** A/B bench infrastructure complete (steps 1-12). A/B
measurements pending — this sandbox has no live MinIO, so the
end-to-end run must happen on CI. Default stays OFF until soak
metrics prove the write-behind path is safe.

## What was built

- `src/batched_delete.rs` (~1170 LoC, 19 unit tests against a
  local axum test server). Worker, struct, XML+SigV4+S3 request
  builder, response parser, retry policy.
- `MNTRS_UNLINK_BATCH=1` opt-in env gate (S3 only, exact match,
  presence-only is NOT enabled).
- `BuiltOperator` refactor exposing the shared `reqwest::Client`
  so the batcher reuses the same TLS config as OpenDAL.
- 4 delete-site replacements (symlink cleanup, regular unlink,
  rmdir, rename-fallback). The rename-fallback gate also fixes
  a **pre-existing data-loss bug**: source delete was running
  unconditionally even when the copy stage returned
  `Ok(false)`. Now gated on `copy_result == Ok(true)`.
- Tombstone set in lookup/getattr/readdir to mask in-flight
  write-behind deletes from the local view.
- `rmdir` as a directory barrier: enqueue + `flush().await`
  before returning so `rm -rf dir` cannot return with deletes
  still pending for the directory itself.
- MinIO e2e test (`tests/e2e/mount/unlink_batch_test.sh`) —
  asserts the backend ends up empty after `rm -rf` and the
  daemon log shows multi-key flushes.
- Bench A/B topology (`bench/run_all.sh` + `bench/render_table.py`)
  with separate S3 prefixes per impl (mandatory: deleting
  through mntrs would otherwise make the subsequent rclone
  deletes operate on already-missing keys).

## Architectural constraint

`fuser::Config::n_threads` defaults to 1 (`session.rs:257`),
serializing FUSE callbacks. A strict `await oneshot` per
unlink yields batches of 1 and recovers no speedup. The only
viable path is **write-behind**: `batcher.delete(path)` returns
`Ok(())` immediately, the FUSE kernel keeps dispatching queued
unlinks, and the worker on `rt()` flushes real 100-key batches
in parallel. Per-key S3 errors are logged, not surfaced.

## Predicted A/B (from probe data, 500-file workload)

| Workload              | mntrs-unbatched | mntrs-batched (predicted) | rclone | Batched vs unbatched |
|-----------------------|----------------:|--------------------------:|-------:|---------------------:|
| `rm -rf 500 files`    | ~574 ms         | ~115 ms                   | ~135 ms| ~5×                  |

The probe was boto3's `delete_objects` against local MinIO
(230 µs/key) — same endpoint, same bucket, same network. The
prediction assumes the batched worker is the bottleneck; if
the FUSE callback serializes harder than the probe measured,
the speedup will be lower but still material.

## Why default stays OFF

1. **Write-behind loses per-callback backend errors.** `rm`
   returns 0 even if S3 returns 500 for every key. Documented
   in the plan under "Tombstones (write-behind only)"; the only
   mitigation is logs + counters + tombstones-removed-on-failure.
2. **Process crash leaves tombstones without their S3 counterpart
   delete.** In-memory tombstone set → orphan S3 objects possible
   on unclean shutdown. Drain is bounded but not guaranteed.
3. **rmdir barrier cannot retroactively fail earlier unlinks.**
   Even with the barrier, individual file-level deletes returned
   to the user before the rmdir returned. The barrier closes
   the directory-marker gap only.
4. **No soak metrics yet.** A/B has not been run against live
   MinIO for sustained periods. We do not know retry behavior
   under realistic network conditions.

## How to run A/B on CI

```bash
# 1. start MinIO on CI runner
docker run -d -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data --console-address :9001

# 2. configure mntrs env vars
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

# 3. run the bench
cd bench
./run_all.sh

# 4. inspect /tmp/mntrs-bench-result.txt and the three-way
#    summary printed at EOF (mntrs / mntrs-batched / rclone,
#    plus the batched-vs-unbatched A/B subtable with geomean
#    speedup).

# 5. inspect /tmp/mntrs-daemon-batch.log for multi-key flush
#    events:
grep 'batched_delete: flush' /tmp/mntrs-daemon-batch.log
```

If the geomean speedup on `rm -rf 10/100/500/deep/mixed` is
≥ 2× and no per-key failures appear in the daemon log, a
follow-up PR can flip the default for S3 backends. Until then,
the env gate stays explicit.

## Files changed (13 commits total)

See `git log --oneline | head -13` for the full sequence.
Key commits:
- Phase 0 probe (FUSE concurrency).
- Pre-existing rename-fallback data-loss fix.
- `apply_operator_with_tls` refactor exposing `reqwest::Client`.
- `Cargo.toml` direct deps for SigV4 (reqsign-aws-v4, reqsign-core,
  reqsign-file-read-tokio, http, quick-xml, md-5, base64).
- `src/batched_delete.rs` worker module + 19 unit tests.
- `src/lib.rs` MntrsFs fields, init/shutdown, helpers, 4 delete
  site replacements, tombstones, rmdir barrier.
- `src/cmd/mount.rs` `BuiltOperator` + S3 config plumbing +
  `MNTRS_UNLINK_BATCH=1` gate.
- `tests/e2e/mount/unlink_batch_test.sh` MinIO end-to-end test.
- `bench/run_all.sh` + `bench/render_table.py` A/B topology +
  three-way column rendering + EOF ordering fix.

## Known follow-ups (not in plan #64)

- Add `BATCHED_DELETE_*` counter to a `/metrics` endpoint once
  one exists.
- Hook `batched_delete` into the prometheus exporter.
- Consider chunked-output keys (>1000) if S3 ever raises the
  cap; current implementation hard-caps at 1000 regardless
  of the configured threshold.
- Drain semantics: the `Shutdown { drain: true }` path waits
  with a bounded timeout. Confirm the FUSE adapter's destroy
  path respects that timeout (currently best-effort).