# mntrs

> Mount remote storage (S3, GCS, HDFS, Azure Blob, etc.) via FUSE.
>
> Linux / macOS / Windows (WinFSP) / Kubernetes (CSI)
>
> [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](#license)
> [![Rust 1.87+](https://img.shields.io/badge/rust-1.87%2B-orange.svg)](https://www.rust-lang.org)

A high-performance FUSE mount for object storage and remote filesystems, written in Rust.
Backed by [Apache OpenDAL](https://github.com/apache/opendal), supporting **13 storage backends**
with a unified caching, prefetching, and write-back pipeline.

---

## Highlights

- **Single-file write cache** — per-handle cache file with `WriteAt` random write support, plus block-level read cache (8 MB)
- **Adaptive prefetcher** with backpressure — chunk size doubles on sequential reads (up to 8 MB)
- **Multi-chunk concurrent read** — `Semaphore`-bounded streams per FUSE read
- **Write-back queue** with `fsync` semantics + `.dirty` sidecar crash recovery
- **HDFS Kerberos** — three backends (native / JNI / WebHDFS)
- **WinFSP** adapter for native Windows support
- **Pure-Rust CSI driver** for Kubernetes with Controller + Node + Identity services
- **CRC64 integrity** for disk cache

---

## Quick Start

```bash
# S3
mntrs mount s3://my-bucket /mnt/s3 \
  --opt region=us-east-1 \
  --opt access-key=AKIA... \
  --opt secret-key=...

# MinIO (self-signed CA)
mntrs mount s3://bucket /mnt/s3 \
  --opt endpoint=https://minio.local:9000 \
  --opt cacert=/etc/ca.crt

# HDFS (Kerberos via kinit)
kinit -kt /etc/security/keytabs/hdfs.keytab hdfs/namenode@REALM
mntrs mount hdfs://namenode:8020 /mnt/hdfs

# HDFS (Kerberos via options)
mntrs mount hdfs://namenode:8020 /mnt/hdfs \
  --opt dfs.namenode.kerberos.principal=hdfs/_HOST@REALM \
  --opt dfs.namenode.kerberos.keytab=/etc/krb5.keytab

# GCS
mntrs mount gs://my-bucket /mnt/gcs

# Local filesystem (passthrough)
mntrs mount fs:///data /mnt/fs

# Unmount
mntrs unmount /mnt/s3
```

---

## Installation

```bash
# From source (Rust 1.87+)
cargo install --path .

# Pre-built binaries (GitHub Releases, all platforms)
# https://github.com/your-org/mntrs/releases

# Docker
docker build -f csi/Dockerfile -t mntrs-csi .
```

### Windows

WinFSP 2.1+ must be installed. Then:

```bash
# Drive letter
mntrs mount s3://bucket X:

# Auto-assign
mntrs mount s3://bucket *

# NTFS directory
mntrs mount s3://bucket C:\mnt\s3
```

---

## Supported Backends

| Scheme | Backend | Auth | Notes |
|--------|---------|------|-------|
| `s3://` | AWS S3 / MinIO / R2 / Ceph | AKID/SK or IAM | Full S3 API |
| `gs://` / `gcs://` | Google Cloud Storage | Service account | |
| `azblob://` | Azure Blob Storage | Connection string / SAS | |
| `hdfs://` / `hdfs-native://` | HDFS (native Rust) | Kerberos via ccache | Default |
| `hdfs-jni://` | HDFS (libhdfs JNI) | Kerberos via options | `--features hdfs-jni` |
| `webhdfs://` | WebHDFS REST | Kerberos / SPENGO | HTTP gateway |
| `oss://` | Alibaba OSS | AKID/SK | |
| `cos://` | Tencent COS | AKID/SK | |
| `obs://` | Huawei OBS | AKID/SK | |
| `b2://` | Backblaze B2 | AKID/SK | |
| `vercel-blob://` | Vercel Blob | Token | |
| `aliyun-drive://` | Aliyun Drive | OAuth | |
| `fs://` / `file://` | Local filesystem | n/a | Passthrough |
| `memory://` / `mem://` | In-memory | n/a | Testing only |

---

## Storage Options

All `--opt key=value` pairs are passed through to the backend. Common keys:

| Key | Description | Example |
|-----|-------------|---------|
| `endpoint` | Service endpoint | `https://s3.custom.com` |
| `access-key` | Access key | `AKIA...` |
| `secret-key` | Secret key | `...` |
| `region` | Region | `us-east-1` |
| `cacert` / `cert` / `key` / `pass` | TLS (curl-compatible) | mTLS supported |
| `insecure` | Skip cert verification | `true` |
| `dfs.namenode.kerberos.*` | HDFS Kerberos config | `hdfs/_HOST@REALM` |
| `storage-class` | S3 default storage class for uploads (S3 backend only) | `GLACIER_IR` — see [`--storage-class`](#storage-class-s3-backend-only) |

### TLS / SSL (curl-compatible)

```bash
# mTLS
mntrs mount s3://bucket /mnt \
  --opt endpoint=https://s3.custom.com \
  --opt cacert=/etc/ca.crt \
  --opt cert=/etc/client.crt \
  --opt key=/etc/client.key

# PKCS12
mntrs mount s3://bucket /mnt \
  --opt cert=/etc/client.p12 --opt cert-type=P12 --opt pass=secret

# Self-signed
mntrs mount s3://bucket /mnt --opt insecure
```

### HDFS Kerberos

Two modes:

**Mode 1** — pre-authenticated (standard `kinit`):

```bash
kinit -kt /etc/security/keytabs/hdfs.keytab hdfs/namenode@REALM
mntrs mount hdfs://namenode:8020 /mnt/hdfs
# hdfs-native auto-detects principal from KRB5CCNAME
```

**Mode 2** — pass via options (hdfs-native):

```bash
mntrs mount hdfs://namenode:8020 /mnt/hdfs \
  --opt dfs.namenode.kerberos.principal=hdfs/_HOST@REALM \
  --opt dfs.namenode.kerberos.keytab=/etc/krb5.keytab
```

**Mode 3** — JNI (requires Java + libhdfs):

```bash
cargo build --features hdfs-jni
mntrs mount hdfs-jni://namenode:8020 /mnt/hdfs \
  --opt kerberos-ticket-cache-path=/tmp/krb5cc \
  --opt user=hdfs
```

---

## `--storage-class` (S3 backend only)

```bash
mntrs mount s3://bucket /mnt --storage-class=GLACIER_IR
```

Sets the default S3 storage class for all uploads in this mount.
The value is forwarded to opendal's S3 builder, which sends
the `x-amz-storage-class` header on PUT / Copy / Multipart.

Valid values: `STANDARD`, `STANDARD_IA`, `ONEZONE_IA`,
`INTELLIGENT_TIERING`, `GLACIER_IR`, `GLACIER`,
`DEEP_ARCHIVE`, `OUTPOSTS`, `REDUCED_REDUNDANCY`. Invalid
values fail at startup (clap value_parser).

Equivalent `--opt` form: `--opt storage-class=GLACIER_IR`.

**Limitations:**

- **S3 backend only.** OSS / COS / OBS / Azblob / GCS
  backends silently ignore the value (their respective
  headers differ; no mntrs flag exists today).
- **Mount-time only.** All uploads in the mount share the
  same class. Use a backend lifecycle policy for
  per-object overrides.
- **Min storage duration charges** apply for IA / GLACIER
  classes — see AWS docs.

See [`docs/vfs-cache-flags.md`](docs/vfs-cache-flags.md#-storage-class-wired--s3-backend-only)
for the full rationale.

---

## Caching

Three-tier cache: **memory → disk → remote**. Block-level (8 MB) indexing. Disk cache survives restarts.

| Flag | CLI default | Code fallback | Effective default | Description |
|------|-------------|---------------|------------------|-------------|
| `--vfs-cache-max-size` | `0` (off) | none (post-#243) | `0` = no LRU | Disk cache upper limit (LRU) |
| `--vfs-cache-min-free-space` | `0` (off) | none (post-#243) | `0` = no floor check | Min free space before eviction |
| `--vfs-cache-max-age` | 3600s | — | 3600s | Max cache file age (absolute mtime, 0 disables — see [Cache flags](docs/vfs-cache-flags.md#vfs-cache-max-age-wired-issue-507)) |
| `--vfs-cache-mode` | `off` | — | `off` | `off` / `minimal` / `writes` / `full` (wired — see [Durability](docs/durability.md#cache-mode-summary)) |
| `--vfs-cache-poll-interval` | 60s | — | 60s | Stale-object poll interval (shadow — see [Durability](docs/durability.md#shadow-fields-rclone-compat-not-implemented)) |
| `--mem-limit` | 256 MB | — | 256 MB | Memory cache upper limit |
| `--dir-cache-time` | 10s | — | 10s | Directory listing TTL |
| `--attr-timeout` | 5s | — | 5s | File attribute TTL (kernel) — bumped 1s→5s (#469) so the #467 FUSE_READDIRPLUS_AUTO cap actually materializes |
| `--stat-cache-ttl` | 1s | — | 1s | Stat TTL (mntrs internal) |
| `--type-cache-ttl` | 1s | — | 1s | File-type cache TTL |
| `--no-modtime` | false | — | false | Disable mtime read on `stat`/`readdir` (writes not pushed to backend anyway; #509) |
| `--use-server-modtime` | false | — | false | Use server-side mtime (vs local cache) |
| `--no-implicit-dir` | false | — | false | Disable S3 implicit dir fallback |
| `--direct-io` | false | — | false | Bypass kernel page cache, direct FUSE access |
| `--vfs-handle-caching` | 0s | — | 0s | Keep file handles open after last close for reuse |
| `--vfs-write-back` | 5s | — | 5s | Max time before dirty file is uploaded |
| `--write-back-cache` | false | — | false | **Opt-in.** FUSE kernel write-back cache. Off by default — daemon's `write()` is called per writeback segment under default. When enabled, the kernel buffers writes and daemon's write handler is skipped for multi-page files (3 known bugs + stress 01/05 fail under unconditional WRITEBACK_CACHE — see `docs/durability.md`). |

> **Issue #243**: `--vfs-cache-max-size` and `--vfs-cache-min-free-space` both have CLI default `0` (= "off") but historically the code path fell back to 1 GiB / 100 MiB when the field was 0. Post-#243.2/3 the `0` value is honored literally (see `src/lib.rs` for the new behavior). If you want a 1 GiB cap, pass `--vfs-cache-max-size 1024` explicitly.

**Disk cache**: write uses file-level cache (`{hash}` hash name), read checks file-level first then block-level (`{hash}_{block}.block`). Recoverable on restart.

---

## Performance

| Flag | CLI default | Effective default | Description |
|------|-------------|------------------|-------------|
| `--vfs-read-chunk-size` | 128 MiB (134217728) | 128 MiB | Initial read chunk size |
| `--vfs-read-chunk-size-limit` | 0 (off) | 128 MiB (fallback) | Chunk doubling ceiling |
| `--vfs-read-chunk-streams` | 1 | 1 | Concurrent read streams (per FUSE read) |
| `--vfs-read-ahead` | 0 | 0 | Wired under `cache-mode=full` — extra lookahead bytes on top of prefetch queue (issue #588); other modes ignore |
| `--async-read` | false | false | Async reads (FUSE kernel) |
| `--vfs-fast-fingerprint` | false | false | Fast change detection (size+mtime) |
| `--vfs-read-wait` | 1s | 1s | Sequential read wait threshold (shadow — see [Durability](docs/durability.md#shadow-fields-rclone-compat-not-implemented)) |
| `--vfs-write-wait` | 1s | 1s | Sequential write wait threshold (wired — coalesces writes on large files, issue #T2-N+1) |
| `--vfs-refresh` | 5m (rclone) | 0 (off) | Periodic remote-state refresh interval (wired; 0 = disabled, issue #592) |

**Adaptive chunk reader**: chunk size doubles on sequential reads, resets to 128 KB on seek. Up to 8 MB cap.

**Prefetcher with backpressure**: 4 in-flight chunks max per file. Replaces naive `thread::spawn` with bounded `PartQueue`.

**Multi-chunk concurrent**: `--vfs-read-chunk-streams=4` fetches 4 S3 parts in parallel via `tokio::Semaphore` for a single FUSE read.

### Benchmark (vs rclone)

4/6 leading, 1/6 tie, 1/6 behind (recoverable by matching `--stat-cache-ttl=300`).

The macOS variant lives at `bench/run_all_mac.sh`. See
`docs/benchmark_macos.md` for methodology and the rclone
auto-detect path. No CI workflow runs it — see issue #304
for the GH runner macFUSE kext limitation.

### Batched S3 deletes (default ON for S3)

For high-fanout `rm -rf` workloads against S3-compatible backends,
mntrs coalesces many deletes into a single S3 `DeleteObjects`
request. **Enabled by default on S3 backends** (Stage C); disable
per-mount with `--unlink-batch=off` or set
`MNTRS_UNLINK_BATCH=0`. Non-S3 backends always use the unbatched
path — the batcher has no S3-protocol backend wired in.

```bash
# default (S3): batched deletes on
mntrs mount "s3://my-bucket" /mnt/data --opt endpoint=http://minio:9000 ...

# explicit OFF — restore strict per-callback S3 DELETE behavior
mntrs mount "s3://my-bucket" /mnt/data --unlink-batch=off --opt endpoint=...

# explicit ON for an S3 mount where MNTRS_UNLINK_BATCH=0 was set
# in the environment
mntrs mount "s3://my-bucket" /mnt/data --unlink-batch=on --opt endpoint=...
```

**Precedence** (top wins):

1. `--unlink-batch=on|off` CLI flag — always wins.
2. `MNTRS_UNLINK_BATCH=1|0` env var — wins over `auto`.
3. `auto` (CLI default when no env var is set):
   - S3 backend → ON (defaults flipped after Stage A's
     1.43× geomean / 1.59× on real workloads).
   - non-S3 backend → OFF.

Observed speedups vs the unbatched path (local MinIO 2025-09-07,
mntrs `--release`):

| Workload              | mntrs unbatched | mntrs batched | speedup |
|-----------------------|----------------:|--------------:|--------:|
| `rm -rf 100 files`    |          85 ms  |        47 ms  |   1.89× |
| `rm -rf 500 files`    |         491 ms  |       318 ms  |   1.54× |
| `rm -rf 60-file deep` |          42 ms  |        22 ms  |   2.90× |
| `rm -rf mixed 52/15M` |          74 ms  |        67 ms  |   1.10× |

(Geomean 1.44× across the 7 rm workloads tested.)

**Semantics**: write-behind. The user's `rm` returns success
before S3 confirms the delete. Per-key failures are logged, not
surfaced. A tombstone in lookup/getattr/readdir masks the
in-flight delete from the local FUSE view; the worker removes
the tombstone on every per-key ack (success, idempotent
NotFound, or permanent failure with an `error!` line). `rmdir`
is a barrier (enqueue + flush().await) so the directory's
deletes don't outlive the rmdir callback.

**Recreate-after-rm**: `rm X && touch X` works without ENOENT.
`create()` and `mkdir()` call `BatchedDeleter::cancel_pending()`
before `op.write` — drains any queued S3 DELETE for the path,
removes the tombstone, and completes the cancelled oneshot with
`ErrorKind::Interrupted`. Without this the in-flight S3 DELETE
would race the new write and either wipe the freshly created
file (data loss) or leave lookup returning ENOENT until the
worker acked the original delete.

**Tuning**:

| Env var                        | Default | Range    | Effect |
|--------------------------------|--------:|---------:|--------|
| `MNTRS_UNLINK_BATCH`           | unset   | 0/1      | Legacy env gate (Stage B). Use `--unlink-batch=` instead. |
| `MNTRS_BATCH_SIZE`             | 100     | 1..1000  | Threshold for immediate flush (S3 hard limit is 1000) |
| `MNTRS_BATCH_FLUSH_DELAY_MS`   | 50      | 1..10000 | ms to wait after first enqueue before deadline flush |
| `MNTRS_BATCH_WORKER_COUNT`     | 4       | 1..16    | Concurrent flushers (matches `rclone --transfers=4`) |
| `MNTRS_BATCH_PROFILE`          | auto    | auto / small / medium / bulk | Runtime profile selection (issue #562 Stage 3) |

Lower `MNTRS_BATCH_FLUSH_DELAY_MS` (e.g. 10) for latency-sensitive
workloads; the trade-off is more `DeleteObjects` requests with
fewer keys each. Higher values (default 50) maximize batch fill.

**Stage 3: workload-adaptive profiles** (issue #562):

The `MNTRS_BATCH_PROFILE` env var picks how the batcher
classifies the current workload. Three profiles map to three
(batch_size, flush_delay, fast_flush_threshold) triples:

| Profile | batch_size | flush_delay | fast_flush_threshold | Use case |
|---------|-----------:|------------:|---------------------:|----------|
| `small` | 20         | 10 ms       | 4                    | Sparse: IDE saves, single-file unlinks |
| `medium`| 100        | 50 ms       | 8                    | Mixed: `rm` of small dirs interleaved with other ops |
| `bulk`  | 500        | 200 ms      | 32                   | Large `rm -rf`, cleanup scripts |

`auto` (default) lets the batcher choose the profile at runtime
based on a sliding-window `p95` of `pending.len()`. Hysteresis
(p95 must exceed 50 to flip toward `bulk` or fall below 5 to
flip toward `small`) and a 5-second cooldown between transitions
keep a single `rm -rf` from bouncing the system. Profile flips
are logged via `tracing::info!` on the `mntrs::batched_delete`
target:

```
batched_delete: profile transition from=Medium to=Bulk hint_p95=87 cooldown_ms=5000
```

Pinning a specific profile (`MNTRS_BATCH_PROFILE=medium`) makes
that profile's `(batch_size, flush_delay, fast_flush_threshold)`
the only values used at runtime — equivalent to the pre-Stage-3
behaviour for that profile's triple. Use this when the auto
classifier's hint doesn't match your workload (e.g. CI benches
that always issue the same `rm -rf` shape).

**Stage 5: ThresholdCalibrator** (issue #562):

A memory-only background task reads `CounterSnapshot` +
`BurstObserver::p95()` every 60 s and emits
`tracing::info!` recommendations when the running workload
suggests the active profile is mis-configured. **The
calibrator never auto-applies** — it logs the proposed
adjustment and bumps `calibrator_recommendations_total`;
the operator decides whether to set `MNTRS_BATCH_SIZE` /
`MNTRS_BATCH_FAST_FLUSH_THRESHOLD` on the next mount.

Recommendation triggers:

| Signal | Trigger | Recommendation |
|---|---|---|
| `retry_total / flushes_total >= 5%` | retry rate is high; per-key overhead dominates | `RaiseBatchSize` (current × 1.25, clamped 1..=1000) |
| `avg_chunk_size < 2 && burst_p95 < 5` | sustained small-burst workload | `LowerFastFlushThreshold` (→ 1) |
| `batch_size > 100 && retry_rate < 1% && avg_chunk_size < batch_size/8` | batch_size is too large for the workload | `LowerBatchSize` (current / 2, clamped 1..=1000) |

Safety guards:

- **Cold-start silence**: no recommendations until
  `flushes_total >= 100` (avoids mis-firing during the
  first few minutes of a fresh mount).
- **Hysteresis**: at most one recommendation per
  10-minute window regardless of input noise.
- **Never auto-applies**: the calibrator is
  observation-only. To act on a recommendation, set
  `MNTRS_BATCH_SIZE` / `MNTRS_BATCH_FAST_FLUSH_THRESHOLD`
  on the next mount and observe whether the new nightly
  ratios improve.

Example log line:

```
batched_delete: calibrator recommendation (memory-only, NOT auto-applied; set MNTRS_BATCH_SIZE / MNTRS_BATCH_FAST_FLUSH_THRESHOLD on next mount to act)
  recommendation=lower_fast_flush_threshold current=8 proposed=1
  avg_chunk_size=1 retry_rate_bps=12 burst_p95=3 current_profile=Medium
  flushes_total=1247
```

`calibrator_recommendations_total` is exposed on the
snapshot so future `/metrics` consumers (Stage 4) can
chart it. A zero value over a 24h run means the active
profile is well-fit and no adjustment is suggested.

**Counters** (process-static, log-scrapable via
`mntrs::batched_delete` target):

- `flushes_total` — successful `DeleteObjects` calls.
- `keys_total` — keys sent across all flushes.
- `single_key_batches_total` — flushes with batch_size = 1
  (overhead indicator; lower is better).
- `max_batch_size_observed` — largest batch ever sent.
- `failures_total` — per-key permanent failures (`AccessDenied`,
  non-idempotent `NoSuchKey`, etc.). Expect 0 in steady state.
- `shutdown_lost_total` — keys dropped on unclean shutdown
  (drain=false or channel close without explicit shutdown).
- `threshold_skipped_total` — enqueue calls routed to the strict
  `delete_backend_strict` path because the pending queue was
  below `MNTRS_BATCH_THRESHOLD` (issue #530).
- `fast_flush_total` — flushes fired via the fast-flush branch
  (pending.len() < fast_flush_threshold at decision time,
  issue #553).
- `profile_transitions_total` — profile flips under
  `MNTRS_BATCH_PROFILE=auto` (issue #562 Stage 3). Under
  steady-state workload this should stay low (< 10/hour);
  high values indicate the burst classifier is oscillating.
- `single_key_fast_delete_total` — single-key flushes that
  used the plain `DELETE /bucket/key` short-circuit instead
  of the multi-key `DeleteObjects` XML path (issue #562
  Stage 1.5). Active when the running profile is `Small`
  (the default for sparse workloads); Medium / Bulk keep
  the XML path because per-key amortisation makes the
  short-circuit's complexity not worth it.
- `retry_total` — retry decisions across both the multi-key
  XML path and the single-key DELETE path (issue #562
  Stage 5 input). `retry_rate_bps = retry_total * 10000 /
  flushes_total`; the Calibrator triggers `RaiseBatchSize`
  when this exceeds 500 (5 %).
- `chunk_size_sum` — cumulative sum of `batch.len()` across
  every flush (issue #562 Stage 5 input). `avg_chunk_size
  = chunk_size_sum / flushes_total`; the Calibrator uses
  it to detect over-batched workloads.
- `calibrator_recommendations_total` — number of
  `tracing::info!` recommendation lines emitted by the
  Calibrator (issue #562 Stage 5). Zero over a 24h run
  means the active profile is well-fit.

Enable with `RUST_LOG=info,mntrs::batched_delete=info`.

**When to disable**: workloads dominated by rare single-file
deletes, where the 50 ms flush deadline is pure overhead. The
unbatched path's per-callback DELETE is faster for those.
Set `--unlink-batch=off` or `MNTRS_UNLINK_BATCH=0`.

See `docs/plan64_stage_a_results.md` for the full measurement
methodology and `bench/unlink_ab.sh` to reproduce locally.

---

## Write

Local write cache with async write-back (5s default delay). Crash-safe.

```bash
# Write
echo "hello" > /mnt/s3/file.txt     # cached + async write-back
cat /mnt/s3/file.txt               # served from cache (hot)

# Sync
sync /mnt/s3/file.txt              # fdatasync → cache file durable on local disk
```

**Mechanisms**:

- **Write-back queue** with exponential backoff (5 attempts; cycle cap 10 at 60s cooldown)
- **fdatasync on flush/release** before the FUSE reply (`Issue #34` — local-durability half)
- **`.dirty` sidecar** for crash recovery (scanned on mount init; left on disk for retry-exhausted paths)
- **`PendingUploadHook`** updates inode size/mtime after successful upload
- **Retry-cycle counter** (4th tuple field, `Issue #53`) prevents silent re-enqueue drops
- **Multipart upload** via `op.writer()` auto-chunks >5 GB
- **CRC64 integrity** for disk cache

For the full durability model — including the remaining rclone-compat
shadow fields (`--vfs-cache-mode`, `--vfs-cache-poll-interval`,
`--poll-interval`, `--vfs-refresh`) that are accepted on the CLI but
not yet implemented — see [`docs/durability.md`](docs/durability.md).
`--vfs-cache-max-age` was wired in issue #507; see
[`docs/vfs-cache-flags.md`](docs/vfs-cache-flags.md#vfs-cache-max-age-wired-issue-507).

---

## Platform Features

### Object Metadata (xattr)

mntrs exposes backend object metadata as FUSE extended attributes on
every file **when `--metadata` is passed** (default **off**, matching
`rclone mount` parity — rclone also defaults `--metadata` to false in
`mount` and true in `serve`). The attribute names follow
[rclone's `--metadata`](https://rclone.org/docs/#metadata) convention
so any tool written for rclone just works:

| xattr name | Source | Notes |
|------------|--------|-------|
| `user.etag` | backend ETag | surrounding quotes stripped (S3 returns `"..."` over the wire) |
| `user.mime_type` | backend `Content-Type` | `user.content-type` is accepted as a backward-compat alias |
| `user.mtime` | backend `Last-Modified` | ISO-8601 |
| `user.content_length` | backend object size | decimal bytes |
| `user.<key>` | backend user metadata | key is normalized: lowercased, dots replaced with underscores (macOS FUSE rejects dots in xattr names) |

**Default is off** to match `rclone mount`. Pass `--metadata` to enable,
or `--no-metadata` to explicitly disable (accepted for rclone-script
parity; equivalent to omitting `--metadata`). When disabled, `getxattr`
returns `ENOSYS` and `listxattr` returns the empty list — avoiding the
per-call backend `stat()` round-trip the surface otherwise requires.

`listxattr` returns only the attributes the backend actually populated
for the object — empty for directories, and on backends without an
ETag/`Content-Type`/`Last-Modified` the corresponding attributes are
absent rather than stubbed. Names are returned in sorted order so
`getfattr -d -m '^user\.'` output is deterministic.

Tools that consume this surface:

```bash
# Show all metadata xattrs for a file
getfattr -d -m '^user\.' /mnt/s3/path/to/file

# Use it from a script
etag=$(stat -c %i /mnt/s3/path/to/file >/dev/null 2>&1 && \
       getfattr --absolute-names -n user.etag /mnt/s3/path/to/file \
         | awk -F'"' '/^user.etag=/ {print $2}')
```

FUSE size queries (`size=0` form passed by the kernel to ask "how big
would this attribute be?") are honored — `getxattr` returns the
attribute size without copying the value bytes, and an undersized
buffer returns `ERANGE` instead of truncating.

**macFUSE 64 KiB truncation warning** (issue #502): macFUSE silently
truncates single xattr values above the documented 64 KiB cap; the
FUSE write returns success but the backend only sees the truncated
bytes, which silently corrupts metadata round-trips such as
`user.author` written by `xattr -w` / Spotlight. When a `setxattr`
value exceeds the configured cap (default 64 KiB), mntrs emits a
`tracing::warn!` with the xattr name and observed byte length
*before* the write is forwarded to the backend. The write still
proceeds — silent metadata corruption is the failure mode this
warning exists to surface, not to block. Use
`--max-xattr-size=<bytes>` to tune the cap (set `0` to disable the
warning entirely when you've confirmed your kernel/backend
combination handles large values correctly). **Strongly consider
writing large metadata to a backend with an explicit size limit**
rather than relying on automatic truncation.

### Daemon Mode

```bash
mntrs mount s3://bucket /mnt/s3 --daemon
mntrs mount s3://bucket /mnt/s3 --daemon --daemon-wait
```

### Systemd

`mntrs install systemd` generates a systemd user service template. `Restart=always` + `ExecStopPost` lazy unmount for crash-safe operation.

### macOS

| Flag | Description |
|------|-------------|
| `--vfs-noapple-double` | Filter `._*` and `.DS_Store` files (Time Machine) |
| `--vfs-noapple-xattr` | Filter `com.apple.*` xattrs |
| `--mount-case-insensitive` | OS-level case-insensitive mount |

### Windows (WinFSP)

Native support via `winfsp = "0.13"`. Conditional compilation (`#[cfg(windows)]`).

```bash
# Drive letter (recommended)
mntrs mount s3://bucket X:

# Auto-assign
mntrs mount s3://bucket *

# NTFS directory
mntrs mount s3://bucket C:\mnt\s3
```

CI tested on Windows with 31 WinFSP integration tests (covering mount/unmount lifecycle, write/read roundtrip, list/create/delete/rename, setattr/truncate, statfs, nested directories, large-file reads, unicode + NFC normalization, symlink create/get/rename/delete, dirty-cache lifecycle, readdir paging, getattr/statfs cache coalescing, volume flush, and mount-internal scheme variants).

### Kubernetes (CSI)

`csi/mntrs-csi/` — Pure Rust CSI driver (tonic 0.12).

```bash
kubectl apply -f csi/deploy/kubernetes/1.20/
```

StorageClass + PVC example:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: mntrs-s3
provisioner: csi-mntrs
parameters:
  storage: "s3://my-bucket"
  prefix: "k8s-pv"
  --opt s3-endpoint=http://minio:9000
reclaimPolicy: Retain
volumeBindingMode: Immediate
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-data
spec:
  storageClassName: mntrs-s3
  accessModes: [ReadWriteMany]
  resources:
    requests: { storage: 1Gi }
```

CSI services:
- **Identity**: `GetPluginInfo` / `GetPluginCapabilities` / `Probe`
- **Controller**: `CreateVolume` / `DeleteVolume` (real implementation)
- **Node**: `NodeStageVolume` / `NodePublishVolume` / `NodeUnstageVolume` / `NodeUnpublishVolume` with per-volume cache dir, write-back wait, and lazy unmount

---

## Architecture

```
src/
├── lib.rs                 # MntrsFs core + fuser impl (Linux/macOS)
├── main.rs                # CLI entry
├── path.rs                # Cross-platform path normalization
├── prefetcher.rs          # PartQueue + backpressure
├── writeback.rs           # Async write-back + CRC64 + PendingUploadHook
├── cmd/
│   ├── mod.rs
│   ├── mount.rs           # Multi-backend routing + TLS + daemon
│   ├── unmount.rs         # Unmount (lazy for safety)
│   ├── list.rs            # List active mounts
│   └── install.rs         # Systemd template generator
└── core_fs/
    ├── mod.rs             # CoreFilesystem trait
    ├── fuser.rs           # FuserAdapter (Linux/macOS)
    └── winfsp.rs          # WinfspAdapter (Windows)

csi/mntrs-csi/
├── Cargo.toml
├── build.rs               # protoc + tonic_build
├── src/
│   ├── main.rs            # 4 CSI services + lifecycle
│   └── csi.rs             # Generated protobuf
└── csi/deploy/kubernetes/  # K8s manifests
```

**Data flow** (FUSE read):

```
FUSE read(ino, offset, size)
  ↓
1. inodes cache hit? → make_attr (fast path)
  ↓ miss
2. attr_cache hit? → make_attr
  ↓ miss
3. network stat() → attr_cache.insert
  ↓
4. cache fd (write handle still open)? → read from fd → return
  ↓ miss
5. mem_cache[(ino, block_idx)]? → return block
  ↓ miss
6. file-level disk cache → mem_cache insert → return
  ↓ miss
7. block-level disk cache (CRC64 verify) → mem_cache insert → return
  ↓ miss
8. prefetcher PartQueue pop → return chunk
  ↓ miss
9. multi-chunk fetch (Semaphore N streams) → disk + mem insert
```

---

## Development

```bash
# Build
cargo build --release

# Test (all 50+ tests)
cargo test --workspace
cargo nextest run --workspace    # 30-50% faster

# Lint
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check

# Backend-specific builds
cargo build --features hdfs-jni       # HDFS via libhdfs

# CSI plugin
cargo build --package mntrs-csi --release

# Benchmarks
cargo bench                         # micro-benchmarks
./bench/run_all.sh                  # vs rclone (MinIO, Linux)
./bench/run_all_mac.sh              # macOS variant (manual, see docs/benchmark_macos.md)
```

### CI Matrix (GitHub Actions)

| Workflow | Environment | Scope |
|----------|-------------|-------|
| `CI` | Linux | Build + test + clippy + fmt |
| `CI - Windows` | Windows | WinFSP + release build + 31 mount integration tests |
| `CI - macOS` | macOS | macFUSE + build + test |
| `Integration Tests` | Linux | S3 / HDFS / memory mount tests + HDFS Kerberos auth |
| `CSI Integration Test` | Linux (k3s) | CSI driver e2e with HDFS backend |
| `CSI e2e` | Linux (k3s) | CSI driver e2e with S3 (MinIO) backend |
| `Benchmark` | Linux | vs rclone performance (MinIO) |
| macOS bench (manual) | macOS developer | `bench/run_all_mac.sh` (issue #304 — no GH runner support) |

---

## Compatibility

| Component | Requirement |
|-----------|-------------|
| Rust | 1.87+ (edition 2024) |
| Linux | FUSE 3 (`libfuse3-dev fuse3`) |
| macOS | macFUSE 4+ |
| Windows | WinFSP 2.1+ |
| Kubernetes | 1.20+ (external-provisioner) |
| HDFS-JNI | Java 11+, libhdfs3 |
| protoc | For CSI builds |

---

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
