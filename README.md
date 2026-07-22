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

## Caching

Three-tier cache: **memory → disk → remote**. Block-level (8 MB) indexing. Disk cache survives restarts.

| Flag | CLI default | Code fallback | Effective default | Description |
|------|-------------|---------------|------------------|-------------|
| `--vfs-cache-max-size` | `0` (off) | none (post-#243) | `0` = no LRU | Disk cache upper limit (LRU) |
| `--vfs-cache-min-free-space` | `0` (off) | none (post-#243) | `0` = no floor check | Min free space before eviction |
| `--vfs-cache-max-age` | 3600s | — | 3600s | Max cache file age (shadow — see [Durability](docs/durability.md#shadow-fields-rclone-compat-not-implemented)) |
| `--vfs-cache-mode` | `off` | — | `off` | `off` / `minimal` / `writes` / `full` (shadow — see [Durability](docs/durability.md#shadow-fields-rclone-compat-not-implemented)) |
| `--vfs-cache-poll-interval` | 60s | — | 60s | Stale-object poll interval (shadow — see [Durability](docs/durability.md#shadow-fields-rclone-compat-not-implemented)) |
| `--mem-limit` | 256 MB | — | 256 MB | Memory cache upper limit |
| `--dir-cache-time` | 10s | — | 10s | Directory listing TTL |
| `--attr-timeout` | 5s | — | 5s | File attribute TTL (kernel) — bumped 1s→5s (#469) so the #467 FUSE_READDIRPLUS_AUTO cap actually materializes |
| `--stat-cache-ttl` | 1s | — | 1s | Stat TTL (mntrs internal) |
| `--type-cache-ttl` | 1s | — | 1s | File-type cache TTL |
| `--no-modtime` | false | — | false | Disable mtime read/write |
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
| `--vfs-read-ahead` | 131072 | 131072 | Bytes prefetched past EOF |
| `--async-read` | false | false | Async reads (FUSE kernel) |
| `--vfs-fast-fingerprint` | false | false | Fast change detection (size+mtime) |
| `--vfs-read-wait` | 1s | 1s | Sequential read wait threshold |
| `--vfs-write-wait` | 1s | 1s | Sequential write wait threshold |

**Adaptive chunk reader**: chunk size doubles on sequential reads, resets to 128 KB on seek. Up to 8 MB cap.

**Prefetcher with backpressure**: 4 in-flight chunks max per file. Replaces naive `thread::spawn` with bounded `PartQueue`.

**Multi-chunk concurrent**: `--vfs-read-chunk-streams=4` fetches 4 S3 parts in parallel via `tokio::Semaphore` for a single FUSE read.

### Benchmark (vs rclone)

4/6 leading, 1/6 tie, 1/6 behind (recoverable by matching `--stat-cache-ttl=300`).

The macOS variant lives at `bench/run_all_mac.sh`. See
`docs/benchmark_macos.md` for methodology and the rclone
auto-detect path. No CI workflow runs it — see issue #304
for the GH runner macFUSE kext limitation.

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

For the full durability model — including the rclone-compat shadow
fields (`--vfs-cache-mode`, `--vfs-cache-max-age`,
`--vfs-cache-poll-interval`, `--poll-interval`, `--vfs-refresh`)
that are accepted on the CLI but not yet implemented — see
[`docs/durability.md`](docs/durability.md).

---

## Platform Features

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
