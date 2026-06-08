# mntrs

> Mount remote storage (S3, GCS, HDFS, Azure Blob, etc.) via FUSE.
>
> Linux / macOS / Windows (WinFSP) / Kubernetes (CSI)

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

# HDFS
mntrs mount hdfs://namenode:8020 /mnt/hdfs

# GCS
mntrs mount gs://my-bucket /mnt/gcs

# Unmount
mntrs unmount /mnt/s3
```

---

## Installation

```bash
# From source
cargo install --path .

# Docker
docker build -f csi/Dockerfile -t mntrs .
```

---

## Supported Backends

| Scheme | Backend | Default |
|--------|---------|---------|
| `s3://` | AWS S3 / MinIO / R2 | ✅ |
| `gs://` / `gcs://` | Google Cloud Storage | ✅ |
| `azblob://` | Azure Blob Storage | ✅ |
| `hdfs://` / `hdfs-native://` | HDFS (native Rust) | ✅ |
| `hdfs-jni://` | HDFS (libhdfs JNI) | `--features hdfs-jni` |
| `webhdfs://` | WebHDFS REST API | ✅ |
| `oss://` | Alibaba OSS | ✅ |
| `cos://` | Tencent COS | ✅ |
| `obs://` | Huawei OBS | ✅ |
| `b2://` | Backblaze B2 | ✅ |
| `vercel-blob://` | Vercel Blob | ✅ |
| `aliyun-drive://` | Aliyun Drive | ✅ |
| `fs://` / `file://` | Local filesystem | ✅ |
| `memory://` | In-memory (testing) | ✅ |

---

## Features

### Storage Options

All `--opt key=value` pairs are passed through to the backend.

| Key | Description | Example |
|-----|-------------|---------|
| `endpoint` | Service endpoint | `https://s3.custom.com` |
| `access-key` | Access key | `AKIA...` |
| `secret-key` | Secret key | `...` |
| `region` | Region | `us-east-1` |

### TLS / SSL (curl compatible)

| Opt | curl equivalent | Description |
|-----|-----------------|-------------|
| `cacert=<path>` | `--cacert` | CA certificate (self-signed) |
| `cert=<path>` | `--cert` | Client certificate (mTLS) |
| `key=<path>` | `--key` | Client private key |
| `pass=<phrase>` | `--pass` | Private key password |
| `cert-type=<type>` | `--cert-type` | Cert type: PEM/DER/P12 |
| `insecure` | `-k` | Skip certificate verification |

```bash
# mTLS
mntrs mount s3://bucket /mnt/s3 \
  --opt endpoint=https://s3.custom.com \
  --opt cacert=/etc/ca.crt \
  --opt cert=/etc/client.crt \
  --opt key=/etc/client.key

# PKCS12
mntrs mount s3://bucket /mnt/s3 \
  --opt endpoint=https://s3.custom.com \
  --opt cacert=/etc/ca.crt \
  --opt cert=/etc/client.p12 \
  --opt cert-type=P12 \
  --opt pass=secret

# Skip verification (internal network)
mntrs mount s3://bucket /mnt/s3 \
  --opt endpoint=https://minio.local:9000 \
  --opt insecure
```

### HDFS Kerberos

```bash
# Native Kerberos
mntrs mount hdfs://namenode:8020 /mnt/hdfs \
  --opt dfs.namenode.kerberos.principal=hdfs/_HOST@REALM \
  --opt dfs.namenode.kerberos.keytab=/etc/krb5.keytab

# HA cluster
mntrs mount hdfs://namenode1:8020,namenode2:8020 /mnt/hdfs

# JNI (requires --features hdfs-jni)
mntrs mount hdfs-jni://namenode:8020 /mnt/hdfs \
  --opt kerberos-ticket-cache-path=/tmp/krb5cc \
  --opt user=hdfs
```

### Caching

| Flag | Default | Description |
|------|---------|-------------|
| `--vfs-cache-max-size` | 1024 MB | Disk cache upper limit |
| `--mem-limit` | 256 MB | Memory cache upper limit |
| `--dir-cache-time` | 10s | Directory cache TTL |
| `--attr-timeout` | 1s | Attribute cache TTL |
| `--stat-cache-ttl` | 1s | Stat cache TTL |
| `--poll-interval` | 60s | Remote poll interval |
| `--vfs-cache-mode` | writes | off/writes/full |
| `--no-modtime` | false | Disable mtime read/write |
| `--use-server-modtime` | false | Use server-side mtime |
| `--vfs-cache-min-free-space` | 100 MB | Min free space before eviction |
| `--vfs-cache-max-age` | 3600s | Max cache file age |

Three cache levels: **mem → disk → remote**. 8MB block-level indexing. Cache survives restarts.

### Performance

| Flag | Default | Description |
|------|---------|-------------|
| `--vfs-read-chunk-size` | 0 (auto) | Read chunk size |
| `--vfs-read-ahead` | 131072 | Read-ahead bytes |
| `--vfs-read-chunk-streams` | 1 | Concurrent read streams |
| `--async-read` | false | Async reads |
| `--vfs-fast-fingerprint` | false | Fast fingerprint (size+mtime) |

Background prefetcher with adaptive doubling (up to 8MB on sequential reads) + backpressure.

### Write

Local write cache with async write-back (5s delay). Supports:

- Exponential backoff retry (3 attempts)
- Crash recovery (`.dirty` sidecar)
- PendingUploadHook (updates inode after upload)
- fsync semantics (flush waits for queue drain)
- Multipart upload (OpenDAL Writer auto-chunks >5GB)

### Daemon

```bash
mntrs mount s3://bucket /mnt/s3 --daemon
mntrs mount s3://bucket /mnt/s3 --daemon --daemon-wait
```

### Systemd

```bash
mntrs install systemd
```

### Kubernetes CSI

`csi/mntrs-csi/` — Pure Rust CSI driver with Identity / Controller / Node services.

```bash
kubectl apply -f csi/deploy/kubernetes/1.20/
```

```yaml
apiVersion: v1
kind: PersistentVolume
spec:
  csi:
    driver: csi-mntrs
    volumeHandle: data-id
    volumeAttributes:
      storage: "s3://my-bucket"
      prefix: "k8s-pv/data"
      s3-endpoint: "http://minio:9000"
```

### Windows (WinFSP)

`#[cfg(windows)]` conditional compilation. Requires WinFSP 2.1+.

```bash
mntrs mount s3://bucket X:           # Drive letter
mntrs mount s3://bucket *            # Auto-assign
mntrs mount s3://bucket C:\mnt\s3    # NTFS directory
```

---

## Architecture

```
src/
  lib.rs              MntrsFs core + fuser impl + CoreFilesystem impl
  main.rs             CLI entry
  path.rs             Path normalization
  prefetcher.rs       Background prefetcher + PartQueue backpressure
  writeback.rs        Async write-back + PendingUploadHook
  cmd/mount.rs        Multi-backend routing + TLS
  cmd/unmount.rs      Unmount
  core_fs/
    mod.rs            CoreFilesystem trait
    fuser.rs          Linux/macOS FUSE adapter
    winfsp.rs         Windows WinFSP adapter (cfg windows)

tests/                71+ tests (Linux)
  cache_test          9 (pure functions)
  vfs_test            22 (statfs/read/cache)
  hdfs_integration    9 (HDFS routing/Kerberos/HA)
  csi_integration     14 (CSI gRPC)
  platform            7 (Linux/macOS/Windows)
  winfsp_integration  15 (WinFSP mount, cfg windows)
```

---

## Development

```bash
cargo build
cargo test
cargo clippy -- -D warnings

# HDFS-JNI
cargo build --features hdfs-jni

# CSI plugin
cargo build --package mntrs-csi
```

### CI Matrix (GitHub Actions)

| Workflow | Environment | Scope |
|----------|-------------|-------|
| ci-core | Linux | FUSE + CSI build + test + clippy + fmt |
| ci-hdfs | Linux | Java + libhdfs3 + --features hdfs-jni |
| ci-windows | Windows | WinFSP + release build + integration |
| bench | Linux | vs rclone benchmark |
| integration | Linux | KDC + miniDFS + csi-sanity |

---

## License

Apache 2.0
