# mntrs

> Mount remote storage (S3, GCS, HDFS, Azure Blob, etc.) to local directory via FUSE.
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

# MinIO (自签名CA)
mntrs mount s3://bucket /mnt/s3 \
  --opt endpoint=https://minio.local:9000 \
  --opt cacert=/etc/ca.crt \
  --opt insecure

# HDFS
mntrs mount hdfs://namenode:8020 /mnt/hdfs

# GCS
mntrs mount gs://my-bucket /mnt/gcs

# 卸载
mntrs unmount /mnt/s3
```

---

## Installation

### From source

```bash
cargo install --path .
```

### Docker

```bash
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

所有 `--opt key=value` 透传给后端。常用参数：

| Key | 说明 | 示例 |
|-----|------|------|
| `endpoint` | 服务端点 | `https://s3.custom.com` |
| `access-key` | 访问密钥 | `AKIA...` |
| `secret-key` | 秘密密钥 | `...` |
| `region` | 区域 | `us-east-1` |

### TLS / SSL (curl 兼容)

| Opt | 对应 curl | 说明 |
|-----|-----------|------|
| `cacert=<path>` | `--cacert` | CA 证书（自签名） |
| `cert=<path>` | `--cert` | 客户端证书（mTLS） |
| `key=<path>` | `--key` | 客户端私钥 |
| `pass=<phrase>` | `--pass` | 私钥密码 |
| `cert-type=<type>` | `--cert-type` | 证书类型: PEM/DER/P12 |
| `insecure` | `-k` | 跳过证书验证 |

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

# 跳过验证（内网）
mntrs mount s3://bucket /mnt/s3 \
  --opt endpoint=https://minio.local:9000 \
  --opt insecure
```

### HDFS Kerberos

```bash
# native Kerberos
mntrs mount hdfs://namenode:8020 /mnt/hdfs \
  --opt dfs.namenode.kerberos.principal=hdfs/_HOST@REALM \
  --opt dfs.namenode.kerberos.keytab=/etc/krb5.keytab

# HA 集群
mntrs mount hdfs://namenode1:8020,namenode2:8020 /mnt/hdfs

# JNI (需 --features hdfs-jni)
mntrs mount hdfs-jni://namenode:8020 /mnt/hdfs \
  --opt kerberos-ticket-cache-path=/tmp/krb5cc \
  --opt user=hdfs
```

### Caching

| Flag | Default | 说明 |
|------|---------|------|
| `--vfs-cache-max-size` | 1024 MB | 磁盘缓存上限 |
| `--mem-limit` | 256 MB | 内存缓存上限 |
| `--dir-cache-time` | 10s | 目录缓存 TTL |
| `--attr-timeout` | 1s | 属性缓存 TTL |
| `--stat-cache-ttl` | 1s | stat 缓存 TTL |
| `--poll-interval` | 60s | 远程轮询间隔 |
| `--vfs-cache-mode` | writes | off/writes/full |
| `--no-modtime` | false | 不读写修改时间 |
| `--use-server-modtime` | false | 用服务端 mtime |
| `--vfs-cache-min-free-space` | 100 MB | 缓存最小剩余空间 |
| `--vfs-cache-max-age` | 3600s | 缓存文件最大年龄 |

缓存分 3 级: **mem → disk → remote**。8MB 块级索引，重启后缓存自动恢复。

### Performance

| Flag | Default | 说明 |
|------|---------|------|
| `--vfs-read-chunk-size` | 0 (auto) | 读块大小 |
| `--vfs-read-ahead` | 131072 | 预读字节数 |
| `--vfs-read-chunk-streams` | 1 | 并发读流数 |
| `--async-read` | false | 异步读 |
| `--vfs-fast-fingerprint` | false | 快速指纹 (size+mtime) |

Prefetcher 后台预取 + 自适应翻倍（顺序读翻倍至 8MB）+ backpressure。

### Write

写时缓存到本地，异步写回远程（5s 延迟）。支持：

- 指数退避重试（3 次）
- Crash recovery（`.dirty` sidecar）
- PendingUploadHook（上传后更新 inode）
- fsync 语义（flush 等待队列清空）
- Multipart upload（OpenDAL Writer 自动分片 >5GB）

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

`csi/mntrs-csi/` — 纯 Rust CSI driver，支持 Identity / Controller / Node 服务。

```bash
# 部署 (见 csi/deploy/kubernetes/1.20/)
kubectl apply -f csi/deploy/kubernetes/1.20/
```

```yaml
# PV 示例
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

`#[cfg(windows)]` 条件编译。需安装 WinFSP 2.1+。

```bash
mntrs mount s3://bucket X:     # 指定盘符
mntrs mount s3://bucket *       # 自动分配
mntrs mount s3://bucket C:\mnt\s3  # NTFS 目录
```

---

## Architecture

```
src/
  lib.rs              MntrsFs core + fuser impl + CoreFilesystem impl
  main.rs             CLI entry
  path.rs             路径归一化
  prefetcher.rs       后台预取 + PartQueue backpressure
  writeback.rs        异步写回 + PendingUploadHook
  cmd/mount.rs        多后端路由 + TLS
  cmd/unmount.rs      卸载
  core_fs/
    mod.rs            CoreFilesystem trait
    fuser.rs          Linux/macOS FUSE adapter
    winfsp.rs         Windows WinFSP adapter (cfg windows)

tests/                46 tests
  cache_test.rs       9 (纯函数)
  vfs_test.rs         11 (statfs/read/cache)
  hdfs_integration    5 (HDFS路由/Kerberos/HA)
  csi_integration     10 (CSI gRPC)
  winfsp_integration  11 (WinFSP mount)
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

CI 矩阵 (GitHub Actions):

| Workflow | 环境 | 内容 |
|----------|------|------|
| ci-core | Linux | FUSE + CSI build + test + clippy + fmt |
| ci-hdfs | Linux | Java + libhdfs3 + --features hdfs-jni |
| ci-windows | Windows | WinFSP + release build + integration test |
| bench | Linux | vs rclone benchmark |
| integration | Linux | KDC + miniDFS + csi-sanity |

---

## License

Apache 2.0
