# mntrs TODO2 — 借鉴 mountpoint-s3 / goofys / geesefs 的改进项

> 来源项目：
> - **mountpoint-s3** — AWS 官方 S3 FUSE (Rust, 9620 行)
> - **goofys** — S3 FUSE (Go, POSIX 兼容)
> - **geesefs** — Azure/GCS FUSE (Go, goofys 分支)

## 🔴 高优先级

| 功能 | 说明 | 来源 |
|------|------|------|
| `statfs` 实现 | 让 `df -h` 有输出，返回合理值（如 1PB 空间 + 1B inodes） | mountpoint-s3, goofys, geesefs |
| S3 xattr | 通过 `getxattr`/`listxattr` 暴露 `etag`、`storage-class`、`content-type` 等 S3 元数据 | goofys, mountpoint-s3 |
| `--storage-class` | 创建对象时指定 S3 存储类（STANDARD/IA/GLACIER 等），作为 upload 参数透传 | goofys |
| `--type-cache-ttl` | 分离目录类型缓存 TTL 和属性缓存 TTL | goofys |
| 八进制权限输入 | `--dir-perms`/`--file-perms` 支持八进制（如 0777）而非仅十进制 | goofys/geesefs |


## 🟡 中优先级 (1项)

| 功能 | 说明 | 来源 |
|------|------|------|
| Prefetcher backpressure | 下载流控 + part queue 防止内存溢出 | mountpoint-s3 |

## ✅ 已借鉴完成 (9项)

| 功能 | 来源 |
|------|------|
| statfs / df -h 输出 | mountpoint-s3, goofys |
| S3 xattr (etag/content-type) | goofys, mountpoint-s3 |
| --storage-class | goofys |
| --type-cache-ttl / --stat-cache-ttl | goofys |
| --no-implicit-dir | goofys |
| Handle 状态机 Read/Write | rclone, mountpoint-s3 |
| ChunkedReader 自适应翻倍 | rclone |
| MemoryLimiter (mem_limit) | mountpoint-s3 |
| io_uring 评估 (不可行) | fuser-iouring |

## 🟢 低优先级 (8项)

| 功能 | 来源 |
|------|------|
| Multipart upload 流式上传 | mountpoint-s3 |
| 块级 DataCache | mountpoint-s3 |
| 多级缓存 | mountpoint-s3 |
| Metablock 结构化 inode | mountpoint-s3 |
| cgroup 内存检测 | geesefs |
| CRC32C 上传校验 | mountpoint-s3 |
| Windows 支持 (WinFsp) | - |
| CSI plugin | - |
