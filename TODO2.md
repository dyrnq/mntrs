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

## 🟡 中优先级

| 功能 | 说明 | 来源 |
|------|------|------|
| `MemoryLimiter` | 全局内存预算管理，prefetch + upload 共享配额，超限拒绝扩容防止 OOM | mountpoint-s3 |
| Prefetcher 自适应窗口 | 逐步扩大 read window（1MB→128MB），随机读时自动 reset + backpressure 流控 | mountpoint-s3 |
| `--no-implicit-dir` | 关闭隐式目录检测（S3 中 key 前缀 `a/b/` 暗示目录存在） | goofys |
| `--stat-cache-ttl` | 分离 StatObject 结果缓存 TTL 和目录列表缓存 TTL | goofys |

## 🟢 低优先级

| 功能 | 说明 | 来源 |
|------|------|------|
| Multipart upload 流式上传 | 大文件用 multipart 分片上传（增量 append + 原子 finalize），小文件用 PutObject | mountpoint-s3 |
| 块级 DataCache | 固定块大小（如 1MB）+ checksum 校验 + 按 ObjectId+BlockIndex 索引，替代当前文件级缓存 | mountpoint-s3 |
| 多级缓存 | disk + in-memory + S3 Express 三级缓存层 | mountpoint-s3 |
| Metablock 结构化 inode | 树状 inode 管理 + pending upload 状态跟踪 + 目录原子操作 | mountpoint-s3 |
| cgroup 内存检测 | 运行时检测 cgroup 内存限制（容器环境），自动调整缓存策略 | geesefs |
| CRC32C 上传校验 | 上传时自动计算 CRC32C 校验和，S3 服务端验证完整性 | mountpoint-s3 |
