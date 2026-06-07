# mntrs vs rclone mount — 参数对照表

基于 `rclone mount --help` 的 96 个参数，按类型分组。

## ✅ mntrs 已支持 (24 项)

| 参数 | 说明 |
|------|------|
| `<storage>` | URL 格式 `s3://bucket`、`gs://bucket` 等 |
| `<mountpoint>` | 本地挂载点 |
| `--read-only` | 只读模式 |
| `--allow-other` | 允许其他用户 |
| `--allow-root` | 允许 root 访问 |
| `--attr-timeout` | 属性缓存 TTL |
| `--dir-cache-time` | 目录缓存 TTL |
| `--volname` | 卷名（FSName） |
| `--write-back-cache` | 内核写缓存 |
| `--daemon` | 后台守护进程（fork+setsid+PID 文件） |
| `--daemon-wait` | 等待挂载就绪后返回 |
| `--daemon-timeout` | daemon-wait 超时（默认 10s） |
| `--vfs-cache-mode` | 缓存模式 off/writes/full |
| `--vfs-cache-max-size` | 缓存空间上限（默认 1GB，LRU 清理） |
| `--vfs-write-back` | 写回延迟（默认 5s）+ 后台队列重试 |
| `--vfs-read-ahead` | 预读下一个 chunk（默认 128KB） |
| `--vfs-read-chunk-size` | 分块读取大小（0=不限） |
| `--default-permissions` | 内核权限检查 |
| `--option` / `-o` | 透传原始 FUSE 参数 |
| `--uid` | 覆盖 uid |
| `--gid` | 覆盖 gid |
| `--umask` | 覆盖权限位 |
| `--dir-perms` | 目录权限 |
| `--file-perms` | 文件权限 |
| `--allow-non-empty` | 允许非空目录挂载 |
| `--cache-dir` | 自定义缓存目录 |
| `--direct-io` | 直接 IO，跳过本地缓存 |
| `--poll-interval` | 远程轮询间隔（默认 60s） |
| `--vfs-cache-max-age` | 缓存文件最大生存时间（默认 3600s） |
| `--exclude` / `--include` | 文件 glob 过滤 |
| `--max-depth` | 递归深度限制 |
| `--links` | 符号链接翻译 |
| `--max-read-ahead` | 最大预读大小 |
| `--devname` | 设备名（mount 表中显示） |
| `--vfs-cache-min-free-space` | 最小剩余磁盘空间触发缓存清理 |
| `--vfs-fast-fingerprint` | 快速指纹（非 checksum） |
| `--async-read` | 异步读取模式 |
| S3 xattr (etag/content-type) | 通过 getxattr 暴露 S3 元数据 |
| `--vfs-refresh` | 启动时刷新全量目录缓存 |
| `--vfs-case-insensitive` | 大小写不敏感 lookup |
| `--vfs-disk-space-total-size` | statfs 自定义总空间 (TB) |
| `--vfs-read-chunk-size-limit` | 分块读取上限 |
| `--vfs-used-is-size` | statfs used 用实际大小 |
| `--version` | 版本号 |
| `unmount <target>` | 卸载指定挂载点 |
| `unmount all` | 卸载全部 |
| `list` | 列出活跃挂载 |
| `install systemd` | 生成 systemd user service 模板 |

## ⬜ mntrs 未支持的 rclone 参数 (~42 项)

### 挂载模式 & 系统集成 (0)
| `--network-mode` | Windows 网络驱动器 |

### 缓存 & 性能 (0)

### VFS 功能 (0)

### macOS / Windows 专用 (3)
| `--noappledouble` | 忽略 Apple Double 文件 |
| `--noapplexattr` | 忽略 Apple 扩展属性 |
| `--mount-case-insensitive` | 大小写敏感 |

### 文件过滤 (1)

## 对比总结

| 维度 | rclone | mntrs |
|------|--------|-------|
| 总参数数 | 96 | 61 |
|  | 100% | 48% (32/66 核心参数) |
| 核心挂载功能 | ✅ | ✅ |
| 守护进程 | ✅ | ✅ |
| VFS 缓存 | ✅ | ✅（LRU 清理、写回队列、预读） |
| 多后端 | 40+ | 4（S3/GCS/AzBlob/HDFS） |
| 文件过滤 | ✅ | ❌ |
| 权限控制 | ✅ | ❌ |
| Windows 支持 | ✅ | ❌ |
 | 100% | 48% (32/66 核心参数) |
| 核心挂载功能 | ✅ | ✅ |
| 守护进程 | ✅ | ✅ |
| VFS 缓存 | ✅ | ✅（LRU 清理、写回队列、预读） |
| 多后端 | 40+ | 4（S3/GCS/AzBlob/HDFS） |
| 文件过滤 | ✅ | ❌ |
| 权限控制 | ✅ | ❌ |
| Windows 支持 | ✅ | ❌ |
