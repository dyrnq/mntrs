# mntrs vs rclone mount — 参数对照表

基于 `rclone mount --help` 的 96 个参数，按类型分组。

## ✅ mntrs 已支持

| 序号 | 参数 | 说明 |
|------|------|------|
| 1 | `<storage>` | URL 格式 `s3://bucket`、`gs://bucket` 等 |
| 2 | `<mountpoint>` | 本地挂载点 |
| 3 | `--read-only` | 只读模式 |
| 4 | `--allow-other` | 允许其他用户 |
| 5 | `--attr-timeout` | 属性缓存 TTL |
| 6 | `--dir-cache-time` | 目录缓存 TTL |
| 7 | `--volname` | 卷名（FSName） |
| 8 | `--write-back-cache` | 内核写缓存 |
| 9 | `--version` | 版本号 |
| 10 | `unmount <target>` | 卸载 |
| 11 | `unmount all` | 卸载全部 |
| 12 | `list` | 列出挂载 |

## ⬜ mntrs 未支持的 rclone 参数

### 挂载模式 & 系统集成

| 参数 | 说明 | 优先级 |
|------|------|--------|
| `--allow-root` | 允许 root 访问 | 🟡 |
| `--allow-non-empty` | 允许挂载到非空目录 | 🟢 |
| `--daemon` | 后台守护进程 | 🟡 |
| `--daemon-timeout` | 后台超时 | 🟡 |
| `--daemon-wait` | 等待挂载就绪 | 🟢 |
| `--devname` | 设备名 | 🟢 |
| `--network-mode` | Windows 网络驱动器 | 🟢 |
| `--fuse-flag` / `-o` | 原始 FUSE 参数 | 🟢 |

### 权限 & 所有权

| 参数 | 说明 | 优先级 |
|------|------|--------|
| `--uid` | 覆盖 uid | 🟢 |
| `--gid` | 覆盖 gid | 🟢 |
| `--umask` | 覆盖权限位 | 🟢 |
| `--dir-perms` | 目录权限 (default 777) | 🟢 |
| `--file-perms` | 文件权限 (default 666) | 🟢 |
| `--default-permissions` | 内核权限检查 | 🟢 |

### 缓存 & 性能

| 参数 | 说明 | 优先级 |
|------|------|--------|
| `--vfs-cache-mode` | 缓存模式 off/minimal/writes/full | 🟡 |
| `--vfs-cache-max-age` | 缓存最大生存时间 | 🟡 |
| `--vfs-cache-max-size` | 缓存最大大小 | 🟡 |
| `--vfs-cache-min-free-space` | 最小剩余磁盘空间 | 🟢 |
| `--vfs-write-back` | 写回延迟 (default 5s) | 🟡 |
| `--vfs-write-wait` | 写入等待超时 | 🟢 |
| `--vfs-read-ahead` | 预读 | 🟢 |
| `--vfs-read-chunk-size` | 分块读取大小 | 🟢 |
| `--vfs-read-chunk-size-limit` | 分块上限 | 🟢 |
| `--vfs-read-chunk-streams` | 并行读取流数 | 🟢 |
| `--vfs-read-wait` | 读等待超时 | 🟢 |
| `--vfs-cache-poll-interval` | 缓存轮询间隔 | 🟢 |
| `--vfs-fast-fingerprint` | 快速指纹 (精度较低) | 🟢 |
| `--async-read` | 异步读 (default true) | 🟢 |
| `--direct-io` | 直接 IO，禁用缓存 | 🟢 |
| `--max-read-ahead` | 最大预读 (default 128Ki) | 🟢 |
| `--cache-dir` | 缓存目录路径 | 🟢 |
| `--poll-interval` | 远程轮询间隔 (default 1m) | 🟢 |

### VFS 功能

| 参数 | 说明 | 优先级 |
|------|------|--------|
| `--vfs-case-insensitive` | 大小写不敏感 | 🟢 |
| `--vfs-links` | 符号链接翻译 | 🟢 |
| `--vfs-block-norm-dupes` | Unicode 规范化去重 | 🟢 |
| `--vfs-disk-space-total-size` | 手动设置磁盘总空间 | 🟢 |
| `--vfs-refresh` | 启动时刷新目录缓存 | 🟢 |
| `--vfs-used-is-size` | 用量计算方式 | 🟢 |
| `--vfs-metadata-extension` | 元数据文件扩展名 | 🟢 |

### macOS / Windows 专用

| 参数 | 说明 |
|------|------|
| `--noappledouble` | 忽略 Apple Double 文件 |
| `--noapplexattr` | 忽略 Apple 扩展属性 |
| `--mount-case-insensitive` | 告诉 OS 挂载大小写敏感 |

### 文件过滤 (rclone sync 相关)

| 参数 | 说明 |
|------|------|
| `--exclude` / `--include` / `--filter` | 文件过滤 |
| `--max-size` / `--min-size` | 文件大小过滤 |
| `--max-age` / `--min-age` | 文件时间过滤 |
| `--max-depth` | 递归深度 |
| `--delete-excluded` | 删除排除文件 |
| `--ignore-case` | 过滤忽略大小写 |

### 其他

| 参数 | 说明 |
|------|------|
| `--no-checksum` | 不比较校验和 |
| `--no-modtime` | 不读/写修改时间 |
| `--no-seek` | 不允许 seek |
| `--links` | 全局符号链接翻译 |
| `--debug-fuse` | FUSE 调试 (mntrs 用 `RUST_LOG=debug`) |

## 对比总结

| 维度 | rclone | mntrs |
|------|-------|-------|
| 总参数数 | 96 | — |
| 已支持 | — | 12 个参数 + 13 个内置功能 |
| 未支持 | — | ~84 个参数 |
| 核心功能可用 | ✅ | ✅ |
| 覆盖度 | 100% | ~15%（但核心参数已覆盖） |
