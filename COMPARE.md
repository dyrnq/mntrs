# mntrs vs rclone VFS 架构对比

> 基于 rclone master (2026-06) 的 vfs/vfscache + lib/multipart 分析

---

## 1. 总览

| 维度 | rclone | mntrs | 差距 |
|------|--------|-------|------|
| 语言 | Go | Rust | — |
| 缓存目录 | 分两级: cache + meta | 单层 `~/.cache/mntrs/<hash>` | mntrs 缺元数据持久化 |
| 写回队列 | 优先级堆 + 指数退避重试 | VecDeque + 3 次退避重试 | 缺少优先级/持久化 |
| 下载器 | `Downloaders` (range-aware) | 直接 `op.read_with().range()` | 无 range 合并 |
| 多段上传 | `lib/multipart` (通用) | 不支持 | ❌ |
| 并发读 | Single / Multi-chunk(streams) | 同上 | 无 backpressure |
| 内存缓存 | 无（直接 disk） | DashMap<ino, Bytes> | 有内存缓存优势 |
| 指纹 | `objectFingerprint` (ETag+ModTime+Size) | `vfs_fast_fingerprint` (size+mtime) | 弱一些但够用 |
| LRU 清理 | 有（后台 cleaner goroutine） | `evict_lru()` 在 write 时触发 | 无后台 cleaner |

## 2. 写路径对比

### rclone
```
write()
  → 本地 cache 文件 write_at
  → 标记 dirty
  → writeback 队列 (优先级堆)
    → 到期或 flush 触发 upload
    → 多次退避重试 (maxUploadDelay=5min)
    → 成功后清除 dirty 标记
```

### mntrs
```
write()
  → 本地 cache 文件 write_at
  → handles[ino] = Write { dirty: true }
  → writeback_queue (VecDeque)
    → 3 次退避重试 (固定间隔)
    → flush 时写入 sidecar
```

**差距**:
- rclone 用 `container/heap` 实现到期时间排序，`WriteBack` 有独立的 timer goroutine
- mntrs 用 `VecDeque` 先进先出，没有优先级、没有更新机制
- rclone 写失败后持久保留在队列中；mntrs 3 次失败直接丢弃

## 3. 读路径对比

### rclone
```
read()
  → Item.FindMissing() 确定缺失 range
  → Downloaders.Download(r) 调度下载
    → 多个 downloader goroutine 并发抓取不同 range
    → WriteAtNoOverwrite() 写入缓存文件
    → 通过 waiter channel 通知 reader
  → 从缓存文件 read_at
```

### mntrs
```
read()
  → mem_cache 命中? → 返回
  → disk_cache 命中? → 读文件, 写 mem_cache
  → 远程 fetch
    → single: 单块 range read
    → multi: concurrent fetch (semaphore 限流)
  → 写入 mem_cache + disk_cache
```

**差距**:
- rclone 的 `Downloaders` 支持**range 粒度的按需下载**——只拉缺失部分
- mntrs 每次都整块拉取（哪怕 cache 已有一部分）
- rclone 有 `FindMissing()` 方法避免重复下载
- mntrs 没有 backpressure：并发读的 `semaphore` 只控制 goroutine 数量，不控制字节量

## 4. 多段上传 (Multipart Upload)

### rclone
```go
// lib/multipart 提供通用模板
func UploadMultipart(ctx, src, in, opt)
  → Open.OpenChunkWriter() 初始化
  → 并行读源、并发上传 chunk
  → 支持 concurrency 限流、pacer 令牌
  → 失败时 abort 清理 (LeavePartsOnError 可选)
```

### mntrs
- **目前完全不支持**多段上传
- 小文件：`op.write(&p, data)` 整个写入
- 大文件：先写 cache → flush 时 `op.write(&p, data)` 整个写
- 超过 5GB 的文件会在写回时失败（S3 单次 PutObject 上限）

**影响**：
- ❌ 无法上传 >5GB 文件
- ❌ 大文件写回时没有进度
- ❌ 不能使用 S3 UploadPart - CopyPart 做服务端拷贝

## 5. 改进建议 (按优先级)

### P0: 多段上传支持 (高)
为 `flush → writeback` 路径加上 multipart uploader。

**做法**：
```rust
// src/multipart.rs
trait ChunkWriter {
    fn write_chunk(&self, part_num: u32, data: Vec<u8>) -> Result<()>;
    fn complete(&self) -> Result<()>;
    fn abort(&self) -> Result<()>;
}
```

利用 OpenDAL 的 `Writer` 或直接 S3 API。写回队列检测文件 >5GB 时走 multipart 路径。

**改动量**: ~300 行新文件 + 修改 writeback 路径。

### P1: 缓存元数据持久化
rclone 用 JSON 文件存每个 cache item 的 `Info`（mtime、size、range bitmap）。

mntrs 重启后 cache 目录不认——所有缓存文件被浪费。

**做法**：`disk_cache_index` 持久化到 `~/.cache/mntrs/meta.json`，启动时加载。
**改动量**: ~100 行。

### P2: 下载器 range 合并
当前每次 read 都发一个新 range 请求。如果多个 reader 读同一个文件的不同部分，range 不合并。

**做法**：对每个 open handle 维护 `Downloaders`，用 `FindMissing` 去重。
**改动量**: ~200 行新文件。

### P3: 写回队列优先级
`WriteBack` 用优先级堆按到期时间排序。当前 FIFO 无法处理不同文件的写回延迟。

**做法**：`BinaryHeap<(Instant, String)>` 替换 `VecDeque`。
**改动量**: ~50 行。

## 6. 不改的

| 特性 | 理由 |
|------|------|
| rclone 的 vfs 层 VFS/Cache/Item 三层分离 | mntrs `MntrsFs` 一身兼，当前够用 |
| rclone 的 `ranges.Ranges` bitmap | 当前整块 cache，不需要按 range 追踪 |
| OpenDAL 本身支持 retry/timeout/concurrency | 已有 `RetryLayer` `TimeoutLayer` `ConcurrentLimitLayer` |
| 内存缓存 | rclone 没有，但 mntrs 有 `mem_cache` 且有效（DashMap + AtomicU64 限流） |

## 7. 一句话总结

> mntrs 的核心差距在**多段上传**和**缓存持久化**。写回+读路径的 range 粒度优化是 P2。
> 当前 single-chunk fetch + 自适应翻倍 + concurrent streams 的读模型对 99% 场景已经够用。
>
> 建议先做 **P0 multipart upload**（~300 行），然后是 **P1 缓存元数据持久化**（~100 行）。
