# Durability model

> **Scope:** This document describes the actual writeback and
> durability behavior of mntrs as of the current `main` branch.
> It supersedes the older "per cache mode" framing — there is
> only one writeback path; the rclone-compat `vfs_cache_mode`
> flag is a **shadow field** that does not affect behavior
> (see [Shadow fields](#shadow-fields-rclone-compat-not-implemented)
> below).

## The single writeback path

All writes — regardless of which rclone-compat flag you set —
flow through the same path:

```
user process               kernel                mntrs FUSE worker        writeback worker
─────────────              ──────                ─────────────────        ────────────────
write(2)                   page cache            write handler
                           accumulates bytes     creates/opens cache file

fsync(2) / close(2)        fdatasync cache fd ──►  flush()/release()
                                                1. f.sync_data()
                                                2. write .dirty sidecar
                                                3. writeback_pending.insert(path)
                                                4. tx.send((ino, path, cpath, 0))
                           returns Ok to user ──►  user thinks "durable"

                                                ────────────────────►   DelayQueue holds task for
                                                                            --vfs-write-back (default 5s)
                                                                            
                                                                            then: 5 attempts upload with
                                                                            exponential backoff
                                                                            
                                                                            on success: drop .dirty sidecar
                                                                            on exhaustion: cycle + 1
                                                                              (60s cooldown, capped at 10)
```

### Step-by-step

1. **User-space write** (`write(2)`) — bytes flow through the
   kernel page cache into the local cache file. No network I/O
   is involved.

2. **fdatasync before the FUSE reply** (`src/lib.rs:3293` flush,
   `src/lib.rs:3480` release) — the FUSE handler calls
   `f.sync_data()` on the cache file's fd **before** the FUSE
   worker replies `Ok` to the kernel. This is the `Issue #34`
   fix. Pre-fix, a power loss between the FUSE reply and the
   kernel's lazy writeback would leave the cache file empty,
   and the async writeback would have nothing to upload.

3. **`.dirty` sidecar** — after the fdatasync succeeds, a
   sidecar at `<cache_path>.dirty` is written containing the
   remote path. This is the **crash-recovery marker**: if the
   daemon is killed between the fdatasync and the async
   upload finishing, the next mount's recovery path sees the
   sidecar and re-enqueues the upload.

4. **writeback_pending dedup** (`src/lib.rs:writeback_pending`,
   `Issue #38`) — both flush and release can fire for the
   same file (a write between them). The pending DashSet
   ensures only one writeback task is in flight per path;
   the second enqueue is skipped.

5. **Task enqueue** (`src/lib.rs:tx.send`,
   `src/writeback.rs:139`) — the task tuple is
   `(ino: u64, path: String, cpath: PathBuf, cycle: u32)`.
   The 4th element is the retry cycle count (`Issue #53`); 0
   means a fresh enqueue. Any site that sends the tuple
   must preserve the cycle.

6. **DelayQueue + worker** (`src/writeback.rs:98 spawn`) —
   the worker holds a `DelayQueue<Task>`. Fresh enqueues
   wait `--vfs-write-back` (default 5 s). Re-enqueues (cycle
   > 0) wait `REENQUEUE_COOLDOWN` (60 s,
   `src/writeback.rs:85`).

7. **Upload with retry** — when the queue head expires, the
   worker reads the cache file and spawns an upload task
   guarded by a static `Semaphore::new(4)`. The upload task
   does **up to 5 attempts** with exponential backoff
   (`Issue #46` for the multipart path). On final failure,
   the task is re-enqueued with `cycle + 1` and the 60 s
   cooldown, **bounded by `MAX_REENQUEUE_CYCLES = 10`**
   (`src/writeback.rs:75`).

8. **Recovery on retry exhaustion** — when the cycle cap is
   exceeded, the task is dropped and the `.dirty` sidecar
   is **left on disk** for the next mount to surface. The
   daemon does not block new writes; the data stays in the
   cache file until the backend recovers. Pre-`Issue #53`
   the log message lied about re-enqueueing while the code
   silently dropped the task — that is the silent-data-loss
   class this whole design is structured to avoid.

## What this guarantees

### Local durability (cache file on stable storage)

**Guaranteed** by the `fdatasync` in flush/release. Without the
explicit sync, the OS page cache could lose the bytes on power
loss. With it, the user can treat `fsync(2)` returning `Ok` as
"this file's data is on local disk."

We deliberately use `fdatasync` (not `fsync`) to match libfuse
`passthrough_hp`'s dup+close pattern: we only need user data
flushed; mtime/ctime updates ride out on the kernel's later
writeback. If a user needs full metadata durability, that is a
separate request — see the `Issue #34` comment at
`src/lib.rs:3510` for the design rationale.

### Remote durability (data in the backend)

**Eventually consistent.** The writeback worker uploads on the
`DelayQueue` schedule above. Default is `--vfs-write-back=5`
seconds. Set this to 0 for an "upload on close" approximation
(1-second floor in `new_test_fs`); even at 0 there is a
one-tick `DelayQueue` delay, not a synchronous upload.

A user-space `fsync(2)` on the file does **not** synchronously
upload — it only fdatasyncs the cache file. The user-space
`fsync` is a local-durability primitive, not a remote-durability
primitive. To wait for the writeback queue, use
`mntrs fsync-wait` (CSI) or rely on the periodic mount-status
output.

### Daemon restart (process killed mid-flight)

The `.dirty` sidecar is the recovery marker. The next mount
sees it and re-enqueues the upload. Cycle count is per-path.
The 4th tuple element (`u32 cycle`) is what makes the
re-enqueue path safe — a stale tuple field was the
`Issue #53` silent-data-loss bug.

### Retry exhaustion (backend persistently failing)

After 10 cycles (~10 minutes of 60-second cooldowns) the task
is dropped and the sidecar is left for the next mount to
surface. The daemon does not block new writes. Recovery is
operator-driven: fix the backend, restart the mount, the
recovery path sees the sidecars and re-enqueues.

## What `--vfs-write-back` actually controls

The `delay` field passed to `writeback::spawn` is
`--vfs-write-back` seconds. It is the **fresh-enqueue delay
only** — not a global throttle. The DelayQueue uses this
delay for cycle=0 tasks; cycle>0 tasks always use the 60 s
`REENQUEUE_COOLDOWN`.

| `--vfs-write-back` | Behavior |
|---|---|
| 0 (effectively 1) | Upload ≈ 1 s after close. Minimum practical value. |
| 5 (default) | Upload ≈ 5 s after close. |
| 60 | Upload ≈ 60 s after close. Useful for write-heavy workloads where the cost of a per-write PUT is high. |

`--vfs-write-back` does **not** affect retry behavior. A
permanently-failing backend always falls into the 60 s
cooldown path.

## Shadow fields (rclone-compat, not implemented)

These flags are parsed, stored in `MntrsFs`, and never read.
They are **no-ops** today. A user who sets them expecting
rclone semantics will get the standard writeback queue with
fdatasync — the same as the default.

| CLI flag | Mntrs field | Status |
|---|---|---|
| `--vfs-cache-mode <mode>` | `cache_mode: String` | Accepted: `off / minimal / writes / full`. **No `match` anywhere**. The mount.rs default is `"off"` (`mount.rs:331`) but `new_test_fs` uses `"writes"` (`lib.rs:4399`) — defaults are inconsistent. |
| `--vfs-cache-max-age <secs>` | `cache_max_age: Duration` | Set from CLI; never read. |
| `--poll-interval <secs>` | `poll_interval: Duration` | Set from CLI; never read. |
| `--vfs-cache-poll-interval <secs>` | (only in `main` args) | Accepted; never plumbed to `MntrsFs`. |
| `--vfs-refresh` | `vfs_refresh: bool` | Set from CLI; never read. |

### What this means in practice

- `--vfs-cache-mode=off` does **not** mean "write-through to the
  backend on close without materializing a local cache." It is
  identical to `--vfs-cache-mode=writes`. Implementing the
  off-mode semantics is a real feature (write-through code
  path) and is not a doc change.
- `--vfs-cache-max-age` does **not** trigger TTL-based cache
  eviction. The disk cache's LRU is bounded by
  `--vfs-cache-max-size` (`lib.rs:544-570`) but has no time
  component.
- `--vfs-cache-poll-interval` and `--vfs-refresh` do **not**
  trigger background stat revalidation. Stale-attribute
  detection is event-driven (open, getattr, read), not polled.

## What the rclone-compat flags that ARE wired up do

| CLI flag | Mntrs field | Effect |
|---|---|---|
| `--vfs-write-back <secs>` | `write_back_delay: Duration` | Fresh-enqueue delay before upload. Default 5 s. |
| `--vfs-read-ahead <bytes>` | `read_ahead: u64` | Prefetch activation threshold. |
| `--vfs-read-chunk-size <bytes>` | `read_chunk_size: u64` | Read chunk size. Clamped to `[128 KiB, 16 MiB]`. |
| `--vfs-cache-min-free-space <bytes>` | `cache_min_free_space: u64` | If > 0, write paths return ENOSPC when free space drops below the floor. |
| `--exclude <pattern>` | `exclude_patterns: Vec<String>` | Filter list/get results. |

## See also

- `src/lib.rs:3293` — `fn flush`
- `src/lib.rs:3480` — `fn release`
- `src/writeback.rs:47` — `pub type Task`
- `src/writeback.rs:75` — `MAX_REENQUEUE_CYCLES`
- `src/writeback.rs:85` — `REENQUEUE_COOLDOWN`
- `src/writeback.rs:98` — `pub fn spawn`
- `docs/benchmark_cat_head_tail.md` — read-path benchmark
- `bench/run_all.sh` — A/B bench for `mem_cache_impl`
  (related but separate concern)

## Related issues

- **#34** — fdatasync on flush (the local-durability half)
- **#38** — `writeback_pending` dedup (the duplicate-enqueue
  half)
- **#46** — multipart upload retry (the network-failure half)
- **#53** — silent-data-loss bug from a `Task` tuple change
  (the retry-cycle history)
- **#55** — block-cache drop on upload success (the
  read-after-write consistency half)
- **#142** — this document (re-framed from "per cache mode"
  to "actual uniform behavior")
