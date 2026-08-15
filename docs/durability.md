# Durability model

> **Scope:** This document describes the actual writeback and
> durability behavior of mntrs as of the current `main` branch.
> The `--vfs-cache-mode` flag is now a real per-mode selector
> (off / writes / full) backed by a typed `CacheMode` enum — see
> [`--vfs-cache-mode` per mode](#vfs-cache-mode-per-mode) below.

## Three writeback concepts (terminology)

The word "writeback" appears in three independent places. Don't
confuse them when debugging:

| # | Name | Where | CLI flag | Default |
|---|---|---|---|---|
| 1 | FUSE kernel writeback | `libfuse InitFlags::FUSE_WRITEBACK_CACHE`, gated at `src/core_fs/fuser.rs:init()` on `FuserAdapter.write_back_cache` | `--write-back-cache` | **off** (since 2026-06-30; opt-in via the flag) |
| 2 | mntrs VFS upload queue | `src/writeback.rs` subsystem, single path per [The single writeback path](#the-single-writeback-path) below | `--vfs-write-back` (delay), `--writeback-immediate` (threshold) | on (always; this is the durability path) |
| 3 | OpenDAL backend cache | opendal internal; mntrs does not currently expose or tune it | (none — opendal internals) | opendal default |

**Concept #1 (FUSE kernel writeback)** is the one this document
refers to when discussing the `--write-back-cache` flag. It was
unconditional at init (`src/core_fs/fuser.rs`) until 2026-06-30,
which caused 3 cache-poisoning bugs (`#331`, `#334`, `#337`),
bench regression in PR #339, and architectural failures in
stress tests 01-large-dir and 05-crash-recovery (multi-page
writes' `write()` handler not called). It is now **opt-in**: the
default reverts to the on-write, off-kernel-buffering behavior
that tests 01-06 assume. CSI drivers keep the default
(disabled) because Pod multi-tenancy demands per-FS-message
observability — they want every `write()` to land in the daemon.

**Concept #2 (mntrs VFS upload queue)** is what almost everyone
means by "the writeback". The rest of this document describes
it.

**Concept #3 (OpenDAL backend cache)** is opaque to mntrs and not
relevant to most debugging.

## The single writeback path

All writes — regardless of `--vfs-cache-mode` — flow through the
same upload pipeline. The mode only changes where the bytes live
locally and whether the `.dirty` sidecar is written.

```
user process               kernel                mntrs FUSE worker        writeback worker
─────────────              ──────                ─────────────────        ────────────────
write(2)                   page cache            write handler
                           accumulates bytes     creates cache file (writes/full)
                                                  OR
                                                  Vec<u8> in fh (off)

fsync(2) / close(2)        fdatasync cache fd ──►  flush()/release()
                                                (writes/full only)        (off: nothing to sync)
                                                1. f.sync_data()
                                                2. write .dirty sidecar
                                                   (writes/full only)
                                                3. writeback_pending.insert(path)
                                                4. tx.send((ino, path, cpath, payload))
                                                   payload = in_memory_bytes (off)
                                                   payload = None  (writes/full)
                           returns Ok to user ──►  user thinks "durable"

                                                ────────────────────►   DelayQueue holds task for
                                                                            --vfs-write-back (default 5s)

                                                                            then: 5 attempts upload with
                                                                            exponential backoff

                                                                            on success: drop .dirty sidecar
                                                                                       (writes/full only)
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

## `--vfs-cache-mode` per mode

`--vfs-cache-mode` selects three orthogonal things in one flag:
**where the write buffer lives**, **whether read blocks are
persisted to disk**, and **what crash-safety guarantees apply**.
Backed by `crate::util::CacheMode` (`src/util.rs`); parsed in
`cmd/mount.rs` via `CacheMode::parse`.

| Mode       | CLI value | Write buffer                  | Read cache                     | `.dirty` sidecar | Crash safety                                                    |
|------------|-----------|-------------------------------|--------------------------------|------------------|------------------------------------------------------------------|
| `off`      | `off`     | in-process `Vec<u8>` per fh   | L1 mem_cache only              | not written      | **dirty bytes lost on daemon crash** before the 5 s write-back lands |
| `writes`   | `writes`  | disk file with `fdatasync`    | L1 mem_cache only              | written          | dirty bytes durable across daemon crash (recovery scan re-enqueues) |
| `full`     | `full`    | disk file with `fdatasync`    | L1 mem_cache + L2 `.block` files | written        | same as `writes`; pre-fetched blocks also persist for cross-session hits |
| `minimal`  | `minimal` | disk file with `fdatasync`; **unlinked after upload** | L1 mem_cache only | written | same crash safety as `writes` during upload window; **no on-disk footprint between writes** |

**Default**: `off`. The user is opting in to crash-safety when
they pass `--vfs-cache-mode=writes` or `--vfs-cache-mode=full`.
This is the inverse of the pre-Issue-B default (`writes`), and
the change is intentional — see [Migration notes](#migration-notes)
below.

### Off mode

No on-disk cache file. The FUSE worker accumulates bytes in
`FileHandleState::Write::in_memory_buffer: Option<Arc<Mutex<Vec<u8>>>>`
(`src/lib.rs`) and the async writeback worker reads from a copy
of that Vec packed into `WritebackTask.in_memory_payload`
(`src/writeback.rs`). The user-facing `fsync(2)` returns `Ok(())`
immediately (bytes are already "durable enough" — they're in
process memory and the writeback worker will upload them on the
5 s schedule). A daemon crash before the upload lands **drops
the dirty bytes**; there is no `.dirty` sidecar to recover from.

`read(2)` consults the in-memory buffer first (read-after-write
view), then falls through to the backend. The L1 mem_cache
(Moka DashMap, see `src/cache.rs`) is still populated on cache
misses — only the on-disk write buffer is gone.

### Writes mode

The pre-Issue-B default. Cache file at
`crate::write_buffer_path(cache_dir, path)` (see Issue A's
rename), `fdatasync` on flush/release (Issue #34), `.dirty`
sidecar for crash recovery, async upload via `writeback::spawn`.

### Full mode

Same as `writes` for writes. Additionally, pre-fetched read
blocks persist to disk (`.block` files, see
`crate::disk_cache_block_path` and `src/block_format.rs`) for
cross-session hits. Useful for read-heavy workloads with stable
file content where re-warming the L1 cache on every mount is
expensive.

### Migration notes

The pre-Issue-B default was `writes`. The new default is `off`.
Existing deployments that relied on the implicit crash safety
of `writes` must now pass `--vfs-cache-mode=writes` (or `full`)
explicitly. CSI deployments in particular should set this in the
mount options — pod reschedule is "soft crash" and loses
un-uploaded bytes only in `off` mode.

For an existing user with `cache_mode: "writes"` baked into a
config file, the field type changed from `String` to `CacheMode`
enum — string values (`"off"`, `"writes"`, `"full"`,
`"minimal"`) still parse via `CacheMode::parse`, so the CLI
contract is source-compatible.

## What the rclone-compat flags that ARE wired up do

| CLI flag | Mntrs field | Effect |
|---|---|---|
| `--vfs-write-back <secs>` | `write_back_delay: Duration` | Fresh-enqueue delay before upload. Default 5 s. |
| `--vfs-read-ahead <bytes>` | `read_ahead: u64` | Prefetch activation threshold. |
| `--vfs-read-chunk-size <bytes>` | `read_chunk_size: u64` | Read chunk size. Clamped to `[128 KiB, 16 MiB]`. |
| `--vfs-cache-min-free-space <bytes>` | `cache_min_free_space: u64` | If > 0, write paths return ENOSPC when free space drops below the floor. |
| `--exclude <pattern>` | `exclude_patterns: Vec<String>` | Filter list/get results. |

## See also

- `src/lib.rs` — `fn flush`, `fn release`, `fn open`, `fn create`,
  `fn read`, `fn write`, `fn fsync`
- `src/writeback.rs` — `WritebackTask`, `MAX_REENQUEUE_CYCLES`,
  `REENQUEUE_COOLDOWN`, `pub fn spawn`
- `src/util.rs` — `CacheMode` enum + `write_buffer_path` /
  `disk_cache_block_path` path helpers
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
- **#583** — `--vfs-cache-mode` three-mode selector
  implementation (the typed `CacheMode` enum, off-mode in-memory
  write buffer, `WritebackTask.in_memory_payload` field)
