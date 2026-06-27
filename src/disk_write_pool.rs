// Disk-IO thread pool for cache writes.
//
// This module owns the process-static MPMC channel
// (`DISK_WRITE_POOL`) and the periodic-fsync thread
// (`DIRTY_CACHE_PATHS`).  FUSE workers submit
// `DiskWriteJob`s via `submit_disk_write` /
// `submit_block_cache_write`; the pool's worker threads
// execute them off the hot path so the FUSE reply isn't
// blocked on local disk I/O.

use std::path::PathBuf;
use std::sync::Arc;

// ── Global Operator (for the write path's prefix fetch) ─────────────

/// Synchronous wrapper for `op.read` used by the
/// write path's background thread (which can't borrow
/// the `&self` op directly). Returns the full file
/// bytes. Used only in the rare "writing at offset >
/// current length" path where the cache file is empty
/// and we need to backfill the prefix from the remote
/// backend.
fn opendal_sync_read(path: &str) -> std::io::Result<Vec<u8>> {
    let op = opendal_sync_op();
    crate::rt()
        .block_on(async move { op.read(path).await })
        .map(|b| b.to_vec())
        .map_err(|e| std::io::Error::other(format!("read failed: {e}")))
}

/// Lazy-initialized global op for the write path's
/// background thread. The thread can't borrow the
/// `&self` op (it's spawned and outlives any single
/// `write()` call), so we keep a global clone.
fn opendal_sync_op() -> opendal::Operator {
    OPENDAL_SYNC_OP
        .get_or_init(|| {
            tracing::warn!(
                "opendal_sync_op accessed before initialization; \
                 this is a bug in the write path's prefix fetch"
            );
            // Bug 27: SAFETY — Memory::default() +
            // Operator::new(...).finish() is infallible
            // for the in-memory backend (no FS, no
            // network, no auth — just a HashMap behind
            // an Arc). The `expect` message preserves
            // the actionable signal if a future opendal
            // upgrade changes that contract; bare
            // .unwrap() would produce a context-free
            // panic on the same condition. This fallback
            // is itself only reached on a pre-init
            // access (logged above) and is a defensive
            // backstop — production code always
            // initializes the cell first.
            opendal::Operator::new(opendal::services::Memory::default())
                .expect("BUG: opendal Memory backend Operator::new is infallible")
                .finish()
        })
        .clone()
}

static OPENDAL_SYNC_OP: once_cell::sync::OnceCell<opendal::Operator> =
    once_cell::sync::OnceCell::new();

/// Set the global op for use by the write path's
/// background thread. Called once during mount
/// initialization. Safe to call multiple times; only
/// the first call wins.
pub fn set_opendal_sync_op(op: opendal::Operator) {
    let _ = OPENDAL_SYNC_OP.set(op);
}

// ── DiskWriteJob ────────────────────────────────────────────────────

/// A single disk I/O job submitted to the pool.
///
/// Three modes (selected by which fields are `Some`):
///   1. **Block-cache write** (`block_cache` is `Some`):
///      write a `.block` file in the v3 on-disk format.
///      Used by the read path after a remote S3 fetch
///      to populate the block-level disk cache (so
///      subsequent reads of the same range hit the fast
///      path). Multiple block writes in a single read
///      are submitted as a batch and run in parallel on
///      the worker pool — that gives cold-read
///      concurrency vs the cc2667f/23d22d9 serial loop.
///   2. **Whole-file write via fd** (`cache_fd` is `Some`):
///      write at `offset` in an already-opened cache file.
///   3. **Whole-file write via path** (`cache_path` is `Some`):
///      open the cache file from `cache_dir + cache_path`,
///      then write at `offset`.
pub(crate) struct DiskWriteJob {
    /// Open file handle + `Mutex` (per-handle). `None`
    /// → use the fallback path that re-opens the cache
    /// file from `self.cache_dir + cache_path`.
    pub(crate) cache_fd: Option<Arc<std::sync::Mutex<std::fs::File>>>,
    /// Fallback path inside `self.cache_dir` (only
    /// used when `cache_fd` is `None`).
    pub(crate) cache_path: Option<PathBuf>,
    /// Remote path (for the rare "write at offset >
    /// current length" prefix fetch). Used by the
    /// whole-file write modes; ignored when
    /// `block_cache` is `Some` (the block-cache mode
    /// stores its target path directly in
    /// `block_cache` to avoid re-deriving it on the
    /// pool worker).
    pub(crate) remote_path: String,
    /// Byte offset in the cache file (whole-file mode).
    pub(crate) offset: u64,
    /// Data to write.
    pub(crate) data: Vec<u8>,
    /// Block-cache write: full on-disk path of the
    /// `.block` file to create. `Some` overrides
    /// `cache_fd` / `cache_path` / `offset` / `remote_path`
    /// — the worker writes the new format
    /// (`MNCR || version || data || CRC32C`) at this
    /// path. The dashmap LRU index update is done by
    /// the caller (FUSE worker) before submit, since
    /// the pool worker has no `&self` reference; see
    /// `submit_block_cache_write` callers.
    pub(crate) block_cache: Option<PathBuf>,
    /// Cache-generation snapshot captured at submit time
    /// (block-cache mode only). The pool worker compares it
    /// against the current `PATH_CACHE_GEN` for `remote_path`
    /// before writing the `.block` file; if a write invalidated
    /// the path in the meantime (bumping the gen), the stale
    /// block write is skipped — otherwise it would re-create a
    /// `.block` file the invalidate already removed, and the
    /// next read would serve it (issue #128). 0 for non-block
    /// modes (the check only runs in the `block_cache` branch).
    pub(crate) cache_gen: u64,
}

/// Per-remote-path cache generation counter (issue #128).
///
/// Bumped by `DiskBlockCache::invalidate_path` on every write to a
/// path. The read path's async block-cache pool job captures the gen
/// at submit time and skips the `.block` write if the gen advanced —
/// i.e. a write invalidated the path while the pool job was queued.
/// Without this, the pool job can land a STALE `.block` file after
/// invalidate, and the next read serves it.
pub(crate) static PATH_CACHE_GEN: once_cell::sync::Lazy<dashmap::DashMap<String, u64>> =
    once_cell::sync::Lazy::new(dashmap::DashMap::new);

/// Read the current cache generation for `path` (0 if unseen).
pub(crate) fn path_cache_gen(path: &str) -> u64 {
    PATH_CACHE_GEN.get(path).map(|g| *g).unwrap_or(0)
}

/// Bump the cache generation for `path` (invalidates pending pool
/// writes captured against an older gen). Called by the write path's
/// L2 invalidate.
pub(crate) fn bump_path_cache_gen(path: &str) {
    PATH_CACHE_GEN
        .entry(path.to_string())
        .and_modify(|g| *g = g.wrapping_add(1))
        .or_insert(1);
}

impl DiskWriteJob {
    /// Execute the disk I/O. Called from a worker
    /// thread. Errors are logged and swallowed —
    /// writeback's `std::fs::read` will return Err on
    /// the next read, and the caller treats that as a
    /// cache miss.
    pub(crate) fn execute(self) {
        // Block-cache write takes precedence over the
        // other two modes. Used by the read path after
        // a remote S3 fetch to populate the block-level
        // disk cache.
        if let Some(path) = &self.block_cache {
            // Issue #128: skip the stale block write if a write
            // invalidated this path since the read captured `cache_gen`.
            // The pool job was queued during a read; if a write (append,
            // truncate, …) bumped the path's gen in the meantime, the
            // block data we're about to persist is now stale and would
            // shadow the fresh whole-file cache / remote on the next read.
            if path_cache_gen(&self.remote_path) != self.cache_gen {
                tracing::debug!(
                    path = %self.remote_path,
                    job_gen = self.cache_gen,
                    cur_gen = path_cache_gen(&self.remote_path),
                    "block-cache pool write skipped (path invalidated since read)"
                );
                return;
            }
            Self::do_block_cache_write(path, &self.remote_path, &self.data);
            return;
        }
        match self.cache_fd {
            Some(fd) => {
                let mut f = match fd.lock() {
                    Ok(f) => f,
                    Err(_) => return,
                };
                // Issue #39: retry once on ENOSPC. The
                // FUSE write() path runs
                // `evict_lru_if_needed()` synchronously
                // before submitting the job (see
                // `fn write` in this file), so the
                // common case is the first attempt
                // succeeds after eviction. The retry
                // here is a safety net for the rare
                // case where the disk is so full that
                // even an empty cache index doesn't
                // meet the min-free-space target —
                // the FUSE reply was already Ok before
                // we got here, so the best we can do
                // is try one more time and log a warn
                // if it still fails.
                if let Err(e) = Self::do_write(&mut f, &self.remote_path, self.offset, &self.data) {
                    if e.kind() == std::io::ErrorKind::StorageFull {
                        tracing::warn!(
                            path = %self.remote_path,
                            "disk cache write hit ENOSPC; retrying once (issue #39)"
                        );
                        if let Err(e2) =
                            Self::do_write(&mut f, &self.remote_path, self.offset, &self.data)
                        {
                            tracing::error!(
                                path = %self.remote_path,
                                error = %e2,
                                "disk cache write still failed after ENOSPC retry; \
                                 FUSE reply was Ok but cache file may be truncated"
                            );
                        }
                    } else {
                        tracing::debug!(
                            path = %self.remote_path,
                            error = %e,
                            "disk cache write (cache_fd) failed"
                        );
                    }
                }
            }
            None => {
                let cpath = match &self.cache_path {
                    Some(p) => p,
                    None => return,
                };
                if let Some(parent) = cpath.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let mut f = match std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .read(true)
                    .open(cpath)
                {
                    Ok(f) => f,
                    Err(_) => return,
                };
                // Same retry-after-ENOSPC pattern as the
                // cache_fd branch (issue #39). The
                // fall-back open path doesn't have a
                // long-lived fd to keep across the
                // retry, but the FUSE-side eviction
                // still helps future writes.
                if let Err(e) = Self::do_write(&mut f, &self.remote_path, self.offset, &self.data) {
                    if e.kind() == std::io::ErrorKind::StorageFull {
                        tracing::warn!(
                            path = %self.remote_path,
                            "disk cache write (no-fd) hit ENOSPC; retrying once (issue #39)"
                        );
                        if let Err(e2) =
                            Self::do_write(&mut f, &self.remote_path, self.offset, &self.data)
                        {
                            tracing::error!(
                                path = %self.remote_path,
                                error = %e2,
                                "disk cache write still failed after ENOSPC retry; \
                                 cache file may be truncated"
                            );
                        }
                    } else {
                        tracing::debug!(
                            path = %self.remote_path,
                            error = %e,
                            "disk cache write (no-fd) failed"
                        );
                    }
                }
            }
        }
        // #8 (durability): register the cache file for
        // periodic fsync. The write above only landed in
        // the OS page cache; without a periodic sync, a
        // power loss or kernel panic can truncate the
        // file to 0 bytes (the cache_fd open created the
        // inode metadata before the data was written).
        // The fsync thread batches sync_data() calls
        // every 5 s, amortizing the cost across all
        // dirty paths.
        if let Some(p) = &self.cache_path {
            register_dirty_cache_path(p);
        }
    }

    /// Write a single block-cache file in the new
    /// format (`MNCR || version || data || CRC32C`).
    /// Mirrors the inline `write_block_cached` but
    /// runs on a worker thread, so the FUSE worker
    /// can submit N block writes for a single remote
    /// fetch and have them all run in parallel on
    /// the pool. This is the cold-read concurrency
    /// win vs the cc2667f/23d22d9 serial loop.
    ///
    /// Note: this helper does NOT update
    /// `disk_cache_index` (the in-memory LRU sort
    /// key) — the caller does that synchronously
    /// before submit, since the pool worker has no
    /// `&self` reference. See `submit_block_cache_write`
    /// callers.
    fn do_block_cache_write(blk_path: &std::path::Path, remote_path: &str, data: &[u8]) {
        use std::io::Write;
        if let Some(parent) = blk_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        // v3 format (DESIGN_VULNS High #4) — shared
        // serializer in block_format.rs so both
        // write_block_cached and this pool worker
        // produce identical on-disk output.
        let buf = match crate::block_format::serialize_v3_block(remote_path, data) {
            Some(b) => b,
            None => return,
        };
        let mut f = match std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(blk_path)
        {
            Ok(f) => f,
            Err(_) => return,
        };
        let ok = f.write_all(&buf).is_ok();
        // Truncate so any stale tail from a v1/v2
        // overwrite is not visible to readers.
        if ok {
            let _ = f.set_len(buf.len() as u64);
        }
        if !ok {
            tracing::debug!(?blk_path, "block cache write (pool) failed");
            return;
        }
        // #8 (durability): the file is in the OS page
        // cache but not necessarily on disk. Register
        // the path with the periodic-fsync thread, which
        // batches `sync_data()` calls every 5 s. Without
        // this, a power loss between the write and the
        // kernel's lazy writeback leaves a 0-byte (or
        // truncated) .block file that the next read sees
        // as corrupt (CRC mismatch → unlink → re-fetch).
        register_dirty_cache_path(blk_path);
    }

    fn do_write(
        f: &mut std::fs::File,
        remote_path: &str,
        offset: u64,
        data: &[u8],
    ) -> std::io::Result<()> {
        use std::io::{Seek, Write};
        let end = offset + data.len() as u64;
        let current_len = f.metadata()?.len();
        // Prefix fetch: when writing at an offset past
        // current length, backfill the missing prefix
        // from the remote backend (so the cache file
        // doesn't get a sparse hole). The cached prefix
        // is what makes the next read faster than
        // re-fetching the whole range from S3.
        if offset > 0
            && current_len == 0
            && offset > current_len
            && let Ok(remote) = opendal_sync_read(remote_path)
            && !remote.is_empty()
        {
            f.write_all(&remote)?;
        }
        let current_len = f.metadata()?.len();
        if end > current_len {
            // Issue #39: surface ENOSPC up to the caller so
            // `execute()` can retry-after-evict. Pre-fix
            // this used `let _ = f.set_len(end)` which
            // swallowed the StorageFull error — the FUSE
            // reply was Ok, but the cache file silently
            // failed to grow, leading to truncated reads
            // on the next access.
            f.set_len(end)?;
        }
        // Issue #60: revert #12's `write_at` change to
        // `seek + write_all`. The original change aimed
        // for one fewer syscall (lseek + pwrite vs
        // lseek + pwrite) but had two regressions:
        //   * `std::os::unix::fs::FileExt` is Unix-only,
        //     so the Windows CI clippy job failed with
        //     `no method named write_at` (issue #59).
        //   * `write_at` (pwrite) does NOT update the
        //     kernel-side fd offset, but the same fd is
        //     shared with the flush/release/fsync paths
        //     which do read+seek. A subsequent read
        //     would see a wrong offset, leading to the
        //     "read pre-existing FAIL: got ''" pattern
        //     in hdfs-kerberos + CSI e2e.
        //
        // The bench's 6x gap vs rclone is dominated by
        // the local disk write itself, not the syscall.
        // Reverting to seek + write_all costs ~30 µs of
        // redundant lseek per 10 MiB write — well
        // below the noise floor of the disk write.
        f.seek(std::io::SeekFrom::Start(offset))?;
        f.write_all(data)?;
        // #6: no `f.flush()`. The OS page cache holds
        // the data and flushes in the background. The
        // writeback worker's `std::fs::read` of this
        // file goes through the same page cache and
        // sees the freshly-written data.
        Ok(())
    }
}

// ── Pool initialization ─────────────────────────────────────────────

/// Bounded MPMC channel. Bounded so a runaway
/// producer (FUSE worker) can't OOM us if the IO
/// workers fall behind. 4096 = up to 4 GiB worth
/// of 1 MiB writes queued before backpressure
/// blocks the FUSE worker.
const DISK_WRITE_QUEUE_CAP: usize = 4096;

static DISK_WRITE_POOL: once_cell::sync::OnceCell<crossbeam_channel::Sender<DiskWriteJob>> =
    once_cell::sync::OnceCell::new();

/// Initialize the disk-IO thread pool. Called once
/// during mount setup (`new_test_fs` and
/// `cmd::mount::mount_internal`). Subsequent calls
/// are true no-ops — guarded at entry by a
/// `DISK_WRITE_POOL.get().is_some()` check, so a
/// repeat call from a test that constructs multiple
/// MntrsFs instances does NOT spawn additional
/// fsync threads (Bug 5: pre-fix the IO workers
/// self-cleaned via dropped sender, but the fsync
/// thread spawn was unconditional and accumulated).
///
/// Lifetime contract: the pool threads (`mntrs-disk-io-*`
/// and `mntrs-fsync`) are process-static. They block
/// forever in `recv()` / `sleep()` and only exit when
/// the process exits. This matches the daemon mount
/// model — there's no in-process pool restart. If a
/// future refactor introduces lifecycle (e.g. mount
/// → unmount → re-mount in the same process), add an
/// explicit `shutdown_disk_write_pool()` that closes
/// the sender and joins workers.
///
/// `num_threads` defaults to
/// `min(num_cpus::get(), 8)` if `None`. Ignored on
/// repeat calls (the original pool size is kept).
pub fn init_disk_write_pool(num_threads: Option<usize>) {
    // Bug 5: idempotent. Check before any thread spawn
    // so a repeat call doesn't leak a fsync thread.
    // The TOCTOU between this `.get()` and the `.set()`
    // below is benign — init is called from
    // single-threaded mount setup; the worst case
    // (two concurrent inits, one wins .set) is the
    // pre-fix behaviour (workers self-clean via the
    // dropped sender; one extra fsync leaks). The
    // common case is now O(1) and leak-free.
    if DISK_WRITE_POOL.get().is_some() {
        return;
    }
    let n = num_threads.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(8)
    });
    let (tx, rx) = crossbeam_channel::bounded::<DiskWriteJob>(DISK_WRITE_QUEUE_CAP);
    for i in 0..n {
        let rx = rx.clone();
        std::thread::Builder::new()
            .name(format!("mntrs-disk-io-{i}"))
            .spawn(move || {
                disk_io_worker_loop(rx);
            })
            .expect("failed to spawn disk-IO worker thread");
    }
    // Drop the original receiver — each worker has
    // its own clone, and `crossbeam-channel` keeps the
    // channel open as long as at least one receiver
    // exists. The remaining clones in the workers are
    // enough to drain the queue.
    drop(rx);
    // If a concurrent init raced us and set the pool
    // first, our `tx` is dropped here — that closes
    // our cloned receivers (the workers spawned above
    // hold them), and those workers exit cleanly via
    // the `while let Ok(_) = rx.recv()` returning Err.
    // Net effect: at most one fsync thread per process,
    // no IO-worker accumulation.
    if DISK_WRITE_POOL.set(tx).is_err() {
        return;
    }
    // #8 (durability): spawn the periodic fsync thread
    // alongside the IO worker pool. Gated by the
    // .set() success above so we never spawn a second
    // fsync thread for a race that lost.
    spawn_fsync_thread();
}

fn disk_io_worker_loop(rx: crossbeam_channel::Receiver<DiskWriteJob>) {
    while let Ok(job) = rx.recv() {
        job.execute();
    }
    // Issue #268.1 O5: surface worker death. Pre-fix
    // this exit was silent; a thread panic or
    // graceful channel drop was invisible. After
    // exit, all cache writes fall through to
    // submit_disk_write's sync-fallback path — that
    // path now logs warn (O5 above), so this error
    // is the upstream cause operators should chase.
    tracing::error!("disk-IO worker exiting (channel closed)");
}

// ── Periodic fsync ──────────────────────────────────────────────────

/// #8 (durability): periodic-fsync interval. Every
/// `FSYNC_INTERVAL_SECS` seconds, the background
/// fsync thread walks `DIRTY_CACHE_PATHS` and calls
/// `File::sync_data()` on each. 5 s is a balance
/// between durability (smaller window = less data
/// lost on power loss) and efficiency (larger window
/// = fewer syscalls, more page-cache amortization).
const FSYNC_INTERVAL_SECS: u64 = 5;

/// Per-tick cap on how many cache files the fsync
/// thread will sync_data() in one wake-up. At ~50 µs
/// per `sync_data` syscall on a warm-page SSD, a
/// 1024-entry batch is ~50 ms of work — bounded enough
/// that the fsync thread doesn't sit blocked on disk I/O
/// for half a second on a 10k+ dirty-paths backlog (Bug
/// 10). Any overflow stays in `DIRTY_CACHE_PATHS` and
/// rolls into the next tick.
///
/// Sustained throughput at this cap is 1024 / 5 s = ~200
/// fsyncs/second per fsync thread. If a workload writes
/// to more than 200 distinct cache files per second
/// sustained, the backlog grows; `spawn_fsync_thread`
/// emits a periodic `warn!` so the operator can either
/// raise the cap or rein the workload in.
const MAX_FSYNC_BATCH_PER_TICK: usize = 1024;

/// Backlog factor above which the fsync thread starts
/// logging a periodic warn (every Nth tick — see
/// `BACKLOG_LOG_EVERY_N_TICKS`). 5× the batch cap means
/// the queue is at least 25 s deep at the current
/// drain rate. Below this we stay quiet.
const FSYNC_BACKLOG_WARN_MULT: usize = 5;
const BACKLOG_LOG_EVERY_N_TICKS: u64 = 6; // ~30 s at 5 s interval

/// Cache file paths that have been written but may
/// not yet be on disk. Inserted by the disk-IO pool
/// workers after every successful write; drained by
/// the periodic fsync thread.
///
/// Set semantics (idempotent insert): a hot-write
/// path stays a single entry no matter how many
/// writes hit it between fsync ticks. The fsync
/// thread removes a path on successful sync, and
/// also on `NotFound` (cache evicted between the
/// last write and this tick).
///
/// Memory bound: O(number of distinct cache files
/// written in the last fsync interval). Each entry
/// is a `PathBuf` (~200 B). With ~10 k unique cache
/// files in a busy mount, this is ~2 MiB — negligible
/// next to the cache itself.
static DIRTY_CACHE_PATHS: once_cell::sync::Lazy<dashmap::DashSet<PathBuf>> =
    once_cell::sync::Lazy::new(dashmap::DashSet::new);

/// Register a cache file path as dirty. Called by
/// the disk-IO pool worker after a successful write
/// to the local cache. The periodic fsync thread
/// picks this up on the next tick.
///
/// Insert is idempotent (DashSet); repeated writes
/// to the same path collapse to one entry. The
/// fsync thread removes the entry after a
/// successful sync, so a steady-state hot file
/// oscillates between "in set" and "not in set" at
/// the tick frequency.
pub(crate) fn register_dirty_cache_path(path: &std::path::Path) {
    DIRTY_CACHE_PATHS.insert(path.to_path_buf());
}

/// Spawn the background fsync thread. Called once
/// from `init_disk_write_pool` (which itself is
/// init-once via OnceCell on `DISK_WRITE_POOL`),
/// so this also runs at most once per process.
///
/// The thread loops forever (lifetime = process):
///   1. sleep `FSYNC_INTERVAL_SECS`
///   2. snapshot the set into a Vec (avoid holding
///      the dashmap iterator while issuing syscalls)
///   3. for each path: open read-only, `sync_data()`,
///      remove from set on success or `NotFound`
///
/// `sync_data` (vs `sync_all`) skips metadata sync,
/// which is what we actually care about — the cache
/// file's data is the durability target; mtime/atime
/// updates can be lost.
///
/// A failed sync (e.g. transient I/O error) leaves
/// the path in the set; next tick retries.
fn spawn_fsync_thread() {
    std::thread::Builder::new()
        .name("mntrs-fsync".to_string())
        .spawn(|| {
            // Bug 10 (alloc + batch): reuse a single
            // Vec across ticks so a sustained
            // workload doesn't churn the allocator
            // with a fresh ~2 MiB Vec every 5 s. The
            // `clear()` before `extend` preserves
            // capacity; only the first tick allocates
            // up to MAX_FSYNC_BATCH_PER_TICK PathBufs
            // worth of pointer slots.
            let mut paths: Vec<PathBuf> = Vec::with_capacity(MAX_FSYNC_BATCH_PER_TICK);
            let mut tick: u64 = 0;
            loop {
                std::thread::sleep(std::time::Duration::from_secs(FSYNC_INTERVAL_SECS));
                tick = tick.wrapping_add(1);
                // Bug 10 (batch cap): take at most
                // MAX_FSYNC_BATCH_PER_TICK paths per
                // tick. iter().take(N) bounds the
                // iter lock-hold time (DashSet holds a
                // per-shard read lock during the
                // iterator's lifetime); processing
                // the snapshot OUTSIDE the iter is
                // unchanged. Overflow stays in the
                // set and rolls into the next tick.
                paths.clear();
                paths.extend(
                    DIRTY_CACHE_PATHS
                        .iter()
                        .take(MAX_FSYNC_BATCH_PER_TICK)
                        .map(|r| r.clone()),
                );
                // Bug 10 (backlog warn): if the set
                // still has FSYNC_BACKLOG_WARN_MULT× the
                // batch worth of entries AFTER we
                // took our slice, the fsync thread
                // can't keep up. Log every
                // BACKLOG_LOG_EVERY_N_TICKS ticks so a
                // genuinely flooded mount surfaces in
                // logs without spamming every tick.
                if paths.len() == MAX_FSYNC_BATCH_PER_TICK
                    && tick.is_multiple_of(BACKLOG_LOG_EVERY_N_TICKS)
                {
                    let remaining = DIRTY_CACHE_PATHS.len();
                    if remaining >= MAX_FSYNC_BATCH_PER_TICK * FSYNC_BACKLOG_WARN_MULT {
                        tracing::warn!(
                            backlog = remaining,
                            batch_cap = MAX_FSYNC_BATCH_PER_TICK,
                            interval_secs = FSYNC_INTERVAL_SECS,
                            "fsync backlog growing — the write rate exceeds the per-tick fsync \
                             drain; durability window is widening past one interval"
                        );
                    }
                }
                for path in paths.iter() {
                    match std::fs::File::open(path) {
                        Ok(f) => {
                            if f.sync_data().is_ok() {
                                DIRTY_CACHE_PATHS.remove(path);
                            }
                            // Sync failed: keep in set,
                            // retry next tick. Don't
                            // log here — a transient
                            // ENOSPC or EIO during one
                            // tick will spam the log
                            // every 5 s otherwise.
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            // File was evicted between
                            // write and sync. Drop the
                            // tracking entry.
                            DIRTY_CACHE_PATHS.remove(path);
                        }
                        Err(_) => {
                            // Transient open failure
                            // (EACCES, ENOMEM, …):
                            // keep, retry next tick.
                        }
                    }
                }
            }
        })
        .expect("failed to spawn fsync thread");
}

// ── Submission API ──────────────────────────────────────────────────

/// Submit a write job to the IO thread pool. Called
/// from the FUSE worker; returns immediately (the
/// disk I/O happens in a worker thread).
///
/// If the pool hasn't been initialized (e.g. a test
/// that bypasses `new_test_fs`), this falls back to
/// running the job synchronously on the FUSE worker.
/// The fallback preserves correctness in tests that
/// haven't set up the pool; production mounts
/// always init it during mount.
pub(crate) fn submit_disk_write(job: Option<DiskWriteJob>) {
    let Some(job) = job else { return };
    if let Some(tx) = DISK_WRITE_POOL.get() {
        // `send` returns Err only if there are zero
        // receivers. We hold N receiver clones in the
        // worker threads, so this can only fail if the
        // runtime is shutting down (e.g. process exit).
        // Fall back to sync execution in that case.
        if let Err(e) = tx.send(job) {
            // Issue #268.1 O5: surface channel-closed
            // fallback. Pre-fix this was silent; the
            // operator couldn't tell whether the pool
            // was alive or whether the runtime was
            // tearing down.
            tracing::warn!(
                error = %e,
                "disk-IO channel closed (runtime shutting down?); executing job synchronously"
            );
            e.0.execute();
        }
    } else {
        // Issue #268.1 O5: pool-not-initialized
        // fallback. Production mounts always init
        // the pool; this branch is reachable only
        // in tests that bypassed new_test_fs.
        // Pre-fix it was silent — a test author
        // couldn't tell why the cache write took
        // the sync path.
        tracing::warn!(
            "submit_disk_write: pool not initialized; executing synchronously on caller"
        );
        job.execute();
    }
}

/// Submit a single block-cache write to the pool.
/// Used by the read path after a remote S3 fetch:
/// instead of writing N blocks serially in the FUSE
/// worker, build N jobs and submit all of them; the
/// pool's worker threads run them in parallel, so the
/// cold-read latency drops from O(N × block_write) to
/// O(max(worker_block_writes)) — typically just one
/// block's worth of disk I/O.
pub(crate) fn submit_block_cache_write(
    cache_dir: &std::path::Path,
    remote_path: &str,
    block_idx: u64,
    data: Vec<u8>,
) {
    let blk_path = crate::cache_block_path(cache_dir, remote_path, block_idx);
    let job = DiskWriteJob {
        cache_fd: None,
        cache_path: None,
        remote_path: remote_path.to_string(),
        offset: 0,
        data,
        block_cache: Some(blk_path),
        cache_gen: path_cache_gen(remote_path),
    };
    submit_disk_write(Some(job));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch_dir(label: &str) -> std::path::PathBuf {
        let d =
            std::env::temp_dir().join(format!("mntrs-dwp-test-{}-{}", label, std::process::id()));
        let _ = std::fs::create_dir_all(&d);
        d
    }

    #[test]
    fn execute_block_cache_mode_writes_file() {
        let dir = scratch_dir("blk");
        let blk_path = dir.join("test.block");
        let job = DiskWriteJob {
            cache_fd: None,
            cache_path: None,
            remote_path: "remote/path.bin".into(),
            offset: 0,
            data: vec![0xAB; 4096],
            block_cache: Some(blk_path.clone()),
            cache_gen: 0,
        };
        job.execute();
        assert!(blk_path.exists(), "block file should be created");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn execute_cache_path_mode_writes_and_registers_dirty() {
        let dir = scratch_dir("cp");
        let cpath = dir.join("cache-file.bin");
        let job = DiskWriteJob {
            cache_fd: None,
            cache_path: Some(cpath.clone()),
            remote_path: "remote/cp.bin".into(),
            offset: 0,
            data: b"cache path data".to_vec(),
            block_cache: None,
            cache_gen: 0,
        };
        job.execute();
        assert!(cpath.exists(), "cache file should exist");
        assert_eq!(std::fs::read(&cpath).unwrap(), b"cache path data");
        assert!(DIRTY_CACHE_PATHS.contains(&cpath));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn execute_cache_path_mode_appends_at_offset() {
        let dir = scratch_dir("cpoff");
        let cpath = dir.join("offset.bin");
        std::fs::write(&cpath, b"aaaaaaaaaa").unwrap();
        let job = DiskWriteJob {
            cache_fd: None,
            cache_path: Some(cpath.clone()),
            remote_path: "offset.bin".into(),
            offset: 10,
            data: b"bbbbbbbbbb".to_vec(),
            block_cache: None,
            cache_gen: 0,
        };
        job.execute();
        assert_eq!(std::fs::read(&cpath).unwrap(), b"aaaaaaaaaabbbbbbbbbb");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn execute_cache_fd_mode_writes_via_fd() {
        let dir = scratch_dir("fd");
        let cpath = dir.join("fd-cache.bin");
        let f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&cpath)
            .unwrap();
        let f = Arc::new(std::sync::Mutex::new(f));
        let job = DiskWriteJob {
            cache_fd: Some(f),
            cache_path: Some(cpath.clone()),
            remote_path: "fd.bin".into(),
            offset: 0,
            data: b"fd data".to_vec(),
            block_cache: None,
            cache_gen: 0,
        };
        job.execute();
        assert_eq!(std::fs::read(&cpath).unwrap(), b"fd data");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn register_dirty_idempotent() {
        let p = std::path::PathBuf::from("/tmp/mntrs-test-dirty-xxx");
        register_dirty_cache_path(&p);
        assert!(DIRTY_CACHE_PATHS.contains(&p));
        let before = DIRTY_CACHE_PATHS.len();
        register_dirty_cache_path(&p);
        assert_eq!(DIRTY_CACHE_PATHS.len(), before);
        DIRTY_CACHE_PATHS.remove(&p);
    }
}
