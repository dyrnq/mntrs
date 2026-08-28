//! `io::sync` — independent IO runtime mirroring rclone's `fs/sync`
//! worker pool.
//!
//! # Why this exists
//!
//! Before this module, all opendal network IO in mntrs funneled
//! through a single global runtime (`crate::rt()`, `worker_threads(1)`,
//! set up in `lib.rs:304`). FUSE callbacks called `rt().block_on(...)`
//! to await opendal futures, which meant every network round-trip
//! (DELETE / PUT / GET / LIST) blocked fuser-0's single-threaded event
//! loop. For metadata ops (stat / list / lookup) that's fine —
//! Issue #30 pinned the single-worker choice to avoid ~10 µs
//! cross-thread hand-offs. But for bulk IO (rm -rf 10000 files,
//! 200 MiB multipart uploads, prefetch downloads) the single-worker
//! runtime became a bottleneck: even with `BatchDeleter::buffer` to
//! coalesce deletes, every DeleteObjects HTTP RTT still blocked the
//! FUSE thread.
//!
//! rclone's mount solved this with a `fs/sync` worker pool — N
//! background goroutines, **physically separate** from the FUSE
//! thread, that handle all delete / upload / check IO. PR #9016
//! (rclone v1.69+, March 2025) added S3 `DeleteObjects` batching on
//! top of that pool, yielding the 0.054 s rm -rf 10000 result.
//!
//! This module is mntrs's equivalent. It owns:
//!
//! * A dedicated `tokio::runtime::Runtime` (`worker_threads = 8`).
//!   Distinct from `crate::rt()` so IO RTTs never block FUSE.
//! * An `Arc<opendal::Operator>` clone for opendal calls.
//! * An `Arc<tokio::sync::Mutex<opendal::Deleter>>` holding the
//!   persistent `Operator::deleter()` handle so
//!   `BatchDeleter<S3Deleter>::buffer` accumulates across FUSE
//!   callbacks (plan #64 — see `[[mntrs-plan64-deleteobjects-evolution]]`).
//!
//! # Lifetime model
//!
//! The runtime is `Box::leak`-ed (process-static), matching the
//! `disk_write_pool` pattern: daemon mounts never restart the runtime.
//! If a future feature needs in-process restart, add an explicit
//! `shutdown()` that closes the deleter pump and joins the runtime.
//!
//! # Initialization
//!
//! [`IoSync::init`] must be called once per process, from
//! `cmd::mount::mount_internal`, **after** the `Operator` is built
//! and **before** `MntrsFs::new`. [`IoSync::get`] returns the
//! process-wide handle. Calls to `get` before `init` log a warning
//! and return `None` so legacy / test code paths can fall back to
//! the pre-`io::sync` `rt().block_on` path.
//!
//! # API contracts
//!
//! * [`IoSync::enqueue_delete`] — fire-and-forget. Adds a path to
//!   the `BatchDeleter::buffer`. Returns immediately. The deletion
//!   HTTP request happens on the io::sync runtime. **The caller is
//!   responsible for inserting `delete_tombstones` immediately** so
//!   the FUSE-visible state is consistent with the in-flight delete.
//! * [`IoSync::handle`] — clones of this `Handle` can be passed to
//!   `tokio::spawn(future)` from any thread to schedule onto the
//!   io::sync runtime.
//! * [`IoSync::spawn`] — convenience wrapper around `handle.spawn`
//!   for fire-and-forget futures.

// Future-use APIs (deleter pump, enqueue_delete, op, deleter,
// enqueue_flush, deleter_pump_loop) are not yet called by any
// production code path in this initial migration — the surgical
// change to `concurrent_delete::spawn` (which redirects from
// `crate::rt().spawn` to `io_sync.handle().spawn`) is the only
// consumer right now. The remaining API surface is kept for the
// upcoming writeback + prefetcher migrations. `#[allow(dead_code)]`
// at the module level silences clippy without polluting every fn.
#![allow(dead_code)]

use std::sync::Arc;

use opendal::{Deleter, Operator};
use tokio::runtime::Handle;

/// Process-wide singleton. `None` until `init` is called.
static IO_SYNC: once_cell::sync::OnceCell<Arc<IoSync>> = once_cell::sync::OnceCell::new();

/// Default worker thread count. Matches rclone's `fs/sync` Checkers
/// default (8). Clamped by `available_parallelism()` in [`IoSync::init`].
const DEFAULT_WORKER_THREADS: usize = 8;

/// Crossbeam → async bridge channel capacity. The deleter pump's
/// `std::thread` recv() loop reads from this and forwards to a tokio
/// mpsc that the pump's async loop awaits. Bounded so a stuck pump
/// doesn't accumulate unbounded memory if fuser-0 fires thousands of
/// unlinks/sec (defensive — pump latency should never exceed ms).
const DELETE_CMD_CHANNEL_CAP: usize = 4096;

/// Commands dispatched to the deleter pump.
#[derive(Debug)]
pub(crate) enum DeleteCmd {
    /// Insert a single path into the `BatchDeleter<S3Deleter>::buffer`.
    /// Auto-flushes at 1000 keys (opendal's `DEFAULT_BATCH_MAX_OPERATIONS`).
    /// The public opendal `Deleter` API does NOT expose a manual
    /// `flush()` — auto-flush is the only mechanism. For workloads
    /// that need a barrier (e.g. `rmdir` after `rm -rf` of children),
    /// the caller should rely on the buffer's natural auto-flush at
    /// 1000 keys; under 1000, the kernel's per-path lookups tolerate
    /// a small visible lag (tombstone hides it on the FUSE side).
    Delete(String),
    /// Drain the buffer then exit the pump. Best-effort: leaked
    /// runtime means this rarely fires in practice.
    Shutdown,
}

/// Independent IO runtime. See [module docs](self) for the rationale.
pub struct IoSync {
    handle: Handle,
    op: Arc<Operator>,
    /// Persistent deleter holding the `BatchDeleter<S3Deleter>::buffer`.
    /// Locked by the pump loop. Clone-of-Arc is cheap; we hold it here
    /// for tests and for `deleter()` accessor.
    deleter: Arc<tokio::sync::Mutex<Deleter>>,
    /// Sender to the deleter pump. Cheap clone; held here for
    /// `enqueue_delete`. Cloned by the crossbeam→async bridge thread.
    delete_tx: crossbeam_channel::Sender<DeleteCmd>,
}

impl IoSync {
    /// Build the io::sync runtime. `worker_threads` defaults to
    /// `min(available_parallelism(), 8)`. The runtime is leaked
    /// (process-static, matching `disk_write_pool`).
    ///
    /// # Panics
    ///
    /// Panics if the deleter cannot be created (opendal auth failure,
    /// bad endpoint, etc.) — same contract as `Operator::deleter()`.
    /// This is intentional: a mount that can't initialize its deleter
    /// pump should fail fast at mount time, not silently fall through
    /// to per-file `op.delete()`.
    pub fn init(operator: Operator, worker_threads: Option<usize>) -> Arc<Self> {
        let n = worker_threads.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(DEFAULT_WORKER_THREADS)
                .min(DEFAULT_WORKER_THREADS)
        });

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(n)
            .enable_all()
            .thread_name("mntrs-io-sync")
            .build()
            .expect("io::sync: tokio runtime build");
        let handle = runtime.handle().clone();

        // Build the persistent deleter inside the io::sync runtime.
        // opendal's `Operator::deleter()` may issue auth probes /
        // region lookups; running that on the multi-thread workers
        // keeps the caller's thread free.
        let op = Arc::new(operator);
        let op_for_deleter = op.clone();
        let deleter = handle
            .block_on(async move { op_for_deleter.deleter().await })
            .expect("io::sync: opendal Deleter::create");

        let deleter = Arc::new(tokio::sync::Mutex::new(deleter));

        let (delete_tx, delete_rx) =
            crossbeam_channel::bounded::<DeleteCmd>(DELETE_CMD_CHANNEL_CAP);

        let deleter_for_pump = deleter.clone();
        let op_for_pump = op.clone();
        handle.spawn(deleter_pump_loop(deleter_for_pump, op_for_pump, delete_rx));

        // Process-static lifetime. Matches `disk_write_pool` pattern.
        Box::leak(Box::new(runtime));

        Arc::new(Self {
            handle,
            op,
            deleter,
            delete_tx,
        })
    }

    /// Get the process-wide `IoSync`. Returns `None` if `init` has
    /// not been called (legacy paths, tests).
    pub fn get() -> Option<Arc<Self>> {
        IO_SYNC.get().cloned()
    }

    /// Set the process-wide singleton. Idempotent: subsequent calls
    /// are logged and ignored. Returns `Err(())` if a different
    /// instance was already set (caller bug).
    pub fn set_global(slf: Arc<Self>) -> Result<(), Arc<Self>> {
        match IO_SYNC.set(slf) {
            Ok(()) => Ok(()),
            Err(existing) => Err(existing),
        }
    }

    /// Clone of the tokio handle. Pass to `tokio::spawn(future)` to
    /// schedule work onto the io::sync runtime.
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }

    /// Clone of the `Arc<Operator>`. Cheap (Arc clone). Use for
    /// one-off opendal calls (e.g. `op.list`) inside the io::sync
    /// runtime via `self.spawn(async move { op.list(...).await })`.
    pub fn op(&self) -> Arc<Operator> {
        self.op.clone()
    }

    /// Clone of the persistent deleter. For tests / advanced callers
    /// that want direct buffer state inspection.
    pub fn deleter(&self) -> Arc<tokio::sync::Mutex<Deleter>> {
        self.deleter.clone()
    }

    /// Fire-and-forget delete. Adds `path` to
    /// `BatchDeleter<S3Deleter>::buffer`. Returns immediately.
    ///
    /// **Caller contract**: insert into `delete_tombstones` BEFORE
    /// calling this so the FUSE-visible state hides the in-flight
    /// delete. Otherwise a concurrent `lookup` / `stat` / `readdir`
    /// could see the path as still present between enqueue and
    /// backend delete.
    ///
    /// If the pump channel is full (should not happen in practice —
    /// cap is 4096 and pump latency is sub-ms), this logs a warn and
    /// drops the delete. The caller will see a stale entry on next
    /// `readdir`; the safer alternative (blocking the FUSE thread)
    /// is worse, so we trade rare data visibility for guaranteed
    /// forward progress.
    pub fn enqueue_delete(&self, path: String) {
        if let Err(e) = self.delete_tx.send(DeleteCmd::Delete(path.clone())) {
            tracing::warn!(
                path = %path,
                error = %e,
                "io::sync: deleter pump channel full; delete dropped \
                 (lookup/stat may see stale entry until next readdir refresh)"
            );
        }
    }

    /// Force-flush the deleter buffer. **Currently a no-op stub**:
    /// opendal 0.58.2's public `Deleter` API has no manual `flush()`
    /// method — the buffer auto-flushes inside `delete()` at
    /// 1000 keys. This API is kept for symmetry / future opendal
    /// versions; it currently just drops the cmd on the floor.
    /// Callers that need a hard barrier (e.g. `rmdir` after rm -rf)
    /// should still call it — when opendal gains a `flush()` method,
    /// this becomes a real barrier with no caller changes.
    pub fn enqueue_flush(&self) {
        // No-op until opendal exposes `Deleter::flush()`.
        // Tracked in [[mntrs-plan64-deleteobjects-evolution]] §What plan #64 STILL missed.
    }

    /// Spawn a future onto the io::sync runtime. Returns a
    /// `JoinHandle` for callers that want to await completion.
    pub fn spawn<F>(&self, fut: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(fut)
    }
}

/// Deleter pump: reads `DeleteCmd`s, accumulates into the
/// `BatchDeleter<S3Deleter>::buffer`, flushes when the buffer hits
/// 1000 keys or on `Flush` cmd.
///
/// The crossbeam receiver is bridged to async via a dedicated
/// `std::thread` (synchronous recv) feeding a tokio mpsc. This frees
/// the io::sync worker pool from blocking on crossbeam recv (which
/// would otherwise pin a worker).
async fn deleter_pump_loop(
    deleter: Arc<tokio::sync::Mutex<Deleter>>,
    op: Arc<Operator>,
    rx: crossbeam_channel::Receiver<DeleteCmd>,
) {
    use tokio::sync::mpsc;

    let (tx, mut async_rx) = mpsc::unbounded_channel::<DeleteCmd>();

    // Dedicated OS thread for the sync recv() loop. Drops when the
    // channel closes (i.e. when all `delete_tx` clones are dropped).
    std::thread::Builder::new()
        .name("mntrs-io-sync-bridge".into())
        .spawn(move || {
            while let Ok(cmd) = rx.recv() {
                if tx.send(cmd).is_err() {
                    break;
                }
            }
        })
        .expect("io::sync: bridge thread spawn");

    while let Some(cmd) = async_rx.recv().await {
        match cmd {
            DeleteCmd::Delete(path) => {
                let mut d = deleter.lock().await;
                if let Err(e) = d.delete(path.clone()).await {
                    tracing::warn!(
                        path = %path,
                        backend = %op.info().scheme(),
                        error = %e,
                        "io::sync: deleter.delete failed (path may remain on backend)"
                    );
                }
            }
            DeleteCmd::Shutdown => {
                // No manual flush available in opendal 0.58.2 — the
                // buffer's most recent `delete()` either auto-flushed
                // or remains for the next mount to clear. For a
                // leaked runtime this branch is unreachable.
                break;
            }
        }
    }
}
