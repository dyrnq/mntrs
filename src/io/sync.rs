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
//! runtime became a bottleneck: every DeleteObjects HTTP RTT still
//! blocked the FUSE thread.
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
//!
//! Deleters live with the `concurrent_delete` module (one per slot
//! of the BatchDeleter-backed pump); they are no longer created
//! here.
//!
//! # Lifetime model
//!
//! The runtime is `Box::leak`-ed (process-static), matching the
//! `disk_write_pool` pattern: daemon mounts never restart the runtime.
//! If a future feature needs in-process restart, add an explicit
//! `shutdown()` that drops the runtime and joins.
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
//! * [`IoSync::handle`] — clones of this `Handle` can be passed to
//!   `tokio::spawn(future)` from any thread to schedule onto the
//!   io::sync runtime. Used by `concurrent_delete::spawn` (pump
//!   tasks) and `prefetcher` (read-ahead downloads).
//! * [`IoSync::spawn`] — convenience wrapper around `handle.spawn`
//!   for fire-and-forget futures.

use std::sync::Arc;

use opendal::Operator;
use tokio::runtime::Handle;

/// Process-wide singleton. `None` until `init` is called.
static IO_SYNC: once_cell::sync::OnceCell<Arc<IoSync>> = once_cell::sync::OnceCell::new();

/// Default worker thread count. Matches rclone's `fs/sync` Checkers
/// default (8). Clamped by `available_parallelism()` in [`IoSync::init`].
const DEFAULT_WORKER_THREADS: usize = 8;

/// Independent IO runtime. See [module docs](self) for the rationale.
///
/// The runtime carries a tokio handle and the `Arc<Operator>` the
/// opendal call sites were built with. The persistent `Deleter` is
/// no longer stored here — the `concurrent_delete` pump owns its
/// own per-slot `Deleter` rooted at `op.info().root()` and uses
/// this runtime's handle to schedule its N pump tasks.
pub struct IoSync {
    handle: Handle,
    #[allow(dead_code)]
    op: Arc<Operator>,
}

impl IoSync {
    /// Build the io::sync runtime. `worker_threads` defaults to
    /// `min(available_parallelism(), 8)`. The runtime is leaked
    /// (process-static, matching `disk_write_pool`).
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

        let op = Arc::new(operator);

        // Process-static lifetime. Matches `disk_write_pool` pattern.
        Box::leak(Box::new(runtime));

        Arc::new(Self { handle, op })
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
}
