//! opendal `BatchDeleter<S3Deleter>`-backed concurrent delete pump.
//!
//! Replaces the previous N-concurrent-single-DELETE pump with opendal's
//! `BatchDeleter<S3Deleter>` (`POST /?delete`, ≤1000 keys per
//! round-trip). Each [`DeleterSlot`] owns one opendal [`opendal::Deleter`]
//! rooted at `op.info().root()`; opendal's internal HashSet buffers up
//! to 1000 keys, auto-flushing via `S3Deleter::delete_batch` when the
//! cap is reached. We pair opendal's buffer with a per-slot side ledger
//! so per-key `oneshot::Receiver` acks + per-key tombstone removal still
//! work — the public API surface (`enqueue`, `flush`, `cancel_pending`,
//! `tombstones`, `is_accepting`, `shutdown`) is preserved byte-for-byte
//! and the FUSE-side callers in `src/lib.rs` need zero changes.
//!
//! ## Why this exists
//!
//! PR #616 added N=8 concurrent single-DELETE workers on a dedicated
//! io::sync runtime, but the underlying HTTP RTT remained: with N=8
//! single DELETEs against reqwest's 16-conn-per-host pool, we still
//! make 10000 round-trips for `rm -rf 10000`. rclone makes **~10**
//! via S3 `DeleteObjects` (one POST per ≤1000 keys). opendal 0.58
//! already has the wire-format plumbing end-to-end (`S3Deleter::delete_batch`
//! via `core.s3_delete_objects`); we just needed to call it from a
//! pump that outlives the FUSE callback.
//!
//! ## Architecture
//!
//! * **N pump tasks** (default 8, rclone `--checkers=8` parity). Each
//!   task owns one [`DeleterSlot`].
//! * **`DeleterSlot`** = `{ Arc<tokio::sync::Mutex<opendal::Deleter>>,
//!   Arc<std::sync::Mutex<VecDeque<PendingDelete>>>, worker_count,
//!   worker_id, AtomicU64 barrier_ack, AtomicUsize last_flush_count }`.
//! * **`Shared`** holds the pending queue + N slot handles + the
//!   tombstone set + the rmdir-barrier epoch counter.
//! * **No drain worker** — opendal owns the buffer; the pump just
//!   feeds it and drains on barrier / shutdown.
//!
//! ## API preservation
//!
//! * `enqueue(relative_path) -> Option<Receiver<io::Result<()>>>`:
//   push to opendal's HashSet (via slot's `Deleter::delete`), push to
//!   side ledger, return per-key receiver.
//! * `flush() -> io::Result<usize>`: rmdir barrier. Bump epoch, wait
//!   for every slot's `barrier_ack >= epoch`, sum `last_flush_count`.
//! * `cancel_pending(relative_path) -> usize`: scan shared pending +
//!   every slot's ledger for matching jobs, ack them with
//!   `Interrupted`. Clear tombstone.
//! * `shutdown(drain: bool)`: `drain=true` calls `flush()` first,
//!   then `fail_all_pending` per slot (best-effort). `drain=false`
//!   just calls `fail_all_pending`.
//!
//! plan: /home/devops/.claude/plans/virtual-noodling-salamander.md

#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opendal::{ErrorKind as OpendalErrorKind, Operator};
use tokio::sync::oneshot;

// ===== Constants =====

pub(crate) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const DEFAULT_MAX_RETRIES: u32 = 3;
pub(crate) const DEFAULT_RETRY_FACTOR: f64 = 2.0;
pub(crate) const DEFAULT_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Default pump pool size. Matches rclone `--checkers=8`. With
/// opendal's BatchDeleter auto-flushing at 1000 keys per slot, the
/// effective ceiling on `rm -rf N` round-trips is `ceil(N/8000)` —
/// 2 round-trips for N=10000 (8 slots × 1000-key buffer).
pub(crate) const DEFAULT_DELETE_WORKER_COUNT: usize = 8;

/// Hard upper bound on the pump pool. Each slot holds a clone of the
/// opendal `Deleter` (cheap; `Arc<S3Core>` is shared) + a tokio mutex
/// + a std::sync::Mutex for the side ledger. Memory pressure is
///   bounded by `Arc<S3Core>` not by the slot count. 16 is generous
///   even on a 32-core box.
pub(crate) const MAX_DELETE_WORKER_COUNT: usize = 16;

// ===== Counters =====
//
// Process-static, like writeback::PENDING_COUNT. Read from any thread
// via [`snapshot`] below.

/// Number of `Deleter::close()` invocations (= S3 DeleteObjects
/// round-trips + single DELETE for size-1 buffers).
static FLUSHES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Number of paths whose `Deleter::delete()` was acked with `Ok`.
static KEYS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Number of paths whose `Deleter::delete()` was acked with `Err`.
static FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Number of paths dropped on shutdown without ever reaching S3.
static SHUTDOWN_LOST_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Number of `close()` retry decisions (status / transport). Each
/// retry of a `close()` increments this once. opendal itself retries
/// inside `HttpClient`, so this only counts our top-level retry loop.
static BATCH_RETRY_TOTAL: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
pub(crate) struct CounterSnapshot {
    pub flushes_total: u64,
    pub keys_total: u64,
    pub failures_total: u64,
    pub shutdown_lost_total: u64,
    /// Per-`close()` retry decisions. Useful for bench harness.
    pub batch_retry_total: u64,
}

pub(crate) fn snapshot() -> CounterSnapshot {
    CounterSnapshot {
        flushes_total: FLUSHES_TOTAL.load(Ordering::Relaxed),
        keys_total: KEYS_TOTAL.load(Ordering::Relaxed),
        failures_total: FAILURES_TOTAL.load(Ordering::Relaxed),
        shutdown_lost_total: SHUTDOWN_LOST_TOTAL.load(Ordering::Relaxed),
        batch_retry_total: BATCH_RETRY_TOTAL.load(Ordering::Relaxed),
    }
}

// ===== Env tuning =====

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(default)
}

// ===== Pending types =====

/// One queued delete. The result sender is consumed by the pump
/// after the S3 response arrives; on shutdown or retry-exhausted
/// failure, it is dropped or sent `Err(...)`.
pub(crate) struct PendingDelete {
    pub relative_path: String,
    pub result_tx: oneshot::Sender<std::io::Result<()>>,
}

// ===== Shared state =====

struct Shared {
    /// Queue of jobs pushed by FUSE callbacks, awaiting pickup by
    /// any pump task. FIFO across all slots: each pump task pops
    /// one at a time. The std::sync::Mutex is held only across the
    /// `pop_front()` (microseconds).
    pending: Mutex<std::collections::VecDeque<PendingDelete>>,
    accepting: AtomicBool,
    /// Wake signal for the pump tasks. `enqueue()` calls
    /// `notify_one()` after pushing a job. `flush()` calls
    /// `notify_waiters()` to fan out the barrier.
    notify: tokio::sync::Notify,
    /// Tombstones shared with `MntrsFs`. Insert by FUSE on unlink;
    /// remove by pump on every per-key terminal outcome.
    tombs: Arc<dashmap::DashSet<String>>,
    /// N pump slots. Each holds its own opendal Deleter (Arc-shared
    /// `S3Core` so the reqwest pool is global) + side ledger +
    /// barrier ack counter + last flush count.
    slots: Arc<Vec<DeleterSlot>>,
    /// Monotonically increasing flush-barrier epoch. `flush()`
    /// increments and waits for `slots[*].barrier_ack >= epoch` on
    /// every slot before returning. `pump_loop` checks the epoch
    /// after each drain and acks if it advanced.
    barrier_epoch: AtomicU64,
}

// ===== Deleter slot =====

struct DeleterSlot {
    deleter: Arc<tokio::sync::Mutex<opendal::Deleter>>,
    /// Side ledger of jobs we've pushed to opendal's HashSet but not
    /// yet drained via `Deleter::close()`. After a successful
    /// `close()`, every entry is acked `Ok` and dropped.
    ledger: Arc<std::sync::Mutex<std::collections::VecDeque<PendingDelete>>>,
    /// Per-slot retry knobs (kept on the slot for `flush_with_retry`).
    config: WorkerConfig,
    worker_id: usize,
    /// `barrier_ack` counter incremented by the pump after each
    /// successful barrier flush. Read by `flush()` to know when all
    /// slots have caught up to a given epoch. `Arc`-wrapped so the
    /// pump task can hold its own clone (no raw pointer).
    barrier_ack: Arc<AtomicU64>,
    /// Size of the last `close()`'s drained batch (for `flush()`'s
    /// return value sum). `Arc`-wrapped so the pump task can hold
    /// its own clone (no raw pointer).
    last_flush_count: Arc<AtomicUsize>,
}

// ===== Public handle =====

/// Cheap-clone handle for FUSE callbacks to enqueue deletes.
/// Dropping all clones drops the `Shared` Arc; pump tasks exit
/// when the runtime drops (their `Arc<Shared>` refs drop too).
#[derive(Clone)]
pub(crate) struct ConcurrentDeleter {
    shared: Arc<Shared>,
}

// ===== Worker config =====

/// Construction-time config. Built by `cmd/mount.rs::build_s3` after
/// parsing the storage URL and CLI options; passed to
/// `concurrent_delete::spawn`.
#[derive(Clone)]
pub(crate) struct WorkerConfig {
    /// opendal operator. The `Deleter` returned by
    /// `op.deleter().await` is rooted at `op.info().root()`.
    pub op: Arc<Operator>,
    /// Number of concurrent pump tasks (default 8, rclone-parity).
    /// Each task owns one DeleterSlot.
    pub worker_count: usize,
    pub request_timeout: Duration,
    pub max_retries: u32,
    pub retry_factor: f64,
    pub retry_initial_backoff: Duration,
}

impl WorkerConfig {
    /// Build the production config from an opendal Operator.
    pub(crate) fn from_operator(op: Arc<Operator>) -> Self {
        let worker_count = env_usize("MNTRS_DELETE_WORKER_COUNT", DEFAULT_DELETE_WORKER_COUNT)
            .clamp(1, MAX_DELETE_WORKER_COUNT);
        Self {
            op,
            worker_count,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_retries: DEFAULT_MAX_RETRIES,
            retry_factor: DEFAULT_RETRY_FACTOR,
            retry_initial_backoff: DEFAULT_RETRY_INITIAL_BACKOFF,
        }
    }
}

// ===== Spawn =====

/// Handle bundle returned by `spawn`. Drop to abandon the pumps
/// (cleaned up when the io::sync runtime drops). Awaiting the
/// `deleters` JoinHandles is supported for graceful-await callers.
pub(crate) struct WorkerHandles {
    pub(crate) deleters: Vec<tokio::task::JoinHandle<()>>,
}

pub(crate) fn spawn(
    config: WorkerConfig,
    tombs: Arc<dashmap::DashSet<String>>,
) -> std::io::Result<(ConcurrentDeleter, WorkerHandles)> {
    let worker_count = config.worker_count.max(1);

    // Build the N DeleterSlots up front. Each slot needs its own
    // `opendal::Deleter` rooted at `op.info().root()` (opendal's
    // internal `Arc<S3Core>` is shared across all N, so the reqwest
    // connection pool is global — no fragmentation).
    //
    // We call `op.deleter().await` inside `io_sync.handle().block_on`
    // because opendal's Deleter constructor may issue auth probes /
    // region lookups, and the FUSE thread (which calls spawn) is not
    // inside a tokio runtime context.
    // Build the N DeleterSlots up front. Each slot needs its own
    // `opendal::Deleter` rooted at `op.info().root()` (opendal's
    // internal `Arc<S3Core>` is shared across all N, so the reqwest
    // connection pool is global — no fragmentation).
    //
    // The init runs on a dedicated OS thread because:
    //   * `op.deleter().await` may issue auth probes / region lookups.
    //   * The FUSE thread (production caller) is NOT inside any
    //     tokio runtime — so `Handle::block_on` works directly.
    //   * Tests call `spawn()` from inside `#[tokio::test]` —
    //     `Handle::block_on` from inside a runtime panics with
    //     "Cannot start a runtime from within a runtime". A
    //     dedicated OS thread is not part of any runtime, so the
    //     `block_on` succeeds.
    // The dedicated thread is one-shot (mount-time only) so the
    // overhead is negligible.
    let op_for_init = config.op.clone();
    let deleter_per_slot: Vec<Arc<tokio::sync::Mutex<opendal::Deleter>>> = {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name("mntrs-concdel-init".into())
            .spawn(move || {
                let init_handle = crate::io::sync::IoSync::get()
                    .map(|s| s.handle())
                    .unwrap_or_else(|| crate::rt().handle().clone());
                let result = init_handle.block_on(async move {
                    let mut out = Vec::with_capacity(worker_count);
                    for _ in 0..worker_count {
                        let d = op_for_init.deleter().await.map_err(|e| {
                            std::io::Error::other(format!(
                                "concurrent_delete::spawn: opendal Deleter::create failed: {e}"
                            ))
                        })?;
                        out.push(Arc::new(tokio::sync::Mutex::new(d)));
                    }
                    std::io::Result::Ok(out)
                });
                let _ = tx.send(result);
            })
            .expect("concurrent_delete::spawn: failed to spawn init thread");
        rx.recv()
            .expect("concurrent_delete::spawn: init thread panicked")?
    };

    let mut slots = Vec::with_capacity(worker_count);
    for (worker_id, deleter) in deleter_per_slot.into_iter().enumerate() {
        slots.push(DeleterSlot {
            deleter,
            ledger: Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new())),
            config: config.clone(),
            worker_id,
            barrier_ack: Arc::new(AtomicU64::new(0)),
            last_flush_count: Arc::new(AtomicUsize::new(0)),
        });
    }
    let slots = Arc::new(slots);

    let shared = Arc::new(Shared {
        pending: Mutex::new(std::collections::VecDeque::new()),
        accepting: AtomicBool::new(true),
        notify: tokio::sync::Notify::new(),
        tombs,
        slots: slots.clone(),
        barrier_epoch: AtomicU64::new(0),
    });
    let handle = ConcurrentDeleter {
        shared: shared.clone(),
    };

    // Spawn N pump tasks on io::sync. The io::sync runtime is
    // multi-threaded (worker_threads=8); each pump task is a real
    // OS-thread-residing future, so N concurrent `Deleter::close()`
    // round-trips can overlap. Falls back to `crate::rt()` (single-
    // threaded by Issue #30 design) if io::sync was not initialized
    // (test / legacy) — preserves pre-#616 behaviour for callers
    // that don't init io::sync.
    let spawn_handle = crate::io::sync::IoSync::get()
        .map(|s| s.handle())
        .unwrap_or_else(|| crate::rt().handle().clone());
    let mut deleter_handles = Vec::with_capacity(worker_count);
    for slot in slots.iter() {
        let sh = shared.clone();
        let slot_ref = DeleterSlotRef {
            deleter: slot.deleter.clone(),
            ledger: slot.ledger.clone(),
            config: slot.config.clone(),
            worker_id: slot.worker_id,
            barrier_ack: slot.barrier_ack.clone(),
            last_flush_count: slot.last_flush_count.clone(),
        };
        deleter_handles.push(spawn_handle.spawn(pump_loop(slot_ref, sh)));
    }

    Ok((
        handle,
        WorkerHandles {
            deleters: deleter_handles,
        },
    ))
}

// ===== ConcurrentDeleter API =====

impl ConcurrentDeleter {
    /// Enqueue a relative path for deletion. Returns `None` if the
    /// pump is shutting down; returns `Some(oneshot::Receiver)`
    /// otherwise. The receiver is acked by the pump after the
    /// per-key outcome (success / failure) is known.
    ///
    /// Per-key failure semantics: under load, opendal's
    /// `Deleter::close()` returns `Ok(())` even if a single key
    /// returned a non-temporary error from S3 (it removed the
    /// succeeded entries and silently dropped the failed one — see
    /// `opendal-core-0.58.2/src/raw/oio/delete/batch_delete.rs:120-128`).
    /// For our purposes, `close() Ok` ⇒ "all keys in the batch were
    /// processed" (succeeded or no-op-404). Failed oneshots are
    /// only emitted when the entire `close()` call exhausts retries.
    pub(crate) fn enqueue(
        &self,
        relative_path: String,
    ) -> Option<oneshot::Receiver<std::io::Result<()>>> {
        if !self.shared.accepting.load(Ordering::Acquire) {
            return None;
        }
        let (tx, rx) = oneshot::channel();
        let job = PendingDelete {
            relative_path,
            result_tx: tx,
        };
        let was_empty = {
            let mut pending = self.shared.pending.lock().expect("pending mutex poisoned");
            let was_empty = pending.is_empty();
            pending.push_back(job);
            was_empty
        };
        if was_empty {
            self.shared.notify.notify_one();
        }
        Some(rx)
    }

    /// Cancel any pending delete for `relative_path`. Used by
    /// `MntrsFs::create()` before the new write fires, so a
    /// create-after-rm doesn't hit two problems at once:
    ///
    ///   1. lookup/getattr/readdir still see the tombstone.
    ///   2. without cancel, the in-flight delete would race the
    ///      new write and either delete the freshly created
    ///      object or idempotent-404 it (false "tombstone leaked").
    ///
    /// Scan order:
    ///   a) Shared pending queue (not yet picked up).
    ///   b) Every slot's side ledger (picked up by pump, in opendal's
    ///      buffer or just-flushed). The µs window between
    ///      `deleter.delete` and `ledger.push_back` is not covered
    ///      — same race as the pre-#606 code.
    pub(crate) fn cancel_pending(&self, relative_path: &str) -> usize {
        let mut cancelled = 0usize;

        // (a) shared pending queue.
        {
            let mut pending = self.shared.pending.lock().expect("pending mutex poisoned");
            let mut kept = std::collections::VecDeque::with_capacity(pending.len());
            for job in pending.drain(..) {
                if job.relative_path == relative_path {
                    let _ = job.result_tx.send(Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "concurrent_deleter: cancelled by recreate",
                    )));
                    cancelled += 1;
                } else {
                    kept.push_back(job);
                }
            }
            *pending = kept;
        }

        // (b) every slot's side ledger.
        for slot in self.shared.slots.iter() {
            let mut ledger = slot.ledger.lock().unwrap();
            let mut kept = std::collections::VecDeque::with_capacity(ledger.len());
            for job in ledger.drain(..) {
                if job.relative_path == relative_path {
                    let _ = job.result_tx.send(Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "concurrent_deleter: cancelled by recreate",
                    )));
                    cancelled += 1;
                } else {
                    kept.push_back(job);
                }
            }
            *ledger = kept;
        }

        // (c) tombstone clear is unconditional — even if no job was
        // pending, the FUSE side may have a stale entry.
        self.shared.tombs.remove(relative_path);
        cancelled
    }

    /// Read-only access to the tombstone set for FUSE-side
    /// filters (lookup, getattr, readdir). Cheap Arc clone.
    pub(crate) fn tombstones(&self) -> Arc<dashmap::DashSet<String>> {
        self.shared.tombs.clone()
    }

    /// Force-flush all currently pending keys. Used by the rmdir
    /// barrier under Policy B so `rm -rf dir` doesn't return before
    /// the dir's deletes have actually been requested. Bumps the
    /// barrier epoch; waits up to 30 s for every slot's `barrier_ack`
    /// to reach the new epoch; sums each slot's `last_flush_count`.
    pub(crate) async fn flush(&self) -> std::io::Result<usize> {
        let epoch = self.shared.barrier_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        // Wake every sleeping pump. `notify_waiters()` is cheap (one
        // atomic op, no allocation); each pump checks the barrier
        // epoch after its current drain and acks if it advanced.
        self.shared.notify.notify_waiters();

        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let acked = self
                .shared
                .slots
                .iter()
                .all(|s| s.barrier_ack.load(Ordering::Acquire) >= epoch);
            if acked {
                break;
            }
            if std::time::Instant::now() >= deadline {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "concurrent_deleter: flush barrier timeout (30s)",
                ));
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let total: usize = self
            .shared
            .slots
            .iter()
            .map(|s| s.last_flush_count.load(Ordering::Relaxed))
            .sum();
        Ok(total)
    }

    /// Graceful shutdown. `drain=true` flushes pending keys
    /// (best-effort) before failing any in-flight oneshots;
    /// `drain=false` fails them immediately.
    pub(crate) async fn shutdown(self, drain: bool) {
        self.shared.accepting.store(false, Ordering::Release);
        if drain {
            let _ = self.flush().await;
        }
        for slot in self.shared.slots.iter() {
            fail_all_pending(slot, &self.shared);
        }
    }

    /// True if the pump is still accepting work.
    pub(crate) fn is_accepting(&self) -> bool {
        self.shared.accepting.load(Ordering::Acquire)
    }

    /// Snapshot of the pending queue length (not yet picked up by
    /// any pump). Cheap (mutex lock + len).
    pub(crate) fn pending_len(&self) -> usize {
        self.shared
            .pending
            .lock()
            .expect("pending mutex poisoned")
            .len()
    }
}

// ===== Pump loop =====
//
// One task per slot. Waits on `Shared::notify.notified()`, pops
// one job at a time from `Shared::pending`, pushes it into the
// slot's opendal `Deleter` (HashSet buffer + auto-flush at 1000),
// then records it in the side ledger. After the inner drain, checks
// the barrier epoch and calls `flush_with_retry` if it advanced.

struct DeleterSlotRef {
    deleter: Arc<tokio::sync::Mutex<opendal::Deleter>>,
    ledger: Arc<std::sync::Mutex<std::collections::VecDeque<PendingDelete>>>,
    config: WorkerConfig,
    worker_id: usize,
    /// Arc-clone of the slot's `barrier_ack` AtomicU64. Both
    /// `pump_loop` (writer) and `flush()` (reader) see the same
    /// counter.
    barrier_ack: Arc<AtomicU64>,
    /// Arc-clone of the slot's `last_flush_count` AtomicUsize.
    last_flush_count: Arc<AtomicUsize>,
}

unsafe impl Send for DeleterSlotRef {}
unsafe impl Sync for DeleterSlotRef {}

async fn pump_loop(slot: DeleterSlotRef, shared: Arc<Shared>) {
    tracing::info!(
        target: "mntrs::concurrent_delete",
        worker_id = slot.worker_id,
        worker_count = slot.config.worker_count,
        "concurrent_delete: pump started (opendal BatchDeleter)",
    );

    loop {
        let barrier_epoch = shared.barrier_epoch.load(Ordering::Acquire);

        // Drain everything currently in the shared pending queue.
        loop {
            let job = {
                let mut pending = shared.pending.lock().expect("pending mutex poisoned");
                pending.pop_front()
            };
            let Some(job) = job else {
                break;
            };

            // Push to opendal's HashSet buffer. Auto-flushes at 1000
            // keys (opendal's BatchDeleter::delete calls
            // flush_buffer when buffer.len() >= max_batch_size).
            // We don't need to manually close() at this boundary
            // because opendal handles it transparently.
            //
            // If Deleter::delete returns Err (opendal's batch
            // returned a non-temporary error mid-auto-flush), the
            // key MAY still be in opendal's buffer. The next
            // close() will retry. We log + push to ledger either
            // way; the per-key outcome is decided by close().
            let mut deleter = slot.deleter.lock().await;
            if let Err(e) = deleter.delete(job.relative_path.as_str()).await {
                tracing::warn!(
                    target: "mntrs::concurrent_delete",
                    worker_id = slot.worker_id,
                    path = %job.relative_path,
                    error = %e,
                    "concurrent_delete: Deleter::delete failed (will retry on next close)",
                );
                FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
            }
            slot.ledger.lock().unwrap().push_back(job);
            drop(deleter);
        }

        // Barrier check: a flush() or shutdown() raised the epoch.
        // Force a `close()` so the caller observes a quiescent state.
        if shared.barrier_epoch.load(Ordering::Acquire) != barrier_epoch {
            let _ = flush_with_retry(&slot, &shared).await;
            slot.barrier_ack.fetch_add(1, Ordering::AcqRel);
        }

        // Sleep until next enqueue or barrier. `notified()` is
        // edge-triggered: if `notify_one()` fired while we were
        // processing, the next call returns immediately. The
        // barrier path uses `notify_waiters()` to fan out.
        shared.notify.notified().await;
    }
}

// ===== flush_with_retry =====
//
// Drains the slot's side ledger (calling `deleter.close()` to flush
// opendal's buffer), acks each per-key oneshot with the batch-level
// outcome (Ok or Err), clears the tombstone for each key, and
// returns the number of keys processed. Retries on transport /
// non-temporary-batch failures up to `config.max_retries` with
// exponential backoff.

enum FlushOutcome {
    Success(usize),
    Error(usize, opendal::Error),
}

async fn flush_with_retry(slot: &DeleterSlotRef, shared: &Shared) -> std::io::Result<usize> {
    let mut attempt = 0u32;
    let mut backoff = slot.config.retry_initial_backoff;
    loop {
        // Drain our side ledger BEFORE calling close() so we can
        // know exactly which keys are "in flight" and ack them
        // based on close()'s outcome. opendal's close() may also
        // drain keys that were auto-flushed at 1000 (those are
        // already gone from opendal's buffer), but they're still
        // in our ledger awaiting ack — same outcome ack is correct.
        let to_flush: Vec<PendingDelete> = {
            let mut ledger = slot.ledger.lock().unwrap();
            ledger.drain(..).collect()
        };
        if to_flush.is_empty() {
            // Nothing to ack; opendal's buffer might still have
            // entries (rare — only if a previous delete() errored
            // before pushing to ledger), but they're not ours.
            return Ok(0);
        }
        let n = to_flush.len();

        let outcome = {
            let mut deleter = slot.deleter.lock().await;
            match tokio::time::timeout(slot.config.request_timeout, deleter.close()).await {
                Ok(Ok(())) => FlushOutcome::Success(n),
                Ok(Err(e)) => FlushOutcome::Error(n, e),
                Err(_elapsed) => FlushOutcome::Error(
                    n,
                    opendal::Error::new(
                        OpendalErrorKind::Unexpected,
                        format!(
                            "concurrent_delete: Deleter::close timed out after {:?}",
                            slot.config.request_timeout
                        ),
                    ),
                ),
            }
        };

        match outcome {
            FlushOutcome::Success(k) => {
                FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
                KEYS_TOTAL.fetch_add(k as u64, Ordering::Relaxed);
                for job in to_flush {
                    shared.tombs.remove(&job.relative_path);
                    let _ = job.result_tx.send(Ok(()));
                }
                slot.last_flush_count.store(k, Ordering::Relaxed);
                return Ok(n);
            }
            FlushOutcome::Error(k, err) => {
                if attempt < slot.config.max_retries {
                    tracing::warn!(
                        target: "mntrs::concurrent_delete",
                        worker_id = slot.worker_id,
                        attempt = attempt + 1,
                        keys = k,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %err,
                        "concurrent_delete: Deleter::close failed; retrying",
                    );
                    BATCH_RETRY_TOTAL.fetch_add(1, Ordering::Relaxed);
                    backoff = next_backoff(backoff, slot.config.retry_factor);
                    attempt += 1;
                    // Push keys back to ledger so the next loop
                    // iteration tries close() again with the same
                    // set. close() is idempotent on the S3 side
                    // (succeeded keys return 404 NoSuchKey →
                    // idempotent-Ok; failed ones retry).
                    slot.ledger.lock().unwrap().extend(to_flush);
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                // Retries exhausted — best-effort ack with Err.
                FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
                FAILURES_TOTAL.fetch_add(k as u64, Ordering::Relaxed);
                let err_msg = format!(
                    "concurrent_delete: Deleter::close exhausted {} retries: {}",
                    slot.config.max_retries, err
                );
                for job in to_flush {
                    shared.tombs.remove(&job.relative_path);
                    let _ = job
                        .result_tx
                        .send(Err(std::io::Error::other(err_msg.clone())));
                }
                return Err(std::io::Error::other(err_msg));
            }
        }
    }
}

fn next_backoff(current: Duration, factor: f64) -> Duration {
    Duration::from_secs_f64(current.as_secs_f64() * factor)
}

// ===== fail_all_pending =====
//
// Best-effort drain on shutdown. Calls `deleter.close()` to flush
// opendal's buffer (so any in-flight batch lands on S3), then
// fails every per-key oneshot with `BrokenPipe`. Tombstones are
// cleared so a subsequent recreate doesn't see ENOENT.

fn fail_all_pending(slot: &DeleterSlot, shared: &Shared) {
    // Best-effort close() to land any in-flight batch on S3.
    // Dedicated OS thread (see `spawn` rationale): callers run
    // either on the FUSE thread (no runtime — production) or
    // inside a `#[tokio::test]` runtime (tests); block_on from
    // inside a runtime panics. A one-shot thread avoids both
    // failure modes.
    let timeout = slot.config.request_timeout;
    let deleter_clone = slot.deleter.clone();
    let flush_ok = {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name("mntrs-concdel-shutdown-flush".into())
            .spawn(move || {
                let init_handle = crate::io::sync::IoSync::get()
                    .map(|s| s.handle())
                    .unwrap_or_else(|| crate::rt().handle().clone());
                let ok = init_handle.block_on(async move {
                    let mut deleter = deleter_clone.lock().await;
                    tokio::time::timeout(timeout, deleter.close()).await.is_ok()
                });
                let _ = tx.send(ok);
            })
            .expect("concurrent_delete::fail_all_pending: flush thread");
        rx.recv().unwrap_or(false)
    };

    // Drain pending queue + slot ledger, ack all with BrokenPipe.
    let pending_jobs: Vec<PendingDelete> = {
        let mut pending = shared.pending.lock().expect("pending mutex poisoned");
        std::mem::take(&mut *pending).into()
    };
    let ledger_jobs: Vec<PendingDelete> = {
        let mut ledger = slot.ledger.lock().unwrap();
        std::mem::take(&mut *ledger).into()
    };

    let mut lost = 0u64;
    for job in pending_jobs.into_iter().chain(ledger_jobs) {
        shared.tombs.remove(&job.relative_path);
        let _ = job.result_tx.send(Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "concurrent_deleter: shutdown lost",
        )));
        lost += 1;
    }
    if lost > 0 {
        SHUTDOWN_LOST_TOTAL.fetch_add(lost, Ordering::Relaxed);
        tracing::warn!(
            target: "mntrs::concurrent_delete",
            worker_id = slot.worker_id,
            lost,
            flush_ok,
            "concurrent_delete: shutdown dropped pending deletes",
        );
    }
}

// ===== Unit tests =====

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;

    /// Process-global mutex serialising all tests that read/write
    /// `MNTRS_DELETE_WORKER_COUNT` via `unsafe { std::env::set_var /
    /// remove_var }`. cargo runs tests in parallel by default;
    /// without this lock a `set_var` from one test can race a
    /// `remove_var` from another, leaving the worker_count field
    /// holding the wrong value when `WorkerConfig::from_operator`
    /// reads it.
    static WORKER_COUNT_ENV_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();
    fn env_lock() -> &'static std::sync::Mutex<()> {
        WORKER_COUNT_ENV_LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    fn memory_op() -> Arc<Operator> {
        Arc::new(Operator::new(opendal::services::Memory::default()).unwrap())
    }

    #[test]
    fn next_backoff_grows_by_factor() {
        let b0 = Duration::from_millis(100);
        assert_eq!(next_backoff(b0, 2.0), Duration::from_millis(200));
        assert_eq!(
            next_backoff(next_backoff(b0, 2.0), 2.0),
            Duration::from_millis(400)
        );
    }

    #[test]
    fn worker_config_from_operator_uses_defaults() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("MNTRS_DELETE_WORKER_COUNT");
        }
        let cfg = WorkerConfig::from_operator(memory_op());
        assert_eq!(cfg.request_timeout, DEFAULT_REQUEST_TIMEOUT);
        assert_eq!(cfg.max_retries, DEFAULT_MAX_RETRIES);
        assert_eq!(cfg.worker_count, DEFAULT_DELETE_WORKER_COUNT);
        assert_eq!(cfg.worker_count, 8);
    }

    #[test]
    fn counter_snapshot_is_const_default() {
        let s = CounterSnapshot::default();
        assert_eq!(s.flushes_total, 0);
        assert_eq!(s.keys_total, 0);
        assert_eq!(s.failures_total, 0);
        assert_eq!(s.shutdown_lost_total, 0);
        assert_eq!(s.batch_retry_total, 0);
    }

    #[test]
    fn worker_count_default_is_eight() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("MNTRS_DELETE_WORKER_COUNT");
        }
        let cfg = WorkerConfig::from_operator(memory_op());
        assert_eq!(cfg.worker_count, DEFAULT_DELETE_WORKER_COUNT);
        assert_eq!(cfg.worker_count, 8);
    }

    #[test]
    fn worker_count_clamped_to_max_16() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("MNTRS_DELETE_WORKER_COUNT", "999");
        }
        let cfg = WorkerConfig::from_operator(memory_op());
        assert_eq!(cfg.worker_count, MAX_DELETE_WORKER_COUNT);
        assert_eq!(cfg.worker_count, 16);
        unsafe {
            std::env::remove_var("MNTRS_DELETE_WORKER_COUNT");
        }
    }

    #[test]
    fn worker_count_clamped_to_min_1() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("MNTRS_DELETE_WORKER_COUNT", "0");
        }
        let cfg = WorkerConfig::from_operator(memory_op());
        assert_eq!(cfg.worker_count, 1);
        unsafe {
            std::env::remove_var("MNTRS_DELETE_WORKER_COUNT");
        }
    }

    // ===== Tombstone lifecycle =====

    #[test]
    fn cancel_pending_drains_jobs_and_clears_tombstone() {
        let tombs = Arc::new(dashmap::DashSet::<String>::new());
        let shared = Arc::new(Shared {
            pending: Mutex::new(std::collections::VecDeque::new()),
            accepting: AtomicBool::new(true),
            notify: tokio::sync::Notify::new(),
            tombs: tombs.clone(),
            slots: Arc::new(Vec::new()),
            barrier_epoch: AtomicU64::new(0),
        });
        let deleter = ConcurrentDeleter {
            shared: shared.clone(),
        };

        // Simulate what unlink does: enqueue + tombstone.
        tombs.insert("p".into());
        {
            let mut pending = shared.pending.lock().unwrap();
            let (otx, _orx) = oneshot::channel();
            pending.push_back(PendingDelete {
                relative_path: "p".into(),
                result_tx: otx,
            });
        }
        assert!(tombs.contains("p"));

        let n = deleter.cancel_pending("p");
        assert_eq!(n, 1, "exactly the one queued job must be cancelled");
        assert!(!tombs.contains("p"), "tombstone must be cleared");
        assert!(
            shared.pending.lock().unwrap().is_empty(),
            "queue must be drained"
        );
    }

    #[test]
    fn cancel_pending_unknown_path_clears_stale_tombstone() {
        let tombs = Arc::new(dashmap::DashSet::<String>::new());
        let shared = Arc::new(Shared {
            pending: Mutex::new(std::collections::VecDeque::new()),
            accepting: AtomicBool::new(true),
            notify: tokio::sync::Notify::new(),
            tombs: tombs.clone(),
            slots: Arc::new(Vec::new()),
            barrier_epoch: AtomicU64::new(0),
        });
        let deleter = ConcurrentDeleter {
            shared: shared.clone(),
        };
        tombs.insert("stale".into());
        let n = deleter.cancel_pending("stale");
        assert_eq!(n, 0);
        assert!(!tombs.contains("stale"));
    }

    #[test]
    fn cancel_pending_absent_path_is_noop() {
        let tombs = Arc::new(dashmap::DashSet::<String>::new());
        let shared = Arc::new(Shared {
            pending: Mutex::new(std::collections::VecDeque::new()),
            accepting: AtomicBool::new(true),
            notify: tokio::sync::Notify::new(),
            tombs: tombs.clone(),
            slots: Arc::new(Vec::new()),
            barrier_epoch: AtomicU64::new(0),
        });
        let deleter = ConcurrentDeleter {
            shared: shared.clone(),
        };
        let n = deleter.cancel_pending("never-existed");
        assert_eq!(n, 0);
        assert!(tombs.is_empty());
    }

    // ===== Integration: enqueue + flush drains memory backend =====
    //
    // Reproduces the BatchDeleter::buffer contract: 2500 enqueued
    // deletes should drain via flush. The Memory backend is
    // hermetic (no HTTP) so this test runs in <100 ms.

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn batchdeleter_pump_drains_via_close() {
        // Pre-populate 2500 paths on a fresh memory op.
        let op = Arc::new(Operator::new(opendal::services::Memory::default()).unwrap());
        for i in 0..2500 {
            op.write(&format!("file_{i:06}"), "x".to_string())
                .await
                .unwrap();
        }

        // Spawn the pump with 1 worker for determinism.
        let cfg = WorkerConfig {
            op: op.clone(),
            worker_count: 1,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_retries: DEFAULT_MAX_RETRIES,
            retry_factor: DEFAULT_RETRY_FACTOR,
            retry_initial_backoff: DEFAULT_RETRY_INITIAL_BACKOFF,
        };
        let (deleter, _h) = spawn(cfg, Arc::new(dashmap::DashSet::new())).unwrap();

        // Enqueue 2500 deletes.
        let mut rxs = Vec::with_capacity(2500);
        for i in 0..2500 {
            let rx = deleter.enqueue(format!("file_{i:06}")).expect("enqueue");
            rxs.push(rx);
        }

        // Flush barrier.
        let flushed = deleter.flush().await.unwrap();
        assert!(
            flushed > 0,
            "flush should report at least one batch drained (got {flushed})"
        );

        // All paths should be deleted from the memory backend.
        let remaining: Vec<_> = op.list("").await.unwrap().into_iter().collect();
        assert_eq!(
            remaining.len(),
            0,
            "all 2500 paths should be deleted after flush, got {} remaining",
            remaining.len()
        );

        // All oneshots should have been acked Ok.
        for (i, rx) in rxs.into_iter().enumerate() {
            match rx.await {
                Ok(Ok(())) => {}
                other => panic!("oneshot {i} returned {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn enqueue_returns_none_after_shutdown() {
        let cfg = WorkerConfig::from_operator(memory_op());
        let (deleter, _h) = spawn(cfg, Arc::new(dashmap::DashSet::new())).unwrap();
        let clone = deleter.clone();
        deleter.shutdown(false).await;
        let r = clone.enqueue("a".into());
        assert!(r.is_none(), "post-shutdown enqueue must reject");
    }

    #[tokio::test]
    async fn spawn_then_drop_exits_cleanly() {
        let cfg = WorkerConfig::from_operator(memory_op());
        let (deleter, _h) = spawn(cfg, Arc::new(dashmap::DashSet::new())).unwrap();
        // Drop the last clone. The Shared Arc drops, but the pump
        // tasks still hold their own Arc<Shared> clones; they idle
        // on `notify.notified().await` until the runtime drops
        // (which doesn't happen here since we're inside the
        // current tokio runtime).
        drop(deleter);
        // Just confirm we can spawn again without panicking on
        // poisoned state.
        let cfg2 = WorkerConfig::from_operator(memory_op());
        let (_d2, _h2) = spawn(cfg2, Arc::new(dashmap::DashSet::new())).unwrap();
    }
}
