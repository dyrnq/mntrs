//! Plan #64: Batched S3 DeleteObjects backend.
//!
//! ## Why this exists
//!
//! opendal 0.58.1 has `BatchDeleter<S3Deleter>` (max_batch_size = 1000)
//! wired into its `delete_with().recursive(true)` path, but the public
//! `Operator::delete()` facade creates a fresh deleter per call
//! (`opendal-core-0.58.1/src/types/operator/operator.rs:1635`). The
//! batcher therefore never accumulates across FUSE callbacks.
//!
//! We bypass opendal's delete path entirely and call S3
//! `DeleteObjects` directly with a long-lived worker. Probe H
//! measured ~5× speedup (574 ms → 115 ms for 500 unlinks on local
//! MinIO; boto3 matches the 115 ms ceiling, confirming MinIO is not
//! the bottleneck).
//!
//! ## Policy B: write-behind
//!
//! Callbacks return `Ok(())` immediately after enqueue. The user's
//! `rm` receives success before S3 confirms deletion. Per-key
//! failures are logged, not surfaced. Default off; opt-in via
//! `MNTRS_UNLINK_BATCH=1`.
//!
//! ## Shutdown
//!
//! Mirrors `writeback::spawn`: dropping the last `ConcurrentDeleter`
//! handle closes the control channel and the worker exits on
//! `rx.recv().await → None`. In-flight pending deletes are completed
//! with `io::ErrorKind::BrokenPipe` (same pattern writeback uses on
//! abrupt sender close).
//!
//! ## References
//!
//! - S3 DeleteObjects spec (AWS S3 API reference)
//! - opendal-service-s3/src/core.rs:1271-1312 (reference impl we mirror)
//! - reqsign-aws-v4 3.0.3 `RequestSigner::sign`
//! - plan: /home/devops/.claude/plans/virtual-noodling-salamander.md

// Public API surface ahead of wire-up: step 5 lands the module; steps
// 6-9 integrate it into MntrsFs. Until then the FUSE-side callers
// don't exist, so most symbols are unused. The `#[allow(dead_code)]`
// is intentional and scoped to this module.
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use reqsign_aws_v4::{
    Credential as AwsCredential, DefaultCredentialProvider, EnvCredentialProvider,
    ProfileCredentialProvider, RequestSigner, StaticCredentialProvider,
};
use reqsign_core::{Context, ProvideCredentialChain, Signer};
use reqsign_file_read_tokio::TokioFileRead;
use tokio::sync::{mpsc, oneshot};

// Note: `Arc<opendal::Operator>` is stashed on `WorkerConfig`
// (Step 1 of plan #64 stage 7). Step 2 will start consuming it
// (`op.deleter().await` per slot) — opendal owns the SigV4 signer
// and reqwest connection pool via the inner `Arc<S3Core>`. The
// field is initialised in `from_s3` from a stub memory operator
// (unused until Step 2 rewrites the consumer side).

// ===== Constants =====

// Plan #64 stage B: retry knobs (kept). Per-mount tuning knobs
// (`MNTRS_BATCH_SIZE`, `MNTRS_BATCH_FLUSH_DELAY_MS`,
// `MNTRS_BATCH_THRESHOLD`, `MNTRS_BATCH_FAST_FLUSH_THRESHOLD`,
// `MNTRS_BATCH_PROFILE`) were dropped along with the Profile /
// Calibrator / BurstObserver subsystem in issue #568 stage 6.
// Issue #570 follow-up: the DeleteObjects XML batch path
// (and its `DEFAULT_BATCH_SIZE` / `DEFAULT_FLUSH_DELAY` /
// `HARD_MAX_KEYS_PER_REQUEST` constants) was also removed.
// Plan #64 stage 7 (Step 1): replaced N concurrent single-DELETE
// with opendal `BatchDeleter<S3Deleter>::buffer` (see §2 of the
// plan). Stage 6/7's retry knobs now wrap `Deleter::close()`
// (the BatchDeleter flush call) instead of the old
// `send_chunk_with_retry`.
pub(crate) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const DEFAULT_MAX_RETRIES: u32 = 3;
pub(crate) const DEFAULT_RETRY_FACTOR: f64 = 2.0;
pub(crate) const DEFAULT_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Issue #562 stage 1 (kept): default deleter pool size. Aligned
/// with rclone `--checkers=8` in issue #568 stage 6 — the N
/// concurrent single-DELETE workers replace the previous flusher
/// pool that drove `DeleteObjects` round-trips. Default 8 wins on
/// every workload size measured in PR #567 nightly.
pub(crate) const DEFAULT_DELETE_WORKER_COUNT: usize = 8;

/// Issue #562 stage 1 (kept): hard upper bound on the worker
/// pool. Each worker holds its own `Signer<AwsCredential>` (cheap
/// clone of region+chain) and shares the `reqwest::Client`
/// connection pool through `Arc` inside `WorkerConfig::http`, so
/// memory pressure is bounded by the channel buffers and the
/// connection pool, not by the signer. Even so, 16 is a generous
/// cap — a misconfigured `MNTRS_DELETE_WORKER_COUNT=10000` would
/// otherwise spawn 10000 clones of the credential chain.
pub(crate) const MAX_DELETE_WORKER_COUNT: usize = 16;

// ===== Counters (plan #64 stage B) =====
//
// Process-static counters, like writeback::PENDING_COUNT. Read
// from any thread via the public accessor functions below.

static FLUSHES_TOTAL: AtomicU64 = AtomicU64::new(0);
static KEYS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHUTDOWN_LOST_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Issue #562 stage 5 (was Calibrator input): per-flush
/// retry count. Bumped every time `send_chunk_with_retry`
/// retries a request (either a retryable HTTP status or a
/// transport error). Atomic so the flusher loop can
/// increment without lock contention.
static RETRY_TOTAL: AtomicU64 = AtomicU64::new(0);

// ===== Profile / BurstObserver / ProfileState / ThresholdCalibrator =====
//
// Stage 6 (issue #568): the Profile / BurstObserver / ProfileState /
// ThresholdCalibrator subsystem from issue #562 stages 3 + 5 has been
// removed. The 5000/10000-file bulk probe (PR #567 nightly) showed
// 99.3% of flushes were batch_size=1 fast flushes — the Profile
// system never reached `Profile::Bulk` in practice. rclone's design
// (N=8 concurrent single-DELETE Checkers, no batching) wins on
// every size we've measured. The original subsystem code is
// preserved in git history (commits 78959fa, b16f081, b8b4ea6).
//
// The block below has been replaced by the comment header above.
// See `Shared` (now slim), `WorkerConfig` (slim — `batch_size` /
// `flush_delay` / `fast_flush_threshold` are still read from env
// for now; Step 2 will move to MNTRS_DELETE_WORKER_COUNT only),
// `spawn` (no calibrator task), and `controller_loop` (now passes
// usize thresholds directly into `decide_next_action` instead of a
// Profile). The batch-XML helpers (`send_chunk_with_retry` etc.)
// remain in the file because the opt-in `MNTRS_DELETE_BATCH=1`
// path uses them — see Step 6 for the final wiring.
// ===== (end of removed subsystem block) =====
#[derive(Default)]
pub(crate) struct CounterSnapshot {
    pub flushes_total: u64,
    pub keys_total: u64,
    pub failures_total: u64,
    pub shutdown_lost_total: u64,
    /// Issue #562 stage 5 (was Calibrator input): total retry
    /// decisions across both the multi-key XML path and the
    /// single-key DELETE path. Kept on the snapshot for the
    /// bench harness and the future /metrics endpoint.
    pub retry_total: u64,
}

pub(crate) fn snapshot() -> CounterSnapshot {
    CounterSnapshot {
        flushes_total: FLUSHES_TOTAL.load(Ordering::Relaxed),
        keys_total: KEYS_TOTAL.load(Ordering::Relaxed),
        failures_total: FAILURES_TOTAL.load(Ordering::Relaxed),
        shutdown_lost_total: SHUTDOWN_LOST_TOTAL.load(Ordering::Relaxed),
        retry_total: RETRY_TOTAL.load(Ordering::Relaxed),
    }
}

/// Read an env var with a default. Used for stage B tuning
/// knobs (`MNTRS_BATCH_FLUSH_DELAY_MS`, `MNTRS_BATCH_SIZE`).
fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(default)
}

// ===== Pending types =====

/// One queued delete. The result sender is consumed by the worker
/// after the S3 response arrives; on shutdown it is dropped (which
/// closes the receiver and signals `RecvError` to anyone still
/// holding the oneshot).
pub(crate) struct PendingDelete {
    pub relative_path: String,
    pub result_tx: oneshot::Sender<std::io::Result<()>>,
}

struct Pending {
    jobs: std::collections::VecDeque<PendingDelete>,
}

impl Pending {
    fn new() -> Self {
        Self {
            jobs: std::collections::VecDeque::with_capacity(16),
        }
    }
    fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }
    fn len(&self) -> usize {
        self.jobs.len()
    }
    /// Pop one job from the front of the queue (FIFO). Used by the
    /// deleter loop. **Stage 6 (issue #568):** O(1) via VecDeque
    /// — the old `flush_one_batch` did `Vec::remove(0)` which was
    /// O(n) per drain slice; with N concurrent deleters each
    /// popping one job at a time, the per-call cost is now
    /// visible on hot paths.
    fn pop_one(&mut self) -> Option<PendingDelete> {
        self.jobs.pop_front()
    }
    fn push(&mut self, job: PendingDelete) {
        self.jobs.push_back(job);
    }
    /// Drain the entire queue. Used by the drain worker for the
    /// `Flush` / `Shutdown { drain: true }` commands (Policy B
    /// rmdir barrier).
    fn drain_all(&mut self) -> Vec<PendingDelete> {
        let drained: Vec<PendingDelete> = self.jobs.drain(..).collect();
        drained
    }
    fn clear(&mut self) {
        self.jobs.clear();
    }
}

// ===== Shared state =====

struct Shared {
    pending: Mutex<Pending>,
    accepting: AtomicBool,
    /// **Stage 6 (issue #568):** edge-triggered wake signal for the
    /// N deleter loops. `enqueue()` calls `notify_one()` after
    /// pushing the job; each `deleter_loop()` calls
    /// `notify.notified().await` to wait for work. Lost
    /// notifications are safe — the woken deleter drains
    /// everything in `pending` before sleeping again, so any job
    /// pushed while no deleter was waiting gets picked up the next
    /// time a deleter wakes (including a notification stored for
    /// the next `notified()` call). `flush_tx`-side `Control::Wake`
    /// commands call `notify_waiters()` to fan out to all
    /// currently-sleeping deleters at once.
    notify: tokio::sync::Notify,
    /// Tombstones for paths that have an in-flight write-behind
    /// delete. Owned by MntrsFs (test + production constructors
    /// both pre-build a DashSet and pass its Arc clone here) so
    /// lookup/getattr/readdir on the FUSE side and worker success
    /// paths on the deleter side see the same set. The worker
    /// clears an entry whenever it acks a per-key result (success,
    /// NotFound idempotent, or permanent failure with an error
    /// log).
    ///
    /// Plan #64 step 10 originally kept tombstones insert-only
    /// with a `TODO` for result-driven cleanup (see lib.rs:5891
    /// history). Stage C default-ON made that TODO a correctness
    /// bug: rm-then-create same path returned ENOENT because the
    /// tombstone outlived the S3 delete. Real fix landed as part
    /// of stage C: worker removes on every per-key ack, and
    /// `cancel_pending` clears tombstone + drops the queued job
    /// without sending the S3 DELETE (called from create() before
    /// op.write).
    tombs: std::sync::Arc<dashmap::DashSet<String>>,
}

// ===== Public handle =====

/// Cheap-clone handle for FUSE callbacks to enqueue deletes. The
/// worker task is owned by the `JoinHandle` returned from `spawn`.
/// Dropping all `ConcurrentDeleter` handles (including the one stored
/// in `MntrsFs`) closes the control channel and the worker exits on
/// `rx.recv().await → None`.
#[derive(Clone)]
pub(crate) struct ConcurrentDeleter {
    shared: Arc<Shared>,
    flush_tx: mpsc::Sender<Control>,
}

// ===== Worker config =====

/// Construction-time config. Built by `cmd/mount.rs::build_s3` after
/// parsing the storage URL and CLI options; passed to
/// `concurrent_delete::spawn`.
#[derive(Clone)]
pub(crate) struct WorkerConfig {
    pub endpoint: url::Url,
    pub bucket: String,
    /// Operator root — prepended to every relative path before
    /// signing. Sourced from `operator.info().root()` so the
    /// batcher deletes exactly what opendal would have.
    pub prefix: String,
    pub region: String,
    /// Explicit static credentials when `--opt access-key=...` +
    /// `--opt secret-key=...` are supplied; otherwise `None` and we
    /// fall back to the env/profile/IMDS chain.
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    /// Shared reqwest client — same TLS settings + connection pool
    /// as opendal uses (see `cmd/mount.rs::build_mount_http_client`).
    pub http: reqwest::Client,
    /// **Plan #64 stage 7 (Step 1, additive):** opendal operator.
    /// The future deleter pump creates one `op.deleter().await`
    /// per slot; the inner `Arc<S3Core>` (and thus the reqwest
    /// connection pool) is shared across slots. **Unused in this
    /// commit** — Step 2 swaps the per-DELETE signer/reqwest path
    /// for `Deleter::delete` + `Deleter::close`, at which point
    /// this becomes the only state the workers need (the six
    /// legacy fields above are dropped).
    pub op: Arc<opendal::Operator>,
    /// Issue #562 stage 1 (renamed in issue #568 stage 6):
    /// number of concurrent deleter loops that share
    /// `Shared::pending`. Each deleter pops one job at a time
    /// and fires `send_single_delete_with_retry` — rclone-style
    /// N concurrent single-DELETE Checkers. Default 8 matches
    /// rclone `--checkers=8`. Sourced from env
    /// `MNTRS_DELETE_WORKER_COUNT` (default 8, clamp 1..=16).
    /// Value 1 reproduces the pre-#562 single-consumer
    /// behaviour.
    pub worker_count: usize,
    pub request_timeout: Duration,
    pub max_retries: u32,
    pub retry_factor: f64,
    pub retry_initial_backoff: Duration,
}

impl WorkerConfig {
    /// Build the production config from S3 mount-time inputs.
    /// `prefix` should be the opendal root (e.g. `/some/dir/`).
    /// The `op` field is initialised from a separately-constructed
    /// opendal Operator (the same one `cmd/mount.rs::build_s3`
    /// returns as `built.operator`); Step 2 collapses this into a
    /// single `from_operator(op)` call that drops the legacy
    /// fields entirely.
    pub(crate) fn from_s3(
        endpoint: url::Url,
        bucket: String,
        prefix: String,
        region: String,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        http: reqwest::Client,
    ) -> Self {
        // Issue #570: the entire batch XML path was dropped.
        // `MNTRS_DELETE_WORKER_COUNT` is the only remaining
        // tuning knob. The previous batch tuners (`MNTRS_BATCH_SIZE`,
        // `MNTRS_BATCH_FLUSH_DELAY_MS`, `MNTRS_BATCH_THRESHOLD`,
        // `MNTRS_BATCH_FAST_FLUSH_THRESHOLD`, `MNTRS_BATCH_PROFILE`)
        // and the Profile / Calibrator / BurstObserver subsystem
        // were all removed along with `send_chunk_with_retry` and
        // the `DeleteObjects` XML body/parser — single path is N
        // concurrent single-DELETE. There is no opt-in knob; if a
        // workload ever needs batch XML again, resurrect the helpers
        // from git history.
        let worker_count = env_usize("MNTRS_DELETE_WORKER_COUNT", DEFAULT_DELETE_WORKER_COUNT)
            .clamp(1, MAX_DELETE_WORKER_COUNT);
        Self {
            endpoint,
            bucket,
            prefix,
            region,
            access_key_id,
            secret_access_key,
            http,
            // Step 1 stub: the live opendal Operator isn't threaded
            // through `from_s3` yet, so we default to an unrooted
            // memory operator that nothing in this commit touches.
            // Step 2 replaces the `from_s3` call site with
            // `from_operator(Arc::new(built.operator.clone()))` and
            // this stub disappears.
            op: Arc::new(opendal::Operator::new(
                opendal::services::Memory::default(),
            ).expect("concurrent_delete::WorkerConfig::from_s3: stub memory operator")),
            worker_count,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_retries: DEFAULT_MAX_RETRIES,
            retry_factor: DEFAULT_RETRY_FACTOR,
            retry_initial_backoff: DEFAULT_RETRY_INITIAL_BACKOFF,
        }
    }
}

// ===== Control commands =====

enum Control {
    /// Wake the worker to re-evaluate the queue (used on first
    /// enqueue to break the worker out of `rx.recv()` so the
    /// empty-buffer fast path can fire without waiting for the
    /// timer).
    Wake,
    /// Force a flush of all pending keys; signal `done` with the
    /// number of keys flushed (or `Err` on transport-level failure).
    /// Used by `ConcurrentDeleter::flush()` (Policy B rmdir barrier).
    Flush {
        done: oneshot::Sender<std::io::Result<usize>>,
    },
    /// Graceful shutdown. `drain=true` flushes pending before exit;
    /// `drain=false` fails all pending oneshots with `BrokenPipe`
    /// and exits. Worker signals `done` when it has stopped
    /// accepting new work.
    Shutdown {
        drain: bool,
        done: oneshot::Sender<()>,
    },
}

// ===== Spawn =====

/// Handle bundle returned by `spawn`. The drain worker owns
/// the `mpsc::Receiver<Control>` and dispatches
/// `Control::Wake` / `Control::Flush` / `Control::Shutdown`;
/// the deleters are the worker-count pool that drains
/// `Shared::pending` and calls `send_single_delete_with_retry`.
///
/// Callers can drop this struct to abandon the workers
/// (they will exit when `flush_tx` and `wake_tx` go out of
/// scope); or await the drain handle first and then
/// the deleter handles for a clean shutdown. MntrsFs
/// currently takes the drop path (fire-and-forget): all
/// `ConcurrentDeleter` clones drop → `flush_tx` drops →
/// controller's `rx.recv()` returns `None` → controller
/// exits → `wake_tx` drops → flushers' `wake_rx.recv()`
/// returns `Err(Sender)` → flushers exit.
#[allow(dead_code)] // the field set is wiring + shutdown paths
pub(crate) struct WorkerHandles {
    /// Drain worker — owns the `mpsc::Receiver<Control>`,
    /// executes `Control::Wake` / `Control::Flush` /
    /// `Control::Shutdown` commands. Drives the rmdir barrier
    /// (`Control::Flush`) and the shutdown drain semantics.
    /// **Stage 6 (issue #568):** the old `controller` field
    /// was renamed to `drain` because the controller's only
    /// remaining job is the `Control::Receiver<Control>`-side
    /// command dispatch; the batching / accumulator / profile
    /// logic moved out (now per-deleter responsibility, no
    /// global accumulator).
    pub(crate) drain: tokio::task::JoinHandle<()>,
    /// N concurrent single-DELETE workers (default 8). Each
    /// pops one key at a time from `Shared::pending`, fires
    /// `send_single_delete_with_retry`, and acks the per-key
    /// oneshot. **Stage 6 (issue #568):** the old `flushers`
    /// field was renamed to `deleters` because the worker no
    /// longer drains a flush batch — every iteration is one
    /// single DELETE.
    pub(crate) deleters: Vec<tokio::task::JoinHandle<()>>,
}

pub(crate) fn spawn(
    config: WorkerConfig,
    tombs: std::sync::Arc<dashmap::DashSet<String>>,
) -> std::io::Result<(ConcurrentDeleter, WorkerHandles)> {
    let (tx, rx) = mpsc::channel::<Control>(64);
    // **Stage 6 (issue #568):** Shared now also carries the
    // `tokio::sync::Notify` used to wake N concurrent deleter
    // loops. Each `enqueue()` calls `notify_one()`; each
    // `deleter_loop()` calls `notified().await`. See the
    // `Shared` doc-comment for the edge-trigger semantics.
    let shared = Arc::new(Shared {
        pending: Mutex::new(Pending::new()),
        accepting: AtomicBool::new(true),
        notify: tokio::sync::Notify::new(),
        tombs,
    });
    let deleter = ConcurrentDeleter {
        shared: shared.clone(),
        flush_tx: tx,
    };
    // Plan #64 stage A bug fix: must use crate::rt().spawn,
    // not bare tokio::spawn. The mount's main thread is not
    // inside a tokio runtime context — bare tokio::spawn
    // panics with "there is no reactor running" before the
    // worker even starts. writeback::spawn uses the same
    // pattern (writeback.rs:174).
    //
    // **Stage 6 (issue #568):** spawn `worker_count` deleter
    // loops (default 8, rclone-style N concurrent single-DELETE)
    // plus 1 drain worker that owns the `mpsc::Receiver<Control>`
    // and dispatches `Control::Wake` (notify all waiters),
    // `Control::Flush` (sync drain, ack oneshot — rmdir barrier),
    // and `Control::Shutdown { drain, done }` (drain or fail).
    // The controller_loop + flusher_loop pair from issue #562
    // stage 1 was retired: there is no global accumulator
    // anymore — every push goes straight to a deleter via the
    // `notify` primitive.
    //
    // **io::sync migration (2026-08-28):** N concurrent single-DELETE
    // workers DO need N OS threads to actually parallelize. The
    // original code spawned all N onto `crate::rt()` which has
    // `worker_threads(1)` (Issue #30 design) — so the 8 workers
    // serialized on a single OS thread, explaining the 10s wall-clock
    // for rm-rf 10000. The `io::sync` runtime has 8 workers and is
    // physically separate from `rt()` (which remains pinned at 1
    // worker for the FUSE metadata-op hot path). All deleter + drain
    // tasks now spawn on `io_sync.handle()`. If io::sync was not
    // initialized (test / legacy), fall back to `rt()` so the
    // existing behavior is preserved — the rm-rf 10000 regression
    // will still be present in that mode, but no worse than today.
    let worker_count = config.worker_count.max(1);
    let spawn_handle = crate::io::sync::IoSync::get()
        .map(|s| s.handle())
        .unwrap_or_else(|| crate::rt().handle().clone());
    let mut deleter_handles = Vec::with_capacity(worker_count);
    for deleter_id in 0..worker_count {
        let cfg = config.clone();
        let sh = shared.clone();
        deleter_handles.push(spawn_handle.spawn(deleter_loop(deleter_id, cfg, sh)));
    }
    let drain_handle = spawn_handle.spawn(drain_worker_loop(config, shared.clone(), rx));
    Ok((
        deleter,
        WorkerHandles {
            drain: drain_handle,
            deleters: deleter_handles,
        },
    ))
}

// ===== ConcurrentDeleter API =====

impl ConcurrentDeleter {
    /// Enqueue a relative path for deletion. Returns `None` if the
    /// worker is shutting down (handle will not accept new work);
    /// returns `Some(oneshot::Receiver)` otherwise. The caller
    /// (MntrsFs write-behind path) drops the receiver — the per-key
    /// S3 outcome is observed via the worker's tracing logs, not
    /// via the receiver. The receiver exists for tests and for any
    /// strict caller.
    ///
    /// Issue #530: caller-side threshold gating. If
    /// `pending.len() < batch_threshold` at the moment of
    /// enqueue, this method returns `None` so the caller
    /// falls back to a strict `delete_backend_strict`. Reason:
    /// the per-key latency floor is `flush_delay` (50 ms by
    /// default), and a one-off unlink or a 10-file `rm -rf`
    /// pays that floor without amortising it across enough
    /// siblings to beat the direct `op.delete()` path.
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
        let mut pending = self.shared.pending.lock().expect("pending mutex poisoned");
        let was_empty = pending.is_empty();
        pending.push(job);
        drop(pending);
        if was_empty {
            // **Stage 6 (issue #568):** notify one sleeping
            // deleter directly. We still send the legacy
            // `Control::Wake` to the drain worker (cheap; it
            // just calls `notify_waiters()`) for callers that
            // race the first-push fast path.
            self.shared.notify.notify_one();
            let _ = self.flush_tx.try_send(Control::Wake);
        }
        Some(rx)
    }

    /// Cancel any pending delete for `relative_path`. Used by
    /// `MntrsFs::create()` (and `mkdir()`) before the new
    /// op.write fires, so a create-after-rm doesn't hit two
    /// problems at once:
    ///
    ///   1. lookup/getattr/readdir still see the tombstone (the
    ///      write-behind S3 delete hasn't landed yet, so the
    ///      path may still exist on the backend).
    ///   2. without cancel, the in-flight delete would race the
    ///      new write and either delete the freshly created
    ///      object (data loss) or, idempotent-NotFound the new
    ///      object (false "tombstone leaked" — worse, kernel
    ///      would surface ENOENT to the user).
    ///
    /// Sync (no need to await the worker): under the pending
    /// mutex we drain matching jobs and complete their oneshots
    /// with `Err(ErrorKind::Interrupted)` so any strict-mode
    /// caller sees a cancel rather than a phantom success. The
    /// tombstone entry is also removed here — by the time this
    /// returns, the next lookup will not see the deleted path
    /// as gone.
    ///
    /// Returns the number of pending jobs cancelled (0 if no
    /// delete was queued for this path).
    pub(crate) fn cancel_pending(&self, relative_path: &str) -> usize {
        let mut cancelled = 0usize;
        {
            let mut pending = self.shared.pending.lock().expect("pending mutex poisoned");
            let mut kept: std::collections::VecDeque<PendingDelete> =
                std::collections::VecDeque::with_capacity(pending.jobs.len());
            for job in pending.jobs.drain(..) {
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
            pending.jobs = kept;
            // **Stage 6 (issue #568):** no deadline tracking in
            // the post-Profile design — the per-key deleter loop
            // has no flush-coalescing window.
        }
        // Tombstone clear is unconditional: even if no job was
        // pending, the FUSE-side lookup will hit it if the user
        // did `rm file; sleep; touch file`. Clearing on every
        // `create()` call is the safest invariant.
        self.shared.tombs.remove(relative_path);
        cancelled
    }

    /// Read-only access to the tombstone set for FUSE-side
    /// filters (lookup, getattr, readdir). Cheap clone of the
    /// Arc — no copy of the underlying DashSet.
    pub(crate) fn tombstones(&self) -> std::sync::Arc<dashmap::DashSet<String>> {
        self.shared.tombs.clone()
    }

    /// Force-flush all currently pending keys. Used by the rmdir
    /// barrier under Policy B so `rm -rf dir` doesn't return before
    /// the dir's deletes have actually been requested. Returns the
    /// total number of keys flushed (across one or more chunks);
    /// `Err` on transport-level failure that exhausted retries.
    pub(crate) async fn flush(&self) -> std::io::Result<usize> {
        let (done_tx, done_rx) = oneshot::channel();
        if self
            .flush_tx
            .send(Control::Flush { done: done_tx })
            .await
            .is_err()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "concurrent_deleter: worker channel closed",
            ));
        }
        done_rx.await.map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "concurrent_deleter: worker dropped flush response",
            )
        })?
    }

    /// Graceful shutdown. `drain=true` flushes pending keys (best
    /// effort — failures are logged, not propagated); `drain=false`
    /// fails all pending oneshots immediately. Used by
    /// `MntrsFs::shutdown` and the Drop fallback.
    pub(crate) async fn shutdown(self, drain: bool) {
        self.shared.accepting.store(false, Ordering::Release);
        let (done_tx, done_rx) = oneshot::channel();
        if self
            .flush_tx
            .send(Control::Shutdown {
                drain,
                done: done_tx,
            })
            .await
            .is_ok()
        {
            let _ = done_rx.await;
        }
        // self dropped here → flush_tx (one of possibly several
        // clones) drops → when the LAST clone drops, the worker's
        // rx.recv().await returns None and the worker exits.
    }

    /// True if the worker is still accepting work.
    pub(crate) fn is_accepting(&self) -> bool {
        self.shared.accepting.load(Ordering::Acquire)
    }

    /// Issue #530: snapshot of the pending queue length. Used
    /// by `MntrsFs::enqueue_backend_delete` to decide whether
    /// the next unlink should go through the batched path or
    /// fall through to a direct `delete_backend_strict`. Cheap
    /// (mutex lock + len read); the cost is dominated by
    /// whatever's already holding the pending mutex.
    pub(crate) fn pending_len(&self) -> usize {
        let pending = self.shared.pending.lock().expect("pending mutex poisoned");
        pending.len()
    }

    // **Stage 6 (issue #568):** `set_batch_threshold` and
    // `batch_threshold()` were removed along with the
    // caller-side threshold gating in `enqueue()`. The
    // `MNTRS_BATCH_THRESHOLD` env knob is gone in Step 2; this
    // commit only deletes the runtime methods.
}

// ===== Worker loop =====
//
// Issue #562 stage 1: split the single consumer loop into
// one controller + N flushers. The controller owns the
// `mpsc::Receiver<Control>` (so `Control::Flush` /
// `Control::Shutdown` ack semantics stay in one place) and
// runs the pure `decide_next_action` helper. Flushers
// subscribe to a `tokio::sync::broadcast` for wakeups and
// drain `Shared::pending` in parallel — their S3
// round-trips overlap. The `Mutex<Pending>` inside
// `flush_one_batch` serialises the drain slice, but the
// S3 call is outside the lock.
// **Stage 6 (issue #568):** the previous comment described
// the issue #562 stage 1 controller + flusher design. Both
// tasks have been retired; see `drain_worker_loop` and
// `deleter_loop` below for the rclone-style N-worker
// concurrent single-DELETE design.

/// Drain worker task. Owns the `mpsc::Receiver<Control>`,
/// the S3 signer, and shutdown accounting. Dispatch loop:
///
/// * `Control::Wake` — fan-out wake to all currently
///   sleeping deleters (`Shared::notify.notify_waiters()`).
///   Used by the caller-side `ConcurrentDeleter::enqueue` first-
///   push path to break any deleter out of
///   `notify.notified().await`. (After the rewrite, deleters
///   also call `notify_one()` themselves on every push, so the
///   `Wake` path is mostly a defensive no-op for callers that
///   race the threshold gate — kept for API compatibility.)
/// * `Control::Flush { done }` — sync drain of all pending
///   keys, ack the oneshot with the number flushed. Used by
///   `ConcurrentDeleter::flush()` (Policy B rmdir barrier):
///   the caller blocks until the drain completes so a
///   subsequent `op.readdir` sees the directory as empty.
/// * `Control::Shutdown { drain, done }` — graceful shutdown.
///   `drain=true` flushes pending before exit (typical);
///   `drain=false` fails all pending oneshots with
///   `BrokenPipe` and counts them under `SHUTDOWN_LOST_TOTAL`.
///   The `done` oneshot fires once the drain / fail is
///   complete.
///
/// **Stage 6 (issue #568):** the previous `controller_loop`
/// also drove the `decide_next_action` accumulator and the
/// `Mutex<Pending>` deadline tracking. Both responsibilities
/// moved into per-deleter territory (each deleter pops one
/// job at a time and runs it through the S3 DELETE pipeline
/// inline) so the drain worker is purely a command dispatcher
/// now. The S3 signer is held only for the lifetime of an
/// in-flight `Control::Flush` (not for the steady state).
async fn drain_worker_loop(
    config: WorkerConfig,
    shared: Arc<Shared>,
    mut rx: mpsc::Receiver<Control>,
) {
    let signer = match build_signer(&config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(
                target: "mntrs::concurrent_delete",
                bucket = %config.bucket,
                prefix = %config.prefix,
                error = %e,
                "concurrent_delete: drain worker failed to build signer, exiting"
            );
            shared.accepting.store(false, Ordering::Release);
            fail_all_pending(&shared);
            return;
        }
    };

    tracing::info!(
        target: "mntrs::concurrent_delete",
        bucket = %config.bucket,
        prefix = %config.prefix,
        worker_count = config.worker_count,
        credential_source = if config.access_key_id.is_some() { "explicit" } else { "default-chain" },
        "concurrent_delete: drain worker started"
    );

    while let Some(cmd) = rx.recv().await {
        match cmd {
            Control::Wake => {
                // Fan out to all currently sleeping deleters.
                // After the issue #568 rewrite, deleters also
                // `notify_one()` themselves on every push, so
                // this is mostly a no-op — but the caller-side
                // FUSE unlink path still sends `Wake` on the
                // first push of an empty queue (see
                // `ConcurrentDeleter::enqueue`) for the
                // "break the worker out of `rx.recv().await`"
                // legacy semantics. `notify_waiters()` is
                // cheap (one atomic op, no allocation).
                shared.notify.notify_waiters();
            }
            Control::Flush { done } => {
                let flushed = do_flush_all(&config, &signer, &shared).await;
                let _ = done.send(flushed);
            }
            Control::Shutdown { drain, done } => {
                if drain {
                    let _ = do_flush_all(&config, &signer, &shared).await;
                } else {
                    let lost = {
                        let mut p = shared.pending.lock().expect("pending mutex poisoned");
                        let n = p.jobs.len() as u64;
                        p.clear();
                        n
                    };
                    if lost > 0 {
                        SHUTDOWN_LOST_TOTAL.fetch_add(lost, Ordering::Relaxed);
                    }
                    fail_all_pending(&shared);
                }
                let _ = done.send(());
                break;
            }
        }
    }

    // Channel closed without explicit shutdown — same
    // accounting as `drain=false`: lost keys count.
    let lost = {
        let mut p = shared.pending.lock().expect("pending mutex poisoned");
        let n = p.jobs.len() as u64;
        p.clear();
        n
    };
    if lost > 0 {
        SHUTDOWN_LOST_TOTAL.fetch_add(lost, Ordering::Relaxed);
    }
    fail_all_pending(&shared);
    tracing::info!(
        target: "mntrs::concurrent_delete",
        "concurrent_delete: drain worker exiting"
    );
}

/// Deleter worker task. Each instance is a single-DELETE
/// loop:
///   1. wait on `Shared::notify.notified()`,
///   2. pop one job from `Shared::pending` (FIFO),
///   3. fire `send_single_delete_with_retry` on the key,
///   4. ack the per-key oneshot + clear the tombstone,
///   5. loop back to step 1 if more work remains.
///
/// **Why rclone-style N concurrent single-DELETE?** The
/// 5000/10000-file probe in PR #567 nightly showed 99.3%
/// of flushes were batch_size=1 fast flushes — the old
/// `BatchSize=100 + FlushDelay=50ms` accumulator never
/// accumulated beyond 1 key in practice on FUSE workloads,
/// so the per-flush DeleteObjects XML round-trip is pure
/// overhead. rclone's design (`backend/s3/s3.go:5170` +
/// `fs/operations/operations.go:599`) is N Checkers each
/// issuing a single HTTP DELETE in parallel. We mirror that
/// here: N deleters each pull one key at a time. The
/// `Mutex<Pending>` is held only across the `pop_front()`
/// (microseconds); the S3 round-trip is unlocked so all N
/// deleters run concurrently against the shared
/// `reqwest::Client` connection pool inside
/// `WorkerConfig::http`.
///
/// **Stage 6 (issue #568):** this replaces the previous
/// `flusher_loop` (which drained a whole batch via
/// `flush_one_batch` on a broadcast wake) and the previous
/// `controller_loop` (which ran `decide_next_action` and
/// drove the accumulator). Both are gone.
///
/// The deleter loop runs forever — there is no graceful
/// shutdown path for an individual deleter. The drain
/// worker marks `Shared::accepting = false` on shutdown,
/// which causes `ConcurrentDeleter::enqueue` to return `None`;
/// in-flight jobs continue processing until the queue is
/// empty. When the drain worker exits it drops the
/// `Shared::notify` along with everything else, so the
/// deleter loops lose their wake signal and idle. They
/// never exit on their own — the destructor of the
/// `crate::rt()` runtime cleans them up when the mount
/// process exits.
async fn deleter_loop(deleter_id: usize, config: WorkerConfig, shared: Arc<Shared>) {
    // Each deleter owns its own signer. The signer holds a
    // reference to the shared `reqwest::Client` connection
    // pool inside `config.http` (the builder wraps it in an
    // Arc), so all N deleters share the pool — that's the
    // whole point of the rclone-style N concurrent design.
    let signer = match build_signer(&config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(
                target: "mntrs::concurrent_delete",
                deleter_id,
                error = %e,
                "concurrent_delete: deleter failed to build signer, exiting"
            );
            return;
        }
    };

    tracing::info!(
        target: "mntrs::concurrent_delete",
        deleter_id,
        worker_count = config.worker_count,
        "concurrent_delete: deleter started"
    );

    loop {
        // Wait for the next enqueue. `notified()` is
        // edge-triggered: if a `notify_one()` already fired
        // while we were processing, the next call here returns
        // immediately. If no work is pending, we sleep here
        // until the next `notify_one()` / `notify_waiters()`
        // call.
        shared.notify.notified().await;

        // Drain everything currently in the queue. The
        // inner `loop` exits when `pop_one()` returns `None`
        // — at that point either we're caught up or another
        // push came in during the drain and we need to
        // re-arm via the outer `notified().await` (the
        // pending `notify_one()` from `enqueue()` is already
        // buffered).
        loop {
            let job = {
                let mut pending = shared.pending.lock().expect("pending mutex poisoned");
                pending.pop_one()
            };
            let Some(job) = job else {
                break;
            };

            // Send the per-key DELETE. This is the bulk of
            // the deleter's time — the mutex is released
            // before the await, so N deleters can be
            // mid-flight on S3 round-trips concurrently.
            let key = join_key(&config.prefix, &job.relative_path);
            // `send_single_delete_with_retry` returns a `Vec`
            // because it was originally designed to take a
            // chunk; the deleter loop always passes a
            // single key, so the Vec has length 1.
            let results = send_single_delete_with_retry(&config, &signer, &key).await;
            // Unwrap the single-result Vec. Errors here would
            // be programmer bugs (caller passed an empty
            // chunk); treat as a transport error.
            let outcome = results.into_iter().next().unwrap_or_else(|| {
                Err(std::io::Error::other(
                    "concurrent_delete: send_single_delete_with_retry returned empty result",
                ))
            });

            // Tally outcome. Per-key counters keep the
            // legacy semantics: FLUSHES_TOTAL counts flush
            // attempts (= per-key DELETE requests), KEYS_TOTAL
            // counts successful keys, FAILURES_TOTAL counts
            // permanent failures.
            match &outcome {
                Ok(()) => {
                    FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
                    KEYS_TOTAL.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                }
            }

            // Clear the tombstone. Plan #64 stage C: every
            // terminal outcome clears the tombstone so the
            // object becomes visible again (write-behind
            // rm-then-create race fix).
            shared.tombs.remove(&job.relative_path);

            // Ack the per-key oneshot. The receiver lives in
            // the FUSE callback (MntrsFs write-behind path);
            // most callers drop the receiver without observing
            // it (Policy B write-behind), but tests and strict
            // callers use it.
            let _ = job.result_tx.send(outcome);
        }
    }
}

/// Flush all pending keys by serial single-DELETE.
///
/// Issue #570: the old batch XML path is gone, so `do_flush_all`
/// no longer drives a `DeleteObjects` round-trip. The drain worker
/// uses this to satisfy `Control::Flush { done }` (Policy B rmdir
/// barrier) — it drains the queue in serial order on its own
/// signer so the caller can block on `done` and observe a
/// consistent post-state. The N concurrent deleter loops continue
/// to issue single-DELETEs in parallel; we hold no shared mutex
/// here other than the brief `drain_all` snapshot.
///
/// Returns the number of keys whose per-key DELETE completed
/// (Ok + idempotent-NotFound + permanent failure all count — the
/// rmdir barrier only needs the S3 round-trip to have landed).
async fn do_flush_all(
    config: &WorkerConfig,
    signer: &Signer<AwsCredential>,
    shared: &Arc<Shared>,
) -> std::io::Result<usize> {
    let batch: Vec<PendingDelete> = {
        let mut pending = shared.pending.lock().expect("pending mutex poisoned");
        pending.drain_all()
    };
    if batch.is_empty() {
        return Ok(0);
    }
    let mut total = 0usize;
    for job in batch {
        let key = join_key(&config.prefix, &job.relative_path);
        let results = send_single_delete_with_retry(config, signer, &key).await;
        let outcome = results.into_iter().next().unwrap_or_else(|| {
            Err(std::io::Error::other(
                "concurrent_delete: send_single_delete_with_retry returned empty result",
            ))
        });
        // Same accounting as the deleter loop: per-key counters,
        // per-key tombstone cleanup, per-key oneshot ack.
        match &outcome {
            Ok(()) => {
                FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
                KEYS_TOTAL.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
            }
        }
        shared.tombs.remove(&job.relative_path);
        let _ = job.result_tx.send(outcome);
        total += 1;
    }
    Ok(total)
}

fn fail_all_pending(shared: &Shared) {
    let jobs = {
        let mut pending = shared.pending.lock().expect("pending mutex poisoned");
        // **Stage 6 (issue #568):** no deadline to clear in
        // the post-Profile design — the per-key deleter loop
        // has no flush-coalescing window.
        std::mem::take(&mut pending.jobs)
    };
    let count = jobs.len() as u64;
    if count > 0 {
        SHUTDOWN_LOST_TOTAL.fetch_add(count, Ordering::Relaxed);
        // Plan #64 stage C: tombstones for shutdown-lost keys must
        // be cleared. Without this, the FUSE side's lookup would
        // keep masking paths whose S3 delete never went out, and a
        // subsequent `touch <path>` would return ENOENT until the
        // mount restarts.
        for job in &jobs {
            shared.tombs.remove(&job.relative_path);
        }
        tracing::warn!(
            target: "mntrs::concurrent_delete",
            lost = count,
            "concurrent_delete: shutdown dropped pending deletes"
        );
    }
    for job in jobs {
        let _ = job.result_tx.send(Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "concurrent_deleter: shutdown lost",
        )));
    }
}

// ===== Signer build =====

/// Build a `reqsign::Signer` for SigV4. If explicit credentials are
/// supplied, prepend a `StaticCredentialProvider` to the chain; the
/// chain itself falls through to env/profile/default-chain.
fn build_signer(config: &WorkerConfig) -> std::io::Result<Signer<AwsCredential>> {
    let ctx = Context::new().with_file_read(TokioFileRead);
    let builder = RequestSigner::new("s3", &config.region);

    let mut chain: ProvideCredentialChain<AwsCredential> = ProvideCredentialChain::new();
    if let (Some(ak), Some(sk)) = (&config.access_key_id, &config.secret_access_key) {
        chain = chain.push(StaticCredentialProvider::new(ak, sk));
    }
    chain = chain
        .push(EnvCredentialProvider::new())
        .push(ProfileCredentialProvider::new())
        .push(DefaultCredentialProvider::new());

    Ok(Signer::new(ctx, chain, builder))
}

// ===== S3 protocol =====

fn is_retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

fn next_backoff(current: Duration, factor: f64) -> Duration {
    Duration::from_secs_f64(current.as_secs_f64() * factor)
}

/// Issue #562 stage 1.5: retry wrapper around the
/// single-key short-circuit. Mirrors `send_chunk_with_retry`
/// semantics (3-attempt retry on retryable status / transport
/// error with exponential backoff) but for a plain
/// `DELETE /bucket/key`. Returns `vec![Ok(())]` on 204 /
/// 200 / idempotent 404 and `vec![Err(...)]` otherwise.
async fn send_single_delete_with_retry(
    config: &WorkerConfig,
    signer: &Signer<AwsCredential>,
    key: &str,
) -> Vec<std::io::Result<()>> {
    let mut attempt = 0u32;
    let mut backoff = config.retry_initial_backoff;
    loop {
        match send_single_delete_request(config, signer, key).await {
            Ok(status) => match status {
                // 204 No Content is the standard S3 single-object
                // DELETE success; 200 with empty body is also
                // accepted by some S3-compatible backends.
                200 | 204 => return vec![Ok(())],
                // 404 = NoSuchKey. Idempotent: the key is
                // already gone, which is the post-condition
                // the caller wanted. Matches the
                // DeleteObjects response-parser policy where
                // NoSuchKey / NoSuchVersion are mapped to Ok.
                404 => return vec![Ok(())],
                s if is_retryable_status(s) && attempt < config.max_retries => {
                    tracing::warn!(
                        target: "mntrs::concurrent_delete",
                        status = s,
                        attempt = attempt + 1,
                        backoff_ms = backoff.as_millis() as u64,
                        "single-key DELETE: retrying after retryable status"
                    );
                    tokio::time::sleep(backoff).await;
                    // Issue #562 stage 5: feed the Calibrator.
                    // Bumped exactly once per retry decision
                    // (multi-key + single-key paths, status +
                    // transport variants). The Calibrator
                    // computes `retry_rate = RETRY_TOTAL /
                    // FLUSHES_TOTAL` from this counter.
                    RETRY_TOTAL.fetch_add(1, Ordering::Relaxed);
                    backoff = next_backoff(backoff, config.retry_factor);
                    attempt += 1;
                    continue;
                }
                s => {
                    let msg = format!(
                        "concurrent_delete: S3 single-object DELETE HTTP {} for key `{}`",
                        s, key
                    );
                    return vec![Err(std::io::Error::other(msg))];
                }
            },
            Err(e) => {
                if attempt < config.max_retries {
                    tracing::warn!(
                        target: "mntrs::concurrent_delete",
                        error = %e,
                        attempt = attempt + 1,
                        backoff_ms = backoff.as_millis() as u64,
                        "single-key DELETE: retrying after transport error"
                    );
                    tokio::time::sleep(backoff).await;
                    // Issue #562 stage 5: feed the Calibrator.
                    // Bumped exactly once per retry decision
                    // (multi-key + single-key paths, status +
                    // transport variants). The Calibrator
                    // computes `retry_rate = RETRY_TOTAL /
                    // FLUSHES_TOTAL` from this counter.
                    RETRY_TOTAL.fetch_add(1, Ordering::Relaxed);
                    backoff = next_backoff(backoff, config.retry_factor);
                    attempt += 1;
                    continue;
                }
                let msg = format!(
                    "concurrent_delete: single-object DELETE transport failure after {} retries: {}",
                    config.max_retries, e
                );
                return vec![Err(std::io::Error::other(msg))];
            }
        }
    }
}

/// Issue #562 stage 1.5: send one signed `DELETE /bucket/key`.
/// Returns the HTTP status code only (no body — S3 single-object
/// DELETE has no per-key response body). Mirrors
/// `send_one_request`'s signing + timeout plumbing but uses
/// HTTP DELETE and skips the content-md5 / content-type
/// headers (no request body on a plain DELETE).
async fn send_single_delete_request(
    config: &WorkerConfig,
    signer: &Signer<AwsCredential>,
    key: &str,
) -> std::io::Result<u16> {
    let mut url = config.endpoint.clone();
    {
        let mut seg = url.path_segments_mut().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "endpoint URL cannot be a base",
            )
        })?;
        seg.clear().push(&config.bucket);
        for segment in key.split('/').filter(|s| !s.is_empty()) {
            seg.push(segment);
        }
    }
    let (parts, _) = http::Request::builder()
        .method(http::Method::DELETE)
        .uri(url.as_str())
        .body(())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?
        .into_parts();
    let mut parts = parts;

    signer
        .sign(&mut parts, None)
        .await
        .map_err(|e| std::io::Error::other(format!("sign: {}", e)))?;

    let http_req: http::Request<Vec<u8>> = http::Request::from_parts(parts, Vec::new());
    let reqwest_req = reqwest::Request::try_from(http_req)
        .map_err(|e| std::io::Error::other(format!("reqwest: {}", e)))?;

    let resp = tokio::time::timeout(config.request_timeout, config.http.execute(reqwest_req))
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "single-object DELETE request timeout after {:?}",
                    config.request_timeout
                ),
            )
        })?
        .map_err(|e| std::io::Error::other(format!("reqwest: {}", e)))?;

    Ok(resp.status().as_u16())
}

// ===== Key joiner =====

/// Join an operator-root prefix with a relative path to form the
/// full S3 object key. Used by the deleter loop (single-DELETE
/// path) and the rmdir-barrier drain (`do_flush_all`).
fn join_key(prefix: &str, rel: &str) -> String {
    let p = prefix.trim_end_matches('/');
    if p.is_empty() {
        rel.to_string()
    } else if let Some(stripped) = rel.strip_prefix('/') {
        format!("{}/{}", p, stripped)
    } else {
        format!("{}/{}", p, rel)
    }
}

// ===== Unit tests =====

#[cfg(test)]
mod tests {
    use super::*;

    /// Process-global mutex serialising all tests that
    /// read/write `MNTRS_DELETE_WORKER_COUNT` via
    /// `unsafe { std::env::set_var / remove_var }`.
    /// cargo runs tests in parallel by default; without
    /// this lock a `set_var` from one test can race a
    /// `remove_var` from another, leaving the worker_count
    /// field holding the wrong value when
    /// `WorkerConfig::from_s3` reads it.
    static WORKER_COUNT_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn retryable_status_set() {
        for s in [408u16, 429, 500, 502, 503, 504] {
            assert!(is_retryable_status(s), "{} should retry", s);
        }
        for s in [200u16, 400, 401, 403, 404, 409] {
            assert!(!is_retryable_status(s), "{} should NOT retry", s);
        }
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
    fn worker_config_from_s3_uses_defaults() {
        // Issue #562 stage 1: the worker_count default lives
        // in env, so unset it before building the config to
        // hit the documented default. Take the env lock
        // first so we don't race the `worker_count_*`
        // sibling tests that set/unset the same env var.
        let _guard = WORKER_COUNT_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("MNTRS_DELETE_WORKER_COUNT");
        }
        let cfg = WorkerConfig::from_s3(
            url::Url::parse("http://localhost:9000").unwrap(),
            "b".into(),
            "/root/".into(),
            "us-east-1".into(),
            Some("ak".into()),
            Some("sk".into()),
            reqwest::Client::new(),
        );
        assert_eq!(cfg.request_timeout, DEFAULT_REQUEST_TIMEOUT);
        assert_eq!(cfg.max_retries, DEFAULT_MAX_RETRIES);
        // Issue #562 stage 1 + Issue #568 stage 6: worker pool
        // defaults to 8 (rclone `--checkers=8`). The previous
        // batch tuners (`batch_size`, `flush_delay`,
        // `fast_flush_threshold`) were removed along with the
        // Profile / Calibrator / BurstObserver subsystem.
        assert_eq!(cfg.worker_count, DEFAULT_DELETE_WORKER_COUNT);
    }

    #[test]
    fn counter_snapshot_is_const_default() {
        // Default snapshot is zero (no I/O). The live counts grow
        // as the worker runs; this only asserts the struct itself.
        // **Stage 6 (issue #568):** the batch-specific counter
        // fields (`single_key_batches_total`,
        // `max_batch_size_observed`, `threshold_skipped_total`,
        // `fast_flush_total`, `single_key_fast_delete_total`,
        // `chunk_size_sum`, `calibrator_recommendations_total`)
        // were dropped along with the rest of the subsystem.
        let s = CounterSnapshot::default();
        assert_eq!(s.flushes_total, 0);
        assert_eq!(s.keys_total, 0);
        assert_eq!(s.failures_total, 0);
        assert_eq!(s.shutdown_lost_total, 0);
        // Issue #562 stage 5: retry_total slot is wired even
        // at rest.
        assert_eq!(s.retry_total, 0);
    }

    #[test]
    fn env_override_parsing_round_trip() {
        // Stage B tuning: just verify the parsing helpers behave.
        // We cannot test the from_s3 env application without
        // mutating process-global env (which races with parallel
        // tests). The end-to-end env behavior is verified by
        // bench/unlink_ab.sh with MNTRS_BATCH_FLUSH_DELAY_MS=10.
        // The bare `unsafe` here is local to test scaffolding.
        unsafe {
            std::env::set_var("MNTRS_TEST_BATCH_SIZE", "25");
            std::env::set_var("MNTRS_TEST_FLUSH_DELAY", "7");
        }
        assert_eq!(env_usize("MNTRS_TEST_BATCH_SIZE", 100), 25);
        assert_eq!(env_u64("MNTRS_TEST_FLUSH_DELAY", 50), 7);
        assert_eq!(env_usize("MNTRS_TEST_MISSING_VAR", 99), 99);
        unsafe {
            std::env::remove_var("MNTRS_TEST_BATCH_SIZE");
            std::env::remove_var("MNTRS_TEST_FLUSH_DELAY");
        }
    }

    #[tokio::test]
    async fn enqueue_returns_none_after_shutdown() {
        let (deleter, _h) = spawn(
            WorkerConfig::from_s3(
                url::Url::parse("http://localhost:9000").unwrap(),
                "b".into(),
                "/".into(),
                "us-east-1".into(),
                Some("ak".into()),
                Some("sk".into()),
                reqwest::Client::new(),
            ),
            std::sync::Arc::new(dashmap::DashSet::new()),
        )
        .unwrap();
        let clone = deleter.clone();
        deleter.shutdown(false).await;
        let r = clone.enqueue("a".into());
        assert!(r.is_none(), "post-shutdown enqueue must reject");
    }

    #[tokio::test]
    async fn spawn_then_drop_exits_worker() {
        let (deleter, handles) = spawn(
            WorkerConfig::from_s3(
                url::Url::parse("http://localhost:9000").unwrap(),
                "b".into(),
                "/".into(),
                "us-east-1".into(),
                Some("ak".into()),
                Some("sk".into()),
                reqwest::Client::new(),
            ),
            std::sync::Arc::new(dashmap::DashSet::new()),
        )
        .unwrap();
        drop(deleter);
        // Issue #562 stage 1 + Issue #568 stage 6: the drain
        // worker owns the `mpsc::Receiver<Control>`. When the
        // last `ConcurrentDeleter` clone is dropped, `flush_tx`
        // closes → drain's `rx.recv()` returns `None` → drain
        // exits. The deleter loops do not exit on their own
        // (they idle on `notify.notified().await`); they are
        // cleaned up when the `crate::rt()` runtime drops.
        // Awaiting the drain handle is the only thing this
        // test can verify cleanly.
        let _ = tokio::time::timeout(Duration::from_secs(2), handles.drain).await;
    }

    // ===== Plan #64 stage C: tombstone lifecycle =====

    /// cancel_pending drains matching queued jobs and clears the
    /// tombstone without sending an S3 DELETE. Reproduces the
    /// `rm X && touch X` race: write-behind enqueue at rm time
    /// inserts the tombstone, then create() calls cancel_pending
    /// to free the path before op.write. Without this the
    /// in-flight S3 DELETE would race the new write.
    #[test]
    fn cancel_pending_drains_jobs_and_clears_tombstone() {
        let tombs = std::sync::Arc::new(dashmap::DashSet::<String>::new());
        // Stub deleter — we don't even need a worker; cancel_pending
        // only touches Shared.pending + Shared.tombs under the lock.
        let shared = std::sync::Arc::new(Shared {
            pending: Mutex::new(Pending::new()),
            accepting: AtomicBool::new(true),
            notify: tokio::sync::Notify::new(),
            tombs: tombs.clone(),
        });
        let (tx, _rx) = mpsc::channel::<Control>(8);
        let deleter = ConcurrentDeleter {
            shared: shared.clone(),
            flush_tx: tx,
        };

        // Simulate what unlink does: enqueue + tombstone.
        tombs.insert("p".into());
        {
            let mut pending = shared.pending.lock().unwrap();
            let (otx, _orx) = oneshot::channel();
            pending.push(PendingDelete {
                relative_path: "p".into(),
                result_tx: otx,
            });
        }
        assert!(tombs.contains("p"));

        // MntrsFs::create calls cancel_pending before op.write.
        let n = deleter.cancel_pending("p");
        assert_eq!(n, 1, "exactly the one queued job must be cancelled");
        assert!(
            !tombs.contains("p"),
            "tombstone must be cleared so lookup returns ENOENT → OK"
        );
        assert!(
            shared.pending.lock().unwrap().jobs.is_empty(),
            "queue must be drained"
        );
    }

    /// cancel_pending is a no-op for paths that aren't queued and
    /// not tombstoned — but it does clear a tombstone if present
    /// (the FUSE side may have stale state from a prior failure).
    #[test]
    fn cancel_pending_unknown_path_clears_stale_tombstone() {
        let tombs = std::sync::Arc::new(dashmap::DashSet::<String>::new());
        let shared = std::sync::Arc::new(Shared {
            pending: Mutex::new(Pending::new()),
            accepting: AtomicBool::new(true),
            notify: tokio::sync::Notify::new(),
            tombs: tombs.clone(),
        });
        let (tx, _rx) = mpsc::channel::<Control>(8);
        let deleter = ConcurrentDeleter {
            shared: shared.clone(),
            flush_tx: tx,
        };
        tombs.insert("stale".into());
        let n = deleter.cancel_pending("stale");
        assert_eq!(n, 0);
        assert!(!tombs.contains("stale"));
    }

    /// cancel_pending on an absent path is a clean no-op.
    #[test]
    fn cancel_pending_absent_path_is_noop() {
        let tombs = std::sync::Arc::new(dashmap::DashSet::<String>::new());
        let shared = std::sync::Arc::new(Shared {
            pending: Mutex::new(Pending::new()),
            accepting: AtomicBool::new(true),
            notify: tokio::sync::Notify::new(),
            tombs: tombs.clone(),
        });
        let (tx, _rx) = mpsc::channel::<Control>(8);
        let deleter = ConcurrentDeleter {
            shared: shared.clone(),
            flush_tx: tx,
        };
        let n = deleter.cancel_pending("never-existed");
        assert_eq!(n, 0);
        assert!(tombs.is_empty());
    }

    // ===== make_test_deleter helper =====

    /// Helper for tests: build a Shared literal + a stub
    /// ConcurrentDeleter whose `flush_tx` channel is a dead end
    /// (no worker is spawned — the tests don't actually need
    /// the worker; they only inspect `enqueue()`'s return
    /// value and `pending_len()`).
    ///
    /// **Stage 6 (issue #568):** the threshold gating in
    /// `enqueue()` was removed wholesale (the accompanying
    /// `THRESHOLD_SKIPPED_TOTAL` counter was dropped too).
    /// The helper is kept as a tiny Shared literal factory
    /// so the cancel_pending tests can build a deleter
    /// without spawning the worker. The `threshold`
    /// parameter is ignored (kept for API compatibility with
    /// the deleted tests).
    fn make_test_deleter_with_threshold(
        threshold: usize,
    ) -> (ConcurrentDeleter, std::sync::Arc<dashmap::DashSet<String>>) {
        let _ = threshold;
        let tombs = std::sync::Arc::new(dashmap::DashSet::<String>::new());
        let shared = std::sync::Arc::new(Shared {
            pending: Mutex::new(Pending::new()),
            accepting: AtomicBool::new(true),
            notify: tokio::sync::Notify::new(),
            tombs: tombs.clone(),
        });
        let (tx, _rx) = mpsc::channel::<Control>(8);
        let deleter = ConcurrentDeleter {
            shared: shared.clone(),
            flush_tx: tx,
        };
        (deleter, tombs)
    }

    // ===== decide_next_action tests (Issue #553) =====
    //
    // **Stage 6 (issue #568):** the `decide_next_action` helper,
    // the `ScheduledAction` enum, and the entire fast-flush /
    // wait-for-deadline accumulator were removed along with the
    // Profile / Calibrator / BurstObserver subsystem. The new
    // design (rclone-style N concurrent single-DELETE) has no
    // decision logic to test — each deleter pops one key, fires
    // `send_single_delete_with_retry`, and loops. The tests
    // below that previously exercised `decide_next_action`
    // (`decide_empty_queue_returns_none`,
    // `decide_full_batch_returns_size_driven_flush`,
    // `decide_above_batch_size_also_size_driven`,
    // `decide_small_batch_fast_flushes_when_threshold_set`,
    // `decide_middle_band_waits_for_deadline`,
    // `decide_threshold_is_strict_less_than`,
    // `decide_threshold_zero_disables_fast_branch`) are gone.

    // ===== Issue #562 stage 1 + Issue #568 stage 6: worker_count env wiring =====
    //
    // These tests share process-global env state via
    // `unsafe { std::env::set_var/remove_var }`. cargo runs
    // tests in parallel by default, so they must serialise
    // on `WORKER_COUNT_ENV_LOCK` (declared at the top of
    // this module) so a `set_var` from one test doesn't race
    // a `remove_var` from another. `worker_config_from_s3_uses_defaults`
    // also acquires the lock for the same reason.
    //
    // **Stage 6 (issue #568):** the env var was renamed from
    // `MNTRS_BATCH_WORKER_COUNT` to `MNTRS_DELETE_WORKER_COUNT`
    // (the old name still worked with the old flusher pool; the
    // new N-deleter pool is conceptually different — single-DELETE
    // per worker, no batching). The default also moved from 4
    // (rclone `--transfers=4` analogy) to 8 (rclone
    // `--checkers=8` analogy).

    /// Default worker_count is 8 when MNTRS_DELETE_WORKER_COUNT
    /// is unset. Matches rclone `--checkers=8` so the S3
    /// worker pool can issue N concurrent single-DELETE
    /// round-trips the same way rclone Checkers do.
    #[test]
    fn worker_count_default_is_eight() {
        let _guard = WORKER_COUNT_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("MNTRS_DELETE_WORKER_COUNT");
        }
        let cfg = WorkerConfig::from_s3(
            url::Url::parse("http://localhost:9000").unwrap(),
            "b".into(),
            "/root/".into(),
            "us-east-1".into(),
            Some("ak".into()),
            Some("sk".into()),
            reqwest::Client::new(),
        );
        assert_eq!(cfg.worker_count, DEFAULT_DELETE_WORKER_COUNT);
        assert_eq!(cfg.worker_count, 8);
    }

    /// Out-of-range env values clamp to MAX_DELETE_WORKER_COUNT
    /// (16). Each deleter holds its own signer clone, and a
    /// misconfigured `MNTRS_DELETE_WORKER_COUNT=999` must not
    /// spawn 999 S3 clients.
    #[test]
    fn worker_count_clamped_to_max_16() {
        let _guard = WORKER_COUNT_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("MNTRS_DELETE_WORKER_COUNT", "999");
        }
        let cfg = WorkerConfig::from_s3(
            url::Url::parse("http://localhost:9000").unwrap(),
            "b".into(),
            "/".into(),
            "us-east-1".into(),
            Some("ak".into()),
            Some("sk".into()),
            reqwest::Client::new(),
        );
        assert_eq!(cfg.worker_count, MAX_DELETE_WORKER_COUNT);
        assert_eq!(cfg.worker_count, 16);
        unsafe {
            std::env::remove_var("MNTRS_DELETE_WORKER_COUNT");
        }
    }

    /// Env values <= 0 clamp to 1, reproducing the pre-#562
    /// single-consumer behaviour. The user's intent for
    /// `MNTRS_DELETE_WORKER_COUNT=0` is "don't multi-task",
    /// which the existing impl achieves with one deleter.
    #[test]
    fn worker_count_clamped_to_min_1() {
        let _guard = WORKER_COUNT_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("MNTRS_DELETE_WORKER_COUNT", "0");
        }
        let cfg = WorkerConfig::from_s3(
            url::Url::parse("http://localhost:9000").unwrap(),
            "b".into(),
            "/".into(),
            "us-east-1".into(),
            Some("ak".into()),
            Some("sk".into()),
            reqwest::Client::new(),
        );
        assert_eq!(cfg.worker_count, 1);
        unsafe {
            std::env::remove_var("MNTRS_DELETE_WORKER_COUNT");
        }
    }
}
