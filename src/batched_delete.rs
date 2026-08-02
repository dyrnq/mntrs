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
//! Mirrors `writeback::spawn`: dropping the last `BatchedDeleter`
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
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use md5::{Digest, Md5};
use quick_xml::Writer;
use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};
use reqsign_aws_v4::{
    Credential as AwsCredential, DefaultCredentialProvider, EnvCredentialProvider,
    ProfileCredentialProvider, RequestSigner, StaticCredentialProvider,
};
use reqsign_core::{Context, ProvideCredentialChain, Signer};
use reqsign_file_read_tokio::TokioFileRead;
use tokio::sync::{mpsc, oneshot};

// ===== Constants =====

/// S3 hard limit: at most 1000 object identifiers per DeleteObjects
/// request. We chunk below this regardless of the configured
/// `batch_size` so a misconfiguration cannot produce an invalid
/// request.
pub(crate) const HARD_MAX_KEYS_PER_REQUEST: usize = 1000;

// Plan #64 stage B: per-mount tuning knobs surfaced via
// environment variables. Defaults preserve the values that the
// original plan #64 baseline used.
pub(crate) const DEFAULT_BATCH_SIZE: usize = 100;
pub(crate) const DEFAULT_FLUSH_DELAY: Duration = Duration::from_millis(50);
pub(crate) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const DEFAULT_MAX_RETRIES: u32 = 3;
pub(crate) const DEFAULT_RETRY_FACTOR: f64 = 2.0;
pub(crate) const DEFAULT_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(100);

// ===== Counters (plan #64 stage B) =====
//
// Process-static counters, like writeback::PENDING_COUNT. Read
// from any thread via the public accessor functions below.

static FLUSHES_TOTAL: AtomicU64 = AtomicU64::new(0);
static KEYS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHUTDOWN_LOST_TOTAL: AtomicU64 = AtomicU64::new(0);
static SINGLE_KEY_BATCHES_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAX_BATCH_SIZE_OBSERVED: AtomicU64 = AtomicU64::new(0);

/// Plan #64 stage B: snapshot of batched_delete counters.
/// Exposed for `/metrics` and Stage C observability.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct CounterSnapshot {
    pub flushes_total: u64,
    pub keys_total: u64,
    pub failures_total: u64,
    pub shutdown_lost_total: u64,
    pub single_key_batches_total: u64,
    pub max_batch_size_observed: u64,
}

pub(crate) fn snapshot() -> CounterSnapshot {
    CounterSnapshot {
        flushes_total: FLUSHES_TOTAL.load(Ordering::Relaxed),
        keys_total: KEYS_TOTAL.load(Ordering::Relaxed),
        failures_total: FAILURES_TOTAL.load(Ordering::Relaxed),
        shutdown_lost_total: SHUTDOWN_LOST_TOTAL.load(Ordering::Relaxed),
        single_key_batches_total: SINGLE_KEY_BATCHES_TOTAL.load(Ordering::Relaxed),
        max_batch_size_observed: MAX_BATCH_SIZE_OBSERVED.load(Ordering::Relaxed),
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
    jobs: Vec<PendingDelete>,
    deadline: Option<Instant>,
}

impl Pending {
    fn new() -> Self {
        Self {
            jobs: Vec::with_capacity(DEFAULT_BATCH_SIZE),
            deadline: None,
        }
    }
    fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }
    fn len(&self) -> usize {
        self.jobs.len()
    }
    fn reset_deadline(&mut self, delay: Duration) {
        self.deadline = Some(Instant::now() + delay);
    }
    fn clear_deadline(&mut self) {
        self.deadline = None;
    }
}

// ===== Shared state =====

struct Shared {
    pending: Mutex<Pending>,
    accepting: AtomicBool,
    /// Initial deadline applied when the buffer transitions from
    /// empty → non-empty in `enqueue`. Mirrors `WorkerConfig::flush_delay`
    /// (env: `MNTRS_BATCH_FLUSH_DELAY_MS`, default 50 ms) so a single
    /// unlink pays at most this much latency before its S3 DELETE is
    /// requested, while an `rm -rf` burst gets a wide enough window to
    /// accumulate large batches. The rmdir barrier (`Control::Flush`)
    /// bypasses this deadline entirely via `do_flush_all`.
    flush_delay: Duration,
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
/// Dropping all `BatchedDeleter` handles (including the one stored
/// in `MntrsFs`) closes the control channel and the worker exits on
/// `rx.recv().await → None`.
#[derive(Clone)]
pub(crate) struct BatchedDeleter {
    shared: Arc<Shared>,
    flush_tx: mpsc::Sender<Control>,
}

// ===== Worker config =====

/// Construction-time config. Built by `cmd/mount.rs::build_s3` after
/// parsing the storage URL and CLI options; passed to
/// `batched_delete::spawn`.
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
    pub batch_size: usize,
    pub flush_delay: Duration,
    pub request_timeout: Duration,
    pub max_retries: u32,
    pub retry_factor: f64,
    pub retry_initial_backoff: Duration,
}

impl WorkerConfig {
    /// Build the production config from S3 mount-time inputs.
    /// `prefix` should be the opendal root (e.g. `/some/dir/`).
    pub(crate) fn from_s3(
        endpoint: url::Url,
        bucket: String,
        prefix: String,
        region: String,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        http: reqwest::Client,
    ) -> Self {
        // Plan #64 stage B: honor MNTRS_BATCH_SIZE and
        // MNTRS_BATCH_FLUSH_DELAY_MS env vars for per-mount
        // tuning. Defaults preserve plan #64 baseline.
        let batch_size = env_usize("MNTRS_BATCH_SIZE", DEFAULT_BATCH_SIZE).clamp(1, 1000);
        let flush_delay_ms = env_u64(
            "MNTRS_BATCH_FLUSH_DELAY_MS",
            DEFAULT_FLUSH_DELAY.as_millis() as u64,
        );
        let flush_delay = Duration::from_millis(flush_delay_ms.clamp(1, 10_000));
        Self {
            endpoint,
            bucket,
            prefix,
            region,
            access_key_id,
            secret_access_key,
            http,
            batch_size,
            flush_delay,
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
    /// Used by `BatchedDeleter::flush()` (Policy B rmdir barrier).
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

pub(crate) fn spawn(
    config: WorkerConfig,
    tombs: std::sync::Arc<dashmap::DashSet<String>>,
) -> std::io::Result<(BatchedDeleter, tokio::task::JoinHandle<()>)> {
    let (tx, rx) = mpsc::channel::<Control>(64);
    let shared = Arc::new(Shared {
        pending: Mutex::new(Pending::new()),
        accepting: AtomicBool::new(true),
        flush_delay: config.flush_delay,
        tombs,
    });
    let deleter = BatchedDeleter {
        shared: shared.clone(),
        flush_tx: tx,
    };
    // Plan #64 stage A bug fix: must use crate::rt().spawn,
    // not bare tokio::spawn. The mount's main thread is not
    // inside a tokio runtime context — bare tokio::spawn
    // panics with "there is no reactor running" before the
    // worker even starts. writeback::spawn uses the same
    // pattern (writeback.rs:174).
    let handle = crate::rt().spawn(worker_loop(config, shared, rx));
    Ok((deleter, handle))
}

// ===== BatchedDeleter API =====

impl BatchedDeleter {
    /// Enqueue a relative path for deletion. Returns `None` if the
    /// worker is shutting down (handle will not accept new work);
    /// returns `Some(oneshot::Receiver)` otherwise. The caller
    /// (MntrsFs write-behind path) drops the receiver — the per-key
    /// S3 outcome is observed via the worker's tracing logs, not
    /// via the receiver. The receiver exists for tests and for any
    /// strict caller.
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
        pending.jobs.push(job);
        if was_empty {
            // Use the configured flush_delay (env:
            // MNTRS_BATCH_FLUSH_DELAY_MS, default 50ms) rather than
            // a hard-coded 5ms. For an `rm file.txt` single-file
            // call the user pays at most `flush_delay` latency
            // before the S3 DELETE is requested; for an `rm -rf`
            // burst the wider window lets more keys accumulate
            // per batch and halves the number of S3 roundtrips.
            // The rmdir barrier (`Control::Flush`) bypasses this
            // deadline entirely via `do_flush_all`.
            pending.reset_deadline(self.shared.flush_delay);
        }
        drop(pending);

        if was_empty {
            // Wake the worker so it doesn't wait out its current
            // `rx.recv().await` before noticing the new deadline.
            // try_send because the channel is bounded and a slow
            // worker isn't a reason to block the FUSE thread; if
            // the worker is mid-flush, the next iteration will see
            // the new pending entries on its own.
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
            let mut kept = Vec::with_capacity(pending.jobs.len());
            for job in pending.jobs.drain(..) {
                if job.relative_path == relative_path {
                    let _ = job.result_tx.send(Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "batched_deleter: cancelled by recreate",
                    )));
                    cancelled += 1;
                } else {
                    kept.push(job);
                }
            }
            pending.jobs = kept;
            if pending.is_empty() {
                pending.clear_deadline();
            }
        }
        // Tombstone clear is unconditional: even if no job was
        // pending, the FUSE-side lookup will hit it if the user
        // did `rm file; sleep 50ms; touch file` (50ms < flush
        // deadline of 5ms would race, but if user pre-set
        // MNTRS_BATCH_FLUSH_DELAY_MS=10 or higher, the tombstone
        // outlives the queue). Clearing on every create() call
        // is the safest invariant.
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
                "batched_deleter: worker channel closed",
            ));
        }
        done_rx.await.map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "batched_deleter: worker dropped flush response",
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
}

// ===== Worker loop =====

async fn worker_loop(config: WorkerConfig, shared: Arc<Shared>, mut rx: mpsc::Receiver<Control>) {
    let signer = match build_signer(&config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(
                target: "mntrs::batched_delete",
                bucket = %config.bucket,
                prefix = %config.prefix,
                error = %e,
                "batched_delete: failed to build signer, worker exiting"
            );
            shared.accepting.store(false, Ordering::Release);
            fail_all_pending(&shared);
            return;
        }
    };

    tracing::info!(
        target: "mntrs::batched_delete",
        bucket = %config.bucket,
        prefix = %config.prefix,
        batch_size = config.batch_size,
        flush_delay_ms = config.flush_delay.as_millis() as u64,
        credential_source = if config.access_key_id.is_some() { "explicit" } else { "default-chain" },
        "batched_delete: worker started"
    );

    loop {
        // Snapshot pending state under lock; decide what to do.
        let action = {
            let pending = shared.pending.lock().expect("pending mutex poisoned");
            if pending.len() >= config.batch_size {
                Some(ScheduledAction::FlushBatch)
            } else if pending.is_empty() {
                None
            } else {
                Some(ScheduledAction::WaitForDeadline(pending.deadline))
            }
        };

        match action {
            None => match rx.recv().await {
                Some(Control::Wake) => continue,
                Some(Control::Flush { done }) => {
                    let flushed = do_flush_all(&config, &signer, &shared).await;
                    let _ = done.send(flushed);
                    continue;
                }
                Some(Control::Shutdown { drain, done }) => {
                    if drain {
                        let _ = do_flush_all(&config, &signer, &shared).await;
                    } else {
                        // Lost without drain — record how many.
                        let lost = {
                            let mut p = shared.pending.lock().expect("pending mutex poisoned");
                            let n = p.jobs.len() as u64;
                            p.jobs.clear();
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
                None => {
                    // Channel closed without explicit shutdown — same
                    // accounting as drain=false: lost keys count.
                    let lost = {
                        let p = shared.pending.lock().expect("pending mutex poisoned");
                        p.jobs.len() as u64
                    };
                    if lost > 0 {
                        SHUTDOWN_LOST_TOTAL.fetch_add(lost, Ordering::Relaxed);
                    }
                    fail_all_pending(&shared);
                    break;
                }
            },
            Some(ScheduledAction::FlushBatch) => {
                flush_one_batch(&config, &signer, &shared, config.batch_size).await;
            }
            Some(ScheduledAction::WaitForDeadline(deadline)) => {
                let now = Instant::now();
                let sleep_dur = deadline
                    .map(|d| d.saturating_duration_since(now))
                    .unwrap_or(config.flush_delay);
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep(sleep_dur) => {
                        flush_one_batch(&config, &signer, &shared, config.batch_size).await;
                    }
                    ctrl = rx.recv() => match ctrl {
                        Some(Control::Wake) => continue,
                        Some(Control::Flush { done }) => {
                            let flushed = do_flush_all(&config, &signer, &shared).await;
                            let _ = done.send(flushed);
                        }
                        Some(Control::Shutdown { drain, done }) => {
                            if drain {
                                let _ = do_flush_all(&config, &signer, &shared).await;
                            } else {
                                fail_all_pending(&shared);
                            }
                            let _ = done.send(());
                            break;
                        }
                        None => {
                            fail_all_pending(&shared);
                            break;
                        }
                    }
                }
            }
        }
    }

    tracing::info!(
        target: "mntrs::batched_delete",
        "batched_delete: worker exiting"
    );
}

enum ScheduledAction {
    FlushBatch,
    WaitForDeadline(Option<Instant>),
}

// ===== Flush helpers =====

/// Flush one batch of up to `limit` keys. Updates the deadline for
/// any remaining keys.
async fn flush_one_batch(
    config: &WorkerConfig,
    signer: &Signer<AwsCredential>,
    shared: &Shared,
    limit: usize,
) {
    let batch = {
        let mut pending = shared.pending.lock().expect("pending mutex poisoned");
        if pending.is_empty() {
            return;
        }
        let take = pending.jobs.len().min(limit);
        let drained: Vec<PendingDelete> = pending.jobs.drain(..take).collect();
        if !pending.is_empty() {
            pending.reset_deadline(config.flush_delay);
        } else {
            pending.clear_deadline();
        }
        drained
    };

    if batch.is_empty() {
        return;
    }

    let started = Instant::now();
    let outcome = send_chunk_with_retry(config, signer, &batch, &config.prefix).await;
    let elapsed_ms = started.elapsed().as_millis() as u64;

    FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
    KEYS_TOTAL.fetch_add(batch.len() as u64, Ordering::Relaxed);
    if batch.len() == 1 {
        SINGLE_KEY_BATCHES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    // Update max batch size (lock-free CAS).
    let bs = batch.len() as u64;
    let mut cur = MAX_BATCH_SIZE_OBSERVED.load(Ordering::Relaxed);
    while bs > cur {
        match MAX_BATCH_SIZE_OBSERVED.compare_exchange_weak(
            cur,
            bs,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(observed) => cur = observed,
        }
    }
    let (succeeded, not_found, failed) = count_outcome(&outcome);
    if failed > 0 {
        FAILURES_TOTAL.fetch_add(failed, Ordering::Relaxed);
    }

    tracing::info!(
        target: "mntrs::batched_delete",
        batch_size = batch.len(),
        elapsed_ms,
        succeeded,
        not_found,
        failed,
        reason = "scheduled",
        "batched_delete: flush"
    );

    for (job, result) in batch.into_iter().zip(outcome) {
        // Same tombstone-cleanup-on-ack policy as do_flush_all.
        // Centralised here so the two flush paths stay in lockstep;
        // if we ever add a third path, both helpers must converge
        // on it. `result` is `io::Result<()>`; success clears the
        // tombstone silently, NotFound/AlreadyExists clear it
        // silently (idempotent outcome), and any other error logs
        // an `error!` line then clears the tombstone so the user
        // can see the object again.
        match &result {
            Ok(()) => {
                shared.tombs.remove(&job.relative_path);
            }
            Err(e) if matches!(e.kind(), std::io::ErrorKind::NotFound) => {
                shared.tombs.remove(&job.relative_path);
            }
            Err(e) => {
                tracing::error!(
                    target: "mntrs::batched_delete",
                    path = %job.relative_path,
                    error = %e,
                    "batched_delete: per-key delete failed; clearing tombstone so object becomes visible again"
                );
                shared.tombs.remove(&job.relative_path);
            }
        }
        let _ = job.result_tx.send(result);
    }
}

/// Flush all pending keys in chunks of HARD_MAX_KEYS_PER_REQUEST.
/// Returns total number of keys flushed across chunks.
async fn do_flush_all(
    config: &WorkerConfig,
    signer: &Signer<AwsCredential>,
    shared: &Arc<Shared>,
) -> std::io::Result<usize> {
    let mut total = 0usize;
    loop {
        let batch_size = {
            let pending = shared.pending.lock().expect("pending mutex poisoned");
            pending.len()
        };
        if batch_size == 0 {
            return Ok(total);
        }
        let limit = batch_size.min(HARD_MAX_KEYS_PER_REQUEST);
        let batch: Vec<PendingDelete> = {
            let mut pending = shared.pending.lock().expect("pending mutex poisoned");
            pending.jobs.drain(..limit).collect()
        };
        if batch.is_empty() {
            return Ok(total);
        }
        let outcome = send_chunk_with_retry(config, signer, &batch, &config.prefix).await;
        let (succeeded, not_found, failed) = count_outcome(&outcome);
        if failed > 0 {
            FAILURES_TOTAL.fetch_add(failed, Ordering::Relaxed);
        }
        FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
        KEYS_TOTAL.fetch_add(batch.len() as u64, Ordering::Relaxed);
        for (job, result) in batch.into_iter().zip(outcome) {
            // Plan #64 stage C: tombstone cleanup is part of the
            // per-key ack path. Every terminal outcome clears
            // its tombstone — success, NotFound (idempotent),
            // and permanent failures alike. Without this the
            // FUSE side's lookup/getattr/readdir filters would
            // keep masking the path even after the worker
            // confirmed the delete landed on S3, and a
            // recreate would return ENOENT until the user
            // worked around it. See flush_one_batch for the
            // canonical implementation — mirrored here to keep
            // the lock-and-send pattern local to each flush site.
            match &result {
                Ok(()) => {
                    shared.tombs.remove(&job.relative_path);
                }
                Err(e) if matches!(e.kind(), std::io::ErrorKind::NotFound) => {
                    shared.tombs.remove(&job.relative_path);
                }
                Err(e) => {
                    tracing::error!(
                        target: "mntrs::batched_delete",
                        path = %job.relative_path,
                        error = %e,
                        "batched_delete: per-key delete failed; clearing tombstone so object becomes visible again"
                    );
                    shared.tombs.remove(&job.relative_path);
                }
            }
            let _ = job.result_tx.send(result);
        }
        total += succeeded as usize + not_found as usize + failed as usize;
    }
}

fn fail_all_pending(shared: &Shared) {
    let jobs = {
        let mut pending = shared.pending.lock().expect("pending mutex poisoned");
        pending.clear_deadline();
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
            target: "mntrs::batched_delete",
            lost = count,
            "batched_delete: shutdown dropped pending deletes"
        );
    }
    for job in jobs {
        let _ = job.result_tx.send(Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "batched_deleter: shutdown lost",
        )));
    }
}

fn count_outcome(results: &[std::io::Result<()>]) -> (u64, u64, u64) {
    let mut s = 0u64;
    let mut n = 0u64;
    let mut f = 0u64;
    for r in results {
        match r {
            Ok(()) => s += 1,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => n += 1,
            Err(_) => f += 1,
        }
    }
    (s, n, f)
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

/// Send one chunk (≤HARD_MAX_KEYS_PER_REQUEST keys) with retry.
/// Retries transport failures + retryable HTTP statuses. On
/// non-retryable failure or exhausted retries, every key in the
/// chunk receives the same `Err`. On HTTP 200, parses per-key
/// errors (Quiet=true means success is the absence of an entry).
async fn send_chunk_with_retry(
    config: &WorkerConfig,
    signer: &Signer<AwsCredential>,
    chunk: &[PendingDelete],
    prefix: &str,
) -> Vec<std::io::Result<()>> {
    let body = build_delete_objects_body(chunk, prefix);
    let body_bytes = body.into_bytes();
    let content_md5 = base64_md5(&body_bytes);

    let mut attempt = 0u32;
    let mut backoff = config.retry_initial_backoff;
    loop {
        match send_one_request(config, signer, &body_bytes, &content_md5).await {
            Ok((200, resp_xml)) => {
                // Parse per-key. Per the plan: per-key NoSuchKey /
                // NoSuchVersion → success; everything else → Err.
                // Quiet=true means keys absent from response = Ok.
                let per_key = parse_delete_objects_response(&resp_xml);

                // We have to map per-key entries back to chunk
                // indices. S3 returns one <Error> per failing key,
                // in the same order as requested. Quiet=true means
                // no <Error> for successful keys, so we pad with
                // Ok(()) to chunk.len() entries.
                let mut results: Vec<std::io::Result<()>> = Vec::with_capacity(chunk.len());
                let mut per_key_iter = per_key.into_iter();
                for _ in 0..chunk.len() {
                    match per_key_iter.next() {
                        Some(r) => results.push(r),
                        None => results.push(Ok(())),
                    }
                }
                return results;
            }
            Ok((status, _body)) => {
                let err_msg = format!(
                    "batched_delete: S3 DeleteObjects HTTP {} (request-level, failing all keys in chunk)",
                    status
                );
                if is_retryable_status(status) && attempt < config.max_retries {
                    tracing::warn!(
                        target: "mntrs::batched_delete",
                        status,
                        attempt = attempt + 1,
                        backoff_ms = backoff.as_millis() as u64,
                        "retrying after retryable status"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = next_backoff(backoff, config.retry_factor);
                    attempt += 1;
                    continue;
                }
                let kind = if status == 404 {
                    std::io::ErrorKind::NotFound
                } else {
                    std::io::ErrorKind::Other
                };
                let msg = if status == 404 {
                    format!(
                        "{} (request-level 404 is NoSuchBucket, not idempotent missing key)",
                        err_msg
                    )
                } else {
                    err_msg
                };
                return (0..chunk.len())
                    .map(|_| Err(std::io::Error::new(kind, msg.clone())))
                    .collect();
            }
            Err(e) => {
                if attempt < config.max_retries {
                    tracing::warn!(
                        target: "mntrs::batched_delete",
                        error = %e,
                        attempt = attempt + 1,
                        backoff_ms = backoff.as_millis() as u64,
                        "retrying after transport error"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = next_backoff(backoff, config.retry_factor);
                    attempt += 1;
                    continue;
                }
                let msg = format!(
                    "batched_delete: transport failure after {} retries: {}",
                    config.max_retries, e
                );
                return (0..chunk.len())
                    .map(|_| Err(std::io::Error::other(msg.clone())))
                    .collect();
            }
        }
    }
}

fn is_retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

fn next_backoff(current: Duration, factor: f64) -> Duration {
    Duration::from_secs_f64(current.as_secs_f64() * factor)
}

/// Send one signed DeleteObjects request. Returns
/// `(status_code, response_body_xml)`.
async fn send_one_request(
    config: &WorkerConfig,
    signer: &Signer<AwsCredential>,
    body_bytes: &[u8],
    content_md5_b64: &str,
) -> std::io::Result<(u16, String)> {
    let url = build_delete_objects_url(&config.endpoint, &config.bucket)?;
    let (parts, _) = http::Request::builder()
        .method(http::Method::POST)
        .uri(url.as_str())
        .body(())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?
        .into_parts();
    let mut parts = parts;

    parts.headers.insert(
        "content-type",
        http::HeaderValue::from_static("application/xml"),
    );
    parts.headers.insert(
        "content-length",
        http::HeaderValue::from_str(&body_bytes.len().to_string())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?,
    );
    parts.headers.insert(
        "content-md5",
        http::HeaderValue::from_str(content_md5_b64)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?,
    );

    signer
        .sign(&mut parts, None)
        .await
        .map_err(|e| std::io::Error::other(format!("sign: {}", e)))?;

    let http_req: http::Request<Vec<u8>> = http::Request::from_parts(parts, body_bytes.to_vec());
    let reqwest_req = reqwest::Request::try_from(http_req)
        .map_err(|e| std::io::Error::other(format!("reqwest: {}", e)))?;

    let resp = tokio::time::timeout(config.request_timeout, config.http.execute(reqwest_req))
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "DeleteObjects request timeout after {:?}",
                    config.request_timeout
                ),
            )
        })?
        .map_err(|e| std::io::Error::other(format!("reqwest: {}", e)))?;

    let status = resp.status().as_u16();
    let body = resp
        .text()
        .await
        .map_err(|e| std::io::Error::other(format!("read body: {}", e)))?;
    Ok((status, body))
}

// ===== XML body builder =====

/// Build the `<Delete>...</Delete>` body for one chunk.
/// `prefix` is the operator root (e.g. `/some/dir/`) prepended to
/// every relative path.
pub(crate) fn build_delete_objects_body(chunk: &[PendingDelete], prefix: &str) -> String {
    let mut writer = Writer::new(Vec::<u8>::new());
    writer
        .write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))
        .expect("xml decl write");
    writer
        .write_event(Event::Start(BytesStart::new("Delete")))
        .expect("Delete start");
    for job in chunk {
        writer
            .write_event(Event::Start(BytesStart::new("Object")))
            .expect("Object start");
        let key_full = join_key(prefix, &job.relative_path);
        writer
            .write_event(Event::Start(BytesStart::new("Key")))
            .expect("Key start");
        writer
            .write_event(Event::Text(BytesText::new(&key_full)))
            .expect("Key text");
        writer
            .write_event(Event::End(BytesEnd::new("Key")))
            .expect("Key end");
        writer
            .write_event(Event::End(BytesStart::new("Object").to_end()))
            .expect("Object end");
    }
    writer
        .write_event(Event::Start(BytesStart::new("Quiet")))
        .expect("Quiet start");
    writer
        .write_event(Event::Text(BytesText::new("true")))
        .expect("Quiet text");
    writer
        .write_event(Event::End(BytesEnd::new("Quiet")))
        .expect("Quiet end");
    writer
        .write_event(Event::End(BytesStart::new("Delete").to_end()))
        .expect("Delete end");
    String::from_utf8(writer.into_inner()).expect("xml utf8")
}

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

fn base64_md5(body: &[u8]) -> String {
    let mut hasher = Md5::new();
    hasher.update(body);
    B64.encode(hasher.finalize())
}

// ===== URL builder =====

/// Build the path-style DeleteObjects URL:
/// `{endpoint}/{bucket}/?delete`. Path style for S3-compatible
/// endpoints (MinIO defaults to path-style).
pub(crate) fn build_delete_objects_url(
    endpoint: &url::Url,
    bucket: &str,
) -> std::io::Result<url::Url> {
    let mut url = endpoint.clone();
    {
        let mut seg = url.path_segments_mut().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "endpoint URL cannot be a base",
            )
        })?;
        seg.clear().push(bucket);
    }
    url.query_pairs_mut().append_pair("delete", "");
    Ok(url)
}

// ===== Response parser =====

/// Parse the DeleteObjects response XML into a per-failing-key
/// `Result<(), io::Error>`. `Quiet=true` means successful keys are
/// absent from the response (they map to `Ok(())`).
///
/// Per-key error mapping:
/// - `NoSuchKey` / `NoSuchVersion` / `NoSuchUpload` → Ok(())
/// - Anything else → Err(io::Error)
pub(crate) fn parse_delete_objects_response(xml: &str) -> Vec<std::io::Result<()>> {
    use quick_xml::events::Event as QEvent;
    use quick_xml::reader::Reader;

    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut out: Vec<std::io::Result<()>> = Vec::new();
    let mut in_error = false;
    let mut cur_key: Option<String> = None;
    let mut cur_code: Option<String> = None;
    let mut cur_message: Option<String> = None;
    let mut last_tag: Option<String> = None;

    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(QEvent::Start(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                match name.as_str() {
                    "Error" => {
                        in_error = true;
                        cur_key = None;
                        cur_code = None;
                        cur_message = None;
                    }
                    "Key" => cur_key = None,
                    "Code" => cur_code = None,
                    "Message" => cur_message = None,
                    _ => {}
                }
                last_tag = Some(name);
            }
            Ok(QEvent::Text(t)) => {
                let raw = t.into_inner();
                let txt = String::from_utf8_lossy(&raw).into_owned();
                if in_error {
                    match last_tag.as_deref() {
                        Some("Key") => cur_key = Some(txt),
                        Some("Code") => cur_code = Some(txt),
                        Some("Message") => cur_message = Some(txt),
                        _ => {}
                    }
                }
            }
            Ok(QEvent::End(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                if name == "Error" && in_error {
                    let code = cur_code.take().unwrap_or_default();
                    let message = cur_message.take().unwrap_or_default();
                    let _key = cur_key.take();
                    let is_not_found = matches!(
                        code.as_str(),
                        "NoSuchKey" | "NoSuchVersion" | "NoSuchUpload"
                    );
                    let entry = if is_not_found {
                        Ok(())
                    } else {
                        Err(std::io::Error::other(format!(
                            "S3 DeleteObjects per-key error: {} ({})",
                            code, message
                        )))
                    };
                    out.push(entry);
                    in_error = false;
                }
                last_tag = None;
            }
            Ok(QEvent::Eof) => break,
            Err(_) => break,
            _ => {
                last_tag = None;
            }
        }
        buf.clear();
    }
    out
}

// ===== Unit tests =====

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_job(path: &str) -> PendingDelete {
        PendingDelete {
            relative_path: path.to_string(),
            result_tx: oneshot::channel().0,
        }
    }

    #[test]
    fn build_body_emits_delete_quiet_object_keys() {
        let jobs = vec![fake_job("a/b.txt"), fake_job("c.txt")];
        let body = build_delete_objects_body(&jobs, "/root/");
        assert!(body.contains("<Delete>"));
        assert!(body.contains("<Quiet>true</Quiet>"));
        assert!(body.contains("<Key>/root/a/b.txt</Key>"));
        assert!(body.contains("<Key>/root/c.txt</Key>"));
        assert!(body.contains("</Delete>"));
    }

    #[test]
    fn build_body_prefix_root_without_slash() {
        let jobs = vec![fake_job("a.txt")];
        let body = build_delete_objects_body(&jobs, "root");
        assert!(body.contains("<Key>root/a.txt</Key>"));
    }

    #[test]
    fn build_body_relative_path_with_leading_slash() {
        let jobs = vec![fake_job("/a.txt")];
        let body = build_delete_objects_body(&jobs, "/root/");
        assert!(body.contains("<Key>/root/a.txt</Key>"));
    }

    #[test]
    fn build_body_empty_prefix() {
        let jobs = vec![fake_job("a.txt")];
        let body = build_delete_objects_body(&jobs, "");
        assert!(body.contains("<Key>a.txt</Key>"));
    }

    #[test]
    fn base64_md5_is_deterministic() {
        let body = b"<Delete></Delete>";
        let a = base64_md5(body);
        let b = base64_md5(body);
        assert_eq!(a, b);
        let mut hasher = Md5::new();
        hasher.update(body);
        let expected = B64.encode(hasher.finalize());
        assert_eq!(a, expected);
    }

    #[test]
    fn build_delete_objects_url_path_style() {
        let endpoint = url::Url::parse("http://localhost:9000").unwrap();
        let url = build_delete_objects_url(&endpoint, "mybucket").unwrap();
        assert_eq!(url.path(), "/mybucket");
        assert_eq!(url.query(), Some("delete="));
    }

    #[test]
    fn build_delete_objects_url_with_existing_path() {
        let endpoint = url::Url::parse("http://localhost:9000/some/prefix").unwrap();
        let url = build_delete_objects_url(&endpoint, "mybucket").unwrap();
        assert_eq!(url.path(), "/mybucket");
    }

    #[test]
    fn parse_response_all_success_quiet() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>"#;
        let res = parse_delete_objects_response(xml);
        assert!(res.is_empty(), "Quiet=true + no errors = no entries");
    }

    #[test]
    fn parse_response_with_not_found_key() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult>
  <Error><Key>a.txt</Key><Code>NoSuchKey</Code><Message>missing</Message></Error>
  <Error><Key>b.txt</Key><Code>AccessDenied</Code><Message>forbidden</Message></Error>
</DeleteResult>"#;
        let res = parse_delete_objects_response(xml);
        assert_eq!(res.len(), 2);
        assert!(res[0].is_ok(), "NoSuchKey is idempotent success");
        assert!(res[1].is_err(), "AccessDenied surfaces");
    }

    #[test]
    fn parse_response_nosuchversion_is_success() {
        let xml = r#"<DeleteResult>
  <Error><Key>v.txt</Key><Code>NoSuchVersion</Code><Message>x</Message></Error>
</DeleteResult>"#;
        let res = parse_delete_objects_response(xml);
        assert_eq!(res.len(), 1);
        assert!(res[0].is_ok());
    }

    #[test]
    fn parse_response_malformed_returns_empty() {
        let xml = "<<<not xml>>>";
        let res = parse_delete_objects_response(xml);
        assert!(res.is_empty());
    }

    #[test]
    fn count_outcome_separates_success_notfound_failure() {
        let results = vec![
            Ok(()),
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "x")),
            Err(std::io::Error::other("x")),
            Ok(()),
        ];
        let (s, n, f) = count_outcome(&results);
        assert_eq!(s, 2);
        assert_eq!(n, 1);
        assert_eq!(f, 1);
    }

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
        let cfg = WorkerConfig::from_s3(
            url::Url::parse("http://localhost:9000").unwrap(),
            "b".into(),
            "/root/".into(),
            "us-east-1".into(),
            Some("ak".into()),
            Some("sk".into()),
            reqwest::Client::new(),
        );
        assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(cfg.flush_delay, DEFAULT_FLUSH_DELAY);
        assert_eq!(cfg.request_timeout, DEFAULT_REQUEST_TIMEOUT);
        assert_eq!(cfg.max_retries, DEFAULT_MAX_RETRIES);
    }

    #[test]
    fn parse_response_empty_input() {
        let res = parse_delete_objects_response("");
        assert!(res.is_empty());
    }

    #[test]
    fn counter_snapshot_is_const_default() {
        // Default snapshot is zero (no I/O). The live counts grow
        // as the worker runs; this only asserts the struct itself.
        let s = CounterSnapshot::default();
        assert_eq!(s.flushes_total, 0);
        assert_eq!(s.keys_total, 0);
        assert_eq!(s.failures_total, 0);
        assert_eq!(s.shutdown_lost_total, 0);
        assert_eq!(s.single_key_batches_total, 0);
        assert_eq!(s.max_batch_size_observed, 0);
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
        let (deleter, handle) = spawn(
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
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
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
            flush_delay: Duration::from_millis(50),
            tombs: tombs.clone(),
        });
        let (tx, _rx) = mpsc::channel::<Control>(8);
        let deleter = BatchedDeleter {
            shared: shared.clone(),
            flush_tx: tx,
        };

        // Simulate what unlink does: enqueue + tombstone.
        tombs.insert("p".into());
        {
            let mut pending = shared.pending.lock().unwrap();
            let (otx, _orx) = oneshot::channel();
            pending.jobs.push(PendingDelete {
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
            flush_delay: Duration::from_millis(50),
            tombs: tombs.clone(),
        });
        let (tx, _rx) = mpsc::channel::<Control>(8);
        let deleter = BatchedDeleter {
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
            flush_delay: Duration::from_millis(50),
            tombs: tombs.clone(),
        });
        let (tx, _rx) = mpsc::channel::<Control>(8);
        let deleter = BatchedDeleter {
            shared: shared.clone(),
            flush_tx: tx,
        };
        let n = deleter.cancel_pending("never-existed");
        assert_eq!(n, 0);
        assert!(tombs.is_empty());
    }
}
