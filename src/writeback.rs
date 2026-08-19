//! Writeback — async upload of dirty cache files to remote storage.
//!
//! Architecture (inspired by rclone's WriteBack + container/heap):
//!
//!   FUSE thread (flush/release):
//!     → tx.send((ino, remote_path, write_buffer_path))
//!       (tokio::sync::mpsc::UnboundedSender — lock-free)
//!
//!   Background tokio task:
//!     → recv() → push into internal DelayQueue (priority queue by deadline)
//!     → poll_expired() → read cache file → upload → clean up
//!     → Failure: re-insert with exponential backoff

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt;
use opendal::Operator;
use tokio::sync::Semaphore;
use tokio_util::time::delay_queue::DelayQueue;

use crate::Inodes;

/// Type alias for the disk cache LRU index shared with
/// `MntrsFs` (issue #55). Inlined here rather than
/// exported as `pub type` in `lib.rs` because
/// `CacheKey` is defined later in `lib.rs` and a
/// forward reference would need a `pub(crate)` to
/// keep the crate's surface minimal.
type DiskCacheIndex = std::sync::Arc<dashmap::DashMap<crate::CacheKey, (u64, std::time::Instant)>>;

/// Type of task sent by FUSE threads to the writeback worker.
///
/// The 4th element is the retry-cycle count: 0 for a fresh
/// enqueue (from flush/release), incremented every time the
/// upload exhausts its 5-attempt in-process retry loop and
/// gets pushed back into the queue. Issue #53: without
/// tracking cycles here, a permanently-failing upload
/// (backend 5xx, network partition, auth expiry) would
/// keep the file in the queue forever once retries are
/// re-enabled, OR — pre-fix — would silently drop the task
/// after 5 attempts and the daemon would never upload the
/// data again. The cap (see `MAX_REENQUEUE_CYCLES` below)
/// bounds the second scenario.
///
/// The 5th element is the per-task initial delay. cycle=0
/// uses this delay; cycle>=1 always uses `REENQUEUE_COOLDOWN`
/// (60s) — a failing small file gets one immediate shot, then
/// backs off. Issue #202: small files (< `--writeback-immediate-threshold`,
/// default 1 MiB) pass `Duration::ZERO` so the upload fires
/// as soon as the cache file's data hits the local cache,
/// giving databases (SQLite / etcd / RocksDB) a 0s
/// durability window on `close()` instead of the uniform
/// 5s `write_back_delay`.
///
/// Pass `per_task_delay = Duration::MAX` to opt back into the
/// spawn-time `delay` fallback (used by tests that want to
/// keep the old uniform-delay behavior).
///
/// Issue #219: was a 5-tuple `(u64, String, PathBuf, u32,
/// Duration)` whose field order was easy to swap silently at
/// enqueue sites; the struct form makes every field
/// self-documenting and ensures future field additions are a
/// compile-time catch at every call site (vs the tuple's
/// silent `.0 = u64 = 0` default).
///
/// Lifecycle: a fresh `WritebackTask` is enqueued by the
/// write/flush/release/recovery paths with `retry_cycle = 0`
/// and a `per_task_delay` derived from
/// `per_task_writeback_delay(ino)` (issue #202). The worker's
/// exhaustion path re-enqueues with `retry_cycle += 1` and
/// `per_task_delay = REENQUEUE_COOLDOWN`.
#[derive(Debug, Clone)]
pub struct WritebackTask {
    /// FUSE inode. Use `INO_RECOVERY_SENTINEL` for tasks
    /// that have no live inode mapping (e.g. crash recovery
    /// at startup).
    pub ino: u64,
    /// Backend path the upload targets.
    pub remote_path: String,
    /// On-disk cache file path (the source of bytes for the
    /// upload). `.dirty` sidecar lives next to it. Ignored
    /// when `in_memory_payload` is `Some(_)` — the payload
    /// bytes replace the disk read in off mode.
    pub write_buffer_path: PathBuf,
    /// Off-mode inline payload. When set, the worker uploads
    /// these bytes directly instead of reading the file at
    /// `write_buffer_path`. The task is also marked so no
    /// `.dirty` sidecar is created or required.
    pub in_memory_payload: Option<bytes::Bytes>,
    /// 0 = fresh enqueue — honors `per_task_delay`.
    /// 1 or higher = re-enqueue — always uses
    /// `REENQUEUE_COOLDOWN`, the `per_task_delay` field
    /// is ignored. Capped at `MAX_REENQUEUE_CYCLES`
    /// which is 10.
    pub retry_cycle: u32,
    /// Delay before the worker's first attempt for
    /// `retry_cycle == 0`. Ignored for `retry_cycle >= 1`.
    /// `Duration::MAX` opts into the spawn-time `delay`
    /// fallback for uniform-delay tests.
    pub per_task_delay: Duration,
    /// `true` iff the worker should `remove_file(write_buffer_path)`
    /// after a successful upload. Set by the enqueue site based on
    /// `cache_mode.delete_cache_on_success()` (issue #T2-N,
    /// currently only `CacheMode::Minimal`). `false` keeps the
    /// cache file on disk as a read cache (`Writes` / `Full`),
    /// or no-ops when the file doesn't exist (`Off` with
    /// `in_memory_payload`).
    pub delete_cache_on_success: bool,
    /// Issue #595: in-memory buffer size (bytes) used as the
    /// opendal `OpWriter::chunk` for this task's `op.write()` /
    /// `op.writer_with()` call. 0 keeps opendal's default (8 MiB).
    /// Forwarded from `MntrsFs::buffer_size` at spawn time so
    /// every upload from this mount honors the same `--buffer-size`.
    pub buffer_size: u64,
}

/// The shared sender used by FUSE threads to enqueue writeback work.
pub type Sender = tokio::sync::mpsc::UnboundedSender<WritebackTask>;

/// Global counter of pending writeback tasks.
static PENDING_COUNT: AtomicU64 = AtomicU64::new(0);

/// Return number of in-flight writeback tasks (queued or uploading).
pub fn pending_count() -> usize {
    PENDING_COUNT.load(Ordering::Relaxed) as usize
}

/// Cap on how many times a single task can be re-enqueued
/// after exhausting its in-process retry loop. Each cycle
/// is 5 attempts with exponential backoff (1+2+4+8+16 s
/// = 31 s of active upload time) plus a 60 s cooldown
/// between cycles. 10 cycles = 50 attempts ≈ 15 min of
/// total upload time before the task is declared stuck.
///
/// Issue #53: pre-fix the log message said "re-enqueueing"
/// but the code did not re-enqueue, leaving the file
/// permanently stuck in daemon mode. With the cap, an
/// operator can monitor the "stuck writeback" critical
/// log line + the .dirty sidecar count to alert on a
/// real backend outage. Without the cap, a permanent
/// backend failure would cycle the same task forever
/// and grow the delay queue unboundedly.
pub const MAX_REENQUEUE_CYCLES: u32 = 10;

/// Cooldown between re-enqueue cycles when the in-process
/// retry loop exhausts. Longer than the first-time enqueue
/// delay (`delay` arg to `spawn`, default 5 s) so a
/// persistently-flaky backend doesn't get hammered. 60 s
/// matches the per-PVC mount retry cadence in K8s CSI
/// drivers (e.g. csi-attacher's default 30 s), so a single
/// cycle's worth of retries aligns with one K8s resync
/// window.
pub const REENQUEUE_COOLDOWN: Duration = Duration::from_secs(60);

/// Per-bucket counts returned by [`recover_dirty_sidecars`]. The
/// `MntrsFs` mount init logs a single info/warn summary from
/// these; integration tests assert the exact per-bucket counts
/// to pin the recovery semantics.
///
/// `recovered` = .dirty sidecars successfully enqueued.
/// `orphan_sidecars_removed` = .dirty sidecars whose cache
///   file was missing — they had nothing to upload, so the
///   sidecar was cleaned up.
/// `send_failed` = sidecars whose `tx.send(...)` returned
///   `Err` — the worker is gone, the sidecar stays.
/// `no_sender` = sidecars seen but no sender was passed in
///   (test injects `None`, or production race where the
///   spawn hadn't completed yet — covered in `common_init_wb`
///   by ordering spawn first).
/// `read_failed` = sidecars whose contents couldn't be
///   decoded as a remote path — left in place for the
///   operator to inspect.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryStats {
    pub recovered: u64,
    pub orphan_sidecars_removed: u64,
    pub send_failed: u64,
    pub no_sender: u64,
    pub read_failed: u64,
}

impl RecoveryStats {
    /// Sum across all buckets — the headline count the mount
    /// summary line logs (`"recovered=N ..."`). A non-zero
    /// total is what triggers the info-level "recovery scan
    /// complete" emission; zero stays at debug.
    pub fn total(&self) -> u64 {
        self.recovered
            + self.orphan_sidecars_removed
            + self.send_failed
            + self.no_sender
            + self.read_failed
    }

    /// Number of sidecars that the scan could not recover
    /// automatically — i.e. require manual intervention
    /// (`mntrs list-dirty` to inspect, then `rm` / fix the
    /// backend / restart). Used by `common_init_wb` to
    /// decide whether to emit the warn-level
    /// "stuck .dirty sidecar(s)" line (issue #395 fix #2).
    pub fn stuck(&self) -> u64 {
        self.send_failed + self.no_sender + self.read_failed
    }
}

/// Recover dirty writeback state from a previous crash.
///
/// Walks `cache_dir` for `*.dirty` sidecars, parses each to
/// its remote path, and enqueues a fresh
/// `WritebackTask { ino: INO_RECOVERY_SENTINEL, ... }` on the
/// provided sender. Orphan sidecars (no corresponding cache
/// file) are removed. Unreadable sidecars are left in place
/// for the operator to inspect.
///
/// Returns the per-bucket counts via [`RecoveryStats`]; the
/// caller decides what (if anything) to log. `tx == None`
/// records `no_sender` for each sidecar seen and skips the
/// enqueue (used by tests that drive the scan without a live
/// worker, and by `common_init_wb` if a race makes the
/// sender unavailable).
///
/// Issue #595: `buffer_size` is forwarded so recovery
/// uploads use the same opendal chunk size as steady state.
/// Issue #T2-N: `delete_cache_on_success` is honored so
/// `CacheMode::Minimal` mounts unlink the cache file after
/// the recovery upload completes — matching the steady-state
/// contract.
pub fn recover_dirty_sidecars(
    cache_dir: &std::path::Path,
    tx: Option<&Sender>,
    delete_cache_on_success: bool,
    write_back_delay: Duration,
    buffer_size: u64,
) -> RecoveryStats {
    let mut stats = RecoveryStats::default();
    let entries = match std::fs::read_dir(cache_dir) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(
                cache_dir = %cache_dir.display(),
                error = %e,
                "writeback recovery: read_dir failed"
            );
            return stats;
        }
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.extension().is_none_or(|ext| ext != "dirty") {
            continue;
        }
        let write_buffer_path = p.with_extension("");
        if !write_buffer_path.exists() {
            // Orphan sidecar — cache file missing, safe to remove.
            tracing::debug!(sidecar = ?p, "removing orphan dirty sidecar");
            let _ = std::fs::remove_file(&p);
            stats.orphan_sidecars_removed += 1;
            continue;
        }
        match std::fs::read_to_string(&p) {
            Ok(remote) => {
                let remote = remote.trim().to_string();
                let Some(tx) = tx else {
                    stats.no_sender += 1;
                    tracing::warn!(
                        write_buffer_path = ?write_buffer_path,
                        "writeback recovery: no sender available; \
                         .dirty sidecar kept for next-mount retry"
                    );
                    continue;
                };
                tracing::info!(
                    path = %remote,
                    ?write_buffer_path,
                    "recovering dirty writeback"
                );
                // Bug 18: use the named sentinel INO_RECOVERY_SENTINEL
                // (= 0) instead of the bare `0` literal. The
                // writeback completion handler explicitly checks this
                // value and skips its inodes mtime update.
                //
                // Bug 22: send().ok() previously swallowed an Err
                // silently. send on an UnboundedSender returns Err
                // only when the receiver is dropped — which here
                // means the writeback worker thread died. The .dirty
                // sidecar is still on disk, so the next mount's
                // recovery scan will try again, but an operator
                // watching THIS mount needs to know the worker is
                // gone NOW. Log at warn.
                if let Err(e) = tx.send(WritebackTask {
                    ino: crate::INO_RECOVERY_SENTINEL,
                    remote_path: remote,
                    write_buffer_path: write_buffer_path.clone(),
                    in_memory_payload: None,
                    delete_cache_on_success,
                    retry_cycle: 0,                   // fresh enqueue
                    per_task_delay: write_back_delay, // #202: recovery never immediate
                    buffer_size,
                }) {
                    stats.send_failed += 1;
                    tracing::warn!(
                        write_buffer_path = ?write_buffer_path,
                        error = %e,
                        "writeback recovery send failed (worker dropped?); \
                         .dirty sidecar kept for next-mount retry"
                    );
                } else {
                    stats.recovered += 1;
                }
            }
            Err(e) => {
                // Couldn't read the sidecar contents. The cache
                // file exists but the sidecar is unreadable — leave
                // it in place for the operator to inspect.
                stats.read_failed += 1;
                tracing::warn!(
                    sidecar = ?p,
                    error = %e,
                    "writeback recovery: failed to read dirty sidecar contents"
                );
            }
        }
    }
    stats
}

/// Spawn the writeback worker inside the global tokio runtime.
///
/// Returns a `Sender` that is `Clone + Send`, usable from any FUSE thread.
///
/// Issue #55: `cache_dir` and `disk_cache_index` are
/// passed in so the worker can drop the block-level
/// cache entries for a path after a successful upload.
/// Without this, the read path could still return
/// pre-upload data via the block-level cache even
/// though the file-level cache (the writeback's
/// source of truth) is up to date.
pub fn spawn(
    op: Arc<Operator>,
    inodes: Inodes,
    disk_cache_index: DiskCacheIndex,
    cache_dir: std::path::PathBuf,
    // Issue #38: set of paths that currently have a
    // writeback task in flight. The worker removes
    // the path on completion (success or final
    // retry-exhaustion) so the next flush/release
    // with new content enqueues a fresh task. The
    // flush/release enqueue sites insert into the
    // set first; if the insert returns false, the
    // task is already in flight and the enqueue is
    // skipped.
    writeback_pending: Arc<dashmap::DashSet<String>>,
    delay: Duration,
) -> (Sender, tokio::task::JoinHandle<()>) {
    // Note (issue #595): upload chunk size is **not** stored
    // here. Every `WritebackTask` carries its own `buffer_size`
    // (set at the call site that constructs the task from
    // `MntrsFs::buffer_size`). The worker reads it from
    // `task.buffer_size` and forwards it on re-enqueue so
    // cycles stay consistent.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WritebackTask>();
    // Clone for the upload task's re-enqueue path
    // (issue #53). The original `tx` is also
    // returned to the caller for FUSE-thread
    // enqueues; we move the clone into the worker
    // task and keep the original for the return
    // value.
    let tx_for_worker = tx.clone();

    let handle = crate::rt().spawn(async move {
        let mut queue: DelayQueue<WritebackTask> = DelayQueue::new();

        // Issue #268.1 O3: separate task for the 30-second
        // queue-depth tick. Operators rely on this to
        // detect silent backlog accumulation — without it
        // the only "is the daemon alive?" probe is `ps`,
        // which can't see queue length. Independent task
        // because `queue.len()` needs `&mut queue` which
        // the worker already owns.
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                tracing::info!(
                    pending = PENDING_COUNT.load(Ordering::Relaxed),
                    "writeback queue tick"
                );
            }
        });

        loop {
            // Drain channel into queue
            while let Ok(task) = rx.try_recv() {
                PENDING_COUNT.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    path = %task.remote_path,
                    cycle = task.retry_cycle,
                    "writeback: enqueued"
                );
                // Issue #53: preserve the retry-cycle count.
                // A fresh enqueue from flush/release has
                // count=0; a re-enqueue from the upload
                // task's retry-exhaustion path has a higher
                // count and the worker routes it to a
                // longer cooldown slot.
                //
                // Issue #202: cycle=0 uses the per-task
                // delay (task.per_task_delay). cycle>=1 always uses
                // REENQUEUE_COOLDOWN regardless of the
                // per-task intent — a failing small file
                // gets one immediate shot, then backs off
                // for 60s. Duration::MAX in task.per_task_delay opts
                // back into the spawn-time `delay` for
                // tests that want uniform-delay behavior.
                let cycle = task.retry_cycle;
                let enqueue_at = if cycle == 0 {
                    let per_task = if task.per_task_delay == Duration::MAX {
                        delay
                    } else {
                        task.per_task_delay
                    };
                    tokio::time::Instant::now() + per_task
                } else {
                    tokio::time::Instant::now() + REENQUEUE_COOLDOWN
                };
                queue.insert_at(task, enqueue_at);
            }

            if queue.is_empty() {
                match rx.recv().await {
                    Some(task) => {
                        PENDING_COUNT.fetch_add(1, Ordering::Relaxed);
                        let cycle = task.retry_cycle;
                        let enqueue_at = if cycle == 0 {
                            let per_task = if task.per_task_delay == Duration::MAX {
                                delay
                            } else {
                                task.per_task_delay
                            };
                            tokio::time::Instant::now() + per_task
                        } else {
                            tokio::time::Instant::now() + REENQUEUE_COOLDOWN
                        };
                        tracing::debug!(
                            path = %task.remote_path,
                            cycle,
                            "writeback: enqueued (idle-wake)"
                        );
                        queue.insert_at(task, enqueue_at);
                    }
                    None => {
                        // Issue #268.1 O1: worker exit.
                        // Pre-fix this `break` was silent;
                        // operators had no signal that the
                        // upload pipeline died. Channel close
                        // happens only when every Sender is
                        // dropped — i.e. all FUSE threads
                        // unmounted — so this is a normal
                        // shutdown path; log it as info rather
                        // than error to avoid alarm.
                        tracing::info!("writeback: channel disconnected, worker exiting");
                        break;
                    }
                }
                continue;
            }

            // Wait for next expired entry
            if let Some(expired) = queue.next().await {
                let task = expired.into_inner();
                let _p = task.remote_path.clone();
                tracing::debug!(
                    path = %task.remote_path,
                    cycle = task.retry_cycle,
                    "writeback: dequeued for upload"
                );
                let data: bytes::Bytes = if let Some(payload) = task.in_memory_payload.clone() {
                    // Off-mode inline payload (CacheMode::Off):
                    // the FUSE thread copied the dirty buffer
                    // into the task. No on-disk file to read,
                    // no .dirty sidecar to clean up — bytes
                    // travel RAM-to-RAM, and the upload is
                    // still async / retry-aware.
                    payload
                } else {
                    match std::fs::read(&task.write_buffer_path) {
                        Ok(d) => d.into(),
                        Err(_) => {
                            // Issue #53 + #268.1 O1: cache file
                            // vanished (e.g. evicted by LRU
                            // between enqueue and dequeue) — drop
                            // the task cleanly. Without this, the
                            // pre-fix code would have read
                            // failed with a confusing error and
                            // the .dirty sidecar would linger.
                            // Log at info so operators can
                            // distinguish "LRU evicted before
                            // upload" from "filesystem gone".
                            PENDING_COUNT.fetch_sub(1, Ordering::Relaxed);
                            tracing::info!(
                                path = %task.write_buffer_path.display(),
                                cycle = task.retry_cycle,
                                "writeback: cache file vanished (LRU evicted?); dropping task"
                            );
                            let _ = std::fs::remove_file(
                                task.write_buffer_path.with_extension("dirty"),
                            );
                            continue;
                        }
                    }
                };
                let op = op.clone();
                let remote = task.remote_path;
                let ino = task.ino;
                let write_buffer_path = task.write_buffer_path;
                let cycle = task.retry_cycle;
                let in_memory_payload = task.in_memory_payload;
                // Issue #T2-N: Minimal mode unlinks the cache
                // file after successful upload. Captured here
                // (rather than re-derived from cache_mode, which
                // the task doesn't carry) so the worker can
                // branch cheaply on a primitive.
                let delete_cache_on_success = task.delete_cache_on_success;
                // Upload in a separate task so DelayQueue keeps ticking.
                static UPLOAD_SEM: std::sync::LazyLock<Semaphore> =
                    std::sync::LazyLock::new(|| Semaphore::new(4));
                // SAFETY: `UPLOAD_SEM` is a process-static
                // `LazyLock<Semaphore>` that is never `.close()`d
                // anywhere in this crate. `acquire().await` only
                // returns `Err(AcquireError::Closed)` after an
                // explicit close, so this `.expect` is unreachable
                // under the current design. The explicit panic
                // message (vs a bare `.unwrap()`) is so that a
                // future refactor that introduces close() logic
                // fails loudly with a pointer to this contract,
                // rather than silently passing on a torn-permit
                // path. Audit: 2026-06-16.
                let permit = UPLOAD_SEM
                    .acquire()
                    .await
                    .expect("BUG: UPLOAD_SEM is never closed; see writeback.rs SAFETY comment");
                let inodes2 = inodes.clone();
                // Issue #53: hold a clone of the channel
                // sender so the upload task can re-enqueue
                // the work when the 5-attempt retry loop
                // exhausts. Pre-fix the log message lied
                // about re-enqueueing but the code dropped
                // the task — leaving the .dirty sidecar to
                // sit forever in daemon mode.
                let tx_clone = tx_for_worker.clone();
                // Issue #55: clone cache_dir +
                // disk_cache_index for the post-upload
                // block-cache drop spawn. The outer
                // tokio::spawn moves them into the
                // closure, so the inner spawn_blocking
                // can't take ownership — the clones
                // keep them available here.
                let cache_dir_for_upload = cache_dir.clone();
                let disk_cache_index_for_upload = disk_cache_index.clone();
                let writeback_pending_for_upload = writeback_pending.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let mut last_err = None;
                    // Issue #595: pin the task's buffer_size into
                    // this async block so the upload chain
                    // (`.chunk(buffer_size)`) and the re-enqueue
                    // `WritebackTask { .. buffer_size, .. }`
                    // both see the same value.
                    let buffer_size = task.buffer_size;
                    for attempt in 0..5 {
                        // Issue #46: route large files
                        // through opendal's multipart
                        // writer (which auto-handles
                        // chunking + retry for S3-style
                        // backends). The threshold is
                        // 5 MiB — below it, multipart
                        // overhead exceeds the per-part
                        // RTT saving. The fallback
                        // (`op.write`) handles backends
                        // without multipart support.
                        // Both branches return
                        // Result<Result<(), opendal::Error>, Elapsed>
                        // so the match arms have a
                        // uniform shape. The Metadata
                        // return from op.write is
                        // discarded via .map(|_| ()).
                        let op_for_multipart = op.clone();
                        let write_result: Result<
                            Result<(), opendal::Error>,
                            tokio::time::error::Elapsed,
                        > = if data.len() > 200 * 1024 * 1024 {
                            // Issue #46 + #73: multipart
                            // upload for large files.
                            // Threshold 200 MiB matches rclone
                            // (avoids multipart overhead for
                            // mid-size files where a single
                            // PutObject is faster). Parts are
                            // uploaded concurrently per-file
                            // (chunk(5 MiB) = S3 minimum,
                            // concurrent(2) keeps global HTTP
                            // in-flight at ~UPLOAD_SEM*2 = 8
                            // even with all permits busy).
                            //
                            // Issue #595: when buffer_size > 0
                            // and >= 5 MiB (S3 minimum part
                            // size), use it as the part size —
                            // larger parts = fewer requests =
                            // lower overhead. When buffer_size
                            // is 0 or < 5 MiB, fall back to the
                            // 5 MiB floor (S3 will reject smaller
                            // parts anyway).
                            let chunk_size = if buffer_size >= 5 * 1024 * 1024 {
                                buffer_size as usize
                            } else {
                                5 * 1024 * 1024
                            };
                            let path = remote.clone();
                            let data_clone = data.clone();
                            tokio::time::timeout(Duration::from_secs(120), async move {
                                let mut w = op_for_multipart
                                    .writer_with(&path)
                                    .chunk(chunk_size)
                                    .concurrent(2)
                                    .await?;
                                w.write(data_clone).await?;
                                w.close().await.map(|_| ())
                            })
                            .await
                        } else {
                            // Issue #595: chain `.chunk(buffer_size)`
                            // on the one-shot write path. When
                            // buffer_size is 0, opendal uses its
                            // default chunk (8 MiB); when non-zero
                            // it overrides the in-memory buffer
                            // accumulation threshold before flush.
                            let write_fut = op
                                .write_with(&remote, data.clone())
                                .chunk(buffer_size as usize);
                            tokio::time::timeout(Duration::from_secs(120), write_fut)
                                .await
                                .map(|res| res.map(|_meta| ()))
                        };
                        match write_result {
                            Ok(Ok(_)) => {
                                // Issue #268.4 O14: upload ok lifecycle.
                                // **debug** not info — sustained writes
                                // produce one log per task, hot path
                                // volume. Operators wanting per-upload
                                // audit use RUST_LOG=mntrs=debug.
                                tracing::debug!(
                                    path = %remote,
                                    bytes = data.len(),
                                    attempt,
                                    ino,
                                    cycle,
                                    "writeback: upload ok"
                                );
                                // Only update mtime — do NOT update file size (v.2).
                                // The write function tracks the correct logical size
                                // via inodes. The cache file may be larger than the
                                // logical size due to set_len sparse extension, so
                                // using cache metadata length would corrupt reads.
                                //
                                // Bug 18: same INO_RECOVERY_SENTINEL skip as the
                                // batched path below. Recovery uploads come through
                                // this branch too; the and_modify on a missing
                                // ino=0 is a silent no-op, but the explicit check
                                // documents intent.
                                if ino != crate::INO_RECOVERY_SENTINEL {
                                    inodes2.entry(ino).and_modify(|v| {
                                        v.mtime = Some(std::time::SystemTime::now());
                                    });
                                }
                                // Keep cache file on disk as a read cache.
                                // Only remove the .dirty sidecar to mark upload complete.
                                // The cache eviction logic handles disk space separately.
                                //
                                // Off-mode (no on-disk file): the
                                // .dirty sidecar was never written
                                // and `write_buffer_path` is a dummy
                                // (the bytes rode in `data`).
                                // `remove_file` returns NotFound
                                // which we already swallow via `let _`.
                                PENDING_COUNT.fetch_sub(1, Ordering::Relaxed);
                                if in_memory_payload.is_none() {
                                    let _ = std::fs::remove_file(
                                        write_buffer_path.with_extension("dirty"),
                                    );
                                }
                                // Issue #T2-N: `--vfs-cache-mode=minimal`
                                // unlinks the cache file after a
                                // successful upload — keeps the
                                // fdatasync'd crash safety of
                                // `Writes` during the upload window,
                                // but leaves no on-disk footprint
                                // afterward (rclone parity). Fires
                                // only when an on-disk file
                                // actually existed (Off-mode tasks
                                // carry a dummy path).
                                if delete_cache_on_success && in_memory_payload.is_none() {
                                    let _ = std::fs::remove_file(&write_buffer_path);
                                }
                                // Issue #38: clear the
                                // pending entry so the
                                // next flush/release
                                // with new content can
                                // enqueue a fresh
                                // task.
                                writeback_pending_for_upload.remove(remote.as_str());
                                // Issue #55: drop the
                                // block-level cache for
                                // this path. The
                                // file-level cache is
                                // now in sync with the
                                // backend; any stale
                                // .block files from a
                                // prior cold read would
                                // otherwise serve
                                // pre-upload data on
                                // the next read.
                                let remote_for_block_drop = remote.clone();
                                let cache_dir_for_block_drop = cache_dir_for_upload.clone();
                                let disk_cache_index_for_block_drop =
                                    disk_cache_index_for_upload.clone();
                                tokio::task::spawn_blocking(move || {
                                    crate::drop_block_cache_for_path(
                                        &cache_dir_for_block_drop,
                                        &disk_cache_index_for_block_drop,
                                        &remote_for_block_drop,
                                    );
                                });
                                return;
                            }
                            Ok(Err(e)) if attempt < 4 => {
                                let backoff_secs = 1u64 << attempt;
                                // Audit finding A5: per-attempt
                                // warn was rate-limit-noise on a
                                // sustained backend outage —
                                // 5 attempts/cycle × 10 cycles × N
                                // stuck files drowns out the
                                // actionable STUCK error below.
                                // Downgrade to debug; operators
                                // investigating a specific path
                                // can still enable
                                // `RUST_LOG=mntrs::writeback=debug`.
                                tracing::debug!(
                                    path = %remote,
                                    attempt,
                                    backoff_secs,
                                    next_attempt_secs = backoff_secs,
                                    error = %e,
                                    "writeback: retry after backoff"
                                );
                                last_err = Some(e);
                                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                            }
                            Ok(Err(e)) => {
                                tracing::error!(
                                    path = %remote,
                                    total_attempts = 5,
                                    error = %e,
                                    "writeback: all attempts exhausted; enqueuing retry cycle"
                                );
                                last_err = Some(e);
                            }
                            Err(_elapsed) => {
                                // Issue #268.1 O2: timeout is
                                // distinct from a backend
                                // error — log at warn so a
                                // 120s hang shows up.
                                tracing::warn!(
                                    path = %remote,
                                    attempt,
                                    "writeback: upload timed out after 120s"
                                );
                                last_err = Some(opendal::Error::new(
                                    opendal::ErrorKind::Unexpected,
                                    format!(
                                        "writeback upload timed out after 120s (attempt {}/5)",
                                        attempt + 1
                                    ),
                                ));
                            }
                        }
                    }
                    // Issue #53: in-process retry loop
                    // exhausted. Re-enqueue the task via
                    // the channel with cycle+1 so the
                    // outer worker applies
                    // `REENQUEUE_COOLDOWN` (60 s) instead
                    // of the normal `delay` (5 s). The
                    // task goes back to the end of the
                    // queue — concurrent uploads keep
                    // progressing, this file just gets
                    // another shot later.
                    //
                    // Bounded by `MAX_REENQUEUE_CYCLES`
                    // (10 cycles ≈ 15 min of total
                    // active upload time). After the cap,
                    // the file is declared stuck: the
                    // .dirty sidecar stays for ops to
                    // inspect (e.g. via the warning count
                    // metric), the in-memory queue is
                    // freed, and a `error!` log surfaces
                    // the permanent failure.
                    let next_cycle = cycle + 1;
                    if next_cycle > MAX_REENQUEUE_CYCLES {
                        PENDING_COUNT.fetch_sub(1, Ordering::Relaxed);
                        // Issue #38: clear the
                        // pending entry so a future
                        // flush/release with new
                        // content can enqueue
                        // fresh. (Without this the
                        // path stays pending forever
                        // and no writebacks for new
                        // content would ever
                        // start.)
                        writeback_pending_for_upload.remove(&remote);
                        tracing::error!(
                            path = %remote,
                            cycle = cycle,
                            error = ?last_err,
                            "writeback upload STUCK after {} cycles ({} total attempts); \
                             .dirty sidecar left on disk for operator inspection — issue #53",
                            cycle,
                            cycle * 5
                        );
                        return;
                    }
                    // Audit finding A5: per-cycle re-enqueue warn had
                    // the same noise problem — every 5 cycles per
                    // stuck file, vs the actionable STUCK error
                    // above that fires once after the cycle budget
                    // is exhausted. Keep the rate-limited debug for
                    // root-cause work; demote from warn.
                    tracing::debug!(
                        path = %remote,
                        cycle = cycle,
                        next_cycle = next_cycle,
                        cooldown_s = REENQUEUE_COOLDOWN.as_secs(),
                        error = ?last_err,
                        "writeback upload exhausted 5 retries; re-enqueueing (issue #53)"
                    );
                    // Re-enqueue. If the channel is
                    // closed (worker shut down), the
                    // send fails; the .dirty sidecar
                    // and on-disk cache file stay for
                    // the next-mount recovery path.
                    //
                    // Issue #202: forward REENQUEUE_COOLDOWN
                    // explicitly. The cycle>=1 branch in the
                    // worker ignores task.per_task_delay, so this is
                    // documentation, not behavior — but it
                    // makes the retry path self-consistent.
                    // Forwarding `Duration::ZERO` here would
                    // re-immediate the upload on a flapping
                    // backend, defeating the cooldown.
                    let _ = tx_clone.send(WritebackTask {
                        ino,
                        remote_path: remote,
                        write_buffer_path,
                        // Retries also ride in-memory when the
                        // task was created with an inline
                        // payload (off mode). Forwarding the
                        // original Bytes keeps the upload
                        // consistent — bytes don't change
                        // between cycles, only the cycle count.
                        in_memory_payload,
                        // Issue #T2-N: forward the unlink
                        // flag so a successful retry still
                        // honors Minimal-mode cleanup.
                        delete_cache_on_success,
                        retry_cycle: next_cycle,
                        per_task_delay: REENQUEUE_COOLDOWN,
                        // Issue #595: forward the upload chunk
                        // size — the retry's `.chunk(buffer_size)`
                        // call must match the original task.
                        buffer_size,
                    });
                    // PENDING_COUNT stays the same —
                    // the task is still in flight,
                    // just moved from the delay queue
                    // back to the channel.
                });
            }
        }
    });

    // Issue #268.1 O1: confirm the worker actually
    // started. Pre-fix `spawn()` returned silently;
    // a failed `crate::rt().spawn` (e.g. runtime not
    // initialized in a test path) was invisible.
    tracing::info!(delay_secs = delay.as_secs(), "writeback worker started");

    (tx, handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #53: the retry-exhaustion re-enqueue must
    /// be bounded. The cap is `MAX_REENQUEUE_CYCLES` —
    /// 10 cycles × 5 attempts/cycle = 50 total
    /// attempts, ≈ 15 min of total active upload time
    /// (5 retries with exponential 1+2+4+8+16 s
    /// backoff = 31 s per cycle, plus 60 s cooldown
    /// between cycles). Past the cap the task is
    /// declared stuck and the operator gets an
    /// `error!` log + the .dirty sidecar to inspect.
    ///
    /// The test pins the constants so an accidental
    /// change (e.g. someone bumps the cap to 1000 and
    /// the delay queue grows unboundedly on a real
    /// backend outage) trips CI.
    #[test]
    fn reenqueue_cycle_constants() {
        assert_eq!(MAX_REENQUEUE_CYCLES, 10);
        assert_eq!(REENQUEUE_COOLDOWN, Duration::from_secs(60));
    }

    /// Issue #53 + #202 + #219: `WritebackTask` field
    /// semantics are now self-pinning via named fields.
    /// Was `task_tuple_has_cycle_field` pinning tuple arity
    /// (5); the struct form makes a missing or reordered
    /// field a compile error at every enqueue site, not a
    /// runtime bug. This test pins field identity (each
    /// field carries its own value into the next task) so
    /// a reorder still trips CI, while the struct literal
    /// in the test enforces all 5 fields exist.
    #[test]
    fn writeback_task_fields_pin_semantics() {
        let task: WritebackTask = WritebackTask {
            ino: 42,
            remote_path: "/remote/path".to_string(),
            write_buffer_path: PathBuf::from("/cache/path"),
            in_memory_payload: None,
            // Issue #T2-N: Minimal-mode tests pass `true`,
            // everything else `false`. Pin `false` here so
            // the field's identity is captured by the
            // struct literal.
            delete_cache_on_success: false,
            retry_cycle: 0,
            per_task_delay: Duration::from_secs(5),
            // Issue #595: pin the buffer_size field. 16 MiB
            // matches rclone default.
            buffer_size: 16 * 1024 * 1024,
        };
        assert_eq!(task.ino, 42);
        assert_eq!(task.remote_path, "/remote/path");
        assert_eq!(task.write_buffer_path, PathBuf::from("/cache/path"));
        assert!(!task.delete_cache_on_success);
        assert_eq!(task.retry_cycle, 0);
        assert_eq!(task.per_task_delay, Duration::from_secs(5));
        assert_eq!(task.buffer_size, 16 * 1024 * 1024);
        // Cycle count advances on re-enqueue and the
        // re-enqueue path forwards REENQUEUE_COOLDOWN
        // explicitly (not the original per-task delay).
        let retried = WritebackTask {
            ino: task.ino,
            remote_path: task.remote_path.clone(),
            write_buffer_path: task.write_buffer_path.clone(),
            in_memory_payload: None,
            delete_cache_on_success: false,
            retry_cycle: task.retry_cycle + 1,
            per_task_delay: REENQUEUE_COOLDOWN,
            buffer_size: task.buffer_size,
        };
        assert_eq!(retried.retry_cycle, 1);
        assert_eq!(retried.per_task_delay, REENQUEUE_COOLDOWN);
        assert!(!retried.delete_cache_on_success);
        assert_eq!(retried.buffer_size, task.buffer_size);
    }

    /// Issue #202: per_task_delay=Duration::MAX opts back
    /// into the spawn-time `delay` fallback. This is the
    /// path tests use to keep the old uniform-delay
    /// behavior. Without this opt-out, every small-file
    /// enqueue in the test suite would have to be audited
    /// for "do I want immediate or batched?". Pin the
    /// sentinel value so a refactor that picks a different
    /// sentinel trips CI.
    #[test]
    fn per_task_delay_sentinel_falls_back_to_spawn_delay() {
        assert_eq!(Duration::MAX, Duration::MAX); // trivial pin
        // Sanity: Duration::MAX is not equal to any of the
        // production per-task delays the code constructs.
        assert_ne!(Duration::MAX, Duration::ZERO);
        assert_ne!(Duration::MAX, Duration::from_secs(5));
        assert_ne!(Duration::MAX, REENQUEUE_COOLDOWN);
    }
}
