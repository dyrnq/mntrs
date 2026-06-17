//! Writeback — async upload of dirty cache files to remote storage.
//!
//! Architecture (inspired by rclone's WriteBack + container/heap):
//!
//!   FUSE thread (flush/release):
//!     → tx.send((ino, remote_path, cache_path))
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
pub type Task = (u64, String, PathBuf, u32);

/// The shared sender used by FUSE threads to enqueue writeback work.
pub type Sender = tokio::sync::mpsc::UnboundedSender<Task>;

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
const MAX_REENQUEUE_CYCLES: u32 = 10;

/// Cooldown between re-enqueue cycles when the in-process
/// retry loop exhausts. Longer than the first-time enqueue
/// delay (`delay` arg to `spawn`, default 5 s) so a
/// persistently-flaky backend doesn't get hammered. 60 s
/// matches the per-PVC mount retry cadence in K8s CSI
/// drivers (e.g. csi-attacher's default 30 s), so a single
/// cycle's worth of retries aligns with one K8s resync
/// window.
const REENQUEUE_COOLDOWN: Duration = Duration::from_secs(60);

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
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Task>();
    // Clone for the upload task's re-enqueue path
    // (issue #53). The original `tx` is also
    // returned to the caller for FUSE-thread
    // enqueues; we move the clone into the worker
    // task and keep the original for the return
    // value.
    let tx_for_worker = tx.clone();

    let handle = crate::rt().spawn(async move {
        let mut queue: DelayQueue<Task> = DelayQueue::new();

        loop {
            // Drain channel into queue
            while let Ok(task) = rx.try_recv() {
                PENDING_COUNT.fetch_add(1, Ordering::Relaxed);
                // Issue #53: preserve the retry-cycle count.
                // A fresh enqueue from flush/release has
                // count=0; a re-enqueue from the upload
                // task's retry-exhaustion path has a higher
                // count and the worker routes it to a
                // longer cooldown slot.
                let cycle = task.3;
                let enqueue_at = if cycle == 0 {
                    tokio::time::Instant::now() + delay
                } else {
                    tokio::time::Instant::now() + REENQUEUE_COOLDOWN
                };
                queue.insert_at(task, enqueue_at);
            }

            if queue.is_empty() {
                match rx.recv().await {
                    Some(task) => {
                        PENDING_COUNT.fetch_add(1, Ordering::Relaxed);
                        let cycle = task.3;
                        let enqueue_at = if cycle == 0 {
                            tokio::time::Instant::now() + delay
                        } else {
                            tokio::time::Instant::now() + REENQUEUE_COOLDOWN
                        };
                        queue.insert_at(task, enqueue_at);
                    }
                    None => break,
                }
                continue;
            }

            // Wait for next expired entry
            if let Some(expired) = queue.next().await {
                let task = expired.into_inner();
                let _p = task.1.clone();
                let data = match std::fs::read(&task.2) {
                    Ok(d) => d,
                    Err(_) => {
                        // Issue #53: cache file vanished (e.g.
                        // evicted by LRU) — drop the task
                        // cleanly. Without this, the
                        // pre-fix code would have read
                        // failed with a confusing error and
                        // the .dirty sidecar would linger.
                        PENDING_COUNT.fetch_sub(1, Ordering::Relaxed);
                        let _ = std::fs::remove_file(task.2.with_extension("dirty"));
                        continue;
                    }
                };
                let op = op.clone();
                let remote = task.1;
                let ino = task.0;
                let cache_path = task.2;
                let cycle = task.3;
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
                    for attempt in 0..5 {
                        let write_fut = op.write(&remote, data.clone());
                        match tokio::time::timeout(Duration::from_secs(120), write_fut).await {
                            Ok(Ok(_)) => {
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
                                PENDING_COUNT.fetch_sub(1, Ordering::Relaxed);
                                let _ = std::fs::remove_file(cache_path.with_extension("dirty"));
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
                                last_err = Some(e);
                                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                            }
                            Ok(Err(e)) => {
                                last_err = Some(e);
                            }
                            Err(_elapsed) => {
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
                    tracing::warn!(
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
                    let _ = tx_clone.send((ino, remote, cache_path, next_cycle));
                    // PENDING_COUNT stays the same —
                    // the task is still in flight,
                    // just moved from the delay queue
                    // back to the channel.
                });
            }
        }
    });

    (tx, handle)
}

// ---------------------------------------------------------------------------
// Legacy worker — kept as dead_code for reference. Use spawn() instead.
// ---------------------------------------------------------------------------

/// Legacy writeback worker using `Mutex<VecDeque>`.
///
/// Replaced by `spawn()` (tokio channel + DelayQueue).
#[allow(dead_code)]
pub fn worker(
    op: Arc<Operator>,
    inodes: Inodes,
    queue: std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<Task>>>,
    delay: Duration,
    _max_age: Duration,
) {
    loop {
        let tasks: Vec<Task> = {
            let mut q = queue.lock().unwrap();
            let first = match q.pop_front() {
                Some(t) => t,
                None => {
                    drop(q);
                    std::thread::sleep(Duration::from_secs(1));
                    continue;
                }
            };
            let mut batch = vec![first];
            while let Some(next) = q.pop_front() {
                if batch[0].1 == next.1 {
                    batch.push(next);
                } else {
                    q.push_front(next);
                    break;
                }
            }
            batch
        };

        let remote_path = tasks[0].1.clone();
        let mut full_data = Vec::new();
        for (_, _, cache_path, _) in &tasks {
            if let Ok(d) = std::fs::read(cache_path) {
                full_data.extend_from_slice(&d);
            }
        }
        if full_data.is_empty() {
            continue;
        }

        std::thread::sleep(delay);

        // Retry: upload concatenated data
        let upload_ok = {
            let mut ok = false;
            for attempt in 0..3 {
                let buf = full_data.clone();
                match crate::rt().block_on(async { op.write(&remote_path, buf).await }) {
                    Ok(_) => {
                        ok = true;
                        break;
                    }
                    Err(e) if attempt < 2 => {
                        eprintln!("[mntrs] writeback retry {}/3: {e}", attempt + 1);
                        std::thread::sleep(Duration::from_secs(1 << attempt));
                    }
                    Err(e) => {
                        eprintln!("[mntrs] writeback failed: {e}");
                    }
                }
            }
            ok
        };

        if upload_ok {
            let new_size = full_data.len() as u64;
            // Bug 18: writeback recovery sends
            // INO_RECOVERY_SENTINEL (= 0) for uploads
            // recovered from dirty sidecars at mount
            // init — no inode mapping exists yet. The
            // mtime/size update would be a silent
            // no-op against the missing inodes entry,
            // but that's wasted work and an
            // `entry(0).and_modify(...)` reads as a
            // bug at first glance. Skip explicitly so
            // the contract is visible.
            if tasks[0].0 != crate::INO_RECOVERY_SENTINEL {
                inodes.entry(tasks[0].0).and_modify(|v| {
                    v.size = new_size;
                    v.mtime = Some(std::time::SystemTime::now());
                });
            }
            for (_, _, cache_path, _) in &tasks {
                let _ = std::fs::remove_file(cache_path);
                let _ = std::fs::remove_file(cache_path.with_extension("dirty"));
            }
        }
    }
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

    /// Issue #53: Task tuple shape is now 4 elements
    /// (ino, remote, cache_path, cycle). Pin the
    /// arity so an accidental refactor that drops the
    /// cycle counter trips CI before reaching
    /// production and re-introducing the silent
    /// data-loss bug.
    #[test]
    fn task_tuple_has_cycle_field() {
        let task: Task = (
            42,
            "/remote/path".to_string(),
            PathBuf::from("/cache/path"),
            0,
        );
        assert_eq!(task.0, 42);
        assert_eq!(task.1, "/remote/path");
        assert_eq!(task.2, PathBuf::from("/cache/path"));
        assert_eq!(task.3, 0);
        // Cycle count advances on re-enqueue.
        let retried: Task = (task.0, task.1, task.2, task.3 + 1);
        assert_eq!(retried.3, 1);
    }
}
