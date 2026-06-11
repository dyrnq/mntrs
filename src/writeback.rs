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

/// Type of task sent by FUSE threads to the writeback worker.
pub type Task = (u64, String, PathBuf);

/// The shared sender used by FUSE threads to enqueue writeback work.
pub type Sender = tokio::sync::mpsc::UnboundedSender<Task>;

/// Global counter of pending writeback tasks.
static PENDING_COUNT: AtomicU64 = AtomicU64::new(0);

/// Return number of in-flight writeback tasks (queued or uploading).
pub fn pending_count() -> usize {
    PENDING_COUNT.load(Ordering::Relaxed) as usize
}

/// Spawn the writeback worker inside the global tokio runtime.
///
/// Returns a `Sender` that is `Clone + Send`, usable from any FUSE thread.
pub fn spawn(
    op: Arc<Operator>,
    inodes: Inodes,
    delay: Duration,
) -> (Sender, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let handle = crate::rt().spawn(async move {
        let mut queue: DelayQueue<Task> = DelayQueue::new();

        loop {
            // Drain channel into queue
            while let Ok(task) = rx.try_recv() {
                PENDING_COUNT.fetch_add(1, Ordering::Relaxed);
                queue.insert_at(task, tokio::time::Instant::now() + delay);
            }

            if queue.is_empty() {
                match rx.recv().await {
                    Some(task) => {
                        PENDING_COUNT.fetch_add(1, Ordering::Relaxed);
                        queue.insert_at(task, tokio::time::Instant::now() + delay);
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
                    Err(_) => continue,
                };
                let op = op.clone();
                let remote = task.1;
                let ino = task.0;
                let cache_path = task.2;
                // Upload in a separate task so DelayQueue keeps ticking
                static UPLOAD_SEM: std::sync::LazyLock<Semaphore> =
                    std::sync::LazyLock::new(|| Semaphore::new(4));
                let permit = UPLOAD_SEM.acquire().await.unwrap();
                let inodes2 = inodes.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let mut last_err = None;
                    for attempt in 0..5 {
                        match op.write(&remote, data.clone()).await {
                            Ok(_) => {
                                // Only update mtime — do NOT update file size (v.2).
                                // The write function tracks the correct logical size
                                // via inodes. The cache file may be larger than the
                                // logical size due to set_len sparse extension, so
                                // using cache metadata length would corrupt reads.
                                inodes2.entry(ino).and_modify(|v| {
                                    v.3 = Some(std::time::SystemTime::now());
                                });
                                // Keep cache file on disk as a read cache.
                                // Only remove the .dirty sidecar to mark upload complete.
                                // The cache eviction logic handles disk space separately.
                                PENDING_COUNT.fetch_sub(1, Ordering::Relaxed);
                                let _ = std::fs::remove_file(cache_path.with_extension("dirty"));
                                return;
                            }
                            Err(e) if attempt < 4 => {
                                last_err = Some(e);
                                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                            }
                            Err(e) => {
                                last_err = Some(e);
                            }
                        }
                    }
                    PENDING_COUNT.fetch_sub(1, Ordering::Relaxed);
                    tracing::warn!(
                        path = %remote,
                        error = ?last_err,
                        "writeback upload failed after 5 retries, re-enqueueing"
                    );
                    // File stays on disk with .dirty sidecar — recovered on next mount
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
        for (_, _, cache_path) in &tasks {
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
            inodes.entry(tasks[0].0).and_modify(|v| {
                v.2 = new_size;
                v.3 = Some(std::time::SystemTime::now());
            });
            for (_, _, cache_path) in &tasks {
                let _ = std::fs::remove_file(cache_path);
                let _ = std::fs::remove_file(cache_path.with_extension("dirty"));
            }
        }
    }
}
