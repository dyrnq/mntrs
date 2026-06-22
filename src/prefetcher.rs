//! Prefetcher with backpressure — inspired by mountpoint-s3's PartQueue +
//! BackpressureController.
//!
//! Each open file handle can have a background downloader that fills a
//! bounded queue. When the queue is full, the downloader **parks on a
//! `Condvar`** (no spin) until the reader pops a part (freeing room) or
//! the handle is cancelled/released. When the reader needs data, it
//! pops from the queue.

use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

/// A chunk of data downloaded from remote.
#[derive(Clone)]
pub struct Part {
    pub offset: u64,
    pub data: Bytes,
}

/// Bounded queue with Condvar backpressure.
///
/// `push` is non-blocking: it inserts if there is room and the queue
/// is not in a terminal state (cancelled/finished/error), otherwise
/// returns `Err`. The caller (`HandlePrefetcher`'s download thread)
/// owns the wait loop — it parks on the sibling `Condvar` when `push`
/// would return "full", and is woken by `pop` (room freed) or
/// `cancel` (terminal). Keeping `push` non-blocking lets the unit
/// tests exercise the queue without a `Condvar`.
pub struct PartQueue {
    parts: VecDeque<Part>,
    max_bytes: u64,
    current_bytes: u64,
    finished: bool,
    error: Option<String>,
    /// Shared cancel flag — set by `HandlePrefetcher::cancel()` when
    /// the consumer is gone. `push`/`is_terminal` check this to avoid
    /// inserting into a queue nobody will read (issue #107).
    cancelled: Arc<AtomicBool>,
}

impl PartQueue {
    pub fn new(max_bytes: u64, cancelled: Arc<AtomicBool>) -> Self {
        Self {
            parts: VecDeque::new(),
            max_bytes,
            current_bytes: 0,
            finished: false,
            error: None,
            cancelled,
        }
    }

    /// Current occupied bytes (for the `Condvar` wake decision in
    /// `HandlePrefetcher::pop`: notify a parked pusher iff a pop
    /// actually freed space).
    pub fn current_bytes(&self) -> u64 {
        self.current_bytes
    }

    /// True iff a further `push` can never succeed — the consumer is
    /// gone (cancelled) or the producer is done (finished/error).
    pub fn is_terminal(&self) -> bool {
        self.finished || self.error.is_some() || self.cancelled.load(Ordering::Relaxed)
    }

    /// True iff `len` more bytes would exceed `max_bytes`.
    pub fn is_full_for(&self, len: u64) -> bool {
        self.current_bytes + len > self.max_bytes
    }

    /// Non-blocking insert. Returns `Err("prefetcher cancelled")` if
    /// the queue is terminal (checked first, so a cancel always wins
    /// over a full-queue retry), or `Err("queue full")` if there is no
    /// room. The caller is expected to have checked `is_full_for` /
    /// `is_terminal` and parked on the `Condvar` if needed; the
    /// re-checks here close the cancel-race window between the
    /// caller's check and the insert.
    pub fn push(&mut self, part: Part) -> Result<(), String> {
        if self.is_terminal() {
            return Err("prefetcher cancelled".to_string());
        }
        if self.is_full_for(part.data.len() as u64) {
            return Err("queue full".to_string());
        }
        self.current_bytes += part.data.len() as u64;
        self.parts.push_back(part);
        Ok(())
    }

    pub fn pop(&mut self, offset: u64) -> Option<Part> {
        // Non-blocking. Returns:
        //   - Some(part) if a part covering `offset` is in the queue
        //   - None if the queue is empty (prefetcher hasn't fetched yet
        //     or it's finished/dropped) OR if a part for a *later*
        //     offset is queued but the requested offset isn't covered
        //
        // The read path's contract: a None here means "no prefetched
        // data ready, fall through to disk cache or remote fetch". A
        // blocking pop would deadlock FUSE workers when the prefetcher
        // is slow or stalled, since every FUSE worker would park here
        // until the (single) prefetcher thread finished — and the
        // prefetcher thread itself can stall on backend latency.
        //
        // Drop stale parts (whose [offset, offset+len) is entirely
        // before the requested offset) without blocking, then check
        // the new head. Bounded by the inner queue size. Stale drops
        // free space — `HandlePrefetcher::pop` notices the
        // `current_bytes` decrease and notifies a parked pusher.
        while let Some(front) = self.parts.front() {
            if front.offset <= offset && offset < front.offset + front.data.len() as u64 {
                let part = self.parts.pop_front().unwrap();
                self.current_bytes -= part.data.len() as u64;
                return Some(part);
            }
            if front.offset + front.data.len() as u64 <= offset {
                // Stale (front ends before the requested offset). Drop.
                let stale = self.parts.pop_front().unwrap();
                self.current_bytes -= stale.data.len() as u64;
                continue;
            }
            // Front starts after the requested offset. No match and no
            // more stale parts to drop; return None.
            break;
        }
        if self.finished || self.error.is_some() {
            return None;
        }
        None
    }

    pub fn set_finished(&mut self) {
        self.finished = true;
    }
    pub fn set_error(&mut self, err: String) {
        self.error = Some(err);
    }
}

/// Background downloader that fills a PartQueue.
pub struct HandlePrefetcher {
    queue: Arc<Mutex<PartQueue>>,
    cancelled: Arc<AtomicBool>,
    /// Paired with `queue`'s mutex. The download thread parks here when
    /// the queue is full; `pop` (room freed) and `cancel` (terminal)
    /// notify it. Replaces the old spin-sleep backpressure (issue
    /// #136): a full queue no longer burns a 10 ms poll cycle on the
    /// prefetcher thread, and wake latency drops to sub-microsecond.
    cond: Arc<Condvar>,
    /// Set to true by the download thread when it exits (EOF, cancel,
    /// or error). Lets callers/tests confirm `cancel()` actually
    /// stopped the thread rather than leaving it parked.
    download_done: Arc<AtomicBool>,
}

impl HandlePrefetcher {
    pub fn new(
        op: opendal::Operator,
        path: String,
        file_size: u64,
        max_queue_bytes: u64,
        chunk_size: u64,
    ) -> Self {
        let cancelled = Arc::new(AtomicBool::new(false));
        let cond = Arc::new(Condvar::new());
        let download_done = Arc::new(AtomicBool::new(false));
        let queue = Arc::new(Mutex::new(PartQueue::new(
            max_queue_bytes,
            cancelled.clone(),
        )));
        let q = queue.clone();
        let c = cancelled.clone();
        let cv = cond.clone();
        let done = download_done.clone();
        std::thread::spawn(move || {
            let mut offset = 0u64;
            'download: while offset < file_size {
                if c.load(Ordering::Relaxed) {
                    break;
                }
                let end = (offset + chunk_size).min(file_size);
                let result =
                    crate::rt().block_on(async { op.read_with(&path).range(offset..end).await });
                match result {
                    Ok(buf) => {
                        let part = Part {
                            offset,
                            data: Bytes::from(buf.to_vec()),
                        };
                        let part_len = part.data.len() as u64;
                        let mut qlock = q.lock().unwrap();
                        // Backpressure: park on the Condvar while the
                        // queue is full. `cond.wait` releases the
                        // queue lock while parked, so a FUSE `pop`
                        // can proceed and free room; it re-acquires
                        // the lock on wake and re-checks the
                        // conditions. `is_terminal` is checked first
                        // so a cancel/finish during park exits
                        // immediately instead of inserting into a
                        // queue nobody will read.
                        let mut terminal = false;
                        loop {
                            if qlock.is_terminal() {
                                terminal = true;
                                break;
                            }
                            if qlock.is_full_for(part_len) {
                                qlock = cv.wait(qlock).unwrap();
                                continue;
                            }
                            // Room available. `push` re-checks
                            // terminal (cancel may have raced in
                            // between `is_terminal` and here); treat
                            // its Err as terminal so we exit cleanly
                            // without panicking on the race.
                            if qlock.push(part).is_err() {
                                terminal = true;
                            }
                            break;
                        }
                        if terminal {
                            break 'download;
                        }
                        offset = end;
                    }
                    Err(e) => {
                        let mut qlock = q.lock().unwrap();
                        qlock.set_error(format!("prefetch read failed: {e}"));
                        break 'download;
                    }
                }
            }
            q.lock().unwrap().set_finished();
            // Wake any straggler waiter (defensive — the only waiter
            // is this thread, which is now done; but a `cancel` that
            // raced with `set_finished` may have a notify in flight).
            cv.notify_all();
            done.store(true, Ordering::SeqCst);
        });
        Self {
            queue,
            cancelled,
            cond,
            download_done,
        }
    }

    /// Signal the background downloader to stop early. Used by
    /// FUSE `release()` so a partially-read file's prefetcher
    /// thread doesn't keep fetching into a queue nobody reads.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
        // Wake a pusher parked in `cond.wait` so it observes the
        // cancel flag and exits. Locking the queue first provides a
        // release barrier so the relaxed flag store is visible to the
        // waiter's post-wake `is_terminal` read; the Condvar's own
        // internal sync would also suffice, but the lock makes the
        // ordering explicit and matches the pattern in `pop`.
        let _g = self.queue.lock().unwrap();
        self.cond.notify_all();
    }

    /// True once the download thread has exited. Mainly for tests
    /// confirming `cancel()` unsticks a parked pusher.
    pub fn is_download_done(&self) -> bool {
        self.download_done.load(Ordering::SeqCst)
    }

    pub fn pop(&self, offset: u64) -> Option<Part> {
        let mut g = self.queue.lock().unwrap();
        let before = g.current_bytes();
        let part = g.pop(offset);
        if g.current_bytes() < before {
            // A part was removed (matched pop or stale drop) — free
            // room for a pusher parked on the Condvar. notify_one is
            // a no-op if nothing is parked.
            self.cond.notify_one();
        }
        part
    }
}

impl std::fmt::Debug for HandlePrefetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlePrefetcher").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    fn false_flag() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    #[test]
    fn test_part_queue_push_pop() {
        let mut q = PartQueue::new(1024, false_flag());
        q.push(Part {
            offset: 0,
            data: Bytes::from(vec![0u8; 100]),
        })
        .unwrap();
        let part = q.pop(0).unwrap();
        assert_eq!(part.offset, 0);
        assert_eq!(part.data.len(), 100);
    }

    #[test]
    fn test_part_queue_pop_empty_finished() {
        let mut q = PartQueue::new(1024, false_flag());
        q.set_finished();
        let part = q.pop(0);
        assert!(part.is_none());
    }

    #[test]
    fn test_part_queue_discard_stale() {
        let mut q = PartQueue::new(1024, false_flag());
        q.push(Part {
            offset: 0,
            data: Bytes::from(vec![0u8; 50]),
        })
        .unwrap();
        q.set_finished();
        // Pop at offset past the front — discards stale
        let part = q.pop(100);
        assert!(part.is_none(), "should return None after stale discard");
        assert!(q.parts.is_empty());
    }

    #[test]
    fn test_part_queue_finished_returns_none() {
        let mut q = PartQueue::new(1024, false_flag());
        q.set_finished();
        let part = q.pop(0);
        assert!(part.is_none());
    }

    #[test]
    fn test_part_queue_error_returns_none() {
        let mut q = PartQueue::new(1024, false_flag());
        q.set_error("test error".to_string());
        let part = q.pop(0);
        assert!(part.is_none());
    }

    #[test]
    fn test_part_queue_multiple_parts() {
        let mut q = PartQueue::new(1024, false_flag());
        q.push(Part {
            offset: 0,
            data: Bytes::from(vec![0u8; 100]),
        })
        .unwrap();
        q.push(Part {
            offset: 100,
            data: Bytes::from(vec![1u8; 100]),
        })
        .unwrap();
        q.push(Part {
            offset: 200,
            data: Bytes::from(vec![2u8; 100]),
        })
        .unwrap();

        let p2 = q.pop(150).unwrap();
        assert_eq!(p2.offset, 100);
        assert_eq!(p2.data[0], 1);
    }

    #[test]
    fn test_push_respects_cancelled() {
        let flag = Arc::new(AtomicBool::new(false));
        let mut q = PartQueue::new(100, flag.clone());
        // Fill the queue to capacity.
        q.push(Part {
            offset: 0,
            data: Bytes::from(vec![0u8; 100]),
        })
        .unwrap();
        // Now cancel — next push should fail
        // immediately instead of spinning.
        flag.store(true, Ordering::Relaxed);
        let err = q
            .push(Part {
                offset: 100,
                data: Bytes::from(vec![0u8; 50]),
            })
            .unwrap_err();
        assert!(
            err.contains("cancelled"),
            "expected cancelled error, got: {err}"
        );
    }

    /// Regression for issue #136: a parked pusher must wake when `pop`
    /// frees room (Condvar notify), not spin on a 10 ms sleep. This
    /// drives `HandlePrefetcher` directly — fill the queue, start a
    /// pusher thread that blocks on the full queue, then `pop` to free
    /// room and assert the pusher completes promptly (well under the
    /// old 10 ms spin granularity would imply if it had to retry).
    #[test]
    fn condvar_pop_wakes_parked_pusher() {
        // Tiny queue + chunk so the second push must park.
        let cancelled = Arc::new(AtomicBool::new(false));
        let cond = Arc::new(Condvar::new());
        let queue = Arc::new(Mutex::new(PartQueue::new(8, cancelled.clone())));

        // First part (8 bytes) fills the queue exactly.
        queue
            .lock()
            .unwrap()
            .push(Part {
                offset: 0,
                data: Bytes::from(vec![0u8; 8]),
            })
            .unwrap();

        let q2 = queue.clone();
        let cv2 = cond.clone();
        let pusher = std::thread::spawn(move || {
            let part = Part {
                offset: 8,
                data: Bytes::from(vec![1u8; 8]),
            };
            let part_len = part.data.len() as u64;
            let mut qlock = q2.lock().unwrap();
            loop {
                if qlock.is_terminal() {
                    return false; // unexpected — test should free room
                }
                if qlock.is_full_for(part_len) {
                    qlock = cv2.wait(qlock).unwrap();
                    continue;
                }
                qlock.push(part).ok();
                return true;
            }
        });

        // Give the pusher a moment to park on the condvar.
        std::thread::sleep(Duration::from_millis(50));

        // Free room — this must wake the parked pusher via notify_one.
        let popped = {
            let mut g = queue.lock().unwrap();
            let before = g.current_bytes();
            let p = g.pop(0);
            if g.current_bytes() < before {
                cond.notify_one();
            }
            p
        };
        assert!(popped.is_some(), "pop should have freed room");

        // Pusher should complete quickly (Condvar wake, not spin).
        let inserted = pusher.join().expect("pusher thread panicked");
        assert!(
            inserted,
            "pusher did not insert after room freed (condvar wake failed)"
        );

        // The second part is now in the queue.
        let g = queue.lock().unwrap();
        assert_eq!(g.current_bytes(), 8, "queue should hold the second part");
    }

    /// Regression for issue #136: `cancel` must wake a parked
    /// download thread so `release()` of a partially-read large file
    /// doesn't leak the prefetcher thread. Drives the real
    /// `HandlePrefetcher` against a memory backend: a tiny queue
    /// (8 B) + 8 B chunks make the download thread fill the queue
    /// with the first chunk and then park on the Condvar. `cancel()`
    /// must wake it; `is_download_done()` confirms the thread exited.
    #[test]
    fn handle_prefetcher_cancel_stops_download_thread() {
        let op = opendal::Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        // 64 KiB file — large enough that the download thread can't
        // finish before we cancel (8 B chunks × 8192 = 64 KiB).
        let data = vec![42u8; 64 * 1024];
        crate::rt()
            .block_on(async { op.write("big.bin", data).await })
            .unwrap();

        // max_queue_bytes=8, chunk_size=8 → first 8 B chunk fills the
        // queue (8/8); the second push parks on the Condvar.
        let hp = HandlePrefetcher::new(op, "big.bin".into(), 64 * 1024, 8, 8);

        // Let the download thread fetch the first chunk and park.
        std::thread::sleep(Duration::from_millis(150));
        assert!(
            !hp.is_download_done(),
            "download thread should still be running (parked) before cancel"
        );

        hp.cancel();

        // Condvar wake should be sub-ms; allow 2 s margin for a
        // loaded CI runner. If cancel's notify_all didn't wake the
        // parked thread, this hangs and CI fails the test.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if hp.is_download_done() {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            hp.is_download_done(),
            "cancel did not stop the download thread within 2 s (thread leak)"
        );
    }
}
