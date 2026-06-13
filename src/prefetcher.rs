//! Prefetcher with backpressure — inspired by mountpoint-s3's PartQueue + BackpressureController.
//!
//! Each open file handle can have a background downloader that fills a bounded queue.
//! When the queue is full, the downloader spins (light backpressure).
//! When the reader needs data, it pops from the queue.

use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// A chunk of data downloaded from remote.
#[derive(Clone)]
pub struct Part {
    pub offset: u64,
    pub data: Bytes,
}

/// Bounded queue with spin-sleep backpressure.
pub struct PartQueue {
    parts: VecDeque<Part>,
    max_bytes: u64,
    current_bytes: u64,
    finished: bool,
    error: Option<String>,
}

impl PartQueue {
    pub fn new(max_bytes: u64) -> Self {
        Self {
            parts: VecDeque::new(),
            max_bytes,
            current_bytes: 0,
            finished: false,
            error: None,
        }
    }

    pub fn push(&mut self, part: Part) -> Result<(), String> {
        while self.current_bytes + part.data.len() as u64 > self.max_bytes {
            if self.finished {
                return Err("prefetcher finished".to_string());
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
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
        // the new head. Bounded by the inner queue size.
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
    cancelled: Arc<std::sync::atomic::AtomicBool>,
}

impl HandlePrefetcher {
    pub fn new(
        op: opendal::Operator,
        path: String,
        file_size: u64,
        max_queue_bytes: u64,
        chunk_size: u64,
    ) -> Self {
        let queue = Arc::new(Mutex::new(PartQueue::new(max_queue_bytes)));
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let q = queue.clone();
        let c = cancelled.clone();
        std::thread::spawn(move || {
            let mut offset = 0u64;
            while offset < file_size {
                if c.load(std::sync::atomic::Ordering::Relaxed) {
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
                        let mut qlock = q.lock().unwrap();
                        if qlock.push(part).is_err() {
                            break;
                        }
                        offset = end;
                    }
                    Err(e) => {
                        let mut qlock = q.lock().unwrap();
                        qlock.set_error(format!("prefetch read failed: {e}"));
                        break;
                    }
                }
            }
            q.lock().unwrap().set_finished();
        });
        Self { queue, cancelled }
    }

    /// Signal the background downloader to stop early. Used by
    /// FUSE `release()` so a partially-read file's prefetcher
    /// thread doesn't keep fetching into a queue nobody reads.
    pub fn cancel(&self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn pop(&self, offset: u64) -> Option<Part> {
        self.queue.lock().unwrap().pop(offset)
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

    #[test]
    fn test_part_queue_push_pop() {
        let mut q = PartQueue::new(1024);
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
        let mut q = PartQueue::new(1024);
        q.set_finished();
        let part = q.pop(0);
        assert!(part.is_none());
    }

    #[test]
    fn test_part_queue_discard_stale() {
        let mut q = PartQueue::new(1024);
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
        let mut q = PartQueue::new(1024);
        q.set_finished();
        let part = q.pop(0);
        assert!(part.is_none());
    }

    #[test]
    fn test_part_queue_error_returns_none() {
        let mut q = PartQueue::new(1024);
        q.set_error("test error".to_string());
        let part = q.pop(0);
        assert!(part.is_none());
    }

    #[test]
    fn test_part_queue_multiple_parts() {
        let mut q = PartQueue::new(1024);
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
}
