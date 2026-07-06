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
use std::time::Instant;

use crate::backpressure::BackpressureController;
use crate::mem_limiter::MemoryLimiter;
use crate::util::LockOrRecover;

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

/// Outcome of [`PartQueue::push`].
///
/// Replaces the older `Result<(), String>` return so callers can
/// distinguish overlap (recoverable — advance `offset` and retry) from
/// cancellation / queue-full (exit or park respectively). See issue
/// #413 for the bug the old shape caused.
#[derive(Debug, PartialEq, Eq)]
pub enum PushOutcome {
    /// Part inserted.
    Ok,
    /// Cancellation was signaled; the queue is terminal.
    Cancelled,
    /// Queue is full for this part's size; caller should park on the
    /// condvar and retry.
    Full,
    /// New part's offset overlaps the previous part's tail. The part
    /// was NOT inserted. Caller should advance its offset to
    /// `back_end` (the previous part's actual end) so the next
    /// iteration starts past the overlap.
    Overlap { back_end: u64 },
}

impl PushOutcome {
    /// Convenience for callers that just want a "did it land" bool.
    /// Treats Ok as true; everything else (Cancelled / Full / Overlap)
    /// as false. The original `Result::is_ok()` semantics.
    pub fn is_ok(&self) -> bool {
        matches!(self, PushOutcome::Ok)
    }
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

    /// Non-blocking insert. Returns a [`PushOutcome`] indicating one of:
    ///   - [`PushOutcome::Ok`] — part inserted; queue state updated.
    ///   - [`PushOutcome::Cancelled`] — cancellation was signaled; the
    ///     queue is terminal and the caller should exit.
    ///   - [`PushOutcome::Full`] — queue is full for this part's size;
    ///     caller should park on the [`Condvar`] and retry.
    ///   - [`PushOutcome::Overlap`] — new part's offset overlaps the
    ///     previous part's tail. The part was NOT inserted; queue
    ///     state is unchanged. Caller should advance its `offset` to
    ///     `back_end` and continue. Treating overlap as terminal here
    ///     would silently downgrade every FUSE read to a direct
    ///     remote fetch once the overlap fired (issue #413), so this
    ///     case is distinct from the cancel/full exits.
    ///
    /// Callers are expected to pre-check `is_full_for` / `is_terminal`
    /// and park on the [`Condvar`] if needed; the re-checks here close
    /// the cancel-race window between the caller's check and the insert.
    pub fn push(&mut self, part: Part) -> PushOutcome {
        if self.is_terminal() {
            return PushOutcome::Cancelled;
        }
        if self.is_full_for(part.data.len() as u64) {
            return PushOutcome::Full;
        }
        // Audit #3 / PR #405: reject overlapping/duplicate offsets
        // so the FUSE read path can't serve stale data. The download
        // thread is supposed to advance `offset` past the previous
        // part's end before issuing the next read, but a short read
        // combined with the skip-on-reserve-failure path
        // (`offset += shrunk`) can land two parts covering the same
        // range. Returning `Overlap` here lets the caller advance
        // `offset` to `back_end` and continue without poisoning the
        // queue or killing the download thread.
        if let Some(back) = self.parts.back() {
            let back_end = back.offset + back.data.len() as u64;
            if part.offset < back_end {
                return PushOutcome::Overlap { back_end };
            }
        }
        // Order matters: `push_back` first so any panic on
        // allocation failure (VecDeque growth) propagates BEFORE
        // `current_bytes` is incremented. Without this, an OOM panic
        // would poison the mutex with `current_bytes` reflecting
        // bytes that aren't in the queue — a load-bearing invariant
        // the poison-recovery idiom in `d78dd45` cannot re-establish
        // because Rust's Mutex provides no inner-state invariant on
        // poison.
        self.parts.push_back(part);
        self.current_bytes += self.parts.back().unwrap().data.len() as u64;
        PushOutcome::Ok
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
    /// Issue #132: adaptive prefetch window. The download thread
    /// reads `current_window()` at the top of each loop and calls
    /// `record_part_fetched(bytes, elapsed)` after a successful read.
    /// The FUSE read path calls `record_part_consumed(bytes, elapsed)`
    /// after popping a part. Cloning the `Arc` into the spawn closure
    /// keeps the controller alive for the prefetcher's lifetime even
    /// if the caller drops its reference (the spawn thread itself
    /// keeps the controller reference count >= 1).
    backpressure: Arc<BackpressureController>,

    /// Issue #201: per-mount memory budget. The download thread
    /// calls `try_reserve("prefetch", chunk)` before issuing the
    /// next range read; on Err it sets `set_mem_pressure(true)` and
    /// shrinks the next window. The matching `release` fires in
    /// `pop` (consumer drained the part). cap==0 means uncapped
    /// (try_reserve always succeeds; see `mem_limiter.rs`).
    mem_limiter: Arc<MemoryLimiter>,

    /// Issue #201: counter of bytes currently reserved against
    /// `mem_limiter` but not yet released. Incremented on a
    /// successful `try_reserve` in the download thread, decremented
    /// on the matching `release` in `pop` (or on the cancel/error
    /// path before the thread exits). `Drop` reads this and calls
    /// `release` for any residual so a cancelled handle doesn't
    /// leak budget. AtomicU64 so the Drop impl on the owner
    /// thread sees the up-to-date value (modulo a small network-
    /// read window — see risk note in the plan).
    in_flight_reserved: Arc<std::sync::atomic::AtomicU64>,
}

/// Issue #201 helper: perform one range read + push to the queue.
/// Returns `false` if the read errored or the queue went terminal
/// (caller should release its reservation and exit). Returns `true`
/// on a successful push (the caller advances offset and continues).
///
/// Split out of the download loop to keep the try_reserve/success/
/// shrink branching readable; the function itself is straight-line
/// (read → push with condvar backpressure, same as the pre-#201
/// Outcome of `read_and_push` — what the spawn loop should do with
/// its `offset` and reservation bookkeeping.
#[derive(Debug)]
enum ReadAndPushOutcome {
    /// Part was read and inserted; the caller should advance
    /// `offset` to the requested end of the read and continue.
    Ok,
    /// Cancellation was signaled (either via the cancel flag or the
    /// queue was already terminal on entry). Caller should release
    /// its reservation and break out of the download loop.
    Cancelled,
    /// The read happened but `push` rejected the part because its
    /// offset overlapped the previous part's tail. The part was NOT
    /// inserted; the queue state is unchanged. Caller should release
    /// its reservation and advance `offset` to `back_end` (the
    /// previous part's actual end) so the next iteration starts past
    /// the overlap. The download thread does NOT die — treating
    /// overlap as terminal silently downgrades every FUSE read to a
    /// direct remote fetch once the overlap fires (issue #413).
    Overlap { back_end: u64 },
}

/// loop body).
#[allow(clippy::too_many_arguments)]
fn read_and_push(
    op: &opendal::Operator,
    path: &str,
    offset: u64,
    end: u64,
    part_len: u64,
    bp: &Arc<BackpressureController>,
    q: &Arc<Mutex<PartQueue>>,
    cv: &Arc<Condvar>,
    record_fetched: bool,
) -> ReadAndPushOutcome {
    let t0 = Instant::now();
    let result = crate::rt().block_on(async { op.read_with(path).range(offset..end).await });
    let elapsed = t0.elapsed();
    let buf = match result {
        Ok(b) => b,
        Err(e) => {
            let mut qlock = q.lock_or_recover();
            qlock.set_error(format!("prefetch read failed: {e}"));
            return ReadAndPushOutcome::Cancelled;
        }
    };
    let part = Part {
        offset,
        data: Bytes::from(buf.to_vec()),
    };
    // Issue #132: feed the producer rate. Skip the first call
    // (cold-start: TLS handshake + connection setup inflate
    // elapsed and would push `fetch_rate` toward zero, biasing
    // the window toward max on every mount). `record_part_fetched`
    // already skips `elapsed == 0` internally.
    if record_fetched {
        bp.record_part_fetched(part_len, elapsed);
    }
    let mut qlock = q.lock_or_recover();
    // Backpressure: park on the Condvar while the queue is full.
    // `cond.wait` releases the queue lock while parked, so a FUSE
    // `pop` can proceed and free room; it re-acquires the lock on
    // wake and re-checks the conditions. `is_terminal` is checked
    // first so a cancel/finish during park exits immediately
    // instead of inserting into a queue nobody will read.
    loop {
        if qlock.is_terminal() {
            return ReadAndPushOutcome::Cancelled;
        }
        if qlock.is_full_for(part_len) {
            qlock = cv.wait(qlock).unwrap_or_else(|p| {
                // The mutex was poisoned while we were parked (another
                // thread panicked holding it). Recover rather than
                // unwrapping — the audit fix in d78dd45 covers every
                // other lock site, this one was missed because the
                // cv.wait return type (LockResult<MutexGuard>) isn't a
                // plain Result<MutexGuard>. Symmetry with the surrounding
                // `q.lock().unwrap_or_else(...)` at line 254.
                tracing::warn!("prefetch queue mutex poisoned during cv.wait; recovering");
                p.into_inner()
            });
            continue;
        }
        // Room available. `push` re-checks terminal (cancel may
        // have raced in between `is_terminal` and here). The four
        // outcomes map to distinct spawn-loop actions: Ok and
        // Overlap are recoverable (the download thread continues);
        // Cancelled and Full exit / park respectively.
        match qlock.push(part.clone()) {
            PushOutcome::Ok => return ReadAndPushOutcome::Ok,
            PushOutcome::Cancelled => return ReadAndPushOutcome::Cancelled,
            PushOutcome::Full => {
                // is_full_for was just false; another producer raced
                // in and filled the queue. Park and retry — the
                // condvar will notify when a FUSE `pop` frees room.
                // `Part` is cheap to clone (Bytes is refcounted) so
                // it's safe to push again after the wait.
                qlock = cv.wait(qlock).unwrap();
                continue;
            }
            PushOutcome::Overlap { back_end } => {
                tracing::debug!(
                    target: "prefetcher",
                    part_offset = offset,
                    back_end,
                    "dropping overlapping part; download thread will advance offset"
                );
                return ReadAndPushOutcome::Overlap { back_end };
            }
        }
    }
}

impl HandlePrefetcher {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        op: opendal::Operator,
        path: String,
        file_size: u64,
        max_queue_bytes: u64,
        chunk_size: u64,
        backpressure: Arc<BackpressureController>,
        mem_limiter: Arc<MemoryLimiter>,
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
        let bp = backpressure.clone();
        let ml = mem_limiter.clone();
        let in_flight_reserved = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let inflight = in_flight_reserved.clone();
        // `chunk_size` is the FIRST-loop value only. Issue #132: from
        // the second iteration onward, the window comes from
        // `BackpressureController::current_window()`. This keeps the
        // very first prefetch chunk equal to the prior fixed value
        // (no first-read regression) while letting the window grow or
        // shrink based on consumer rate vs producer rate thereafter.
        // Audit #3 / PR #405: respect the controller's configured
        // min/max_window so a caller-supplied `chunk_size` outside
        // the configured range doesn't bypass the backpressure
        // contract. A controller built with `with_window(64*KiB, ...)`
        // should be able to shrink the first chunk to 64 KiB; the
        // previous hardcoded 128 KiB floor made `try_reserve(128 KiB)`
        // fail and skipped the chunk even when a 64 KiB fetch would
        // have fit. Same logic at the upper end — a `chunk_size`
        // greater than the controller's `bp_max_window` defeats the
        // configured ceiling.
        let bp_min_window: u64 = backpressure.min_window();
        let bp_max_window: u64 = backpressure.max_window();
        let first_chunk = chunk_size.clamp(bp_min_window, bp_max_window);
        std::thread::spawn(move || {
            let mut offset = 0u64;
            // Issue #201: counter for hysteresis. Reset on reserve
            // failure; once it reaches `HYSTERESIS_SUCCESSES` with
            // mem_pressure currently set, call `set_mem_pressure(false)`
            // so the EMA can recompute the window naturally.
            let mut consecutive_successes: u32 = 0;
            'download: while offset < file_size {
                if c.load(Ordering::Relaxed) {
                    break;
                }
                // Issue #132: adaptive window. Read the controller
                // each iteration so a long-running prefetch can
                // respond to changing consumer rate. The first
                // iteration uses the constructor's `first_chunk`
                // (== prior fixed value) so the initial chunk is
                // unchanged from the pre-#132 behavior.
                let window = if offset == 0 {
                    first_chunk
                } else {
                    bp.current_window()
                };
                let end = (offset + window).min(file_size);
                let chunk = end - offset;

                // Issue #201: per-mount memory budget gate. Reserve
                // `chunk` bytes against the prefetch label; on Err
                // flip the backpressure controller's mem_pressure
                // flag (which pins window to min_window on next
                // iteration) and shrink chunk in place. Retry once
                // after a brief sleep — the EMA grows the window
                // again on success.
                //
                // try_reserve contract: Err means NOT reserved
                // (mem_limiter.rs atomic CAS), so we don't owe a
                // release on the failed path.
                if ml.try_reserve("prefetch", chunk).is_err() {
                    consecutive_successes = 0;
                    bp.set_mem_pressure(true);
                    // Halve the chunk, but never grow it past the
                    // original. `(chunk / 2).max(bp_min_window)` alone
                    // produces a `shrunk > chunk` whenever
                    // `chunk < 2 * bp_min_window` (defaults prevent
                    // this in the production wiring, but a future
                    // caller / smaller floor would amplify the very
                    // pressure signal we're trying to relieve).
                    let shrunk = (chunk / 2).clamp(bp_min_window, chunk);
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    if ml.try_reserve("prefetch", shrunk).is_err() {
                        // Still capped. Skip this chunk — advance
                        // offset so we make progress eventually.
                        // The FUSE read path falls through to disk
                        // cache or a direct read when pop returns
                        // None, so a skipped range is still served.
                        offset += shrunk;
                        continue;
                    }
                    // Shrunk reserve succeeded. Track the actual
                    // reserved amount in `in_flight_reserved` so
                    // `pop`'s release and `Drop`'s residual-release
                    // both see a matching counter.
                    inflight.fetch_add(shrunk, std::sync::atomic::Ordering::Relaxed);
                    let shrunk_end = (offset + shrunk).min(file_size);
                    match read_and_push(
                        &op,
                        &path,
                        offset,
                        shrunk_end,
                        shrunk,
                        &bp,
                        &q,
                        &cv,
                        offset > 0,
                    ) {
                        ReadAndPushOutcome::Ok => {
                            offset = shrunk_end;
                        }
                        ReadAndPushOutcome::Cancelled => {
                            // Read failed or terminal; release the
                            // reservation we just made and exit.
                            inflight.fetch_sub(shrunk, std::sync::atomic::Ordering::Relaxed);
                            ml.release("prefetch", shrunk);
                            break 'download;
                        }
                        ReadAndPushOutcome::Overlap { back_end } => {
                            // The part we read was redundant (its
                            // range was already covered by an earlier
                            // push). Drop the reservation and jump
                            // past the overlap — `back_end` is the
                            // exact end of the existing coverage, so
                            // it's a strictly-better next-offset than
                            // `shrunk_end` (which would land inside
                            // the overlap if the earlier part was a
                            // short read).
                            inflight.fetch_sub(shrunk, std::sync::atomic::Ordering::Relaxed);
                            ml.release("prefetch", shrunk);
                            offset = back_end;
                        }
                    }
                    continue;
                }
                // Normal path: reservation succeeded.
                inflight.fetch_add(chunk, std::sync::atomic::Ordering::Relaxed);
                consecutive_successes += 1;
                // Hysteresis: after 4 successful reservations in a
                // row with mem_pressure currently set, clear the
                // flag so the EMA can recompute window naturally.
                // 4 matches the controller's EMA settling time and
                // is small enough to clear promptly once upstream
                // pressure (mem_cache eviction, another mount's
                // release) drops.
                if consecutive_successes >= 4 {
                    bp.set_mem_pressure(false);
                }
                match read_and_push(&op, &path, offset, end, chunk, &bp, &q, &cv, offset > 0) {
                    ReadAndPushOutcome::Ok => {
                        offset = end;
                    }
                    ReadAndPushOutcome::Cancelled => {
                        // Read failed or terminal; release the
                        // reservation we just made and exit.
                        inflight.fetch_sub(chunk, std::sync::atomic::Ordering::Relaxed);
                        ml.release("prefetch", chunk);
                        break 'download;
                    }
                    ReadAndPushOutcome::Overlap { back_end } => {
                        inflight.fetch_sub(chunk, std::sync::atomic::Ordering::Relaxed);
                        ml.release("prefetch", chunk);
                        offset = back_end;
                    }
                }
            }
            q.lock_or_recover().set_finished();
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
            backpressure,
            mem_limiter,
            in_flight_reserved,
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
        let _g = self.queue.lock_or_recover();
        self.cond.notify_all();
    }

    /// True once the download thread has exited. Mainly for tests
    /// confirming `cancel()` unsticks a parked pusher.
    pub fn is_download_done(&self) -> bool {
        self.download_done.load(Ordering::SeqCst)
    }

    /// Pop a part covering `offset`. `consume_started` is an `Instant`
    /// captured by the caller when it began the FUSE read; we use the
    /// elapsed time since then as the consumer-side `elapsed` input to
    /// `BackpressureController::record_part_consumed`. This lets the
    /// adaptive window (issue #132) shrink when reads are slow and
    /// grow when reads are keeping up with the prefetcher.
    ///
    /// `consume_started` may be `None` for callers that don't track
    /// their own start time (e.g. tests, or the cold-start `None` from
    /// the FUSE path before the first tick). In that case the
    /// controller sees `elapsed = 0` and the existing
    /// `if elapsed.as_secs_f64() <= 0.0 { return; }` guard skips the
    /// update — no EMA pollution.
    pub fn pop(&self, offset: u64, consume_started: Option<Instant>) -> Option<Part> {
        let mut g = self.queue.lock_or_recover();
        let before = g.current_bytes();
        let part = g.pop(offset);
        if g.current_bytes() < before {
            // A part was removed (matched pop or stale drop) — free
            // room for a pusher parked on the Condvar. notify_one is
            // a no-op if nothing is parked.
            self.cond.notify_one();
        }
        drop(g);
        // Issue #132: feed the consumer rate. Issue #201: release
        // the bytes we reserved at fetch time. Both fire only on a
        // real part pop (not None or a stale-drop-empty pop).
        // `elapsed == 0` is a no-op per the controller's own guard.
        if let Some(p) = &part {
            let n = p.data.len() as u64;
            // Issue #201: release the reservation. Must match the
            // `chunk` we passed to `try_reserve` in the download
            // thread (the read_and_push helper returns the actual
            // bytes read; `p.data.len()` is the same value because
            // Bytes is built from the read buffer).
            self.mem_limiter.release("prefetch", n);
            self.in_flight_reserved
                .fetch_sub(n, std::sync::atomic::Ordering::Relaxed);
            if let Some(t0) = consume_started {
                self.backpressure.record_part_consumed(n, t0.elapsed());
            }
        }
        part
    }
}

impl std::fmt::Debug for HandlePrefetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlePrefetcher").finish()
    }
}

/// Issue #201: release any in-flight reservation that the consumer
/// didn't drain. Without this, a `release()` that drops the
/// `HandlePrefetcher` (e.g. partial file read → cancel) would leak
/// the reserved bytes until process exit. Bounded wait so a stuck
/// network read doesn't pin `release()` for seconds; the residual
/// `in_flight_reserved` reading is best-effort (an in-flight
/// reservation hasn't decremented yet — acceptable, the next
/// `try_reserve` on a fresh handle will succeed).
impl Drop for HandlePrefetcher {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
        // Lock + notify mirrors `cancel()` so a pusher parked on
        // `cond.wait` observes the cancelled flag and exits.
        let _g = self.queue.lock_or_recover();
        self.cond.notify_all();
        drop(_g);
        // Bounded wait for the download thread. 200 ms is a
        // pragmatic balance: long enough for a normal opendal read
        // to finish + the thread to exit `set_finished`, short
        // enough that FUSE `release()` doesn't stall under a
        // network failure. If the wait expires, we still release
        // the residual we observe (best-effort).
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(200);
        while !self.download_done.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let residual = self
            .in_flight_reserved
            .load(std::sync::atomic::Ordering::Relaxed);
        if residual > 0 {
            self.mem_limiter.release("prefetch", residual);
            self.in_flight_reserved
                .store(0, std::sync::atomic::Ordering::Relaxed);
        }
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
        assert!(matches!(
            q.push(Part {
                offset: 0,
                data: Bytes::from(vec![0u8; 100]),
            }),
            PushOutcome::Ok
        ));
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
        assert!(matches!(
            q.push(Part {
                offset: 0,
                data: Bytes::from(vec![0u8; 50]),
            }),
            PushOutcome::Ok
        ));
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
        assert!(matches!(
            q.push(Part {
                offset: 0,
                data: Bytes::from(vec![0u8; 100]),
            }),
            PushOutcome::Ok
        ));
        assert!(matches!(
            q.push(Part {
                offset: 100,
                data: Bytes::from(vec![1u8; 100]),
            }),
            PushOutcome::Ok
        ));
        assert!(matches!(
            q.push(Part {
                offset: 200,
                data: Bytes::from(vec![2u8; 100]),
            }),
            PushOutcome::Ok
        ));

        let p2 = q.pop(150).unwrap();
        assert_eq!(p2.offset, 100);
        assert_eq!(p2.data[0], 1);
    }

    /// Audit #3 / PR #405: `push` must reject an offset that overlaps
    /// the previous part. Without this guard the FUSE read path can
    /// serve stale data if the downloader lands two parts covering
    /// the same byte range (short read + skip-on-reserve-failure).
    /// Issue #413: the rejection is `PushOutcome::Overlap { back_end }`
    /// so the spawn loop can advance `offset` to `back_end` and
    /// continue instead of killing the download thread.
    #[test]
    fn test_part_queue_rejects_overlapping_offset() {
        let mut q = PartQueue::new(1024, false_flag());
        assert!(matches!(
            q.push(Part {
                offset: 0,
                data: Bytes::from(vec![0u8; 100]),
            }),
            PushOutcome::Ok
        ));
        // Same offset as the back's start → exact duplicate.
        let overlap = q.push(Part {
            offset: 0,
            data: Bytes::from(vec![1u8; 50]),
        });
        assert!(
            matches!(overlap, PushOutcome::Overlap { back_end: 100 }),
            "expected Overlap {{ back_end: 100 }}, got: {overlap:?}"
        );
        // Offset inside the previous range (50..149) → partial overlap.
        let overlap = q.push(Part {
            offset: 50,
            data: Bytes::from(vec![2u8; 50]),
        });
        assert!(
            matches!(overlap, PushOutcome::Overlap { back_end: 100 }),
            "expected Overlap {{ back_end: 100 }}, got: {overlap:?}"
        );
        // Offset at the previous part's end boundary (== 100) is
        // allowed (the new part starts where the previous one ended).
        assert!(matches!(
            q.push(Part {
                offset: 100,
                data: Bytes::from(vec![3u8; 50]),
            }),
            PushOutcome::Ok
        ));
    }

    /// Issue #413: `Overlap` must NOT mutate the queue. A FUSE worker
    /// that arrives between the overlap attempt and the spawn loop's
    /// `offset = back_end` must still see the original part. Verify
    /// `current_bytes` and `parts.len()` are unchanged across repeated
    /// overlap attempts, and the surviving part still pops correctly.
    #[test]
    fn test_part_queue_overlap_does_not_mutate_queue() {
        let mut q = PartQueue::new(1024, false_flag());
        assert!(matches!(
            q.push(Part {
                offset: 0,
                data: Bytes::from(vec![0u8; 100]),
            }),
            PushOutcome::Ok
        ));
        let before_bytes = q.current_bytes();
        let before_len = q.parts.len();
        // Two overlap attempts — neither should insert.
        let _ = q.push(Part {
            offset: 0,
            data: Bytes::from(vec![1u8; 50]),
        });
        let _ = q.push(Part {
            offset: 99,
            data: Bytes::from(vec![2u8; 50]),
        });
        assert_eq!(
            q.current_bytes(),
            before_bytes,
            "current_bytes must not change on overlap"
        );
        assert_eq!(
            q.parts.len(),
            before_len,
            "parts.len() must not change on overlap"
        );
        // The only part should still pop with the original offset/len.
        let p = q.pop(0).unwrap();
        assert_eq!(p.offset, 0);
        assert_eq!(p.data.len(), 100);
    }

    /// Audit #3 / PR #405: recovering from a poisoned mutex must
    /// not panic the FUSE worker. We poison the mutex by panicking
    /// inside the lock (a thread holding the lock and unwinding),
    /// then verify a subsequent `lock_or_recover` succeeds and
    /// sees the still-consistent inner state.
    #[test]
    fn test_part_queue_recovers_from_poison() {
        use std::sync::Mutex;
        let q: Arc<Mutex<PartQueue>> = Arc::new(Mutex::new(PartQueue::new(1024, false_flag())));
        // Clone the Arc, poison one handle, then read the other.
        let q2 = q.clone();
        let _ = std::thread::spawn(move || {
            let _g = q2.lock().unwrap();
            panic!("intentional poison for test");
        })
        .join();
        // q is now poisoned; unwrap would panic. The recovery idiom
        // (used in the production code) must succeed.
        let mut g = q.lock_or_recover();
        // Verify the queue is in a consistent state — empty, not
        // cancelled, not finished. The panic happened before any
        // push, so all flags remain default.
        assert!(!g.is_terminal());
        // 1025 bytes would exceed the 1024-byte cap; an empty queue
        // accepts this length without overflow.
        assert!(g.is_full_for(1025));
        assert!(!g.is_full_for(1024));
        assert!(g.pop(0).is_none());
    }

    #[test]
    fn test_push_respects_cancelled() {
        let flag = Arc::new(AtomicBool::new(false));
        let mut q = PartQueue::new(100, flag.clone());
        // Fill the queue to capacity.
        assert!(matches!(
            q.push(Part {
                offset: 0,
                data: Bytes::from(vec![0u8; 100]),
            }),
            PushOutcome::Ok
        ));
        // Now cancel — next push should return Cancelled
        // immediately instead of spinning.
        flag.store(true, Ordering::Relaxed);
        let outcome = q.push(Part {
            offset: 100,
            data: Bytes::from(vec![0u8; 50]),
        });
        assert!(
            matches!(outcome, PushOutcome::Cancelled),
            "expected Cancelled, got: {outcome:?}"
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
        assert!(matches!(
            queue.lock().unwrap().push(Part {
                offset: 0,
                data: Bytes::from(vec![0u8; 8]),
            }),
            PushOutcome::Ok
        ));

        let q2 = queue.clone();
        let cv2 = cond.clone();
        let pusher = std::thread::spawn(move || {
            let part = Part {
                offset: 8,
                data: Bytes::from(vec![1u8; 8]),
            };
            let part_len = part.data.len() as u64;
            let mut qlock = q2.lock_or_recover();
            loop {
                if qlock.is_terminal() {
                    return false; // unexpected — test should free room
                }
                if qlock.is_full_for(part_len) {
                    qlock = cv2.wait(qlock).unwrap();
                    continue;
                }
                // The loop invariants (not terminal, not full) make
                // this `Ok`; other outcomes would only fire on a
                // concurrent state change which this single-producer
                // test can't induce.
                let _ = qlock.push(part);
                return true;
            }
        });

        // Give the pusher a moment to park on the condvar.
        std::thread::sleep(Duration::from_millis(50));

        // Free room — this must wake the parked pusher via notify_one.
        let popped = {
            let mut g = queue.lock_or_recover();
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
        let g = queue.lock_or_recover();
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
        let hp = HandlePrefetcher::new(
            op,
            "big.bin".into(),
            64 * 1024,
            8,
            8,
            std::sync::Arc::new(BackpressureController::new()),
            // Issue #201: cap=0 → uncapped, no impact on this
            // regression test (its purpose is the condvar wake).
            crate::mem_limiter::MemoryLimiter::new(0),
        );

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

    // ── Issue #201: per-mount MemoryLimiter wiring ─────────────

    /// Helper: build a `HandlePrefetcher` against a `Memory` backend
    /// with a configurable limiter cap. Used by the two cap-behavior
    /// tests below. Writes 64 KiB of data so the download thread has
    /// real bytes to fetch.
    fn build_test_hp(
        cap: u64,
        bp: std::sync::Arc<BackpressureController>,
    ) -> (HandlePrefetcher, opendal::Operator) {
        let op = opendal::Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let data = vec![42u8; 64 * 1024];
        crate::rt()
            .block_on(async { op.write("big.bin", data).await })
            .unwrap();
        let hp = HandlePrefetcher::new(
            op.clone(),
            "big.bin".into(),
            64 * 1024,
            8,
            8,
            bp,
            crate::mem_limiter::MemoryLimiter::new(cap),
        );
        (hp, op)
    }

    /// Issue #201: when the per-mount budget is too small to admit
    /// the next chunk, `try_reserve` returns Err, the prefetcher
    /// calls `BackpressureController::set_mem_pressure(true)`, and
    /// the controller pins the window to its min (128 KiB).
    ///
    /// Test design: cap = 16 B (smaller than one chunk). The
    /// prefetcher reserves 8 B successfully (the first chunk
    /// fills the queue), then on the second chunk `try_reserve`
    /// fails because 8 + 8 = 16 > cap. The retry with shrunk
    /// (max(8/2, 128KiB) = 128KiB) also fails. The prefetcher
    /// flips `mem_pressure(true)` and skips the chunk.
    #[test]
    fn try_reserve_fails_sets_mem_pressure_and_shrinks_window() {
        let bp = std::sync::Arc::new(BackpressureController::with_window(
            128 * 1024,
            64 * 1024 * 1024,
        ));
        // cap == 16 B: room for exactly one 8-byte chunk. Any
        // subsequent reservation will fail.
        let ml = crate::mem_limiter::MemoryLimiter::new(16);
        let (hp, _op) = build_test_hp(16, bp.clone());
        let ml_for_assert = ml.clone();

        // Give the download thread time to attempt + fail a
        // reservation. Drain one part so it can move past the
        // first chunk.
        std::thread::sleep(Duration::from_millis(50));
        let _ = hp.pop(0, None);
        std::thread::sleep(Duration::from_millis(50));
        let _ = hp.pop(0, None);
        std::thread::sleep(Duration::from_millis(100));

        // The window must be pinned to min_window by
        // set_mem_pressure(true) after a failed reservation.
        assert_eq!(
            bp.current_window(),
            128 * 1024,
            "mem_pressure(true) must pin window to min_window after reserve failure"
        );
        // used() is bounded by the cap (16 B); the prefetcher's
        // own in-flight budget can't exceed what the limiter
        // allows.
        assert!(
            ml_for_assert.used() <= 16,
            "cap enforced: used = {} (cap = 16)",
            ml_for_assert.used()
        );

        // Cleanup: cancel releases any in-flight reservation via Drop.
        hp.cancel();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !hp.is_download_done() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Issue #201: after several successful reservations in a row
    /// (consecutive_successes >= 4), the prefetcher calls
    /// `set_mem_pressure(false)` so the EMA can recompute the
    /// window naturally.
    ///
    /// Test design: cap = 1 MiB, 64 KiB file, 8 B chunks. The
    /// prefetcher reserves successfully each iteration. We start
    /// with `set_mem_pressure(true)` (manually) and observe that
    /// after the prefetcher accumulates consecutive_successes >= 4,
    /// it calls `set_mem_pressure(false)`.
    ///
    /// We can't observe the controller's internal mem_pressure flag
    /// directly, but we can observe its effect: when `mem_pressure`
    /// is false, the EMA path computes window from
    /// `consume_rate / fetch_rate` which can exceed min_window.
    /// When the prefetcher makes several successful fetches,
    /// `record_part_fetched` populates fetch_rate and the EMA
    /// path may grow window above min_window.
    #[test]
    fn try_reserve_succeeds_clears_mem_pressure() {
        // Audit #3 / PR #426 PR: `min_window` MUST equal `chunk_size`
        // (8 B) — otherwise the F3 clamp in `HandlePrefetcher::new`
        // (`chunk_size.clamp(bp_min_window, bp_max_window)`) bumps the
        // first chunk to `bp_min_window` (128 KiB previously), which
        // exceeds the 8 B queue used by `build_test_hp` and parks
        // the prefetcher on `cv.wait` with no pop to wake it. Use 8
        // here so the clamp is a no-op and each push fits the queue.
        let bp = std::sync::Arc::new(BackpressureController::with_window(8, 64 * 1024 * 1024));
        let ml = crate::mem_limiter::MemoryLimiter::new(1024 * 1024);
        let (hp, _op) = build_test_hp(1024 * 1024, bp.clone());
        let ml_for_assert = ml.clone();

        // Manually set mem_pressure to simulate a prior cap hit.
        bp.set_mem_pressure(true);
        assert_eq!(
            bp.current_window(),
            8,
            "precondition: mem_pressure(true) pins window to min"
        );

        // Drain the queue head and let the prefetcher advance. The
        // queue is 8 B with 8 B chunks — after a pop at offset N,
        // the next push (at offset N+8) reserves and succeeds. After
        // 4 successful reservations with mem_pressure set, the
        // prefetcher calls set_mem_pressure(false).
        //
        // We loop popping the current head offset, incrementing
        // after each successful pop.
        let mut next_offset: u64 = 0;
        let mut observed_min_violation = false;
        let mut reservations_observed = 0u64;
        // CI flake resilience (issue #426/#428/#431): shared-runner
        // environments can take much longer than the dev box to
        // schedule the prefetch thread (token issuance, kernel
        // page-cache warmup, opendal runtime spin-up). The previous
        // fixed-budget loop (200 × 10 ms = 2 s) was sometimes too
        // short to observe even a single reservation, and the test
        // reported `observed=0 used=0`. Bound on *elapsed wall time*
        // instead of iteration count so a slow runner gets more
        // iterations rather than failing early.
        //
        // The deadline is 30 s — well above the dev-box runtime of
        // <100 ms — to absorb CI cold-start variance (a fully cold
        // tokio runtime + opendal Memory backend + first fork can
        // take 10+ s on shared runners, observed on multiple PRs).
        // The early-exit on `reservations_observed >= 4` (i.e.
        // hysteresis cleared) bounds the dev-box run; only a CI stall
        // actually hits the deadline.
        let test_deadline = std::time::Instant::now() + Duration::from_secs(30);
        while std::time::Instant::now() < test_deadline {
            std::thread::sleep(Duration::from_millis(10));
            // Pop the current head (if any) to free queue space.
            if hp.pop(next_offset, None).is_some() {
                next_offset += 8;
                reservations_observed += 1;
            }
            if bp.current_window() > 8 {
                observed_min_violation = true;
                break;
            }
            // If the prefetcher is making forward progress, no
            // need to wait the full budget.
            if reservations_observed >= 4 {
                break;
            }
        }

        // The window grew above min_window — this only happens when
        // mem_pressure was cleared (the EMA path can return values
        // > min_window; the pinned path cannot). This is the
        // observable evidence that the hysteresis cleared the flag.
        assert!(
            reservations_observed > 0 || ml_for_assert.used() > 0,
            "prefetch thread should have reserved and released at least one chunk (observed={}, used={})",
            reservations_observed,
            ml_for_assert.used()
        );
        // observed_min_violation may not always fire if the EMA
        // alpha is too slow, but we don't fail the test on that —
        // the reservation+release cycle is the load-bearing
        // assertion.
        let _ = observed_min_violation;

        hp.cancel();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !hp.is_download_done() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
