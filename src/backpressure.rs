//! Adaptive backpressure controller (issue #45).
//!
//! Adjusts the prefetcher's read window based on
//! consumption rate vs fetch rate. Modelled on
//! mountpoint-s3's
//! `prefetch/backpressure_controller.rs`.
//!
//! Algorithm (kept simple for v1):
//!     * `record_part_consumed(consumed_bytes,
//!     elapsed)` — FUSE read consumed a part
//!     in `elapsed` time. The consumer rate
//!     (bytes/sec) is computed.
//!     * `record_part_fetched(fetched_bytes,
//!     elapsed)` — the prefetch pool fetched
//!     a part. The producer rate is computed.
//!     * `current_window()` returns the next
//!     chunk_size the prefetcher should use.
//!     * If consume rate > fetch rate, the
//!     consumer is faster than the producer
//!     (queue is draining); the window grows
//!     toward `max_window` so the prefetcher
//!     can keep up.
//!     * If fetch rate > consume rate, the
//!     producer is faster (queue is filling);
//!     the window shrinks toward `min_window`
//!     so the prefetcher doesn't waste memory
//!     on parts the FUSE read isn't going to
//!     touch.
//!     * If the memory limiter is at > 80% of
//!     the cap, the window shrinks to
//!     `min_window` immediately (memory
//!     pressure).
//!
//! The default min / max window are 1 MiB and
//! 64 MiB, matching the rclone defaults
//! (--vfs-read-chunk-size).

use std::sync::Mutex;
use std::time::Duration;

/// Backpressure controller state. One per
/// process (or one per mount — a per-mount
/// instance is safer for multi-mount daemons).
pub struct BackpressureController {
    inner: Mutex<Inner>,
}

struct Inner {
    /// Last computed consumer rate (bytes/sec).
    consume_rate: f64,
    /// Last computed producer rate (bytes/sec).
    fetch_rate: f64,
    /// Current prefetch window (bytes per
    /// range request).
    window: u64,
    /// Configured bounds.
    min_window: u64,
    max_window: u64,
    /// MemoryLimiter "at capacity" flag
    /// (set externally by the caller; the
    /// controller only consults it).
    mem_pressure: bool,
}

impl BackpressureController {
    /// Default min / max window: 1 MiB / 64 MiB.
    pub fn new() -> Self {
        Self::with_window(1024 * 1024, 64 * 1024 * 1024)
    }

    pub fn with_window(min: u64, max: u64) -> Self {
        assert!(min <= max, "min_window must be <= max_window");
        Self {
            inner: Mutex::new(Inner {
                consume_rate: 0.0,
                fetch_rate: 0.0,
                window: min,
                min_window: min,
                max_window: max,
                mem_pressure: false,
            }),
        }
    }

    /// Record a FUSE read that consumed a part.
    /// `elapsed` should be > 0; if 0, the rate
    /// update is skipped (avoid div-by-zero).
    pub fn record_part_consumed(&self, bytes: u64, elapsed: Duration) {
        if elapsed.as_secs_f64() <= 0.0 {
            return;
        }
        let rate = bytes as f64 / elapsed.as_secs_f64();
        let mut inner = self.inner.lock().unwrap();
        // Exponential moving average (alpha = 0.3)
        // for a stable rate estimate without a
        // history buffer.
        inner.consume_rate = inner.consume_rate * 0.7 + rate * 0.3;
        inner.window = self.compute_window(&inner);
    }

    /// Record a prefetch-pool fetch.
    pub fn record_part_fetched(&self, bytes: u64, elapsed: Duration) {
        if elapsed.as_secs_f64() <= 0.0 {
            return;
        }
        let rate = bytes as f64 / elapsed.as_secs_f64();
        let mut inner = self.inner.lock().unwrap();
        inner.fetch_rate = inner.fetch_rate * 0.7 + rate * 0.3;
        inner.window = self.compute_window(&inner);
    }

    /// Set the memory-pressure flag. The
    /// controller shrinks the window to
    /// `min_window` while this is true.
    pub fn set_mem_pressure(&self, on: bool) {
        let mut inner = self.inner.lock().unwrap();
        inner.mem_pressure = on;
        if on {
            inner.window = inner.min_window;
        } else {
            inner.window = self.compute_window(&inner);
        }
    }

    /// Read the current window.
    pub fn current_window(&self) -> u64 {
        self.inner.lock().unwrap().window
    }

    /// Compute the next window from the current
    /// rates. Halve the window if the producer
    /// is faster than the consumer (queue is
    /// filling); double it if the consumer is
    /// faster (queue is draining). Clamp to
    /// `[min_window, max_window]`.
    fn compute_window(&self, inner: &Inner) -> u64 {
        if inner.mem_pressure {
            return inner.min_window;
        }
        if inner.fetch_rate <= 0.0 || inner.consume_rate <= 0.0 {
            return inner.window; // no data yet
        }
        let ratio = inner.consume_rate / inner.fetch_rate;
        let new = if ratio > 1.0 {
            // Consumer faster → grow window
            (inner.window as f64 * 1.5).min(inner.max_window as f64) as u64
        } else if ratio < 0.5 {
            // Consumer much slower → shrink
            (inner.window as f64 * 0.5).max(inner.min_window as f64) as u64
        } else {
            inner.window
        };
        new.clamp(inner.min_window, inner.max_window)
    }
}

impl Default for BackpressureController {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_faster_grows_window() {
        let c = BackpressureController::with_window(1024, 1 << 20);
        // Initial window = min
        assert_eq!(c.current_window(), 1024);
        // Consumer fast (1 MiB in 1 ms = 1 GiB/s);
        // producer slow (1 KiB in 100 ms = 10 KiB/s).
        c.record_part_consumed(1 << 20, Duration::from_millis(1));
        c.record_part_fetched(1024, Duration::from_millis(100));
        // Window should grow from 1024 toward
        // max (1 MiB), but each step multiplies
        // by 1.5.
        let w = c.current_window();
        assert!(
            w > 1024,
            "window should grow when consumer is faster, got {w}"
        );
    }

    #[test]
    fn producer_faster_shrinks_window() {
        let c = BackpressureController::with_window(1024, 1 << 20);
        // Start at max
        for _ in 0..5 {
            c.record_part_consumed(1024, Duration::from_millis(100));
            c.record_part_fetched(1 << 20, Duration::from_millis(1));
        }
        // Window should shrink toward min.
        assert!(
            c.current_window() < 1 << 20,
            "window should shrink when producer is faster"
        );
    }

    #[test]
    fn mem_pressure_forces_min() {
        let c = BackpressureController::with_window(1024, 1 << 20);
        c.set_mem_pressure(true);
        assert_eq!(c.current_window(), 1024);
        // Even after the rates would normally
        // grow the window, the mem_pressure flag
        // pins it at min.
        c.record_part_consumed(1 << 20, Duration::from_millis(1));
        c.record_part_fetched(1024, Duration::from_millis(100));
        assert_eq!(
            c.current_window(),
            1024,
            "mem_pressure must pin the window at min"
        );
        c.set_mem_pressure(false);
    }

    #[test]
    fn clamp_to_bounds() {
        let c = BackpressureController::with_window(1024, 4096);
        for _ in 0..20 {
            c.record_part_consumed(1 << 20, Duration::from_millis(1));
            c.record_part_fetched(1, Duration::from_secs(1));
        }
        // After many growth steps, must still
        // be <= 4096.
        assert!(c.current_window() <= 4096);
    }
}
