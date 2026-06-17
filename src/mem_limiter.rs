//! Global MemoryLimiter (issue #44).
//!
//! A process-wide memory budget for the byte
//! consumers that can grow unboundedly:
//! mem_cache, block-level disk cache, and the
//! async upload buffers. Modelled on
//! mountpoint-s3's `mem_limiter.rs`.
//!
//! API:
//!     * `MemoryLimiter::new(cap_bytes)` — process-wide
//!     budget. Wired from the CLI's
//!     `--mem-limit <bytes>` (existing flag,
//!     see commit 5127da3).
//!     * `try_reserve(label, n)` — atomic reserve
//!     + commit. Returns Ok(()) if `n` bytes fit
//!     in the remaining budget, Err(()) if not.
//!     `label` is for diagnostics (\"mem_cache\",
//!     \"prefetch\", \"upload\") — the limiter tracks
//!     usage by label for the snapshot.
//!     * `release(label, n)` — release previously
//!     reserved bytes (e.g. on mem_cache eviction
//!     or prefetch completion).
//!     * `snapshot() -> String` — JSON-formatted
//!     usage by label, for the structured error
//!     log (issue #50) or the metrics endpoint
//!     (issue #47).
//!
//! The limiter is intentionally lightweight:
//!     * Single `AtomicU64` for the global counter
//!     * `DashMap<String, AtomicU64>` for per-label
//!     tracking
//!     * No per-thread accounting (the per-label
//!     view is enough for ops visibility)
//!
//! Pre-fix mntrs had a single `--mem-limit`
//! cap but no per-source accounting — the cap
//! was enforced via mem_cache only, with
//! prefetch + upload able to grow without
//! bound. This change centralises the cap.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// One budget consumer. Tracks the bytes it has
/// reserved + the current allocation.
#[derive(Debug)]
pub struct Allocation {
    pub label: &'static str,
    pub bytes: AtomicU64,
}

impl Allocation {
    pub const fn new(label: &'static str) -> Self {
        Self {
            label,
            bytes: AtomicU64::new(0),
        }
    }
    pub fn current(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}

/// Process-wide memory budget.
pub struct MemoryLimiter {
    cap: u64,
    used: AtomicU64,
    by_label: dashmap::DashMap<&'static str, Arc<Allocation>>,
}

impl MemoryLimiter {
    /// Create a limiter with `cap` bytes of
    /// budget. `cap == 0` disables enforcement
    /// (all reservations succeed).
    pub fn new(cap: u64) -> Arc<Self> {
        Arc::new(Self {
            cap,
            used: AtomicU64::new(0),
            by_label: dashmap::DashMap::new(),
        })
    }

    /// Reserve `n` bytes against the budget,
    /// attributed to `label`. Returns Ok(()) if
    /// the reservation fits; Err with a static
    /// reason if the cap would be exceeded (and
    /// the reservation is NOT taken — the caller
    /// is expected to fall back to a smaller
    /// size or fail the operation).
    ///
    /// Atomic: concurrent calls are serialised
    /// via the global `used` counter. The
    /// per-label Allocation is updated on
    /// success only.
    pub fn try_reserve(&self, label: &'static str, n: u64) -> Result<(), &'static str> {
        if self.cap == 0 {
            // Uncapped — just record the label.
            // Saturate against i64::MAX for the
            // per-label view (the global `used`
            // still uses u64 arithmetic).
            let safe = n.min(i64::MAX as u64);
            // Still bump the global counter so
            // `used()` reports the true
            // uncapped total.
            self.used.fetch_add(safe, Ordering::Relaxed);
            self.record(label, safe as i64);
            return Ok(());
        }
        // CAS loop: increment `used` only if
        // `used + n <= cap`. The relaxed ordering
        // is fine because the limiter is a
        // best-effort budget, not a hard quota —
        // a transient over-shoot by N concurrent
        // calls is acceptable.
        loop {
            let current = self.used.load(Ordering::Relaxed);
            let new = current.saturating_add(n);
            if new > self.cap {
                return Err("memory cap exceeded");
            }
            if self
                .used
                .compare_exchange(current, new, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                self.record(label, n.min(i64::MAX as u64) as i64);
                return Ok(());
            }
        }
    }

    /// Release `n` bytes previously reserved
    /// against `label`. Saturating — going
    /// below 0 is a no-op (caller bug; a stale
    /// release is harmless).
    pub fn release(&self, label: &'static str, n: u64) {
        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(n))
            })
            .ok();
        self.record(label, -(n as i64));
    }

    /// Update the per-label Allocation. Positive
    /// `delta` = grew, negative = shrunk.
    fn record(&self, label: &'static str, delta: i64) {
        let alloc = self
            .by_label
            .entry(label)
            .or_insert_with(|| Arc::new(Allocation::new(label)))
            .clone();
        if delta >= 0 {
            alloc.bytes.fetch_add(delta as u64, Ordering::Relaxed);
        } else {
            alloc
                .bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                    Some(c.saturating_sub((-delta) as u64))
                })
                .ok();
        }
    }

    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }
    pub fn cap(&self) -> u64 {
        self.cap
    }
    pub fn remaining(&self) -> u64 {
        self.cap.saturating_sub(self.used())
    }

    /// Render a JSON snapshot of the per-label
    /// usage, suitable for the structured error
    /// log (issue #50) or the metrics endpoint
    /// (issue #47).
    pub fn snapshot_json(&self) -> String {
        let mut out = String::with_capacity(256);
        out.push_str("{\"mem_limit\":");
        out.push_str(&self.cap.to_string());
        out.push_str(",\"mem_used\":");
        out.push_str(&self.used().to_string());
        out.push_str(",\"by_label\":{");
        let mut first = true;
        for entry in self.by_label.iter() {
            if !first {
                out.push(',');
            }
            first = false;
            out.push('"');
            out.push_str(entry.key());
            out.push_str("\":");
            let n = entry.value().current();
            out.push_str(&n.to_string());
        }
        out.push_str("}}");
        out
    }
}

/// Process-wide default limiter. Lazily
/// initialised on first `global()` call. CLI
/// flag `--mem-limit <bytes>` (already wired,
/// commit 5127da3) sets the cap. The cap is
/// passed through `install(cap)` which is the
/// only intended init point.
static LIMITER: std::sync::OnceLock<Arc<MemoryLimiter>> = std::sync::OnceLock::new();

pub fn install(cap: u64) {
    let _ = LIMITER.set(MemoryLimiter::new(cap));
}

pub fn global() -> Option<Arc<MemoryLimiter>> {
    LIMITER.get().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_release_round_trip() {
        let l = MemoryLimiter::new(1024);
        assert!(l.try_reserve("test", 512).is_ok());
        assert_eq!(l.used(), 512);
        l.release("test", 512);
        assert_eq!(l.used(), 0);
    }

    #[test]
    fn reserve_fails_over_cap() {
        let l = MemoryLimiter::new(100);
        assert!(l.try_reserve("test", 60).is_ok());
        // 60 + 50 = 110 > 100, must fail.
        assert!(l.try_reserve("test", 50).is_err());
        // The failed reservation does NOT
        // consume budget.
        assert_eq!(l.used(), 60);
    }

    #[test]
    fn uncapped_accepts_anything() {
        let l = MemoryLimiter::new(0);
        // Use a value that fits in i64 (the
        // per-label Allocation uses i64 for
        // delta tracking). 1 EiB > u32::MAX but
        // well under i64::MAX.
        let n: u64 = 1 << 60; // 1 EiB
        assert!(l.try_reserve("huge", n).is_ok());
        assert_eq!(l.used(), n);
    }

    #[test]
    fn snapshot_json_format() {
        let l = MemoryLimiter::new(1000);
        l.try_reserve("a", 100).unwrap();
        l.try_reserve("b", 200).unwrap();
        let s = l.snapshot_json();
        assert!(s.contains("\"mem_limit\":1000"));
        assert!(s.contains("\"mem_used\":300"));
        assert!(s.contains("\"a\":100"));
        assert!(s.contains("\"b\":200"));
    }
}
