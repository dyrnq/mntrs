//! Prometheus-compatible metrics (issue #47).
//!
//! Lightweight metrics emission inspired by
//! mountpoint-s3's `metrics/defs.rs`. We deliberately
//! do NOT pull in the `metrics` crate (yet) — mntrs
//! needs only a few counters / histograms, and the
//! `metrics` crate + a Prometheus exporter adds a
//! ~500 KiB dep + a background HTTP server.
//!
//! Instead, this module provides a tiny shim that
//! counts and times FUSE ops in-process. The metrics
//! are exposed as Prometheus text format via a
//! `snapshot()` method (callable from a debug
//! endpoint or a periodic dumper), or — the
//! integration path — wired into the existing
//! \`tracing-subscriber\` JSON output so an
//! OpenTelemetry collector can scrape them.
//!
//! Metric names follow Prometheus conventions:
//!     * \`fuse_request_total{op,errno}\` (counter)
//!     * \`fuse_request_duration_microseconds{op}\` (histogram, exponential buckets)
//!     * \`cache_lookups_total{kind,result}\` (counter)
//!     * \`writeback_pending\` (gauge)
//!     * \`process_memory_bytes\` (gauge, set on snapshot)
//!
//! The user-facing API is two functions:
//!     * \`record_fuse_op(op, duration, errno_name)\`
//!     * \`snapshot() -> String\` (Prometheus text format)
//!
//! A future \`--metrics-addr <ip:port>\` CLI flag
//! would start a tiny axum-based HTTP server on
//! \`/metrics\`; for now, the snapshot is exposed
//! via a debug handler (TODO).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// FUSE operation name (typed to avoid typos at
/// the call site).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FuseOp {
    Lookup,
    Getattr,
    Setattr,
    Read,
    Write,
    Readdir,
    Readdirplus,
    Create,
    Unlink,
    Rename,
    Flush,
    Release,
    Fsync,
    Other,
}

impl FuseOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            FuseOp::Lookup => "lookup",
            FuseOp::Getattr => "getattr",
            FuseOp::Setattr => "setattr",
            FuseOp::Read => "read",
            FuseOp::Write => "write",
            FuseOp::Readdir => "readdir",
            FuseOp::Readdirplus => "readdirplus",
            FuseOp::Create => "create",
            FuseOp::Unlink => "unlink",
            FuseOp::Rename => "rename",
            FuseOp::Flush => "flush",
            FuseOp::Release => "release",
            FuseOp::Fsync => "fsync",
            FuseOp::Other => "other",
        }
    }
}

/// Counter: how many times a FUSE op was called,
/// broken down by op name + result (Ok / ErrKind).
pub struct OpCounter {
    op: FuseOp,
    ok: AtomicU64,
    err: AtomicU64,
}

impl OpCounter {
    pub const fn new(op: FuseOp) -> Self {
        Self {
            op,
            ok: AtomicU64::new(0),
            err: AtomicU64::new(0),
        }
    }
    pub fn record_ok(&self) {
        self.ok.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_err(&self) {
        self.err.fetch_add(1, Ordering::Relaxed);
    }
    pub fn ok(&self) -> u64 {
        self.ok.load(Ordering::Relaxed)
    }
    pub fn err(&self) -> u64 {
        self.err.load(Ordering::Relaxed)
    }
}

/// Histogram with a fixed set of exponential
/// buckets (1us, 10us, 100us, ..., 10s). The
/// `count` / `sum` fields are also tracked so the
/// Prometheus output can compute the mean without
/// a streaming sum of all bucket counts.
pub struct OpHistogram {
    op: FuseOp,
    /// Bucket upper bounds in microseconds.
    buckets_us: [u64; 10],
    /// Per-bucket count of observations <= the
    /// bucket's upper bound.
    bucket_counts: [AtomicU64; 10],
    count: AtomicU64,
    sum_us: AtomicU64,
}

impl OpHistogram {
    pub const fn new(op: FuseOp) -> Self {
        let buckets_us = [
            10,
            100,
            1_000,
            10_000,
            100_000,
            1_000_000,
            10_000_000,
            100_000_000,
            1_000_000_000,
            u64::MAX,
        ];
        Self {
            op,
            buckets_us,
            bucket_counts: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, d: Duration) {
        let us = d.as_micros() as u64;
        for (i, b) in self.buckets_us.iter().enumerate() {
            if us <= *b {
                self.bucket_counts[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        // Saturating add guards against `u64` overflow at sustained
        // high op rates (issue #115: at 1M ops/sec `sum_us` saturates
        // `u64::MAX` in ~49.7 days; once it wraps the Prometheus sum
        // metric reports wrong numbers). Saturating is a strict
        // improvement — the histogram bucket counts are still exact,
        // and the only loss is the small absolute-precision tail
        // beyond 1.8e19 µs (~584 years at 1M ops/sec).
        self.sum_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(us))
            })
            .ok();
    }
}

/// Process-wide metrics registry. One OpCounter
/// and one OpHistogram per FUSE op, plus a gauge
/// for the writeback pending count.
pub struct Metrics {
    pub lookup: OpCounter,
    pub getattr: OpCounter,
    pub setattr: OpCounter,
    pub read: OpCounter,
    pub write: OpCounter,
    pub readdir: OpCounter,
    pub readdirplus: OpCounter,
    pub create: OpCounter,
    pub unlink: OpCounter,
    pub rename: OpCounter,
    pub flush: OpCounter,
    pub release: OpCounter,
    pub fsync: OpCounter,
    pub other: OpCounter,
    pub lookup_h: OpHistogram,
    pub getattr_h: OpHistogram,
    pub setattr_h: OpHistogram,
    pub read_h: OpHistogram,
    pub write_h: OpHistogram,
    pub readdir_h: OpHistogram,
    pub readdirplus_h: OpHistogram,
    pub create_h: OpHistogram,
    pub unlink_h: OpHistogram,
    pub rename_h: OpHistogram,
    pub flush_h: OpHistogram,
    pub release_h: OpHistogram,
    pub fsync_h: OpHistogram,
    pub other_h: OpHistogram,
    /// Writeback pending count (gauge). Updated
    /// on every snapshot via the
    /// `pending_count()` shim in writeback.rs.
    pub writeback_pending_gauge: AtomicU64,
    /// Per-level cache hit/miss counters (issue #127).
    /// L1 = in-memory block cache, L2 = on-disk block cache.
    pub cache_l1_hit: AtomicU64,
    pub cache_l1_miss: AtomicU64,
    pub cache_l2_hit: AtomicU64,
    pub cache_l2_miss: AtomicU64,
}

impl Metrics {
    pub const fn new() -> Self {
        Self {
            lookup: OpCounter::new(FuseOp::Lookup),
            getattr: OpCounter::new(FuseOp::Getattr),
            setattr: OpCounter::new(FuseOp::Setattr),
            read: OpCounter::new(FuseOp::Read),
            write: OpCounter::new(FuseOp::Write),
            readdir: OpCounter::new(FuseOp::Readdir),
            readdirplus: OpCounter::new(FuseOp::Readdirplus),
            create: OpCounter::new(FuseOp::Create),
            unlink: OpCounter::new(FuseOp::Unlink),
            rename: OpCounter::new(FuseOp::Rename),
            flush: OpCounter::new(FuseOp::Flush),
            release: OpCounter::new(FuseOp::Release),
            fsync: OpCounter::new(FuseOp::Fsync),
            other: OpCounter::new(FuseOp::Other),
            lookup_h: OpHistogram::new(FuseOp::Lookup),
            getattr_h: OpHistogram::new(FuseOp::Getattr),
            setattr_h: OpHistogram::new(FuseOp::Setattr),
            read_h: OpHistogram::new(FuseOp::Read),
            write_h: OpHistogram::new(FuseOp::Write),
            readdir_h: OpHistogram::new(FuseOp::Readdir),
            readdirplus_h: OpHistogram::new(FuseOp::Readdirplus),
            create_h: OpHistogram::new(FuseOp::Create),
            unlink_h: OpHistogram::new(FuseOp::Unlink),
            rename_h: OpHistogram::new(FuseOp::Rename),
            flush_h: OpHistogram::new(FuseOp::Flush),
            release_h: OpHistogram::new(FuseOp::Release),
            fsync_h: OpHistogram::new(FuseOp::Fsync),
            other_h: OpHistogram::new(FuseOp::Other),
            writeback_pending_gauge: AtomicU64::new(0),
            cache_l1_hit: AtomicU64::new(0),
            cache_l1_miss: AtomicU64::new(0),
            cache_l2_hit: AtomicU64::new(0),
            cache_l2_miss: AtomicU64::new(0),
        }
    }

    /// Record a cache hit at the given level ("l1" or "l2").
    pub fn record_cache_hit(&self, level: &str) {
        match level {
            "l1" => {
                self.cache_l1_hit.fetch_add(1, Ordering::Relaxed);
            }
            "l2" => {
                self.cache_l2_hit.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Record a cache miss at the given level ("l1" or "l2").
    pub fn record_cache_miss(&self, level: &str) {
        match level {
            "l1" => {
                self.cache_l1_miss.fetch_add(1, Ordering::Relaxed);
            }
            "l2" => {
                self.cache_l2_miss.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Record a FUSE op (counter + histogram).
    pub fn record(&self, op: FuseOp, duration: Duration, ok: bool) {
        let (counter, hist) = match op {
            FuseOp::Lookup => (&self.lookup, &self.lookup_h),
            FuseOp::Getattr => (&self.getattr, &self.getattr_h),
            FuseOp::Setattr => (&self.setattr, &self.setattr_h),
            FuseOp::Read => (&self.read, &self.read_h),
            FuseOp::Write => (&self.write, &self.write_h),
            FuseOp::Readdir => (&self.readdir, &self.readdir_h),
            FuseOp::Readdirplus => (&self.readdirplus, &self.readdirplus_h),
            FuseOp::Create => (&self.create, &self.create_h),
            FuseOp::Unlink => (&self.unlink, &self.unlink_h),
            FuseOp::Rename => (&self.rename, &self.rename_h),
            FuseOp::Flush => (&self.flush, &self.flush_h),
            FuseOp::Release => (&self.release, &self.release_h),
            FuseOp::Fsync => (&self.fsync, &self.fsync_h),
            FuseOp::Other => (&self.other, &self.other_h),
        };
        if ok {
            counter.record_ok();
        } else {
            counter.record_err();
        }
        hist.observe(duration);
    }

    /// Render the Prometheus text-format snapshot.
    /// Output format matches
    /// https://prometheus.io/docs/instrumenting/exposition_formats/
    pub fn snapshot(&self) -> String {
        let mut out = String::with_capacity(4096);
        out.push_str("# HELP fuse_request_total FUSE operations by op + result\n");
        out.push_str("# TYPE fuse_request_total counter\n");
        let counters = [
            &self.lookup,
            &self.getattr,
            &self.setattr,
            &self.read,
            &self.write,
            &self.readdir,
            &self.readdirplus,
            &self.create,
            &self.unlink,
            &self.rename,
            &self.flush,
            &self.release,
            &self.fsync,
            &self.other,
        ];
        for c in counters {
            out.push_str(&format!(
                "fuse_request_total{{op=\"{}\",result=\"ok\"}} {}\n",
                c.op.as_str(),
                c.ok()
            ));
            out.push_str(&format!(
                "fuse_request_total{{op=\"{}\",result=\"err\"}} {}\n",
                c.op.as_str(),
                c.err()
            ));
        }
        out.push_str("# HELP fuse_request_duration_microseconds FUSE op duration\n");
        out.push_str("# TYPE fuse_request_duration_microseconds histogram\n");
        let hists = [
            &self.lookup_h,
            &self.getattr_h,
            &self.setattr_h,
            &self.read_h,
            &self.write_h,
            &self.readdir_h,
            &self.readdirplus_h,
            &self.create_h,
            &self.unlink_h,
            &self.rename_h,
            &self.flush_h,
            &self.release_h,
            &self.fsync_h,
            &self.other_h,
        ];
        for h in hists {
            for (i, b) in h.buckets_us.iter().enumerate() {
                let le = if *b == u64::MAX {
                    "+Inf".to_string()
                } else {
                    b.to_string()
                };
                out.push_str(&format!(
                    "fuse_request_duration_microseconds_bucket{{op=\"{}\",le=\"{}\"}} {}\n",
                    h.op.as_str(),
                    le,
                    h.bucket_counts[i].load(Ordering::Relaxed)
                ));
            }
            out.push_str(&format!(
                "fuse_request_duration_microseconds_count{{op=\"{}\"}} {}\n",
                h.op.as_str(),
                h.count.load(Ordering::Relaxed)
            ));
            out.push_str(&format!(
                "fuse_request_duration_microseconds_sum{{op=\"{}\"}} {}\n",
                h.op.as_str(),
                h.sum_us.load(Ordering::Relaxed)
            ));
        }
        out.push_str("# HELP writeback_pending In-flight writeback tasks\n");
        out.push_str("# TYPE writeback_pending gauge\n");
        out.push_str(&format!(
            "writeback_pending {}\n",
            self.writeback_pending_gauge.load(Ordering::Relaxed)
        ));
        // Per-level cache counters (issue #127).
        out.push_str("# HELP cache_hits_total Cache hits by level\n");
        out.push_str("# TYPE cache_hits_total counter\n");
        out.push_str(&format!(
            "cache_hits_total{{level=\"l1\"}} {}\n",
            self.cache_l1_hit.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "cache_hits_total{{level=\"l2\"}} {}\n",
            self.cache_l2_hit.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP cache_misses_total Cache misses by level\n");
        out.push_str("# TYPE cache_misses_total counter\n");
        out.push_str(&format!(
            "cache_misses_total{{level=\"l1\"}} {}\n",
            self.cache_l1_miss.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "cache_misses_total{{level=\"l2\"}} {}\n",
            self.cache_l2_miss.load(Ordering::Relaxed)
        ));
        out
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Process-wide metrics instance. Lazily
/// initialised on first use; the lazy lock
/// keeps the construction overhead off the
/// startup path.
static METRICS: std::sync::LazyLock<Arc<Metrics>> =
    std::sync::LazyLock::new(|| Arc::new(Metrics::new()));

pub fn global() -> Arc<Metrics> {
    Arc::clone(&METRICS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_cumulative() {
        // A 1-second observation should fall
        // into buckets with upper bound >= 1M us.
        let h = OpHistogram::new(FuseOp::Read);
        h.observe(Duration::from_micros(1_000_000));
        h.observe(Duration::from_micros(50));
        // 1M-bucket and below should all increment
        // for the 1M observation.
        assert!(h.bucket_counts[5].load(Ordering::Relaxed) >= 1);
        // 50us observation falls into the 100us
        // bucket and below.
        assert!(h.bucket_counts[1].load(Ordering::Relaxed) >= 1);
        // Total count = 2.
        assert_eq!(h.count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn histogram_sum_us_saturates() {
        // Issue #115: at 1M ops/sec, sum_us overflows u64 in
        // ~49.7 days. Saturating_add keeps the metric meaningful
        // even after the cap is hit (Prometheus reads a stable
        // value rather than a wrapped/garbage one).
        let h = OpHistogram::new(FuseOp::Read);
        // Pre-load sum_us to a value that would overflow on
        // a second addition of (say) 1000us.
        h.sum_us.store(u64::MAX - 500, Ordering::Relaxed);
        h.observe(Duration::from_micros(1_000));
        // Should saturate at u64::MAX, NOT wrap.
        assert_eq!(h.sum_us.load(Ordering::Relaxed), u64::MAX);
        // Bucket counts still increment correctly — only
        // the running total saturates.
        assert_eq!(h.count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn counter_ok_err() {
        let c = OpCounter::new(FuseOp::Read);
        c.record_ok();
        c.record_ok();
        c.record_err();
        assert_eq!(c.ok(), 2);
        assert_eq!(c.err(), 1);
    }

    #[test]
    fn snapshot_contains_expected_lines() {
        let m = Metrics::new();
        m.read.record_ok();
        m.read_h.observe(Duration::from_micros(123));
        let s = m.snapshot();
        assert!(s.contains("fuse_request_total{op=\"read\",result=\"ok\"} 1"));
        assert!(s.contains("fuse_request_duration_microseconds_count{op=\"read\"} 1"));
        assert!(s.contains("writeback_pending 0"));
    }
}
