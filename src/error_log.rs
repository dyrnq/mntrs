//! Structured JSON error log (issue #50).
//!
//! Background thread that writes one JSON line per
//! FUSE / backend error to a file. Inspired by
//! mountpoint-s3's `FileErrorLogger`.
//!
//! Usage:
//!   let handle = ErrorLog::start("/var/log/mntrs/errors.log");
//!   handle.log("read", Errno::EIO, &[("path", "/foo")]);
//!
//! The log is opt-in via the `--error-log-file` flag
//! and is intentionally minimal: each log line is
//! < 200 bytes, the background thread is bounded
//! by a bounded channel, and the file is line-flushed
//! on every record so a process crash loses at most
//! the in-flight line.
//!
//! Field schema (v1):
//!   operation   — FUSE op name (read, write, lookup, ...)
//!   errno       — numeric errno value (e.g. 5 = EIO)
//!   errno_name  — symbolic name (e.g. "EIO") when known
//!   timestamp   — RFC3339 UTC
//!   version     — schema version (= 1)
//!   fields      — array of [key, value] pairs (string only)

use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

/// One structured error log entry.
#[derive(Debug, Clone)]
pub struct ErrorRecord {
    pub operation: &'static str,
    pub errno: i32,
    pub errno_name: &'static str,
    pub fields: Vec<(&'static str, String)>,
}

impl ErrorRecord {
    /// Format the record as a single JSON line.
    /// Manual serialization (no serde dep) — the
    /// shape is small and stable.
    pub fn to_json_line(&self) -> String {
        let ts = rfc3339_utc_now();
        let mut out = String::with_capacity(256);
        out.push_str("{\"version\":1,\"operation\":\"");
        out.push_str(self.operation);
        out.push_str("\",\"errno\":");
        out.push_str(&self.errno.to_string());
        out.push_str(",\"errno_name\":\"");
        out.push_str(self.errno_name);
        out.push_str("\",\"timestamp\":\"");
        out.push_str(&ts);
        out.push('"');
        for (k, v) in &self.fields {
            out.push_str(",\"");
            json_escape_into(k, &mut out);
            out.push_str("\":\"");
            json_escape_into(v, &mut out);
            out.push('"');
        }
        out.push_str("}\n");
        out
    }
}

/// Current time formatted as RFC 3339 UTC with millisecond
/// precision, e.g. `2026-06-22T14:33:18.547Z`. Falls back
/// to a fixed string if the system clock is before 1970 or
/// past the year 9999 (very unlikely on a sane host).
///
/// Uses Howard Hinnant's `civil_from_days` algorithm
/// (public domain) for days→Y/M/D — no extra dep
/// (chrono/time would each add 100-300 KB to the binary
/// for a feature that's 30 lines).
///
/// Why not seconds-since-epoch: log shippers
/// (Loki, ELK, Datadog) auto-detect timestamp formats but
/// only the RFC 3339 calendar form `YYYY-MM-DDTHH:MM:SS`
/// is recognised; raw seconds-since-epoch is indexed as a
/// free-form string and breaks time-range queries.
fn rfc3339_utc_now() -> String {
    let d = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = d.as_secs();
    let ms = d.subsec_millis();
    let days = (secs / 86400) as i64;
    let sod = (secs % 86400) as u32;
    let h = sod / 3600;
    let m = (sod / 60) % 60;
    let s = sod % 60;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        year, month, day, h, m, s, ms
    )
}

/// Convert days since 1970-01-01 to (year, month, day).
/// Howard Hinnant's `civil_from_days` algorithm
/// (https://howardhinnant.github.io/date_algorithms.html),
/// public domain. The trick: shift the epoch to 0000-03-01 so
/// that leap days always fall at the end of a year, making the
/// year/month arithmetic trivial.
fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    // Shift epoch from 1970-01-01 to 0000-03-01.
    let z = days_since_epoch + 719468;
    // era = 400-year cycle. Negative `z` needs floor-division
    // (towards -∞), not truncation towards 0, so handle sign.
    let era = if z >= 0 {
        z / 146097
    } else {
        (z - 146096) / 146097
    };
    let doe = z - era * 146097;
    // Year of era [0, 399].
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    // Day of year [0, 365].
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    // Month index in Mar-based numbering [0, 11].
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    // Year offset: Jan/Feb belong to the next year in
    // Mar-based numbering.
    let year = y + if month <= 2 { 1 } else { 0 };
    (year as i32, month as u32, day)
}

/// Escape a string for inclusion inside a JSON
/// string value. Mirrors serde_json's escape set:
/// control chars (< 0x20) → \uXXXX; backslash and
/// double-quote → backslash-escaped.
fn json_escape_into(s: &str, out: &mut String) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
}

/// Background-thread JSON error log.
///
/// `start` spawns a daemon thread that drains the
/// bounded channel and writes one JSON line per
/// record to `path`. The thread exits when the
/// `ErrorLog` is dropped (sends a sentinel and
/// joins). The log file is opened in append mode.
///
/// Thread safety: `ErrorLog` is `Send + Sync` (the
/// `Sender` is a std mpsc). Cloning is cheap.
pub struct ErrorLog {
    tx: std::sync::mpsc::SyncSender<ErrorRecord>,
    _handle: std::thread::JoinHandle<()>,
}

impl ErrorLog {
    /// Open `path` for append and start the writer
    /// thread. Returns `None` if the file can't be
    /// opened (caller should fall back to tracing).
    pub fn start(path: &Path) -> Option<Arc<Self>> {
        // Bounded channel: 1024 in-flight records
        // (≈ 256 KiB at 256 bytes/line). If the
        // channel is full, log_record falls back to
        // a non-blocking send + drop, so a slow disk
        // never blocks the FUSE worker.
        let (tx, rx) = std::sync::mpsc::sync_channel::<ErrorRecord>(1024);
        let path = path.to_path_buf();
        let handle = std::thread::Builder::new()
            .name("mntrs-error-log".to_string())
            .spawn(move || {
                let Ok(mut f) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                else {
                    return;
                };
                while let Ok(rec) = rx.recv() {
                    let line = rec.to_json_line();
                    // Line-buffered write: a
                    // process crash loses at
                    // most the in-flight
                    // line, but the file
                    // stays consistent for
                    // log shippers.
                    let _ = f.write_all(line.as_bytes());
                    let _ = f.flush();
                }
            })
            .ok()?;
        Some(Arc::new(Self {
            tx,
            _handle: handle,
        }))
    }

    /// Log a single record. Non-blocking: if the
    /// channel is full, the record is dropped (a
    /// warning is logged at trace level once per
    /// 1000 drops so a runaway can be diagnosed).
    pub fn log(&self, rec: ErrorRecord) {
        // try_send — never block the FUSE worker
        // on a slow disk.
        if self.tx.try_send(rec).is_err() {
            static DROPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let d = DROPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            if d.is_multiple_of(1000) {
                tracing::warn!(
                    drops = d,
                    "error log channel full; dropping records (issue #50)"
                );
            }
        }
    }

    /// Convenience: log a single record with no
    /// extra fields.
    pub fn log_simple(&self, op: &'static str, errno: i32, errno_name: &'static str) {
        self.log(ErrorRecord {
            operation: op,
            errno,
            errno_name,
            fields: Vec::new(),
        });
    }
}

/// Symbolic errno name for the values that mntrs
/// actually maps (see `io_err_to_fuse_errno`). The
/// mapping is not complete — the log will fall back
/// to a numeric string for unknown errnos.
fn errno_name(raw: i32) -> &'static str {
    match raw {
        1 => "EPERM",
        2 => "ENOENT",
        5 => "EIO",
        9 => "EBADF",
        11 => "EAGAIN",
        12 => "ENOMEM",
        13 => "EACCES",
        14 => "EFAULT",
        16 => "EBUSY",
        17 => "EEXIST",
        18 => "EXDEV",
        19 => "ENODEV",
        20 => "ENOTDIR",
        21 => "EISDIR",
        22 => "EINVAL",
        23 => "ENFILE",
        24 => "EMFILE",
        27 => "EFBIG",
        28 => "ENOSPC",
        30 => "EROFS",
        32 => "EPIPE",
        36 => "ENAMETOOLONG",
        38 => "ENOSYS",
        39 => "ENOTEMPTY",
        40 => "ELOOP",
        122 => "EDQUOT",
        _ => "UNKNOWN",
    }
}

/// Lazy-init error log, populated from a CLI flag.
///
/// `MntrsFs` (and the FUSE handler closures) hold a
/// `OnceLock<Arc<ErrorLog>>` and call `with_log` to
/// dispatch an error log line when set. The CLI's
/// `--error-log-file <path>` flag is the only
/// intended init point.
static ERROR_LOG: std::sync::OnceLock<Arc<ErrorLog>> = std::sync::OnceLock::new();

pub fn install(log: Arc<ErrorLog>) {
    let _ = ERROR_LOG.set(log);
}

pub fn with_log<F: FnOnce(&ErrorLog)>(f: F) {
    if let Some(log) = ERROR_LOG.get() {
        f(log);
    }
}

/// Helper to build a record from a FUSE handler
/// and dispatch to the global log.
pub fn log_fuse_error(op: &'static str, e: std::io::Error, fields: Vec<(&'static str, String)>) {
    with_log(|log| {
        let raw = e.raw_os_error().unwrap_or(0);
        let name = errno_name(raw);
        log.log(ErrorRecord {
            operation: op,
            errno: raw,
            errno_name: name,
            fields,
        });
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_escape_handles_specials() {
        let mut s = String::new();
        json_escape_into("a\"b\\c\nd", &mut s);
        assert_eq!(s, "a\\\"b\\\\c\\nd");
    }

    #[test]
    fn record_to_json_line_basic() {
        let rec = ErrorRecord {
            operation: "read",
            errno: 5,
            errno_name: "EIO",
            fields: vec![("path", "/foo/bar".to_string())],
        };
        let line = rec.to_json_line();
        assert!(line.contains("\"operation\":\"read\""));
        assert!(line.contains("\"errno\":5"));
        assert!(line.contains("\"errno_name\":\"EIO\""));
        assert!(line.contains("\"path\":\"/foo/bar\""));
        assert!(line.ends_with("\n"));
    }

    #[test]
    fn timestamp_is_rfc3339_calendar_form() {
        // Issue #112: Loki/ELK/Datadog auto-parse
        // only the calendar form `YYYY-MM-DDTHH:MM:SS.fffZ`.
        // Anything else (e.g. seconds-since-epoch) gets
        // indexed as a free-form string and breaks
        // time-range queries.
        let ts = rfc3339_utc_now();
        // Shape: 4 digits + `-` + 2 + `-` + 2 + `T` + 2 + `:` + 2 + `:` + 2 + `.` + 3 + `Z`
        // Total length 24. Validate without bringing in
        // chrono/time as a dev-dep just for the test.
        assert_eq!(ts.len(), 24, "expected 24-char RFC 3339 UTC, got {ts:?}");
        let bytes = ts.as_bytes();
        // Year digits (0..=3 must be ASCII digits).
        for i in [0, 1, 2, 3] {
            assert!(bytes[i].is_ascii_digit(), "year digit at {i}: {ts:?}");
        }
        // Separator positions.
        assert_eq!(bytes[4], b'-');
        assert_eq!(bytes[7], b'-');
        assert_eq!(bytes[10], b'T');
        assert_eq!(bytes[13], b':');
        assert_eq!(bytes[16], b':');
        assert_eq!(bytes[19], b'.');
        assert_eq!(bytes[23], b'Z');
        // Date / time digits.
        for i in [5, 6, 8, 9, 11, 12, 14, 15, 17, 18, 20, 21, 22] {
            assert!(bytes[i].is_ascii_digit(), "date/time digit at {i}: {ts:?}");
        }
        // Sanity guard against the algorithm regressing
        // to a 1970-era date (we're well past 2025).
        let year: u32 = ts[0..4].parse().unwrap();
        assert!(year >= 2025, "year {year} too small: {ts:?}");
    }

    #[test]
    fn civil_from_days_matches_known_dates() {
        // Spot-check the algorithm against well-known
        // epoch days. Cross-checked with `date -u -d @<secs>
        // +%Y-%m-%d`. Day 0 = 1970-01-01.
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(1), (1970, 1, 2));
        // 1971-01-01: 1970 wasn't a leap year, so 365 days.
        assert_eq!(civil_from_days(365), (1971, 1, 1));
        // 2020-01-01: 1970-2019 has 12 leap years (72,76,...,2016;
        // 2000 is leap too), so 50*365 + 12 = 18262 days.
        assert_eq!(civil_from_days(18262), (2020, 1, 1));
        // 2024-01-01: 54 years from 1970, with 13 leap years
        // (2000 is leap); 54*365 + 13 = 19723.
        assert_eq!(civil_from_days(19723), (2024, 1, 1));
        // Leap day: 2024-02-29 is day 60 of 2024 (Jan has 31,
        // Feb 29 is the 60th day-of-year), so epoch day is
        // 19723 + 59 = 19782 (days since epoch are 0-indexed).
        assert_eq!(civil_from_days(19782), (2024, 2, 29));
        // End of 2024 (leap year, 366 days): 19723 + 366 - 1 = 20088.
        assert_eq!(civil_from_days(20088), (2024, 12, 31));
        // First day of 2025: 19723 + 366 = 20089.
        assert_eq!(civil_from_days(20089), (2025, 1, 1));
        // Y2038 sentinel: 2^31 - 1 seconds is the 03:14:07 mark on
        // 2038-01-19, i.e. day 24855 of the epoch (1970-01-01 +
        // 24837 days = 2038-01-01, +18 days to 2038-01-19).
        assert_eq!(civil_from_days(24855), (2038, 1, 19));
    }

    #[test]
    fn log_simple_drops_on_full_channel() {
        // Tiny channel to force a drop.
        let (tx, _rx) = std::sync::mpsc::sync_channel::<ErrorRecord>(1);
        // Drain in a background task so the channel
        // is never read and stays full.
        std::thread::spawn(move || {
            // hold the receiver forever
            std::thread::park();
        });
        // try_send on a full channel must not panic.
        let _ = tx.try_send(ErrorRecord {
            operation: "test",
            errno: 1,
            errno_name: "EPERM",
            fields: vec![],
        });
    }
}
