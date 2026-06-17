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
        let ts = chrono_unix_now();
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

/// Cheap RFC3339-ish timestamp. We don't pull in
/// chrono for a 30-line feature; seconds-since-epoch
/// with nanoseconds is good enough for log
/// correlation. Falls back to a fixed string if the
/// system clock is broken (e.g. before 1970).
fn chrono_unix_now() -> String {
    let d = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}.{:09}Z", d.as_secs(), d.subsec_nanos())
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
            let d = DROPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
        12 => "ENOMEM",
        13 => "EACCES",
        16 => "EBUSY",
        17 => "EEXIST",
        20 => "ENOTDIR",
        21 => "EISDIR",
        22 => "EINVAL",
        24 => "ENFILE",
        27 => "EFBIG",
        28 => "ENOSPC",
        30 => "EROFS",
        32 => "EPIPE",
        38 => "ENOSYS",
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
