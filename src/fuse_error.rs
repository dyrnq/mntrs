//! Unified FUSE error handling macro (issue #49).
//!
//! `fuse_error!` reduces the boilerplate at every
//! error return site in the FUSE adapter: it logs
//! the error, calls the structured error log if
//! installed, and dispatches the reply to the
//! kernel — all in one expression. Modelled on
//! mountpoint-s3's `fuse_error!` macro (which adds
//! metrics counters and unique-request tagging on
//! top of log + reply).
//!
//! Usage:
//! ```ignore
//! fuse_error!("read", reply, e, op);
//! ```
//!
//! The macro expects the surrounding function to
//! have a `reply: ReplyX` and an `op: &'static str`
//! (the FUSE op name) in scope. The error value
//! `e: std::io::Error` is mapped to the
//! corresponding fuser `Errno` via the existing
//! `io_err_to_fuse_errno` helper.
//!
//! Three things happen on every error:
//!   1. `tracing::warn!` is called with the op + err
//!   2. `error_log::log_fuse_error` is called (if
//!      installed) so the structured JSON log gets
//!      a record
//!   3. `reply.error(errno)` is dispatched
//!
//! The error log is opt-in (the `--error-log-file`
//! flag, see issue #50); the macro no-ops on it
//! when the log is not installed, so there's no
//! per-call overhead.

/// Unified FUSE error macro.
///
/// Required variables in the calling scope:
///     * `op` — `&'static str` FUSE operation name
///     (e.g. `"read"`, `"write"`, `"lookup"`)
///     * `reply` — the FUSE `Reply*` value
///     * `e` — the `std::io::Error` value
///
/// Effect:
///     * `tracing::warn!(op, error=%e, ...)`
///     * `error_log::log_fuse_error(op, e, vec![])`
///     (no-op if the error log is not installed)
///     * `reply.error(io_err_to_fuse_errno(e))`
///
/// Note: the macro is intentionally lightweight —
/// no metrics counters, no unique-request tagging
/// (those would require hooking the fuser Request
/// type into every call site, which is a much
/// bigger refactor for a small ergonomic win).
/// The structured log (issue #50) is the
/// production-diagnostics channel; tracing
/// provides the human-readable stream.
#[macro_export]
macro_rules! fuse_error {
    ($op:expr, $reply:expr, $e:expr) => {{
        let err = $e;
        ::tracing::warn!(op = $op, error = %err, "fuse op failed");
        $crate::error_log::log_fuse_error($op, err, vec![]);
        $reply.error($crate::core_fs::fuser::io_err_to_fuse_errno(err));
    }};
}

#[cfg(test)]
mod tests {
    // The macro expands to: tracing::warn!, log,
    // reply.error. Each of those is testable in
    // isolation; the macro itself is a thin
    // wrapper. We don't add a test that depends
    // on tracing-test setup — the integration
    // tests in tests/fuse_integration_test.rs
    // exercise the dispatch end-to-end.
}
