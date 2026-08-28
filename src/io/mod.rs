//! IO coordination modules.
//!
//! Currently hosts [`sync`] — the independent IO runtime that mirrors
//! rclone's `fs/sync` worker pool. See `sync.rs` for the architectural
//! rationale.

pub(crate) mod sync;
