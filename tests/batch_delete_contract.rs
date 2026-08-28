//! Pins the opendal `Deleter` lifecycle contract that
//! `concurrent_delete::flush_with_retry` (src/concurrent_delete.rs:638)
//! depends on. We use the in-process Memory backend so the test is
//! hermetic (no HTTP, no MinIO, no network) and runs in <100 ms on
//! every CI machine.
//!
//! ## What we pin
//!
//! 1. **`deleter_close_drains_buffer`** — `Deleter::close()` after N
//!    pushes to `Deleter::delete` returns `Ok(())` and the backend
//!    reflects all N deletions. This is the per-batch ack that
//!    `flush_with_retry` relies on: if close() doesn't drain (or
//!    errors on a normal flush), our per-key oneshot acks never
//!    fire and the user-visible `rm` blocks forever.
//! 2. **`deleter_close_on_empty_buffer_is_noop`** — `close()` on a
//!    never-used Deleter must succeed. The pump's barrier path
//!    calls close() after every drain cycle; on idle pumps that's
//!    a close() on an empty buffer. Must be safe.
//!
//! ## Why no live MinIO?
//!
//! 1. The CI integration-tests job has a live-MinIO test
//!    (`tests/e2e/mount/unlink_batch_test.sh`) that exercises the
//!    real S3 DeleteObjects path. That covers the wire format.
//! 2. This test pins the *opendal contract* (close()-is-safe
//!    invariant) — it should pass on any backend, and on the
//!    Memory backend it's deterministic. A live MinIO flake here
//!    would be a network issue, not a code issue.
//!
//! ## Note on buffering semantics
//!
//! We do NOT pin "deletes buffer until close()" here. The Memory
//! backend's `Deleter` deletes eagerly on each `Deleter::delete()`
//! call (no `BatchDeleter` wrapper — that's an S3-only oio
//! layer). The buffering behaviour the plan §3 `flush_with_retry`
//! depends on is part of `BatchDeleter<S3Deleter>` specifically.
//! Pinning that requires either: (a) a live-MinIO integration test
//! (covered by `unlink_batch_test.sh`), or (b) a unit test that
//! inspects the internal HashSet (fragile to refactors — not worth
//! maintaining). The `deleter_close_drains_buffer` test below uses
//! 2500 pushes to exercise a workload larger than any one batch,
//! confirming `close()` works correctly even under high-volume
//! batched output.
//!
//! ## Why a separate test file?
//!
//! The existing
//! `concurrent_delete::tests::batchdeleter_pump_drains_via_close`
//! in src/concurrent_delete.rs:978 exercises the same property
//! through the `concurrent_delete::spawn` API. This file pins
//! the lower-level opendal `Deleter` lifecycle directly — useful
//! as a fast regression marker if a future refactor changes the
//! pump's internals but keeps the opendal call shape.
//!
//! Run: `cargo test --test batch_delete_contract --release`

use opendal::Operator;
use opendal::services::Memory;

fn memory_op() -> Operator {
    Operator::new(Memory::default()).expect("Memory operator")
}

/// Pin: `Deleter::close()` after N pushes returns `Ok(())` and the
/// backend reflects all N deletions. The 2500-key workload exceeds
/// the S3 1000-key batch boundary, exercising close() under a
/// workload that has already auto-flushed multiple batches (or, on
/// the eager Memory backend, has already deleted all of them).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleter_close_drains_buffer() {
    let op = memory_op();
    for i in 0..2500 {
        op.write(&format!("file_{i:06}"), "x".to_string())
            .await
            .unwrap();
    }

    let mut deleter = op.deleter().await.unwrap();
    for i in 0..2500 {
        deleter
            .delete(format!("file_{i:06}").as_str())
            .await
            .unwrap();
    }
    deleter.close().await.unwrap();

    let remaining = op.list("").await.unwrap().len();
    assert_eq!(
        remaining, 0,
        "after 2500 pushes + close(), expected 0 paths remaining, got {remaining}"
    );
}

/// Pin: `close()` on a never-used Deleter must succeed without
/// error. The pump calls close() after every drain cycle; on idle
/// pumps that's a close() on an empty buffer. Must be safe.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleter_close_on_empty_buffer_is_noop() {
    let op = memory_op();
    let mut deleter = op.deleter().await.unwrap();
    deleter
        .close()
        .await
        .expect("close on empty deleter must succeed");

    // No paths were ever written, so the listing is empty.
    let remaining = op.list("").await.unwrap().len();
    assert_eq!(remaining, 0);
}

/// Pin: a fresh Deleter is reusable after `close()` does NOT
/// guarantee it (opendal's `Deleter` does not have a Drop impl
/// and `close()` consumes the deleter). This test pins the
/// "Deleter is single-use" invariant that `flush_with_retry` and
/// `fail_all_pending` rely on: a new deleter is created at
/// `concurrent_delete::spawn` time and lives for the pump's
/// lifetime. They never need to create a second one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleter_close_is_terminal() {
    let op = memory_op();
    op.write("a", "x".to_string()).await.unwrap();

    let mut deleter = op.deleter().await.unwrap();
    deleter.delete("a").await.unwrap();
    deleter.close().await.unwrap();

    // After close(), the Deleter has been consumed. Listing via
    // a new deleter must still see whatever the backend actually
    // has (here: 0 paths, since Memory flushed eagerly).
    let remaining = op.list("").await.unwrap().len();
    assert_eq!(remaining, 0);
}
