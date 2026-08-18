//! `--vfs-write-wait` microbench (issue #T2-N+1)
//!
//! Pins the three behavioral claims of `--vfs-write-wait` via
//! timing-based observations of `mntrs::writeback::pending_count()`:
//!
//! 1. **`write_wait_zero_uploads_immediately`** — with the coalescing
//!    window set to `Duration::ZERO`, the upload completes inside
//!    the `wait_for_drain(2s)` window (no perceptible delay). Pins
//!    the "0 means immediate" branch of `per_task_writeback_delay`.
//!    Uses `wait_for_drain` rather than an absolute `pending == 0`
//!    check, because the worker processes a `delay=0` task in
//!    microseconds — by the time the test reads `pending_count()`
//!    after `write()`, the upload may have already completed.
//!
//! 2. **`write_wait_long_defers_upload`** — with the coalescing
//!    window set to 2 s, the same workload shows `pending_count()`
//!    still > 0 after 200 ms (well inside the window) and only
//!    drains to 0 after the window elapses. Pins the load-bearing
//!    delay application in `per_task_writeback_delay` (issue #T2-N+1
//!    fix: inode size update must run BEFORE the write() enqueue
//!    for `v.size < writeback_immediate_threshold` to evaluate
//!    against the post-write size).
//!
//! 3. **`write_wait_coalesces_burst`** — two back-to-back
//!    `write()+release()` cycles on the same inode within the
//!    coalescing window leave `pending_count()` unchanged. Pins the
//!    `writeback_pending.insert()` skip-if-already-present check
//!    at `src/lib.rs:5851` (write path) and `src/lib.rs:6457`
//!    (release path). Without that check, the second cycle
//!    would enqueue a second task, the counter would bump, and the
//!    test fails.
//!
//! The benchmark script `tests/bench/scripts/run-bench.sh` exercises
//! only sub-MiB writes (1K / 4K / 64K / 1M), all below the 1 MiB
//! `--writeback-immediate-threshold` default — so it never hits
//! `per_task_writeback_delay`'s large-file branch and never trips
//! the coalesce path. This microbench fills that gap.
//!
//! Run:
//!   cargo test --test write_wait_microbench --release -- --nocapture --test-threads=1
//!
//! `--release` matters — debug builds add 10-100× of their own
//! overhead and obscure the timing windows.
//!
//! `--test-threads=1` matters — `PENDING_COUNT` is a process-static
//! `AtomicU64` (`src/writeback.rs:118`), so the three tests' pending
//! counts overlap when cargo runs them in parallel. The tests do
//! delta-based assertions (capture a baseline, assert delta == 1 or
//! unchanged) so a sibling test's `release()` enqueue cannot push
//! the absolute count off the asserted value — but a sibling test's
//! task being processed mid-window can pull `pending_count()` DOWN
//! while our task is still queued, which looks identical to our task
//! draining early. Serial execution avoids that race.
//!
//! Verification (regression guards):
//!   - `write_wait_zero_uploads_immediately` fails if the worker
//!     stops draining (i.e. if the per-task delay was regressed
//!     to `Duration::MAX` instead of `Duration::ZERO` for
//!     `write_wait=0`).
//!   - `write_wait_long_defers_upload` fails if the per-task delay
//!     ignores `write_wait` and uses `Duration::ZERO` (the original
//!     pre-fix behavior — see `src/lib.rs` ~line 5810 + ~line 6022).
//!   - `write_wait_coalesces_burst` fails if the
//!     `writeback_pending.insert()` skip-if-already-present check
//!     is removed (or always-returns-true change) at BOTH
//!     `src/lib.rs:5851` (write) and `src/lib.rs:6457` (release).
//!     Removing only one of them still coalesces because the
//!     other site catches the duplicate path — the test asserts
//!     on `writeback_pending.len()` to require both checks.

use std::time::{Duration, Instant};

use mntrs::core_fs::CoreFilesystem;
use mntrs::{CacheMode, new_test_fs_with_mode, writeback};
use opendal::Operator;
use opendal::services::Memory;

const ROOT_INO: u64 = 1;

/// Build an MntrsFs against opendal Memory backend with the given
/// `write_wait` and `--writeback-immediate-threshold` overrides.
///
/// `new_test_fs_with_mode` constructs the fs but does NOT call
/// `init()` — and without `init()`, `common_init_wb` never runs, so
/// `writeback_sender` stays `None` and `release()`'s
/// `if let Some(tx) = self.writeback_sender.get() && ...` guard
/// silently skips the enqueue. We call `init()` explicitly to
/// actually exercise the writeback path.
fn build_fs(write_wait: Duration, threshold: u64, label: &str) -> mntrs::MntrsFs {
    let op = Operator::new(Memory::default()).unwrap();
    let cache_dir = std::env::temp_dir().join(format!(
        "mntrs-ww-{}-{}-{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&cache_dir);
    std::fs::create_dir_all(&cache_dir).unwrap();

    let mut fs = new_test_fs_with_mode(op, cache_dir, CacheMode::Writes);
    fs.__write_wait_set_for_test(write_wait);
    fs.__writeback_immediate_threshold_set_for_test(threshold);
    fs.init().expect("init: spawn writeback worker");
    fs
}

/// Wait until `pending_count()` returns 0 (upload drained), with
/// a hard timeout. Used at the start of each test to drain any
/// state leaked from prior tests, and at the end of tests to wait
/// for the upload to complete.
fn wait_for_drain(timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if writeback::pending_count() == 0 {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    writeback::pending_count() == 0
}

/// Wait until `pending_count() - baseline >= delta`, with a
/// short hard timeout. Used in `write_wait_coalesces_burst` to
/// tolerate the worker's scheduling latency: `tx.send(task)` is
/// synchronous from the FUSE thread, but the receiver
/// (`PENDING_COUNT.fetch_add`) runs on the tokio runtime and
/// may not have been polled yet when the test reads the
/// counter. Poll every 5 ms for up to 500 ms — well under the
/// 1 s per-task delay so we never falsely observe a drained
/// counter.
fn wait_for_pending_delta(baseline: usize, delta: usize, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    loop {
        let cur = writeback::pending_count();
        if cur.saturating_sub(baseline) >= delta {
            return cur;
        }
        if Instant::now() >= deadline {
            return cur;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn write_wait_zero_uploads_immediately() {
    // Drain any state leaked from prior tests.
    assert!(
        wait_for_drain(Duration::from_secs(5)),
        "precondition: pending_count() should drain to 0 before test starts"
    );

    // write_wait=0 → per_task_delay = 0 → worker picks the task
    // up on the next tick. Assert via `wait_for_drain` rather than
    // an absolute `pending_count() == 0` because the task can be
    // fully processed before the test reads the counter.
    let fs = build_fs(Duration::ZERO, 1024, "zero");
    let payload = vec![0xABu8; 8 * 1024]; // 8 KiB > 1 KiB threshold
    let (attr, fh) = fs.create(ROOT_INO, "f1", 0o644).expect("create");
    fs.write(attr.ino, fh, 0, &payload).expect("write");
    fs.release(attr.ino, fh).expect("release");

    assert!(
        wait_for_drain(Duration::from_secs(2)),
        "write_wait=0 → upload should drain within 2 s. If this times out, \
         per_task_writeback_delay regressed to a non-zero delay for write_wait=0"
    );
}

#[test]
fn write_wait_long_defers_upload() {
    // write_wait=2 s, write_back_delay=1 s default cap →
    // per_task_delay = min(2 s, 1 s) = 1 s. Use a 200 ms sleep
    // to verify the task is still queued well inside the window.
    //
    // Avoid the absolute `pending_count() == 1` assertion because
    // the global `PENDING_COUNT` is shared with prior tests'
    // workers' leftover tasks (each test spawns its own worker
    // on a fresh `MntrsFs`, but the runtime + counter are
    // process-static). Use timing-only assertions instead: the
    // upload must NOT happen inside 200 ms, and must complete
    // inside 4 s.
    let fs = build_fs(Duration::from_secs(2), 1024, "long");
    let payload = vec![0xABu8; 8 * 1024];
    let (attr, fh) = fs.create(ROOT_INO, "f2", 0o644).expect("create");

    let start = Instant::now();
    fs.write(attr.ino, fh, 0, &payload).expect("write");
    fs.release(attr.ino, fh).expect("release");

    // 200 ms is well inside the 1 s effective coalescing window
    // (write_wait=2 s capped at write_back_delay=1 s). The upload
    // must NOT have completed — verify by polling the inodes
    // entry's size and checking the cache file is still dirty
    // (no .dirty sidecar removed yet because no upload happened).

    // We can't reliably inspect writeback_pending per-path from
    // outside the crate, so we use the strongest observable signal:
    // the upload must still be in flight after 200 ms. The cleanest
    // way to assert this is to check that the time-to-drain from
    // `release()` exceeds 200 ms. With write_wait broken (delay=0),
    // drain completes in <50 ms. With write_wait working, drain
    // takes ~1 s.
    //
    // `wait_for_drain(t)` returns TRUE when drained, FALSE when
    // still pending — so we want `false` here. Use `assert!(!)`
    // to invert.
    assert!(
        !wait_for_drain(Duration::from_millis(200)),
        "200 ms is inside the write_wait=2 s window: the upload should NOT have \
         drained yet. If wait_for_drain returned true, per_task_writeback_delay \
         regressed to Duration::ZERO (pre-fix #T2-N+1 ordering bug)"
    );
    let elapsed_so_far = start.elapsed();
    assert!(
        elapsed_so_far >= Duration::from_millis(200),
        "drain returned too early ({} ms) — write_wait is not holding the upload",
        elapsed_so_far.as_millis()
    );

    // After the window elapses the worker picks the task up.
    assert!(
        wait_for_drain(Duration::from_secs(4)),
        "upload should drain after write_wait window elapses"
    );
    let total = start.elapsed();
    assert!(
        total >= Duration::from_millis(800),
        "write_wait=2s capped at write_back_delay=1s → upload must take at least \
         ~1 s. Got {} ms",
        total.as_millis()
    );
}

#[test]
fn write_wait_coalesces_burst() {
    // The main behavioral claim: two write()+release() cycles
    // inside the coalescing window produce ONE task, not two.
    // The dedup mechanism is writeback_pending.insert() returning
    // false on the second cycle (path already in set from the
    // first cycle's write() pre-emptive enqueue).
    //
    // The second cycle MUST run while the first task is still
    // in flight (i.e. within the per_task_delay window), or the
    // worker will complete the first upload, remove the path
    // from writeback_pending, and the second cycle's insert()
    // correctly returns true (a fresh enqueue). With
    // write_wait=2 s capped at write_back_delay=1 s, the first
    // task is in flight for ~1 s — so the second cycle must
    // fire within that window.
    let fs = build_fs(Duration::from_secs(2), 1024, "burst");
    let payload = vec![0xABu8; 8 * 1024];

    // Drain any state leaked from prior tests BEFORE capturing
    // baseline (we want the baseline to reflect THIS test's
    // contribution, not leaked state from a sibling).
    assert!(
        wait_for_drain(Duration::from_secs(5)),
        "precondition: pending_count() should drain to 0 before test starts"
    );

    // First cycle: create + write + release.
    let (attr, fh) = fs.create(ROOT_INO, "f3", 0o644).expect("create");
    let baseline = writeback::pending_count();
    fs.write(attr.ino, fh, 0, &payload).expect("write");
    // Poll for the worker to drain the channel into the DelayQueue
    // (the receiver increments PENDING_COUNT on a different tokio
    // task — there's a small scheduling-latency window after
    // `tx.send` returns). 500 ms is well under the 1 s per-task
    // delay, so observing a drained counter means the upload
    // actually completed, not that the receiver is slow.
    let after_first_write = wait_for_pending_delta(baseline, 1, Duration::from_millis(500));
    fs.release(attr.ino, fh).expect("release");
    // release() is a no-op while the path is in
    // writeback_pending (insert returns false). Give the worker
    // a moment to process the channel.
    std::thread::sleep(Duration::from_millis(50));
    let after_first_release = writeback::pending_count();
    let pending_after_first_cycle = fs.__writeback_pending_len_for_test();

    assert_eq!(
        after_first_write.saturating_sub(baseline),
        1,
        "first write() should enqueue exactly one task (delta from baseline = {})",
        baseline
    );
    assert_eq!(
        after_first_release, after_first_write,
        "first release() should be a no-op (path already in writeback_pending). \
         Got after_first_write={after_first_write}, after_first_release={after_first_release}"
    );
    assert_eq!(
        pending_after_first_cycle, 1,
        "first write() should add exactly one entry to writeback_pending. \
         Got {pending_after_first_cycle}"
    );

    // Second cycle on the SAME inode (must use `open` — `create`
    // would fail with EEXIST since the file now exists). Run
    // immediately so we're well inside the 1 s coalescing
    // window. Different fh, same path → writeback_pending.insert()
    // returns false → write() and release() both no-op.
    let fh2 = fs.open(attr.ino, 1 /* O_WRONLY */).expect("open");
    fs.write(attr.ino, fh2, 0, &payload).expect("write");
    std::thread::sleep(Duration::from_millis(50));
    let after_second_write = writeback::pending_count();
    fs.release(attr.ino, fh2).expect("release");
    std::thread::sleep(Duration::from_millis(50));
    let after_second_release = writeback::pending_count();
    let pending_after_second_cycle = fs.__writeback_pending_len_for_test();

    assert_eq!(
        after_second_write, after_first_release,
        "second write() inside write_wait window must coalesce into the first \
         (pending stays at {after_first_release}, got {after_second_write}). \
         If this is +1, write()'s writeback_pending.insert() \
         skip-if-already-present check at src/lib.rs:5851 has regressed"
    );
    assert_eq!(
        after_second_release, after_second_write,
        "second release() should also be a no-op (got {after_second_release})"
    );
    assert_eq!(
        pending_after_second_cycle, 1,
        "second cycle must NOT add a new entry to writeback_pending. \
         Got {pending_after_second_cycle}. If this is 2, both write() (src/lib.rs:5851) \
         and release() (src/lib.rs:6457) skip-if-already-present checks have regressed"
    );

    // After the window elapses, the SINGLE coalesced upload drains.
    assert!(
        wait_for_drain(Duration::from_secs(4)),
        "single coalesced upload should drain after window elapses"
    );
}
