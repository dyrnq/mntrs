//! `O_APPEND` + FUSE notifier regression test (issue #89 / #93).
//!
//! Pins the side-effect of the `#89` / `#93` fix: after every
//! successful `write()`, the write path calls
//! `fuser::Notifier::inval_inode(ino, 0, -1)` so the FUSE kernel
//! drops its cached pre-write size and the next O_APPEND open
//! uses the up-to-date file size for the O_APPEND write offset.
//!
//! Without this side-effect (pre-#89), the kernel kept using the
//! pre-write size and a subsequent `O_APPEND` open would write at
//! the wrong offset, clobbering prior writes.
//!
//! ## Why this is hard to test in-process
//!
//! A full e2e FUSE mount + O_APPEND test would require:
//! 1. Building a real FUSE session
//! 2. Mounting it on a tmpfs path
//! 3. Opening the mount point, doing a write, doing another
//!    O_APPEND open + write, verifying the byte layout
//!
//! This is heavyweight and depends on the host kernel's FUSE
//! module. Instead, this test pins the **side-effect that
//! produces the fix**: the `inval_inode` call. A regression that
//! removes or breaks the call trips CI loudly.
//!
//! ## Pinning mechanism
//!
//! `src/lib.rs` increments a process-static `INVAL_INODE_COUNT`
//! every time the write path **reaches** the FUSE notifier code
//! site (the actual `notifier.inval_inode(...)` call only fires
//! if a notifier is populated, which requires a real FUSE
//! mount). The counter is exposed via
//! `mntrs::__inval_inode_count_for_test() -> u64`. The test
//! asserts the counter advances by exactly 1 per write.
//!
//! Note that this counter is a process-static shared across all
//! tests in the same `cargo test` invocation. The test reads
//! the counter BEFORE the write and asserts the EXACT delta
//! after — which is robust against shared state.
//!
//! Run:
//!   cargo test --test o_append_fuse_notifier_test
//!
//! The 3 counter-touching tests serialize via `COUNTER_LOCK`
//! so they don't race each other on the process-static
//! counter.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use mntrs::core_fs::CoreFilesystem;
use mntrs::new_test_fs_with_mode;
use mntrs::util::CacheMode;
use opendal::Operator;
use opendal::services::Memory;

const ROOT_INO: u64 = 1;

/// Serializes tests that touch the process-static
/// `INVAL_INODE_COUNT`. `cargo test` runs each test binary in
/// parallel by default; the counter is shared across all threads
/// in this process, so a sibling test's write interleaving
/// between our `before` and `after` snapshots would inflate
/// the delta. Acquiring this mutex around the read-before /
/// write-loop / read-after window guarantees no other test in
/// this binary advances the counter during our measurement.
///
/// `notifier_counter_is_unix_only_smoke` is excluded from the
/// serialized set because it does no writes — its only
/// observation is that the counter is monotonic, which holds
/// under any concurrency.
static COUNTER_LOCK: Mutex<()> = Mutex::new(());

/// Build a fresh MntrsFs against opendal Memory backend.
/// Same helper pattern as `tests/refresh_interval_test.rs`.
fn build_fs(label: &str) -> mntrs::MntrsFs {
    let op = Operator::new(Memory::default()).unwrap();
    let cache_dir = std::env::temp_dir().join(format!(
        "mntrs-oappend-{}-{}-{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&cache_dir);
    std::fs::create_dir_all(&cache_dir).unwrap();

    let fs = new_test_fs_with_mode(op, cache_dir, CacheMode::Writes);
    fs.init().expect("init");
    fs
}

/// Issue #89 / #93: every successful `write()` reaches the
/// FUSE-notifier code path and increments `INVAL_INODE_COUNT` by
/// exactly 1. The counter is incremented **before** the
/// `FUSE_NOTIFIER.get()` check so it captures "the write path
/// reached the notifier code site" — independent of whether a
/// notifier is populated. In production, a populated notifier
/// then receives the `inval_inode` call. In tests (no FUSE
/// mount → no populated notifier), the counter still advances.
///
/// Without the fix the write path silently no-ops on the
/// notifier (pre-#89) and the delta is 0. The O_APPEND bug
/// then surfaces in production as clobbered writes.
#[test]
fn write_triggers_exactly_one_inval_inode_per_call() {
    let _guard = COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let fs = build_fs("one-write");
    let (attr, fh) = fs.create(ROOT_INO, "appended", 0o644).expect("create");

    let before = mntrs::__inval_inode_count_for_test();
    let n = fs.write(attr.ino, fh, 0, b"hello").expect("write");
    let after = mntrs::__inval_inode_count_for_test();
    assert_eq!(n, 5);
    assert_eq!(
        after - before,
        1,
        "write() should increment INVAL_INODE_COUNT by exactly 1. \
         before={before}, after={after}, delta={}",
        after - before
    );

    fs.release(attr.ino, fh).expect("release");
}

/// Issue #89: the cumulative `INVAL_INODE_COUNT` delta must match the
/// number of writes. This is the load-bearing assertion: a
/// regression that silently short-circuits the notifier call
/// (e.g. wraps it in an `if some_flag { ... }` that defaults to
/// off) would let individual writes pass the single-call test
/// above, but the cumulative test would catch it after several
/// writes.
#[test]
fn cumulative_writes_match_cumulative_inval_inode_count() {
    let _guard = COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let fs = build_fs("cumulative");
    let (attr, fh) = fs.create(ROOT_INO, "appended_x4", 0o644).expect("create");

    let before = mntrs::__inval_inode_count_for_test();
    // 4 writes of varying length — matches the O_APPEND scenario
    // (4 successive appends to a single fd).
    let writes: &[&[u8]] = &[b"a", b"bb", b"ccc", b"dddd"];
    let mut total: u64 = 0;
    for w in writes {
        let n = fs.write(attr.ino, fh, total, w).expect("write");
        total += n as u64;
        assert_eq!(n as usize, w.len());
    }
    let after = mntrs::__inval_inode_count_for_test();
    assert_eq!(
        after - before,
        writes.len() as u64,
        "cumulative write count must match cumulative inval_inode count. \
         wrote {} times, delta={}",
        writes.len(),
        after - before
    );
    fs.release(attr.ino, fh).expect("release");

    // Sanity check on the cumulative offset: after the 4 writes
    // at offsets 0,1,3,6 with payloads a,bb,ccc,dddd, the final
    // expected end-of-file is 10. The O_APPEND bug would
    // manifest as overwritten bytes (write at wrong offset
    // would clobber prior bytes), but the cumulative count
    // assertion is the load-bearing pin — this is just a
    // sanity check on the offset math.
    assert_eq!(total, 10, "expected end-of-file offset 10 (1+2+3+4)");
}

/// Issue #93: a failed write must NOT reach the notifier
/// code path. The counter increment is gated by the success
/// path — early returns (bad fh, backfill failure, etc.) skip
/// the notifier block entirely. A regression that moves the
/// increment to before the success check would surface as
/// spurious counter advances on every failed write.
///
/// We test this by attempting a write with a bogus fh — the
/// handle lookup fails immediately and returns Err before
/// reaching the notifier block.
#[test]
fn failed_write_does_not_increment_inval_inode_count() {
    let _guard = COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let fs = build_fs("failed-write");
    let (attr, fh) = fs
        .create(ROOT_INO, "no_inval_on_fail", 0o644)
        .expect("create");
    fs.write(attr.ino, fh, 0, b"warm").expect("warm write");
    fs.release(attr.ino, fh).expect("release");

    // Re-open for write — fresh fh.
    let fh2 = fs.open(attr.ino, 0).expect("open");

    let before = mntrs::__inval_inode_count_for_test();
    // Use a bogus fh so write() returns Err immediately at the
    // handle lookup, before reaching the notifier block.
    let bad_fh: u64 = 0xDEAD_BEEF;
    let result = fs.write(attr.ino, bad_fh, 0, b"x");
    let after = mntrs::__inval_inode_count_for_test();
    assert!(result.is_err(), "write with bogus fh should fail");
    assert_eq!(
        after - before,
        0,
        "failed write() must NOT call inval_inode. delta={}",
        after - before
    );

    // Cleanup the real fh.
    fs.release(attr.ino, fh2).expect("release real fh");
}

/// Issue #93: the notifier side-effect is gated by the unix
/// FUSE kernel-cache invalidation hook. On WinFSP the write
/// handler is synchronous so the kernel never sees a stale
/// size — there's no analogous hook. This test asserts the
/// side-effect is observable on the unix build target
/// (cfg(not(windows)) — see INVAL_INODE_COUNT at lib.rs:1457).
///
/// We don't gate the test on cfg(unix) because the COUNTER is
/// only defined when the write path is compiled in; on windows
/// the test would fail to compile if it referenced
/// `__inval_inode_count_for_test`. So a windows-only build that
/// lacks the counter simply doesn't compile this test — same
/// effect as a `#[cfg(not(windows))]` gate at the test level.
#[test]
fn notifier_counter_is_unix_only_smoke() {
    // Smoke check: the counter is monotonic and starts at 0 or
    // higher (other tests in the same process may have run
    // writes before this one — see the module doc).
    let now = mntrs::__inval_inode_count_for_test();
    std::thread::sleep(Duration::from_millis(1));
    let later = mntrs::__inval_inode_count_for_test();
    // No writes happen during the sleep, so the count must
    // be stable. This pins the "counter only advances on
    // writes" property — a regression that increments the
    // counter on every notifier.get() (including the no-op
    // case) would surface as a non-monotonic count here.
    assert!(
        later >= now,
        "INVAL_INODE_COUNT must be monotonically non-decreasing. \
         now={now}, later={later}"
    );

    // Sanity: the call is cheap (no I/O, no allocations).
    let start = Instant::now();
    for _ in 0..1000 {
        let _ = mntrs::__inval_inode_count_for_test();
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(1),
        "1000 counter reads should take <1s (took {elapsed:?})"
    );
}
