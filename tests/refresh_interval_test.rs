//! `--vfs-refresh` integration test (issue #592 / PR #593).
//!
//! Pins the three behavioral claims of the periodic refresh worker
//! that `MntrsFs::spawn_refresh_worker` (src/lib.rs:1857) drives
//! on `crate::rt()`:
//!
//! 1. **`refresh_interval_zero_does_not_spawn_worker`** — with the
//!    interval at the default `Duration::ZERO`, `init()` must
//!    fast-path through `spawn_refresh_worker` and never spawn the
//!    periodic task. The proof is observable: caches stay populated
//!    even after sleeping well past the default "would have fired"
//!    window. Pins the conservative default (the explicit doc-comment
//!    claim at src/lib.rs:1844: "No-op when the interval is zero
//!    (the conservative default — opt-in)").
//!
//! 2. **`refresh_interval_nonzero_clears_caches_after_tick`** —
//!    with the interval set to 100 ms, the spawned task fires once
//!    after the first tick (the spawned task itself skips the
//!    immediate first tick — see src/lib.rs:1874) and clears both
//!    `dir_cache` and `attr_cache`. Sleep 250 ms (≥2 ticks) and
//!    verify both cache lengths drop to 0. Pins the
//!    **side-effect** contract — clearing is the visible behavior,
//!    not the scheduling.
//!
//! 3. **`refresh_worker_preserves_inodes`** — the refresh worker
//!    clears `dir_cache` and `attr_cache` but MUST NOT touch
//!    `inodes`. The FUSE kernel holds `ino` references that
//!    would dangle if we removed the entries. Set up an inode,
//!    sleep past the refresh interval, verify the inode is still
//!    there. This pins the "what the worker does NOT touch"
//!    half of the contract — without this test, a future refactor
//!    that accidentally clears `inodes` too would only fail at
//!    runtime under FUSE, never in CI.
//!
//! Test shims (mirrors `__write_wait_set_for_test` /
//! `__writeback_immediate_threshold_set_for_test` from PR #594):
//!
//! - `__refresh_interval_set_for_test(d)` — override interval
//! - `__dir_cache_len_for_test()` — observe dir_cache length
//! - `__attr_cache_len_for_test()` — observe attr_cache length
//! - `__inodes_len_for_test()` — observe inodes length
//!
//! Run:
//!   cargo test --test refresh_interval_test -- --nocapture --test-threads=1
//!
//! `--test-threads=1` matters because the test depends on
//! wall-clock timing: a sibling test running on a different thread
//! could wake the OS scheduler and let our `init()` task progress
//! faster than expected, but the inverse is also true — a sibling
//! hogging CPU could slow our scheduler. Serial execution keeps the
//! timing assumptions stable.

use std::time::{Duration, Instant};

use mntrs::core_fs::CoreFilesystem;
use mntrs::new_test_fs_with_mode;
use mntrs::util::CacheMode;
use opendal::Operator;
use opendal::services::Memory;

const ROOT_INO: u64 = 1;

/// Build a fresh MntrsFs against opendal Memory backend with
/// the requested `refresh_interval`. Calls `init()` so the
/// refresh worker is actually spawned (or fast-pathed out).
fn build_fs(refresh_interval: Duration, label: &str) -> mntrs::MntrsFs {
    let op = Operator::new(Memory::default()).unwrap();
    let cache_dir = std::env::temp_dir().join(format!(
        "mntrs-refresh-{}-{}-{}",
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
    fs.__refresh_interval_set_for_test(refresh_interval);
    fs.init()
        .expect("init: spawn refresh worker (or fast-path)");
    fs
}

/// Wait until the predicate is true, with a hard timeout.
fn wait_until(timeout: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if pred() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    pred()
}

#[test]
fn refresh_interval_zero_does_not_spawn_worker() {
    // Default (Duration::ZERO) → fast-path: spawn_refresh_worker
    // returns without spawning. So caches populated by `create` /
    // `getattr` before mount-time MUST stay populated even after
    // sleeping well past the "would-have-fired" window (100 ms+).
    //
    // We pick 300 ms — three times the 100 ms tick that test #2 uses.
    // If a regression makes `refresh_interval=0` accidentally spawn
    // a 100 ms-tick worker, the cache will clear in ~200 ms and
    // this test fails.
    let fs = build_fs(Duration::ZERO, "zero");

    // `create()` populates `dir_cache` (via `cache_add_entry` —
    // src/lib.rs:3125) for the parent path. `getattr()` populates
    // `attr_cache` (via `stat_op` — src/lib.rs:2666).
    let (attr, fh) = fs
        .create(ROOT_INO, "warm_attr_target", 0o644)
        .expect("create");
    fs.release(attr.ino, fh).expect("release");

    // `create()` populates `dir_cache` for the parent path via
    // `cache_add_entry` (src/lib.rs:3125). `attr_cache` is not
    // guaranteed to be populated by `create` + `getattr` because
    // `getattr` takes the inodes fast-path (src/lib.rs:4143) for
    // entries with an inodes mtime — `stat_op` (the only writer
    // of `attr_cache`) is skipped. That's OK: the load-bearing
    // signal for "worker not spawned" is `dir_cache` surviving.
    // The refresh worker also clears `attr_cache`, but if the test
    // never populated it, it stays at 0 throughout.
    let dir_before = fs.__dir_cache_len_for_test();
    assert!(
        dir_before >= 1,
        "precondition: create should populate dir_cache. dir_before={dir_before}"
    );

    // Sleep past the "would-have-fired" window. If the worker
    // was (incorrectly) spawned with a 100ms tick, dir_cache
    // will clear here.
    std::thread::sleep(Duration::from_millis(300));

    let dir_after = fs.__dir_cache_len_for_test();
    assert_eq!(
        dir_after, dir_before,
        "dir_cache cleared ({dir_before} → {dir_after}) with refresh_interval=0. \
         The worker should not have been spawned."
    );
}

#[test]
fn refresh_interval_nonzero_clears_caches_after_tick() {
    // refresh_interval=100ms → worker spawns, fires after the
    // first tick (~100 ms; the spawned task consumes the first
    // immediate tick — see src/lib.rs:1874). Sleep 250 ms (well
    // past two ticks) and verify both caches are empty.
    let fs = build_fs(Duration::from_millis(100), "nonzero");

    // `create()` populates `dir_cache`; `getattr()` populates
    // `attr_cache`.
    let (attr, fh) = fs
        .create(ROOT_INO, "warm_attr_target", 0o644)
        .expect("create");
    fs.release(attr.ino, fh).expect("release");

    // `create()` populates `dir_cache` (via `cache_add_entry` —
    // src/lib.rs:3125). For `attr_cache`, the natural path is
    // `lookup()` → `stat_op_async` (src/lib.rs:3868) → cache
    // insert (src/lib.rs:2682), but `lookup` short-circuits on
    // a `dir_cache` hit (src/lib.rs:3820) — and the just-created
    // entry IS in `dir_cache`. So `attr_cache` stays empty in
    // the test setup. The `__attr_cache_insert_for_test` shim
    // lets us seed `attr_cache` directly to verify the worker
    // also clears it (the worker's body at src/lib.rs:1891-1892
    // clears both maps in the same tick).
    fs.__attr_cache_insert_for_test("synthetic_attr_target");

    assert!(
        fs.__dir_cache_len_for_test() >= 1 && fs.__attr_cache_len_for_test() >= 1,
        "precondition: both caches should be populated before refresh worker fires. \
         dir_cache_len={}, attr_cache_len={}",
        fs.__dir_cache_len_for_test(),
        fs.__attr_cache_len_for_test()
    );

    // Wait for both caches to drain. The worker fires every 100 ms;
    // a 2-second hard timeout covers scheduling latency on a busy CI
    // runner.
    let drained = wait_until(Duration::from_secs(2), || {
        fs.__dir_cache_len_for_test() == 0 && fs.__attr_cache_len_for_test() == 0
    });
    assert!(
        drained,
        "refresh_interval=100ms should clear dir_cache and attr_cache within 2s. \
         dir_cache_len={}, attr_cache_len={}",
        fs.__dir_cache_len_for_test(),
        fs.__attr_cache_len_for_test()
    );
}

#[test]
fn refresh_worker_preserves_inodes() {
    // The refresh worker MUST NOT touch `inodes`. FUSE kernel holds
    // `ino` references that would dangle if the entry disappeared
    // from the inodes DashMap. This is the "what the worker does
    // NOT do" half of the contract — without this test, a future
    // refactor that accidentally adds `self.inodes.clear()` to the
    // worker's body would only manifest under FUSE at runtime,
    // never in CI.
    let fs = build_fs(Duration::from_millis(100), "preserve-inodes");

    // Create one file → one inode entry (root is ino=1; the new
    // file gets ino=2).
    let (attr, fh) = fs.create(ROOT_INO, "preserve_me", 0o644).expect("create");
    fs.release(attr.ino, fh).expect("release");

    let inodes_before = fs.__inodes_len_for_test();
    assert!(
        inodes_before >= 2,
        "precondition: root (ino=1) + new file should both be in inodes; got {inodes_before}"
    );

    // Sleep past two refresh ticks. The worker will fire and clear
    // dir_cache + attr_cache, but MUST leave inodes intact.
    std::thread::sleep(Duration::from_millis(250));

    let inodes_after = fs.__inodes_len_for_test();
    assert_eq!(
        inodes_after, inodes_before,
        "inodes mutated by refresh worker ({inodes_before} → {inodes_after}). \
         The worker must NOT clear inodes — FUSE holds ino refs that would dangle."
    );

    // And the inode for our specific file is still resolvable —
    // not just present in the map, but usable.
    let _ = fs
        .getattr(attr.ino)
        .expect("getattr on preserved inode should succeed");
}
