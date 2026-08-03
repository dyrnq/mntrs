//! Issue #502 — tests for the macFUSE 64 KiB xattr
//! silent-truncation warning + `--max-xattr-size` CLI gate.
//!
//! The warn is a `tracing::warn!` in production; the integration
//! test assertions ride on a per-instance atomic counter
//! (`MntrsFs::xattr_oversize_warn_count`) that the warn path
//! bumps. This avoids wiring a `tracing-subscriber` Layer into
//! the test binary — the test surface stays narrow and the
//! counter is the deterministic contract.
//!
//! Per-instance (not a global `static`) so concurrent tests
//! don't race on a shared atomic. The earlier static-counter
//! design failed under `--test-threads=2+` because each test's
//! `reset` raced with another test's `fetch_add`.
//!
//! The four cases below cover the issue's verification list:
//!
//! 1. `setxattr_above_default_cap_warns` — default 64 KiB cap,
//!    a 65 KiB value bumps the counter exactly once.
//! 2. `setxattr_below_default_cap_no_warn` — 64 KiB value
//!    (at the boundary, not over) leaves the counter untouched.
//! 3. `max_xattr_size_zero_disables_warn` — cap = 0, even a
//!    1 MiB value still passes silently (user has opted into
//!    trusting the kernel/backend combo).
//! 4. `max_xattr_size_custom_threshold_warns` — cap = 32 KiB,
//!    a 40 KiB value bumps the counter exactly once.
//!
//! Note: the `--metadata` gate (issue #500) sits *before* the
//! size check, so the test `MntrsFs` instances must have it
//! enabled. `__metadata_set_for_test(true)` is the project's
//! test-only shim for that — same pattern as `xattr_set_test.rs`.

use mntrs::core_fs::CoreFilesystem;
use mntrs::new_test_fs;
use opendal::Operator;
use opendal::services::Memory;

/// Build a fresh `MntrsFs` backed by opendal's Memory backend.
fn make_fs() -> mntrs::MntrsFs {
    let op = Operator::new(Memory::default()).unwrap();
    let dir = std::env::temp_dir().join(format!("mntrs-xattr-cap-test-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let mut fs = new_test_fs(op, dir);
    // Enable the --metadata write surface (issue #500); the
    // size warn runs *after* the gate so the test must pass
    // the gate first.
    fs.__metadata_set_for_test(true);
    fs
}

/// Build a small file so setxattr has a real ino to operate on.
/// Default-cap tests just need a 1-byte file; the oversize value
/// goes into the xattr, not the file content.
fn seed_file(fs: &mntrs::MntrsFs, name: &str) -> u64 {
    let (attr, fh) = fs.create(1, name, 0o644).expect("create should succeed");
    fs.write(attr.ino, fh, 0, b"hi")
        .expect("write should succeed");
    fs.flush(attr.ino, fh).expect("flush should succeed");
    fs.release(attr.ino, fh).expect("release should succeed");
    attr.ino
}

// ── 1. default cap (64 KiB) + over-sized value ─────────────────────

/// Default cap (64 KiB, set by `new_test_fs` for production defaults
/// — though the test fs uses 0 by default, we set the field
/// explicitly to model the CLI default). A 65 KiB value bumps
/// the counter exactly once.
#[test]
fn setxattr_above_default_cap_warns() {
    let mut fs = make_fs();
    // Issue #502: new_test_fs defaults to 0 (warn disabled) so
    // existing tests don't trip. For this test we want the
    // production default of 64 KiB.
    fs.__max_xattr_size_set_for_test(64 * 1024);
    let ino = seed_file(&fs, "above.txt");
    fs.__xattr_oversize_warn_reset_for_test();
    let before = fs.__xattr_oversize_warn_count_for_test();

    // 65 KiB — 1 byte over the cap.
    let oversize = vec![b'x'; 65 * 1024];
    // We don't care about the return value here: the Memory
    // backend (with --metadata enabled) doesn't persist the
    // user_metadata map, so setxattr may return an other()
    // error. The warning fires *before* the backend round-trip,
    // so the warn path is independent of the write outcome.
    let _ = fs.setxattr(ino, "user.note", &oversize, 0);

    let after = fs.__xattr_oversize_warn_count_for_test();
    assert_eq!(
        after - before,
        1,
        "expected exactly one oversize-warn bump for 65 KiB input under 64 KiB cap"
    );
}

// ── 2. default cap + at-the-boundary value (no warn) ──────────────

/// 64 KiB is *at* the cap, not over it. The check is `value.len() > max`
/// (strict), so the boundary value must NOT fire the warn.
#[test]
fn setxattr_below_default_cap_no_warn() {
    let mut fs = make_fs();
    fs.__max_xattr_size_set_for_test(64 * 1024);
    let ino = seed_file(&fs, "at_cap.txt");
    fs.__xattr_oversize_warn_reset_for_test();
    let before = fs.__xattr_oversize_warn_count_for_test();

    // Exactly 64 KiB — at the cap. The check is strict `-`,
    // not `>=`, so this is benign.
    let at_cap = vec![b'x'; 64 * 1024];
    let _ = fs.setxattr(ino, "user.note", &at_cap, 0);

    let after = fs.__xattr_oversize_warn_count_for_test();
    assert_eq!(
        after - before,
        0,
        "expected NO oversize-warn bump for value exactly at the cap"
    );
}

// ── 3. cap = 0 disables the warning entirely ──────────────────────

/// `max_xattr_size = 0` is the documented "I trust the kernel"
/// opt-out. Even a 1 MiB value must pass silently.
#[test]
fn max_xattr_size_zero_disables_warn() {
    let mut fs = make_fs();
    // The default is already 0, but be explicit so the test's
    // intent is obvious at the call site.
    fs.__max_xattr_size_set_for_test(0);
    let ino = seed_file(&fs, "no_warn.txt");
    fs.__xattr_oversize_warn_reset_for_test();
    let before = fs.__xattr_oversize_warn_count_for_test();

    // 1 MiB — well over the typical 64 KiB cap, but cap is 0.
    let huge = vec![b'x'; 1024 * 1024];
    let _ = fs.setxattr(ino, "user.note", &huge, 0);

    let after = fs.__xattr_oversize_warn_count_for_test();
    assert_eq!(
        after - before,
        0,
        "expected NO oversize-warn bump when cap is 0 (warning disabled)"
    );
}

// ── 4. custom cap threshold fires the warn ────────────────────────

/// Custom cap (32 KiB) is respected: a 40 KiB value bumps the
/// counter exactly once. This is the "user picked a smaller
/// cap on purpose" path.
#[test]
fn max_xattr_size_custom_threshold_warns() {
    let mut fs = make_fs();
    fs.__max_xattr_size_set_for_test(32 * 1024);
    let ino = seed_file(&fs, "custom.txt");
    fs.__xattr_oversize_warn_reset_for_test();
    let before = fs.__xattr_oversize_warn_count_for_test();

    // 40 KiB — over the 32 KiB custom cap.
    let over = vec![b'x'; 40 * 1024];
    let _ = fs.setxattr(ino, "user.note", &over, 0);

    let after = fs.__xattr_oversize_warn_count_for_test();
    assert_eq!(
        after - before,
        1,
        "expected exactly one oversize-warn bump for 40 KiB input under 32 KiB custom cap"
    );
}
