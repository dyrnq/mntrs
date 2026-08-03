//! Issue #500 — tests for the rclone-parity xattr *write*
//! surface on `MntrsFs`. The read surface (getxattr /
//! listxattr) is covered in `tests/xattr_metadata_test.rs`;
//! this file adds the write-path contract:
//!
//! * `--metadata` gate (rclone parity — surface disabled →
//!   `Unsupported`)
//! * `user.*` namespace filter (only `user.<key>` is writable;
//!   well-known derived names + non-user namespaces →
//!   `Unsupported`)
//! * UTF-8 validation (opendal's user_metadata map is
//!   `HashMap<String, String>` — non-UTF-8 → `InvalidInput`)
//! * Full setxattr → getxattr roundtrip (writes via
//!   `op.write_with(...).user_metadata(map)`; subsequent
//!   stat returns the merged map)
//! * removexattr drops the named key (subsequent getxattr
//!   returns NotFound; removexattr on absent key also returns
//!   NotFound, not Ok — POSIX semantics)
//!
//! Backends used:
//!   * `Memory` for the gate / namespace / UTF-8 /
//!     NotFound-when-absent cases (no `user_metadata`
//!     roundtrip support — see writer.rs:51 in
//!     opendal-core-0.57/src/services/memory, which only
//!     persists cache_control / content_disposition /
//!     content_type / content_encoding from OpWrite).
//!   * `Fs` (Unix-only) for the full roundtrip — the opendal
//!     fs backend advertises `write_with_user_metadata: true`
//!     (backend.rs:148 in opendal-service-fs-0.58.1) and
//!     persists the metadata as xattrs on the tempdir file.
//!
//! All tests drive the public `CoreFilesystem` API
//! (`setxattr` / `removexattr` / `getxattr`) — same surface
//! the fuser adapter routes into.

use mntrs::core_fs::CoreFilesystem;
use mntrs::new_test_fs;
use opendal::Operator;
use opendal::services::Memory;

/// Build a fresh `MntrsFs` backed by opendal's Memory backend.
fn make_fs() -> mntrs::MntrsFs {
    let op = Operator::new(Memory::default()).unwrap();
    let dir = std::env::temp_dir().join(format!("mntrs-xattr-set-test-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    new_test_fs(op, dir)
}

/// `MntrsFs` with the rclone `--metadata` surface enabled.
/// Default `new_test_fs` disables it (rclone mount parity);
/// the `__metadata_set_for_test(true)` shim is the only way
/// to flip it from outside the crate.
fn enabled_fs() -> mntrs::MntrsFs {
    let mut fs = make_fs();
    fs.__metadata_set_for_test(true);
    fs
}

/// Write a small file via the public API so we have a real
/// inode to query. Returns the ino so callers can drive the
/// xattr methods.
fn write_file(fs: &mntrs::MntrsFs, name: &str, bytes: &[u8]) -> u64 {
    let (attr, fh) = fs.create(1, name, 0o644).expect("create should succeed");
    fs.write(attr.ino, fh, 0, bytes)
        .expect("write should succeed");
    fs.flush(attr.ino, fh).expect("flush should succeed");
    fs.release(attr.ino, fh).expect("release should succeed");
    attr.ino
}

/// Assert that an io::Error is Unsupported (issue #500 gate
/// contract — the `--metadata` opt-in + well-known filter +
/// non-user-namespace filter all surface as Unsupported per
/// the rclone parity pattern set in PR #496).
fn assert_unsupported(err: std::io::Error) {
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::Unsupported,
        "expected Unsupported, got {err:?}"
    );
}

// ── gate: --metadata disabled ──────────────────────────────────────

/// When the user does NOT pass `--metadata`, setxattr returns
/// Unsupported — same gate as the read path at getxattr (issue
/// #465 / PR #496). Symmetry matters: if you can't read
/// xattrs, you can't write them.
#[test]
fn setxattr_disabled_returns_unsupported() {
    let fs = make_fs(); // metadata=false
    let ino = write_file(&fs, "a.txt", b"hello");
    let err = fs
        .setxattr(ino, "user.foo", b"bar", 0)
        .expect_err("setxattr should error when --metadata disabled");
    assert_unsupported(err);
}

#[test]
fn removexattr_disabled_returns_unsupported() {
    let fs = make_fs(); // metadata=false
    let ino = write_file(&fs, "a.txt", b"hello");
    let err = fs
        .removexattr(ino, "user.foo")
        .expect_err("removexattr should error when --metadata disabled");
    assert_unsupported(err);
}

// ── namespace filter: well-known + non-user ────────────────────────

/// Well-known derived xattrs (etag, mime_type, mtime,
/// content_length) are *populated by the backend on stat* —
/// they're not entries in the user_metadata map. Trying to
/// overwrite them via setxattr would be a lie, so we reject
/// with Unsupported (matches the getxattr semantics: you can
/// read them, but you can't write them).
#[test]
fn setxattr_wellknown_returns_unsupported() {
    let fs = enabled_fs();
    let ino = write_file(&fs, "a.txt", b"hello");
    for name in [
        "user.etag",
        "user.mime_type",
        "user.mtime",
        "user.content_length",
        "s3.etag",
        "s3.content-type",
    ] {
        let err = fs
            .setxattr(ino, name, b"x", 0)
            .expect_err(&format!("setxattr({name:?}) should error"));
        assert_unsupported(err);
    }
}

#[test]
fn removexattr_wellknown_returns_unsupported() {
    let fs = enabled_fs();
    let ino = write_file(&fs, "a.txt", b"hello");
    for name in [
        "user.etag",
        "user.mime_type",
        "user.mtime",
        "user.content_length",
    ] {
        let err = fs
            .removexattr(ino, name)
            .expect_err(&format!("removexattr({name:?}) should error"));
        assert_unsupported(err);
    }
}

/// Non-`user.*` namespaces (system.*, security.*, trusted.*,
/// etc.) are out of scope for the rclone `--metadata`
/// surface — we don't even read them on the read path, so
/// writing them is meaningless.
#[test]
fn setxattr_non_user_namespace_returns_unsupported() {
    let fs = enabled_fs();
    let ino = write_file(&fs, "a.txt", b"hello");
    let err = fs
        .setxattr(ino, "system.posix_acl_access", b"x", 0)
        .expect_err("system.* namespace should be rejected");
    assert_unsupported(err);
}

/// The empty `user.` form (no key) is malformed.
#[test]
fn setxattr_empty_user_key_returns_unsupported() {
    let fs = enabled_fs();
    let ino = write_file(&fs, "a.txt", b"hello");
    let err = fs
        .setxattr(ino, "user.", b"x", 0)
        .expect_err("user. with empty key should be rejected");
    assert_unsupported(err);
}

// ── value validation: UTF-8 ────────────────────────────────────────

/// opendal's user_metadata is `HashMap<String, String>`. The
/// xattr value bytes must be valid UTF-8 to roundtrip —
/// reject non-UTF-8 with InvalidInput so callers see EINVAL
/// rather than a generic opendal error.
#[test]
fn setxattr_non_utf8_returns_invalid_input() {
    let fs = enabled_fs();
    let ino = write_file(&fs, "a.txt", b"hello");
    let err = fs
        .setxattr(ino, "user.foo", b"\xff\xfe\xfd", 0)
        .expect_err("non-UTF-8 value should be rejected");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::InvalidInput,
        "expected InvalidInput, got {err:?}"
    );
}

// ── validation on directories ─────────────────────────────────────

/// `--metadata` applies to objects only (rclone parity).
/// Directories have no etag / content_length to expose on
/// the read path, so the write path also rejects them.
#[test]
fn setxattr_on_directory_returns_unsupported() {
    let fs = enabled_fs();
    let dir_attr = fs.mkdir(1, "d").expect("mkdir ok");
    let err = fs
        .setxattr(dir_attr.ino, "user.foo", b"bar", 0)
        .expect_err("setxattr on directory should be rejected");
    assert_unsupported(err);
}

// ── NotFound semantics for removexattr ────────────────────────────

/// When the named key isn't present, removexattr returns
/// NotFound so the kernel surfaces ENODATA rather than
/// silently succeeding. POSIX semantics (matches `xattr -d`
/// on an absent key returning ENODATA).
#[test]
fn removexattr_absent_key_returns_not_found() {
    let fs = enabled_fs();
    let ino = write_file(&fs, "a.txt", b"hello");
    // Set then immediately remove.
    fs.setxattr(ino, "user.tmp", b"v", 0).expect("setxattr ok");
    // Note: on the Memory backend the user_metadata
    // roundtrip through stat() is a no-op (the Memory
    // writer discards user_metadata — see writer.rs in
    // opendal-core-0.57/src/services/memory). So this test
    // asserts the NotFound contract even without a
    // successful setxattr having made the key visible
    // through stat. The removexattr's own
    // `existing.contains_key(&key)` check still runs — it
    // just sees the empty map and returns NotFound.
    let err = fs
        .removexattr(ino, "user.tmp")
        .expect_err("removexattr on absent key should error");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::NotFound,
        "expected NotFound, got {err:?}"
    );
}

// ── happy path: setxattr returns Ok ────────────────────────────────

/// On the Memory backend, setxattr writes succeed (the
/// write_with call returns Ok) but the metadata isn't
/// persisted through subsequent stat() — Memory's writer
/// only persists a handful of headers (see writer.rs:51).
///
/// This test verifies the contract we *can* verify on
/// Memory: setxattr returns Ok and doesn't corrupt the
/// entry. The full roundtrip is exercised by the Unix-only
/// fs-backend tests below.
#[test]
fn setxattr_user_key_returns_ok() {
    let fs = enabled_fs();
    let ino = write_file(&fs, "data.txt", b"hello");
    fs.setxattr(ino, "user.foo", b"bar", 0)
        .expect("setxattr on user.<key> should succeed");
    // The attribute cache should have been invalidated;
    // re-stat still works without panic.
    let _ = fs
        .getattr(ino)
        .expect("getattr should still resolve after setxattr");
}

/// setxattr with a key that needs normalization
/// (`user.My.Key` → `my_key`) must still return Ok. We don't
/// assert the exact wire form here because Memory's
/// user_metadata isn't roundtripped through stat — the
/// parse path is exercised; the storage path is exercised
/// by the Unix-only fs roundtrip tests.
#[test]
fn setxattr_normalizes_key_and_returns_ok() {
    let fs = enabled_fs();
    let ino = write_file(&fs, "data.txt", b"hello");
    fs.setxattr(ino, "user.My.Key", b"v", 0)
        .expect("setxattr with mixed-case + dotted key should succeed");
}

// ── roundtrip: Unix-only via opendal fs backend ────────────────────

/// Full setxattr → getxattr roundtrip against a real backend
/// that preserves user_metadata. opendal's fs backend
/// advertises `write_with_user_metadata: true` (cfg(unix)
/// in opendal-service-fs-0.58.1/backend.rs:148) and stores
/// the metadata as filesystem xattrs on the tempdir file.
///
/// cfg(unix): Windows fs backend doesn't advertise
/// write_with_user_metadata, so a roundtrip there would
/// silently no-op. The Memory-backend tests above cover
/// the gate / filter / validation paths on Windows.
#[cfg(unix)]
#[test]
fn setxattr_user_key_roundtrips_on_fs_backend() {
    use opendal::services::Fs;

    let tmp =
        std::env::temp_dir().join(format!("mntrs-xattr-set-roundtrip-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);

    let op = Operator::new(Fs::default().root(&tmp)).unwrap();
    let mut fs = new_test_fs(op, tmp.clone());
    fs.__metadata_set_for_test(true);

    let ino = write_file(&fs, "data.txt", b"hello");
    fs.setxattr(ino, "user.foo", b"bar", 0)
        .expect("setxattr ok");
    let value = fs.getxattr(ino, "user.foo").expect("getxattr user.foo ok");
    assert_eq!(
        value,
        b"bar".to_vec(),
        "setxattr then getxattr should return the same bytes"
    );
}

#[cfg(unix)]
#[test]
fn setxattr_normalizes_key_on_fs_backend() {
    use opendal::services::Fs;

    let tmp = std::env::temp_dir().join(format!("mntrs-xattr-set-norm-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);

    let op = Operator::new(Fs::default().root(&tmp)).unwrap();
    let mut fs = new_test_fs(op, tmp.clone());
    fs.__metadata_set_for_test(true);

    let ino = write_file(&fs, "data.txt", b"hello");
    // Mixed case + dotted form. The normalization rule
    // (lowercase + dots→underscores) means the stored key
    // is `my_key`, and the canonical xattr name advertised
    // by listxattr is `user.my_key`. getxattr on either
    // spelling should resolve because both the read path
    // (xattr_value_for) and the write path
    // (parse_user_xattr_key) run the same normalization.
    fs.setxattr(ino, "user.My.Key", b"v", 0)
        .expect("setxattr ok");
    let value = fs
        .getxattr(ino, "user.My.Key")
        .expect("getxattr user.My.Key ok");
    assert_eq!(value, b"v".to_vec());
}

#[cfg(unix)]
#[test]
fn removexattr_user_key_drops_it_on_fs_backend() {
    use opendal::services::Fs;

    let tmp = std::env::temp_dir().join(format!("mntrs-xattr-set-rm-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);

    let op = Operator::new(Fs::default().root(&tmp)).unwrap();
    let mut fs = new_test_fs(op, tmp.clone());
    fs.__metadata_set_for_test(true);

    let ino = write_file(&fs, "data.txt", b"hello");
    fs.setxattr(ino, "user.foo", b"bar", 0)
        .expect("setxattr ok");
    fs.removexattr(ino, "user.foo").expect("removexattr ok");
    // Subsequent getxattr must return NotFound.
    let err = fs
        .getxattr(ino, "user.foo")
        .expect_err("getxattr after removexattr should error");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::NotFound,
        "expected NotFound, got {err:?}"
    );
}

/// Removing a key that was never set must return NotFound,
/// not Ok. POSIX `xattr -d` returns ENODATA in this case,
/// and the kernel surfaces NotFound as ENODATA via the
/// fuser adapter's `io_err_to_fuse_errno`.
#[cfg(unix)]
#[test]
fn removexattr_truly_absent_key_returns_not_found_on_fs_backend() {
    use opendal::services::Fs;

    let tmp = std::env::temp_dir().join(format!("mntrs-xattr-set-rm404-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);

    let op = Operator::new(Fs::default().root(&tmp)).unwrap();
    let mut fs = new_test_fs(op, tmp.clone());
    fs.__metadata_set_for_test(true);

    let ino = write_file(&fs, "data.txt", b"hello");
    // No prior setxattr — the key isn't present.
    let err = fs
        .removexattr(ino, "user.absent")
        .expect_err("removexattr on absent key should error");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::NotFound,
        "expected NotFound, got {err:?}"
    );
}
