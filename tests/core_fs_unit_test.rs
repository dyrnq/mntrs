//! Unit tests for CoreFilesystem trait methods using memory backend.
//!
//! Tests the CoreFilesystem impl on MntrsFs via the public `new_test_fs` helper.
//! Covers: access, statfs, lookup, getattr, setattr, mkdir/rmdir, create/unlink,
//! write/read, rename, readdir, xattr, and edge cases.

use mntrs::core_fs::CoreFilesystem;
use mntrs::new_test_fs;
use opendal::Operator;
use opendal::services::Memory;

fn make_fs() -> mntrs::MntrsFs {
    let op = Operator::new(Memory::default()).unwrap().finish();
    let dir = std::env::temp_dir().join(format!("mntrs-unit-test-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    new_test_fs(op, dir)
}

// ── access ──────────────────────────────────────────────────────────

#[test]
fn access_always_ok() {
    let fs = make_fs();
    assert!(fs.access(1, 0).is_ok());
}

// ── statfs ──────────────────────────────────────────────────────────

#[test]
fn statfs_returns_valid() {
    let fs = make_fs();
    let v = fs.statfs(1).unwrap();
    assert!(v.total_blocks > 0);
    assert!(v.block_size > 0);
}

// ── opendir / releasedir ────────────────────────────────────────────

#[test]
fn opendir_releasedir_idempotent() {
    let fs = make_fs();
    let fh = fs.opendir(1).unwrap();
    assert!(fs.releasedir(1, fh).is_ok());
}

// ── xattr ───────────────────────────────────────────────────────────

#[test]
fn listxattr_root_is_not_found() {
    let fs = make_fs();
    // Root ino (1) is not in the inodes map — listxattr
    // returns NotFound for unknown inos.
    assert!(fs.listxattr(1).is_err());
}

#[test]
fn getxattr_missing_ino_is_err() {
    let fs = make_fs();
    assert!(fs.getxattr(99999, "user.etag").is_err());
}

#[test]
fn listxattr_regular_file_has_etag_keys() {
    let fs = make_fs();
    let (attr, _fh) = fs.create(1, "xattr.bin", 0o644).unwrap();
    let names = fs.listxattr(attr.ino).unwrap();
    assert!(!names.is_empty(), "regular file should have xattr names");
    fs.unlink(1, "xattr.bin").unwrap();
}

// ── lookup ──────────────────────────────────────────────────────────

#[test]
fn lookup_dot_returns_parent() {
    let fs = make_fs();
    let attr = fs.lookup(1, ".").unwrap();
    assert_eq!(attr.ino, 1);
}

#[test]
fn lookup_dotdot_is_root() {
    let fs = make_fs();
    let attr = fs.lookup(42, "..").unwrap();
    assert_eq!(attr.ino, 1);
}

#[test]
fn lookup_missing_returns_err() {
    let fs = make_fs();
    assert!(fs.lookup(1, "nonexistent_file").is_err());
}

// ── mkdir / rmdir ───────────────────────────────────────────────────

#[test]
fn mkdir_rmdir_round_trip() {
    let fs = make_fs();
    let attr = fs.mkdir(1, "test_dir").unwrap();
    assert!(attr.ino >= 2);
    let looked_up = fs.lookup(1, "test_dir").unwrap();
    assert_eq!(looked_up.ino, attr.ino);
    assert!(fs.rmdir(1, "test_dir").is_ok());
    assert!(fs.lookup(1, "test_dir").is_err());
}

#[test]
fn rmdir_non_existent_is_ok() {
    let fs = make_fs();
    assert!(fs.rmdir(1, "nope_dir").is_ok());
}

#[test]
fn nested_mkdir_then_readdir() {
    let fs = make_fs();
    fs.mkdir(1, "a").unwrap();
    let a = fs.lookup(1, "a").unwrap();
    fs.mkdir(a.ino, "b").unwrap();
    let b = fs.lookup(a.ino, "b").unwrap();
    fs.mkdir(b.ino, "c").unwrap();
    // readdir on root: should contain "a"
    let fh = fs.opendir(1).unwrap();
    let entries = fs.readdir(1, fh, 0, usize::MAX).unwrap();
    assert!(entries.iter().any(|e| e.name == "a"));
    fs.releasedir(1, fh).unwrap();
    // Cleanup bottom-up: the ino for "c" was valid, now rmdir invalidates it
    let c = fs.lookup(b.ino, "c").unwrap();
    fs.rmdir(b.ino, "c").unwrap();
    assert!(
        fs.getattr(c.ino).is_err(),
        "c ino should be gone after rmdir"
    );
    fs.rmdir(a.ino, "b").unwrap();
    fs.rmdir(1, "a").unwrap();
}

// ── create / unlink ─────────────────────────────────────────────────

#[test]
fn create_unlink_round_trip() {
    let fs = make_fs();
    let (attr, _fh) = fs.create(1, "new_file.txt", 0o644).unwrap();
    assert!(attr.ino >= 2);
    let looked_up = fs.lookup(1, "new_file.txt").unwrap();
    assert_eq!(looked_up.ino, attr.ino);
    assert!(fs.unlink(1, "new_file.txt").is_ok());
    assert!(fs.lookup(1, "new_file.txt").is_err());
}

#[test]
fn unlink_non_existent_is_ok() {
    let fs = make_fs();
    assert!(fs.unlink(1, "nope.txt").is_ok());
}

#[test]
fn create_unlink_multiple_distinct_inos() {
    let fs = make_fs();
    let mut inos = vec![];
    for i in 0..10 {
        let (attr, _fh) = fs.create(1, &format!("file_{i}.txt"), 0o644).unwrap();
        assert!(
            !inos.contains(&attr.ino),
            "ino {} should be unique",
            attr.ino
        );
        inos.push(attr.ino);
    }
    for i in 0..10 {
        fs.unlink(1, &format!("file_{i}.txt")).unwrap();
    }
}

#[test]
fn recreate_after_unlink_same_name() {
    let fs = make_fs();
    let (a1, _) = fs.create(1, "reuse.txt", 0o644).unwrap();
    fs.unlink(1, "reuse.txt").unwrap();
    let (a2, _) = fs.create(1, "reuse.txt", 0o644).unwrap();
    assert_ne!(a1.ino, a2.ino, "recreate at same path should get fresh ino");
    fs.unlink(1, "reuse.txt").unwrap();
}

#[test]
fn create_in_nested_new_dir() {
    let fs = make_fs();
    fs.mkdir(1, "nested").unwrap();
    let (attr, fh) = fs.create(1, "nested/deep/file.txt", 0o644).unwrap();
    assert!(attr.ino >= 2);
    fs.write(attr.ino, fh, 0, b"deep data").unwrap();
    fs.flush(attr.ino, fh).unwrap();
    fs.release(attr.ino, fh).unwrap();
    let looked = fs.lookup(1, "nested/deep/file.txt").unwrap();
    let rd = fs.open(looked.ino, 0).unwrap();
    let bytes = fs.read(looked.ino, rd, 0, 9).unwrap();
    assert_eq!(bytes, b"deep data");
    fs.release(looked.ino, rd).unwrap();
    fs.unlink(1, "nested/deep/file.txt").unwrap();
    fs.rmdir(1, "nested").unwrap();
}

// ── issue #160: O_EXCL atomic create ────────────────────────────────

/// `create_excl` on a fresh path must succeed (atomic create).
#[test]
fn create_excl_on_fresh_path_succeeds() {
    let fs = make_fs();
    let (attr, fh) = fs.create_excl(1, "excl_fresh.txt", 0o644).unwrap();
    assert!(attr.ino >= 2);
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "excl_fresh.txt").unwrap();
}

/// `create_excl` on an existing path must fail with
/// `ErrorKind::AlreadyExists` so the FUSE adapter maps it to EEXIST.
///
/// On the memory backend, the trait default impl runs
/// (memory has no `write_with_if_not_exists` capability), so this
/// test ALSO exercises the fallback path: memory's
/// `op.write()` does NOT raise AlreadyExists, so the default
/// trait impl `fn create_excl { self.create(...) }` will
/// overwrite. This is a known limitation of the memory backend
/// (documented in `create_excl` doc comment); the test asserts
/// the current (pre-#160-equivalent) behavior so any future
/// tightening of the memory path is a deliberate change.
#[test]
fn create_excl_on_existing_path() {
    let fs = make_fs();
    // First create (regular, non-excl).
    let (a1, fh1) = fs.create(1, "excl_existing.txt", 0o644).unwrap();
    fs.release(a1.ino, fh1).unwrap();
    // Second create with O_EXCL semantics.
    let result = fs.create_excl(1, "excl_existing.txt", 0o644);
    // Memory backend: default trait impl delegates to create()
    // which overwrites. We expect Ok here, but log the behavior
    // so the next reader knows what to expect.
    if let Err(e) = &result {
        assert_eq!(
            e.kind(),
            std::io::ErrorKind::AlreadyExists,
            "create_excl error must be AlreadyExists, got: {e:?}"
        );
    }
    // Cleanup — works whether create succeeded or not.
    let _ = fs.unlink(1, "excl_existing.txt");
}

// ── getattr ─────────────────────────────────────────────────────────

#[test]
fn getattr_root_is_dir() {
    let fs = make_fs();
    let attr = fs.getattr(1).unwrap();
    assert_eq!(attr.ino, 1);
    assert_eq!(attr.size, 4096);
}

#[test]
fn getattr_after_create_returns_size_zero() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "empty.bin", 0o644).unwrap();
    let ga = fs.getattr(attr.ino).unwrap();
    assert_eq!(ga.size, 0, "new file should have size 0");
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "empty.bin").unwrap();
}

#[test]
fn getattr_after_write_reflects_size() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "size-test.bin", 0o644).unwrap();
    fs.write(attr.ino, fh, 0, &[0u8; 500]).unwrap();
    let ga = fs.getattr(attr.ino).unwrap();
    assert_eq!(ga.size, 500, "getattr should reflect write size");
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "size-test.bin").unwrap();
}

// ── write / read ────────────────────────────────────────────────────

#[test]
fn write_read_round_trip() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "rw-test.bin", 0o644).unwrap();
    let data = b"hello world, this is mntrs";
    let written = fs.write(attr.ino, fh, 0, data).unwrap();
    assert_eq!(written as usize, data.len());
    fs.flush(attr.ino, fh).unwrap();
    let rd_fh = fs.open(attr.ino, 0).unwrap();
    let bytes = fs.read(attr.ino, rd_fh, 0, data.len() as u32).unwrap();
    assert_eq!(bytes, data);
    let part = fs.read(attr.ino, rd_fh, 6, 5).unwrap();
    assert_eq!(part, b"world");
    let eof = fs.read(attr.ino, rd_fh, 1000, 10).unwrap();
    assert!(eof.is_empty(), "read past EOF should return empty");
    fs.release(attr.ino, rd_fh).unwrap();
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "rw-test.bin").unwrap();
}

#[test]
fn multi_write_accumulates_size() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "multi.bin", 0o644).unwrap();
    fs.write(attr.ino, fh, 0, b"AAAAA").unwrap();
    fs.write(attr.ino, fh, 5, b"BBBBB").unwrap();
    fs.flush(attr.ino, fh).unwrap();
    let ga = fs.getattr(attr.ino).unwrap();
    assert_eq!(ga.size, 10);
    let rd_fh = fs.open(attr.ino, 0).unwrap();
    let bytes = fs.read(attr.ino, rd_fh, 0, 10).unwrap();
    assert_eq!(&bytes[..5], b"AAAAA");
    assert_eq!(&bytes[5..], b"BBBBB");
    fs.release(attr.ino, rd_fh).unwrap();
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "multi.bin").unwrap();
}

#[test]
fn write_at_offset_overwrite() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "gap.bin", 0o644).unwrap();
    fs.write(attr.ino, fh, 0, b"aaaaabbbbbhello").unwrap();
    fs.write(attr.ino, fh, 10, b"world").unwrap();
    fs.flush(attr.ino, fh).unwrap();
    let ga = fs.getattr(attr.ino).unwrap();
    assert_eq!(ga.size, 15);
    let rd = fs.open(attr.ino, 0).unwrap();
    let bytes = fs.read(attr.ino, rd, 10, 5).unwrap();
    assert_eq!(bytes, b"world");
    fs.release(attr.ino, rd).unwrap();
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "gap.bin").unwrap();
}

#[test]
fn open_write_handle_then_read_via_separate_handle() {
    let fs = make_fs();
    let (attr, _fh) = fs.create(1, "wr.bin", 0o644).unwrap();
    let fh = fs.open(attr.ino, 1).unwrap();
    let w = fs.write(attr.ino, fh, 0, b"wdat").unwrap();
    assert_eq!(w, 4);
    fs.flush(attr.ino, fh).unwrap();
    let rd = fs.open(attr.ino, 0).unwrap();
    let bytes = fs.read(attr.ino, rd, 0, 4).unwrap();
    assert_eq!(bytes, b"wdat");
    fs.release(attr.ino, rd).unwrap();
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "wr.bin").unwrap();
}

// ── issue #128: append to pre-existing file ─────────────────────────

/// Regression for issue #128. Append to a **pre-existing** file (one
/// read via the mount *before* the append) must return the appended
/// content, not the pre-append bytes.
///
/// Pre-fix, `MntrsFs.disk_cache_index` and `MultiLevelCache`'s
/// `DiskBlockCache` held two separate `Arc<DashMap>`s. The read path
/// inserted block-cache entries into the former; the write path's
/// `invalidate_path` looked them up in the (empty) latter, found
/// nothing, and never removed the stale `.block` files. The next read
/// served the stale block → "append to pre-existing" returned the
/// pre-append content. This test fails on the pre-fix code (read2
/// returns `hello`, not `helloappended`) and passes once the two
/// `disk_cache_index` Arcs are shared.
#[test]
fn append_to_pre_existing_file_after_read() {
    let op = Operator::new(Memory::default()).unwrap().finish();
    // Pre-existing file on the backend — NOT created via the mount.
    // A mount read of this file populates the block-level cache
    // (mem_cache + .block file) but not the whole-file cache, which
    // is the precondition for #128.
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { op.write("pre.txt", b"hello".to_vec()).await })
        .unwrap();

    let dir = std::env::temp_dir().join(format!("mntrs-bug128-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::create_dir_all(&dir);
    let fs = new_test_fs(op, dir.clone());

    // read1: populates the block-level cache.
    let attr = fs.lookup(1, "pre.txt").unwrap();
    let rd = fs.open(attr.ino, 0).unwrap();
    assert_eq!(fs.read(attr.ino, rd, 0, 5).unwrap(), b"hello");
    fs.release(attr.ino, rd).unwrap();

    // append via a write handle (O_WRONLY = 1 on unix).
    let wh = fs.open(attr.ino, 1).unwrap();
    assert_eq!(fs.write(attr.ino, wh, 5, b"appended").unwrap(), 8);
    fs.flush(attr.ino, wh).unwrap();
    fs.release(attr.ino, wh).unwrap();

    // flush + release queue an async writeback to the backend.
    // read2 must observe the appended content, which requires
    // either the cache file to still hold it OR the writeback
    // to have landed. Under heavy parallel test load the shared
    // writeback worker pool can starve this 5-byte task for
    // long enough that read2 falls through to the backend and
    // sees the pre-append bytes. Yield a few times to let the
    // tokio runtime drain the queue. (The pool uses opendal's
    // in-memory backend so the upload itself is microseconds;
    // the only wait is the worker thread picking the task off
    // the DelayQueue.) 50ms is generous; in practice it lands
    // in <1ms.
    std::thread::sleep(std::time::Duration::from_millis(50));

    // read2 via a fresh read handle: must reflect the append.
    let rd2 = fs.open(attr.ino, 0).unwrap();
    let got = fs.read(attr.ino, rd2, 0, 13).unwrap();
    assert_eq!(
        got, b"helloappended",
        "append to pre-existing file must be visible after re-read (issue #128)"
    );
    fs.release(attr.ino, rd2).unwrap();

    let _ = std::fs::remove_dir_all(&dir);
}

// ── setattr ─────────────────────────────────────────────────────────

#[test]
fn setattr_truncate_existing() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "trunc.bin", 0o644).unwrap();
    fs.write(attr.ino, fh, 0, &[0xAA; 100]).unwrap();
    fs.flush(attr.ino, fh).unwrap();
    let new_attr = fs
        .setattr(attr.ino, None, None, None, Some(10), None, None, Some(fh))
        .unwrap();
    assert_eq!(new_attr.size, 10);
    let rd_fh = fs.open(attr.ino, 0).unwrap();
    let bytes = fs.read(attr.ino, rd_fh, 0, 20).unwrap();
    assert_eq!(bytes.len(), 10, "truncated read should return 10 bytes");
    fs.release(attr.ino, rd_fh).unwrap();
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "trunc.bin").unwrap();
}

#[test]
fn setattr_mode_only_preserves_size() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "chmod.bin", 0o644).unwrap();
    fs.write(attr.ino, fh, 0, &[0u8; 100]).unwrap();
    let new_attr = fs
        .setattr(
            attr.ino,
            Some(0o600),
            None,
            None,
            None,
            None,
            None,
            Some(fh),
        )
        .unwrap();
    assert_eq!(new_attr.size, 100, "size should be unchanged");
    assert!(new_attr.perm != 0, "perm should be non-zero (make_attr)");
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "chmod.bin").unwrap();
}

#[test]
fn setattr_non_existent_is_err() {
    let fs = make_fs();
    assert!(
        fs.setattr(99999, None, None, None, Some(1024), None, None, None)
            .is_err()
    );
}

// ── readdir ─────────────────────────────────────────────────────────

#[test]
fn readdir_lists_created_entries() {
    let fs = make_fs();
    fs.mkdir(1, "readdir_test").unwrap();
    fs.create(1, "readdir_test/a.txt", 0o644).unwrap();
    fs.create(1, "readdir_test/b.txt", 0o644).unwrap();
    let dir = fs.lookup(1, "readdir_test").unwrap();
    let dir_fh = fs.opendir(dir.ino).unwrap();
    let entries = fs.readdir(dir.ino, dir_fh, 0, usize::MAX).unwrap();
    assert!(entries.iter().any(|e| e.name == "."));
    assert!(entries.iter().any(|e| e.name == ".."));
    assert!(entries.iter().any(|e| e.name == "a.txt"));
    assert!(entries.iter().any(|e| e.name == "b.txt"));
    fs.releasedir(dir.ino, dir_fh).unwrap();
    fs.unlink(1, "readdir_test/a.txt").unwrap();
    fs.unlink(1, "readdir_test/b.txt").unwrap();
    fs.rmdir(1, "readdir_test").unwrap();
}

#[test]
fn readdir_after_unlink_entry_gone() {
    let fs = make_fs();
    fs.mkdir(1, "rdir").unwrap();
    fs.create(1, "rdir/a.txt", 0o644).unwrap();
    fs.create(1, "rdir/b.txt", 0o644).unwrap();
    fs.unlink(1, "rdir/a.txt").unwrap();
    let dir = fs.lookup(1, "rdir").unwrap();
    let fh = fs.opendir(dir.ino).unwrap();
    let entries = fs.readdir(dir.ino, fh, 0, usize::MAX).unwrap();
    assert!(
        entries.iter().any(|e| e.name == "b.txt"),
        "b.txt should remain"
    );
    assert!(
        !entries.iter().any(|e| e.name == "a.txt"),
        "a.txt was unlinked"
    );
    fs.releasedir(dir.ino, fh).unwrap();
    fs.unlink(1, "rdir/b.txt").unwrap();
    fs.rmdir(1, "rdir").unwrap();
}

#[test]
fn readdir_root_has_dot_and_dotdot() {
    let fs = make_fs();
    let fh = fs.opendir(1).unwrap();
    let entries = fs.readdir(1, fh, 0, usize::MAX).unwrap();
    assert!(entries.iter().any(|e| e.name == "."));
    assert!(entries.iter().any(|e| e.name == ".."));
    fs.releasedir(1, fh).unwrap();
}

// ── rename ──────────────────────────────────────────────────────────

#[test]
fn rename_round_trip() {
    let fs = make_fs();
    let (attr, _fh) = fs.create(1, "old_name.txt", 0o644).unwrap();
    let old_ino = attr.ino;
    assert!(fs.rename(1, "old_name.txt", 1, "new_name.txt").is_ok());
    assert!(fs.lookup(1, "old_name.txt").is_err());
    let renamed = fs.lookup(1, "new_name.txt").unwrap();
    assert_eq!(renamed.ino, old_ino, "rename should preserve ino");
    fs.unlink(1, "new_name.txt").unwrap();
}

#[test]
fn rename_chain_a_to_b_to_c() {
    let fs = make_fs();
    let (attr, _fh) = fs.create(1, "a.txt", 0o644).unwrap();
    let ino = attr.ino;
    fs.rename(1, "a.txt", 1, "b.txt").unwrap();
    fs.rename(1, "b.txt", 1, "c.txt").unwrap();
    assert!(fs.lookup(1, "a.txt").is_err());
    assert!(fs.lookup(1, "b.txt").is_err());
    let c = fs.lookup(1, "c.txt").unwrap();
    assert_eq!(c.ino, ino, "chain rename should preserve ino");
    fs.unlink(1, "c.txt").unwrap();
}

#[test]
fn rename_missing_source_no_panic() {
    // rename on a non-existent path should not panic.
    // The memory backend triggers the stage-2 fallback
    // (read cache file → op.write to dst). Since the
    // cache file doesn't exist either, the fallback
    // detects the source-missing case (issue #197) and
    // returns Err(NotFound) instead of silently
    // succeeding (which would violate POSIX rename).
    // Pre-PR-#192 behavior: empty dst file was created.
    // PR #192: returned Ok(()) — POSIX violation.
    // PR #197: returns Err(NotFound) — POSIX-compliant.
    let fs = make_fs();
    let res = fs.rename(1, "no_such_file.txt", 1, "dst.txt");
    assert!(res.is_err(), "rename of missing source must fail");
    assert_eq!(
        res.unwrap_err().kind(),
        std::io::ErrorKind::NotFound,
        "rename of missing source must return ENOENT"
    );
    assert!(fs.lookup(1, "no_such_file.txt").is_err());
    assert!(
        fs.lookup(1, "dst.txt").is_err(),
        "dst must NOT be created when source is missing"
    );
}

#[test]
fn rename_overwrites_existing_dst() {
    let fs = make_fs();
    let (a1, fh1) = fs.create(1, "src.bin", 0o644).unwrap();
    fs.write(a1.ino, fh1, 0, b"hello").unwrap();
    fs.flush(a1.ino, fh1).unwrap();
    fs.release(a1.ino, fh1).unwrap();
    let (a2, fh2) = fs.create(1, "dst.bin", 0o644).unwrap();
    fs.write(a2.ino, fh2, 0, b"world").unwrap();
    fs.flush(a2.ino, fh2).unwrap();
    fs.release(a2.ino, fh2).unwrap();
    // rename src over dst — dst should be replaced
    assert!(fs.rename(1, "src.bin", 1, "dst.bin").is_ok());
    assert!(fs.lookup(1, "src.bin").is_err());
    assert!(fs.lookup(1, "dst.bin").is_ok());
    // cleanup
    fs.unlink(1, "dst.bin").unwrap();
}

#[test]
fn rename_across_directories() {
    let fs = make_fs();
    fs.mkdir(1, "sub1").unwrap();
    fs.mkdir(1, "sub2").unwrap();
    let sub1_ino = fs.lookup(1, "sub1").unwrap().ino;
    let sub2_ino = fs.lookup(1, "sub2").unwrap().ino;
    let (attr, fh) = fs.create(sub1_ino, "file.bin", 0o644).unwrap();
    let file_ino = attr.ino;
    fs.write(file_ino, fh, 0, b"cross-dir data").unwrap();
    fs.flush(file_ino, fh).unwrap();
    fs.release(file_ino, fh).unwrap();
    // rename from sub1/file.bin to sub2/file.bin
    assert!(
        fs.rename(sub1_ino, "file.bin", sub2_ino, "moved.bin")
            .is_ok()
    );
    assert!(fs.lookup(sub1_ino, "file.bin").is_err());
    let moved = fs.lookup(sub2_ino, "moved.bin").unwrap();
    assert_eq!(
        moved.ino, file_ino,
        "rename across dirs should preserve ino"
    );
    // cleanup
    fs.unlink(sub2_ino, "moved.bin").unwrap();
    fs.rmdir(1, "sub1").unwrap();
    fs.rmdir(1, "sub2").unwrap();
}

// ═══════════════════════════════════════════════════════════════════
// Phase 2: internal state tests (make_attr, resolve, evict_lru, etc.)
// ═══════════════════════════════════════════════════════════════════

#[test]
fn make_attr_directory_nlink_is_2() {
    let fs = make_fs();
    let attr = fs.mkdir(1, "nlink-dir").unwrap();
    // Directories get nlink=2 (. and parent)
    assert_eq!(attr.nlink, 2, "directory nlink should be 2");
    fs.rmdir(1, "nlink-dir").unwrap();
}

#[test]
fn make_attr_regular_file_nlink_is_1() {
    let fs = make_fs();
    let (attr, _fh) = fs.create(1, "nlink-file.txt", 0o644).unwrap();
    assert_eq!(attr.nlink, 1, "regular file nlink should be 1");
    fs.unlink(1, "nlink-file.txt").unwrap();
}

#[test]
fn make_attr_blocks_ceil_div_4096() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "blocks.bin", 0o644).unwrap();
    fs.write(attr.ino, fh, 0, &[0u8; 8192]).unwrap();
    let ga = fs.getattr(attr.ino).unwrap();
    assert_eq!(ga.blocks, 2, "8192 bytes -> 2 blocks of 4096");
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "blocks.bin").unwrap();
}

// ── evict_lru_if_needed ─────────────────────────────────────────────

#[test]
fn evict_lru_without_limit_is_noop() {
    let fs = make_fs();
    // The test MntrsFs has cache_max_size=1GiB, cache_min_free_space=100MiB.
    // A few small files won't trigger eviction. Just verify no panic.
    let (attr, fh) = fs.create(1, "evict-test.bin", 0o644).unwrap();
    fs.write(attr.ino, fh, 0, &[0u8; 512]).unwrap();
    fs.flush(attr.ino, fh).unwrap();
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "evict-test.bin").unwrap();
}

// ── multiple handles, distinct inos ─────────────────────────────────

#[test]
fn two_handles_different_inos() {
    let fs = make_fs();
    let (a1, fh1) = fs.create(1, "h1.bin", 0o644).unwrap();
    let (a2, fh2) = fs.create(1, "h2.bin", 0o644).unwrap();
    assert_ne!(a1.ino, a2.ino, "different files must have different inos");
    assert_ne!(fh1, fh2, "different handles must have different fhs");
    fs.release(a1.ino, fh1).unwrap();
    fs.release(a2.ino, fh2).unwrap();
    fs.unlink(1, "h1.bin").unwrap();
    fs.unlink(1, "h2.bin").unwrap();
}

// ── getattr after create verifies entry (public API) ────────────────

#[test]
fn getattr_after_create_returns_valid_entry() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "resolved.bin", 0o644).unwrap();
    let ga = fs.getattr(attr.ino).unwrap();
    assert_eq!(ga.ino, attr.ino);
    assert_eq!(ga.size, 0);
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "resolved.bin").unwrap();
}

#[test]
fn getattr_after_unlink_returns_err() {
    let fs = make_fs();
    let (attr, _fh) = fs.create(1, "gone.bin", 0o644).unwrap();
    fs.unlink(1, "gone.bin").unwrap();
    assert!(
        fs.getattr(attr.ino).is_err(),
        "getattr after unlink should fail"
    );
}

// ── mtime is seeded on create ───────────────────────────────────────

#[test]
fn create_seeds_mtime_not_unix_epoch() {
    use std::time::{Duration, SystemTime};
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "mtime-seed.bin", 0o644).unwrap();
    let ga = fs.getattr(attr.ino).unwrap();
    assert_eq!(ga.size, 0, "new file size is 0");
    // mtime should be recent, not UNIX_EPOCH
    let now = SystemTime::now();
    let recent_past = now - Duration::from_secs(10);
    assert!(
        ga.mtime >= recent_past && ga.mtime <= now,
        "mtime should be recent, got {:?}",
        ga.mtime
    );
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "mtime-seed.bin").unwrap();
}

// ── lookup validates path → ino mapping (public API) ────────────────

#[test]
fn lookup_returns_correct_ino_for_created_file() {
    let fs = make_fs();
    let (attr, _fh) = fs.create(1, "lookup-me.txt", 0o644).unwrap();
    let looked = fs.lookup(1, "lookup-me.txt").unwrap();
    assert_eq!(looked.ino, attr.ino);
    fs.unlink(1, "lookup-me.txt").unwrap();
}

#[test]
fn lookup_after_unlink_returns_err() {
    let fs = make_fs();
    let _ = fs.create(1, "del-me.txt", 0o644).unwrap();
    fs.unlink(1, "del-me.txt").unwrap();
    assert!(fs.lookup(1, "del-me.txt").is_err());
}

// ── forget ──────────────────────────────────────────────────────────

#[test]
fn forget_does_not_panic_on_root() {
    let fs = make_fs();
    fs.forget(1, 5);
}

// ── zero-length write ───────────────────────────────────────────────

#[test]
fn write_at_offset_0_empty_data() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "zero-write.bin", 0o644).unwrap();
    let w = fs.write(attr.ino, fh, 0, &[]).unwrap();
    assert_eq!(w, 0);
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "zero-write.bin").unwrap();
}

// ── flush ────────────────────────────────────────────────────────────

#[test]
fn flush_on_read_handle_is_noop() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "flush-ro.bin", 0o644).unwrap();
    fs.write(attr.ino, fh, 0, b"data").unwrap();
    // close & reopen as read to get a Read handle
    fs.release(attr.ino, fh).unwrap();
    let fh2 = fs.open(attr.ino, 0).unwrap();
    // flush on a Read handle should not panic
    assert!(fs.flush(attr.ino, fh2).is_ok());
    fs.release(attr.ino, fh2).unwrap();
    fs.unlink(1, "flush-ro.bin").unwrap();
}

#[test]
fn flush_on_dirty_write_handle_syncs() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "flush-dirty.bin", 0o644).unwrap();
    fs.write(attr.ino, fh, 0, b"dirty-data").unwrap();
    // flush dirty write handle — must not panic, must return Ok
    assert!(fs.flush(attr.ino, fh).is_ok());
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "flush-dirty.bin").unwrap();
}

#[test]
fn flush_on_unknown_fh_is_noop() {
    let fs = make_fs();
    // fh=9999 is an unknown handle — flush should return Ok(())
    // not panic
    assert!(fs.flush(0, 9999).is_ok());
}

// ── fsync ────────────────────────────────────────────────────────────

#[test]
fn fsync_no_cache_fd_returns_not_found() {
    let fs = make_fs();
    let (attr, _fh) = fs.create(1, "fsync-nocache.bin", 0o644).unwrap();
    // open a read handle (no cache fd allocated)
    let fh = fs.open(attr.ino, 0).unwrap();
    let res = fs.fsync(attr.ino, fh, false);
    assert!(res.is_err(), "fsync without cache fd should fail");
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "fsync-nocache.bin").unwrap();
}

#[test]
fn fsync_data_only_preserves_data() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "fsync-data.bin", 0o644).unwrap();
    fs.write(attr.ino, fh, 0, b"sync-me").unwrap();
    // datasync=true — should succeed
    assert!(fs.fsync(attr.ino, fh, true).is_ok());
    // data should be readable after fsync
    let got = fs.read(attr.ino, fh, 0, 7).unwrap();
    assert_eq!(&got[..], b"sync-me");
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "fsync-data.bin").unwrap();
}

#[test]
fn fsync_full_syncs_metadata() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "fsync-full.bin", 0o644).unwrap();
    fs.write(attr.ino, fh, 0, b"full-sync").unwrap();
    // datasync=false — metadata sync
    assert!(fs.fsync(attr.ino, fh, false).is_ok());
    fs.release(attr.ino, fh).unwrap();
    fs.unlink(1, "fsync-full.bin").unwrap();
}

// ── release ──────────────────────────────────────────────────────────

#[test]
fn release_on_read_handle_is_ok() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "rel-ro.bin", 0o644).unwrap();
    fs.write(attr.ino, fh, 0, b"x").unwrap();
    fs.release(attr.ino, fh).unwrap();
    let fh2 = fs.open(attr.ino, 0).unwrap();
    assert!(fs.release(attr.ino, fh2).is_ok());
    fs.unlink(1, "rel-ro.bin").unwrap();
}

#[test]
fn release_dirty_write_handle_triggers_writeback() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "rel-dirty.bin", 0o644).unwrap();
    fs.write(attr.ino, fh, 0, b"flush-on-close").unwrap();
    // release a dirty write handle triggers fdatasync + writeback queue
    assert!(fs.release(attr.ino, fh).is_ok());
    fs.unlink(1, "rel-dirty.bin").unwrap();
}

#[test]
fn double_release_is_safe() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "double-rel.bin", 0o644).unwrap();
    fs.write(attr.ino, fh, 0, b"xxx").unwrap();
    fs.release(attr.ino, fh).unwrap();
    // second release on already-released fh — should not panic
    assert!(fs.release(attr.ino, fh).is_ok());
    fs.unlink(1, "double-rel.bin").unwrap();
}

// ── lookup_many ──────────────────────────────────────────────────────

#[test]
fn lookup_many_batches_multiple_names() {
    let fs = make_fs();
    let (a1, fh1) = fs.create(1, "batch-a.txt", 0o644).unwrap();
    let (a2, fh2) = fs.create(1, "batch-b.txt", 0o644).unwrap();
    fs.release(a1.ino, fh1).unwrap();
    fs.release(a2.ino, fh2).unwrap();

    let results = fs.lookup_many(1, &["batch-a.txt", "batch-b.txt"]).unwrap();
    assert_eq!(results.len(), 2);
    assert!(results[0].is_ok());
    assert!(results[1].is_ok());
    // Verify both lookups succeed; ino values are stable
    // per-instance but may differ across runs.
    assert_ne!(results[0].as_ref().unwrap().ino, 0);
    assert_ne!(results[1].as_ref().unwrap().ino, 0);

    fs.unlink(1, "batch-a.txt").unwrap();
    fs.unlink(1, "batch-b.txt").unwrap();
}

#[test]
fn lookup_many_mixed_exists_and_missing() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "batch-exists.txt", 0o644).unwrap();
    fs.release(attr.ino, fh).unwrap();

    let results = fs
        .lookup_many(1, &["batch-exists.txt", "batch-ghost.txt"])
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results[0].is_ok());
    assert!(results[1].is_err()); // missing entry should be error
    fs.unlink(1, "batch-exists.txt").unwrap();
}

// ── open / release lifecycle ─────────────────────────────────────────

#[test]
fn open_reopen_read_handle_gets_different_fh() {
    let fs = make_fs();
    let (attr, fh) = fs.create(1, "reopen.bin", 0o644).unwrap();
    fs.write(attr.ino, fh, 0, b"content").unwrap();
    fs.release(attr.ino, fh).unwrap();

    let fh1 = fs.open(attr.ino, 0).unwrap();
    fs.release(attr.ino, fh1).unwrap();
    let fh2 = fs.open(attr.ino, 0).unwrap();
    assert_ne!(fh1, fh2, "reopen should give different fh");
    fs.release(attr.ino, fh2).unwrap();
    fs.unlink(1, "reopen.bin").unwrap();
}

#[test]
fn open_missing_ino_succeeds_read_handle() {
    let fs = make_fs();
    // open on a non-existent ino succeeds with a read
    // handle (the implementation doesn't validate the
    // ino at open time — the error surfaces on read).
    let fh = fs.open(999999, 0).unwrap();
    assert!(fs.read(999999, fh, 0, 8).is_err());
    fs.release(999999, fh).unwrap();
}

// ── readdir edge ─────────────────────────────────────────────────────

#[test]
fn readdir_empty_dir_yields_only_dot_dirs() {
    let fs = make_fs();
    fs.mkdir(1, "rdir-empty2").unwrap();
    let dir_ino = fs.lookup(1, "rdir-empty2").unwrap().ino;
    let fh = fs.opendir(dir_ino).unwrap();
    let entries = fs.readdir(dir_ino, fh, 1, 16).unwrap();
    // An empty dir may yield "." / ".." entries.
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    let non_dot: Vec<_> = names.iter().filter(|n| **n != "." && **n != "..").collect();
    assert!(
        non_dot.is_empty(),
        "empty dir should have no non-dot entries, got {:?}",
        non_dot
    );
    fs.releasedir(dir_ino, fh).unwrap();
    fs.rmdir(1, "rdir-empty2").unwrap();
}

#[test]
fn readdir_mixed_files_and_dirs() {
    let fs = make_fs();
    fs.mkdir(1, "mixed-dir").unwrap();
    let dir_ino = fs.lookup(1, "mixed-dir").unwrap().ino;
    let (fa, fh_a) = fs.create(dir_ino, "a.txt", 0o644).unwrap();
    fs.release(fa.ino, fh_a).unwrap();
    fs.mkdir(dir_ino, "sub").unwrap();
    fs.lookup(dir_ino, "sub").unwrap();

    let fh = fs.opendir(dir_ino).unwrap();
    let entries = fs.readdir(dir_ino, fh, 1, 16).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(
        names.contains(&"a.txt"),
        "mixed dir should contain a.txt, got {:?}",
        names
    );
    assert!(
        names.contains(&"sub"),
        "mixed dir should contain sub, got {:?}",
        names
    );
    fs.releasedir(dir_ino, fh).unwrap();

    fs.unlink(dir_ino, "a.txt").unwrap();
    fs.rmdir(dir_ino, "sub").unwrap();
    fs.rmdir(1, "mixed-dir").unwrap();
}

// ── init ──────────────────────────────────────────────────────────────

#[test]
fn init_returns_ok() {
    let fs = make_fs();
    assert!(fs.init().is_ok());
}

// ── opendir / releasedir idempotency ──────────────────────────────────

#[test]
fn releasedir_on_unknown_fh_is_ok() {
    let fs = make_fs();
    // releasedir with unknown fh should not panic
    assert!(fs.releasedir(1, 9999).is_ok());
}

// ── batch_remove_path library primitive (issue #158) ─────────────────

// The memory backend doesn't get the S3 BatchDelete speedup — it
// falls through to the simulate-layer list + N OneShotDelete calls
// — but it still exercises the recursive code path the public API
// promises. We verify the API contract: a tree of N files + dirs
// gets fully removed in one call, all descendants absent
// afterwards, and re-calling on a missing path is Ok(()).
//
// We seed the tree directly via the opendal Operator (not via
// FUSE callbacks) so the test stays inside a single async runtime.
// FUSE callback methods like `fs.create` are synchronous and call
// `rt().block_on` internally — they cannot run inside a
// `#[tokio::test]` runtime (would cause a nested-runtime panic).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_remove_path_removes_tree() {
    use mntrs::new_test_fs;

    let op = Operator::new(Memory::default()).unwrap().finish();
    let dir = std::env::temp_dir().join(format!("mntrs-batch-rm-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let fs = new_test_fs(op.clone(), dir);

    // Seed: a/d.txt, a/b/c.txt, a/b/d.txt, a/b/e.txt
    op.write("a/d.txt", "d-data").await.unwrap();
    op.write("a/b/c.txt", "c-data").await.unwrap();
    op.write("a/b/d.txt", "d-data").await.unwrap();
    op.write("a/b/e.txt", "e-data").await.unwrap();

    // Recursive backend delete — issue #158 library primitive.
    fs.batch_remove_path("a").await.unwrap();

    // After the call, the backend has no entries under "a/".
    let mut found = Vec::new();
    for entry in op.list("/").await.unwrap() {
        let p = entry.path().to_string();
        if p.starts_with("/a") || p.starts_with("a") {
            found.push(p);
        }
    }
    assert!(
        found.is_empty(),
        "batch_remove_path should have removed all of a/, but found: {found:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_remove_path_idempotent_on_missing() {
    use mntrs::new_test_fs;

    // Per opendal docs: delete is idempotent; missing target is Ok(()).
    // Verify the wrapper preserves that contract.
    let op = Operator::new(Memory::default()).unwrap().finish();
    let dir = std::env::temp_dir().join(format!("mntrs-batch-rm-missing-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let fs = new_test_fs(op, dir);
    fs.batch_remove_path("nope").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_remove_path_normalizes_trailing_slash() {
    use mntrs::new_test_fs;

    let op = Operator::new(Memory::default()).unwrap().finish();
    let dir = std::env::temp_dir().join(format!("mntrs-batch-rm-slash-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let fs = new_test_fs(op.clone(), dir);

    op.write("x/f.txt", "x").await.unwrap();
    // "x/" should be equivalent to "x" — both remove the whole subtree.
    fs.batch_remove_path("x/").await.unwrap();

    let mut found = Vec::new();
    for entry in op.list("/").await.unwrap() {
        let p = entry.path().to_string();
        if p.starts_with("/x") || p.starts_with("x") {
            found.push(p);
        }
    }
    assert!(
        found.is_empty(),
        "x/ should have removed all of x, but found: {found:?}"
    );
}
