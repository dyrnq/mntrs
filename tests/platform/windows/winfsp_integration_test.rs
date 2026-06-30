#![cfg(windows)]

use std::sync::Arc;
use std::time::Duration;

use opendal::Operator;
use opendal::services::Memory;

use mntrs::MntrsFs;
use mntrs::core_fs::CoreFilesystem;

// One-shot tracing subscriber init. The `mntrs.exe` binary
// initializes a global subscriber in main.rs; the test binary
// doesn't, so without this RUST_LOG is silently dropped. Gated on
// RUST_LOG being set so the test stays quiet by default.
#[ctor::ctor]
fn __init_tracing() {
    if std::env::var_os("RUST_LOG").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .with_test_writer()
            .try_init();
    }
}

/// Build a MntrsFs backed by in-memory OpenDAL (no real S3 needed).
/// Delegates to the public `mntrs::new_test_fs` helper which wires
/// the multi-level cache + writeback globals; the struct has too
/// many private fields to construct by hand here.
fn make_memory_fs() -> MntrsFs {
    let builder = Memory::default();
    let op = Operator::new(builder).unwrap().finish();
    let cache_dir = std::env::temp_dir().join("mntrs-wintest");
    let _ = std::fs::create_dir_all(&cache_dir);
    mntrs::new_test_fs(op, cache_dir)
}

fn rt_block_on<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    static RT: once_cell::sync::OnceCell<tokio::runtime::Runtime> =
        once_cell::sync::OnceCell::new();
    let rt = RT.get_or_init(|| tokio::runtime::Runtime::new().expect("tokio rt"));
    rt.block_on(f)
}

fn write_remote(fs: &MntrsFs, path: &str, data: &[u8]) {
    let op = fs.op.clone();
    let p = path.to_string();
    let d = data.to_vec();
    rt_block_on(async move {
        op.write(&p, d).await.unwrap();
    });
}

/// Wait for filesystem to settle.
fn settle() {
    std::thread::sleep(Duration::from_millis(200));
}

// ============================================================
// Tests
// ============================================================

#[test]
fn winfsp_mount_unmount_lifecycle() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    settle();
    // mount succeeded — guard keeps it alive
    drop(guard);
}

#[test]
fn winfsp_write_read_roundtrip() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    // Write via the in-memory backend (opendal), read back through
    // the WinFSP mount. The mount is at `guard.mount_path` (e.g.
    // "E:\\"); using a relative path here would hit the test
    // runner's CWD, not the mount, and the test would silently
    // test local I/O instead.
    write_remote(&fs, "test.txt", b"hello winfsp");
    let read = std::fs::read_to_string(format!("{mp}test.txt")).unwrap_or_default();
    assert_eq!(read, "hello winfsp");

    drop(guard);
}

#[test]
fn winfsp_list_directory() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    write_remote(&fs, "hello.txt", b"a");
    write_remote(&fs, "world.txt", b"b");

    let entries: Vec<String> = std::fs::read_dir(mp.as_str())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(entries.contains(&"hello.txt".to_string()));
    assert!(entries.contains(&"world.txt".to_string()));

    drop(guard);
}

#[test]
fn winfsp_create_delete() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    write_remote(&fs, "delete_me.txt", b"x");
    assert!(std::fs::read(format!("{mp}delete_me.txt")).is_ok());

    // Issue #298: std::fs::remove_file goes through IRP_MJ_SET_INFORMATION
    // (sets FILE_DELETE_ON_CLOSE) followed by IRP_MJ_CLEANUP. Pre-fix,
    // WinFspAdapter inherited the FileSystemContext trait's default
    // no-op cleanup, so the backend (opendal memory) kept the file
    // indefinitely even though the Win32 handle was closed.
    std::fs::remove_file(format!("{mp}delete_me.txt")).unwrap();
    assert!(std::fs::read(format!("{mp}delete_me.txt")).is_err());

    // Verify the opendal backend also no longer has the entry —
    // not just the WinFSP side. A regression where only the Win32
    // view is cleared would still leak at the backend level.
    let op = fs.op.clone();
    let still_present =
        rt_block_on(async move { op.exists("delete_me.txt").await.unwrap_or(true) });
    assert!(
        !still_present,
        "backend still has delete_me.txt after mount-side remove"
    );

    drop(guard);
}

#[test]
fn winfsp_rename_file() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    write_remote(&fs, "old.txt", b"rename me");
    std::fs::rename(format!("{mp}old.txt"), format!("{mp}new.txt")).unwrap();
    let read = std::fs::read_to_string(format!("{mp}new.txt")).unwrap_or_default();
    assert_eq!(read, "rename me");
    assert!(std::fs::read(format!("{mp}old.txt")).is_err());

    drop(guard);
}

/// Issue #78 regression: rename a file inside a subdirectory
/// whose parent was created via the opendal backend (no prior
/// mount-side lookup, no inode cached for the parent). Pre-fix,
/// `lib.rs::rename` derived the src path by walking
/// `self.resolve(_parent)` which returned `(root_ino, name)` —
/// the backend rename hit the wrong level and the file was not
/// moved. With `rename_paths` (WinFSP supplies the full paths
/// directly) the op.rename gets the correct `subdir/old.txt`.
#[test]
fn winfsp_rename_opendal_only_source() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    // Create the source via opendal — no mount-side touch, so
    // the subdir has no ino in the cache.
    write_remote(&fs, "subdir/old.txt", b"nested");
    // Touch the parent via mount so it shows up in readdir
    // (the rename itself shouldn't depend on this, but it
    // mirrors realistic use: a process writes to opendal,
    // another process renames via the mount).
    std::fs::create_dir_all(format!("{mp}subdir")).unwrap();
    settle();

    // The actual rename — this is the failing call pre-#78.
    std::fs::rename(
        format!("{mp}subdir\\old.txt"),
        format!("{mp}subdir\\new.txt"),
    )
    .unwrap();

    // The new name should be readable with the original content.
    let read = std::fs::read_to_string(format!("{mp}subdir\\new.txt")).unwrap_or_default();
    assert_eq!(read, "nested");
    // The old name should be gone (both via mount and via backend).
    assert!(std::fs::read(format!("{mp}subdir\\old.txt")).is_err());

    let op = fs.op.clone();
    let op2 = op.clone();
    let dst_exists = rt_block_on(async move { op.exists("subdir/new.txt").await.unwrap_or(false) });
    let src_exists = rt_block_on(async move { op2.exists("subdir/old.txt").await.unwrap_or(true) });
    assert!(dst_exists, "backend missing subdir/new.txt after rename");
    assert!(!src_exists, "backend still has subdir/old.txt after rename");

    drop(guard);
}

#[test]
fn winfsp_setattr_truncate() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    write_remote(&fs, "trunc.txt", b"hello world");
    // Truncate via SetEndOfFile — this IRP goes through the WinFSP
    // dispatcher added in #294, so the test only passes when the
    // dispatcher is actually running.
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(format!("{mp}trunc.txt"))
        .unwrap();
    f.set_len(5).unwrap();
    drop(f);

    let read = std::fs::read(format!("{mp}trunc.txt")).unwrap();
    assert_eq!(read.len(), 5);
    assert_eq!(&read, b"hello");

    drop(guard);
}

#[test]
fn winfsp_statfs_reports_volume() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    // statfs IRP goes through the WinFSP dispatcher. With the
    // pre-#294 helper (no start_with_threads), the call would hang
    // forever at the kernel side.
    let _ = std::fs::metadata(mp.as_str()).unwrap();

    drop(guard);
}

#[test]
fn winfsp_nested_directory() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    std::fs::create_dir_all(format!("{mp}a\\b\\c")).unwrap();

    // Write file in nested dir (via opendal backend, then read back
    // through the mount).
    write_remote(&fs, "a/b/c/deep.txt", b"nested");
    let read = std::fs::read_to_string(format!("{mp}a\\b\\c\\deep.txt")).unwrap_or_default();
    assert_eq!(read, "nested");

    // List nested dir through the mount.
    let entries: Vec<String> = std::fs::read_dir(format!("{mp}a\\b\\c"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(entries.contains(&"deep.txt".to_string()));

    drop(guard);
}

#[test]
fn winfsp_large_file_read() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    // Issue #302: >1MB to exercise the multi-chunk
    // fetch path. WinFSP's default VolumeParams
    // (`AlwaysUseDoubleBuffering=1`) caps the per-IRP
    // read buffer at 64 KiB, so a 2 MiB file is split
    // across multiple read() callbacks. The fix in
    // WinFspAdapter::read loops until the buffer is
    // full or the backend returns short, so
    // std::fs::read sees the full 2 MiB.
    let large_data = vec![0xABu8; 2 * 1024 * 1024];
    write_remote(&fs, "large.bin", &large_data);

    // Use File::open + read_to_end with an explicit large buffer
    // (instead of std::fs::read, which uses a stack-buffer
    // read_to_end loop and hits the 64 KiB IRP cap path).
    use std::io::Read;
    let mut f = std::fs::File::open(format!("{mp}large.bin")).unwrap();
    let mut read = Vec::with_capacity(2 * 1024 * 1024);
    f.read_to_end(&mut read).unwrap();
    eprintln!("read_to_end got {} bytes", read.len());
    assert_eq!(read.len(), 2 * 1024 * 1024);
    assert_eq!(read[0], 0xAB);
    assert_eq!(read[read.len() - 1], 0xAB);
    // Cross-check the middle 64 KiB boundary: byte
    // index 1 MiB - 1 should also be 0xAB (the
    // pre-fix 64 KiB cap would have read only
    // indices 0..=65535, leaving the 1 MiB byte as
    // default 0).
    assert_eq!(read[1024 * 1024 - 1], 0xAB);
    assert_eq!(read[1024 * 1024], 0xAB);

    drop(guard);
}

#[test]
fn winfsp_unicode_filename() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    let name = "中文文件.txt";
    write_remote(&fs, name, b"unicode");
    let read = std::fs::read_to_string(format!("{mp}{name}")).unwrap_or_default();
    assert_eq!(read, "unicode");

    let entries: Vec<String> = std::fs::read_dir(mp.as_str())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(entries.contains(&name.to_string()));

    drop(guard);
}

// ============================================================
// Issue #307 regression tests: Unicode NFC normalization on
// adapter entry points. The "café" literal in each test is
// NFC (precomposed é, U+00E9) — exactly what the Win32 layer
// hands to WinFSP via `RtlNormalizeString`. The fix ensures
// every kernel-supplied name is NFC-normalized at the
// callback boundary (get_security_by_name / open / create /
// rename / cleanup), so the trait methods always see NFC and
// the backend gets NFC keys consistently. The strong
// correctness proof lives in the `nfc_*` unit tests in
// src/util.rs; these are end-to-end contract tests that the
// helper integrates cleanly with the WinFSP dispatcher and
// the in-memory opendal backend.
// ============================================================

/// Issue #307: write an NFC-named file through the WinFSP
/// mount (exercising the `create` callback's NFC normalize
/// step) and read it back (exercising `get_security_by_name`,
/// `open`, and `read`). Pre-fix the Win32 layer would send
/// the raw UTF-16 path to the adapter; the adapter now
/// NFC-normalizes before the trait lookup, so the backend
/// sees a stable canonical key.
///
/// We deliberately don't also assert `read_dir` visibility
/// here: the dir_listers snapshot (issue #23) is taken at
/// `opendir` time, BEFORE the mount-side `create` finishes,
/// so the just-created file won't appear in a follow-up
/// readdir. That's a pre-existing limitation unrelated to
/// #307; the helper's unit tests cover the canonicalization
/// correctness directly.
#[test]
fn winfsp_nfc_create_and_read() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    // "café.txt" — NFC (precomposed é, U+00E9).
    let name = "café.txt";
    std::fs::write(format!("{mp}{name}"), b"x").unwrap();
    let read = std::fs::read_to_string(format!("{mp}{name}")).unwrap();
    assert_eq!(read, "x");

    drop(guard);
}

/// Issue #307: rename an NFC-named file through the WinFSP
/// mount (exercising the `rename` callback's normalize step
/// on both `file_name` and `new_file_name`). Pre-fix the
/// adapter didn't normalize the rename arguments; now both
/// names are NFC before `rename_paths` resolves parent inos.
#[test]
fn winfsp_nfc_rename() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    let src = "café_a.txt";
    let dst = "café_b.txt";
    std::fs::write(format!("{mp}{src}"), b"hello").unwrap();
    std::fs::rename(format!("{mp}{src}"), format!("{mp}{dst}")).unwrap();

    // src gone, dst present with same content.
    assert!(!std::path::Path::new(&format!("{mp}{src}")).exists());
    let read = std::fs::read_to_string(format!("{mp}{dst}")).unwrap();
    assert_eq!(read, "hello");

    drop(guard);
}

/// Issue #307: create + delete an NFC-named file through the
/// WinFSP mount (exercising the `cleanup` callback's NFC
/// normalize step). Pre-fix the basename passed to
/// `inner.unlink` could be a different Unicode form than the
/// `create` callback used, causing a NotFound on the backend
/// delete. Post-fix both ends agree on NFC.
///
/// Note: the post-delete assertion goes through the backend
/// (`op.exists`) rather than `std::fs::read` / `Path::exists`,
/// because the WinFSP cleanup callback (IRP_MJ_CLEANUP) fires
/// on the dispatcher thread (issue #294) — the same racy shape
/// as the existing `winfsp_create_delete` test at L119. Polling
/// the backend directly gives a deterministic answer.
#[test]
fn winfsp_nfc_delete() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    let name = "café_to_delete.txt";
    std::fs::write(format!("{mp}{name}"), b"bye").unwrap();
    assert!(std::path::Path::new(&format!("{mp}{name}")).exists());
    std::fs::remove_file(format!("{mp}{name}")).unwrap();
    settle();

    // Backend-side check: poll until the cleanup callback fires
    // and the in-memory opendal op no longer sees the entry.
    // Bounded retry because the dispatcher may be busy.
    let mut backend_gone = false;
    for _ in 0..20 {
        let op = fs.op.clone();
        let still_present =
            rt_block_on(async move { op.exists("café_to_delete.txt").await.unwrap_or(true) });
        if !still_present {
            backend_gone = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert!(
        backend_gone,
        "backend still has {name} after mount-side remove (cleanup callback never fired)"
    );

    drop(guard);
}

/// Issue #341: deleting a file that is still in the dirty
/// write-back cache must not let the upload worker re-create
/// it in the backend seconds later. Sequence:
///
/// 1. Mount-side `std::fs::write` → `WinFspAdapter::write`
///    → dirty cache file + `writeback_pending` set entry
///    with `per_task_delay = write_back_delay` (1 s in
///    `new_test_fs`, 5 s in production — both defer the
///    upload past the user's delete).
/// 2. Mount-side `std::fs::remove_file` → Win32
///    `IRP_MJ_SET_INFORMATION` + `IRP_MJ_CLEANUP` →
///    `WinFspAdapter::cleanup` → `inner.unlink` →
///    `op.delete` returns `NotFound` (file isn't in the
///    backend yet). Pre-fix the cleanup callback logged
///    this as "idempotent ok" and returned, BUT the
///    `writeback_pending` entry was left intact; the
///    worker later picked the task up and uploaded the
///    cached bytes — the file reappeared seconds after
///    the user thought they deleted it.
/// 3. Post-fix: `MntrsFs::unlink` removes the
///    `writeback_pending` entry + `.dirty` sidecar
///    BEFORE `op.delete`. The worker (still in the
///    delay queue, or already past it but not yet
///    dequeued) sees no pending task and never
///    schedules the upload.
/// 4. Poll the backend for >`write_back_delay`
///    (2.5 s in this test, well past the 1 s test
///    delay). `op.exists(name)` MUST return false.
///
/// The "delete reappears" regression is data corruption
/// from the user's POV — they believe V:\_ci_delete.txt
/// is gone, then see it surface again. Pre-fix this test
/// failed at the `!still_present` assertion at 2.5 s
/// post-delete; post-fix it passes cleanly.
#[test]
fn winfsp_create_delete_dirty_cache() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    let name = "_ci_delete_dirty.txt";

    // Step 1: write via the mount. This fires IRP_MJ_CREATE +
    // IRP_MJ_WRITE. The adapter's write path enqueues the
    // file in `writeback_pending` with the test fs's
    // `write_back_delay` (1 s) and a `.dirty` sidecar.
    std::fs::write(format!("{mp}{name}"), b"delete me").unwrap();
    assert!(
        std::path::Path::new(&format!("{mp}{name}")).exists(),
        "mount-side write did not produce a visible file"
    );

    // Brief settle for the dirty cache entry to land in
    // `writeback_pending` — well under the 1 s delay so
    // the worker hasn't dequeued yet.
    std::thread::sleep(Duration::from_millis(100));

    // Step 2: delete immediately. WinFSP will fire
    // FspCleanupDelete; our cleanup callback dispatches
    // `inner.unlink` which now drops the pending writeback
    // before the backend delete (Issue #341 fix).
    std::fs::remove_file(format!("{mp}{name}")).unwrap();

    // Step 3: wait longer than `write_back_delay`. Pre-fix
    // the worker's delay timer would fire here and upload
    // the cached bytes, making `op.exists` flip back to
    // `true`. Post-fix the upload is never scheduled.
    std::thread::sleep(Duration::from_millis(2500));

    // Step 4: verify the backend never re-uploaded the file.
    let op = fs.op.clone();
    let name_owned = name.to_string();
    let still_present = rt_block_on(async move { op.exists(&name_owned).await.unwrap_or(true) });
    assert!(
        !still_present,
        "{name} reappeared in the backend after delete \
         (Issue #341: writeback worker re-uploaded a file \
         whose delete raced the dirty-cache flush)"
    );

    drop(guard);
}

#[test]
fn winfsp_write_large_file() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    // Write via WinFSP — std::fs::write goes through the dispatcher
    // added in #294, so this IRP only completes when the dispatcher
    // is actually running. Pre-fix the call would hang at the kernel.
    let data = vec![0x42u8; 512 * 1024]; // 512KB
    std::fs::write(format!("{mp}big_write.bin"), &data).unwrap();

    // Read back through the mount to verify round-trip.
    let read = std::fs::read(format!("{mp}big_write.bin")).unwrap();
    assert_eq!(read.len(), 512 * 1024);
    assert_eq!(read[0], 0x42);
    assert_eq!(read[read.len() - 1], 0x42);

    drop(guard);
}

// ============================================================
// Issue #306 regression tests: readdir_with_attrs (N+1 RTT + marker paging)
// ============================================================

/// Issue #306: readdir_with_attrs must return the entry's
/// real `size` (not the synthesized zero) so Explorer
/// shows "N bytes" instead of "0 bytes" without a follow-up
/// `get_file_info` click. Pre-fix, the WinFSP adapter built
/// a `CoreFileAttr { size: 0, mtime: UNIX_EPOCH, ... }`
/// for every entry; post-fix it pulls real attrs from the
/// dir_cache snapshot via `batch_lookup_from_dir_cache`.
///
/// The test calls `readdir_with_attrs` directly through
/// `MntrsFs` (no WinFSP dispatcher needed). The WinFSP
/// adapter's only contract is that it forwards `(ino, fh,
/// marker)` to this method; the bug fix lives at the trait
/// method override, which is what we exercise here.
#[test]
fn winfsp_readdir_real_attrs() {
    use std::time::SystemTime;

    let fs = make_memory_fs();

    // Write 100 files with sizes 1..=100 bytes so each
    // entry has a distinct, easy-to-verify size.
    for i in 1..=100u64 {
        let name = format!("f_{:03}.txt", i);
        let body = vec![b'A'; i as usize];
        write_remote(&fs, &name, &body);
    }

    // opendir pins the per-fh snapshot (lib.rs:2754).
    let dir_fh = <MntrsFs as CoreFilesystem>::opendir(&fs, 1).expect("opendir root");
    let pages = <MntrsFs as CoreFilesystem>::readdir_with_attrs(&fs, 1, dir_fh, "")
        .expect("readdir_with_attrs");

    // Expect 100 file entries plus the synthesized "." /
    // ".." (readdir_materialise prepends them at
    // lib.rs:1007-1018).
    assert!(
        pages.len() >= 100,
        "expected >= 100 entries (got {})",
        pages.len()
    );

    // For each `f_NNN.txt` entry the size must equal N (the
    // real body size), NOT 0 (the pre-fix synthesized
    // value).
    let mut seen_sizes: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for (entry, attr) in &pages {
        if entry.name == "." || entry.name == ".." {
            continue;
        }
        seen_sizes.insert(entry.name.clone(), attr.size);
    }
    for i in 1..=100u64 {
        let name = format!("f_{:03}.txt", i);
        let got = seen_sizes
            .get(&name)
            .copied()
            .unwrap_or_else(|| panic!("entry {name} missing from readdir_with_attrs result"));
        assert_eq!(
            got, i,
            "entry {name}: expected size={i}, got {got} (pre-fix would be 0)"
        );
    }

    // mtime check: skip if no entry has a real mtime (some
    // backends like opendal memory don't populate
    // last_modified; if every entry has mtime=epoch the
    // attr is honestly reflecting the backend, not the
    // pre-fix synthesized zero). The size assertion above
    // is the real win — the rest is a soft check.
    let real_mtimes = pages
        .iter()
        .filter(|(e, _)| e.name != "." && e.name != "..")
        .filter(|(_, a)| a.mtime != SystemTime::UNIX_EPOCH)
        .count();
    if real_mtimes > 0 {
        eprintln!("{real_mtimes}/100 entries have non-epoch mtime");
    } else {
        eprintln!("backend does not populate last_modified — size check above is the real win");
    }

    // Cleanup: drop the per-fh snapshot so the test_helpers
    // drop on the WinFspAdapter doesn't leak it (we never
    // mounted, but be tidy).
    let _ = <MntrsFs as CoreFilesystem>::releasedir(&fs, 1, dir_fh);
}

/// Issue #306: readdir_with_attrs must slice the per-fh
/// snapshot by marker so each page only contains entries
/// strictly greater than the marker. Pre-fix the WinFSP
/// adapter re-materialised the full Vec on every page
/// (`inner.readdir(ino, fh, 0, 0)`), which (a) wasted work
/// and (b) made the listing sensitive to concurrent
/// dir_cache invalidations between pages.
///
/// Post-fix: each call returns only the next slice, and
/// walking the directory by feeding back the last entry's
/// name as the next marker must visit every entry exactly
/// once with no duplicates.
#[test]
fn winfsp_readdir_marker_paging() {
    use std::collections::HashSet;

    let fs = make_memory_fs();

    // Create 100 small files.
    for i in 0..100u64 {
        let name = format!("p_{:03}.bin", i);
        write_remote(&fs, &name, b"x");
    }

    let dir_fh = <MntrsFs as CoreFilesystem>::opendir(&fs, 1).expect("opendir root");

    // Walk: each call passes the last delivered name as
    // the next marker. Empty marker = first page. The
    // returned slice must be non-empty until the dir is
    // exhausted.
    let mut marker = String::new();
    let mut collected: Vec<String> = Vec::new();
    let mut page_count = 0usize;
    loop {
        let page = <MntrsFs as CoreFilesystem>::readdir_with_attrs(&fs, 1, dir_fh, &marker)
            .expect("readdir_with_attrs page");
        if page.is_empty() {
            break;
        }
        page_count += 1;
        // Sanity: every entry's name must be strictly >
        // the marker (WinFSP marker semantics).
        for (entry, _attr) in &page {
            assert!(
                entry.name.as_str() > marker.as_str(),
                "entry {} not > marker {:?} (page {})",
                entry.name,
                marker,
                page_count
            );
        }
        marker = page.last().unwrap().0.name.clone();
        collected.extend(page.into_iter().map(|(e, _)| e.name));
        // Safety bound: never loop more than N+1 times.
        assert!(page_count <= 200, "paging did not terminate in 200 pages");
    }

    // No duplicates.
    let unique: HashSet<&String> = collected.iter().collect();
    assert_eq!(
        unique.len(),
        collected.len(),
        "marker paging produced duplicates: {:?}",
        collected
    );

    // Every file we wrote must show up exactly once.
    for i in 0..100u64 {
        let name = format!("p_{:03}.bin", i);
        assert_eq!(
            collected.iter().filter(|n| n.as_str() == name).count(),
            1,
            "{name} not visited exactly once across pages"
        );
    }

    let _ = <MntrsFs as CoreFilesystem>::releasedir(&fs, 1, dir_fh);
}

// ============================================================
// 通用 mount_internal 参数测试 (non-Windows specific)
// ============================================================
#[test]
fn test_generic_mount_internal_schemes() {
    let tmp = std::env::temp_dir().join(format!("mntrs-gen-mount-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);
    let opts = std::collections::HashMap::new();

    for scheme in &["s3", "gs", "azblob", "oss", "cos", "obs", "b2", "hdfs"] {
        let storage = format!("{}://bucket", scheme);
        let result =
            mntrs::cmd::mount::mount_internal(&storage, tmp.to_str().unwrap(), &opts, false);
        // Should fail gracefully (no credentials) not panic
        assert!(result.is_err(), "{} should fail gracefully", scheme);
    }
    let _ = std::fs::remove_dir_all(&tmp);
}

/// mount_internal with TLS options
#[test]
fn test_generic_mount_tls_options() {
    let tmp = std::env::temp_dir().join(format!("mntrs-gen-tls-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);

    // With cacert that doesn't exist — should fail
    let opts = std::collections::HashMap::from([(
        "cacert".to_string(),
        "/nonexistent/ca.pem".to_string(),
    )]);
    let result =
        mntrs::cmd::mount::mount_internal("s3://bucket", tmp.to_str().unwrap(), &opts, false);
    assert!(result.is_err(), "nonexistent cacert should fail");

    // With cert that doesn't exist
    let opts = std::collections::HashMap::from([
        ("cert".to_string(), "/nonexistent/cert.pem".to_string()),
        ("key".to_string(), "/nonexistent/key.pem".to_string()),
    ]);
    let result =
        mntrs::cmd::mount::mount_internal("s3://bucket", tmp.to_str().unwrap(), &opts, false);
    assert!(result.is_err(), "nonexistent cert should fail");

    let _ = std::fs::remove_dir_all(&tmp);
}

/// 验证各种 vfs 参数不被 mount_internal 忽略
#[test]
fn test_generic_mount_vfs_params() {
    let tmp = std::env::temp_dir().join(format!("mntrs-gen-vfs-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);

    // Various VFS params passed as --opt
    let opts = std::collections::HashMap::from([
        ("dir_cache_time".to_string(), "30".to_string()),
        ("attr_timeout".to_string(), "5".to_string()),
        ("vfs_cache_max_size".to_string(), "2048".to_string()),
        ("vfs_write_back".to_string(), "10".to_string()),
        ("vfs_read_ahead".to_string(), "262144".to_string()),
        ("read_only".to_string(), "true".to_string()),
    ]);
    let result =
        mntrs::cmd::mount::mount_internal("s3://bucket", tmp.to_str().unwrap(), &opts, false);
    let _ = std::fs::remove_dir_all(&tmp);
    assert!(result.is_err(), "should fail gracefully with vfs params");
}

/// unmount_internal with various mount paths
#[test]
fn test_generic_unmount_various_paths() {
    // Absolute path
    let r = mntrs::cmd::mount::unmount_internal("/tmp/mntrs-test-abs");
    assert!(r.is_ok(), "unmount absolute path should be graceful");

    // Relative path
    let r = mntrs::cmd::mount::unmount_internal("relative-path");
    assert!(r.is_ok(), "unmount relative path should be graceful");

    // Path with special chars
    let r = mntrs::cmd::mount::unmount_internal("/tmp/mntrs test with spaces");
    assert!(r.is_ok(), "unmount path with spaces should be graceful");
}
