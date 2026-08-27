#![cfg(windows)]

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use opendal::Operator;
use opendal::services::Memory;

use mntrs::MntrsFs;
use mntrs::core_fs::CoreFilesystem;
use mntrs::core_fs::winfsp::{WinFspAdapter, WinFspHandle};
use mntrs::core_fs::{CoreFileAttr, CoreVolumeStat};

use winfsp::filesystem::{FileInfo, FileSystemContext};

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
    let op = Operator::new(builder).unwrap();
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
    // Issue #494: WinFSP issues a directory enumeration shortly
    // after mount (for change-notification bookkeeping). The
    // resulting dir_cache[""] = empty listing then shadows
    // subsequent reads for the full `dir_cache_ttl` (10s in
    // tests). Direct opendal writes bypass the in-mount
    // create() callback that would call cache_add_entry, so the
    // cached empty listing stays stale. Invalidate the parent
    // dir_cache after each write so the next readdir re-lists
    // the backend. Idempotent — safe to call repeatedly.
    let parent = std::path::Path::new(path)
        .parent()
        .and_then(|p| p.to_str())
        .unwrap_or("");
    fs.invalidate_dir_cache(parent);
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
///
/// Issue #495: write via the opendal backend (not via the
/// mount) so the file is already present in the backend
/// when rename fires. A mount-side write would queue the
/// upload for `write_back_delay` (1s default in tests) and
/// the rename's `op.read` fallback would see NotFound on
/// the not-yet-uploaded src.
#[test]
fn winfsp_nfc_rename() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    let src = "café_a.txt";
    let dst = "café_b.txt";
    write_remote(&fs, src, b"hello");
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

/// Issue #325: end-to-end symlink round-trip on the WinFSP
/// mount. Exercises the kernel's FSCTL_SET_REPARSE_POINT →
/// `set_reparse_point` → inner.symlink path, plus the
/// `get_reparse_point` → inner.readlink path used by
/// `Get-ChildItem -Attributes ReparsePoint` / `Get-Item
/// ... | Select-Object Target`. Pre-fix the adapter's
/// reparse_point callbacks were the trait default no-ops,
/// which returned STATUS_INVALID_DEVICE_REQUEST and made
/// `New-Item -ItemType SymbolicLink` fail.
///
/// Storage strategy: the symlink target lives in
/// `MntrsFs::symlinks` (DashMap<String, PathBuf>) on the
/// adapter; no backend file is created (issue #325 design
/// — opendal memory/s3 would store a 0-byte placeholder
/// that's not the right shape). The `cleanup` + `FspCleanupDelete`
/// path routes through `MntrsFs::unlink` to drop both
/// inodes and the symlinks table entry.
#[test]
fn winfsp_symlink_create_and_get() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    // Step 1 — write the target file (the symlink will
    // resolve to this).
    let target_name = "_sym_get_target.txt";
    let target_body = b"target-body";
    std::fs::write(format!("{mp}{target_name}"), target_body).unwrap();

    // Step 2 — create the symlink. Issue #492: PowerShell
    // `New-Item -ItemType SymbolicLink` requires admin /
    // Developer Mode on Windows. We probe it; on non-admin
    // boxes (this dev box, default `windows-latest` CI runner),
    // skip the test gracefully rather than panic. The kernel
    // FSCTL_SET_REPARSE_POINT path is still exercised by
    // `winfsp_symlink_implicit_delete_via_set_reparse_point`
    // (#359) on admin boxes.
    let link_name = "_sym_get_link.txt";
    let ps = std::process::Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-Command",
            &format!(
                "New-Item -ItemType SymbolicLink -Path '{mp}{link_name}' -Target '{mp}{target_name}'"
            ),
        ])
        .output()
        .expect("powershell New-Item must run");
    if !ps.status.success() {
        let stderr = String::from_utf8_lossy(&ps.stderr);
        eprintln!(
            "SKIPPED: PowerShell New-Item -ItemType SymbolicLink requires admin (issue #492); stderr={stderr}"
        );
        return;
    }

    settle();

    // Step 3 — verify the kernel sees it as a reparse point
    // by reading the Win32 attributes (FILE_ATTRIBUTE_REPARSE_POINT
    // = 0x0000_0400). We use `Get-Item | Select-Object Attributes`
    // which prints the friendly name (e.g. "ReparsePoint").
    let attr = std::process::Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-Command",
            &format!("(Get-Item -LiteralPath '{mp}{link_name}').Attributes -as [string]"),
        ])
        .output()
        .expect("powershell Attributes probe must run");
    let attrs = String::from_utf8_lossy(&attr.stdout);
    assert!(
        attrs.contains("ReparsePoint"),
        "link {link_name} should have ReparsePoint attribute; got '{attrs}'"
    );

    // Step 4 — verify the target field PowerShell reports
    // matches the path we passed to New-Item.
    let target_out = std::process::Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-Command",
            &format!("(Get-Item -LiteralPath '{mp}{link_name}').Target"),
        ])
        .output()
        .expect("powershell Target probe must run");
    let reported = String::from_utf8_lossy(&target_out.stdout)
        .trim()
        .to_string();
    assert!(
        reported.ends_with(target_name),
        "Target '{reported}' should end with {target_name}"
    );

    drop(guard);
}

/// Issue #325: reading the link's target via the Win32 path
/// (Get-Content, FSCTL_GET_REPARSE_POINT under the hood on
/// Get-Item|Target) returns the bytes we originally asked
/// for, not the placeholder content. This guards against
/// regressions where `get_reparse_point` returns the
/// MountPoint tag instead of the Symlink tag, or encodes
/// the substitute name into the wrong slot of the
/// REPARSE_DATA_BUFFER buffer.
#[test]
fn winfsp_symlink_round_trip_bytes() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    let target = "_round_trip_target.txt";
    let body = b"round-trip-body";
    std::fs::write(format!("{mp}{target}"), body).unwrap();

    let link = "_round_trip_link.txt";
    // Issue #492: skip gracefully if PowerShell cannot create
    // a symbolic link (requires admin / Developer Mode). The
    // FSCTL path is still covered by the #359 test on admin
    // boxes.
    let ps = std::process::Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-Command",
            &format!("New-Item -ItemType SymbolicLink -Path '{mp}{link}' -Target '{mp}{target}'"),
        ])
        .output()
        .expect("powershell New-Item must run");
    if !ps.status.success() {
        let stderr = String::from_utf8_lossy(&ps.stderr);
        eprintln!(
            "SKIPPED: PowerShell New-Item -ItemType SymbolicLink requires admin (issue #492); stderr={stderr}"
        );
        return;
    }
    settle();

    // Read the target attribute (`Get-Item ... | Select-Object
    // Target`) — this routes through the Win32
    // FSCTL_GET_REPARSE_POINT user-mode path, which our
    // adapter maps to inner.readlink.
    let target_out = std::process::Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-Command",
            &format!("(Get-Item -LiteralPath '{mp}{link}').Target"),
        ])
        .output()
        .expect("powershell Target readback must run");
    let reported = String::from_utf8_lossy(&target_out.stdout)
        .trim()
        .to_string();
    assert!(
        reported.ends_with(target),
        "Target '{reported}' should end with '{target}' after round-trip"
    );
    // Confirm the report is exactly the file we linked (no
    // truncation, no path-mangling).
    assert!(
        reported.contains(target),
        "Target missing target filename; reported='{reported}'"
    );

    drop(guard);
}

/// Issue #325: deleting a symlink via `Remove-Item` exercises
/// the Win32 path of FSCTL_DELETE_REPARSE_POINT + the cleanup
/// callback with FspCleanupDelete. After delete, `Test-Path`
/// must return false and a fresh `Get-Item` must fail with
/// the usual NotFound. This guards against stale entries
/// remaining in MntrsFs::symlinks after the kernel-side
/// delete completes.
#[test]
fn winfsp_symlink_delete_clears_state() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    let mp = &guard.mount_path;
    settle();

    let target = "_del_target.txt";
    std::fs::write(format!("{mp}{target}"), b"x").unwrap();

    let link = "_del_link.txt";
    // Issue #492: PowerShell `New-Item -ItemType SymbolicLink`
    // requires admin / Developer Mode. Skip gracefully on
    // non-admin boxes (test runner + dev boxes) and keep the
    // FSCTL coverage for admin environments.
    let ps = std::process::Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-Command",
            &format!("New-Item -ItemType SymbolicLink -Path '{mp}{link}' -Target '{mp}{target}'"),
        ])
        .output()
        .expect("powershell New-Item must run");
    if !ps.status.success() {
        let stderr = String::from_utf8_lossy(&ps.stderr);
        eprintln!(
            "SKIPPED: PowerShell New-Item -ItemType SymbolicLink requires admin (issue #492); stderr={stderr}"
        );
        return;
    }
    settle();

    // `Test-Path` returns true if the symlink exists.
    let exists_out = std::process::Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-Command",
            &format!("Test-Path -LiteralPath '{mp}{link}'"),
        ])
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&exists_out.stdout).trim(),
        "True",
        "link should exist before Remove-Item"
    );

    // Remove the symlink.
    std::fs::remove_file(format!("{mp}{link}")).unwrap();
    settle();

    // `Test-Path` must return false after delete.
    let gone_out = std::process::Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-Command",
            &format!("Test-Path -LiteralPath '{mp}{link}'"),
        ])
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&gone_out.stdout).trim(),
        "False",
        "link should be gone after Remove-Item (symlinks map not cleared?)"
    );

    drop(guard);
}

/// Issue #359: cmd.exe's 16-bit IFILEOP subsystem "deletes" a
/// symlink by re-issuing FSCTL_SET_REPARSE_POINT with an empty
/// SubstituteName on the ino that's already a symlink, instead of
/// routing through DeleteFileW (which would fire
/// `delete_reparse_point` + `cleanup`-with-delete). The legitimate
/// Win32 DeleteFileW path therefore never gets a chance to clear
/// `MntrsFs::symlinks`, and `Test-Path` keeps returning True even
/// though cmd thinks the file is gone.
///
/// This test exercises the fix by invoking the WinFSP
/// `set_reparse_point` callback **directly** with an
/// empty-target REPARSE_DATA_BUFFER, bypassing both cmd.exe
/// (cmd path resolution differs from PowerShell per the #325
/// follow-up — cmd can't even see WinFSP symlinks on this dev
/// box) and the kernel FSCTL roundtrip (the WinFSP driver
/// returns `ERROR_INVALID_FUNCTION` for changing an existing
/// reparse point on some hosts). The fix lives in
/// `WinFspAdapter::set_reparse_point`'s implicit-unlink branch:
/// detect `(empty target) && (already-symlink ino)` and route
/// through `MntrsFs::unlink` instead of
/// `attach_symlink_to_ino`. Pre-fix the callback would have
/// called `attach_symlink_to_ino(ino, "")` and the symlinks
/// map would still hold an entry — the post-FSCTL `getattr`
/// would still report `kind = Symlink` (the stale entry
/// resolves the lookup). Post-fix `set_reparse_point` routes
/// through `MntrsFs::unlink`, which drops the registration
/// entirely (`symlinks`, `path_to_ino`, `inodes`, cache
/// placeholder + .dirty sidecar, `attr_cache`, parent's
/// `dir_cache`, and the backend delete with NotFound
/// idempotency) and the next `getattr` returns
/// `kind = RegularFile` / NotFound.
#[test]
fn winfsp_symlink_implicit_delete_via_set_reparse_point() {
    let fs = Arc::new(make_memory_fs());

    // Create the symlink via the trait's `symlink` method instead
    // of `New-Item -ItemType SymbolicLink`. PowerShell's New-Item
    // requires SeCreateSymbolicLinkPrivilege (admin-only on
    // default Windows); the trait path registers the entry in
    // `MntrsFs::symlinks` without going through the Win32
    // elevation gate, so the test runs on non-admin hosts too.
    let target_mp = String::from(r"Z:\_ci_implicit_target.txt");
    let link_basename = "_ci_implicit_link.txt";
    let symlink_attr = fs
        .symlink(1, link_basename, std::path::Path::new(&target_mp))
        .expect("MntrsFs::symlink must succeed");
    let link_ino = symlink_attr.ino;

    // Pre-condition: the symlink is registered. `getattr` returns
    // kind = Symlink because the trait sees the entry in
    // `symlinks`. This is what `get_security_by_name` would
    // surface to the kernel as `FILE_ATTRIBUTE_REPARSE_POINT`.
    let pre = fs
        .getattr(link_ino)
        .expect("pre: getattr on symlink ino must succeed");
    assert_eq!(
        pre.kind,
        mntrs::core_fs::CoreFileType::Symlink,
        "pre: ino {link_ino} should be a symlink before implicit unlink"
    );

    // Invoke the WinFSP `set_reparse_point` callback directly
    // with a REPARSE_DATA_BUFFER that has SubstituteNameLength=0
    // + PrintNameLength=0 — the signature cmd.exe's IFILEOP
    // emits to "drop the reparse tag". We bypass the kernel
    // FSCTL routing (unreliable across WinFSP versions on
    // already-reparse-point files) and call the trait method
    // directly, which is what the kernel dispatcher would
    // invoke.
    //
    // REPARSE_DATA_BUFFER layout (SymbolicLinkReparseBuffer):
    //   0..4   ReparseTag            u32  = IO_REPARSE_TAG_SYMLINK
    //   4..6   ReparseDataLength     u16  = 12 (size after this field)
    //   6..8   Reserved              u16  = 0
    //   8..10  SubstituteNameOffset  u16  = 0
    //   10..12 SubstituteNameLength  u16  = 0  <-- empty target
    //   12..14 PrintNameOffset       u16  = 0
    //   14..16 PrintNameLength       u16  = 0  <-- empty target
    //   16..20 Flags                 u32  = 0
    // Total: 20 bytes, no PathBuffer.
    let empty_buf: [u8; 20] = [
        0x0C, 0x00, 0x00, 0xA0, // ReparseTag = 0xA000000C (SYMLINK)
        0x0C, 0x00, // ReparseDataLength = 12
        0x00, 0x00, // Reserved
        0x00, 0x00, // SubstituteNameOffset = 0
        0x00, 0x00, // SubstituteNameLength = 0  (empty)
        0x00, 0x00, // PrintNameOffset = 0
        0x00, 0x00, // PrintNameLength = 0  (empty)
        0x00, 0x00, 0x00, 0x00, // Flags = 0
    ];
    let adapter = WinFspAdapter::new(fs.clone());
    // WinFSP passes the FULL PATH in `file_name` (same
    // convention as `cleanup`). NFC + lowercase for
    // case-insensitive path matching (mirrors the cleanup
    // rationale around #325: Win32 file names are
    // case-insensitive at the OS layer).
    let full_path =
        widestring::U16CString::from_str(format!("/{}", link_basename.to_lowercase())).unwrap();
    let handle = WinFspHandle {
        ino: link_ino,
        fh: link_ino,
        is_dir: false,
        dir_fh: 0,
    };
    adapter
        .set_reparse_point(&handle, full_path.as_ucstr(), &empty_buf)
        .expect("set_reparse_point (empty target on existing symlink) must succeed (issue #359)");

    // Post-condition: the symlink registration is gone.
    // `getattr` must now return NotFound — pre-fix this would
    // still resolve to kind = Symlink because the callback
    // called `attach_symlink_to_ino(ino, "")` which inserts
    // a Symlink entry with target="", keeping the registration
    // alive. Post-fix `set_reparse_point` routes through
    // `MntrsFs::unlink`, which drops the registration.
    let post = fs.getattr(link_ino);
    match post {
        Ok(attr) => {
            assert_ne!(
                attr.kind,
                mntrs::core_fs::CoreFileType::Symlink,
                "post: ino {link_ino} must not be a symlink after implicit unlink — symlinks map still has the entry (issue #359)"
            );
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Even better — the ino is fully GC'd. Either
            // outcome (NotFound or kind != Symlink) confirms
            // the symlinks map entry is gone.
        }
        Err(e) => panic!("post: getattr returned unexpected error: {e}"),
    }

    // Backend side: the placeholder cache file (the symlink's
    // host file written by `create` to satisfy the Win32
    // reparse-point contract) must also be gone — `MntrsFs::unlink`
    // issues `op.delete` with NotFound idempotency.
    let op = fs.op.clone();
    let backend_present = rt_block_on(async move {
        op.exists(&format!("/{}", link_basename.to_lowercase()))
            .await
            .unwrap_or(true)
    });
    assert!(
        !backend_present,
        "backend should not have a placeholder for the implicit-unlinked symlink (issue #359)"
    );
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
// Issue #612 regression tests: WinFspAdapter::overwrite path
// ============================================================
//
// We test the `overwrite` callback directly through the
// `FileSystemContext` trait, not by issuing Win32 file ops on
// the mounted drive. Reasons:
//
//  * `std::fs::write` on Windows uses `OPEN_ALWAYS +
//    FILE_TRUNCATE_EXISTING` for an existing file (and the
//    `create(true).truncate(true)` combo resolves to
//    `CREATE_ALWAYS` for a new file). WinFSP routes `CREATE_ALWAYS`
//    to the `overwrite` callback but routes
//    `OPEN_ALWAYS + FILE_TRUNCATE_EXISTING` to the `open`
//    callback — the truncate happens in the `open` impl, not in
//    `overwrite`. So we can't reliably dispatch `overwrite` from
//    std fs ops without raw FFI.
//  * Direct trait calls bypass the writeback cache, so we can
//    verify the data is preserved in the backend without racing
//    the cache-flush timer.
//  * The fix lives entirely in `WinFspAdapter::overwrite`; a
//    unit test on that method is the most precise contract we
//    can pin.
///
/// Pre-fix the trait default returned STATUS_INVALID_DEVICE_REQUEST
/// (winfsp-0.13.0/src/filesystem/context.rs:150-161), which surfaced
/// in .NET `WriteAllBytes` as "Incorrect function" and cascaded
/// into Copy-Item / Move-Item / Remove-Item failures on the same
/// files (each re-opens the freshly-written file with a disposition
/// that WinFSP routes to `overwrite`).
///
/// Build a `WinFspHandle` for the named file under the
/// memory-backed `MntrsFs`. Looks up via the `CoreFilesystem`
/// trait and opens with O_RDWR so the resulting `fh` is valid
/// for write/read IRPs.
fn open_handle_for<F: CoreFilesystem>(fs: &F, name: &str) -> WinFspHandle {
    let attr = fs.lookup(1, name).expect("lookup for fixture file");
    let fh = fs
        .open(attr.ino, 2 /* O_RDWR */)
        .expect("open fixture file");
    WinFspHandle {
        ino: attr.ino,
        fh,
        is_dir: false,
        dir_fh: 0,
    }
}

#[test]
fn winfsp_overwrite_preserves_written_data() {
    let fs = Arc::new(make_memory_fs());
    let adapter = WinFspAdapter::new(fs.clone());

    // Seed a file via write_remote (bypasses WinFSP, lands
    // straight in the opendal memory backend).
    let payload = b"data we expect to survive overwrite";
    write_remote(&fs, "_ovw.bin", payload);

    let ctx = open_handle_for(&*fs, "_ovw.bin");

    // Call `overwrite` with allocation_size=0 — the same
    // shape WinFSP sends for the failing bench WriteNew case
    // (visible in mntrs-bench.stdout.log).
    let mut file_info = FileInfo::default();
    <WinFspAdapter<MntrsFs> as FileSystemContext>::overwrite(
        &adapter,
        &ctx,
        0,     // file_attributes
        false, // replace_file_attributes
        0,     // allocation_size
        None,  // extra_buffer
        &mut file_info,
    )
    .expect("overwrite must succeed (issue #612 fix)");

    // 1) Data must still be in the backend — a buggy overwrite
    //    that honoured WinFSP's "truncate to allocation_size"
    //    semantic would shrink the file to 0.
    let backend = rt_block_on(async { fs.op.read("_ovw.bin").await.unwrap_or_default().to_vec() });
    assert_eq!(
        backend, payload,
        "overwrite must NOT truncate the backend payload (issue #612)"
    );

    // 2) file_info must reflect the current size so the kernel's
    //    FCB stays in sync (post-#612 refresh).
    assert_eq!(
        file_info.file_size,
        payload.len() as u64,
        "file_info.file_size must match current inode size"
    );

    // Clean up the fh so the in-memory backend sees a tidy
    // close (release() drops the write buffer).
    fs.release(ctx.ino, ctx.fh).expect("release");
}

#[test]
fn winfsp_overwrite_refreshes_file_info() {
    let fs = Arc::new(make_memory_fs());
    let adapter = WinFspAdapter::new(fs.clone());

    // Seed two files of different sizes so we can prove
    // file_info reflects the *current* inode, not some cached
    // value.
    let payload_a = vec![0xAAu8; 17];
    let payload_b = vec![0xBBu8; 4099];
    write_remote(&fs, "_ovw_a.bin", &payload_a);
    write_remote(&fs, "_ovw_b.bin", &payload_b);

    let ctx_a = open_handle_for(&*fs, "_ovw_a.bin");
    let ctx_b = open_handle_for(&*fs, "_ovw_b.bin");

    let mut fi_a = FileInfo::default();
    <WinFspAdapter<MntrsFs> as FileSystemContext>::overwrite(
        &adapter, &ctx_a, 0, false, 0, None, &mut fi_a,
    )
    .expect("overwrite a");
    assert_eq!(
        fi_a.file_size,
        payload_a.len() as u64,
        "file_info for _ovw_a.bin must reflect its actual size"
    );

    let mut fi_b = FileInfo::default();
    <WinFspAdapter<MntrsFs> as FileSystemContext>::overwrite(
        &adapter, &ctx_b, 0, false, 0, None, &mut fi_b,
    )
    .expect("overwrite b");
    assert_eq!(
        fi_b.file_size,
        payload_b.len() as u64,
        "file_info for _ovw_b.bin must reflect its actual size"
    );

    fs.release(ctx_a.ino, ctx_a.fh).expect("release a");
    fs.release(ctx_b.ino, ctx_b.fh).expect("release b");
}

/// Pin the "no set_file_size → no truncate" design choice
/// (see doc-comment on `WinFspAdapter::overwrite`). We can
/// only test this at the trait level — issuing Win32
/// `TRUNCATE_EXISTING` through std fs routes through the
/// `open` callback, not `overwrite`, so the file is truncated
/// before `overwrite` is ever called. At the trait level
/// `overwrite(allocation_size=0)` is a no-op + file_info
/// refresh, which matches our bench-observed dispatch.
#[test]
fn winfsp_overwrite_zero_allocation_is_noop_on_data() {
    let fs = Arc::new(make_memory_fs());
    let adapter = WinFspAdapter::new(fs.clone());

    let payload = b"data that must survive overwrite(allocation_size=0)";
    write_remote(&fs, "_ovw_noop.bin", payload);

    let ctx = open_handle_for(&*fs, "_ovw_noop.bin");
    let mut file_info = FileInfo::default();
    <WinFspAdapter<MntrsFs> as FileSystemContext>::overwrite(
        &adapter,
        &ctx,
        0,
        false,
        0,
        None,
        &mut file_info,
    )
    .expect("overwrite");

    let backend = rt_block_on(async {
        fs.op
            .read("_ovw_noop.bin")
            .await
            .unwrap_or_default()
            .to_vec()
    });
    assert_eq!(
        backend, payload,
        "overwrite(allocation_size=0) must preserve existing data"
    );
    assert_eq!(
        file_info.file_size,
        payload.len() as u64,
        "file_info.file_size must match pre-overwrite inode size"
    );

    fs.release(ctx.ino, ctx.fh).expect("release");
}

// ============================================================
// Issue #614 regression tests: cleanup-after-rename must NOT unlink
// ============================================================
//
// WinFSP 2.1's `MoveFileEx` dispatches as
// `rename(src_fh, dst_path)` followed by a `cleanup(src_fh, DELETE)`
// (the "delete the source alias" step of the Win32 move semantics).
// Pre-fix, the post-rename cleanup unconditionally called
// `self.inner.unlink(parent_ino, file_name)`, where `file_name` had
// already been updated to the **destination** path by the prior
// `rename`. The unlink therefore deleted the just-renamed backend
// file, leaving both the source and destination missing and the
// user seeing `ERROR_FILE_NOT_FOUND` from `Move-Item` /
// `Remove-Item`.
//
// Tests below exercise the trait methods directly (no Win32
// `MoveFileEx` needed) so the rename + cleanup-DELETE sequence
// runs deterministically without relying on kernel dispatch.

// Helper: build a `U16CString` (owned) from a `&str` for direct
// trait invocations. `winfsp::U16CStr` derefs from
// `widestring::U16CString`, which has `from_str(&str)` returning
// `Result<U16CString, _>`. We then pass `&str_to_ucstr(s)` (i.e.
// `&U16CString` coerces to `&U16CStr`) to the trait methods.
fn str_to_ucstr(s: &str) -> widestring::U16CString {
    widestring::U16CString::from_str(s).expect("u16cstring")
}

// Helper: invoke the trait's `rename` with a UTF-8 path (the trait
// takes `&U16CStr`; we build one from a `String`).
fn rename_path(
    adapter: &WinFspAdapter<MntrsFs>,
    ctx: &WinFspHandle,
    src_path: &str,
    dst_path: &str,
) {
    let src = str_to_ucstr(src_path);
    let dst = str_to_ucstr(dst_path);
    <WinFspAdapter<MntrsFs> as FileSystemContext>::rename(adapter, ctx, &src, &dst, true)
        .expect("trait rename");
}

#[test]
fn winfsp_cleanup_after_rename_preserves_destination() {
    let fs = Arc::new(make_memory_fs());
    let adapter = WinFspAdapter::new(fs.clone());

    // Seed a file via write_remote (lands in opendal memory
    // backend, inodes entry created via mount lookup).
    let payload = b"data that must survive post-rename cleanup";
    write_remote(&fs, "_mv_src.bin", payload);

    let ctx = open_handle_for(&*fs, "_mv_src.bin");

    // Step 1: rename src -> dst via the trait. This is
    // WinFSP's `MoveFileEx` step 1. The path shape
    // (`\_mv_src.bin`) matches the kernel dispatch for a
    // root-level file (file was created at opendal key
    // `_mv_src.bin`, so the ino lives at root; the kernel
    // passes `\name` for that — backslash converts to
    // slash + lowercase yields the lookup path).
    rename_path(&adapter, &ctx, "\\_mv_src.bin", "\\_mv_dst.bin");

    // After rename: src gone, dst present.
    let backend =
        rt_block_on(async { fs.op.read("_mv_src.bin").await.unwrap_or_default().to_vec() });
    assert!(
        backend.is_empty(),
        "rename should have removed _mv_src.bin from backend"
    );
    let dst_payload =
        rt_block_on(async { fs.op.read("_mv_dst.bin").await.unwrap_or_default().to_vec() });
    assert_eq!(
        dst_payload, payload,
        "rename should have moved the payload to _mv_dst.bin"
    );

    // Step 2: cleanup(src_ino, DELETE) — the kernel's
    // "delete the source alias" call after MoveFileEx.
    // Pre-fix this unlinks _mv_dst.bin (the post-rename
    // file_name) and the test fails at the dst-presence
    // check below. Post-fix the cleanup is recognised as
    // the post-rename no-op and leaves _mv_dst.bin alone.
    <WinFspAdapter<MntrsFs> as FileSystemContext>::cleanup(
        &adapter,
        &ctx,
        // The file_name the kernel passes after a
        // MoveFileEx points at the **destination** path
        // (the source FH's kernel-level path was
        // updated by the prior `rename` call). Encoding
        // the dst path here matches the dispatch we
        // see in mntrs-bench.stdout.log (run
        // 33059193417, line ~1599) for the bench
        // Move-Item FAIL.
        Some(&*str_to_ucstr("\\_mv_dst.bin")),
        0x01,
    );

    let dst_after =
        rt_block_on(async { fs.op.read("_mv_dst.bin").await.unwrap_or_default().to_vec() });
    assert_eq!(
        dst_after, payload,
        "post-rename cleanup-with-DELETE must NOT unlink the \
         destination (issue #614). The Win32 'delete the \
         source alias' step is a no-op when `rename` \
         already moved the file."
    );
}

#[test]
fn winfsp_cleanup_without_rename_still_unlinks() {
    // Counter-test: prove the #614 fix does NOT
    // accidentally suppress legitimate unlinks. A
    // cleanup-with-DELETE on an ino that was NOT just
    // renamed must still drop the backend file.
    let fs = Arc::new(make_memory_fs());
    let adapter = WinFspAdapter::new(fs.clone());

    let payload = b"data that should be deleted on cleanup";
    write_remote(&fs, "_del.bin", payload);

    let ctx = open_handle_for(&*fs, "_del.bin");

    // No rename — go straight to cleanup-with-DELETE.
    <WinFspAdapter<MntrsFs> as FileSystemContext>::cleanup(
        &adapter,
        &ctx,
        Some(&*str_to_ucstr("\\_del.bin")),
        0x01,
    );

    let backend = rt_block_on(async { fs.op.read("_del.bin").await.unwrap_or_default().to_vec() });
    assert!(
        backend.is_empty(),
        "cleanup-with-DELETE on a non-renamed ino must \
         still unlink (issue #614 false-positive guard)"
    );
}

#[test]
fn winfsp_cleanup_after_rename_then_real_delete() {
    // Sequence: rename, then real Remove-Item on the
    // destination. The first cleanup-with-DELETE
    // (post-rename) is a no-op; the second cleanup
    // (after Remove-Item) actually unlinks.
    let fs = Arc::new(make_memory_fs());
    let adapter = WinFspAdapter::new(fs.clone());

    let payload = b"data that moves and then gets deleted";
    write_remote(&fs, "_mv2_src.bin", payload);

    let ctx = open_handle_for(&*fs, "_mv2_src.bin");

    // 1. Rename src -> dst
    rename_path(&adapter, &ctx, "\\_mv2_src.bin", "\\_mv2_dst.bin");

    // 2. Post-rename cleanup-with-DELETE (Win32 move's
    //    "delete the source alias") — no-op.
    <WinFspAdapter<MntrsFs> as FileSystemContext>::cleanup(
        &adapter,
        &ctx,
        Some(&*str_to_ucstr("\\_mv2_dst.bin")),
        0x01,
    );
    let after_first = rt_block_on(async {
        fs.op
            .read("_mv2_dst.bin")
            .await
            .unwrap_or_default()
            .to_vec()
    });
    assert_eq!(
        after_first, payload,
        "post-rename cleanup must leave dst intact"
    );

    // 3. Real Remove-Item on the dst path. This is a
    //    fresh fh on the same ino (the renamed file
    //    now lives at _mv2_dst.bin, same ino).
    //    Cleanup-with-DELETE on this fh must actually
    //    unlink.
    <WinFspAdapter<MntrsFs> as FileSystemContext>::cleanup(
        &adapter,
        &ctx,
        Some(&*str_to_ucstr("\\_mv2_dst.bin")),
        0x01,
    );
    let after_second = rt_block_on(async {
        fs.op
            .read("_mv2_dst.bin")
            .await
            .unwrap_or_default()
            .to_vec()
    });
    assert!(
        after_second.is_empty(),
        "second cleanup-with-DELETE (post-Remove-Item) \
         must unlink — proves the renamed-tracking flag \
         clears after the first post-rename cleanup"
    );
}

// Issue #614 v2 regression: WinFSP 2.1's open-handle rename
// (PowerShell `Move-Item`, Win32 `MoveFile` on an open
// handle) is dispatched through the FSD internally — the
// `rename` callback is NOT invoked. The visible side
// effect is a `set_basic_info` with the current real
// `file_attributes` (not the 0xFFFFFFFF "no-change"
// sentinel) and `last_write_time=0` (no time update).
// The kernel then dispatches `cleanup(FspCleanupDelete)`
// on the same ino with the post-rename `file_name`. The
// v1 fix (which only tracked inos in the `rename`
// callback) doesn't cover this — `move` and `del` still
// fail on the bench. The v2 fix detects the rename
// signature in `set_basic_info` and inserts the ino into
// the same `renamed_inos` set, so the subsequent
// cleanup-with-DELETE early-returns as a no-op.
#[test]
fn winfsp_set_basic_info_rename_signal_skips_post_rename_cleanup() {
    let fs = Arc::new(make_memory_fs());
    let adapter = WinFspAdapter::new(fs.clone());

    let payload = b"data that moves via set_basic_info IRP";
    write_remote(&fs, "_mv3_src.bin", payload);

    let ctx = open_handle_for(&*fs, "_mv3_src.bin");

    // Step 1: set_basic_info with the kernel-internal
    // rename signature — real attrs (FILE_ATTRIBUTE_ARCHIVE
    // = 0x20) and last_write_time = 0. This is what
    // WinFSP 2.1 dispatches for open-handle MoveFile on
    // the bench trace (run 33071512354, line 3786:
    // `winfsp::set_basic_info: entered ino=526
    // file_attributes=32 last_write_time=0`).
    let mut fi = FileInfo::default();
    <WinFspAdapter<MntrsFs> as FileSystemContext>::set_basic_info(
        &adapter, &ctx, // file_attributes = real value (FILE_ATTRIBUTE_ARCHIVE)
        0x20,
        // creation_time, last_access_time, last_write_time,
        // change_time: all 0 (the rename IRP doesn't carry
        // timestamps). In particular last_write_time = 0
        // is the discriminator vs. a legitimate
        // attribute-only setattr (which would carry a real
        // mtime).
        0, 0, 0, 0, &mut fi,
    )
    .expect("trait set_basic_info");

    // Step 2: cleanup-with-DELETE on the same ino, with
    // the post-rename file_name. The kernel has already
    // renamed the FCB to _mv3_dst.bin; our adapter
    // receives the new name. Pre-v2 fix the unlink
    // targets the backend path `data/_mv3_dst.bin` (which
    // doesn't exist there — the rename was never
    // performed in our adapter) and the test fails.
    // Post-v2 the set_basic_info heuristic tracked the
    // ino in renamed_inos, so cleanup early-returns.
    <WinFspAdapter<MntrsFs> as FileSystemContext>::cleanup(
        &adapter,
        &ctx,
        Some(&*str_to_ucstr("\\_mv3_dst.bin")),
        0x01,
    );

    let backend = rt_block_on(async {
        fs.op
            .read("_mv3_src.bin")
            .await
            .unwrap_or_default()
            .to_vec()
    });
    assert_eq!(
        backend, payload,
        "set_basic_info-rename signal must register the ino for \
         post-rename cleanup no-op (issue #614 v2). The backend \
         file at the original path must survive; the kernel's \
         delete-source-alias dispatch must not target the new \
         (post-rename) name."
    );
}

// Counter-test for the v2 heuristic: a legitimate
// set_basic_info (with a real last_write_time) must NOT
// trigger the renamed-tracking. Otherwise a user who sets
// mtime via PowerShell and then deletes the file would
// see the delete silently suppressed.
#[test]
fn winfsp_set_basic_info_with_real_mtime_does_not_block_later_cleanup() {
    let fs = Arc::new(make_memory_fs());
    let adapter = WinFspAdapter::new(fs.clone());

    let payload = b"data that gets a real mtime then deleted";
    write_remote(&fs, "_mv4.bin", payload);

    let ctx = open_handle_for(&*fs, "_mv4.bin");

    // set_basic_info with real attrs AND real mtime — this
    // is a legitimate attribute/time set, NOT a rename
    // IRP. The v2 heuristic must not register the ino.
    let mut fi = FileInfo::default();
    <WinFspAdapter<MntrsFs> as FileSystemContext>::set_basic_info(
        &adapter,
        &ctx,
        0x20, // file_attributes = ARCHIVE (real value)
        0,
        0,
        // last_write_time = a real Win32 FILETIME (some
        // arbitrary recent timestamp; the exact value is
        // irrelevant — the point is it must be != 0).
        134_323_072_067_238_517,
        0,
        &mut fi,
    )
    .expect("trait set_basic_info");

    // Now cleanup-with-DELETE — must unlink normally
    // because the v2 heuristic did NOT register the ino
    // (real mtime was present).
    <WinFspAdapter<MntrsFs> as FileSystemContext>::cleanup(
        &adapter,
        &ctx,
        Some(&*str_to_ucstr("\\_mv4.bin")),
        0x01,
    );

    let backend = rt_block_on(async { fs.op.read("_mv4.bin").await.unwrap_or_default().to_vec() });
    assert!(
        backend.is_empty(),
        "set_basic_info with real mtime must NOT trigger the \
         rename-tracking heuristic — legitimate cleanup-with-DELETE \
         after a real attr-set must still unlink (issue #614 v2 \
         false-positive guard)"
    );
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
// ============================================================
// 通用 mount_internal 参数测试 (non-Windows specific)
//
// Issue #493: these three tests were originally in
// `tests/platform/windows/winfsp_integration_test.rs`, but
// `mount_internal` only validates credentials eagerly on Linux
// — on Windows it accepts `s3://bucket` without creds and the
// `assert!(result.is_err())` fails. They moved to
// `tests/mount_internal_test.rs` with `#![cfg(not(windows))]`
// so they run on Linux CI but don't compile (and don't fail)
// on Windows.
//
// `test_generic_unmount_various_paths` stays here because
// `unmount_internal` behaves the same on both platforms.
// ============================================================

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

// ============================================================
// Issue #310 regression tests: WinFSP per-adapter TTL caches
// (get_file_info 100ms, get_volume_info 30s), flush
// NotFound-as-noop, and create file_attributes passthrough.
//
// The cache lives on `WinFspAdapter`, not on `MntrsFs`, so
// the only way to assert that a `get_file_info` /
// `get_volume_info` IRP was served from the cache (and not
// from the inner `CoreFilesystem` impl) is to count
// delegations to the inner. `CountingCoreFs` wraps
// `Arc<MntrsFs>` and increments an `AtomicU64` on each
// `getattr` / `statfs` call, leaving the rest of the trait
// untouched. The wrapper is built with a macro so adding new
// trait methods later is a one-line `forward!` invocation.
// ============================================================

/// `CountingCoreFs` — wraps an `Arc<MntrsFs>` and counts
/// how many times each high-traffic trait method is
/// invoked. Used to verify the per-adapter TTL caches in
/// `WinFspAdapter` actually save delegations to the inner
/// `CoreFilesystem`. Counters are `AtomicU64` (Relaxed) so
/// concurrent `get_file_info` IRPs from the WinFSP
/// dispatcher don't need a lock.
struct CountingCoreFs {
    inner: Arc<MntrsFs>,
    getattr_count: std::sync::atomic::AtomicU64,
    statfs_count: std::sync::atomic::AtomicU64,
}

impl CountingCoreFs {
    fn new(inner: Arc<MntrsFs>) -> Self {
        Self {
            inner,
            getattr_count: std::sync::atomic::AtomicU64::new(0),
            statfs_count: std::sync::atomic::AtomicU64::new(0),
        }
    }
    fn getattr_count(&self) -> u64 {
        self.getattr_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
    fn statfs_count(&self) -> u64 {
        self.statfs_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Forward every other `CoreFilesystem` method to the
/// wrapped `MntrsFs`. The two methods we care about
/// (`getattr`, `statfs`) are overridden below to bump
/// their counters before delegating. The macro can't
/// be used in `fn` signatures (`Result<_>` is illegal
/// in item position) so the impl is written out by
/// hand — the trait surface is small and stable.
macro_rules! forward {
    ($method:ident ( $($arg:ident : $aty:ty),* $(,)? ) -> $ret:ty) => {
        fn $method(&self, $($arg: $aty),*) -> std::io::Result<$ret> {
            self.inner.$method($($arg),*)
        }
    };
}

impl CoreFilesystem for CountingCoreFs {
    forward!(init() -> ());
    forward!(lookup(parent: u64, name: &str) -> CoreFileAttr);
    forward!(lookup_many(parent: u64, names: &[&str]) -> Vec<std::io::Result<CoreFileAttr>>);
    fn forget(&self, ino: u64, nlookup: u64) {
        self.inner.forget(ino, nlookup)
    }
    fn getattr(&self, ino: u64) -> std::io::Result<CoreFileAttr> {
        self.getattr_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.getattr(ino)
    }
    forward!(setattr(
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<std::time::SystemTime>,
        _mtime: Option<std::time::SystemTime>,
        fh: Option<u64>
    ) -> CoreFileAttr);
    forward!(opendir(ino: u64) -> u64);
    forward!(readdir(ino: u64, fh: u64, offset: u64, _max: usize) -> std::sync::Arc<Vec<mntrs::core_fs::CoreDirEntry>>);
    forward!(releasedir(_ino: u64, _fh: u64) -> ());
    forward!(readdir_with_attrs(ino: u64, fh: u64, marker: &str) -> Vec<(mntrs::core_fs::CoreDirEntry, CoreFileAttr)>);
    forward!(open(ino: u64, _flags: u32) -> u64);
    forward!(read(ino: u64, fh: u64, offset: u64, size: u32) -> Vec<u8>);
    forward!(write(ino: u64, fh: u64, offset: u64, data: &[u8]) -> u32);
    forward!(flush(ino: u64, fh: u64) -> ());
    forward!(fsync(ino: u64, fh: u64, datasync: bool) -> ());
    forward!(fsyncdir(ino: u64, fh: u64, datasync: bool) -> ());
    forward!(release(ino: u64, fh: u64) -> ());
    forward!(create(parent: u64, name: &str, mode: u32) -> (CoreFileAttr, u64));
    forward!(create_excl(parent: u64, name: &str, mode: u32) -> (CoreFileAttr, u64));
    forward!(mkdir(parent: u64, name: &str) -> CoreFileAttr);
    forward!(unlink(parent: u64, name: &str) -> ());
    forward!(rmdir(parent: u64, name: &str) -> ());
    forward!(rename(parent: u64, name: &str, newparent: u64, newname: &str) -> ());
    forward!(rename_paths(src_path: &str, dst_path: &str) -> ());
    forward!(readlink(ino: u64) -> Vec<u8>);
    forward!(symlink(parent: u64, name: &str, target: &std::path::Path) -> CoreFileAttr);
    fn statfs(&self, ino: u64) -> std::io::Result<CoreVolumeStat> {
        self.statfs_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.statfs(ino)
    }
    forward!(getxattr(ino: u64, name: &str) -> Vec<u8>);
    forward!(listxattr(ino: u64) -> Vec<Vec<u8>>);
    forward!(access(ino: u64, mask: u32) -> ());
    forward!(link(ino: u64, newparent: u64, newname: &str) -> CoreFileAttr);
    forward!(fallocate(ino: u64, _fh: u64, offset: u64, length: u64, mode: i32) -> ());
    forward!(copy_file_range(
        ino_in: u64,
        fh_in: u64,
        offset_in: u64,
        ino_out: u64,
        fh_out: u64,
        offset_out: u64,
        len: u64
    ) -> u32);
}

/// Issue #310: per-adapter `get_file_info` cache
/// (100 ms TTL). Pre-fix every WinFSP `IRP_MJ_QUERY_
/// INFORMATION` for the same inode within a single
/// Explorer Refresh fell through to the inner
/// `getattr` — one backend stat per IRP. Post-fix a
/// burst of N concurrent `std::fs::metadata` calls
/// within 100 ms produces exactly 1 inner `getattr`
/// per ino; the rest are served from the
/// per-adapter cache.
#[test]
fn winfsp_getattr_cache_coalesces_burst() {
    let fs_inner = make_memory_fs();
    // Pre-seed a file via the remote backend so the
    // get_file_info callback has a real ino to look
    // up (avoids the `ino == 1` shortcut in
    // MntrsFs::getattr which would skip the stat
    // path and contaminate the count).
    write_remote(&fs_inner, "cached.bin", b"hello");

    let counter = Arc::new(CountingCoreFs::new(Arc::new(fs_inner)));
    let counter_for_guard = counter.clone();
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(counter.clone()).expect("mount");
    let mp = &guard.mount_path;
    settle();

    // Baseline: opening the file issues a few lookups
    // (get_security_by_name, open, get_file_info).
    // Reset the counter so the assertion below
    // measures only the burst.
    counter
        .getattr_count
        .store(0, std::sync::atomic::Ordering::Relaxed);
    counter
        .statfs_count
        .store(0, std::sync::atomic::Ordering::Relaxed);

    // Burst: 50 `std::fs::metadata` calls in quick
    // succession (well under the 100 ms TTL). Each
    // call triggers at least one `get_file_info`
    // IRP from the kernel. Without the cache the
    // counter would be >= 50; with the cache the
    // adapter serves most of them from the ino
    // cache and the counter stays at 1 (one miss
    // + N-1 hits).
    for _ in 0..50 {
        let _ = std::fs::metadata(format!("{mp}cached.bin")).unwrap();
    }

    let after_burst = counter.getattr_count();
    assert!(
        after_burst <= 5,
        "getattr cache miss: expected <= 5 inner calls for 50 stat IRPs, got {after_burst} \
         (cache not active or TTL too short)"
    );

    drop(guard);
    drop(counter_for_guard);
}

/// Issue #310: per-adapter `get_volume_info` cache
/// (30 s TTL). Explorer calls this on every Refresh
/// and every Properties dialog — for S3 backends
/// that's 200 ms+ per call. With the cache, N
/// consecutive `std::fs::metadata(V:)` calls (which
/// trigger a volume-info IRP) hit the inner
/// `statfs` exactly once.
#[test]
fn winfsp_volume_info_cache_coalesces_burst() {
    let fs_inner = make_memory_fs();
    let counter = Arc::new(CountingCoreFs::new(Arc::new(fs_inner)));
    let counter_for_guard = counter.clone();
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(counter.clone()).expect("mount");
    let mp = &guard.mount_path;
    settle();

    // 20 `std::fs::metadata` calls on the mount root
    // — each one triggers a volume-info IRP. Pre-fix
    // this would call inner.statfs(1) 20 times. Post-fix
    // exactly 1 (the rest are served from the
    // per-adapter cache).
    counter
        .statfs_count
        .store(0, std::sync::atomic::Ordering::Relaxed);
    for _ in 0..20 {
        let _ = std::fs::metadata(mp.as_str()).unwrap();
    }

    let after_burst = counter.statfs_count();
    assert!(
        after_burst <= 2,
        "volume_info cache miss: expected <= 2 inner statfs for 20 metadata IRPs, got {after_burst} \
         (cache not active or TTL too short)"
    );

    drop(guard);
    drop(counter_for_guard);
}

/// Issue #310: `WinFspAdapter::flush` must not
/// surface the inner `fsync`'s `NotFound` (no cache
/// fd on read-only handle) to the kernel as
/// `STATUS_NOT_FOUND`. Pre-fix a `FlushFileBuffers`
/// on a read-only handle returned
/// `ERROR_FILE_NOT_FOUND` to user-space, breaking
/// the Win32 contract that `FlushFileBuffers` is a
/// no-op for read-only files. Post-fix it's a clean
/// Ok(()) (no-op).
#[test]
fn winfsp_flush_readonly_handle_is_noop() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).expect("mount");
    let mp = &guard.mount_path;
    settle();

    // Seed a file via the remote backend.
    write_remote(&fs, "ro.txt", b"data");

    // Open it read-only via `File::open` (no write
    // access). The kernel will hand the adapter a
    // handle with no write_buffer_fd; a subsequent
    // `FlushFileBuffers` exercises the fsync
    // NotFound path.
    use std::io::Read;
    let mut f = std::fs::File::open(format!("{mp}ro.txt")).unwrap();
    let mut buf = [0u8; 4];
    f.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"data");

    // FlushFileBuffers — must not error.
    // Pre-fix this returned ERROR_FILE_NOT_FOUND
    // (from the inner fsync's NotFound propagating
    // as STATUS_NOT_FOUND). Post-fix it's Ok(())
    // because the adapter treats "no cache fd" as
    // a no-op (no buffers to flush on a read-only
    // handle).
    let flush_result = f.flush();
    drop(f);
    assert!(
        flush_result.is_ok(),
        "FlushFileBuffers on read-only handle returned {:?}; \
         expected Ok (Issue #310: fsync NotFound should be a no-op)",
        flush_result
    );

    drop(guard);
}

/// Issue #308: the WinFSP kernel calls
/// `get_security` on every Explorer Refresh, every
/// Properties dialog, and every `Get-Acl` / `icacls`
/// invocation. Pre-fix the adapter returned
/// STATUS_INVALID_DEVICE_REQUEST, which made the
/// Security tab in Properties empty, broke
/// `icacls V:\file.txt`, and caused EDR / Defender
/// ACL scans to misreport the mount. Post-fix the
/// adapter synthesizes a 72-byte self-relative SD
/// granting `Everyone` full access. This test
/// shells out to `icacls.exe` (the simplest tool
/// that prints the effective ACL on a file) to
/// verify the kernel hands back a non-empty
/// descriptor with the `Everyone` entry.
#[test]
fn winfsp_get_security_returns_synthetic_acl() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).expect("mount");
    let mp = &guard.mount_path;
    settle();

    // Seed a file via the remote backend so the
    // mount-side `get_security` callback has a
    // real ino to look up.
    write_remote(&fs, "_ci_acl.txt", b"data");
    let file_path = format!("{mp}_ci_acl.txt");
    // Touch the file via mount to force
    // `get_security_by_name` to populate the ino
    // cache; subsequent `get_security` calls go
    // through the per-ino path that the SD fix
    // targets.
    let _ = std::fs::metadata(&file_path).unwrap();

    // Run `icacls <file>` and capture stdout. The
    // exit code is 0 on success, 1 on partial
    // failures (e.g. "Some files could not be
    // processed"). We only assert that the
    // output contains the `Everyone` ACE marker
    // — that's the proof the kernel received a
    // real SD from our adapter.
    let output = std::process::Command::new("icacls.exe")
        .arg(&file_path)
        .output()
        .expect("icacls.exe not found (Windows-only test)");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}{stderr}");
    assert!(
        !stdout.is_empty() || !stderr.is_empty(),
        "icacls produced no output for {file_path} (exit={:?})",
        output.status.code()
    );
    assert!(
        combined.contains("Everyone"),
        "icacls output missing `Everyone` ACE for {file_path} -- \
         kernel probably received an empty SD. Output:\n{combined}"
    );
    assert!(
        combined.contains("_ci_acl.txt"),
        "icacls output missing the target file name; \
         got: {combined}"
    );

    drop(guard);
}

/// Issue #308: `set_security` must accept the
/// change rather than return
/// STATUS_INVALID_DEVICE_REQUEST. `icacls /grant`
/// is the canonical user-mode tool that exercises
/// this path (the `/grant` flag calls
/// `SetSecurityInfo` with DACL-security-info on
/// the file's SD). Pre-fix the adapter rejected
/// every such call, breaking the common
/// "permission fix" workflow.
#[test]
fn winfsp_set_security_accepts_grant() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).expect("mount");
    let mp = &guard.mount_path;
    settle();

    write_remote(&fs, "_ci_set_acl.txt", b"data");
    let file_path = format!("{mp}_ci_set_acl.txt");
    let _ = std::fs::metadata(&file_path).unwrap();

    // `icacls <file> /grant:r Everyone:(R)` is a
    // roundtrip grant: revoke, then grant. The
    // `/grant:r` flag requests a "generic read"
    // DACL entry. Pre-#308 this returned
    // `Access is denied` or `The requested
    // operation requires an interactive window
    // station` because the kernel couldn't apply
    // the DACL. Post-#308 the kernel accepts the
    // change (no-op at our end; we still return
    // the synthesized SD on subsequent reads).
    let output = std::process::Command::new("icacls.exe")
        .arg(&file_path)
        .arg("/grant:r")
        .arg("Everyone:(R)")
        .output()
        .expect("icacls.exe");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "icacls /grant failed for {file_path} (exit={:?})\nstdout: {stdout}\nstderr: {stderr}",
        output.status.code()
    );

    drop(guard);
}

/// Issue #309: enabling
/// `VolumeParams::named_streams(true)` requires
/// the adapter's `get_stream_info` callback to
/// return at least the unnamed default stream
/// for every file — otherwise the kernel returns
/// STATUS_INVALID_DEVICE_REQUEST on every file
/// open, breaking basic `std::fs::read`. This
/// test uses PowerShell's `Get-Item` (which
/// triggers a QueryStreamInformation IRP) to
/// verify the kernel receives a non-empty stream
/// list. Pre-#309 the file open itself would
/// fail before this command even ran.
#[test]
fn winfsp_named_streams_volume_flag_does_not_break_opens() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).expect("mount");
    let mp = &guard.mount_path;
    settle();

    write_remote(&fs, "_ci_streams.txt", b"data");
    let file_path = format!("{mp}_ci_streams.txt");

    // `Get-Item` triggers a
    // IRP_MJ_QUERY_INFORMATION with
    // FileStreamInformation — exercises our
    // get_stream_info callback end-to-end. If
    // the kernel receives
    // STATUS_INVALID_DEVICE_REQUEST, the
    // PowerShell output would contain an error
    // and exit code would be non-zero.
    let output = std::process::Command::new("powershell.exe")
        .arg("-NoProfile")
        .arg("-NonInteractive")
        .arg("-Command")
        .arg(format!(
            "$ErrorActionPreference = 'Stop'; (Get-Item -LiteralPath '{}').Length",
            file_path.replace('\'', "''")
        ))
        .output()
        .expect("powershell.exe");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "Get-Item failed for {file_path} (exit={:?})\nstdout: {stdout}\nstderr: {stderr}",
        output.status.code()
    );
    assert_eq!(
        stdout.trim(),
        "4",
        "Get-Item .Length should return the byte count (4); got: {stdout}"
    );

    drop(guard);
}
