#![cfg(windows)]

use std::sync::Arc;
use std::time::Duration;

use opendal::Operator;
use opendal::services::Memory;

use mntrs::MntrsFs;

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
#[ignore = "TODO(#299): rename fails when source file was created via opendal backend (no ino in cache)"]
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

#[test]
#[ignore = "TODO(#300): set_file_size via mount truncates to zeros (loses original content)"]
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
#[ignore = "TODO(#301): nested directory created via backend not visible in mount readdir (stale dir_cache)"]
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
// 通用 mount_internal 参数测试 (non-Windows specific)
// ============================================================

/// mount_internal with various backend schemes
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
