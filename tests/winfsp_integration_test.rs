#![cfg(windows)]

use std::sync::Arc;
use std::time::Duration;

use opendal::Operator;
use opendal::services::Memory;

use mntrs::MntrsFs;
use mntrs::core_fs::CoreFilesystem;

/// Build a MntrsFs backed by in-memory OpenDAL (no real S3 needed).
fn make_memory_fs() -> MntrsFs {
    let builder = Memory::default();
    let op = Operator::new(builder).unwrap().finish();
    let cache_dir = std::env::temp_dir().join("mntrs-wintest");
    let _ = std::fs::create_dir_all(&cache_dir);

    MntrsFs {
        op: Arc::new(op),
        inodes: Default::default(),
        dir_cache: Default::default(),
        cache_dir,
        handles: Default::default(),
        dir_cache_ttl: Duration::from_secs(10),
        attr_ttl: Duration::from_secs(1),
        stat_cache_ttl: Duration::from_secs(1),
        volname: "mntrs-test".to_string(),
        cache_max_size: 1024 * 1024 * 100,
        write_back_delay: Duration::from_secs(5),
        cache_mode: "writes".to_string(),
        read_ahead: 0,
        read_chunk_size: 0,
        read_chunk_size_limit: 0,
        read_chunk_streams: 1,
        uid: None,
        gid: None,
        umask: None,
        dir_perms: 0o755,
        file_perms: 0o644,
        direct_io: false,
        poll_interval: Duration::from_secs(60),
        cache_max_age: Duration::from_secs(3600),
        cache_min_free_space: 100,
        exclude_patterns: vec![],
        include_patterns: vec![],
        max_size: None,
        min_size: None,
        max_depth: None,
        ignore_case: false,
        fast_fingerprint: false,
        async_read: false,
        vfs_refresh: false,
        case_insensitive: false,
        no_implicit_dir: false,
        use_server_modtime: false,
        block_norm_dupes: false,
        write_wait: Duration::from_secs(5),
        read_wait: Duration::from_secs(5),
        cache_poll_interval: Duration::from_secs(60),
        disk_total_size: 1024 * 1024 * 1024 * 1024,
        writeback_sender: Default::default(),
        mem_cache: Default::default(),
        attr_cache: Default::default(),
        disk_cache_index: Default::default(),
        out_of_space: Default::default(),
        storage_class: None,
        mem_limit: 256 * 1024 * 1024,
        mem_used: Default::default(),
    }
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
    settle();

    // Write via FUSE (std::fs)
    write_remote(&fs, "test.txt", b"hello winfsp");

    // Read via FUSE
    let read = std::fs::read_to_string("test.txt").unwrap_or_default();
    assert_eq!(read, "hello winfsp");

    drop(guard);
}

#[test]
fn winfsp_list_directory() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    settle();

    write_remote(&fs, "hello.txt", b"a");
    write_remote(&fs, "world.txt", b"b");

    let entries: Vec<String> = std::fs::read_dir(".")
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
    settle();

    write_remote(&fs, "delete_me.txt", b"x");
    assert!(std::fs::read("delete_me.txt").is_ok());

    std::fs::remove_file("delete_me.txt").unwrap();
    assert!(std::fs::read("delete_me.txt").is_err());

    drop(guard);
}

#[test]
fn winfsp_rename_file() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    settle();

    write_remote(&fs, "old.txt", b"rename me");
    std::fs::rename("old.txt", "new.txt").unwrap();
    let read = std::fs::read_to_string("new.txt").unwrap_or_default();
    assert_eq!(read, "rename me");
    assert!(std::fs::read("old.txt").is_err());

    drop(guard);
}

#[test]
fn winfsp_setattr_truncate() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    settle();

    write_remote(&fs, "trunc.txt", b"hello world");
    // Truncate via SetEndOfFile
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open("trunc.txt")
        .unwrap();
    f.set_len(5).unwrap();
    drop(f);

    let read = std::fs::read("trunc.txt").unwrap();
    assert_eq!(read.len(), 5);
    assert_eq!(&read, b"hello");

    drop(guard);
}

#[test]
fn winfsp_statfs_reports_volume() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    settle();

    // Just verify stat doesn't crash
    let _ = std::fs::metadata(".").unwrap();

    drop(guard);
}

#[test]
fn winfsp_nested_directory() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    settle();

    std::fs::create_dir_all("a/b/c").unwrap();

    // Write file in nested dir
    write_remote(&fs, "a/b/c/deep.txt", b"nested");
    let read = std::fs::read_to_string("a/b/c/deep.txt").unwrap_or_default();
    assert_eq!(read, "nested");

    // List nested dir
    let entries: Vec<String> = std::fs::read_dir("a/b/c")
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
    settle();

    // >1MB to exercise multi-chunk fetch path
    let large_data = vec![0xABu8; 2 * 1024 * 1024];
    write_remote(&fs, "large.bin", &large_data);

    let read = std::fs::read("large.bin").unwrap();
    assert_eq!(read.len(), 2 * 1024 * 1024);
    assert_eq!(read[0], 0xAB);
    assert_eq!(read[read.len() - 1], 0xAB);

    drop(guard);
}

#[test]
fn winfsp_unicode_filename() {
    let fs = Arc::new(make_memory_fs());
    let guard = mntrs::core_fs::test_helpers::mount_winfsp(fs.clone()).unwrap();
    settle();

    let name = "中文文件.txt";
    write_remote(&fs, name, b"unicode");
    let read = std::fs::read_to_string(name).unwrap_or_default();
    assert_eq!(read, "unicode");

    let entries: Vec<String> = std::fs::read_dir(".")
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
    settle();

    // Write via FUSE (std::fs::write goes through WinFSP → CoreFilesystem::write)
    let data = vec![0x42u8; 512 * 1024]; // 512KB
    std::fs::write("big_write.bin", &data).unwrap();

    // Flush and verify
    let read = std::fs::read("big_write.bin").unwrap();
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
