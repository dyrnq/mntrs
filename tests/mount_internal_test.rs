//! Cross-platform tests for `mntrs::cmd::mount::mount_internal`.
//!
//! Issue #493: these three tests were originally in
//! `tests/platform/windows/winfsp_integration_test.rs` but
//! `mount_internal` only validates credentials eagerly on Linux —
//! on Windows it accepts `s3://bucket` without creds and the
//! `assert!(result.is_err())` fails. They were moved here with
//! `#![cfg(not(windows))]` so they run on Linux CI but don't
//! compile (and don't fail) on Windows.
//!
//! `test_generic_unmount_various_paths` stays in the WinFSP
//! integration file because `unmount_internal` behaves the same
//! on both platforms.
#![cfg(not(windows))]

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
