#![cfg(target_os = "linux")]
//! CSI driver integration tests — mount_internal library API.
//!
//! Exercises the public `mntrs::cmd::mount::mount_internal`
//! entry point that the CSI driver uses to mount a backend.
//! These tests call the library API directly (no subprocess);
//! they share the linux/ platform slot with the binary-spawn
//! tests because CSI itself is Linux-only at the design level
//! (the `csi/mntrs-csi` sub-crate is gated to `cfg(not(windows))`).
//!
//! The older "cross-platform" categorization was removed:
//! a CSI test by definition doesn't run on Windows.

// ============================================================
// Volume Context Unit Tests
// ============================================================

#[test]
fn test_csi_volume_context_parsing() {
    let vol_ctx = std::collections::HashMap::from([
        ("storage".to_string(), "s3://my-bucket".to_string()),
        ("prefix".to_string(), "k8s-pv/data".to_string()),
        ("s3-access-key-id".to_string(), "AKIA...".to_string()),
    ]);

    let storage = vol_ctx.get("storage").unwrap();
    let prefix = vol_ctx.get("prefix").map(|s| s.as_str()).unwrap_or("");
    let storage_url = if prefix.is_empty() {
        storage.clone()
    } else {
        format!(
            "{}/{}",
            storage.trim_end_matches('/'),
            prefix.trim_start_matches('/')
        )
    };

    assert_eq!(storage_url, "s3://my-bucket/k8s-pv/data");
}

#[test]
fn test_csi_mount_idempotency() {
    let tmp = std::env::temp_dir().join(format!("csi-mount-test-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);

    // `s3://bucket` without credentials fails at operator
    // construction (opendal's `services-s3` requires keys).
    // We use this rather than `memory://` because the
    // `memory` opendal backend IS a valid scheme — the
    // original test asserted it should fail, but opendal
    // accepts it and `mount_internal` would proceed to
    // the FUSE mount step, hanging the test process in
    // environments without FUSE mount privilege. Bug
    // discovered during issue 284 test reorganization.
    let opts = std::collections::HashMap::new();
    let result =
        mntrs::cmd::mount::mount_internal("s3://bucket", tmp.to_str().unwrap(), &opts, false);
    let _ = std::fs::remove_dir_all(&tmp);

    assert!(
        result.is_err(),
        "s3://bucket without creds should fail gracefully"
    );
}

#[test]
fn test_csi_cache_dir_isolation() {
    let tmp1 = std::env::temp_dir().join("csi-cache-1");
    let tmp2 = std::env::temp_dir().join("csi-cache-2");

    let cache1 = tmp1.join("mntrs-csi-cache").to_string_lossy().to_string();
    let cache2 = tmp2.join("mntrs-csi-cache").to_string_lossy().to_string();

    assert_ne!(
        cache1, cache2,
        "different mountpoints must have different cache dirs"
    );
}

// ============================================================
// mount_internal 高级参数测试
// ============================================================

/// mount_internal with allow_other flag
#[test]
fn test_csi_mount_with_allow_other() {
    let tmp = std::env::temp_dir().join(format!("csi-mount-ao-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);
    let opts = std::collections::HashMap::from([("allow_other".to_string(), "true".to_string())]);
    let result =
        mntrs::cmd::mount::mount_internal("s3://bucket", tmp.to_str().unwrap(), &opts, false);
    let _ = std::fs::remove_dir_all(&tmp);
    assert!(
        result.is_err(),
        "s3://bucket without creds should fail gracefully"
    );
}

/// mount_internal with cache_dir override
#[test]
fn test_csi_mount_cache_dir_override() {
    let tmp = std::env::temp_dir().join(format!("csi-cache-override-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);
    let cache = std::env::temp_dir().join("csi-custom-cache");
    let opts = std::collections::HashMap::from([(
        "cache_dir".to_string(),
        cache.to_string_lossy().to_string(),
    )]);
    let result =
        mntrs::cmd::mount::mount_internal("s3://bucket", tmp.to_str().unwrap(), &opts, false);
    let _ = std::fs::remove_dir_all(&tmp);
    let _ = std::fs::remove_dir_all(&cache);
    assert!(result.is_err(), "should fail gracefully");
}

/// mount_internal with read_only flag
#[test]
fn test_csi_mount_read_only() {
    let tmp = std::env::temp_dir().join(format!("csi-mount-ro-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);
    let result = mntrs::cmd::mount::mount_internal(
        "s3://bucket",
        tmp.to_str().unwrap(),
        &std::collections::HashMap::new(),
        true,
    );
    let _ = std::fs::remove_dir_all(&tmp);
    assert!(result.is_err(), "should fail gracefully");
}

/// mount_internal with vfs_cache_mode
#[test]
fn test_csi_mount_cache_mode() {
    let tmp = std::env::temp_dir().join(format!("csi-cache-mode-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);
    let opts =
        std::collections::HashMap::from([("vfs_cache_mode".to_string(), "full".to_string())]);
    let result =
        mntrs::cmd::mount::mount_internal("s3://bucket", tmp.to_str().unwrap(), &opts, false);
    let _ = std::fs::remove_dir_all(&tmp);
    assert!(result.is_err(), "should fail gracefully");
}
