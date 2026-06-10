use mntrs::cmd::mount::build_operator_sync;

// ============================================================
// 全平台通用: build_operator 基本路径（不触发 FUSE mount）
// ============================================================

#[test]
fn platform_build_operator_memory() {
    let op = build_operator_sync("memory://bucket", &std::collections::HashMap::new());
    assert!(op.is_ok());
}

#[test]
fn platform_build_operator_invalid_scheme() {
    let result = build_operator_sync("invalid-scheme://bucket", &std::collections::HashMap::new());
    assert!(result.is_err());
}

// ============================================================
// Linux 特定
// ============================================================

#[cfg(target_os = "linux")]
mod linux_tests {
    use mntrs::cmd::mount::build_operator_sync;

    #[test]
    fn linux_is_mount_point() {
        assert!(!mntrs::cmd::mount::is_mount_point("/proc/cmdline"));
    }

    #[test]
    fn linux_fuse_mount_options() {
        let mut opts = std::collections::HashMap::new();
        opts.insert("allow_other".to_string(), "true".to_string());
        let op = build_operator_sync("memory://bucket", &opts).unwrap();
        let tmp = std::env::temp_dir().join(format!("linux-fuse-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);
        let _fs = mntrs::new_test_fs(op, tmp);
    }

    #[test]
    fn linux_fuse_init_path() {
        let op = build_operator_sync("memory://bucket", &std::collections::HashMap::new()).unwrap();
        let tmp = std::env::temp_dir().join(format!("linux-init-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);
        let _fs = mntrs::new_test_fs(op, tmp);
    }
}

// FUSE mount tests skipped: mount_internal blocks forever
// (FUSE session loop), cannot be used in unit tests without
// spawning a separate process.
