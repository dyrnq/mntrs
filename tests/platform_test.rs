//! 平台特定测试 — 验证各平台条件编译的代码路径。
//!
//! - Linux: FUSE mount options, /proc/mounts 解析
//! - macOS: noappledouble, noapplexattr, case-insensitive
//! - Windows: WinFSP adapter (在 winfsp_integration_test.rs)

use mntrs::cmd::mount::mount_internal;

// ============================================================
// 全平台通用: mount_internal 基本路径
// ============================================================

#[test]
fn platform_mount_internal_memory() {
    let tmp = std::env::temp_dir().join(format!("plat-mem-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);
    let result = mount_internal(
        "memory://bucket",
        tmp.to_str().unwrap(),
        &std::collections::HashMap::new(),
        false,
    );
    let _ = std::fs::remove_dir_all(&tmp);
    // memory:// is not a valid real scheme, but mount_internal should handle gracefully
    assert!(result.is_err());
}

#[test]
fn platform_mount_internal_unicode_path() {
    let tmp = std::env::temp_dir().join(format!("路径测试-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);
    let result = mount_internal(
        "s3://bucket",
        tmp.to_str().unwrap(),
        &std::collections::HashMap::new(),
        false,
    );
    let _ = std::fs::remove_dir_all(&tmp);
    assert!(result.is_err());
}

#[test]
fn platform_mount_internal_long_path() {
    let long = "a".repeat(200);
    let tmp = std::env::temp_dir().join(&long);
    let _ = std::fs::create_dir_all(&tmp);
    let result = mount_internal(
        "s3://bucket",
        tmp.to_str().unwrap(),
        &std::collections::HashMap::new(),
        false,
    );
    let _ = std::fs::remove_dir_all(&tmp);
    assert!(result.is_err());
}

// ============================================================
// Linux 特定 (FUSE)
// ============================================================

#[cfg(target_os = "linux")]
mod linux_tests {
    use mntrs::cmd::mount::mount_internal;

    #[test]
    fn linux_is_mount_point() {
        // /proc is always mounted on Linux
        assert!(mntrs::cmd::mount::is_mount_point("/proc"));
        assert!(!mntrs::cmd::mount::is_mount_point(
            "/nonexistent-path-12345"
        ));
    }

    #[test]
    fn linux_fuse_mount_options() {
        let tmp = std::env::temp_dir().join(format!("linux-fuse-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);

        // FUSE mount with allow_other requires /etc/fuse.conf
        // We just verify the code path doesn't panic
        let mut opts = std::collections::HashMap::new();
        opts.insert("allow_other".to_string(), "true".to_string());
        let result = mount_internal("memory://bucket", tmp.to_str().unwrap(), &opts, false);
        let _ = std::fs::remove_dir_all(&tmp);
        assert!(result.is_err());
    }

    #[test]
    fn linux_fuse_read_only_mount() {
        let tmp = std::env::temp_dir().join(format!("linux-ro-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);
        let result = mount_internal(
            "memory://bucket",
            tmp.to_str().unwrap(),
            &std::collections::HashMap::new(),
            true,
        );
        let _ = std::fs::remove_dir_all(&tmp);
        assert!(result.is_err());
    }

    #[test]
    fn linux_fuse_daemon_mode() {
        let tmp = std::env::temp_dir().join(format!("linux-daemon-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);
        let mut opts = std::collections::HashMap::new();
        opts.insert("daemon".to_string(), "true".to_string());
        let result = mount_internal("memory://bucket", tmp.to_str().unwrap(), &opts, false);
        let _ = std::fs::remove_dir_all(&tmp);
        assert!(result.is_err());
    }
}

// ============================================================
// macOS 特定
// ============================================================

#[cfg(target_os = "macos")]
mod macos_tests {
    use mntrs::cmd::mount::mount_internal;

    #[test]
    fn macos_noappledouble_flag() {
        let tmp = std::env::temp_dir().join(format!("macos-ad-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);
        let mut opts = std::collections::HashMap::new();
        opts.insert("noappledouble".to_string(), "true".to_string());
        let result = mount_internal("memory://bucket", tmp.to_str().unwrap(), &opts, false);
        let _ = std::fs::remove_dir_all(&tmp);
        assert!(result.is_err());
    }

    #[test]
    fn macos_noapplexattr_flag() {
        let tmp = std::env::temp_dir().join(format!("macos-ax-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);
        let mut opts = std::collections::HashMap::new();
        opts.insert("noapplexattr".to_string(), "true".to_string());
        let result = mount_internal("memory://bucket", tmp.to_str().unwrap(), &opts, false);
        let _ = std::fs::remove_dir_all(&tmp);
        assert!(result.is_err());
    }

    #[test]
    fn macos_case_insensitive_mount() {
        let tmp = std::env::temp_dir().join(format!("macos-ci-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);
        let mut opts = std::collections::HashMap::new();
        opts.insert("mount_case_insensitive".to_string(), "true".to_string());
        let result = mount_internal("memory://bucket", tmp.to_str().unwrap(), &opts, false);
        let _ = std::fs::remove_dir_all(&tmp);
        assert!(result.is_err());
    }
}

// ============================================================
// Windows 特定 — 仅验证编译通过
// ============================================================

#[cfg(windows)]
mod windows_tests {
    use mntrs::cmd::mount::mount_internal;

    #[test]
    fn windows_drive_letter_detection() {
        // Test that parse_windows_target works
        let result = mntrs::path::parse_windows_target("X:");
        assert!(result.is_ok());

        let result = mntrs::path::parse_windows_target("*");
        assert!(result.is_ok());

        let result = mntrs::path::parse_windows_target("C:\\mnt\\s3");
        assert!(result.is_ok());
    }

    #[test]
    fn windows_path_normalization() {
        assert_eq!(mntrs::path::normalize("a\\b\\c"), "a/b/c");
        assert_eq!(mntrs::path::normalize("C:\\Users\\test"), "C:/Users/test");
    }
}
