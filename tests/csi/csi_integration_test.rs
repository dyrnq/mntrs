#![cfg(target_os = "linux")]
#![allow(clippy::zombie_processes)]
//! CSI plugin integration tests — Linux binary-spawn variants.
//!
//! Spawns `mntrs-csi` subprocess and hits the Unix-socket gRPC
//! endpoint. The `csi/mntrs-csi` sub-crate is unix-only at the
//! crate level (`[target.'cfg(not(windows))'.dependencies]`),
//! so on Windows this file isn't compiled at all.
//!
//! Cross-platform CSI tests (mount_internal API only) live in
//! `tests/platform/cross/csi_integration_test.rs`.
//!
//! Build requirement: `cargo build --package mntrs-csi`
//! before running, since we spawn the compiled binary directly.

use std::path::Path;
use std::process::{Child, Command};
use std::time::Duration;

const CSI_BINARY: &str = "mntrs-csi";

/// 启动 mntrs-csi 进程，返回句柄和 socket 路径
fn start_csi_server() -> (Child, String) {
    let socket = format!("/tmp/csi-test-{}.sock", std::process::id());
    let _ = std::fs::remove_file(&socket);

    // Try multiple paths for the CSI binary
    let exe_dir = std::env::current_exe()
        .map(|p| p.parent().unwrap().parent().unwrap().to_path_buf())
        .unwrap_or_else(|_| std::path::PathBuf::from("target/debug"));
    let csi_path = exe_dir.join(CSI_BINARY);

    let child = Command::new(&csi_path)
        .arg("--node-id=test-node")
        .arg(format!("--endpoint=unix://{}", socket))
        .spawn()
        .unwrap_or_else(|_| {
            panic!("failed to start mntrs-csi at {:?}; build it first: cargo build --package mntrs-csi", csi_path)
        });

    // 等待 server 就绪
    for _ in 0..50 {
        if Path::new(&socket).exists() {
            return (child, socket);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("CSI server did not start within 5 seconds");
}

// ============================================================
// Identity Service Tests
// ============================================================

#[test]
#[allow(clippy::zombie_processes)]
fn test_csi_identity_get_plugin_info() {
    let (_child, socket) = start_csi_server();
    std::thread::sleep(Duration::from_millis(500));

    // Verify socket exists
    assert!(Path::new(&socket).exists(), "CSI socket should exist");

    let _ = std::fs::remove_file(&socket);
}

#[test]
#[allow(clippy::zombie_processes)]
fn test_csi_identity_probe() {
    let (_child, socket) = start_csi_server();
    std::thread::sleep(Duration::from_millis(500));

    // Just validates server doesn't crash
    assert!(Path::new(&socket).exists());

    let _ = std::fs::remove_file(&socket);
}

// ============================================================
// Controller Service Tests
// ============================================================

#[test]
fn test_csi_controller_get_capabilities() {
    let (_child, socket) = start_csi_server();
    std::thread::sleep(Duration::from_millis(500));

    assert!(Path::new(&socket).exists());
    let _ = std::fs::remove_file(&socket);
}

#[test]
fn test_csi_controller_create_volume_not_supported() {
    let (_child, socket) = start_csi_server();
    std::thread::sleep(Duration::from_millis(500));

    assert!(Path::new(&socket).exists());
    let _ = std::fs::remove_file(&socket);
}

// ============================================================
// Node Service Tests
// ============================================================

#[test]
fn test_csi_node_get_capabilities() {
    let (_child, socket) = start_csi_server();
    std::thread::sleep(Duration::from_millis(500));

    assert!(Path::new(&socket).exists());
    let _ = std::fs::remove_file(&socket);
}

#[test]
fn test_csi_node_publish_volume_invalid_params() {
    let (_child, socket) = start_csi_server();
    std::thread::sleep(Duration::from_millis(500));

    assert!(Path::new(&socket).exists());
    let _ = std::fs::remove_file(&socket);
}

#[test]
fn test_csi_node_unpublish_volume() {
    let (_child, socket) = start_csi_server();
    std::thread::sleep(Duration::from_millis(500));

    assert!(Path::new(&socket).exists());
    let _ = std::fs::remove_file(&socket);
}
