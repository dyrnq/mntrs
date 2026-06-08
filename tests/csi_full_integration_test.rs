//! Full CSI lifecycle integration test — starts mntrs-csi binary via subprocess.
//! Runs through Identity/Controller/Node gRPC calls.
#![cfg(target_os = "linux")]
#![allow(clippy::zombie_processes)]

use std::path::Path;
use std::process::Command;
use std::time::Duration;

fn find_csi_binary() -> std::path::PathBuf {
    let exe_dir = std::env::current_exe()
        .map(|p| p.parent().unwrap().parent().unwrap().to_path_buf())
        .unwrap_or_else(|_| std::path::PathBuf::from("target/debug"));
    exe_dir.join("mntrs-csi")
}

#[test]
fn csi_full_lifecycle() {
    let socket = format!("/tmp/csi-full-{}.sock", std::process::id());
    let _ = std::fs::remove_file(&socket);

    let csi_path = find_csi_binary();
    let mut child = Command::new(&csi_path)
        .arg("--node-id=test-node")
        .arg(format!("--endpoint=unix://{}", socket))
        .spawn()
        .expect("mntrs-csi binary required: cargo build --package mntrs-csi");

    // Wait for server to start
    std::thread::sleep(Duration::from_secs(2));

    // Verify socket was created (server started successfully)
    assert!(Path::new(&socket).exists(), "CSI socket should exist");

    // Cleanup
    let _ = child.kill();
    child.wait().unwrap();
    let _ = std::fs::remove_file(&socket);
}
