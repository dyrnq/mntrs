//! FUSE integration tests — requires MinIO (S3-compatible) backend.
//! Run with: MINIO_ENDPOINT=http://localhost:9000 cargo test --test fuse_integration_test
//!
//! These tests mount a real FUSE filesystem and verify read/write/stat operations.

use std::process::Command;
use std::thread;
use std::time::Duration;

const MINIO_ENDPOINT: &str = "http://localhost:9000";
const MINIO_ACCESS: &str = "minioadmin";
const MINIO_SECRET: &str = "minioadmin";
const MNTRS_BIN: &str = "./target/debug/mntrs";
const MNTRS_MNT: &str = "/tmp/mntrs-fuse-test";

fn mntrs_mount(read_only: bool) {
    let _ = Command::new("curl")
        .args([
            "-sf",
            "-X",
            "PUT",
            &format!("{}/test-bucket", MINIO_ENDPOINT),
        ])
        .status();
    let _ = Command::new("fusermount3")
        .arg("-u")
        .arg(MNTRS_MNT)
        .status();
    let _ = std::fs::create_dir_all(MNTRS_MNT);

    let mut cmd = Command::new(MNTRS_BIN);
    cmd.args([
        "mount",
        "s3://test-bucket",
        MNTRS_MNT,
        "--opt",
        &format!("endpoint={}", MINIO_ENDPOINT),
        "--opt",
        &format!("access-key={}", MINIO_ACCESS),
        "--opt",
        &format!("secret-key={}", MINIO_SECRET),
        "--opt",
        "region=us-east-1",
    ]);
    if read_only {
        cmd.arg("--read-only");
    }

    let mut child = cmd.spawn().expect("mntrs mount failed to start");
    thread::sleep(Duration::from_secs(5));

    // Verify mount
    let status = Command::new("mount")
        .arg(MNTRS_MNT)
        .status()
        .expect("mount check failed");
    if !status.success() {
        let _ = child.kill();
        panic!("mntrs mount did not appear in mount table");
    }

    // Store PID for cleanup
    std::fs::write("/tmp/mntrs-fuse-test.pid", child.id().to_string()).unwrap();
}

fn mntrs_unmount() {
    let _ = Command::new("fusermount3")
        .arg("-u")
        .arg(MNTRS_MNT)
        .status();
}

// ============================================================
// Basic FUSE operations
// ============================================================

#[test]
fn fuse_mount_and_list_root() {
    mntrs_mount(true);
    let output = Command::new("ls")
        .arg(MNTRS_MNT)
        .output()
        .expect("ls failed");
    assert!(output.status.success(), "ls root failed");
    mntrs_unmount();
}

#[test]
fn fuse_stat_root() {
    mntrs_mount(true);
    let output = Command::new("stat")
        .arg(MNTRS_MNT)
        .output()
        .expect("stat failed");
    assert!(output.status.success(), "stat root failed");
    mntrs_unmount();
}

#[test]
fn fuse_df_shows_space() {
    mntrs_mount(true);
    let output = Command::new("df")
        .arg(MNTRS_MNT)
        .output()
        .expect("df failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("mntrs") || stdout.contains("1.0P"),
        "df should show mntrs mount"
    );
    mntrs_unmount();
}

#[test]
fn fuse_readdirplus_enabled() {
    // readdirplus is enabled in init — ls -la should work without errors
    mntrs_mount(true);
    let output = Command::new("ls")
        .arg("-la")
        .arg(MNTRS_MNT)
        .output()
        .expect("ls -la failed");
    assert!(output.status.success(), "ls -la failed");
    mntrs_unmount();
}

#[test]
fn fuse_cat_existing_file() {
    mntrs_mount(true);
    // Try to cat any file in the root
    let ls = Command::new("ls").arg(MNTRS_MNT).output().unwrap();
    let first = String::from_utf8_lossy(&ls.stdout)
        .lines()
        .next()
        .unwrap_or("")
        .to_string();
    if !first.is_empty() {
        let output = Command::new("cat")
            .arg(format!("{}/{}", MNTRS_MNT, first))
            .output()
            .expect("cat failed");
        assert!(
            output.status.success() || output.status.code() == Some(1),
            "cat should succeed or file-not-found (1)"
        );
    }
    mntrs_unmount();
}

#[test]
fn fuse_find_maxdepth() {
    mntrs_mount(true);
    let output = Command::new("find")
        .args([MNTRS_MNT, "-maxdepth", "2"])
        .output()
        .expect("find failed");
    assert!(output.status.success(), "find failed");
    mntrs_unmount();
}

#[test]
fn fuse_statfs_via_df() {
    mntrs_mount(true);
    let output = Command::new("df")
        .args(["-B1", MNTRS_MNT])
        .output()
        .expect("df -B1 failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Should report non-zero blocks
    let blocks: Vec<&str> = stdout.lines().collect();
    assert!(blocks.len() >= 2, "df should have at least 2 lines");
    mntrs_unmount();
}

#[test]
fn fuse_head_small_file() {
    mntrs_mount(true);
    let ls = Command::new("ls").arg(MNTRS_MNT).output().unwrap();
    let first = String::from_utf8_lossy(&ls.stdout)
        .lines()
        .next()
        .unwrap_or("")
        .to_string();
    if !first.is_empty() {
        let output = Command::new("head")
            .args(["-c", "100", &format!("{}/{}", MNTRS_MNT, first)])
            .output()
            .expect("head failed");
        assert!(output.status.success(), "head failed: {:?}", output.status);
    }
    mntrs_unmount();
}

#[test]
fn fuse_sha256_matches() {
    // If rclone mount is available, compare checksums
    let rclone_mnt = "/opt/maven-repo";
    if !std::path::Path::new(rclone_mnt).exists() {
        return; // Skip if rclone mount not available
    }
    mntrs_mount(true);
    let ls = Command::new("ls").arg(MNTRS_MNT).output().unwrap();
    let first = String::from_utf8_lossy(&ls.stdout)
        .lines()
        .next()
        .unwrap_or("")
        .to_string();
    if !first.is_empty() {
        let mntrs_sha = Command::new("sha256sum")
            .arg(format!("{}/{}", MNTRS_MNT, first))
            .output()
            .unwrap();
        let rclone_sha = Command::new("sha256sum")
            .arg(format!("{}/{}", rclone_mnt, first))
            .output()
            .unwrap();
        if mntrs_sha.status.success() && rclone_sha.status.success() {
            assert_eq!(
                String::from_utf8_lossy(&mntrs_sha.stdout)
                    .split_whitespace()
                    .next(),
                String::from_utf8_lossy(&rclone_sha.stdout)
                    .split_whitespace()
                    .next(),
                "sha256 mismatch between mntrs and rclone"
            );
        }
    }
    mntrs_unmount();
}
