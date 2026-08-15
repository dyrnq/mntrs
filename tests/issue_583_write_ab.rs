//! Issue #583 — A/B benchmark: CacheMode::Off vs CacheMode::Writes.
//!
//! Original motivation: mntrs writes were 9× slower than rclone
//! on the small-new-file workload (1 KiB writes, lots of fdatasync).
//! Pre-#583 the only code path was the writes-mode path: per-file
//! disk cache open + fdatasync on flush/release. The new off-mode
//! path keeps dirty bytes in an in-memory `Vec<u8>` and skips the
//! disk entirely.
//!
//! This microbench builds an MntrsFs with each mode against the
//! opendal Memory backend (no S3 RTT, no MinIO required) and runs
//! the same workload through both:
//!
//!   - N small new-file writes (1 KiB each)
//!   - each followed by close()/release() (the durability point)
//!
//! The Memory backend makes the relative off-vs-writes comparison
//! sharp: the only fixed cost difference is the FUSE-side buffer
//! logic itself (fdatasync vs in-memory enqueue).
//!
//! Run:
//!   cargo test --test issue_583_write_ab --release -- --nocapture
//!
//! The `--release` matters — debug builds add 10-100× of their own
//! overhead and obscure the ratio.
//!
//! Verification:
//!   - Off mode should be ≥2× faster than writes mode (conservative
//!     regression floor). The original 9× number was measured on
//!     real S3, where the disk-fdatasync cost dominated; on Memory
//!     the fdatasync still runs but the S3 PUT cost is gone, so we
//!     expect a smaller but still significant speedup.
//!   - If a future refactor regresses off mode back to writes-mode
//!     speed (e.g. accidentally fdatasync'ing in the off path),
//!     the assertion below trips CI.

use std::time::Instant;

use mntrs::core_fs::CoreFilesystem;
use mntrs::{CacheMode, new_test_fs_with_mode};
use opendal::Operator;
use opendal::services::Memory;

/// Build a fresh MntrsFs with the requested cache_mode against
/// an opendal Memory backend (no real S3 dependency).
fn build_fs(cache_mode: CacheMode, label: &str) -> mntrs::MntrsFs {
    let op = Operator::new(Memory::default()).unwrap();
    let cache_dir = std::env::temp_dir().join(format!(
        "mntrs-583-ab-{}-{}-{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&cache_dir);
    std::fs::create_dir_all(&cache_dir).unwrap();

    let fs = new_test_fs_with_mode(op, cache_dir, cache_mode);
    // The default write_back_delay (1 s) is fine — on the Memory
    // backend there's no real S3 upload to race with, and the
    // writeback worker runs on a different thread so it doesn't
    // contend with this measurement loop.
    fs
}

const ROOT_INO: u64 = 1;

fn bench_writes(mode: CacheMode, label: &str, n: usize) -> (std::time::Duration, usize) {
    let fs = build_fs(mode, label);

    let payload = vec![0xABu8; 1024]; // 1 KiB

    // Each iteration creates a fresh file (different name), writes
    // it, releases. The original 9× gap workload was 1 KiB new-file
    // writes — that's the workload that triggered Issue #583.
    let started = Instant::now();
    for i in 0..n {
        let name = format!("f_{label}_{i}");

        // create() returns (CoreFileAttr, fh) — we use attr.ino for
        // subsequent calls. Root ino is 1 by FUSE convention.
        let (attr, fh) = fs.create(ROOT_INO, &name, 0o644).expect("create");
        // open() → write() → release() per file. This is the
        // workload shape that exposed the 9× gap originally:
        // each close triggers the fdatasync path on writes mode;
        // off mode just enqueues the in-memory buffer.
        let _fh2 = fs.open(attr.ino, 1 /* O_WRONLY */).expect("open");
        fs.write(attr.ino, fh, 0, &payload).expect("write");
        fs.release(attr.ino, fh).expect("release");
        fs.unlink(ROOT_INO, &name).expect("unlink");
    }
    let elapsed = started.elapsed();
    (elapsed, n)
}

#[test]
fn off_vs_writes_throughput() {
    const N: usize = 200;

    println!(
        "\n=== Issue #583 write-throughput A/B \
         (Memory backend, {N} new files × 1 KiB) ===\n"
    );

    // Warmup: prime the opendal pool / runtime for both modes so
    // the first measured iteration isn't paying connection setup.
    let _ = bench_writes(CacheMode::Off, "off-warmup", 5);
    let _ = bench_writes(CacheMode::Writes, "writes-warmup", 5);

    let (off_dur, off_n) = bench_writes(CacheMode::Off, "off", N);
    let off_per = off_dur.as_nanos() / off_n as u128;
    let off_thru = (off_n as f64) / off_dur.as_secs_f64();
    println!("  CacheMode::Off    : {off_dur:>12?} ({off_per:>9} ns/op, {off_thru:>7.1} files/s)");

    let (wr_dur, wr_n) = bench_writes(CacheMode::Writes, "writes", N);
    let wr_per = wr_dur.as_nanos() / wr_n as u128;
    let wr_thru = (wr_n as f64) / wr_dur.as_secs_f64();
    println!("  CacheMode::Writes : {wr_dur:>12?} ({wr_per:>9} ns/op, {wr_thru:>7.1} files/s)");

    let speedup = wr_per as f64 / off_per as f64;
    println!("\n  off-mode speedup over writes: {speedup:.2}×");

    // Regression guard: off mode should be ≥2× faster than writes
    // on a fresh file workload of 1 KiB writes. The original 9×
    // number was on a real S3 backend with RTT; Memory backend
    // removes the RTT so we expect a smaller but still significant
    // speedup. If a future refactor regresses off mode back to
    // writes-mode speed (e.g. accidentally fdatasync'ing in the
    // off path), this trips CI.
    assert!(
        speedup >= 2.0,
        "off mode should be ≥2× faster than writes (got {speedup:.2}×) — \
         a regression here means the in-memory buffer path is no longer \
         skipping fdatasync"
    );
}
