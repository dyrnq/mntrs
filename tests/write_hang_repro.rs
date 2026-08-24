//! Reproducer for write-hang E2E bug (regression of bench-comparison
//! + memory-stress-loop + hdfs-kerberos + mount-tests/lifecycle
//! + memory-stress-loop hitting 6h timeout).
//!
//! CI evidence (2026-08-24, runs 32632405705 / 32632405693):
//!
//! - `bench-comparison` (moka + dashmap, MinIO backend): categories
//!   1-5 (DirList / Stat / SeqRead / ddRead / RandRead) complete in
//!   ~5 s. `=== 6. Write ===` header prints at 06:01:12. Then 5 h
//!   57 min of silence. Post-cancel log:
//!   `Terminate orphan process: pid (6032) (cp)` — a `cp` of 1 KiB to
//!   the FUSE-mounted MinIO bucket hung for the full 6 h.
//! - `memory-stress-loop` (Memory backend, no remote): build done
//!   at 06:02:51; `./stress_loop.sh 50` started; iter 1's first
//!   write never returned. 5 h 56 min of silence.
//! - `lifecycle-stress`, `hdfs-kerberos`, `mount-tests (memory/s3/hdfs)`,
//!   `s3-lifecycle-stress`: same pattern — test command launches,
//!   nothing else prints, killed at 6 h.
//!
//! Root cause is in the FUSE write path itself (not MinIO-specific —
//! Memory backend also hangs). Likely suspects from the recent
//! cache_fd split (PR #584, issue #A + #583) and the related
//! writeback / release / disk_write_pool fixes (commits 610c6b0,
//! 1b1cfea, 2b7235b, 6e6a58f, PR #589-#591, #594).
//!
//! This test pins the bug with a single-file workload, hard
//! timeout, and the `Memory` backend (no S3 / HDFS dependency).
//! Run:
//!
//!   cargo test --test write_hang_repro --release -- --nocapture
//!
//! Expected (pre-bug-fix): the assertion trips — write() does not
//! return within 10 s.
//! Expected (post-bug-fix): the assertion holds — write() returns
//! in < 1 s on the Memory backend.
//!
//! Timeout budget: 10 s. The original issue_583_write_ab microbench
//! reports 200×1 KiB Off mode at 6.94 ms total (~35 µs/write); a
//! 10 s budget is ~285 000× normal cost, so any genuine fix returns
//! orders of magnitude faster than the timeout.

use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use mntrs::core_fs::CoreFilesystem;
use mntrs::{CacheMode, new_test_fs_with_mode};
use opendal::Operator;
use opendal::services::Memory;

const ROOT_INO: u64 = 1;
const TIMEOUT: Duration = Duration::from_secs(10);

/// Spin up an MntrsFs against an in-process opendal Memory backend.
/// Mirrors `tests/issue_583_write_ab.rs::build_fs` exactly so the
/// two microbenches share the same setup shape.
fn build_fs(mode: CacheMode) -> mntrs::MntrsFs {
    let op = Operator::new(Memory::default()).unwrap();
    let cache_dir = std::env::temp_dir().join(format!(
        "mntrs-write-hang-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        match mode {
            CacheMode::Off => "off",
            _ => "writes",
        }
    ));
    let _ = std::fs::remove_dir_all(&cache_dir);
    std::fs::create_dir_all(&cache_dir).unwrap();
    new_test_fs_with_mode(op, cache_dir, mode)
}

/// Run a closure on a worker thread, fail the test if it doesn't
/// return within `timeout`. Returns the closure's output (Ok or
/// Err). If the timeout fires, returns `Err(())` and the worker
/// thread is leaked (test process exit cleans up).
fn assert_returns_within<F, T>(label: &str, timeout: Duration, f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    let started = Instant::now();
    let handle = thread::Builder::new()
        .name(format!("write-hang-repro/{label}"))
        .spawn(move || {
            let result = f();
            let _ = tx.send(result);
        })
        .expect("spawn worker thread");

    match rx.recv_timeout(timeout) {
        Ok(v) => {
            let elapsed = started.elapsed();
            println!("  [{label}] returned in {elapsed:?}");
            handle.join().expect("worker thread panic");
            v
        }
        Err(_) => {
            // Don't join — the worker is stuck. Test fails loudly
            // so CI surfaces the hang instead of waiting 6 h for
            // the GHA wall-clock kill.
            panic!(
                "[{label}] did not return within {timeout:?} — \
                 this is the FUSE write-hang bug; see module docs"
            );
        }
    }
}

fn run_workload(label: &str, mode: CacheMode) {
    println!(
        "\n=== Write-hang reproducer \
         ({label}, Memory backend, 1 KiB write, 10 s hard timeout) ===\n"
    );

    // Arc lets each `assert_returns_within` closure own its own
    // cheap clone — the test thread is the only consumer of the
    // worker closure here, but we still want each closure to
    // satisfy `FnOnce() -> T + Send + 'static`, which a borrowed
    // `&MntrsFs` cannot.
    let fs = Arc::new(build_fs(mode));
    let payload = vec![0xABu8; 1024]; // 1 KiB — the workload size
    // that hangs in the CI runs
    let name = format!("hang_repro_{label}.bin");

    // create(): should return in < 1 ms. If this hangs, the bug is
    // in the inode allocation / inodes map insert, not the write path.
    let fs_c = Arc::clone(&fs);
    let name_c = name.clone();
    let (attr, fh) = assert_returns_within("create", TIMEOUT, move || {
        fs_c.create(ROOT_INO, &name_c, 0o644).expect("create")
    });
    println!("  ino={}, fh={fh}", attr.ino);

    // open(): should return in < 1 ms.
    let fs_o = Arc::clone(&fs);
    let ino = attr.ino;
    let _fh2 = assert_returns_within("open", TIMEOUT, move || {
        fs_o.open(ino, 1 /* O_WRONLY */).expect("open")
    });

    // write(): the suspect call. This is what hangs in CI.
    let fs_w = Arc::clone(&fs);
    let payload_w = payload.clone();
    assert_returns_within("write", TIMEOUT, move || {
        fs_w.write(ino, fh, 0, &payload_w).expect("write")
    });

    // release(): also suspect — recent PRs (#594 and earlier) fixed
    // the ordering of "inode size update vs writeback enqueue" and
    // the "cache_fd is None + dirty" path. If release hangs, the bug
    // is here.
    let fs_r = Arc::clone(&fs);
    assert_returns_within("release", TIMEOUT, move || {
        fs_r.release(ino, fh).expect("release")
    });

    println!("\n✅ all four FUSE write-path calls returned within 10 s");
}

#[test]
fn create_write_release_writes_mode_returns_within_10s() {
    run_workload("writes", CacheMode::Writes);
}

/// Same workload but with CacheMode::Off. The CI runs that hit 6 h
/// were using whatever cache mode the default mount configures; this
/// test pins both code paths independently so the bug can be narrowed
/// to a specific mode once a fix lands.
#[test]
fn create_write_release_off_mode_returns_within_10s() {
    run_workload("off", CacheMode::Off);
}
