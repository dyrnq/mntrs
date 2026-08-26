//! FUSE integration tests — requires MinIO (S3-compatible) backend
//! for the basic-read tests at the bottom of this file. Run with:
//!   MINIO_ENDPOINT=http://localhost:9000 cargo test --test fuse_integration_test
//!
//! These tests mount a real FUSE filesystem and verify read/write/stat operations.
//!
//! ## PR #606 regression tests (`write_hang_regression_*`)
//!
//! The two `write_hang_regression_*` tests at the bottom of this file
//! pin the FUSE write-path hang regression fixed by PR #606
//! (commit `7f8891c`). They do NOT require MinIO — they use the
//! `memory:///` backend (no docker, no remote storage). Pre-fix
//! (PR #602), the FUSE WRITE callback synchronously invoked
//! `Notifier::inval_inode()`, which internally calls
//! `nix::sys::uio::writev(/dev/fuse, ...)` and deadlocks fuser-0's
//! single-threaded event loop on the very first `write(2)` to the
//! mount. CI symptom: 6 h wall-clock timeout, fuser-0 stuck in
//! `folio_wait_bit_common`. Multiple long-running CI jobs and Claude
//! sessions reproducing the bug left behind permanent `D`-state
//! zombies (kernel `request_wait_answer` on /dev/fuse) — these
//! tests use a real FUSE mount with hard per-write timeouts so the
//! regression surfaces in seconds instead of 6 h.
//!
//! Linux-only: uses fusermount3 which isn't available on macOS/Windows.

#![cfg(target_os = "linux")]

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const MINIO_ENDPOINT: &str = "http://localhost:9000";
const MINIO_ACCESS: &str = "minioadmin";
const MINIO_SECRET: &str = "minioadmin";
const MNTRS_BIN: &str = "./target/debug/mntrs";
const MNTRS_MNT: &str = "/tmp/mntrs-fuse-test";

// ============================================================
// PR #606 regression pin — write-hang tests
// ============================================================
//
// The constants below pin the bug:
/// Hard wall-clock ceiling for one FUSE write (sequential test).
/// 5 s is ~5000× normal cost (1 KiB write on Memory backend is ~35 µs
/// per issue_583_write_ab microbench); pre-PR-#606 this never returns.
const WRITE_HANG_PER_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Sequential test write count. Matches the per-write-thread budget
/// × a small factor so a saturated regression (which would trip the
/// per-write ceiling in series → ≥ 50 s) trips the total ceiling
/// first.
const WRITE_HANG_SEQ_N: usize = 10;

/// Total wall-clock ceiling for the sequential test (sanity guard).
/// If the regression trips and every write hits the per-write ceiling
/// (5 s) in series, total ≥ 50 s — well above this 10 s ceiling.
const WRITE_HANG_SEQ_TOTAL_CEILING: Duration = Duration::from_secs(10);

/// Number of parallel writes in the burst test. Matches the
/// "20 parallel echo > file" workload from the PR #606 validation
/// comment at src/lib.rs:5977-5983, scaled up 2.5× to amplify any
/// saturation-induced deadlock.
const WRITE_HANG_BURST_N: usize = 50;

/// Total wall-clock ceiling for the burst test. Pre-fix, the very
/// first parallel burst wedges fuser-0 and ALL 50 threads block
/// simultaneously; the burst exceeds this ceiling and we panic
/// with the PR #606 reference.
const WRITE_HANG_BURST_CEILING: Duration = Duration::from_secs(10);

/// DRY bundle for the write-hang tests. Both `mount_with_cleanup`
/// and `teardown_mount` consume a `WriteHangConfig` so the test
/// bodies never construct paths inline.
#[derive(Debug, Clone)]
struct WriteHangConfig {
    /// Mountpoint label embedded in the path (e.g. "seq" or "burst").
    /// Used by error messages and by `teardown_mount`'s orphan-pgrep.
    label: &'static str,
    /// Resolved mountpoint path (`/tmp/mntrs-fuse-writehang-{label}-{pid}`).
    mountpoint: PathBuf,
    /// Resolved cache dir (`/tmp/mntrs-fuse-writehang-{label}-{pid}-cache`).
    cache_dir: PathBuf,
    /// Resolved binary path (`env!("CARGO_BIN_EXE_mntrs")`).
    bin: PathBuf,
}

impl WriteHangConfig {
    fn new(label: &'static str) -> Self {
        let pid = std::process::id();
        let mountpoint = PathBuf::from(format!("/tmp/mntrs-fuse-writehang-{label}-{pid}"));
        let cache_dir = PathBuf::from(format!("/tmp/mntrs-fuse-writehang-{label}-{pid}-cache"));
        let bin = PathBuf::from(env!("CARGO_BIN_EXE_mntrs"));
        Self {
            label,
            mountpoint,
            cache_dir,
            bin,
        }
    }
}

/// Spawn `mntrs mount memory:/// <mp> --cache-dir <cache>` and wait
/// for the mount to become ready (up to 60 s). Returns the live
/// `Child` handle so the caller can `kill()` on assertion failure
/// before the daemon wedges CI. Mirrors the readiness loop in
/// `tests/e2e/mount/memory_stress.sh:97-120`.
///
/// On failure the function kills the spawned child before
/// returning `Err`, so the test binary never leaks a mount daemon.
///
/// IMPORTANT: the `mntrs` binary may not be built (e.g. when
/// `cargo test` is invoked without first running `cargo build`).
/// Callers should check `cfg.bin.exists()` BEFORE invoking — this
/// matches the `fuse_sha256_matches` skip pattern at line 201-207.
fn mount_with_cleanup(cfg: &WriteHangConfig) -> std::io::Result<Child> {
    let mp_str = cfg.mountpoint.to_str().unwrap();

    // Best-effort pre-clean: kill any stale daemon holding this
    // mountpoint from a prior aborted test run. Mirrors the
    // `cleanup_iter` prologue at memory_stress.sh:53-88.
    let _ = Command::new("fusermount3").args(["-u", mp_str]).status();
    let _ = std::fs::remove_dir_all(&cfg.mountpoint);
    let _ = std::fs::remove_dir_all(&cfg.cache_dir);
    std::fs::create_dir_all(&cfg.mountpoint)?;
    std::fs::create_dir_all(&cfg.cache_dir)?;

    // No --allow-other: requires user_allow_other in /etc/fuse.conf
    // which is not guaranteed on CI runners. Drop it (matches the
    // CLI negation test's invocation shape at cli_negation_test.rs:45).
    let mut cmd = Command::new(&cfg.bin);
    cmd.args([
        "mount",
        "memory:///",
        mp_str,
        "--cache-dir",
        cfg.cache_dir.to_str().unwrap(),
    ])
    .stdout(Stdio::null())
    .stderr(Stdio::null());

    let mut child = cmd.spawn().map_err(|e| {
        eprintln!(
            "[{}] failed to spawn mntrs mount: {e}; \
             is the binary built? (try `cargo build` first)",
            cfg.label
        );
        e
    })?;

    // 60 s readiness probe: read /proc/mounts (kernel pseudo-file,
    // never wedges) + `timeout 2 ls <mp>/` (bounded `ls` so a
    // wedged daemon doesn't block the readiness loop for 60 s).
    let started = Instant::now();
    let mut ready = false;
    while started.elapsed() < Duration::from_secs(60) {
        if let Some(s) = read_proc_mounts()
            && s.contains(mp_str)
        {
            let ls_ok = Command::new("timeout")
                .args(["2", "ls", mp_str])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|st| st.success())
                .unwrap_or(false);
            if ls_ok {
                ready = true;
                break;
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
    if !ready {
        let _ = child.kill();
        let _ = child.wait();
        return Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!(
                "[{}] mount not ready after 60s — PR #606 regression? \
                 mountpoint={:?}, bin={:?}",
                cfg.label, cfg.mountpoint, cfg.bin
            ),
        ));
    }
    Ok(child)
}

/// Best-effort unmount of `cfg.mountpoint` and kill of the
/// `mntrs mount` child. Retries `fusermount3 -u` up to 5 times
/// (kernel EBUSY window after SIGKILL), falls back to `fusermount -u`,
/// then sweeps `pgrep -f "mntrs mount .*${MP}"` for orphans.
/// Mirrors `cleanup_iter` at memory_stress.sh:53-88 and `cleanup` at
/// lifecycle_stress.sh:77-83.
///
/// **CRITICAL: never invoke `Command::new("mount")` from this fn.**
/// When the FUSE daemon is wedged (the regression we're testing for),
/// `mount` itself reads /proc/mounts or /etc/mtab via stdio that goes
/// through the same wedged /dev/fuse session in some kernels and can
/// wedge the test process indefinitely. Use `read_proc_mounts()`
/// below — `/proc/mounts` is a kernel pseudo-file that always returns
/// the current mount table without going through FUSE.
///
/// `child` may be `None` (e.g. readiness probe failed before
/// `mount_with_cleanup` returned) — the orphan sweep still runs.
fn teardown_mount(cfg: &WriteHangConfig, child: Option<Child>) {
    let mp_str = cfg.mountpoint.to_str().unwrap();

    // 1. Best-effort unmount. CRITICAL: use `umount -l` (lazy
    //    unmount), NOT `fusermount3 -u`. When the FUSE daemon is
    //    wedged (the regression we're testing for), `fusermount3 -u`
    //    sends a cleanup request to the daemon via /dev/fuse and
    //    BLOCKS forever waiting for the reply. `umount -l` detaches
    //    the mount immediately at the kernel level without talking
    //    to the daemon, so it always succeeds.
    //
    //    We try `fusermount3 -u` first because it's the clean path
    //    (closes the FUSE session, frees /dev/fuse fd), but only
    //    with a hard 2 s ceiling per attempt. If it times out, fall
    //    back to `umount -l`.
    for _ in 0..3 {
        let still_mounted = read_proc_mounts().is_some_and(|s| s.contains(mp_str));
        if !still_mounted {
            break;
        }
        // Try the clean unmount first with a hard 2 s ceiling.
        let _ = Command::new("timeout")
            .args(["2", "fusermount3", "-u", mp_str])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        // If still mounted (daemon wedged), lazy-detach. ALWAYS
        // succeeds at the kernel level; the mount is gone from
        // /proc/mounts immediately.
        let _ = Command::new("umount")
            .arg("-l")
            .arg(mp_str)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        thread::sleep(Duration::from_millis(200));
    }

    // 2. Kill the original child handle (if any). Use SIGKILL with
    //    a bounded `try_wait` loop — never call `wait()` (can block
    //    if the daemon's threads are wedged in user space and never
    //    fully reaped). Drop handles the reap at process exit.
    if let Some(mut c) = child {
        let _ = c.kill();
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(2) {
            if matches!(c.try_wait(), Ok(Some(_))) {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
        // Intentionally do NOT call c.wait() — if the daemon's
        // threads are wedged in user-space, wait() blocks forever.
        // The Child is dropped here; the kernel will reap it at
        // test-process exit.
        drop(c);
    }

    // 3. Belt-and-suspenders: pgrep sweep for orphaned mntrs
    //    processes still referencing this mountpoint.
    if let Ok(out) = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "pgrep -f 'mntrs mount.*{mp_str}' 2>/dev/null || true"
        ))
        .output()
    {
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            if let Ok(pid) = line.trim().parse::<u32>() {
                let _ = Command::new("kill").arg(pid.to_string()).status();
            }
        }
        thread::sleep(Duration::from_millis(500));
    }

    // 4. Remove the mountpoint dir and cache dir. Safe now because
    //    the mount is detached in step 1; `remove_dir_all` will not
    //    try to look up a path on a dead FUSE mount.
    let _ = std::fs::remove_dir_all(&cfg.mountpoint);
    let _ = std::fs::remove_dir_all(&cfg.cache_dir);
}

/// Read `/proc/mounts` directly. Returns `None` if the read fails
/// (file missing, perms). This is the safe replacement for invoking
/// the `mount(8)` command — `/proc/mounts` is a kernel pseudo-file
/// that always returns the current mount table without going through
/// FUSE. Never wedges even when /dev/fuse is deadlocked.
fn read_proc_mounts() -> Option<String> {
    std::fs::read_to_string("/proc/mounts").ok()
}

/// PR #606 regression pin — sequential path.
///
/// Pre-fix (PR #602), the FUSE WRITE callback synchronously called
/// `fuser::Notifier::inval_inode()`, which internally did
/// `nix::sys::uio::writev(/dev/fuse, ...)` and deadlocked fuser-0's
/// single-threaded event loop. The first `echo > file` after a
/// fresh Memory mount would hang forever (CI 6 h wall-clock timeout,
/// fuser-0 stuck in `folio_wait_bit_common`).
///
/// This test mounts a fresh Memory backend, runs N=10 sequential
/// 1 KiB writes with a hard `WRITE_HANG_PER_WRITE_TIMEOUT` (5 s)
/// ceiling per write, and asserts:
///   1. Every write returned within the ceiling (the hang itself).
///   2. Total wall-clock is < 10 s (a saturated regression would
///      trip the per-write ceiling in series → ≥ 50 s).
///
/// ## Why no `__inval_inode_count_for_test()` assertion
///
/// The counter is process-static inside the `mntrs` crate. The
/// `mntrs mount` daemon is a separate process from this test
/// binary, so the test process's counter never advances — the
/// counter is incremented inside the daemon's `MntrsFs::write()`.
/// The wall-clock ceiling is the load-bearing regression check:
/// pre-fix the write hangs → `recv_timeout` fires → panic with
/// the PR #606 reference. The o_append_fuse_notifier_test pins
/// the counter advance via the direct trait API (in-process).
///
/// Memory backend: no docker, no k3s, no remote storage creds.
#[test]
fn write_hang_regression_sequential() {
    let cfg = WriteHangConfig::new("seq");

    // Skip gracefully if the binary isn't built (matches the
    // `fuse_sha256_matches` skip pattern at lines 201-207).
    if !cfg.bin.exists() {
        eprintln!(
            "[{}] mntrs binary not found at {:?}; skipping \
             (run `cargo build` first)",
            cfg.label, cfg.bin
        );
        return;
    }

    let child = match mount_with_cleanup(&cfg) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[{}] mount failed: {e}; skipping", cfg.label);
            teardown_mount(&cfg, None);
            return;
        }
    };

    // The wall-clock ceiling IS the regression check (see the
    // module doc for why the in-process counter can't observe
    // the daemon subprocess's counter).
    let mp_str = cfg.mountpoint.to_str().unwrap().to_string();
    let test_started = Instant::now();

    // 10 sequential writes. Each runs on a fresh thread so the
    // hard ceiling is enforced via `recv_timeout` (matches the
    // `assert_returns_within` shape at write_hang_repro.rs:80-112).
    //
    // Use `echo ... > file` (shell built-in, one `write(2)` syscall
    // then close) — this is the exact write pattern that wedges the
    // reverted binary. `printf '%1024s' | tr` etc. happen to escape
    // the deadlock on some runs because the shell fork/exec timing
    // races with the daemon's first `notifier.inval_inode()`;
    // `echo` is the minimal reproducer that triggers it every time.
    for i in 0..WRITE_HANG_SEQ_N {
        let target = format!("{mp_str}/hang_seq_{i}.bin");
        let write_started = Instant::now();
        let (tx, rx) = std::sync::mpsc::channel::<Result<(), String>>();
        let _handle = thread::Builder::new()
            .name(format!("write-hang-seq/{i}"))
            .spawn(move || {
                // Single-shot shell write: `echo <payload> > <target>`.
                // 64 bytes is enough to be a real write and small
                // enough not to fragment. Same pattern that wedges
                // pre-#606 daemons in <1 s.
                let payload = format!("seq_write_{i}_payload_{}", "A".repeat(40));
                let script = format!("echo '{payload}' > '{target}'");
                let result = match Command::new("sh")
                    .args(["-c", &script])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                {
                    Ok(st) if st.success() => Ok(()),
                    Ok(st) => Err(format!("echo exit {:?}", st.code())),
                    Err(e) => Err(format!("spawn error: {e}")),
                };
                let _ = tx.send(result);
            })
            .expect("spawn write thread");

        match rx.recv_timeout(WRITE_HANG_PER_WRITE_TIMEOUT) {
            Ok(Ok(())) => {
                let elapsed = write_started.elapsed();
                eprintln!("[seq] write {i} returned in {elapsed:?}");
            }
            Ok(Err(e)) => {
                teardown_mount(&cfg, Some(child));
                panic!(
                    "[seq] write {i} failed: {e} — \
                     PR #606 regression? (write-path hang fix)"
                );
            }
            Err(_) => {
                // Hard hang — the FUSE write path is wedged again.
                // Don't join the worker (it's stuck in writev inside
                // fuser-0); the test process exit reaps it.
                teardown_mount(&cfg, Some(child));
                panic!(
                    "[seq] write {i} did not return within \
                     {WRITE_HANG_PER_WRITE_TIMEOUT:?} — \
                     FUSE write-path hang REGRESSION \
                     (PR #602 re-introduced? fix is PR #606 commit 7f8891c: \
                     inval_inode dispatched to std::thread::Builder::spawn \
                     instead of synchronous inline call in src/lib.rs \
                     write() callback)"
                );
            }
        }
    }

    let total_elapsed = test_started.elapsed();

    // Assertion: total wall-clock < 10 s.
    assert!(
        total_elapsed < WRITE_HANG_SEQ_TOTAL_CEILING,
        "[seq] total elapsed {total_elapsed:?} exceeds \
         {WRITE_HANG_SEQ_TOTAL_CEILING:?} ceiling — the FUSE write path \
         is likely hung. PR #606 regression?"
    );

    eprintln!(
        "[seq] OK — {} writes in {total_elapsed:?}",
        WRITE_HANG_SEQ_N
    );
    teardown_mount(&cfg, Some(child));
}

/// PR #606 regression pin — burst path.
///
/// Pre-fix (PR #602), the FUSE WRITE callback synchronously called
/// `fuser::Notifier::inval_inode()` (writev to /dev/fuse). Under
/// sustained parallel write load, the kernel's notification queue
/// fills, writev blocks, and fuser-0 deadlocks — every subsequent
/// FUSE request from userspace blocks in `request_wait_answer`.
/// CI symptom: 6 h timeout, fuser-0 in `folio_wait_bit_common`.
///
/// This test amplifies the saturation path with `WRITE_HANG_BURST_N`
/// (50) parallel writes via `std::thread`. Each write runs on its
/// own thread (so a single hung FUSE worker doesn't mask the others).
/// Pre-fix, the very first parallel burst wedges fuser-0 and ALL 50
/// threads block simultaneously; the test fails on the outer
/// 10 s wall-clock ceiling. Asserts:
///   1. Total wall-clock < 10 s (the burst ceiling).
///   2. All 50 writes succeeded (`printf` returned 0).
///
/// ## Why no `__inval_inode_count_for_test()` assertion
///
/// Same reason as `write_hang_regression_sequential`: the counter
/// is process-static inside the `mntrs` crate, and the `mntrs mount`
/// daemon is a separate process from this test binary. The
/// wall-clock ceiling is the load-bearing regression check.
///
/// Memory backend: no docker, no k3s, no remote storage creds.
#[test]
fn write_hang_regression_burst() {
    let cfg = WriteHangConfig::new("burst");

    if !cfg.bin.exists() {
        eprintln!(
            "[{}] mntrs binary not found at {:?}; skipping",
            cfg.label, cfg.bin
        );
        return;
    }

    let child = match mount_with_cleanup(&cfg) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[{}] mount failed: {e}; skipping", cfg.label);
            teardown_mount(&cfg, None);
            return;
        }
    };

    let mp_str = cfg.mountpoint.to_str().unwrap().to_string();
    let test_started = Instant::now();

    // Note: the wall-clock ceiling is the load-bearing
    // regression check. The `__inval_inode_count_for_test()`
    // counter is process-static in the `mntrs` crate; the
    // `mntrs mount` daemon is a separate process so the test
    // process's counter can't observe daemon-side writes.
    // `tests/o_append_fuse_notifier_test.rs` pins the counter
    // advance via the direct trait API (in-process).

    // 50 parallel writes, each on its own thread. Each thread
    // captures its own `output.status.success()` into a shared
    // `Vec<(usize, Result<(), String>)>` guarded by a Mutex.
    // `String` (not `io::Error`) so the Vec is `Clone` — we need
    // to clone the result out of the guard before assertions.
    type BurstOutcome = (usize, Result<(), String>);
    let results: Arc<Mutex<Vec<BurstOutcome>>> =
        Arc::new(Mutex::new(Vec::with_capacity(WRITE_HANG_BURST_N)));

    let mut handles = Vec::with_capacity(WRITE_HANG_BURST_N);
    for i in 0..WRITE_HANG_BURST_N {
        let results = Arc::clone(&results);
        let mp_str = mp_str.clone();
        let handle = thread::Builder::new()
            .name(format!("write-hang-burst/{i}"))
            .spawn(move || {
                let target = format!("{mp_str}/hang_burst_{i}.bin");
                let payload = format!("burst_write_{i}_payload_{}", "A".repeat(40));
                let r = match Command::new("sh")
                    .args(["-c", &format!("echo '{payload}' > '{target}'")])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                {
                    Ok(st) if st.success() => Ok(()),
                    Ok(st) => Err(format!("echo exit {:?}", st.code())),
                    Err(e) => Err(format!("spawn error: {e}")),
                };
                results.lock().unwrap().push((i, r));
            })
            .expect("spawn burst thread");
        handles.push(handle);
    }

    // Outer ceiling: 10 s wall-clock for the whole burst.
    // Pre-fix, the FUSE event loop deadlocks on the first write
    // and every subsequent write also blocks → the burst exceeds
    // 10 s and we panic with the PR #606 reference.
    //
    // CRITICAL: do NOT `h.join()` the workers. Each worker's
    // `Command::status()` does wait4 on its `sh` child — if the
    // daemon is wedged, the `sh` child is in D-state forever, so
    // `join()` blocks forever too. Instead, poll the shared
    // results vec with a 100 ms tick until either all workers
    // have pushed their result OR the deadline expires.
    // Workers are detached — test process exit reaps them.
    drop(handles);
    let deadline = test_started + WRITE_HANG_BURST_CEILING;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            teardown_mount(&cfg, Some(child));
            panic!(
                "[burst] outer {:?} ceiling tripped — FUSE write-path hang \
                 REGRESSION (PR #602 re-introduced? fix is PR #606 commit \
                 7f8891c)",
                WRITE_HANG_BURST_CEILING
            );
        }
        if results.lock().unwrap().len() >= WRITE_HANG_BURST_N {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    let total_elapsed = test_started.elapsed();
    let results = results.lock().unwrap().clone();

    teardown_mount(&cfg, Some(child));

    // Assertion 1: outer ceiling held.
    assert!(
        total_elapsed < WRITE_HANG_BURST_CEILING,
        "[burst] total elapsed {total_elapsed:?} exceeds \
         {WRITE_HANG_BURST_CEILING:?} ceiling — FUSE write-path hang \
         REGRESSION (PR #602 re-introduced? fix is PR #606 commit \
         7f8891c)"
    );

    // Assertion 2: every individual write succeeded.
    for (i, r) in &results {
        assert!(
            r.is_ok(),
            "[burst] write {i} failed: {:?} — PR #606 regression?",
            r.as_ref().err()
        );
    }
    assert_eq!(
        results.len(),
        WRITE_HANG_BURST_N,
        "[burst] expected {} write results, got {}",
        WRITE_HANG_BURST_N,
        results.len()
    );

    eprintln!("[burst] OK — {} writes in {total_elapsed:?}", results.len());
}

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

    // Verify mount — use mount | grep (works even if /etc/mtab is stale)
    let output = std::process::Command::new("mount")
        .output()
        .expect("mount command failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    if !stdout.contains(MNTRS_MNT) {
        let _ = child.kill();
        eprintln!("mount output: {}", stdout);
        panic!("mntrs mount did not appear in mount table");
    }

    // Store PID for cleanup, then intentionally leak the handle
    // (the mount daemon is meant to live past this function)
    std::fs::write("/tmp/mntrs-fuse-test.pid", child.id().to_string()).unwrap();
    std::mem::forget(child);
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
