//! Integration tests for `writeback::spawn` covering the upload
//! pipeline end-to-end against the memory backend. Goal: lift
//! `src/writeback.rs` coverage from ~10% to >50% without an external
//! S3/HDFS dep.
//!
//! Covers:
//!   - Happy path: fresh enqueue → upload → .dirty removed
//!   - Cache file vanished: orphan .dirty → .dirty cleaned, no panic
//!   - INO_RECOVERY_SENTINEL: crash-recovery upload (ino=0) skips
//!     inode update
//!   - Empty file: zero-byte upload succeeds
//!   - .dirty sidecar lifecycle: stays until upload completes, then
//!     removed
//!   - Multiple sequential uploads on the same worker
//!
//! NOT covered (would need mock backend):
//!   - 5-attempt retry loop on transient failures
//!   - 120s timeout error path
//!   - MAX_REENQUEUE_CYCLES cap math (constant is pinned in unit test)
//!   - Multipart upload path (>200 MiB — too large for a unit test)

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use mntrs::writeback::WritebackTask;
use mntrs::{FileType, InodeEntry, Inodes, writeback};
use opendal::Operator;
use opendal::services::{Memory, S3};

/// Unique tempdir per test process; avoids cross-test contamination.
fn scratch_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "mntrs-wb-it-{}-{}-{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Inodes + cache_dir + writeback_pending + memory op wired together.
struct Harness {
    op: Arc<Operator>,
    inodes: Inodes,
    // Held to mirror `MntrsFs`'s lifetime — the worker spawn's
    // post-upload `drop_block_cache_for_path` reads through this.
    #[allow(dead_code)]
    disk_cache_index: Arc<dashmap::DashMap<mntrs::util::CacheKey, (u64, std::time::Instant)>>,
    writeback_pending: Arc<dashmap::DashSet<String>>,
    cache_dir: PathBuf,
    sender: writeback::Sender,
    handle: tokio::task::JoinHandle<()>,
    rt: tokio::runtime::Runtime,
    /// Spawn-time `delay` arg passed to `writeback::spawn`. The
    /// `enqueue_default` helper uses this for the pre-#202
    /// uniform-delay behavior. New tests that want to exercise
    /// the per-task delay path should call `enqueue` directly
    /// with `Duration::ZERO` or another explicit value.
    spawn_delay: Duration,
}

impl Harness {
    fn new(label: &str) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let op = Arc::new(Operator::new(Memory::default()).unwrap());
        let inodes: Inodes = Arc::new(dashmap::DashMap::new());
        let disk_cache_index: Arc<dashmap::DashMap<_, _>> = Arc::new(dashmap::DashMap::new());
        let writeback_pending: Arc<dashmap::DashSet<_>> = Arc::new(dashmap::DashSet::new());
        let cache_dir = scratch_dir(label);

        // Use a tiny write-back delay so tests run fast.
        let (tx, handle) = rt.block_on(async {
            writeback::spawn(
                op.clone(),
                inodes.clone(),
                disk_cache_index.clone(),
                cache_dir.clone(),
                writeback_pending.clone(),
                Duration::from_millis(100),
            )
        });

        Self {
            op,
            inodes,
            disk_cache_index,
            writeback_pending,
            cache_dir,
            sender: tx,
            handle,
            rt,
            spawn_delay: Duration::from_millis(100),
        }
    }

    /// Write `content` to a cache file and create the `.dirty` sidecar.
    /// Returns the cache path + the .dirty path.
    fn stage_file(&self, name: &str, content: &[u8]) -> (PathBuf, PathBuf) {
        let write_buffer_path = self.cache_dir.join(name);
        std::fs::write(&write_buffer_path, content).unwrap();
        let dirty = write_buffer_path.with_extension("dirty");
        std::fs::write(&dirty, name).unwrap();
        (write_buffer_path, dirty)
    }

    /// Enqueue a fresh upload (cycle=0) with the given per-task
    /// delay. Pass `Duration::ZERO` to test the immediate-upload
    /// path (issue #202); pass the harness default (100 ms) to
    /// test the uniform-delay fallback path.
    fn enqueue(&self, ino: u64, remote: &str, write_buffer_path: PathBuf, delay: Duration) {
        self.sender
            .send(WritebackTask {
                ino,
                remote_path: remote.to_string(),
                write_buffer_path,
                in_memory_payload: None,
                // Issue #T2-N: existing tests don't exercise
                // Minimal mode; they want the on-disk file to
                // survive upload (Writes/Full semantics).
                delete_cache_on_success: false,
                retry_cycle: 0,
                per_task_delay: delay,
                // Issue #595: 16 MiB matches rclone's default.
                // Tests don't assert on chunk; they only need
                // the upload to succeed.
                buffer_size: 16 * 1024 * 1024,
            })
            .unwrap();
    }

    /// Convenience: enqueue with the harness's spawn-time delay
    /// (the pre-#202 uniform behavior). Preserves the call shape
    /// of the 7 tests added in PR #216 — they all use the uniform
    /// delay to keep the timing assumptions simple.
    fn enqueue_default(&self, ino: u64, remote: &str, write_buffer_path: PathBuf) {
        let delay = self.spawn_delay;
        self.enqueue(ino, remote, write_buffer_path, delay);
    }

    /// Wait for the .dirty sidecar to disappear (upload completed).
    /// Polls every 50ms up to `max_ms` total. Returns true if drained.
    fn wait_drain(&self, dirty: &Path, max_ms: u64) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_millis(max_ms) {
            if !dirty.exists() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        !dirty.exists()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        // Drop the sender so the worker exits its recv() loop, then
        // abort the runtime which cancels all tasks. This is the same
        // pattern as the production daemon's SIGKILL recovery path
        // tested by stress/05.
        self.handle.abort();
        // Take the runtime out by replacing with a no-op so we don't
        // move-out of `self.rt` (Drop can't move out of &mut).
        let rt = std::mem::replace(
            &mut self.rt,
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap(),
        );
        rt.shutdown_timeout(Duration::from_millis(50));
    }
}

#[test]
fn happy_path_uploads_and_removes_dirty_sidecar() {
    let h = Harness::new("happy");
    let (write_buffer_path, dirty) = h.stage_file("a.bin", b"hello writeback");

    // Track in inodes so the upload updates mtime.
    h.inodes.insert(
        100,
        InodeEntry {
            path: std::sync::Arc::from("/remote/a.bin"),
            kind: FileType::RegularFile,
            size: 5,
            mtime: None,
        },
    );

    h.enqueue_default(100, "/remote/a.bin", write_buffer_path);

    assert!(
        h.wait_drain(&dirty, 5_000),
        "dirty sidecar removed within 5s"
    );

    // Verify backend has the file
    h.rt.block_on(async {
        let buf = h.op.read("/remote/a.bin").await.unwrap();
        assert_eq!(buf.to_vec(), b"hello writeback");
    });
}

#[test]
fn cache_file_vanished_cleans_dirty_without_panic() {
    let h = Harness::new("vanished");
    let (write_buffer_path, dirty) = h.stage_file("gone.bin", b"data");

    // Remove the cache file BEFORE upload fires (simulates LRU eviction)
    std::fs::remove_file(&write_buffer_path).unwrap();
    // dirty sidecar must still be present (orphan)
    assert!(dirty.exists());

    h.enqueue_default(42, "/remote/gone.bin", write_buffer_path);

    // Worker should drop the task cleanly:
    //   PENDING_COUNT -= 1
    //   .dirty sidecar removed
    //   no upload attempted, no panic
    assert!(
        h.wait_drain(&dirty, 5_000),
        "orphan .dirty cleaned within 5s"
    );

    // Backend must NOT have the file (upload was skipped).
    h.rt.block_on(async {
        let res = h.op.read("/remote/gone.bin").await;
        assert!(res.is_err(), "backend should not have the file");
    });
}

#[test]
fn ino_recovery_sentinel_uploads_without_inode_update() {
    let h = Harness::new("recovery");
    let (write_buffer_path, dirty) = h.stage_file("recovery.bin", b"recovered data");

    // INO_RECOVERY_SENTINEL = 0 — no inodes entry to update.
    h.enqueue_default(
        mntrs::INO_RECOVERY_SENTINEL,
        "/recovery/path",
        write_buffer_path,
    );

    assert!(h.wait_drain(&dirty, 5_000), "dirty removed within 5s");

    h.rt.block_on(async {
        let buf = h.op.read("/recovery/path").await.unwrap();
        assert_eq!(buf.to_vec(), b"recovered data");
    });

    // Sanity: no inodes entry was created or modified.
    assert!(h.inodes.is_empty(), "no inodes entries touched");
}

#[test]
fn empty_file_uploads_successfully() {
    let h = Harness::new("empty");
    let (write_buffer_path, dirty) = h.stage_file("empty.bin", b"");

    h.enqueue_default(7, "/remote/empty.bin", write_buffer_path);

    assert!(h.wait_drain(&dirty, 5_000), "dirty removed for empty file");

    h.rt.block_on(async {
        let meta = h.op.stat("/remote/empty.bin").await.unwrap();
        assert_eq!(meta.content_length(), 0);
    });
}

#[test]
fn multiple_files_upload_in_order_or_out_of_order_but_all_complete() {
    // Verifies the worker correctly drains multiple enqueued tasks.
    // Don't assert FIFO order — writeback is best-effort with up to
    // UPLOAD_SEM=4 concurrent uploads, so completion order may differ
    // from enqueue order. Just verify all complete.
    let h = Harness::new("multi");

    let mut dirtys = Vec::new();
    for i in 0..8 {
        let name = format!("file_{i}.bin");
        let content = format!("content of file {i}");
        let (write_buffer_path, dirty) = h.stage_file(&name, content.as_bytes());
        h.enqueue_default(
            (i + 1) as u64,
            &format!("/remote/{name}"),
            write_buffer_path,
        );
        dirtys.push(dirty);
    }

    for d in &dirtys {
        assert!(
            h.wait_drain(d, 10_000),
            "all .dirty sidecars removed within 10s"
        );
    }

    h.rt.block_on(async {
        for i in 0..8 {
            let buf = h.op.read(&format!("/remote/file_{i}.bin")).await.unwrap();
            assert_eq!(buf.to_vec(), format!("content of file {i}").as_bytes());
        }
    });
}

#[test]
fn writeback_pending_set_cleared_after_upload() {
    // Issue #38: writeback_pending must remove the path after upload
    // so the next flush/release with new content can enqueue a fresh task.
    let h = Harness::new("pending");
    let (write_buffer_path, dirty) = h.stage_file("pending.bin", b"x");

    // Mimic flush/release inserting into writeback_pending
    h.writeback_pending
        .insert("/remote/pending.bin".to_string());

    h.enqueue_default(50, "/remote/pending.bin", write_buffer_path);

    assert!(h.wait_drain(&dirty, 5_000), "dirty removed");

    // writeback_pending should now be empty (worker cleared it on success)
    assert!(
        h.writeback_pending.is_empty(),
        "writeback_pending cleared after upload (issue #38)"
    );
}

#[test]
fn cycle1_re_enqueue_uses_longer_cooldown_constant() {
    // Document the contract: cycle>=1 routes to REENQUEUE_COOLDOWN
    // (60s) vs fresh enqueue's `delay` arg. We can't time-wait 60s
    // in a unit test, but the constants are pinned in writeback.rs's
    // inline tests — this is a documentation test that the constants
    // haven't silently shifted.
    //
    // If someone tightens REENQUEUE_COOLDOWN without intending to,
    // this test catches it as a behavior change worth reviewing.
    assert_eq!(writeback::REENQUEUE_COOLDOWN, Duration::from_secs(60));
    assert_eq!(writeback::MAX_REENQUEUE_CYCLES, 10);
}

#[test]
fn small_file_with_immediate_delay_uploads_fast() {
    // Issue #202: a per-task delay of ZERO must upload on the next
    // worker tick (no 5s queue wait). This is the "small file fast
    // path" — databases (SQLite, etcd, RocksDB) writing many small
    // files get sub-second durability instead of 5s.
    //
    // We use `enqueue` (not `enqueue_default`) to send a literal
    // Duration::ZERO so the worker's cycle=0 branch sees a real
    // per-task delay, not the harness's 100ms spawn-time fallback.
    let h = Harness::new("small-immediate");
    let (write_buffer_path, dirty) = h.stage_file("small.bin", b"tiny payload");

    h.sender
        .send(WritebackTask {
            ino: 1,
            remote_path: "/remote/small.bin".to_string(),
            write_buffer_path,
            in_memory_payload: None,
            // Issue #T2-N: harness tests don't exercise
            // Minimal mode; keep the on-disk file after
            // upload (Writes/Full semantics).
            delete_cache_on_success: false,
            retry_cycle: 0,
            per_task_delay: Duration::ZERO,
            buffer_size: 16 * 1024 * 1024,
        })
        .unwrap();

    // .dirty must disappear fast — well under 5s. We give 200ms
    // headroom for the worker tick + async runtime wakeup.
    assert!(
        h.wait_drain(&dirty, 200),
        "immediate-delay upload completed in <200ms (issue #202)"
    );

    h.rt.block_on(async {
        let buf = h.op.read("/remote/small.bin").await.unwrap();
        assert_eq!(buf.to_vec(), b"tiny payload");
    });
}

#[test]
fn large_file_with_default_delay_uses_5s() {
    // Regression for the non-immediate path: a 5s per-task delay
    // must NOT upload in 1s. Without this assertion someone could
    // accidentally route all enqueues through Duration::ZERO and
    // the regression would be silent (only visible in prod via
    // runaway upload churn).
    //
    // We don't actually wait 5s — the upper bound test below would
    // be flaky on a slow CI runner. Instead we assert the negative:
    // at 1s, .dirty must still be present.
    let h = Harness::new("large-delayed");
    let (write_buffer_path, dirty) = h.stage_file("large.bin", b"big payload");

    h.sender
        .send(WritebackTask {
            ino: 2,
            remote_path: "/remote/large.bin".to_string(),
            write_buffer_path,
            in_memory_payload: None,
            // Issue #T2-N: not Minimal; keep file on disk.
            delete_cache_on_success: false,
            retry_cycle: 0,
            per_task_delay: Duration::from_secs(5),
            buffer_size: 16 * 1024 * 1024,
        })
        .unwrap();

    // 5s delay means .dirty survives at least 1s.
    assert!(
        !h.wait_drain(&dirty, 1_000),
        "5s-delay upload still pending at 1s (issue #202 non-immediate path)"
    );
}

/// Issue #583: in `CacheMode::Off`, the FUSE worker packs the
/// dirty buffer into `WritebackTask.in_memory_payload` instead of
/// staging a cache file + `.dirty` sidecar. The worker must
/// upload the inline bytes and leave no `.dirty` artifact
/// (because none was created in the first place).
///
/// This is the regression guard for the off-mode writeback path.
/// Pre-#583 the field didn't exist and the worker always did
/// `fs::read(cpath)`; a future refactor that re-introduces that
/// branch unconditionally would trip this test (the cache file
/// doesn't exist — only the inline payload does).
#[test]
fn in_memory_payload_uploads_inline_bytes_without_cache_file() {
    let h = Harness::new("off-mode");

    // Deliberately do NOT call `stage_file` — the off-mode path
    // must work with no on-disk cache file at all. Construct a
    // dummy path that won't exist; the worker must never touch
    // it because `in_memory_payload` is `Some(_)`.
    let phantom_path = h.cache_dir.join("phantom.bin");
    assert!(!phantom_path.exists());

    let payload = bytes::Bytes::from_static(b"off-mode inline bytes");
    h.sender
        .send(WritebackTask {
            ino: 7,
            remote_path: "/remote/off.bin".to_string(),
            // Worker ignores this path when in_memory_payload is
            // Some(_) — passing the phantom confirms the field
            // takes precedence and no fs::read is attempted.
            write_buffer_path: phantom_path.clone(),
            in_memory_payload: Some(payload.clone()),
            // Issue #T2-N: off-mode tasks carry no on-disk
            // file; the worker's `delete_cache_on_success &&
            // in_memory_payload.is_none()` guard ensures this
            // is a no-op for off-mode tasks.
            delete_cache_on_success: false,
            retry_cycle: 0,
            per_task_delay: Duration::ZERO,
            buffer_size: 16 * 1024 * 1024,
        })
        .unwrap();

    // Wait for the upload to land — polled via a small busy
    // loop on the backend, not on `.dirty` (off mode never
    // writes one).
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        let got =
            h.rt.block_on(async { h.op.read("/remote/off.bin").await.ok() });
        let matches = matches!(&got, Some(buf) if buf.to_vec() == payload.to_vec());
        if matches {
            // Backend received the inline bytes — the path
            // through `in_memory_payload` worked.
            assert!(
                !phantom_path.exists(),
                "off-mode writeback must NOT touch the on-disk phantom path"
            );
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("off-mode inline payload did not upload within 5s");
}

/// Issue #TBD: when a writeback upload fails, the `.dirty` sidecar
/// stays on disk indefinitely. The retry loop will retry for up to
/// ~75 seconds (5 attempts × 1+2+4+8 s backoff = 15 s + 60 s
/// cooldown) per cycle, and after `MAX_REENQUEUE_CYCLES = 10`
/// cycles (≈ 15 min total) a single `error!` log line fires — but
/// the `.dirty` sidecar still stays for "operator inspection" with
/// no CLI (`mntrs list .dirty`), no metric, and no mount-time
/// surface. The user has no way to know their write is silently
/// failing from the application layer.
///
/// Repro: build an S3 Operator pointed at port 1 (no listener) so
/// every `op.write()` returns connection-refused. Stage a file +
/// `.dirty` sidecar. Enqueue with `per_task_delay: ZERO` so the
/// worker processes it on the next tick. After the 5-attempt
/// in-process loop exhausts and the task is re-enqueued at
/// `cycle = 1` (60 s cooldown), the bug surface is observable:
/// `.dirty` still on disk, no surface to find it.
///
/// The user-perceived bug is identical at cycle 0 vs cycle 9 vs
/// cycle 10 — silent failure starts on the FIRST attempt. The
/// 15-minute wait for STUCK just means the user has even less
/// signal of a real problem. This test exercises the
/// first-cycle-failure surface, which is the actionable one.
#[test]
fn dirty_sidecar_lingers_with_no_surface_on_upload_failure() {
    use opendal::ErrorKind;

    // Port 1 = guaranteed not listening. Connection refused
    // returns in <100ms per attempt.
    let failing_op = Arc::new(
        Operator::new(
            S3::default()
                .endpoint("http://127.0.0.1:1")
                .region("us-east-1")
                .bucket("nobody")
                .access_key_id("AKIAFAKE")
                .secret_access_key("fake"),
        )
        .unwrap(),
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let inodes: Inodes = Arc::new(dashmap::DashMap::new());
    let disk_cache_index: Arc<dashmap::DashMap<_, _>> = Arc::new(dashmap::DashMap::new());
    let writeback_pending: Arc<dashmap::DashSet<_>> = Arc::new(dashmap::DashSet::new());
    let cache_dir = scratch_dir("dirty-no-surface");

    let (tx, handle) = rt.block_on(async {
        writeback::spawn(
            failing_op.clone(),
            inodes.clone(),
            disk_cache_index.clone(),
            cache_dir.clone(),
            writeback_pending.clone(),
            Duration::from_millis(100),
        )
    });

    // Stage cache file + .dirty sidecar (mimics flush/release).
    let write_buffer_path = cache_dir.join("stuck.bin");
    std::fs::write(&write_buffer_path, b"data destined to be stuck").unwrap();
    let dirty = write_buffer_path.with_extension("dirty");
    std::fs::write(&dirty, "stuck.bin").unwrap();
    assert!(dirty.exists(), "precondition: .dirty sidecar present");
    assert!(
        write_buffer_path.exists(),
        "precondition: cache file present"
    );

    // Mimic flush/release inserting into writeback_pending first
    // (so the worker's eventual failure path actually clears it).
    writeback_pending.insert("/remote/stuck.bin".to_string());

    // Fresh task: cycle=0, immediate delay. Worker will run
    // 5 attempts (15s of backoff) then re-enqueue at cycle=1.
    tx.send(WritebackTask {
        ino: 1,
        remote_path: "/remote/stuck.bin".to_string(),
        write_buffer_path: write_buffer_path.clone(),
        in_memory_payload: None,
        // Issue #T2-N: not Minimal; file should survive.
        delete_cache_on_success: false,
        retry_cycle: 0,
        per_task_delay: Duration::ZERO,
        buffer_size: 16 * 1024 * 1024,
    })
    .unwrap();

    // Wait for first 5-attempt loop to complete. Each attempt
    // fails on connection-refused in <100ms; backoff sleeps are
    // 1+2+4+8 = 15s. Plus a small fudge for runtime wakeup.
    std::thread::sleep(Duration::from_millis(20_000));

    // CORE BUG ASSERTION (1): after the 5-attempt loop exhausts,
    // .dirty sidecar is still on disk. The user has no way to
    // know the upload is failing — no CLI, no metric, no
    // mount-time surface. The .dirty just sits there until the
    // 60s cooldown elapses and the task is tried again.
    assert!(
        dirty.exists(),
        "BUG: after 5 failed upload attempts, .dirty sidecar is \
         still on disk with NO operator-visible surface (no \
         `mntrs list .dirty`, no metric, no mount-time warning). \
         The user's write is silently lost from the application layer."
    );
    assert!(
        write_buffer_path.exists(),
        "cache file should also persist (writeback leaves the file \
         for next-mount recovery — see lib.rs:1239)"
    );

    // CORE BUG ASSERTION (2): the task is re-enqueued at cycle=1
    // (60s cooldown), still in retry. writeback_pending has been
    // removed by the worker's re-enqueue path, but a fresh write
    // to the same path would re-insert and bounce. The
    // application-level user has no way to know any of this.
    //
    // We don't assert on writeback_pending here because the
    // worker removed it before re-enqueuing (issue #38). What we
    // do assert is that the file is STILL pending from a
    // user-visible standpoint: the .dirty is on disk, and
    // there's no surface to enumerate it.

    // Sanity: confirm the failing backend is actually rejecting
    // writes with a non-NotFound error kind, so the test is
    // really hitting the connection-refused path (not e.g. an
    // auth issue that would mask the bug).
    let kind_check = rt.block_on(async {
        failing_op
            .write("/remote/probe", b"x".as_slice())
            .await
            .map(|_| ())
    });
    let kind_summary = match &kind_check {
        Ok(()) => Ok(()),
        Err(e) => Err(e.kind()),
    };
    assert!(
        matches!(kind_summary, Err(k) if k != ErrorKind::NotFound),
        "failing op should reject with a connection error, not NotFound: got {:?}",
        kind_summary
    );

    // Cleanup
    drop(tx);
    handle.abort();
    rt.shutdown_timeout(Duration::from_millis(100));
}

/// Issue #T2-N: CacheMode::Minimal semantics — the worker unlinks
/// the on-disk cache file after a successful upload. Writes/Full
/// keep the file as a read cache; Off never wrote one to begin with.
///
/// This pins the worker branch at `src/writeback.rs:490`:
/// `if delete_cache_on_success && in_memory_payload.is_none() { unlink }`.
///
/// Pre-fix regression: a future refactor that drops the unlink (or
/// flips the predicate to `Writes`-style "keep") would silently
/// regress Minimal to Writes semantics — users would suddenly see
/// disk usage they didn't ask for. This test catches that.
#[test]
fn minimal_mode_unlinks_cache_file_after_successful_upload() {
    let h = Harness::new("minimal-unlinks");

    // Stage a fake cache file + .dirty sidecar (the worker
    // unlinks both, but the test cares about the cache file —
    // the .dirty lifecycle is already covered by other tests).
    let (write_buffer_path, dirty) = h.stage_file("minimal.bin", b"minimal payload");

    assert!(
        write_buffer_path.exists(),
        "precondition: cache file present on disk"
    );
    assert!(dirty.exists(), "precondition: .dirty sidecar present");

    // Build the task with delete_cache_on_success = true (the
    // enqueue path in lib.rs sets this from
    // `cache_mode.delete_cache_on_success()` — currently only
    // true for `CacheMode::Minimal`).
    let payload_bytes = bytes::Bytes::from_static(b"minimal payload");
    h.sender
        .send(WritebackTask {
            ino: 42,
            remote_path: "/remote/minimal.bin".to_string(),
            write_buffer_path: write_buffer_path.clone(),
            in_memory_payload: None,
            delete_cache_on_success: true,
            retry_cycle: 0,
            per_task_delay: Duration::ZERO,
            buffer_size: 16 * 1024 * 1024,
        })
        .unwrap();

    // Poll for completion — Minimal-mode cleanup should leave
    // the cache file GONE while the backend received the bytes.
    // Both transitions must be observed; if the .dirty disappeared
    // but the cache file remained, the unlink branch is broken.
    let started = std::time::Instant::now();
    let mut backend_received = false;
    let mut cache_unlinked = false;
    while started.elapsed() < Duration::from_secs(5) {
        if !dirty.exists() && !cache_unlinked {
            // .dirty removed: upload completed. The unlink
            // branch fires immediately after, on the same
            // task. Poll for the cache file removal with a
            // tight retry — in practice both are near-instant.
            let mut gone_at = false;
            let inner_start = std::time::Instant::now();
            while inner_start.elapsed() < Duration::from_millis(500) {
                if !write_buffer_path.exists() {
                    gone_at = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            cache_unlinked = gone_at;
        }
        if !cache_unlinked {
            std::thread::sleep(Duration::from_millis(20));
            continue;
        }
        // Cache file is gone — verify the backend has the bytes.
        let got =
            h.rt.block_on(async { h.op.read("/remote/minimal.bin").await.ok() });
        if let Some(buf) = got
            && buf.to_vec() == payload_bytes.to_vec()
        {
            backend_received = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    assert!(
        cache_unlinked,
        "Minimal mode must unlink the cache file after upload (still present at {})",
        write_buffer_path.display()
    );
    assert!(
        backend_received,
        "Minimal mode must still upload the bytes — backend never received /remote/minimal.bin"
    );
    // And the .dirty sidecar is gone too (existing cleanup, not
    // specific to Minimal — but assert for completeness).
    assert!(
        !dirty.exists(),
        ".dirty sidecar must be cleaned (existing worker contract)"
    );
}
