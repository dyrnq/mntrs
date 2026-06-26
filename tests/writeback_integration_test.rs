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

use mntrs::{FileType, InodeEntry, Inodes, writeback};
use opendal::Operator;
use opendal::services::Memory;

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

        let op = Arc::new(Operator::new(Memory::default()).unwrap().finish());
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
        let cache_path = self.cache_dir.join(name);
        std::fs::write(&cache_path, content).unwrap();
        let dirty = cache_path.with_extension("dirty");
        std::fs::write(&dirty, name).unwrap();
        (cache_path, dirty)
    }

    /// Enqueue a fresh upload (cycle=0) with the given per-task
    /// delay. Pass `Duration::ZERO` to test the immediate-upload
    /// path (issue #202); pass the harness default (100 ms) to
    /// test the uniform-delay fallback path.
    fn enqueue(&self, ino: u64, remote: &str, cache_path: PathBuf, delay: Duration) {
        self.sender
            .send((ino, remote.to_string(), cache_path, 0, delay))
            .unwrap();
    }

    /// Convenience: enqueue with the harness's spawn-time delay
    /// (the pre-#202 uniform behavior). Preserves the call shape
    /// of the 7 tests added in PR #216 — they all use the uniform
    /// delay to keep the timing assumptions simple.
    fn enqueue_default(&self, ino: u64, remote: &str, cache_path: PathBuf) {
        let delay = self.spawn_delay;
        self.enqueue(ino, remote, cache_path, delay);
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
    let (cache_path, dirty) = h.stage_file("a.bin", b"hello writeback");

    // Track in inodes so the upload updates mtime.
    h.inodes.insert(
        100,
        InodeEntry {
            path: "/remote/a.bin".to_string(),
            kind: FileType::RegularFile,
            size: 5,
            mtime: None,
        },
    );

    h.enqueue_default(100, "/remote/a.bin", cache_path);

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
    let (cache_path, dirty) = h.stage_file("gone.bin", b"data");

    // Remove the cache file BEFORE upload fires (simulates LRU eviction)
    std::fs::remove_file(&cache_path).unwrap();
    // dirty sidecar must still be present (orphan)
    assert!(dirty.exists());

    h.enqueue_default(42, "/remote/gone.bin", cache_path);

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
    let (cache_path, dirty) = h.stage_file("recovery.bin", b"recovered data");

    // INO_RECOVERY_SENTINEL = 0 — no inodes entry to update.
    h.enqueue_default(mntrs::INO_RECOVERY_SENTINEL, "/recovery/path", cache_path);

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
    let (cache_path, dirty) = h.stage_file("empty.bin", b"");

    h.enqueue_default(7, "/remote/empty.bin", cache_path);

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
        let (cache_path, dirty) = h.stage_file(&name, content.as_bytes());
        h.enqueue_default((i + 1) as u64, &format!("/remote/{name}"), cache_path);
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
    let (cache_path, dirty) = h.stage_file("pending.bin", b"x");

    // Mimic flush/release inserting into writeback_pending
    h.writeback_pending
        .insert("/remote/pending.bin".to_string());

    h.enqueue_default(50, "/remote/pending.bin", cache_path);

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
    let (cache_path, dirty) = h.stage_file("small.bin", b"tiny payload");

    h.sender
        .send((
            1,
            "/remote/small.bin".to_string(),
            cache_path,
            0,
            Duration::ZERO,
        ))
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
    let (cache_path, dirty) = h.stage_file("large.bin", b"big payload");

    h.sender
        .send((
            2,
            "/remote/large.bin".to_string(),
            cache_path,
            0,
            Duration::from_secs(5),
        ))
        .unwrap();

    // 5s delay means .dirty survives at least 1s.
    assert!(
        !h.wait_drain(&dirty, 1_000),
        "5s-delay upload still pending at 1s (issue #202 non-immediate path)"
    );
}
