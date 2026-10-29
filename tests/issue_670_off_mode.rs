//! Regression test for issue #670 — off-mode writes corrupted the
//! remote object and read-after-write came back empty.
//!
//! The `hdfs-kerberos` CI job mounts with the default
//! `--vfs-cache-mode=off` and runs:
//!
//! ```text
//! echo "hello kerberos" > f   # step 3
//! cat f                      # step 4 — read back
//! echo "more" >> f           # step 5 — append
//! cat f                      # step 5 — verify
//! ```
//!
//! Pre-fix, step 4 could return an empty file (`read back FAIL:
//! got ''`) and step 5 uploaded `[NULs] + "more"` — the remote
//! object was silently corrupted. Both symptoms had the same root
//! cause: in off mode the bytes lived in a **per-handle** `Vec<u8>`
//! that was never seeded from the backend.
//!
//!   * a fresh handle started empty, so the off-mode write branch
//!     (`Vec::resize(end, 0)`) fabricated a zero-filled prefix for
//!     an append, and
//!   * a read through a *different* handle could not see the bytes
//!     until the async upload landed.
//!
//! Post-fix the buffer is shared per path, so an append continues
//! where the previous handle left off and a read sees the pending
//! bytes immediately.

use std::time::{Duration, Instant};

use mntrs::core_fs::CoreFilesystem;
use mntrs::util::CacheMode;
use opendal::Operator;
use opendal::services::Memory;

/// One-shot tracing init so the writeback worker's own logs are
/// visible when a test fails.
fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("mntrs=debug")),
        )
        .with_test_writer()
        .try_init();
}

fn make_fs(mode: CacheMode) -> (mntrs::MntrsFs, Operator, std::path::PathBuf) {
    init_tracing();
    let op = Operator::new(Memory::default()).unwrap();
    let cache_dir =
        std::env::temp_dir().join(format!("mntrs-issue670-{}-{:p}", std::process::id(), &op));
    let _ = std::fs::create_dir_all(&cache_dir);
    let fs = mntrs::new_test_fs_with_mode(op.clone(), cache_dir.clone(), mode);
    fs.init().expect("init (spawns writeback worker)");
    (fs, op, cache_dir)
}

/// Poll the backend until `path` reads back as `want` (or timeout).
fn wait_remote(op: &Operator, path: &str, want: &[u8]) -> Vec<u8> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let start = Instant::now();
    loop {
        let got = rt
            .block_on(async { op.read(path).await.ok() })
            .map(|b| b.to_vec())
            .unwrap_or_default();
        if got == want || start.elapsed() > Duration::from_secs(15) {
            return got;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn remote_now(op: &Operator, path: &str) -> Vec<u8> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { op.read(path).await.ok() })
        .map(|b| b.to_vec())
        .unwrap_or_default()
}

/// Render NUL bytes visibly so a corrupted payload is obvious in
/// the assertion output.
fn show(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| {
            if *b == 0 {
                "NUL".to_string()
            } else {
                (*b as char).to_string()
            }
        })
        .collect()
}

/// The CI's step 3: create the file and write 15 bytes.
fn write_initial(fs: &mntrs::MntrsFs, path: &str, body: &[u8]) -> u64 {
    let (attr, wfh) = CoreFilesystem::create(fs, 1, path, 0o644).expect("create");
    let n = CoreFilesystem::write(fs, attr.ino, wfh, 0, body).expect("write");
    assert_eq!(n as usize, body.len(), "short write");
    // The FUSE kernel sends FLUSH on close(2).
    CoreFilesystem::flush(fs, attr.ino, wfh).expect("flush");
    CoreFilesystem::release(fs, attr.ino, wfh).expect("release");
    attr.ino
}

/// The CI's step 5: `echo "more" >> f` — a *separate* open for
/// write, appending at the current end offset.
fn append(fs: &mntrs::MntrsFs, ino: u64, path: &str, offset: u64, body: &[u8]) {
    let wfh = CoreFilesystem::open(fs, ino, 1).expect("open for append");
    let n = CoreFilesystem::write(fs, ino, wfh, offset, body).expect("append write");
    assert_eq!(n as usize, body.len(), "short append write");
    CoreFilesystem::flush(fs, ino, wfh).expect("flush append");
    CoreFilesystem::release(fs, ino, wfh).expect("release append");
    let _ = path;
}

/// The CI's step 4 / step 5 verify: `cat f` through a fresh read
/// handle.
fn cat(fs: &mntrs::MntrsFs, ino: u64) -> Vec<u8> {
    let rfh = CoreFilesystem::open(fs, ino, 0).expect("open for read");
    let out = CoreFilesystem::read(fs, ino, rfh, 0, 4096).expect("read");
    CoreFilesystem::release(fs, ino, rfh).expect("release read");
    out
}

/// Issue #670: an append in off mode must not fabricate a
/// zero-filled prefix — the remote object has to end up with the
/// concatenated content.
#[test]
fn off_mode_append_does_not_zero_fill_the_prefix() {
    let (fs, op, _cache_dir) = make_fs(CacheMode::Off);
    let path = "small.txt";
    let initial = b"hello kerberos\n";
    let appended = b"more\n";

    let ino = write_initial(&fs, path, initial);

    // The append must continue where the first write stopped.
    append(&fs, ino, path, initial.len() as u64, appended);

    let mut expected = initial.to_vec();
    expected.extend_from_slice(appended);

    // The read-back (step 5's verify) must see both parts.
    let got = cat(&fs, ino);
    assert_eq!(
        show(&got),
        show(&expected),
        "read-back content mismatch (NUL bytes mean the off-mode \
         buffer fabricated a zero prefix)"
    );

    // And the remote object must carry the same bytes — the
    // writeback uploads the whole pending buffer.
    let remote = wait_remote(&op, path, &expected);
    assert_eq!(
        show(&remote),
        show(&expected),
        "remote content mismatch (NUL bytes mean the off-mode \
         buffer fabricated a zero prefix)"
    );
}

/// Issue #670: a read through a *different* handle must see the
/// bytes that have not been uploaded yet (read-after-write).
#[test]
fn off_mode_read_after_write_sees_pending_bytes() {
    let (fs, op, _cache_dir) = make_fs(CacheMode::Off);
    let path = "small.txt";
    let initial = b"hello kerberos\n";

    let ino = write_initial(&fs, path, initial);

    // No wait: the upload is still in flight. The read must be
    // served from the local pending buffer.
    let got = cat(&fs, ino);
    assert_eq!(
        show(&got),
        show(initial),
        "read-after-write returned the wrong content"
    );

    let remote = wait_remote(&op, path, initial);
    assert_eq!(show(&remote), show(initial), "remote content mismatch");
}

/// Issue #670: `flush()` used to clear the handle's `dirty` flag
/// even when it did not enqueue a writeback, so the following
/// `release()` skipped its own enqueue and the bytes never
/// reached the backend at all.
#[test]
fn off_mode_flush_then_release_still_uploads() {
    let (fs, op, _cache_dir) = make_fs(CacheMode::Off);
    let path = "flushed.txt";
    let body = b"flushed bytes\n";

    let ino = write_initial(&fs, path, body);

    let remote = wait_remote(&op, path, body);
    assert_eq!(
        show(&remote),
        show(body),
        "the writeback never reached the backend"
    );
    let _ = ino;
}

/// Issue #670: the same shape without an explicit flush — the
/// `release()` enqueue is the only path that hands the bytes to
/// the worker.
#[test]
fn off_mode_release_without_flush_uploads() {
    let (fs, op, _cache_dir) = make_fs(CacheMode::Off);
    let path = "released.txt";
    let body = b"released bytes\n";

    let (attr, wfh) = CoreFilesystem::create(&fs, 1, path, 0o644).expect("create");
    CoreFilesystem::write(&fs, attr.ino, wfh, 0, body).expect("write");
    CoreFilesystem::release(&fs, attr.ino, wfh).expect("release");

    let remote = wait_remote(&op, path, body);
    assert_eq!(
        show(&remote),
        show(body),
        "the writeback never reached the backend"
    );
}

/// Issue #670: the CI's step 6 — appending to a file that already
/// exists on the backend (`echo "appended" >> pre-existing`).
/// The pre-existing bytes must survive: the write-open seeds the
/// pending buffer from the backend, so the append continues after
/// them instead of replacing them with NUL bytes.
#[test]
fn off_mode_append_to_existing_backend_object() {
    let (fs, op, _cache_dir) = make_fs(CacheMode::Off);
    let path = "pre-existing.txt";
    let initial = b"hello kerberos hdfs\n";
    let appended = b"appended\n";

    // Seed the object directly on the backend — the CI does the
    // same with `hdfs dfs -put` before the mount starts.
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { op.write(path, initial.to_vec()).await })
        .expect("seed the backend object");

    // The mount learns about the object on the first lookup (the
    // CI's step 2 `cat` does exactly this).
    let attr = CoreFilesystem::lookup(&fs, 1, path).expect("lookup");
    let ino = attr.ino;
    assert_eq!(
        attr.size,
        initial.len() as u64,
        "lookup should report the backend size"
    );

    append(&fs, ino, path, initial.len() as u64, appended);

    let mut expected = initial.to_vec();
    expected.extend_from_slice(appended);

    let got = cat(&fs, ino);
    assert_eq!(
        show(&got),
        show(&expected),
        "read-back content mismatch (NUL bytes mean the off-mode \
         buffer fabricated a zero prefix)"
    );

    let remote = wait_remote(&op, path, &expected);
    assert_eq!(
        show(&remote),
        show(&expected),
        "remote content mismatch — the pre-existing bytes were lost"
    );
}

/// Sanity check: the disk-backed mode keeps its old behaviour —
/// the append is served from the cache file, not from a
/// zero-filled buffer.
#[test]
fn writes_mode_append_is_unaffected() {
    let (fs, op, _cache_dir) = make_fs(CacheMode::Writes);
    let path = "small.txt";
    let initial = b"hello kerberos\n";
    let appended = b"more\n";

    let ino = write_initial(&fs, path, initial);
    append(&fs, ino, path, initial.len() as u64, appended);

    let mut expected = initial.to_vec();
    expected.extend_from_slice(appended);

    let remote = wait_remote(&op, path, &expected);
    assert_eq!(
        show(&remote),
        show(&expected),
        "remote content mismatch in writes mode"
    );
    let _ = remote_now(&op, path);
}
