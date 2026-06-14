#![allow(unexpected_cfgs)]
#![cfg_attr(windows, allow(dead_code, unused_imports, unused_variables))]
#![recursion_limit = "256"]
pub mod cache;
pub mod cmd;
pub mod core_fs;
pub mod http_client;
pub mod path;
pub mod prefetcher;
pub mod writeback;

/// Shared inode table type for writeback callback.
pub const CACHE_BLOCK_SIZE: u64 = 8 * 1024 * 1024;
pub type Inodes = Arc<dashmap::DashMap<u64, (String, FileType, u64, Option<SystemTime>)>>;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

// `MemCache` trait is in scope via the `pub mem_cache:
// Arc<dyn MemCache>` field declaration below; no explicit
// `use` needed because the call sites use method syntax
// (`.get(...)`, `.put(...)`, etc.) which is dispatched
// dynamically through the trait object.

#[cfg(unix)]
use fuser::{FileAttr, FileType, INodeNo};

#[cfg(not(unix))]
/// Stub type for non-Unix platforms — mirrors fuser::FileType variants used in shared state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Directory,
    RegularFile,
    Symlink,
    NamedPipe,
    BlockDevice,
    CharDevice,
    Socket,
}

#[cfg(not(unix))]
/// Stub — mirrors fuser::INodeNo. Needed because `make_attr` is used in CoreFilesystem impl.
#[cfg(not(unix))]
#[derive(Debug, Clone, Copy)]
pub struct INodeNo(pub u64);
#[cfg(not(unix))]
impl From<u64> for INodeNo {
    fn from(v: u64) -> Self {
        INodeNo(v)
    }
}
#[cfg(not(unix))]
impl From<INodeNo> for u64 {
    fn from(v: INodeNo) -> u64 {
        v.0
    }
}

#[cfg(not(unix))]
/// Stub — mirrors fuser::FileAttr. Needed because `make_attr` is used in CoreFilesystem impl.
#[derive(Debug, Clone)]
pub struct FileAttr {
    pub ino: INodeNo,
    pub size: u64,
    pub blocks: u64,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub crtime: SystemTime,
    pub kind: FileType,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
    pub flags: u32,
}
use futures::StreamExt;
use opendal::{EntryMode, Operator};

fn rt() -> &'static tokio::runtime::Runtime {
    static RT: once_cell::sync::OnceCell<tokio::runtime::Runtime> =
        once_cell::sync::OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("tokio rt")
    })
}

// TTL now comes from MntrsFs.attr_ttl field
static NEXT_INO: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(2);
static NEXT_HANDLE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
// DIR_CACHE_TTL now comes from MntrsFs.dir_cache_ttl field

/// Per-open-file-handle state
#[derive(Debug)]
enum FileHandleState {
    Read {
        path: String,
        last_offset: u64,
        chunk_size: u64,
        prefetcher: Option<std::sync::Arc<prefetcher::HandlePrefetcher>>,
    },
    Write {
        path: String,
        cache_fd: Option<Arc<std::sync::Mutex<std::fs::File>>>,
        dirty: bool,
        dirty_since: Option<std::time::Instant>,
    },
}

impl Clone for FileHandleState {
    fn clone(&self) -> Self {
        match self {
            FileHandleState::Read {
                path,
                last_offset,
                chunk_size,
                prefetcher,
            } => FileHandleState::Read {
                path: path.clone(),
                last_offset: *last_offset,
                chunk_size: *chunk_size,
                prefetcher: prefetcher.clone(),
            },
            FileHandleState::Write {
                path,
                cache_fd,
                dirty,
                dirty_since,
            } => FileHandleState::Write {
                path: path.clone(),
                cache_fd: cache_fd.clone(),
                dirty: *dirty,
                dirty_since: *dirty_since,
            },
        }
    }
}

impl FileHandleState {
    fn path(&self) -> &str {
        match self {
            FileHandleState::Read { path, .. } => path,
            FileHandleState::Write { path, .. } => path,
        }
    }
}

#[allow(clippy::type_complexity)]
#[allow(dead_code)]
pub struct MntrsFs {
    /// Underlying OpenDAL operator. Exposed `pub` so the integration
    /// tests in `tests/` can seed fixtures (write initial files,
    /// verify backend state) without going through the FUSE layer.
    /// Production code paths use the helper methods.
    pub op: Arc<Operator>,
    /// Per-inode metadata. Exposed `pub` so the integration tests
    /// in `tests/bug_regression_test.rs` can simulate a `BATCHFORGET`
    /// by removing the ino entry, then re-lookup to verify the new
    /// ino is self-consistent with the cache-file state (Bug F fix
    /// — `CoreFilesystem::lookup` / `getattr` now consider the
    /// local cache file's size, not just the backend).
    pub inodes: dashmap::DashMap<u64, (String, FileType, u64, Option<std::time::SystemTime>)>,
    dir_cache: dashmap::DashMap<
        String,
        (
            std::time::Instant,
            dashmap::DashMap<String, (EntryMode, u64, std::time::SystemTime)>,
        ),
    >,
    /// Local on-disk cache directory. `pub` so integration tests
    /// can construct / inspect cache-file paths (e.g. for the Bug F
    /// regression test that simulates a pending writeback).
    pub cache_dir: PathBuf,
    handles: dashmap::DashMap<u64, FileHandleState>,
    pub(crate) dir_cache_ttl: Duration,
    pub(crate) attr_ttl: Duration,
    pub(crate) stat_cache_ttl: Duration,
    pub(crate) volname: String,
    pub(crate) cache_max_size: u64,
    pub(crate) write_back_delay: Duration,
    pub(crate) cache_mode: String,
    pub(crate) read_ahead: u64,
    /// Minimum file size (bytes) for which the read-path prefetcher
    /// is activated on open(). 0 disables prefetching entirely.
    /// Default: 64 MiB. See `maybe_create_prefetcher` for the
    /// activation logic and issue #16 for the cat-100M motivation.
    pub(crate) prefetch_threshold: u64,
    /// Upper bound (MiB) on the prefetch in-memory PartQueue.
    /// Caps the cost of a file that's opened but only partially
    /// read. Default: 64 MiB.
    pub(crate) prefetch_queue_mb: u64,
    pub(crate) read_chunk_size: u64,
    pub(crate) read_chunk_size_limit: u64,
    pub(crate) read_chunk_streams: u32,
    pub(crate) uid: Option<u32>,
    pub(crate) gid: Option<u32>,
    pub(crate) umask: Option<u32>,
    pub(crate) dir_perms: u16,
    pub(crate) file_perms: u16,
    pub(crate) link_perms: u16,
    pub(crate) direct_io: bool,
    pub(crate) poll_interval: Duration,
    pub(crate) cache_max_age: Duration,
    pub(crate) cache_min_free_space: u64,
    pub(crate) exclude_patterns: Vec<String>,
    pub(crate) include_patterns: Vec<String>,
    pub(crate) max_size: Option<u64>,
    pub(crate) min_size: Option<u64>,
    pub(crate) max_depth: Option<usize>,
    pub(crate) ignore_case: bool,
    pub(crate) fast_fingerprint: bool,
    pub(crate) async_read: bool,
    pub(crate) vfs_refresh: bool,
    pub(crate) case_insensitive: bool,
    pub(crate) no_implicit_dir: bool,
    pub(crate) use_server_modtime: bool,
    pub(crate) no_apple_double: bool,
    pub(crate) no_apple_xattr: bool,
    pub(crate) hash_filter: Option<(usize, usize)>,
    pub(crate) block_norm_dupes: bool,
    pub(crate) write_wait: Duration,
    pub(crate) read_wait: Duration,
    pub(crate) handle_caching: Duration,
    pub(crate) cache_poll_interval: Duration,
    pub(crate) disk_total_size: u64,
    writeback_sender: std::sync::OnceLock<writeback::Sender>,

    /// Per-(inode, block) in-memory read cache. Held as a
    /// `dyn MemCache` trait object so the underlying
    /// implementation can be swapped (DashMap today, moka
    /// behind a flag) without touching the read/write call
    /// sites. All impls are `Send + Sync` (the trait bound),
    /// so the `Arc<dyn MemCache>` is safe to share across the
    /// FUSE worker threads + the metrics logger thread + the
    /// writeback task.
    pub mem_cache: std::sync::Arc<dyn crate::cache::MemCache>,
    attr_cache: dashmap::DashMap<
        String,
        (
            FileType,
            u64,
            Option<std::time::SystemTime>,
            std::time::Instant,
        ),
    >,
    #[allow(clippy::type_complexity)]
    disk_cache_index: dashmap::DashMap<String, (u64, std::time::SystemTime)>,
    out_of_space: std::sync::atomic::AtomicBool,
    pub(crate) storage_class: Option<String>,
}

/// Convert an opendal::Error to std::io::Error, preserving the kind so
/// FUSE callers (via `io_err_to_fuse_errno`) get the right POSIX errno.
///
/// Without this, every backend failure collapsed to
/// `ErrorKind::Other` → `Errno::EIO`, which broke POSIX semantics
/// (unlink on missing file, rmdir on non-empty dir, etc.).
///
/// `pub` so the integration tests in `tests/bug_regression_test.rs`
/// can verify the mapping directly without going through the FUSE
/// adapter (Bug D fix). The function is otherwise an internal
/// helper used by the CoreFilesystem impls.
pub fn opendal_to_io_error(e: &opendal::Error, op: &str) -> std::io::Error {
    use opendal::ErrorKind;
    use std::io::ErrorKind as IoKind;
    let kind = match e.kind() {
        ErrorKind::NotFound => IoKind::NotFound,
        ErrorKind::AlreadyExists => IoKind::AlreadyExists,
        ErrorKind::PermissionDenied => IoKind::PermissionDenied,
        ErrorKind::IsADirectory => IoKind::IsADirectory,
        ErrorKind::NotADirectory => IoKind::NotADirectory,
        ErrorKind::Unsupported => IoKind::Unsupported,
        _ => IoKind::Other,
    };
    std::io::Error::new(kind, format!("{op} failed: {e}"))
}

/// Convert OpenDAL Timestamp to std::time::SystemTime, clamped to UNIX_EPOCH.
fn opendal_timestamp_to_system_time(ts: impl Into<std::time::SystemTime>) -> std::time::SystemTime {
    let st: std::time::SystemTime = ts.into();
    if st < std::time::UNIX_EPOCH {
        std::time::UNIX_EPOCH
    } else {
        st
    }
}
impl MntrsFs {
    /// Create a background prefetcher for a file handle, or `None` if
    /// the file is below `prefetch_threshold` or prefetching is
    /// disabled. The prefetcher streams chunks into a bounded
    /// PartQueue; the read-path pops them, so the FUSE `read()` for
    /// sequential-from-start workloads (cat, dd, head -c large) lands
    /// on already-fetched data instead of issuing 1 RTT per chunk.
    ///
    /// Previously gated on `read_chunk_streams > 1`, which made
    /// prefetching unreachable for default configs (`read_chunk_streams`
    /// defaults to 1, the serial-fetch path). The new gate is
    /// `file_size >= prefetch_threshold`, default 64 MiB. Issue #16
    /// (`cat 100M` 6.35× slower than rclone) was the motivation; the
    /// existing 16 MiB chunk cap (commit fc5e974) still protects
    /// `head -c1K` from over-fetch.
    ///
    /// Cancellation: the spawned downloader thread exits when
    /// `release()` drops the handle and calls `HandlePrefetcher::cancel()`.
    /// Without cancel, the thread would spin on a full queue forever
    /// for partially-read files.
    fn maybe_create_prefetcher(
        &self,
        ino: u64,
        path: &str,
    ) -> Option<std::sync::Arc<prefetcher::HandlePrefetcher>> {
        let file_size = self.resolve(ino).map(|(_, _, s, _)| s).unwrap_or(0);
        if self.prefetch_threshold == 0 || file_size < self.prefetch_threshold {
            return None;
        }
        // chunk_size cap matches the read-path hard cap (16 MiB) so
        // prefetched parts align with the mem_cache block size (8 MiB).
        let chunk = self.read_chunk_size.clamp(131072, 16 * 1024 * 1024);
        let max_queue = self.prefetch_queue_mb.max(1) * 1024 * 1024;
        Some(std::sync::Arc::new(prefetcher::HandlePrefetcher::new(
            self.op.as_ref().clone(),
            path.to_string(),
            file_size,
            max_queue,
            chunk,
        )))
    }

    fn make_attr(&self, ino: u64, size: u64, kind: FileType, mtime: SystemTime) -> FileAttr {
        let base_perm = if kind == FileType::Directory {
            self.dir_perms
        } else {
            self.file_perms
        };
        let perm = match self.umask {
            Some(m) => base_perm & !(m as u16),
            None => base_perm,
        };
        let uid = self.uid.unwrap_or(1000);
        let gid = self.gid.unwrap_or(1000);
        FileAttr {
            ino: INodeNo(ino),
            size,
            blocks: size.div_ceil(4096),
            atime: mtime,
            mtime,
            ctime: mtime,
            crtime: mtime,
            kind,
            perm,
            nlink: if kind == FileType::Directory { 2 } else { 1 },
            uid,
            gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }
}

pub fn path_hash(path: &str) -> u64 {
    let mut h: u64 = 0x811c9dc5;
    for b in path.bytes() {
        h = h.wrapping_mul(0x01000193) ^ b as u64;
    }
    (h & 0x7FFFFFFFFFFFFFFF).max(2)
}

pub fn fnmatch(pattern: &str, name: &str, ignore_case: bool) -> bool {
    let (p, n): (Vec<char>, Vec<char>) = if ignore_case {
        (
            pattern.to_lowercase().chars().collect(),
            name.to_lowercase().chars().collect(),
        )
    } else {
        (pattern.chars().collect(), name.chars().collect())
    };
    let (pl, nl) = (p.len(), n.len());
    let mut pi = 0;
    let mut ni = 0;
    let mut star = None;
    let mut match_start = 0;
    while ni < nl {
        if pi < pl && p[pi] == '*' {
            star = Some(pi);
            match_start = ni;
            pi += 1;
        } else if pi < pl && (p[pi] == '?' || p[pi] == n[ni]) {
            pi += 1;
            ni += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            match_start += 1;
            ni = match_start;
        } else {
            return false;
        }
    }
    while pi < pl && p[pi] == '*' {
        pi += 1;
    }
    pi == pl
}

pub fn cache_path(cache_dir: &Path, path: &str) -> PathBuf {
    cache_path_block(cache_dir, path, 0)
}

/// Block-level cache path. block_index=0 means whole file (backward compatible).
pub fn cache_path_block(cache_dir: &Path, path: &str, block_index: u64) -> PathBuf {
    let base = format!("{:020x}", path_hash(path));
    if block_index == 0 {
        cache_dir.join(&base)
    } else {
        cache_dir.join(format!("{}_{:04x}", base, block_index))
    }
}

/// Cache file path for a specific block. Encodes block_idx for restart recovery.
pub fn cache_block_path(cache_dir: &Path, path: &str, block_idx: u64) -> PathBuf {
    cache_dir.join(format!("{:020x}_{:010x}.block", path_hash(path), block_idx))
}

/// Remove block-level cache entries for a path. O(K) where K is
/// the inodes.size() / CACHE_BLOCK_SIZE — direct `remove_file` per
/// block, no `read_dir` over the whole cache dir.
///
/// Replaces the previous `read_dir(&cache_dir).filter(starts_with(prefix))`
/// pattern, which was O(N) over the entire cache (N entries in
/// the dir, mostly unrelated to this ino). On a CSI mount with
/// 1000+ cached files, the old scan was ~4ms per unlink/rename/
/// setattr/rmdir — a 5× slowdown vs rclone on a single unlink
/// (issue #17's remaining gap).
///
/// Stale block files (the inodes entry was removed but the block
/// file on disk was missed) are tolerated: `remove_file` returns
/// an error and we silently ignore it. A future `cache_index`
/// rebuild at startup will surface any genuine orphans.
pub(crate) fn remove_block_cache_files(cache_dir: &Path, full_path: &str, size: u64) {
    let n_blocks = size.div_ceil(CACHE_BLOCK_SIZE);
    for blk in 0..n_blocks {
        let bpath = cache_block_path(cache_dir, full_path, blk);
        let _ = std::fs::remove_file(&bpath);
    }
}

/// Scan cache dir for block files and rebuild disk_cache_index.
/// Loaded at startup so cache is warm across restarts.
pub fn load_cache_index(cache_dir: &Path) -> Vec<(String, u64, u64, std::time::SystemTime)> {
    let mut entries = Vec::new();
    let Ok(dir) = std::fs::read_dir(cache_dir) else {
        return entries;
    };
    for entry in dir.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        // Parse "hash_blockidx.block" format
        if let Some(rest) = name.strip_suffix(".block")
            && let Some(block_str) = rest.split('_').nth(1)
            && let Ok(block_idx) = u64::from_str_radix(block_str, 16)
            && let Ok(meta) = entry.metadata()
            && let Ok(mtime) = meta.modified()
        {
            entries.push((name, block_idx, meta.len(), mtime));
        }
    }
    entries
}

impl MntrsFs {
    fn resolve(&self, ino: u64) -> Option<(String, FileType, u64, Option<std::time::SystemTime>)> {
        self.inodes.get(&ino).map(|r| r.clone())
    }

    /// Background thread that periodically clears stale directory cache entries.
    pub fn start_cache_poller(&self) {
        let dir_cache = self.dir_cache.clone();
        let dir_cache_ttl = self.dir_cache_ttl;
        let interval = self.cache_poll_interval;
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(interval);
                let now = std::time::Instant::now();
                dir_cache.retain(|_k, (t, _v)| now.duration_since(*t) < dir_cache_ttl);
            }
        });
    }

    /// Recover writeback queue + spawn worker. Shared by fuser + CoreFilesystem init.
    fn common_init_wb(&self) {
        self.alloc_ino("", FileType::Directory, 4096);

        // Spawn writeback worker FIRST so the sender is available
        // for the recovery scan below. Previously the scan ran before
        // spawn, so writeback_sender.get() always returned None and
        // recovery tasks were silently dropped while .dirty sidecars
        // were deleted — causing permanent data loss on crash restart.
        crate::rt();
        let op = self.op.clone();
        let delay = self.write_back_delay;
        let inodes = Arc::new(self.inodes.clone());
        let (tx, _handle) = crate::writeback::spawn(op, inodes, delay);
        self.writeback_sender.set(tx).ok();

        // Recover writeback queue from dirty sidecars.
        // Do NOT delete .dirty here — the upload completion handler
        // (writeback.rs) removes it after a successful upload.
        // Deleting before upload completes would cause data loss if
        // the process crashes again before the upload finishes.
        if let Ok(entries) = std::fs::read_dir(&self.cache_dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.extension().is_some_and(|ext| ext == "dirty") {
                    let cache_path = p.with_extension("");
                    if !cache_path.exists() {
                        // Orphan sidecar — cache file missing, safe to remove
                        tracing::debug!(sidecar=?p, "removing orphan dirty sidecar");
                        let _ = std::fs::remove_file(&p);
                        continue;
                    }
                    if let Ok(remote) = std::fs::read_to_string(&p) {
                        let remote = remote.trim().to_string();
                        if let Some(tx) = self.writeback_sender.get() {
                            tracing::info!(path=%remote, ?cache_path, "recovering dirty writeback");
                            // ino=0: inode mapping is not populated at this
                            // point; the mtime update in the upload completion
                            // handler will be a no-op.  Acceptable — the next
                            // stat() will refresh mtime from the remote.
                            tx.send((0, remote, cache_path)).ok();
                        }
                    }
                }
            }
        }
    }

    fn alloc_ino(&self, path: &str, kind: FileType, size: u64) -> u64 {
        let ino = NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inodes
            .entry(ino)
            .and_modify(|v| v.2 = size)
            .or_insert((path.to_string(), kind, size, None));
        ino
    }

    /// Same as `alloc_ino` but seeds the inodes entry's mtime slot
    /// with the given timestamp. Used by mkdir/create so that
    /// `getattr` can fall back to it when `stat_op` returns None
    /// (Bug C — see `CoreFilesystem::getattr`). The 4-tuple's mtime
    /// was always `None` before this helper; we still keep the
    /// 3-arg `alloc_ino` for callers that don't have a meaningful
    /// mtime at hand (e.g. internal re-lookups).
    fn alloc_ino_with_mtime(
        &self,
        path: &str,
        kind: FileType,
        size: u64,
        mtime: SystemTime,
    ) -> u64 {
        let ino = NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inodes
            .entry(ino)
            .and_modify(|v| v.2 = size)
            .or_insert((path.to_string(), kind, size, Some(mtime)));
        ino
    }

    /// Look up the ino currently registered for `path` (linear scan — the
    /// inodes map is small, typically O(open-files) plus cached lookups).
    ///
    /// Needed because `inodes` is keyed by the `NEXT_INO` counter that
    /// `alloc_ino` mints, *not* by `path_hash`. Operations that receive a
    /// full path (mkdir/rmdir/unlink) and need to remove the ino entry
    /// must look up the counter by path before calling `inodes.remove`.
    /// Using `path_hash(&path)` here — as the rename pre-fix code did —
    /// is a silent no-op: the FUSE kernel then keeps using the stale
    /// ino for subsequent operations on the same path, and a recreate
    /// at the same path collides with the lingering entry.
    ///
    /// `pub(crate)` so integration tests in `tests/` can verify the
    /// rename/rmdir/unlink leak fix.
    pub(crate) fn find_ino_by_path(&self, path: &str) -> Option<u64> {
        for entry in self.inodes.iter() {
            if entry.value().0 == path {
                return Some(*entry.key());
            }
        }
        None
    }

    /// Recursively create `full_path` (and any missing parents) on the
    /// backend. Returns Ok(()) when every level either was created or
    /// already existed; propagates only *non-recoverable* errors
    /// (network/auth/permission).
    ///
    /// Error policy (per backend quirks surfaced in the e2e tests):
    ///
    ///   * `Unsupported` — some backends (e.g. flat-namespace stores)
    ///     do not implement `create_dir` because directories are
    ///     implicit. Treat as success: the dir is "known" by virtue
    ///     of objects living under it.
    ///   * `AlreadyExists` — idempotent. mkdir -p on an existing
    ///     tree must not fail.
    ///   * `NotFound` for an *intermediate* — only happens if the
    ///     backend has no implicit-dir semantics. We surface it as
    ///     an error so the caller (mkdir) can decide what to do.
    ///   * Anything else — propagate.
    fn mkdir_chain(&self, full_path: &str) -> std::io::Result<()> {
        // Collect every dir level we need to ensure exists, leaf last.
        // For full_path = "a/b/c" we walk up: ["a/b/c/", "a/b/", "a/"].
        // Reversed: ["a/", "a/b/", "a/b/c/"].
        let mut chain: Vec<String> = Vec::new();
        let mut cur = full_path.trim_end_matches('/').to_string();
        while !cur.is_empty() {
            chain.push(format!("{}/", cur));
            match cur.rfind('/') {
                Some(pos) => cur.truncate(pos),
                None => cur.clear(),
            }
        }
        chain.reverse();

        let op = self.op.clone();
        rt().block_on(async move {
            // Try just the leaf first. On S3/GCS/OSS/etc. (flat-namespace
            // with implicit dirs) this is 1 round-trip and the
            // intermediate "a/", "a/b/" don't need to exist as actual
            // objects — they're "common prefixes" surfaced by list
            // operations. The pre-fix code did 3 sequential PUTs for a
            // 3-level path, which is what made `mkdir` 2-3× slower than
            // rclone in the bench (issue #17).
            let leaf = chain.last().expect("chain built from non-empty path");
            match op.create_dir(leaf).await {
                Ok(()) => return Ok(()),
                Err(e)
                    if e.kind() == opendal::ErrorKind::Unsupported
                        || e.kind() == opendal::ErrorKind::AlreadyExists =>
                {
                    return Ok(());
                }
                Err(e) if e.kind() == opendal::ErrorKind::NotFound => {
                    // Leaf create_dir returned NotFound — almost
                    // certainly because an intermediate is missing on a
                    // hierarchical-namespace backend (HDFS, WebHDFS).
                    // Fall through to the full chain.
                    tracing::debug!(path = %leaf,
                        "leaf create_dir returned NotFound; \
                         falling back to full mkdir_chain");
                }
                Err(e) => {
                    // Other error on the leaf (e.g. auth, 5xx). Don't
                    // try the chain — the chain would likely fail the
                    // same way, and the additional 2 PUTs would
                    // amplify the failure cost.
                    return Err(std::io::Error::other(format!(
                        "create_dir({leaf}) failed: {e}"
                    )));
                }
            }

            // Full chain (hierarchical-namespace fallback). The 3 PUTs
            // are issued concurrently so wall-clock latency is 1
            // round-trip (not 3). We can do this because the 3 levels
            // are independent — no level depends on another's success
            // for its own request to be well-formed.
            let futs = chain.iter().map(|p| op.create_dir(p));
            let results = futures::future::join_all(futs).await;
            for (p, r) in chain.iter().zip(results) {
                match r {
                    Ok(()) => {}
                    Err(e) if e.kind() == opendal::ErrorKind::Unsupported => {
                        tracing::debug!(path = %p,
                            "backend does not support create_dir; treating as implicit dir");
                    }
                    Err(e) if e.kind() == opendal::ErrorKind::AlreadyExists => {
                        // Idempotent — the dir is already there.
                    }
                    Err(e) => {
                        return Err(std::io::Error::other(format!(
                            "create_dir({p}) failed: {e}"
                        )));
                    }
                }
            }
            Ok(())
        })
    }

    fn stat_op(&self, path: &str) -> Option<(FileType, u64, Option<SystemTime>)> {
        // Check attr cache first
        if let Some(entry) = self.attr_cache.get(path) {
            let (kind, size, mtime, ts) = entry.value();
            if ts.elapsed() < self.stat_cache_ttl {
                return Some((*kind, *size, *mtime));
            }
        }
        let result = rt().block_on(async {
            let op = self.op.clone();
            let p = path.to_string();
            match op.stat(&p).await {
                Ok(meta) => {
                    let kind = match meta.mode() {
                        EntryMode::DIR => FileType::Directory,
                        _ => FileType::RegularFile,
                    };
                    let mtime = if self.use_server_modtime {
                        meta.last_modified().map(opendal_timestamp_to_system_time)
                    } else {
                        None
                    };
                    Some((kind, meta.content_length(), mtime))
                }
                Err(_) => {
                    if self.no_implicit_dir {
                        return None;
                    }
                    let op2 = self.op.clone();
                    let p2 = format!("{}/", path.trim_end_matches('/'));
                    if let Ok(mut l) = op2.lister(&p2).await
                        && l.next().await.is_some()
                    {
                        return Some((FileType::Directory, 4096, None));
                    }
                    None
                }
            }
        });
        if let Some((kind, size, mtime)) = result {
            self.attr_cache.insert(
                path.to_string(),
                (kind, size, mtime, std::time::Instant::now()),
            );
        }
        result
    }

    fn list_op(
        &self,
        path: &str,
    ) -> Result<Vec<(String, EntryMode, u64, SystemTime)>, opendal::Error> {
        {
            if let Some(entry) = self.dir_cache.get(path) {
                let (t, entries) = entry.value();
                let age = t.elapsed();
                if age < self.dir_cache_ttl {
                    return Ok(entries
                        .iter()
                        .map(|r| {
                            let (name, (mode, size, mtime)) = r.pair();
                            (name.clone(), *mode, *size, *mtime)
                        })
                        .collect());
                }
                // Cache expired — drop and re-read from remote
                drop(entry);
                self.dir_cache.remove(path);
            }
        }
        let depth = path.matches('/').count();
        // Per SESSION_PITFALLS §2.6: never swallow backend errors. A lister
        // init failure (auth, permission, network reset) used to be
        // silently dropped via .ok()?/.unwrap_or_default(), which made
        // mntrs return an empty FUSE directory on every backend problem
        // — debugging required guessing the root cause. Now we propagate
        // the error so the FUSE reply carries EIO/ENOENT and the
        // tracing pipeline (RUST_LOG + MNTRS_DAEMON_LOG) records the
        // opendal error verbatim.
        //
        // Bug B follow-up: the *one* exception is `NotFound`, which on
        // most backends means "the dir exists in our model but the
        // backend has no record of it" (e.g. an empty dir on S3, or
        // a just-mkdir'd dir on memory before any child was written).
        // For implicit-dir semantics (the default, matching rclone
        // VFS), an empty listing is the right answer. We still return
        // a cached empty entry so subsequent readdirs don't pay the
        // backend round-trip cost.
        let mut result = rt().block_on(async {
            let op = self.op.clone();
            let p = path.to_string();
            // Bug B follow-up: if the lister init returns NotFound,
            // treat it as "this dir exists in our model but has no
            // entries on the backend right now" — return an empty
            // listing rather than propagating EIO. This matches
            // rclone VFS implicit-dir semantics. We still surface
            // every other lister-init error (auth, permission, network).
            let mut lister = match op.lister(&p).await {
                Ok(l) => l,
                Err(e) if e.kind() == opendal::ErrorKind::NotFound => {
                    return Ok::<_, opendal::Error>(Vec::new());
                }
                Err(e) => return Err(e),
            };
            let mut out = vec![];
            while let Some(item) = lister.next().await {
                let entry = item?;
                let name = entry.name().trim_end_matches('/').to_string();
                let mode = entry.metadata().mode();
                let content_length = entry.metadata().content_length();
                // Apply filters
                if let Some(max_depth) = self.max_depth
                    && depth >= max_depth
                    && mode == EntryMode::DIR
                {
                    continue;
                }
                if let Some(ms) = self.max_size
                    && content_length > ms
                {
                    continue;
                }
                if let Some(ms) = self.min_size
                    && content_length < ms
                {
                    continue;
                }
                // exclude/include glob patterns
                if !self.exclude_patterns.is_empty() {
                    let matched = self
                        .exclude_patterns
                        .iter()
                        .any(|pat| fnmatch(pat, &name, self.ignore_case));
                    if matched {
                        continue;
                    }
                }
                // Skip Apple Double files on macOS
                if self.no_apple_double && name.starts_with("._") {
                    continue;
                }
                if !self.include_patterns.is_empty() {
                    let matched = self
                        .include_patterns
                        .iter()
                        .any(|pat| fnmatch(pat, &name, self.ignore_case));
                    if !matched {
                        continue;
                    }
                }
                let size = content_length;
                let mtime = entry
                    .metadata()
                    .last_modified()
                    .map(opendal_timestamp_to_system_time)
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                out.push((name, mode, size, mtime));
            }
            Ok::<_, opendal::Error>(out)
        })?;
        // Deduplicate by Unicode-normalized name if enabled
        if self.block_norm_dupes && !result.is_empty() {
            let mut seen = std::collections::HashSet::new();
            result.retain(|(name, ..)| {
                use unicode_normalization::UnicodeNormalization;
                let norm: String = name.nfc().collect::<String>();
                seen.insert(norm)
            });
        }
        // Store entries individually (like rclone DirEntry per name).
        // Only cache on success — caching an empty Vec from an error
        // would propagate the failure for dir_cache_ttl.
        let dir_entries: dashmap::DashMap<String, (EntryMode, u64, SystemTime)> = result
            .iter()
            .map(|(name, mode, size, mtime)| (name.clone(), (*mode, *size, *mtime)))
            .collect();
        self.dir_cache
            .insert(path.to_string(), (std::time::Instant::now(), dir_entries));

        // Also pre-populate attr_cache for every entry. The FUSE
        // kernel follows `readdir` with one `lookup` per entry, and
        // `lookup` calls `stat_op` which by default issues a backend
        // HEAD/STAT. S3/GCS/OSS/COS all return size + last_modified
        // inline in the list response (we already extracted them
        // above), so we can serve the post-readdir lookups from
        // memory instead of N extra round-trips. For a 500-file
        // directory, this turns 500 HEADs into 0.
        //
        // Cache TTL is the same `attr_ttl` used everywhere else so
        // the entries are treated as fresh for the same window.
        for (name, mode, size, mtime) in &result {
            let kind = match mode {
                EntryMode::DIR => FileType::Directory,
                _ => FileType::RegularFile,
            };
            let full = if path.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", path, name)
            };
            self.attr_cache
                .insert(full, (kind, *size, Some(*mtime), std::time::Instant::now()));
        }

        Ok(result)
    }

    /// Add a single entry to directory cache (like rclone addObject).
    /// Called after create() / mkdir() to avoid full directory re-read.
    ///
    /// Bug B fix: the pre-fix version only updated an *existing* cache
    /// entry. When mkdir -p created a chain like `a/b/c` and the
    /// parent's dir_cache was cold (no prior readdir had populated
    /// it), the new entry was silently dropped. The next readdir on
    /// the parent fell through to the backend, where the path was
    /// empty/missing, and the user got EIO. The fix initializes the
    /// cache with just the new entry when the parent slot is empty,
    /// so the subsequent readdir sees it. (A later readdir that
    /// actually hits the backend will re-merge; that's harmless —
    /// the cache-add path is idempotent for the same name+mode.)
    fn cache_add_entry(
        &self,
        parent_path: &str,
        name: &str,
        mode: EntryMode,
        size: u64,
        mtime: SystemTime,
    ) {
        if let Some(entry) = self.dir_cache.get(parent_path) {
            let (_, entries) = entry.value();
            entries.insert(name.to_string(), (mode, size, mtime));
        } else {
            let entries: dashmap::DashMap<String, (EntryMode, u64, SystemTime)> =
                dashmap::DashMap::new();
            entries.insert(name.to_string(), (mode, size, mtime));
            self.dir_cache.insert(
                parent_path.to_string(),
                (std::time::Instant::now(), entries),
            );
        }
    }

    /// Remove a single entry from directory cache (like rclone delObject).
    /// Called after unlink/rmdir to avoid full directory re-read.
    fn cache_remove_entry(&self, parent_path: &str, name: &str) {
        if let Some(entry) = self.dir_cache.get(parent_path) {
            let (_, entries) = entry.value();
            entries.remove(name);
        }
    }

    /// Full invalidation: remove directory cache and all sub-paths.
    /// Used for rename (both src and dst sides) where we can't cheaply update.
    fn invalidate_dir_cache(&self, path: &str) {
        self.dir_cache.remove(path);
        let prefix = format!("{}/", path);
        self.dir_cache.retain(|k, _| !k.starts_with(&prefix));
        if let Some(slash) = path.rfind('/') {
            let parent = &path[..slash];
            if !parent.is_empty() {
                self.dir_cache.remove(parent);
            }
        }
    }
}

use crate::core_fs::{CoreDirEntry, CoreFileAttr, CoreFileType, CoreFilesystem, CoreVolumeStat};

impl CoreFilesystem for MntrsFs {
    fn init(&self) -> std::io::Result<()> {
        self.common_init_wb();
        Ok(())
    }

    fn access(&self, _ino: u64, _mask: u32) -> std::io::Result<()> {
        Ok(())
    }

    fn lookup(&self, parent: u64, name: &str) -> std::io::Result<CoreFileAttr> {
        if name == "." || name == ".." {
            let p = if name == "." { parent } else { 1 };
            return Ok(to_core_attr(&self.make_attr(
                p,
                4096,
                FileType::Directory,
                SystemTime::UNIX_EPOCH,
            )));
        }
        let parent_path = self
            .resolve(parent)
            .map(|(p, _, _, _)| p)
            .unwrap_or_default();
        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };
        // stat_op talks to the backend, but freshly-written data
        // is still in the local cache file (5s async writeback
        // delay). If the backend says "not found" but a cache
        // file exists, trust the cache file. Same pattern as
        // read() and the rename fallback.
        //
        // Pre-existing-bug fix (CI test 6, HDFS append): when the
        // backend reports a small size but the local cache file
        // is larger (a recent write hasn't been uploaded yet),
        // the larger cache-file size wins. The previous version
        // blindly used the backend's size, so a follow-up
        // `cat <file>` after `echo "x" >> <file>` reported the
        // pre-append size and truncated the read to that. The
        // FUSE kernel uses our getattr-returned size as the
        // authoritative EOF, so a stale lookup made every
        // post-write read see the old length. Lookup is the
        // first call after a `BATCHFORGET`, so it has to be
        // self-consistent with the cache-file state.
        let (kind, size, mtime) = if let Some((k, s, m)) = self.stat_op(&full_path) {
            let cpath = crate::cache_path(&self.cache_dir, &full_path);
            let cache_size = std::fs::metadata(&cpath).map(|m| m.len()).unwrap_or(0);
            (k, s.max(cache_size), m)
        } else {
            let cpath = crate::cache_path(&self.cache_dir, &full_path);
            match std::fs::metadata(&cpath) {
                Ok(meta) => {
                    let mt = meta.modified().ok();
                    (FileType::RegularFile, meta.len(), mt)
                }
                Err(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "lookup: not on backend, no cache file",
                    ));
                }
            }
        };
        // Allocate a new ino for this lookup. alloc_ino's
        // NEXT_INO counter is the canonical ino; the FUSE
        // kernel stores whatever we return here and reuses
        // it for subsequent open/read/write. or_insert on the
        // inodes map is fine — if the entry exists, we keep
        // the previous (path, kind, size, mtime); if not, we
        // create one with the values we just resolved.
        let ino = self
            .find_ino_by_path(&full_path)
            .unwrap_or_else(|| self.alloc_ino(&full_path, kind, size));
        Ok(to_core_attr(&self.make_attr(
            ino,
            size,
            kind,
            mtime.unwrap_or(SystemTime::UNIX_EPOCH),
        )))
    }

    fn getattr(&self, ino: u64) -> std::io::Result<CoreFileAttr> {
        if ino == 1 {
            return Ok(to_core_attr(&self.make_attr(
                1,
                4096,
                FileType::Directory,
                SystemTime::UNIX_EPOCH,
            )));
        }
        if let Some((path, kind, inodes_size, inodes_mtime)) = self.resolve(ino) {
            let (_, backend_size, backend_mtime) =
                self.stat_op(&path).unwrap_or((kind, inodes_size, None));
            // Use the larger of inodes size, backend size, and the
            // on-disk cache file size.
            //
            //   * inodes_size — updated synchronously by write() and
            //     setattr(); always reflects the most recent local change
            //   * backend_size — what stat_op() reports from the remote
            //     backend (via opendal). This LAGS during async writeback
            //     and is permanently 0 for backends that have no
            //     on-server state to stat (notably memory://, which is
            //     in-process only)
            //   * cache_size — the local cache file's byte length.
            //     This is the source of truth for the most-recent
            //     write that the user has issued but the backend
            //     hasn't seen yet (writeback delay). The previous
            //     version ignored it; the FUSE kernel then saw a
            //     pre-write size for a freshly-appended file and
            //     truncated the read to that. The same pattern is
            //     now applied to `lookup` (see that function's
            //     comment).
            let cache_size = std::fs::metadata(crate::cache_path(&self.cache_dir, &path))
                .map(|m| m.len())
                .unwrap_or(0);
            let size = inodes_size.max(backend_size).max(cache_size);
            // Bug C fix (deeper layer): prefer the backend's mtime
            // (when it has one — e.g. the user opted into
            // `use_server_modtime`), then fall back to the inodes
            // entry's mtime (which `alloc_ino` and the write path
            // populate with `now()`), and only then to UNIX_EPOCH.
            //
            // The pre-fix `mtime.unwrap_or(UNIX_EPOCH)` discarded
            // the inodes mtime entirely, so a freshly-mkdir'd or
            // freshly-written file's stat always showed 1970-01-01
            // regardless of how the upper layers set the timestamp.
            // This was masked by callers that did `let _ =` on
            // stat_op's None return, but the visible symptom — `ls
            // -la` showing 1970 — is exactly what the audit caught.
            let mtime = backend_mtime
                .or(inodes_mtime)
                .unwrap_or(SystemTime::UNIX_EPOCH);
            Ok(to_core_attr(&self.make_attr(ino, size, kind, mtime)))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "ino not found",
            ))
        }
    }

    fn setattr(
        &self,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<SystemTime>,
        _mtime: Option<SystemTime>,
    ) -> std::io::Result<CoreFileAttr> {
        if let Some((_p, kind, _, _)) = self.resolve(ino) {
            if let Some(s) = size {
                // Truncate the inodes size to the new value.
                //
                // The previous implementation called `alloc_ino(&_p, kind, s)`,
                // which under the hood is `or_insert` on the inodes map.
                // `or_insert` only inserts when the entry is vacant — so
                // a truncate from 18 → 0 on an existing file silently
                // did nothing, leaving the kernel thinking the file was
                // still 18 bytes while the cache file had been
                // (partially) overwritten by a smaller write.
                //
                // The fix uses `and_modify` to unconditionally overwrite
                // the size field, which is what truncation actually
                // means semantically. We do NOT touch mtime here
                // (setattr's mtime is handled by the `make_attr` call
                // below with `SystemTime::now()`).
                self.inodes.entry(ino).and_modify(|v| {
                    v.2 = s;
                });
                // Truncate the on-disk cache file too, so subsequent
                // reads at offset ≥ s return EOF instead of leftover
                // bytes from the previous content. Without this, a
                // cat after truncate could read 18 bytes of stale
                // content even though our inodes says 10.
                let cpath = crate::cache_path(&self.cache_dir, &_p);
                if cpath.exists() {
                    // Open with write access so the resulting File
                    // holds a writable handle; the set_len() call below
                    // is the actual side effect — we don't write any
                    // bytes here, only shrink/grow the file size to
                    // match the truncate request. The `let _ =`
                    // discards any IO error (file vanished between
                    // exists() and open(), permissions, etc.) —
                    // truncation is best-effort: a partial truncation
                    // would leave the cache file slightly larger than
                    // logical size, which the read path already
                    // tolerates by using the smaller of cache and
                    // inodes size.
                    let _ = std::fs::OpenOptions::new()
                        .write(true)
                        .open(&cpath)
                        .map(|f| f.set_len(s));
                }
            }
            Ok(to_core_attr(&self.make_attr(
                ino,
                size.unwrap_or(0),
                kind,
                SystemTime::now(),
            )))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "ino not found",
            ))
        }
    }

    fn readdir(&self, ino: u64) -> std::io::Result<Vec<CoreDirEntry>> {
        let path = self.resolve(ino).map(|(p, _, _, _)| p).unwrap_or_default();
        let list_path = if path.is_empty() {
            String::new()
        } else {
            format!("{}/", path)
        };
        // Per SESSION_PITFALLS §2.6: propagate list_op errors to FUSE.
        // A swallowed backend error used to surface as an empty
        // directory (CI looked green, root cause was invisible).
        let listed = self.list_op(&list_path).map_err(|e| {
            tracing::warn!(path = %list_path, error = %e,
                    "CoreFilesystem::readdir: list_op failed");
            std::io::Error::other(e)
        })?;
        let mut entries = vec![
            CoreDirEntry {
                ino,
                kind: CoreFileType::Directory,
                name: ".".to_string(),
            },
            CoreDirEntry {
                ino: 1,
                kind: CoreFileType::Directory,
                name: "..".to_string(),
            },
        ];
        // hdfs-native quirk: the first entry of op.lister(p) is the queried
        // path itself. After trim_end_matches('/') inside list_op:
        //   lister("/")      → entries[0].name = ""       ← was caught
        //   lister("/test/") → entries[0].name = "/test"
        //   lister("/test")  → entries[0].name = "test"
        // Without filtering all three, the FUSE reply contains a phantom
        // entry that matches the parent dir name. ls -R then descends into
        // it and gets EIO on stat, plus the root listing can show an empty
        // name (kernel EIO on readdir).
        // hdfs-native quirk: the first entry of op.lister(p) is a phantom
        // whose name is the LAST path component of p (with any trailing
        // slash already trimmed by list_op). Confirmed by direct probe:
        //   lister("/")         → [0].name = ""        (root, no component)
        //   lister("/test/")    → [0].name = "test"
        //   lister("/test/sub/")→ [0].name = "sub"
        // Without filtering, the FUSE reply contains a phantom that
        // matches the parent dir's basename. ls -R then descends into it
        // and gets EIO on stat, plus the root listing can show an empty
        // name (kernel EIO on readdir). Per SESSION_PITFALLS §2.4.
        let queried_last = std::path::Path::new(&list_path)
            .components()
            .next_back()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .unwrap_or_default();
        for (name, mode, size, _mtime) in listed {
            if name.is_empty() || name == "/" || (name == queried_last && !queried_last.is_empty())
            {
                continue;
            }
            let kind = match mode {
                EntryMode::DIR => CoreFileType::Directory,
                _ => CoreFileType::RegularFile,
            };
            // name from list_op already includes path prefix (e.g., "many/file_0001.txt")
            // Extract just the filename for display, use full path for inode allocation
            let display_name = name
                .rsplit_once('/')
                .map(|(_, n)| n.to_string())
                .unwrap_or_else(|| name.clone());
            let ino = self.alloc_ino(
                &name,
                match kind {
                    CoreFileType::Directory => FileType::Directory,
                    _ => FileType::RegularFile,
                },
                size,
            );
            entries.push(CoreDirEntry {
                ino,
                kind,
                name: display_name,
            });
        }
        Ok(entries)
    }

    fn open(&self, ino: u64, _flags: u32) -> std::io::Result<u64> {
        let path = self.resolve(ino).map(|(p, _, _, _)| p).unwrap_or_default();
        let fh = NEXT_HANDLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Check if flags contain write access (O_WRONLY=1, O_RDWR=2)
        let is_write = if cfg!(unix) {
            (_flags & 0x3) != 0
        } else {
            false
        };
        if is_write {
            let cpath = crate::cache_path(&self.cache_dir, &path);
            if let Some(parent) = cpath.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let cache_fd = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .read(true)
                .open(&cpath)
                .ok();
            self.handles.insert(
                fh,
                FileHandleState::Write {
                    path,
                    cache_fd: cache_fd.map(|f| std::sync::Arc::new(std::sync::Mutex::new(f))),
                    dirty: false,
                    dirty_since: None,
                },
            );
        } else {
            // Activate the background prefetcher for files large enough
            // to benefit (default threshold 64 MiB; see
            // `maybe_create_prefetcher`). Called before the path move
            // below so we don't have to clone.
            let prefetcher = self.maybe_create_prefetcher(ino, &path);
            self.handles.insert(
                fh,
                FileHandleState::Read {
                    path,
                    last_offset: 0,
                    chunk_size: self.read_chunk_size.max(131072),
                    prefetcher,
                },
            );
        }
        Ok(fh)
    }

    fn read(&self, ino: u64, fh: u64, offset: u64, size: u32) -> std::io::Result<Vec<u8>> {
        let (path, file_size) = self
            .resolve(ino)
            .map(|(p, _, s, _)| (p, s))
            .ok_or(std::io::ErrorKind::NotFound)?;
        // Defensive size reconciliation (see CoreFilesystem::read history
        // for the full explanation). inodes is the FUSE-protocol
        // authoritative size, but the on-disk cache file may have
        // grown more recently than the inodes entry.
        let cache_meta_size = std::fs::metadata(crate::cache_path(&self.cache_dir, &path))
            .map(|m| m.len())
            .unwrap_or(0);
        let actual_size = cache_meta_size.max(file_size);
        if offset >= actual_size {
            return Ok(vec![]);
        }
        let cap = actual_size - offset;
        let block_idx = offset / CACHE_BLOCK_SIZE;

        // 1. Try read from cache fd first (write handle still open)
        if !self.direct_io {
            let cache_fd = self.handles.get(&fh).and_then(|e| {
                if let crate::FileHandleState::Read { .. } = e.value() {
                    None
                } else if let crate::FileHandleState::Write {
                    cache_fd: Some(fd), ..
                } = e.value()
                {
                    Some(fd.clone())
                } else {
                    None
                }
            });
            if let Some(fd) = cache_fd {
                use std::io::{Read, Seek};
                let mut f = fd.lock().unwrap();
                let file_len = f.metadata()?.len();
                if offset < file_len {
                    let read_size = (size as u64).min(file_len - offset) as usize;
                    let mut buf = vec![0u8; read_size];
                    f.seek(std::io::SeekFrom::Start(offset))?;
                    f.read_exact(&mut buf)?;
                    return Ok(buf);
                }
            }
        }

        // 2. mem_cache fast path
        if let Some(data) = self.mem_cache.get(ino, block_idx) {
            let start = offset as usize;
            let end = (start + size as usize).min(data.len());
            return if start < data.len() {
                Ok(data[start..end].to_vec())
            } else {
                Ok(vec![])
            };
        }

        // 3. Try prefetcher (backpressure-aware background download)
        if let Some(h) = self.handles.get(&fh)
            && let FileHandleState::Read {
                prefetcher: Some(p),
                ..
            } = h.value()
            && let Some(part) = p.pop(offset)
        {
            // Populate mem_cache for the prefetched part so subsequent
            // reads on the same block range hit the fast path above.
            // part.data is up to 16 MiB (chunk_size cap) and may span
            // 1-2 CACHE_BLOCK_SIZE blocks; cheap iteration.
            let first_blk = part.offset / CACHE_BLOCK_SIZE;
            let data = part.data.clone();
            let n_blks = (data.len() as u64).div_ceil(CACHE_BLOCK_SIZE);
            for i in 0..n_blks {
                let s = (i * CACHE_BLOCK_SIZE) as usize;
                let e = ((i + 1) * CACHE_BLOCK_SIZE) as usize;
                self.mem_cache
                    .put(ino, first_blk + i, data.slice(s..e.min(data.len())));
            }
            let start = (offset - part.offset) as usize;
            let end = (start + size as usize).min(data.len());
            return if start < data.len() {
                Ok(data[start..end].to_vec())
            } else {
                Ok(vec![])
            };
        }

        // 4. File-level disk cache (whole file)
        if !self.direct_io {
            let fcpath = crate::cache_path(&self.cache_dir, &path);
            if fcpath.exists()
                && let Ok(data) = std::fs::read(&fcpath)
            {
                let b = bytes::Bytes::from(data);
                let start = offset as usize;
                let end = (start + size as usize).min(b.len());
                let result = if start < b.len() {
                    b[start..end].to_vec()
                } else {
                    vec![]
                };
                self.mem_cache.put(ino, offset / CACHE_BLOCK_SIZE, b);
                return Ok(result);
            }
            // 5. Block-level disk cache
            let cpath = crate::cache_block_path(&self.cache_dir, &path, block_idx);
            if cpath.exists()
                && let Ok(data) = std::fs::read(&cpath)
            {
                let b = bytes::Bytes::from(data);
                let start = offset as usize;
                let end = (start + size as usize).min(b.len());
                let result = if start < b.len() {
                    b[start..end].to_vec()
                } else {
                    vec![]
                };
                self.mem_cache.put(ino, offset / CACHE_BLOCK_SIZE, b);
                return Ok(result);
            }
        }

        // 6. Remote fetch with 16 MiB cap (commit fc5e974) and 8 MiB
        // block split for mem_cache. fetch_size is bounded by:
        //   - user config (read_chunk_size, default 128 MiB)
        //   - hard cap (16 MiB) protecting `head -c1K` from over-fetch
        //   - cap (bytes remaining to file end)
        let user_cap = if self.read_chunk_size > 0 {
            self.read_chunk_size
        } else {
            8 * 1024 * 1024
        };
        let hard_cap = 16 * 1024 * 1024;
        let fetch_size = user_cap.min(hard_cap).min(cap);

        let op = self.op.clone();
        let p = path.clone();
        let buf = rt()
            .block_on(async move { op.read_with(&p).range(offset..offset + fetch_size).await })
            .map_err(|_| std::io::Error::other("read failed"))?;
        let b: bytes::Bytes = buf.to_vec().into();
        let len = (b.len() as u32).min(size) as usize;
        let result = b[..len].to_vec();
        // Populate mem_cache for ALL blocks covered by this fetch,
        // not just the first one. Without this, a 16 MiB fetch
        // would store the entire 16 MiB under one (ino, block_idx)
        // key, evicting anything else in cache and forcing the
        // next read on a neighbouring block to re-fetch from
        // remote. Bytes::slice is zero-copy.
        let first_blk = offset / CACHE_BLOCK_SIZE;
        let n_blks = (b.len() as u64).div_ceil(CACHE_BLOCK_SIZE);
        for i in 0..n_blks {
            let s = (i * CACHE_BLOCK_SIZE) as usize;
            let e = ((i + 1) * CACHE_BLOCK_SIZE) as usize;
            self.mem_cache
                .put(ino, first_blk + i, b.slice(s..e.min(b.len())));
        }
        // Also populate block-level disk cache so subsequent reads
        // of the same range hit the fast path on disk (rclone's
        // `--vfs-cache-mode full` parity). Each block is a separate
        // file under `cache_dir/{hash}_{block_idx:010x}.block`; the
        // read path already checks for these (CoreFilesystem::read
        // step 5) — they were just never written until now.
        //
        // `b.slice(s..e)` is a zero-copy Bytes view, and the file
        // is opened with create+truncate(false) so a re-read
        // overwrites the cached chunk in place. Write failures
        // are non-fatal: log + continue. The mem_cache copy above
        // is what the FUSE worker actually returns to the kernel.
        if !self.direct_io {
            for i in 0..n_blks {
                let s = (i * CACHE_BLOCK_SIZE) as usize;
                let e = ((i + 1) * CACHE_BLOCK_SIZE) as usize;
                let blk_path = crate::cache_block_path(&self.cache_dir, &path, first_blk + i);
                if let Some(parent) = blk_path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                if let Ok(mut f) = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(&blk_path)
                {
                    use std::io::Write;
                    let slice = b.slice(s..e.min(b.len()));
                    if let Err(e) = f.write_all(&slice) {
                        tracing::debug!(?blk_path, error=%e, "block cache write failed");
                    }
                }
            }
        }
        Ok(result)
    }

    fn write(&self, _ino: u64, _fh: u64, _offset: u64, _data: &[u8]) -> std::io::Result<u32> {
        let fh_val = _fh;
        let path = self
            .handles
            .get(&fh_val)
            .map(|r| r.value().path().to_string())
            .ok_or(std::io::ErrorKind::NotFound)?;

        if self.direct_io {
            let op = self.op.clone();
            let p = path.clone();
            let d = _data.to_vec();
            rt().block_on(async move { op.write(&p, d).await })
                .map_err(|_| std::io::Error::other("write failed"))?;
            return Ok(_data.len() as u32);
        }

        // Write via single cache fd (like rclone RWFileHandle)
        let cache_fd = self.handles.get(&fh_val).and_then(|e| {
            if let crate::FileHandleState::Write {
                cache_fd: Some(fd), ..
            } = e.value()
            {
                Some(fd.clone())
            } else {
                None
            }
        });

        match &cache_fd {
            Some(fd) => {
                use std::io::{Seek, Write};
                let mut f = fd.lock().unwrap();
                let end = _offset + _data.len() as u64;
                let current_len = f.metadata()?.len();
                // When writing at an offset beyond the cache file length,
                // fetch the missing prefix from the remote backend to avoid
                // creating a sparse (zero-filled) cache that corrupts reads.
                if _offset > 0 && current_len == 0 && _offset > current_len {
                    let op = self.op.clone();
                    let p = path.clone();
                    if let Ok(remote) = rt().block_on(async { op.read(&p).await }) {
                        let prefix = remote.to_vec();
                        if !prefix.is_empty() {
                            let _ = f.write_all(&prefix);
                        }
                    }
                }
                let current_len = f.metadata()?.len();
                if end > current_len {
                    f.set_len(end)?;
                }
                f.seek(std::io::SeekFrom::Start(_offset))?;
                f.write_all(_data)?;
                f.flush()?;
            }
            None => {
                // Fallback: open cache file directly
                let cpath = crate::cache_path(&self.cache_dir, &path);
                if let Some(parent) = cpath.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                use std::io::{Seek, Write};
                let mut f = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .read(true)
                    .open(&cpath)?;
                let end = _offset + _data.len() as u64;
                let current_len = f.metadata()?.len();
                if end > current_len {
                    f.set_len(end)?;
                }
                f.seek(std::io::SeekFrom::Start(_offset))?;
                f.write_all(_data)?;
                f.flush()?;
            }
        }

        self.disk_cache_index.insert(
            path.clone(),
            (_data.len() as u64, std::time::SystemTime::now()),
        );
        let written = _data.len() as u32;

        // Update inodes size — must CREATE the entry if it doesn't exist.
        //
        // The naive `entry(_ino).and_modify(...)` is a no-op when the
        // ino has not been registered in the inodes map yet. This
        // happens on the very first write to a brand-new file: the
        // FUSE kernel can hand us a write() before the lookup()
        // induced alloc_ino() ever runs (the kernel does a stat cache
        // lookup in parallel, or the write is initiated by an
        // application that already has a file descriptor from outside
        // this mount). When that occurs, and_modify silently does
        // nothing, the inodes map keeps a stale `None` (or a
        // 0-sized entry from a prior iter), the kernel then sees
        // size=0 from our getattr, asks for 0 bytes, and the user
        // observes an empty file.
        //
        // The fix is the two-step `and_modify().or_insert_with()`:
        //   - if an entry exists, only grow its size (never shrink
        //     on a single write — setattr() owns truncation)
        //   - if no entry exists, create one seeded with the new
        //     write's end offset
        //
        // The initial mtime is set to `now()` (Bug C fix); the
        // and_modify branch also updates it on every subsequent write
        // so a read-after-write sees a fresh mtime even before the
        // writeback upload has landed.
        let end = _offset + _data.len() as u64;
        let write_mtime = std::time::SystemTime::now();
        self.inodes
            .entry(_ino)
            .and_modify(|v| {
                if end > v.2 {
                    v.2 = end;
                }
                v.3 = Some(write_mtime);
            })
            .or_insert_with(|| (path.clone(), FileType::RegularFile, end, Some(write_mtime)));

        // Invalidate mem_cache for this ino.
        //
        // mem_cache is a per-(ino, block_idx) DashMap of recently-read
        // Bytes, populated lazily by the read path on a cache miss.
        // Writes change the underlying on-disk cache file but leave
        // mem_cache entries stale — they hold the pre-write content.
        // A subsequent read that consults mem_cache first would
        // otherwise return data capped at the old entry's length
        // (since the read code does `b[start..end].min(b.len())`).
        //
        // The classic symptom: write 18 bytes, read returns 18
        // bytes (good); append 10 bytes, the second read hits
        // mem_cache and returns only the first 18 bytes (bad — the
        // appended tail is silently lost). This is the original
        // d4d19c8 flake: tests 5 ("append + verify") and 6
        // ("append to pre-existing file") would intermittently see
        // truncated content.
        //
        // We use `retain` to drop every block_idx for this ino in
        // one pass, because a single write can span multiple
        // CACHE_BLOCK_SIZE-aligned blocks and we don't track exactly
        // which ones. The cost is O(mem_cache size for this shard);
        // mem_cache uses DashMap so shards are independent and the
        // retain only locks the affected shard(s).
        self.mem_cache.invalidate_ino(_ino);

        self.handles.insert(
            fh_val,
            crate::FileHandleState::Write {
                path: path.clone(),
                cache_fd,
                dirty: true,
                dirty_since: Some(std::time::Instant::now()),
            },
        );
        Ok(written)
    }
    fn flush(&self, _ino: u64, _fh: u64) -> std::io::Result<()> {
        // Look up the handle to find the path and dirty state
        let fh_val = _fh;
        let (path, dirty) = {
            let entry = self.handles.get(&fh_val).map(|r| r.clone());
            if let Some(crate::FileHandleState::Write {
                path: p, dirty: d, ..
            }) = entry
            {
                (p, d)
            } else {
                return Ok(());
            }
        };
        if dirty {
            // Push single cache file to writeback queue
            let cpath = crate::cache_path(&self.cache_dir, &path);
            if cpath.exists() {
                let sidecar = cpath.with_extension("dirty");
                if let Err(e) = std::fs::write(&sidecar, path.as_bytes()) {
                    tracing::warn!(error=%e, path=?sidecar, "sidecar write failed");
                }
                if let Some(tx) = self.writeback_sender.get() {
                    tx.send((_ino, path.clone(), cpath)).ok();
                }
                tracing::debug!(path=%path, "flush queued writeback");
            }
            // Mark handle clean; writeback happens asynchronously
            let cache_fd = self.handles.get(&_fh).and_then(|e| {
                if let crate::FileHandleState::Write {
                    cache_fd: Some(fd), ..
                } = e.value()
                {
                    Some(fd.clone())
                } else {
                    None
                }
            });
            self.handles.insert(
                _fh,
                crate::FileHandleState::Write {
                    path: path.clone(),
                    cache_fd,
                    dirty: false,
                    dirty_since: None,
                },
            );
        }
        Ok(())
    }
    fn release(&self, _ino: u64, fh: u64) -> std::io::Result<()> {
        // On release, trigger writeback for dirty handles
        let was_dirty = if let Some(entry) = self.handles.get(&fh)
            && let crate::FileHandleState::Write {
                path, dirty: true, ..
            } = entry.value()
        {
            let cpath = crate::cache_path(&self.cache_dir, path);
            if cpath.exists() {
                let sidecar = cpath.with_extension("dirty");
                let _ = std::fs::write(&sidecar, path.as_bytes());
                if let Some(tx) = self.writeback_sender.get() {
                    tx.send((_ino, path.clone(), cpath)).ok();
                }
                tracing::debug!(path=%path, "release queued writeback");
            }
            true
        } else {
            false
        };

        // Signal any in-flight prefetcher to stop. Without this, a
        // partially-read file (e.g. `head -c 1K 100M`) leaves its
        // background downloader thread running until either the queue
        // fills (then it spin-sleeps forever) or it reaches EOF on
        // its own. For a long-running mntrs process that opens many
        // large files, that adds up to a slow resource leak.
        //
        // `cancel()` only flips an AtomicBool; the downloader
        // checks it at the top of its next loop iteration and
        // exits cleanly. Cheap and safe to call on None.
        if let Some(entry) = self.handles.get(&fh) {
            if let crate::FileHandleState::Read {
                prefetcher: Some(p),
                ..
            } = entry.value()
            {
                p.cancel();
            }
        }

        if self.handle_caching > std::time::Duration::ZERO && !was_dirty {
            // Keep handle alive for handle_caching duration so reopen can reuse cache fd
            let fd_to_keep = self.handles.get(&fh).and_then(|e| {
                if let crate::FileHandleState::Write {
                    cache_fd: Some(fd), ..
                } = e.value()
                {
                    Some(fd.clone())
                } else {
                    None
                }
            });
            if let Some(_fd) = fd_to_keep {
                // Handle stays in map; it will be cleaned up when handle_caching expires
                // or when a new open for this inode reuses/replaces it
                return Ok(());
            }
        }

        self.handles.remove(&fh);
        Ok(())
    }

    fn create(&self, _parent: u64, name: &str, _mode: u32) -> std::io::Result<CoreFileAttr> {
        let parent_path = self
            .resolve(_parent)
            .map(|(p, _, _, _)| p)
            .unwrap_or_default();
        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };
        let op = self.op.clone();
        let p = full_path.clone();
        rt().block_on(async move { op.write(&p, Vec::<u8>::new()).await })
            .map_err(|e| opendal_to_io_error(&e, "create"))?;
        // Synthesize metadata: we just wrote an empty file via op.write,
        // so the size is 0 and the kind is RegularFile. No need for a
        // post-write HEAD/stat to fetch what we already know — that was
        // 1 extra round-trip per `touch new` / `create` (issue #17).
        // mtime is `now()` because the write just happened.
        // (The pre-fix `stat_op` was returning (FileType::RegularFile,
        // 0, None) anyway via its `unwrap_or` fallback when the
        // backend hadn't yet propagated, so the mtime slot was already
        // unreliable — we now make it explicit and save the round-trip.)
        let (kind, size, mtime) = (FileType::RegularFile, 0u64, Some(SystemTime::now()));
        // Bug C fix: seed the inodes mtime so a follow-up getattr
        // (before the backend's stat_op caches anything) doesn't
        // fall back to UNIX_EPOCH. mtime is now always Some(_), so
        // unwrap_or is dead — the fallback remains defensive in case
        // someone refactors mtime back to Option.
        let now = SystemTime::now();
        let ino = self.alloc_ino_with_mtime(&full_path, kind, size, mtime.unwrap_or(now));
        // Insert Write handle so follow-up write() can find the path
        // Create cache file for write handle
        let cpath = crate::cache_path(&self.cache_dir, &full_path);
        if let Some(parent) = cpath.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let cache_fd = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&cpath)
            .ok()
            .map(|f| Arc::new(std::sync::Mutex::new(f)));
        self.handles.insert(
            ino,
            FileHandleState::Write {
                path: full_path,
                cache_fd,
                dirty: false,
                dirty_since: None,
            },
        );
        self.cache_add_entry(
            &parent_path,
            name,
            if kind == FileType::Directory {
                EntryMode::DIR
            } else {
                EntryMode::FILE
            },
            size,
            mtime.unwrap_or(SystemTime::UNIX_EPOCH),
        );
        Ok(to_core_attr(&self.make_attr(
            ino,
            size,
            kind,
            mtime.unwrap_or(SystemTime::UNIX_EPOCH),
        )))
    }

    fn mkdir(&self, _parent: u64, name: &str) -> std::io::Result<CoreFileAttr> {
        let parent_path = self
            .resolve(_parent)
            .map(|(p, _, _, _)| p)
            .unwrap_or_default();
        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };
        // Recursively create the entire path (parents + leaf).
        // Bug A fix: a single create_dir on "a/b/c/" leaves "a/" and
        // "a/b/" un-created on flat-namespace backends, so subsequent
        // `ls a/` returns EIO. mkdir_chain walks up and creates each
        // level, treating Unsupported (implicit-dir backends) and
        // AlreadyExists (idempotent) as success.
        self.mkdir_chain(&full_path)?;
        let now = SystemTime::now();
        // Bug C follow-up: use the mtime-aware allocator so the
        // inodes entry's mtime slot is populated. The pre-fix
        // `alloc_ino` left it as `None`, and `getattr` would
        // then fall back to UNIX_EPOCH (see Bug C fix in
        // `CoreFilesystem::getattr`).
        let ino = self.alloc_ino_with_mtime(&full_path, FileType::Directory, 4096, now);
        // Bug B fix: prime the parent's dir_cache so a readdir on the
        // parent sees this new entry without a full backend re-list.
        self.cache_add_entry(&parent_path, name, EntryMode::DIR, 4096, now);
        Ok(to_core_attr(&self.make_attr(
            ino,
            4096,
            FileType::Directory,
            now,
        )))
    }

    fn unlink(&self, _parent: u64, name: &str) -> std::io::Result<()> {
        let parent_path = self
            .resolve(_parent)
            .map(|(p, _, _, _)| p)
            .unwrap_or_default();
        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };
        let op = self.op.clone();
        let p = full_path.clone();
        // Bug D fix: preserve the opendal error kind so POSIX callers
        // get the right errno (NotFound→ENOENT, IsADirectory→EISDIR,
        // etc.) instead of a blanket EIO. The previous
        // `map_err(|_| Error::other("unlink failed"))` swallowed the
        // kind, which meant unlink on a non-existent file returned
        // EIO — apps like rm would treat it as a generic I/O error
        // and refuse to continue.
        rt().block_on(async move { op.delete(&p).await })
            .map_err(|e| opendal_to_io_error(&e, "unlink"))?;
        let cpath = crate::cache_path(&self.cache_dir, &full_path);
        let _ = std::fs::remove_file(&cpath);
        // Clean block-level cache entries. O(K) via inodes.size()
        // (see `remove_block_cache_files` for the rationale and the
        // previous-O(N) bug this replaces).
        if let Some((_path, _kind, size, _mtime)) = self.inodes.iter().find_map(|entry| {
            let (p, kind, sz, mtime) = entry.value();
            if p == &full_path {
                Some((p.clone(), *kind, *sz, *mtime))
            } else {
                None
            }
        }) {
            remove_block_cache_files(&self.cache_dir, &full_path, size);
        }
        self.disk_cache_index.remove(&full_path);
        // Bug E fix: inodes is keyed by the NEXT_INO counter, not
        // path_hash. Use find_ino_by_path to locate the correct ino
        // before removing. path_hash(&full_path) was a no-op
        // (path_hash is FNV-1a of the path, NEXT_INO is a monotonic
        // counter — they almost never coincide), so the inodes entry
        // leaked across the unlink, and a subsequent create at the
        // same path collided with the stale ino.
        if let Some(ino) = self.find_ino_by_path(&full_path) {
            self.inodes.remove(&ino);
        }
        self.attr_cache.remove(&full_path);
        self.cache_remove_entry(&parent_path, name);
        Ok(())
    }

    fn rmdir(&self, _parent: u64, name: &str) -> std::io::Result<()> {
        let parent_path = self
            .resolve(_parent)
            .map(|(p, _, _, _)| p)
            .unwrap_or_default();
        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };
        let dir_path = format!("{}/", full_path.trim_end_matches('/'));
        let op = self.op.clone();
        let p = dir_path.clone();
        // Bug D fix: same as unlink — preserve the opendal error
        // kind. POSIX requires rmdir on a non-empty directory to
        // return EEXIST ("EEXIST: directory not empty"); the previous
        // blanket EIO left rm -rf in an undefined state on such
        // backends (some pre-check emptyness, some don't).
        rt().block_on(async move { op.delete(&p).await })
            .map_err(|e| opendal_to_io_error(&e, "rmdir"))?;
        // Bug E fix: inodes keyed by NEXT_INO, not path_hash.
        if let Some(ino) = self.find_ino_by_path(&full_path) {
            self.inodes.remove(&ino);
        }
        self.attr_cache.remove(&full_path);
        self.cache_remove_entry(&parent_path, name);
        self.invalidate_dir_cache(&full_path);
        Ok(())
    }

    fn rename(
        &self,
        _parent: u64,
        name: &str,
        _newparent: u64,
        newname: &str,
    ) -> std::io::Result<()> {
        let parent_path = self
            .resolve(_parent)
            .map(|(p, _, _, _)| p)
            .unwrap_or_default();
        let newparent_path = self
            .resolve(_newparent)
            .map(|(p, _, _, _)| p)
            .unwrap_or_default();
        let src = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };
        let dst = if newparent_path.is_empty() {
            newname.to_string()
        } else {
            format!("{}/{}", newparent_path, newname)
        };
        let op = self.op.clone();
        let src_clone = src.clone();
        let dst_clone = dst.clone();
        // Atomic rename — model: "copy-then-delete with rollback on
        // failure":
        //   1. Try server-side rename. If it returns Unsupported
        //      (opendal: backends like memory://, webhdfs that
        //      don't expose rename), fall through to copy+delete.
        //      Any other error: do NOT touch local state and
        //      return Ok(()) so the next read sees the unchanged
        //      src (no silent data loss).
        //   2. In the copy+delete fallback, if copy fails, do NOT
        //      delete src. If copy succeeds, delete src; if delete
        //      fails, log loudly but proceed (dst is already
        //      visible on the backend; preserving dst is more
        //      important than enforcing atomicity).
        //
        // Pre-delete of dst was removed (issue #17). On S3, the
        // copy step in `op.rename` uses PUT with overwrite semantics,
        // so a pre-delete is a wasted round-trip. On hierarchical
        // backends (HDFS, etc.) `op.rename` is atomic. On the
        // Unsupported fallback path, op.write to dst overwrites the
        // existing key (opendal's `Writer` is overwrite on S3 / GCS
        // / OSS / COS / OBS); for the rare backend where op.write
        // is create-only (memory, some WebHDFS deployments), the
        // copy may return AlreadyExists which the fallback treats as
        // a hard error — that's the same behavior as before this
        // change, except now we don't pay the cost of the
        // unconditional pre-delete.
        let backend_ok = rt().block_on(async move {
            match op.rename(&src_clone, &dst_clone).await {
                Ok(()) => true,
                Err(e) if e.kind() == opendal::ErrorKind::Unsupported => {
                    tracing::debug!(
                        path = %src_clone, error = %e,
                        "backend does not support server-side rename; falling back to local-cache read + write+delete"
                    );
                    let cpath_src = crate::cache_path(&self.cache_dir, &src_clone);
                    let bytes = match std::fs::read(&cpath_src) {
                        Ok(b) => b,
                        Err(read_err) if read_err.kind() == std::io::ErrorKind::NotFound => {
                            Vec::new()
                        }
                        Err(read_err) => {
                            tracing::error!(
                                path = %cpath_src.display(), error = %read_err,
                                "rename fallback: read cache file failed, keeping source intact"
                            );
                            return false;
                        }
                    };
                    let write_res = op.write(&dst_clone, bytes).await;
                    if let Err(write_err) = write_res {
                        tracing::error!(
                            src = %src_clone, dst = %dst_clone, error = %write_err,
                            "rename fallback: write dst failed, keeping source intact"
                        );
                        return false;
                    }
                    let del_res = op.delete(&src_clone).await;
                    if del_res.is_err() {
                        tracing::warn!(
                            src = %src_clone, dst = %dst_clone,
                            "rename fallback: write ok, delete failed — both visible"
                        );
                    }
                    true
                }
                Err(e) => {
                    tracing::warn!(
                        path = %src_clone, error = %e,
                        "server-side rename failed with non-Unsupported error; not falling back"
                    );
                    false
                }
            }
        });
        if !backend_ok {
            return Ok(());
        }
        // Migrate cache file
        let cpath_src = crate::cache_path(&self.cache_dir, &src);
        let cpath_dst = crate::cache_path(&self.cache_dir, &dst);
        if cpath_src.exists() && !cpath_dst.exists() {
            if let Some(parent) = cpath_dst.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::rename(&cpath_src, &cpath_dst);
        } else {
            let _ = std::fs::remove_file(&cpath_src);
        }
        // Migrate inodes src -> dst. The inodes map is keyed by
        // the NEXT_INO counter (alloc_ino), not by path_hash —
        // so the FUSE kernel, which already knows the ino for
        // the source file, will keep using that same ino for
        // the destination after rename. All we need to do is
        // change the entry's `path` field from src to dst; the
        // ino stays the same. This avoids the previous
        // implementation's mistake of inserting at path_hash
        // (which is a different number from the counter) and
        // leaving the FUSE kernel with a stale ino->path map.
        let src_ino = self
            .inodes
            .iter()
            .find(|e| e.value().0 == src)
            .map(|e| *e.key());
        if let Some(src_ino) = src_ino {
            // In-place path update. Size/mtime/ino are unchanged.
            self.inodes.entry(src_ino).and_modify(|v| {
                v.0 = dst.clone();
            });
        }

        if let Some(entry) = self.attr_cache.get(&src).map(|e| *e.value()) {
            self.attr_cache.insert(dst.to_string(), entry);
        }
        self.attr_cache.remove(&src);
        self.invalidate_dir_cache(&src);
        self.invalidate_dir_cache(&dst);
        Ok(())
    }

    fn statfs(&self, _ino: u64) -> std::io::Result<CoreVolumeStat> {
        let bs = 4096u32;
        let total = if self.disk_total_size > 0 {
            self.disk_total_size / bs as u64
        } else {
            256 * 1024 * 1024
        };
        Ok(CoreVolumeStat {
            total_blocks: total,
            free_blocks: total,
            avail_blocks: total,
            total_inodes: 1_000_000_000,
            free_inodes: 1_000_000_000,
            block_size: bs,
            max_name_len: 255,
        })
    }

    fn opendir(&self, _ino: u64) -> std::io::Result<u64> {
        Ok(0)
    }
    fn releasedir(&self, _ino: u64, _fh: u64) -> std::io::Result<()> {
        Ok(())
    }

    fn getxattr(&self, ino: u64, name: &str) -> std::io::Result<Vec<u8>> {
        if let Some((p, _, _, _)) = self.resolve(ino) {
            let op = self.op.clone();
            let p2 = p.clone();
            match rt().block_on(async move { op.stat(&p2).await }) {
                Ok(meta) => match name {
                    "user.etag" | "s3.etag" => {
                        meta.etag().map(|e| e.as_bytes().to_vec()).ok_or_else(|| {
                            std::io::Error::new(std::io::ErrorKind::NotFound, "no etag")
                        })
                    }
                    "user.content-type" | "s3.content-type" => meta
                        .content_type()
                        .map(|c| c.as_bytes().to_vec())
                        .ok_or_else(|| {
                            std::io::Error::new(std::io::ErrorKind::NotFound, "no content-type")
                        }),
                    _ => Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "xattr not found",
                    )),
                },
                Err(_) => Err(std::io::Error::other("stat failed")),
            }
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "ino not found",
            ))
        }
    }

    fn listxattr(&self, ino: u64) -> std::io::Result<Vec<Vec<u8>>> {
        if let Some((_, kind, _, _)) = self.resolve(ino) {
            if kind == FileType::Directory {
                return Ok(vec![]);
            }
            Ok(vec![b"user.etag".to_vec(), b"user.content-type".to_vec()])
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "ino not found",
            ))
        }
    }

    fn forget(&self, _ino: u64, _nlookup: u64) {
        // FUSE forget: kernel no longer needs this inode.
        // Clean up our local state to prevent leakage.
        let ino = _ino;
        // Don't forget root inode
        if ino == 1 {
            return;
        }
        if let Some((path, _, _, _)) = self.resolve(ino) {
            self.inodes.remove(&ino);
            self.attr_cache.remove(&path);
            // Clean up any open file handles for this inode
            self.handles.retain(|k, v| k != &ino && v.path() != path);
        }
    }
}

fn to_core_attr(a: &FileAttr) -> CoreFileAttr {
    CoreFileAttr {
        ino: a.ino.into(),
        size: a.size,
        blocks: a.blocks,
        atime: a.atime,
        mtime: a.mtime,
        ctime: a.ctime,
        crtime: a.crtime,
        kind: match a.kind {
            FileType::Directory => CoreFileType::Directory,
            _ => CoreFileType::RegularFile,
        },
        perm: a.perm,
        nlink: a.nlink,
        uid: a.uid,
        gid: a.gid,
        rdev: a.rdev,
        blksize: a.blksize,
        flags: a.flags,
    }
}

/// Install a panic hook that logs to a file before crashing.
/// Useful in container/CSI environments where stderr may be lost.
pub fn install_panic_logger() {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let msg = format!("panic: {info}");
        let location = info.location().map(|l| l.to_string()).unwrap_or_default();
        let backtrace = std::backtrace::Backtrace::force_capture();
        let report = format!("{msg}\n  at {location}\n  backtrace:\n{backtrace}\n");
        // Always write to stderr
        // Also write to file
        if let Ok(path) = std::env::var("MNTRS_PANIC_LOG") {
            let _ = std::fs::write(&path, &report);
        } else {
            let default_path = format!("/tmp/mntrs-panic.{}.log", std::process::id());
            let _ = std::fs::write(default_path, &report);
        }
        prev(info);
    }));
}

/// Detect cgroup v1 memory limit (bytes). Returns None if not in a cgroup.
/// Reads /sys/fs/cgroup/memory/memory.limit_in_bytes.
/// Falls back to /proc/self/cgroup for container-specific path.
pub fn detect_cgroup_memory_limit() -> Option<u64> {
    // Try cgroup v1 first (most common in K8s)
    let cgroup_paths = ["/sys/fs/cgroup/memory/memory.limit_in_bytes"];
    for path in &cgroup_paths {
        if let Ok(content) = std::fs::read_to_string(path) {
            let val: u64 = content.trim().parse().ok()?;
            if val > 0 && val < u64::MAX {
                return Some(val);
            }
        }
    }
    // Try cgroup v2
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let trimmed = content.trim();
        if trimmed != "max"
            && let Ok(val) = trimmed.parse::<u64>()
            && val > 0
        {
            return Some(val);
        }
    }
    None
}

/// Lightweight checksummed buffer for cache integrity validation.
/// Uses CRC32C (same as mountpoint-s3 and S3's native checksum).
pub struct ChecksummedBytes {
    data: bytes::Bytes,
    checksum: u32, // CRC32C
}

impl ChecksummedBytes {
    /// Create from raw bytes, computing checksum.
    pub fn new(data: bytes::Bytes) -> Self {
        let checksum = crc32c_checksum(&data);
        Self { data, checksum }
    }

    /// Create without checksum validation (for data from trusted source).
    pub fn new_unchecked(data: bytes::Bytes) -> Self {
        Self { data, checksum: 0 }
    }

    /// Validate integrity and return inner data.
    pub fn into_inner(self) -> std::io::Result<bytes::Bytes> {
        if self.checksum == 0 {
            return Ok(self.data);
        }
        let actual = crc32c_checksum(&self.data);
        if actual != self.checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "cache checksum mismatch: expected {:#x}, got {:#x}",
                    self.checksum, actual
                ),
            ));
        }
        Ok(self.data)
    }

    /// Get checksum for serialization.
    pub fn checksum(&self) -> u32 {
        self.checksum
    }

    /// Get reference to data without validation.
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

/// Compute CRC32C checksum.
fn crc32c_checksum(data: &[u8]) -> u32 {
    // Use a simple polynomial CRC32C implementation
    // CRC32C (Castagnoli) polynomial: 0x1EDC6F41
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0x82F63B78;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xFFFFFFFF
}

pub fn new_test_fs(op: opendal::Operator, cache_dir: std::path::PathBuf) -> MntrsFs {
    MntrsFs {
        op: Arc::new(op),
        inodes: Default::default(),
        dir_cache: Default::default(),
        cache_dir,
        handles: Default::default(),
        dir_cache_ttl: std::time::Duration::from_secs(10),
        attr_ttl: std::time::Duration::from_secs(1),
        stat_cache_ttl: std::time::Duration::from_secs(10),
        volname: "test".into(),
        cache_max_size: 1024 * 1024 * 1024,
        write_back_delay: std::time::Duration::from_secs(1),
        cache_mode: "writes".into(),
        read_ahead: 0,
        prefetch_threshold: 64 * 1024 * 1024,
        prefetch_queue_mb: 64,
        read_chunk_size: 0,
        read_chunk_size_limit: 0,
        read_chunk_streams: 1,
        uid: None,
        gid: None,
        umask: None,
        dir_perms: 0o755,
        file_perms: 0o644,
        link_perms: 0o777,
        direct_io: false,
        poll_interval: std::time::Duration::from_secs(60),
        cache_max_age: std::time::Duration::from_secs(3600),
        cache_min_free_space: 100 * 1024 * 1024,
        exclude_patterns: vec![],
        include_patterns: vec![],
        max_size: None,
        min_size: None,
        max_depth: None,
        ignore_case: false,
        fast_fingerprint: false,
        async_read: false,
        vfs_refresh: false,
        case_insensitive: false,
        no_implicit_dir: false,
        use_server_modtime: false,
        no_apple_double: false,
        no_apple_xattr: false,
        hash_filter: None,
        block_norm_dupes: false,
        write_wait: std::time::Duration::from_secs(0),
        read_wait: std::time::Duration::from_secs(0),
        cache_poll_interval: std::time::Duration::from_secs(60),
        handle_caching: std::time::Duration::from_secs(0),
        disk_total_size: 0,
        writeback_sender: std::sync::OnceLock::new(),
        // Unbounded mem_cache for unit tests. Production mounts
        // overwrite this in cmd/mount.rs after the size is known.
        mem_cache: std::sync::Arc::new(crate::cache::DashMapMemCache::new(0)),
        attr_cache: Default::default(),
        disk_cache_index: Default::default(),
        out_of_space: std::sync::atomic::AtomicBool::new(false),
        storage_class: None,
    }
}
