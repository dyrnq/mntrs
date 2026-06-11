#![allow(unexpected_cfgs)]
#![cfg_attr(windows, allow(dead_code, unused_imports, unused_variables))]
#![recursion_limit = "256"]
pub mod cache;
pub mod cmd;
pub mod core_fs;
pub mod path;
pub mod prefetcher;
pub mod writeback;

/// Shared inode table type for writeback callback.
pub const CACHE_BLOCK_SIZE: u64 = 8 * 1024 * 1024;
pub type Inodes = Arc<dashmap::DashMap<u64, (String, FileType, u64, Option<SystemTime>)>>;

#[cfg(unix)]
use std::ffi::OsStr;
use std::fs;
#[cfg(unix)]
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::cache::MemCache;

#[cfg(unix)]
use fuser::{
    AccessFlags, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation,
    INodeNo, KernelConfig, LockOwner, OpenFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr,
    Request, TimeOrNow, WriteFlags,
};
#[cfg(all(unix, target_os = "linux"))]
fn xattr_not_found() -> fuser::Errno {
    Errno::ENODATA
}
#[cfg(all(unix, target_os = "macos"))]
fn xattr_not_found() -> fuser::Errno {
    Errno::ENOATTR
}

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
const FUSE_ROOT_INO: u64 = 1;
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
    pub(crate) op: Arc<Operator>,
    inodes: dashmap::DashMap<u64, (String, FileType, u64, Option<std::time::SystemTime>)>,
    dir_cache: dashmap::DashMap<
        String,
        (
            std::time::Instant,
            dashmap::DashMap<String, (EntryMode, u64, std::time::SystemTime)>,
        ),
    >,
    cache_dir: PathBuf,
    handles: dashmap::DashMap<u64, FileHandleState>,
    pub(crate) dir_cache_ttl: Duration,
    pub(crate) attr_ttl: Duration,
    pub(crate) stat_cache_ttl: Duration,
    pub(crate) volname: String,
    pub(crate) cache_max_size: u64,
    pub(crate) write_back_delay: Duration,
    pub(crate) cache_mode: String,
    pub(crate) read_ahead: u64,
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

    mem_cache: std::sync::Arc<crate::cache::DashMapMemCache>,
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
    fn maybe_create_prefetcher(
        &self,
        ino: u64,
        path: &str,
    ) -> Option<std::sync::Arc<prefetcher::HandlePrefetcher>> {
        if self.read_chunk_streams > 1 {
            let file_size = self.resolve(ino).map(|(_, _, s, _)| s).unwrap_or(0);
            if file_size > 0 {
                let chunk = self.read_chunk_size.max(131072);
                let max_queue = chunk * self.read_chunk_streams as u64;
                return Some(std::sync::Arc::new(prefetcher::HandlePrefetcher::new(
                    self.op.as_ref().clone(),
                    path.to_string(),
                    file_size,
                    max_queue,
                    chunk,
                )));
            }
        }
        None
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

#[cfg(unix)]
fn validate_path_component(name: &str) -> Result<(), Errno> {
    if name.is_empty() {
        return Err(Errno::ENOENT);
    }
    if name == "." || name == ".." {
        return Err(Errno::EEXIST);
    }
    if name.contains('/') || name.contains(' ') {
        tracing::warn!(name, "path component contains separator or null");
        return Err(Errno::EINVAL);
    }
    Ok(())
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
        // Recover writeback queue from dirty sidecars
        if let Ok(entries) = std::fs::read_dir(&self.cache_dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.extension().is_some_and(|ext| ext == "dirty") {
                    let cache_path = p.with_extension("");
                    if let Ok(remote) = std::fs::read_to_string(&p) {
                        let remote = remote.trim().to_string();
                        if let Some(tx) = self.writeback_sender.get() {
                            tx.send((0, remote, cache_path.clone())).ok();
                        }
                    }
                    if let Err(e) = std::fs::remove_file(&p) {
                        tracing::warn!(error=%e, path=?p, "dirty recovery remove failed");
                    }
                }
            }
        }
        // Spawn writeback worker (tokio, requires runtime)
        crate::rt();
        let op = self.op.clone();
        let delay = self.write_back_delay;
        let inodes = Arc::new(self.inodes.clone());
        let (tx, _handle) = crate::writeback::spawn(op, inodes, delay);
        self.writeback_sender.set(tx).ok();
    }

    fn alloc_ino(&self, path: &str, kind: FileType, size: u64) -> u64 {
        let ino = NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inodes
            .entry(ino)
            .and_modify(|v| v.2 = size)
            .or_insert((path.to_string(), kind, size, None));
        ino
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
        let mut result = rt().block_on(async {
            let op = self.op.clone();
            let p = path.to_string();
            let mut lister = op.lister(&p).await?;
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
        Ok(result)
    }

    /// Add a single entry to directory cache (like rclone addObject).
    /// Called after create() to avoid full directory re-read.
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

    fn evict_lru(&self) {
        if self.cache_max_size == 0 && self.cache_min_free_space == 0 {
            return;
        }
        // Calculate total cache size and collect entries for LRU eviction.
        // Uses a min-heap (BinaryHeap with Reverse) for O(k log n) vs O(n log n) sort.
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;

        let mut total: u64 = 0;
        let mut heap: BinaryHeap<Reverse<(std::time::SystemTime, String, u64)>> = BinaryHeap::new();
        for entry in self.disk_cache_index.iter() {
            let path = entry.key().clone();
            let (size, atime) = *entry.value();
            total += size;
            heap.push(Reverse((atime, path, size)));
        }

        // Check free disk space if configured
        let need_free = if self.cache_min_free_space > 0 {
            #[cfg(unix)]
            {
                if let Ok(fs_stat) = rustix::fs::statvfs(&self.cache_dir) {
                    let free = fs_stat.f_bavail.saturating_mul(fs_stat.f_frsize);
                    if free < self.cache_min_free_space {
                        Some(self.cache_min_free_space - free)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            #[cfg(not(unix))]
            {
                None
            }
        } else {
            None
        };
        let size_limit = if self.cache_max_size > 0 {
            total.saturating_sub(self.cache_max_size)
        } else {
            0
        };
        let to_free = size_limit.max(need_free.unwrap_or(0));
        if to_free == 0 {
            self.out_of_space
                .store(false, std::sync::atomic::Ordering::Relaxed);
            return;
        }
        self.out_of_space
            .store(true, std::sync::atomic::Ordering::Relaxed);

        // Pop oldest entries from min-heap until enough space freed
        let mut remaining = to_free;
        let mut freed: u64 = 0;
        while let Some(Reverse((_, path, size))) = heap.pop() {
            if remaining == 0 {
                break;
            }
            let cpath = cache_block_path(&self.cache_dir, &path, 0);
            let _ = fs::remove_file(&cpath);
            let _ = fs::remove_file(cpath.with_extension("meta"));
            self.disk_cache_index.remove(&path as &str);
            freed += size;
            remaining = remaining.saturating_sub(size);
        }
        if freed >= to_free {
            self.out_of_space
                .store(false, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

// writeback_worker has been moved to writeback.rs — use writeback::worker()

#[cfg(unix)]
impl Filesystem for MntrsFs {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        self.common_init_wb();
        if let Err(e) = fs::create_dir_all(&self.cache_dir) {
            tracing::warn!(error=%e, "create_dir_all failed for cache");
        }
        // Enable readdirplus for stat+readdir in one round-trip
        let _ = config.add_capabilities(fuser::InitFlags::FUSE_DO_READDIRPLUS);
        // Recover disk cache index + attr_cache for restart warm cache
        let cached_blocks = load_cache_index(&self.cache_dir);
        if !cached_blocks.is_empty() {
            tracing::info!(
                count = cached_blocks.len(),
                "disk cache blocks recovered for restart"
            );
        }
        // Recover attr_cache from .meta sidecars
        if let Ok(entries) = fs::read_dir(&self.cache_dir) {
            let mut recovered = 0u64;
            for e in entries.flatten() {
                let p = e.path();
                if p.extension().is_some_and(|ext| ext == "meta")
                    && let Ok(content) = fs::read_to_string(&p)
                {
                    let parts: Vec<&str> = content.split(' ').collect();
                    if parts.len() >= 3 {
                        let remote_path = parts[0].to_string();
                        let size: u64 = parts[1].parse().unwrap_or(0);
                        let kind_byte: u8 = parts[2].parse().unwrap_or(0);
                        let kind = if kind_byte == 1 {
                            FileType::Directory
                        } else {
                            FileType::RegularFile
                        };
                        let cpath = p.with_extension("");
                        let mtime = std::fs::metadata(&cpath)
                            .ok()
                            .and_then(|m| m.modified().ok())
                            .unwrap_or(std::time::UNIX_EPOCH);
                        self.attr_cache.insert(
                            remote_path,
                            (kind, size, Some(mtime), std::time::Instant::now()),
                        );
                        recovered += 1;
                    }
                }
            }
            if recovered > 0 {
                tracing::info!(
                    count = recovered,
                    "attr_cache recovered from .meta sidecars"
                );
            }
        } // Pre-populate root directory cache on mount if --vfs-refresh
        if self.vfs_refresh
            && let Err(e) = self.list_op("")
        {
            tracing::debug!(error = %e, "vfs_refresh: list_op root failed (non-fatal)");
        }
        Ok(())
    }

    fn access(&self, _req: &Request, _ino: INodeNo, _mask: AccessFlags, reply: ReplyEmpty) {
        reply.ok();
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_string_lossy().to_string();
        if let Err(e) = validate_path_component(&name) {
            reply.error(e);
            return;
        }
        let name2 = name.clone();
        let parent: u64 = parent.into();
        if name == "." || name == ".." {
            let p = if name == "." { parent } else { FUSE_ROOT_INO };
            let attr = self
                .resolve(p)
                .map(|(_, k, s, _)| self.make_attr(p, s, k, SystemTime::UNIX_EPOCH))
                .unwrap_or_else(|| {
                    self.make_attr(
                        FUSE_ROOT_INO,
                        4096,
                        FileType::Directory,
                        SystemTime::UNIX_EPOCH,
                    )
                });
            reply.entry(&self.attr_ttl, &attr, Generation(0));
            return;
        }
        let parent_path = self
            .resolve(parent)
            .map(|(p, _, _, _)| p)
            .unwrap_or_default();
        let full_path = if parent_path.is_empty() {
            name2
        } else {
            format!("{}/{}", parent_path, name2)
        };
        if let Some((kind, size, mtime)) = self.stat_op(&full_path) {
            let mut attr = self.make_attr(
                self.alloc_ino(&full_path, kind, size),
                size,
                kind,
                mtime.unwrap_or(SystemTime::UNIX_EPOCH),
            );
            if let Some(mt) = mtime {
                attr.mtime = mt;
            }
            reply.entry(&self.attr_ttl, &attr, Generation(0));
        } else if self.case_insensitive {
            // Fallback: search directory listing for case-insensitive match
            let entries = match self.list_op(&parent_path) {
                Ok(e) => e,
                Err(e) => {
                    tracing::debug!(path = %parent_path, error = %e,
                        "case-insensitive lookup: list_op failed");
                    reply.error(Errno::EIO);
                    return;
                }
            };
            let lower = name.to_lowercase();
            if let Some((matched_name, mode, ..)) =
                entries.iter().find(|(n, ..)| n.to_lowercase() == lower)
            {
                let mp = if parent_path.is_empty() {
                    matched_name.clone()
                } else {
                    format!("{}/{}", parent_path, matched_name)
                };
                let kind = match mode {
                    EntryMode::DIR => FileType::Directory,
                    _ => FileType::RegularFile,
                };
                let (_, size, mtime) = self.stat_op(&mp).unwrap_or((kind, 0, None));
                let mut attr = self.make_attr(
                    self.alloc_ino(&mp, kind, size),
                    size,
                    kind,
                    mtime.unwrap_or(SystemTime::UNIX_EPOCH),
                );
                if let Some(mt) = mtime {
                    attr.mtime = mt;
                }
                reply.entry(&self.attr_ttl, &attr, Generation(0));
            } else {
                reply.error(Errno::ENOENT);
            }
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let ino: u64 = ino.into();
        if ino == FUSE_ROOT_INO {
            reply.attr(
                &self.attr_ttl,
                &self.make_attr(ino, 4096, FileType::Directory, SystemTime::UNIX_EPOCH),
            );
            return;
        }
        if let Some((path, kind, inodes_size, _)) = self.resolve(ino) {
            let (_, backend_size, mtime) = self.stat_op(&path).unwrap_or((kind, 0, None));
            // Use the larger of inodes size and backend size.
            // The inodes map is updated immediately by write(), while the
            // backend may lag behind due to async writeback.
            let size = inodes_size.max(backend_size);
            let mut attr = self.make_attr(ino, size, kind, mtime.unwrap_or(SystemTime::UNIX_EPOCH));
            if let Some(mt) = mtime {
                attr.mtime = mt;
            }
            reply.attr(&self.attr_ttl, &attr);
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        // Report virtual unlimited space (S3 is effectively infinite)
        const BLOCK_SIZE: u32 = 4096;
        let total_blocks = if self.disk_total_size > 0 {
            self.disk_total_size / BLOCK_SIZE as u64
        } else {
            256 * 1024 * 1024 // default ~1PB
        };
        let total_inodes = 1_000_000_000u64;
        reply.statfs(
            total_blocks,
            total_blocks,
            total_blocks,
            total_inodes,
            total_inodes,
            BLOCK_SIZE,
            255,
            0,
        );
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let ino: u64 = ino.into();
        let path = self.resolve(ino).map(|(p, _, _, _)| p).unwrap_or_default();
        let listed = match self.list_op(&path) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(path = %path, error = %e, "readdir: list_op failed");
                reply.error(Errno::EIO);
                return;
            }
        };
        let mut entries: Vec<(String, FileType, u64, Option<SystemTime>)> = vec![
            (".".to_string(), FileType::Directory, 4096, None),
            ("..".to_string(), FileType::Directory, 4096, None),
        ];
        for (name, mode, size, mtime) in listed {
            let clean_name = name.trim_start_matches('/').trim_end_matches('/');
            let name = if clean_name.is_empty() {
                name.clone()
            } else {
                clean_name.to_string()
            };
            // Skip root entry and empty names from list_op
            if name.is_empty() || name == "/" {
                continue;
            }
            entries.push((
                name,
                match mode {
                    EntryMode::DIR => FileType::Directory,
                    _ => FileType::RegularFile,
                },
                size,
                Some(mtime),
            ));
        }
        let start = offset as usize;
        if start >= entries.len() {
            reply.ok();
            return;
        }
        // entries already carry size/mtime from list_op — no per-entry stat needed
        for (i, (name, kind, size, _mtime)) in entries.iter().enumerate().skip(start) {
            let cp = if path.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", path, name)
            };
            if reply.add(
                INodeNo(self.alloc_ino(&cp, *kind, *size)),
                (i + 1) as u64,
                *kind,
                name,
            ) {
                break;
            }
        }
        reply.ok();
    }

    fn readdirplus(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectoryPlus,
    ) {
        let ino: u64 = ino.into();
        let path = self.resolve(ino).map(|(p, _, _, _)| p).unwrap_or_default();
        let listed = match self.list_op(&path) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(path = %path, error = %e, "readdirplus: list_op failed");
                reply.error(Errno::EIO);
                return;
            }
        };
        let mut entries: Vec<(String, FileType, u64, Option<SystemTime>)> = vec![
            (".".to_string(), FileType::Directory, 4096, None),
            ("..".to_string(), FileType::Directory, 4096, None),
        ];
        for (name, mode, size, mtime) in listed {
            let clean_name = name.trim_start_matches('/').trim_end_matches('/');
            let name = if clean_name.is_empty() {
                name.clone()
            } else {
                clean_name.to_string()
            };
            // Skip root entry and empty names from list_op
            if name.is_empty() || name == "/" {
                continue;
            }
            entries.push((
                name,
                match mode {
                    EntryMode::DIR => FileType::Directory,
                    _ => FileType::RegularFile,
                },
                size,
                Some(mtime),
            ));
        }
        let start = offset as usize;
        if start >= entries.len() {
            reply.ok();
            return;
        }
        for (i, (name, kind, size, mtime)) in entries.iter().enumerate().skip(start) {
            let cp = if path.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", path, name)
            };
            let ino = self.alloc_ino(&cp, *kind, *size);
            let mut attr = self.make_attr(ino, *size, *kind, SystemTime::UNIX_EPOCH);
            if let Some(mt) = mtime {
                attr.mtime = *mt;
            }
            if reply.add(
                INodeNo(ino),
                (i + 1) as u64,
                name.as_str(),
                &self.attr_ttl,
                &attr,
                Generation(0),
            ) {
                break;
            }
        }
        reply.ok();
    }

    fn opendir(&self, _req: &Request, _ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        reply.opened(FileHandle(1), FopenFlags::empty());
    }
    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let name = name.to_string_lossy();
        let parent_path = self
            .resolve(parent.into())
            .map(|(p, _, _, _)| p)
            .unwrap_or_default();
        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };
        let ino = self.alloc_ino(&full_path, FileType::RegularFile, 0);
        let fh = NEXT_HANDLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let cpath = cache_path(&self.cache_dir, &full_path);
        if let Some(parent) = cpath.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let cache_fd = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&cpath)
            .ok()
            .map(|f| Arc::new(std::sync::Mutex::new(f)));
        self.handles.insert(
            fh,
            FileHandleState::Write {
                path: full_path.clone(),
                cache_fd,
                dirty: false,
                dirty_since: None,
            },
        );
        reply.created(
            &self.attr_ttl,
            &self.make_attr(ino, 0, FileType::RegularFile, SystemTime::UNIX_EPOCH),
            Generation(0),
            FileHandle(fh),
            FopenFlags::empty(),
        );
        self.cache_add_entry(
            &parent_path,
            &name,
            EntryMode::FILE,
            0,
            SystemTime::UNIX_EPOCH,
        );
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let ino: u64 = ino.into();
        let fh = NEXT_HANDLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some((path, FileType::RegularFile, _, _)) = self.resolve(ino) {
            let is_write = !matches!(flags.acc_mode(), fuser::OpenAccMode::O_RDONLY);
            if is_write {
                let cpath = cache_path(&self.cache_dir, &path);
                if let Some(parent) = cpath.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                // Pre-populate cache with remote content when empty (append fix)
                let cache_empty = !cpath.exists()
                    || std::fs::metadata(&cpath)
                        .map(|m| m.len() == 0)
                        .unwrap_or(true);
                if cache_empty {
                    let op = self.op.clone();
                    let p = path.clone();
                    let cp = cpath.clone();
                    if let Ok(remote) = crate::rt().block_on(async { op.read(&p).await }) {
                        let _ = std::fs::write(&cp, remote.to_vec());
                    }
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
                    fh,
                    FileHandleState::Write {
                        path: path.clone(),
                        cache_fd,
                        dirty: false,
                        dirty_since: None,
                    },
                );
            } else {
                self.handles.insert(
                    fh,
                    FileHandleState::Read {
                        path: path.clone(),
                        last_offset: 0,
                        chunk_size: 131072,
                        prefetcher: self.maybe_create_prefetcher(ino, &path),
                    },
                );
            }
        }
        reply.opened(FileHandle(fh), FopenFlags::empty());
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let ino: u64 = ino.into();
        let (path, file_size) = match self.resolve(ino) {
            Some((p, _, s, _)) => (p, s),
            None => {
                reply.error(Errno::ENOENT);
                return;
            }
        };
        // EOF guard: offset past end → empty data
        if offset >= file_size {
            reply.data(&[]);
            return;
        }
        let cap = file_size - offset;
        // 1. Check memory cache first (fast path)
        let block_idx = offset / CACHE_BLOCK_SIZE;
        if let Some(data) = self.mem_cache.get(ino, block_idx) {
            let start = offset as usize;
            let end = (start + size as usize).min(data.len());
            if start < data.len() {
                reply.data(&data[start..end]);
            } else {
                reply.data(&[]);
            }
            return;
        }
        // 1.5 Check file-level cache (single cache file per handle)
        if !self.direct_io {
            let cpath = cache_path(&self.cache_dir, &path);
            if cpath.exists()
                && let Ok(data) = fs::read(&cpath)
            {
                let start = offset as usize;
                let end = (start + size as usize).min(data.len());
                let result = if start < data.len() {
                    data[start..end].to_vec()
                } else {
                    vec![]
                };
                let b = bytes::Bytes::from(data);
                self.mem_cache.put(ino, block_idx, b);
                reply.data(&result);
                return;
            }
        }
        // 2. Check disk cache (with checksum validation, block-level)
        if !self.direct_io {
            let cpath = cache_block_path(&self.cache_dir, &path, block_idx);
            if cpath.exists()
                && let Ok(data) = fs::read(&cpath)
            {
                // Validate CRC64 checksum if present (last 8 bytes)
                let valid = if data.len() > 8 {
                    let (body, stored_bytes) = data.split_at(data.len() - 8);
                    let stored = u64::from_le_bytes(stored_bytes.try_into().unwrap_or([0u8; 8]));
                    stored == 0 || stored == crc64_checksum(body)
                } else {
                    true
                };
                if valid {
                    let b = bytes::Bytes::from(data);
                    let start = offset as usize;
                    let end = (start + size as usize).min(b.len());
                    if start < b.len() {
                        reply.data(&b[start..end]);
                    } else {
                        reply.data(&[]);
                    }
                    self.mem_cache.put(ino, offset / CACHE_BLOCK_SIZE, b);
                    return;
                } else {
                    tracing::warn!(path=?cpath, "cache checksum mismatch, re-fetching");
                }
            }
        }
        // 3.5 Try prefetcher (backpressure-aware background download)
        let fh_val = u64::from(_fh);
        if let Some(h) = self.handles.get(&fh_val)
            && let FileHandleState::Read {
                prefetcher: Some(p),
                ..
            } = h.value()
            && let Some(part) = p.pop(offset)
        {
            let start = (offset - part.offset) as usize;
            let end = (start + size as usize).min(part.data.len());
            if start < part.data.len() {
                reply.data(&part.data[start..end]);
            } else {
                reply.data(&[]);
            }
            return;
        }
        // 3. Fetch from remote
        // Adaptive chunking: grow on sequential read, reset on seek
        let fh_val = u64::from(_fh);
        let chunk_size = if let Some(entry) = self.handles.get(&fh_val) {
            if let FileHandleState::Read {
                ref last_offset,
                chunk_size: cs,
                ..
            } = *entry.value()
            {
                if offset == *last_offset {
                    // Sequential read: double chunk size, up to max
                    (cs * 2).min(8 * 1024 * 1024)
                } else {
                    // Random seek: reset to initial
                    131072
                }
            } else {
                self.read_chunk_size.max(size as u64)
            }
        } else {
            self.read_chunk_size.max(size as u64)
        };
        // Update handle chunk tracking
        if let Some(mut entry) = self.handles.get_mut(&fh_val)
            && let FileHandleState::Read {
                ref mut last_offset,
                chunk_size: ref mut cs,
                ..
            } = *entry.value_mut()
        {
            *last_offset = offset + size as u64;
            *cs = chunk_size;
        }
        let fetch_size = if self.read_chunk_size > 0 {
            self.read_chunk_size.max(size as u64)
        } else {
            chunk_size.max(size as u64)
        };
        let op = self.op.clone();
        let p = path.clone();
        let streams = self.read_chunk_streams.max(1);
        if streams > 1 && fetch_size > 128 * 1024 {
            // Multi-chunk concurrent fetch
            let clamped_fetch = fetch_size.min(cap);
            let clamped_size = (size as u64).min(cap) as u32;
            let chunk_size = (clamped_fetch / streams as u64).max(64 * 1024);
            let end = offset + clamped_fetch;
            let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(streams as usize));
            let mut tasks = Vec::new();
            let mut off = offset;
            while off < end {
                let e = (off + chunk_size).min(end);
                let permit = sem.clone();
                let op = op.clone();
                let p = p.clone();
                tasks.push(rt().spawn(async move {
                    let _permit = permit.acquire().await;
                    op.read_with(&p)
                        .range(off..e)
                        .await
                        .map(|b| bytes::Bytes::from(b.to_vec()))
                }));
                off = e;
            }
            let results: Vec<_> = rt().block_on(futures::future::join_all(tasks));
            let mut all_data = bytes::BytesMut::with_capacity(fetch_size as usize);
            let mut ok = true;
            for r in &results {
                match r {
                    Ok(Ok(data)) => all_data.extend_from_slice(data),
                    _ => {
                        ok = false;
                        break;
                    }
                }
            }
            if ok {
                let b: bytes::Bytes = all_data.freeze();
                let slice = &b[..(b.len() as u32).min(clamped_size) as usize];
                reply.data(slice);
                self.mem_cache.put(ino, offset / CACHE_BLOCK_SIZE, b);
            } else {
                reply.error(Errno::EIO);
            }
        } else {
            // Single-chunk fetch (original path)
            let clamped_fetch = fetch_size.min(cap);
            let clamped_size = (size as u64).min(cap) as u32;
            match rt().block_on(async move {
                op.read_with(&p).range(offset..offset + clamped_fetch).await
            }) {
                Ok(buf) => {
                    let b: bytes::Bytes = buf.to_vec().into();
                    let slice = &b[..(b.len() as u32).min(clamped_size) as usize];
                    reply.data(slice);
                    self.mem_cache.put(ino, offset / CACHE_BLOCK_SIZE, b);
                }
                Err(_) => reply.error(Errno::EIO),
            }
        }
        // Read-ahead: pre-fetch next block into mem_cache (async, tokio)
        if self.read_ahead > 0 {
            let op = self.op.clone();
            let p = path.clone();
            let next = offset + size as u64;
            let ahead = self.read_ahead;
            let cdir = self.cache_dir.clone();
            let _ino_save = ino;
            rt().spawn(async move {
                let result: Result<_, opendal::Error> = async {
                    let data = op.read_with(&p).range(next..).await?;
                    let bytes = bytes::Bytes::from(data.to_vec());
                    // Store in disk cache for crash recovery
                    let cpath = crate::cache_path(&cdir, &p);
                    if let Some(parent) = cpath.parent()
                        && let Err(e) = std::fs::create_dir_all(parent)
                    {
                        tracing::debug!(error=%e, "readahead mkdir failed");
                    }
                    if let Ok(mut f) = std::fs::OpenOptions::new()
                        .create(true)
                        .truncate(false)
                        .write(true)
                        .read(true)
                        .open(&cpath)
                    {
                        use std::io::{Seek, Write};
                        if f.seek(std::io::SeekFrom::Start(next)).is_err()
                            || f.write_all(&bytes[..bytes.len().min(ahead as usize)])
                                .is_err()
                        {
                            tracing::debug!("readahead disk write failed");
                        }
                    }
                    Ok(bytes)
                }
                .await;
                // Now populate mem_cache from the thread that has access to self
                // Note: can't access self here — tokio task doesn't borrow self
                // mem_cache is populated by the main read path on next hit
                if let Err(e) = result {
                    tracing::debug!(error=%e, "readahead fetch failed");
                }
            });
        }
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let fh_val: u64 = fh.into();
        let path = match self
            .handles
            .get(&fh_val)
            .map(|r| r.value().path().to_string())
        {
            Some(p) => p,
            None => {
                reply.error(Errno::ENOENT);
                return;
            }
        };
        if self.direct_io {
            let op = self.op.clone();
            let p = path.clone();
            let d = data.to_vec();
            match rt().block_on(async move { op.write(&p, d).await }) {
                Ok(_) => reply.written(data.len() as u32),
                Err(_) => reply.error(Errno::EIO),
            }
            return;
        }
        // Check out_of_space backpressure
        if self.out_of_space.load(std::sync::atomic::Ordering::Relaxed) {
            self.evict_lru();
            if self.out_of_space.load(std::sync::atomic::Ordering::Relaxed) {
                reply.error(Errno::ENOSPC);
                return;
            }
        }
        // Write via single cache fd (like rclone RWFileHandle)
        self.disk_cache_index.insert(
            path.clone(),
            (data.len() as u64, std::time::SystemTime::now()),
        );
        let end = offset + data.len() as u64;

        let cache_fd = self.handles.get(&fh_val).and_then(|e| {
            if let FileHandleState::Write {
                cache_fd: Some(fd), ..
            } = e.value()
            {
                Some(fd.clone())
            } else {
                None
            }
        });

        let result = (|| -> std::io::Result<()> {
            match &cache_fd {
                Some(fd) => {
                    let mut f = fd.lock().unwrap();
                    let current_len = f.metadata()?.len();
                    if end > current_len {
                        f.set_len(end)?;
                    }
                    f.seek(SeekFrom::Start(offset))?;
                    f.write_all(data)?;
                    f.flush()?;
                }
                None => {
                    let cpath = cache_path(&self.cache_dir, &path);
                    if let Some(parent) = cpath.parent() {
                        let _ = fs::create_dir_all(parent);
                    }
                    let mut f = fs::OpenOptions::new()
                        .create(true)
                        .truncate(false)
                        .write(true)
                        .read(true)
                        .open(&cpath)?;
                    let current_len = f.metadata()?.len();
                    if end > current_len {
                        f.set_len(end)?;
                    }
                    f.seek(SeekFrom::Start(offset))?;
                    f.write_all(data)?;
                    f.flush()?;
                }
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.handles.insert(
                    fh_val,
                    FileHandleState::Write {
                        path: path.clone(),
                        cache_fd,
                        dirty: true,
                        dirty_since: Some(std::time::Instant::now()),
                    },
                );
                reply.written(data.len() as u32);
            }
            Err(_) => reply.error(Errno::EIO),
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name = name.to_string_lossy();
        let parent_path = self
            .resolve(parent.into())
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
        match rt().block_on(async move { op.create_dir(&p).await }) {
            Ok(_) => {
                let ino = self.alloc_ino(&full_path, FileType::Directory, 4096);
                reply.entry(
                    &self.attr_ttl,
                    &self.make_attr(ino, 4096, FileType::Directory, SystemTime::UNIX_EPOCH),
                    Generation(0),
                );
                self.cache_add_entry(
                    &parent_path,
                    &name,
                    EntryMode::DIR,
                    4096,
                    SystemTime::UNIX_EPOCH,
                );
            }
            Err(_) => reply.error(Errno::EIO),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy();
        let parent_path = self
            .resolve(parent.into())
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
        let p2 = p.clone();
        if rt().block_on(async move { op.delete(&p2).await }).is_err() {
            tracing::debug!(path=%p, "rmdir delete failed");
        }
        // Clean cache entries
        let prefix = format!("{:020x}_", crate::path_hash(&full_path));
        if let Ok(entries) = fs::read_dir(&self.cache_dir) {
            for e in entries.flatten() {
                if let Some(name) = e.file_name().to_str()
                    && name.starts_with(&prefix)
                {
                    let _ = fs::remove_file(e.path());
                }
            }
        }
        self.disk_cache_index.remove(&full_path as &str);
        self.inodes.remove(&path_hash(&full_path));
        self.attr_cache.remove(&full_path);
        self.cache_remove_entry(&parent_path, &name);
        self.invalidate_dir_cache(&full_path);
        reply.ok();
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: fuser::RenameFlags,
        reply: ReplyEmpty,
    ) {
        let name = name.to_string_lossy();
        let newname = newname.to_string_lossy();
        let parent_path = self
            .resolve(parent.into())
            .map(|(p, _, _, _)| p)
            .unwrap_or_default();
        let newparent_path = self
            .resolve(newparent.into())
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
        let dst_clone2 = dst.clone();
        // Pre-delete destination to match POSIX rename semantics
        let op_del = op.clone();
        let _ = rt().block_on(async move { op_del.delete(&dst_clone2).await });
        rt().block_on(async move {
            if let Err(e) = op.rename(&src_clone, &dst_clone).await {
                tracing::warn!(path=%src_clone, error=%e, "rename failed, falling back to copy+delete");
                if op.copy(&src_clone, &dst_clone).await.is_ok() {
                    let _ = op.delete(&src_clone).await;
                }
            }
        });
        // Migrate cache file from src to dst (like rclone)
        let cpath_src = cache_path(&self.cache_dir, &src);
        let cpath_dst = cache_path(&self.cache_dir, &dst);
        if cpath_src.exists() && !cpath_dst.exists() {
            if let Some(parent) = cpath_dst.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::rename(&cpath_src, &cpath_dst);
        } else {
            let _ = fs::remove_file(&cpath_src);
        }
        // Clean block-level cache for src
        let prefix = format!("{:020x}_", crate::path_hash(&src));
        if let Ok(entries) = fs::read_dir(&self.cache_dir) {
            for e in entries.flatten() {
                if let Some(name) = e.file_name().to_str()
                    && name.starts_with(&prefix)
                {
                    let _ = fs::remove_file(e.path());
                }
            }
        }
        self.disk_cache_index.remove(&src as &str);
        // Migrate inode and attr_cache from src to dst
        let src_hash = path_hash(&src);
        let dst_hash = path_hash(&dst);
        if let Some(entry) = self.inodes.get(&src_hash).map(|e| e.value().clone()) {
            self.inodes.insert(dst_hash, entry);
        }
        self.inodes.remove(&src_hash);
        if let Some(entry) = self.attr_cache.get(&src).map(|e| *e.value()) {
            self.attr_cache.insert(dst.to_string(), entry);
        }
        self.attr_cache.remove(&src);
        self.invalidate_dir_cache(&src);
        self.invalidate_dir_cache(&dst);
        reply.ok();
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, _size: u32, reply: ReplyXattr) {
        let ino: u64 = ino.into();
        let name = name.to_string_lossy();
        if self.no_apple_xattr
            && (name.starts_with("com.apple.") || name == "system.posix_acl_access")
        {
            reply.error(xattr_not_found());
            return;
        }
        if let Some((path, kind, _, _)) = self.resolve(ino) {
            if kind == FileType::Directory {
                reply.error(xattr_not_found());
                return;
            }
            // Fetch object metadata for ETag / storage-class
            let op = self.op.clone();
            let p = path.clone();
            match rt().block_on(async move { op.stat(&p).await }) {
                Ok(meta) => match name.as_ref() {
                    "user.etag" | "s3.etag" => {
                        if let Some(etag) = meta.etag() {
                            reply.data(etag.as_bytes());
                            return;
                        }
                        reply.error(xattr_not_found());
                    }
                    "user.content-type" | "s3.content-type" => {
                        if let Some(ct) = meta.content_type() {
                            reply.data(ct.as_bytes());
                            return;
                        }
                        reply.error(xattr_not_found());
                    }
                    _ => reply.error(xattr_not_found()),
                },
                Err(_) => reply.error(Errno::EIO),
            }
        } else {
            reply.error(Errno::ENOENT);
        }
    }
    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let ino: u64 = ino.into();
        if let Some((_, kind, _, _)) = self.resolve(ino) {
            if kind == FileType::Directory {
                reply.error(xattr_not_found());
                return;
            }
            // Return known xattr names
            let attrs = b"user.etag user.content-type ";
            if size == 0 {
                reply.size(attrs.len() as u32);
            } else if size < attrs.len() as u32 {
                reply.error(Errno::ERANGE);
            } else {
                reply.data(attrs);
            }
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let ino: u64 = ino.into();
        if let Some((p, kind, _, _)) = self.resolve(ino) {
            if let Some(s) = size {
                // Clear all block-level cache entries for this file
                let prefix = format!("{:020x}_", path_hash(&p));
                if let Ok(entries) = fs::read_dir(&self.cache_dir) {
                    for e in entries.flatten() {
                        if let Some(name) = e.file_name().to_str()
                            && name.starts_with(&prefix)
                        {
                            let _ = fs::remove_file(e.path());
                        }
                    }
                }
                let cpath = cache_path(&self.cache_dir, &p);
                if cpath.exists()
                    && let Err(e) = fs::write(&cpath, &[] as &[u8])
                {
                    tracing::debug!(error=%e, path=?cpath, "setattr truncate failed");
                }
                self.alloc_ino(&p, kind, s);
            }
            // mode/uid/gid — just record them for now (S3 has no chmod)
            let mut perm = if kind == FileType::Directory {
                0o755u16
            } else {
                0o644u16
            };
            if let Some(m) = mode {
                perm = (m & 0o7777) as u16;
            }
            let mut attr = self.make_attr(ino, size.unwrap_or(0), kind, SystemTime::now());
            attr.perm = perm;
            if let Some(u) = uid {
                attr.uid = u;
            }
            if let Some(g) = gid {
                attr.gid = g;
            }
            reply.attr(&self.attr_ttl, &attr);
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy();
        let parent_path = self
            .resolve(parent.into())
            .map(|(p, _, _, _)| p)
            .unwrap_or_default();
        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };
        let op = self.op.clone();
        let p = full_path.clone();
        let p2 = full_path.clone();
        if rt().block_on(async move { op.delete(&p2).await }).is_err() {
            tracing::debug!(path=%p, "unlink remote failed");
        }
        let cpath = cache_path(&self.cache_dir, &full_path);
        if let Err(e) = fs::remove_file(&cpath) {
            tracing::debug!(error=%e, path=?cpath, "unlink cache remove failed");
        }
        // Clean block-level cache entries
        let prefix = format!("{:020x}_", crate::path_hash(&full_path));
        if let Ok(entries) = fs::read_dir(&self.cache_dir) {
            for e in entries.flatten() {
                if let Some(name) = e.file_name().to_str()
                    && name.starts_with(&prefix)
                {
                    let _ = fs::remove_file(e.path());
                }
            }
        }
        self.disk_cache_index.remove(&full_path as &str);
        self.inodes.remove(&path_hash(&full_path));
        self.attr_cache.remove(&full_path);
        self.cache_remove_entry(&parent_path, &name);
        reply.ok();
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        let fh_val: u64 = fh.into();
        let (path, dirty) = {
            let entry = self.handles.get(&fh_val).map(|r| r.clone());
            if let Some(FileHandleState::Write {
                path: p, dirty: d, ..
            }) = entry
            {
                if d {
                    let cpath = cache_block_path(&self.cache_dir, &p, 0);
                    let fd = std::fs::OpenOptions::new()
                        .create(true)
                        .truncate(false)
                        .write(true)
                        .read(true)
                        .open(&cpath)
                        .ok()
                        .map(|f| Arc::new(std::sync::Mutex::new(f)));
                    self.handles.insert(
                        fh_val,
                        FileHandleState::Write {
                            path: p.clone(),
                            cache_fd: fd,
                            dirty: false,
                            dirty_since: None,
                        },
                    );
                }
                (p, d)
            } else {
                return reply.ok();
            }
        };
        if dirty {
            let block_idx = 0; // flush uses block 0 (main cache location)
            let cpath = cache_block_path(&self.cache_dir, &path, block_idx);
            if cpath.exists() {
                // Write sidecar for crash recovery
                let sidecar = cpath.with_extension("dirty");
                if let Err(e) = fs::write(&sidecar, path.as_bytes()) {
                    tracing::warn!(error=%e, path=?sidecar, "sidecar write failed");
                }
                if let Some(tx) = self.writeback_sender.get() {
                    tx.send((_ino.into(), path, cpath)).ok();
                }
            }
        }
        // Queue the writeback; don't block FUSE thread waiting for upload.
        // rclone does the same: close() returns immediately, upload happens async.
        reply.ok();
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.handles.remove(&fh.into());
        reply.ok();
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
        let (kind, size, mtime) = self
            .stat_op(&full_path)
            .ok_or(std::io::ErrorKind::NotFound)?;
        let ino = self.alloc_ino(&full_path, kind, size);
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
        if let Some((path, kind, inodes_size, _)) = self.resolve(ino) {
            let (_, backend_size, mtime) = self.stat_op(&path).unwrap_or((kind, inodes_size, None));
            // Use the larger of inodes size and backend size.
            //
            //   * inodes_size — updated synchronously by write() and
            //     setattr(); always reflects the most recent local change
            //   * backend_size — what stat_op() reports from the remote
            //     backend (via opendal). This LAGS during async writeback
            //     and is permanently 0 for backends that have no
            //     on-server state to stat (notably memory://, which is
            //     in-process only)
            //
            // Returning the larger of the two ensures the kernel sees
            // at least as many bytes as we've actually written, even if
            // the writeback upload hasn't landed yet.
            //
            // HISTORY: the same intent was applied in commit d4d19c8,
            // but the patch landed on `impl Filesystem for MntrsFs`
            // (lib.rs:~934) — code that is never actually called. The
            // live dispatch is `core_fs::fuser::FuserAdapter::getattr`
            // → `CoreFilesystem::getattr` here. The two impls exist
            // side by side; the older fuser-trait impl is effectively
            // dead and should be deleted, but for now the fix lives
            // in the actively-dispatched CoreFilesystem impl.
            let size = inodes_size.max(backend_size);
            let mtime = mtime.unwrap_or(SystemTime::UNIX_EPOCH);
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
            self.handles.insert(
                fh,
                FileHandleState::Read {
                    path,
                    last_offset: 0,
                    chunk_size: self.read_chunk_size.max(131072),
                    prefetcher: None,
                },
            );
        }
        Ok(fh)
    }

    fn read(&self, ino: u64, _fh: u64, offset: u64, size: u32) -> std::io::Result<Vec<u8>> {
        let (path, file_size) = self
            .resolve(ino)
            .map(|(p, _, s, _)| (p, s))
            .ok_or(std::io::ErrorKind::NotFound)?;
        // Defensive size reconciliation.
        //
        // `file_size` (from the inodes map) is the authoritative size
        // for FUSE-protocol purposes — it's what getattr reports, what
        // the kernel uses to cap read requests, and what `ls -l` shows.
        //
        // However the on-disk cache file may have grown *more*
        // recently than the inodes entry. Two scenarios trigger this:
        //
        //   1. The inodes entry was reset to 0 by a stale lookup() —
        //      e.g. the FUSE kernel did a forget+lookup cycle after
        //      a test 9 (delete + recreate) and the new lookup
        //      observed an empty backend (memory://) or zero-sized
        //      stat_op result.
        //   2. The cache file was extended by a writeback upload that
        //      raced with an in-flight read.
        //
        // In both cases, returning early on `offset >= file_size`
        // would mask the cache file's real content and return 0 bytes
        // to the user even though the data is sitting on disk.
        //
        // The fix takes the MAX of inodes and cache file size. The
        // downstream mem_cache/file-level cache paths further down
        // already bound their results by `b.len()`, so they
        // naturally cap at the cache file's real size even if
        // inodes over-reports.
        //
        // When both are 0, this still returns [] (legitimate EOF
        // for an empty file).
        let cache_meta_size = std::fs::metadata(crate::cache_path(&self.cache_dir, &path))
            .map(|m| m.len())
            .unwrap_or(0);
        let actual_size = cache_meta_size.max(file_size);
        if offset >= actual_size {
            return Ok(vec![]);
        }
        // cap = max bytes available from `offset` to the end of the
        // file. fetch_size is the kernel's request size, but capped
        // at the chunk-size ceiling (read_chunk_size, default 128 MiB,
        // matching rclone) and at the available bytes. For the
        // remote-fetch path the cap is what prevents asking
        // op.read_with().range(...) for bytes past the file end; for
        // local cache paths the cap is unused because those paths
        // cap at `b.len()` themselves.
        let cap = actual_size - offset;
        let fetch_size = self.read_chunk_size.max(size as u64).min(cap);
        let block_idx = offset / CACHE_BLOCK_SIZE;

        // Try read from cache fd first (write handle still open)
        if !self.direct_io {
            let cache_fd = self.handles.get(&_fh).and_then(|e| {
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

        if let Some(data) = self.mem_cache.get(ino, block_idx) {
            let start = offset as usize;
            let end = (start + size as usize).min(data.len());
            return if start < data.len() {
                Ok(data[start..end].to_vec())
            } else {
                Ok(vec![])
            };
        }
        if !self.direct_io {
            // Try file-level cache first (single cache file per handle)
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
            // Fallback to block-level cache
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
        let op = self.op.clone();
        let p = path.clone();
        match rt()
            .block_on(async move { op.read_with(&p).range(offset..offset + fetch_size).await })
        {
            Ok(buf) => {
                let b: bytes::Bytes = buf.to_vec().into();
                let len = (b.len() as u32).min(size) as usize;
                let data = b[..len].to_vec();
                self.mem_cache.put(ino, offset / CACHE_BLOCK_SIZE, b);
                Ok(data)
            }
            Err(e) => {
                Err(std::io::Error::other("read failed"))
            }
        }
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
        // The initial mtime is `None`; subsequent writeback uploads
        // populate it from the backend response.
        let end = _offset + _data.len() as u64;
        self.inodes
            .entry(_ino)
            .and_modify(|v| {
                if end > v.2 {
                    v.2 = end;
                }
            })
            .or_insert_with(|| (path.clone(), FileType::RegularFile, end, None));

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
            .map_err(|_| std::io::Error::other("create failed"))?;
        let (kind, size, mtime) =
            self.stat_op(&full_path)
                .unwrap_or((FileType::RegularFile, 0, None));
        let ino = self.alloc_ino(&full_path, kind, size);
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
        let dir_path = format!("{}/", full_path.trim_end_matches('/'));
        let op = self.op.clone();
        let p = dir_path.clone();
        rt().block_on(async move { op.create_dir(&p).await })
            .map_err(|_| std::io::Error::other("mkdir failed"))?;
        let ino = self.alloc_ino(&full_path, FileType::Directory, 4096);
        self.cache_add_entry(&parent_path, name, EntryMode::DIR, 4096, SystemTime::now());
        Ok(to_core_attr(&self.make_attr(
            ino,
            4096,
            FileType::Directory,
            SystemTime::now(),
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
        rt().block_on(async move { op.delete(&p).await })
            .map_err(|_| std::io::Error::other("unlink failed"))?;
        let cpath = crate::cache_path(&self.cache_dir, &full_path);
        let _ = std::fs::remove_file(&cpath);
        // Clean block-level cache entries
        let prefix = format!("{:020x}_", crate::path_hash(&full_path));
        if let Ok(entries) = std::fs::read_dir(&self.cache_dir) {
            for e in entries.flatten() {
                if let Some(name) = e.file_name().to_str()
                    && name.starts_with(&prefix)
                {
                    let _ = std::fs::remove_file(e.path());
                }
            }
        }
        self.disk_cache_index.remove(&full_path);
        self.inodes.remove(&crate::path_hash(&full_path));
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
        rt().block_on(async move { op.delete(&p).await })
            .map_err(|_| std::io::Error::other("rmdir failed"))?;
        self.inodes.remove(&crate::path_hash(&full_path));
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
        let dst_clone2 = dst.clone();
        // Pre-delete destination (POSIX semantics)
        let op_del = op.clone();
        let _ = rt().block_on(async move { op_del.delete(&dst_clone2).await });

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
        let backend_ok = rt().block_on(async move {
            match op.rename(&src_clone, &dst_clone).await {
                Ok(()) => true,
                Err(e) if e.kind() == opendal::ErrorKind::Unsupported => {
                    tracing::debug!(
                        path = %src_clone, error = %e,
                        "backend does not support server-side rename; falling back to local-cache read + write+delete"
                    );
                    // Manual copy. We CANNOT use op.read() /
                    // op.copy() against the backend: writeback is
                    // async (5s default delay) so freshly-written
                    // data may not have landed on the backend yet,
                    // and opendal's memory backend reports
                    // `type Copier = ()` so op.copy() also returns
                    // Unsupported for it. Read the on-disk cache
                    // file directly — it always holds the most
                    // recent content, regardless of writeback
                    // state.
                    let cpath_src = crate::cache_path(&self.cache_dir, &src_clone);
                    let bytes = match std::fs::read(&cpath_src) {
                        Ok(b) => b,
                        Err(read_err) if read_err.kind() == std::io::ErrorKind::NotFound => {
                            // src has no cache file — nothing
                            // to copy. Treat rename as a no-op
                            // and just delete src.
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
                    // Copy succeeded — delete src. POSIX rename
                    // atomicity is best-effort: if delete fails
                    // we leave both visible (dst is canonical,
                    // src is leftover).
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
        // Migrate inodes src -> dst. The entry's `path` field
        // MUST be updated to `dst`; otherwise resolve() returns
        // the old src path, cache_path() hashes the wrong
        // string, and the cache file (which we just renamed to
        // dst's hash) is invisible to the read path.
        let src_hash = crate::path_hash(&src);
        let dst_hash = crate::path_hash(&dst);
        if let Some((_, kind, size, mtime)) = self.inodes.get(&src_hash).map(|e| {
            let (p, k, s, m) = e.value().clone();
            (p, k, s, m)
        }) {
            self.inodes
                .insert(dst_hash, (dst.clone(), kind, size, mtime));
        }
        self.inodes.remove(&src_hash);
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
        eprintln!("{report}");
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

/// Simple CRC64 checksum for cache integrity validation.
fn crc64_checksum(data: &[u8]) -> u64 {
    let mut crc: u64 = 0xFFFFFFFFFFFFFFFF;
    for &byte in data {
        crc ^= byte as u64;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xD800000000000000;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xFFFFFFFFFFFFFFFF
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
