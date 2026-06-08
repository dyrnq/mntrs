#![recursion_limit = "256"]
pub mod cmd;
pub mod core_fs;
pub mod path;
pub mod prefetcher;
pub mod writeback;

/// Shared inode table type for writeback callback.
pub const CACHE_BLOCK_SIZE: u64 = 8 * 1024 * 1024;
pub type Inodes = Arc<dashmap::DashMap<u64, (String, FileType, u64, Option<SystemTime>)>>;

use std::collections::VecDeque;
use std::ffi::OsStr;
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

#[cfg(unix)]
use fuser::{
    AccessFlags, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation,
    INodeNo, KernelConfig, LockOwner, OpenFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr,
    Request, TimeOrNow, WriteFlags,
};

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
pub struct MntrsFs {
    pub op: Arc<Operator>,
    inodes: dashmap::DashMap<u64, (String, FileType, u64, Option<std::time::SystemTime>)>,
    dir_cache: dashmap::DashMap<
        String,
        (
            std::time::Instant,
            std::sync::Arc<Vec<(String, EntryMode, u64, std::time::SystemTime)>>,
        ),
    >,
    cache_dir: PathBuf,
    handles: dashmap::DashMap<u64, FileHandleState>,
    pub dir_cache_ttl: Duration,
    pub attr_ttl: Duration,
    pub stat_cache_ttl: Duration,
    pub volname: String,
    pub cache_max_size: u64,
    pub write_back_delay: Duration,
    pub cache_mode: String,
    pub read_ahead: u64,
    pub read_chunk_size: u64,
    pub read_chunk_size_limit: u64,
    pub read_chunk_streams: u32,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub umask: Option<u32>,
    pub dir_perms: u16,
    pub file_perms: u16,
    pub direct_io: bool,
    pub poll_interval: Duration,
    pub cache_max_age: Duration,
    pub cache_min_free_space: u64,
    pub exclude_patterns: Vec<String>,
    pub include_patterns: Vec<String>,
    pub max_size: Option<u64>,
    pub min_size: Option<u64>,
    pub max_depth: Option<usize>,
    pub ignore_case: bool,
    pub fast_fingerprint: bool,
    pub async_read: bool,
    pub vfs_refresh: bool,
    pub case_insensitive: bool,
    pub no_implicit_dir: bool,
    pub use_server_modtime: bool,
    pub no_apple_double: bool,
    pub no_apple_xattr: bool,
    pub hash_filter: Option<(usize, usize)>,
    pub block_norm_dupes: bool,
    pub write_wait: Duration,
    pub read_wait: Duration,
    pub handle_caching: Duration,
    pub cache_poll_interval: Duration,
    pub disk_total_size: u64,
    writeback_queue: Arc<Mutex<VecDeque<(u64, String, PathBuf)>>>,
    mem_cache: dashmap::DashMap<(u64, u64), bytes::Bytes>,
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
    pub storage_class: Option<String>,
    pub mem_limit: u64,
    mem_used: std::sync::atomic::AtomicU64,
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

    fn alloc_ino(&self, path: &str, kind: FileType, size: u64) -> u64 {
        let ino = path_hash(path);
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

    fn list_op(&self, path: &str) -> Vec<(String, EntryMode, u64, SystemTime)> {
        {
            if let Some(entry) = self.dir_cache.get(path) {
                let (t, entries) = entry.value();
                let entries = entries.clone();
                let age = t.elapsed();
                if age < self.dir_cache_ttl {
                    return entries.as_ref().clone();
                }
                // Cache expired — re-read from remote
                tracing::debug!(
                    path,
                    age_ms = age.as_millis(),
                    "Re-reading directory ({}ms old)",
                    age.as_millis()
                );
            }
        }
        let depth = path.matches('/').count();
        let mut result = rt()
            .block_on(async {
                let op = self.op.clone();
                let p = path.to_string();
                let mut lister = op.lister(&p).await.ok()?;
                let mut out = vec![];
                while let Some(Ok(entry)) = lister.next().await {
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
                Some(out)
            })
            .unwrap_or_default();
        // Deduplicate by Unicode-normalized name if enabled
        if self.block_norm_dupes && !result.is_empty() {
            let mut seen = std::collections::HashSet::new();
            result.retain(|(name, ..)| {
                use unicode_normalization::UnicodeNormalization;
                let norm: String = name.nfc().collect::<String>();
                seen.insert(norm)
            });
        }
        self.dir_cache.insert(
            path.to_string(),
            (
                std::time::Instant::now(),
                std::sync::Arc::new(result.clone()),
            ),
        );
        result
    }

    fn mem_cache_insert(&self, ino: u64, block_idx: u64, data: bytes::Bytes) {
        let size = data.len() as u64;
        let new_total = self
            .mem_used
            .fetch_add(size, std::sync::atomic::Ordering::Relaxed)
            + size;
        if new_total > self.mem_limit {
            // Evict oldest entries from mem_cache until under limit
            let mut to_free = new_total.saturating_sub(self.mem_limit);
            let mut victims: Vec<(u64, u64)> = Vec::new();
            for entry in self.mem_cache.iter() {
                if to_free == 0 {
                    break;
                }
                victims.push(*entry.key());
                to_free = to_free.saturating_sub(entry.value().len() as u64);
            }
            for v in &victims {
                if let Some((_, removed)) = self.mem_cache.remove(v) {
                    self.mem_used
                        .fetch_sub(removed.len() as u64, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
        self.mem_cache.insert((ino, block_idx), data);
    }

    fn evict_lru(&self) {
        if self.cache_max_size == 0 && self.cache_min_free_space == 0 {
            return;
        }
        // Collect from in-memory index instead of fs::read_dir
        let mut files: Vec<(String, u64, std::time::SystemTime)> = Vec::new();
        let mut total: u64 = 0;
        for entry in self.disk_cache_index.iter() {
            let path: String = entry.key().clone();
            let (size, atime) = *entry.value();
            total += size;
            files.push((path, size, atime));
        }
        // Check free disk space if configured
        let need_free = if self.cache_min_free_space > 0 {
            #[cfg(unix)]
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
        // LRU: sort by access time ascending, remove oldest until under limit
        files.sort_by_key(|(_, _, atime)| *atime);
        let mut remaining = to_free;
        let mut freed: u64 = 0;
        for (path, size, _) in files {
            if remaining == 0 {
                break;
            }
            let block_idx = 0; // flush uses block 0 (main cache location)
            let cpath = cache_block_path(&self.cache_dir, &path, block_idx);
            let _ = fs::remove_file(&cpath);
            let _ = fs::remove_file(cpath.with_extension("meta"));
            self.disk_cache_index.remove(&path as &str);
            freed += size;
            remaining = remaining.saturating_sub(size);
        }
        // If we freed enough, clear out_of_space
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
        self.alloc_ino("", FileType::Directory, 4096);
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
        }

        // Recover writeback queue from dirty sidecars
        if let Ok(entries) = fs::read_dir(&self.cache_dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.extension().is_some_and(|ext| ext == "dirty") {
                    let cache_path = p.with_extension("");
                    if let Ok(remote) = fs::read_to_string(&p) {
                        let remote = remote.trim().to_string();
                        if !remote.is_empty() && cache_path.exists() {
                            self.writeback_queue.lock().unwrap().push_back((
                                0,
                                remote,
                                cache_path.clone(),
                            ));
                        }
                    }
                    if let Err(e) = fs::remove_file(&p) {
                        tracing::warn!(error=%e, path=?p, "dirty recovery remove failed");
                    }
                }
            }
        }
        // Spawn writeback worker thread
        let op = self.op.clone();
        let queue = self.writeback_queue.clone();
        let delay = self.write_back_delay;
        let max_age = self.cache_max_age;
        let inodes = Arc::new(self.inodes.clone());
        thread::spawn(move || writeback::worker(op, inodes, queue, delay, max_age));
        // Pre-populate root directory cache on mount if --vfs-refresh
        if self.vfs_refresh {
            let _ = self.list_op("");
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
            let entries = self.list_op(&parent_path);
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
        if let Some((path, kind, _, _)) = self.resolve(ino) {
            let (_, size, mtime) = self.stat_op(&path).unwrap_or((kind, 0, None));
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
        if ino != FUSE_ROOT_INO {
            reply.error(Errno::ENOENT);
            return;
        }
        let path = self.resolve(ino).map(|(p, _, _, _)| p).unwrap_or_default();
        let mut entries: Vec<(String, FileType, u64, Option<SystemTime>)> = vec![
            (".".to_string(), FileType::Directory, 4096, None),
            ("..".to_string(), FileType::Directory, 4096, None),
        ];
        for (name, mode, size, mtime) in self.list_op(&path) {
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
        for (i, (name, kind, _size, _mtime)) in entries.iter().enumerate().skip(start) {
            let cp = if path.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", path, name)
            };
            let (_, size, _mtime) = self.stat_op(&cp).unwrap_or((*kind, 0, None));
            if reply.add(
                INodeNo(self.alloc_ino(&cp, *kind, size)),
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
        if ino != FUSE_ROOT_INO {
            reply.error(Errno::ENOENT);
            return;
        }
        let path = self.resolve(ino).map(|(p, _, _, _)| p).unwrap_or_default();
        let mut entries: Vec<(String, FileType, u64, Option<SystemTime>)> = vec![
            (".".to_string(), FileType::Directory, 4096, None),
            ("..".to_string(), FileType::Directory, 4096, None),
        ];
        for (name, mode, size, mtime) in self.list_op(&path) {
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
            ino,
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
            FileHandle(ino),
            FopenFlags::empty(),
        );
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let ino: u64 = ino.into();
        if let Some((path, FileType::RegularFile, _, _)) = self.resolve(ino) {
            self.handles.insert(
                ino,
                FileHandleState::Read {
                    path: path.clone(),
                    last_offset: 0,
                    chunk_size: 131072,
                    prefetcher: self.maybe_create_prefetcher(ino, &path),
                },
            );
        }
        reply.opened(FileHandle(ino), FopenFlags::empty());
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
        if let Some(entry) = self.mem_cache.get(&(ino, block_idx)) {
            let data = entry.value().clone();
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
                self.mem_cache_insert(ino, block_idx, b);
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
                    self.mem_cache_insert(ino, offset / CACHE_BLOCK_SIZE, b);
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
                self.mem_cache_insert(ino, offset / CACHE_BLOCK_SIZE, b);
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
                    self.mem_cache_insert(ino, offset / CACHE_BLOCK_SIZE, b);
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
        rt().block_on(async move {
            if op.copy(&src_clone, &dst).await.is_ok() && op.delete(&src_clone).await.is_err() {
                tracing::debug!(path=%src_clone, "rename delete failed");
            }
        });
        let cpath = cache_path(&self.cache_dir, &src);
        let _ = fs::remove_file(&cpath);
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
        self.inodes.remove(&path_hash(&src));
        self.attr_cache.remove(&src);
        reply.ok();
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, _size: u32, reply: ReplyXattr) {
        let ino: u64 = ino.into();
        let name = name.to_string_lossy();
        if self.no_apple_xattr
            && (name.starts_with("com.apple.") || name == "system.posix_acl_access")
        {
            reply.error(Errno::ENODATA);
            return;
        }
        if let Some((path, kind, _, _)) = self.resolve(ino) {
            if kind == FileType::Directory {
                reply.error(Errno::ENODATA);
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
                        reply.error(Errno::ENODATA);
                    }
                    "user.content-type" | "s3.content-type" => {
                        if let Some(ct) = meta.content_type() {
                            reply.data(ct.as_bytes());
                            return;
                        }
                        reply.error(Errno::ENODATA);
                    }
                    _ => reply.error(Errno::ENODATA),
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
                reply.error(Errno::ENODATA);
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
                self.writeback_queue
                    .lock()
                    .unwrap()
                    .push_back((_ino.into(), path, cpath));
            }
        }
        if dirty {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
            loop {
                let pending = self.writeback_queue.lock().unwrap().len();
                if pending == 0 {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    tracing::warn!("fsync timeout waiting for writeback");
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
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
        self.alloc_ino("", FileType::Directory, 4096);
        // Recover writeback queue from dirty sidecars
        if let Ok(entries) = std::fs::read_dir(&self.cache_dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.extension().is_some_and(|ext| ext == "dirty") {
                    let cache_path = p.with_extension("");
                    if let Ok(remote) = std::fs::read_to_string(&p) {
                        let remote = remote.trim().to_string();
                        if !remote.is_empty() && cache_path.exists() {
                            self.writeback_queue.lock().unwrap().push_back((
                                0,
                                remote,
                                cache_path.clone(),
                            ));
                        }
                    }
                    if let Err(e) = std::fs::remove_file(&p) {
                        tracing::warn!(error=%e, path=?p, "dirty recovery remove failed");
                    }
                }
            }
        }
        // Spawn writeback worker thread
        let op = self.op.clone();
        let queue = self.writeback_queue.clone();
        let delay = self.write_back_delay;
        let max_age = self.cache_max_age;
        let inodes = Arc::new(self.inodes.clone());
        std::thread::spawn(move || crate::writeback::worker(op, inodes, queue, delay, max_age));
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
        if let Some((path, kind, size, _)) = self.resolve(ino) {
            let (_, _, mtime) = self.stat_op(&path).unwrap_or((kind, size, None));
            Ok(to_core_attr(&self.make_attr(
                ino,
                size,
                kind,
                mtime.unwrap_or(SystemTime::UNIX_EPOCH),
            )))
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
                self.alloc_ino(&_p, kind, s);
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
        if ino != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "not root",
            ));
        }
        let path = self.resolve(ino).map(|(p, _, _, _)| p).unwrap_or_default();
        let mut entries = vec![
            CoreDirEntry {
                ino: 1,
                kind: CoreFileType::Directory,
                name: ".".to_string(),
            },
            CoreDirEntry {
                ino: 1,
                kind: CoreFileType::Directory,
                name: "..".to_string(),
            },
        ];
        for (name, mode, size, _mtime) in self.list_op(&path) {
            let kind = match mode {
                EntryMode::DIR => CoreFileType::Directory,
                _ => CoreFileType::RegularFile,
            };
            let cp = if path.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", path, name)
            };
            let ino = self.alloc_ino(
                &cp,
                match kind {
                    CoreFileType::Directory => FileType::Directory,
                    _ => FileType::RegularFile,
                },
                size,
            );
            entries.push(CoreDirEntry { ino, kind, name });
        }
        Ok(entries)
    }

    fn open(&self, ino: u64, _flags: u32) -> std::io::Result<u64> {
        let path = self.resolve(ino).map(|(p, _, _, _)| p).unwrap_or_default();

        // If handle caching is active, check for existing write handle for this inode
        if self.handle_caching > std::time::Duration::ZERO {
            if let Some(entry) = self.handles.get(&ino) {
                if let crate::FileHandleState::Write { path: existing_path, cache_fd: Some(_fd), .. } = entry.value() {
                    if *existing_path == path {
                        // Reuse existing cached handle
                        return Ok(ino);
                    }
                }
            }
        }

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
                ino,
                FileHandleState::Write {
                    path,
                    cache_fd: cache_fd.map(|f| std::sync::Arc::new(std::sync::Mutex::new(f))),
                    dirty: false,
                    dirty_since: None,
                },
            );
        } else {
            self.handles.insert(
                ino,
                FileHandleState::Read {
                    path,
                    last_offset: 0,
                    chunk_size: self.read_chunk_size.max(131072),
                    prefetcher: None,
                },
            );
        }
        Ok(ino)
    }

    fn read(&self, ino: u64, _fh: u64, offset: u64, size: u32) -> std::io::Result<Vec<u8>> {
        let (path, file_size) = self
            .resolve(ino)
            .map(|(p, _, s, _)| (p, s))
            .ok_or(std::io::ErrorKind::NotFound)?;
        if offset >= file_size {
            return Ok(vec![]);
        }
        let cap = file_size - offset;
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

        if let Some(entry) = self.mem_cache.get(&(ino, block_idx)) {
            let data = entry.value();
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
                self.mem_cache_insert(ino, offset / CACHE_BLOCK_SIZE, b);
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
                self.mem_cache_insert(ino, offset / CACHE_BLOCK_SIZE, b);
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
                self.mem_cache_insert(ino, offset / CACHE_BLOCK_SIZE, b);
                Ok(data)
            }
            Err(_) => Err(std::io::Error::other("read failed")),
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

        // Update inodes size
        let end = _offset + _data.len() as u64;
        self.inodes.entry(_ino).and_modify(|v| {
            if end > v.2 {
                v.2 = end;
            }
        });

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
                self.writeback_queue
                    .lock()
                    .unwrap()
                    .push_back((_ino, path.clone(), cpath));
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
                self.writeback_queue
                    .lock()
                    .unwrap()
                    .push_back((_ino, path.clone(), cpath));
                tracing::debug!(path=%path, "release queued writeback");
            }
            true
        } else {
            false
        };

        if self.handle_caching > std::time::Duration::ZERO && !was_dirty {
            // Keep handle alive for handle_caching duration so reopen can reuse cache fd
            let fd_to_keep = self.handles.get(&fh).and_then(|e| {
                if let crate::FileHandleState::Write { cache_fd: Some(fd), .. } = e.value() {
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
        rt().block_on(async move {
            if op.copy(&src_clone, &dst).await.is_ok() {
                let _ = op.delete(&src_clone).await;
            }
        });
        self.inodes.remove(&crate::path_hash(&src));
        self.attr_cache.remove(&src);
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
        writeback_queue: Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new())),
        mem_cache: Default::default(),
        attr_cache: Default::default(),
        disk_cache_index: Default::default(),
        out_of_space: std::sync::atomic::AtomicBool::new(false),
        storage_class: None,
        mem_limit: u64::MAX,
        mem_used: std::sync::atomic::AtomicU64::new(0),
    }
}
