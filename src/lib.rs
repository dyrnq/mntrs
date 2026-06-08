#![recursion_limit = "256"]
pub mod cmd;

use std::collections::VecDeque;
use std::ffi::OsStr;
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    AccessFlags, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation,
    INodeNo, KernelConfig, LockOwner, OpenFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr,
    Request, TimeOrNow, WriteFlags,
};
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
#[derive(Debug, Clone)]
enum FileHandleState {
    Read {
        path: String,
        last_offset: u64,
        chunk_size: u64,
    },
    Write {
        path: String,
        dirty: bool,
        #[allow(dead_code)]
        dirty_since: Option<std::time::Instant>,
    },
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
    inodes: dashmap::DashMap<u64, (String, FileType, u64)>,
    dir_cache:
        dashmap::DashMap<String, (std::time::Instant, std::sync::Arc<Vec<(String, EntryMode)>>)>,
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
    pub block_norm_dupes: bool,
    pub write_wait: Duration,
    pub read_wait: Duration,
    pub cache_poll_interval: Duration,
    pub disk_total_size: u64,
    writeback_queue: Arc<Mutex<VecDeque<(String, PathBuf)>>>,
    mem_cache: dashmap::DashMap<u64, bytes::Bytes>,
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

impl MntrsFs {
    fn make_attr(&self, ino: u64, size: u64, kind: FileType) -> FileAttr {
        let now = UNIX_EPOCH;
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
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
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
    cache_dir.join(format!("{:020x}", path_hash(path)))
}

impl MntrsFs {
    fn resolve(&self, ino: u64) -> Option<(String, FileType, u64)> {
        self.inodes.get(&ino).map(|r| r.clone())
    }

    fn alloc_ino(&self, path: &str, kind: FileType, size: u64) -> u64 {
        let ino = path_hash(path);
        self.inodes
            .entry(ino)
            .and_modify(|v| v.2 = size)
            .or_insert((path.to_string(), kind, size));
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
                        meta.last_modified().map(SystemTime::from)
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

    fn list_op(&self, path: &str) -> Vec<(String, EntryMode)> {
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
                    if !self.include_patterns.is_empty() {
                        let matched = self
                            .include_patterns
                            .iter()
                            .any(|pat| fnmatch(pat, &name, self.ignore_case));
                        if !matched {
                            continue;
                        }
                    }
                    out.push((name, mode));
                }
                Some(out)
            })
            .unwrap_or_default();
        // Deduplicate by Unicode-normalized name if enabled
        if self.block_norm_dupes && !result.is_empty() {
            let mut seen = std::collections::HashSet::new();
            result.retain(|(name, _)| {
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

    fn mem_cache_insert(&self, ino: u64, data: bytes::Bytes) {
        let size = data.len() as u64;
        let new_total = self
            .mem_used
            .fetch_add(size, std::sync::atomic::Ordering::Relaxed)
            + size;
        if new_total > self.mem_limit {
            // Evict oldest entries from mem_cache until under limit
            let mut to_free = new_total.saturating_sub(self.mem_limit);
            let mut victims: Vec<u64> = Vec::new();
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
        self.mem_cache.insert(ino, data);
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
            let cpath = cache_path(&self.cache_dir, &path);
            let _ = fs::remove_file(&cpath);
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

fn writeback_worker(
    op: Arc<Operator>,
    queue: Arc<Mutex<VecDeque<(String, PathBuf)>>>,
    delay: Duration,
    max_age: Duration,
) {
    loop {
        let task = {
            let mut q = queue.lock().unwrap();
            q.pop_front()
        };
        let (remote_path, cache_path) = match task {
            Some(t) => t,
            None => {
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        if delay > Duration::ZERO {
            thread::sleep(delay);
        }
        // Check cache max age: skip stale files
        if max_age > Duration::ZERO
            && let Ok(meta) = fs::metadata(&cache_path)
            && let Ok(elapsed) = meta.modified().unwrap_or(std::time::UNIX_EPOCH).elapsed()
            && elapsed > max_age
        {
            let _ = fs::remove_file(&cache_path);
            continue;
        }
        let data = match fs::read(&cache_path) {
            Ok(d) if !d.is_empty() => d,
            _ => {
                let _ = fs::remove_file(&cache_path);
                // Note: disk_cache_index cleanup happens via the path in evict_lru
                continue;
            }
        };
        let op = op.clone();
        let p = remote_path.clone();
        // Retry up to 3 times with exponential backoff
        for attempt in 0..3 {
            let r = rt().block_on(async { op.write(&p, data.clone()).await });
            match r {
                Ok(_) => {
                    if let Err(e) = fs::remove_file(&cache_path) {
                        tracing::debug!(error=%e, path=?cache_path, "writeback ok remove failed");
                    }
                    if let Err(e) = fs::remove_file(cache_path.with_extension("dirty")) {
                        tracing::debug!(error=%e, "writeback dirty remove failed");
                    }
                    break;
                }
                Err(e) if attempt < 2 => {
                    eprintln!("[mntrs] writeback retry {}/3 for {p}: {e}", attempt + 1);
                    thread::sleep(Duration::from_secs(1 << attempt));
                }
                Err(e) => {
                    eprintln!("[mntrs] writeback failed for {p}: {e}");
                }
            }
        }
    }
}

impl Filesystem for MntrsFs {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        self.alloc_ino("", FileType::Directory, 4096);
        if let Err(e) = fs::create_dir_all(&self.cache_dir) {
            tracing::warn!(error=%e, "create_dir_all failed for cache");
        }
        // Enable readdirplus for stat+readdir in one round-trip
        let _ = config.add_capabilities(fuser::InitFlags::FUSE_DO_READDIRPLUS);
        // Recover writeback queue from dirty sidecars
        if let Ok(entries) = fs::read_dir(&self.cache_dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.extension().is_some_and(|ext| ext == "dirty") {
                    let cache_path = p.with_extension("");
                    if let Ok(remote) = fs::read_to_string(&p) {
                        let remote = remote.trim().to_string();
                        if !remote.is_empty() && cache_path.exists() {
                            self.writeback_queue
                                .lock()
                                .unwrap()
                                .push_back((remote, cache_path));
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
        thread::spawn(move || writeback_worker(op, queue, delay, max_age));
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
        let name2 = name.clone();
        let parent: u64 = parent.into();
        if name == "." || name == ".." {
            let p = if name == "." { parent } else { FUSE_ROOT_INO };
            let attr = self
                .resolve(p)
                .map(|(_, k, s)| self.make_attr(p, s, k))
                .unwrap_or_else(|| self.make_attr(FUSE_ROOT_INO, 4096, FileType::Directory));
            reply.entry(&self.attr_ttl, &attr, Generation(0));
            return;
        }
        let parent_path = self.resolve(parent).map(|(p, _, _)| p).unwrap_or_default();
        let full_path = if parent_path.is_empty() {
            name2
        } else {
            format!("{}/{}", parent_path, name2)
        };
        if let Some((kind, size, mtime)) = self.stat_op(&full_path) {
            let mut attr = self.make_attr(self.alloc_ino(&full_path, kind, size), size, kind);
            if let Some(mt) = mtime {
                attr.mtime = mt;
            }
            reply.entry(&self.attr_ttl, &attr, Generation(0));
        } else if self.case_insensitive {
            // Fallback: search directory listing for case-insensitive match
            let entries = self.list_op(&parent_path);
            let lower = name.to_lowercase();
            if let Some((matched_name, mode)) =
                entries.iter().find(|(n, _)| n.to_lowercase() == lower)
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
                let mut attr = self.make_attr(self.alloc_ino(&mp, kind, size), size, kind);
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
                &self.make_attr(ino, 4096, FileType::Directory),
            );
            return;
        }
        if let Some((path, kind, _)) = self.resolve(ino) {
            let (_, size, mtime) = self.stat_op(&path).unwrap_or((kind, 0, None));
            let mut attr = self.make_attr(ino, size, kind);
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
        let path = self.resolve(ino).map(|(p, _, _)| p).unwrap_or_default();
        let mut entries = vec![
            (".".to_string(), FileType::Directory),
            ("..".to_string(), FileType::Directory),
        ];
        for (name, mode) in self.list_op(&path) {
            entries.push((
                name,
                match mode {
                    EntryMode::DIR => FileType::Directory,
                    _ => FileType::RegularFile,
                },
            ));
        }
        let start = offset as usize;
        if start >= entries.len() {
            reply.ok();
            return;
        }
        for (i, (name, kind)) in entries.iter().enumerate().skip(start) {
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
        let path = self.resolve(ino).map(|(p, _, _)| p).unwrap_or_default();
        let mut entries = vec![
            (".".to_string(), FileType::Directory),
            ("..".to_string(), FileType::Directory),
        ];
        for (name, mode) in self.list_op(&path) {
            entries.push((
                name,
                match mode {
                    EntryMode::DIR => FileType::Directory,
                    _ => FileType::RegularFile,
                },
            ));
        }
        let start = offset as usize;
        if start >= entries.len() {
            reply.ok();
            return;
        }
        for (i, (name, kind)) in entries.iter().enumerate().skip(start) {
            let cp = if path.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", path, name)
            };
            let (size, mtime) = match self.stat_op(&cp) {
                Some((_, s, mt)) => (s, mt),
                None => {
                    tracing::debug!(path = %cp, "readdirplus stat_op failed, skip");
                    continue;
                }
            };
            let ino = self.alloc_ino(&cp, *kind, size);
            let mut attr = self.make_attr(ino, size, *kind);
            if let Some(mt) = mtime {
                attr.mtime = mt;
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
            .map(|(p, _, _)| p)
            .unwrap_or_default();
        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };
        let ino = self.alloc_ino(&full_path, FileType::RegularFile, 0);
        self.handles.insert(
            ino,
            FileHandleState::Write {
                path: full_path.clone(),
                dirty: false,
                dirty_since: None,
            },
        );
        reply.created(
            &self.attr_ttl,
            &self.make_attr(ino, 0, FileType::RegularFile),
            Generation(0),
            FileHandle(ino),
            FopenFlags::empty(),
        );
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let ino: u64 = ino.into();
        if let Some((path, FileType::RegularFile, _)) = self.resolve(ino) {
            self.handles.insert(
                ino,
                FileHandleState::Read {
                    path,
                    last_offset: 0,
                    chunk_size: 131072,
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
            Some((p, _, s)) => (p, s),
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
        if let Some(entry) = self.mem_cache.get(&ino) {
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
        // 2. Check disk cache
        if !self.direct_io {
            let cpath = cache_path(&self.cache_dir, &path);
            if cpath.exists()
                && let Ok(data) = fs::read(&cpath)
            {
                // Populate memory cache
                let b = bytes::Bytes::from(data);
                let start = offset as usize;
                let end = (start + size as usize).min(b.len());
                if start < b.len() {
                    reply.data(&b[start..end]);
                } else {
                    reply.data(&[]);
                }
                self.mem_cache_insert(ino, b);
                return;
            }
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
                self.mem_cache_insert(ino, b);
            } else {
                reply.error(Errno::EIO);
            }
        } else {
            // Single-chunk fetch (original path)
            let clamped_fetch = fetch_size.min(cap);
            let clamped_size = (size as u64).min(cap) as u32;
            match rt()
                .block_on(async move { op.read_with(&p).range(offset..offset + clamped_fetch).await })
            {
                Ok(buf) => {
                    let b: bytes::Bytes = buf.to_vec().into();
                    let slice = &b[..(b.len() as u32).min(clamped_size) as usize];
                    reply.data(slice);
                    self.mem_cache_insert(ino, b);
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
        let cpath = cache_path(&self.cache_dir, &path);
        if let Some(parent) = cpath.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let result = (|| -> std::io::Result<()> {
            let file = fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .read(true)
                .open(&cpath)?;
            self.disk_cache_index.insert(
                path.clone(),
                (data.len() as u64, std::time::SystemTime::now()),
            );
            let end = offset + data.len() as u64;
            let current_len = file.metadata()?.len();
            if end > current_len {
                file.set_len(end)?;
            }
            let mut f = file;
            f.seek(SeekFrom::Start(offset))?;
            f.write_all(data)?;
            f.flush()?;
            Ok(())
        })();
        self.evict_lru();
        match result {
            Ok(()) => {
                self.handles.insert(
                    fh_val,
                    FileHandleState::Write {
                        path: path.clone(),
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
            .map(|(p, _, _)| p)
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
                    &self.make_attr(ino, 4096, FileType::Directory),
                    Generation(0),
                );
            }
            Err(_) => reply.error(Errno::EIO),
        }
    }

    fn rmdir(&self, _req: &Request, _parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy();
        let dir_path = format!("{}/", name.trim_end_matches('/'));
        let op = self.op.clone();
        let p = dir_path.clone();
        let p2 = p.clone();
        if rt().block_on(async move { op.delete(&p2).await }).is_err() {
            tracing::debug!(path=%p, "rmdir delete failed");
        }
        reply.ok();
    }

    fn rename(
        &self,
        _req: &Request,
        _parent: INodeNo,
        name: &OsStr,
        _newparent: INodeNo,
        newname: &OsStr,
        _flags: fuser::RenameFlags,
        reply: ReplyEmpty,
    ) {
        let src = name.to_string_lossy().to_string();
        let dst = newname.to_string_lossy().to_string();
        let op = self.op.clone();
        rt().block_on(async move {
            if op.copy(&src, &dst).await.is_ok() && op.delete(&src).await.is_err() {
                tracing::debug!(path=%src, "rename delete failed");
            }
        });
        reply.ok();
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, _size: u32, reply: ReplyXattr) {
        let ino: u64 = ino.into();
        let name = name.to_string_lossy();
        if let Some((path, kind, _)) = self.resolve(ino) {
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
        if let Some((_, kind, _)) = self.resolve(ino) {
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
        if let Some((p, kind, _)) = self.resolve(ino) {
            if let Some(s) = size {
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
            let mut attr = self.make_attr(ino, size.unwrap_or(0), kind);
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

    fn unlink(&self, _req: &Request, _parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy();
        let op = self.op.clone();
        let p = name.to_string();
        let p2 = p.clone();
        if rt().block_on(async move { op.delete(&p2).await }).is_err() {
            tracing::debug!(path=%p, "unlink remote failed");
        }
        // Also remove from local cache
        let cpath = cache_path(&self.cache_dir, &name);
        if let Err(e) = fs::remove_file(&cpath) {
            tracing::debug!(error=%e, path=?cpath, "unlink cache remove failed");
        }
        self.disk_cache_index.remove(&name as &str);
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
                    self.handles.insert(
                        fh_val,
                        FileHandleState::Write {
                            path: p.clone(),
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
            let cpath = cache_path(&self.cache_dir, &path);
            if cpath.exists() {
                // Write sidecar for crash recovery
                let sidecar = cpath.with_extension("dirty");
                if let Err(e) = fs::write(&sidecar, path.as_bytes()) {
                    tracing::warn!(error=%e, path=?sidecar, "sidecar write failed");
                }
                self.writeback_queue
                    .lock()
                    .unwrap()
                    .push_back((path, cpath));
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
