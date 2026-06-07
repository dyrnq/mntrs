pub mod cmd;

use std::collections::VecDeque;
use std::ffi::OsStr;
use std::fs;
use std::io::{Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{TimeOrNow,
    FileAttr, FileType, Filesystem, KernelConfig,
    ReplyAttr, ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, ReplyCreate, ReplyStatfs, ReplyXattr,
    Request, INodeNo, FileHandle, OpenFlags, WriteFlags, AccessFlags, Errno, FopenFlags, Generation,
    LockOwner,
};
use futures::StreamExt;
use opendal::{EntryMode, Operator};

fn rt() -> &'static tokio::runtime::Runtime {
    static RT: once_cell::sync::OnceCell<tokio::runtime::Runtime> = once_cell::sync::OnceCell::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().expect("tokio rt"))
}

// TTL now comes from MntrsFs.attr_ttl field
const FUSE_ROOT_INO: u64 = 1;
// DIR_CACHE_TTL now comes from MntrsFs.dir_cache_ttl field

pub struct MntrsFs {
    pub op: Arc<Operator>,
    inodes: dashmap::DashMap<u64, (String, FileType, u64)>,
    dir_cache: dashmap::DashMap<String, (std::time::Instant, Vec<(String, EntryMode)>)>,
    cache_dir: PathBuf,
    handles: dashmap::DashMap<u64, (String, bool, Option<std::time::Instant>)>, // +dirty_since
    pub dir_cache_ttl: Duration,
    pub attr_ttl: Duration,
    pub volname: String,
    pub cache_max_size: u64,
    pub write_back_delay: Duration,
    pub cache_mode: String,
    pub read_ahead: u64,
    pub read_chunk_size: u64,
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
    pub write_wait: Duration,
    pub read_wait: Duration,
    pub cache_poll_interval: Duration,
    pub disk_total_size: u64,
    writeback_queue: Arc<Mutex<VecDeque<(String, PathBuf)>>>,
    mem_cache: dashmap::DashMap<u64, bytes::Bytes>,
    
}

impl MntrsFs {
    fn make_attr(&self, ino: u64, size: u64, kind: FileType) -> FileAttr {
        let now = UNIX_EPOCH;
        let base_perm = if kind == FileType::Directory { self.dir_perms } else { self.file_perms };
        let perm = match self.umask { Some(m) => base_perm & !(m as u16), None => base_perm };
        let uid = self.uid.unwrap_or(1000);
        let gid = self.gid.unwrap_or(1000);
        FileAttr {
            ino: INodeNo(ino), size, blocks: size.div_ceil(4096),
            atime: now, mtime: now, ctime: now, crtime: now,
            kind, perm,
            nlink: if kind == FileType::Directory { 2 } else { 1 },
            uid, gid, rdev: 0, blksize: 4096, flags: 0,
        }
    }
}

fn path_hash(path: &str) -> u64 {
    let mut h: u64 = 0x811c9dc5;
    for b in path.bytes() { h = h.wrapping_mul(0x01000193) ^ b as u64; }
    (h & 0x7FFFFFFFFFFFFFFF).max(2)
}

fn fnmatch(pattern: &str, name: &str, ignore_case: bool) -> bool {
    let (p, n): (Vec<char>, Vec<char>) = if ignore_case {
        (pattern.to_lowercase().chars().collect(), name.to_lowercase().chars().collect())
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
    while pi < pl && p[pi] == '*' { pi += 1; }
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
        self.inodes.entry(ino).or_insert((path.to_string(), kind, size));
        ino
    }

    fn stat_op(&self, path: &str) -> Option<(FileType, u64)> {
        rt().block_on(async {
            let op = self.op.clone();
            let p = path.to_string();
            match op.stat(&p).await {
                Ok(meta) => {
                    let kind = match meta.mode() { EntryMode::DIR => FileType::Directory, _ => FileType::RegularFile };
                    Some((kind, meta.content_length()))
                }
                Err(_) => {
                    let op2 = self.op.clone();
                    let p2 = format!("{}/", path.trim_end_matches('/'));
                    if let Ok(mut l) = op2.lister(&p2).await
                        && l.next().await.is_some() { return Some((FileType::Directory, 4096)); }
                    None
                }
            }
        })
    }

    fn list_op(&self, path: &str) -> Vec<(String, EntryMode)> {
        {
            if let Some(entry) = self.dir_cache.get(path) {
                let (t, entries) = entry.value();
                if t.elapsed() < self.dir_cache_ttl { return entries.clone(); }
            }
        }
        let depth = path.matches('/').count();
        let result = rt().block_on(async {
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
                    && depth >= max_depth && mode == EntryMode::DIR { continue; }
                if let Some(ms) = self.max_size && content_length > ms { continue; }
                if let Some(ms) = self.min_size && content_length < ms { continue; }
                // exclude/include glob patterns
                if !self.exclude_patterns.is_empty() {
                    let matched = self.exclude_patterns.iter().any(|pat| fnmatch(pat, &name, self.ignore_case));
                    if matched { continue; }
                }
                if !self.include_patterns.is_empty() {
                    let matched = self.include_patterns.iter().any(|pat| fnmatch(pat, &name, self.ignore_case));
                    if !matched { continue; }
                }
                out.push((name, mode));
            }
            Some(out)
        }).unwrap_or_default();
        self.dir_cache.insert(path.to_string(), (std::time::Instant::now(), result.clone()));
        result
    }

    fn evict_lru(&self) {
        if self.cache_max_size == 0 && self.cache_min_free_space == 0 { return; }
        // Collect all cached files with their sizes and access times
        let mut files: Vec<(PathBuf, u64, std::time::SystemTime)> = Vec::new();
        let mut total: u64 = 0;
        if let Ok(entries) = fs::read_dir(&self.cache_dir) {
            for e in entries.flatten() {
                if let Ok(meta) = e.metadata()
                    && meta.is_file() {
                        let size = meta.len();
                        total += size;
                        files.push((e.path(), size, meta.accessed().unwrap_or(std::time::UNIX_EPOCH)));
                }
            }
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
        if to_free == 0 { return; }
        // LRU: sort by access time ascending, remove oldest until under limit
        files.sort_by_key(|(_, _, atime)| *atime);
        let mut remaining = to_free;
        for (path, size, _) in files {
            if remaining == 0 { break; }
            let _ = fs::remove_file(&path);
            remaining = remaining.saturating_sub(size);
        }
    }
}

fn writeback_worker(op: Arc<Operator>, queue: Arc<Mutex<VecDeque<(String, PathBuf)>>>, delay: Duration, max_age: Duration) {
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
                    && elapsed > max_age {
                        let _ = fs::remove_file(&cache_path);
                        continue;
                    }
        let data = match fs::read(&cache_path) {
            Ok(d) if !d.is_empty() => d,
            _ => { let _ = fs::remove_file(&cache_path); continue; }
        };
        let op = op.clone();
        let p = remote_path.clone();
        // Retry up to 3 times with exponential backoff
        for attempt in 0..3 {
            let r = rt().block_on(async { op.write(&p, data.clone()).await });
            match r {
                Ok(_) => {
                    let _ = fs::remove_file(&cache_path);
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
        let _ = fs::create_dir_all(&self.cache_dir);
        // Enable readdirplus for stat+readdir in one round-trip
        let _ = config.add_capabilities(fuser::InitFlags::FUSE_DO_READDIRPLUS);
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

    fn access(&self, _req: &Request, _ino: INodeNo, _mask: AccessFlags, reply: ReplyEmpty) { reply.ok(); }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_string_lossy().to_string();
        let name2 = name.clone();
        let parent: u64 = parent.into();
        if name == "." || name == ".." {
            let p = if name == "." { parent } else { FUSE_ROOT_INO };
            let attr = self.resolve(p).map(|(_, k, s)| self.make_attr(p, s, k)).unwrap_or_else(|| self.make_attr(FUSE_ROOT_INO, 4096, FileType::Directory));
            reply.entry(&self.attr_ttl, &attr, Generation(0)); return;
        }
        let parent_path = self.resolve(parent).map(|(p, _, _)| p).unwrap_or_default();
        let full_path = if parent_path.is_empty() { name2 } else { format!("{}/{}", parent_path, name2) };
        if let Some((kind, size)) = self.stat_op(&full_path) {
            reply.entry(&self.attr_ttl, &self.make_attr(self.alloc_ino(&full_path, kind, size), size, kind), Generation(0));
        } else if self.case_insensitive {
            // Fallback: search directory listing for case-insensitive match
            let entries = self.list_op(&parent_path);
            let lower = name.to_lowercase();
            if let Some((matched_name, mode)) = entries.iter().find(|(n, _)| n.to_lowercase() == lower) {
                let mp = if parent_path.is_empty() { matched_name.clone() } else { format!("{}/{}", parent_path, matched_name) };
                let kind = match mode { EntryMode::DIR => FileType::Directory, _ => FileType::RegularFile };
                let size = self.stat_op(&mp).map(|(_, s)| s).unwrap_or(0);
                reply.entry(&self.attr_ttl, &self.make_attr(self.alloc_ino(&mp, kind, size), size, kind), Generation(0));
            } else {
                reply.error(Errno::ENOENT);
            }
        } else { reply.error(Errno::ENOENT); }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let ino: u64 = ino.into();
        if ino == FUSE_ROOT_INO { reply.attr(&self.attr_ttl, &self.make_attr(ino, 4096, FileType::Directory)); return; }
        // Use cached attr from inode table — skip network stat_op
        // S3 objects are immutable; only refresh on write/open
        if let Some((_, kind, size)) = self.resolve(ino) {
            reply.attr(&self.attr_ttl, &self.make_attr(ino, size, kind));
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
        reply.statfs(total_blocks, total_blocks, total_blocks, total_inodes, total_inodes, BLOCK_SIZE, 255, 0);
    }

    fn readdir(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, offset: u64, mut reply: ReplyDirectory) {
        let ino: u64 = ino.into();
        if ino != FUSE_ROOT_INO { reply.error(Errno::ENOENT); return; }
        let path = self.resolve(ino).map(|(p, _, _)| p).unwrap_or_default();
        let mut entries = vec![(".".to_string(), FileType::Directory), ("..".to_string(), FileType::Directory)];
        for (name, mode) in self.list_op(&path) {
            entries.push((name, match mode { EntryMode::DIR => FileType::Directory, _ => FileType::RegularFile }));
        }
        let start = offset as usize;
        if start >= entries.len() { reply.ok(); return; }
        for (i, (name, kind)) in entries.iter().enumerate().skip(start) {
            let cp = if path.is_empty() { name.clone() } else { format!("{}/{}", path, name) };
            let size = self.stat_op(&cp).map(|(_, s)| s).unwrap_or(0);
            if reply.add(INodeNo(self.alloc_ino(&cp, *kind, size)), (i + 1) as u64, *kind, name) { break; }
        }
        reply.ok();
    }

    fn readdirplus(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, offset: u64, mut reply: ReplyDirectoryPlus) {
        let ino: u64 = ino.into();
        if ino != FUSE_ROOT_INO { reply.error(Errno::ENOENT); return; }
        let path = self.resolve(ino).map(|(p, _, _)| p).unwrap_or_default();
        let mut entries = vec![(".".to_string(), FileType::Directory), ("..".to_string(), FileType::Directory)];
        for (name, mode) in self.list_op(&path) {
            entries.push((name, match mode { EntryMode::DIR => FileType::Directory, _ => FileType::RegularFile }));
        }
        let start = offset as usize;
        if start >= entries.len() { reply.ok(); return; }
        for (i, (name, kind)) in entries.iter().enumerate().skip(start) {
            let cp = if path.is_empty() { name.clone() } else { format!("{}/{}", path, name) };
            let ino = self.alloc_ino(&cp, *kind, 0);
            let attr = self.make_attr(ino, 0, *kind);
            if reply.add(INodeNo(ino), (i + 1) as u64, name.as_str(), &self.attr_ttl, &attr, Generation(0)) { break; }
        }
        reply.ok();
    }

    fn opendir(&self, _req: &Request, _ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) { reply.opened(FileHandle(1), FopenFlags::empty()); }
    fn releasedir(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle, _flags: OpenFlags, reply: ReplyEmpty) { reply.ok(); }

    fn create(&self, _req: &Request, parent: INodeNo, name: &OsStr, _mode: u32, _umask: u32, _flags: i32, reply: ReplyCreate) {
        let name = name.to_string_lossy();
        let parent_path = self.resolve(parent.into()).map(|(p, _, _)| p).unwrap_or_default();
        let full_path = if parent_path.is_empty() { name.to_string() } else { format!("{}/{}", parent_path, name) };
        let ino = self.alloc_ino(&full_path, FileType::RegularFile, 0);
        self.handles.insert(ino, (full_path.clone(), false, None));
        reply.created(&self.attr_ttl, &self.make_attr(ino, 0, FileType::RegularFile), Generation(0), FileHandle(ino), FopenFlags::empty());
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let ino: u64 = ino.into();
        if let Some((path, FileType::RegularFile, _)) = self.resolve(ino) {
            self.handles.insert(ino, (path, false, None));
        }
        reply.opened(FileHandle(ino), FopenFlags::empty());
    }

    fn read(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, offset: u64, size: u32, _flags: OpenFlags, _lock_owner: Option<LockOwner>, reply: ReplyData) {
        let ino: u64 = ino.into();
        let path = match self.resolve(ino) { Some((p, _, _)) => p, None => { reply.error(Errno::ENOENT); return; } };
        // 1. Check memory cache first (fast path)
        if let Some(entry) = self.mem_cache.get(&ino) {
            let data = entry.value().clone();
            let start = offset as usize;
            let end = (start + size as usize).min(data.len());
            if start < data.len() { reply.data(&data[start..end]); } else { reply.data(&[]); }
            return;
        }
        // 2. Check disk cache
        if !self.direct_io {
            let cpath = cache_path(&self.cache_dir, &path);
            if cpath.exists()
                && let Ok(data) = fs::read(&cpath) {
                    // Populate memory cache
                    let b = bytes::Bytes::from(data);
                    let start = offset as usize;
                    let end = (start + size as usize).min(b.len());
                    if start < b.len() { reply.data(&b[start..end]); } else { reply.data(&[]); }
                    self.mem_cache.insert(ino, b);
                    return;
                }
        }
        // 3. Fetch from remote
        let op = self.op.clone(); let p = path.clone();
        let fetch_size = if self.read_chunk_size > 0 { self.read_chunk_size.max(size as u64) } else { u64::MAX };
        match rt().block_on(async move { op.read_with(&p).range(offset..offset+fetch_size).await }) {
            Ok(buf) => {
                let b: bytes::Bytes = buf.to_vec().into();
                let slice = &b[..(b.len() as u32).min(size) as usize];
                reply.data(slice);
                self.mem_cache.insert(ino, b);
            }
            Err(_) => reply.error(Errno::EIO),
        }
        // Read-ahead: pre-fetch next block in background
        if self.read_ahead > 0 {
            let op = self.op.clone();
            let p = path.clone();
            let next = offset + size as u64;
            let ahead = self.read_ahead;
            let cdir = self.cache_dir.clone();
            thread::spawn(move || {
                let _ = rt().block_on(async {
                    let data = op.read_with(&p).range(next..).await?;
                    let bytes = data.to_vec();
                    // Store pre-fetched data in local cache
                    let cpath = crate::cache_path(&cdir, &p);
                    use std::io::{Write, Seek};
                    if let Some(parent) = cpath.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    if let Ok(mut f) = std::fs::OpenOptions::new()
                        .create(true).truncate(false).write(true).read(true).open(&cpath)
                    {
                        let _ = f.seek(std::io::SeekFrom::Start(next));
                        let _ = f.write_all(&bytes[..bytes.len().min(ahead as usize)]);
                    }
                    Ok::<_, opendal::Error>(())
                });
            });
        }
    }

    fn write(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, offset: u64, data: &[u8], _write_flags: WriteFlags, _flags: OpenFlags, _lock_owner: Option<LockOwner>, reply: ReplyWrite) {
        let fh_val: u64 = fh.into();
        let path = match self.handles.get(&fh_val).map(|r| { let (p, d, _) = r.clone(); (p, d) }) {
            Some((p, _)) => p,
            None => { reply.error(Errno::ENOENT); return; }
        };
        if self.direct_io {
            let op = self.op.clone(); let p = path.clone(); let d = data.to_vec();
            match rt().block_on(async move { op.write(&p, d).await }) {
                Ok(_) => reply.written(data.len() as u32),
                Err(_) => reply.error(Errno::EIO),
            }
            return;
        }
        let cpath = cache_path(&self.cache_dir, &path);
        if let Some(parent) = cpath.parent() { let _ = fs::create_dir_all(parent); }
        let result = (|| -> std::io::Result<()> {
            let file = fs::OpenOptions::new().create(true).truncate(true).write(true).read(true).open(&cpath)?;
            let end = offset + data.len() as u64;
            let current_len = file.metadata()?.len();
            if end > current_len { file.set_len(end)?; }
            let mut f = file; f.seek(SeekFrom::Start(offset))?; f.write_all(data)?; f.flush()?; Ok(())
        })();
        self.evict_lru();
        match result {
            Ok(()) => { self.handles.insert(fh_val, (path, true, Some(std::time::Instant::now()))); reply.written(data.len() as u32); }
            Err(_) => reply.error(Errno::EIO),
        }
    }

    fn mkdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, _mode: u32, _umask: u32, reply: ReplyEntry) {
        let name = name.to_string_lossy();
        let parent_path = self.resolve(parent.into()).map(|(p, _, _)| p).unwrap_or_default();
        let full_path = if parent_path.is_empty() { name.to_string() } else { format!("{}/{}", parent_path, name) };
        let dir_path = format!("{}/", full_path.trim_end_matches('/'));
        let op = self.op.clone(); let p = dir_path.clone();
        match rt().block_on(async move { op.create_dir(&p).await }) {
            Ok(_) => {
                let ino = self.alloc_ino(&full_path, FileType::Directory, 4096);
                reply.entry(&self.attr_ttl, &self.make_attr(ino, 4096, FileType::Directory), Generation(0));
            }
            Err(_) => reply.error(Errno::EIO),
        }
    }

    fn rmdir(&self, _req: &Request, _parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy();
        let dir_path = format!("{}/", name.trim_end_matches('/'));
        let op = self.op.clone(); let p = dir_path.clone();
        let _ = rt().block_on(async move { op.delete(&p).await });
        reply.ok();
    }

    fn rename(&self, _req: &Request, _parent: INodeNo, name: &OsStr, _newparent: INodeNo, newname: &OsStr, _flags: fuser::RenameFlags, reply: ReplyEmpty) {
        let src = name.to_string_lossy().to_string();
        let dst = newname.to_string_lossy().to_string();
        let op = self.op.clone();
        rt().block_on(async move {
            if op.copy(&src, &dst).await.is_ok() {
                let _ = op.delete(&src).await;
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
                Ok(meta) => {
                    match name.as_ref() {
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
                    }
                }
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

    fn setattr(&self, _req: &Request, ino: INodeNo, mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>, _atime: Option<TimeOrNow>, _mtime: Option<TimeOrNow>, _ctime: Option<SystemTime>, _fh: Option<FileHandle>, _crtime: Option<SystemTime>, _chgtime: Option<SystemTime>, _bkuptime: Option<SystemTime>, _flags: Option<fuser::BsdFileFlags>, reply: ReplyAttr) {
        let ino: u64 = ino.into();
        if let Some((p, kind, _)) = self.resolve(ino) {
            if let Some(s) = size {
                let cpath = cache_path(&self.cache_dir, &p);
                if cpath.exists() { let _ = fs::write(&cpath, &[] as &[u8]); }
                self.alloc_ino(&p, kind, s);
            }
            // mode/uid/gid — just record them for now (S3 has no chmod)
            let mut perm = if kind == FileType::Directory { 0o755u16 } else { 0o644u16 };
            if let Some(m) = mode { perm = (m & 0o7777) as u16; }
            let mut attr = self.make_attr(ino, size.unwrap_or(0), kind);
            attr.perm = perm;
            if let Some(u) = uid { attr.uid = u; }
            if let Some(g) = gid { attr.gid = g; }
            reply.attr(&self.attr_ttl, &attr);
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn unlink(&self, _req: &Request, _parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy();
        let op = self.op.clone(); let p = name.to_string();
        let _ = rt().block_on(async move { op.delete(&p).await });
        // Also remove from local cache
        let cpath = cache_path(&self.cache_dir, &name);
        let _ = fs::remove_file(&cpath);
        reply.ok();
    }

    fn flush(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, _lock_owner: LockOwner, reply: ReplyEmpty) {
        let fh_val: u64 = fh.into();
        let (path, dirty) = {
            let entry = self.handles.get(&fh_val).map(|r| r.clone());
            if let Some((p, d, _)) = entry {
                if d { self.handles.insert(fh_val, (p.clone(), false, None)); }
                (p, d)
            } else { return reply.ok(); }
        };
        if dirty {
            let cpath = cache_path(&self.cache_dir, &path);
            if cpath.exists() {
                self.writeback_queue.lock().unwrap().push_back((path, cpath));
            }
        }
        reply.ok();
    }

    fn release(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, _flags: OpenFlags, _lock_owner: Option<LockOwner>, _flush: bool, reply: ReplyEmpty) {
        self.handles.remove(&fh.into());
        reply.ok();
    }
}
