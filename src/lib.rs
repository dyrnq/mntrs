pub mod cmd;

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::io::{Write, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{TimeOrNow,
    FileAttr, FileType, Filesystem, KernelConfig,
    ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, ReplyCreate, ReplyXattr,
    Request, INodeNo, FileHandle, OpenFlags, WriteFlags, AccessFlags, Errno, FopenFlags, Generation,
    LockOwner,
};
use futures::StreamExt;
use opendal::{EntryMode, Operator};

fn rt() -> &'static tokio::runtime::Runtime {
    static RT: once_cell::sync::OnceCell<tokio::runtime::Runtime> = once_cell::sync::OnceCell::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().expect("tokio rt"))
}

const TTL: Duration = Duration::from_secs(1);
const FUSE_ROOT_INO: u64 = 1;
const DIR_CACHE_TTL: Duration = Duration::from_secs(10);

pub struct MntrsFs {
    pub op: Arc<Operator>,
    inodes: Mutex<HashMap<u64, (String, FileType, u64)>>,
    dir_cache: Mutex<HashMap<String, (std::time::Instant, Vec<(String, EntryMode)>)>>,
    cache_dir: PathBuf,
    handles: Mutex<HashMap<u64, (String, bool)>>,
}

fn make_attr(ino: u64, size: u64, kind: FileType) -> FileAttr {
    let now = UNIX_EPOCH;
    let perm = if kind == FileType::Directory { 0o755u16 } else { 0o644u16 };
    FileAttr {
        ino: INodeNo(ino), size, blocks: (size + 4095) / 4096,
        atime: now, mtime: now, ctime: now, crtime: now,
        kind, perm,
        nlink: if kind == FileType::Directory { 2 } else { 1 },
        uid: 1000, gid: 1000, rdev: 0, blksize: 4096, flags: 0,
    }
}

fn path_hash(path: &str) -> u64 {
    let mut h: u64 = 0x811c9dc5;
    for b in path.bytes() { h = h.wrapping_mul(0x01000193) ^ b as u64; }
    (h & 0x7FFFFFFFFFFFFFFF).max(2)
}

fn cache_path(cache_dir: &PathBuf, path: &str) -> PathBuf {
    cache_dir.join(format!("{:020x}", path_hash(path)))
}

impl MntrsFs {
    fn resolve(&self, ino: u64) -> Option<(String, FileType, u64)> {
        self.inodes.lock().unwrap().get(&ino).cloned()
    }

    fn alloc_ino(&self, path: &str, kind: FileType, size: u64) -> u64 {
        let ino = path_hash(path);
        let mut map = self.inodes.lock().unwrap();
        map.entry(ino).or_insert((path.to_string(), kind, size));
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
                    if let Ok(mut l) = op2.lister(&p2).await {
                        if l.next().await.is_some() { return Some((FileType::Directory, 4096)); }
                    }
                    None
                }
            }
        })
    }

    fn list_op(&self, path: &str) -> Vec<(String, EntryMode)> {
        {
            let cache = self.dir_cache.lock().unwrap();
            if let Some((t, entries)) = cache.get(path) {
                if t.elapsed() < DIR_CACHE_TTL { return entries.clone(); }
            }
        }
        let result = rt().block_on(async {
            let op = self.op.clone();
            let p = path.to_string();
            let mut lister = op.lister(&p).await.ok()?;
            let mut out = vec![];
            while let Some(Ok(entry)) = lister.next().await {
                let name = entry.name().trim_end_matches('/').to_string();
                let mode = entry.metadata().mode();
                out.push((name, mode));
            }
            Some(out)
        }).unwrap_or_default();
        self.dir_cache.lock().unwrap().insert(path.to_string(), (std::time::Instant::now(), result.clone()));
        result
    }
}

impl Filesystem for MntrsFs {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> std::io::Result<()> {
        self.alloc_ino("", FileType::Directory, 4096);
        let _ = fs::create_dir_all(&self.cache_dir);
        Ok(())
    }

    fn access(&self, _req: &Request, _ino: INodeNo, _mask: AccessFlags, reply: ReplyEmpty) { reply.ok(); }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_string_lossy().to_string();
        let name2 = name.clone();
        let parent: u64 = parent.into();
        if name == "." || name == ".." {
            let p = if name == "." { parent } else { FUSE_ROOT_INO };
            let attr = self.resolve(p).map(|(_, k, s)| make_attr(p, s, k)).unwrap_or_else(|| make_attr(FUSE_ROOT_INO, 4096, FileType::Directory));
            reply.entry(&TTL, &attr, Generation(0)); return;
        }
        let parent_path = self.resolve(parent).map(|(p, _, _)| p).unwrap_or_default();
        let full_path = if parent_path.is_empty() { name2 } else { format!("{}/{}", parent_path, name2) };
        if let Some((kind, size)) = self.stat_op(&full_path) {
            reply.entry(&TTL, &make_attr(self.alloc_ino(&full_path, kind, size), size, kind), Generation(0));
        } else { reply.error(Errno::ENOENT); }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let ino: u64 = ino.into();
        if ino == FUSE_ROOT_INO { reply.attr(&TTL, &make_attr(ino, 4096, FileType::Directory)); return; }
        match self.resolve(ino) {
            Some((path, kind, _)) => {
                let (ak, asz) = self.stat_op(&path).unwrap_or((kind, 0));
                reply.attr(&TTL, &make_attr(ino, asz, ak));
            }
            None => reply.error(Errno::ENOENT),
        }
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
        for i in start..entries.len() {
            let (ref name, kind) = entries[i];
            let cp = if path.is_empty() { name.clone() } else { format!("{}/{}", path, name) };
            let size = self.stat_op(&cp).map(|(_, s)| s).unwrap_or(0);
            if reply.add(INodeNo(self.alloc_ino(&cp, kind, size)), (i + 1) as u64, kind, name) { break; }
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
        self.handles.lock().unwrap().insert(ino, (full_path.clone(), false));
        reply.created(&TTL, &make_attr(ino, 0, FileType::RegularFile), Generation(0), FileHandle(ino), FopenFlags::empty());
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let ino: u64 = ino.into();
        if let Some((path, FileType::RegularFile, _)) = self.resolve(ino) {
            self.handles.lock().unwrap().insert(ino, (path, false));
        }
        reply.opened(FileHandle(ino), FopenFlags::empty());
    }

    fn read(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, offset: u64, size: u32, _flags: OpenFlags, _lock_owner: Option<LockOwner>, reply: ReplyData) {
        let ino: u64 = ino.into();
        let path = match self.resolve(ino) { Some((p, _, _)) => p, None => { reply.error(Errno::ENOENT); return; } };
        let cpath = cache_path(&self.cache_dir, &path);
        if cpath.exists() {
            if let Ok(data) = fs::read(&cpath) {
                let start = offset as usize;
                let end = (start + size as usize).min(data.len());
                if start < data.len() { reply.data(&data[start..end]); } else { reply.data(&[]); }
                return;
            }
        }
        let op = self.op.clone(); let p = path.clone();
        match rt().block_on(async move { op.read_with(&p).range(offset..).await }) {
            Ok(buf) => { let b = buf.to_vec(); reply.data(&b[..(b.len() as u32).min(size) as usize]); }
            Err(_) => reply.error(Errno::EIO),
        }
    }

    fn write(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, offset: u64, data: &[u8], _write_flags: WriteFlags, _flags: OpenFlags, _lock_owner: Option<LockOwner>, reply: ReplyWrite) {
        let fh_val: u64 = fh.into();
        let path = match self.handles.lock().unwrap().get(&fh_val).cloned() {
            Some((p, _)) => p,
            None => { reply.error(Errno::ENOENT); return; }
        };
        let cpath = cache_path(&self.cache_dir, &path);
        if let Some(parent) = cpath.parent() { let _ = fs::create_dir_all(parent); }
        let result = (|| -> std::io::Result<()> {
            let file = fs::OpenOptions::new().create(true).write(true).read(true).open(&cpath)?;
            let end = offset as u64 + data.len() as u64;
            let current_len = file.metadata()?.len();
            if end > current_len { file.set_len(end)?; }
            let mut f = file; f.seek(SeekFrom::Start(offset))?; f.write_all(data)?; f.flush()?; Ok(())
        })();
        match result {
            Ok(()) => { self.handles.lock().unwrap().insert(fh_val, (path, true)); reply.written(data.len() as u32); }
            Err(_) => reply.error(Errno::EIO),
        }
    }

    fn getxattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, _size: u32, reply: ReplyXattr) {
        reply.error(Errno::ENODATA);
    }
    fn listxattr(&self, _req: &Request, _ino: INodeNo, _size: u32, reply: ReplyXattr) {
        reply.error(Errno::ENODATA);
    }

    fn setattr(&self, _req: &Request, ino: INodeNo, _mode: Option<u32>, _uid: Option<u32>, _gid: Option<u32>, size: Option<u64>, _atime: Option<TimeOrNow>, _mtime: Option<TimeOrNow>, _ctime: Option<SystemTime>, _fh: Option<FileHandle>, _crtime: Option<SystemTime>, _chgtime: Option<SystemTime>, _bkuptime: Option<SystemTime>, _flags: Option<fuser::BsdFileFlags>, reply: ReplyAttr) {
        let ino: u64 = ino.into();
        if let Some((p, kind, _)) = self.resolve(ino) {
            if let Some(s) = size {
                let cpath = cache_path(&self.cache_dir, &p);
                if cpath.exists() { let _ = fs::write(&cpath, &[] as &[u8]); }
                self.alloc_ino(&p, kind, s);
            }
            reply.attr(&TTL, &make_attr(ino, size.unwrap_or(0), kind));
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

    fn flush(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle, _lock_owner: LockOwner, reply: ReplyEmpty) {
        reply.ok();
    }

    fn release(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, _flags: OpenFlags, _lock_owner: Option<LockOwner>, _flush: bool, reply: ReplyEmpty) {
        self.handles.lock().unwrap().remove(&fh.into());
        reply.ok();
    }
}
