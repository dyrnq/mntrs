pub mod cmd;

use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig,
    ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite,
    Request, INodeNo, FileHandle, OpenFlags, WriteFlags, AccessFlags, Errno, LockOwner,
};
use futures::StreamExt;
use opendal::{EntryMode, Operator};

fn rt() -> &'static tokio::runtime::Runtime {
    static RT: once_cell::sync::OnceCell<tokio::runtime::Runtime> = once_cell::sync::OnceCell::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().expect("tokio rt"))
}

const TTL: Duration = Duration::from_secs(1);
const FUSE_ROOT_INO: u64 = 1;

pub struct MntrsFs {
    pub op: Arc<Operator>,
    inodes: Mutex<HashMap<u64, (String, FileType, u64)>>,
    dir_cache: Mutex<HashMap<String, (std::time::Instant, Vec<(String, EntryMode)>)>>,
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
                    let kind = match meta.mode() {
                        EntryMode::DIR => FileType::Directory,
                        _ => FileType::RegularFile,
                    };
                    Some((kind, meta.content_length()))
                }
                Err(_) => {
                    let op2 = self.op.clone();
                    let p2 = format!("{}/", path.trim_end_matches('/'));
                    if let Ok(mut l) = op2.lister(&p2).await {
                        if l.next().await.is_some() {
                            return Some((FileType::Directory, 4096));
                        }
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
                if t.elapsed() < Duration::from_secs(10) {
                    return entries.clone();
                }
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
        let mut cache = self.dir_cache.lock().unwrap();
        cache.insert(path.to_string(), (std::time::Instant::now(), result.clone()));
        result
    }
}

impl Filesystem for MntrsFs {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> std::io::Result<()> {
        self.alloc_ino("", FileType::Directory, 4096);
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
            let attr = self.resolve(p)
                .map(|(_, k, s)| make_attr(p, s, k))
                .unwrap_or_else(|| make_attr(FUSE_ROOT_INO, 4096, FileType::Directory));
            reply.entry(&TTL, &attr, fuser::Generation(0));
            return;
        }
        let parent_path = self.resolve(parent).map(|(p, _, _)| p).unwrap_or_default();
        let full_path = if parent_path.is_empty() { name2.clone() } else { format!("{}/{}", parent_path, name2) };
        if let Some((kind, size)) = self.stat_op(&full_path) {
            let ino = self.alloc_ino(&full_path, kind, size);
            reply.entry(&TTL, &make_attr(ino, size, kind), fuser::Generation(0));
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let ino: u64 = ino.into();
        if ino == FUSE_ROOT_INO {
            reply.attr(&TTL, &make_attr(ino, 4096, FileType::Directory));
            return;
        }
        match self.resolve(ino) {
            Some((path, kind, _size)) => {
                let (actual_kind, actual_size) = self.stat_op(&path).unwrap_or((kind, 0));
                reply.attr(&TTL, &make_attr(ino, actual_size, actual_kind));
            }
            None => reply.error(Errno::ENOENT),
        }
    }

    fn readdir(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, offset: u64, mut reply: ReplyDirectory) {
        let ino: u64 = ino.into();
        if ino != FUSE_ROOT_INO { reply.error(Errno::ENOENT); return; }
        let path = self.resolve(ino).map(|(p, _, _)| p).unwrap_or_default();

        let mut entries: Vec<(String, FileType)> = vec![
            (".".to_string(), FileType::Directory),
            ("..".to_string(), FileType::Directory),
        ];
        let list_path = path.clone();
        for (name, mode) in self.list_op(&list_path) {
            let kind = match mode { EntryMode::DIR => FileType::Directory, _ => FileType::RegularFile };
            entries.push((name, kind));
        }

        let start = if offset <= 0 { 0 } else { offset as usize };
        if start >= entries.len() { reply.ok(); return; }
        for i in start..entries.len() {
            let (ref name, kind) = entries[i];
            let child_path = if path.is_empty() { name.clone() } else { format!("{}/{}", path, name) };
            let size = self.stat_op(&child_path).map(|(_, s)| s).unwrap_or(0);
            let ino = self.alloc_ino(&child_path, kind, size);
            if reply.add(INodeNo(ino), (i + 1) as u64, kind, name) { break; }
        }
        reply.ok();
    }

    fn opendir(&self, _req: &Request, _ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) { reply.opened(FileHandle(1), fuser::FopenFlags::empty()); }
    fn releasedir(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle, _flags: OpenFlags, reply: ReplyEmpty) { reply.ok(); }
    fn open(&self, _req: &Request, _ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) { reply.opened(FileHandle(0), fuser::FopenFlags::empty()); }

    fn read(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, offset: u64, size: u32, _flags: OpenFlags, _lock_owner: Option<LockOwner>, reply: ReplyData) {
        let ino: u64 = ino.into();
        let path = match self.resolve(ino) {
            Some((p, _, _)) => p,
            None => { reply.error(Errno::ENOENT); return; }
        };
        let op = self.op.clone();
        let p = path.clone();
        let result = rt().block_on(async move { op.read_with(&p).range(offset..).await });
        match result {
            Ok(buf) => {
                let bytes = buf.to_vec();
                let len = (bytes.len() as u32).min(size) as usize;
                reply.data(&bytes[..len]);
            }
            Err(_) => reply.error(Errno::EIO),
        }
    }

    fn write(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle, _offset: u64, _data: &[u8], _write_flags: WriteFlags, _flags: OpenFlags, _lock_owner: Option<LockOwner>, reply: ReplyWrite) {
        reply.error(Errno::EIO);
    }

    fn flush(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle, _lock_owner: LockOwner, reply: ReplyEmpty) { reply.ok(); }
    fn release(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle, _flags: OpenFlags, _lock_owner: Option<LockOwner>, _flush: bool, reply: ReplyEmpty) { reply.ok(); }
}
