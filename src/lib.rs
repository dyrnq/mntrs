pub mod cmd;

use std::collections::HashMap;
use std::time::Instant;
use std::ffi::OsStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig,
    ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite,
    Request,
};
use futures::StreamExt;
use libc::{ENOENT, EIO};
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
    dir_cache: Mutex<HashMap<String, (Instant, Vec<(String, EntryMode)>)>>,
}

const DIR_CACHE_TTL: Duration = Duration::from_secs(10);

fn make_attr(ino: u64, size: u64, kind: FileType) -> FileAttr {
    let now = UNIX_EPOCH;
    let perm = if kind == FileType::Directory { 0o755u16 } else { 0o644u16 };
    FileAttr {
        ino, size, blocks: (size + 4095) / 4096,
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
                    // S3 directories are prefix-based; check if path/ has children
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
                if t.elapsed() < DIR_CACHE_TTL {
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
        cache.insert(path.to_string(), (Instant::now(), result.clone()));
        result
    }
}

impl Filesystem for MntrsFs {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), i32> {
        self.alloc_ino("", FileType::Directory, 4096);
        Ok(())
    }

    fn access(&mut self, _req: &Request<'_>, _ino: u64, _mask: i32, reply: ReplyEmpty) { reply.ok(); }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_string_lossy().to_string();
        if name == "." || name == ".." {
            let p = if name == "." { parent } else { FUSE_ROOT_INO };
            let attr = self.resolve(p)
                .map(|(_, k, s)| make_attr(p, s, k))
                .unwrap_or_else(|| make_attr(FUSE_ROOT_INO, 4096, FileType::Directory));
            reply.entry(&TTL, &attr, 0);
            return;
        }

        let parent_path = self.resolve(parent).map(|(p, _, _)| p).unwrap_or_default();
        let full_path = if parent_path.is_empty() { name.clone() } else { format!("{}/{}", parent_path, name) };

        if let Some((kind, size)) = self.stat_op(&full_path) {
            let ino = self.alloc_ino(&full_path, kind, size);
            reply.entry(&TTL, &make_attr(ino, size, kind), 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if ino == FUSE_ROOT_INO {
            reply.attr(&TTL, &make_attr(ino, 4096, FileType::Directory));
            return;
        }
        match self.resolve(ino) {
            Some((path, kind, _size)) => {
                let (actual_kind, actual_size) = self.stat_op(&path).unwrap_or((kind, 0));
                reply.attr(&TTL, &make_attr(ino, actual_size, actual_kind));
            }
            None => reply.error(ENOENT),
        }
    }

    fn readdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
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
            let size = 0u64; // size from stat_op would add HEAD overhead
            let ino = self.alloc_ino(&child_path, kind, size);
            if reply.add(ino, (i + 1) as i64, kind, name) { break; }
        }
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) { reply.opened(1, 0); }
    fn releasedir(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _flags: i32, reply: ReplyEmpty) { reply.ok(); }
    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) { reply.opened(0, 0); }

    fn read(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, size: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyData) {
        let path = match self.resolve(ino) {
            Some((p, _, _)) => p,
            None => { reply.error(ENOENT); return; }
        };
        let op = self.op.clone();
        let p = path.clone();
        let result = rt().block_on(async move {
            op.read_with(&p).range(offset as u64..).await
        });
        match result {
            Ok(buf) => {
                let bytes = buf.to_vec();
                let len = bytes.len().min(size as usize);
                reply.data(&bytes[..len]);
            }
            Err(_) => reply.error(EIO),
        }
    }

    fn write(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _offset: i64, _data: &[u8], _write_flags: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyWrite) { reply.error(EIO); }
    fn flush(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) { reply.ok(); }
    fn release(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _flags: i32, _lock_owner: Option<u64>, _flush: bool, reply: ReplyEmpty) { reply.ok(); }
}
