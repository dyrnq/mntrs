pub mod cmd;

use std::ffi::OsStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig,
    ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite,
    Request,
};
use futures::StreamExt;
use libc::{ENOENT, EIO, ENOSYS};
use opendal::{EntryMode, Metadata, Operator};
use once_cell::sync::OnceCell;

fn rt() -> &'static tokio::runtime::Runtime {
    static RT: OnceCell<tokio::runtime::Runtime> = OnceCell::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().expect("tokio rt"))
}

const TTL: Duration = Duration::from_secs(1);
const FUSE_ROOT_INO: u64 = 1;

pub struct MntrsFs {
    pub op: Arc<Operator>,
}

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

impl MntrsFs {
    fn inode_for(&self, name: &str) -> u64 {
        let mut h = 0x811c9dc5u64;
        for b in name.bytes() { h = h.wrapping_mul(0x01000193) ^ b as u64; }
        (h & 0x7FFFFFFFFFFFFFFF).max(2)
    }
}

impl Filesystem for MntrsFs {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), i32> { Ok(()) }
    fn access(&mut self, _req: &Request<'_>, _ino: u64, _mask: i32, reply: ReplyEmpty) { reply.ok(); }

    fn lookup(&mut self, _req: &Request<'_>, _parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_string_lossy().to_string();
        let name2 = name.clone();
        if name == "." || name == ".." {
            reply.entry(&TTL, &make_attr(FUSE_ROOT_INO, 4096, FileType::Directory), 0);
            return;
        }
        let op = self.op.clone();
        let result = rt().block_on(async move { op.stat(&name2).await });
        match result {
            Ok(meta) => {
                let ino = self.inode_for(&name);
                let kind = match meta.mode() {
                    EntryMode::DIR => FileType::Directory,
                    _ => FileType::RegularFile,
                };
                reply.entry(&TTL, &make_attr(ino, meta.content_length(), kind), 0);
            }
            Err(_) => reply.error(ENOENT),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, _ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        reply.attr(&TTL, &make_attr(FUSE_ROOT_INO, 4096, FileType::Directory));
    }

    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(1, 0);
    }

    fn readdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        if ino != FUSE_ROOT_INO { reply.error(ENOENT); return; }

        let entries: &[(&str, FileType)] = &[
            (".", FileType::Directory),
            ("..", FileType::Directory),
        ];

        // Fetch from S3
        let op = self.op.clone();
        let remote: Vec<(String, EntryMode)> = rt().block_on(async move {
            let mut lister = op.lister("").await.ok()?;
            let mut out = vec![];
            while let Some(Ok(entry)) = lister.next().await {
                let name = entry.name().trim_end_matches('/').to_string();
                let mode = entry.metadata().mode();
                out.push((name, mode));
            }
            Some(out)
        }).unwrap_or_default();

        let mut all: Vec<(&str, FileType)> = entries.to_vec();
        let (static_names, static_types): (Vec<_>, Vec<_>) = all.iter().map(|(n, k)| (*n, *k)).unzip();
        drop(all);

        // 把 entries 改为包含远程条目
        let start = if offset <= 0 { 0 } else { offset as usize };
        let total_entries = 2 + remote.len();
        if start >= total_entries { reply.ok(); return; }

        let mut idx = 0;
        if start <= idx && idx < total_entries {
            let ino = FUSE_ROOT_INO;
            if reply.add(ino, (idx + 1) as i64, FileType::Directory, ".") { reply.ok(); return; }
        }
        idx = 1;
        if start <= idx && idx < total_entries {
            if reply.add(FUSE_ROOT_INO, (idx + 1) as i64, FileType::Directory, "..") { reply.ok(); return; }
        }
        idx = 2;
        for (name, mode) in &remote {
            if start > idx { idx += 1; continue; }
            let ino = self.inode_for(name);
            let kind = match mode { EntryMode::DIR => FileType::Directory, _ => FileType::RegularFile };
            if reply.add(ino, (idx + 1) as i64, kind, name) { break; }
            idx += 1;
        }

        reply.ok();
    }

    fn releasedir(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _flags: i32, reply: ReplyEmpty) { reply.ok(); }
    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) { reply.opened(0, 0); }
    fn read(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, offset: i64, size: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyData) { reply.error(EIO); }
    fn write(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, offset: i64, data: &[u8], _write_flags: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyWrite) { reply.error(EIO); }
    fn flush(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) { reply.ok(); }
    fn release(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _flags: i32, _lock_owner: Option<u64>, _flush: bool, reply: ReplyEmpty) { reply.ok(); }
}
