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
use libc::{ENOENT, EIO};
use opendal::{EntryMode, Metadata, Operator};
use tokio::runtime::Runtime;

// 全局多线程 Tokio Runtime，不依赖 FUSE 工作线程
fn rt() -> &'static Runtime {
    static RT: once_cell::sync::OnceCell<Runtime> = once_cell::sync::OnceCell::new();
    RT.get_or_init(|| {
        Runtime::new().expect("failed to create tokio runtime")
    })
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

    fn root_attr(&self) -> FileAttr {
        make_attr(FUSE_ROOT_INO, 4096, FileType::Directory)
    }
}

impl Filesystem for MntrsFs {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), i32> { Ok(()) }
    fn access(&mut self, _req: &Request<'_>, _ino: u64, _mask: i32, reply: ReplyEmpty) { reply.ok(); }

    fn lookup(&mut self, _req: &Request<'_>, _parent: u64, name: &OsStr, reply: ReplyEntry) {
        std::fs::write("/tmp/mntrs-lookup.log", format!("lookup called parent={} name={:?}\n", _parent, name)).ok();
        let name = name.to_string_lossy().to_string();
        let name_owned = name.clone();
        if name == "." || name == ".." {
            reply.entry(&TTL, &self.root_attr(), 0);
        } else {
            let op = self.op.clone();
            let result = rt().block_on(async move { op.stat(&name_owned).await });
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
    }

    fn getattr(&mut self, _req: &Request<'_>, _ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        reply.attr(&TTL, &self.root_attr());
    }

    fn readdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, _offset: i64, mut reply: ReplyDirectory) {
        std::fs::write("/tmp/mntrs-readdir.log", format!("readdir called ino={} offset={} NO ASYNC\n", ino, _offset)).ok();
        if ino != FUSE_ROOT_INO { reply.error(ENOENT); return; }

        reply.add(FUSE_ROOT_INO, 1, FileType::Directory, ".");
        reply.add(FUSE_ROOT_INO, 2, FileType::Directory, "..");
        reply.add(3, 3, FileType::RegularFile, "hello.txt");
        reply.add(4, 4, FileType::Directory, "testdir");

        reply.ok();
    }

    fn readdirplus(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectoryPlus,
    ) {
        std::fs::write("/tmp/mntrs-readdirplus.log", format!("readdirplus called ino={} offset={}\n", ino, offset)).ok();
        if ino != FUSE_ROOT_INO { reply.error(libc::ENOSYS); return; }

        let op = self.op.clone();
        let entries: Vec<(String, EntryMode)> = rt().block_on(async move {
            let mut lister = op.lister("").await.ok()?;
            let mut out = vec![];
            while let Some(Ok(entry)) = lister.next().await {
                let name = entry.name().trim_end_matches('/').to_string();
                let mode = entry.metadata().mode();
                out.push((name, mode));
            }
            Some(out)
        }).unwrap_or_default();

        reply.add(FUSE_ROOT_INO, 1, ".", &TTL, &make_attr(FUSE_ROOT_INO, 4096, FileType::Directory), 0);
        reply.add(FUSE_ROOT_INO, 2, "..", &TTL, &make_attr(FUSE_ROOT_INO, 4096, FileType::Directory), 0);

        for (i, (name, mode)) in entries.iter().enumerate() {
            let ino_child = self.inode_for(name);
            let kind = match mode { EntryMode::DIR => FileType::Directory, _ => FileType::RegularFile };
            let size = if kind == FileType::Directory { 4096 } else { 0 };
            if reply.add(ino_child, (i + 3) as i64, name, &TTL, &make_attr(ino_child, size, kind), 0) {
                break;
            }
        }

        reply.ok();
    }

    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) { reply.opened(0, 0); }

    fn read(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _offset: i64, size: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyData) {
        reply.error(EIO);
    }

    fn write(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _offset: i64, data: &[u8], _write_flags: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyWrite) {
        reply.error(EIO);
    }

    fn flush(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) { reply.ok(); }
    fn release(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _flags: i32, _lock_owner: Option<u64>, _flush: bool, reply: ReplyEmpty) { reply.ok(); }
}
