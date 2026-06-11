//! fuser adapter — bridges `CoreFilesystem` to `fuser::Filesystem`.

use std::ffi::OsStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    AccessFlags, Errno, FileAttr, FileHandle, FileType, FopenFlags, Generation, INodeNo,
    KernelConfig, LockOwner, OpenFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr,
    Request, TimeOrNow, WriteFlags,
};

use super::{CoreFileAttr, CoreFileType, CoreFilesystem};

fn to_fuse_filetype(ft: CoreFileType) -> FileType {
    match ft {
        CoreFileType::Directory => FileType::Directory,
        CoreFileType::RegularFile => FileType::RegularFile,
        CoreFileType::Symlink => FileType::Symlink,
        CoreFileType::NamedPipe => FileType::NamedPipe,
        CoreFileType::CharDevice => FileType::CharDevice,
        CoreFileType::BlockDevice => FileType::BlockDevice,
        CoreFileType::Socket => FileType::Socket,
    }
}

fn from_core_attr(a: &CoreFileAttr) -> FileAttr {
    FileAttr {
        ino: INodeNo(a.ino),
        size: a.size,
        blocks: a.blocks,
        atime: a.atime,
        mtime: a.mtime,
        ctime: a.ctime,
        crtime: a.crtime,
        kind: to_fuse_filetype(a.kind),
        perm: a.perm,
        nlink: a.nlink,
        uid: a.uid,
        gid: a.gid,
        rdev: a.rdev,
        blksize: a.blksize,
        flags: a.flags,
    }
}

fn io_err_to_fuse_errno(e: std::io::Error) -> Errno {
    match e.kind() {
        std::io::ErrorKind::NotFound => Errno::ENOENT,
        std::io::ErrorKind::PermissionDenied => Errno::EACCES,
        std::io::ErrorKind::AlreadyExists => Errno::EEXIST,
        std::io::ErrorKind::InvalidInput => Errno::EINVAL,
        std::io::ErrorKind::NotADirectory => Errno::ENOTDIR,
        std::io::ErrorKind::IsADirectory => Errno::EISDIR,
        std::io::ErrorKind::OutOfMemory => Errno::ENOMEM,
        std::io::ErrorKind::StorageFull => Errno::ENOSPC,
        std::io::ErrorKind::TimedOut => Errno::ETIMEDOUT,
        std::io::ErrorKind::Interrupted => Errno::EINTR,
        std::io::ErrorKind::Unsupported => Errno::ENOSYS,
        _ => {
            let code = e.raw_os_error().unwrap_or(0);
            if code > 0 {
                Errno::from_i32(code)
            } else {
                Errno::EIO
            }
        }
    }
}

/// Adapter that wraps a `CoreFilesystem` and implements `fuser::Filesystem`.
pub struct FuserAdapter<F: CoreFilesystem + 'static> {
    pub inner: F,
    pub dir_cache_ttl: Duration,
    pub attr_ttl: Duration,
    pub direct_io: bool,
}

impl<F: CoreFilesystem + 'static> FuserAdapter<F> {
    pub fn new(inner: F, dir_cache_ttl: Duration, attr_ttl: Duration, direct_io: bool) -> Self {
        Self {
            inner,
            dir_cache_ttl,
            attr_ttl,
            direct_io,
        }
    }
}

impl<F: CoreFilesystem + 'static> fuser::Filesystem for FuserAdapter<F> {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> std::io::Result<()> {
        self.inner.init()
    }

    fn access(&self, _req: &Request, _ino: INodeNo, _mask: AccessFlags, reply: ReplyEmpty) {
        let ino: u64 = _ino.into();
        match self.inner.access(ino, _mask.bits() as u32) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_string_lossy();
        match self.inner.lookup(parent.into(), &name) {
            Ok(attr) => {
                let fattr = from_core_attr(&attr);
                reply.entry(&self.attr_ttl, &fattr, Generation(0));
            }
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match self.inner.getattr(ino.into()) {
            Ok(attr) => {
                let fattr = from_core_attr(&attr);
                // Reply with TTL=0 so the FUSE kernel does not cache
                // a stale file size across writes.
                //
                // The fuser library forwards a `valid` Duration to the
                // kernel via `fuse_entry_param::valid` / `attr_valid`.
                // When this is non-zero, the kernel stores the returned
                // attribute in its own dentry/inode cache and serves
                // the cached value to userspace for the requested
                // duration, *without* re-issuing a FUSE getattr. For
                // the default --attr-timeout=1 (1 second), any read
                // within 1s of a write sees the PRE-write size — the
                // kernel asks the filesystem to read the cached
                // smaller size and returns truncated content.
                //
                // Setting TTL=0 (Duration::ZERO) means the kernel
                // always treats the response as immediately stale and
                // re-fetches getattr on the next access. The extra
                // round-trip cost is one synchronous getattr per file
                // operation, which for the in-memory `inodes` DashMap
                // is a single hash lookup (sub-microsecond). For
                // network backends the `CoreFilesystem::getattr` is
                // a `stat_op` which is one round-trip to the remote,
                // so the total cost is one stat per open+read+close,
                // matching rclone's VFS behavior.
                //
                // An alternative would be to keep the CLI default TTL
                // and call `Session::notifier().inval_inode(ino, 0, 0)`
                // after every size-changing write, but that requires
                // wiring a Notifier handle into MntrsFs, complicating
                // the type system (the FuserAdapter moves `inner` into
                // a generic context, blocking post-mount access to
                // the same Arc). TTL=0 is the simpler, more robust
                // fix and the cost is bounded.
                reply.attr(&Duration::ZERO, &fattr);
            }
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
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
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let atime = atime.map(|t| match t {
            TimeOrNow::SpecificTime(t) => t,
            TimeOrNow::Now => SystemTime::now(),
        });
        let mtime = mtime.map(|t| match t {
            TimeOrNow::SpecificTime(t) => t,
            TimeOrNow::Now => SystemTime::now(),
        });
        match self
            .inner
            .setattr(ino.into(), mode, uid, gid, size, atime, mtime)
        {
            Ok(attr) => {
                let fattr = from_core_attr(&attr);
                reply.attr(&self.attr_ttl, &fattr);
            }
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        match self.inner.readdir(ino.into()) {
            Ok(entries) => {
                let start = offset as usize;
                if start >= entries.len() {
                    reply.ok();
                    return;
                }
                for (i, entry) in entries.iter().enumerate().skip(start) {
                    if reply.add(
                        INodeNo(entry.ino),
                        (i + 1) as u64,
                        to_fuse_filetype(entry.kind),
                        &entry.name,
                    ) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn readdirplus(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectoryPlus,
    ) {
        match self.inner.readdir(ino.into()) {
            Ok(entries) => {
                let start = offset as usize;
                if start >= entries.len() {
                    reply.ok();
                    return;
                }
                for (i, entry) in entries.iter().enumerate().skip(start) {
                    // For each directory entry, do a lookup to get full attr
                    let attr = self.inner.lookup(ino.into(), &entry.name).ok();
                    let fattr = attr.as_ref().map(from_core_attr).unwrap_or_else(|| {
                        let a = CoreFileAttr {
                            ino: entry.ino,
                            size: 0,
                            blocks: 0,
                            atime: UNIX_EPOCH,
                            mtime: UNIX_EPOCH,
                            ctime: UNIX_EPOCH,
                            crtime: UNIX_EPOCH,
                            kind: entry.kind,
                            perm: 0,
                            nlink: 1,
                            uid: 0,
                            gid: 0,
                            rdev: 0,
                            blksize: 4096,
                            flags: 0,
                        };
                        from_core_attr(&a)
                    });
                    if reply.add(
                        INodeNo(entry.ino),
                        (i + 1) as u64,
                        &entry.name,
                        &self.attr_ttl,
                        &fattr,
                        Generation(0),
                    ) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        match self.inner.opendir(ino.into()) {
            Ok(fh) => reply.opened(FileHandle(fh), FopenFlags::empty()),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn releasedir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        match self.inner.releasedir(ino.into(), _fh.into()) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let name = name.to_string_lossy();
        match self.inner.create(parent.into(), &name, mode) {
            Ok(attr) => {
                let ino = attr.ino;
                let fattr = from_core_attr(&attr);
                let flags = if self.direct_io {
                    FopenFlags::FOPEN_DIRECT_IO
                } else {
                    FopenFlags::empty()
                };
                reply.created(
                    &self.attr_ttl,
                    &fattr,
                    Generation(0),
                    FileHandle(ino),
                    flags,
                );
            }
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let flags = if self.direct_io {
            FopenFlags::FOPEN_DIRECT_IO
        } else {
            FopenFlags::empty()
        };
        match self.inner.open(ino.into(), _flags.0 as u32) {
            Ok(fh) => reply.opened(FileHandle(fh), flags),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
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
        match self.inner.read(ino.into(), _fh.into(), offset, size) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        match self.inner.write(ino.into(), fh.into(), offset, data) {
            Ok(written) => reply.written(written),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        match self.inner.flush(ino.into(), fh.into()) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        match self.inner.release(ino.into(), fh.into()) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
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
        match self.inner.mkdir(parent.into(), &name) {
            Ok(attr) => {
                let fattr = from_core_attr(&attr);
                reply.entry(&self.attr_ttl, &fattr, Generation(0));
            }
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn rmdir(&self, _req: &Request, _parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy();
        match self.inner.rmdir(_parent.into(), &name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
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
        let name = name.to_string_lossy();
        let newname = newname.to_string_lossy();
        match self
            .inner
            .rename(_parent.into(), &name, _newparent.into(), &newname)
        {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, _size: u32, reply: ReplyXattr) {
        let name = name.to_string_lossy();
        match self.inner.getxattr(ino.into(), &name) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        match self.inner.listxattr(ino.into()) {
            Ok(names) => {
                let mut flat = Vec::new();
                for n in &names {
                    flat.extend_from_slice(n);
                    flat.push(0);
                }
                if size == 0 {
                    reply.size(flat.len() as u32);
                } else if (size as usize) < flat.len() {
                    reply.error(Errno::ERANGE);
                } else {
                    reply.data(&flat);
                }
            }
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        match self.inner.statfs(_ino.into()) {
            Ok(v) => reply.statfs(
                v.total_blocks,
                v.free_blocks,
                v.avail_blocks,
                v.total_inodes,
                v.free_inodes,
                v.block_size,
                v.max_name_len,
                0,
            ),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn unlink(&self, _req: &Request, _parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy();
        match self.inner.unlink(_parent.into(), &name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn forget(&self, _req: &Request, _ino: INodeNo, _nlookup: u64) {
        self.inner.forget(_ino.into(), _nlookup);
    }
}
