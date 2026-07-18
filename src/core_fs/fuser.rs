//! fuser adapter — bridges `CoreFilesystem` to `fuser::Filesystem`.

use std::ffi::OsStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    AccessFlags, Errno, FileAttr, FileHandle, FileType, FopenFlags, Generation, INodeNo, InitFlags,
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

/// Convert a `std::io::Error` into the closest `fuser::Errno`. Public to
/// `crate` so the legacy `impl Filesystem for MntrsFs` in `lib.rs` can
/// reuse the same mapping (it predates the `CoreFilesystem` adapter but
/// shares dispatch through `reply.error(...)`).
pub(crate) fn io_err_to_fuse_errno(e: std::io::Error) -> Errno {
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
///
/// `write_back_cache` gates `InitFlags::FUSE_WRITEBACK_CACHE` in `init()`. Off
/// by default (matches the `--write-back-cache` CLI flag's `bool` default
/// in `src/main.rs`). When off, the kernel does NOT buffer writes; the
/// daemon's `write()` handler is called per writeback segment as before
/// (#79/#80/#81). This restores daemon-side write observability and avoids
/// 3 known cache-poisoning bugs (#331 read, #334 prefetch, #337 remote-fetch)
/// plus the stress-suite failures under WRITEBACK_CACHE.
pub struct FuserAdapter<F: CoreFilesystem + 'static> {
    pub inner: F,
    pub dir_cache_ttl: Duration,
    pub attr_ttl: Duration,
    pub direct_io: bool,
    pub write_back_cache: bool,
}

impl<F: CoreFilesystem + 'static> FuserAdapter<F> {
    pub fn new(
        inner: F,
        dir_cache_ttl: Duration,
        attr_ttl: Duration,
        direct_io: bool,
        write_back_cache: bool,
    ) -> Self {
        Self {
            inner,
            dir_cache_ttl,
            attr_ttl,
            direct_io,
            write_back_cache,
        }
    }
}

impl<F: CoreFilesystem + 'static> fuser::Filesystem for FuserAdapter<F> {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        // #79: tune KernelConfig for remote backend latency.
        // Defaults are libfuse `max_background=12`, `congestion_threshold=8`,
        // which caps in-flight FUSE requests at 12 — bottleneck for S3/HDFS
        // where each request has 10-100ms latency. Bump to 64/48 to let
        // more requests pipeline before kernel throttling.
        let _ = config.set_max_write(128 * 1024);
        let _ = config.set_max_readahead(1024 * 1024);
        let _ = config.set_max_background(64);
        let _ = config.set_congestion_threshold(48);

        // #80: PARALLEL_DIROPS — kernel allows lookup() and readdir() to
        // run concurrently for the same directory. Big win for `ls -la`,
        // `find`, `tree` on large dirs.
        let _ = config.add_capabilities(InitFlags::FUSE_PARALLEL_DIROPS);

        // #81: WRITEBACK_CACHE — kernel buffers small writes and merges
        // them before sending to the filesystem. Cuts FUSE write requests
        // for small-write workloads (`dd bs=4k` etc).
        //
        // **Opt-in** since 2026-06-30: gated on `self.write_back_cache`
        // (CLI: `--write-back-cache`). Default is OFF because under
        // WRITEBACK_CACHE:
        //   - daemon's `write()` is never called for multi-page files
        //     (kernel page cache holds the data)
        //   - 3 cache-poisoning bugs (#331/#334/#337) shipped post-merge
        //   - stress 01/05 architecturally fail (no cache files for
        //     multi-page bodies; daemon drain never settles)
        // Users who want the small-write optimization can opt in with
        // `--write-back-cache`. The kernel-side mount option
        // `writeback_cache` at cmd/mount.rs is gated on the same flag.
        if self.write_back_cache {
            // macFUSE has its own kernel-managed write buffering
            // outside the FUSE writeback capability — the
            // InitFlags::FUSE_WRITEBACK_CACHE bit is silently
            // dropped, so requesting it produces no warning and
            // no behavior. Warn at the actual capability-declaration
            // site (not the CLI mount wrapper) so library users /
            // CSI drivers get the same diagnostic.
            #[cfg(target_os = "macos")]
            tracing::warn!(
                "--write-back-cache is ignored on macOS: macFUSE manages its own \
                 write buffering; the FUSE writeback capability is not exposed \
                 through macFUSE. Drop the flag on macOS hosts."
            );
            let _ = config.add_capabilities(InitFlags::FUSE_WRITEBACK_CACHE);
        }

        // #optim: FUSE_HAS_EXPIRE_ONLY (protocol 7.38+, fuser 0.17
        // declares 7.40). Instead of invalidating a cached kernel entry
        // on a cache miss, the kernel can just mark it as expired
        // (TTL-gated). mntrs already uses attr_ttl / dir_cache_ttl —
        // this tells the kernel to use the same TTL model, avoiding
        // unnecessary full invalidation on stat_op misses over
        // high-latency backends (S3 5-15ms HEAD).
        let _ = config.add_capabilities(InitFlags::FUSE_HAS_EXPIRE_ONLY);

        // #88: FUSE_CAP_ASYNC_DIO — when --direct-io is set, request
        // separate execution contexts for opened files vs the FS operations
        // on them. Per libfuse include/fuse_common.h:328, this gives
        // better responsiveness under direct-io because writers don't
        // block metadata operations on the same fd.
        if self.direct_io {
            let _ = config.add_capabilities(InitFlags::FUSE_ASYNC_DIO);
        }

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
        // Issue #307: NFC-normalize the kernel-supplied name so
        // a file uploaded via macOS HFS+/APFS (NFD) and read
        // here via Linux VFS (NFC) hits the same backend key.
        let name = crate::util::nfc(&name.to_string_lossy());
        // Issue #47: metrics (lookup)
        let metrics = crate::metrics::global();
        let start = std::time::Instant::now();
        let result = self.inner.lookup(parent.into(), &name);
        let elapsed = start.elapsed();
        match &result {
            Ok(_) => metrics.lookup.record_ok(),
            Err(_) => metrics.lookup.record_err(),
        }
        metrics.lookup_h.observe(elapsed);
        match result {
            Ok(attr) => {
                let fattr = from_core_attr(&attr);
                // Bug 24: TTL = Duration::ZERO is intentional and
                // asymmetric with the create-style replies below
                // (mkdir / create / symlink — all `self.attr_ttl`).
                //
                // Rationale: lookup resolves an EXISTING entry,
                // whose attrs (especially size) can change behind
                // our back via:
                //   * a concurrent write through another open
                //     handle in this mount, or
                //   * an out-of-band backend update (S3 PUT to
                //     the same key from another client).
                // If we returned `self.attr_ttl` here, the kernel
                // would cache the lookup-time attrs and serve
                // stale size to userspace for the TTL window —
                // defeating the same protection that `getattr`
                // provides (see getattr's long comment below for
                // the truncated-read failure mode this prevents).
                //
                // The mkdir/create/symlink sites get to use
                // `self.attr_ttl` because the kernel JUST issued
                // the creation; the attrs we reply are
                // authoritative for the immediate future (size=0
                // for fresh create, size=4096 for fresh dir,
                // immutable link target for symlink). The kernel
                // can safely cache them until the TTL expires.
                reply.entry(&Duration::ZERO, &fattr, Generation(0));
            }
            Err(e) => {
                reply.error(io_err_to_fuse_errno(e));
            }
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        // Issue #47: metrics (getattr)
        let metrics = crate::metrics::global();
        let start = std::time::Instant::now();
        let result = self.inner.getattr(ino.into());
        let elapsed = start.elapsed();
        match &result {
            Ok(_) => metrics.getattr.record_ok(),
            Err(_) => metrics.getattr.record_err(),
        }
        metrics.getattr_h.observe(elapsed);
        match result {
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
        fh: Option<FileHandle>,
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
        // Issue #42: forward the kernel's optional open fh
        // so the trait impl can call ftruncate(fh, size)
        // on the cache fd rather than re-opening the file
        // by path. The fuser API exposes the fh as
        // `Option<FileHandle>` (None when setattr came
        // from a path-based syscall like `truncate(path)`).
        let fh_u64 = fh.map(|h| h.0);
        match self
            .inner
            .setattr(ino.into(), mode, uid, gid, size, atime, mtime, fh_u64)
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
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        // Issue #23: pull a single page from the per-fh
        // dir-lister snapshot. The materialisation
        // happened once in opendir (called the first
        // time the kernel opened this directory); the
        // fh we receive here is the same fh opendir
        // returned. The cookie is the kernel's
        // "1 + index of last entry delivered".
        //
        // Phase 2 (perf-readdir-zero-copy): `inner.readdir`
        // returns the full materialised Vec wrapped in an
        // `Arc`. We slice via `&arc[start..]` (a borrow of
        // the same backing memory, no copy) — pre-fix this
        // path did `entries[start..].to_vec()` on every page,
        // which is `O(page_size)` heap allocation. The Arc
        // is shared across all pagination calls for the same
        // fh, so `Arc::ptr_eq` holds (verified by regression
        // test `readdir_zero_copy_same_arc_across_pages`).
        //
        // #436: pre-Phase-2 the inner impl pre-sliced and
        // returned `Vec`, then we iterated entries directly
        // (after the double-slice bug fix). Phase 2 pushes
        // the offset arithmetic back into the adapter where
        // the rclone mount2 `dirStream` pattern lives, so
        // the slicing cost is `O(1)` instead of `O(N)`
        // allocations per page.
        //
        // For the pre-#23 fallback (fh=0, the trait
        // default), the implementation re-materialises
        // on every call, so pagination is still correct
        // — just slower and subject to the same
        // "list changed between pages" risk as before.
        match self.inner.readdir(ino.into(), fh.into(), offset, 0) {
            Ok(entries_arc) => {
                let start = offset as usize;
                let page: &[super::CoreDirEntry] = if start >= entries_arc.len() {
                    &[]
                } else {
                    &entries_arc[start..]
                };
                for (offset_i, entry) in page.iter().enumerate() {
                    let i = offset as usize + offset_i;
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
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectoryPlus,
    ) {
        // Issue #23: same per-fh slice path as readdir
        // above. The per-fh snapshot also pins inode
        // allocation (alloc_ino) for each entry, so the
        // lookups below resolve to the same ino the
        // entry was emitted with — vs the pre-#23
        // path which could allocate a different ino
        // for the same name across pages.
        //
        // Phase 2 (perf-readdir-zero-copy): same Arc
        // slice as readdir — see the readdir handler
        // comment for the rclone mount2 dirStream
        // rationale. The `lookup_many` batch below
        // receives `&[&str]` derived from the slice,
        // not from a cloned Vec.
        match self.inner.readdir(ino.into(), fh.into(), offset, 0) {
            Ok(entries_arc) => {
                let start = offset as usize;
                let page: &[super::CoreDirEntry] = if start >= entries_arc.len() {
                    &[]
                } else {
                    &entries_arc[start..]
                };
                // Issue #29: batch the per-entry lookups
                // so the implementation can serve the
                // whole page from its dir_cache
                // snapshot in one call (0 RTTs on a
                // warm cache) instead of N individual
                // trait lookups (N RTTs in the worst
                // case). For `ls -la` on a 500-file
                // directory this drops 500 stat RTTs
                // to 0; for `find maxdepth1` it
                // eliminates the per-entry stat
                // completely.
                let names: Vec<&str> = page.iter().map(|e| e.name.as_str()).collect();
                let attr_results =
                    self.inner
                        .lookup_many(ino.into(), &names)
                        .unwrap_or_else(|_| {
                            names
                                .iter()
                                .map(|_| Err(std::io::ErrorKind::Other.into()))
                                .collect()
                        });
                for (offset_i, (entry, attr_res)) in
                    page.iter().zip(attr_results.iter()).enumerate()
                {
                    let i = offset as usize + offset_i;
                    // For each directory entry, do a lookup to get full attr
                    let attr = attr_res.as_ref().ok();
                    let fattr = attr.map(from_core_attr).unwrap_or_else(|| {
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
        // Issue #23: forward to the trait's opendir
        // which materialises the dir entries and returns
        // a per-fh handle. Pre-fix this used the trait
        // default that returns Ok(0) — a sentinel that
        // skips the per-fh state and falls back to the
        // re-materialise-on-every-page path.
        match self.inner.opendir(ino.into()) {
            Ok(fh) => reply.opened(FileHandle(fh), FopenFlags::empty()),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn releasedir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        // Issue #23: drop the per-fh snapshot. Idempotent
        // — see MntrsFs::releasedir.
        match self.inner.releasedir(ino.into(), fh.into()) {
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
        flags: i32,
        reply: ReplyCreate,
    ) {
        // Issue #307: NFC-normalize (see lookup for rationale).
        let name = crate::util::nfc(&name.to_string_lossy());
        // Issue #160: thread O_EXCL through to the
        // implementation. libc::O_EXCL is 0o200 on Linux/macOS
        // and 0x40000000 on Windows — we use the bitmask
        // directly so this compiles cross-platform without a
        // `libc` dep. When the kernel passes O_EXCL, the
        // create MUST fail with EEXIST if the target exists;
        // when it does not, the create can overwrite (POSIX
        // O_CREAT-without-O_EXCL semantics). On backends that
        // support `if_not_exists` (S3, GCS, azblob, etc.) the
        // implementation maps this to one atomic write. On
        // backends that don't (memory, HDFS), the
        // implementation falls back to `create()` so this
        // matches pre-#160 overwrite behavior — no regression.
        let excl = (flags & 0o200) != 0 || (flags & 0x40000000) != 0;
        let result = if excl {
            self.inner.create_excl(parent.into(), &name, mode)
        } else {
            self.inner.create(parent.into(), &name, mode)
        };
        match result {
            Ok((attr, fh)) => {
                // Issue #51: forward the implementation-
                // minted `fh` (from NEXT_HANDLE) instead
                // of using `attr.ino`. Pre-fix the
                // adapter used `attr.ino` as the
                // FileHandle returned to the kernel,
                // which collided with `open()`'s
                // NEXT_HANDLE counter and silently
                // overwrote the create's Write state on
                // the second open() (deterministic data
                // corruption — see issue text for the
                // 3-step repro).
                let fattr = from_core_attr(&attr);
                let fopen_flags = if self.direct_io {
                    FopenFlags::FOPEN_DIRECT_IO
                } else {
                    FopenFlags::empty()
                };
                reply.created(
                    &self.attr_ttl,
                    &fattr,
                    Generation(0),
                    FileHandle(fh),
                    fopen_flags,
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
        // Issue #47: record the read op in the
        // Prometheus metrics. Wrapped in a
        // short-lived Arc clone — metrics::global()
        // is a LazyLock<Arc<...>> so the clone is
        // a single refcount bump. The start/stop
        // pair captures the full op duration
        // including the trait dispatch and the
        // reply data copy.
        let metrics = crate::metrics::global();
        let start = std::time::Instant::now();
        let result = self.inner.read(ino.into(), _fh.into(), offset, size);
        let elapsed = start.elapsed();
        match &result {
            Ok(_) => metrics.read.record_ok(),
            Err(_) => metrics.read.record_err(),
        }
        metrics.read_h.observe(elapsed);
        match result {
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
        // Issue #47: metrics. Same pattern as read.
        let metrics = crate::metrics::global();
        let start = std::time::Instant::now();
        let result = self.inner.write(ino.into(), fh.into(), offset, data);
        let elapsed = start.elapsed();
        match &result {
            Ok(_) => metrics.write.record_ok(),
            Err(_) => metrics.write.record_err(),
        }
        metrics.write_h.observe(elapsed);
        match result {
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

    /// Issue #35: forward fsync to the trait. Pre-fix this
    /// handler didn't exist on the FuserAdapter, so every
    /// `fsync(2)` from user-space (e.g. SQLite's
    /// `xSync` callback after every transaction commit)
    /// returned `ENOSYS`. The fuser 0.17 default body
    /// matches that behaviour, but the in-memory
    /// `register_dirty_cache_path` fsync thread + the
    /// `cache_fd::sync_all` on close path are not
    /// triggered by FUSE's fsync — they only fire from
    /// the kernel's own dirty-page writeback. A database
    /// workload depends on this hook to know "the bytes
    /// you wrote are now durable on the local cache
    /// disk" before it acks the commit. With this in
    /// place, the kernel sees `Ok` and the database
    /// proceeds; the cache file is `fdatasync`'d by
    /// `MntrsFs::fsync` against the open cache fd.
    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.inner.fsync(ino.into(), fh.into(), datasync) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    /// Issue #35 (mirror of fsync): forward fsyncdir to
    /// the trait. The fuser default returns `ENOSYS`;
    /// pre-fix there was no override, so a user-space
    /// `fsyncdir(2)` (rare, but used by some database
    /// fsync paths on metadata updates) hit the default
    /// behaviour. The trait default is `Ok(())` because
    /// most backends have no directory-data to sync;
    /// `MntrsFs` keeps the default unless a future
    /// backend needs explicit dir-fsync.
    fn fsyncdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.inner.fsyncdir(ino.into(), fh.into(), datasync) {
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
        // Issue #307: NFC-normalize (see lookup for rationale).
        let name = crate::util::nfc(&name.to_string_lossy());
        match self.inner.mkdir(parent.into(), &name) {
            Ok(attr) => {
                let fattr = from_core_attr(&attr);
                reply.entry(&self.attr_ttl, &fattr, Generation(0));
            }
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn rmdir(&self, _req: &Request, _parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        // Issue #307: NFC-normalize (see lookup for rationale).
        let name = crate::util::nfc(&name.to_string_lossy());
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
        // Issue #307: NFC-normalize both names (see lookup).
        let name = crate::util::nfc(&name.to_string_lossy());
        let newname = crate::util::nfc(&newname.to_string_lossy());
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
        // Issue #307: NFC-normalize (see lookup for rationale).
        let name = crate::util::nfc(&name.to_string_lossy());
        match self.inner.unlink(_parent.into(), &name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    /// Bug 17: forward readlink to the CoreFilesystem trait.
    /// Pre-fix this handler didn't exist, so the kernel got
    /// the fuser default which returns ENOSYS on every
    /// readlink — the user-space `readlink` syscall then
    /// produced "Function not implemented" even on backends
    /// that COULD support symlinks. Now the trait default
    /// returns Unsupported (mapped to ENOSYS by
    /// `io_err_to_fuse_errno`), and a future fs-backend
    /// override can return the real link target.
    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        match self.inner.readlink(ino.into()) {
            Ok(target) => reply.data(&target),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    /// Bug 17 counterpart to `readlink`: forward symlink
    /// creation to the trait. Pre-fix this also didn't
    /// exist; FUSE's default symlink handler is ENOSYS
    /// regardless of backend capability. The trait default
    /// preserves that behaviour (Unsupported); an fs-backend
    /// impl can override with `std::os::unix::fs::symlink`.
    fn symlink(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        // Issue #307: NFC-normalize (see lookup for rationale).
        let name = crate::util::nfc(&name.to_string_lossy());
        match self.inner.symlink(parent.into(), &name, target) {
            Ok(attr) => {
                let fattr = from_core_attr(&attr);
                reply.entry(&self.attr_ttl, &fattr, Generation(0));
            }
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    fn forget(&self, _req: &Request, _ino: INodeNo, _nlookup: u64) {
        self.inner.forget(_ino.into(), _nlookup);
    }

    /// Issue #25: forward `link` to the trait. The
    /// trait default returns Unsupported (object
    /// stores have no native hard-link primitive);
    /// an fs-backend override can serve a real
    /// `std::fs::hard_link`. Pre-fix the fuser
    /// default returned EPERM on every link().
    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        // Issue #307: NFC-normalize (see lookup for rationale).
        let name = crate::util::nfc(&newname.to_string_lossy());
        match self.inner.link(ino.into(), newparent.into(), &name) {
            Ok(attr) => {
                let fattr = from_core_attr(&attr);
                reply.entry(&self.attr_ttl, &fattr, Generation(0));
            }
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    /// Issue #25: forward `fallocate` to the trait.
    /// The default impl is `setattr(ino, size =
    /// offset + length)`, which grows the cache
    /// file to cover the requested range. Pre-fix
    /// the fuser default returned ENOSYS.
    fn fallocate(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        match self
            .inner
            .fallocate(ino.into(), fh.into(), offset, length, mode)
        {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }

    /// Issue #25 / #46: forward `copy_file_range` to
    /// the trait. The default impl is read + write
    /// passthrough (correct but extra RTTs on object
    /// stores). Backends with a native server-side
    /// copy primitive (S3 CopyObject, HDFS concat)
    /// can override for a single-RTT optimization.
    fn copy_file_range(
        &self,
        _req: &Request,
        ino_in: INodeNo,
        fh_in: FileHandle,
        offset_in: u64,
        ino_out: INodeNo,
        fh_out: FileHandle,
        offset_out: u64,
        len: u64,
        _flags: fuser::CopyFileRangeFlags,
        reply: ReplyWrite,
    ) {
        match self.inner.copy_file_range(
            ino_in.into(),
            fh_in.into(),
            offset_in,
            ino_out.into(),
            fh_out.into(),
            offset_out,
            len,
        ) {
            Ok(written) => reply.written(written),
            Err(e) => reply.error(io_err_to_fuse_errno(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::io_err_to_fuse_errno;

    /// Extract the inner i32 from a fuser::Errno.
    /// Safe: fuser::Errno is #[repr(transparent)] over i32.
    fn errno_i32(e: fuser::Errno) -> i32 {
        unsafe { std::mem::transmute::<fuser::Errno, i32>(e) }
    }

    // ── explicit ErrorKind → Errno mappings ──────────────────────

    #[test]
    fn not_found_maps_to_enoent() {
        let e = std::io::Error::from(std::io::ErrorKind::NotFound);
        assert_eq!(errno_i32(io_err_to_fuse_errno(e)), libc::ENOENT);
    }

    #[test]
    fn permission_denied_maps_to_eacces() {
        let e = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        assert_eq!(errno_i32(io_err_to_fuse_errno(e)), libc::EACCES);
    }

    #[test]
    fn already_exists_maps_to_eexist() {
        let e = std::io::Error::from(std::io::ErrorKind::AlreadyExists);
        assert_eq!(errno_i32(io_err_to_fuse_errno(e)), libc::EEXIST);
    }

    #[test]
    fn invalid_input_maps_to_einval() {
        let e = std::io::Error::from(std::io::ErrorKind::InvalidInput);
        assert_eq!(errno_i32(io_err_to_fuse_errno(e)), libc::EINVAL);
    }

    #[test]
    fn not_a_directory_maps_to_enotdir() {
        let e = std::io::Error::from(std::io::ErrorKind::NotADirectory);
        assert_eq!(errno_i32(io_err_to_fuse_errno(e)), libc::ENOTDIR);
    }

    #[test]
    fn is_a_directory_maps_to_eisdir() {
        let e = std::io::Error::from(std::io::ErrorKind::IsADirectory);
        assert_eq!(errno_i32(io_err_to_fuse_errno(e)), libc::EISDIR);
    }

    #[test]
    fn out_of_memory_maps_to_enomem() {
        let e = std::io::Error::from(std::io::ErrorKind::OutOfMemory);
        assert_eq!(errno_i32(io_err_to_fuse_errno(e)), libc::ENOMEM);
    }

    #[test]
    fn storage_full_maps_to_enospc() {
        let e = std::io::Error::from(std::io::ErrorKind::StorageFull);
        assert_eq!(errno_i32(io_err_to_fuse_errno(e)), libc::ENOSPC);
    }

    #[test]
    fn timed_out_maps_to_etimedout() {
        let e = std::io::Error::from(std::io::ErrorKind::TimedOut);
        assert_eq!(errno_i32(io_err_to_fuse_errno(e)), libc::ETIMEDOUT);
    }

    #[test]
    fn interrupted_maps_to_eintr() {
        let e = std::io::Error::from(std::io::ErrorKind::Interrupted);
        assert_eq!(errno_i32(io_err_to_fuse_errno(e)), libc::EINTR);
    }

    #[test]
    fn unsupported_maps_to_enosys() {
        let e = std::io::Error::from(std::io::ErrorKind::Unsupported);
        assert_eq!(errno_i32(io_err_to_fuse_errno(e)), libc::ENOSYS);
    }

    // ── fallback: unknown kind → EIO ─────────────────────────────

    #[test]
    fn unknown_kind_falls_back_to_eio() {
        let e = std::io::Error::other("custom");
        assert_eq!(errno_i32(io_err_to_fuse_errno(e)), libc::EIO);
    }

    // ── raw OS error code passthrough ────────────────────────────

    #[test]
    fn raw_os_error_maps_to_corresponding_errno() {
        let e = std::io::Error::from_raw_os_error(9); // EBADF
        assert_eq!(errno_i32(io_err_to_fuse_errno(e)), 9);
    }

    #[test]
    fn raw_os_error_zero_falls_back_to_eio() {
        let e = std::io::Error::from_raw_os_error(0);
        assert_eq!(errno_i32(io_err_to_fuse_errno(e)), libc::EIO);
    }
}
