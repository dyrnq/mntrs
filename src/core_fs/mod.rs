//! Platform-independent filesystem trait and core types.
//!
//! This module defines the abstraction layer that both fuser (Linux/macOS)
//! and winfsp (Windows) adapters implement.

use std::time::SystemTime;

/// Platform-independent file type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoreFileType {
    Directory,
    RegularFile,
    Symlink,
    NamedPipe,
    CharDevice,
    BlockDevice,
    Socket,
}

/// Platform-independent file attributes returned by lookup/getattr.
#[derive(Clone, Copy, Debug)]
pub struct CoreFileAttr {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub crtime: SystemTime,
    pub kind: CoreFileType,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
    pub flags: u32,
}

/// A directory entry (for readdir).
#[derive(Clone, Debug)]
pub struct CoreDirEntry {
    pub ino: u64,
    pub kind: CoreFileType,
    pub name: String,
}

/// Volume statistics (for statfs).
#[derive(Clone, Debug)]
pub struct CoreVolumeStat {
    pub total_blocks: u64,
    pub free_blocks: u64,
    pub avail_blocks: u64,
    pub total_inodes: u64,
    pub free_inodes: u64,
    pub block_size: u32,
    pub max_name_len: u32,
}

/// The platform-independent filesystem trait.
///
/// All methods return `std::io::Result` with `io::ErrorKind::PermissionDenied`,
/// `io::ErrorKind::NotFound`, `io::ErrorKind::AlreadyExists`,
/// `io::ErrorKind::InvalidInput`, etc. mapping to the appropriate OS error.
///
/// Platform adapters (fuser, winfsp) implement the conversion to their
/// respective error/reply types.
#[allow(clippy::too_many_arguments)]
pub trait CoreFilesystem: Send + Sync {
    /// Initialize the filesystem (called once at mount time).
    fn init(&self) -> std::io::Result<()>;

    /// Look up a directory entry by name and return its inode + attributes.
    fn lookup(&self, parent: u64, name: &str) -> std::io::Result<CoreFileAttr>;

    /// Batch lookup multiple entries in a parent directory.
    ///
    /// Issue #29: readdirplus issues one lookup per
    /// entry, each of which is a remote RTT in the
    /// worst case. Implementations that already have
    /// the attrs in memory (e.g. `MntrsFs` after a
    /// recent `list_op`) can serve the whole batch
    /// from the existing snapshot, turning N RTTs
    /// into 0.
    ///
    /// The default implementation falls back to N
    /// individual `lookup` calls — preserves the
    /// pre-fix behaviour for any external test
    /// fakes.
    fn lookup_many(
        &self,
        parent: u64,
        names: &[&str],
    ) -> std::io::Result<Vec<std::io::Result<CoreFileAttr>>> {
        let mut out = Vec::with_capacity(names.len());
        for n in names {
            out.push(self.lookup(parent, n));
        }
        Ok(out)
    }

    /// Forget about an inode (decrement reference count).
    fn forget(&self, _ino: u64, _nlookup: u64) {}

    /// Get file attributes.
    fn getattr(&self, ino: u64) -> std::io::Result<CoreFileAttr>;

    /// Set file attributes.
    ///
    /// `fh` is the open file handle when the kernel has one
    /// (e.g. FUSE `setattr` was issued through an open fd;
    /// `truncate(2)` on an open fd goes through this path).
    /// Adapters that don't carry a per-fh context can pass
    /// `None`, in which case the implementation falls back
    /// to a path-based attribute update.
    ///
    /// Issue #42: when `fh.is_some()` and `size.is_some()`,
    /// the implementation should call `ftruncate(fh, size)`
    /// against the open cache fd rather than re-opening
    /// the cache file by path. The fd path avoids a path
    /// → fd lookup, is more correct on a writer that's
    /// currently mutating the file (no race with the
    /// writer's open file description), and matches
    /// libfuse passthrough_hp.
    fn setattr(
        &self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<SystemTime>,
        _mtime: Option<SystemTime>,
        fh: Option<u64>,
    ) -> std::io::Result<CoreFileAttr>;

    /// Read directory entries.
    ///
    /// Issue #23 / DESIGN_READDIR_STREAMING: the FUSE
    /// protocol paginates readdir by cookie. The
    /// pre-fix `readdir(ino) -> Vec<CoreDirEntry>` API
    /// re-materialized the list on every page (via
    /// `dir_cache` + `list_op`). If a concurrent mutation
    /// (create/unlink) invalidated the dir cache between
    /// the first and second page, the second `readdir`
    /// could produce a different list at the same
    /// `start` offset — leading to skipped or duplicate
    /// entries delivered to user-space.
    ///
    /// The fix is a 3-call API:
    ///   * `opendir(ino)` materializes the list once
    ///     and returns a per-fh handle. The default
    ///     returns a sentinel fh of 0 (no per-fh state,
    ///     falls back to the pre-#23 re-materialize path).
    ///   * `readdir(ino, fh, offset)` slices into the
    ///     per-fh state for non-zero fh, or re-materialises
    ///     on every call for fh=0 (the pre-#23 fallback).
    ///   * `releasedir(ino, fh)` drops the per-fh state.
    fn opendir(&self, ino: u64) -> std::io::Result<u64> {
        let _ = ino;
        Ok(0)
    }

    /// Read the next page of directory entries.
    /// `offset` is the FUSE cookie (= 1 + index of the
    /// last entry the kernel consumed). `max` is a hint;
    /// the implementation may return fewer (or up to all
    /// remaining) entries.
    ///
    /// Required method: implementations that use
    /// per-fh state (issue #23) implement this directly;
    /// test fakes can fall back to the pre-#23 behaviour
    /// by re-materialising on every call. There is no
    /// default body because the only public impl
    /// (`MntrsFs`) always has per-fh state available
    /// and slicing is the right primitive.
    fn readdir(
        &self,
        ino: u64,
        fh: u64,
        offset: u64,
        _max: usize,
    ) -> std::io::Result<Vec<CoreDirEntry>>;

    /// Release the per-fh readdir state. The default is
    /// a no-op (no per-fh state to release under the
    /// re-materialize path).
    fn releasedir(&self, _ino: u64, _fh: u64) -> std::io::Result<()> {
        Ok(())
    }

    /// Open a file (return a handle id).
    fn open(&self, ino: u64, _flags: u32) -> std::io::Result<u64>;

    /// Read data from an open file handle.
    fn read(&self, ino: u64, fh: u64, offset: u64, size: u32) -> std::io::Result<Vec<u8>>;

    /// Write data to an open file handle.
    fn write(&self, ino: u64, fh: u64, offset: u64, data: &[u8]) -> std::io::Result<u32>;

    /// Flush buffered data for an open handle.
    fn flush(&self, ino: u64, fh: u64) -> std::io::Result<()>;

    /// Sync file contents to stable storage.
    ///
    /// Issue #35: SQLite / etcd / RocksDB / LMDB call
    /// `fsync(2)` on every transaction commit to guarantee
    /// journal durability. The fuser default for this
    /// callback is `ENOSYS`; databases on a FUSE mount
    /// built on the default adapter silently lose commit
    /// guarantees. The winfsp default also returns an
    /// error.
    ///
    /// `datasync` mirrors the FUSE flag: when true, only
    /// user data needs to be flushed (mtime / ctime can
    /// stay in the page cache); when false, the
    /// implementation must also persist metadata.
    ///
    /// Default returns `Unsupported` (mapped to `ENOSYS`
    /// by the fuser adapter) so external test fakes
    /// continue to compile when the trait gains this
    /// method.
    fn fsync(&self, ino: u64, fh: u64, datasync: bool) -> std::io::Result<()> {
        let _ = (ino, fh, datasync);
        Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
    }

    /// Sync directory contents to stable storage.
    ///
    /// Same rationale as `fsync` (issue #35): databases
    /// that `opendir` + `fsyncdir` after a metadata update
    /// get ENOSYS on the default adapter. Mirrors
    /// libfuse passthrough_hp's `sfs_fsyncdir`.
    ///
    /// `datasync` mirrors the FUSE flag. For most
    /// backends, fsyncdir on a directory is a no-op (the
    /// directory's own data blocks are tiny and the
    /// backend directory listing is usually served from
    /// a separate metadata service). The default
    /// implementation returns Ok(()) to preserve the
    /// pre-existing semantics for backends where
    /// dir-fsync is meaningless.
    fn fsyncdir(&self, ino: u64, fh: u64, datasync: bool) -> std::io::Result<()> {
        let _ = (ino, fh, datasync);
        Ok(())
    }

    /// Release (close) an open file handle.
    fn release(&self, ino: u64, fh: u64) -> std::io::Result<()>;

    /// Create a file in a directory.
    ///
    /// Returns `(attr, fh)` — `attr` carries the new
    /// inode's metadata, `fh` is a fresh open file
    /// handle minted by the implementation (typically
    /// via `NEXT_HANDLE.fetch_add(1)`).
    ///
    /// Issue #51: pre-fix the fuser adapter used
    /// `attr.ino` as the `FileHandle` returned to the
    /// kernel. The `handles` DashMap is shared between
    /// `create()` and `open()`, but `open()` uses
    /// `NEXT_HANDLE` (a separate counter) for its key.
    /// When `attr.ino` collided with an `open()`'s
    /// `NEXT_HANDLE` value, the second `open()`
    /// silently overwrote the first `create()`'s
    /// Write state — a deterministic data-corruption
    /// bug ("create a.txt, open b.txt, open c.txt;
    /// read(b.txt) returns c.txt's data").
    ///
    /// The trait now exposes a separate `fh` so the
    /// adapter can return a non-colliding handle.
    fn create(&self, parent: u64, name: &str, mode: u32) -> std::io::Result<(CoreFileAttr, u64)>;

    /// Create a directory.
    fn mkdir(&self, parent: u64, name: &str) -> std::io::Result<CoreFileAttr>;

    /// Remove a file.
    fn unlink(&self, parent: u64, name: &str) -> std::io::Result<()>;

    /// Remove a directory.
    fn rmdir(&self, parent: u64, name: &str) -> std::io::Result<()>;

    /// Rename a file or directory.
    fn rename(&self, parent: u64, name: &str, newparent: u64, newname: &str)
    -> std::io::Result<()>;

    /// Read the target of a symbolic link.
    ///
    /// Bug 17: pre-fix this method did not exist on the trait,
    /// even though `CoreFileType::Symlink` was already in the
    /// enum and the fuser adapter mapped it through. The kernel
    /// would call FUSE `readlink(ino)` on any entry exposed as
    /// `S_IFLNK`, and without a trait method to forward to, the
    /// adapter's default behaviour (ENOSYS) propagated to user
    /// space — `ls -la` showed the link with `??????????` perms
    /// and `readlink` returned `Function not implemented`.
    ///
    /// Default implementation returns
    /// `io::ErrorKind::Unsupported` (mapped to ENOSYS by the
    /// fuser adapter). The current `MntrsFs` impl uses the
    /// default because opendal 0.57's `EntryMode` doesn't
    /// distinguish symlinks from regular files (the `fs` backend
    /// follows links transparently), so we never produce a
    /// `Symlink` entry in the first place. A future fs-backend
    /// special case can override this with `std::fs::read_link`
    /// against the local mount root.
    fn readlink(&self, ino: u64) -> std::io::Result<Vec<u8>> {
        let _ = ino;
        Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
    }

    /// Create a symbolic link `name` under `parent` that points
    /// at `target`. `target` is the literal link contents (may
    /// be relative or absolute); it is NOT resolved here.
    ///
    /// Same Bug 17 rationale as `readlink`: the trait method
    /// didn't exist, so creating a symlink on any FUSE mount
    /// (regardless of backend capability) returned ENOSYS.
    /// Default returns Unsupported; an fs-backend impl can
    /// forward to `std::os::unix::fs::symlink`.
    fn symlink(
        &self,
        parent: u64,
        name: &str,
        target: &std::path::Path,
    ) -> std::io::Result<CoreFileAttr> {
        let _ = (parent, name, target);
        Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
    }

    /// Get volume statistics.
    fn statfs(&self, ino: u64) -> std::io::Result<CoreVolumeStat>;

    /// Get extended attribute value.
    fn getxattr(&self, ino: u64, name: &str) -> std::io::Result<Vec<u8>>;

    /// List extended attribute names.
    fn listxattr(&self, ino: u64) -> std::io::Result<Vec<Vec<u8>>>;

    /// Check access permissions.
    fn access(&self, ino: u64, mask: u32) -> std::io::Result<()>;

    /// Create a hard link.
    ///
    /// Issue #25: pre-fix the fuser default returned
    /// `EPERM` on every link, breaking POSIX apps that
    /// rely on hard links for atomic file replacement
    /// (e.g. `mv`, package managers' `rename(2)`-via-
    /// -link fallbacks). Object stores don't have a
    /// native hard link primitive, so the default
    /// returns `Unsupported` (mapped to ENOSYS); an
    /// fs-backend impl can override with a real
    /// `std::fs::hard_link`.
    fn link(&self, ino: u64, newparent: u64, newname: &str) -> std::io::Result<CoreFileAttr> {
        let _ = (ino, newparent, newname);
        Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
    }

    /// Allocate / deallocate space in a file.
    ///
    /// Issue #25: pre-fix returned ENOSYS. Databases
    /// (SQLite, etc.) and some apps use `fallocate` to
    /// pre-extend files, which avoids the
    /// set_len-then-write pattern that would
    /// otherwise create a sparse hole. The default
    /// here is a `set_len` to `offset + length` —
    /// matches the typical use case and works for
    /// both object stores (via opendal's eventual
    /// `set_len` on the cache file) and local fs.
    fn fallocate(
        &self,
        ino: u64,
        _fh: u64,
        offset: u64,
        length: u64,
        mode: i32,
    ) -> std::io::Result<()> {
        // Mode bits (from fcntl.h):
        //   0x00 = default (allocate)
        //   0x01 = KEEP_SIZE (don't grow file size)
        //   0x02 = PUNCH_HOLE (deallocate)
        // We handle allocate + KEEP_SIZE; PUNCH_HOLE
        // is a no-op for now (object stores don't have
        // a hole primitive). The default
        // setattr(ino, size) at offset+length grows
        // the cache file to cover the requested range.
        let _ = mode;
        self.setattr(
            ino,
            None,
            None,
            None,
            Some(offset + length),
            None,
            None,
            None,
        )
        .map(|_| ())
    }

    /// Copy `len` bytes from one file to another
    /// without going through user-space (the kernel
    /// splice optimization).
    ///
    /// Issue #25 / #46: pre-fix returned ENOSYS.
    /// The trait's default is a read + write
    /// passthrough — sub-optimal for object stores
    /// (extra GET + PUT) but correct. Backends with
    /// a native server-side copy (S3 CopyObject, HDFS
    /// `concat`/`rename`, etc.) can override for
    /// a single-RTT optimization.
    fn copy_file_range(
        &self,
        ino_in: u64,
        fh_in: u64,
        offset_in: u64,
        ino_out: u64,
        fh_out: u64,
        offset_out: u64,
        len: u64,
    ) -> std::io::Result<u32> {
        // Default: read the source chunk via the
        // existing read path, write via the existing
        // write path. The reads hit mem_cache; the
        // writes go through the writeback pool. The
        // backends with a native copy primitive
        // (S3 CopyObject, etc.) should override this.
        let data = self.read(ino_in, fh_in, offset_in, (len.min(u32::MAX as u64)) as u32)?;
        let written = self.write(ino_out, fh_out, offset_out, &data)?;
        Ok(written)
    }
}

#[cfg(unix)]
pub mod fuser;

#[cfg(windows)]
pub mod winfsp;

/// Helper to expose MntrsFs (or any CoreFilesystem impl) for integration testing.
/// On Windows, mounts via WinFSP; on Linux this is a no-op.
#[cfg(windows)]
pub mod test_helpers {
    use crate::core_fs::CoreFilesystem;
    use crate::core_fs::winfsp::WinFspAdapter;
    use std::sync::Arc;
    use winfsp::host::{FileSystemHost, MountPoint};

    /// Mount a CoreFilesystem on a Windows drive letter (auto-assigned).
    /// Returns the mount handle. Dropping it unmounts.
    pub fn mount_winfsp<F: CoreFilesystem + 'static>(fs: Arc<F>) -> std::io::Result<MountGuard<F>> {
        let adapter = WinFspAdapter::new(fs);
        let mut host = FileSystemHost::new(winfsp::host::VolumeParams::default(), adapter)
            .map_err(|e| std::io::Error::other(format!("FileSystemHost::new: {e}")))?;
        host.mount(MountPoint::NextFreeDrive)
            .map_err(|e| std::io::Error::other(format!("host.mount: {e}")))?;
        Ok(MountGuard::<F> { host: Some(host) })
    }

    /// RAII guard that unmounts on drop.
    pub struct MountGuard<F: CoreFilesystem + 'static> {
        host: Option<FileSystemHost<WinFspAdapter<F>>>,
    }

    impl<F: CoreFilesystem + 'static> Drop for MountGuard<F> {
        fn drop(&mut self) {
            if let Some(mut host) = self.host.take() {
                host.stop();
            }
        }
    }
}
