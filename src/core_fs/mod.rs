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

    /// Read directory entries with their attrs in one call.
    ///
    /// Issue #306: WinFSP's `read_directory` callback wants
    /// `(entry, attr)` pairs so it can populate the DirInfo
    /// with the entry's real size/mtime without a per-entry
    /// follow-up `get_file_info` IRP. FUSE's readdir doesn't
    /// need this — fuser's readdirplus already provides the
    /// same batch via `lookup_many` / `MntrsFs`'s
    /// `batch_lookup_from_dir_cache` overrides. The default
    /// impl here lets any external test fake keep compiling
    /// without overriding.
    ///
    /// `marker` semantics: returns entries with name strictly
    /// greater than marker (matches WinFSP's marker model).
    /// Empty marker = first page.
    ///
    /// Default: falls back to `readdir(ino, fh, 0, 0)` then
    /// `getattr(entry.ino)` per entry sequentially. Slow
    /// (N RTTs in the worst case) but correct. `MntrsFs`
    /// overrides to serve attrs from the `dir_cache`
    /// snapshot, avoiding the RTTs entirely for the common
    /// case.
    fn readdir_with_attrs(
        &self,
        ino: u64,
        fh: u64,
        marker: &str,
    ) -> std::io::Result<Vec<(CoreDirEntry, CoreFileAttr)>> {
        let entries = self.readdir(ino, fh, 0, 0)?;
        let mut out = Vec::with_capacity(entries.len());
        for e in entries {
            if !marker.is_empty() && e.name.as_str() <= marker {
                continue;
            }
            // Skip entries whose attrs disappear between
            // readdir and getattr (race with concurrent
            // unlink). The pre-fix winfsp behavior of
            // emitting a zero-attr entry would be worse:
            // Explorer shows "0 bytes / unknown date"
            // instead of just dropping the row.
            match self.getattr(e.ino) {
                Ok(attr) => out.push((e, attr)),
                Err(_) => continue,
            }
        }
        Ok(out)
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

    /// Create a file atomically — fail with EEXIST if the
    /// target already exists. Used by the fuser adapter
    /// when the kernel passes `O_CREAT|O_EXCL` (issue #160).
    ///
    /// Default implementation: calls `create()` and ignores
    /// the O_EXCL semantics, so backends that don't override
    /// this get the pre-existing "overwrite on O_CREAT" POSIX
    /// behavior. Overrides should map backend "already exists"
    /// to `io::ErrorKind::AlreadyExists` so the fuser adapter
    /// can return EEXIST to user space.
    fn create_excl(
        &self,
        parent: u64,
        name: &str,
        mode: u32,
    ) -> std::io::Result<(CoreFileAttr, u64)> {
        self.create(parent, name, mode)
    }

    /// Create a directory.
    fn mkdir(&self, parent: u64, name: &str) -> std::io::Result<CoreFileAttr>;

    /// Remove a file.
    fn unlink(&self, parent: u64, name: &str) -> std::io::Result<()>;

    /// Remove a directory.
    fn rmdir(&self, parent: u64, name: &str) -> std::io::Result<()>;

    /// Rename a file or directory.
    fn rename(&self, parent: u64, name: &str, newparent: u64, newname: &str)
    -> std::io::Result<()>;

    /// Rename by explicit absolute paths.
    ///
    /// Issue #78: WinFSP's `get_security_by_name` → `rename` flow
    /// can fire on a path whose parent directory has not been
    /// `lookup`'d (no ino in the in-memory cache), so the
    /// `(parent, name, ...)` tuple produced by walking
    /// `inner.lookup` falls back to `(root_ino=1, name)` and
    /// `lib.rs::rename` issues `op.rename("name", "newname")` at
    /// the wrong level — the real src is `/subdir/name`.
    ///
    /// Adapters that have full path info available (WinFSP passes
    /// `\subdir\file` into the `rename` callback) call this
    /// method instead of `rename` so the backend op gets the
    /// correct absolute src and dst. The default implementation
    /// splits each path on the last `/`, resolves the parent via
    /// `lookup` (falling back to `parent=1` like the pre-#78
    /// code), and forwards to `rename(parent, name, newparent,
    /// newname)` — preserves existing behavior for adapters that
    /// don't have full paths (FUSE) and for tests that don't
    /// override.
    fn rename_paths(&self, src_path: &str, dst_path: &str) -> std::io::Result<()> {
        let split_last = |p: &str| -> (u64, String) {
            match p.rsplit_once('/') {
                Some((parent, name)) if !name.is_empty() => {
                    // Walk parent via lookup. Falls back to 1
                    // (root) when any component misses; same
                    // pre-#78 behavior so the default impl is
                    // a no-op upgrade.
                    let trimmed = parent.trim_matches('/');
                    if trimmed.is_empty() {
                        return (1, name.to_string());
                    }
                    let mut cur = 1u64;
                    for c in trimmed.split('/') {
                        if c.is_empty() {
                            continue;
                        }
                        match self.lookup(cur, c) {
                            Ok(a) => cur = a.ino,
                            Err(_) => return (1, name.to_string()),
                        }
                    }
                    (cur, name.to_string())
                }
                _ => (1, p.to_string()),
            }
        };
        let (sp, sn) = split_last(src_path);
        let (dp, dn) = split_last(dst_path);
        self.rename(sp, &sn, dp, &dn)
    }

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

    /// Issue #325: promote an EXISTING ino (already created by
    /// the placeholder `create` callback) to a symlink with the
    /// given target. Differs from `symlink` in that the ino was
    /// allocated by the adapter's `create` — the kernel holds the
    /// handle to this ino, so we must NOT allocate a new one.
    ///
    /// Win32 flow for `New-Item -ItemType SymbolicLink V:\link V:\target`:
    ///   1. `CreateFileW(FILE_OPEN_REPARSE_POINT)` → adapter `create`
    ///      allocates an ino for the placeholder and writes a 0-byte
    ///      file to the backend so the ino has something to point at.
    ///   2. `FSCTL_SET_REPARSE_POINT` → adapter `set_reparse_point`
    ///      decodes the target bytes from the REPARSE_DATA_BUFFER
    ///      and calls THIS method with the placeholder ino.
    ///
    /// Without this method the adapter's `set_reparse_point` would
    /// have to call `symlink(parent, name, target)` — which
    /// allocates a fresh ino — leaving the kernel's handle
    /// pointing at the placeholder ino (kind=RegularFile) while the
    /// `symlinks` map points at a different ino. Subsequent
    /// `getattr`/`get_file_info` on the placeholder ino would then
    /// return RegularFile (no reparse bit, `LinkType=""`, `Target=""`)
    /// instead of the symlink we just registered.
    ///
    /// Default returns Unsupported; only MntrsFs overrides.
    fn attach_symlink_to_ino(
        &self,
        ino: u64,
        target: &std::path::Path,
    ) -> std::io::Result<CoreFileAttr> {
        let _ = (ino, target);
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
    use std::sync::atomic::{AtomicU8, Ordering};
    use winfsp::host::{FileSystemHost, MountPoint};

    /// Per-test-binary counter that allocates a distinct drive letter for
    /// each `mount_winfsp` call. Tests run in parallel by default; sharing
    /// a single drive would race on the WinFSP volume namespace. Counts
    /// down from `Z:` so we never collide with real system drives (A-C are
    /// reserved; D onward is fair game on most systems). The pool is sized
    /// for the 11 mount-touching platform tests + headroom.
    ///
    /// Note: this is per-process. Different test binaries in the same CI
    /// job get their own counter, which is fine — each binary's
    /// `cargo test` invocation is a separate process.
    static NEXT_DRIVE: AtomicU8 = AtomicU8::new(b'Z');

    /// Allocate the next free drive letter from `Z:` downward. Returns
    /// `None` once the pool is exhausted (caller should error).
    fn allocate_drive_letter() -> Option<char> {
        NEXT_DRIVE
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |c| {
                if c < b'P' { None } else { Some(c - 1) }
            })
            .ok()
            .map(|b| b as char)
    }

    /// Mount a CoreFilesystem on a Windows drive letter (auto-assigned
    /// from a per-process pool). Returns a guard whose `mount_path` is
    /// the absolute Win32 path of the volume root (e.g. `E:\\`) — tests
    /// must use this path for I/O, not relative paths, so the operations
    /// hit the WinFSP driver rather than the test runner's CWD.
    ///
    /// Dropping the guard stops the dispatcher and removes the mount.
    pub fn mount_winfsp<F: CoreFilesystem + 'static>(fs: Arc<F>) -> std::io::Result<MountGuard<F>> {
        let drive = allocate_drive_letter().ok_or_else(|| {
            std::io::Error::other("test_helpers::mount_winfsp: drive-letter pool exhausted (Z-P)")
        })?;
        let mountpoint = format!("{}:", drive);
        let adapter = WinFspAdapter::new(fs);
        // Annotate the type as `FileSystemHost<_, FineGuard>` so the
        // `start_with_threads` call below resolves to the FineGuard
        // impl (the CoarseGuard impl has the same signature and is
        // also visible, so unannotated inference fails with E0034).
        // Production mount at src/cmd/mount.rs:1370 makes the same
        // annotation.
        let mut host: winfsp::host::FileSystemHost<_, winfsp::host::FineGuard> =
            FileSystemHost::new(winfsp::host::VolumeParams::default(), adapter)
                .map_err(|e| std::io::Error::other(format!("FileSystemHost::new: {e}")))?;
        // `host.mount` accepts anything `&M: Into<MountPoint>` — the
        // &str conversion is provided by the winfsp crate's blanket
        // `impl<S: AsRef<OsStr>> From<&S> for MountPoint`. Production
        // mount at src/cmd/mount.rs:1377 uses the same shape.
        host.mount(&mountpoint)
            .map_err(|e| std::io::Error::other(format!("host.mount: {e}")))?;
        // Issue #294: spawn the WinFSP user-mode dispatcher threads that
        // service Win32 IRPs by calling back into FileSystemContext
        // methods. Without this, `host.mount()` only registers the
        // volume with the driver (`FspFileSystemSetMountPoint`) but no
        // thread consumes the IRP queue, so any I/O to the mount hangs
        // at the kernel side. Production mount at src/cmd/mount.rs:1395
        // makes the same call.
        host.start_with_threads(0)
            .map_err(|e| std::io::Error::other(format!("host.start: {e}")))?;
        // Path with trailing backslash — convenient for `format!("{mp}foo")`.
        let mount_path = format!("{}\\", mountpoint);
        Ok(MountGuard::<F> {
            host: Some(host),
            mount_path,
        })
    }

    /// RAII guard that unmounts on drop. `mount_path` is the absolute
    /// Win32 path of the volume root (e.g. `E:\\`); tests use it to
    /// build paths that actually go through the WinFSP driver.
    pub struct MountGuard<F: CoreFilesystem + 'static> {
        host: Option<FileSystemHost<WinFspAdapter<F>>>,
        pub mount_path: String,
    }

    impl<F: CoreFilesystem + 'static> Drop for MountGuard<F> {
        fn drop(&mut self) {
            if let Some(mut host) = self.host.take() {
                host.stop();
            }
        }
    }
}
