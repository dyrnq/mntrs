//! winfsp adapter — bridges `CoreFilesystem` to `winfsp::FileSystemContext`.
//!
//! Windows only: requires winfsp 0.13+ and WinFSP 2.1 driver installed.
//!
//! Mapping from CoreFilesystem to WinFSP:
//!   getattr  → get_file_info
//!   lookup   → get_security_by_name + get_dir_info_by_name (WinFSP combines)
//!   readdir  → read_directory
//!   open     → open
//!   release  → close
//!   read     → read
//!   write    → write
//!   create   → create
//!   unlink   → set_delete + cleanup
//!   rename   → rename
//!   setattr  → set_basic_info + set_file_size
//!   statfs   → get_volume_info
//!   flush    → flush
//!   getxattr → get_extended_attributes

#![cfg(windows)]

use std::ffi::c_void;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use winfsp::FspError;

use widestring::U16CStr;
use windows::Win32::Foundation::STATUS_INVALID_DEVICE_REQUEST;
use winfsp::Result;
use winfsp::filesystem::{
    DirInfo, FileInfo, FileSecurity, FileSystemContext, ModificationDescriptor, OpenFileInfo,
    VolumeInfo,
};
use winfsp_sys::FILE_ACCESS_RIGHTS;

// Win32 file attribute constants (same as win32 API)
const FILE_ATTRIBUTE_READONLY: u32 = 0x00000001;
const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x00000010;
const FILE_ATTRIBUTE_ARCHIVE: u32 = 0x00000020;
const FILE_ATTRIBUTE_NORMAL: u32 = 0x00000080;

use super::{CoreFileAttr, CoreFileType, CoreFilesystem, CoreVolumeStat};

/// Win32 file attributes derived from CoreFileType + permissions.
fn core_kind_to_file_attributes(kind: CoreFileType, perm: u16) -> u32 {
    let mut attrs = match kind {
        CoreFileType::Directory => FILE_ATTRIBUTE_DIRECTORY,
        _ => FILE_ATTRIBUTE_NORMAL,
    };
    if perm & 0o200 == 0 {
        attrs |= FILE_ATTRIBUTE_READONLY;
    }
    attrs | FILE_ATTRIBUTE_ARCHIVE
}

/// Convert CoreFileAttr to WinFSP FileInfo (in place).
fn core_attr_to_file_info(attr: &CoreFileAttr, file_info: &mut FileInfo) {
    file_info.file_attributes = core_kind_to_file_attributes(attr.kind, attr.perm);
    file_info.file_size = attr.size;
    file_info.allocation_size = attr.size.next_power_of_two();
    file_info.creation_time = system_time_to_win32(attr.crtime);
    file_info.last_access_time = system_time_to_win32(attr.atime);
    file_info.last_write_time = system_time_to_win32(attr.mtime);
    file_info.change_time = system_time_to_win32(attr.ctime);
    file_info.index_number = attr.ino;
    file_info.hard_links = attr.nlink;
}

fn system_time_to_win32(t: SystemTime) -> u64 {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    // Windows epoch is 1601-01-01, Unix epoch is 1970-01-01: difference = 11644473600 seconds
    (d.as_secs() + 11644473600) * 10_000_000 + d.subsec_nanos() as u64 / 100
}

fn core_volume_to_volume_info(v: &CoreVolumeStat, out: &mut VolumeInfo) {
    out.total_size = v.total_blocks * v.block_size as u64;
    out.free_size = v.free_blocks * v.block_size as u64;
}

/// map std::io::Error to winfsp::Result error type
fn io_err_to_status(e: std::io::Error) -> winfsp::FspError {
    match e.kind() {
        std::io::ErrorKind::NotFound => {
            FspError::NTSTATUS(windows::Win32::Foundation::STATUS_OBJECT_NAME_NOT_FOUND.0)
        }
        std::io::ErrorKind::PermissionDenied => {
            FspError::NTSTATUS(windows::Win32::Foundation::STATUS_ACCESS_DENIED.0)
        }
        std::io::ErrorKind::AlreadyExists => {
            FspError::NTSTATUS(windows::Win32::Foundation::STATUS_OBJECT_NAME_COLLISION.0)
        }
        std::io::ErrorKind::InvalidInput => {
            FspError::NTSTATUS(windows::Win32::Foundation::STATUS_INVALID_PARAMETER.0)
        }
        std::io::ErrorKind::StorageFull => {
            FspError::NTSTATUS(windows::Win32::Foundation::STATUS_DISK_FULL.0)
        }
        _ => FspError::NTSTATUS(windows::Win32::Foundation::STATUS_UNSUCCESSFUL.0),
    }
}

/// A per-handle context for WinFSP.
///
/// Bug 11: pre-fix this only carried `ino` and was
/// used as BOTH ino AND fh in every read/write/flush/
/// close call. That collapsed all concurrent opens of
/// the same file onto one synthetic fh, so each open's
/// state (cache_fd, prefetcher, dirty flags in
/// FileHandleState) clobbered the others'. Adding a
/// distinct `fh` minted by `CoreFilesystem::open`
/// restores per-handle isolation and is what the Linux
/// (fuser) adapter has always done.
#[derive(Clone)]
pub struct WinFspHandle {
    pub ino: u64,
    /// File handle returned by `CoreFilesystem::open`.
    /// Equal to `ino` for directories (we don't call
    /// `open`/`release` on dirs in this adapter — WinFSP
    /// has no opendir/releasedir distinct from open).
    pub fh: u64,
    pub is_dir: bool,
}

/// Translate WinFSP's GRANTED_ACCESS bitmask to the
/// POSIX-style flag word that `CoreFilesystem::open`
/// expects (low 2 bits: 0=O_RDONLY, 1=O_WRONLY, 2=O_RDWR).
///
/// WinFSP grants access bits per the Windows ACL
/// model. Any write-style right (FILE_WRITE_DATA,
/// FILE_APPEND_DATA, GENERIC_WRITE, GENERIC_ALL,
/// MAXIMUM_ALLOWED) maps to O_RDWR rather than
/// O_WRONLY — the cache_fd path opens the local cache
/// file read+write (for prefix-fetch on offset writes),
/// and a write-only POSIX flag would forbid that.
///
/// Read-only access maps to O_RDONLY; the open() handler
/// then takes the FileHandleState::Read branch (no
/// cache_fd) and the read path uses the on-disk block
/// cache + remote fetch.
fn winfsp_access_to_open_flags(granted_access: winfsp_sys::FILE_ACCESS_RIGHTS) -> u32 {
    // Windows access mask constants. Match
    // `windows::Win32::Storage::FileSystem` and
    // winfsp-sys conventions.
    const FILE_WRITE_DATA: u32 = 0x0000_0002;
    const FILE_APPEND_DATA: u32 = 0x0000_0004;
    const GENERIC_WRITE: u32 = 0x4000_0000;
    const GENERIC_ALL: u32 = 0x1000_0000;
    const MAXIMUM_ALLOWED: u32 = 0x0200_0000;
    // `granted_access` is a `type FILE_ACCESS_RIGHTS = u32`
    // alias, so the `as u32` is a no-op cast that
    // `clippy::unnecessary_cast` flags on Windows CI. Linux
    // CI doesn't compile this file, so the lint slipped
    // through there.
    let writes_granted = (granted_access
        & (FILE_WRITE_DATA | FILE_APPEND_DATA | GENERIC_WRITE | GENERIC_ALL | MAXIMUM_ALLOWED))
        != 0;
    if writes_granted { 2 } else { 0 } // O_RDWR : O_RDONLY
}

/// WinFSP adapter that wraps a `CoreFilesystem`.
pub struct WinFspAdapter<F: CoreFilesystem + 'static> {
    pub inner: Arc<F>,
}

impl<F: CoreFilesystem + 'static> WinFspAdapter<F> {
    pub fn new(inner: Arc<F>) -> Self {
        Self { inner }
    }

    /// Issue #56: resolve a full parent path (e.g.
    /// "/subdir") to an inode by walking the
    /// intermediate directories via the trait's
    /// `lookup`. Returns None if any intermediate
    /// lookup fails — the caller falls back to
    /// parent=1 (root), which is the only safe
    /// default when we can't prove the parent.
    fn parent_ino_for(&self, full_path: &str) -> Option<u64> {
        // Strip leading/trailing slashes; split
        // into components.
        let trimmed = full_path.trim_matches('/');
        if trimmed.is_empty() {
            // "/" → root inode (= 1, per mntrs's
            // lookup_count convention)
            return Some(1);
        }
        let mut current_parent = 1u64;
        for component in trimmed.split('/') {
            if component.is_empty() {
                continue;
            }
            match self.inner.lookup(current_parent, component) {
                Ok(attr) => current_parent = attr.ino,
                Err(_) => return None,
            }
        }
        Some(current_parent)
    }
}

impl<F: CoreFilesystem + 'static> FileSystemContext for WinFspAdapter<F> {
    type FileContext = WinFspHandle;

    fn get_security_by_name(
        &self,
        file_name: &U16CStr,
        _security_descriptor: Option<&mut [c_void]>,
        _reparse_point_resolver: impl FnOnce(&U16CStr) -> Option<FileSecurity>,
    ) -> Result<FileSecurity> {
        let name = file_name.to_string_lossy();
        let path = name.replace('\\', "/");
        let parent = 1u64; // root
        let _ = self.inner.lookup(parent, &path).map_err(io_err_to_status)?;
        Ok(FileSecurity {
            reparse: false,
            sz_security_descriptor: 0,
            attributes: FILE_ATTRIBUTE_NORMAL,
        })
    }

    fn open(
        &self,
        file_name: &U16CStr,
        _create_options: u32,
        _granted_access: FILE_ACCESS_RIGHTS,
        _file_info: &mut OpenFileInfo,
    ) -> Result<Self::FileContext> {
        let name = file_name.to_string_lossy();
        let path = name.replace('\\', "/");
        let attr = self.inner.lookup(1, &path).map_err(io_err_to_status)?;
        let is_dir = attr.kind == CoreFileType::Directory;
        let ino = attr.ino;
        // Bug 11: actually call CoreFilesystem::open so
        // the per-handle FileHandleState (cache_fd for
        // writes, prefetcher for reads) gets populated.
        // Pre-fix the WinFspHandle was just { ino,
        // is_dir } with no fh and no inner.open() — so
        // every Windows write hit a missing handle and
        // failed at handles.get(fh). Directories don't
        // need a separate fh (this adapter has no
        // distinct opendir/closedir path), so we reuse
        // ino as the dir "fh" — only files take the
        // open() round-trip.
        let fh = if is_dir {
            ino
        } else {
            let flags = winfsp_access_to_open_flags(_granted_access);
            self.inner.open(ino, flags).map_err(io_err_to_status)?
        };
        // WinFSP open() sets FileInfo via OpenFileInfo; kernel auto-fills from response
        Ok(WinFspHandle { ino, fh, is_dir })
    }

    fn close(&self, _context: Self::FileContext) {
        // Bug 11: use the real fh, not ino. Pre-fix
        // close() called release(ino, ino) which would
        // try to release the ino as if it were a fh
        // and skip the real handle. With the real fh
        // here, FileHandleState entries are actually
        // removed and cache_fds (Arc<Mutex<File>>)
        // get their last strong ref dropped.
        if !_context.is_dir {
            let _ = self.inner.release(_context.ino, _context.fh);
        }
    }

    fn get_file_info(&self, context: &Self::FileContext, file_info: &mut FileInfo) -> Result<()> {
        let attr = self.inner.getattr(context.ino).map_err(io_err_to_status)?;
        core_attr_to_file_info(&attr, file_info);
        Ok(())
    }

    fn read(&self, context: &Self::FileContext, buffer: &mut [u8], offset: u64) -> Result<u32> {
        let size = buffer.len() as u32;
        let data = self
            .inner
            .read(context.ino, context.fh, offset, size)
            .map_err(io_err_to_status)?;
        let n = data.len().min(buffer.len());
        buffer[..n].copy_from_slice(&data[..n]);
        Ok(n as u32)
    }

    fn write(
        &self,
        context: &Self::FileContext,
        buffer: &[u8],
        offset: u64,
        _write_to_eof: bool,
        _constrained_io: bool,
        _file_info: &mut FileInfo,
    ) -> Result<u32> {
        self.inner
            .write(context.ino, context.fh, offset, buffer)
            .map_err(io_err_to_status)
    }

    fn rename(
        &self,
        _context: &Self::FileContext,
        file_name: &U16CStr,
        new_file_name: &U16CStr,
        _replace_if_exists: bool,
    ) -> Result<()> {
        // Issue #56: WinFSP passes full paths (e.g.
        // "/subdir/file.txt") as `file_name`. The
        // pre-fix code hardcoded `parent = 1` and
        // relied on the path concatenation
        // `format!("{}/{}", root_path, src)` to
        // accidentally produce the correct full
        // path. That works only because the root
        // path is empty AND src starts with "/". For
        // non-root mounts (e.g. --root subdir/) or
        // relative src paths, the result is wrong
        // and the rename lands at the wrong place
        // (or fails with NotFound).
        //
        // Fix: extract the parent directory from the
        // full src path, look up its ino, and pass
        // (parent_ino, basename, newparent_ino,
        // newname) to the trait's rename.
        let src_full = file_name.to_string_lossy().replace('\\', "/");
        let dst_full = new_file_name.to_string_lossy().replace('\\', "/");
        let (src_parent_path, src_name) = match src_full.rsplit_once('/') {
            Some((parent, name)) if !name.is_empty() => (parent.to_string(), name.to_string()),
            // No slash or empty basename — treat as
            // root-level rename (parent=1).
            _ => ("/".to_string(), src_full.clone()),
        };
        let (dst_parent_path, dst_name) = match dst_full.rsplit_once('/') {
            Some((parent, name)) if !name.is_empty() => (parent.to_string(), name.to_string()),
            _ => ("/".to_string(), dst_full.clone()),
        };
        // Resolve the parent paths to inodes via
        // lookup. WinFSP's pre-fix hardcoded 1
        // because the path concatenation masked the
        // bug; the trait's rename(parent_ino, name,
        // newparent_ino, newname) signature requires
        // a real parent ino.
        let src_parent_ino = self.parent_ino_for(&src_parent_path).unwrap_or(1);
        let dst_parent_ino = self.parent_ino_for(&dst_parent_path).unwrap_or(1);
        self.inner
            .rename(src_parent_ino, &src_name, dst_parent_ino, &dst_name)
            .map_err(io_err_to_status)
    }

    fn set_basic_info(
        &self,
        _context: &Self::FileContext,
        _file_attributes: u32,
        _creation_time: u64,
        _last_access_time: u64,
        _last_write_time: u64,
        _change_time: u64,
        _file_info: &mut FileInfo,
    ) -> Result<()> {
        // For now: no-op (S3 doesn't support Windows file attributes)
        // mtime/atime would need CoreFilesystem::setattr with mtime
        Ok(())
    }

    fn set_file_size(
        &self,
        context: &Self::FileContext,
        new_size: u64,
        _set_allocation_size: bool,
        _file_info: &mut FileInfo,
    ) -> Result<()> {
        // CoreFilesystem::setattr with size=Some(new_size).
        // Issue #42: pass the open fh so the impl can
        // ftruncate the cache fd directly. context.fh is
        // the per-handle value WinFspAdapter minted in
        // `open` (see the WinFspHandle struct comment);
        // for a directory handle (where WinFSP didn't
        // call open) fh equals ino, which the impl falls
        // back from gracefully.
        self.inner
            .setattr(
                context.ino,
                None,
                None,
                None,
                Some(new_size),
                None,
                None,
                Some(context.fh),
            )
            .map_err(io_err_to_status)?;
        Ok(())
    }

    fn get_volume_info(&self, out_volume_info: &mut VolumeInfo) -> Result<()> {
        let v = self.inner.statfs(1).map_err(io_err_to_status)?;
        core_volume_to_volume_info(&v, out_volume_info);
        Ok(())
    }

    fn get_security(
        &self,
        _context: &Self::FileContext,
        _security_descriptor: Option<&mut [c_void]>,
    ) -> Result<u64> {
        Err(STATUS_INVALID_DEVICE_REQUEST.into())
    }

    fn set_security(
        &self,
        _context: &Self::FileContext,
        _security_information: u32,
        _modification_descriptor: ModificationDescriptor,
    ) -> Result<()> {
        Err(STATUS_INVALID_DEVICE_REQUEST.into())
    }

    fn flush(&self, context: Option<&Self::FileContext>, _file_info: &mut FileInfo) -> Result<()> {
        if let Some(ctx) = context {
            // Bug 11: use the real fh, not ino. Same
            // rationale as `close`/`read`/`write`.
            //
            // Issue #35: WinFSP's `flush` is the
            // user-space `FlushFileBuffers` semantic
            // (force-cached-data-to-disk), which is the
            // FUSE `fsync` equivalent. The pre-fix
            // adapter forwarded to `CoreFilesystem::flush`
            // (queue-async-writeback), which is a
            // different operation entirely. We now call
            // `fsync` (datasync=true — user data only,
            // matching FlushFileBuffers semantics) and
            // keep the existing `flush` call as a
            // best-effort writeback trigger for backends
            // where the fsync-on-cache-fd isn't enough
            // (e.g. cloud storage where "durable" means
            // uploaded, not just on local disk).
            self.inner
                .fsync(ctx.ino, ctx.fh, true)
                .map_err(io_err_to_status)?;
            self.inner
                .flush(ctx.ino, ctx.fh)
                .map_err(io_err_to_status)?;
        }
        Ok(())
    }

    fn get_dir_info_by_name(
        &self,
        _context: &Self::FileContext,
        _file_name: &U16CStr,
        _out_dir_info: &mut DirInfo,
    ) -> Result<()> {
        // Called during read_directory pattern matching (only when
        // VolumeParams::pass_query_directory_filename is enabled).
        Err(STATUS_INVALID_DEVICE_REQUEST.into())
    }
}

#[cfg(all(feature = "async-io", windows))]
impl<F: CoreFilesystem + 'static> winfsp::filesystem::AsyncFileSystemContext for WinFspAdapter<F> {
    fn spawn_task(&self, future: impl std::future::Future<Output = ()> + Send + 'static) {
        // Bug 12: pre-fix spawned a fresh OS thread +
        // a fresh single-threaded tokio runtime per
        // future. Each Runtime::new() builds its own
        // worker pool, IO/timer drivers, allocs ~hundreds
        // of KiB, and then is dropped after a single
        // future. Under any nontrivial WinFSP call rate
        // that's pure waste — and the thread spawn alone
        // is ~10 µs per call.
        //
        // Reuse the shared multi-thread runtime already
        // built once per process by `rt()` (the same
        // runtime the synchronous read/write paths
        // use). `spawn` is fire-and-forget; the future
        // runs on the shared worker pool.
        crate::rt().spawn(future);
    }
}
