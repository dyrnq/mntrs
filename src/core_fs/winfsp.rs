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
    DirInfo, DirMarker, FileInfo, FileSecurity, FileSystemContext, ModificationDescriptor,
    OpenFileInfo, VolumeInfo,
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

/// Issue #249: minimal wildcard match for the
/// `read_directory` `pattern` argument (WinFSP passes
/// the user's `dir *.txt`-style filter here). Supports
/// `*` (any sequence of chars) and `?` (single char).
/// Case-insensitive by default (matches Windows dir
/// default). Full glob is out of scope — Windows uses
/// `*` and `?` only; Win32's `FindFirstFile` accepts the
/// same subset we handle here.
fn match_wildcard(pattern: &str, name: &str, case_insensitive: bool) -> bool {
    let (p, n) = if case_insensitive {
        (pattern.to_lowercase(), name.to_lowercase())
    } else {
        (pattern.to_string(), name.to_string())
    };
    wildcard_match_inner(p.as_bytes(), n.as_bytes())
}

fn wildcard_match_inner(pat: &[u8], name: &[u8]) -> bool {
    // Recursive wildcard match: `*` matches any
    // (possibly empty) sequence; `?` matches exactly one
    // char; everything else is a literal byte match.
    // Returns true iff the entire `name` is consumed.
    fn helper(pat: &[u8], name: &[u8]) -> bool {
        match (pat.first(), name.first()) {
            (None, None) => true,
            (None, Some(_)) => false,
            (Some(b'*'), _) => {
                // Skip the `*`; either match zero chars
                // (try matching rest of pattern against
                // current name) or one+ chars (try
                // matching rest of pattern against rest
                // of name).
                helper(&pat[1..], name) || (!name.is_empty() && helper(pat, &name[1..]))
            }
            (Some(b'?'), Some(_)) => helper(&pat[1..], &name[1..]),
            (Some(a), Some(b)) if a == b => helper(&pat[1..], &name[1..]),
            _ => false,
        }
    }
    helper(pat, name)
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
///
/// Issue #249: `dir_fh` is the per-fh directory lister
/// handle returned by `CoreFilesystem::opendir` for
/// directory handles. Pre-fix the adapter had no
/// `read_directory` callback (returning
/// STATUS_INVALID_DEVICE_REQUEST), so dirs were
/// unreadable. With the dispatcher now started (see
/// mount.rs host.start_with_threads) the kernel actually
/// delivers IRPs to this adapter, so we need a real impl.
#[derive(Clone)]
pub struct WinFspHandle {
    pub ino: u64,
    /// File handle returned by `CoreFilesystem::open`.
    /// Equal to `ino` for files where WinFSP didn't
    /// expose a separate open/release path that maps to
    /// our trait (kept for backwards compatibility with
    /// bug 11 — see the close path).
    pub fh: u64,
    pub is_dir: bool,
    /// Per-fh directory lister handle returned by
    /// `CoreFilesystem::opendir`. `0` means "no lister
    /// materialised yet" (the read_directory callback
    /// will call opendir on demand). Only meaningful
    /// for directories.
    pub dir_fh: u64,
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
        tracing::debug!(name = %name, "winfsp::get_security_by_name: entered");
        let path = name.replace('\\', "/");
        let parent = 1u64; // root
        // Issue #249 follow-up: WinFSP's pre-open stat uses
        // the `attributes` field here to decide whether the
        // target is a file or a directory. The kernel then
        // uses that decision to route the readdir IRP. If we
        // return FILE_ATTRIBUTE_NORMAL for a directory entry,
        // the kernel thinks the target is a regular file and
        // rejects the readdir with STATUS_OBJECT_NAME_INVALID
        // (Win32 ERROR_INVALID_NAME = 267, "目录名无效").
        // The pre-fix code always returned FILE_ATTRIBUTE_NORMAL,
        // which is exactly the bug we're hunting here. Now we
        // look up the actual entry and return its kind-correct
        // attributes (mirroring what we do in open's FileInfo
        // and in get_file_info).
        let attr = self.inner.lookup(parent, &path).map_err(|e| {
            tracing::debug!(name = %name, error = %e, "winfsp::get_security_by_name: lookup failed");
            io_err_to_status(e)
        })?;
        let attributes = core_kind_to_file_attributes(attr.kind, attr.perm);
        tracing::debug!(name = %name, ?attr.kind, attributes, "winfsp::get_security_by_name: ok");
        Ok(FileSecurity {
            reparse: false,
            sz_security_descriptor: 0,
            attributes,
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
        tracing::debug!(name = %name, "winfsp::open: entered");
        let path = name.replace('\\', "/");
        let attr = self.inner.lookup(1, &path).map_err(|e| {
            tracing::debug!(name = %name, error = %e, "winfsp::open: lookup failed");
            io_err_to_status(e)
        })?;
        let is_dir = attr.kind == CoreFileType::Directory;
        let ino = attr.ino;
        tracing::debug!(name = %name, ino, is_dir, "winfsp::open: lookup ok");
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
        // Issue #249 follow-up: actually fill the FileInfo
        // (AsMut<FileInfo> impl on OpenFileInfo). Pre-fix
        // we left it at default — file size 0, mtime
        // unset, no FILE_ATTRIBUTE_DIRECTORY bit — which
        // makes the kernel think the handle is for a
        // regular 0-byte file even when it's a directory.
        // That breaks every subsequent readdir IRP
        // (WinFSP sees FileInfo.FileAttributes without
        // FILE_ATTRIBUTE_DIRECTORY and routes the
        // QueryDirectory IRP somewhere that returns
        // STATUS_OBJECT_NAME_INVALID → Win32
        // ERROR_INVALID_NAME = 267, which is exactly
        // the "目录名无效" we observed from `dir V:\`).
        core_attr_to_file_info(&attr, _file_info.as_mut());
        // WinFSP open() sets FileInfo via OpenFileInfo; kernel auto-fills from response
        // Issue #249: also mint a per-fh directory lister
        // handle for directory opens so the read_directory
        // callback can paginate off the cached entry list
        // (issue #23 path) instead of re-listing the backend
        // on every page. For files we leave dir_fh=0; the
        // read_directory callback ignores it because is_dir.
        let dir_fh = if is_dir {
            self.inner.opendir(ino).map_err(io_err_to_status)?
        } else {
            0
        };
        Ok(WinFspHandle {
            ino,
            fh,
            is_dir,
            dir_fh,
        })
    }

    // Issue #249 follow-up: WinFSP kernel routes
    // create-new-file requests (PowerShell's Set-Content,
    // Explorer drag-drop, cmd `echo > V:\foo`, copy/paste,
    // `New-Item -ItemType File`, etc.) through this
    // callback, NOT through `open`. Pre-fix the adapter
    // inherited the trait default of returning
    // STATUS_INVALID_DEVICE_REQUEST, which caused the
    // kernel to fail every CreateFileW with
    // IRP_MJ_CREATE and FILE_OPEN_IF — users saw
    // "位置不可用" in Explorer, "Get-Content writer IO
    // error" from PowerShell, and `echo > V:\foo.txt`
    // from cmd all rejected. The fix routes the WinFSP
    // create-options bits into the equivalent
    // CoreFilesystem::create call and emits an
    // OpenFileInfo populated with the freshly-minted
    // attribute so the kernel can cache the entry.
    #[allow(clippy::too_many_arguments)]
    fn create(
        &self,
        file_name: &U16CStr,
        create_options: u32,
        _granted_access: FILE_ACCESS_RIGHTS,
        file_attributes: u32,
        _security_descriptor: Option<&[c_void]>,
        _allocation_size: u64,
        _extra_buffer: Option<&[u8]>,
        _extra_buffer_is_reparse_point: bool,
        file_info: &mut OpenFileInfo,
    ) -> Result<Self::FileContext> {
        let name = file_name.to_string_lossy();
        tracing::debug!(name = %name, ?create_options, file_attributes, "winfsp::create: entered");
        // Win32 create_options bits (matches
        // windows::Win32::Storage::FileSystem):
        //   FILE_DIRECTORY_FILE   = 0x0000_0001
        //   FILE_NON_DIRECTORY_FILE = 0x0000_0040
        //   FILE_DELETE_ON_CLOSE = 0x0000_1000
        // We treat the request as a directory create
        // when FILE_DIRECTORY_FILE is set and FILE_NON_
        // DIRECTORY_FILE is not; everything else is a
        // file. The CoreFilesystem trait exposes
        // distinct `create` and `mkdir` calls so we
        // route based on that bit.
        const FILE_DIRECTORY_FILE: u32 = 0x0000_0001;
        const FILE_NON_DIRECTORY_FILE: u32 = 0x0000_0040;
        let is_dir = (create_options & FILE_DIRECTORY_FILE) != 0
            && (create_options & FILE_NON_DIRECTORY_FILE) == 0;
        tracing::debug!(name = %name, is_dir, "winfsp::create: classifying request");

        let (attr, fh) = if is_dir {
            let attr = self
                .inner
                .mkdir(1u64, &name.replace('\\', "/"))
                .map_err(io_err_to_status)?;
            let ino = attr.ino;
            // Directories don't need an `open` fh —
            // the readdir/cleanup path uses the
            // synthesized inode as the directory handle.
            (attr, ino)
        } else {
            // CoreFilesystem::create(parent, name, mode)
            // returns (CoreFileAttr, fh) where fh is a
            // per-handle value the write path keeps in
            // FileHandleState. mode=0o644 matches the
            // POSIX default — the WinFSP `file_attributes`
            // is used by `core_kind_to_file_attributes`
            // downstream, not by the backend directly.
            let (attr, fh) = self
                .inner
                .create(1u64, &name.replace('\\', "/"), 0o644)
                .map_err(io_err_to_status)?;
            (attr, fh)
        };
        let ino = attr.ino;
        // Populate the OpenFileInfo so the kernel caches
        // the freshly-created entry's attributes.
        core_attr_to_file_info(&attr, file_info.as_mut());
        let dir_fh = if is_dir {
            self.inner.opendir(ino).map_err(io_err_to_status)?
        } else {
            0
        };
        tracing::debug!(name = %name, ino, fh, is_dir, dir_fh, "winfsp::create: ok");
        Ok(WinFspHandle {
            ino,
            fh,
            is_dir,
            dir_fh,
        })
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
        } else if _context.dir_fh != 0 {
            // Issue #249: drop the per-fh directory
            // lister we minted in `open`. The trait
            // default for releasedir is a no-op, but
            // MntrsFs implements it to remove the
            // opendir entry from `dir_listers` — without
            // this call, every dir open leaks a
            // DashMap entry until process exit.
            let _ = self.inner.releasedir(_context.ino, _context.dir_fh);
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
        tracing::debug!(
            total_size = out_volume_info.total_size,
            free_size = out_volume_info.free_size,
            "winfsp::get_volume_info: ok"
        );
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

    // Issue #249: implement read_directory. Pre-fix this was
    // the trait default `Err(STATUS_INVALID_DEVICE_REQUEST)`,
    // which made every `dir V:\` fail with "directory name
    // invalid" once the WinFSP dispatcher was actually started.
    //
    // WinFSP invokes this callback with:
    //   * `context`       — our WinFspHandle for the open dir
    //   * `pattern`       — wildcard (e.g. `*.txt`) or None
    //   * `marker`        — last entry name returned in the
    //                        previous page (None on first call)
    //   * `buffer`        — WinFSP-managed output buffer;
    //                        fill via FspFileSystemAddDirInfo
    //
    // Strategy:
    //   1. Materialise the full entry list once per
    //      opendir (the dir_fh on context already pins a
    //      per-handle snapshot via issue #23's
    //      `dir_listers`, so subsequent calls re-use it).
    //   2. Walk entries, skipping names <= marker
    //      (WinFSP's marker semantics: marker is the
    //      last delivered name; next page starts with
    //      entries strictly greater than marker).
    //   3. Pack each surviving entry into the buffer
    //      via FspFileSystemAddDirInfo, which returns
    //      FALSE when the buffer is full — we stop and
    //      return bytes-written.
    //
    // WinFSP's pattern is supported here as a substring
    // match (case-insensitive) — sufficient for the
    // common `dir *.txt` case; full glob would require
    // translating the wildcard which is out of scope for
    // #249.
    fn read_directory(
        &self,
        context: &Self::FileContext,
        pattern: Option<&U16CStr>,
        marker: DirMarker,
        buffer: &mut [u8],
    ) -> Result<u32> {
        use winfsp::filesystem::WideNameInfo;
        use winfsp_sys::FspFileSystemAddDirInfo;

        // Issue #249: read_directory is the WinFSP
        // counterpart of FUSE `readdir`. It's called by
        // the WinFSP kernel once per directory enumeration
        // request — typically each time Explorer opens a
        // folder, `dir V:\foo`, `Get-ChildItem V:\foo`,
        // etc. We populate the user-supplied `buffer`
        // with one or more FSP_FSCTL_DIR_INFO entries
        // (one per child) terminated by a NULL entry
        // (DirInfo::finalize_buffer) that signals EOF.
        //
        // Without the EOF marker, the kernel re-invokes
        // us with the same marker forever (we observed
        // 33866 calls in 3 seconds during one earlier
        // test, and after fixing the empty-cursor bug
        // below, ~600k calls before this run). The marker
        // semantics are: "deliver every entry whose name
        // is lexicographically greater than `marker`";
        // marker is None on the first call.

        // Defensive: a non-directory handle shouldn't reach
        // here (WinFSP only calls ReadDirectory on
        // directory handles). Return invalid-request
        // instead of panicking on the slice access below.
        tracing::debug!(
            ino = context.ino,
            dir_fh = context.dir_fh,
            "winfsp::read_directory: entered"
        );
        if !context.is_dir {
            tracing::debug!("winfsp::read_directory: not a dir, returning INVALID_DEVICE_REQUEST");
            return Err(STATUS_INVALID_DEVICE_REQUEST.into());
        }

        // Materialise the full entry list. `inner.readdir`
        // with `offset=0` returns the full Vec (issue #23
        // per-fh snapshot is already populated by
        // `opendir(dir_fh)` in the open path).
        let entries = self
            .inner
            .readdir(context.ino, context.dir_fh, 0, 0)
            .map_err(io_err_to_status)?;
        tracing::debug!(
            ino = context.ino,
            dir_fh = context.dir_fh,
            entry_count = entries.len(),
            "winfsp::read_directory: got entries"
        );

        // Convert marker (last delivered entry name) into
        // something we can compare against `entry.name`.
        // `marker.inner_as_cstr()` returns
        // `Option<&U16CStr>` (null-terminated UTF-16) when
        // a marker is set.
        let marker_u16: Option<Vec<u16>> = marker.inner_as_cstr().map(|m| m.as_slice().to_vec());
        let marker_dbg = marker_u16
            .as_ref()
            .map(|m| String::from_utf16_lossy(m))
            .unwrap_or_else(|| "<none>".to_string());
        tracing::debug!(marker = %marker_dbg, "winfsp::read_directory: marker state");

        let mut cursor: u32 = 0;
        for entry in &entries {
            let name_u16: Vec<u16> = entry
                .name
                .encode_utf16()
                .chain(std::iter::once(0))
                .collect();

            // Build a minimal CoreFileAttr for this
            // entry — the trait's readdir returns
            // `(ino, kind, name)` only, not the full
            // attr, so we synthesize one with the
            // available fields. Size/mtime are zeroed
            // (Explorer shows them as blank for readdir
            // entries until the kernel follows up with
            // getattr on each entry; that's the
            // pre-existing readdirplus optimization on
            // the FUSE side — Win32's FindNextFile
            // works the same way).
            let attr = crate::core_fs::CoreFileAttr {
                ino: entry.ino,
                kind: entry.kind,
                perm: 0o777,
                size: 0,
                blksize: 4096,
                blocks: 0,
                crtime: std::time::SystemTime::UNIX_EPOCH,
                mtime: std::time::SystemTime::UNIX_EPOCH,
                ctime: std::time::SystemTime::UNIX_EPOCH,
                atime: std::time::SystemTime::UNIX_EPOCH,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0,
            };

            // Skip entries <= marker (WinFSP marker is
            // exclusive: entries strictly greater than
            // marker are returned).
            if let Some(m) = &marker_u16 {
                // Compare u16 slices lexicographically.
                // BUG FIX: both `name_u16` and `m` are
                // null-terminated, but they come from
                // different sources — `name_u16` is
                // built from Rust's `encode_utf16().chain(0u16)`
                // (always includes trailing NUL), while
                // `m` is `U16CStr::as_slice()` which
                // ALSO includes the trailing NUL
                // (U16CStr's slice is the data up to
                // but NOT including the terminating NUL
                // in many APIs — but inner_as_cstr() in
                // winfsp 0.13 returns `&U16CStr` whose
                // `as_slice()` excludes the NUL).
                // The mismatch was causing our lex
                // comparison to fail to suppress entries
                // already delivered: e.g. when marker is
                // "." (slice = [0x2E]) and name_u16 is
                // [0x2E, 0x00], the trailing NUL on
                // name_u16 made it "greater than" the
                // marker lexically, so the entry got
                // re-sent on every read_directory call.
                // The result was an infinite re-delivery
                // loop. Strip trailing NULs from BOTH
                // sides before comparing so the comparison
                // is on the actual entry names.
                let name_no_nul: &[u16] = if name_u16.last() == Some(&0) {
                    &name_u16[..name_u16.len() - 1]
                } else {
                    &name_u16[..]
                };
                let marker_no_nul: &[u16] = if m.last() == Some(&0) {
                    &m[..m.len() - 1]
                } else {
                    &m[..]
                };
                if name_no_nul <= marker_no_nul {
                    continue;
                }
            }

            // Pattern filter (simple substring / wildcard).
            // Skip for now if pattern present and doesn't
            // match — `*.txt` becomes "ends with .txt";
            // any other wildcard is treated as no-match
            // (Win32's dir *.txt is the common case).
            if let Some(pat) = pattern {
                let pat_str = pat.to_string_lossy();
                if !match_wildcard(&pat_str, &entry.name, /* case_insensitive */ true) {
                    continue;
                }
            }

            // Build the high-level DirInfo<255> wrapper
            // (size, FileInfo, padding, file_name). The
            // wrapper handles the trailing-NUL bookkeeping
            // and computes Size correctly so
            // FspFileSystemAddDirInfo's overflow check
            // works.
            let mut di = DirInfo::<255>::new();
            core_attr_to_file_info(&attr, di.file_info_mut());
            // set_name_raw takes a &[u16] WITHOUT trailing
            // NUL and calls set_size() with byte_len. Use
            // it instead of poking name_buffer / set_size
            // directly (those are on the private
            // WideNameInfoInternal trait). Returns
            // FspError directly — propagate with `?` (the
            // function already returns winfsp::Result).
            di.set_name_raw(name_u16.as_slice())?;

            // Try to add this entry. FspFileSystemAddDirInfo
            // returns FALSE when the buffer can't fit
            // another entry — we stop and report what we
            // packed so far (WinFSP will call back with
            // the same marker to fetch the next page).
            let added = unsafe {
                FspFileSystemAddDirInfo(
                    (&mut di as *mut DirInfo<255>).cast(),
                    buffer.as_mut_ptr() as winfsp_sys::PVOID,
                    buffer.len() as u32,
                    &mut cursor,
                )
            };
            tracing::debug!(
                name = %entry.name,
                cursor_before = cursor,
                added,
                "winfsp::read_directory: tried to add entry"
            );
            if added == 0 {
                break;
            }
        }
        // Finalize: send a NULL DirInfo entry to
        // signal EOF. Without this, WinFSP interprets
        // the empty cursor as "more data pending" and
        // re-invokes read_directory with the last
        // marker — pre-fix this caused an infinite
        // re-entry loop (the user-space log grew to
        // 33866 lines within seconds of `dir V:\`).
        let finalize_ok = DirInfo::<255>::finalize_buffer(buffer, &mut cursor);
        tracing::debug!(cursor, finalize_ok, "winfsp::read_directory: returning");
        Ok(cursor)
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
