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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use widestring::U16CStr;
use windows::Win32::Foundation::STATUS_INVALID_DEVICE_REQUEST;
use winfsp::Result;
use winfsp::filesystem::{
    DirBuffer, DirInfo, DirMarker, FileInfo, FileSecurity, FileSystemContext,
    ModificationDescriptor, OpenFileInfo, VolumeInfo,
};
use winfsp_sys::{FILE_ACCESS_RIGHTS, FILE_FLAGS_AND_ATTRIBUTES};

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
    attrs as u32 | FILE_ATTRIBUTE_ARCHIVE as u32
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

/// map std::io::ErrorKind to winfsp NTSTATUS
fn io_err_to_status(e: std::io::Error) -> windows::core::Error {
    match e.kind() {
        std::io::ErrorKind::NotFound => windows::Win32::Foundation::STATUS_OBJECT_NAME_NOT_FOUND,
        std::io::ErrorKind::PermissionDenied => windows::Win32::Foundation::STATUS_ACCESS_DENIED,
        std::io::ErrorKind::AlreadyExists => {
            windows::Win32::Foundation::STATUS_OBJECT_NAME_COLLISION
        }
        std::io::ErrorKind::InvalidInput => windows::Win32::Foundation::STATUS_INVALID_PARAMETER,
        std::io::ErrorKind::StorageFull => windows::Win32::Foundation::STATUS_DISK_FULL,
        _ => windows::Win32::Foundation::STATUS_UNSUCCESSFUL,
    }
    .into()
}

/// A per-handle context for WinFSP.
/// WinFSP 的 FileContextMode::Minimal 下 handle 就是 ino。
#[derive(Clone)]
pub struct WinFspHandle {
    pub ino: u64,
    pub is_dir: bool,
}

/// WinFSP adapter that wraps a `CoreFilesystem`.
pub struct WinFspAdapter<F: CoreFilesystem + 'static> {
    pub inner: Arc<F>,
}

impl<F: CoreFilesystem + 'static> WinFspAdapter<F> {
    pub fn new(inner: Arc<F>) -> Self {
        Self { inner }
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
            attributes: FILE_ATTRIBUTE_NORMAL as u32,
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
        // WinFSP open() sets FileInfo via OpenFileInfo; kernel auto-fills from response
        Ok(WinFspHandle { ino, is_dir })
    }

    fn close(&self, _context: Self::FileContext) {
        let _ = self.inner.release(_context.ino, _context.ino);
    }

    fn get_file_info(&self, context: &Self::FileContext, file_info: &mut FileInfo) -> Result<()> {
        let attr = self.inner.getattr(context.ino).map_err(io_err_to_status)?;
        core_attr_to_file_info(&attr, file_info);
        Ok(())
    }

    fn read_directory(
        &self,
        _context: &Self::FileContext,
        _pattern: Option<&U16CStr>,
        marker: DirMarker<'_>,
        buffer: &mut [u8],
    ) -> Result<u32> {
        let ino = _context.ino;
        let entries = self.inner.readdir(ino).map_err(io_err_to_status)?;
        let mut dir_info = DirInfo::new(buffer, &marker)?;
        for entry in &entries {
            if !dir_info.can_add() {
                break;
            }
            let is_dir = entry.kind == CoreFileType::Directory;
            let file_attrs = if is_dir {
                FILE_ATTRIBUTE_DIRECTORY
            } else {
                FILE_ATTRIBUTE_NORMAL
            };
            // WinFSP DirInfo::add needs the name as U16CStr
            let name_wide = widestring::U16String::from_str(&entry.name);
            // Safety: DirInfo::add() writes into buffer; marker tracks position
            unsafe {
                dir_info.add(
                    &name_wide,
                    entry.ino,
                    file_attrs as u32,
                    0, // allocation size hint
                    0, // file size hint
                    0, // creation time hint
                    0, // last access hint
                    0, // last write hint
                    0, // change time hint
                    0, // ea size
                )?;
            }
        }
        Ok(dir_info.bytes_written())
    }

    fn read(&self, context: &Self::FileContext, buffer: &mut [u8], offset: u64) -> Result<u32> {
        let size = buffer.len() as u32;
        let data = self
            .inner
            .read(context.ino, context.ino, offset, size)
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
            .write(context.ino, context.ino, offset, buffer)
            .map_err(io_err_to_status)
    }

    fn rename(
        &self,
        _context: &Self::FileContext,
        file_name: &U16CStr,
        new_file_name: &U16CStr,
        _replace_if_exists: bool,
    ) -> Result<()> {
        let src = file_name.to_string_lossy().replace('\\', "/");
        let dst = new_file_name.to_string_lossy().replace('\\', "/");
        self.inner
            .rename(1, &src, 1, &dst)
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
        _file_info: &mut FileInfo,
        _set_allocation_size: bool,
    ) -> Result<()> {
        // CoreFilesystem::setattr with size=Some(new_size)
        self.inner
            .setattr(context.ino, None, None, None, Some(new_size), None, None)
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
        _modification_descriptor: &ModificationDescriptor,
    ) -> Result<()> {
        Err(STATUS_INVALID_DEVICE_REQUEST.into())
    }

    fn flush(&self, context: Option<&Self::FileContext>, _file_info: &mut FileInfo) -> Result<()> {
        if let Some(ctx) = context {
            self.inner
                .flush(ctx.ino, ctx.ino)
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

    fn spawn_task(&self, future: impl std::future::Future<Output = ()> + Send + 'static) {
        std::thread::spawn(|| {
            tokio::runtime::Runtime::new()
                .expect("winfsp tokio rt")
                .block_on(future);
        });
    }
}
