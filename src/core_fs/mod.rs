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

    /// Forget about an inode (decrement reference count).
    fn forget(&self, _ino: u64, _nlookup: u64) {}

    /// Get file attributes.
    fn getattr(&self, ino: u64) -> std::io::Result<CoreFileAttr>;

    /// Set file attributes.
    fn setattr(
        &self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<SystemTime>,
        _mtime: Option<SystemTime>,
    ) -> std::io::Result<CoreFileAttr>;

    /// Read directory entries.
    fn readdir(&self, ino: u64) -> std::io::Result<Vec<CoreDirEntry>>;

    /// Open a file (return a handle id).
    fn open(&self, ino: u64, _flags: u32) -> std::io::Result<u64>;

    /// Read data from an open file handle.
    fn read(&self, ino: u64, fh: u64, offset: u64, size: u32) -> std::io::Result<Vec<u8>>;

    /// Write data to an open file handle.
    fn write(&self, ino: u64, fh: u64, offset: u64, data: &[u8]) -> std::io::Result<u32>;

    /// Flush buffered data for an open handle.
    fn flush(&self, ino: u64, fh: u64) -> std::io::Result<()>;

    /// Release (close) an open file handle.
    fn release(&self, ino: u64, fh: u64) -> std::io::Result<()>;

    /// Create a file in a directory.
    fn create(&self, parent: u64, name: &str, mode: u32) -> std::io::Result<CoreFileAttr>;

    /// Create a directory.
    fn mkdir(&self, parent: u64, name: &str) -> std::io::Result<CoreFileAttr>;

    /// Remove a file.
    fn unlink(&self, parent: u64, name: &str) -> std::io::Result<()>;

    /// Remove a directory.
    fn rmdir(&self, parent: u64, name: &str) -> std::io::Result<()>;

    /// Rename a file or directory.
    fn rename(&self, parent: u64, name: &str, newparent: u64, newname: &str)
    -> std::io::Result<()>;

    /// Get volume statistics.
    fn statfs(&self, ino: u64) -> std::io::Result<CoreVolumeStat>;

    /// Open a directory (return a handle id).
    fn opendir(&self, ino: u64) -> std::io::Result<u64> {
        let _ = ino;
        Ok(0)
    }

    /// Release an open directory handle.
    fn releasedir(&self, _ino: u64, _fh: u64) -> std::io::Result<()> {
        Ok(())
    }

    /// Get extended attribute value.
    fn getxattr(&self, ino: u64, name: &str) -> std::io::Result<Vec<u8>>;

    /// List extended attribute names.
    fn listxattr(&self, ino: u64) -> std::io::Result<Vec<Vec<u8>>>;

    /// Check access permissions.
    fn access(&self, ino: u64, mask: u32) -> std::io::Result<()>;
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
    pub fn mount_winfsp<F: CoreFilesystem + 'static>(fs: Arc<F>) -> std::io::Result<MountGuard> {
        let adapter = WinFspAdapter::new(fs);
        let mut host = FileSystemHost::new(winfsp::host::VolumeParams::default(), adapter)
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("FileSystemHost::new: {e}"),
                )
            })?;
        let _mp = host.mount(MountPoint::NextFreeDrive).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("host.mount: {e}"))
        })?;
        Ok(MountGuard { host: Some(host) })
    }

    /// RAII guard that unmounts on drop.
    pub struct MountGuard {
        host: Option<FileSystemHost<WinFspAdapter<crate::MntrsFs>>>,
    }

    impl Drop for MountGuard {
        fn drop(&mut self) {
            if let Some(host) = self.host.take() {
                host.stop();
            }
        }
    }
}
