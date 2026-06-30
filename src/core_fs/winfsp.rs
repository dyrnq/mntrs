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

use std::collections::HashMap;
use std::ffi::c_void;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use winfsp::FspError;

use widestring::U16CStr;
use windows::Win32::Foundation::{STATUS_INVALID_DEVICE_REQUEST, STATUS_UNSUCCESSFUL};
use winfsp::Result;
use winfsp::constants::FspCleanupFlags;
use winfsp::filesystem::{
    DirInfo, DirMarker, FileInfo, FileSecurity, FileSystemContext, ModificationDescriptor,
    OpenFileInfo, VolumeInfo,
};
use winfsp_sys::FILE_ACCESS_RIGHTS;

// Win32 file attribute constants (same as win32 API)
const FILE_ATTRIBUTE_READONLY: u32 = 0x00000001;
const FILE_ATTRIBUTE_HIDDEN: u32 = 0x00000002;
const FILE_ATTRIBUTE_SYSTEM: u32 = 0x00000004;
const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x00000010;
const FILE_ATTRIBUTE_ARCHIVE: u32 = 0x00000020;
const FILE_ATTRIBUTE_NORMAL: u32 = 0x00000080;
const FILE_ATTRIBUTE_TEMPORARY: u32 = 0x00000100;
const FILE_ATTRIBUTE_OFFLINE: u32 = 0x00001000;
const FILE_ATTRIBUTE_NOT_CONTENT_INDEXED: u32 = 0x00002000;

// Issue #310: per-adapter TTL caches.
//
// `GETATTR_CACHE_TTL` is short (100 ms) because a single
// Explorer Refresh fans out into many concurrent
// `IRP_MJ_QUERY_INFORMATION` IRPs (one per file) — without a
// cache, every IRP goes to the backend (S3 ≈ 200 ms per stat).
// 100 ms is long enough to coalesce the burst, short enough
// that a freshly-written file's new size is visible on the
// next Explorer interaction.
//
// `VOLUME_INFO_CACHE_TTL` is longer (30 s) because
// `get_volume_info` only depends on the disk's static
// capacity — and Explorer calls it on every Refresh and every
// Properties dialog. 30 s is the right balance between
// "Explorer Refresh feels instant" and "operator can resize
// the volume and see it within 30 s".
const GETATTR_CACHE_TTL: std::time::Duration = std::time::Duration::from_millis(100);
const VOLUME_INFO_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(30);

// User-meaningful file-attribute bits that the kernel
// hands us in `create`'s `file_attributes` and that we
// want to OR into the returned `FileInfo.file_attributes`.
// Bits like FILE_ATTRIBUTE_DIRECTORY / NORMAL / ARCHIVE are
// set by `core_kind_to_file_attributes` based on the
// backend's kind+perm and are not user-meaningful on
// create (the kernel sets them based on the create-options
// bits FILE_DIRECTORY_FILE / FILE_NON_DIRECTORY_FILE).
const USER_FILE_ATTR_PASSTHROUGH: u32 = FILE_ATTRIBUTE_READONLY
    | FILE_ATTRIBUTE_HIDDEN
    | FILE_ATTRIBUTE_SYSTEM
    | FILE_ATTRIBUTE_TEMPORARY
    | FILE_ATTRIBUTE_OFFLINE
    | FILE_ATTRIBUTE_NOT_CONTENT_INDEXED;

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
    tracing::trace!(
        ino = attr.ino,
        size = attr.size,
        "core_attr_to_file_info: setting file_size"
    );
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

/// map std::io::Error to winfsp::Result error type.
///
/// Issue #305 Tier 1: pre-fix only 5 ErrorKind variants mapped to
/// specific NTSTATUS codes; everything else collapsed to
/// `STATUS_UNSUCCESSFUL` (0xC0000001). The kernel sees that as
/// "unspecified error" — Explorer's "Retry / Cancel" dialog shows the
/// generic message, robocopy /MIR treats the file as in-use rather
/// than missing, and `Get-Content` on a missing file reports
/// "device error" instead of "path not found". The mapping below
/// covers every common variant opendal surfaces so user-mode tools
/// see actionable errors.
fn io_err_to_status(e: std::io::Error) -> winfsp::FspError {
    use windows::Win32::Foundation::{
        NTSTATUS, STATUS_ACCESS_DENIED, STATUS_CANCELLED, STATUS_CONNECTION_ABORTED,
        STATUS_CONNECTION_REFUSED, STATUS_CONNECTION_RESET, STATUS_DISK_FULL, STATUS_END_OF_FILE,
        STATUS_INVALID_PARAMETER, STATUS_IO_TIMEOUT, STATUS_NETWORK_NAME_DELETED,
        STATUS_OBJECT_NAME_COLLISION, STATUS_OBJECT_NAME_NOT_FOUND, STATUS_PIPE_DISCONNECTED,
        STATUS_UNSUCCESSFUL,
    };
    // NTSTATUS is a transparent newtype; FspError::NTSTATUS takes
    // the raw i32 underneath. .0 un-nests it without an extra
    // conversion.
    let nt = |s: NTSTATUS| FspError::NTSTATUS(s.0);
    match e.kind() {
        // "the file/dir/object is not there" — most common path error.
        // Win32 callers (cmd, Explorer, robocopy) use this to decide
        // between "skip" (NotFound) vs "retry" (everything else).
        std::io::ErrorKind::NotFound => nt(STATUS_OBJECT_NAME_NOT_FOUND),
        // ACL / write-protected / immutable file.
        std::io::ErrorKind::PermissionDenied => nt(STATUS_ACCESS_DENIED),
        // CreateFile(FILE_CREATE) on a path that already exists;
        // WinFSP passes this through to e.g. `New-Item` / robocopy.
        std::io::ErrorKind::AlreadyExists => nt(STATUS_OBJECT_NAME_COLLISION),
        // Bad UTF-8 in path, negative seek, etc.
        std::io::ErrorKind::InvalidInput => nt(STATUS_INVALID_PARAMETER),
        // opendal surfaces S3 507 Insufficient Storage / Azure
        // ServerBusy + disk-full conditions here.
        std::io::ErrorKind::StorageFull => nt(STATUS_DISK_FULL),
        // Backends with their own timeout layer (opendal TimeoutLayer
        // + rclone's HTTPTimeout) surface here. Without this mapping
        // the WinFSP driver timeout (10s, see #312) masks the real
        // cause as STATUS_UNSUCCESSFUL.
        std::io::ErrorKind::TimedOut => nt(STATUS_IO_TIMEOUT),
        // TLS / HTTP connection lost mid-request. Without this map,
        // resume-on-read sees "device error" instead of "connection
        // reset" and Explorer pops a generic retry dialog.
        std::io::ErrorKind::ConnectionReset => nt(STATUS_CONNECTION_RESET),
        // Local TCP socket closed by peer (rare for object storage,
        // but covers httpx/hyper keep-alive races).
        std::io::ErrorKind::ConnectionAborted => nt(STATUS_CONNECTION_ABORTED),
        // Endpoint closed listener — S3-compatible stores in private
        // VPCs / misconfigured DNS.
        std::io::ErrorKind::ConnectionRefused => nt(STATUS_CONNECTION_REFUSED),
        // SMB / NFS mapped-drive read/write where the peer dropped.
        std::io::ErrorKind::BrokenPipe => nt(STATUS_PIPE_DISCONNECTED),
        // opendal surfaces read cancellations here. The kernel treats
        // STATUS_CANCELLED as "retry-allowed" which is the correct
        // semantic for an interrupted read.
        std::io::ErrorKind::Interrupted => nt(STATUS_CANCELLED),
        // HTTP body returned fewer bytes than Content-Length claimed.
        // Without this map, large file reads through `Range:` cuts
        // see "device error" instead of the actionable "unexpected
        // EOF" — robocopy retries the whole file instead of resuming.
        std::io::ErrorKind::UnexpectedEof => nt(STATUS_END_OF_FILE),
        // Backend accepted the request but wrote zero bytes (S3 200
        // with empty body on a PUT, Azure 0-length Blob). Almost
        // always a quota / permission issue that DISK_FULL surfaces
        // more usefully than UNSUCCESSFUL.
        std::io::ErrorKind::WriteZero => nt(STATUS_DISK_FULL),
        // Network name deleted (SMB session expired, RDS gateway
        // timeout). Distinct from ConnectionReset so admin tools can
        // recognise it.
        std::io::ErrorKind::NetworkUnreachable => nt(STATUS_NETWORK_NAME_DELETED),
        // Genuinely unmapped (e.g. ErrorKind::Other from a backend
        // that wrapped a domain-specific code). Preserve the source
        // error string in a tracing event so operators can diagnose
        // without enabling kernel debug logs, then fall back to
        // STATUS_UNSUCCESSFUL.
        _ => {
            tracing::debug!(
                error = %e,
                kind = ?e.kind(),
                "io_err_to_status: unmapped std::io::ErrorKind; mapping to STATUS_UNSUCCESSFUL"
            );
            nt(STATUS_UNSUCCESSFUL)
        }
    }
}

/// Run a kernel-callback body inside `catch_unwind` so a panic
/// surfaces to the WinFSP driver as `STATUS_UNSUCCESSFUL`
/// (0xC0000001) instead of unwinding across the WinFSP FFI
/// boundary — which is undefined behaviour and on Windows
/// typically tears down the entire mount process.
///
/// Issue #314 (#305 audit): every `FileSystemContext` method
/// on `WinFspAdapter` is called directly by WinFSP's
/// dispatcher threads (started via `host.start_with_threads(0)`).
/// A `panic!` (or any other Rust unwind) inside one of those
/// bodies crosses the `extern "system"` frames the dispatcher
/// owns; the Windows kernel may then either kill the process
/// outright or hang the request that triggered the callback
/// (with no observable error to the user-mode caller). Wrapping
/// the body guarantees:
///
///   * the kernel gets a clean error it can return to the
///     user-mode syscall (`GetLastError()` becomes
///     `ERROR_UNHANDLED_EXCEPTION` / equivalent),
///   * subsequent requests on the same dispatcher thread keep
///     flowing,
///   * the panic message is captured in our logs so the bug
///     is diagnosable postmortem.
///
/// `AssertUnwindSafe` is required because the captured state
/// (`&self`, `&mut file_info`, etc.) is not generally
/// `UnwindSafe` (the auto-trait requires the inner types to
/// be `RefUnwindSafe`, which `Arc<F: CoreFilesystem>` is but
/// raw `&mut FileInfo` isn't). We assert the safety claim
/// manually: even if a panic leaves the inner state
/// inconsistent, returning an error to the kernel is the
/// correct behaviour — the next request that hits a corrupt
/// inode will surface the same way.
fn catch_panic<F, R>(method: &'static str, f: F) -> Result<R>
where
    F: FnOnce() -> Result<R>,
{
    match std::panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(payload) => {
            let msg = panic_message(&payload);
            tracing::error!(
                method,
                panic = %msg,
                "winfsp callback panicked; returning STATUS_UNSUCCESSFUL to kernel"
            );
            Err(FspError::NTSTATUS(STATUS_UNSUCCESSFUL.0))
        }
    }
}

/// Same panic-safety contract as [`catch_panic`] but for the
/// one `FileSystemContext` method whose return type is `()`
/// (`close`): a panic is logged and swallowed — there is no
/// error to propagate to the kernel, but the catch prevents
/// the unwind from crossing the FFI boundary.
fn swallow_panic<F>(method: &'static str, f: F)
where
    F: FnOnce(),
{
    if let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(f)) {
        let msg = panic_message(&payload);
        tracing::error!(
            method,
            panic = %msg,
            "winfsp close-callback panicked; ignoring"
        );
    }
}

/// Extract a human-readable panic message from the payload
/// returned by `catch_unwind`. Most panics are either
/// `&'static str` (from `panic!("foo")`) or `String` (from
/// `panic!("{}", x)` or `unwrap` on a non-string `Err`); we
/// fall back to a generic note for any other payload type
/// (Boxed custom types, `None`-style panics, etc.).
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&'static str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string())
}

/// Convert a Win32 FILETIME (u64, 100-ns intervals since 1601-01-01
/// UTC) into a [`SystemTime`].
///
/// Returns `None` for the Win32 "leave unchanged" sentinel (`0`)
/// and for any value earlier than the Unix epoch (pre-1601 wrap or
/// caller bug) — both cases are surfaced as `None` so the caller
/// can skip the corresponding `setattr` argument instead of
/// silently setting mtime / atime to the Unix epoch. Real
/// timestamps are converted by subtracting the 1601→1970 offset.
///
/// Rationale: WinFSP's `set_basic_info` passes raw FILETIME u64s;
/// the `windows` crate's `FILETIME` struct has no
/// `to_system_time()` helper. The math is straightforward but
/// subtle — the offset is 369 years × ~365.25 days × 86_400 s ×
/// 10^7 (100ns units), or `0x019DB1DED53E8000` as a hex constant
/// (`116_444_736_000_000_000`).
fn filetime_u64_to_system_time(ft: u64) -> Option<SystemTime> {
    // 0 = "leave unchanged" per Win32 SetFileTime contract.
    if ft == 0 {
        return None;
    }
    // Offset between 1601-01-01 and 1970-01-01 in 100-ns ticks.
    const FT_UNIX_OFFSET: u64 = 0x019D_B1DE_D53E_8000;
    if ft < FT_UNIX_OFFSET {
        // Pre-Unix-epoch FILETIME (e.g. accidentally fed the raw
        // low/high dwords without packing). Treat as unset rather
        // than crashing on the subtraction below.
        return None;
    }
    let unix_100ns = ft - FT_UNIX_OFFSET;
    // Convert 100-ns ticks to nanos (×100), then to Duration.
    // Saturation: u64::MAX nanos ≈ 584 years — well beyond any
    // plausible 1601-3000 FILETIME, so the cast is safe.
    let nanos = unix_100ns.saturating_mul(100);
    UNIX_EPOCH.checked_add(std::time::Duration::from_nanos(nanos))
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
    /// Issue #310: per-adapter TTL cache for `get_file_info`.
    /// Keyed by inode (`u64`) — the WinFSP callback only
    /// hands us an inode via `WinFspHandle.ino`, not a path,
    /// so we can't reuse `MntrsFs::attr_cache` (path-keyed)
    /// directly. The cache saves one backend `stat_op`
    /// round-trip per `IRP_MJ_QUERY_INFORMATION` IRP within
    /// `GETATTR_CACHE_TTL` (100 ms). The inode→attr map is
    /// bounded by the number of files the mount has touched
    /// in the last 100 ms; for typical Explorer workloads
    /// (a Refresh fans out to a few hundred files) the map
    /// holds at most a few hundred entries and is naturally
    /// trimmed by TTL expiry on the next miss.
    getattr_cache: Mutex<HashMap<u64, (Instant, CoreFileAttr)>>,
    /// Issue #310: per-adapter TTL cache for `get_volume_info`.
    /// Holds the most recent `CoreVolumeStat` for
    /// `VOLUME_INFO_CACHE_TTL` (30 s). `get_volume_info` is
    /// the inner.statfs(1) call which is 200 ms+ on S3; the
    /// cache keeps Explorer Refresh snappy.
    volume_info_cache: Mutex<Option<(Instant, CoreVolumeStat)>>,
}

impl<F: CoreFilesystem + 'static> WinFspAdapter<F> {
    pub fn new(inner: Arc<F>) -> Self {
        Self {
            inner,
            getattr_cache: Mutex::new(HashMap::new()),
            volume_info_cache: Mutex::new(None),
        }
    }

    /// Issue #310: drop a single inode's cached `attr`
    /// after a delete (and, in the future, rename).
    /// Called from `cleanup` when the kernel passes
    /// `FspCleanupDelete`. Without this an immediately-
    /// following `IRP_MJ_QUERY_INFORMATION` for the same
    /// ino would hit the 100 ms TTL entry and report the
    /// file as still present, masking the delete.
    fn invalidate_getattr_cache_ino(&self, ino: u64) {
        let mut cache = self.getattr_cache.lock().unwrap_or_else(|e| e.into_inner());
        if cache.remove(&ino).is_some() {
            tracing::trace!(ino, "winfsp: invalidated getattr cache entry on delete");
        }
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
            // Issue #307: NFC-normalize each parent component
            // defensively. Current callers (cleanup) already
            // pass NFC post-fix, so this is a no-op today;
            // future callers that bypass cleanup's normalize
            // step stay correct.
            let component = crate::util::nfc(component);
            match self.inner.lookup(current_parent, &component) {
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
        // Issue #314: panic safety wrapper — see catch_panic.
        catch_panic(
            "get_security_by_name",
            AssertUnwindSafe(|| {
                // Issue #307: NFC-normalize the kernel-supplied
                // name so cross-adapter lookups (macOS FUSE uploads
                // NFD, WinFSP queries NFC) hit the same backend key.
                let name = crate::util::nfc(&file_name.to_string_lossy());
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
            }),
        )
    }

    fn open(
        &self,
        file_name: &U16CStr,
        _create_options: u32,
        _granted_access: FILE_ACCESS_RIGHTS,
        _file_info: &mut OpenFileInfo,
    ) -> Result<Self::FileContext> {
        // Issue #314: panic safety wrapper — see catch_panic.
        catch_panic(
            "open",
            AssertUnwindSafe(|| {
                // Issue #307: NFC-normalize the kernel-supplied
                // name (see get_security_by_name for rationale).
                let name = crate::util::nfc(&file_name.to_string_lossy());
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
            }),
        )
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
        // Issue #314: panic safety wrapper — see catch_panic.
        catch_panic(
            "create",
            AssertUnwindSafe(|| {
                // Issue #307: NFC-normalize the kernel-supplied
                // name (see get_security_by_name for rationale).
                let name = crate::util::nfc(&file_name.to_string_lossy());
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
                // Issue #310: the kernel passes the
                // user-requested file attributes (HIDDEN,
                // SYSTEM, READONLY, etc.) in `file_attributes`.
                // `core_attr_to_file_info` derives the bits
                // from the backend's kind+perm (so it sets
                // DIRECTORY / NORMAL / ARCHIVE / READONLY
                // based on the attr). The user-meaningful
                // bits are dropped — a `New-Item -Force` on a
                // hidden file would still show up as a normal
                // file in Explorer. OR in the meaningful bits
                // (see USER_FILE_ATTR_PASSTHROUGH) so they
                // round-trip into the kernel's cached
                // FileInfo. The class bits (DIRECTORY /
                // NORMAL / ARCHIVE) are NOT passed through;
                // those are determined by the create-options
                // bits and the backend's kind+perm.
                let user_attrs = file_attributes & USER_FILE_ATTR_PASSTHROUGH;
                if user_attrs != 0 {
                    tracing::debug!(
                        ino,
                        file_attributes,
                        user_attrs,
                        "winfsp::create: passing through user-requested file attributes"
                    );
                    file_info.as_mut().file_attributes |= user_attrs;
                }
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
            }),
        )
    }

    fn close(&self, _context: Self::FileContext) {
        // Issue #314: panic safety wrapper — see swallow_panic.
        // `close` returns `()` (no error path to surface a
        // panic to the kernel), so we log + swallow instead of
        // mapping to STATUS_UNSUCCESSFUL.
        swallow_panic(
            "close",
            AssertUnwindSafe(|| {
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
            }),
        );
    }

    fn get_file_info(&self, context: &Self::FileContext, file_info: &mut FileInfo) -> Result<()> {
        // Issue #314: panic safety wrapper — see catch_panic.
        catch_panic(
            "get_file_info",
            AssertUnwindSafe(|| {
                // Issue #310: per-adapter TTL cache. The
                // WinFSP kernel issues many concurrent
                // `IRP_MJ_QUERY_INFORMATION` IRPs for the
                // same file during a single Explorer Refresh
                // (size / mtime / attr lookups). Each one
                // falling through to `inner.getattr(ino)`
                // hits the backend. A 100 ms TTL coalesces
                // the burst; the next miss after the burst
                // returns a fresh value.
                let ino = context.ino;
                let now = Instant::now();
                let cached: Option<CoreFileAttr> = {
                    let cache = self.getattr_cache.lock().unwrap_or_else(|e| e.into_inner());
                    cache.get(&ino).and_then(|(ts, attr)| {
                        if now.duration_since(*ts) < GETATTR_CACHE_TTL {
                            Some(*attr)
                        } else {
                            None
                        }
                    })
                };
                let attr = match cached {
                    Some(attr) => {
                        tracing::trace!(ino, "winfsp::get_file_info: cache hit");
                        attr
                    }
                    None => {
                        let attr = self.inner.getattr(ino).map_err(io_err_to_status)?;
                        // Best-effort trim: if the map
                        // grows past a few thousand entries
                        // (very long-running mount with
                        // constant inode churn), drop the
                        // stale entries. The TTL above
                        // would also clean entries on the
                        // next miss, but a hard cap is
                        // cheaper than unbounded memory.
                        let mut cache =
                            self.getattr_cache.lock().unwrap_or_else(|e| e.into_inner());
                        if cache.len() > 4096 {
                            let cutoff = now.checked_sub(GETATTR_CACHE_TTL).unwrap_or(now);
                            cache.retain(|_, (ts, _)| *ts > cutoff);
                        }
                        cache.insert(ino, (now, attr));
                        tracing::trace!(
                            ino,
                            size = attr.size,
                            "winfsp::get_file_info: cache miss; populated"
                        );
                        attr
                    }
                };
                core_attr_to_file_info(&attr, file_info);
                Ok(())
            }),
        )
    }

    fn read(&self, context: &Self::FileContext, buffer: &mut [u8], offset: u64) -> Result<u32> {
        // Issue #314: panic safety wrapper — see catch_panic.
        catch_panic(
            "read",
            AssertUnwindSafe(|| {
                tracing::trace!(
                    ino = context.ino,
                    buffer_len = buffer.len(),
                    offset,
                    "winfsp::read: entered"
                );
                // Issue #302: WinFSP's default `VolumeParams`
                // sets `AlwaysUseDoubleBuffering=1`, which
                // caps the per-IRP read buffer at
                // `FSP_FSCTL_TRANSACT_BATCH_BUFFER_SIZEMIN`
                // (64 KiB) — see winfsp-sys fsctl.h. The
                // Windows kernel driver still issues multiple
                // 64 KiB IRPs for a 2 MiB ReadFile, but our
                // adapter sees each IRP separately and must
                // return <= buffer.len() bytes per call.
                // Pre-fix the code asked inner.read for the
                // full buffer length in one shot, then
                // returned whatever the backend gave back —
                // which on the opendal memory backend capped
                // at 64 KiB, so a 2 MiB `std::fs::read` on a
                // 2 MiB file returned only the first 64 KiB.
                //
                // Fix: loop, asking inner for the remaining
                // bytes at increasing offsets, until the
                // buffer is full or the backend returns
                // short (EOF). The short read signals EOF
                // exactly when the kernel expects it, so
                // `std::fs::read` (which loops on ReadFile
                // until 0 bytes) terminates correctly. Each
                // inner.read is bounded by `remaining` so
                // the backend can return whatever it
                // physically has without truncating our
                // state.
                let mut written = 0usize;
                let mut cur_offset = offset;
                while written < buffer.len() {
                    let remaining = (buffer.len() - written) as u32;
                    let data = self
                        .inner
                        .read(context.ino, context.fh, cur_offset, remaining)
                        .map_err(io_err_to_status)?;
                    let n = data.len();
                    if n == 0 {
                        // EOF: kernel will see 0 bytes
                        // returned and stop the ReadFile
                        // loop in std::fs::read.
                        break;
                    }
                    // data.len() may exceed remaining if
                    // the backend ignores the size hint
                    // (e.g. returns the whole file);
                    // truncate to the buffer slot we have
                    // left.
                    let copy = n.min(remaining as usize);
                    buffer[written..written + copy].copy_from_slice(&data[..copy]);
                    written += copy;
                    cur_offset += copy as u64;
                    // Backend returned less than asked for:
                    // treat as EOF so we don't loop forever
                    // on a backend that always returns one
                    // chunk regardless of `size`.
                    if n < remaining as usize {
                        break;
                    }
                }
                tracing::trace!(
                    ino = context.ino,
                    buffer_len = buffer.len(),
                    written,
                    offset,
                    "winfsp::read: returning"
                );
                Ok(written as u32)
            }),
        )
    }

    fn write(
        &self,
        context: &Self::FileContext,
        buffer: &[u8],
        offset: u64,
        _write_to_eof: bool,
        _constrained_io: bool,
        file_info: &mut FileInfo,
    ) -> Result<u32> {
        // Issue #314: panic safety wrapper — see catch_panic.
        catch_panic(
            "write",
            AssertUnwindSafe(|| {
                let written = self
                    .inner
                    .write(context.ino, context.fh, offset, buffer)
                    .map_err(io_err_to_status)?;
                // Issue #332: WinFSP contract — the write
                // callback MUST populate `file_info` (at
                // minimum `file_size`) before returning. The
                // kernel reads the new file size from this
                // buffer to update its FCB; if the field
                // stays at 0 the kernel treats the IRP as
                // failed and the user-mode `WriteFile` /
                // `Set-Content` / `Out-File` / `echo >` all
                // hang forever at the close side. Pre-fix
                // we returned Ok(written) without touching
                // file_info — explaining why New-Item
                // (no write) succeeded but every API that
                // actually wrote bytes hung.
                let post_attr = self.inner.getattr(context.ino).map_err(io_err_to_status)?;
                core_attr_to_file_info(&post_attr, file_info);
                Ok(written)
            }),
        )
    }

    fn rename(
        &self,
        _context: &Self::FileContext,
        file_name: &U16CStr,
        new_file_name: &U16CStr,
        _replace_if_exists: bool,
    ) -> Result<()> {
        // Issue #314: panic safety wrapper — see catch_panic.
        catch_panic(
            "rename",
            AssertUnwindSafe(|| {
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
                // Issue #307: NFC-normalize both names so a macOS
                // FUSE upload (NFD) and a WinFSP rename (NFC) hit
                // the same backend key.
                let src_full = crate::util::nfc(&file_name.to_string_lossy()).replace('\\', "/");
                let dst_full =
                    crate::util::nfc(&new_file_name.to_string_lossy()).replace('\\', "/");
                // Issue #78 regression: WinFSP normalizes
                // paths through Windows' case-insensitive
                // layer, so the callback receives the
                // canonical-case path from the directory
                // entry (e.g. `OLD.TXT` even when the user
                // wrote `old.txt`). opendal backends are
                // case-sensitive on the path string, so
                // opendal memory would NotFound a lookup
                // for `OLD.TXT` when the data was stored as
                // `old.txt`. Lower-case here so the backend
                // op hits the same key the test originally
                // wrote. The mount itself is a case-
                // insensitive view (Windows is), so
                // lowercasing can't lose data — at worst
                // it's a no-op for an already-lowercase
                // path.
                #[cfg(windows)]
                let (src_full, dst_full) = (src_full.to_lowercase(), dst_full.to_lowercase());
                // Issue #78: WinFSP can fire rename on a path
                // whose parent directory was never `lookup`'d
                // (opendal-only files), so the inode cache has
                // no entry for the parent. parent_ino_for would
                // fall back to ino=1 and `lib.rs::rename` would
                // see `parent_path=""`, producing
                // `op.rename("name", "newname")` at the wrong
                // level — the real src is `/subdir/name`.
                //
                // Forward the full paths via the trait's
                // `rename_paths` (added in #78) which the
                // MntrsFs impl overrides to talk to opendal
                // directly. The FUSE kernel only invokes
                // `rename(parent, name, ...)` after a prior
                // successful lookup, so the inode cache is
                // always populated on that path — keeping the
                // old `(parent, name)` derivation is correct on
                // FUSE and the default impl on the trait
                // preserves that behaviour.
                self.inner
                    .rename_paths(&src_full, &dst_full)
                    .map_err(io_err_to_status)
            }),
        )
    }

    // Issue #298: pre-fix the FileSystemContext trait's
    // default no-op `cleanup` was inherited, so Win32
    // DeleteFile / rm dir on a WinFSP volume never reached
    // the backend — files accumulated forever on the
    // opendal side, and the in-memory cache thought the
    // file still existed until process exit. Now we
    // inspect the cleanup flags and dispatch to
    // `inner.unlink` / `inner.rmdir` when the kernel asks
    // for the delete.
    //
    // The WinFSP docs (and the trait default) are explicit:
    // "The file should never be deleted in this function
    // [set_delete]; instead, set a flag to indicate that
    // the file is to be deleted later by cleanup." We
    // follow that contract — `set_delete` is a no-op
    // (the cleanup-time check below handles the actual
    // decision based on FspCleanupDelete), and `cleanup`
    // performs the backend delete.
    //
    // Idempotency: the kernel may call cleanup multiple
    // times for the same handle (e.g. after a failed
    // IRP_MJ_SET_INFORMATION that requested
    // FILE_DELETE_ON_CLOSE). A second cleanup sees the
    // file already gone in the backend — that's a
    // NotFound on inner.unlink, which we map to Ok
    // (the desired outcome from the kernel's POV: the
    // file no longer exists).
    //
    // Error mapping: cleanup returns () (the trait
    // signature is `fn cleanup(...) {}` with no Result).
    // Errors can only be surfaced via tracing::warn.
    // The kernel treats a cleanup that doesn't panic
    // as success — STATUS_UNSUCCESSFUL would have to go
    // through a panic, which we want to avoid for the
    // cleanup path. Worst case is a leaked backend file
    // that the user can `mntrs unmount` and re-create
    // to clean up.
    fn cleanup(&self, context: &Self::FileContext, file_name: Option<&U16CStr>, flags: u32) {
        // Issue #314: panic safety wrapper — `cleanup`
        // returns `()` (no Result), so we log + swallow
        // panics instead of mapping to STATUS_UNSUCCESSFUL.
        swallow_panic(
            "cleanup",
            AssertUnwindSafe(|| {
                // Issue #310: a delete-imminent cleanup
                // must invalidate the per-adapter
                // `get_file_info` cache. The kernel's
                // next `get_file_info` for the same ino
                // (e.g. an immediately-following
                // `std::fs::read` to confirm the delete
                // took effect) would otherwise hit the
                // 100 ms TTL entry and report the file
                // as still present, masking the delete
                // for up to 100 ms.
                if FspCleanupFlags::FspCleanupDelete.is_flagged(flags) {
                    self.invalidate_getattr_cache_ino(context.ino);
                }
                // FspCleanupDelete = 0x01 — kernel asked
                // us to actually remove the file. Without
                // this flag, cleanup is just "last handle
                // closed" and no backend action is needed.
                if !FspCleanupFlags::FspCleanupDelete.is_flagged(flags) {
                    tracing::trace!(
                        ino = context.ino,
                        is_dir = context.is_dir,
                        flags,
                        "winfsp::cleanup: no FspCleanupDelete, skipping backend unlink"
                    );
                    return;
                }
                // file_name is None on volume-cleanup
                // (rare; only seen on unmount), which
                // doesn't carry a per-file target.
                let Some(file_name) = file_name else {
                    tracing::trace!(
                        ino = context.ino,
                        "winfsp::cleanup: no file_name (volume cleanup), skipping"
                    );
                    return;
                };
                // Issue #307: NFC-normalize the kernel-supplied
                // name (see get_security_by_name for rationale).
                let full_path = crate::util::nfc(&file_name.to_string_lossy()).replace('\\', "/");
                // Split into (parent_path, basename).
                // WinFSP paths look like "/dir/file.txt"
                // for non-root or "/file.txt" for root.
                // The basename is what unlink/rmdir want.
                let (parent_path, basename) = match full_path.rsplit_once('/') {
                    Some((parent, name)) if !name.is_empty() => {
                        (parent.to_string(), name.to_string())
                    }
                    _ => ("/".to_string(), full_path.clone()),
                };
                let parent_ino = self.parent_ino_for(&parent_path).unwrap_or(1);
                tracing::debug!(
                    ino = context.ino,
                    is_dir = context.is_dir,
                    parent_ino,
                    basename = %basename,
                    "winfsp::cleanup: dispatching backend delete"
                );
                let result = if context.is_dir {
                    self.inner.rmdir(parent_ino, &basename)
                } else {
                    self.inner.unlink(parent_ino, &basename)
                };
                // Map NotFound to Ok for idempotency (see
                // doc comment above). All other errors
                // log a warning — we can't surface them
                // to the kernel because cleanup returns
                // ().
                match result {
                    Ok(()) => tracing::debug!(
                        ino = context.ino,
                        basename = %basename,
                        "winfsp::cleanup: backend delete ok"
                    ),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        tracing::debug!(
                            ino = context.ino,
                            basename = %basename,
                            "winfsp::cleanup: backend already absent (idempotent)"
                        );
                    }
                    Err(e) => tracing::warn!(
                        ino = context.ino,
                        basename = %basename,
                        error = %e,
                        "winfsp::cleanup: backend delete failed (file leaked at backend)"
                    ),
                }
            }),
        );
    }

    // Issue #298: set_delete is intentionally a no-op.
    // The actual delete decision is made in `cleanup`
    // based on FspCleanupDelete. This matches the
    // WinFSP doc guidance ("set a flag in set_delete;
    // act on it in cleanup") — except the "flag" we
    // honour is the kernel-side one WinFSP passes
    // through in the cleanup flags bitfield, not a
    // per-handle struct field. Implementing set_delete
    // would require either per-handle state in
    // WinFspHandle or a side channel that complicates
    // the close/release ordering; the cleanup-time
    // check is the standard idiom for stateless
    // FUSE/WinFSP adapters.
    fn set_delete(
        &self,
        _context: &Self::FileContext,
        _file_name: &U16CStr,
        _delete_file: bool,
    ) -> Result<()> {
        // No-op: see doc comment above. The decision is
        // made in `cleanup` via FspCleanupDelete.
        Ok(())
    }

    fn set_basic_info(
        &self,
        context: &Self::FileContext,
        file_attributes: u32,
        creation_time: u64,
        last_access_time: u64,
        last_write_time: u64,
        change_time: u64,
        file_info: &mut FileInfo,
    ) -> Result<()> {
        // Issue #314: panic safety wrapper — see catch_panic.
        catch_panic(
            "set_basic_info",
            AssertUnwindSafe(|| {
                // Issue #305 Tier 1: pre-fix this was a no-op, so
                // Explorer Properties → Modified Date, robocopy /MIR, and
                // PowerShell Set-LastWriteTime silently failed (kernel cache
                // showed the new value but the in-memory + backend attr
                // stayed at the old value). Forward to inner.setattr with
                // atime + mtime so MntrsFs updates its inodes map; populate
                // the output FileInfo so WinFSP's per-handle cache also
                // reflects the change.
                //
                // WinFSP passes Win32 FILETIME values (100-nanosecond
                // intervals since 1601-01-01 UTC). u64 == 0 means "leave
                // unchanged" per the Win32 contract; pass None in that case
                // so setattr doesn't overwrite a valid timestamp with epoch.
                let atime = filetime_u64_to_system_time(last_access_time);
                let mtime = filetime_u64_to_system_time(last_write_time);
                let crtime = filetime_u64_to_system_time(creation_time);
                let ctime = filetime_u64_to_system_time(change_time);

                // Forward mtime + atime to the trait. We ignore crtime /
                // ctime here because the CoreFilesystem::setattr trait only
                // exposes atime + mtime; creation time is set at create()
                // and change time is kernel-tracked. If both atime and mtime
                // are None (kernel sent two 0 FILETIMEs), short-circuit —
                // there's nothing to write back, but we still need to
                // populate file_info with current state.
                let attr = self
                    .inner
                    .setattr(
                        context.ino,
                        None,
                        None,
                        None,
                        None,
                        atime,
                        mtime,
                        Some(context.fh),
                    )
                    .map_err(io_err_to_status)?;

                // Populate the output FileInfo. file_attributes, the four
                // timestamps, and file_size are what WinFSP caches per
                // handle / per FCB — these determine what subsequent
                // GetFileInformationByHandle / FindFirstFile return without
                // a roundtrip to the adapter.
                let _ = file_attributes; // accepted but not echoed back;
                // future #310 will thread
                // file_attributes through statfs /
                // getattr for full round-trip.
                let _ = (creation_time, change_time); // see comment above;
                // atime + mtime only
                // via the trait today.
                file_info.file_attributes = file_attributes;
                file_info.creation_time = creation_time;
                file_info.last_access_time = last_access_time;
                file_info.last_write_time = last_write_time;
                file_info.change_time = change_time;
                // File size comes from the post-setattr attr (mirrors
                // getattr's value). Allocation_size is rounded up to the
                // 4096-byte sector the VolumeParams uses.
                file_info.file_size = attr.size;
                file_info.allocation_size = attr.size.div_ceil(4096) * 4096;
                file_info.index_number = context.ino;
                file_info.hard_links = 0;
                file_info.ea_size = 0;
                file_info.reparse_tag = 0;

                // Touch crtime / ctime so unused warnings stay quiet on the
                // surface even when we don't thread them into setattr yet
                // (kept for the #310 follow-up).
                let _ = (crtime, ctime);

                Ok(())
            }),
        )
    }

    fn set_file_size(
        &self,
        context: &Self::FileContext,
        new_size: u64,
        _set_allocation_size: bool,
        _file_info: &mut FileInfo,
    ) -> Result<()> {
        // Issue #314: panic safety wrapper — see catch_panic.
        catch_panic(
            "set_file_size",
            AssertUnwindSafe(|| {
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
            }),
        )
    }

    fn get_volume_info(&self, out_volume_info: &mut VolumeInfo) -> Result<()> {
        // Issue #314: panic safety wrapper — see catch_panic.
        catch_panic(
            "get_volume_info",
            AssertUnwindSafe(|| {
                // Issue #310: per-adapter TTL cache. Explorer
                // calls `get_volume_info` on every Refresh
                // and on every Properties dialog — for S3
                // backends that's 200 ms+ per call. 30 s TTL
                // keeps the visible flow snappy while still
                // letting an operator see a resize within
                // 30 s. The cache holds a single entry (the
                // most recent stat) — no growth concern.
                let now = Instant::now();
                let cached: Option<CoreVolumeStat> = {
                    let cache = self
                        .volume_info_cache
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    cache.as_ref().and_then(|(ts, v)| {
                        if now.duration_since(*ts) < VOLUME_INFO_CACHE_TTL {
                            Some(v.clone())
                        } else {
                            None
                        }
                    })
                };
                let v = match cached {
                    Some(v) => {
                        tracing::trace!("winfsp::get_volume_info: cache hit");
                        v
                    }
                    None => {
                        let v = self.inner.statfs(1).map_err(io_err_to_status)?;
                        *self
                            .volume_info_cache
                            .lock()
                            .unwrap_or_else(|e| e.into_inner()) = Some((now, v.clone()));
                        tracing::trace!("winfsp::get_volume_info: cache miss; populated");
                        v
                    }
                };
                core_volume_to_volume_info(&v, out_volume_info);
                tracing::debug!(
                    total_size = out_volume_info.total_size,
                    free_size = out_volume_info.free_size,
                    "winfsp::get_volume_info: ok"
                );
                Ok(())
            }),
        )
    }

    fn get_security(
        &self,
        _context: &Self::FileContext,
        _security_descriptor: Option<&mut [c_void]>,
    ) -> Result<u64> {
        // Issue #314: panic safety wrapper — see catch_panic.
        // Body is a one-line error return; wrapping it costs
        // nothing and keeps the panic-safety contract uniform
        // across the adapter's public surface.
        catch_panic(
            "get_security",
            AssertUnwindSafe(|| Err(STATUS_INVALID_DEVICE_REQUEST.into())),
        )
    }

    fn set_security(
        &self,
        _context: &Self::FileContext,
        _security_information: u32,
        _modification_descriptor: ModificationDescriptor,
    ) -> Result<()> {
        // Issue #314: panic safety wrapper — see catch_panic.
        catch_panic(
            "set_security",
            AssertUnwindSafe(|| Err(STATUS_INVALID_DEVICE_REQUEST.into())),
        )
    }

    fn flush(&self, context: Option<&Self::FileContext>, _file_info: &mut FileInfo) -> Result<()> {
        // Issue #314: panic safety wrapper — see catch_panic.
        catch_panic(
            "flush",
            AssertUnwindSafe(|| {
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
                    //
                    // Issue #310: a read-only handle has no
                    // `cache_fd` in the trait, so `fsync`
                    // returns `NotFound` to signal
                    // "nothing to flush". That is a
                    // semantic no-op for FlushFileBuffers
                    // (the user didn't write anything
                    // through this handle), not an error.
                    // Surface it as a successful no-op
                    // instead of STATUS_NOT_FOUND, which
                    // would make Win32 FlushFileBuffers
                    // fail for any file opened read-only.
                    if let Err(e) = self.inner.fsync(ctx.ino, ctx.fh, true) {
                        if e.kind() == std::io::ErrorKind::NotFound {
                            tracing::trace!(
                                ino = ctx.ino,
                                fh = ctx.fh,
                                "winfsp::flush: fsync returned NotFound (read-only \
                                 handle, no cache fd); treating as no-op"
                            );
                        } else {
                            return Err(io_err_to_status(e));
                        }
                    }
                    self.inner
                        .flush(ctx.ino, ctx.fh)
                        .map_err(io_err_to_status)?;
                }
                Ok(())
            }),
        )
    }

    fn get_dir_info_by_name(
        &self,
        _context: &Self::FileContext,
        _file_name: &U16CStr,
        _out_dir_info: &mut DirInfo,
    ) -> Result<()> {
        // Issue #314: panic safety wrapper — see catch_panic.
        // Called during read_directory pattern matching (only when
        // VolumeParams::pass_query_directory_filename is enabled).
        catch_panic(
            "get_dir_info_by_name",
            AssertUnwindSafe(|| Err(STATUS_INVALID_DEVICE_REQUEST.into())),
        )
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
        // Issue #314: panic safety wrapper — see catch_panic.
        catch_panic(
            "read_directory",
            AssertUnwindSafe(|| {
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
                    tracing::debug!(
                        "winfsp::read_directory: not a dir, returning INVALID_DEVICE_REQUEST"
                    );
                    return Err(STATUS_INVALID_DEVICE_REQUEST.into());
                }

                // Issue #306: readdir_with_attrs serves entries
                // + attrs in one call, sliced by marker against
                // the per-fh snapshot pinned by opendir. Two
                // perf wins:
                //   * No backend RTT per entry — attrs come
                //     from the dir_cache snapshot that opendir
                //     populated via list_op.
                //   * O(page-size) per call instead of O(N) —
                //     partition_point slices the snapshot by
                //     marker instead of re-materialising the
                //     full Vec on each WinFSP page.
                //
                // Convert the WinFSP marker (null-terminated
                // UTF-16) to a String once. `inner_as_cstr()`
                // returns `Option<&U16CStr>` (None on the
                // first call where there's no marker).
                let marker_str = marker
                    .inner_as_cstr()
                    .map(|m| m.to_string_lossy())
                    .unwrap_or_default();
                let entries_with_attrs = self
                    .inner
                    .readdir_with_attrs(context.ino, context.dir_fh, &marker_str)
                    .map_err(io_err_to_status)?;
                tracing::debug!(
                    ino = context.ino,
                    dir_fh = context.dir_fh,
                    marker = %marker_str,
                    entry_count = entries_with_attrs.len(),
                    "winfsp::read_directory: got entries"
                );

                let mut cursor: u32 = 0;
                for (entry, attr) in &entries_with_attrs {
                    // Pattern filter (simple substring /
                    // wildcard). Skip for now if pattern
                    // present and doesn't match — `*.txt`
                    // becomes "ends with .txt"; any other
                    // wildcard is treated as no-match
                    // (Win32's dir *.txt is the common case).
                    if let Some(pat) = pattern {
                        let pat_str = pat.to_string_lossy();
                        if !match_wildcard(&pat_str, &entry.name, /* case_insensitive */ true) {
                            continue;
                        }
                    }

                    let name_u16: Vec<u16> = entry
                        .name
                        .encode_utf16()
                        .chain(std::iter::once(0))
                        .collect();

                    // Build the high-level DirInfo<255> wrapper
                    // (size, FileInfo, padding, file_name).
                    // `core_attr_to_file_info` now copies the
                    // entry's REAL size + mtime from `attr`
                    // (pre-fix it zeroed both, which made
                    // Explorer show "0 bytes / unknown date"
                    // until each entry was individually
                    // clicked). The wrapper handles the
                    // trailing-NUL bookkeeping and computes
                    // Size correctly so
                    // FspFileSystemAddDirInfo's overflow
                    // check works.
                    let mut di = DirInfo::<255>::new();
                    core_attr_to_file_info(attr, di.file_info_mut());
                    // set_name_raw takes a &[u16] WITHOUT
                    // trailing NUL and calls set_size() with
                    // byte_len. Use it instead of poking
                    // name_buffer / set_size directly (those
                    // are on the private WideNameInfoInternal
                    // trait). Returns FspError directly —
                    // propagate with `?` (the function already
                    // returns winfsp::Result).
                    di.set_name_raw(name_u16.as_slice())?;

                    // Try to add this entry.
                    // FspFileSystemAddDirInfo returns FALSE
                    // when the buffer can't fit another entry
                    // — we stop and report what we packed so
                    // far (WinFSP will call back with the
                    // same marker to fetch the next page).
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
            }),
        )
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

// Issue #314: panic-safety unit tests for the
// catch_panic / swallow_panic helpers. These are
// intentionally NOT gated on a feature — they live in the
// `tests` module of a `#[cfg(windows)]` file so they
// automatically compile only when targeting Windows (which
// is also the only platform the helpers are reachable
// from, since the FileSystemContext impl on
// WinFspAdapter is also #[cfg(windows)]).
#[cfg(test)]
mod tests {
    use super::*;
    use winfsp::FspError;

    /// catch_panic must map a panic in the callback body to
    /// STATUS_UNSUCCESSFUL (0xC0000001) instead of letting
    /// the unwind cross the FFI boundary. The kernel sees
    /// this as a regular IO error and can keep servicing
    /// subsequent requests.
    #[test]
    fn catch_panic_converts_panic_to_status_unsuccessful() {
        let result = catch_panic("test_method", || -> Result<()> {
            panic!("intentional test panic");
        });
        // NTSTATUS is a transparent newtype around i32;
        // the kernel-visible payload in the FspError is
        // the raw `i32`. Compare against the const's raw
        // value via a match guard (constant expressions
        // aren't allowed directly in patterns).
        match result {
            Err(FspError::NTSTATUS(nt)) if nt == STATUS_UNSUCCESSFUL.0 => {}
            other => panic!("expected NTSTATUS(STATUS_UNSUCCESSFUL), got {:?}", other),
        }
    }

    /// catch_panic must propagate a normal `Err` return
    /// unchanged — it must not swallow or transform
    /// application-level errors. This guards against an
    /// over-eager wrap that loses error information.
    #[test]
    fn catch_panic_propagates_normal_error() {
        let result: Result<()> =
            catch_panic("test_method", || Err(STATUS_INVALID_DEVICE_REQUEST.into()));
        match result {
            Err(FspError::NTSTATUS(nt)) if nt == STATUS_INVALID_DEVICE_REQUEST.0 => {}
            other => panic!(
                "expected NTSTATUS(STATUS_INVALID_DEVICE_REQUEST), got {:?}",
                other
            ),
        }
    }

    /// catch_panic must pass through a normal `Ok` return
    /// unchanged. The closure's return value must reach the
    /// caller verbatim.
    #[test]
    fn catch_panic_propagates_ok() {
        let result: Result<u32> = catch_panic("test_method", || Ok(42));
        assert_eq!(result.unwrap(), 42);
    }

    /// catch_panic must extract `&'static str` and `String`
    /// payloads from the panic — these are the common cases
    /// from `panic!("foo")` and `panic!("{}", x)` / `unwrap`
    /// on a non-string Err. Custom payload types fall back
    /// to a placeholder string but must NOT crash.
    #[test]
    fn catch_panic_extracts_str_and_string_payloads() {
        // &'static str payload
        let _ = catch_panic("t", || -> Result<()> { panic!("hello") });

        // String payload (simulated via unwrap on Err(String))
        let s = String::from("world");
        let _ = catch_panic("t", || -> Result<()> {
            let _ = s.parse::<i32>().unwrap();
            Ok(())
        });
    }

    /// swallow_panic must absorb a panic without panicking
    /// out of itself — the WinFSP dispatcher expects close()
    /// to return `()` always. A second panic from the panic
    /// handler would itself be UB.
    #[test]
    fn swallow_panic_absorbs_panic_without_propagating() {
        // No assertion — the test is that this returns at
        // all. If swallow_panic didn't catch, the test
        // runner would abort.
        swallow_panic("close_test", || {
            panic!("intentional test panic in close path");
        });
    }

    /// panic_message helper must produce a string for
    /// &'static str, String, and unknown payloads. The
    /// WinFSP error log uses this for diagnostics; an
    /// empty or panic-prone message would defeat the
    /// purpose of the whole #314 wrap.
    #[test]
    fn panic_message_handles_all_payload_types() {
        // Build payloads by catching real panics — this is
        // the only way to construct a `Box<dyn Any + Send>`
        // from inside the test process.
        let static_str_payload = std::panic::catch_unwind(|| {
            panic!("static str payload");
        })
        .unwrap_err();
        let msg = panic_message(&static_str_payload);
        assert_eq!(msg, "static str payload");

        let string_payload = std::panic::catch_unwind(|| {
            panic!("{}", "owned string payload");
        })
        .unwrap_err();
        let msg = panic_message(&string_payload);
        assert_eq!(msg, "owned string payload");

        // Custom payload type falls back to the placeholder.
        let custom_payload: Box<dyn std::any::Any + Send> = Box::new(42i32);
        let msg = panic_message(&custom_payload);
        assert_eq!(msg, "<non-string panic payload>");
    }
}
