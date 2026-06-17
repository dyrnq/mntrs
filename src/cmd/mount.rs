#![allow(clippy::type_complexity)]
use crate::MntrsFs;
use anyhow::{Result, anyhow};
#[cfg(not(windows))]
use fuser::MountOption;
use opendal::Operator;
use opendal::layers::{ConcurrentLimitLayer, RetryLayer, TimeoutLayer};
use opendal::services::{
    AliyunDrive, Azblob, B2, Cos, Fs, Gcs, HdfsNative, Memory, Obs, Oss, S3, VercelBlob, Webdav,
};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
#[cfg(not(windows))]
use std::os::fd::AsRawFd;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::OnceLock;

fn rt_block_on<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    // Originally: independent OnceCell<Runtime> (see below).
    // Cross-runtime .await on an Operator's HTTP client deadlocks.
    crate::rt().block_on(f)
    // Original code:
    // static RT: OnceCell<tokio::runtime::Runtime> = OnceCell::new();
    // let rt = RT.get_or_init(|| tokio::runtime::Runtime::new().expect("tokio rt"));
    // rt.block_on(f)
}

fn mounts_db() -> String {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    format!("{}/.local/share/mntrs/mounts.txt", home)
}

pub struct MountInfo {
    pub storage: String,
    pub mountpoint: String,
    pub pid: String,
    pub user: String,
    pub read_only: bool,
    pub backend: String,
}

pub fn read_mounts() -> Vec<MountInfo> {
    let path = mounts_db();
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(_) => return vec![],
    };
    let reader = BufReader::new(file);
    reader
        .lines()
        .map_while(Result::ok)
        .filter_map(|l| {
            let parts: Vec<&str> = l.split('\0').collect();
            if parts.len() < 6 {
                return None;
            }
            // Bug 23: filter rows with empty critical
            // fields. A malformed line (e.g. a partial
            // write from a crashed `record_mount`) can
            // have all 6 \0 separators but an empty
            // storage/mountpoint/pid string, which would
            // surface in `list` as a blank table row.
            // pid is the most useful diagnostic — empty
            // pid means the writer crashed before
            // capturing std::process::id(), and the row
            // can't be acted on by the user (no kill /
            // unmount). Drop the row, log at debug
            // since this is an opportunistic recovery
            // path not a hot loop.
            if parts[2].is_empty() || parts[0].is_empty() || parts[1].is_empty() {
                tracing::debug!(line=%l, "read_mounts: skipping malformed entry (empty critical field)");
                return None;
            }
            Some(MountInfo {
                storage: parts[0].to_string(),
                mountpoint: parts[1].to_string(),
                pid: parts[2].to_string(),
                user: parts[3].to_string(),
                read_only: parts[4] == "ro",
                backend: parts[5].to_string(),
            })
        })
        .collect()
}

fn record_mount(storage: &str, mountpoint: &str, read_only: bool) {
    let path = mounts_db();
    // Bug 26: bare .unwrap() replaced with .expect() so a
    // contract-breaking panic carries an actionable
    // message. mounts_db() returns
    // "{HOME}/.local/share/mntrs/mounts.txt" — the path
    // always contains at least one '/' separator, so
    // parent() can only return None if a future
    // refactor changes mounts_db() to a single-segment
    // relative path. The expect catches that case
    // loudly instead of crashing with "called unwrap on
    // None".
    let dir = std::path::Path::new(&path)
        .parent()
        .expect("BUG: mounts_db() path must have a parent directory");
    let _ = std::fs::create_dir_all(dir);
    // Atomically rewrite: tmp + rename (POSIX atomic)
    let tmp = format!("{}.tmp.{}", path, std::process::id());
    let mut lines = Vec::new();
    if let Ok(existing) = std::fs::read_to_string(&path) {
        for l in existing.lines() {
            if l.split('\0').nth(1) != Some(mountpoint) {
                lines.push(l.to_string());
            }
        }
    }
    let pid = std::process::id().to_string();
    let user = std::env::var("USER").unwrap_or_else(|_| "?".into());
    let ro = if read_only { "ro" } else { "rw" };
    let backend = storage.split(':').next().unwrap_or("?");
    lines.insert(
        0,
        format!(
            "{}\0{}\0{}\0{}\0{}\0{}",
            storage, mountpoint, pid, user, ro, backend
        ),
    );
    let content = lines.join("\n") + "\n";
    if std::fs::write(&tmp, &content).is_ok() {
        let _ = std::fs::rename(&tmp, &path);
    }
}

fn remove_mount(mountpoint: &str) {
    let path = mounts_db();
    // Bug 31: mirror record_mount's tmp + rename pattern
    // so the rewrite is atomic. Pre-fix this used
    // read_to_string + filter + write — two non-atomic
    // syscalls. If a concurrent record_mount rename
    // landed between our read and write, our write
    // would overwrite the freshly-recorded entry and
    // it would silently disappear from the list.
    //
    // Race window: record_mount runs in mount setup,
    // remove_mount in unmount/cleanup. Different
    // lifecycle phases, low real-world probability —
    // but two mounts started concurrently (e.g. an
    // automation script kicking off N mounts in
    // parallel) where one finishes + unmounts while
    // another is starting can hit it. Atomic rename
    // closes the window for free.
    let content = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(_) => return,
    };
    let filtered: Vec<&str> = content
        .lines()
        .filter(|l| l.split('\0').nth(1) != Some(mountpoint))
        .collect();
    let new_body = filtered.join("\n");
    let tmp = format!("{}.tmp.{}", path, std::process::id());
    if std::fs::write(&tmp, &new_body).is_ok() {
        if let Err(e) = std::fs::rename(&tmp, &path) {
            tracing::debug!(error=%e, "mounts db rename failed");
            // Rename failed — clean the tmp so it
            // doesn't accumulate. Ignore the unlink
            // result; worst case is a leftover
            // {path}.tmp.{pid} which the next
            // remove_mount will overwrite.
            let _ = std::fs::remove_file(&tmp);
        }
    } else {
        tracing::debug!("mounts db tmp write failed");
    }
}

static CLEANUP_MP: OnceLock<String> = OnceLock::new();
static SHUTDOWN_REQUESTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
/// Distinguishes "shutdown was triggered by SIGINT/SIGTERM" (the
/// `handler` signal fn) from "shutdown was triggered by the FUSE
/// session ending" (external `fusermount3 -u`, `umount_and_join`
/// from `unmount_internal`, etc.). The fuse-signal-watcher thread
/// only needs to spawn its own `fusermount3 -u` child in the first
/// case; in the second case the mount is already gone and spawning
/// a redundant child would orphan it on parent exit and leak 2
/// pipe FDs + 1 devnull fd per cycle (lifecycle_stress catches
/// this as a real 12-15 fd/post-unmount retention per mount).
static SHUTDOWN_BY_SIGNAL: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Active FUSE BackgroundSession. Stored by mount() so that
/// unmount_internal() can gracefully shut down the FUSE daemon thread
/// (umount_and_join) instead of leaking it via std::mem::forget.
#[cfg(not(windows))]
static FUSE_SESSION: std::sync::Mutex<Option<fuser::BackgroundSession>> =
    std::sync::Mutex::new(None);

extern "C" fn cleanup() {
    if let Some(mp) = CLEANUP_MP.get() {
        // Issue #24: skip the fusermount3 spawn if the
        // signal watcher already did it (SHUTDOWN_BY_SIGNAL
        // was set by the signal handler before the watcher
        // ran). Pre-fix both paths fired fusermount3 in the
        // signal-exit flow — second one was a no-op
        // ("device not mounted") but produced a confusing
        // error in trace logs. The remove_mount cleanup
        // below still runs unconditionally — it's an
        // in-process bookkeeping update, not an external
        // side effect.
        if !SHUTDOWN_BY_SIGNAL.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = Command::new("fusermount3")
                .arg("-u")
                .arg(mp)
                .status()
                .or_else(|_| Command::new("fusermount").arg("-u").arg(mp).status());
        }
        remove_mount(mp);
    }
}

/// Simplified mount entry point for CSI plugin.
/// Uses defaults for all the FUSE tuning parameters.
/// Check if a path is already a mount point by checking /proc/mounts.
#[cfg(windows)]
pub fn is_mount_point(_path: &str) -> bool {
    // On Windows, WinFSP handles mount point detection internally
    false
}

///
/// Check if a path is already a mount point on macOS.
#[cfg(target_os = "macos")]
pub fn is_mount_point(path: &str) -> bool {
    use std::process::Command;
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| std::path::PathBuf::from(path));
    let output = Command::new("mount")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .unwrap_or_default();
    output.lines().any(|line| {
        let fields: Vec<&str> = line.split_whitespace().collect();
        fields.len() >= 3 && fields[2] == canonical.to_string_lossy().as_ref()
    })
}

#[cfg(target_os = "linux")]
pub fn is_mount_point(path: &str) -> bool {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| std::path::PathBuf::from(path));
    let canonical_str = canonical.to_string_lossy();
    if let Ok(content) = std::fs::read_to_string("/proc/mounts") {
        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 && parts[1] == canonical_str.as_ref() {
                return true;
            }
        }
    }
    false
}

/// Simplified mount entry point for CSI plugin.
/// Returns Ok(()) if already mounted (idempotent).
pub fn mount_internal(
    storage_url: &str,
    mountpoint: &str,
    opts: &std::collections::HashMap<String, String>,
    read_only: bool,
) -> anyhow::Result<()> {
    // Isolated cache dir per mount (CSI prevents disk leak across volumes)
    let cache_suffix = mountpoint.replace(['/', ':'], "_");
    let cache_dir = format!("/tmp/mntrs-csi-cache/{}", cache_suffix);
    let _ = std::fs::create_dir_all(&cache_dir);

    // Idempotency: if already mounted, return success
    if is_mount_point(mountpoint) {
        tracing::info!(mountpoint, "already mounted, skipping");
        return Ok(());
    }

    // Stale mount cleanup: unmount any leftover from previous crashes
    #[cfg(target_os = "linux")]
    {
        let result = std::process::Command::new("fusermount3")
            .arg("-u")
            .arg("-z")
            .arg(mountpoint)
            .status()
            .or_else(|_| {
                std::process::Command::new("fusermount")
                    .arg("-u")
                    .arg("-z")
                    .arg(mountpoint)
                    .status()
            });
        if let Ok(status) = result {
            tracing::debug!(mountpoint, exit = ?status.code(), "stale mount cleanup");
        }
    }
    mount(
        storage_url,
        mountpoint,
        opts,
        read_only,
        false,                    // network_mode
        300,                      // dir_cache_time (5min)
        1,                        // attr_timeout
        10,                       // type_cache_ttl
        1,                        // stat_cache_ttl
        true,                     // allow_other (CSI: Pods access as non-root)
        false,                    // debug_fuse
        "mntrs-csi",              // volname
        None,                     // devname
        false,                    // write_back_cache
        &[],                      // fuse_options
        &[],                      // fuse_flags
        false,                    // daemon (no fork — std::thread::spawn holds session)
        false,                    // daemon_wait
        10,                       // daemon_timeout
        false,                    // allow_root
        false,                    // allow_idmap
        0,                        // vfs_cache_max_size (off)
        256,                      // mem_limit
        "dashmap",                // mem_cache_impl (default)
        0,                        // mem_cache_metrics_interval_secs (off)
        5,                        // vfs_write_back
        "off",                    // vfs_cache_mode
        0,                        // vfs_read_ahead (off)
        128 * 1024 * 1024,        // vfs_read_chunk_size (128MiB)
        false,                    // default_permissions
        None,                     // uid
        None,                     // gid
        None,                     // umask
        None,                     // dir_perms
        None,                     // file_perms
        None,                     // link_perms
        false,                    // allow_non_empty
        Some(cache_dir.as_str()), // cache_dir (CSI isolated)
        false,                    // direct_io
        60,                       // poll_interval
        3600,                     // vfs_cache_max_age
        0,                        // vfs_cache_min_free_space (off)
        vec![],                   // exclude
        vec![],                   // include
        None,                     // max_size
        None,                     // min_size
        None,                     // max_depth
        false,                    // ignore_case
        false,                    // no_modtime
        false,                    // use_server_modtime
        false,                    // no_checksum
        false,                    // no_seek
        false,                    // links
        false,                    // noapple_double
        false,                    // noapple_xattr,
        None,                     // hash_filter
        false,                    // mount_case_insensitive
        131072,                   // max_read_ahead
        0,                        // vfs_read_chunk_size_limit
        0,                        // vfs_read_chunk_streams (serial)
        16777216,                 // vfs_prefetch_threshold (16 MiB)
        64,                       // vfs_prefetch_queue_mb
        false,                    // vfs_fast_fingerprint
        false,                    // async_read
        false,                    // vfs_refresh
        false,                    // vfs_case_insensitive
        false,                    // no_implicit_dir
        false,                    // vfs_block_norm_dupes
        false,                    // vfs_links
        false,                    // vfs_used_is_size
        None,                     // vfs_metadata_extension
        None,                     // storage_class
        1,                        // vfs_write_wait (1s)
        1,                        // vfs_read_wait (1s)
        60,                       // vfs_cache_poll_interval
        0,                        // vfs_handle_caching
        0,                        // vfs_disk_space_total_size (off)
    )
}

/// Simplified unmount entry point for CSI plugin.
/// Unmount for CSI plugin.
/// Waits for writeback queue to drain (up to 5 min), then unmounts.
/// Falls back to lazy unmount if regular unmount fails.
fn cache_dir_for_mount(mountpoint: &str) -> String {
    let suffix = mountpoint.replace(['/', ':'], "_");
    format!("/tmp/mntrs-csi-cache/{}", suffix)
}

pub fn build_operator_sync(storage_url: &str, opts: &HashMap<String, String>) -> Result<Operator> {
    rt_block_on(build_operator(storage_url, opts))
}

pub fn unmount_internal(mountpoint: &str) -> anyhow::Result<()> {
    // Phase 0: note cache dir for cleanup after unmount
    let _cache_dir = cache_dir_for_mount(mountpoint);

    // Phase 1: writeback drain skipped in CSI path to avoid blocking gRPC server.
    // Writeback continues in background; dirty files will be recovered on next mount.
    let pending = crate::cmd::mount::pending_writebacks();
    if pending > 0 {
        tracing::info!(
            mountpoint,
            pending,
            "unmount with pending writeback (background upload continues)"
        );
    }
    // Phase 2: try graceful shutdown via FUSE session.
    // Take the BackgroundSession and call umount_and_join(), which does
    // fusermount3 -u internally then joins the daemon thread. This
    // guarantees the daemon has exited and released all resources.
    // FUSE_SESSION is a Unix-only static (#[cfg(not(windows))] above);
    // WinFSP shutdown uses a different path.
    #[cfg(not(windows))]
    {
        let session = FUSE_SESSION.lock().ok().and_then(|mut g| g.take());
        if let Some(session) = session {
            tracing::debug!(mountpoint, "graceful FUSE session shutdown");
            if session.umount_and_join().is_ok() {
                // Success — wait for mount to fully disappear from /proc/mounts
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                while std::time::Instant::now() < deadline {
                    if !is_mount_point(mountpoint) {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                let cache_dir = cache_dir_for_mount(mountpoint);
                // Bug 30: ENOENT here is the common case
                // when the mount used a non-default
                // cache-dir (notably CSI, which sets
                // cache-dir to {MNTRS_CACHE_DIR}/{volume_id}
                // and cleans that path itself in
                // node_unstage_volume). The cache_dir
                // helper derives a /tmp/mntrs-csi-cache/
                // <slug> path from the mountpoint that
                // CSI never used; the remove_dir_all here
                // then fails with NotFound on every CSI
                // unmount. Suppress that case so the warn
                // log only fires for real cleanup
                // problems (permissions, EIO).
                match std::fs::remove_dir_all(&cache_dir) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        tracing::warn!(cache_dir, error=%e, "cache cleanup failed");
                    }
                }
                return Ok(());
            }
            tracing::warn!(
                mountpoint,
                "umount_and_join failed, falling back to fusermount3"
            );
        }
    }

    // Phase 3: fallback — raw fusermount3 unmount
    if let Err(e) = crate::cmd::unmount::unmount(mountpoint) {
        tracing::warn!(mountpoint, error=%e, "regular unmount failed, trying lazy");
        // Phase 4: lazy unmount fallback
        let _ = std::process::Command::new("fusermount3")
            .arg("-u")
            .arg("-z")
            .arg(mountpoint)
            .status()
            .or_else(|_| {
                std::process::Command::new("fusermount")
                    .arg("-u")
                    .arg("-z")
                    .arg(mountpoint)
                    .status()
            });
    }
    // Wait for mount to disappear from /proc/mounts (up to 5s)
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if !is_mount_point(mountpoint) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    // Phase 5: clean up isolated cache directory
    let cache_dir = cache_dir_for_mount(mountpoint);
    // Bug 30 (mirror of Phase 2 above): silence ENOENT
    // when CSI/custom cache-dir overrides put the
    // actual files elsewhere.
    match std::fs::remove_dir_all(&cache_dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            tracing::warn!(cache_dir, error=%e, "cache cleanup failed");
        }
    }
    Ok(())
}

/// Returns number of in-flight writeback tasks.
pub fn pending_writebacks() -> usize {
    crate::writeback::pending_count()
}
#[allow(clippy::too_many_arguments)]
#[allow(unused_variables)]
#[allow(unused_imports)]
pub fn mount(
    storage_url: &str,
    mountpoint: &str,
    opts: &HashMap<String, String>,
    read_only: bool,
    _network_mode: bool,
    dir_cache_time: u64,
    attr_timeout: u64,
    _type_cache_ttl: u64,
    stat_cache_ttl: u64,
    allow_other: bool,
    debug_fuse: bool,
    volname: &str,
    devname: Option<&str>,
    write_back_cache: bool,
    fuse_options: &[String],
    fuse_flags: &[String],
    daemon: bool,
    daemon_wait: bool,
    _daemon_timeout: u64,
    allow_root: bool,
    _allow_idmap: bool,
    vfs_cache_max_size: u64,
    mem_limit: u64,
    // Underlying `MemCache` impl to wire up. The PoC accepts
    // two options:
    //   * "dashmap" (default) — `DashMapMemCache`, the legacy
    //     implementation. FIFO eviction, hand-rolled, ~5
    //     crates. No new dep. Matches rclone parity and is
    //     what every prior benchmark was run against.
    //   * "moka" — `MokaMemCache` (moka::sync::Cache with
    //     TinyLFU). Better skewed-access hit rates, but
    //     pulls in parking_lot/crossbeam/arcshift and a
    //     background maintenance thread. Use this for
    //     head-to-head A/B via `--mem-cache-metrics-interval`.
    mem_cache_impl: &str,
    // Seconds between mem_cache stats tracing events. 0 = off
    // (no background thread spawned). When > 0, one
    // structured `tracing::info!(target: "mntrs::mem_cache",
    // ...)` event is emitted per tick — the building block for
    // a future Prometheus exporter or log-scraper dashboard.
    // The goal of the work in `cache.rs` is to give every
    // `MemCache` impl a uniform `stats()` view so that, when
    // a moka impl lands, the same `tracing` event format
    // works for both, and a head-to-head comparison is one
    // log filter away.
    mem_cache_metrics_interval_secs: u64,
    vfs_write_back: u64,
    vfs_cache_mode: &str,
    vfs_read_ahead: u64,
    vfs_read_chunk_size: u64,
    default_permissions: bool,
    uid: Option<u32>,
    gid: Option<u32>,
    umask: Option<u32>,
    dir_perms: Option<u32>,
    file_perms: Option<u32>,
    link_perms: Option<u32>,
    allow_non_empty: bool,
    cache_dir: Option<&str>,
    direct_io: bool,
    poll_interval: u64,
    vfs_cache_max_age: u64,
    vfs_cache_min_free_space: u64,
    exclude: Vec<String>,
    include: Vec<String>,
    max_size: Option<u64>,
    min_size: Option<u64>,
    max_depth: Option<usize>,
    ignore_case: bool,
    _no_modtime: bool,
    use_server_modtime: bool,
    _no_checksum: bool,
    _no_seek: bool,
    _links: bool,
    _no_apple_double: bool,
    _no_apple_xattr: bool,
    hash_filter: Option<String>,
    _mount_case_insensitive: bool,
    _max_read_ahead: u64,
    vfs_read_chunk_size_limit: u64,
    vfs_read_chunk_streams: u32,
    vfs_prefetch_threshold: u64,
    vfs_prefetch_queue_mb: u64,
    vfs_fast_fingerprint: bool,
    async_read: bool,
    vfs_refresh: bool,
    vfs_case_insensitive: bool,
    no_implicit_dir: bool,
    vfs_block_norm_dupes: bool,
    _vfs_links: bool,
    _vfs_used_is_size: bool,
    _vfs_metadata_extension: Option<String>,
    storage_class: Option<&str>,
    vfs_write_wait: u64,
    vfs_read_wait: u64,
    vfs_cache_poll_interval: u64,
    vfs_handle_caching: u64,
    vfs_disk_space_total_size: u64,
) -> Result<()> {
    let op = rt_block_on(build_operator(storage_url, opts))?;
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    let cache_dir_path = if let Some(cd) = cache_dir {
        std::path::PathBuf::from(cd)
    } else {
        std::path::PathBuf::from(format!("{}/.cache/mntrs", home))
    };
    // mem_limit is interpreted as MiB by the CLI; 0 means
    // "unbounded" (still tracked, but no eviction triggered).
    // Production default is 256 MiB (see `cli_defaults`
    // above).
    //
    // The impl is selected by the `--mem-cache-impl` flag.
    // Both impls are constructed as `Arc<dyn MemCache>`,
    // so the call sites in `lib.rs` (which use method
    // syntax) are unchanged — that's the whole point of
    // the trait. We dispatch on a small string here
    // rather than a full enum to keep the CLI surface
    // stable across impl additions (moka and any future
    // impl can be added without breaking the flag list).
    let mem_cache_bytes = if mem_limit > 0 {
        mem_limit * 1024 * 1024
    } else {
        0
    };
    let mem_cache: std::sync::Arc<dyn crate::cache::MemCache> = match mem_cache_impl {
        "dashmap" => std::sync::Arc::new(crate::cache::DashMapMemCache::new(mem_cache_bytes)),
        "moka" => std::sync::Arc::new(crate::cache::MokaMemCache::new(mem_cache_bytes)),
        other => {
            return Err(anyhow!(
                "unknown --mem-cache-impl {other:?}; valid: dashmap, moka"
            ));
        }
    };
    let fs = MntrsFs {
        op: Arc::new(op),
        inodes: dashmap::DashMap::new(),
        path_to_ino: dashmap::DashMap::new(),
        lookup_count: dashmap::DashMap::new(),
        dir_cache: dashmap::DashMap::new(),
        cache_dir: cache_dir_path,
        handles: dashmap::DashMap::new(),
        // Issue #23: per-fh readdir snapshots. Empty
        // until opendir() populates an entry.
        dir_listers: dashmap::DashMap::new(),
        // Issue #38: empty pending set; populated on
        // first flush/release.
        writeback_pending: std::sync::Arc::new(dashmap::DashSet::new()),
        dir_cache_ttl: std::time::Duration::from_secs(dir_cache_time),
        attr_ttl: std::time::Duration::from_secs(attr_timeout),
        stat_cache_ttl: std::time::Duration::from_secs(stat_cache_ttl),
        volname: volname.to_string(),
        cache_max_size: vfs_cache_max_size * 1024 * 1024,
        write_back_delay: std::time::Duration::from_secs(vfs_write_back),
        cache_mode: vfs_cache_mode.to_string(),
        read_ahead: vfs_read_ahead,
        read_chunk_size: vfs_read_chunk_size,
        read_chunk_size_limit: vfs_read_chunk_size_limit,
        read_chunk_streams: vfs_read_chunk_streams,
        prefetch_threshold: vfs_prefetch_threshold,
        prefetch_queue_mb: vfs_prefetch_queue_mb,
        uid,
        gid,
        umask,
        dir_perms: dir_perms.unwrap_or(0o777) as u16,
        file_perms: file_perms.unwrap_or(0o666) as u16,
        link_perms: link_perms.unwrap_or(0o777) as u16,
        direct_io,
        poll_interval: std::time::Duration::from_secs(poll_interval.max(1)),
        cache_max_age: std::time::Duration::from_secs(vfs_cache_max_age),
        cache_min_free_space: vfs_cache_min_free_space * 1024 * 1024,
        exclude_patterns: exclude,
        include_patterns: include,
        max_size,
        min_size,
        max_depth,
        ignore_case,
        fast_fingerprint: vfs_fast_fingerprint,
        async_read,
        vfs_refresh,
        case_insensitive: vfs_case_insensitive,
        no_implicit_dir,
        use_server_modtime,
        no_apple_double: false,
        no_apple_xattr: false,
        hash_filter: hash_filter.as_ref().and_then(|hf| {
            let mut parts = hf.splitn(2, '/');
            let k: usize = parts.next()?.parse().ok()?;
            let n: usize = parts.next()?.parse().ok()?;
            if k == 0 || k > n { None } else { Some((k, n)) }
        }),
        block_norm_dupes: vfs_block_norm_dupes,
        write_wait: std::time::Duration::from_secs(vfs_write_wait),
        read_wait: std::time::Duration::from_secs(vfs_read_wait),
        cache_poll_interval: std::time::Duration::from_secs(vfs_cache_poll_interval),
        handle_caching: std::time::Duration::from_secs(vfs_handle_caching),
        disk_total_size: vfs_disk_space_total_size * 1024 * 1024 * 1024 * 1024, // TB to bytes
        writeback_sender: std::sync::OnceLock::new(),

        mem_cache,
        attr_cache: dashmap::DashMap::new(),
        disk_cache_index: std::sync::Arc::new(dashmap::DashMap::new()),
        storage_class: storage_class.map(|s| s.to_string()),
    };

    // Create pipe for daemon_wait parent-child synchronization
    #[cfg(not(windows))]
    let wait_pipe = if daemon_wait {
        match rustix::pipe::pipe() {
            Ok((r, w)) => {
                // Take ownership of raw fds so they aren't closed on drop until we're done
                let r_fd = r.as_raw_fd();
                let w_fd = w.as_raw_fd();
                // Prevent OwnedFd from closing on drop — we manage lifetime manually
                std::mem::forget(r);
                std::mem::forget(w);
                Some((r_fd, w_fd))
            }
            Err(_) => return Err(anyhow!("pipe failed")),
        }
    } else {
        None
    };

    // Re-exec daemon (rclone-style): parent spawns a child process that does
    // the actual mount. Parent exits immediately; child stays alive holding
    // the FUSE session. This avoids fork+tokio incompatibility.
    #[cfg(not(windows))]
    if daemon && std::env::var_os("MNTRS_INTERNAL_DAEMON").is_none() {
        let bin = std::env::current_exe().unwrap_or_else(|_| std::path::PathBuf::from("mntrs"));
        let args: Vec<String> = std::env::args()
            .skip(1)
            .map(|a| {
                if a == "--daemon" {
                    "--internal-daemon".to_string()
                } else {
                    a.clone()
                }
            })
            .collect();
        std::process::Command::new(&bin)
            .args(&args)
            .env("MNTRS_INTERNAL_DAEMON", "1")
            .stdin(std::process::Stdio::null())
            // Daemon stdio is detached in the re-exec'd child (see block
            // below). Spawning with null here is safe — the child re-opens
            // its own fds 1/2 to /dev/null (or MNTRS_DAEMON_LOG).
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|e| anyhow!("failed to spawn daemon: {}", e))?;
        std::process::exit(0);
    }

    // Re-exec child: treat as daemon (stay alive after mount)
    #[cfg(not(windows))]
    let daemon = daemon || std::env::var_os("MNTRS_INTERNAL_DAEMON").is_some();

    // Detach stdio in the daemon child. Without this, the daemon inherits
    // the calling shell's pipe (e.g. `bash run_all.sh | tee` or shell's
    // `> MOUNT_LOG 2>&1 &`). The daemon keeps the write end of that pipe
    // open for its entire lifetime (the FUSE session), so the parent
    // shell/tee never sees EOF and the runner hangs waiting for the step
    // to finish. Symptom in CI: bench-comparison job sits at 9-27 min
    // post-bench with orphan `tee` + `mntrs` processes.
    //
    // Fix per daemon(3): dup2 the desired target onto fds 1,2. We default
    // to /dev/null to drop output; CI sets MNTRS_DAEMON_LOG to a file
    // path (typically MOUNT_LOG) so the artifact upload still captures
    // tracing for post-mortem.
    #[cfg(not(windows))]
    if std::env::var_os("MNTRS_INTERNAL_DAEMON").is_some() && daemon {
        unsafe {
            let target: i32 = match std::env::var_os("MNTRS_DAEMON_LOG") {
                Some(p) => std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(p.to_string_lossy().as_ref())
                    .map(|f| {
                        use std::os::unix::io::IntoRawFd;
                        f.into_raw_fd()
                    })
                    .unwrap_or(-1),
                None => libc::open(c"/dev/null".as_ptr(), libc::O_RDWR),
            };
            if target >= 0 {
                libc::dup2(target, libc::STDOUT_FILENO);
                libc::dup2(target, libc::STDERR_FILENO);
                if target > 2 {
                    libc::close(target);
                }
            }
            // If target < 0 (e.g. /dev/null missing, disk full, log path
            // unwritable), silently fall through: the daemon keeps the
            // inherited stdio, which is the pre-b3f85dc behavior. Worse
            // case is a hung pipe — never worse than the regression.
        }
    }

    let mount_path = Path::new(mountpoint);
    #[cfg(not(windows))]
    let mut cfg: fuser::Config = Default::default();
    if debug_fuse {
        #[cfg(target_os = "linux")]
        unsafe {
            std::env::set_var("FUSE_DEBUG", "1");
        }
    }
    // Check /etc/fuse.conf for user_allow_other when --allow-other is used
    #[cfg(target_os = "linux")]
    if allow_other && unsafe { libc::geteuid() != 0 } {
        let fuse_conf = std::path::Path::new("/etc/fuse.conf");
        if fuse_conf.exists() {
            if let Ok(content) = std::fs::read_to_string(fuse_conf)
                && !content.lines().any(|l| l.trim() == "user_allow_other")
            {
                return Err(anyhow!(
                    "--allow-other requires 'user_allow_other' in /etc/fuse.conf. "
                ));
            }
        } else {
            return Err(anyhow!(
                "--allow-other requires /etc/fuse.conf with 'user_allow_other'. "
            ));
        }
    }
    #[cfg(not(windows))]
    if allow_other || allow_root {
        cfg.acl = fuser::SessionACL::All;
    }
    #[cfg(not(windows))]
    {
        cfg.mount_options = vec![
            if read_only {
                MountOption::RO
            } else {
                MountOption::RW
            },
            MountOption::Exec,
            MountOption::FSName(devname.unwrap_or(volname).to_string()),
        ];
        if write_back_cache {
            cfg.mount_options
                .push(MountOption::CUSTOM("writeback_cache".to_string()));
        }
        if allow_root {
            cfg.mount_options
                .push(MountOption::CUSTOM("allow_root".to_string()));
        }
        #[cfg(target_os = "macos")]
        {
            if _no_apple_double {
                cfg.mount_options
                    .push(MountOption::CUSTOM("noappledouble".to_string()));
            }
            if _no_apple_xattr {
                cfg.mount_options
                    .push(MountOption::CUSTOM("noapplexattr".to_string()));
            }
            if _mount_case_insensitive {
                cfg.mount_options
                    .push(MountOption::CUSTOM("mount_case_insensitive".to_string()));
            }
        }
        if default_permissions {
            cfg.mount_options
                .push(MountOption::CUSTOM("default_permissions".to_string()));
        }
        if allow_non_empty {
            cfg.mount_options
                .push(MountOption::CUSTOM("nonempty".to_string()));
        }
        for opt in fuse_options {
            cfg.mount_options.push(MountOption::CUSTOM(opt.clone()));
        }
        for flag in fuse_flags {
            cfg.mount_options.push(MountOption::CUSTOM(flag.clone()));
        }
    }

    #[cfg(not(windows))]
    {
        use crate::core_fs::fuser::FuserAdapter;
        fs.start_cache_poller();
        // mem_cache observability: when --mem-cache-metrics-interval
        // > 0, spawn a background thread that emits one structured
        // tracing event per tick with the current counters. The
        // `MemCache::stats()` snapshot is the source of truth (see
        // the `MemCacheStats` docstring in `cache.rs`); this loop
        // just gives it an output channel. We use tracing's
        // structured fields rather than `format!` so log
        // aggregators (Loki, Datadog, etc.) can index each
        // counter without parsing a freeform string.
        //
        // `Relaxed` snapshots are fine for monitoring: the
        // counters may briefly be inconsistent across fields
        // under concurrent traffic (e.g. `inserts` may have
        // advanced but its `used_bytes` update hasn't been
        // observed yet). The shape of the numbers over time is
        // what we want; a single-instant exact cross-field view
        // is not.
        //
        // The thread runs until the mount is killed. It owns its
        // own clone of the mem_cache `Arc`, so dropping `fs`
        // when the adapter is consumed doesn't drop the cache
        // out from under the logger.
        if mem_cache_metrics_interval_secs > 0 {
            let mem_cache_for_metrics = fs.mem_cache.clone();
            use crate::cache::MemCache;
            let interval = std::time::Duration::from_secs(mem_cache_metrics_interval_secs);
            std::thread::Builder::new()
                .name("mem_cache_metrics".into())
                .spawn(move || {
                    // Issue #27: graceful shutdown. Pre-fix
                    // this was a bare `loop { sleep }` with no
                    // exit signal — the daemon's exit killed
                    // the thread but unit tests that
                    // constructed an adapter and dropped it
                    // left an orphan thread.
                    //
                    // Reuse the existing
                    // `SHUTDOWN_REQUESTED` AtomicBool that
                    // both unmount_internal and the SIGINT/
                    // SIGTERM handler set. Sleep in 100 ms
                    // ticks so the longest exit latency is
                    // ~100 ms regardless of the configured
                    // metrics interval. Falls out of the
                    // loop cleanly once flagged.
                    let tick = std::time::Duration::from_millis(100);
                    let mut elapsed = std::time::Duration::ZERO;
                    while !SHUTDOWN_REQUESTED.load(std::sync::atomic::Ordering::Relaxed) {
                        std::thread::sleep(tick);
                        elapsed += tick;
                        if elapsed < interval {
                            continue;
                        }
                        elapsed = std::time::Duration::ZERO;
                        let s = mem_cache_for_metrics.stats();
                        tracing::info!(
                            target: "mntrs::mem_cache",
                            hits = s.hits,
                            misses = s.misses,
                            hit_rate_pct = format!("{:.2}", s.hit_rate() * 100.0),
                            inserts = s.inserts,
                            evictions = s.evictions,
                            entries = s.entries,
                            used_bytes = s.used_bytes,
                            capacity_bytes = s.capacity_bytes,
                            utilization_pct = format!("{:.1}", s.utilization() * 100.0),
                            "mem_cache_stats"
                        );
                    }
                })
                .map_err(|e| anyhow!("failed to spawn mem_cache metrics thread: {e}"))?;
        }
        let adapter = FuserAdapter::new(
            fs,
            std::time::Duration::from_secs(dir_cache_time),
            std::time::Duration::from_secs(attr_timeout),
            direct_io,
        );
        let session = fuser::spawn_mount2(adapter, mount_path, &cfg)?;
        // Store session so unmount_internal can gracefully shut it down
        // instead of leaking the daemon thread via std::mem::forget.
        // FUSE_SESSION is Unix-only (WinFSP uses a different teardown path).
        #[cfg(not(windows))]
        if let Ok(mut guard) = FUSE_SESSION.lock() {
            *guard = Some(session);
        }
        record_mount(storage_url, mountpoint, read_only);
        if daemon_wait {
            // Close write end of pipe to signal parent (POLLHUP on read end)
            if let Some((_r, w)) = wait_pipe.as_ref() {
                unsafe {
                    rustix::io::close(*w);
                }
            }
        }
    }

    #[cfg(windows)]
    {
        use crate::core_fs::winfsp::WinFspAdapter;
        use std::sync::Arc;
        let adapter = WinFspAdapter::new(Arc::new(fs));
        let mut vol_params = winfsp::host::VolumeParams::default();
        vol_params.filesystem_name(volname);
        let mut host: winfsp::host::FileSystemHost<_, winfsp::host::FineGuard> =
            winfsp::host::FileSystemHost::new(vol_params, adapter)
                .map_err(|e| anyhow::anyhow!("FileSystemHost::new: {e}"))?;
        host.mount(mountpoint)
            .map_err(|e| anyhow::anyhow!("host.mount: {e}"))?;
    }

    // Daemon-wait: signal mount readiness before entering daemon loop.
    // rclone-style: --daemon --daemon-wait means "fork, mount, signal, stay alive".
    // Without fork, we signal via pipe then enter the keep-alive loop.
    // CSI (daemon=false, daemon_wait=true): signal then return control to caller.
    #[cfg(not(windows))]
    if let Some((r, w)) = wait_pipe {
        unsafe {
            rustix::io::close(w);
        }
        if daemon_wait {
            let deadline =
                std::time::Instant::now() + std::time::Duration::from_secs(_daemon_timeout);
            while std::time::Instant::now() < deadline {
                let mut pfd = libc::pollfd {
                    fd: r,
                    events: libc::POLLIN,
                    revents: 0,
                };
                let ms = (deadline - std::time::Instant::now()).as_millis().min(100) as i32;
                if unsafe { libc::poll(&mut pfd, 1, ms) } > 0
                    && pfd.revents & (libc::POLLIN | libc::POLLHUP) != 0
                {
                    break;
                }
            }
        }
        unsafe {
            rustix::io::close(r);
        }
    }

    // Set up signal handling for clean shutdown.
    CLEANUP_MP.set(mountpoint.to_string()).ok();
    unsafe {
        libc::atexit(cleanup);
    }
    unsafe {
        libc::signal(libc::SIGINT, handler as *const () as libc::sighandler_t);
        libc::signal(libc::SIGTERM, handler as *const () as libc::sighandler_t);
    }

    // Spawn a watcher thread: when a signal sets SHUTDOWN_REQUESTED,
    // call fusermount3 -u so the FUSE daemon gets ENODEV and exits.
    //
    // FD-leak fix: previously the watcher unconditionally spawned a
    // fusermount3 -u child once SHUTDOWN_REQUESTED was true. In the
    // session-end path (external `fusermount3 -u` from the test, or
    // `unmount_internal` → `umount_and_join`), the mount was already
    // gone — the watcher's child was redundant and, because the
    // parent process exits as soon as `session.join()` returns,
    // the child got orphaned. Each orphan held 2 pipe FDs (the
    // stdout/stderr pipes `Command::new(...).status()` opens)
    // and got adopted by init, leaking 2 FDs per cycle. The
    // lifecycle_stress test caught this as 12-15 fds/post-unmount.
    //
    // The signal handler now also sets SHUTDOWN_BY_SIGNAL, so the
    // watcher can distinguish "I'm exiting because of a SIGTERM"
    // (where spawning the unmount child is the whole point) from
    // "the session ended on its own" (where the child is just
    // noise). The check is racy with the signal handler but the
    // worst case is a single missed unmount attempt on the signal
    // path, which is harmless: the process is exiting anyway and
    // the kernel cleans up the FUSE mount on process death.
    #[cfg(not(windows))]
    {
        let mp = mountpoint.to_string();
        std::thread::Builder::new()
            .name("fuse-signal-watcher".into())
            .spawn(move || {
                while !SHUTDOWN_REQUESTED.load(std::sync::atomic::Ordering::Relaxed) {
                    std::thread::sleep(std::time::Duration::from_millis(500));
                }
                if !SHUTDOWN_BY_SIGNAL.load(std::sync::atomic::Ordering::Relaxed) {
                    return;
                }
                tracing::info!("signal received, unmounting...");
                let _ = Command::new("fusermount3")
                    .arg("-u")
                    .arg(&mp)
                    .status()
                    .or_else(|_| Command::new("fusermount").arg("-u").arg(&mp).status());
            })
            .ok();

        // Block until the FUSE session ends.
        // This happens when:
        //   - unmount_internal() takes the session and calls umount_and_join()
        //   - External fusermount3 -u disconnects the kernel side
        //   - A signal triggers the watcher thread above to call fusermount3 -u
        // FUSE_SESSION is Unix-only; on WinFSP, spawn_mount2 returns
        // immediately and the daemon is supervised differently, so
        // this entire block is Unix-only.
        #[cfg(not(windows))]
        {
            let session = FUSE_SESSION.lock().ok().and_then(|mut g| g.take());
            if let Some(session) = session {
                if let Err(e) = session.join() {
                    tracing::warn!(error=%e, "FUSE session ended with error");
                }
            } else {
                // Session was taken by unmount_internal — wait for mount to disappear
                while is_mount_point(mountpoint) {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
            // Signal the watcher thread to exit (it loops on SHUTDOWN_REQUESTED)
            SHUTDOWN_REQUESTED.store(true, std::sync::atomic::Ordering::Relaxed);
            remove_mount(mountpoint);
        }

        #[cfg(windows)]
        loop {
            // spawn_mount2 on WinFSP returns once the mount is registered;
            // keep the process alive until unmount/shutdown is requested.
            std::thread::sleep(std::time::Duration::from_secs(3600));
        }
    }

    Ok(())
}

fn apply_operator_with_tls(
    builder: impl opendal::Builder,
    opts: &std::collections::HashMap<String, String>,
) -> Result<Operator> {
    // Check for curl-compatible TLS flags: --opt cacert=... --opt cert=...
    let insecure = opts.contains_key("insecure");
    let has_tls = insecure || opts.contains_key("cacert") || opts.contains_key("cert");
    let op = if has_tls {
        // Bug 15: assert we're being built from inside
        // crate::rt() before we construct the
        // reqwest::Client. The non-TLS branch below
        // gets this guarantee for free from
        // `crate::http_client::shared()`'s init assertion;
        // the TLS branch builds a per-mount client
        // directly, so it needs the same check.
        //
        // Why: reqwest::Client's hyper connector binds
        // to whichever tokio runtime drives the FIRST
        // .await on a request — not at .build() time.
        // Today every apply_operator_with_tls caller
        // runs inside rt_block_on (= crate::rt()), so
        // the first .await is on the right runtime by
        // construction. The assertion catches a future
        // refactor that ever calls apply_operator_with_tls
        // from a different (or no) tokio runtime, which
        // would silently bind the hyper connector to
        // the wrong reactor and produce hard-to-debug
        // deadlocks in writeback. See src/http_client.rs
        // for the full root cause writeup (the same
        // bug pattern caused csi-e2e run 27407577059 to
        // hang at pending=3).
        let init_handle = tokio::runtime::Handle::current();
        let expected = crate::rt().handle();
        debug_assert!(
            init_handle.id() == expected.id(),
            "apply_operator_with_tls TLS branch must be reached from inside crate::rt(); \
             got a different (or no) tokio runtime — reqwest::Client built here would bind \
             its hyper connector to that other runtime on first .await and deadlock when \
             writeback (which runs in crate::rt()) tries to drive it."
        );
        let mut rb = reqwest::Client::builder();
        if insecure {
            rb = rb.danger_accept_invalid_certs(true);
        }
        if let Some(path) = opts.get("cacert") {
            let buf = std::fs::read(path).map_err(|e| anyhow!("read cacert '{}': {}", path, e))?;
            let ca = reqwest::Certificate::from_pem(&buf)
                .map_err(|e| anyhow!("invalid cacert '{}': {}", path, e))?;
            rb = rb.add_root_certificate(ca);
        }
        if let Some(cert_path) = opts.get("cert") {
            let buf = std::fs::read(cert_path)
                .map_err(|e| anyhow!("read cert '{}': {}", cert_path, e))?;
            let identity =
                reqwest::Identity::from_pem(&buf).map_err(|e| anyhow!("invalid cert: {}", e))?;
            rb = rb.identity(identity);
        }
        let client = rb.build().map_err(|e| anyhow!("build TLS client: {}", e))?;
        Operator::new(builder)?
            .layer(opendal::layers::HttpClientLayer::new(
                opendal::raw::HttpClient::with(client),
            ))
            .layer(TimeoutLayer::new().with_io_timeout(std::time::Duration::from_secs(30)))
            .layer(RetryLayer::new().with_max_times(3).with_factor(2.0))
            .layer(ConcurrentLimitLayer::new(16))
            .finish()
    } else {
        // Non-TLS path: still wrap an explicit HttpClientLayer so we never
        // touch opendal's `GLOBAL_REQWEST_CLIENT` LazyLock. Otherwise that
        // client gets instantiated lazily on the first .await, binding its
        // hyper connector to whichever tokio runtime Handle::current()
        // happens to be at that moment — and if writeback (in crate::rt())
        // ever drives an op that ends up the first caller, the binding
        // mismatches the rt_block_on / cmd/mount runtime and we deadlock
        // (see src/http_client.rs for the full root cause). Using
        // `shared()` here forces the binding to happen on the first
        // apply_operator_with_tls call, which always runs inside
        // `crate::rt()` via `rt_block_on`.
        Operator::new(builder)?
            .layer(opendal::layers::HttpClientLayer::new(
                opendal::raw::HttpClient::with(crate::http_client::shared().clone()),
            ))
            .layer(TimeoutLayer::new().with_io_timeout(std::time::Duration::from_secs(30)))
            .layer(RetryLayer::new().with_max_times(3).with_factor(2.0))
            .layer(ConcurrentLimitLayer::new(16))
            .finish()
    };
    Ok(op)
}

async fn build_operator(storage_url: &str, opts: &HashMap<String, String>) -> Result<Operator> {
    let url = url::Url::parse(storage_url)
        .map_err(|e| anyhow!("invalid storage URL '{storage_url}': {e}"))?;
    match url.scheme() {
        "s3" => build_s3(&url, opts).await,
        "gs" | "gcs" => build_gcs(&url, opts).await,
        "azblob" => build_azblob(&url, opts).await,
        "hdfs" | "hdfs-native" => build_hdfs_native(&url, opts).await,
        #[cfg(feature = "hdfs-jni")]
        "hdfs-jni" => build_hdfs_jni(&url, opts).await,
        "webhdfs" => build_webhdfs(&url, opts).await,
        "oss" => build_oss(&url, opts).await,
        "cos" => build_cos(&url, opts).await,
        "obs" => build_obs(&url, opts).await,
        "b2" => build_b2(&url, opts).await,
        "vercel" | "vercel-blob" => build_vercel_blob(&url, opts).await,
        "fs" | "file" => build_fs(&url, opts).await,
        "memory" | "mem" => build_memory(&url, opts).await,
        "webdav" | "dav" => build_webdav(&url, opts).await,
        "aliyun" | "aliyun-drive" => build_aliyun_drive(&url, opts).await,
        s => Err(anyhow!(
            "unsupported scheme '{s}'; try s3://, gs://, azblob://, hdfs://, hdfs-jni://, webhdfs://, oss://, cos://, obs://, b2://"
        )),
    }
}

async fn build_s3(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let bucket = url.host_str().ok_or_else(|| anyhow!("missing bucket"))?;
    let mut builder = S3::default().bucket(bucket);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() {
        builder = builder.root(p);
    }
    if let Some(v) = opts.get("endpoint") {
        builder = builder.endpoint(v);
    }
    if let Some(v) = opts.get("access-key") {
        builder = builder.access_key_id(v);
    }
    if let Some(v) = opts.get("secret-key") {
        builder = builder.secret_access_key(v);
    }
    if let Some(v) = opts.get("region") {
        builder = builder.region(v);
    }
    apply_operator_with_tls(builder, opts)
}

async fn build_gcs(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let bucket = url.host_str().ok_or_else(|| anyhow!("missing bucket"))?;
    let mut builder = Gcs::default().bucket(bucket);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() {
        builder = builder.root(p);
    }
    apply_operator_with_tls(builder, opts)
}

async fn build_azblob(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let container = url.host_str().ok_or_else(|| anyhow!("missing container"))?;
    let mut builder = Azblob::default().container(container);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() {
        builder = builder.root(p);
    }
    if let Some(v) = opts.get("account-name") {
        builder = builder.account_name(v);
    }
    if let Some(v) = opts.get("account-key") {
        builder = builder.account_key(v);
    }
    apply_operator_with_tls(builder, opts)
}

async fn build_hdfs_native(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let namenode = url.host_str().ok_or_else(|| anyhow!("missing namenode"))?;
    let port = url.port().unwrap_or(8020);
    // Issue #22: opendal's `name_node` expects the full
    // `hdfs://host:port` URI form. Pre-fix we passed bare
    // `host:port`; it worked for single-NN setups because
    // `init_hdfs_config` then injected the address into
    // `dfs.namenode.rpc-address.nameservice.nn0` verbatim
    // (and a bare `host:port` is what that config field
    // accepts). But for HA multi-NN where namenode is
    // already a comma-joined list, the missing scheme
    // prefix wedged the per-NN split — opendal sees a
    // single bogus URI with embedded commas.
    let addr = format!("hdfs://{}:{}", namenode, port);
    let mut builder = HdfsNative::default().name_node(&addr);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() {
        builder = builder.root(p);
    }
    // Pass through all options to hdfs-native client.
    // This enables Kerberos, HA, and other advanced HDFS configurations:
    //   --opt dfs.namenode.kerberos.principal=hdfs/_HOST@REALM
    //   --opt dfs.namenode.kerberos.keytab=/etc/krb5.keytab
    //   --opt dfs.ha.namenodes.nameservice=nn0,nn1
    //   --opt dfs.namenode.rpc-address.nameservice.nn0=namenode1:8020
    if !opts.is_empty() {
        builder = builder.options(opts.clone());
    }
    apply_operator_with_tls(builder, opts)
}

/// Build HDFS operator using JNI-based libhdfs (requires Java).
/// Enabled with: cargo build --features hdfs-jni
/// Supports Kerberos via --opt kerberos-ticket-cache-path and --opt user.
#[cfg(feature = "hdfs-jni")]
async fn build_hdfs_jni(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let namenode = url.host_str().ok_or_else(|| anyhow!("missing namenode"))?;
    let port = url.port().unwrap_or(8020);
    let addr = format!("{}:{}", namenode, port);
    let mut builder = opendal::services::Hdfs::default().name_node(&addr);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() {
        builder = builder.root(p);
    }
    for (k, v) in opts {
        match k.as_str() {
            "user" => builder = builder.user(v),
            "kerberos-ticket-cache-path" | "kerberos_ticket_cache_path" => {
                builder = builder.kerberos_ticket_cache_path(v);
            }
            _ => tracing::warn!("ignored unsupported hdfs-jni option: {k}={v}"),
        }
    }
    apply_operator_with_tls(builder, opts)
}

/// Build WebHDFS operator (HDFS REST API gateway).
async fn build_webhdfs(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let endpoint = format!(
        "{}://{}{}",
        url.scheme(),
        url.host_str().ok_or_else(|| anyhow!("missing host"))?,
        url.port().map_or(String::new(), |p| format!(":{p}")),
    );
    let mut builder = opendal::services::Webhdfs::default().endpoint(&endpoint);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() {
        builder = builder.root(p);
    }
    for (k, v) in opts {
        match k.as_str() {
            "user-name" | "user_name" | "user" => builder = builder.user_name(v),
            "delegation" => builder = builder.delegation(v),
            _ => tracing::warn!("ignored unsupported webhdfs option: {k}={v}"),
        }
    }
    apply_operator_with_tls(builder, opts)
}

// Bug 7 / dead-code cleanup: a stale `fn daemonize`
// (double-fork + setsid + stdio redirect) sat here
// behind `#[allow(dead_code)]`. It was never called —
// the actual daemon model is re-exec (see line ~633:
// the parent spawns the child via `Command::new(exe)
// .env("MNTRS_INTERNAL_DAEMON", "1")` rather than
// fork+setsid). The audit caught that the dead
// function had a bug too: setsid() failure returned
// `Err` from the child, but the parent had already
// `exit(0)`'d — so the shell saw success and the
// daemon silently died. Rather than fix dead code
// that no one calls, the whole function and its
// supporting `DAEMON_PIPE_WR` static are removed.
// If a fork-based daemon path ever comes back, build
// it fresh against the current pipe/wait_pipe
// machinery in `mount_internal` (which DOES correctly
// surface child startup errors back through the
// status pipe).

async fn build_oss(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let bucket = url.host_str().ok_or_else(|| anyhow!("missing bucket"))?;
    let mut builder = Oss::default().bucket(bucket);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() {
        builder = builder.root(p);
    }
    if let Some(v) = opts.get("endpoint") {
        builder = builder.endpoint(v);
    }
    if let Some(v) = opts.get("access-key") {
        builder = builder.access_key_id(v);
    }
    if let Some(v) = opts.get("secret-key") {
        builder = builder.access_key_secret(v);
    }
    apply_operator_with_tls(builder, opts)
}

async fn build_cos(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let bucket = url.host_str().ok_or_else(|| anyhow!("missing bucket"))?;
    let mut builder = Cos::default().bucket(bucket);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() {
        builder = builder.root(p);
    }
    if let Some(v) = opts.get("endpoint") {
        builder = builder.endpoint(v);
    }
    if let Some(v) = opts.get("secret-id") {
        builder = builder.secret_id(v);
    }
    if let Some(v) = opts.get("secret-key") {
        builder = builder.secret_key(v);
    }
    apply_operator_with_tls(builder, opts)
}

async fn build_obs(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let bucket = url.host_str().ok_or_else(|| anyhow!("missing bucket"))?;
    let mut builder = Obs::default().bucket(bucket);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() {
        builder = builder.root(p);
    }
    if let Some(v) = opts.get("endpoint") {
        builder = builder.endpoint(v);
    }
    if let Some(v) = opts.get("access-key") {
        builder = builder.access_key_id(v);
    }
    if let Some(v) = opts.get("secret-key") {
        builder = builder.secret_access_key(v);
    }
    apply_operator_with_tls(builder, opts)
}

async fn build_b2(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let bucket = url.host_str().ok_or_else(|| anyhow!("missing bucket"))?;
    let mut builder = B2::default().bucket(bucket);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() {
        builder = builder.root(p);
    }
    if let Some(v) = opts.get("application-key-id") {
        builder = builder.application_key_id(v);
    }
    if let Some(v) = opts.get("application-key") {
        builder = builder.application_key(v);
    }
    apply_operator_with_tls(builder, opts)
}

async fn build_vercel_blob(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let mut builder = VercelBlob::default();
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() {
        builder = builder.root(p);
    }
    if let Some(v) = opts.get("token") {
        builder = builder.token(v);
    }
    apply_operator_with_tls(builder, opts)
}

async fn build_aliyun_drive(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let mut builder = AliyunDrive::default();
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() {
        builder = builder.root(p);
    }
    if let Some(v) = opts.get("access-token") {
        builder = builder.access_token(v);
    }
    if let Some(v) = opts.get("refresh-token") {
        builder = builder.refresh_token(v);
    }
    if let Some(v) = opts.get("client-id") {
        builder = builder.client_id(v);
    }
    if let Some(v) = opts.get("client-secret") {
        builder = builder.client_secret(v);
    }
    if let Some(v) = opts.get("drive-type") {
        builder = builder.drive_type(v);
    }
    apply_operator_with_tls(builder, opts)
}

async fn build_fs(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let root = url.path().to_string();
    let builder = Fs::default().root(&root);
    apply_operator_with_tls(builder, opts)
}

async fn build_memory(_url: &url::Url, _opts: &HashMap<String, String>) -> Result<Operator> {
    let builder = Memory::default();
    apply_operator_with_tls(builder, _opts)
}

/// Async-signal-safe: only sets atomic flags.
/// The main loop checks SHUTDOWN_REQUESTED and performs proper
/// cleanup; the watcher thread additionally checks
/// SHUTDOWN_BY_SIGNAL to decide whether to spawn its own
/// `fusermount3 -u` (only needed for the signal path — the
/// session-end path is already unmounted externally).
extern "C" fn handler(_: i32) {
    SHUTDOWN_REQUESTED.store(true, std::sync::atomic::Ordering::Relaxed);
    SHUTDOWN_BY_SIGNAL.store(true, std::sync::atomic::Ordering::Relaxed);
}

async fn build_webdav(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let endpoint = format!(
        "{}://{}",
        url.scheme(),
        url.host_str().unwrap_or("localhost")
    );
    let mut builder = Webdav::default().endpoint(&endpoint);
    let p = url.path();
    if !p.is_empty() && p != "/" {
        builder = builder.root(p);
    }
    if let Some(v) = opts.get("username") {
        builder = builder.username(v);
    }
    if let Some(v) = opts.get("password") {
        builder = builder.password(v);
    }
    if let Some(v) = opts.get("token") {
        builder = builder.token(v);
    }
    apply_operator_with_tls(builder, opts)
}
