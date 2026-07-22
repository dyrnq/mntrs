#![allow(clippy::type_complexity)]
use crate::MntrsFs;
use anyhow::{Result, anyhow};
#[cfg(not(windows))]
use fuser::MountOption;
use opendal::Operator;
use opendal::layers::{CapabilityCheckLayer, ConcurrentLimitLayer, RetryLayer, TimeoutLayer};
#[cfg(feature = "sftp")]
use opendal::services::Sftp;
use opendal::services::{
    AliyunDrive, Azblob, B2, Cos, Fs, Gcs, HdfsNative, Memory, Obs, Oss, S3, VercelBlob, Webdav,
};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
#[cfg(not(windows))]
use std::os::fd::AsRawFd;
use std::path::Path;
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

/// Issue #261.4: pub so `unmount.rs` can target the same path.
pub fn mounts_db_path() -> String {
    // Use XDG helper. If HOME is unset (containers, CI runners,
    // fresh daemon) we surface a clear error to the caller instead
    // of silently writing to /tmp.
    match crate::util::data_dir() {
        Ok(dir) => dir
            .join("mntrs")
            .join("mounts.txt")
            .to_string_lossy()
            .to_string(),
        Err(e) => {
            tracing::warn!(error=%e, "mounts_db_path: cannot determine data dir; falling back to legacy /tmp path");
            // Last-resort fallback for backward compat with pre-#261.4
            // deployments; warn loudly so the operator sees it.
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
            format!("{}/.local/share/mntrs/mounts.txt", home)
        }
    }
}

fn mounts_db() -> String {
    mounts_db_path()
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
    // Issue #384: canonicalize the mountpoint so the row written
    // here and the row filtered by `remove_mount` / `unmount`'s
    // db cleanup agree byte-for-byte. On macOS `/tmp` is a
    // symlink to `/private/tmp` — without canonicalization a
    // user-supplied `/tmp/foo` here and `/private/tmp/foo` in
    // the unmount path produce a stale entry.
    let canon_mp = crate::util::canonicalize_mountpoint(mountpoint);
    // Atomically rewrite: tmp + rename (POSIX atomic)
    let tmp = format!("{}.tmp.{}", path, std::process::id());
    let mut lines = Vec::new();
    if let Ok(existing) = std::fs::read_to_string(&path) {
        for l in existing.lines() {
            if l.split('\0').nth(1) != Some(canon_mp.as_str()) {
                lines.push(l.to_string());
            }
        }
    }
    let pid = std::process::id().to_string();
    let user = std::env::var("USER").unwrap_or_else(|_| "?".into());
    let ro = if read_only { "ro" } else { "rw" };
    let backend = storage.split(':').next().unwrap_or("?");
    // Strip userinfo before writing to mounts.txt — the file is
    // 0644 by default, and a credentialed URL would otherwise
    // persist in a world-readable file.
    let storage_safe = crate::util::redact_storage_url(storage);
    lines.insert(
        0,
        format!(
            "{}\0{}\0{}\0{}\0{}\0{}",
            storage_safe, canon_mp, pid, user, ro, backend
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
    // Issue #384: canonicalize the mountpoint before
    // filtering so the byte-equality match against the row
    // `record_mount` wrote (also canonicalized) succeeds on
    // macOS where `/tmp` is a symlink to `/private/tmp`.
    let canon_mp = crate::util::canonicalize_mountpoint(mountpoint);
    let content = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(_) => return,
    };
    let filtered: Vec<&str> = content
        .lines()
        .filter(|l| l.split('\0').nth(1) != Some(canon_mp.as_str()))
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
/// Issue #261.2: maps mountpoint → the actual cache dir the mount
/// used (which may come from `opts["cache-dir"]` when CSI sets it,
/// or fall back to `/tmp/mntrs-csi-cache/<slug>` for CLI mounts).
/// `unmount_internal` queries this map so its cleanup targets the
/// same path the mount wrote to — previously it always derived
/// the `/tmp/...` slug, missing CSI's `MNTRS_CACHE_DIR` paths.
static MOUNT_CACHE_DIR: std::sync::LazyLock<std::sync::Mutex<HashMap<String, String>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));
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
            // Issue #374: reuse the cfg-gated helpers from
            // `unmount`. Vanilla macOS has no `fusermount*`
            // binary, so the in-line shell-out was previously
            // silently failing and leaking the mount on
            // atexit / SIGTERM paths. The macOS helper falls
            // back to `umount(8)`; the non-macos unix helper
            // is byte-identical to the pre-fix Linux chain.
            //
            // `cleanup()` is module-level (not under a
            // `#[cfg(unix)]` block), so the helper references
            // here use `cfg(all(unix, not(target_os = "macos")))`
            // / `cfg(target_os = "macos")` to keep Windows
            // builds untouched (the branches compile to dead
            // code on Windows). See the same construction at
            // `unmount.rs::fuse_unmount_via_fusermount`.
            #[cfg(all(unix, not(target_os = "macos")))]
            {
                let _ = crate::cmd::unmount::fuse_unmount_via_fusermount(mp);
            }
            #[cfg(target_os = "macos")]
            {
                let _ = crate::cmd::unmount::fuse_unmount_macos_with_umount(mp);
            }
        }
        remove_mount(mp);
    }
}

/// Simplified mount entry point for CSI plugin.
/// Uses defaults for all the FUSE tuning parameters.
/// Check if a path is already a mount point by querying the Win32 DOS
/// device manager.
///
/// Issue #305 Tier 1: previous stub always returned `false`, so
/// `mount_internal`'s idempotency check let a second `mntrs mount ... V:`
/// proceed and collide with the live volume (WinFSP error
/// STATUS_OBJECT_NAME_COLLISION 0xC0000035).
///
/// Implementation: ask the Win32 DOS device manager whether `path`
/// resolves to a device. `QueryDosDeviceW` returns:
/// - non-zero (length of the device target path) iff the drive letter
///   is mapped to some device — including WinFSP drives (which point
///   at `\Device\WinFsp.{GUID}\X:` or similar) and network drives,
/// - 0 with `GetLastError() == ERROR_FILE_NOT_FOUND` (2) for an
///   unmapped drive letter.
///
/// `GetVolumeNameForVolumeMountPointW` was tried first but returns
/// `ERROR_INVALID_FUNCTION` for WinFSP volumes — they don't register
/// a real NTFS-style volume GUID, so we can't distinguish "unmounted"
/// from "WinFSP mount" via that API.
///
/// Issue #328 race hardening: the first process's `host.mount(V:)`
/// returns `Ok` once WinFSP's user-mode `DefineDosDeviceW` call has
/// completed, but there is a small window (observed pre-fix) where a
/// second process's `QueryDosDeviceW("V:")` still returns 0 — the
/// kernel-side mountpoint table is briefly invisible cross-process
/// even though the user-mode call succeeded. We re-poll up to
/// `IS_MOUNT_POINT_REPOLL_ATTEMPTS` times with `IS_MOUNT_POINT_REPOLL_DELAY_MS`
/// between attempts so the second mount's idempotency check fires
/// instead of racing past it into `host.mount()` collision.
#[cfg(windows)]
pub fn is_mount_point(path: &str) -> bool {
    use windows::Win32::Storage::FileSystem::QueryDosDeviceW;

    // QueryDosDeviceW takes a drive letter WITHOUT the trailing
    // backslash ("V:", not "V:\\"). Pass through whatever the caller
    // gave us after stripping a single trailing slash if present.
    let trimmed = path.strip_suffix('\\').unwrap_or(path);

    // 1024-char buffer covers any plausible device target path
    // (WinFSP uses "\Device\WinFsp.{GUID}\X:" — ~80 chars; SMB
    // mapped drives use "\Device\LanmanRedirector\..." — ~60 chars).
    let mut buf = [0u16; 1024];
    let wide: Vec<u16> = trimmed.encode_utf16().chain(std::iter::once(0)).collect();

    for attempt in 0..IS_MOUNT_POINT_REPOLL_ATTEMPTS {
        let result =
            unsafe { QueryDosDeviceW(windows::core::PCWSTR(wide.as_ptr()), Some(&mut buf)) };

        // QueryDosDeviceW returns 0 on failure, length (excluding the
        // null terminator) on success. An empty result string ("")
        // would be length 0 too but never happens for a real device.
        if result > 0 {
            return true;
        }

        let err = std::io::Error::last_os_error();
        // Distinguish "drive letter not assigned yet" (err=2
        // ERROR_FILE_NOT_FOUND — the kernel-side table hasn't been
        // updated cross-process yet) from "drive letter is permanently
        // unmapped / malformed input" (some other error code).
        // For err=2 we re-poll; for anything else we treat it as
        // "mounted" to avoid collisions on weird input. The first
        // mount's host.mount has either succeeded (in which case the
        // kernel-side state is propagating) or it has not yet been
        // attempted by anyone; both cases resolve to "wait and retry"
        // on err=2 and "fail safe, treat as mounted" otherwise.
        if err.raw_os_error() != Some(2) {
            tracing::debug!(
                path,
                attempt,
                error = %err,
                "is_mount_point(windows): QueryDosDeviceW returned unexpected error; treating as mount point"
            );
            return true;
        }

        if attempt + 1 < IS_MOUNT_POINT_REPOLL_ATTEMPTS {
            tracing::trace!(
                path,
                attempt,
                max = IS_MOUNT_POINT_REPOLL_ATTEMPTS,
                "is_mount_point(windows): QueryDosDeviceW returned ERROR_FILE_NOT_FOUND; re-polling"
            );
            std::thread::sleep(std::time::Duration::from_millis(
                IS_MOUNT_POINT_REPOLL_DELAY_MS,
            ));
        }
    }

    // Exhausted retries and still saw ERROR_FILE_NOT_FOUND — the
    // drive letter is genuinely unmapped as far as the kernel-side
    // table is concerned. Caller may proceed to host.mount().
    tracing::trace!(
        path,
        attempts = IS_MOUNT_POINT_REPOLL_ATTEMPTS,
        "is_mount_point(windows): drive letter unmapped after retries"
    );
    false
}

/// Issue #328 race hardening: number of re-poll attempts in
/// `is_mount_point(windows)` before declaring the drive letter
/// unmapped. The first mount's `host.mount()` returns Ok once
/// WinFSP's user-mode `DefineDosDeviceW` has succeeded, but the
/// kernel-side mountpoint table is briefly invisible to other
/// processes (we measured ~50-200 ms on a quiet Win10 VM; the 5×
/// 100 ms budget below = 500 ms ceiling is well under the 10 s
/// budget imposed by mount-test.ps1 sub-test 13).
#[cfg(windows)]
const IS_MOUNT_POINT_REPOLL_ATTEMPTS: u32 = 5;
#[cfg(windows)]
const IS_MOUNT_POINT_REPOLL_DELAY_MS: u64 = 100;

///
/// Check if a path is already a mount point on macOS.
///
/// Issue #376: previously this used `split_whitespace()` and
/// matched `fields[2]` against the canonical mount path. That
/// silently broke when the mount point contained whitespace
/// (e.g. `/Volumes/My Drive/test`), where the path itself got
/// split across fields and `fields[2]` became `/Volumes/My` — a
/// false negative on the idempotency check at `mount.rs:856` and
/// the wait-loops at lines 614, 691, 1918.
///
/// macOS `mount(8)` output format is
/// `<special> on <mount_point> (<opts>)`. We anchor on the literal
/// substring `on <canonical> (` so whitespace inside the path is
/// matched verbatim. The trailing ` (` anchor prevents false
/// matches like `/Volumes/foo` against a line ending in
/// `/Volumes/foo-bar (...)`.
#[cfg(target_os = "macos")]
pub fn is_mount_point(path: &str) -> bool {
    use std::process::Command;
    // Issue #384: route the input through `canonicalize_mountpoint`
    // so a mountpoint passed as `/tmp/foo` (the user-facing form)
    // is normalized to `/private/tmp/foo` (the form the kernel
    // records). Pre-fix an inline `fs::canonicalize(...).unwrap_or`
    // was used; when canonicalize fails (e.g. the path doesn't
    // exist yet — a fresh-mount race) the fallback returned the
    // raw user input, which doesn't match what `mount(8)` printed
    // and let a second mount slip through (issue #328). The shared
    // helper handles the failure case uniformly and matches the
    // fallback semantics used by `record_mount` / `remove_mount` /
    // `unmount`.
    let canonical = crate::util::canonicalize_mountpoint(path);
    let needle = format!("on {} (", canonical);
    let output = Command::new("mount")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .unwrap_or_default();
    output.lines().any(|line| line.contains(&needle))
}

#[cfg(target_os = "linux")]
pub fn is_mount_point(path: &str) -> bool {
    // Same #384 fix as the macOS branch — canonicalize via the
    // shared helper so symlink-resolved inputs (`/tmp/foo` ->
    // `/private/tmp/foo`) match the kernel's recorded form in
    // `/proc/mounts`.
    let canonical = crate::util::canonicalize_mountpoint(path);
    if let Ok(content) = std::fs::read_to_string("/proc/mounts") {
        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 && parts[1] == canonical {
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
    // Issue #410: at the start of every macOS mount, surface the
    // macFUSE kext state so a user running `mntrs mount` against
    // an under-configured system gets a hint instead of an opaque
    // mount failure. Logs:
    //   * Loaded   → `tracing::info!` with the macFUSE version
    //                (useful for diagnostics without a separate
    //                `kextstat` call).
    //   * Not loaded → `tracing::warn!` with actionable guidance
    //                  covering the four common failure modes
    //                  (not installed / not approved / Apple
    //                  Silicon full-security / helper not running).
    // The check is informational only — the mount proceeds
    // regardless, and the existing failure path (which surfaces
    // a useful mount(8) error when the kext is absent) is
    // unchanged. `#[cfg(target_os = "macos")]` keeps the code
    // out of non-mac builds; CSI on Linux never pays the
    // shell-out cost.
    #[cfg(target_os = "macos")]
    {
        match macfuse_kext_loaded() {
            Some(version) => {
                tracing::info!(
                    macfuse_version = %version,
                    "macFUSE kext loaded"
                );
            }
            None => {
                tracing::warn!(
                    "macFUSE kext not loaded. FUSE mounts will fail until it is. \
                     Common causes: \
                     (1) macFUSE not installed — run the macFUSE installer from https://macfuse.io; \
                     (2) Apple Silicon: reduced security not enabled — boot to Recovery Mode, \
                     Startup Security Utility, set to Reduced Security; \
                     (3) macOS Catalina+: kext not approved — System Settings → Privacy & Security, \
                     scroll to bottom, click Allow (30-min window after install). \
                     Verify with: kextstat | grep macfuse"
                );
            }
        }
    }

    // Issue #261.2: CSI handler passes the operator-configured
    // MNTRS_CACHE_DIR via `opts["cache-dir"]` (see csi/mntrs-csi/src/main.rs
    // stage FUSE mount path). Honor it here so cache files land
    // under the operator-chosen persistent path (K8s ReadOnlyRootFS
    // + tmpfs-safe). CLI `mntrs mount` doesn't pass the key, so
    // we fall back to `cache_dir_for_mount(mountpoint)`, which is
    // cfg-isolated: per-user temp on macOS (Issue #382), the
    // original /tmp path on Linux / other unix (CLI users run
    // locally and choose where to mount).
    let cache_dir = opts
        .get("cache-dir")
        .cloned()
        .unwrap_or_else(|| cache_dir_for_mount(mountpoint));
    let _ = std::fs::create_dir_all(&cache_dir);
    // Register mountpoint→cache_dir so unmount_internal can find
    // the same path during cleanup (Bug 30 follow-up).
    if let Ok(mut map) = MOUNT_CACHE_DIR.lock() {
        map.insert(mountpoint.to_string(), cache_dir.clone());
    }

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
        5, // attr_timeout — #469: bumped 1s→5s so #467 FUSE_READDIRPLUS_AUTO cap can materialize bench wins; kernel attr cache survives multi-stat bursts within 5s
        10, // type_cache_ttl
        1, // stat_cache_ttl
        true, // allow_other (CSI: Pods access as non-root)
        false, // debug_fuse
        "mntrs-csi", // volname
        None, // devname
        false, // write_back_cache (CSI: strict write-through; kernel writeback disabled by design — Pod multi-tenancy demands per-FS-message observability)
        &[],   // fuse_options
        &[],   // fuse_flags
        false, // daemon (no fork — std::thread::spawn holds session)
        false, // daemon_wait
        10,    // daemon_timeout
        false, // allow_root
        false, // allow_idmap
        0,     // vfs_cache_max_size (off)
        256,   // mem_limit
        "dashmap", // mem_cache_impl (default)
        0,     // mem_cache_metrics_interval_secs (off)
        5,     // vfs_write_back
        1024 * 1024, // writeback_immediate_threshold (1 MiB) — #202: small files skip the 5s delay queue
        "off",       // vfs_cache_mode
        0,           // vfs_read_ahead (off)
        128 * 1024 * 1024, // vfs_read_chunk_size (128MiB)
        false,       // default_permissions
        None,        // uid
        None,        // gid
        None,        // umask
        None,        // dir_perms
        None,        // file_perms
        None,        // link_perms
        false,       // allow_non_empty
        Some(cache_dir.as_str()), // cache_dir (CSI isolated)
        false,       // direct_io
        Some(60),    // poll_interval (CSI: fixed 60s, no deprecation warning)
        3600,        // vfs_cache_max_age
        0,           // vfs_cache_min_free_space (off)
        vec![],      // exclude
        vec![],      // include
        None,        // max_size
        None,        // min_size
        None,        // max_depth
        false,       // ignore_case
        false,       // no_modtime
        false,       // use_server_modtime
        false,       // no_checksum
        false,       // no_seek
        false,       // links
        false,       // noapple_double
        false,       // noapple_xattr,
        false,       // no_macos_metadata (CSI runs on Linux; macOS metadata filter is a no-op)
        None,        // hash_filter
        false,       // mount_case_insensitive
        false,       // negative_vncache (CSI: Linux — ignored)
        false,       // auto_cache (CSI: Linux — ignored)
        60,          // daemon_timeout_macos (CSI: Linux — ignored)
        false,       // slow_statfs (CSI: Linux — ignored)
        None,        // volume_name (CSI default — derive from mountpoint at runtime)
        false,       // finder_local (CSI runs on Linux; macOS mount option ignored)
        131072,      // max_read_ahead
        0,           // vfs_read_chunk_size_limit
        // Issue #31: bump default chunk_streams from 0
        // (serial) to 4. rclone's default
        // --vfs-read-chunk-streams is 4; the bench
        // showed cold-start of 100M files at
        // 1.3x slower with serial streams. 4 is
        // enough to keep the network busy without
        // saturating memory or concurrent-upload
        // pools. Operators who want serial can pass
        // `--vfs-read-chunk-streams=0` on the CLI.
        4,        // vfs_read_chunk_streams (parallel)
        16777216, // vfs_prefetch_threshold (16 MiB)
        64,       // vfs_prefetch_queue_mb
        false,    // vfs_fast_fingerprint
        false,    // async_read
        false,    // vfs_refresh
        false,    // vfs_case_insensitive
        false,    // no_implicit_dir
        false,    // vfs_block_norm_dupes
        false,    // vfs_links
        false,    // vfs_used_is_size
        None,     // vfs_metadata_extension
        None,     // storage_class
        1,        // vfs_write_wait (1s)
        1,        // vfs_read_wait (1s)
        60,       // vfs_cache_poll_interval
        0,        // vfs_handle_caching
        0,        // vfs_disk_space_total_size (off)
        false, // vfs_read_stale_on_backend_error (CSI: never stale-on-error; data integrity > uptime)
        0, // winfsp_dispatcher_threads (CSI: driver default 8; CSI pods don't need pinned count)
    )
}

/// Compute the CLI-fallback cache directory for a mount, used by
/// `mount_internal` (when `opts["cache-dir"]` is not set) and by
/// `unmount_internal`'s cleanup. CSI operators that pass
/// `cache-dir` explicitly in `opts` are unaffected.
///
/// Issue #382: macOS launchd always sets `$TMPDIR` to a per-user
/// mode-0700 path under `/var/folders/.../T/`. The unconditional
/// `/tmp/mntrs-csi-cache/<slug>` fallback landed on `/private/tmp`
/// on macOS — world-readable and shared across all users on the
/// host. Per-user temp avoids the cross-user cache leak on shared
/// macOS hosts (CI runners, multi-user build servers). Linux and
/// other unix keep the original `/tmp` path (byte-identical to
/// pre-fix), so a change to the macOS branch cannot accidentally
/// regress a Linux CI run.
fn cache_dir_for_mount(mountpoint: &str) -> String {
    let suffix = mountpoint.replace(['/', ':'], "_");
    #[cfg(target_os = "macos")]
    {
        let base = std::env::var("TMPDIR")
            .unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().to_string());
        format!("{}/mntrs-csi-cache/{}", base.trim_end_matches('/'), suffix)
    }
    #[cfg(not(target_os = "macos"))]
    {
        format!("/tmp/mntrs-csi-cache/{}", suffix)
    }
}

pub fn build_operator_sync(storage_url: &str, opts: &HashMap<String, String>) -> Result<Operator> {
    rt_block_on(build_operator(storage_url, opts))
}

pub fn unmount_internal(mountpoint: &str) -> anyhow::Result<()> {
    // Issue #261.2: look up the actual cache_dir the mount used.
    // Falls back to /tmp/mntrs-csi-cache/<slug> for legacy callers
    // (e.g. if mount_internal was bypassed or for an old running mount).
    let _cache_dir = {
        if let Ok(map) = MOUNT_CACHE_DIR.lock() {
            map.get(mountpoint).cloned()
        } else {
            None
        }
    }
    .unwrap_or_else(|| cache_dir_for_mount(mountpoint));

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
                let cache_dir = {
                    if let Ok(map) = MOUNT_CACHE_DIR.lock() {
                        map.get(mountpoint).cloned()
                    } else {
                        None
                    }
                }
                .unwrap_or_else(|| cache_dir_for_mount(mountpoint));
                // Bug 30: ENOENT here is the common case
                // when the mount used a non-default
                // cache-dir (notably CSI, which sets
                // cache-dir to {MNTRS_CACHE_DIR}/{volume_id}
                // and cleans that path itself in
                // node_unstage_volume). With #261.2 the map
                // holds the real CSI path so we look there
                // first; the /tmp helper is only a fallback.
                // CSI cleans up its own path during
                // node_unstage_volume — that step still
                // happens before this code runs, so a NotFound
                // here is normal. Suppress that case so the
                // warn log only fires for real cleanup
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

    // Phase 3: Windows uses Win32 (handled inside cmd::unmount::unmount
    // via the cfg(windows) branch — #249). On Windows there's no lazy
    // fallback: Win32 errors are propagated so the caller sees a
    // clear `DefineDosDeviceW failed` / `DeleteVolumeMountPointW
    // failed` instead of the old silent fusermount3 failure.
    #[cfg(windows)]
    {
        crate::cmd::unmount::unmount(mountpoint)?;
    }

    // Phase 3 + 4 (Unix): first try regular unmount; on failure, fall
    // back to lazy (-z) fusermount3 / fusermount.
    #[cfg(not(windows))]
    {
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
    // Issue #202: files below this size (bytes) upload
    // immediately on flush/release. `0` disables immediate
    // upload. Default 1 MiB.
    writeback_immediate_threshold: u64,
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
    poll_interval: Option<u64>,
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
    no_apple_double: bool,
    no_apple_xattr: bool,
    no_macos_metadata: bool,
    hash_filter: Option<String>,
    mount_case_insensitive: bool,
    negative_vncache: bool,
    auto_cache: bool,
    daemon_timeout_macos: u64,
    slow_statfs: bool,
    volume_name: Option<&str>,
    finder_local: bool,
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
    // Issue #257: opt-in stale-on-backend-error read
    // fallback. Default false. Users opt in to
    // "stale is better than EIO" semantics via
    // --vfs-read-stale-on-backend-error.
    vfs_read_stale_on_backend_error: bool,
    // Issue #316a (WinFSP audit #305): pinned WinFSP dispatcher
    // thread count. 0 = driver default 8 (matches pre-fix
    // hardcoded `start_with_threads(0)`). >0 = user-overridden.
    // Unix: accepted by clap but ignored here (unix FUSE has its
    // own dispatcher pool — fuser backend).
    winfsp_dispatcher_threads: u32,
) -> Result<()> {
    // Issue #328: idempotency check at the CLI entry point.
    // When V: is already mounted (by an earlier `mntrs mount`
    // process, or by anything else — WinFSP / SMB / NTFS),
    // reject this invocation with a clear, non-zero exit
    // error instead of silently succeeding (which would mask
    // user confusion) or racing into `host.mount()` and
    // colliding with STATUS_OBJECT_NAME_COLLISION 0xC0000035
    // (which used to slip past into the keep-alive loop and
    // hang forever — see #328 history).
    //
    // The CSI path (`mount_internal` at line ~374) keeps the
    // Ok(())-on-already-mounted behavior because CSI mounts
    // can be retried idempotently by a sidecar; only the CLI
    // path is required to fail loudly.
    //
    // `is_mount_point` itself re-polls (see IS_MOUNT_POINT_REPOLL_*
    // constants below) to absorb the brief kernel-side race
    // window between the first process's `host.mount` returning
    // Ok and the drive letter becoming visible to a second
    // process's `QueryDosDeviceW`. We add a final post-check
    // after `host.mount` succeeds (line ~1583) for the
    // reverse race — in case the first mount lands between
    // this idempotency check and our `host.mount`.
    if is_mount_point(mountpoint) {
        tracing::info!(
            mountpoint,
            "already mounted, refusing to mount twice (issue #328)"
        );
        return Err(anyhow!(
            "already mounted at {mountpoint}; run `mntrs unmount {mountpoint}` first"
        ));
    }

    // Issue #209: --poll-interval is the legacy rclone alias
    // for --vfs-cache-poll-interval. When the user explicitly
    // sets it (Some), emit a one-time deprecation warning.
    // Default (None) is silent — no behavior change for users
    // who never touched the legacy flag.
    if poll_interval.is_some() {
        tracing::warn!("--poll-interval is deprecated; use --vfs-cache-poll-interval instead");
    }
    // Issue #62: elapsed_ms checkpoints at every major
    // mount() step. At `tracing::debug!` so they are
    // silent under the default INFO filter — turn them
    // on with `RUST_LOG=mntrs=debug` (or `=trace`).
    // One Instant::now() read + one tracing event per
    // checkpoint (~1 µs total). Useful when the next
    // integration-test failure shows a slow step and
    // we need to bisect the mount path.
    let _t_mount = std::time::Instant::now();
    tracing::debug!(
        // redact any userinfo — the raw `storage_url` may carry
        // the RFC-1738 form `s3://KEY:SECRET@host/bucket` which
        // `url::Url::parse` accepts and opendal then echoes
        // through error messages.
        backend = %crate::util::redact_storage_url(storage_url),
        mountpoint = %mountpoint,
        "mount: entered, about to build_operator"
    );
    let op = rt_block_on(build_operator(storage_url, opts))?;
    tracing::debug!(
        elapsed_ms = _t_mount.elapsed().as_millis() as u64,
        "mount: after build_operator"
    );
    // Initialize the global op for the write path's
    // background thread (it can't borrow `&self.op`,
    // which lives on the FUSE worker). Without this,
    // the lazy `opendal_sync_op` fallback in `lib.rs`
    // returns a brand-new empty `Memory::default()`
    // operator — the FUSE write path's prefix fetch
    // then reads from that empty backend instead of
    // the user-configured `memory://`/`s3://`/etc.
    // backend, so every read returns "" and the
    // regression test in `memory_stress.sh` fails
    // (closes the Integration Tests "memory mount
    // not ready after 60s" failure on commit 977854d).
    crate::set_opendal_sync_op(op.clone());
    // Initialize the disk-IO thread pool. Without
    // it, `submit_disk_write` falls back to running
    // the I/O job synchronously on the FUSE worker
    // thread — the same thread that runs
    // `fuser::session::run()`. A sync disk I/O there
    // starves the FUSE event loop, the kernel mount
    // never completes, and `mount | grep` reports
    // the FUSE mount as not present for the full
    // 60s readiness probe. `init_disk_write_pool`
    // is guarded by `DISK_WRITE_POOL.get().is_some()`
    // so repeat calls (e.g. CSI test that calls
    // `mount()` again with a different cache dir)
    // do NOT spawn extra fsync threads (Bug 5).
    crate::init_disk_write_pool(None);
    tracing::debug!(
        elapsed_ms = _t_mount.elapsed().as_millis() as u64,
        "mount: after init calls (opendal_sync_op + disk_write_pool)"
    );
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
    // Issue #201: per-mount MemoryLimiter. Same cap as
    // mem_cache_bytes (the `--mem-limit` MiB value expressed in
    // bytes). When mem_limit == 0, cap=0 disables enforcement —
    // all `try_reserve` calls succeed and `used()` tracks the
    // uncapped total for diagnostics only.
    //
    // We do NOT call `mem_limiter::install` / `init_global`:
    // per-mount isolation is the design goal. Two mounts in the
    // same daemon process get independent budgets. The global
    // `LIMITER: OnceLock` stays unset so `mem_limiter::global()`
    // returns None for any future caller that wants a
    // process-wide default — leaving that door open without
    // forcing a single shared cap.
    let mem_limiter: std::sync::Arc<crate::mem_limiter::MemoryLimiter> =
        crate::mem_limiter::MemoryLimiter::new(mem_cache_bytes);
    let mem_cache: std::sync::Arc<dyn crate::cache::MemCache> = match mem_cache_impl {
        "dashmap" => std::sync::Arc::new(crate::cache::DashMapMemCache::new(mem_cache_bytes)),
        "moka" => std::sync::Arc::new(crate::cache::MokaMemCache::new(mem_cache_bytes)),
        "foyer" => std::sync::Arc::new(crate::cache::FoyerMemCache::new(mem_cache_bytes)),
        other => {
            return Err(anyhow!(
                "unknown --mem-cache-impl {other:?}; valid: dashmap, moka, foyer"
            ));
        }
    };
    tracing::debug!(
        elapsed_ms = _t_mount.elapsed().as_millis() as u64,
        "mount: after mem_cache creation"
    );
    // Issue #268.2 O13: surface effective config at
    // startup. Pre-fix operators had to grep --help to
    // see what defaults were applied; with 50+ knobs
    // and many XDG/HOME fallbacks, the "what is the
    // daemon actually doing" question took 10 min to
    // answer on first incident. One info line at
    // mount startup answers it immediately.
    //
    // Level: info (not debug) so it appears in
    // RUST_LOG=info. Stripped on `mount --daemon`
    // for parity with `mntrs mount` foreground output.
    tracing::info!(
        // keep secret out of stderr (mode 0644 — captured by
        // launchd into macOS unified log).
        storage_url = %crate::util::redact_storage_url(storage_url),
        mountpoint = %mountpoint,
        volname = %volname,
        read_only,
        vfs_cache_max_size,
        mem_limit_mb = mem_limit / 1024 / 1024,
        dir_cache_time,
        attr_timeout,
        stat_cache_ttl,
        vfs_write_back,
        writeback_immediate_threshold,
        vfs_read_stale_on_backend_error,
        "mount: starting with effective config"
    );
    // Issue #128: the L2 block-cache index MUST be the same Arc
    // shared by `MntrsFs.disk_cache_index` (where the read path's
    // step6 inserts block entries) and `MultiLevelCache`'s
    // `DiskBlockCache` (where the write path's invalidate looks
    // them up to remove stale block files). Pre-fix these were two
    // separate `Arc::new(DashMap::new())` calls, so invalidate
    // always saw an empty map (`keys_found=0`), never removed the
    // stale `.block` files, and the next read served them —
    // "append to pre-existing file" returned the pre-append
    // content. Create the Arc once and pass it to both.
    let disk_cache_index: std::sync::Arc<
        dashmap::DashMap<crate::util::CacheKey, (u64, std::time::Instant)>,
    > = std::sync::Arc::new(dashmap::DashMap::new());
    let fs = MntrsFs {
        op: Arc::new(op),
        inodes: dashmap::DashMap::new(),
        path_to_ino: dashmap::DashMap::new(),
        lookup_count: dashmap::DashMap::new(),
        dir_cache: dashmap::DashMap::new(),
        cache_dir: cache_dir_path.clone(),
        handles: dashmap::DashMap::new(),
        // Issue #23: per-fh readdir snapshots. Empty
        // until opendir() populates an entry.
        dir_listers: dashmap::DashMap::new(),
        // Issue #38: empty pending set; populated on
        // first flush/release.
        writeback_pending: std::sync::Arc::new(dashmap::DashSet::new()),
        // Issue #325: in-memory symlink target table. Empty at
        // mount start; populated by `MntrsFs::symlink` when
        // user-mode code creates a symbolic link (Win32
        // `New-Item -ItemType SymbolicLink` → WinFSP
        // `set_reparse_point` → inner.symlink).
        symlinks: dashmap::DashMap::new(),
        // Issue #132: shared adaptive prefetch window controller.
        backpressure: std::sync::Arc::new(crate::backpressure::BackpressureController::new()),
        // Issue #201: per-mount prefetch budget. Same cap as
        // mem_cache_bytes; the budget is shared between in-flight
        // prefetch and the mem_cache, by design.
        mem_limiter: mem_limiter.clone(),
        dir_cache_ttl: std::time::Duration::from_secs(dir_cache_time),
        attr_ttl: std::time::Duration::from_secs(attr_timeout),
        stat_cache_ttl: std::time::Duration::from_secs(stat_cache_ttl),
        volname: volname.to_string(),
        cache_max_size: vfs_cache_max_size * 1024 * 1024,
        write_back_delay: std::time::Duration::from_secs(vfs_write_back),
        // Issue #202: small files skip the 5s delay queue.
        // The per_task_writeback_delay helper at lib.rs:900
        // uses inodes.size vs this threshold to decide
        // Duration::ZERO (immediate) vs write_back_delay
        // (5s batch). 0 disables the fast path entirely.
        writeback_immediate_threshold,
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
        direct_io,
        // Issue #257: opt-in stale-on-backend-error read
        // fallback. Default false. Users who want
        // "stale is better than EIO" semantics set
        // `--vfs-read-stale-on-backend-error`.
        read_stale_on_backend_error: vfs_read_stale_on_backend_error,
        // Issue #209: --poll-interval is deprecated; route the
        // legacy value into `cache_poll_interval` and warn the
        // user. None (unset) means use --vfs-cache-poll-interval.
        cache_poll_interval: std::time::Duration::from_secs(
            poll_interval.unwrap_or(vfs_cache_poll_interval).max(1),
        ),
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
        no_apple_double,
        no_apple_xattr,
        no_macos_metadata,
        block_norm_dupes: vfs_block_norm_dupes,
        // Issue #209: cache_poll_interval is set above (line 786-789)
        // to also accept the legacy --poll-interval value.
        handle_caching: std::time::Duration::from_secs(vfs_handle_caching),
        disk_total_size: vfs_disk_space_total_size * 1024 * 1024 * 1024 * 1024, // TB to bytes
        writeback_sender: std::sync::OnceLock::new(),
        // Unix-only — see MntrsFs::fuse_notifier in lib.rs.
        // The setter (set_fuse_notifier) is also unix-only and is
        // called from this same mount path immediately after this
        // struct literal. On Windows the field disappears and the
        // call site is also gated (issue #93).
        #[cfg(not(windows))]
        fuse_notifier: std::sync::OnceLock::new(),

        mem_cache: mem_cache.clone(),
        attr_cache: dashmap::DashMap::new(),
        disk_cache_index: disk_cache_index.clone(),
        multi_cache: {
            crate::multi_level_cache::MultiLevelCache::new(
                mem_cache.clone(),
                cache_dir_path.clone(),
                disk_cache_index.clone(),
                direct_io,
                crate::metrics::global(),
            )
        },
    };
    tracing::debug!(
        elapsed_ms = _t_mount.elapsed().as_millis() as u64,
        "mount: after MntrsFs construction"
    );

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
    // the actual mount. Parent exits AFTER the child signals mount-readiness
    // (POLLHUP on the parent's r_fd, fired when the child closes its copy of
    // w_fd after the FUSE session is up). Without this wait, the caller
    // (CI script / shell) races the mount: `mountpoint -q` runs before the
    // child has called spawn_mount2, and the test sees "Transport endpoint
    // is not connected" (issue #62).
    //
    // The child keeps its w_fd open until just after spawn_mount2 returns
    // (see the post-mount block below). The parent has already closed its
    // own w_fd above (so POLLHUP on the parent's r_fd fires iff the child
    // closes its w_fd).
    #[cfg(not(windows))]
    if daemon && std::env::var_os("MNTRS_INTERNAL_DAEMON").is_none() {
        // Close our own copy of the write end so POLLHUP on our read end
        // tracks the child's write-end closure (which the child does right
        // after a successful mount). Without this, both parent and child
        // hold the write end open and POLLHUP never fires.
        if let Some((_r, w)) = wait_pipe {
            unsafe {
                rustix::io::close(w);
            }
        }

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

        // Block in the parent until the child closes its w_fd (POLLHUP
        // on r_fd) or we hit the daemon-timeout. This is what makes
        // `--daemon --daemon-wait` actually wait for the mount to be
        // live before exit(0).
        if let Some((r, _w)) = wait_pipe {
            let deadline =
                std::time::Instant::now() + std::time::Duration::from_secs(_daemon_timeout);
            let mut signaled = false;
            while std::time::Instant::now() < deadline {
                let mut pfd = libc::pollfd {
                    fd: r,
                    events: libc::POLLHUP,
                    revents: 0,
                };
                // 250 ms tick — short enough to react near the deadline
                // and long enough not to busy-loop.
                let n = unsafe { libc::poll(&mut pfd, 1, 250) };
                if n > 0 && (pfd.revents & libc::POLLHUP) != 0 {
                    signaled = true;
                    break;
                }
            }
            if !signaled {
                eprintln!(
                    "mntrs: daemon did not signal mount readiness within {}s; \
                     child may have crashed. Check MNTRS_DAEMON_LOG if set.",
                    _daemon_timeout
                );
            }
            unsafe {
                rustix::io::close(r);
            }
        }
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
                Some(p) => {
                    // Issue #391 follow-up: the daemon log carries
                    // tracing output (file paths, error context,
                    // even a redacted storage_url still reveals
                    // bucket + host). Force mode 0o600 on unix so
                    // a peer user on a shared box can't `tail -f`
                    // the log. On non-unix we fall back to the
                    // original default-perms open.
                    #[cfg(unix)]
                    let open = {
                        use std::os::unix::fs::OpenOptionsExt;
                        std::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .mode(0o600)
                            .open(p.to_string_lossy().as_ref())
                    };
                    #[cfg(not(unix))]
                    let open = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(p.to_string_lossy().as_ref());
                    open.map(|f| {
                        use std::os::unix::io::IntoRawFd;
                        f.into_raw_fd()
                    })
                    .unwrap_or(-1)
                }
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
        // #361: the kernel-side mount option `writeback_cache` is the
        // legacy libfuse 2 syntax. Modern libfuse 3.7+ (incl. 3.17 on
        // ubuntu-24.04) honors `InitFlags::FUSE_WRITEBACK_CACHE`
        // declared in `FuserAdapter::init()` (src/core_fs/fuser.rs:142).
        // Passing the option name here makes `fusermount3` exit with
        // "unknown option 'writeback_cache'" before INIT happens, so
        // the kernel never gets a chance to enable per-inode writeback.
        //
        // We rely solely on the INIT capability flag now. The kernel
        // still controls per-inode writeback opt-in via the inode flag
        // returned in the lookup/create reply (kernel >= 4.18 honors
        // this; older kernels silently ignore it and fall back to
        // synchronous writes).
        //
        // The CLI `--write-back-cache` still exists for users who want
        // the small-write optimization; the `write_back_cache` field
        // on `FuserAdapter` is what gates the capability declaration.
        let _ = write_back_cache; // see FuserAdapter::init()
        // The macOS diagnostic for --write-back-cache is emitted at
        // the actual capability-declaration site (FuserAdapter::init),
        // not here, so library users / CSI drivers that bypass this
        // CLI wrapper still see it.
        if allow_root {
            cfg.mount_options
                .push(MountOption::CUSTOM("allow_root".to_string()));
        }
        #[cfg(target_os = "macos")]
        {
            if no_apple_double {
                cfg.mount_options
                    .push(MountOption::CUSTOM("noappledouble".to_string()));
            }
            if no_apple_xattr {
                cfg.mount_options
                    .push(MountOption::CUSTOM("noapplexattr".to_string()));
            }
            if mount_case_insensitive {
                cfg.mount_options
                    .push(MountOption::CUSTOM("case_insensitive".to_string()));
            }
            // Issue #464: Finder-friendly mount.
            //
            // `-o volname=<name>` makes Finder's sidebar and
            // `diskutil list` show a readable name instead of the
            // raw mountpoint path. Default derivation: `mntrs-<basename>`,
            // truncated to 64 chars (macFUSE hard limit on volume
            // name length; longer names silently fail with EINVAL).
            //
            // `-o local` marks the volume as local for macFUSE kernel
            // caching — repeated small-file ops (Finder Get Info,
            // QuickLook, Spotlight) hit a faster code path. Default
            // on for parity with sshfs / rclone; users with exotic
            // backends (rare case of stale-cache issues) can opt
            // out via `--no-finder-local`.
            let volname = derive_macos_volname(mountpoint, volume_name);
            cfg.mount_options
                .push(MountOption::CUSTOM(format!("volname={}", volname)));
            if finder_local {
                cfg.mount_options
                    .push(MountOption::CUSTOM("local".to_string()));
            }
            // Issue #466: macFUSE-specific kernel cache options.
            //
            // `-o negative_vncache` (default off): disables macFUSE's
            // 5-second negative-vnode cache. With it off, the kernel
            // re-asks the filesystem on every lookup, so freshly
            // deleted files disappear immediately in Finder instead
            // of sitting in cache for 5s. Costs an extra roundtrip
            // per failed lookup (negligible since we cache stat()
            // results in InodeEntry already).
            //
            // `-o auto_cache` (default off): kernel caches attributes
            // for the default attr_timeout (1s). Reduces stat() RTTs
            // for repeated small-file ops.
            //
            // `-o daemon_timeout=<N>`: macFUSE kills the mount if the
            // FUSE session is unresponsive for N seconds. Default 60s
            // matches macFUSE built-in default; CLI flag is the
            // surface for CI scripts (set 5s for fast-fail) and
            // heavy-backend warmup (set 300s).
            if negative_vncache {
                cfg.mount_options
                    .push(MountOption::CUSTOM("negative_vncache".to_string()));
            }
            if auto_cache {
                cfg.mount_options
                    .push(MountOption::CUSTOM("auto_cache".to_string()));
            }
            cfg.mount_options.push(MountOption::CUSTOM(format!(
                "daemon_timeout={}",
                daemon_timeout_macos
            )));
            // Issue #471: pass `-o slow_statfs` so the kernel
            // doesn't block Finder / Spotlight / diskutil on a slow
            // statfs roundtrip. Default true; rclone enables this
            // for every macFUSE mount for the same reason.
            if slow_statfs {
                cfg.mount_options
                    .push(MountOption::CUSTOM("slow_statfs".to_string()));
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
            write_back_cache,
        );
        tracing::info!(
            elapsed_ms = _t_mount.elapsed().as_millis() as u64,
            "mount: after FuserAdapter::new, about to call spawn_mount2"
        );
        let session = fuser::spawn_mount2(adapter, mount_path, &cfg)?;
        tracing::info!(
            elapsed_ms = _t_mount.elapsed().as_millis() as u64,
            "mount: fuser::spawn_mount2 returned (this is where fuser::session prints Mounting)"
        );
        // Store session so unmount_internal can gracefully shut it down
        // instead of leaking the daemon thread via std::mem::forget.
        // FUSE_SESSION is Unix-only (WinFSP uses a different teardown path).
        #[cfg(not(windows))]
        if let Ok(mut guard) = FUSE_SESSION.lock() {
            // #89: stash the kernel notifier so the write handler
            // can invalidate the kernel's attr cache after each
            // write — see `set_fuse_notifier` and the write path
            // tracing for details. ENOENT-class errors here just
            // mean the mount didn't reach FUSE_INIT yet (impossible
            // since spawn_mount2 just returned); treat any failure
            // as a soft no-op since the worst case is the kernel
            // using stale attrs for one more op.
            crate::set_fuse_notifier(session.notifier());
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

    // #245 BUG-1: declare `host` at function scope, not in an
    // inner `{}` block. The `{}` form dropped `host` at the
    // closing brace — `FileSystemHost`'s `Drop` impl calls
    // FspFileSystemRemoveMountPoint + FspIoqStop, so the
    // WinFSP volume was torn down before the keep-alive
    // loop (also dead-coded by the cfg nesting that
    // produced #245) could even run. Hoisting the
    // initializer to function scope means `host` lives
    // until `mount_internal` returns, which is when the
    // WinFSP-level unmount should happen.
    #[cfg(windows)]
    let host: winfsp::host::FileSystemHost<_, winfsp::host::FineGuard> = {
        use crate::core_fs::winfsp::WinFspAdapter;
        use std::sync::Arc;
        tracing::debug!(mountpoint, "mount(windows): constructing WinFspAdapter");
        let adapter = WinFspAdapter::new(Arc::new(fs));
        let mut vol_params = winfsp::host::VolumeParams::default();
        // Match rclone's VolumeParams so Explorer shows free/total space
        // the same way (rclone mount sets --FileSystemName and a 4096-byte
        // sector; without sector_size Explorer falls back to 512-byte
        // sectors and the progress bar disappears for our volume).
        // Issue #305 Tier 1: give the volume a real creation
        // time + serial number. fsutil fsinfo volumeinfo V:
        // reports 1970-01-01 00:00:00 + serial 0 when both are
        // zero, which Windows treats as "never assigned" — chkdsk,
        // Defender, and a few installers refuse to touch the
        // drive. Derive a stable non-zero serial from the
        // mountpoint path so two V: and W: mounts never collide.
        let volume_creation_time: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        // Stable hash: sum bytes; cast to u32. Two distinct
        // mountpoints at the same instant get distinct serials
        // because the mountpoint bytes are different. Re-mounting
        // the same mountpoint gets a fresh serial — accepted by
        // Win32 (the kernel only requires non-zero, not stable).
        let volume_serial_number: u32 = {
            let mut h: u32 = 0x811C9DC5; // FNV-1a 32-bit offset basis
            for b in mountpoint.as_bytes() {
                h ^= *b as u32;
                h = h.wrapping_mul(0x01000193); // FNV prime
            }
            // Force non-zero even if mountpoint is empty
            // (defensive — `mountpoint` is always non-empty here).
            if h == 0 { 1 } else { h }
        };
        vol_params
            .filesystem_name(volname)
            .sector_size(4096)
            .sectors_per_allocation_unit(1)
            .max_component_length(255)
            .volume_creation_time(volume_creation_time)
            .volume_serial_number(volume_serial_number)
            // Issue #305 Tier 1: bound the kernel-visible timeout
            // for hung backends. Driver defaults to 60s for every
            // IRP — a slow S3 GET freezes the whole V: drive for
            // the full minute, including unrelated readdir calls
            // (Explorer Refresh, Get-ChildItem). 10s for FileInfo
            // and 30s for DirInfo matches rclone's recommended
            // values and keeps the volume responsive.
            .file_info_timeout(10_000)
            .dir_info_timeout(30_000)
            // Issue #249 follow-up: enable the Win32 FS
            // features that cmd.exe / Explorer expect from a
            // "real" volume. Without these the kernel returns
            // errors for many user-mode operations — e.g.
            // cmd's `echo > V:\foo.txt` redirect fails
            // because the OS rejects an open with
            // FILE_NON_DIRECTORY_FILE on a volume that
            // doesn't claim to support streams, or a
            // write that needs a default ACL on a volume
            // that doesn't claim persistent ACLs. The
            // rclone mount uses the same set.
            .case_preserved_names(true)
            .case_sensitive_search(false)
            .unicode_on_disk(true)
            .persistent_acls(true)
            // Issue #309: advertise named-stream /
            // reparse-point / EA capability to the
            // kernel. The actual callbacks return
            // sensible defaults (see winfsp.rs) —
            // `get_stream_info` returns the unnamed
            // stream per file; reparse and EA
            // callbacks keep the trait default
            // (INVALID_DEVICE_REQUEST) because the
            // backend has no symlink or EA storage
            // yet. Enabling the flags here is
            // non-breaking: the kernel only calls
            // the reparse / EA callbacks when
            // user-mode requests them, and a
            // INVALID_DEVICE_REQUEST from the
            // callback is mapped to a normal "not
            // supported" error.
            .reparse_points(true)
            .named_streams(true)
            .extended_attributes(true)
            // Issue #305 Tier 1: advertise read-only at the
            // Win32 layer too, and skip post_cleanup for RO
            // mounts — there's nothing to clean up since
            // no writes are happening.
            .read_only_volume(read_only)
            .post_cleanup_when_modified_only(!read_only)
            .flush_and_purge_on_cleanup(false)
            .pass_query_directory_pattern(true)
            // Issue #309: enable the kernel-side
            // name query path. The adapter's
            // `get_dir_info_by_name` returns
            // INVALID_DEVICE_REQUEST today
            // (the trait default); the kernel
            // falls back to the per-pattern
            // enumeration path so this is
            // non-breaking.
            .pass_query_directory_filename(false);
        tracing::debug!(
            mountpoint,
            volname,
            "mount(windows): calling FileSystemHost::new_with_timer"
        );
        // Issue #360: `new_with_timer` instead of `new` so WinFSP's
        // threadpool timer polls the adapter's
        // `NotifyingFileSystemContext::should_notify` every 100 ms
        // and drains pending `FILE_ACTION_REMOVED` events into
        // the kernel. Without the timer, PowerShell / Explorer
        // never learn that a file was deleted and the per-volume
        // dir cache keeps reporting the deleted entry as
        // present (`Test-Path` returns True after Remove-Item).
        //
        // 100 ms is the rclone mount's notification interval;
        // small enough that a user typing `Remove-Item` sees the
        // change within ~1 frame, large enough that the timer
        // wakeup overhead is negligible on idle mounts.
        //
        // `new_with_timer` takes `FileSystemParams` (a superset
        // of `VolumeParams`) rather than `VolumeParams`; we
        // build it via `FileSystemParams::default_params(...)`
        // which mirrors the `FileSystemHost::new` defaults.
        let fs_params = winfsp::host::FileSystemParams::default_params(vol_params);
        // Annotate the type as `FileSystemHost<_, FineGuard>` so the
        // `start_with_threads` call below resolves to the FineGuard impl
        // (the CoarseGuard impl has the same signature and is also
        // visible, so unannotated inference fails with E0034).
        let mut host: winfsp::host::FileSystemHost<_, winfsp::host::FineGuard> =
            winfsp::host::FileSystemHost::new_with_timer::<_, 100>(fs_params, adapter)
                .map_err(|e| anyhow::anyhow!("FileSystemHost::new_with_timer: {e}"))?;
        tracing::debug!(
            mountpoint,
            "mount(windows): FileSystemHost::new returned; calling host.mount"
        );
        host.mount(mountpoint).map_err(|e| {
            // Issue #328: if the first `is_mount_point` poll raced
            // past the live volume and we made it to host.mount,
            // the Win32 kernel returns STATUS_OBJECT_NAME_COLLISION
            // (0xC0000035). Surface a clear, actionable error
            // naming the mountpoint instead of the raw NTSTATUS —
            // mount-test.ps1 sub-test 13 greps stderr for the
            // mountpoint path as part of its hard assertion.
            anyhow::anyhow!(
                "host.mount({mountpoint}) failed: {e}; \
                 the drive letter is likely already mounted by another process. \
                 Run `mntrs unmount {mountpoint}` first"
            )
        })?;
        tracing::debug!(
            mountpoint,
            "mount(windows): host.mount returned; calling host.start"
        );
        // Issue #328 sanity check: `host.mount` returned Ok, so
        // WinFSP thinks the drive letter is ours. Confirm that
        // `QueryDosDeviceW` also sees the DOS device now — if it
        // still reports ERROR_FILE_NOT_FOUND, something went wrong
        // in the kernel-side mountpoint table update and the
        // keep-alive loop below would hang without anyone able to
        // talk to V:. Fail loudly so the operator sees a clean
        // error instead of a silent hang.
        #[cfg(windows)]
        if !is_mount_point(mountpoint) {
            return Err(anyhow!(
                "host.mount({mountpoint}) returned Ok but the drive letter is \
                 not visible to QueryDosDeviceW; the WinFSP kernel-side \
                 mountpoint table may be in an inconsistent state. \
                 Try `mntrs unmount {mountpoint}` and remount"
            ));
        }
        // Issue #249 follow-up: winfsp 0.13 split the old
        // `FspFileSystemStart` step into two — `host.mount` calls
        // `FspFileSystemSetMountPoint` (registers V: with the WinFSP
        // driver / volume namespace) and `host.start_with_threads(0)`
        // calls `FspFileSystemStartDispatcher` (spawns the user-mode
        // dispatcher threads that service Win32 IRPs by calling back
        // into `FileSystemContext` methods). Pre-fix the mount code
        // only called `mount()`, so `fsptool lsvol` showed V:
        // registered but no IRP ever reached a callback — `dir V:\`,
        // `Test-Path V:\`, `fsutil fsinfo volumeinfo V:` all hung
        // forever at the kernel side, because the IRP sat in the
        // volume queue with no one to handle it.
        host.start_with_threads(winfsp_dispatcher_threads)
            .map_err(|e| anyhow::anyhow!("host.start: {e}"))?;
        tracing::debug!(
            mountpoint,
            "mount(windows): host.start returned; volume is live and dispatching"
        );
        // Issue #311: the FUSER path (mount.rs:1266+ block) calls
        // record_mount on Unix. The WinFSP path above is its own
        // cfg(windows) block and never reaches the Unix
        // record_mount site, so on Windows the mounts.txt entry
        // was never written. That broke `mntrs list` (always
        // empty) and the cross-process `mntrs unmount V:`
        // owning-PID lookup at unmount.rs:244-247. Add the call
        // here, immediately after the volume is live, so a
        // stale-DOS-device crash leaves a discoverable record.
        record_mount(storage_url, mountpoint, read_only);
        host
    };

    // Daemon-wait: signal mount readiness before entering daemon loop.
    // rclone-style: --daemon --daemon-wait means "fork, mount, signal, stay alive".
    // Without fork, we signal via pipe then enter the keep-alive loop.
    // CSI (daemon=false, daemon_wait=true): signal then return control to caller.
    //
    // Issue #286 root cause 3: in the re-exec'd child (MNTRS_INTERNAL_DAEMON=1),
    // the parent has already transferred both pipe fds to the child and the
    // child already closed its w_fd copy at line ~1370 (right after
    // spawn_mount2 returned, to fire parent's POLLHUP). Closing the same fd
    // again here triggers `rustix::io::close` -> EBADF -> panic
    // (`assertion failed: !(-4095..0).contains(&(self.raw as isize))` at
    // rustix 1.1.4 reg.rs:116) -> process exit(101). Also `close(r)` here
    // races with the parent's poll, sometimes firing POLLHUP prematurely.
    //
    // Fix: only run this block when we're NOT the re-exec'd child — i.e. when
    // the caller is the actual CSI/foreground path that owns its own wait_pipe.
    // The re-exec'd daemon path skips it entirely (parent closed its w_fd
    // before fork and we already closed ours at line 1370).
    #[cfg(not(windows))]
    if let Some((r, w)) = wait_pipe
        && std::env::var_os("MNTRS_INTERNAL_DAEMON").is_none()
    {
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
    // Issue #305 Tier 2: switch from `libc::signal` to
    // `sigaction` so the handler stays installed across the
    // first invocation. macOS's libc::signal uses System-V
    // semantics that reset the disposition to SIG_DFL on
    // handler return — a second Ctrl+C (the natural user
    // reaction when the first one appears to hang) silently
    // kills the daemon, skipping mountpoint unmount + leaving
    // mounts.txt stale. glibc 2.0+ switched to BSD-style
    // persistent handlers, so Linux never caught this. SA_RESTART
    // auto-restarts interrupted syscalls so the FUSE read loop
    // doesn't return EINTR after a Ctrl+C the user didn't mean
    // to send. On Windows this branch is unreachable (the
    // libc::signal call below was already a no-op there).
    unsafe {
        install_sigaction(libc::SIGINT, handler);
        install_sigaction(libc::SIGTERM, handler);
    }

    // Issue #305 Tier 1: route Windows console events through
    // SHUTDOWN_REQUESTED so the keep-alive loop can drain cleanly.
    // `libc::signal(SIGINT, ...)` above is a no-op on Windows for
    // console-attached processes (no SIGINT delivery from Ctrl+C;
    // SIGTERM is delivered by `taskkill /F` and Task Manager End
    // Task, which we still want to handle). The Win32 console
    // subsystem emits CTRL_C_EVENT / CTRL_BREAK_EVENT for keyboard
    // interrupts, and CTRL_CLOSE_EVENT / CTRL_LOGOFF_EVENT /
    // CTRL_SHUTDOWN_EVENT for window close + logoff + shutdown —
    // all of which previously hung the console until the user
    // closed the window.
    //
    // The handler returns TRUE so the OS does NOT terminate the
    // process; SHUTDOWN_REQUESTED is observed by the keep-alive
    // loop below, which calls `remove_mount(mountpoint)` and exits
    // cleanly. We intentionally register the same handler for all
    // five event types — same code path, same async semantics.
    #[cfg(windows)]
    {
        use windows::Win32::System::Console::{
            CTRL_BREAK_EVENT, CTRL_C_EVENT, CTRL_CLOSE_EVENT, CTRL_LOGOFF_EVENT,
            CTRL_SHUTDOWN_EVENT, SetConsoleCtrlHandler,
        };
        use windows::core::BOOL;

        unsafe extern "system" fn console_ctrl_handler(_ctrl_type: u32) -> BOOL {
            SHUTDOWN_REQUESTED.store(true, std::sync::atomic::Ordering::Relaxed);
            // TRUE = "I handled it; do not terminate me". The
            // keep-alive loop will observe SHUTDOWN_REQUESTED,
            // call remove_mount(mountpoint), and exit 0.
            BOOL(1)
        }

        // The add=false form means "do not add our handler to the
        // chain of handlers" — there's no other handler in this
        // add=true registers the handler; add=false would
        // unregister a previously-registered handler by function
        // pointer, which fails (ERROR_INVALID_PARAMETER 0x80070057)
        // when called with a handler that was never registered.
        let result = unsafe {
            SetConsoleCtrlHandler(
                Some(console_ctrl_handler),
                // SAFETY: the function pointer is `extern "system"`
                // with the PHANDLER_ROUTINE signature; Windows
                // stores it until the process exits (or until we
                // call SetConsoleCtrlHandler with None, which we
                // never do — the process exit path is the same as
                // removing the handler).
                true,
            )
        };
        if let Err(e) = result {
            tracing::warn!(
                error = %e,
                "mount(windows): SetConsoleCtrlHandler failed; Ctrl+C will not be observed by the keep-alive loop. \
                 The Win32 console subsystem will fall back to terminating the process."
            );
        }
        // Use the event-type constants so they remain referenced
        // even if the handler above changes signature. (Some
        // static analysers flag unused constants on Windows builds.)
        let _ = (
            CTRL_C_EVENT,
            CTRL_BREAK_EVENT,
            CTRL_CLOSE_EVENT,
            CTRL_LOGOFF_EVENT,
            CTRL_SHUTDOWN_EVENT,
        );
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
                // Issue #374: reuse the cfg-gated helpers from
                // `unmount`. Vanilla macOS has no `fusermount*`
                // binary, so the in-line shell-out was previously
                // silently failing and leaking the mount on
                // SIGINT/SIGTERM paths (the watcher thread is
                // what runs in the Ctrl-C case). The macOS
                // helper falls back to `umount(8)`; the
                // non-macos unix helper is byte-identical to
                // the pre-fix Linux chain.
                //
                // The signal-watcher `spawn()` block is inside
                // `mount_internal`, which is module-level
                // (compiles on Windows too) — same reason as
                // `cleanup()` above. Use `cfg(all(unix,
                // not(target_os = "macos")))` /
                // `cfg(target_os = "macos")` to keep Windows
                // untouched.
                #[cfg(all(unix, not(target_os = "macos")))]
                {
                    let _ = crate::cmd::unmount::fuse_unmount_via_fusermount(&mp);
                }
                #[cfg(target_os = "macos")]
                {
                    let _ = crate::cmd::unmount::fuse_unmount_macos_with_umount(&mp);
                }
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
        }
    }

    // #245 BUG-1: Windows keep-alive. Was previously nested
    // inside the `#[cfg(not(windows))]` block above (where
    // the inner `#[cfg(windows)]` loop was unreachable). With
    // `host` now hoisted to function scope (see the `let
    // mut host` block near the top of mount_internal), this
    // loop keeps the WinFSP process alive until
    // SHUTDOWN_REQUESTED is set — the signal handler sets
    // it, and the new BUG-2 IPC path (separate issue) will
    // also set it from the unmount command. On break, `host`
    // is dropped at function return, which triggers
    // FspFileSystemRemoveMountPoint + FspIoqStop in the
    // winfsp crate's Drop impl.
    #[cfg(windows)]
    {
        tracing::debug!(mountpoint, "mount(windows): entering keep-alive loop");
        while !SHUTDOWN_REQUESTED.load(std::sync::atomic::Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        tracing::debug!(
            mountpoint,
            "mount(windows): SHUTDOWN_REQUESTED=true, exiting keep-alive loop"
        );
    }

    // mounts.txt cleanup — applies to all platforms. Was
    // previously nested inside the Unix keep-alive block, so
    // Windows never cleaned up its mounts.txt entry.
    remove_mount(mountpoint);

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
            .layer(CapabilityCheckLayer::new())
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
            .layer(CapabilityCheckLayer::new())
            .finish()
    };
    Ok(op)
}

async fn build_operator(storage_url: &str, opts: &HashMap<String, String>) -> Result<Operator> {
    let url = url::Url::parse(storage_url).map_err(|e| {
        anyhow!(
            "invalid storage URL '{}': {e}",
            crate::util::redact_storage_url(storage_url)
        )
    })?;
    match url.scheme() {
        #[cfg(not(feature = "sftp"))]
        "sftp" => Err(anyhow!(
            "sftp support requires building with `--features sftp` (the openssh crate \
             doesn't compile on Windows; opt-in to keep default builds portable)"
        )),
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
        #[cfg(feature = "sftp")]
        "sftp" => build_sftp(&url, opts).await,
        "aliyun" | "aliyun-drive" => build_aliyun_drive(&url, opts).await,
        s => Err(anyhow!(
            "unsupported scheme '{s}'; try s3://, gs://, azblob://, hdfs://, webhdfs://, oss://, cos://, obs://, b2://, webdav://, sftp://"
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
    // #72: skip AWS SDK config probing at mount startup.
    // Without this, S3 builder probes ~/.aws/config, ~/.aws/credentials,
    // and IMDS in CI/containers that don't have these — adds 100-500ms
    // of stall per mount. We only do this when both access_key and
    // secret_key are explicitly provided (otherwise users relying on
    // env vars / IAM role / IMDS would break).
    let explicit_creds = opts.get("access-key").is_some() && opts.get("secret-key").is_some();
    if explicit_creds {
        builder = builder.disable_config_load();
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
    // #76: explicit checksum algorithm. Default is None (opendal picks
    // CRC32C for AWS S3, which adds server-side validation overhead
    // on MinIO/local S3 where it's not needed). Users can force a
    // specific algorithm (CRC32C/CRC32/SHA1/SHA256) or pass an empty
    // string to disable checksum PUT headers entirely.
    if let Some(v) = opts.get("checksum-algorithm") {
        builder = builder.checksum_algorithm(v);
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
            // N-7 fix: tell users that hdfs-jni doesn't support
            // dfs.* options; use hdfs-native for Kerberos with
            // principal/keytab config.
            k if k.starts_with("dfs.") => {
                tracing::warn!(
                    "hdfs-jni does not support {k}={v}; \
                     use hdfs:// (hdfs-native) for Kerberos principal/keytab"
                );
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

/// Install a signal handler that persists across invocations.
///
/// Issue #305 Tier 2: `libc::signal()` on macOS uses System-V
/// semantics that reset the disposition to `SIG_DFL` on handler
/// return. A second Ctrl+C — the natural user reaction when the
/// first one appears to hang — silently kills the daemon, skipping
/// the FUSE unmount and leaving a stale `mounts.txt` entry behind.
/// glibc 2.0+ switched to BSD-style persistent handlers, so Linux
/// never caught this. `sigaction` with `SA_RESTART` gives BSD-style
/// persistence plus auto-restart of interrupted syscalls (so a
/// Ctrl+C the user didn't mean to send doesn't bubble EINTR up to
/// the FUSE read loop).
///
/// Gated on `#[cfg(unix)]`: `libc::sigaction` / `sigemptyset` /
/// `sigaddset` / `SA_RESTART` are POSIX-only and don't exist in the
/// libc crate's Windows target. Windows is unaffected by the
/// System-V-vs-BSD issue (it never used libc::signal for console
/// events — those go through `windows::Win32::System::Console`,
/// which is wired up just above the call site), so the Windows
/// fallback delegates to the original `libc::signal` call to keep
/// behaviour byte-identical to pre-fix.
///
/// # Safety
///
/// `signo` must be a valid signal number (typically `SIGINT` /
/// `SIGTERM`). `handler` must be a pointer to a function with the
/// `extern "C" fn(i32)` ABI that is safe to call from a signal
/// context — only async-signal-safe operations inside.
#[cfg(unix)]
unsafe fn install_sigaction(signo: i32, handler: extern "C" fn(i32)) {
    // SAFETY: `sa_sigaction` is the C function-pointer field of
    // `sigaction`; the libc bindings require a `sighandler_t` (a
    // pointer-sized fn-ptr cast). `libc::SIG_DFL` / `SIG_IGN` are
    // the two magic values; supplying a real fn-ptr is the
    // standard "install handler" idiom.
    let handler_ptr = handler as *const () as libc::sighandler_t;
    // SAFETY: zeroing a `libc::sigaction` is the standard
    // initialization idiom — every field has a meaningful zero
    // (`sa_mask` = empty set, `sa_flags` = 0, `sa_sigaction` =
    // `SIG_DFL`). Edition 2024 requires explicit `unsafe { }`
    // around unsafe ops even inside an `unsafe fn`.
    let mut sa: libc::sigaction = unsafe { std::mem::zeroed() };
    sa.sa_sigaction = handler_ptr;
    // SAFETY: `sa_mask` is a `sigset_t`; zero-initializing it via
    // `sigemptyset` is the standard idiom for "no extra signals
    // masked during handler execution". `sigaddset` is the only
    // mutator we need.
    unsafe {
        libc::sigemptyset(&mut sa.sa_mask);
        // Don't block SIGTERM while handling SIGINT (or vice
        // versa) — a user pressing Ctrl+C twice in quick
        // succession would otherwise delay the second delivery
        // by up to one signal mask re-evaluation.
        libc::sigaddset(&mut sa.sa_mask, libc::SIGINT);
        libc::sigaddset(&mut sa.sa_mask, libc::SIGTERM);
    }
    // SA_RESTART: auto-restart interrupted syscalls so the FUSE
    // read/write loop doesn't return EINTR to callers after a
    // benign Ctrl+C. Without it, every read/write needs an
    // explicit EINTR retry, and a missed EINTR surfaces as a
    // spurious short-read to the application.
    sa.sa_flags = libc::SA_RESTART;
    // SAFETY: `signo` is validated by the caller; `sa` is fully
    // initialized (zeroed, sigaction set, mask set, flags set).
    // `libc::sigaction` returns 0 on success and -1 on error; we
    // don't propagate the failure because a non-critical signal
    // installation shouldn't fail the mount — pre-fix the
    // `libc::signal` call also ignored its return value.
    unsafe {
        let ret = libc::sigaction(signo, &sa, std::ptr::null_mut());
        if ret != 0 {
            tracing::warn!(
                signo,
                error = %std::io::Error::last_os_error(),
                "install_sigaction failed; falling back to default disposition"
            );
        }
    }
}

/// Windows fallback for `install_sigaction`. The POSIX signal API
/// doesn't exist on Windows (no `sigaction` / `sigemptyset` /
/// `SA_RESTART` in the libc crate's Windows target). Windows
/// console events go through `windows::Win32::System::Console`
/// (wired up just above the call site in `mount_internal`), so
/// this is effectively a no-op stub that preserves the original
/// `libc::signal` semantics for the rare `taskkill /F` path that
/// does deliver a real `SIGTERM` to a non-console-attached
/// process. Behaviour is byte-identical to pre-fix.
#[cfg(not(unix))]
unsafe fn install_sigaction(signo: i32, handler: extern "C" fn(i32)) {
    unsafe {
        libc::signal(signo, handler as *const () as libc::sighandler_t);
    }
}

async fn build_webdav(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    // Prefer explicit --opt endpoint over URL-derived host.
    // When no endpoint opt is given, derive from the URL host
    // but use http:// scheme (opendal's WebDAV service expects
    // http:// or https://, not webdav://).
    let endpoint = if let Some(ep) = opts.get("endpoint") {
        ep.clone()
    } else {
        let host = url.host_str().unwrap_or("localhost");
        let port = url.port().map(|p| format!(":{p}")).unwrap_or_default();
        format!("http://{host}{port}")
    };
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

/// Build an SFTP operator from the URL and options.
///
/// Usage:
///   mntrs mount sftp://host:22/remote/path /mnt --opt user=root --opt key=/root/.ssh/id_rsa
///   mntrs mount sftp://host/remote/path /mnt --opt user=admin --opt password=secret
///
/// Options (via --opt):
///   user                 — SSH username (default: current user)
///   key                  — path to SSH private key file
///   known_hosts_strategy — "accept" to skip host key verification (default: strict)
#[cfg(feature = "sftp")]
async fn build_sftp(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let host = url.host_str().unwrap_or("localhost");
    let port = url.port().unwrap_or(22);
    // opendal SFTP expects the endpoint in `ssh://[user@]host[:port]`
    // format (same as openssh). A bare `host:port` is passed to ssh as
    // the destination argument, which ssh then tries to resolve as a
    // hostname (failing with "Could not resolve hostname host:port").
    let endpoint = format!("ssh://{host}:{port}");
    let mut builder = Sftp::default().endpoint(&endpoint);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() {
        builder = builder.root(p);
    }
    if let Some(v) = opts.get("user") {
        builder = builder.user(v);
    }
    if let Some(v) = opts.get("key") {
        builder = builder.key(v);
    }
    if let Some(v) = opts.get("known_hosts_strategy") {
        builder = builder.known_hosts_strategy(v);
    }
    // SFTP uses SSH transport (no TLS layer needed).
    Ok(Operator::new(builder)?.finish())
}

#[cfg(test)]
mod tests_261_2 {
    //! Tests for Issue #261.2: CSI cache-dir propagation.
    //!
    //! The helpers here (`cache_dir_for_mount`, `MOUNT_CACHE_DIR` map
    //! lookup, mountpoint→cache_dir registration) are the wiring that
    //! makes `mount_internal` honor the CSI-supplied `opts["cache-dir"]`
    //! and `unmount_internal` clean up the right path.
    //!
    //! What we test:
    //! 1. `cache_dir_for_mount` falls back to `/tmp/mntrs-csi-cache/<slug>`
    //!    for CLI mounts (unchanged behavior).
    //! 2. The `MOUNT_CACHE_DIR` map round-trips the mountpoint→cache_dir
    //!    insertion that `mount_internal` performs.
    //! 3. The map's lookup-or-helper-fallback logic in `unmount_internal`
    //!    correctly prefers the map entry over the helper.
    //!
    //! We do NOT call `mount_internal`/`unmount_internal` directly here:
    //! both spawn FUSE sessions / fork threads, which can't run in a
    //! `cargo test` unit-test harness. The cache-dir plumbing is small
    //! enough that helper-level tests give us the safety net we need.

    use super::{MOUNT_CACHE_DIR, cache_dir_for_mount};

    /// Sanity: the legacy fallback path is unchanged from pre-#261.2.
    /// CLI `mntrs mount /a/pvc` still gets the canonical cache
    /// directory. Pre-Issue #382 the path was `/tmp/mntrs-csi-cache/...`
    /// on every platform; post-#382 macOS uses the per-user `$TMPDIR`
    /// base (cross-user temp-leak fix) while Linux / other unix keep
    /// the original `/tmp` base.
    #[test]
    fn cache_dir_for_mount_cli_fallback_unchanged() {
        let path = cache_dir_for_mount("/a/pvc-1/globalmount");
        #[cfg(target_os = "macos")]
        let expected = {
            let base = std::env::var("TMPDIR")
                .unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().to_string());
            format!(
                "{}/mntrs-csi-cache/_a_pvc-1_globalmount",
                base.trim_end_matches('/')
            )
        };
        #[cfg(not(target_os = "macos"))]
        let expected = "/tmp/mntrs-csi-cache/_a_pvc-1_globalmount";
        assert_eq!(path, expected);
    }

    /// Mountpoint with drive-letter colon (Windows-path-style mountpoint).
    /// Replacement turns `:` into `_` so the suffix is fs-safe.
    /// (Issue #382 only changes the base directory; the suffix
    /// derivation is the same on every platform.)
    #[test]
    fn cache_dir_for_mount_colon_replaced() {
        let path = cache_dir_for_mount("C:/mnt");
        #[cfg(target_os = "macos")]
        let expected = {
            let base = std::env::var("TMPDIR")
                .unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().to_string());
            format!("{}/mntrs-csi-cache/C__mnt", base.trim_end_matches('/'))
        };
        #[cfg(not(target_os = "macos"))]
        let expected = "/tmp/mntrs-csi-cache/C__mnt";
        assert_eq!(path, expected);
    }

    /// Issue #382: on macOS the cache-dir base is `$TMPDIR` (per-user,
    /// mode 0700 under `/var/folders/.../T/`), not `/tmp` (which
    /// resolves to `/private/tmp`, mode 1777, shared across users).
    /// This test asserts the contract: the returned path's base is
    /// NOT `/tmp` on macOS.
    #[cfg(target_os = "macos")]
    #[test]
    fn cache_dir_for_mount_macos_uses_per_user_temp() {
        let path = cache_dir_for_mount("/some/mount");
        // Sanity: must not leak into the shared /tmp tree.
        assert!(
            !path.starts_with("/tmp/"),
            "macOS cache_dir_for_mount must not start with /tmp (got {path})"
        );
        // Sanity: must land under TMPDIR (or temp_dir fallback) and
        // carry the canonical suffix.
        let expected_base = std::env::var("TMPDIR")
            .unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().to_string());
        let expected_prefix = format!("{}/mntrs-csi-cache/", expected_base.trim_end_matches('/'));
        assert!(
            path.starts_with(&expected_prefix),
            "macOS cache_dir_for_mount must start with {expected_prefix} (got {path})"
        );
    }

    /// Issue #261.2 core: register mountpoint→cache_dir in the global
    /// map and read it back. This mirrors what mount_internal does
    /// for CSI: `opts["cache-dir"]` value gets registered, then
    /// unmount_internal looks it up.
    #[test]
    fn mount_cache_dir_register_and_lookup() {
        let mp = "/tmp/test-mount-261-2";
        let cs = "/var/lib/mntrs/cache/volume-xyz";
        // Clear any leftover entry from previous tests.
        if let Ok(mut map) = MOUNT_CACHE_DIR.lock() {
            map.remove(mp);
        }
        // Register (mirrors mount_internal L287-290).
        if let Ok(mut map) = MOUNT_CACHE_DIR.lock() {
            map.insert(mp.to_string(), cs.to_string());
        }
        // Lookup (mirrors unmount_internal L440-449).
        let resolved = if let Ok(map) = MOUNT_CACHE_DIR.lock() {
            map.get(mp).cloned()
        } else {
            None
        }
        .unwrap_or_else(|| cache_dir_for_mount(mp));
        assert_eq!(resolved, cs);
        // Cleanup.
        if let Ok(mut map) = MOUNT_CACHE_DIR.lock() {
            map.remove(mp);
        }
    }

    /// Lookup miss falls back to the legacy helper. Mounts that bypassed
    /// mount_internal (or stale maps after a crash) still get a sensible
    /// cleanup target.
    #[test]
    fn mount_cache_dir_miss_falls_back_to_helper() {
        let mp = "/tmp/never-registered-mountpoint";
        // Make sure nothing is registered.
        if let Ok(mut map) = MOUNT_CACHE_DIR.lock() {
            map.remove(mp);
        }
        let resolved = if let Ok(map) = MOUNT_CACHE_DIR.lock() {
            map.get(mp).cloned()
        } else {
            None
        }
        .unwrap_or_else(|| cache_dir_for_mount(mp));
        assert_eq!(resolved, cache_dir_for_mount(mp));
        #[cfg(target_os = "macos")]
        let expected_suffix = {
            let base = std::env::var("TMPDIR")
                .unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().to_string());
            format!(
                "{}/mntrs-csi-cache/_tmp_never-registered-mountpoint",
                base.trim_end_matches('/')
            )
        };
        #[cfg(not(target_os = "macos"))]
        let expected_suffix = "/tmp/mntrs-csi-cache/_tmp_never-registered-mountpoint";
        assert_eq!(resolved, expected_suffix);
    }

    /// Map entry wins even if helper would compute a different value.
    /// Critical: CSI passes a path totally unrelated to mountpoint
    /// slug, so the helper MUST NOT override the map.
    #[test]
    fn mount_cache_dir_entry_wins_over_helper() {
        let mp = "/mnt/csi/pvc-abc";
        // Helper would compute: the suffix is `_mnt_csi_pvc-abc` on
        // every platform; the base directory differs on macOS
        // (Issue #382) so we accept either `/tmp/mntrs-csi-cache/` or
        // `<TMPDIR>/mntrs-csi-cache/` as the helper output.
        let helper_path = cache_dir_for_mount(mp);
        assert!(
            helper_path.ends_with("/mntrs-csi-cache/_mnt_csi_pvc-abc"),
            "helper path should carry canonical suffix (got {helper_path})"
        );
        #[cfg(target_os = "macos")]
        assert!(
            !helper_path.starts_with("/tmp/"),
            "macOS helper must not land in shared /tmp (got {helper_path})"
        );
        #[cfg(not(target_os = "macos"))]
        assert!(
            helper_path.starts_with("/tmp/mntrs-csi-cache/"),
            "non-macOS helper should land in /tmp/mntrs-csi-cache (got {helper_path})"
        );
        // CSI registers: /var/lib/mntrs/cache/<encoded_volume_id>
        let csi_path = "/var/lib/mntrs/cache/_s3_bucket_prefix";
        if let Ok(mut map) = MOUNT_CACHE_DIR.lock() {
            map.insert(mp.to_string(), csi_path.to_string());
        }
        let resolved = if let Ok(map) = MOUNT_CACHE_DIR.lock() {
            map.get(mp).cloned()
        } else {
            None
        }
        .unwrap_or_else(|| cache_dir_for_mount(mp));
        assert_eq!(resolved, csi_path);
        assert_ne!(resolved, helper_path);
        // Cleanup.
        if let Ok(mut map) = MOUNT_CACHE_DIR.lock() {
            map.remove(mp);
        }
    }

    // ── Issue #384: record_mount / remove_mount agree on canonical form ──
    //
    // Pre-#384: `record_mount` wrote the raw user-supplied mountpoint to
    // `mounts.txt`, and `remove_mount` filtered on byte-equality against
    // the raw user-supplied mountpoint. On macOS `/tmp/foo` and
    // `/private/tmp/foo` are byte-different, so a mount-via-one-form /
    // unmount-via-the-other-form round trip left a stale entry.
    //
    // These tests build a symlink under a tempdir, register the mount
    // through one of the two forms, then ask `remove_mount` to clean it
    // up via the *other* form. Post-#384 both callsites canonicalize, so
    // the row matches and the file ends up empty (or at least stripped
    // of the registered row).
    //
    // We redirect `HOME` to the tempdir so the `mounts_db()` path lands
    // inside it — `mounts_db()` consults `HOME` / `XDG_DATA_HOME` and we
    // don't want to pollute the developer's real `~/.local/share/mntrs/`.

    use super::{record_mount, remove_mount};
    // `std::os::unix::fs::symlink` is only available on unix. The
    // two tests that use it are already `#[cfg(unix)]`-gated, but
    // the `use` itself needs its own gate so a Windows build
    // doesn't choke on the unresolved import (CI run 28526350657).
    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    /// Mount via the canonical form, remove via the symlinked form.
    /// Post-fix this must succeed (the recorded row is in canonical
    /// form; the filter canonicalizes the request too — they match).
    #[cfg(unix)]
    #[test]
    fn record_remove_symlink_agreement_canonical_then_symlinked() {
        let _env_lock = crate::util::tests_env_mutex();
        let home = tempfile::tempdir().unwrap();
        // SAFETY: set_var/remove_var are unsafe in recent rustc. The
        // mutex above serializes against any other test touching HOME.
        unsafe {
            std::env::set_var("HOME", home.path());
            std::env::remove_var("XDG_DATA_HOME");
        }
        // Two real dirs joined by a symlink, mimicking /tmp → /private/tmp.
        let real = home.path().join("real");
        let link_dir = home.path().join("link");
        std::fs::create_dir(&real).unwrap();
        std::fs::create_dir(&link_dir).unwrap();
        let target = real.join("mnt");
        let alias = link_dir.join("mnt");
        std::fs::create_dir(&target).unwrap();
        symlink(&target, &alias).unwrap();

        // Canonicalize the target up-front. On macOS `tempdir()` lives
        // under `/var/folders/...` which is itself a symlink to
        // `/private/var/folders/...` — `record_mount` runs
        // `fs::canonicalize` on the input, so the recorded row will
        // be the canonical form, not the raw `target` string. Assert
        // against the canonical form so the test reflects what the
        // implementation actually writes.
        let canon = std::fs::canonicalize(&target)
            .unwrap()
            .to_string_lossy()
            .to_string();

        // Record using the canonical form (real path).
        record_mount("memory:///", target.to_str().unwrap(), false);
        let db = crate::cmd::mount::mounts_db_path();
        let contents = std::fs::read_to_string(&db).unwrap();
        assert!(
            contents
                .lines()
                .any(|l| l.split('\0').nth(1) == Some(canon.as_str())),
            "recorded row should carry canonical mountpoint {canon:?} (got {contents:?})"
        );
        // Remove using the symlinked form (alias). Pre-#384 this would
        // miss because the recorded row is canonical but the filter
        // looks for the raw symlinked form.
        remove_mount(alias.to_str().unwrap());
        let after = std::fs::read_to_string(&db).unwrap_or_default();
        assert!(
            !after
                .lines()
                .any(|l| l.split('\0').nth(1) == Some(canon.as_str())),
            "remove_mount must strip the canonical row even when called with the symlinked form (after: {after:?})"
        );
    }

    /// Mount via the symlinked form, remove via the canonical form.
    /// Same shape as the previous test, exercising the opposite
    /// direction. After #384 both ends canonicalize, so the row
    /// matches in either order.
    #[cfg(unix)]
    #[test]
    fn record_remove_symlink_agreement_symlinked_then_canonical() {
        let _env_lock = crate::util::tests_env_mutex();
        let home = tempfile::tempdir().unwrap();
        unsafe {
            std::env::set_var("HOME", home.path());
            std::env::remove_var("XDG_DATA_HOME");
        }
        let real = home.path().join("real");
        let link_dir = home.path().join("link");
        std::fs::create_dir(&real).unwrap();
        std::fs::create_dir(&link_dir).unwrap();
        let target = real.join("mnt");
        let alias = link_dir.join("mnt");
        std::fs::create_dir(&target).unwrap();
        symlink(&target, &alias).unwrap();

        // Record using the symlinked form (alias). `record_mount`
        // canonicalizes internally, so the row stored is the
        // canonical form.
        record_mount("memory:///", alias.to_str().unwrap(), false);
        let db = crate::cmd::mount::mounts_db_path();
        let contents = std::fs::read_to_string(&db).unwrap();
        // `record_mount` canonicalizes; both the symlinked and the
        // canonical inputs collapse to the same stored form.
        let canon = std::fs::canonicalize(&target)
            .unwrap()
            .to_string_lossy()
            .to_string();
        assert!(
            contents
                .lines()
                .any(|l| l.split('\0').nth(1) == Some(canon.as_str())),
            "recorded row should carry canonical mountpoint {canon:?} (got {contents:?})"
        );
        // Remove using the canonical form directly. Should match.
        remove_mount(&canon);
        let after = std::fs::read_to_string(&db).unwrap_or_default();
        assert!(
            !after
                .lines()
                .any(|l| l.split('\0').nth(1) == Some(canon.as_str())),
            "remove_mount(canonical) must strip the row (after: {after:?})"
        );
    }
}

// ─── macFUSE kext diagnostic (issue #410) ──────────────────────────
//
// At the start of every macOS mount, `mount_internal` calls
// `macfuse_kext_loaded()` to surface a one-line info / warn about
// the macFUSE kext state. Without this, a user running `mntrs
// mount` on a system without the kext loaded (or with the
// post-install Approval step pending) gets an opaque mount
// failure with no hint about which step is missing.
//
// The shell-out (`kextstat`) is on every mount attempt but is
// cheap (<10 ms in practice). `macfuse_kext_loaded` is not
// directly unit-testable (depends on `kextstat` being present +
// actual kext state), but `parse_macos_kext_version` is a pure
// function over the stdout string, so the parsing logic is
// fully covered by the test module below.

/// Return `Some(version)` if the macFUSE kext is currently loaded,
/// `None` otherwise. Uses `kextstat -l` (loaded kexts only) and
/// scans the output for any line whose Name field contains
/// "macfuse" — covering both macFUSE 4.x bundle IDs
/// (`com.google.macfuse.filesystems.macfuse`) and 5.x
/// (`io.macfuse.filesystems.macfuse`).
///
/// Failure modes that resolve to `None`:
/// - `kextstat` not on `$PATH` (very rare — ships in
///   `/usr/sbin/` which is in the default PATH).
/// - `kextstat` exits non-zero (e.g. SIP disabled, in which case
///   the user has bigger problems and a `None` is fine — they
///   can still see the warn message).
/// - The kext is not loaded (the normal "fix the install" path).
/// - `kextstat` output doesn't parse (we return `None` rather
///   than panic on a malformed line — log a wrong version is
///   worse than log a missing-version warning).
#[cfg(target_os = "macos")]
fn macfuse_kext_loaded() -> Option<String> {
    use std::process::Stdio;
    use std::time::Duration;

    let mut child = std::process::Command::new("kextstat")
        .arg("-l")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;

    // Bound the diagnostic so a wedged `kextstat` (kernel-extension
    // bus hung, buggy binary, future sandboxing change) cannot block
    // mount startup indefinitely. 1 s is well above the observed
    // <10 ms cold path; if it ever legitimately takes longer the
    // user just gets the no-kext warning (the mount itself is
    // unaffected — this check is informational only).
    //
    // Implemented as a manual `try_wait` poll loop instead of via the
    // `wait-timeout` crate to keep the dep graph minimal (this is the
    // only timeout-bounded `Child` call site in the codebase). 10 ms
    // poll cadence caps the loop at ~100 iterations before the deadline.
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    let status = loop {
        match child.try_wait() {
            Ok(Some(s)) => break s,
            Ok(None) if std::time::Instant::now() >= deadline => {
                // Timed out — kill the child and bail out without a
                // version (the mount will still proceed and the user
                // will see the standard no-kext warning).
                let _ = child.kill();
                let _ = child.wait();
                tracing::warn!("kextstat did not exit within 1s; assuming macFUSE kext not loaded");
                return None;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(10)),
            Err(_) => return None,
        }
    };
    if !status.success() {
        return None;
    }
    let output = child.wait_with_output().ok()?;
    parse_macos_kext_version(&String::from_utf8_lossy(&output.stdout))
}

/// Pure parser over `kextstat` stdout. `kextstat -l` output format
/// (modern macOS):
///
/// ```text
/// Index Refs Address            Size       Wired      Name (Version) UUID <Linked Against>
///     1    0 0xffffff7f8a1b0000 0x14000    0x14000    io.macfuse.filesystems.macfuse (5.1.3) ...
/// ```
///
/// For each line that contains "macfuse" (covers both 4.x and
/// 5.x bundle IDs), extract the substring inside the trailing
/// `(...)`. `rsplit_once('(')` / `split_once(')')` instead of
/// manual index arithmetic because the address fields don't
/// contain parens and the line layout is otherwise variable
/// (kextstat output width changes between macOS releases).
#[cfg(target_os = "macos")]
fn parse_macos_kext_version(stdout: &str) -> Option<String> {
    for line in stdout.lines() {
        if !line.contains("macfuse") {
            continue;
        }
        // `rsplit_once('(')` anchors on the LAST `(` (the
        // version one), so any `(` that might appear in the
        // address / UUID fields to its left is ignored.
        let (_, after_open) = line.rsplit_once('(')?;
        let (version, _) = after_open.split_once(')')?;
        return Some(version.to_string());
    }
    None
}

/// Derive the macOS volume name shown in Finder / `diskutil list`.
///
/// Priority:
/// 1. `--volume-name <NAME>` if provided — used verbatim (caller has
///    already opted into the override; we do not silently mangle it
///    beyond the 64-char macFUSE truncation, since users picking a
///    custom name generally know what they want).
/// 2. Otherwise: `mntrs-<basename(mountpoint)>`, truncated to 64 chars.
///    The basename is the path's last segment with leading `/` and any
///    trailing slashes stripped. For a mountpoint of `/` (root, weird
///    but legal) the basename is empty, so we fall back to `mntrs-root`.
///
/// **macFUSE hard limit:** volume names longer than 64 chars are
/// rejected by the kernel at mount time with `EINVAL`. Truncation
/// keeps the mount working; the visible truncation in Finder is the
/// cost of avoiding a cryptic error.
///
/// Issue #464.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn derive_macos_volname(mountpoint: &str, override_name: Option<&str>) -> String {
    const MAX_VOLNAME_LEN: usize = 64;
    if let Some(name) = override_name {
        // Custom override: respect user choice but still cap at
        // the macFUSE limit so a typo doesn't blow up the mount.
        if name.len() > MAX_VOLNAME_LEN {
            return name.chars().take(MAX_VOLNAME_LEN).collect();
        }
        return name.to_string();
    }
    // Auto-derive: take the last path segment, strip trailing slashes,
    // prefix with `mntrs-`. Edge cases:
    //   * `/`                  → basename ""     → "mntrs-root"
    //   * `/Volumes/foo`       → basename "foo"  → "mntrs-foo"
    //   * `/Volumes/foo/`      → basename "foo"  → "mntrs-foo"
    //   * `foo` (relative)     → basename "foo"  → "mntrs-foo"
    let trimmed = mountpoint.trim_end_matches('/');
    let basename = trimmed.rsplit('/').next().unwrap_or("");
    let candidate = if basename.is_empty() {
        "mntrs-root".to_string()
    } else {
        format!("mntrs-{}", basename)
    };
    if candidate.len() > MAX_VOLNAME_LEN {
        candidate.chars().take(MAX_VOLNAME_LEN).collect()
    } else {
        candidate
    }
}

#[cfg(all(target_os = "macos", test))]
mod kext_tests {
    use super::parse_macos_kext_version;

    /// macFUSE 5.x line — bundle ID `io.macfuse.filesystems.macfuse`,
    /// version `5.1.3`. Real kextstat output observed on a stock
    /// macOS 15.4 host with macFUSE 5.1.3 installed.
    #[test]
    fn parse_macfuse_5x() {
        let stdout = "\
Index Refs Address            Size       Wired      Name (Version) UUID <Linked Against>
    1    0 0xffffff7f8a1b0000 0x14000    0x14000    io.macfuse.filesystems.macfuse (5.1.3) ABCDEF12-3456-7890-ABCD-EF1234567890 <1 2 3 4 5 6 7>";
        assert_eq!(parse_macos_kext_version(stdout), Some("5.1.3".to_string()));
    }

    /// macFUSE 4.x line — bundle ID `com.google.macfuse.filesystems.macfuse`,
    /// version `4.8.1`. Regression test for old installs that the
    /// `contains("macfuse")` filter covers.
    #[test]
    fn parse_macfuse_4x() {
        let stdout = "\
Index Refs Address            Size       Wired      Name (Version) UUID <Linked Against>
  147    2 0xffffff7f8a1b0000 0x14000    0x14000    com.google.macfuse.filesystems.macfuse (4.8.1) <1 2>";
        assert_eq!(parse_macos_kext_version(stdout), Some("4.8.1".to_string()));
    }

    /// Empty kextstat output (no kexts loaded — sandboxed env).
    #[test]
    fn parse_empty_returns_none() {
        assert_eq!(parse_macos_kext_version(""), None);
    }

    /// Header-only output with no data lines.
    #[test]
    fn parse_header_only_returns_none() {
        let stdout = "Index Refs Address            Size       Wired      Name (Version) UUID <Linked Against>\n";
        assert_eq!(parse_macos_kext_version(stdout), None);
    }

    /// Unrelated kexts loaded but no macFUSE — must return None
    /// without false-positive on a similar-looking bundle ID.
    #[test]
    fn parse_unrelated_kexts_returns_none() {
        let stdout = "\
Index Refs Address            Size       Wired      Name (Version) UUID <Linked Against>
   10    0 0xffffff7f80a00000 0x9000     0x9000     com.apple.iokit.IOACPIFamily (1.4) <1 2>";
        assert_eq!(parse_macos_kext_version(stdout), None);
    }

    /// Line that contains "macfuse" but lacks a `(` — a corner
    /// case in some debug logging output. Must not panic.
    #[test]
    fn parse_no_paren_returns_none() {
        let stdout = "io.macfuse.filesystems.macfuse missing version\n";
        assert_eq!(parse_macos_kext_version(stdout), None);
    }

    /// Line that contains `(...)` but no macfuse substring.
    /// Must not match.
    #[test]
    fn parse_paren_without_macfuse_returns_none() {
        let stdout = "com.apple.iokit.IOACPIFamily (1.4) <1 2>\n";
        assert_eq!(parse_macos_kext_version(stdout), None);
    }

    /// Malformed: `(` but no `)` after it. Must not panic, must
    /// return None rather than producing an empty version string.
    #[test]
    fn parse_unclosed_paren_returns_none() {
        let stdout = "io.macfuse.filesystems.macfuse (5.1.3 without close\n";
        assert_eq!(parse_macos_kext_version(stdout), None);
    }
}

// `derive_macos_volname` is pure Rust (no macOS-specific syscalls)
// so the test module compiles on every platform; only the helper
// itself is gated on `cfg(target_os = "macos")` at the call site
// (the cfg at the call site keeps the unused-parameter warning
// off Linux/Windows builds). Tests run on Linux CI to keep the
// derivation logic platform-neutral without an extra cfg gate.
//
// Issue #464.
#[cfg(test)]
mod volname_tests {
    use super::derive_macos_volname;

    #[test]
    fn default_derives_from_mountpoint_basename() {
        assert_eq!(derive_macos_volname("/Volumes/foo", None), "mntrs-foo");
        assert_eq!(derive_macos_volname("/Volumes/foo", None), "mntrs-foo");
    }

    #[test]
    fn default_strips_trailing_slashes() {
        assert_eq!(derive_macos_volname("/Volumes/foo/", None), "mntrs-foo");
        assert_eq!(derive_macos_volname("/Volumes/foo//", None), "mntrs-foo");
    }

    #[test]
    fn default_handles_relative_path() {
        assert_eq!(derive_macos_volname("foo", None), "mntrs-foo");
        assert_eq!(derive_macos_volname("foo/bar", None), "mntrs-bar");
    }

    #[test]
    fn default_handles_root_mountpoint() {
        // `/` trims to "" then basename is "" → "mntrs-root"
        assert_eq!(derive_macos_volname("/", None), "mntrs-root");
        assert_eq!(derive_macos_volname("", None), "mntrs-root");
    }

    #[test]
    fn default_truncates_at_64_chars() {
        // basename is 80 chars long → "mntrs-" + 80 = 86, truncated to 64
        let long = "a".repeat(80);
        let mp = format!("/Volumes/{}", long);
        let derived = derive_macos_volname(&mp, None);
        assert_eq!(derived.len(), 64);
        assert!(derived.starts_with("mntrs-"));
        // Truncation is char-based not byte-based — but 'a' is ASCII,
        // so byte == char count here. The behavior under multi-byte
        // input is covered by the override-truncation test below.
        assert_eq!(&derived[..6], "mntrs-");
        assert_eq!(&derived[6..], &"a".repeat(58));
    }

    #[test]
    fn override_used_verbatim_when_under_limit() {
        assert_eq!(
            derive_macos_volname("/Volumes/foo", Some("My Bucket")),
            "My Bucket"
        );
    }

    #[test]
    fn override_truncates_at_64_chars() {
        let long = "b".repeat(100);
        let derived = derive_macos_volname("/Volumes/foo", Some(&long));
        assert_eq!(derived.len(), 64);
        assert!(derived.chars().all(|c| c == 'b'));
    }

    #[test]
    fn override_does_not_inject_mntrs_prefix() {
        // Critical: when user picks a name, we do NOT silently
        // prepend `mntrs-` — the override is treated as the final
        // visible name. This is what users expect from a CLI flag
        // called `--volume-name`.
        assert_eq!(
            derive_macos_volname("/Volumes/foo", Some("backup")),
            "backup"
        );
    }
}
