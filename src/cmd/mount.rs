#![allow(clippy::type_complexity)]
use crate::MntrsFs;
use anyhow::{Result, anyhow};
#[cfg(not(windows))]
use fuser::MountOption;
use once_cell::sync::OnceCell;
use opendal::Operator;
use opendal::layers::{ConcurrentLimitLayer, RetryLayer, TimeoutLayer};
use opendal::services::{
    AliyunDrive, Azblob, B2, Cos, Fs, Gcs, HdfsNative, Memory, Obs, Oss, S3, VercelBlob, Webdav,
};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
#[cfg(not(windows))]
use std::os::fd::{AsRawFd, FromRawFd};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::OnceLock;

fn rt_block_on<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    static RT: OnceCell<tokio::runtime::Runtime> = OnceCell::new();
    let rt = RT.get_or_init(|| tokio::runtime::Runtime::new().expect("tokio rt"));
    rt.block_on(f)
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
    let dir = std::path::Path::new(&path).parent().unwrap();
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
    if let Ok(content) = fs::read_to_string(&path) {
        let filtered: Vec<&str> = content
            .lines()
            .filter(|l| l.split('\0').nth(1) != Some(mountpoint))
            .collect();
        if let Err(e) = fs::write(&path, filtered.join("\n")) {
            tracing::debug!(error=%e, "mounts db cleanup failed");
        }
    }
}

static CLEANUP_MP: OnceLock<String> = OnceLock::new();
static SHUTDOWN_REQUESTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

extern "C" fn cleanup() {
    if let Some(mp) = CLEANUP_MP.get() {
        let _ = Command::new("fusermount3")
            .arg("-u")
            .arg(mp)
            .status()
            .or_else(|_| Command::new("fusermount").arg("-u").arg(mp).status());
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

    // Phase 1: wait for writeback queue to drain
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    while std::time::Instant::now() < deadline {
        let pending = crate::cmd::mount::pending_writebacks();
        if pending == 0 {
            break;
        }
        tracing::info!(mountpoint, pending, "waiting for writeback to complete");
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
    // Phase 2: unmount
    if let Err(e) = crate::cmd::unmount::unmount(mountpoint) {
        tracing::warn!(mountpoint, error=%e, "regular unmount failed, trying lazy");
        // Phase 3: lazy unmount fallback
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
    // Phase 4: clean up isolated cache directory
    let cache_dir = cache_dir_for_mount(mountpoint);
    if let Err(e) = std::fs::remove_dir_all(&cache_dir) {
        tracing::warn!(cache_dir, error=%e, "cache cleanup failed");
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
    let fs = MntrsFs {
        op: Arc::new(op),
        inodes: dashmap::DashMap::new(),
        dir_cache: dashmap::DashMap::new(),
        cache_dir: cache_dir_path,
        handles: dashmap::DashMap::new(),
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

        mem_cache: dashmap::DashMap::new(),
        attr_cache: dashmap::DashMap::new(),
        disk_cache_index: dashmap::DashMap::new(),
        out_of_space: std::sync::atomic::AtomicBool::new(false),
        storage_class: storage_class.map(|s| s.to_string()),
        mem_limit: if mem_limit > 0 {
            mem_limit * 1024 * 1024
        } else {
            u64::MAX
        },
        mem_used: std::sync::atomic::AtomicU64::new(0),
        mem_access_order: std::sync::Mutex::new(std::collections::VecDeque::new()),
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

    // fuser::spawn_mount2 already spawns a background thread.
    // Daemon mode: keep main thread alive to prevent session drop (which unmounts).

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
        let adapter = FuserAdapter::new(
            fs,
            std::time::Duration::from_secs(dir_cache_time),
            std::time::Duration::from_secs(attr_timeout),
            direct_io,
        );
        let session = fuser::spawn_mount2(adapter, mount_path, &cfg)?;
        record_mount(storage_url, mountpoint, read_only);
        if daemon_wait {
            unblock_parent();
        }
        // Prevent session drop on thread exit (keeps FUSE mounted)
        std::mem::forget(session);
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

    if daemon {
        // Keep the process alive indefinitely (FUSE session runs in background thread).
        // Ctrl-C or SIGTERM will trigger cleanup via atexit.
        loop {
            std::thread::sleep(std::time::Duration::from_secs(3600));
        }
    }

    // Foreground / daemon-wait: called from CSI or interactive use, return control.
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

    CLEANUP_MP.set(mountpoint.to_string()).ok();
    unsafe {
        libc::atexit(cleanup);
    }
    unsafe {
        libc::signal(libc::SIGINT, handler as *const () as libc::sighandler_t);
        libc::signal(libc::SIGTERM, handler as *const () as libc::sighandler_t);
    }

    loop {
        if SHUTDOWN_REQUESTED.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::info!("shutdown requested, cleaning up...");
            cleanup();
            std::process::exit(0);
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn apply_operator_with_tls(
    builder: impl opendal::Builder,
    opts: &std::collections::HashMap<String, String>,
) -> Result<Operator> {
    // Check for curl-compatible TLS flags: --opt cacert=... --opt cert=...
    let insecure = opts.contains_key("insecure");
    let has_tls = insecure || opts.contains_key("cacert") || opts.contains_key("cert");
    let op = if has_tls {
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
        Operator::new(builder)?
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
    let addr = format!("{}:{}", namenode, port);
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

#[cfg(not(windows))]
static DAEMON_PIPE_WR: OnceLock<i32> = OnceLock::new();

#[cfg(not(windows))]
#[allow(dead_code)]
fn daemonize(mountpoint: &str, wait_pipe: Option<i32>) -> Result<()> {
    // fork/setsid require unsafe — rustix intentionally doesn't wrap them
    match unsafe { libc::fork() } {
        -1 => return Err(anyhow!("fork failed")),
        0 => {}
        _ => std::process::exit(0),
    }
    if unsafe { libc::setsid() } < 0 {
        return Err(anyhow!("setsid failed"));
    }
    match unsafe { libc::fork() } {
        -1 => return Err(anyhow!("second fork failed")),
        0 => {}
        _ => std::process::exit(0),
    }
    if let Some(fd) = wait_pipe {
        DAEMON_PIPE_WR.set(fd).ok();
    }
    if let Err(e) = std::env::set_current_dir("/") {
        tracing::debug!(error=%e, "daemon chdir failed");
    }
    // Use rustix for safe fd operations
    let devnull = rustix::fs::open(
        "/dev/null",
        rustix::fs::OFlags::RDWR,
        rustix::fs::Mode::empty(),
    )
    .unwrap_or_else(|_| {
        // Safety: fd 0 is always valid (stdin)
        #[cfg(not(windows))]
        unsafe {
            rustix::fd::OwnedFd::from_raw_fd(std::os::fd::RawFd::from(0))
        }
    });
    if rustix::stdio::dup2_stdin(&devnull).is_err() {
        tracing::debug!("daemon dup2 stdin failed");
    }
    if rustix::stdio::dup2_stdout(&devnull).is_err() {
        tracing::debug!("daemon dup2 stdout failed");
    }
    if rustix::stdio::dup2_stderr(&devnull).is_err() {
        tracing::debug!("daemon dup2 stderr failed");
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    let pid_dir = format!("{}/.local/share/mntrs", home);
    if let Err(e) = fs::create_dir_all(&pid_dir) {
        tracing::debug!(error=%e, "pid dir create failed");
    }
    let pid = std::process::id();
    let pid_path = format!("{}/{}.pid", pid_dir, mountpoint.replace('/', "_"));
    if let Ok(mut f) = File::create(&pid_path)
        && writeln!(f, "{}", pid).is_err()
    {
        tracing::debug!("pid file write failed");
    }
    Ok(())
}

#[cfg(not(windows))]
fn unblock_parent() {
    if let Some(&fd) = DAEMON_PIPE_WR.get() {
        // Safety: fd was created by pipe() and is valid
        unsafe {
            rustix::io::close(fd);
        }
    }
}

#[cfg(windows)]
fn unblock_parent() {}

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

/// Async-signal-safe: only sets an atomic flag.
/// The main loop checks SHUTDOWN_REQUESTED and performs proper cleanup.
extern "C" fn handler(_: i32) {
    SHUTDOWN_REQUESTED.store(true, std::sync::atomic::Ordering::Relaxed);
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
