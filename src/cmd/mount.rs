use crate::MntrsFs;
use anyhow::{Result, anyhow};
use std::path::Path;
use std::sync::Arc;
use std::collections::HashMap;
use std::fs::{self, OpenOptions, File};
use std::io::{Write, BufRead, BufReader};
use std::process::Command;
use opendal::Operator;
use opendal::layers::{TimeoutLayer, RetryLayer, ConcurrentLimitLayer};
use opendal::services::{S3, Gcs, Azblob, HdfsNative, Oss, Cos, Obs, B2, VercelBlob, AliyunDrive};
use fuser::MountOption;
use once_cell::sync::OnceCell;
use std::sync::OnceLock;
use std::os::fd::{AsRawFd, FromRawFd};

fn rt_block_on<F, T>(f: F) -> T where F: std::future::Future<Output = T> {
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

#[allow(dead_code)]
fn mount_alive(mp: &str) -> bool {
    if !std::path::Path::new(mp).exists() { return false; }
    // Check /proc/mounts — dead FUSE mount may still have a directory but no entry
    if let Ok(mounts) = std::fs::read_to_string("/proc/mounts")
        && !mounts.lines().any(|l| l.contains(mp)) {
            return false;
        }
    // read_dir returns Transport endpoint not connected on dead FUSE mounts
    std::path::Path::new(mp).read_dir().is_ok()
}

pub fn read_mounts() -> Vec<MountInfo> {
    let path = mounts_db();
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(_) => return vec![],
    };
    let reader = BufReader::new(file);
    reader.lines()
        .map_while(Result::ok)
        .filter_map(|l| {
            let parts: Vec<&str> = l.split('|').collect();
            if parts.len() < 6 { return None; }
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
    let dir = Path::new(&path).parent().unwrap();
    if let Err(e) = fs::create_dir_all(dir) { tracing::debug!(error=%e, "mounts db dir create failed"); }
    // Remove existing entry for this mountpoint
    if let Ok(content) = fs::read_to_string(&path) {
        let filtered: Vec<&str> = content.lines()
            .filter(|l| !l.contains(mountpoint))
            .collect();
        if let Err(e) = fs::write(&path, filtered.join("\n") + "\n") { tracing::debug!(error=%e, "mounts db write failed"); }
    }
    let pid = std::process::id().to_string();
    let user = std::env::var("USER").unwrap_or_else(|_| "?".into());
    let ro = if read_only { "ro" } else { "rw" };
    let backend = storage.split(':').next().unwrap_or("?");
    let line = format!("{}|{}|{}|{}|{}|{}\n", storage, mountpoint, pid, user, ro, backend);
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&path)
        && let Err(e) = f.write_all(line.as_bytes()) { tracing::debug!(error=%e, "mounts db append failed"); }
}

#[allow(dead_code)]
fn remove_mount(mountpoint: &str) {
    let path = mounts_db();
    if let Ok(content) = fs::read_to_string(&path) {
        let filtered: Vec<&str> = content.lines()
            .filter(|l| !l.contains(mountpoint))
            .collect();
        if let Err(e) = fs::write(&path, filtered.join("\n")) { tracing::debug!(error=%e, "mounts db cleanup failed"); }
    }
}

static CLEANUP_MP: OnceLock<String> = OnceLock::new();

extern "C" fn cleanup() {
    if let Some(mp) = CLEANUP_MP.get() {
        let _ = Command::new("fusermount3").arg("-u").arg(mp).status()
            .or_else(|_| Command::new("fusermount").arg("-u").arg(mp).status());
        let path = mounts_db();
        if let Ok(content) = fs::read_to_string(&path) {
            let filtered: Vec<&str> = content.lines()
                .filter(|l| !l.contains(mp.as_str()))
                .collect();
            if let Err(e) = fs::write(&path, filtered.join("\n")) { tracing::debug!(error=%e, "mounts db cleanup failed"); }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn mount(storage_url: &str, mountpoint: &str, opts: &HashMap<String, String>, read_only: bool,
                dir_cache_time: u64, attr_timeout: u64, _type_cache_ttl: u64, stat_cache_ttl: u64, allow_other: bool, volname: &str, devname: Option<&str>, write_back_cache: bool, fuse_options: &[String],
                daemon: bool, daemon_wait: bool, _daemon_timeout: u64, allow_root: bool, vfs_cache_max_size: u64, mem_limit: u64, vfs_write_back: u64, vfs_cache_mode: &str, vfs_read_ahead: u64, vfs_read_chunk_size: u64, default_permissions: bool,
                uid: Option<u32>, gid: Option<u32>, umask: Option<u32>, dir_perms: Option<u32>, file_perms: Option<u32>,
                allow_non_empty: bool, cache_dir: Option<&str>, direct_io: bool, poll_interval: u64, vfs_cache_max_age: u64, vfs_cache_min_free_space: u64, exclude: Vec<String>, include: Vec<String>, max_size: Option<u64>, min_size: Option<u64>, max_depth: Option<usize>, ignore_case: bool, _no_modtime: bool, _no_checksum: bool, _no_seek: bool, _links: bool, _max_read_ahead: u64, vfs_read_chunk_size_limit: u64, vfs_read_chunk_streams: u32, vfs_fast_fingerprint: bool, async_read: bool, vfs_refresh: bool, vfs_case_insensitive: bool, no_implicit_dir: bool, vfs_block_norm_dupes: bool, _vfs_links: bool, _vfs_used_is_size: bool, _vfs_metadata_extension: Option<String>, storage_class: Option<&str>, vfs_write_wait: u64, vfs_read_wait: u64, vfs_cache_poll_interval: u64, vfs_disk_space_total_size: u64) -> Result<()> {
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
        dir_perms: dir_perms.unwrap_or(0o755) as u16,
        file_perms: file_perms.unwrap_or(0o644) as u16,
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
        block_norm_dupes: vfs_block_norm_dupes,
        write_wait: std::time::Duration::from_secs(vfs_write_wait),
        read_wait: std::time::Duration::from_secs(vfs_read_wait),
        cache_poll_interval: std::time::Duration::from_secs(vfs_cache_poll_interval),
        disk_total_size: vfs_disk_space_total_size * 1024 * 1024 * 1024 * 1024, // TB to bytes
        writeback_queue: Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new())),
        mem_cache: dashmap::DashMap::new(),
        attr_cache: dashmap::DashMap::new(),
        disk_cache_index: dashmap::DashMap::new(),
        out_of_space: std::sync::atomic::AtomicBool::new(false),
        storage_class: storage_class.map(|s| s.to_string()),
        mem_limit: if mem_limit > 0 { mem_limit * 1024 * 1024 } else { u64::MAX },
        mem_used: std::sync::atomic::AtomicU64::new(0),
    };

    // Create pipe for daemon_wait parent-child synchronization
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

    if daemon {
        daemonize(mountpoint, wait_pipe.map(|(_, w)| w))?;
        // After daemonize returns (in grandchild), close read end if we inherited it
        if let Some((r, _)) = wait_pipe {
            unsafe { rustix::io::close(r); }
        }
    }

    let mount_path = Path::new(mountpoint);
    let mut cfg: fuser::Config = Default::default();
    if allow_other || allow_root {
        cfg.acl = fuser::SessionACL::All;
    }
    cfg.mount_options = vec![
        if read_only { MountOption::RO } else { MountOption::RW },
        MountOption::Exec,
        MountOption::FSName(devname.unwrap_or(volname).to_string()),
    ];
    if write_back_cache {
        cfg.mount_options.push(MountOption::CUSTOM("writeback_cache".to_string()));
    }
    if allow_root {
        cfg.mount_options.push(MountOption::CUSTOM("allow_root".to_string()));
    }
    if default_permissions {
        cfg.mount_options.push(MountOption::CUSTOM("default_permissions".to_string()));
    }
    if allow_non_empty {
        cfg.mount_options.push(MountOption::CUSTOM("nonempty".to_string()));
    }
    for opt in fuse_options {
        cfg.mount_options.push(MountOption::CUSTOM(opt.clone()));
    }
    let session = fuser::spawn_mount2(fs, mount_path, &cfg)?;

    record_mount(storage_url, mountpoint, read_only);

    if daemon_wait {
        unblock_parent();
    }

    // Foreground mode with --daemon-wait: parent waits for pipe close
    if !daemon
        && let Some((r, w)) = wait_pipe {
            unsafe { rustix::io::close(w); }
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_secs(_daemon_timeout);
            // Use libc::poll for timeout-based polling
            while std::time::Instant::now() < deadline {
                let mut pfd = libc::pollfd { fd: r, events: libc::POLLIN, revents: 0 };
                let ms = (deadline - std::time::Instant::now()).as_millis().min(100) as i32;
                if unsafe { libc::poll(&mut pfd, 1, ms) } > 0
                    && pfd.revents & (libc::POLLIN | libc::POLLHUP) != 0
                {
                    break;
                }
            }
            unsafe { rustix::io::close(r); }
            std::process::exit(0);
    }

    CLEANUP_MP.set(mountpoint.to_string()).ok();
    unsafe { libc::atexit(cleanup); }
    unsafe {
        libc::signal(libc::SIGINT, handler as *const () as libc::sighandler_t);
        libc::signal(libc::SIGTERM, handler as *const () as libc::sighandler_t);
    }

    std::mem::forget(session);
    loop { std::thread::sleep(std::time::Duration::from_secs(3600)); }
}


fn apply_operator(builder: impl opendal::Builder) -> Result<Operator> {
    let op: Operator = Operator::new(builder)?
        .layer(TimeoutLayer::new().with_io_timeout(std::time::Duration::from_secs(30)))
        .layer(RetryLayer::new().with_max_times(3).with_factor(2.0))
        .layer(ConcurrentLimitLayer::new(16))
        .finish();
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
        "oss" => build_oss(&url, opts).await,
        "cos" => build_cos(&url, opts).await,
        "obs" => build_obs(&url, opts).await,
        "b2" => build_b2(&url, opts).await,
        "vercel" | "vercel-blob" => build_vercel_blob(&url, opts).await,
        "aliyun" | "aliyun-drive" => build_aliyun_drive(&url, opts).await,
        s => Err(anyhow!("unsupported scheme '{s}'; try s3://, gs://, azblob://, hdfs://, oss://, cos://, obs://, b2://")),
    }
}

async fn build_s3(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let bucket = url.host_str().ok_or_else(|| anyhow!("missing bucket"))?;
    let mut builder = S3::default().bucket(bucket);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() { builder = builder.root(p); }
    if let Some(v) = opts.get("endpoint") { builder = builder.endpoint(v); }
    if let Some(v) = opts.get("access-key") { builder = builder.access_key_id(v); }
    if let Some(v) = opts.get("secret-key") { builder = builder.secret_access_key(v); }
    if let Some(v) = opts.get("region") { builder = builder.region(v); }
    apply_operator(builder)
}

async fn build_gcs(url: &url::Url, _opts: &HashMap<String, String>) -> Result<Operator> {
    let bucket = url.host_str().ok_or_else(|| anyhow!("missing bucket"))?;
    let mut builder = Gcs::default().bucket(bucket);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() { builder = builder.root(p); }
    apply_operator(builder)
}

async fn build_azblob(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let container = url.host_str().ok_or_else(|| anyhow!("missing container"))?;
    let mut builder = Azblob::default().container(container);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() { builder = builder.root(p); }
    if let Some(v) = opts.get("account-name") { builder = builder.account_name(v); }
    if let Some(v) = opts.get("account-key") { builder = builder.account_key(v); }
    apply_operator(builder)
}

async fn build_hdfs_native(url: &url::Url, _opts: &HashMap<String, String>) -> Result<Operator> {
    let namenode = url.host_str().ok_or_else(|| anyhow!("missing namenode"))?;
    let port = url.port().unwrap_or(8020);
    let addr = format!("{}:{}", namenode, port);
    let mut builder = HdfsNative::default().name_node(&addr);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() { builder = builder.root(p); }
    apply_operator(builder)
}

static DAEMON_PIPE_WR: OnceLock<i32> = OnceLock::new();

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
    if let Err(e) = std::env::set_current_dir("/") { tracing::debug!(error=%e, "daemon chdir failed"); }
    // Use rustix for safe fd operations
    let devnull = rustix::fs::open("/dev/null", rustix::fs::OFlags::RDWR, rustix::fs::Mode::empty())
        .unwrap_or_else(|_| {
            // Safety: fd 0 is always valid (stdin)
            unsafe { rustix::fd::OwnedFd::from_raw_fd(std::os::fd::RawFd::from(0)) }
        });
    if rustix::stdio::dup2_stdin(&devnull).is_err() { tracing::debug!("daemon dup2 stdin failed"); }
    if rustix::stdio::dup2_stdout(&devnull).is_err() { tracing::debug!("daemon dup2 stdout failed"); }
    if rustix::stdio::dup2_stderr(&devnull).is_err() { tracing::debug!("daemon dup2 stderr failed"); }
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    let pid_dir = format!("{}/.local/share/mntrs", home);
    if let Err(e) = fs::create_dir_all(&pid_dir) { tracing::debug!(error=%e, "pid dir create failed"); }
    let pid = std::process::id();
    let pid_path = format!("{}/{}.pid", pid_dir, mountpoint.replace('/', "_"));
    if let Ok(mut f) = File::create(&pid_path)
        && writeln!(f, "{}", pid).is_err() { tracing::debug!("pid file write failed"); }
    Ok(())
}

fn unblock_parent() {
    if let Some(&fd) = DAEMON_PIPE_WR.get() {
        // Safety: fd was created by pipe() and is valid
        unsafe { rustix::io::close(fd); }
    }
}

async fn build_oss(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let bucket = url.host_str().ok_or_else(|| anyhow!("missing bucket"))?;
    let mut builder = Oss::default().bucket(bucket);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() { builder = builder.root(p); }
    if let Some(v) = opts.get("endpoint") { builder = builder.endpoint(v); }
    if let Some(v) = opts.get("access-key") { builder = builder.access_key_id(v); }
    if let Some(v) = opts.get("secret-key") { builder = builder.access_key_secret(v); }
    apply_operator(builder)
}

async fn build_cos(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let bucket = url.host_str().ok_or_else(|| anyhow!("missing bucket"))?;
    let mut builder = Cos::default().bucket(bucket);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() { builder = builder.root(p); }
    if let Some(v) = opts.get("endpoint") { builder = builder.endpoint(v); }
    if let Some(v) = opts.get("secret-id") { builder = builder.secret_id(v); }
    if let Some(v) = opts.get("secret-key") { builder = builder.secret_key(v); }
    apply_operator(builder)
}

async fn build_obs(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let bucket = url.host_str().ok_or_else(|| anyhow!("missing bucket"))?;
    let mut builder = Obs::default().bucket(bucket);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() { builder = builder.root(p); }
    if let Some(v) = opts.get("endpoint") { builder = builder.endpoint(v); }
    if let Some(v) = opts.get("access-key") { builder = builder.access_key_id(v); }
    if let Some(v) = opts.get("secret-key") { builder = builder.secret_access_key(v); }
    apply_operator(builder)
}

async fn build_b2(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let bucket = url.host_str().ok_or_else(|| anyhow!("missing bucket"))?;
    let mut builder = B2::default().bucket(bucket);
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() { builder = builder.root(p); }
    if let Some(v) = opts.get("application-key-id") { builder = builder.application_key_id(v); }
    if let Some(v) = opts.get("application-key") { builder = builder.application_key(v); }
    apply_operator(builder)
}

async fn build_vercel_blob(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let mut builder = VercelBlob::default();
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() { builder = builder.root(p); }
    if let Some(v) = opts.get("token") { builder = builder.token(v); }
    apply_operator(builder)
}

async fn build_aliyun_drive(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let mut builder = AliyunDrive::default();
    let p = url.path().trim_start_matches('/');
    if !p.is_empty() { builder = builder.root(p); }
    if let Some(v) = opts.get("access-token") { builder = builder.access_token(v); }
    if let Some(v) = opts.get("refresh-token") { builder = builder.refresh_token(v); }
    if let Some(v) = opts.get("client-id") { builder = builder.client_id(v); }
    if let Some(v) = opts.get("client-secret") { builder = builder.client_secret(v); }
    if let Some(v) = opts.get("drive-type") { builder = builder.drive_type(v); }
    apply_operator(builder)
}

extern "C" fn handler(_: i32) {
    std::process::exit(0);
}
