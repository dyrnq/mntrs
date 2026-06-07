use crate::MntrsFs;
use anyhow::{Result, anyhow};
use std::path::Path;
use std::sync::Arc;
use std::collections::HashMap;
use std::fs::{self, OpenOptions, File};
use std::io::{Write, BufRead, BufReader};
use std::process::Command;
use opendal::Operator;
use opendal::layers::TimeoutLayer;
use opendal::services::{S3, Gcs, Azblob, HdfsNative};
use fuser::MountOption;
use once_cell::sync::OnceCell;
use std::sync::OnceLock;

fn rt_block_on<F, T>(f: F) -> T where F: std::future::Future<Output = T> {
    static RT: OnceCell<tokio::runtime::Runtime> = OnceCell::new();
    let rt = RT.get_or_init(|| tokio::runtime::Runtime::new().expect("tokio rt"));
    rt.block_on(f)
}

fn mounts_db() -> String {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    format!("{}/.local/share/mntrs/mounts.txt", home)
}

pub fn read_mounts() -> Vec<(String, String)> {
    let path = mounts_db();
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(_) => return vec![],
    };
    let reader = BufReader::new(file);
    reader.lines()
        .filter_map(|l| l.ok())
        .filter_map(|l| {
            let idx = l.find(' ')?;
            Some((l[..idx].to_string(), l[idx+1..].to_string()))
        })
        .collect()
}

fn record_mount(storage: &str, mountpoint: &str) {
    let path = mounts_db();
    let dir = Path::new(&path).parent().unwrap();
    let _ = fs::create_dir_all(dir);
    // Remove existing entry for this mountpoint
    if let Ok(content) = fs::read_to_string(&path) {
        let filtered: Vec<&str> = content.lines()
            .filter(|l| !l.contains(mountpoint))
            .collect();
        let _ = fs::write(&path, filtered.join("\n") + "\n");
    }
    let line = format!("{} {}\n", storage, mountpoint);
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&path) {
        let _ = f.write_all(line.as_bytes());
    }
}

fn remove_mount(mountpoint: &str) {
    let path = mounts_db();
    if let Ok(content) = fs::read_to_string(&path) {
        let filtered: Vec<&str> = content.lines()
            .filter(|l| !l.contains(mountpoint))
            .collect();
        let _ = fs::write(&path, filtered.join("\n"));
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
            let _ = fs::write(&path, filtered.join("\n"));
        }
    }
}

pub fn mount(storage_url: &str, mountpoint: &str, opts: &HashMap<String, String>, read_only: bool) -> Result<()> {
    let op = rt_block_on(build_operator(storage_url, opts))?;
    let fs = MntrsFs {
        op: Arc::new(op),
        inodes: std::sync::Mutex::new(std::collections::HashMap::new()),
        dir_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
    };

    let mount_path = Path::new(mountpoint);
    let session = fuser::spawn_mount2(fs, mount_path, &[
        if read_only { MountOption::RO } else { MountOption::RW },
        MountOption::Exec,
    ])?;

    record_mount(storage_url, mountpoint);

    CLEANUP_MP.set(mountpoint.to_string()).ok();
    unsafe { libc::atexit(cleanup); }
    unsafe {
        libc::signal(libc::SIGINT, handler as libc::sighandler_t);
        libc::signal(libc::SIGTERM, handler as libc::sighandler_t);
    }

    std::mem::forget(session);
    loop { std::thread::sleep(std::time::Duration::from_secs(3600)); }
}

extern "C" fn handler(_: i32) {
    std::process::exit(0);
}

fn apply_operator(builder: impl opendal::Builder) -> Result<Operator> {
    let op: Operator = Operator::new(builder)?
        .layer(TimeoutLayer::new().with_io_timeout(std::time::Duration::from_secs(5)))
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
        s => Err(anyhow!("unsupported scheme '{s}'; try s3://, gs://, azblob://, hdfs://")),
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
