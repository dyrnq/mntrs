use crate::MntrsFs;
use anyhow::{Result, anyhow};
use std::path::Path;
use std::sync::Arc;
use std::collections::HashMap;
use opendal::Operator;
use opendal::services::S3;

pub fn mount(storage_url: &str, mountpoint: &str, opts: &HashMap<String, String>) -> Result<()> {
    let op = rt_block_on(build_operator(storage_url, opts))?;
    let fs = MntrsFs { op: Arc::new(op) };

    let mount_path = Path::new(mountpoint);
    let session = fuser::spawn_mount2(fs, mount_path, &[])?;
    session.join();
    Ok(())
}

fn rt_block_on<F, T>(f: F) -> T where F: std::future::Future<Output = T> {
    static RT: once_cell::sync::OnceCell<tokio::runtime::Runtime> = once_cell::sync::OnceCell::new();
    let rt = RT.get_or_init(|| tokio::runtime::Runtime::new().expect("tokio rt"));
    rt.block_on(f)
}

async fn build_operator(storage_url: &str, opts: &HashMap<String, String>) -> Result<Operator> {
    let url = url::Url::parse(storage_url)
        .map_err(|e| anyhow!("invalid storage URL '{storage_url}': {e}"))?;
    let scheme = url.scheme();

    match scheme {
        "s3" => build_s3(&url, opts).await,
        _ => Err(anyhow!("unsupported storage scheme '{scheme}'; supported: s3, hdfs, gs, azblob")),
    }
}

async fn build_s3(url: &url::Url, opts: &HashMap<String, String>) -> Result<Operator> {
    let bucket = url.host_str()
        .ok_or_else(|| anyhow!("missing bucket name in s3 URL, expected s3://bucket"))?;

    let mut builder = S3::default();
    builder = builder.bucket(bucket);

    let prefix = url.path().trim_start_matches('/');
    if !prefix.is_empty() { builder = builder.root(prefix); }
    if let Some(ep) = opts.get("endpoint") { builder = builder.endpoint(ep); }
    if let Some(ak) = opts.get("access-key") { builder = builder.access_key_id(ak); }
    if let Some(sk) = opts.get("secret-key") { builder = builder.secret_access_key(sk); }
    if let Some(region) = opts.get("region") { builder = builder.region(region); }

    let op: Operator = Operator::new(builder)?.finish();
    Ok(op)
}
