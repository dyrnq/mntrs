//! Writeback worker — async upload of dirty cache files to remote storage.
//!
//! The worker runs in a background thread, consuming from a shared queue.
//! Each file is uploaded using OpenDAL's Writer (supports multipart for >5GB).
//! On success, the inode table is updated (PendingUploadHook).
//! On failure, retries up to 3 times with exponential backoff.

use std::collections::VecDeque;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opendal::Operator;

use crate::Inodes;

/// Run the writeback worker loop. Intended to be spawned as a background thread.
pub fn worker(
    op: Arc<Operator>,
    inodes: Inodes,
    queue: Arc<Mutex<VecDeque<(u64, String, PathBuf)>>>,
    delay: Duration,
    max_age: Duration,
) {
    loop {
        let task = {
            let mut q = queue.lock().unwrap();
            q.pop_front()
        };
        let (_ino, remote_path, cache_path) = match task {
            Some(t) => t,
            None => {
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        if delay > Duration::ZERO {
            thread::sleep(delay);
        }
        // Check cache max age: skip stale files
        if max_age > Duration::ZERO
            && let Ok(meta) = fs::metadata(&cache_path)
            && let Ok(elapsed) = meta.modified().unwrap_or(UNIX_EPOCH).elapsed()
            && elapsed > max_age
        {
            let _ = fs::remove_file(&cache_path);
            continue;
        }
        let data = match fs::read(&cache_path) {
            Ok(d) if !d.is_empty() => d,
            _ => {
                let _ = fs::remove_file(&cache_path);
                continue;
            }
        };
        let op = op.clone();
        let p = remote_path.clone();
        // Write checksum-footer to cache before upload (for disk integrity)
        let crc = crate::crc64_checksum(&data);
        let mut data_with_csum = data.clone();
        data_with_csum.extend_from_slice(&crc.to_le_bytes());

        for attempt in 0..3 {
            let r = crate::rt().block_on(async {
                match op.writer(&p).await {
                    Ok(mut w) => {
                        w.write(bytes::Bytes::from(data.clone())).await?;
                        w.close().await
                    }
                    Err(e) => Err(e),
                }
            });
            match r {
                Ok(_) => {
                    // PendingUploadHook: update inode size/mtime after upload
                    let new_size = data.len() as u64;
                    inodes.entry(_ino).and_modify(|v| {
                        v.2 = new_size;
                        v.3 = Some(SystemTime::now());
                    });
                    // Replace cache file with checksummed version
                    if let Err(e) = fs::write(&cache_path, &data_with_csum) {
                        tracing::debug!(error=%e, path=?cache_path, "writeback checksum store failed");
                    } else {
                        tracing::trace!(path=?cache_path, "writeback checksum stored");
                    }
                    if let Err(e) = fs::remove_file(cache_path.with_extension("dirty")) {
                        tracing::debug!(error=%e, "writeback dirty remove failed");
                    }
                    break;
                }
                Err(e) if attempt < 2 => {
                    eprintln!("[mntrs] writeback retry {}/3 for {p}: {e}", attempt + 1);
                    thread::sleep(Duration::from_secs(1 << attempt));
                }
                Err(e) => {
                    eprintln!("[mntrs] writeback failed for {p}: {e}");
                }
            }
        }
    }
}
