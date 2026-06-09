//! Writeback worker — async upload of dirty cache files to remote storage.
//!
//! The worker runs in a background thread, consuming from a shared queue.
//! It aggregates all block-level cache files for the same remote path
//! into a single upload, then uploads them in one PUT request.

use std::collections::VecDeque;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use opendal::Operator;

use crate::Inodes;

/// Run the writeback worker loop. Intended to be spawned as a background thread.
pub fn worker(
    op: Arc<Operator>,
    inodes: Inodes,
    queue: Arc<Mutex<VecDeque<(u64, String, PathBuf)>>>,
    delay: Duration,
    _max_age: Duration,
) {
    loop {
        // Collect all pending tasks for the same remote path
        let tasks: Vec<(u64, String, PathBuf)> = {
            let mut q = queue.lock().unwrap();
            let first = match q.pop_front() {
                Some(t) => t,
                None => {
                    drop(q);
                    thread::sleep(Duration::from_secs(1));
                    continue;
                }
            };
            let mut batch = vec![first];
            while let Some(next) = q.pop_front() {
                if batch[0].1 == next.1 {
                    batch.push(next);
                } else {
                    q.push_front(next);
                    break;
                }
            }
            batch
        };

        let (ino, remote_path) = (tasks[0].0, tasks[0].1.clone());
        let p = remote_path.clone();

        // Read and sort all blocks by block index
        let mut blocks: Vec<(u64, Vec<u8>)> = Vec::with_capacity(tasks.len());
        for (_, _, cache_path) in &tasks {
            let name = cache_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            let block_idx = name
                .rsplit('_')
                .nth(1)
                .and_then(|s| u64::from_str_radix(s, 16).ok())
                .unwrap_or(0);
            match fs::read(cache_path) {
                Ok(d) => blocks.push((block_idx, d)),
                Err(_) => continue,
            }
        }
        if blocks.is_empty() {
            continue;
        }
        blocks.sort_by_key(|(idx, _)| *idx);

        if delay > Duration::ZERO {
            thread::sleep(delay);
        }

        // Concatenate blocks in order and upload as single file
        let total_len: usize = blocks.iter().map(|(_, d)| d.len()).sum();
        let mut full_data = Vec::with_capacity(total_len);
        for (_, data) in &blocks {
            full_data.extend_from_slice(data);
        }

        let upload_ok = 'retry: {
            for attempt in 0..3 {
                let buf = full_data.clone();
                match crate::rt().block_on(async { op.write(&p, buf).await }) {
                    Ok(_) => break 'retry true,
                    Err(e) if attempt < 2 => {
                        eprintln!("[mntrs] writeback retry {}/3 for {p}: {e}", attempt + 1);
                        thread::sleep(Duration::from_secs(1 << attempt));
                    }
                    Err(e) => {
                        eprintln!("[mntrs] writeback failed for {p}: {e}");
                        break 'retry false;
                    }
                }
            }
            false
        };

        if upload_ok {
            let new_size = total_len as u64;
            inodes.entry(ino).and_modify(|v| {
                v.2 = new_size;
                v.3 = Some(std::time::SystemTime::now());
            });
            // Clean up all block cache files + dirty sidecars
            for (_, _, cache_path) in &tasks {
                let _ = fs::remove_file(cache_path);
                let _ = fs::remove_file(cache_path.with_extension("dirty"));
            }
            // Write aggregated checksummed cache (file-level)
            let crc = crate::crc64_checksum(&full_data);
            let mut data_with_csum = full_data;
            data_with_csum.extend_from_slice(&crc.to_le_bytes());
            let first_path = &tasks[0].2;
            if let Some(parent) = first_path.parent() {
                let hash_name = first_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|n| n.split('_').next())
                    .unwrap_or("");
                let agg_path = parent.join(hash_name);
                let _ = fs::write(&agg_path, &data_with_csum);
            }
        }
    }
}

/// Start writeback worker via Tokio channel (non-blocking for FUSE threads).
/// Returns a Sender and a JoinHandle.
///
/// Unlike the legacy `worker()` which uses Arc<Mutex<VecDeque>>,
/// this version uses `tokio::sync::mpsc::unbounded_channel`.
/// FUSE threads call `tx.send()` without blocking on a Mutex.
pub fn start_channel(
    op: Arc<Operator>,
    inodes: Inodes,
    delay: Duration,
    max_age: Duration,
) -> (
    tokio::sync::mpsc::UnboundedSender<(u64, String, PathBuf)>,
    tokio::task::JoinHandle<()>,
) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let handle = tokio::spawn(async move {
        let mut pending: VecDeque<(u64, String, PathBuf)> = VecDeque::new();

        loop {
            if pending.is_empty() {
                match rx.recv().await {
                    Some(item) => pending.push_back(item),
                    None => break,
                }
            }

            while let Ok(item) = rx.try_recv() {
                pending.push_back(item);
            }

            let first = pending.pop_front().unwrap();
            let remote_path = first.1.clone();
            let mut batch = vec![first];

            let mut i = 0;
            while i < pending.len() {
                if pending[i].1 == remote_path {
                    batch.push(pending.remove(i).unwrap());
                } else {
                    i += 1;
                }
            }

            let ino = batch[0].0;

            let mut blocks: Vec<(u64, Vec<u8>)> = Vec::with_capacity(batch.len());
            for (_, _, cache_path) in &batch {
                let name = cache_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("");
                let block_idx =
                    u64::from_str_radix(name.rsplit('_').nth(1).unwrap_or("0"), 16).unwrap_or(0);
                match fs::read(cache_path) {
                    Ok(d) => blocks.push((block_idx, d)),
                    Err(_) => continue,
                }
            }
            if blocks.is_empty() {
                continue;
            }
            blocks.sort_by_key(|(idx, _)| *idx);

            if delay > Duration::ZERO {
                tokio::time::sleep(delay).await;
            }

            let total_len: usize = blocks.iter().map(|(_, d)| d.len()).sum();
            let mut full_data = Vec::with_capacity(total_len);
            for (_, data) in &blocks {
                full_data.extend_from_slice(data);
            }

            let p = remote_path.clone();
            let upload_ok = {
                let op = op.clone();
                let buf: Vec<u8> = full_data.clone();
                let mut ok = false;
                for attempt in 0..3 {
                    match op.write(&p, buf.clone()).await {
                        Ok(_) => {
                            ok = true;
                            break;
                        }
                        Err(e) if attempt < 2 => {
                            eprintln!("[mntrs] writeback retry {}/3 for {p}: {e}", attempt + 1);
                            tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                        }
                        Err(e) => {
                            eprintln!("[mntrs] writeback failed for {p}: {e}");
                        }
                    }
                }
                ok
            };

            if upload_ok {
                let new_size = total_len as u64;
                inodes.entry(ino).and_modify(|v| {
                    v.2 = new_size;
                    v.3 = Some(std::time::SystemTime::now());
                });
                for (_, _, cache_path) in &batch {
                    let _ = fs::remove_file(cache_path);
                    let _ = fs::remove_file(cache_path.with_extension("dirty"));
                }
            }
        }
    });

    (tx, handle)
}
