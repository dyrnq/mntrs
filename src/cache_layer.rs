// Cache layer abstraction for the multi-level cache.
//
// This module defines the `CacheLayer` trait and two implementations:
//
//   * `MemCacheLayer` — L1 (in-memory, wraps `Arc<dyn MemCache>`)
//   * `DiskBlockCache` — L2 (on-disk block cache, CRC32C + LZ4)
//
// Both are composed by `MultiLevelCache` in `multi_level_cache.rs`
// which provides a unified `read_block` → L1 → L2 lookup chain.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;

use crate::cache::MemCache;
use crate::util::CacheKey;

// ── CacheLayer trait ────────────────────────────────────────────────

/// Abstraction for a single cache level (L1 or L2).
///
/// Both `MemCacheLayer` (L1) and `DiskBlockCache` (L2) implement this
/// trait. The `MultiLevelCache` composes them into a unified lookup
/// chain: L1 → L2 → miss.
pub(crate) trait CacheLayer: Send + Sync {
    /// Look up a cached block. Returns `None` on miss.
    ///
    /// `path` is the remote storage path (needed by L2 for the
    /// on-disk file name via `cache_block_path`). `ino` is the FUSE
    /// inode number (needed by L1 for the DashMap key). `block_idx`
    /// is `offset / CACHE_BLOCK_SIZE`.
    fn get_block(&self, path: &str, ino: u64, block_idx: u64) -> Option<Bytes>;

    /// Insert or replace a cached block. Returns `true` if the
    /// write succeeded.
    fn put_block(&self, path: &str, ino: u64, block_idx: u64, data: Bytes) -> bool;

    /// Drop every cached block for the given path/inode.
    fn invalidate_path(&self, path: &str, ino: u64);
}

// ── L1: MemCacheLayer ───────────────────────────────────────────────

/// L1 adapter: wraps `Arc<dyn MemCache>` to implement `CacheLayer`.
pub(crate) struct MemCacheLayer {
    inner: Arc<dyn MemCache>,
}

impl MemCacheLayer {
    pub fn new(inner: Arc<dyn MemCache>) -> Self {
        Self { inner }
    }
}

impl CacheLayer for MemCacheLayer {
    fn get_block(&self, _path: &str, ino: u64, block_idx: u64) -> Option<Bytes> {
        let r = self.inner.get(ino, block_idx);
        tracing::debug!(
            ino = ino,
            block_idx = block_idx,
            hit = r.is_some(),
            "L1 get_block"
        );
        r
    }

    fn put_block(&self, _path: &str, ino: u64, block_idx: u64, data: Bytes) -> bool {
        self.inner.put(ino, block_idx, data);
        true
    }

    fn invalidate_path(&self, _path: &str, ino: u64) {
        tracing::debug!(ino, "L1 invalidate_path (mem_cache by ino)");
        self.inner.invalidate_ino(ino);
    }
}

// ── L2: DiskBlockCache ──────────────────────────────────────────────

/// L2 on-disk block cache. Wraps the existing block-format read/write
/// logic (`block_format::read_block_cached`, `block_format::serialize_v3_block`)
/// and the `disk_cache_index` LRU tracking.
pub(crate) struct DiskBlockCache {
    cache_dir: PathBuf,
    disk_cache_index: Arc<DashMap<CacheKey, (u64, Instant)>>,
    direct_io: bool,
}

impl DiskBlockCache {
    pub fn new(
        cache_dir: PathBuf,
        disk_cache_index: Arc<DashMap<CacheKey, (u64, Instant)>>,
        direct_io: bool,
    ) -> Self {
        Self {
            cache_dir,
            disk_cache_index,
            direct_io,
        }
    }
}

impl CacheLayer for DiskBlockCache {
    fn get_block(&self, path: &str, _ino: u64, block_idx: u64) -> Option<Bytes> {
        if self.direct_io {
            return None;
        }
        // Issue #130: previously this method refused to serve
        // blocks whose `(path, block_idx)` was not in
        // `disk_cache_index`, on the theory that any such file
        // was an orphan from a prior mount whose in-memory
        // index had been lost. The companion "preheating" loop
        // in `MultiLevelCache::new` was supposed to repopulate
        // the index at startup, but the preheater inserted
        // `(filename, block_idx)` instead of `(path, block_idx)`
        // — filenames don't encode the original remote path
        // (only the path hash, which is one-way), so the
        // preheated entries never matched and the `contains_key`
        // guard served essentially as a "skip L2 entirely" gate.
        //
        // The fix removes the preheating loop and trusts the
        // on-disk file existence check below. The first read
        // after restart fetches from remote and populates
        // `disk_cache_index` correctly via `bump_in_memory_atime`
        // + `put_block`; subsequent reads hit L2. The LRU
        // eviction logic in `disk_cache_index` sees entries as
        // they're touched, which is fine for cache pressure
        // decisions.
        let cpath = crate::cache_block_path(&self.cache_dir, path, block_idx);
        if !cpath.exists() {
            return None;
        }
        tracing::debug!(
            path = %path,
            block_idx = block_idx,
            cpath = %cpath.display(),
            "L2 get_block: HIT"
        );
        let data = crate::block_format::read_block_cached(&cpath, path)?;
        // Bug B fix: bump the in-memory LRU sort key on every
        // cache hit. The on-disk atime is unreliable on relatime
        // mount defaults.
        crate::bump_in_memory_atime(&self.disk_cache_index, &(path.to_string(), Some(block_idx)));
        Some(data)
    }

    fn put_block(&self, path: &str, _ino: u64, block_idx: u64, data: Bytes) -> bool {
        if self.direct_io {
            return false;
        }
        let blk_path = crate::cache_block_path(&self.cache_dir, path, block_idx);
        if let Some(parent) = blk_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let buf = match crate::block_format::serialize_v3_block(path, &data) {
            Some(b) => b,
            None => return false,
        };
        let written_size = buf.len() as u64;
        let wrote = write_block(&blk_path, &buf, written_size);
        if wrote {
            self.disk_cache_index.insert(
                (path.to_string(), Some(block_idx)),
                (written_size, Instant::now()),
            );
        }
        wrote
    }

    fn invalidate_path(&self, path: &str, _ino: u64) {
        // Collect matching block-level keys, then remove.
        // Only remove block-level entries (block_idx = Some(_)).
        // The file-level entry (block_idx = None) is the whole-file
        // cache written by the write handler — removing it here
        // would destroy the cache file we just wrote to.
        let to_remove: Vec<CacheKey> = self
            .disk_cache_index
            .iter()
            .filter(|entry| entry.key().0 == path && entry.key().1.is_some())
            .map(|entry| entry.key().clone())
            .collect();
        tracing::debug!(
            path = %path,
            keys_found = to_remove.len(),
            "L2 invalidate_path (block files by disk_cache_index)"
        );
        for key in to_remove {
            if let Some(idx) = key.1 {
                let blk_path = crate::cache_block_path(&self.cache_dir, &key.0, idx);
                let _ = std::fs::remove_file(&blk_path);
            }
            self.disk_cache_index.remove(&key);
        }
        // Issue #128: bump the path's cache generation so any async
        // block-cache pool job captured by a read BEFORE this write
        // (and still queued) self-skips instead of re-creating a stale
        // `.block` file the loop above just removed.
        crate::disk_write_pool::bump_path_cache_gen(path);
    }
}

/// Write a serialized block to disk at mode 0o600 (unix) or
/// default perms elsewhere. Returns true on success. Centralized
/// so the rest of the file has one place to audit the perm.
fn write_block(blk_path: &std::path::Path, buf: &[u8], written_size: u64) -> bool {
    use std::io::Write;
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        match std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .open(blk_path)
        {
            Ok(mut f) => {
                let ok = f.write_all(buf).is_ok();
                if ok {
                    let _ = f.set_len(written_size);
                }
                if !ok {
                    tracing::debug!(?blk_path, "L2 block cache write failed");
                }
                ok
            }
            Err(_) => false,
        }
    }
    #[cfg(not(unix))]
    {
        match std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(blk_path)
        {
            Ok(mut f) => {
                let ok = f.write_all(buf).is_ok();
                if ok {
                    let _ = f.set_len(written_size);
                }
                if !ok {
                    tracing::debug!(?blk_path, "L2 block cache write failed");
                }
                ok
            }
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::DashMapMemCache;

    #[test]
    fn mem_cache_layer_round_trip() {
        let mc: Arc<dyn MemCache> = Arc::new(DashMapMemCache::new(0));
        let layer = MemCacheLayer::new(mc);
        let data = Bytes::from_static(b"hello block");
        assert!(layer.put_block("test/path", 42, 0, data.clone()));
        let got = layer.get_block("test/path", 42, 0).unwrap();
        assert_eq!(got.as_ref(), b"hello block");
        // Miss on different ino
        assert!(layer.get_block("test/path", 99, 0).is_none());
    }

    #[test]
    fn mem_cache_layer_invalidate() {
        let mc: Arc<dyn MemCache> = Arc::new(DashMapMemCache::new(0));
        let layer = MemCacheLayer::new(mc);
        layer.put_block("a", 1, 0, Bytes::from_static(b"b0"));
        layer.put_block("a", 1, 1, Bytes::from_static(b"b1"));
        layer.put_block("a", 2, 0, Bytes::from_static(b"other"));
        layer.invalidate_path("a", 1);
        assert!(layer.get_block("a", 1, 0).is_none());
        assert!(layer.get_block("a", 1, 1).is_none());
        // ino=2 unaffected
        assert!(layer.get_block("a", 2, 0).is_some());
    }

    #[test]
    fn disk_block_cache_round_trip() {
        let dir = std::env::temp_dir().join(format!(
            "mntrs-cache-layer-test-{}-{}",
            std::process::id(),
            "rt"
        ));
        let _ = std::fs::create_dir_all(&dir);
        let idx = Arc::new(DashMap::new());
        let l2 = DiskBlockCache::new(dir.clone(), idx, false);
        let data = Bytes::from(vec![0xAB; 4096]);
        assert!(l2.put_block("test/file.bin", 10, 3, data.clone()));
        let got = l2.get_block("test/file.bin", 10, 3).unwrap();
        assert_eq!(got.as_ref(), data.as_ref());
        // Miss on wrong block
        assert!(l2.get_block("test/file.bin", 10, 4).is_none());
        // Invalidate
        l2.invalidate_path("test/file.bin", 10);
        assert!(l2.get_block("test/file.bin", 10, 3).is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disk_block_cache_direct_io_bypass() {
        let dir = std::env::temp_dir().join(format!(
            "mntrs-cache-layer-test-{}-{}",
            std::process::id(),
            "dio"
        ));
        let _ = std::fs::create_dir_all(&dir);
        let idx = Arc::new(DashMap::new());
        let l2 = DiskBlockCache::new(dir.clone(), idx, true);
        assert!(!l2.put_block("x", 1, 0, Bytes::from_static(b"data")));
        assert!(l2.get_block("x", 1, 0).is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
