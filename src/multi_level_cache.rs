// Multi-level cache: unified L1 → L2 → miss lookup chain.
//
// Composes `MemCacheLayer` (L1, in-memory) and `DiskBlockCache`
// (L2, on-disk CRC32C + LZ4) into a single `read_block` call
// that the FUSE read path uses instead of inline cache checks.
//
// On a miss at both levels, the caller fetches from the remote
// backend (L3) and calls `populate` to backfill L1 + L2.
//
// The `invalidate` method drops both L1 and L2 entries for a
// given path/inode, used by the write path.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;

use crate::cache::MemCache;
use crate::cache_layer::{CacheLayer, DiskBlockCache, MemCacheLayer};
use crate::metrics::Metrics;
use crate::util::CacheKey;

// ── L1 admission policy ─────────────────────────────────────────────

/// Controls which fetched blocks are promoted to L1 (memory).
///
/// The goal is to prevent one-shot sequential scans (e.g. `cat` of
/// a 10 GiB file) from evicting hot blocks in L1. By default, only
/// single-block remote fetches are promoted — multi-block fetches
/// land in L2 only, so the next read on the same block hits L2
/// (disk, ~50 µs) instead of L1 (memory, ~100 ns) but doesn't
/// evict the working set.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdmissionPolicy {
    /// Promote every fetched block to L1.
    All,
    /// Promote only single-block fetches; multi-block fetches
    /// go to L2 only. Default.
    SingleBlockOnly,
    /// Never promote to L1 (L2 only). Useful for direct-IO-like
    /// workloads that bypass the memory cache.
    None,
}

// ── MultiLevelCache ─────────────────────────────────────────────────

pub(crate) struct MultiLevelCache {
    l1: MemCacheLayer,
    l2: DiskBlockCache,
    metrics: Arc<Metrics>,
}

impl MultiLevelCache {
    pub fn new(
        mem_cache: Arc<dyn MemCache>,
        cache_dir: PathBuf,
        disk_cache_index: Arc<DashMap<CacheKey, (u64, Instant)>>,
        direct_io: bool,
        metrics: Arc<Metrics>,
    ) -> Self {
        // L2 preheating: scan the cache directory for existing
        // `.block` files and populate the disk_cache_index so
        // the first read after a restart doesn't treat warm L2
        // blocks as misses. `load_cache_index` parses the
        // `{hash}_{block_idx:010x}.block` filenames and returns
        // (name, block_idx, size, mtime) tuples.
        if !direct_io {
            let entries = crate::block_format::load_cache_index(&cache_dir);
            let now = Instant::now();
            for (_name, _block_idx, size, _mtime) in entries {
                // The block filename encodes the path hash but
                // not the original remote path. We can't
                // reconstruct the CacheKey `(path, Some(idx))`
                // without the path, so we store a synthetic key
                // with the filename as the path component. The
                // actual CacheKey will be populated on the first
                // read (when the caller passes the real path),
                // and the preheated entry will be superseded.
                //
                // For now, preheating serves as a "the L2 disk
                // has data" signal for the LRU evictor — it
                // knows the cache dir isn't empty and can make
                // better eviction decisions.
                disk_cache_index.insert((_name, Some(_block_idx)), (size, now));
            }
            tracing::debug!(
                preheated = disk_cache_index.len(),
                "multi-level cache: L2 preheated from disk"
            );
        }
        Self {
            l1: MemCacheLayer::new(mem_cache),
            l2: DiskBlockCache::new(cache_dir, disk_cache_index, direct_io),
            metrics,
        }
    }

    /// Unified block lookup: L1 → L2 (backfill L1 on L2 hit).
    /// Returns `None` when both levels miss — the caller should
    /// fetch from the remote backend (L3) and call `populate`.
    pub fn read_block(&self, path: &str, ino: u64, block_idx: u64) -> Option<Bytes> {
        // L1: in-memory cache
        if let Some(data) = self.l1.get_block(path, ino, block_idx) {
            self.metrics.record_cache_hit("l1");
            return Some(data);
        }
        self.metrics.record_cache_miss("l1");

        // L2: on-disk block cache
        if let Some(data) = self.l2.get_block(path, ino, block_idx) {
            self.metrics.record_cache_hit("l2");
            // Backfill L1 so the next read on this block hits memory.
            self.l1.put_block(path, ino, block_idx, data.clone());
            return Some(data);
        }
        self.metrics.record_cache_miss("l2");

        None
    }

    /// Backfill L1 + L2 after a remote fetch. `blocks` contains
    /// one `Bytes` per 8 MiB block, starting at `first_block_idx`.
    ///
    /// The `admission` policy controls L1 promotion:
    ///   * `All` — every block goes to L1 + L2
    ///   * `SingleBlockOnly` — only single-block fetches go to L1;
    ///     multi-block fetches go to L2 only (avoids scan pollution)
    ///   * `None` — L2 only
    #[allow(dead_code)]
    pub fn populate(
        &self,
        path: &str,
        ino: u64,
        first_block_idx: u64,
        blocks: &[Bytes],
        admission: AdmissionPolicy,
    ) {
        let promote_l1 = match admission {
            AdmissionPolicy::All => true,
            AdmissionPolicy::SingleBlockOnly => blocks.len() == 1,
            AdmissionPolicy::None => false,
        };

        for (i, data) in blocks.iter().enumerate() {
            let block_idx = first_block_idx + i as u64;
            // L2 always gets the block (disk is cheap, survives restarts).
            self.l2.put_block(path, ino, block_idx, data.clone());
            // L1 per admission policy.
            if promote_l1 {
                self.l1.put_block(path, ino, block_idx, data.clone());
            }
        }
    }

    /// Drop both L1 and L2 entries for the given path/inode.
    /// Called by the write path after a successful local write
    /// to prevent stale cached data from shadowing the new content.
    pub fn invalidate(&self, path: &str, ino: u64) {
        self.l1.invalidate_path(path, ino);
        self.l2.invalidate_path(path, ino);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::DashMapMemCache;

    fn test_mlc(dir: PathBuf) -> MultiLevelCache {
        let mc: Arc<dyn MemCache> = Arc::new(DashMapMemCache::new(0));
        let idx = Arc::new(DashMap::new());
        MultiLevelCache::new(mc, dir, idx, false, crate::metrics::global())
    }

    #[test]
    fn read_block_l1_hit() {
        let dir = std::env::temp_dir().join(format!("mntrs-mlc-test-{}-l1", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let mlc = test_mlc(dir.clone());
        let data = Bytes::from_static(b"block data");
        mlc.l1.put_block("file.bin", 1, 0, data.clone());
        let got = mlc.read_block("file.bin", 1, 0).unwrap();
        assert_eq!(got.as_ref(), b"block data");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_block_l2_hit_backfills_l1() {
        let dir = std::env::temp_dir().join(format!("mntrs-mlc-test-{}-l2", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let mlc = test_mlc(dir.clone());
        let data = Bytes::from(vec![0xAB; 4096]);
        // Put in L2 only
        mlc.l2.put_block("file.bin", 1, 2, data.clone());
        // L1 should miss
        assert!(mlc.l1.get_block("file.bin", 1, 2).is_none());
        // read_block should find it in L2 and backfill L1
        let got = mlc.read_block("file.bin", 1, 2).unwrap();
        assert_eq!(got.as_ref(), data.as_ref());
        // Now L1 should have it
        assert!(mlc.l1.get_block("file.bin", 1, 2).is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_block_both_miss() {
        let dir = std::env::temp_dir().join(format!("mntrs-mlc-test-{}-miss", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let mlc = test_mlc(dir.clone());
        assert!(mlc.read_block("nope.bin", 99, 0).is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn populate_single_block_admits_l1() {
        let dir = std::env::temp_dir().join(format!("mntrs-mlc-test-{}-pop1", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let mlc = test_mlc(dir.clone());
        let data = Bytes::from_static(b"single");
        mlc.populate(
            "f.bin",
            1,
            0,
            std::slice::from_ref(&data),
            AdmissionPolicy::SingleBlockOnly,
        );
        assert!(mlc.l1.get_block("f.bin", 1, 0).is_some());
        assert!(mlc.l2.get_block("f.bin", 1, 0).is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn populate_multi_block_skips_l1_with_single_block_only() {
        let dir = std::env::temp_dir().join(format!("mntrs-mlc-test-{}-popn", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let mlc = test_mlc(dir.clone());
        let blocks = vec![Bytes::from_static(b"block0"), Bytes::from_static(b"block1")];
        mlc.populate("f.bin", 1, 0, &blocks, AdmissionPolicy::SingleBlockOnly);
        // L1 should NOT have them (multi-block with SingleBlockOnly)
        assert!(mlc.l1.get_block("f.bin", 1, 0).is_none());
        assert!(mlc.l1.get_block("f.bin", 1, 1).is_none());
        // L2 should have both
        assert!(mlc.l2.get_block("f.bin", 1, 0).is_some());
        assert!(mlc.l2.get_block("f.bin", 1, 1).is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalidate_drops_both_levels() {
        let dir = std::env::temp_dir().join(format!("mntrs-mlc-test-{}-inv", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let mlc = test_mlc(dir.clone());
        let data = Bytes::from_static(b"data");
        mlc.l1.put_block("f.bin", 1, 0, data.clone());
        mlc.l2.put_block("f.bin", 1, 0, data.clone());
        mlc.invalidate("f.bin", 1);
        assert!(mlc.l1.get_block("f.bin", 1, 0).is_none());
        assert!(mlc.l2.get_block("f.bin", 1, 0).is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
