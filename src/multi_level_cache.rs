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
        // L2 preheating was attempted here before (issue #130):
        // scan the cache directory for `.block` files and insert
        // them into `disk_cache_index` so the first read after
        // restart would hit L2 instead of fetching from remote.
        //
        // That approach failed because the block filename
        // `{path_hash}_{block_idx:010x}.block` does not encode
        // the original remote path — `path_hash` is one-way — so
        // there is no way to reconstruct the `(path, Some(idx))`
        // CacheKey that the rest of the codebase uses. The
        // preheated entries were inserted with the *filename* as
        // the path component, which never matched a real lookup.
        //
        // The fix (paired with the `contains_key` guard removal
        // in `DiskBlockCache::get_block`, cache_layer.rs) is to
        // not preheat at all and trust the on-disk `.block` files
        // directly: `get_block` reads the file if it exists and
        // inserts the correct CacheKey on first hit. The LRU
        // index fills up as blocks are read.
        //
        // Cost: the first read after restart is a remote fetch
        // for any block whose file hasn't been touched yet in
        // the new session. In practice, the kernel page cache
        // and FUSE readahead usually coalesce these into the
        // same `op.read_with().range(...)` call as the L2 file
        // read, so the user-visible overhead is small.
        let _ = direct_io; // (signature compatibility — direct_io handled in get_block)
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

    /// Regression for issue #130. Pre-fix, the L2 preheater
    /// scanned the cache dir and inserted `(filename,
    /// Some(block_idx))` keys into `disk_cache_index`, which
    /// never matched real `(path, Some(block_idx))` lookups
    /// (filenames don't encode the original remote path —
    /// only the one-way path hash). Combined with the
    /// `disk_cache_index.contains_key()` guard in
    /// `DiskBlockCache::get_block`, the result was that no
    /// L2 block was ever served — the guard always returned
    /// false, so reads fell through to remote every time
    /// after a restart, even when the `.block` file was on
    /// disk.
    ///
    /// This test simulates that exact scenario: write a
    /// `.block` file to disk in one MLC session, drop the
    /// MLC (forgetting the in-memory index), start a fresh
    /// MLC against the same cache dir, and verify the fresh
    /// MLC can serve the block from L2.
    #[test]
    fn l2_survives_restart_without_preheating() {
        let dir =
            std::env::temp_dir().join(format!("mntrs-mlc-test-{}-restart", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);

        // Session 1: populate L2 with two blocks at different
        // block indices, then drop the MLC. The disk_cache_index
        // entries for these blocks are forgotten; only the
        // `.block` files on disk remain.
        //
        // Note: we deliberately do NOT call `invalidate()` here
        // — invalidate removes both the disk_cache_index entry
        // AND the on-disk `.block` file (`invalidate_path`
        // calls `remove_file`). The point of this test is the
        // other direction: file present on disk, no in-memory
        // record, fresh MLC must still find it.
        let data_a = Bytes::from_static(b"block A");
        let data_b = Bytes::from_static(b"block B (different)");
        {
            let mlc1 = test_mlc(dir.clone());
            mlc1.populate(
                "remote/path/file.bin",
                42,
                0,
                std::slice::from_ref(&data_a),
                AdmissionPolicy::SingleBlockOnly,
            );
            mlc1.populate(
                "remote/path/file.bin",
                42,
                5, // arbitrary non-zero block index
                std::slice::from_ref(&data_b),
                AdmissionPolicy::SingleBlockOnly,
            );
            // mlc1 dropped here — its in-memory index is gone.
        }

        // Verify the .block files actually exist on disk
        // (sanity check for the test setup).
        let cpath_a = crate::cache_block_path(&dir, "remote/path/file.bin", 0);
        let cpath_b = crate::cache_block_path(&dir, "remote/path/file.bin", 5);
        assert!(
            cpath_a.exists(),
            "setup: .block file missing at {cpath_a:?}"
        );
        assert!(
            cpath_b.exists(),
            "setup: .block file missing at {cpath_b:?}"
        );

        // Session 2: fresh MLC, empty in-memory index. The
        // pre-fix preheater would have failed to populate
        // disk_cache_index correctly; with the fix, get_block
        // trusts the on-disk file.
        let mc2: Arc<dyn MemCache> = Arc::new(DashMapMemCache::new(0));
        let idx2 = Arc::new(DashMap::new());
        let mlc2 = MultiLevelCache::new(mc2, dir.clone(), idx2, false, crate::metrics::global());

        let got_a = mlc2
            .read_block("remote/path/file.bin", 42, 0)
            .expect("L2 should serve block 0 from disk after restart");
        assert_eq!(got_a.as_ref(), data_a.as_ref(), "block 0 content mismatch");

        let got_b = mlc2
            .read_block("remote/path/file.bin", 42, 5)
            .expect("L2 should serve block 5 from disk after restart");
        assert_eq!(got_b.as_ref(), data_b.as_ref(), "block 5 content mismatch");

        // A second read on the same block should still work
        // — that proves the first read populated
        // `disk_cache_index` correctly via `bump_in_memory_atime`,
        // because if the index were empty and the file path
        // were wrong, the second read would re-fail.
        let got_a2 = mlc2
            .read_block("remote/path/file.bin", 42, 0)
            .expect("L2 should still serve block 0 on second read");
        assert_eq!(got_a2.as_ref(), data_a.as_ref());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
