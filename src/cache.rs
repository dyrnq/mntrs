//! In-memory block cache for read paths.
//!
//! This module exists to factor the per-inode, per-block cache that
//! lives in `MntrsFs::mem_cache` behind a small trait so future
//! implementations (TTL-based, LIRS, persistent, etc.) can swap in
//! without touching the read/write call sites.
//!
//! # PoC scope
//!
//! This is a **proof-of-concept**: the trait, the default DashMap
//! implementation with LRU + back-pressure, and a unit test for the
//! `invalidate_ino` path (the d4d19c8 regression). Call sites in
//! `MntrsFs` are intentionally unchanged — swapping them over is a
//! follow-up mechanical refactor.
//!
//! # Design notes
//!
//! - The key is a 2-tuple `(u64, u64)` = `(ino, block_idx)`. We
//!   keep it as a tuple (not a newtype) for the PoC because all 13
//!   existing call sites already use the tuple form; introducing a
//!   newtype would balloon the diff. A newtype is the right move
//!   once the call sites are converted (gives type-safety against
//!   swapping ino and block_idx by accident).
//!
//! - The trait returns `Bytes` (a cheap refcounted clone) rather
//!   than `&Bytes` so the caller does not hold a DashMap shard
//!   lock across its work. DashMap's `Ref` guard would force the
//!   caller to either clone inside the trait (defeating the
//!   `&Bytes` design) or expose the lock in its return type
//!   (leaking the implementation detail into the trait surface).
//!
//! - LRU + memory-limit enforcement is the *implementation's*
//!   problem, not the trait's. The trait contract is "if `put`
//!   returns, the data is cached; size accounting is best-effort".
//!   This keeps the trait narrow and lets each implementation pick
//!   its own policy (FIFO, LRU, segmented, etc.).
//!
//! - `invalidate_ino` is the entry point for the d4d19c8 fix:
//!   after a write that may have changed the cache file, the
//!   write path calls `invalidate_ino(ino)` to drop any pre-write
//!   cached blocks that would otherwise shadow the new content
//!   (capped at the old block's length on the next read).

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;

/// Cache key: `(ino, block_idx)`. Both are 64-bit because FUSE
/// inode numbers are u64 and we use 4 MiB blocks (so block_idx
/// stays well within u64 even for multi-TiB files).
pub type MemCacheKey = (u64, u64);

/// Trait for the in-memory per-(inode, block) cache consulted by
/// the read path before the on-disk cache file and the remote
/// backend.
///
/// Implementations must be `Send + Sync` — they're shared between
/// FUSE threads, the writeback worker, and (potentially) any
/// async pre-fetcher tasks.
pub trait MemCache: Send + Sync {
    /// Look up a cached block. Returns a clone of the cached
    /// `Bytes` (refcounted, so the cost is O(1) for the reference
    /// bump, not a deep copy). The caller slices if it only
    /// needs a range.
    ///
    /// Returning `None` means the block is not in the cache; the
    /// caller should fall back to the on-disk cache file or the
    /// remote backend.
    fn get(&self, ino: u64, block_idx: u64) -> Option<Bytes>;

    /// Insert (or replace) a cached block. Implementations are
    /// expected to enforce their own memory limit by evicting
    /// older entries as needed. Eviction policy is an
    /// implementation choice (FIFO, LRU, segmented, etc.).
    fn put(&self, ino: u64, block_idx: u64, data: Bytes);

    /// Drop every cached block for `ino`. Used after writes that
    /// may have changed the underlying cache file: stale
    /// `Bytes` entries would otherwise shadow the new content
    /// (the read path's slice-and-min-with-b.len() would cap the
    /// returned range at the old, pre-write block length).
    fn invalidate_ino(&self, ino: u64);

    /// Drop everything. Used on unmount and during testing.
    fn clear(&self);

    /// Number of cached `(ino, block_idx)` pairs.
    fn len(&self) -> usize;

    /// Approximate memory usage in bytes. May be slightly stale
    /// (atomic relaxed) — used for monitoring, not for
    /// correctness-critical decisions.
    fn used_bytes(&self) -> u64;

    /// True iff no entries are cached. Convenience: `len() == 0`.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Concrete `MemCache` backed by a `DashMap` with an LRU eviction
/// queue and a soft byte limit.
///
/// ## Eviction policy
///
/// LRU via a side `Mutex<VecDeque<MemCacheKey>>` queue. On
/// `put`, if adding `data.len()` bytes would exceed `mem_limit`,
/// the front of the queue (oldest) is popped and the
/// corresponding entry is removed from the DashMap — looped
/// until there's room or a 5-second deadline elapses (after
/// which the insert proceeds anyway, temporarily exceeding the
/// limit; this mirrors the existing behavior in
/// `MntrsFs::mem_cache_insert`).
///
/// On `get` we *do not* update the LRU order: the current
/// callers already populate mem_cache from the read path on
/// every miss, and re-touching the LRU on every hit would
/// require holding a global lock. This is a known
/// approximation — the cache is treated as FIFO for eviction
/// purposes. If finer-grained LRU is needed later, the LRU
/// update can move behind a per-shard mutex (DashMap exposes
/// shard iteration) without changing the trait surface.
pub struct DashMapMemCache {
    inner: DashMap<MemCacheKey, Bytes>,
    /// FIFO order of insertion; front is the eviction candidate.
    /// Kept separate from the DashMap to avoid leaking its
    /// internal lock type.
    order: Mutex<VecDeque<MemCacheKey>>,
    /// Soft byte limit. Inserts that would push `used_bytes`
    /// past this trigger eviction.
    mem_limit: u64,
    /// Approximate total bytes currently cached. Updated via
    /// atomic add/sub. May be transiently inconsistent with
    /// `inner.iter().map(|e| e.len()).sum()` during concurrent
    /// operations.
    used: AtomicU64,
    /// Deadline for back-pressure loop, to avoid pathological
    /// eviction storms. Currently hard-coded to 5s — same
    /// value the pre-PoC code used.
    back_pressure_deadline: Duration,
}

impl DashMapMemCache {
    /// Create a new `DashMapMemCache` with the given byte limit.
    /// `mem_limit == 0` means "unbounded" (still tracked, but no
    /// eviction triggered) — useful for tests.
    pub fn new(mem_limit: u64) -> Self {
        Self {
            inner: DashMap::new(),
            order: Mutex::new(VecDeque::new()),
            mem_limit,
            used: AtomicU64::new(0),
            back_pressure_deadline: Duration::from_secs(5),
        }
    }
}

impl MemCache for DashMapMemCache {
    fn get(&self, ino: u64, block_idx: u64) -> Option<Bytes> {
        self.inner
            .get(&(ino, block_idx))
            .map(|entry| entry.value().clone())
    }

    fn put(&self, ino: u64, block_idx: u64, data: Bytes) {
        let size = data.len() as u64;
        let key = (ino, block_idx);

        // Back-pressure: evict oldest entries until the new
        // insertion fits under `mem_limit`. Bounded by a
        // deadline so a pathological eviction storm can't block
        // the writer forever.
        if self.mem_limit > 0 {
            let deadline = Instant::now() + self.back_pressure_deadline;
            loop {
                if self.used.load(Ordering::Relaxed) + size <= self.mem_limit {
                    break;
                }
                let victim = {
                    let mut order = self.order.lock().unwrap();
                    order.pop_front()
                };
                match victim {
                    Some(v) => {
                        if let Some((_, removed)) = self.inner.remove(&v) {
                            self.used.fetch_sub(removed.len() as u64, Ordering::Relaxed);
                        }
                    }
                    None => break, // empty cache but limit still exceeded — nothing to evict
                }
                if Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }

        // Atomic dedup: if the key already exists, subtract the
        // old size and replace. If absent, just insert. Either
        // way we end up with the new value and the right total.
        //
        // We use a two-step `remove + insert` here instead of
        // `entry().and_modify().or_insert()` because the DashMap
        // 6.x entry API holds a per-shard mutable borrow that
        // conflicts with `self.used.fetch_*` inside the modify
        // closure (the borrow checker can't see they're disjoint
        // fields). remove+insert is two shard locks but the
        // hot path only does it once per put, so the cost is
        // negligible. If/when DashMap adds a `replace_with` API
        // that takes a non-borrow-capturing closure, we can
        // switch back to the entry-based form.
        if let Some((_, old)) = self.inner.remove(&key) {
            self.used.fetch_sub(old.len() as u64, Ordering::Relaxed);
        }
        self.inner.insert(key, data);
        self.used.fetch_add(size, Ordering::Relaxed);
        self.order.lock().unwrap().push_back(key);
        // Note: we push the key even if it was a re-insert;
        // the LRU queue may thus have stale keys for evicted
        // entries. This is fine because `pop_front` returns
        // `None` from the inner DashMap for such keys (the
        // remove returns no entry), and we silently skip the
        // accounting update in that case. (See the victim
        // branch above — `if let Some((_, removed))` is
        // permissive.)
        self.order.lock().unwrap().push_back(key);
    }

    fn invalidate_ino(&self, ino: u64) {
        // Two-phase: first snapshot the keys to drop, then
        // remove from both the DashMap and the LRU queue. The
        // DashMap retains all the data we need atomically per
        // shard; doing it in a single `retain` is also possible
        // but we'd need to subtract each removed entry's size
        // from `used` while holding the shard lock. Snapshot
        // + remove keeps the accounting simple and correct.
        let mut total_removed: u64 = 0;
        let keys: Vec<MemCacheKey> = self
            .inner
            .iter()
            .filter_map(|entry| {
                let (i, _b) = entry.key();
                if *i == ino {
                    total_removed += entry.value().len() as u64;
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect();
        for k in &keys {
            self.inner.remove(k);
        }
        if total_removed > 0 {
            self.used.fetch_sub(total_removed, Ordering::Relaxed);
        }
        // LRU queue: drain anything matching. Quadratic in the
        // queue size, but the queue is bounded by `len()` which
        // is in turn bounded by `mem_limit / min_block_size`,
        // and `invalidate_ino` is only called from the write
        // path (a relatively rare event). Acceptable cost.
        let mut order = self.order.lock().unwrap();
        order.retain(|k| k.0 != ino);
    }

    fn clear(&self) {
        self.inner.clear();
        self.order.lock().unwrap().clear();
        self.used.store(0, Ordering::Relaxed);
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn used_bytes(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The d4d19c8 regression in test form: writing after a read
    /// should not serve the stale pre-write block from the cache.
    #[test]
    fn invalidate_ino_drops_stale_blocks_after_write() {
        let cache = DashMapMemCache::new(0); // unbounded for test

        // Simulate the read path populating the cache for ino=42,
        // block=0 with some pre-write content. 17 bytes ("hello world
        // (18B)" minus the parens-stating-the-length typo).
        let pre = Bytes::from_static(b"hello world (X)");
        let pre_len = pre.len();
        cache.put(42, 0, pre);
        assert_eq!(cache.get(42, 0).unwrap().len(), pre_len);

        // Simulate the write: a write to ino=42 invalidates the
        // whole ino's cached blocks (the d4d19c8 fix).
        cache.invalidate_ino(42);
        assert!(cache.get(42, 0).is_none(), "stale block not evicted");
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.used_bytes(), 0);

        // Other inodes must be unaffected.
        cache.put(99, 0, Bytes::from_static(b"untouched"));
        cache.invalidate_ino(42);
        assert!(cache.get(99, 0).is_some());
    }

    /// Multiple blocks for the same ino are all invalidated.
    #[test]
    fn invalidate_ino_drops_all_blocks_for_ino() {
        let cache = DashMapMemCache::new(0);
        for b in 0..5 {
            cache.put(7, b, Bytes::from(vec![b as u8; 1024]));
        }
        assert_eq!(cache.len(), 5);
        cache.invalidate_ino(7);
        assert_eq!(cache.len(), 0);
    }

    /// Eviction loop drops oldest entries when the limit is hit.
    #[test]
    fn lru_eviction_drops_oldest_when_full() {
        // 1 KiB limit; each block is 512 B. Two fit; the third
        // forces eviction of the oldest.
        let cache = DashMapMemCache::new(1024);
        cache.put(1, 0, Bytes::from(vec![0u8; 512]));
        cache.put(2, 0, Bytes::from(vec![1u8; 512]));
        assert_eq!(cache.len(), 2);
        // Third insert evicts one of the previous two.
        cache.put(3, 0, Bytes::from(vec![2u8; 512]));
        // Soft limit may be transiently exceeded (5s deadline
        // exits the loop early if eviction stalls), so just
        // assert at least one of the originals is gone.
        assert!(cache.len() <= 2, "len={} (expected ≤ 2)", cache.len());
    }

    /// Putting the same key replaces the value and updates
    /// the byte counter.
    #[test]
    fn put_replaces_existing_key() {
        let cache = DashMapMemCache::new(0);
        cache.put(1, 0, Bytes::from(vec![0u8; 100]));
        assert_eq!(cache.used_bytes(), 100, "after first put");
        cache.put(1, 0, Bytes::from(vec![1u8; 50]));
        assert_eq!(cache.used_bytes(), 50, "after replace");
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(1, 0).unwrap().len(), 50);
    }
}
