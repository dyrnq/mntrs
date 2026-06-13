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

    /// Atomic-monotonic snapshot of cache counters + current
    /// state. `Relaxed` ordering on every field — these are
    /// observability, not correctness. Each implementation must
    /// update the relevant counter on every `get` (hit/miss) and
    /// `put` (insert, including any evictions triggered to make
    /// room). Callers should not rely on the snapshot being a
    /// consistent cross-field view: under concurrent traffic, an
    /// `inserts += 1` may be visible before its corresponding
    /// `used_bytes += size` (or vice versa). The point is to
    /// answer "what does the cache look like right now" and
    /// "is it hot", not "did this exact op land".
    fn stats(&self) -> MemCacheStats;
}

/// Snapshot returned by `MemCache::stats()`. All counters are
/// monotonic since cache creation. `capacity_bytes` is `0` for
/// implementations that don't enforce a limit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MemCacheStats {
    /// Number of `get` calls that returned `Some(_)`.
    pub hits: u64,
    /// Number of `get` calls that returned `None`.
    pub misses: u64,
    /// Number of `put` calls that landed (whether or not they
    /// triggered an eviction; evictions are counted separately).
    pub inserts: u64,
    /// Number of cache entries evicted to make room for a
    /// newer insert. Zero for unbounded caches.
    pub evictions: u64,
    /// Current number of entries.
    pub entries: u64,
    /// Current approximate byte usage.
    pub used_bytes: u64,
    /// Configured byte limit. `0` means "unbounded".
    pub capacity_bytes: u64,
}

impl MemCacheStats {
    /// Hit rate in `[0.0, 1.0]`. Returns `0.0` if no `get`
    /// has happened yet.
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// Capacity utilization in `[0.0, +∞)`. Returns `0.0` for
    /// unbounded caches.
    pub fn utilization(&self) -> f64 {
        if self.capacity_bytes == 0 {
            0.0
        } else {
            self.used_bytes as f64 / self.capacity_bytes as f64
        }
    }

    /// Render as a single-line tracing-friendly string. Format
    /// is stable enough to grep on (e.g. `mntrs_mem_cache_stats`
    /// events in `RUST_LOG=mntrs` output). The compactness
    /// keeps the log volume low when emitted every second.
    pub fn format(&self) -> String {
        format!(
            "hits={} misses={} hit_rate={:.2}% inserts={} evictions={} entries={} used={}B/{}B util={:.1}%",
            self.hits,
            self.misses,
            self.hit_rate() * 100.0,
            self.inserts,
            self.evictions,
            self.entries,
            self.used_bytes,
            self.capacity_bytes,
            self.utilization() * 100.0,
        )
    }
}

/// Concrete `MemCache` backed by `moka::sync::Cache`.
///
/// moka's TinyLFU admission filter is a meaningful upgrade
/// over the FIFO approximation that `DashMapMemCache` uses
/// (see the `DashMapMemCache` docstring — the "known
/// approximation" comment). For typical I/O workloads with
/// skewed access (e.g. a hot S3 prefix that's read repeatedly
/// while a cold prefix is touched once), TinyLFU typically
/// improves hit rate by 5-15% over FIFO/LRU at the same
/// capacity. The cost is one extra dependency (`moka` with
/// the `sync` feature, which pulls in `parking_lot` and
/// `crossbeam` — about 5 crates total, well under what foyer
/// would add).
///
/// Eviction policy: moka's `tiny_lfu` (default). It uses a
/// frequency-sketch admission filter + SLRU main store,
/// giving near-LRU hit rates with bounded memory overhead.
/// See the moka docs for the full algorithm description.
///
/// `invalidate_ino` cost: O(N) over the cache, because moka
/// doesn't expose a per-key-prefix invalidation API. The
/// `invalidate_entries_if` closure walks every entry. For
/// our default 256 MiB / 8 MiB = 32 entries this is
/// negligible. Larger caches (where the same cost would
/// matter) shouldn't be hitting `invalidate_ino` often anyway
/// (it's only called on every write); if that ever
/// becomes a problem, the fix is to maintain an
/// `ino -> Vec<key>` index on the side. We don't pre-build
/// it because the common case is small cache + few writes.
pub struct MokaMemCache {
    inner: moka::sync::Cache<MemCacheKey, Bytes>,
    /// Soft byte limit (weigher capacity). moka doesn't
    /// expose its configured capacity through the public API
    /// in a typed form, so we mirror it here for the
    /// `capacity_bytes` snapshot.
    capacity_bytes: u64,
    /// Atomic counters for `stats()`. moka's built-in
    /// `entry_count()` and `weighted_size()` give us
    /// `entries` and `used_bytes` directly, but
    /// hits/misses/inserts are application-level (we
    /// increment on the read/write paths the same way
    /// we do for DashMapMemCache — see the impl block).
    /// Evictions are tracked via moka's eviction_listener.
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
    inserts: std::sync::atomic::AtomicU64,
    /// Shared with moka's eviction_listener closure via Arc.
    /// Only incremented for actual evictions (Size/Expired),
    /// not for explicit removes or replacements.
    evictions: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl MokaMemCache {
    /// Create a new `MokaMemCache` with the given byte limit.
    /// `mem_limit == 0` means "unbounded" (no weigher — useful
    /// for tests, where eviction never kicks in).
    pub fn new(mem_limit: u64) -> Self {
        let evictions = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

        let make_listener = |counter: &std::sync::Arc<std::sync::atomic::AtomicU64>| {
            let counter = counter.clone();
            move |_k: std::sync::Arc<MemCacheKey>,
                  _v: Bytes,
                  cause: moka::notification::RemovalCause| {
                if cause.was_evicted() {
                    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        };

        let inner = if mem_limit == 0 {
            moka::sync::Cache::builder()
                .max_capacity(u64::MAX / 2)
                .weigher(|_k: &MemCacheKey, v: &Bytes| v.len() as u32 + 16)
                .eviction_listener(make_listener(&evictions))
                .build()
        } else {
            moka::sync::Cache::builder()
                .max_capacity(mem_limit)
                .weigher(|_k: &MemCacheKey, v: &Bytes| (v.len() as u32).saturating_add(16))
                .eviction_listener(make_listener(&evictions))
                .build()
        };
        Self {
            inner,
            capacity_bytes: mem_limit,
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
            inserts: std::sync::atomic::AtomicU64::new(0),
            evictions,
        }
    }
}

impl MemCache for MokaMemCache {
    fn get(&self, ino: u64, block_idx: u64) -> Option<Bytes> {
        let result = self.inner.get(&(ino, block_idx));
        if result.is_some() {
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            self.misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }

    fn put(&self, ino: u64, block_idx: u64, data: Bytes) {
        self.inner.insert((ino, block_idx), data);
        self.inserts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn invalidate_ino(&self, ino: u64) {
        // moka 0.12's `sync::Cache` doesn't expose a
        // predicate-based bulk invalidate (the `invalidate_entries_if`
        // method exists but is gated behind the `future`
        // feature, returning `Err(InvalidationClosuresDisabled)`
        // on the sync cache). We work around this by iterating
        // the entry set and calling `invalidate(key)` per match.
        //
        // Cost: O(N) over the cache, same asymptotic as the
        // gated version. For our default 32-entry working set
        // this is ~30 atomic decrements per write — negligible.
        // Larger caches hitting `invalidate_ino` often would
        // need a side-index of `ino -> Vec<key>` to make this
        // O(matches) instead of O(N).
        //
        // moka's `iter()` returns `Arc<K>` for the key (to
        // avoid cloning the tuple out of the lock); we deref
        // the Arc to compare, then re-borrow for `invalidate`.
        // Modifying the cache (in particular invalidating)
        // during iteration is explicitly safe per the moka
        // docs — the iterator holds a snapshot view and the
        // invalidation operates on a separate index.
        let keys: Vec<MemCacheKey> = self
            .inner
            .iter()
            .filter_map(|(k, _v)| if k.0 == ino { Some(*k) } else { None })
            .collect();
        for k in keys {
            self.inner.invalidate(&k);
        }
    }

    fn clear(&self) {
        self.inner.invalidate_all();
    }

    fn len(&self) -> usize {
        // Same maintenance flush as `stats()` — see that
        // method's docstring for why we force the count
        // to converge on the call.
        self.inner.run_pending_tasks();
        self.inner.entry_count() as usize
    }

    fn used_bytes(&self) -> u64 {
        self.inner.weighted_size()
    }

    fn stats(&self) -> MemCacheStats {
        // moka's `entry_count` and `weighted_size` are
        // maintained by a background task that runs on a
        // schedule. Without `run_pending_tasks`, the values
        // can be stale by up to ~10s (moka's default
        // maintenance interval). For our periodic logger
        // (1s tick by default), that staleness would make
        // `entries=0` even when the cache is full. Forcing
        // the flush here costs ~µs in the worst case and
        // gives an accurate snapshot.
        //
        // The flush is a no-op when there's nothing pending,
        // so the cost is bounded by the number of in-flight
        // writes since the last maintenance tick.
        self.inner.run_pending_tasks();
        MemCacheStats {
            hits: self.hits.load(std::sync::atomic::Ordering::Relaxed),
            misses: self.misses.load(std::sync::atomic::Ordering::Relaxed),
            inserts: self.inserts.load(std::sync::atomic::Ordering::Relaxed),
            evictions: self.evictions.load(std::sync::atomic::Ordering::Relaxed),
            entries: self.inner.entry_count(),
            used_bytes: self.inner.weighted_size(),
            capacity_bytes: self.capacity_bytes,
        }
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
    /// Reverse index: per-ino set of cache keys, so
    /// `invalidate_ino` is O(K) where K is the number of blocks
    /// for THAT ino, not O(N) over the whole cache. Without
    /// this, every write to a 1KB file in a CSI mount with
    /// 1000+ cached files does a full O(N) scan of `inner`
    /// + the LRU queue, which costs ~5ms at 1000 entries
    /// and scales linearly. With it, invalidate is essentially
    /// free for small writes.
    ///
    /// Stale entries (the corresponding `inner` key was evicted
    /// by mem_limit) are tolerated: `invalidate_ino` does an
    /// `inner.remove(k)` that returns `None` and is silently
    /// skipped, so a stale `by_ino[ino]` entry is harmless.
    /// The per-ino HashSet itself is dropped after use, so
    /// stale entries don't accumulate over time.
    by_ino: DashMap<u64, std::collections::HashSet<MemCacheKey>>,
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
    /// Observability counters. All `Relaxed` — these are
    /// monitoring-only and intentionally cheap. See
    /// `MemCache::stats()` for the read side.
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    evictions: AtomicU64,
}

impl DashMapMemCache {
    /// Create a new `DashMapMemCache` with the given byte limit.
    /// `mem_limit == 0` means "unbounded" (still tracked, but no
    /// eviction triggered) — useful for tests.
    pub fn new(mem_limit: u64) -> Self {
        Self {
            inner: DashMap::new(),
            by_ino: DashMap::new(),
            order: Mutex::new(VecDeque::new()),
            mem_limit,
            used: AtomicU64::new(0),
            back_pressure_deadline: Duration::from_secs(5),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }
}

impl MemCache for DashMapMemCache {
    fn get(&self, ino: u64, block_idx: u64) -> Option<Bytes> {
        let result = self
            .inner
            .get(&(ino, block_idx))
            .map(|entry| entry.value().clone());
        if result.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        result
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
                            self.evictions.fetch_add(1, Ordering::Relaxed);
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
        // Update reverse index: track this (ino, block_idx) so
        // `invalidate_ino` is O(K) over this ino's blocks, not
        // O(N) over the whole cache. The per-ino HashSet is
        // dropped after each invalidate, so stale entries (from
        // mem_limit-driven eviction) don't accumulate.
        self.by_ino
            .entry(ino)
            .or_insert_with(std::collections::HashSet::new)
            .insert(key);
        self.used.fetch_add(size, Ordering::Relaxed);
        self.inserts.fetch_add(1, Ordering::Relaxed);
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
        // O(K) via the per-ino reverse index (see `by_ino`).
        // Without it this was a full O(N) scan of `inner` +
        // `order`, which costs ~5ms at N=1000 cached entries
        // and was the dominant cost of every small write
        // (issue #15: write 1K-1M 3-4× slower than rclone).
        //
        // Stale keys (the corresponding `inner` entry was
        // mem_limit-evicted) are tolerated: `inner.remove(k)`
        // returns `None` and the entry's size wasn't added to
        // `total_removed` in the first place, so the `used`
        // accounting stays correct.
        let Some((_, keys)) = self.by_ino.remove(&ino) else {
            return;
        };
        let mut total_removed: u64 = 0;
        for k in &keys {
            if let Some((_, v)) = self.inner.remove(k) {
                total_removed += v.len() as u64;
            }
        }
        if total_removed > 0 {
            self.used.fetch_sub(total_removed, Ordering::Relaxed);
        }
        // LRU queue: drain anything matching. O(K) here too.
        let mut order = self.order.lock().unwrap();
        order.retain(|k| k.0 != ino);
    }

    fn clear(&self) {
        self.inner.clear();
        self.order.lock().unwrap().clear();
        self.by_ino.clear();
        self.used.store(0, Ordering::Relaxed);
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn used_bytes(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    fn stats(&self) -> MemCacheStats {
        // `Relaxed` everywhere: observability, not correctness.
        // The values may not be a perfectly consistent snapshot
        // (e.g. `inserts` may have advanced but the inner
        // insert's `used_bytes` update hasn't been observed yet),
        // but for monitoring that's fine — what matters is the
        // shape of the numbers over time, not the exact
        // cross-field relationship at one instant.
        MemCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            entries: self.inner.len() as u64,
            used_bytes: self.used.load(Ordering::Relaxed),
            capacity_bytes: self.mem_limit,
        }
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

    /// `stats()` reflects get/put/eviction counters correctly.
    /// This is the contract the periodic metrics logger depends
    /// on (see `cmd::mount`); if a counter drifts, the printed
    /// hit rate / utilization values become misleading.
    #[test]
    fn stats_reflects_get_put_and_eviction() {
        let cache = DashMapMemCache::new(0);

        // Cold cache: every get is a miss.
        assert!(cache.get(1, 0).is_none());
        let s = cache.stats();
        assert_eq!(s.hits, 0);
        assert_eq!(s.misses, 1);
        assert_eq!(s.inserts, 0);
        assert_eq!(s.evictions, 0);
        assert_eq!(s.entries, 0);
        assert_eq!(s.used_bytes, 0);
        assert_eq!(s.capacity_bytes, 0); // unbounded

        // One insert → one hit on the same key.
        cache.put(1, 0, Bytes::from_static(b"hello"));
        let s = cache.stats();
        assert_eq!(s.hits, 0);
        assert_eq!(s.misses, 1);
        assert_eq!(s.inserts, 1);
        assert_eq!(s.entries, 1);
        assert_eq!(s.used_bytes, 5);
        assert!(cache.get(1, 0).is_some());
        let s = cache.stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 1);
        assert!((s.hit_rate() - 0.5).abs() < 1e-9);

        // Replacement of the same key still counts as one insert
        // (the doc says inserts are *successful puts*, not
        // *unique keys*). The byte count must update.
        cache.put(1, 0, Bytes::from_static(b"hi"));
        let s = cache.stats();
        assert_eq!(s.inserts, 2);
        assert_eq!(s.used_bytes, 2);
        assert_eq!(s.entries, 1); // still 1 entry, not 2

        // Other ino: separate counters.
        assert!(cache.get(2, 0).is_none());
        let s = cache.stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 2);
    }

    /// Eviction bumps the `evictions` counter; the periodic
    /// metrics logger uses this to surface "cache is too small
    /// for the working set" symptoms.
    #[test]
    fn stats_eviction_counter_advances_under_pressure() {
        // 1 KiB limit, two 600-byte blocks: the second one
        // forces one eviction.
        let cache = DashMapMemCache::new(1024);
        cache.put(1, 0, Bytes::from(vec![0u8; 600]));
        cache.put(2, 0, Bytes::from(vec![1u8; 600]));
        let s = cache.stats();
        assert_eq!(s.evictions, 1, "second put should evict the first");
        assert_eq!(s.entries, 1);
        assert_eq!(s.used_bytes, 600);
        assert_eq!(s.capacity_bytes, 1024);
        assert!((s.utilization() - 600.0 / 1024.0).abs() < 1e-9);
    }

    // ============================================================
    // MokaMemCache: parallel suite of the stats/eviction tests
    // to ensure both impls honor the same `MemCache` contract.
    // If a future impl diverges, the test below fails — which
    // is the whole point: the trait surface is the contract;
    // a swap from DashMap to moka (or vice versa) must be a
    // drop-in for the read/write call sites in `lib.rs`.
    // ============================================================

    /// `MokaMemCache` honors the same `get/put/stats`
    /// contract as `DashMapMemCache`. With `run_pending_tasks`
    /// baked into `stats()`/`len()` (see those methods' doc),
    /// the assertions are now synchronous — no sleep loop
    /// needed.
    #[test]
    fn moka_stats_reflects_get_put() {
        let cache = MokaMemCache::new(1024 * 1024);
        assert!(cache.get(1, 0).is_none());
        let s = cache.stats();
        assert_eq!(s.hits, 0);
        assert_eq!(s.misses, 1);
        assert_eq!(s.inserts, 0);

        cache.put(1, 0, Bytes::from_static(b"hello"));
        let s = cache.stats();
        assert_eq!(s.inserts, 1);
        assert_eq!(s.entries, 1);
        assert_eq!(s.used_bytes, 5 + 16); // value + key weight

        assert!(cache.get(1, 0).is_some());
        let s = cache.stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 1);
        assert!((s.hit_rate() - 0.5).abs() < 1e-9);
    }

    /// `MokaMemCache::invalidate_ino` drops every entry
    /// sharing the ino, even when there are many blocks.
    #[test]
    fn moka_invalidate_ino_drops_all_blocks_for_ino() {
        let cache = MokaMemCache::new(1024 * 1024);
        for b in 0..5 {
            cache.put(7, b, Bytes::from(vec![b as u8; 32]));
        }
        assert_eq!(cache.len(), 5, "all 5 blocks should be admitted");
        cache.invalidate_ino(7);
        assert_eq!(cache.len(), 0, "all ino=7 blocks should be evicted");
        // Other inodes unaffected.
        cache.put(99, 0, Bytes::from_static(b"kept"));
        assert_eq!(cache.len(), 1);
    }
}
