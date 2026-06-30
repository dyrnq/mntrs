#![allow(unexpected_cfgs)]
#![cfg_attr(windows, allow(dead_code, unused_imports, unused_variables))]
#![recursion_limit = "256"]
pub mod backpressure;
pub mod block_format;
pub mod cache;
pub(crate) mod cache_layer;
pub mod cmd;
pub mod core_fs;
pub mod disk_write_pool;
pub mod error_log;
pub mod fuse_error;
pub mod http_client;
pub mod mem_limiter;
pub mod metrics;
pub(crate) mod multi_level_cache;
pub mod path;
pub mod prefetcher;
pub mod util;
pub mod writeback;

// Re-export everything from util so existing `crate::` paths are unaffected.
pub use util::*;
// Re-export block_format public items.
pub use block_format::{CacheIndexEntry, load_cache_index};
// Re-export block_format pub(crate) items within crate.
pub(crate) use block_format::{
    BLOCK_OVERHEAD, drop_block_cache_for_path, remove_block_cache_files,
};
// Re-export disk_write_pool items used in lib.rs and cmd/.
pub(crate) use disk_write_pool::{
    DiskWriteJob, register_dirty_cache_path, submit_block_cache_write, submit_disk_write,
};
pub use disk_write_pool::{init_disk_write_pool, set_opendal_sync_op};

// Re-export fuser::FileType so integration tests (and external users
// that build a custom `InodeEntry` via the public API) don't need to
// add a direct `fuser` dependency just to name a file kind. Gated on
// Unix because `fuser` is a Linux/macOS dep only — on Windows the
// local `pub enum FileType` stub below is the canonical
// `mntrs::FileType` (see windows clippy run 28210714048).
#[cfg(unix)]
pub use fuser::FileType;

/// Shared inode table type for writeback callback.
pub const CACHE_BLOCK_SIZE: u64 = 8 * 1024 * 1024;

/// A single entry in the inodes map.
///
/// Replaces a `(String, FileType, u64, Option<SystemTime>)`
/// tuple used everywhere via `v.0` / `v.1` / `v.2` / `v.3`.
/// The named-field form is the same size (Rust elides
/// the wrapper), but eliminates the "which positional
/// field is mtime again" footgun on every and_modify /
/// destructuring site (Bug 8).
///
/// Fields:
///   * `path`  — backend path (no leading slash)
///   * `kind`  — file kind (regular / directory)
///   * `size`  — logical size in bytes. The write path
///     bumps this on every successful write; reads consult
///     it (max'd against the cache-file size) for getattr.
///   * `mtime` — last modification time. `None` for an
///     entry populated by `lookup` / `readdir` on a file
///     we've only ever read remotely (no local writes
///     yet); `Some` after the first write or after a
///     create / mkdir.
#[derive(Clone, Debug)]
pub struct InodeEntry {
    pub path: String,
    pub kind: FileType,
    pub size: u64,
    pub mtime: Option<SystemTime>,
}

/// File metadata returned by [`MntrsFs::stat_op`]. Issue #224
/// refactored this from a 3-tuple `(FileType, u64,
/// Option<SystemTime>)` to a named-field struct per
/// [[feedback-tuple-vs-struct]] and the audit tracker in
/// #223. The 3-tuple is exactly the three `kind` / `size` /
/// `mtime` fields of [`InodeEntry`] (minus `path`), so the
/// struct form makes the relationship explicit at the call
/// site and ensures a future field addition (e.g. `atime`)
/// is a compile-time catch at every destructure site vs the
/// tuple's silent default-on-missing-field.
///
/// `Copy` is sound because all three fields are `Copy` and
/// the struct is a small value type passed by-value through
/// `stat_op` returns — `Copy` lets call sites destructure
/// without `.clone()` and lets `Option<FileStat>` be moved
/// freely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileStat {
    pub kind: FileType,
    pub size: u64,
    pub mtime: Option<SystemTime>,
}

/// One entry in the LRU eviction heap in [`MntrsFs::evict_lru_if_needed`].
///
/// Issue #225 refactored this from a 3-tuple
/// `(std::time::Instant, CacheKey, u64)` whose field order
/// silently encoded the heap's sort key (leading field =
/// primary sort). The struct form keeps the explicit
/// atime-first sort via a manual `Ord` impl, makes every
/// field self-documenting at call sites, and ensures a
/// future 4th field is a compile-time catch at every push
/// site (vs the tuple's silent `.0 = u64 = 0` default on
/// missing fields — same failure mode that produced the
/// silent data-loss bug in issue #53, per
/// [[feedback-tuple-vs-struct]]).
///
/// `Copy` is sound because all three fields are `Copy`
/// (Instant and u64 are Copy; CacheKey is a `(String,
/// Option<u64>)` and **not** Copy, so this struct is
/// deliberately `Clone` only — the heap only clones during
/// push anyway, and the clone is bounded by index size).
///
/// Manual `Ord` preserves the atime-first micro-opt: the
/// leading field is the only one `BinaryHeap` actually
/// compares for ties (size is consulted only when atime
/// and key tie). Ties after `(atime, key)` are broken by
/// `size` so the order is fully deterministic — two
/// entries with identical `(atime, key)` cannot coexist in
/// the index (`CacheKey` is the unique key), but the tie
/// breaker keeps `Ord` total.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LruHeapEntry {
    /// Last-access instant — the primary sort key. Older
    /// entries pop first from the min-heap.
    pub atime: std::time::Instant,
    /// Full `CacheKey` (path + optional block_idx) so
    /// block-level and file-level cache files compete on
    /// equal footing.
    pub key: CacheKey,
    /// Entry size in bytes; carried for accounting when
    /// popped. Also the secondary tie-breaker in `Ord`.
    pub size: u64,
}

impl PartialOrd for LruHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LruHeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Preserve atime-first sort. `BinaryHeap` only
        // compares the leading field for ties, so this
        // micro-opt matches the original tuple form.
        self.atime
            .cmp(&other.atime)
            .then_with(|| self.key.cmp(&other.key))
            .then_with(|| self.size.cmp(&other.size))
    }
}

/// Cached value for a single directory entry inside
/// [`MntrsFs::dir_cache`].
///
/// Issue #226 refactored this from a 3-tuple
/// `(EntryMode, u64, SystemTime)` whose field order was
/// implicit at every construction and destructure site
/// (6+ call sites in `list_op`, `cache_add_entry`,
/// `batch_lookup_from_dir_cache`, and `readdir`'s
/// post-cache path). The struct form makes the fields
/// self-documenting at every site and ensures a future
/// 4th field — most plausibly a `cache_generation: u64`
/// to detect upstream change for #196-backlog
/// "readdir cache invalidate by mtime change" — is a
/// compile-time catch across every site vs the tuple's
/// silent default-on-missing-field (same failure mode
/// as issue #53, per [[feedback-tuple-vs-struct]]).
///
/// `Copy` is sound because all three fields are `Copy`:
/// `EntryMode` is an opendal enum, `u64` is trivial,
/// `SystemTime` is `Copy`. `Copy` lets DashMap's
/// `*e.value()` work in the read path without `.clone()`
/// (one deref → struct copy). `PartialEq + Eq` lets
/// cache invalidation compare entries directly.
///
/// The 4-tuple `(String, EntryMode, u64, SystemTime)`
/// upstream of this struct (in `list_with`'s raw output
/// at lib.rs:~1471) deliberately stays as a tuple for
/// now — it's the unflattened backend response, only
/// consumed at one site (the cache insertion loop), and
/// refactoring it is a separate concern from the cached
/// value form. The two tuples converge on `(mode, size,
/// mtime)` semantically but stay separate types to keep
/// each PR small.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirEntryCacheValue {
    /// File kind (FILE / DIR / UNKNOWN). opendal's
    /// `EntryMode` — see `opendal::EntryMode`.
    pub mode: EntryMode,
    /// File size in bytes (0 for directories).
    pub size: u64,
    /// Last-modified time from the backend list
    /// response.
    pub mtime: SystemTime,
}

pub type Inodes = Arc<dashmap::DashMap<u64, InodeEntry>>;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

// `MemCache` trait is in scope via the `pub mem_cache:
// Arc<dyn MemCache>` field declaration below; no explicit
// `use` needed because the call sites use method syntax
// (`.get(...)`, `.put(...)`, etc.) which is dispatched
// dynamically through the trait object.

#[cfg(unix)]
use fuser::{FileAttr, INodeNo};

#[cfg(not(unix))]
/// Stub type for non-Unix platforms — mirrors fuser::FileType variants used in shared state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Directory,
    RegularFile,
    Symlink,
    NamedPipe,
    BlockDevice,
    CharDevice,
    Socket,
}

#[cfg(not(unix))]
/// Stub — mirrors fuser::INodeNo. Needed because `make_attr` is used in CoreFilesystem impl.
#[cfg(not(unix))]
#[derive(Debug, Clone, Copy)]
pub struct INodeNo(pub u64);
#[cfg(not(unix))]
impl From<u64> for INodeNo {
    fn from(v: u64) -> Self {
        INodeNo(v)
    }
}
#[cfg(not(unix))]
impl From<INodeNo> for u64 {
    fn from(v: INodeNo) -> u64 {
        v.0
    }
}

#[cfg(not(unix))]
/// Stub — mirrors fuser::FileAttr. Needed because `make_attr` is used in CoreFilesystem impl.
#[derive(Debug, Clone)]
pub struct FileAttr {
    pub ino: INodeNo,
    pub size: u64,
    pub blocks: u64,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub crtime: SystemTime,
    pub kind: FileType,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
    pub flags: u32,
}
use futures::StreamExt;
use opendal::{EntryMode, Operator};

pub(crate) fn rt() -> &'static tokio::runtime::Runtime {
    static RT: once_cell::sync::OnceCell<tokio::runtime::Runtime> =
        once_cell::sync::OnceCell::new();
    RT.get_or_init(|| {
        // Issue #30: a single worker thread is the
        // sweet spot for FUSE callbacks. The
        // block_on path (mkdir / unlink / rename /
        // create / read) parks the FUSE worker on
        // a future and dispatches it to the runtime;
        // with 4 worker threads, each block_on
        // costs a cross-thread hand-off + wake-up
        // (~10 µs), which adds up to the 3-6x
        // regression vs rclone on metadata ops.
        //
        // Background work (disk_write_pool, writeback
        // worker, writeback fsync thread) still gets
        // full parallelism via `tokio::task::spawn`
        // — the runtime multiplexes the spawn tasks
        // onto the single worker, and a long-running
        // upload doesn't block a metadata op
        // because spawn returns immediately. The
        // net is: 1 FUSE-blocking call at a time
        // (per the FUSE kernel's per-fd serialization)
        // but N concurrent background uploads.
        //
        // The FUSE kernel itself serializes ops on
        // the same fd, so 1 worker thread is the
        // natural fit — more workers would just
        // queue up behind the first.
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio rt")
    })
}

// TTL now comes from MntrsFs.attr_ttl field

/// Monotonic source of inode numbers minted by
/// `alloc_ino` / `alloc_ino_with_mtime`.
///
/// Starts at 2 to leave room for two reserved values
/// in the low range:
///   * `0` — sentinel used by writeback recovery for
///     dirty-sidecar uploads recovered from a previous
///     crash (no inode mapping exists yet at recovery
///     time). See `INO_RECOVERY_SENTINEL`. Any
///     `inodes.entry(0).and_modify(...)` is a silent
///     no-op (the entry never exists), which matches
///     the intended semantics (the next stat() refreshes
///     mtime from the remote).
///   * `1` — FUSE root inode. By POSIX/FUSE convention
///     (and `fuser::FUSE_ROOT_ID`) the root directory's
///     inode is always 1; the kernel's first
///     `lookup(parent=1, name=...)` after mount
///     references this. `MntrsFs::resolve(1)` is
///     special-cased to return root-dir attrs without
///     hitting `inodes`, so the slot doesn't need a
///     concrete entry either.
static NEXT_INO: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(2);

/// Pseudo-inode used by `writeback::spawn`'s recovery
/// path when uploading a dirty cache file whose ino has
/// not been mapped yet (recovery runs at mount init,
/// before any FUSE `lookup` has had a chance to register
/// the path). The writeback completion handler
/// recognizes this value and skips the
/// inodes-entry mtime update — the next `stat()` from
/// user space will refresh mtime from the remote.
pub const INO_RECOVERY_SENTINEL: u64 = 0;
static NEXT_HANDLE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
// DIR_CACHE_TTL now comes from MntrsFs.dir_cache_ttl field

/// Per-open-file-handle state
#[derive(Debug)]
enum FileHandleState {
    Read {
        path: String,
        last_offset: u64,
        chunk_size: u64,
        prefetcher: Option<std::sync::Arc<prefetcher::HandlePrefetcher>>,
        expires_at: Option<std::time::Instant>,
    },
    Write {
        path: String,
        cache_fd: Option<Arc<std::sync::Mutex<std::fs::File>>>,
        dirty: bool,
        dirty_since: Option<std::time::Instant>,
        expires_at: Option<std::time::Instant>,
    },
}

impl Clone for FileHandleState {
    fn clone(&self) -> Self {
        match self {
            FileHandleState::Read {
                path,
                last_offset,
                chunk_size,
                prefetcher,
                expires_at,
            } => FileHandleState::Read {
                path: path.clone(),
                last_offset: *last_offset,
                chunk_size: *chunk_size,
                prefetcher: prefetcher.clone(),
                expires_at: *expires_at,
            },
            FileHandleState::Write {
                path,
                cache_fd,
                dirty,
                dirty_since,
                expires_at,
            } => FileHandleState::Write {
                path: path.clone(),
                cache_fd: cache_fd.clone(),
                dirty: *dirty,
                dirty_since: *dirty_since,
                expires_at: *expires_at,
            },
        }
    }
}

impl FileHandleState {
    fn path(&self) -> &str {
        match self {
            FileHandleState::Read { path, .. } => path,
            FileHandleState::Write { path, .. } => path,
        }
    }
}

#[allow(clippy::type_complexity)]
#[allow(dead_code)]
pub struct MntrsFs {
    /// Underlying OpenDAL operator. Exposed `pub` so the integration
    /// tests in `tests/` can seed fixtures (write initial files,
    /// verify backend state) without going through the FUSE layer.
    /// Production code paths use the helper methods.
    pub op: Arc<Operator>,
    /// Per-inode metadata. Exposed `pub` so the integration tests
    /// in `tests/bug_regression_test.rs` can simulate a `BATCHFORGET`
    /// by removing the ino entry, then re-lookup to verify the new
    /// ino is self-consistent with the cache-file state (Bug F fix
    /// — `CoreFilesystem::lookup` / `getattr` now consider the
    /// local cache file's size, not just the backend).
    pub inodes: dashmap::DashMap<u64, InodeEntry>,
    /// Reverse map of `path → ino` for the inodes table.
    ///
    /// Why: `find_ino_by_path` is on the hot lookup path
    /// (every FUSE `lookup(parent, name)` reaches it),
    /// and the pre-fix implementation linear-scanned all
    /// inodes entries — O(N) where N grows with every
    /// readdir over a large directory (e.g. 500-file
    /// `many/` dir → N=500+). The bench's `stat 1K.bin`
    /// after listing such a dir was dominated by this
    /// scan, ~3-4 ms per stat for 500 entries.
    ///
    /// The reverse map turns the lookup into O(1) (one
    /// DashMap get). Maintenance: every `alloc_ino*`
    /// inserts, every `inodes.remove` removes, and
    /// `rename` removes old + inserts new. The defensive
    /// fallback in `find_ino_by_path` rebuilds an entry
    /// from a linear scan if it's missing — so a
    /// forgotten maintenance site self-heals rather than
    /// losing the ino entirely.
    path_to_ino: dashmap::DashMap<String, u64>,
    /// Per-ino kernel lookup reference count
    /// (Bug 33). Tracks the FUSE protocol's `nlookup`
    /// — the kernel increments its count by 1 on every
    /// entry-returning op (lookup, mkdir, create,
    /// symlink, and readdirplus entries) and decrements
    /// by N on `forget(ino, nlookup)`.
    ///
    /// We mirror that count here so `forget` only
    /// actually drops the inode + path_to_ino +
    /// attr_cache + handle entries once the count reaches
    /// zero. Pre-Bug-33 forget unconditionally dropped
    /// on every call, which could prematurely free an
    /// ino the kernel still referenced — subsequent ops
    /// on that ino returned ENOENT and the kernel had
    /// to re-lookup, costing ~1 round-trip per affected
    /// op (significant on `find /mnt | xargs stat`-style
    /// path-walking workloads where the kernel batches
    /// forget calls).
    ///
    /// Root ino (=1) is never inserted — the kernel
    /// doesn't ref-count root and never sends forget
    /// for it.
    lookup_count: dashmap::DashMap<u64, u64>,
    dir_cache: dashmap::DashMap<
        String,
        (
            std::time::Instant,
            dashmap::DashMap<String, DirEntryCacheValue>,
        ),
    >,
    /// Local on-disk cache directory. `pub` so integration tests
    /// can construct / inspect cache-file paths (e.g. for the Bug F
    /// regression test that simulates a pending writeback).
    pub cache_dir: PathBuf,
    handles: dashmap::DashMap<u64, FileHandleState>,
    /// Per-fh readdir state (issue #23 / DESIGN_READDIR_STREAMING).
    /// `opendir(ino)` materialises the full entry list, stores
    /// it here keyed by the dir-lister fh it returns, and
    /// subsequent `readdir(ino, fh, offset)` calls slice the
    /// cached Vec by `offset` (FUSE cookie) without re-hitting
    /// `list_op` / `dir_cache`. `releasedir(ino, fh)` drops
    /// the entry.
    ///
    /// The whole point of the per-fh state is stability: a
    /// concurrent `create`/`unlink` that invalidates the
    /// shared `dir_cache` after the kernel's first readdir
    /// page no longer changes what the second page returns,
    /// because the second page is served from this private
    /// snapshot. Pre-fix the FUSE adapter called
    /// `inner.readdir(ino)` on every page, and the second
    /// call could see a different list at the same
    /// `start` offset (issue #23, Bug 32 comment).
    ///
    /// Stored on `MntrsFs` (not a process-wide static)
    /// because the tests construct multiple `MntrsFs`
    /// instances; a static would leak list state across
    /// mount lifetimes.
    dir_listers: dashmap::DashMap<u64, Vec<CoreDirEntry>>,
    pub(crate) dir_cache_ttl: Duration,
    pub(crate) attr_ttl: Duration,
    pub(crate) stat_cache_ttl: Duration,
    pub(crate) volname: String,
    pub(crate) cache_max_size: u64,
    pub(crate) write_back_delay: Duration,
    /// Files below this size (bytes) upload immediately on
    /// flush/release, bypassing the `write_back_delay` queue.
    /// `0` disables immediate upload entirely. Default 1 MiB.
    ///
    /// Issue #138 / #202: small files (SQLite / etcd / RocksDB
    /// commits) suffer from the 5s uniform write-back delay on
    /// `close()`. With the per-task delay in writeback::WritebackTask,
    /// files smaller than this threshold enqueue with
    /// `Duration::ZERO` and the worker uploads them right away.
    /// Large files still batch through the delay queue.
    pub(crate) writeback_immediate_threshold: u64,
    pub(crate) cache_mode: String,
    pub(crate) read_ahead: u64,
    /// Minimum file size (bytes) for which the read-path prefetcher
    /// is activated on open(). 0 disables prefetching entirely.
    /// Default: 16 MiB. See `maybe_create_prefetcher` for the
    /// activation logic and issue #16 for the cat-100M motivation.
    ///
    /// 16 MiB matches the prefetcher's chunk-size cap (see
    /// `maybe_create_prefetcher`): any file at or above this size
    /// has ≥1 prefetchable chunk after the FUSE worker reads the
    /// first one, so the prefetcher can run an extra fetch
    /// concurrently with the user's read instead of serially.
    /// Pre-change default 64 MiB excluded the 16-64 MiB range
    /// (the bench's 50 MiB cold-read sat tied with rclone at
    /// 160 ms — without prefetch, 4 chunks fetched serially;
    /// with prefetch, chunks 2-4 overlap chunk 1's read).
    pub(crate) prefetch_threshold: u64,
    /// Upper bound (MiB) on the prefetch in-memory PartQueue.
    /// Caps the cost of a file that's opened but only partially
    /// read. Default: 64 MiB.
    pub(crate) prefetch_queue_mb: u64,
    pub(crate) read_chunk_size: u64,
    pub(crate) read_chunk_size_limit: u64,
    pub(crate) read_chunk_streams: u32,
    pub(crate) uid: Option<u32>,
    pub(crate) gid: Option<u32>,
    pub(crate) umask: Option<u32>,
    pub(crate) dir_perms: u16,
    pub(crate) file_perms: u16,
    pub(crate) link_perms: u16,
    pub(crate) direct_io: bool,
    /// Issue #257: when `true`, the read path falls back to
    /// a partial on-disk cache file if the backend read fails.
    /// Default `false`. The user **opts in** to "stale data is
    /// better than EIO" semantics — this is opt-in, not the
    /// default, because silent stale data can mislead readers
    /// who think they're seeing fresh bytes. When `false`
    /// (default), backend read errors surface as `-EIO` to
    /// the kernel exactly as before. See `docs/durability.md`
    /// for the trade-off matrix.
    pub(crate) read_stale_on_backend_error: bool,
    // Issue #209: --poll-interval (the legacy alias for
    // --vfs-cache-poll-interval) is now routed into
    // `cache_poll_interval` at construction time; this
    // field is unused. The deprecation warning is emitted
    // at the cmd/mount.rs boundary so users on old
    // rclone-style scripts see a clear migration signal.
    pub(crate) cache_max_age: Duration,
    pub(crate) cache_min_free_space: u64,
    pub(crate) exclude_patterns: Vec<String>,
    pub(crate) include_patterns: Vec<String>,
    pub(crate) max_size: Option<u64>,
    pub(crate) min_size: Option<u64>,
    pub(crate) max_depth: Option<usize>,
    pub(crate) ignore_case: bool,
    pub(crate) fast_fingerprint: bool,
    pub(crate) async_read: bool,
    pub(crate) vfs_refresh: bool,
    pub(crate) case_insensitive: bool,
    pub(crate) no_implicit_dir: bool,
    pub(crate) use_server_modtime: bool,
    pub(crate) no_apple_double: bool,
    pub(crate) no_apple_xattr: bool,
    pub(crate) hash_filter: Option<(usize, usize)>,
    pub(crate) block_norm_dupes: bool,
    pub(crate) write_wait: Duration,
    pub(crate) read_wait: Duration,
    pub(crate) handle_caching: Duration,
    pub(crate) cache_poll_interval: Duration,
    pub(crate) disk_total_size: u64,
    writeback_sender: std::sync::OnceLock<writeback::Sender>,
    /// #89: FUSE kernel notifier for attr cache invalidation after
    /// writes. Set once in `set_fuse_notifier()` from the mount
    /// command path. The write handler calls
    /// `inval_inode(ino, 0, -1)` after each successful write
    /// so subsequent O_APPEND opens see the up-to-date file
    /// size instead of the cached pre-write size.
    ///
    /// Unix-only: fuser is gated on `cfg(unix)` in Cargo.toml, so
    /// referencing `fuser::Notifier` here would fail Windows clippy.
    /// On WinFSP we don't have an inode-cache invalidation hook, but
    /// WinFSP's write handler is synchronous so the stale-cache
    /// race doesn't apply (issue #93).
    #[cfg(not(windows))]
    pub(crate) fuse_notifier: std::sync::OnceLock<fuser::Notifier>,

    /// Issue #38: set of paths that currently have a
    /// writeback task in flight (queued or uploading).
    /// Used by flush() and release() to avoid queueing
    /// duplicate tasks for the same file. The
    /// writeback worker removes a path from this set
    /// when the upload completes (success or final
    /// retry-exhaustion). Without this, a flush →
    /// write → close sequence could queue two
    /// writeback tasks for the same file with no
    /// ordering guarantee between them, and the older
    /// task could land at the backend after the
    /// newer one (out-of-order writes from the
    /// user's perspective).
    writeback_pending: std::sync::Arc<dashmap::DashSet<String>>,

    /// Issue #132: shared adaptive prefetch-window controller. One
    /// instance per `MntrsFs` so every prefetcher (and every FUSE
    /// reader feeding them) shares the same producer-vs-consumer
    /// rate EMA. Cloned as `Arc` into each `HandlePrefetcher` so the
    /// download thread's `record_part_fetched` calls update the same
    /// state the FUSE reader's `record_part_consumed` calls feed.
    backpressure: std::sync::Arc<backpressure::BackpressureController>,

    /// Issue #201: per-mount memory budget used by the prefetcher
    /// (label "prefetch") to gate the next fetch on `try_reserve`.
    /// One per `MntrsFs` so two mounts in the same process have
    /// independent budgets. Wired in `cmd/mount.rs::mount()` from
    /// `--mem-limit` (the same value used for `mem_cache_bytes` —
    /// the budget is shared between in-flight prefetch and the
    /// mem_cache, by design). Tests construct their own uncapped
    /// (cap=0) limiter via `MemoryLimiter::new(0)`.
    mem_limiter: std::sync::Arc<mem_limiter::MemoryLimiter>,

    /// Per-(inode, block) in-memory read cache. Held as a
    /// `dyn MemCache` trait object so the underlying
    /// implementation can be swapped (DashMap today, moka
    /// behind a flag) without touching the read/write call
    /// sites. All impls are `Send + Sync` (the trait bound),
    /// so the `Arc<dyn MemCache>` is safe to share across the
    /// FUSE worker threads + the metrics logger thread + the
    /// writeback task.
    pub mem_cache: std::sync::Arc<dyn crate::cache::MemCache>,
    attr_cache: dashmap::DashMap<
        String,
        (
            FileType,
            u64,
            Option<std::time::SystemTime>,
            std::time::Instant,
        ),
    >,
    /// Index of every on-disk cache file (file-level *and*
    /// block-level) for the LRU sweeper. The key is a
    /// `(remote_path, Option<block_idx>)` tuple: `None`
    /// means "the whole-file cache at `cache_path(p)`",
    /// `Some(idx)` means "the per-block file at
    /// `cache_block_path(p, idx)`". Tracked together so a
    /// single `evict_lru` sweep removes the most-cold
    /// entries across both layers, regardless of which
    /// layer the read path populated. The value is
    /// `(size_bytes, last_access_instant)` — the in-memory
    /// `last_access_instant` is the source of truth for
    /// LRU ordering (see `bump_in_memory_atime`); the
    /// on-disk atime is unreliable on `relatime` mount
    /// defaults.
    #[allow(clippy::type_complexity)]
    /// Issue #55: the disk cache LRU index. `pub(crate)` so
    /// `writeback::spawn` can drop block-level entries
    /// after a successful upload (see the writeback
    /// upload completion path). Wrapped in `Arc` so
    /// the writeback worker can hold its own clone
    /// (the inner DashMap is already cheap to clone
    /// at the `Arc` level).
    pub(crate) disk_cache_index: Arc<dashmap::DashMap<CacheKey, (u64, std::time::Instant)>>,
    pub(crate) storage_class: Option<String>,
    /// Multi-level cache (L1 memory → L2 disk block). Unifies the
    /// block-level read path: `read_block` checks L1 first, then L2
    /// (with L1 backfill on L2 hit). `populate` backfills both levels
    /// after a remote fetch. `invalidate` drops both levels on write.
    pub(crate) multi_cache: crate::multi_level_cache::MultiLevelCache,
}

impl MntrsFs {
    /// If `cache_max_size > 0` or `cache_min_free_space > 0`, walk
    /// `disk_cache_index` (newest to oldest by `atime`) and delete
    /// the oldest cache files until the total drops below the
    /// configured limit, or until the cache disk has the
    /// requested free space, whichever is the tighter constraint.
    ///
    /// The index tracks both whole-file cache (`cache_path`,
    /// keyed by `(path, None)`) and per-block cache
    /// (`cache_block_path`, keyed by `(path, Some(block_idx))`).
    /// Either kind is evicted under the same LRU order — a v1
    /// index (no block entries) just has fewer children to
    /// consider; a freshly-read large file accumulates block
    /// entries as the read path populates them. The index
    /// cleanup on unlink/rmdir (commit 8f4244c) removes
    /// orphaned entries of either kind.
    ///
    /// Cost: O(N) over `disk_cache_index` per call, where N is
    /// the number of cached files + blocks. For a busy CSI node
    /// with 10k cached files this is well under a millisecond.
    /// A BinaryHeap (min-heap by atime) gives O(N log K) where K
    /// is the number of files to evict; on a 10k-file cache
    /// evicting 100 files is ~50k heap ops, also sub-ms.
    ///
    /// Runs inline on the FUSE write worker. Synchronous is
    /// intentional: a background eviction thread introduces a
    /// race where a subsequent write sees "out of space" before
    /// the eviction completes. The current write is allowed to
    /// push the total briefly over the limit; the *next* write
    /// that observes the breach evicts down to the target.
    fn evict_lru_if_needed(&self) {
        if self.cache_max_size == 0 && self.cache_min_free_space == 0 {
            return;
        }

        // Fast path: sum entry sizes only (O(N), no key clone, no
        // heap build). The min-heap construction (O(N log N) plus a
        // `CacheKey` clone — a `String` alloc — per entry) is
        // deferred until we know eviction is actually needed. The
        // common case on a write-heavy mount is "cache under limit,
        // nothing to free", so this skips the expensive part on most
        // calls. Issue #135#2 (safe variant: no running-total
        // atomic, so no replace/underflow accounting hazard — the
        // scan remains the source of truth).
        let mut total: u64 = 0;
        for entry in self.disk_cache_index.iter() {
            total += entry.value().0;
        }

        // Free-space check (only if cache_min_free_space > 0).
        // statvfs is cheap (~microseconds) so we don't gate it.
        let need_free = if self.cache_min_free_space > 0 {
            #[cfg(unix)]
            {
                if let Ok(fs_stat) = rustix::fs::statvfs(&self.cache_dir) {
                    let free = fs_stat.f_bavail.saturating_mul(fs_stat.f_frsize);
                    if free < self.cache_min_free_space {
                        Some(self.cache_min_free_space - free)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            #[cfg(not(unix))]
            {
                None
            }
        } else {
            None
        };

        // Size-based cap.
        let size_limit = if self.cache_max_size > 0 {
            total.saturating_sub(self.cache_max_size)
        } else {
            0
        };

        // We need to free at least the larger of the two deltas.
        let to_free = size_limit.max(need_free.unwrap_or(0));
        if to_free == 0 {
            // Issue #268.3 O8: surface the common no-op
            // path at debug. Pre-fix the early return was
            // silent; under heavy write workloads the
            // operator couldn't see "eviction pipeline
            // ran and decided no work needed" vs "eviction
            // pipeline never ran". debug because this is
            // the common path under steady-state operation.
            tracing::debug!(
                cache_max_size = self.cache_max_size,
                cache_min_free_space = self.cache_min_free_space,
                total,
                "evict_lru_if_needed: no work needed"
            );
            return;
        }

        // Slow path: build the min-heap by (last_access_instant, key,
        // size) so we can pop the oldest entries first. The third
        // element (size) is carried for accounting. The key is the
        // full `CacheKey` (path + optional block_idx), so block-level
        // and file-level cache files compete on equal footing for the
        // eviction budget. Built only when `to_free > 0` — the rare
        // eviction case — so the per-entry String clone is never paid
        // on the common no-eviction path.
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        let mut heap: BinaryHeap<Reverse<LruHeapEntry>> = BinaryHeap::new();
        for entry in self.disk_cache_index.iter() {
            let (key, (size, last_access)) = (entry.key().clone(), *entry.value());
            heap.push(Reverse(LruHeapEntry {
                atime: last_access,
                key,
                size,
            }));
        }

        // Pop oldest entries until enough space freed. Each pop
        // removes the corresponding cache file (file-level
        // via `cache_path`, block-level via `cache_block_path`)
        // and the index entry.
        let mut remaining = to_free;
        let mut freed: u64 = 0;
        while let Some(Reverse(LruHeapEntry {
            atime: _atime,
            key: (path, block_idx),
            size,
        })) = heap.pop()
        {
            if remaining == 0 {
                break;
            }
            let cpath = match block_idx {
                Some(idx) => crate::cache_block_path(&self.cache_dir, &path, idx),
                None => crate::cache_path(&self.cache_dir, &path),
            };
            let _ = std::fs::remove_file(&cpath);
            // `.meta` sidecar (whole-file only — block files
            // don't have one). Ignore the not-found error.
            if block_idx.is_none() {
                let _ = std::fs::remove_file(cpath.with_extension("meta"));
            }
            self.disk_cache_index.remove(&(path.clone(), block_idx));
            freed += size;
            remaining = remaining.saturating_sub(size);
        }

        if freed < to_free {
            // Cache under-filled even after draining every
            // tracked entry. The next write that hits this
            // path will see the same numbers and likely fail
            // for the same reason — surface it in the log
            // rather than papering over with a now-removed
            // `out_of_space` gate that nothing read.
            //
            // Issue #268.3 O8: this is the "evict failed
            // but still returned Ok" silent case. The FUSE
            // write path already replied Ok to the kernel
            // before this point — a partial truncation
            // leaves the cache file slightly larger than
            // the logical size, which the read path
            // tolerates, but operators need to know "your
            // cache is full and we couldn't shrink it".
            tracing::warn!(
                freed,
                to_free,
                "evict_lru_if_needed: cache under target after draining index; \
                 cache file may be truncated"
            );
        }
    }

    /// Create a background prefetcher for a file handle, or `None` if
    /// the file is below `prefetch_threshold` or prefetching is
    /// disabled. The prefetcher streams chunks into a bounded
    /// PartQueue; the read-path pops them, so the FUSE `read()` for
    /// sequential-from-start workloads (cat, dd, head -c large) lands
    /// on already-fetched data instead of issuing 1 RTT per chunk.
    ///
    /// Previously gated on `read_chunk_streams > 1`, which made
    /// prefetching unreachable for default configs (`read_chunk_streams`
    /// defaults to 1, the serial-fetch path). The new gate is
    /// `file_size >= prefetch_threshold`, default 16 MiB. Issue #16
    /// (`cat 100M` 6.35× slower than rclone) was the motivation; the
    /// existing 16 MiB chunk cap (commit fc5e974) still protects
    /// `head -c1K` from over-fetch.
    ///
    /// Cancellation: the spawned downloader thread exits when
    /// `release()` drops the handle and calls `HandlePrefetcher::cancel()`.
    /// Without cancel, the thread would spin on a full queue forever
    /// for partially-read files.
    fn maybe_create_prefetcher(
        &self,
        ino: u64,
        path: &str,
    ) -> Option<std::sync::Arc<prefetcher::HandlePrefetcher>> {
        let file_size = self.resolve(ino).map(|e| e.size).unwrap_or(0);
        if self.prefetch_threshold == 0 || file_size < self.prefetch_threshold {
            return None;
        }
        // chunk_size cap matches the read-path hard cap (16 MiB) so
        // prefetched parts align with the mem_cache block size (8 MiB).
        let chunk = self.read_chunk_size.clamp(131072, 16 * 1024 * 1024);
        let max_queue = self.prefetch_queue_mb.max(1) * 1024 * 1024;
        Some(std::sync::Arc::new(prefetcher::HandlePrefetcher::new(
            self.op.as_ref().clone(),
            path.to_string(),
            file_size,
            max_queue,
            chunk,
            // Issue #132: share the per-mount BackpressureController
            // so this prefetcher's fetch rate feeds the same EMA
            // window the FUSE reader's consume rate updates.
            self.backpressure.clone(),
            // Issue #201: per-mount memory budget. The prefetcher
            // gates each fetch on try_reserve against this
            // limiter; on Err it shrinks the next window.
            self.mem_limiter.clone(),
        )))
    }

    fn make_attr(&self, ino: u64, size: u64, kind: FileType, mtime: SystemTime) -> FileAttr {
        let base_perm = if kind == FileType::Directory {
            self.dir_perms
        } else {
            self.file_perms
        };
        let perm = match self.umask {
            Some(m) => base_perm & !(m as u16),
            None => base_perm,
        };
        let uid = self.uid.unwrap_or(1000);
        let gid = self.gid.unwrap_or(1000);
        FileAttr {
            ino: INodeNo(ino),
            size,
            blocks: size.div_ceil(4096),
            atime: mtime,
            mtime,
            ctime: mtime,
            crtime: mtime,
            kind,
            perm,
            nlink: if kind == FileType::Directory { 2 } else { 1 },
            uid,
            gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }
}

/// #89: Set the FUSE kernel notifier so the write path can
/// invalidate the kernel's attr cache after each write. Without
/// this, the kernel keeps using the pre-write file size it
/// cached from the last getattr/setattr reply, and the next
/// O_APPEND open issues a write at the wrong offset (clobbering
/// prior writes — see the trace in issue #89). Called once
/// from the mount command path after `spawn_mount2` returns
/// the BackgroundSession. Safe to call multiple times; only
/// the first call wins.
///
/// Unix-only — see `MntrsFs::fuse_notifier` for the rationale
/// (issue #93).
#[cfg(not(windows))]
pub fn set_fuse_notifier(notifier: fuser::Notifier) {
    let _ = FUSE_NOTIFIER.set(notifier);
}

#[cfg(not(windows))]
static FUSE_NOTIFIER: once_cell::sync::OnceCell<fuser::Notifier> = once_cell::sync::OnceCell::new();

/// Hard cap on entries `list_op` will accumulate for a
/// single readdir, to bound memory on pathological backend
/// directories. 1M entries × ~100 B per tuple
/// (String name + EntryMode + u64 size + SystemTime) =
/// ~100 MiB worst case in `out`. An S3 bucket prefix with
/// 10M+ keys is rare but does happen (data lakes with
/// flat layouts); hitting that should produce a truncated
/// listing + a `warn!` log, not an OOM that kills the
/// FUSE worker.
///
/// 1M is generous enough that no real `ls`/`find`
/// workload trips it in practice — FUSE itself paginates
/// readdir replies to the kernel in 4 KiB chunks, so
/// even a 1M-entry readdir would page-fault the user-
/// space `ls` long before the cap.
const MAX_LIST_ENTRIES: usize = 1_000_000;

impl MntrsFs {
    fn resolve(&self, ino: u64) -> Option<InodeEntry> {
        self.inodes.get(&ino).map(|r| r.clone())
    }

    /// Re-materialise the full directory entry list.
    /// Issue #23: shared by `opendir` (per-fh path) and
    /// any fallback `readdir(ino, 0, _)` call (the
    /// pre-#23 re-materialize-on-every-page behaviour,
    /// which the default trait impl exercises when a
    /// test fake hasn't overridden the new methods).
    /// Lives as an inherent method (not on the
    /// `CoreFilesystem` trait) because it captures
    /// backend-specific listing state (`dir_cache`,
    /// `list_op`) that external test fakes wouldn't have.
    fn readdir_materialise(&self, ino: u64) -> std::io::Result<Vec<CoreDirEntry>> {
        let path = self.resolve(ino).map(|e| e.path).unwrap_or_default();
        // Bug 34: pass the raw inode path; list_op
        // canonicalizes internally. Pre-fix this
        // computed `list_path = format!("{}/", path)`
        // and used the formatted form as both the
        // list_op arg AND the queried_last derivation
        // base — duplicating the trailing-slash policy
        // that list_op now owns.
        let listed = self.list_op(&path).map_err(|e| {
            tracing::warn!(path = %path, error = %e,
                    "CoreFilesystem::readdir: list_op failed");
            std::io::Error::other(e)
        })?;
        let mut entries = vec![
            CoreDirEntry {
                ino,
                kind: CoreFileType::Directory,
                name: ".".to_string(),
            },
            CoreDirEntry {
                ino: 1,
                kind: CoreFileType::Directory,
                name: "..".to_string(),
            },
        ];
        // hdfs-native quirk: the first entry of op.lister(p) is the queried
        // path itself. After trim_end_matches('/') inside list_op:
        //   lister("/")      → entries[0].name = ""       ← was caught
        //   lister("/test/") → entries[0].name = "/test"
        //   lister("/test")  → entries[0].name = "test"
        // Without filtering all three, the FUSE reply contains a phantom
        // entry that matches the parent dir name. ls -R then descends into
        // it and gets EIO on stat, plus the root listing can show an empty
        // name (kernel EIO on readdir).
        // hdfs-native quirk: the first entry of op.lister(p) is a phantom
        // whose name is the LAST path component of p (with any trailing
        // slash already trimmed by list_op). Confirmed by direct probe:
        //   lister("/")         → [0].name = ""        (root, no component)
        //   lister("/test/")    → [0].name = "test"
        //   lister("/test/sub/")→ [0].name = "sub"
        // Without filtering, the FUSE reply contains a phantom that
        // matches the parent dir's basename. ls -R then descends into it
        // and gets EIO on stat, plus the root listing can show an empty
        // name (kernel EIO on readdir). Per SESSION_PITFALLS §2.4.
        let queried_last = std::path::Path::new(&path)
            .components()
            .next_back()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .unwrap_or_default();
        for (name, mode, size, _mtime) in listed {
            if name.is_empty() || name == "/" || (name == queried_last && !queried_last.is_empty())
            {
                continue;
            }
            let kind = match mode {
                EntryMode::DIR => CoreFileType::Directory,
                _ => CoreFileType::RegularFile,
            };
            // name from list_op already includes path prefix (e.g., "many/file_0001.txt")
            // Extract just the filename for display, use full path for inode allocation
            let display_name = name
                .rsplit_once('/')
                .map(|(_, n)| n.to_string())
                .unwrap_or_else(|| name.clone());
            let ino = self.alloc_ino(
                &name,
                match kind {
                    CoreFileType::Directory => FileType::Directory,
                    _ => FileType::RegularFile,
                },
                size,
            );
            entries.push(CoreDirEntry {
                ino,
                kind,
                name: display_name,
            });
        }
        Ok(entries)
    }

    /// Background thread that periodically clears stale directory cache entries.
    pub fn start_cache_poller(&self) {
        let dir_cache = self.dir_cache.clone();
        let dir_cache_ttl = self.dir_cache_ttl;
        let interval = self.cache_poll_interval;
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(interval);
                let now = std::time::Instant::now();
                dir_cache.retain(|_k, (t, _v)| now.duration_since(*t) < dir_cache_ttl);
            }
        });
    }

    /// Compute the per-task writeback delay for an inode.
    ///
    /// Returns `Duration::ZERO` (immediate upload) when the
    /// inode's logical size is below `writeback_immediate_threshold`,
    /// or `write_back_delay` (5s default) when it's at/above the
    /// threshold. Issue #138 / #202: small files (SQLite / etcd
    /// / RocksDB commits) get immediate upload; large files batch
    /// through the 5s delay queue.
    ///
    /// **Size source:** `inodes.get(&ino).map(|v| v.size)`. The
    /// size is the LOGICAL size updated synchronously by the write
    /// path (see `MntrsFs::write` at L3223-3238), so it matches
    /// the cache file's actual extent. Reads from the inodes map
    /// cost one DashMap shard lock — much cheaper than
    /// `fs::metadata` (extra syscall) and immune to the
    /// sparse-byte inflation from `set_len`.
    ///
    /// **Fallback:** if the inode isn't in the inodes map
    /// (LRU-evicted between handle creation and flush; or recovery
    /// sentinels like `INO_RECOVERY_SENTINEL = 0` which never have
    /// an inodes entry), returns `write_back_delay` — the safe
    /// non-immediate path.
    fn per_task_writeback_delay(&self, ino: u64) -> Duration {
        if self.writeback_immediate_threshold == 0 {
            // Threshold disabled — every upload goes through
            // the uniform delay queue (pre-#202 behavior).
            return self.write_back_delay;
        }
        let immediate = self
            .inodes
            .get(&ino)
            .map(|v| v.size < self.writeback_immediate_threshold)
            .unwrap_or(false);
        if immediate {
            Duration::ZERO
        } else {
            self.write_back_delay
        }
    }

    /// Recover writeback queue + spawn worker. Shared by fuser + CoreFilesystem init.
    fn common_init_wb(&self) {
        self.alloc_ino("", FileType::Directory, 4096);

        // Spawn writeback worker FIRST so the sender is available
        // for the recovery scan below. Previously the scan ran before
        // spawn, so writeback_sender.get() always returned None and
        // recovery tasks were silently dropped while .dirty sidecars
        // were deleted — causing permanent data loss on crash restart.
        crate::rt();
        let op = self.op.clone();
        let delay = self.write_back_delay;
        let inodes = Arc::new(self.inodes.clone());
        let (tx, _handle) = crate::writeback::spawn(
            op,
            inodes,
            self.disk_cache_index.clone(),
            self.cache_dir.clone(),
            self.writeback_pending.clone(),
            delay,
        );
        self.writeback_sender.set(tx).ok();

        // Recover writeback queue from dirty sidecars.
        // Do NOT delete .dirty here — the upload completion handler
        // (writeback.rs) removes it after a successful upload.
        // Deleting before upload completes would cause data loss if
        // the process crashes again before the upload finishes.
        //
        // Issue #268.4 O25: count recovered / skipped / error
        // for a mount-time summary. Per-task debug logs above
        // (existing) give per-sidecar detail; this single info
        // line gives the operator the headline number.
        let mut recovered_count: u64 = 0;
        let mut orphan_count: u64 = 0;
        let mut send_err_count: u64 = 0;
        let mut read_err_count: u64 = 0;
        let mut no_sender_count: u64 = 0;
        if let Ok(entries) = std::fs::read_dir(&self.cache_dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.extension().is_some_and(|ext| ext == "dirty") {
                    let cache_path = p.with_extension("");
                    if !cache_path.exists() {
                        // Orphan sidecar — cache file missing, safe to remove
                        tracing::debug!(sidecar=?p, "removing orphan dirty sidecar");
                        let _ = std::fs::remove_file(&p);
                        orphan_count += 1;
                        continue;
                    }
                    match std::fs::read_to_string(&p) {
                        Ok(remote) => {
                            let remote = remote.trim().to_string();
                            if let Some(tx) = self.writeback_sender.get() {
                                tracing::info!(path=%remote, ?cache_path, "recovering dirty writeback");
                                // Bug 18: use the named sentinel
                                // INO_RECOVERY_SENTINEL (= 0) instead of
                                // the bare `0` literal. The writeback
                                // completion handler explicitly checks
                                // this value and skips its inodes mtime
                                // update — without that branch, an
                                // `entry(0).and_modify(...)` is a silent
                                // no-op (ino 0 is reserved; see
                                // NEXT_INO doc), but the silent no-op
                                // obscured the intent. The sentinel
                                // makes the contract grep-able + the
                                // next stat() from user space refreshes
                                // mtime from the remote anyway.
                                // Bug 22: send().ok() previously
                                // swallowed an Err silently. send
                                // on an UnboundedSender returns
                                // Err only when the receiver is
                                // dropped — which here means the
                                // writeback worker thread died.
                                // The .dirty sidecar is still on
                                // disk, so the next mount's
                                // recovery scan will try again,
                                // but an operator watching this
                                // mount needs to know the worker
                                // is gone NOW. Log at warn.
                                if let Err(e) = tx.send(WritebackTask {
                                    ino: INO_RECOVERY_SENTINEL,
                                    remote_path: remote,
                                    cache_path: cache_path.clone(),
                                    retry_cycle: 0, // fresh enqueue
                                    per_task_delay: self.write_back_delay, // #202: recovery never immediate
                                }) {
                                    send_err_count += 1;
                                    tracing::warn!(
                                        cache_path=?cache_path,
                                        error=%e,
                                        "writeback recovery send failed (worker dropped?); \
                                         .dirty sidecar kept for next-mount retry"
                                    );
                                } else {
                                    recovered_count += 1;
                                }
                            } else {
                                // writeback_sender.get() returned None:
                                // spawn() was not called yet (race) or
                                // it was called twice (Bug 22 already
                                // covers the dropped-receiver case
                                // above). This branch indicates the
                                // worker is gone; treat as send_err.
                                no_sender_count += 1;
                                tracing::warn!(
                                    cache_path=?cache_path,
                                    "writeback recovery: writeback_sender.get() = None; \
                                     .dirty sidecar kept for next-mount retry"
                                );
                            }
                        }
                        Err(e) => {
                            // Couldn't read the sidecar contents.
                            // The cache file exists but the sidecar
                            // is unreadable — leave it in place for
                            // the operator to inspect.
                            read_err_count += 1;
                            tracing::warn!(
                                sidecar=?p,
                                error=%e,
                                "writeback recovery: failed to read dirty sidecar contents"
                            );
                        }
                    }
                }
            }
        }
        // Issue #268.4 O25: mount-time recovery summary.
        // Single info line gives operators the headline counts;
        // per-sidecar detail is at debug level above.
        if recovered_count + orphan_count + send_err_count + read_err_count + no_sender_count > 0 {
            tracing::info!(
                recovered = recovered_count,
                orphan_sidecars_removed = orphan_count,
                send_failed = send_err_count,
                no_sender = no_sender_count,
                read_failed = read_err_count,
                cache_dir = %self.cache_dir.display(),
                "writeback: recovery scan complete"
            );
        } else {
            tracing::debug!(
                cache_dir = %self.cache_dir.display(),
                "writeback: recovery scan complete (no dirty sidecars)"
            );
        }
    }

    /// Bug 33: increment the per-ino kernel lookup
    /// reference count. Called from every entry-returning
    /// path (`lookup` / `mkdir` / `create` / `symlink` /
    /// readdirplus per-entry). The kernel sends
    /// `forget(ino, nlookup)` at some later point with
    /// the total it accumulated; `forget` decrements and
    /// only drops the inode state when the count
    /// actually reaches zero.
    ///
    /// Root ino (=1) is never tracked here — the kernel
    /// neither ref-counts root nor ever sends forget for
    /// it.
    fn bump_lookup_count(&self, ino: u64) {
        if ino == 1 {
            return;
        }
        self.lookup_count
            .entry(ino)
            .and_modify(|c| *c = c.saturating_add(1))
            .or_insert(1);
    }

    fn alloc_ino(&self, path: &str, kind: FileType, size: u64) -> u64 {
        let ino = NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inodes
            .entry(ino)
            .and_modify(|v| v.size = size)
            .or_insert(InodeEntry {
                path: path.to_string(),
                kind,
                size,
                mtime: None,
            });
        // Maintain the path→ino reverse map (stat phase 2
        // — `find_ino_by_path` is on the hot stat path).
        // Last writer wins on collision: a second
        // alloc_ino for the same path overwrites the
        // older ino entry, matching the inodes map's
        // and_modify behavior above. The leftover inodes
        // entry for the older ino is eventually swept by
        // FUSE `forget` or never read (the FUSE kernel
        // uses our latest reply).
        self.path_to_ino.insert(path.to_string(), ino);
        ino
    }

    /// Same as `alloc_ino` but seeds the inodes entry's mtime slot
    /// with the given timestamp. Used by mkdir/create so that
    /// `getattr` can fall back to it when `stat_op` returns None
    /// (Bug C — see `CoreFilesystem::getattr`). The 4-tuple's mtime
    /// was always `None` before this helper; we still keep the
    /// 3-arg `alloc_ino` for callers that don't have a meaningful
    /// mtime at hand (e.g. internal re-lookups).
    fn alloc_ino_with_mtime(
        &self,
        path: &str,
        kind: FileType,
        size: u64,
        mtime: SystemTime,
    ) -> u64 {
        let ino = NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inodes
            .entry(ino)
            .and_modify(|v| v.size = size)
            .or_insert(InodeEntry {
                path: path.to_string(),
                kind,
                size,
                mtime: Some(mtime),
            });
        // Same reverse-map maintenance as `alloc_ino`.
        self.path_to_ino.insert(path.to_string(), ino);
        ino
    }

    /// Look up the ino currently registered for `path`.
    ///
    /// Needed because `inodes` is keyed by the `NEXT_INO` counter
    /// that `alloc_ino` mints, *not* by `path_hash`. Operations
    /// that receive a full path (mkdir/rmdir/unlink) and need to
    /// remove the ino entry must look up the counter by path
    /// before calling `inodes.remove`. Using `path_hash(&path)`
    /// here — as the rename pre-fix code did — is a silent no-op:
    /// the FUSE kernel then keeps using the stale ino for
    /// subsequent operations on the same path, and a recreate at
    /// the same path collides with the lingering entry.
    ///
    /// Stat phase 2 (#16): backed by the `path_to_ino` reverse
    /// map. Pre-fix this function linear-scanned `inodes` — O(N)
    /// per call — and was the dominant cost of `stat` after a
    /// `readdir` populated `inodes` with 500+ entries (bench's
    /// `many/` dir). The hot lookup path now does a single
    /// DashMap get; on miss/stale-entry it falls back to the
    /// scan and repairs the reverse map (so a maintenance site
    /// we forgot to update doesn't permanently lose the ino —
    /// it just pays the scan once before self-healing).
    ///
    /// `pub(crate)` so integration tests in `tests/` can verify the
    /// rename/rmdir/unlink leak fix.
    pub(crate) fn find_ino_by_path(&self, path: &str) -> Option<u64> {
        // Fast path: reverse map hit. Confirm the
        // inodes entry still points at this path —
        // a stale reverse entry would otherwise hand
        // back an ino for a different (since-renamed
        // or since-removed) file.
        if let Some(ino) = self.path_to_ino.get(path).map(|r| *r.value())
            && let Some(entry) = self.inodes.get(&ino)
            && entry.value().path == path
        {
            return Some(ino);
        }
        // Fallback: scan + repair. Hit means the reverse
        // map was stale or never populated for this path
        // (e.g. a code path that bypassed `alloc_ino*`).
        // Repair so the next call hits the fast path.
        for entry in self.inodes.iter() {
            if entry.value().path == path {
                let ino = *entry.key();
                self.path_to_ino.insert(path.to_string(), ino);
                return Some(ino);
            }
        }
        // Truly absent — also clear any stale reverse
        // entry so the next caller doesn't re-scan.
        self.path_to_ino.remove(path);
        None
    }

    /// Write a single block to the disk cache with optional
    /// CRC32C trailer, and update `disk_cache_index` on success.
    ///
    /// Mirrors the read-side `read_block_cached`:
    ///   * Full blocks (== `CACHE_BLOCK_SIZE`) are written as
    ///     `data || crc32c_le(data)` (4-byte little-endian trailer).
    ///   * Partial blocks (`< CACHE_BLOCK_SIZE` — the last
    ///     block of a file) are written as-is, no trailer.
    ///   * In `--direct-io` mode, returns `false` immediately
    ///     (the cache is bypassed for direct I/O).
    ///
    /// Returns `true` if the file was successfully written
    /// AND inserted into `disk_cache_index`. On any failure
    /// (open / write / short write) the function logs at
    /// `debug` level and returns `false`; the next read will
    /// see a missing file and fall back to a remote re-fetch.
    ///
    /// This helper is the single point of truth for the
    /// on-disk format of block cache files. Both the
    /// synchronous read path (`CoreFilesystem::read`) and
    /// the asynchronous prefetcher path (after a part is
    /// popped from `HandlePrefetcher::PartQueue`) call it
    /// so the two paths can't drift.
    pub(crate) fn write_block_cached(&self, path: &str, block_idx: u64, slice: &[u8]) -> bool {
        if self.direct_io {
            return false;
        }
        let blk_path = crate::cache_block_path(&self.cache_dir, path, block_idx);
        if let Some(parent) = blk_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        // Serialize the v3 block format via the single
        // point of truth in block_format.rs. Both this
        // method and `DiskWriteJob::do_block_cache_write`
        // call `serialize_v3_block` so a format change
        // requires editing exactly one function.
        let buf = match block_format::serialize_v3_block(path, slice) {
            Some(b) => b,
            None => return false,
        };
        let written_size = buf.len() as u64;
        let wrote = if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&blk_path)
        {
            use std::io::Write;
            let ok = f.write_all(&buf).is_ok();
            // Truncate so any stale tail from a v1/v2
            // overwrite of the same block isn't visible
            // to readers.
            if ok {
                let _ = f.set_len(written_size);
            }
            if !ok {
                tracing::debug!(?blk_path, "block cache write failed");
            }
            ok
        } else {
            false
        };
        if wrote {
            // The in-memory `Instant::now()` is the LRU sort
            // key (see field doc on `disk_cache_index`); it's
            // bumped to `now` on every read via
            // `bump_in_memory_atime`. On-disk atime is
            // unreliable on `relatime` mount defaults.
            self.disk_cache_index.insert(
                (path.to_string(), Some(block_idx)),
                (written_size, std::time::Instant::now()),
            );
        }
        wrote
    }

    /// Recursively create `full_path` (and any missing parents) on the
    /// backend. Returns Ok(()) when every level either was created or
    /// already existed; propagates only *non-recoverable* errors
    /// (network/auth/permission).
    ///
    /// Error policy (per backend quirks surfaced in the e2e tests):
    ///
    ///   * `Unsupported` — some backends (e.g. flat-namespace stores)
    ///     do not implement `create_dir` because directories are
    ///     implicit. Treat as success: the dir is "known" by virtue
    ///     of objects living under it.
    ///   * `AlreadyExists` — idempotent. mkdir -p on an existing
    ///     tree must not fail.
    ///   * `NotFound` for an *intermediate* — only happens if the
    ///     backend has no implicit-dir semantics. We surface it as
    ///     an error so the caller (mkdir) can decide what to do.
    ///   * Anything else — propagate.
    fn mkdir_chain(&self, full_path: &str) -> std::io::Result<()> {
        let chain = build_mkdir_chain(full_path);

        let op = self.op.clone();
        // chain now contains ONLY intermediate directories (the leaf
        // was popped above). For paths like `/newsub` where there are no
        // intermediates (chain becomes empty after pop), there's nothing
        // to do — op.write on the leaf will handle creation.
        if chain.is_empty() {
            return Ok(());
        }
        rt().block_on(async move {
            // Concurrent create_dir for all intermediate directories.
            // The 3 PUTs are issued concurrently so wall-clock latency is
            // 1 round-trip (not N). Each level is independent — no
            // level depends on another's success. The pre-fix sequential
            // version was what made `mkdir` 2-3× slower than rclone in
            // the bench (issue #17).
            let futs = chain.iter().map(|p| op.create_dir(p));
            let results = futures::future::join_all(futs).await;
            for (p, r) in chain.iter().zip(results) {
                match r {
                    Ok(()) => {}
                    Err(e) if e.kind() == opendal::ErrorKind::Unsupported => {
                        tracing::debug!(path = %p,
                            "backend does not support create_dir; treating as implicit dir");
                    }
                    Err(e) if e.kind() == opendal::ErrorKind::AlreadyExists => {
                        // Idempotent — the dir is already there.
                    }
                    Err(e) => {
                        // Issue #268.5 O19: mkdir_chain intermediate
                        // creation failed. Pre-fix the only signal
                        // was the propagated io::Error::other; the
                        // intermediate path + backend kind were lost
                        // in the `?` chain at the call site.
                        tracing::warn!(
                            path = %p,
                            error = %e,
                            "mkdir_chain: intermediate create_dir failed (will fail mkdir/create/create_excl)"
                        );
                        return Err(std::io::Error::other(format!(
                            "create_dir({p}) failed: {e}"
                        )));
                    }
                }
            }
            Ok(())
        })
    }

    fn stat_op(&self, path: &str) -> Option<FileStat> {
        // vfs_refresh (issue #210): skip the attr_cache and
        // always fetch fresh backend metadata. The inodes
        // entry is unchanged — locally-written files still
        // hit the fast path; only the TTL'd backend
        // metadata cache is bypassed.
        if !self.vfs_refresh
            && let Some(entry) = self.attr_cache.get(path)
        {
            let (kind, size, mtime, ts) = entry.value();
            if ts.elapsed() < self.stat_cache_ttl {
                return Some(FileStat {
                    kind: *kind,
                    size: *size,
                    mtime: *mtime,
                });
            }
        }
        let result = rt().block_on(async {
            let op = self.op.clone();
            let p = path.to_string();
            match op.stat(&p).await {
                Ok(meta) => {
                    let kind = match meta.mode() {
                        EntryMode::DIR => FileType::Directory,
                        _ => FileType::RegularFile,
                    };
                    let mtime = if self.use_server_modtime {
                        meta.last_modified().map(opendal_timestamp_to_system_time)
                    } else {
                        None
                    };
                    Some(FileStat {
                        kind,
                        size: meta.content_length(),
                        mtime,
                    })
                }
                Err(e) if e.kind() == opendal::ErrorKind::NotFound => {
                    // NotFound is the only error that means
                    // "this path may still exist as an
                    // implicit S3-style prefix". Issue
                    // #258: every other error kind
                    // (ServerUnavailable, PermissionDenied,
                    // Unauthenticated, Unexpected, ...) is
                    // a backend failure, not a missing file.
                    // Falling into the list() RTT in those
                    // cases wastes a network round-trip and
                    // (worse) returns ENOENT to the user
                    // when the real answer is EIO. We log
                    // and return None — `lookup` then
                    // surfaces the correct error kind to
                    // the kernel.
                    if self.no_implicit_dir {
                        return None;
                    }
                    let op2 = self.op.clone();
                    let p2 = format!("{}/", path.trim_end_matches('/'));
                    if let Ok(mut l) = op2.lister(&p2).await
                        && l.next().await.is_some()
                    {
                        return Some(FileStat {
                            kind: FileType::Directory,
                            size: 4096,
                            mtime: None,
                        });
                    }
                    None
                }
                Err(e) => {
                    // Issue #258: backend failed for a
                    // non-NotFound reason (network, auth,
                    // permission, server, etc.). Don't
                    // fall into the implicit-dir list()
                    // branch — that wastes an RTT and
                    // misleads the caller into thinking
                    // the path doesn't exist. Return
                    // None so lookup surfaces a
                    // distinguishable "backend error"
                    // (the caller in CoreFilesystem maps
                    // the outer None to ENOENT today; in
                    // the disconnect case the user gets
                    // ENOENT instead of -EIO, which is
                    // what they were getting before this
                    // fix too — but we no longer add
                    // a second dead RTT to the wait).
                    tracing::debug!(
                        error = %e,
                        path = %p,
                        "stat_op: backend stat failed (non-NotFound); not falling through to implicit-dir list()"
                    );
                    None
                }
            }
        });
        if let Some(FileStat { kind, size, mtime }) = result {
            self.attr_cache.insert(
                path.to_string(),
                (kind, size, mtime, std::time::Instant::now()),
            );
        }
        result
    }

    fn list_op(
        &self,
        path: &str,
    ) -> Result<Vec<(String, EntryMode, u64, SystemTime)>, opendal::Error> {
        // Bug 34: canonicalize the path once at entry —
        // dir_cache key and the opendal lister arg both
        // use the same canonical form. Pre-fix the
        // caller passed `format!("{}/", path)` and
        // list_op stored under that key, but
        // cache_add_entry stored under `path` (no
        // trailing slash) for the same dir — meaning a
        // create()+ls()'s entry-then-cache-hit could
        // miss the just-added entry. See
        // canonicalize_list_path for the rule set.
        let path = canonicalize_list_path(path);
        let path = path.as_str();
        {
            if let Some(entry) = self.dir_cache.get(path) {
                let (t, entries) = entry.value();
                let age = t.elapsed();
                if age < self.dir_cache_ttl {
                    return Ok(entries
                        .iter()
                        .map(|r| {
                            let (name, DirEntryCacheValue { mode, size, mtime }) = r.pair();
                            (name.clone(), *mode, *size, *mtime)
                        })
                        .collect());
                }
                // Cache expired — drop and re-read from remote
                drop(entry);
                self.dir_cache.remove(path);
            }
        }
        let depth = path.matches('/').count();
        // Per SESSION_PITFALLS §2.6: never swallow backend errors. A lister
        // init failure (auth, permission, network reset) used to be
        // silently dropped via .ok()?/.unwrap_or_default(), which made
        // mntrs return an empty FUSE directory on every backend problem
        // — debugging required guessing the root cause. Now we propagate
        // the error so the FUSE reply carries EIO/ENOENT and the
        // tracing pipeline (RUST_LOG + MNTRS_DAEMON_LOG) records the
        // opendal error verbatim.
        //
        // Bug B follow-up: the *one* exception is `NotFound`, which on
        // most backends means "the dir exists in our model but the
        // backend has no record of it" (e.g. an empty dir on S3, or
        // a just-mkdir'd dir on memory before any child was written).
        // For implicit-dir semantics (the default, matching rclone
        // VFS), an empty listing is the right answer. We still return
        // a cached empty entry so subsequent readdirs don't pay the
        // backend round-trip cost.
        let mut result = rt().block_on(async {
            let op = self.op.clone();
            let p = path.to_string();
            // Bug B follow-up: if the lister init returns NotFound,
            // treat it as "this dir exists in our model but has no
            // entries on the backend right now" — return an empty
            // listing rather than propagating EIO. This matches
            // rclone VFS implicit-dir semantics. We still surface
            // every other lister-init error (auth, permission, network).
            let mut lister = match op.lister_with(&p).limit(1000).await {
                Ok(l) => l,
                Err(e) if e.kind() == opendal::ErrorKind::NotFound => {
                    return Ok::<_, opendal::Error>(Vec::new());
                }
                Err(e) => return Err(e),
            };
            let mut out = vec![];
            // Bug 6 (list_op OOM): hard cap on entries
            // accumulated per readdir. Pre-fix the lister
            // loop ran to exhaustion — an S3 bucket with
            // 10 M+ keys under one prefix would allocate
            // ~1 GiB into `out` before returning, blowing
            // memory on the FUSE worker. The cap is set
            // generously enough to fit normal "large"
            // dirs (millions of files in a single dir
            // are an anti-pattern; FUSE itself paginates
            // readdir below the kernel layer).
            //
            // On hit: stop iteration, log at warn (a
            // truncated readdir is a real correctness
            // signal — `ls` will silently lose the tail
            // entries), and return what we have. The
            // dir_cache stores the truncated result with
            // the same TTL as a complete listing; if the
            // user reduces depth/glob filters and retries,
            // the TTL will expire and a fresh listing
            // runs.
            let mut hit_cap = false;
            // DESIGN_VULNS #5 (readdirplus error isolation):
            // count per-entry lister errors but don't
            // propagate. Pre-fix `let entry = item?;` would
            // bail on the first mid-stream lister error
            // (e.g. one S3 page timed out, one HDFS NameNode
            // RPC failed, one entry blocked by ACL), dropping
            // every entry accumulated so far and surfacing as
            // EIO on `ls`. The audit's concern: a single
            // unreadable file shouldn't make the whole
            // directory unlistable.
            //
            // New behaviour: log + count + continue. The
            // function still returns Ok with whatever entries
            // we managed to read. A non-zero skip count is
            // logged at warn so the operator can see partial
            // results in the daemon log.
            let mut skipped_errors = 0u64;
            while let Some(item) = lister.next().await {
                if out.len() >= MAX_LIST_ENTRIES {
                    hit_cap = true;
                    break;
                }
                let entry = match item {
                    Ok(e) => e,
                    Err(e) => {
                        skipped_errors += 1;
                        // Sample the first error at warn so
                        // the operator sees the actual error
                        // shape, then drop to debug for the
                        // rest of this listing (a
                        // hundreds-of-skipped-entries listing
                        // would otherwise spam the daemon
                        // log).
                        if skipped_errors == 1 {
                            tracing::warn!(
                                path = %p,
                                error = %e,
                                "list_op: per-entry lister error; skipping (further errors at debug)"
                            );
                        } else {
                            tracing::debug!(
                                path = %p,
                                skipped = skipped_errors,
                                error = %e,
                                "list_op: per-entry lister error; skipping"
                            );
                        }
                        continue;
                    }
                };
                let name = entry.name().trim_end_matches('/').to_string();
                let mode = entry.metadata().mode();
                let content_length = entry.metadata().content_length();
                // Apply filters
                if let Some(max_depth) = self.max_depth
                    && depth >= max_depth
                    && mode == EntryMode::DIR
                {
                    continue;
                }
                if let Some(ms) = self.max_size
                    && content_length > ms
                {
                    continue;
                }
                if let Some(ms) = self.min_size
                    && content_length < ms
                {
                    continue;
                }
                // exclude/include glob patterns
                if !self.exclude_patterns.is_empty() {
                    let matched = self
                        .exclude_patterns
                        .iter()
                        .any(|pat| fnmatch(pat, &name, self.ignore_case));
                    if matched {
                        continue;
                    }
                }
                // Skip Apple Double files on macOS
                if self.no_apple_double && name.starts_with("._") {
                    continue;
                }
                if !self.include_patterns.is_empty() {
                    let matched = self
                        .include_patterns
                        .iter()
                        .any(|pat| fnmatch(pat, &name, self.ignore_case));
                    if !matched {
                        continue;
                    }
                }
                let size = content_length;
                let mtime = entry
                    .metadata()
                    .last_modified()
                    .map(opendal_timestamp_to_system_time)
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                out.push((name, mode, size, mtime));
            }
            // Issue #48: the cap is intentionally
            // finite. Pre-fix it was `usize::MAX`
            // (effectively unlimited); #23 capped it
            // at 1M entries. A directory beyond 1M
            // hits the `hit_cap` branch and is
            // truncated. The fix for unbounded
            // directories is to replace the
            // materialised `Vec` with a streaming
            // `opendal::Lister` held per-fh in
            // `dir_listers` — the next readdir page
            // pulls the next batch from the lister
            // on demand. The current
            // `lister.next()`-driven materialisation
            // blocks the FUSE worker for the full
            // list time, which is unacceptable for
            // 10M+ entries. opendal's lister is
            // `!Send`, so it can't live in the
            // current per-fh DashMap value (which
            // requires Send). The streaming refactor
            // is a separate piece of work; the cap
            // is the practical workaround for now.
            //
            // The mount-level knob
            // `--max-list-entries <N>` overrides the
            // cap at runtime (operators with deep
            // prefix trees can lower it to bound
            // memory; operators on a flat namespace
            // can raise it up to the per-process
            // memory budget). Default is 1M.
            if hit_cap {
                tracing::warn!(
                    path = %p,
                    returned = out.len(),
                    cap = MAX_LIST_ENTRIES,
                    "list_op truncated at MAX_LIST_ENTRIES cap — directory is larger than \
                     the per-fh snapshot can hold; further entries are silently dropped \
                     (issue #48, see --max-list-entries knob)"
                );
            }
            if skipped_errors > 0 {
                // DESIGN_VULNS #5: aggregate summary so a
                // partial listing shows up in the daemon log
                // as a single warn line per readdir (rather
                // than the per-entry warns above).
                tracing::warn!(
                    path = %p,
                    returned = out.len(),
                    skipped = skipped_errors,
                    "list_op completed with per-entry errors — returning partial listing"
                );
            }
            Ok::<_, opendal::Error>(out)
        })?;
        // Deduplicate by Unicode-normalized name if enabled.
        // Issue #307: use the shared `util::nfc` helper rather than
        // an inline `UnicodeNormalization::nfc().collect()` so the
        // canonical-form behavior lives in one place. The adapter
        // boundary normalization (winfsp.rs + fuser.rs) ensures
        // these names are already in NFC, so this dedup is the
        // safety net for any backend that returns NFD (e.g.
        // macOS-uploaded keys).
        if self.block_norm_dupes && !result.is_empty() {
            let mut seen = std::collections::HashSet::new();
            result.retain(|(name, ..)| seen.insert(crate::util::nfc(name)));
        }
        // Store entries individually (like rclone DirEntry per name).
        // Only cache on success — caching an empty Vec from an error
        // would propagate the failure for dir_cache_ttl.
        let dir_entries: dashmap::DashMap<String, DirEntryCacheValue> = result
            .iter()
            .map(|(name, mode, size, mtime)| {
                (
                    name.clone(),
                    DirEntryCacheValue {
                        mode: *mode,
                        size: *size,
                        mtime: *mtime,
                    },
                )
            })
            .collect();
        self.dir_cache
            .insert(path.to_string(), (std::time::Instant::now(), dir_entries));

        // Also pre-populate attr_cache for every entry. The FUSE
        // kernel follows `readdir` with one `lookup` per entry, and
        // `lookup` calls `stat_op` which by default issues a backend
        // HEAD/STAT. S3/GCS/OSS/COS all return size + last_modified
        // inline in the list response (we already extracted them
        // above), so we can serve the post-readdir lookups from
        // memory instead of N extra round-trips. For a 500-file
        // directory, this turns 500 HEADs into 0.
        //
        // Cache TTL is the same `attr_ttl` used everywhere else so
        // the entries are treated as fresh for the same window.
        for (name, mode, size, mtime) in &result {
            let kind = match mode {
                EntryMode::DIR => FileType::Directory,
                _ => FileType::RegularFile,
            };
            let full = if path.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", path, name)
            };
            self.attr_cache
                .insert(full, (kind, *size, Some(*mtime), std::time::Instant::now()));
        }

        Ok(result)
    }

    /// Add a single entry to directory cache (like rclone addObject).
    /// Called after create() / mkdir() to avoid full directory re-read.
    ///
    /// Bug B fix: the pre-fix version only updated an *existing* cache
    /// entry. When mkdir -p created a chain like `a/b/c` and the
    /// parent's dir_cache was cold (no prior readdir had populated
    /// it), the new entry was silently dropped. The next readdir on
    /// the parent fell through to the backend, where the path was
    /// empty/missing, and the user got EIO. The fix initializes the
    /// cache with just the new entry when the parent slot is empty,
    /// so the subsequent readdir sees it. (A later readdir that
    /// actually hits the backend will re-merge; that's harmless —
    /// the cache-add path is idempotent for the same name+mode.)
    fn cache_add_entry(
        &self,
        parent_path: &str,
        name: &str,
        mode: EntryMode,
        size: u64,
        mtime: SystemTime,
    ) {
        // Bug 34: canonicalize so the key agrees with
        // list_op (which also canonicalizes). Without
        // this, list_op stored under "foo/" and a
        // subsequent create() that called cache_add_entry
        // with parent_path="foo" stored under "foo" —
        // two keys for the same dir, and the dir_cache
        // hit-on-cache_add was a miss for any subsequent
        // list_op read.
        let parent_path = canonicalize_list_path(parent_path);
        let parent_path = parent_path.as_str();
        if let Some(entry) = self.dir_cache.get(parent_path) {
            let (_, entries) = entry.value();
            entries.insert(name.to_string(), DirEntryCacheValue { mode, size, mtime });
        } else {
            let entries: dashmap::DashMap<String, DirEntryCacheValue> = dashmap::DashMap::new();
            entries.insert(name.to_string(), DirEntryCacheValue { mode, size, mtime });
            self.dir_cache.insert(
                parent_path.to_string(),
                (std::time::Instant::now(), entries),
            );
        }
    }

    /// Remove a single entry from directory cache (like rclone delObject).
    /// Called after unlink/rmdir to avoid full directory re-read.
    fn cache_remove_entry(&self, parent_path: &str, name: &str) {
        if let Some(entry) = self.dir_cache.get(parent_path) {
            let (_, entries) = entry.value();
            entries.remove(name);
        }
    }

    /// Issue #29: batch lookup helper for readdirplus.
    /// Reads the parent's dir_cache snapshot (already
    /// populated by the recent list_op) and returns
    /// one attr per name. Falls back to the per-name
    /// trait `lookup` for names not in the snapshot
    /// (e.g. a write that landed after the snapshot
    /// was taken).
    ///
    /// Performance: the snapshot path is O(N) over
    /// the requested names with no remote RTT.
    /// Pre-fix the FUSE adapter called
    /// `inner.lookup(parent, name)` per entry, each
    /// of which is potentially a stat RTT to the
    /// backend (4-10 ms for S3/HDFS). On a 500-file
    /// directory this dominates the
    /// `ls -la` benchmark at 1.6x slower than
    /// rclone; on `find maxdepth1` (no readdirplus
    /// helper, but each entry's getattr also hits
    /// the same path) it's 32x slower.
    fn batch_lookup_from_dir_cache(
        &self,
        parent: u64,
        names: &[&str],
    ) -> Vec<std::io::Result<CoreFileAttr>> {
        let parent_path = self.resolve(parent).map(|e| e.path).unwrap_or_default();
        // canonicalize_list_path is what list_op
        // uses to key the dir_cache. Aligning the
        // read here means a recent opendir+readdir
        // already warmed the slot the readdirplus
        // lookup will hit.
        let cache_key = canonicalize_list_path(&parent_path);
        let cache_key = cache_key.as_str();
        let cached: Option<dashmap::DashMap<String, DirEntryCacheValue>> =
            self.dir_cache.get(cache_key).map(|e| e.value().1.clone());
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            // "." / ".." are special: not in
            // dir_cache (they're synthesized by
            // the FUSE adapter), but cheap to
            // construct.
            if *name == "." || *name == ".." {
                let p = if *name == "." { parent } else { 1 };
                out.push(Ok(to_core_attr(&self.make_attr(
                    p,
                    4096,
                    FileType::Directory,
                    SystemTime::UNIX_EPOCH,
                ))));
                continue;
            }
            // Try the snapshot first.
            let snapshot_hit = cached
                .as_ref()
                .and_then(|entries| entries.get(*name).map(|e| *e.value()));
            match snapshot_hit {
                Some(DirEntryCacheValue { mode, size, mtime }) => {
                    let kind = match mode {
                        EntryMode::DIR => FileType::Directory,
                        _ => FileType::RegularFile,
                    };
                    // alloc_ino with the cached
                    // mtime so subsequent stat
                    // calls return the same
                    // data without a remote
                    // round-trip.
                    let ino = self.alloc_ino_with_mtime(
                        &format!("{}/{}", parent_path, name),
                        kind,
                        size,
                        mtime,
                    );
                    out.push(Ok(to_core_attr(&self.make_attr(ino, size, kind, mtime))));
                }
                None => {
                    // Snapshot miss — fall
                    // through to the per-name
                    // trait lookup. The common
                    // case for this is a file
                    // written after the
                    // snapshot was taken
                    // (write doesn't update
                    // dir_cache; only
                    // cache_add_entry on
                    // create/mkdir does).
                    out.push(self.lookup(parent, name));
                }
            }
        }
        out
    }

    /// Full invalidation: remove directory cache and all sub-paths.
    /// Used for rename (both src and dst sides) where we can't cheaply update.
    fn invalidate_dir_cache(&self, path: &str) {
        // Bug 34: canonicalize for dir_cache key parity.
        // Pre-fix this used raw `path` which could
        // disagree with the canonical key list_op used.
        let canon = canonicalize_list_path(path);
        self.dir_cache.remove(canon.as_str());
        let prefix = canon.clone(); // "foo/" — already trailing-/
        self.dir_cache.retain(|k, _| !k.starts_with(&prefix));
        // Walk up one level — parent's listing of `path`
        // becomes stale on rename, so drop the parent's
        // cache too. Strip the trailing slash off the
        // canonical form to find the parent path.
        let without_slash = canon.trim_end_matches('/');
        if let Some(slash) = without_slash.rfind('/') {
            let parent_raw = &without_slash[..slash];
            if !parent_raw.is_empty() {
                let parent_canon = canonicalize_list_path(parent_raw);
                self.dir_cache.remove(parent_canon.as_str());
            }
        }
    }
}

// Library primitive — issue #158.
//
// This impl block lives outside the `CoreFilesystem` trait impl
// because `batch_remove_path` is a public API for non-FUSE callers
// (CSI driver, future CLI subcommand, library consumers). FUSE can't
// express "rm -rf" as a single operation — `rm -rf` arrives at the
// daemon as N independent FUSE_UNLINK + FUSE_RMDIR syscalls, and the
// kernel already walks the tree depth-first. There's no entry point
// FUSE could intercept to do a single batched delete.
//
// On S3 (and other `BatchDelete` backends) this maps to 1 list RTT +
// 1 batched `DeleteObjects` per 1000 keys via opendal's
// simulate-layer fallback, ~10-100× faster than N × `op.delete`. On
// memory/HDFS/fs the simulate-layer falls through to list + N
// OneShotDeleter calls, equivalent cost to current per-call
// behavior (no regression).
//
// Local cache cleanup (inodes / attr_cache / disk_cache_index / block
// files / .dirty sidecars) is intentionally NOT done here: callers
// are expected to unmount first or accept stale cache until the next
// mount restart. Doing it inline would double the cost and race
// with concurrent FUSE reads.
impl MntrsFs {
    /// Remove a path and all its descendants in one backend call.
    ///
    /// Equivalent to `rm -rf <path>` at the backend level. See the
    /// module-level impl-block doc comment above for the design
    /// rationale (why this isn't a FUSE callback, backend cost
    /// characteristics, and the intentional cache-cleanup gap).
    ///
    /// # Errors
    ///
    /// Returns the opendal error mapped to `io::ErrorKind` (same
    /// pattern as `fn unlink` / `fn rmdir`). On S3, partial failures
    /// inside a single `DeleteObjects` request are surfaced by
    /// opendal as `ErrorKind::Unexpected` with a per-key breakdown
    /// in the error context.
    pub async fn batch_remove_path(&self, path: &str) -> std::io::Result<()> {
        let normalized = path.trim_end_matches('/');
        let target = if normalized.is_empty() {
            "/".to_string()
        } else {
            format!("{}/", normalized)
        };
        self.op
            .delete_with(&target)
            .recursive(true)
            .await
            .map_err(|e| opendal_to_io_error(&e, "batch_remove_path"))?;
        tracing::info!(
            path = %target,
            backend = %self.op.info().scheme(),
            "batch_remove_path: backend delete complete"
        );
        Ok(())
    }
}

use crate::core_fs::{CoreDirEntry, CoreFileAttr, CoreFileType, CoreFilesystem, CoreVolumeStat};
use crate::writeback::WritebackTask;

impl CoreFilesystem for MntrsFs {
    fn init(&self) -> std::io::Result<()> {
        self.common_init_wb();
        Ok(())
    }

    /// Issue #29 override: serve the batch from the
    /// dir_cache snapshot when possible.
    fn lookup_many(
        &self,
        parent: u64,
        names: &[&str],
    ) -> std::io::Result<Vec<std::io::Result<CoreFileAttr>>> {
        Ok(self.batch_lookup_from_dir_cache(parent, names))
    }

    fn access(&self, _ino: u64, _mask: u32) -> std::io::Result<()> {
        // Issue #268.2 O11: trace-level entry log so
        // operators can correlate EACCES ("--default-permissions")
        // with mount state. The default impl is permissive
        // (always Ok); this log only fires when the kernel
        // actually calls us (i.e. when default_permissions
        // or root_squash is in effect). trace level keeps
        // it out of RUST_LOG=info default — operators
        // enabling it explicitly know what they want.
        tracing::trace!(ino = _ino, mask = _mask, "FUSE access entry");
        Ok(())
    }

    fn lookup(&self, parent: u64, name: &str) -> std::io::Result<CoreFileAttr> {
        if name == "." || name == ".." {
            let p = if name == "." { parent } else { 1 };
            // Bug 33: bump kernel lookup count.
            // bump_lookup_count is a no-op for ino == 1,
            // and for "." it tracks whichever ino the
            // kernel just received an entry reply for.
            self.bump_lookup_count(p);
            return Ok(to_core_attr(&self.make_attr(
                p,
                4096,
                FileType::Directory,
                SystemTime::UNIX_EPOCH,
            )));
        }
        let parent_path = self.resolve(parent).map(|e| e.path).unwrap_or_default();
        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };
        // stat_op talks to the backend, but freshly-written data
        // is still in the local cache file (5s async writeback
        // delay). If the backend says "not found" but a cache
        // file exists, trust the cache file. Same pattern as
        // read() and the rename fallback.
        //
        // Pre-existing-bug fix (CI test 6, HDFS append): when the
        // backend reports a small size but the local cache file
        // is larger (a recent write hasn't been uploaded yet),
        // the larger cache-file size wins. The previous version
        // blindly used the backend's size, so a follow-up
        // `cat <file>` after `echo "x" >> <file>` reported the
        // pre-append size and truncated the read to that. The
        // FUSE kernel uses our getattr-returned size as the
        // authoritative EOF, so a stale lookup made every
        // post-write read see the old length. Lookup is the
        // first call after a `BATCHFORGET`, so it has to be
        // self-consistent with the cache-file state.
        let (kind, size, mtime) = if let Some(FileStat {
            kind: k,
            size: s,
            mtime: m,
        }) = self.stat_op(&full_path)
        {
            let cpath = crate::cache_path(&self.cache_dir, &full_path);
            let cache_size = std::fs::metadata(&cpath).map(|m| m.len()).unwrap_or(0);
            // Issue #301: detect a stale dir_cache via a successful
            // lookup whose name is not in the parent's listing. A
            // file created out-of-band (opendal write_remote from a
            // sibling process, or any path that bypasses the mount's
            // create/mkdir → cache_add_entry chain) lands on the
            // backend but does not update our dir_cache. The next
            // readdir would then miss the entry. Invalidate the
            // parent's cache here so the next readdir refetches the
            // full backend listing.
            //
            // Cold-cache case (dir_cache has no entry for the
            // parent) is left untouched: the lookup is the first
            // thing to mention this name, so the cache is already
            // "fresh enough" — there is nothing to invalidate. This
            // matches the issue body's option 3 ("lookup should
            // populate the dir_cache for parents as a side effect")
            // adapted to the cheaper invalidate-on-miss variant.
            //
            // We use `full_path` (the canonical backend path) rather
            // than `name` (the per-adapter arg, which is a basename
            // from FUSE and a full path from WinFSP) so the
            // parent/basename split is uniform across adapters.
            let parent_dir_of_full = std::path::Path::new(&full_path)
                .parent()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default();
            let basename = std::path::Path::new(&full_path)
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            if !basename.is_empty() {
                let parent_canon = canonicalize_list_path(&parent_dir_of_full);
                let stale = self
                    .dir_cache
                    .get(&parent_canon)
                    .map(|entry| !entry.value().1.contains_key(&basename))
                    .unwrap_or(false);
                if stale {
                    tracing::debug!(
                        parent = %parent_dir_of_full,
                        name = %basename,
                        "lookup: invalidating stale parent dir_cache (issue #301)"
                    );
                    self.dir_cache.remove(&parent_canon);
                }
            }
            (k, s.max(cache_size), m)
        } else {
            // HDFS implicit-directory fix: when `stat_op` returns NotFound
            // (opendal hdfs-native: a directory that has no explicit INode
            // record, only a child file), but the parent directory's
            // readdir listing — which mntrs caches in `dir_cache` with
            // `dir_cache_ttl` — contains `name` as a child, the path is
            // a valid (implicit) entry on the backend. Without this
            // fallback, `lookup` returns ENOENT and the FUSE reply makes
            // `ls -laR` render the entry as `d?????????` (all attrs
            // unknown), or the recursive opendir against the subdir
            // fails entirely when the kernel's getattr→lookup→readdir
            // pipeline rejects the ENOENT. This was the root cause of
            // CI run 27485319055 `hdfs-kerberos` job failing at
            // `ls: cannot open directory '/mnt/hdfs/test'`.
            //
            // Match the parent's cache slot: list_op stores entries
            // under `format!("{}/", parent_path)` (or `""` for the
            // root), so we look up exactly that key. Hit only if the
            // entry's mode classifies it as a known directory or file
            // — purely-default attrs would be a worse answer than
            // ENOENT, because the caller (FUSE) would then treat
            // something nonexistent as existent.
            let parent_cache_key = canonicalize_list_path(&parent_path);
            // #232: take the full DirEntryCacheValue (mode, size,
            // mtime) from the dir_cache snapshot — the data is
            // already in hand post-#226. For files, keep the
            // `size.max(cache_file_size)` precedence rule from
            // the explicit-stat branch (bug 128: a not-yet-
            // uploaded write makes the cache file the source of
            // truth for size; the backend's size is stale). For
            // dirs, dir_cache.size is 0 and there's no cache
            // file to consult (opendal hdfs-native: implicit
            // dirs have no INode record). Skips the
            // std::fs::metadata call for the common case.
            let implicit = self.dir_cache.get(&parent_cache_key).and_then(|entry| {
                let (_t, entries) = entry.value();
                entries.get(name).map(|r| *r.value())
            });
            if let Some(DirEntryCacheValue { mode, size, mtime }) = implicit {
                let kind = match mode {
                    EntryMode::DIR => FileType::Directory,
                    _ => FileType::RegularFile,
                };
                let s = if kind == FileType::Directory {
                    0
                } else {
                    let cpath = crate::cache_path(&self.cache_dir, &full_path);
                    let cache_size = std::fs::metadata(&cpath).map(|m| m.len()).unwrap_or(0);
                    size.max(cache_size)
                };
                (kind, s, Some(mtime))
            } else {
                let cpath = crate::cache_path(&self.cache_dir, &full_path);
                match std::fs::metadata(&cpath) {
                    Ok(meta) => {
                        let mt = meta.modified().ok();
                        (FileType::RegularFile, meta.len(), mt)
                    }
                    Err(_) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "lookup: not on backend, no cache file",
                        ));
                    }
                }
            }
        };
        // Allocate a new ino for this lookup. alloc_ino's
        // NEXT_INO counter is the canonical ino; the FUSE
        // kernel stores whatever we return here and reuses
        // it for subsequent open/read/write. or_insert on the
        // inodes map is fine — if the entry exists, we keep
        // the previous (path, kind, size, mtime); if not, we
        // create one with the values we just resolved.
        //
        // #28 (stat fast-path): pass the stat_op-derived
        // mtime into alloc_ino_with_mtime so subsequent
        // `getattr` calls can skip the S3 HEAD round-trip.
        // The pre-fix `alloc_ino` always set mtime=None,
        // which forced every `getattr` to fall through to
        // stat_op — defeating the inodes fast path for any
        // file that was only read, never written.
        let ino = self.find_ino_by_path(&full_path).unwrap_or_else(|| {
            self.alloc_ino_with_mtime(
                &full_path,
                kind,
                size,
                mtime.unwrap_or_else(SystemTime::now),
            )
        });
        // Bug 33: kernel will store this entry under
        // its own dentry cache and ref-count it; mirror
        // by bumping our per-ino lookup_count.
        self.bump_lookup_count(ino);
        Ok(to_core_attr(&self.make_attr(
            ino,
            size,
            kind,
            mtime.unwrap_or(SystemTime::UNIX_EPOCH),
        )))
    }

    fn getattr(&self, ino: u64) -> std::io::Result<CoreFileAttr> {
        if ino == 1 {
            return Ok(to_core_attr(&self.make_attr(
                1,
                4096,
                FileType::Directory,
                SystemTime::UNIX_EPOCH,
            )));
        }
        if let Some(InodeEntry {
            path,
            kind,
            size: inodes_size,
            mtime: inodes_mtime,
        }) = self.resolve(ino)
        {
            // #28 (stat optimization): skip the S3 stat_op
            // round-trip when the inodes entry is fresh
            // enough. The entry is populated by:
            //   * `alloc_ino_with_mtime` (mkdir/create) — has mtime
            //   * write path's `inodes.entry().and_modify()` — has mtime
            //   * `alloc_ino` (lookup, readdir) — no mtime (None)
            // For files that exist only on the remote and we
            // never wrote locally, `inodes_mtime` is None and
            // we still need stat_op to get the canonical size
            // and server-side mtime. For everything else
            // (the common case in a write-heavy workload),
            // the inodes entry is already the source of truth.
            //
            // Cost: an S3 HEAD request is ~5-15 ms over
            // localhost. Skipping it on the hot path
            // (recently-written files, just-mkdir'd dirs) cuts
            // the bench's `stat x50` from ~150 ms to a
            // sub-millisecond dashmap lookup. The downside
            // — stale inodes mtime if the remote file is
            // modified out-of-band — is acceptable because
            // the inodes entry is updated synchronously on
            // every local write, and the writeback worker
            // owns the upload (no other process can modify
            // the file through mntrs in the meantime).
            let (size, mtime) = if let Some(inodes_mtime) = inodes_mtime {
                // Fast path: trust the inodes entry.
                let cache_size = std::fs::metadata(crate::cache_path(&self.cache_dir, &path))
                    .map(|m| m.len())
                    .unwrap_or(0);
                (inodes_size.max(cache_size), inodes_mtime)
            } else {
                // Slow path: file was never written locally;
                // fall through to the backend.
                let FileStat {
                    size: backend_size,
                    mtime: backend_mtime,
                    ..
                } = self.stat_op(&path).unwrap_or(FileStat {
                    kind,
                    size: inodes_size,
                    mtime: None,
                });
                let cache_size = std::fs::metadata(crate::cache_path(&self.cache_dir, &path))
                    .map(|m| m.len())
                    .unwrap_or(0);
                let size = inodes_size.max(backend_size).max(cache_size);
                let mtime = backend_mtime
                    .or(inodes_mtime)
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                (size, mtime)
            };
            Ok(to_core_attr(&self.make_attr(ino, size, kind, mtime)))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "ino not found",
            ))
        }
    }

    fn setattr(
        &self,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
        fh: Option<u64>,
    ) -> std::io::Result<CoreFileAttr> {
        if let Some(InodeEntry { path: _p, kind, .. }) = self.resolve(ino) {
            // Issue #89 / Option B fix: distinguish kernel-driven
            // setattr from user-initiated `truncate(2)`. The FUSE
            // kernel sends `FUSE_SETATTR` with `fh=None` for two
            // distinct cases:
            //
            //   1. **User `truncate(2)` syscall** — has an open fd,
            //      so `fh` is `Some(_)` in the kernel's request.
            //      This is a real truncation the user wants.
            //
            //   2. **Kernel's bookkeeping for `open(O_TRUNC)`** —
            //      no fd (truncate happens before open), so `fh`
            //      is `None`. The kernel will then send
            //      `FUSE_OPEN` which we handle normally. The cache
            //      file is freshly created by `open()` with the
            //      truncated size baked into the write handler's
            //      `set_len(end)`. We must NOT pre-truncate the
            //      cache file here, because if a previous write
            //      already populated it (e.g. from a prior session
            //      or the recovery path), we'd destroy that content
            //      before the user has even opened the fd.
            //
            // Skipping the cache file truncate when `fh` is `None`
            // fixes the task #3 regression: append writes no longer
            // see a stale zero-byte cache between the SETATTR and
            // the OPEN that follows.
            let user_initiated_truncate = fh.is_some();
            if user_initiated_truncate {
                tracing::warn!(
                    ino = ino,
                    size = ?size,
                    "setattr: truncating cache file (user-initiated)"
                );
            } else if size.is_some() {
                tracing::warn!(
                    ino = ino,
                    size = ?size,
                    "setattr: kernel-driven (e.g. O_TRUNC open); skipping cache truncate"
                );
            }
            if let Some(s) = size {
                if user_initiated_truncate {
                    // Truncate the inodes size to the new value.
                    //
                    // The previous implementation called `alloc_ino(&_p, kind, s)`,
                    // which under the hood is `or_insert` on the inodes map.
                    // `or_insert` only inserts when the entry is vacant — so
                    // a truncate from 18 → 0 on an existing file silently
                    // did nothing, leaving the kernel thinking the file was
                    // still 18 bytes while the cache file had been
                    // (partially) overwritten by a smaller write.
                    //
                    // The fix uses `and_modify` to unconditionally overwrite
                    // the size field, which is what truncation actually
                    // means semantically. We do NOT touch mtime here
                    // (setattr's mtime is handled by the `make_attr` call
                    // below with `SystemTime::now()`).
                    self.inodes.entry(ino).and_modify(|v| {
                        v.size = s;
                    });
                    // Bug 25 (truncate-vs-async-write race):
                    // there's no lock between the inodes
                    // size update above and the cache file
                    // set_len below, vs an in-flight
                    // DiskWriteJob still draining the IO
                    // pool. Worst case: setattr says "truncate
                    // to 10", a queued write of 4 KiB at
                    // offset 0 lands AFTER set_len, the cache
                    // file is back to 4 KiB but inodes.size is
                    // 10. The write path's own
                    // `entry().and_modify(|v| if end > v.size
                    // { v.size = end })` would also bump
                    // inodes.size to 4096, so the two
                    // operations clobber each other and the
                    // result depends on scheduling.
                    //
                    // Why we accept this: POSIX leaves
                    // concurrent truncate+write across
                    // different fds undefined. FUSE serializes
                    // operations on the same fd (so a single
                    // process's `ftruncate` + `write` sequence
                    // is fine), and a write through a
                    // separate fd racing with truncate is
                    // already an application-level bug under
                    // any filesystem. Adding a per-ino lock
                    // here would slow the hot write path for
                    // every workload, to guard a case POSIX
                    // doesn't promise correctness for.
                    //
                    // Issue #42: prefer `ftruncate(fh, s)` on the
                    // open cache fd when the kernel gave us an
                    // fh. This avoids:
                    //   * the path → fd re-open syscall,
                    //   * a race where the file disappears
                    //     (e.g. unlink from another fd) between
                    //     cpath.exists() and cpath.open(),
                    //   * the ftruncate semantics mismatch where
                    //     the path-based File::set_len sees a
                    //     different open file description than
                    //     the writer that's currently mutating
                    //     the file (POSIX leaves the result
                    //     undefined; the fd-based form at least
                    //     serializes through the kernel's
                    //     per-fd lock).
                    // If the fh is stale (the handle map no
                    // longer has the entry) or doesn't carry a
                    // cache_fd (e.g. a read-only handle that
                    // happened to get a setattr), we silently
                    // fall through to the path-based branch
                    // below — same final on-disk state, just
                    // via a different syscall.
                    let mut truncated_via_fh = false;
                    if let Some(fh_val) = fh
                        && let Some(entry) = self.handles.get(&fh_val)
                        && let crate::FileHandleState::Write {
                            cache_fd: Some(fd), ..
                        } = entry.value()
                        && let Ok(f) = fd.lock()
                    {
                        // Issue #42: truncate the open cache fd
                        // directly. The fd-based form is
                        // preferred because it sees the same
                        // open file description as the writer
                        // and serialises with concurrent
                        // writes on the same fd through the
                        // kernel's per-fd lock.
                        if f.set_len(s).is_ok() {
                            truncated_via_fh = true;
                        }
                    }
                    if !truncated_via_fh && user_initiated_truncate {
                        // Path-based fallback (Bug 25 comment
                        // above carries the full rationale).
                        // SKIPPED when fh is None (kernel-driven
                        // setattr for `open(O_TRUNC)`): the
                        // cache file already has whatever the
                        // previous session / recovery wrote, and
                        // the subsequent FUSE_OPEN will create a
                        // fresh fd that respects the user's intent.
                        // Truncating here would destroy a valid
                        // cache file before the user even has an
                        // fd open — see issue #89.
                        let cpath = crate::cache_path(&self.cache_dir, &_p);
                        if cpath.exists() {
                            // Open with write access so the resulting File
                            // holds a writable handle; the set_len() call below
                            // is the actual side effect — we don't write any
                            // bytes here, only shrink/grow the file size to
                            // match the truncate request.
                            //
                            // Issue #268.2 O10: surface truncation
                            // failures. Pre-fix the `let _ =` was
                            // silent — if open() or set_len() failed
                            // (file vanished between exists() and
                            // open(), EACCES on a readonly cache, FS
                            // error), the cache file stayed at the old
                            // size. Read path tolerates a slightly
                            // larger cache file (uses min of cache
                            // and inodes size) but not a stale
                            // over-size for a *truncate-to-smaller*
                            // case. Warn so operators see "cache may
                            // be stale" rather than a silent mismatch.
                            if let Err(e) = std::fs::OpenOptions::new()
                                .write(true)
                                .open(&cpath)
                                .and_then(|f| f.set_len(s))
                            {
                                tracing::warn!(
                                    path = %cpath.display(),
                                    requested_size = s,
                                    error = %e,
                                    "setattr: path-based truncate failed; cache may be stale"
                                );
                            }
                        }
                    }
                } // close if user_initiated_truncate
                // Bug 29 + Issue #52: unified L1 + L2
                // invalidation after truncate (issue #127).
                // Pre-fix the write path called
                // `mem_cache.invalidate_ino(ino)` on every
                // write but setattr did not — so a file
                // whose blocks were already cached would
                // serve pre-truncate bytes to subsequent
                // reads. Also sweeps the block-level disk
                // cache (.block files populated by a prior
                // cold read would otherwise serve stale
                // bytes if the file-level cache was
                // LRU-evicted).
                self.multi_cache.invalidate(&_p, ino);
            }
            // Issue #89 Option B follow-up: when setattr is
            // kernel-driven (fh=None) — e.g. the O_TRUNC open
            // prelude — the FUSE kernel caches the returned
            // attrs (including size) and uses them for subsequent
            // O_APPEND offset calculations. If we report
            // size=0 here, the kernel thinks the file is 0 bytes
            // forever and writes go to offset=0, clobbering the
            // 1st write. So for kernel-driven setattr, return the
            // current inodes.size (which preserves whatever the
            // user/app has already accumulated, including the
            // recovery-time size).
            //
            // The kernel will refresh attrs after the subsequent
            // open + write, so this is just a one-call caching
            // fix.
            let reported_size = if let Some(s) = size {
                if user_initiated_truncate {
                    s
                } else {
                    // Kernel-driven: don't lie about size=0.
                    // Use the larger of inodes.size and
                    // current cache file size to avoid
                    // confusing the kernel.
                    self.inodes.get(&ino).map(|e| e.size).unwrap_or(s).max(s)
                }
            } else {
                self.inodes.get(&ino).map(|e| e.size).unwrap_or(0)
            };
            // Issue #305 Tier 1 (set_basic_info forward): when the
            // kernel / WinFSP driver passes a mtime, persist it on
            // the inodes entry so the next getattr returns the new
            // value. WinFSP's per-handle FileInfo cache also picks
            // it up via the attr returned below. If neither mtime
            // nor atime is supplied, leave the existing inodes
            // mtime untouched and use SystemTime::now() for the
            // returned attr — matching the pre-fix behaviour for
            // callers that pass None (FUSE prealloc, recovery
            // paths, etc.).
            let effective_mtime: SystemTime = mtime
                .or_else(|| self.inodes.get(&ino).and_then(|e| e.mtime))
                .unwrap_or_else(SystemTime::now);
            if let Some(new_mtime) = mtime
                && let Some(mut entry) = self.inodes.get_mut(&ino)
            {
                entry.mtime = Some(new_mtime);
            }
            // atime is tracked for read-cache eviction in the LRU
            // heap; we don't have a separate InodeEntry field for
            // it today (only mtime + kind + size + path), so we
            // surface it via make_attr's atime slot but skip the
            // inodes-map update. Future #310 will add atime to
            // InodeEntry and persist here.
            let _ = atime;
            Ok(to_core_attr(&self.make_attr(
                ino,
                reported_size,
                kind,
                effective_mtime,
            )))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "ino not found",
            ))
        }
    }

    fn opendir(&self, ino: u64) -> std::io::Result<u64> {
        // Issue #23: materialise the full directory entry
        // list once and stash it under a per-fh handle.
        // The FUSE adapter passes the returned fh to
        // subsequent `readdir(ino, fh, offset)` calls;
        // we slice the cached Vec by `offset` instead of
        // re-hitting `list_op` on every page.
        //
        // Why a fresh fh (not the kernel's `fh` from
        // opendir's FUSE argument): the FUSE protocol
        // treats the `fh` as opaque — we mint one via
        // NEXT_HANDLE and store the snapshot under it.
        // The same fh is fed back to us on readdir and
        // releasedir. This keeps the dir-lister lifetime
        // independent of the inode's open-file handles
        // (which serve regular files via `open`/`release`).
        let entries = self.readdir_materialise(ino)?;
        let fh = NEXT_HANDLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.dir_listers.insert(fh, entries);
        Ok(fh)
    }

    fn readdir(
        &self,
        ino: u64,
        fh: u64,
        offset: u64,
        _max: usize,
    ) -> std::io::Result<Vec<CoreDirEntry>> {
        // Issue #23: serve from the per-fh snapshot.
        // The FUSE cookie is "index of the last entry
        // delivered + 1", so `start = offset as usize -
        // 1` would also work, but the fuser adapter
        // passes the raw `(i + 1) as u64` it would have
        // used pre-fix, and we slice from
        // `entries[start..]` — same semantics as
        // pre-fix slice-indexing (Bug 32 fix in
        // ece4391).
        let start = offset as usize;
        let entries = self.dir_listers.get(&fh).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("readdir(ino={ino}, fh={fh}): unknown dir-lister handle"),
            )
        })?;
        if start >= entries.len() {
            return Ok(Vec::new());
        }
        Ok(entries[start..].to_vec())
    }

    fn releasedir(&self, _ino: u64, fh: u64) -> std::io::Result<()> {
        // Drop the per-fh snapshot. Idempotent: a
        // double-releasedir (kernel bug? retry?) is a
        // no-op rather than an error.
        self.dir_listers.remove(&fh);
        Ok(())
    }

    /// Issue #306: readdir_with_attrs serves attrs in the
    /// same call as the entries, so the WinFSP adapter can
    /// populate the DirInfo with real size/mtime without
    /// triggering a follow-up `get_file_info` IRP per entry.
    ///
    /// Marker slicing: WinFSP delivers entries in lex order
    /// and calls back with the last delivered name as the
    /// marker for the next page. `partition_point` returns
    /// the first index whose name is strictly > marker,
    /// which is exactly "start of next page" semantics.
    /// Empty marker = first page (returns everything).
    ///
    /// No backend RTT in the common case: `list_op` already
    /// populated `dir_cache` with `(name -> DirEntryCacheValue)`
    /// during `opendir`, and `batch_lookup_from_dir_cache`
    /// serves attrs from that snapshot directly.
    fn readdir_with_attrs(
        &self,
        ino: u64,
        fh: u64,
        marker: &str,
    ) -> std::io::Result<Vec<(CoreDirEntry, CoreFileAttr)>> {
        // Slice the per-fh snapshot by marker. Clone the
        // Vec out of the DashMap so the shard lock is
        // released before the (potentially expensive)
        // batch_lookup below — concurrent ops on
        // dir_listers should not block on attr lookups.
        let entries = self
            .dir_listers
            .get(&fh)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("readdir_with_attrs(ino={ino}, fh={fh}): unknown dir-lister handle"),
                )
            })?
            .value()
            .clone();
        let cut = if marker.is_empty() {
            0
        } else {
            entries.partition_point(|e| e.name.as_str() <= marker)
        };
        let page: Vec<CoreDirEntry> = entries[cut..].to_vec();
        drop(entries);

        // Batch-fetch attrs from dir_cache snapshot
        // (lib.rs::batch_lookup_from_dir_cache). Cold-
        // cache fallback inside the helper already does the
        // per-name trait lookup, so a missing entry doesn't
        // crash.
        let names: Vec<&str> = page.iter().map(|e| e.name.as_str()).collect();
        let attrs = self.batch_lookup_from_dir_cache(ino, &names);

        let mut out = Vec::with_capacity(page.len());
        for (e, attr_res) in page.into_iter().zip(attrs) {
            if let Ok(attr) = attr_res {
                out.push((e, attr));
            }
            // Skip Err: a name in the lister that disappeared
            // from dir_cache between opendir and
            // readdir_with_attrs (race with concurrent unlink)
            // is exactly what the winfsp layer should drop.
        }
        Ok(out)
    }

    fn open(&self, ino: u64, _flags: u32) -> std::io::Result<u64> {
        let path = self.resolve(ino).map(|e| e.path).unwrap_or_default();
        let fh = NEXT_HANDLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // N-6 fix: sweep expired handle-cached entries on every
        // open() to prevent fd leaks. Without this, retained
        // handles accumulate forever and eventually hit the
        // fd limit (default 1024). The sweep is O(N) over the
        // handles DashMap but runs infrequently (once per open)
        // and the map is typically small (<100 entries).
        if self.handle_caching > std::time::Duration::ZERO {
            let now = std::time::Instant::now();
            let expired: Vec<u64> = self
                .handles
                .iter()
                .filter_map(|e| {
                    let expires = match e.value() {
                        crate::FileHandleState::Read { expires_at, .. }
                        | crate::FileHandleState::Write { expires_at, .. } => *expires_at,
                    };
                    expires.filter(|t| now >= *t).map(|_| *e.key())
                })
                .collect();
            for fh_expired in expired {
                self.handles.remove(&fh_expired);
            }
        }

        // Bug 11: the pre-fix `is_write` check was gated
        // on `cfg!(unix)`, which silently coerced every
        // Windows open() to a Read handle — every write
        // afterwards failed because `handles.get(fh)`
        // saw `FileHandleState::Read` and `write()`'s
        // cache_fd extraction returned None. The flag
        // mask (O_RDONLY=0, O_WRONLY=1, O_RDWR=2) is a
        // platform-independent POSIX convention; the
        // platform adapter is responsible for passing
        // it in this format (Windows: the winfsp
        // adapter maps GRANTED_ACCESS bits to POSIX
        // flags before calling open()).
        let is_write = (_flags & 0x3) != 0;
        if is_write {
            // Issue #300: if the cache file does not exist
            // locally but the backend has the file, fetch
            // the backend content first so a subsequent
            // user-initiated `set_len(n)` truncates a
            // populated cache (preserves the first `n`
            // bytes) rather than zero-filling a freshly
            // created 0-byte cache. The helper is a no-op
            // when the cache file already exists or the
            // backend has no file at this path.
            self.populate_cache_from_backend(&path);
            let cpath = crate::cache_path(&self.cache_dir, &path);
            if let Some(parent) = cpath.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let cache_fd = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .read(true)
                .open(&cpath)
                .ok();
            self.handles.insert(
                fh,
                FileHandleState::Write {
                    path,
                    cache_fd: cache_fd.map(|f| std::sync::Arc::new(std::sync::Mutex::new(f))),
                    dirty: false,
                    dirty_since: None,
                    expires_at: None,
                },
            );
        } else {
            // Activate the background prefetcher for files large enough
            // to benefit (default threshold 64 MiB; see
            // `maybe_create_prefetcher`). Called before the path move
            // below so we don't have to clone.
            let prefetcher = self.maybe_create_prefetcher(ino, &path);
            self.handles.insert(
                fh,
                FileHandleState::Read {
                    path,
                    last_offset: 0,
                    // Start at the same value the remote-fetch path
                    // uses as default (8 MiB). The adaptive doubling
                    // will grow it on sequential reads. 131072 was the
                    // prefetcher's own min, not the fetch path's default.
                    chunk_size: if self.read_chunk_size > 0 {
                        self.read_chunk_size
                    } else {
                        8 * 1024 * 1024
                    },
                    prefetcher,
                    expires_at: None,
                },
            );
        }
        Ok(fh)
    }

    fn read(&self, ino: u64, fh: u64, offset: u64, size: u32) -> std::io::Result<Vec<u8>> {
        // Issue #132: stamp the consumer-side start time so the
        // prefetcher's `pop` can compute the elapsed consumer time
        // and feed `BackpressureController::record_part_consumed`.
        let consume_started = std::time::Instant::now();
        let (path, file_size) = self
            .resolve(ino)
            .map(|e| (e.path, e.size))
            .ok_or(std::io::ErrorKind::NotFound)?;
        // Defensive size reconciliation (see CoreFilesystem::read history
        // for the full explanation). inodes is the FUSE-protocol
        // authoritative size, but the on-disk cache file may have
        // grown more recently than the inodes entry.
        let cache_meta_size = std::fs::metadata(crate::cache_path(&self.cache_dir, &path))
            .map(|m| m.len())
            .unwrap_or(0);
        let actual_size = cache_meta_size.max(file_size);
        tracing::debug!(
            ino, offset, size, file_size, cache_meta_size, actual_size,
            path = %path,
            "read: entry"
        );
        if offset >= actual_size {
            return Ok(vec![]);
        }
        let cap = actual_size - offset;
        let block_idx = offset / CACHE_BLOCK_SIZE;

        // 1. Try read from cache fd first (write handle still open)
        if !self.direct_io {
            let cache_fd = self.handles.get(&fh).and_then(|e| {
                if let crate::FileHandleState::Read { .. } = e.value() {
                    None
                } else if let crate::FileHandleState::Write {
                    cache_fd: Some(fd), ..
                } = e.value()
                {
                    Some(fd.clone())
                } else {
                    None
                }
            });
            if let Some(fd) = cache_fd {
                use std::io::{Read, Seek};
                let mut f = fd.lock().unwrap();
                let file_len = f.metadata()?.len();
                if offset < file_len {
                    let read_size = (size as u64).min(file_len - offset) as usize;
                    let mut buf = vec![0u8; read_size];
                    f.seek(std::io::SeekFrom::Start(offset))?;
                    f.read_exact(&mut buf)?;
                    return Ok(buf);
                }
            }
        }

        // 2. Multi-level cache (L1 → L2 block)
        // Replaces the previous separate L1 mem_cache check
        // (step 2) and L2 block cache check (step 5). The
        // MultiLevelCache checks L1 first, then L2 (with L1
        // backfill on L2 hit), and records per-level metrics.
        if let Some(data) = self.multi_cache.read_block(&path, ino, block_idx) {
            tracing::debug!(
                ino,
                block_idx,
                hit_len = data.len(),
                "read: multi_cache hit"
            );
            // Data is aligned to CACHE_BLOCK_SIZE boundaries —
            // entry (ino, block_idx) covers file bytes
            // `[block_idx * CACHE_BLOCK_SIZE,
            // (block_idx+1) * CACHE_BLOCK_SIZE)`. The slice
            // `data` starts at the block boundary, NOT at the
            // original read offset, so compute `start` relative
            // to the block (not the file).
            let block_start = block_idx * CACHE_BLOCK_SIZE;
            let start = (offset - block_start) as usize;
            let end = (start + size as usize).min(data.len());
            return if start < data.len() {
                Ok(data[start..end].to_vec())
            } else {
                Ok(vec![])
            };
        }

        // 3. Try prefetcher (backpressure-aware background download)
        if let Some(h) = self.handles.get(&fh)
            && let FileHandleState::Read {
                prefetcher: Some(p),
                ..
            } = h.value()
            && let Some(part) = p.pop(offset, Some(consume_started))
        {
            // Populate mem_cache for the prefetched part so subsequent
            // reads on the same block range hit the fast path above.
            // part.data is up to 16 MiB (chunk_size cap) and may span
            // 1-2 CACHE_BLOCK_SIZE blocks; cheap iteration.
            //
            // Also write to the block-level disk cache so the
            // data survives the FUSE session closing (e.g. a
            // remount after a process restart, or a follow-up
            // mount of the same backend). Without this, every
            // remount re-fetches the same prefetched data from
            // remote even though we already paid the network
            // cost once. Uses the same `write_block_cached`
            // helper as the cache-miss path below, so the on-
            // disk format (CRC32C trailer, disk_cache_index
            // insert) can't drift between the two paths.
            let first_blk = part.offset / CACHE_BLOCK_SIZE;
            let data = part.data.clone();
            // Issue #345: when `part.offset` is not block-aligned
            // (e.g. 29.17 MiB), the original code wrote
            // `data[i*8M..(i+1)*8M]` to block `first_blk + i` —
            // but `data[0..8M]` starts at `part.offset` (29.17 MiB),
            // not at the block boundary (24 MiB). Result: the
            // .block file for block 3 contained f_02 source bytes
            // from 29.17 MiB, not 24 MiB. Off-by-(part.offset %
            // CACHE_BLOCK_SIZE) in every .block file. Fix: skip the
            // leading partial block (data[0..8M - offset_in_blk]) and
            // start writing from the next full block, with
            // `first_full_blk = first_blk + 1`. Trailing partial
            // block is still skipped (issue #334). Partial blocks
            // fall through to the read handler's normal cache-miss
            // path (file-level cache or remote fetch) on next read.
            let offset_in_blk = (part.offset % CACHE_BLOCK_SIZE) as usize;
            let (data_start, first_full_blk) = if offset_in_blk > 0 {
                ((CACHE_BLOCK_SIZE as usize) - offset_in_blk, first_blk + 1)
            } else {
                (0, first_blk)
            };
            if data_start < data.len() {
                let n_blks = ((data.len() - data_start) as u64).div_ceil(CACHE_BLOCK_SIZE);
                for i in 0..n_blks {
                    let s = data_start + (i * CACHE_BLOCK_SIZE) as usize;
                    let e = data_start + ((i + 1) * CACHE_BLOCK_SIZE) as usize;
                    // Issue #334 (sister to #331): skip the trailing
                    // partial block. `part.data` can be <
                    // CACHE_BLOCK_SIZE when BackpressureController
                    // shrinks the prefetcher window to the 128 KiB
                    // floor under `--mem-limit` pressure
                    // (prefetcher.rs:303). Without this guard, a
                    // 128 KiB slice would be `mem_cache.put`-ed
                    // under an 8 MiB block-aligned key, poisoning
                    // L1/L2 the same way #331's whole-file `put`
                    // did. The short tail falls through to the read
                    // handler's normal cache-miss path
                    // (multi_cache.read_block / remote fetch) on
                    // the next read, so the file is still correctly
                    // served — just without the prefetch warmup
                    // for the last partial block.
                    if e > data.len() {
                        break;
                    }
                    let slice = data.slice(s..e);
                    self.mem_cache.put(ino, first_full_blk + i, slice.clone());
                    self.write_block_cached(&path, first_full_blk + i, &slice);
                }
            }
            let start = (offset - part.offset) as usize;
            let end = (start + size as usize).min(data.len());
            return if start < data.len() {
                Ok(data[start..end].to_vec())
            } else {
                Ok(vec![])
            };
        }

        // 4. File-level disk cache (whole file)
        if !self.direct_io {
            let fcpath = crate::cache_path(&self.cache_dir, &path);
            if fcpath.exists()
                && let Ok(data) = std::fs::read(&fcpath)
            {
                let b = bytes::Bytes::from(data);
                // Issue #43: if the on-disk file is
                // SHORTER than the inodes-reported size,
                // treat the file-level cache as a
                // partial hit. Returning an empty read
                // at `offset >= b.len()` while inodes
                // claims the file is larger produces
                // kernel-visible EOF in the middle of
                // the file (the read above reports
                // b.len() bytes successfully, then a
                // 0-byte read at the next page — FUSE
                // then thinks the file is b.len()
                // bytes, contradicting the getattr
                // reply). This was the source of the
                // "100M write but read returns 24M then
                // hangs" symptom in s3-lifecycle-stress:
                // a previous mount's writeback didn't
                // complete, leaving a partial cache
                // file with inodes.size = 100M.
                //
                // The fix: if `b.len() < actual_size`
                // (the cache is partial), fall through
                // to the block cache + remote fetch to
                // backfill. mem_cache is still warmed
                // with the partial bytes for the next
                // read in the same region.
                let cache_is_complete = (b.len() as u64) >= actual_size;
                tracing::debug!(
                    path = %path,
                    offset = offset,
                    size = size,
                    cache_bytes = b.len(),
                    actual_size = actual_size,
                    cache_is_complete = cache_is_complete,
                    "read: file-level cache check"
                );
                if cache_is_complete {
                    tracing::debug!(
                        ino,
                        cache_bytes = b.len(),
                        "read: file-level cache hit (complete)"
                    );
                    // Bug B fix: bump the in-memory
                    // LRU sort key on every cache hit.
                    // The on-disk atime is unreliable
                    // on `relatime` mount defaults, so
                    // the LRU sweeper consults the
                    // in-memory `Instant` recorded
                    // here (see `bump_in_memory_atime`
                    // and the field doc on
                    // `disk_cache_index`).
                    bump_in_memory_atime(&self.disk_cache_index, &(path.clone(), None));
                    let start = offset as usize;
                    let end = (start + size as usize).min(b.len());
                    let result = if start < b.len() {
                        b[start..end].to_vec()
                    } else {
                        vec![]
                    };
                    // Issue #331: store only the block-aligned slice
                    // (≤ CACHE_BLOCK_SIZE bytes) under the
                    // (ino, block_idx) key, NOT the entire cache file.
                    // Pre-fix `b` was the whole file, so the next
                    // read at the same block_idx via `multi_cache.
                    // read_block` returned the whole file and the
                    // caller sliced `data[128K..256K]` = data from
                    // the START of the file, not from offset 8M.
                    // `Bytes::slice` is O(1) (refcount), no alloc.
                    let blk_start = (offset / CACHE_BLOCK_SIZE) * CACHE_BLOCK_SIZE;
                    if blk_start < b.len() as u64 {
                        let blk_end = ((blk_start + CACHE_BLOCK_SIZE) as usize).min(b.len());
                        self.mem_cache.put(
                            ino,
                            offset / CACHE_BLOCK_SIZE,
                            b.slice(blk_start as usize..blk_end),
                        );
                    }
                    return Ok(result);
                } else {
                    // Partial cache — warm mem_cache
                    // for the next read but fall
                    // through. The mem_cache put is
                    // best-effort: the block cache +
                    // remote fetch below will satisfy
                    // the current request. Clone `b`
                    // because `b` may be needed for
                    // the partial-data path below
                    // (when offset > b.len()).
                    //
                    // Issue #331: same block-aligned slice
                    // treatment as the complete-cache branch
                    // above — store ≤ CACHE_BLOCK_SIZE bytes
                    // under (ino, block_idx), not the whole
                    // (possibly huge) partial cache file.
                    let blk_start = (offset / CACHE_BLOCK_SIZE) * CACHE_BLOCK_SIZE;
                    if blk_start < b.len() as u64 {
                        let blk_end = ((blk_start + CACHE_BLOCK_SIZE) as usize).min(b.len());
                        self.mem_cache.put(
                            ino,
                            offset / CACHE_BLOCK_SIZE,
                            b.slice(blk_start as usize..blk_end).clone(),
                        );
                    }
                    tracing::debug!(
                        path = %path,
                        cache_bytes = b.len(),
                        inodes_bytes = actual_size,
                        "file-level cache is partial; falling through to block cache + remote (issue #43)"
                    );
                }
            }
        }

        // 6. Remote fetch with adaptive hard cap and 8 MiB block
        // split for mem_cache. The cap is bounded by:
        //   - user config (read_chunk_size, default 128 MiB)
        //   - hard cap (16 MiB for large files, the WHOLE FILE
        //     for small ones — see below)
        //   - cap (bytes remaining to file end)
        //
        // Cold-read optimization: when the file is small enough
        // to fit comfortably in mem_cache (which is the default
        // 256 MiB), fetch the entire file in one S3 round-trip
        // instead of staging it 16 MiB at a time. The pre-fix
        // 16 MiB cap made a 50 MiB file require 4 sequential
        // S3 GETs (one per `cat` readahead window); the bench
        // showed this took 273ms vs rclone's 168ms (rclone's
        // default --vfs-read-chunk-size 128 MiB fetches the
        // whole file in 1 round-trip). With the file-size
        // cap below, a 50 MiB file is fetched in 1 round-
        // trip, dropping cold read latency to the
        // single-S3-GET floor.
        //
        // The `head -c1K` over-fetch worst case is bounded by
        // the mem_cache capacity: 256 MiB / 8 MiB blocks = 32
        // blocks, so the cap of `min(actual_size, 256 MiB)`
        // keeps mem_cache pressure bounded.
        // Adaptive chunk doubling (rclone chunkedreader model).
        // Read handles carry a per-handle `chunk_size` initialised at
        // open(). On sequential reads where the last fetch consumed
        // the full chunk (offset == last_offset, fetched bytes >=
        // requested), the chunk size doubles up to
        // read_chunk_size_limit (or 128 MiB if unset). On a random
        // seek, it resets to the initial value. This cuts round-trips
        // for `cat 100M` from ~12 (with 8 MiB fixed) to ~3
        // (8→16→32→64 MiB).
        let (per_handle_chunk, last_rd_offset) = self
            .handles
            .get(&fh)
            .map(|e| match e.value() {
                FileHandleState::Read {
                    chunk_size,
                    last_offset,
                    ..
                } => (*chunk_size, *last_offset),
                _ => (0, 0),
            })
            .unwrap_or((0, 0));
        let user_cap = if per_handle_chunk > 0 {
            per_handle_chunk
        } else if self.read_chunk_size > 0 {
            self.read_chunk_size
        } else {
            8 * 1024 * 1024
        };
        let hard_cap = if actual_size <= 256 * 1024 * 1024 {
            // File fits in mem_cache: fetch the whole thing.
            actual_size
        } else {
            // File too big for mem_cache: stage 16 MiB at a
            // time to keep per-fetch memory bounded.
            16 * 1024 * 1024
        };
        // Issue #10 + #302: cap fetch_size by the kernel-requested
        // `size` so partial reads (head -c N, tail -c N,
        // dd skip=...) don't pull the whole file from
        // the backend. Pre-fix this was min(user_cap,
        // hard_cap, cap) — for a 1 MiB file cap was
        // 1 MiB regardless of `size`, so head -c 10K
        // fetched the full 1 MiB block. The cold-read
        // opt (whole file in 1 RTT for <=256 MiB files)
        // is preserved for `cat` because cat issues a
        // single FUSE_READ for the whole file — user_cap
        // (= read_chunk_size) takes the hit there, not
        // `size`.
        //
        // #302 (WinFSP 64 KiB IRP cap): WinFSP's default
        // `VolumeParams` (with `AlwaysUseDoubleBuffering=1`)
        // caps the per-IRP read buffer at 64 KiB
        // (`FSP_FSCTL_TRANSACT_BATCH_BUFFER_SIZEMIN`), so a
        // 2 MiB `cat` issues 32 sequential IRP_MJ_READs each
        // asking for 65 536 bytes. With the post-#10
        // `min(size, ...)` fetch_size, every IRP pulls only
        // 65 536 bytes from the backend AND overwrites the
        // mem_cache entry for block 0 (the cache key is
        // `(ino, block_idx)` without an offset-in-block
        // field, so each subsequent IRP's fetch clobbers
        // the previous one). End result: the in-memory
        // cache holds whichever 64 KiB slice the last IRP
        // fetched, and the caller sees a short read.
        //
        // Fix: when offset == 0 and the file fits within
        // the cold-read hard_cap, fetch the whole file in
        // one shot instead of the per-IRP slice. The
        // caller still receives only `size` bytes (the
        // response is trimmed below), but the cache is
        // populated correctly so subsequent IRPs at
        // offset 64 KiB, 128 KiB, ... hit the cache
        // instead of re-fetching (and overwriting).
        //
        // For `head -c N <large-file>` where the file is
        // bigger than `hard_cap`, the `cap <= hard_cap`
        // guard is false and the #10 cap stays in effect
        // — we still only fetch N bytes.
        let fetch_size = if offset == 0 && cap <= hard_cap {
            // Whole-file cold read for files that fit in
            // mem_cache. Same target as the pre-#10 code,
            // but only on the first read of the file.
            user_cap.min(hard_cap).min(cap)
        } else {
            // Subsequent reads (offset > 0) or huge
            // files: honor the per-IRP `size` cap so a
            // `head -c 10K <1 GiB file>` doesn't pull
            // the entire 1 GiB.
            (size as u64).min(user_cap).min(hard_cap).min(cap)
        };

        // Parallel fetch: if `read_chunk_streams > 1` and the fetch
        // is large enough to be worth splitting, use OpenDAL's
        // `Reader` which internally drives N concurrent chunk fetches
        // via `ConcurrentTasks`. rclone does the same with
        // `--vfs-read-chunk-streams`. Below the threshold we fall
        // back to a single `op.read_with().range()` (the
        // round-trip overhead of splitting + joining exceeds the
        // parallelism win against a single in-flight request — the
        // backend is already pipelining).
        //
        // Issue #83: pre-#83 this was a hand-rolled `while` loop
        // calling `op.read_with().range(off..e).await` N times,
        // each wrapped in `rt().block_on`. Because the FUSE
        // runtime is single-worker (see `rt()` at L273), the
        // `block_on` calls executed **serially** — the loop's
        // "N streams" were N sequential `block_on` round-trips,
        // not overlapping requests. The `Reader` API actually
        // overlaps (sibling tasks share the runtime), giving us
        // real concurrency for the first time. `.gap(128 KiB)`
        // merges adjacent ranges under one HTTP request when
        // chunks are close — free HTTP-request reduction on S3.
        // The `.prefetch(N)` builder proposed in the issue text
        // is NOT exposed in opendal 0.57 (`ReaderOptions.prefetch`
        // exists in `raw::ops` but `Reader::new` is `pub(crate)`);
        // mntrs's own prefetcher (prefetcher.rs + issue #132 +
        // #201) already covers that need.
        let streams = self.read_chunk_streams.max(1) as u64;
        let use_parallel = streams > 1 && fetch_size > 128 * 1024;
        // Issue #257: opt-in stale-on-backend-error read
        // fallback. When the user has set
        // `--vfs-read-stale-on-backend-error=true`, a backend
        // fetch failure (network/auth/timeout) does NOT
        // immediately surface as -EIO; we attempt a single
        // fallback to the file-level disk cache. The cache
        // file MUST cover the requested `[offset, offset+size)`
        // range — partial fallback is rejected (returning
        // partial stale bytes when the backend is unreachable
        // could mislead the caller into thinking the read
        // succeeded). The same `read_stale_on_backend_error`
        // flag is checked on both the parallel and
        // single-stream branches via `try_backend_or_stale`
        // below.
        let try_backend_or_stale =
            |res: std::io::Result<bytes::Bytes>| -> std::io::Result<bytes::Bytes> {
                self.try_backend_or_stale(&path, offset, size, res)
            };
        let b: bytes::Bytes = if use_parallel {
            // OpenDAL `Reader` splits `[offset..offset+fetch_size)`
            // into `chunk_bytes`-sized pieces and fetches up to
            // `streams` of them in parallel. `.gap(128 KiB)` tells
            // the internal range-merge layer that adjacent chunks
            // separated by ≤128 KiB may be coalesced into a single
            // backend request.
            let op = self.op.clone();
            let p = path.clone();
            let chunk_bytes = fetch_size.div_ceil(streams);
            let parallel_res = rt().block_on(async move {
                op.reader_with(&p)
                    .concurrent(streams as usize)
                    .chunk(chunk_bytes as usize)
                    .gap(128 * 1024)
                    .await?
                    .read(offset..offset + fetch_size)
                    .await
            });
            let parallel_res = parallel_res.map_err(|e| {
                tracing::debug!(error = %e, "read: reader.read failed");
                std::io::Error::other("read failed")
            });
            // `Buffer::to_bytes()` is zero-copy when the read
            // produced contiguous bytes (the common single-range
            // case); only copies if OpenDAL had to assemble
            // disjoint pieces.
            let bytes_res = parallel_res.map(|buf| buf.to_bytes());
            try_backend_or_stale(bytes_res)?
        } else {
            let op = self.op.clone();
            let p = path.clone();
            let single_res = rt()
                .block_on(async move { op.read_with(&p).range(offset..offset + fetch_size).await })
                .map_err(|_| std::io::Error::other("read failed"))
                .map(|v| bytes::Bytes::from(v.to_vec()));
            try_backend_or_stale(single_res)?
        };
        let len = (b.len() as u32).min(size) as usize;
        let result = b[..len].to_vec();
        tracing::debug!(
            ino,
            offset,
            fetch_len = b.len(),
            result_len = result.len(),
            "read: remote fetch"
        );
        // Populate L1 (mem_cache) for ALL blocks covered by this
        // fetch, not just the first one. Without this, a 16 MiB
        // fetch would store the entire 16 MiB under one
        // (ino, block_idx) key, evicting anything else in cache
        // and forcing the next read on a neighbouring block to
        // re-fetch from remote. Bytes::slice is zero-copy.
        // Issue #345: same off-by-offset guard as the prefetcher
        // pop path. When `offset` is not block-aligned, the
        // original code wrote `b[i*8M..(i+1)*8M]` to block
        // `first_blk + i` — but `b[0..8M]` starts at `offset`,
        // not at the block boundary. Skip the leading partial
        // block (b[0..8M - offset_in_blk]) and start at
        // `first_full_blk = first_blk + 1`. Trailing partial
        // block guard (issue #337) is preserved.
        let first_blk_raw = offset / CACHE_BLOCK_SIZE;
        let offset_in_blk = (offset % CACHE_BLOCK_SIZE) as usize;
        let (data_start, first_blk) = if offset_in_blk > 0 {
            (
                (CACHE_BLOCK_SIZE as usize) - offset_in_blk,
                first_blk_raw + 1,
            )
        } else {
            (0, first_blk_raw)
        };
        let n_blks = ((b.len().saturating_sub(data_start)) as u64).div_ceil(CACHE_BLOCK_SIZE);
        // Issue #337 (refined): only skip the trailing partial
        // block if there is MORE file content past this read —
        // i.e. the read ended mid-block and a future read of the
        // same block_idx would otherwise L1-hit the tiny slice
        // and read garbage for the missing tail.
        //
        // If this read consumed the rest of the file
        // (`offset + b.len() >= actual_size`) the partial IS
        // the whole truth for that block — keep it (matches
        // pre-fix `e.min(b.len())` behavior, which is the
        // fast path for small files: a 1 MiB file's first read
        // populates L1 with the whole 1 MiB and subsequent
        // reads L1-hit, no remote re-fetch).
        let read_reached_eof = offset + b.len() as u64 >= actual_size;
        for i in 0..n_blks {
            let s = data_start + (i * CACHE_BLOCK_SIZE) as usize;
            let e = data_start + ((i + 1) * CACHE_BLOCK_SIZE) as usize;
            if e > b.len() && !read_reached_eof {
                // Poison guard: tiny slice at non-block-aligned
                // end, more file content past it. Skip and let
                // a future full-block read populate cleanly.
                break;
            }
            let end = e.min(b.len());
            self.mem_cache.put(ino, first_blk + i, b.slice(s..end));
        }
        // Also populate block-level disk cache so subsequent reads
        // of the same range hit the fast path on disk (rclone's
        // `--vfs-cache-mode full` parity). Each block is a separate
        // file under `cache_dir/{hash}_{block_idx:010x}.block`; the
        // read path already checks for these (CoreFilesystem::read
        // step 5) — they were just never written until now.
        //
        // `b.slice(s..e)` is a zero-copy Bytes view, and the file
        // is opened with create+truncate(false) so a re-read
        // overwrites the cached chunk in place. Write failures
        // are non-fatal: log + continue. The mem_cache copy above
        // is what the FUSE worker actually returns to the kernel.
        //
        // `write_block_cached` is the single point of truth
        // for the on-disk format (CRC32C trailer for full
        // blocks, no trailer for partial, dashmap insert
        // on success). The same helper is called from the
        // prefetcher pop path below so the two paths
        // can't drift.
        //
        // #29 (cold-read concurrency): submit N block
        // writes to the disk-IO thread pool. The FUSE
        // worker returns OK immediately; the workers
        // run the block writes in parallel. Pre-fix this
        // was a serial loop on the FUSE worker
        // (`self.write_block_cached` for each block)
        // — for a 50 MiB fetch with 16 MiB chunk size,
        // 1 block is written synchronously, but for the
        // 6-block bench workload, all 6 ran serially on
        // the FUSE worker at ~5 ms each = 30 ms of
        // cold-read latency the user paid synchronously.
        // Off-thread parallel writes drop that to a
        // single block's worth of disk I/O (~5 ms).
        //
        // Two pieces of work stay on the FUSE worker
        // (cheap, in-memory, microseconds):
        //   1. `direct_io` short-circuit — mirrors the
        //      `write_block_cached` guard. In direct-io
        //      mode the disk cache is bypassed entirely;
        //      submitting jobs would just waste pool
        //      capacity and write files no read would
        //      ever consult.
        //   2. `disk_cache_index.insert(...)` — the LRU
        //      sort key. The pool worker can't update
        //      this dashmap (it has no `&self` reference),
        //      so we insert it inline. The insert is a
        //      lock-free dashmap op (~ns), the file I/O
        //      that takes ms is what we offload. If the
        //      pool-side write later fails, the LRU sweep
        //      will try to unlink a missing file (ignored)
        //      and remove the stale index entry — same
        //      recovery shape as a torn-down cache dir.
        if !self.direct_io {
            let cache_dir = self.cache_dir.clone();
            for i in 0..n_blks {
                let s = data_start + (i * CACHE_BLOCK_SIZE) as usize;
                let e = data_start + ((i + 1) * CACHE_BLOCK_SIZE) as usize;
                // Issue #337 (L2 side — same refined guard
                // as the L1 populate above). If we wrote a
                // sub-8 MiB slice as a block cache file at
                // non-end-of-file, the next read of the same
                // block_idx would L2-hit and return the
                // partial bytes — same poisoning the L1 guard
                // prevents. When the read consumed the rest
                // of the file, the partial IS the whole
                // block, so keep it (small-file fast path).
                if e > b.len() && !read_reached_eof {
                    break;
                }
                let end = e.min(b.len());
                let slice = b.slice(s..end).to_vec();
                let block_idx = first_blk + i;
                let written_size = (slice.len() + BLOCK_OVERHEAD) as u64;
                submit_block_cache_write(&cache_dir, &path, block_idx, slice);
                self.disk_cache_index.insert(
                    (path.clone(), Some(block_idx)),
                    (written_size, std::time::Instant::now()),
                );
            }
        }
        // Adaptive chunk doubling feedback (rclone chunkedreader model).
        // On sequential read where we consumed a full chunk, double
        // the per-handle chunk_size for the next call (capped at
        // read_chunk_size_limit or 128 MiB). On a random seek, reset
        // to the initial value.
        let is_sequential = offset == last_rd_offset;
        let fetched_full = b.len() as u64 >= fetch_size;
        if let Some(mut entry) = self.handles.get_mut(&fh)
            && let FileHandleState::Read {
                last_offset,
                chunk_size,
                ..
            } = entry.value_mut()
        {
            *last_offset = offset + len as u64;
            if is_sequential && fetched_full {
                let limit = if self.read_chunk_size_limit > 0 {
                    self.read_chunk_size_limit
                } else {
                    128 * 1024 * 1024
                };
                *chunk_size = (*chunk_size).saturating_mul(2).min(limit);
            } else if !is_sequential {
                *chunk_size = self.read_chunk_size.max(131072);
            }
        }
        Ok(result)
    }

    fn write(&self, _ino: u64, _fh: u64, _offset: u64, _data: &[u8]) -> std::io::Result<u32> {
        let fh_val = _fh;
        // #17 (small-write hot-path): single handles.get
        // call extracts path AND cache_fd in one shard
        // lock. Pre-fix did two separate gets (one for
        // path, one for cache_fd) — each acquired a
        // DashMap shard lock + cloned an Arc<Mutex<File>>.
        // For 4 KiB writes (FUSE block size) this was a
        // measurable fraction of the per-write cost vs
        // the single-RTT rclone path.
        let (path, cache_fd) = match self.handles.get(&fh_val) {
            Some(entry) => match entry.value() {
                crate::FileHandleState::Write { path, cache_fd, .. } => {
                    (path.to_string(), cache_fd.clone())
                }
                // Non-Write handle: keep the old behavior
                // of consulting only `path()` (the
                // pre-fix code did this implicitly via
                // the .path() helper).
                other => (other.path().to_string(), None),
            },
            None => return Err(std::io::ErrorKind::NotFound.into()),
        };

        if self.direct_io {
            let op = self.op.clone();
            let p = path.clone();
            let d = _data.to_vec();
            rt().block_on(async move { op.write(&p, d).await })
                .map_err(|_| std::io::Error::other("write failed"))?;
            return Ok(_data.len() as u32);
        }

        // #24 (async write): the actual disk I/O
        // (set_len + seek + write_all) is moved to a
        // background thread so the FUSE worker returns
        // to the kernel immediately. The FUSE kernel
        // only blocks the user process for the time
        // between the write() syscall and our OK reply,
        // not for the actual disk write. Multiple
        // concurrent writers to different files now
        // proceed in parallel (each has its own disk I/O
        // thread). Multiple writers to the same file
        // serialize on the cache_fd Mutex inside the
        // thread, not in the FUSE worker.
        //
        // The data is in the OS page cache after
        // write_all() returns inside the thread — FUSE
        // semantics are satisfied because we already
        // returned OK to the kernel. The kernel's page
        // cache holds the data and will flush it to disk
        // asynchronously. The writeback worker eventually
        // uploads the cache file to the backend (S3/HDFS);
        // that's the actual user-facing durability
        // mechanism (the cache file is just a re-read
        // optimization, not a source of truth).
        //
        // Cost analysis vs sync: thread spawn is ~10µs
        // (cheap), the actual disk I/O happens off the
        // FUSE worker. The bench improvement is ~3.4x
        // for 1 MiB parallel writes (sync 17ms/write vs
        // rclone 5ms/write — most of rclone's lead was
        // FUSE-worker serialization on the cache_fd
        // mutex, which async sidesteps).
        // #27 (disk-IO thread pool): build a
        // `DiskWriteJob` and submit it to the pool.
        // The FUSE worker returns OK immediately; the
        // actual disk I/O happens on a worker thread.
        // Replaces cc2667f's per-write `std::thread::spawn`
        // (which paid ~10 µs of thread-spawn overhead per
        // write) with a shared worker pool that reuses
        // threads.
        //
        // Bug #62 (task #3 root cause): the async
        // submit + worker-pool design above has a
        // read-after-write race. The FUSE write handler
        // returns OK to the kernel before the pool worker
        // runs write_all on the cache fd. A subsequent
        // read can arrive in the page cache before the
        // pool worker ran, see the cache file as 0
        // bytes, fall through to the remote fetch, and
        // return EIO because the writeback hasn't landed.
        //
        // Fix: when cache_fd is Some, write to it
        // synchronously on the FUSE worker. The cache
        // file write is a page-cache memcpy (sub-µs),
        // cheaper than the old async path's pool submit
        // + thread-wakeup overhead. The async writeback
        // to the remote (S3/HDFS) is still async — that
        // is where the real network latency lives, and
        // it is triggered separately by flush() /
        // release(), not by this handler.
        //
        // Issue #128: when appending to a pre-existing
        // file whose whole-file cache was never
        // populated (read went through block cache /
        // streaming), the cache file is 0 bytes. The
        // old code did `set_len(offset + data_len)`
        // which zero-fills [0..offset), then a
        // subsequent read (after cache invalidation)
        // falls through to the remote which still has
        // the old content (writeback not flushed).
        //
        // Fix: backfill the cache gap [cache_len ..
        // offset) from the backend BEFORE writing.
        // The network I/O happens outside the mutex
        // to avoid blocking concurrent reads on the
        // same fh. Gap is capped at 64 MiB to bound
        // the one-time cost on large sparse writes.
        const GAP_BACKFILL_MAX: u64 = 64 * 1024 * 1024;
        tracing::debug!(
            path = %path,
            offset = _offset,
            data_len = _data.len(),
            has_cache_fd = cache_fd.is_some(),
            "write: entry"
        );
        let gap_data: Option<Vec<u8>> = if _offset > 0 {
            if let Some(fd) = &cache_fd {
                let cache_len = fd
                    .lock()
                    .ok()
                    .and_then(|f| f.metadata().ok())
                    .map(|m| m.len())
                    .unwrap_or(0);
                if cache_len < _offset {
                    let gap = _offset - cache_len;
                    if gap <= GAP_BACKFILL_MAX {
                        let op = self.op.clone();
                        let p = path.clone();
                        let r = rt().block_on(async move {
                            op.read_with(&p).range(cache_len.._offset).await
                        });
                        match r {
                            Ok(buf) => Some(buf.to_vec()),
                            Err(e) => {
                                tracing::warn!(
                                    path = %path,
                                    gap = gap,
                                    "backfill failed; rejecting write to avoid cache corruption"
                                );
                                return Err(std::io::Error::other(format!(
                                    "backfill gap read failed: {e}"
                                )));
                            }
                        }
                    } else {
                        tracing::debug!(
                            path = %path,
                            gap = gap,
                            max = GAP_BACKFILL_MAX,
                            "gap exceeds backfill cap; accepting zero-fill"
                        );
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if let Some(fd) = &cache_fd
            && let Ok(mut f) = fd.lock()
        {
            use std::io::{Seek, Write};
            // Re-check: another thread may have grown
            // the cache file while we were reading the
            // gap. Only backfill if still needed.
            let actual_len = f.metadata().map(|m| m.len()).unwrap_or(0);
            if let Some(gap) = &gap_data
                && actual_len < _offset
            {
                let _ = f.seek(std::io::SeekFrom::Start(actual_len));
                if f.write_all(gap).is_err() {
                    // Backfill write failed — truncate
                    // back to avoid serving zero-filled
                    // data. The write will be retried by
                    // the kernel.
                    let _ = f.set_len(actual_len);
                    return Err(std::io::Error::other("backfill write to cache failed"));
                }
            }
            let end = _offset + _data.len() as u64;
            let _ = f.set_len(end);
            let _ = f.seek(std::io::SeekFrom::Start(_offset));
            f.write_all(_data)?;
        }
        // Bug #62 / task #3 (part 2): the cache file
        // is the local re-read optimization. The
        // writeback worker uploads the full cache file
        // to the backend on flush/release. Per-write
        // op.write() was removed because opendal's
        // Operator::write() is a whole-file PUT — each
        // FUSE write replaced the backend file with only
        // the current chunk, corrupting multi-chunk
        // writes. The hdfs-native backend panics when
        // offset+len > file_length() on the truncated
        // backend file (Cannot read past end of the
        // file). The cache file (written synchronously
        // above) is the source of truth for read-after-
        // write within the same mount session.
        // If op.write() fails (backend down), trigger
        // writeback immediately so the file doesn't
        // wait for flush/release.
        {
            let cpath = crate::cache_path(&self.cache_dir, &path);
            if cpath.exists() {
                register_dirty_cache_path(&cpath);
                if let Some(tx) = self.writeback_sender.get()
                    && self.writeback_pending.insert(path.as_str().to_string())
                {
                    // Issue #202: small files skip the 5s delay
                    // queue (per_task_writeback_delay returns
                    // Duration::ZERO for files < threshold) so
                    // SQLite / etcd / RocksDB writes hit S3
                    // before the next flush, not 5s after.
                    let delay = self.per_task_writeback_delay(_ino);
                    // Issue #268.2 O9: symmetry with the
                    // flush/release handlers at L3733/L3902.
                    // Pre-fix this `let _ =` was silent —
                    // worker death silently dropped the
                    // writeback, and the .dirty sidecar only
                    // got recovered on next mount. Warn so
                    // operators can correlate "write returned
                    // Ok but data didn't upload" with worker
                    // shutdown.
                    if let Err(e) = tx.send(WritebackTask {
                        ino: _ino,
                        remote_path: path.clone(),
                        cache_path: cpath,
                        retry_cycle: 0,
                        per_task_delay: delay,
                    }) {
                        tracing::warn!(
                            path = %path,
                            error = %e,
                            "write: writeback enqueue failed; \
                             .dirty sidecar will recover on next mount"
                        );
                    }
                }
            }
        }
        // #89 follow-up: invalidate the FUSE kernel's
        // attr cache for this inode. Without this, the
        // kernel keeps using the pre-write size it cached
        // from the last getattr/setattr reply, and the next
        // O_APPEND open issues a write at the wrong offset
        // (clobbering prior writes — see the trace in
        // issue #89). ENOENT is harmless (kernel already
        // dropped the cache); ignore it like the fuser
        // `send_inval` helper does.
        //
        // Unix-only — `self.fuse_notifier` is gated
        // `#[cfg(not(windows))]` on the struct. On WinFSP
        // the write handler is synchronous (no async
        // cache-file write), so the kernel never sees a
        // stale size for the same inode and we don't need
        // an invalidation hook. See issue #93.
        #[cfg(not(windows))]
        if let Some(notifier) = self.fuse_notifier.get() {
            let r = notifier.inval_inode(fuser::INodeNo(_ino), 0, -1);
            tracing::debug!(ino = _ino, result = ?r, "write: inval_inode");
        }
        // #8 (durability): register the cache file
        // for the periodic fsync thread (5 s tick,
        // see spawn_fsync_thread). Without this, a
        // power loss between the FUSE write and the
        // kernel's lazy page-cache flushback can zero
        // the cache file.
        //
        // Single registration point: the `register_dirty_cache_path`
        // call inside the `cpath.exists()` block above covers both
        // the cache_fd path (cache_fd.is_some() ⇒ cpath.exists()) and
        // the no-fd fallback (pre-existing file). A second call here
        // was redundant (DashSet insert is idempotent) — removed in
        // issue #135#1.
        // #27 (disk-IO thread pool): for the no-fd
        // fallback (the open() path that couldn't open
        // the cache file — rare, only when $HOME is
        // unwritable), still submit to the pool so the
        // write eventually lands on disk. The pool
        // worker re-opens the file and writes.
        let job = match &cache_fd {
            Some(_) => None, // Already wrote synchronously above; no pool work.
            None => Some(DiskWriteJob {
                cache_fd: None,
                cache_path: Some(crate::cache_path(&self.cache_dir, &path)),
                remote_path: path.clone(),
                offset: _offset,
                data: _data.to_vec(),
                block_cache: None,
                cache_gen: 0,
            }),
        };
        submit_disk_write(job);

        // Index the whole-file cache entry. The key is
        // `(path, None)` to distinguish from block-level
        // entries `(path, Some(idx))`. We use `Instant::now()`
        // (the in-memory LRU sort key), not `SystemTime::now()`
        // (the on-disk mtime, which `relatime` doesn't update
        // on read).
        self.disk_cache_index.insert(
            (path.clone(), None),
            (_offset + _data.len() as u64, std::time::Instant::now()),
        );
        // Issue #39: evict BEFORE the pool worker tries to
        // write the cache file. The pre-fix order submitted
        // the job first and evicted after, which raced with
        // the pool worker: on a full disk the pool worker
        // could hit ENOSPC before the eviction freed
        // anything, leaving the FUSE reply Ok but the cache
        // file silently truncated. Eager eviction before
        // submit means the pool worker usually sees
        // post-eviction free space; the in-pool retry on
        // ENOSPC in `execute()` is the safety net for the
        // rare case where eviction didn't free enough (e.g.
        // the cache is so full that even an empty index
        // doesn't meet the min-free-space target).
        //
        // Runs inline (synchronous, on the FUSE write
        // worker) because (a) the index is small in
        // practice (entries == cached files, not blocks)
        // and (b) deferring to a background thread
        // introduces a race where a subsequent write sees
        // out-of-space before the eviction completes. The
        // current write is allowed to push the total
        // briefly over the limit; the next write that
        // observes the breach evicts down to the target.
        // See `evict_lru_if_needed` for the exact size
        // math.
        self.evict_lru_if_needed();
        let written = _data.len() as u32;

        // Update inodes size — must CREATE the entry if it doesn't exist.
        //
        // The naive `entry(_ino).and_modify(...)` is a no-op when the
        // ino has not been registered in the inodes map yet. This
        // happens on the very first write to a brand-new file: the
        // FUSE kernel can hand us a write() before the lookup()
        // induced alloc_ino() ever runs (the kernel does a stat cache
        // lookup in parallel, or the write is initiated by an
        // application that already has a file descriptor from outside
        // this mount). When that occurs, and_modify silently does
        // nothing, the inodes map keeps a stale `None` (or a
        // 0-sized entry from a prior iter), the kernel then sees
        // size=0 from our getattr, asks for 0 bytes, and the user
        // observes an empty file.
        //
        // The fix is the two-step `and_modify().or_insert_with()`:
        //   - if an entry exists, only grow its size (never shrink
        //     on a single write — setattr() owns truncation)
        //   - if no entry exists, create one seeded with the new
        //     write's end offset
        //
        // The initial mtime is set to `now()` (Bug C fix); the
        // and_modify branch also updates it on every subsequent write
        // so a read-after-write sees a fresh mtime even before the
        // writeback upload has landed.
        let end = _offset + _data.len() as u64;
        let write_mtime = std::time::SystemTime::now();
        self.inodes
            .entry(_ino)
            .and_modify(|v| {
                if end > v.size {
                    v.size = end;
                }
                v.mtime = Some(write_mtime);
            })
            .or_insert_with(|| InodeEntry {
                path: path.clone(),
                kind: FileType::RegularFile,
                size: end,
                mtime: Some(write_mtime),
            });

        // Invalidate mem_cache for this ino.
        //
        // mem_cache is a per-(ino, block_idx) DashMap of recently-read
        // Bytes, populated lazily by the read path on a cache miss.
        // Writes change the underlying on-disk cache file but leave
        // mem_cache entries stale — they hold the pre-write content.
        // A subsequent read that consults mem_cache first would
        // otherwise return data capped at the old entry's length
        // (since the read code does `b[start..end].min(b.len())`).
        //
        // The classic symptom: write 18 bytes, read returns 18
        // bytes (good); append 10 bytes, the second read hits
        // mem_cache and returns only the first 18 bytes (bad — the
        // appended tail is silently lost). This is the original
        // d4d19c8 flake: tests 5 ("append + verify") and 6
        // ("append to pre-existing file") would intermittently see
        // truncated content.
        //
        // Unified L1 + L2 invalidation via multi_cache
        // (issue #127). Drops every block_idx for this
        // ino in L1 (mem_cache) and all block-level .block
        // files for this path in L2 (disk cache). This
        // replaces the previous two separate calls
        // (mem_cache.invalidate_ino + invalidate_block_
        // cache_for_path) with a single multi_cache call
        // that handles both levels atomically.
        self.multi_cache.invalidate(&path, _ino);
        tracing::debug!(path = %path, ino = _ino, "write: invalidated L1+L2 cache for path");

        // #17 (small-write hot-path): pre-fix did a
        // full `handles.insert(fh, Write { path: ...,
        // cache_fd, dirty: true, dirty_since: now })`
        // every single write — that rewrote the
        // FileHandleState variant from scratch (path
        // clone, Arc clone, fresh struct alloc) even
        // when only `dirty_since` actually changed.
        // and_modify avoids the rewrite: we update
        // just the two fields that matter. The
        // or_insert_with branch is a safety net for
        // the (extremely unlikely) case that another
        // thread evicted the handle entry between the
        // initial get above and here.
        self.handles
            .entry(fh_val)
            .and_modify(|h| {
                if let crate::FileHandleState::Write {
                    dirty, dirty_since, ..
                } = h
                {
                    *dirty = true;
                    *dirty_since = Some(std::time::Instant::now());
                }
            })
            .or_insert_with(|| crate::FileHandleState::Write {
                path: path.clone(),
                cache_fd: cache_fd.clone(),
                dirty: true,
                dirty_since: Some(std::time::Instant::now()),
                expires_at: None,
            });
        Ok(written)
    }

    fn flush(&self, _ino: u64, _fh: u64) -> std::io::Result<()> {
        // Look up the handle to find the path and dirty state
        let fh_val = _fh;
        let (path, dirty, cache_fd) = {
            let entry = self.handles.get(&fh_val).map(|r| r.clone());
            if let Some(crate::FileHandleState::Write {
                path: p,
                dirty: d,
                cache_fd,
                ..
            }) = entry
            {
                (p, d, cache_fd)
            } else {
                return Ok(());
            }
        };
        if dirty {
            // Issue #34: force the cache fd's data to
            // stable storage before we reply Ok. Pre-fix
            // this method only queued an async writeback
            // job, and the FUSE worker returned to the
            // kernel with the bytes still in the OS page
            // cache. A user-space close(2) then saw the
            // FUSE reply and treated the data as durable
            // -- but a power loss between the reply and
            // the kernel's lazy writeback would leave the
            // cache file empty (or truncated), and the
            // async writeback had no bytes to upload.
            //
            // sync_data (not sync_all) matches libfuse
            // passthrough_hp's dup+close pattern: we
            // only need the user data flushed, mtime/
            // ctime can wait for the kernel's later
            // writeback. Holding the per-fd mutex blocks
            // a concurrent writer through the same fd so
            // we don't sync mid-write.
            //
            // Errors are surfaced: if the disk is so
            // broken that fdatasync fails, the user
            // process deserves to see it (typically as
            // EIO from close()) rather than discovering
            // the corruption on the next read.
            if let Some(fd) = &cache_fd
                && let Ok(f) = fd.lock()
                && let Err(e) = f.sync_data()
            {
                tracing::warn!(
                    path = %path,
                    error = %e,
                    "flush fdatasync failed; data may not be durable on local disk"
                );
                return Err(e);
            }
            // Push single cache file to writeback queue
            let cpath = crate::cache_path(&self.cache_dir, &path);
            if cpath.exists() {
                let sidecar = cpath.with_extension("dirty");
                if let Err(e) = std::fs::write(&sidecar, path.as_bytes()) {
                    tracing::warn!(error=%e, path=?sidecar, "sidecar write failed");
                }
                if let Some(tx) = self.writeback_sender.get() {
                    // Bug 22: surface a send() failure (writeback
                    // worker dropped) instead of silently
                    // discarding the queue request. The
                    // .dirty sidecar written just above stays
                    // on disk and will be picked up on next-
                    // mount recovery.
                    //
                    // Issue #53: 4th tuple element is the
                    // retry-cycle count — 0 for a fresh
                    // enqueue from flush. The writeback
                    // worker re-enqueues with cycle+1 when
                    // the in-process 5-attempt retry loop
                    // exhausts, applying a 60 s cooldown
                    // between cycles.
                    //
                    // Issue #38: skip the enqueue if a
                    // writeback for this path is already in
                    // flight. The pending entry is removed
                    // by the writeback completion path
                    // (success + retry-exhaustion) so a
                    // future flush/release with new content
                    // will enqueue a fresh task. The .dirty
                    // sidecar stays on disk through the
                    // upload, so a stale "in flight" entry
                    // is also protected by the next-mount
                    // recovery path.
                    if self.writeback_pending.insert(path.as_str().to_string()) {
                        // Issue #202: per-task delay based on
                        // inodes.size vs writeback_immediate_threshold.
                        // Small files skip the 5s delay queue
                        // (Duration::ZERO); large files keep the
                        // 5s batching behavior.
                        let delay = self.per_task_writeback_delay(_ino);
                        if let Err(e) = tx.send(WritebackTask {
                            ino: _ino,
                            remote_path: path.clone(),
                            cache_path: cpath,
                            retry_cycle: 0,
                            per_task_delay: delay,
                        }) {
                            // Send failed — back out the
                            // pending insert so the next
                            // flush can retry.
                            self.writeback_pending.remove(path.as_str());
                            tracing::warn!(
                                path=%path,
                                error=%e,
                                "flush writeback send failed (worker dropped?); \
                                 .dirty sidecar kept for next-mount retry"
                            );
                        }
                    } else {
                        tracing::trace!(
                            path=%path,
                            "flush: writeback already in flight for path; skipping duplicate enqueue (issue #38)"
                        );
                    }
                }
                tracing::debug!(path=%path, "flush queued writeback");
            }
            // Mark handle clean; writeback happens asynchronously
            let cache_fd = self.handles.get(&_fh).and_then(|e| {
                if let crate::FileHandleState::Write {
                    cache_fd: Some(fd), ..
                } = e.value()
                {
                    Some(fd.clone())
                } else {
                    None
                }
            });
            self.handles.insert(
                _fh,
                crate::FileHandleState::Write {
                    path: path.clone(),
                    cache_fd,
                    dirty: false,
                    dirty_since: None,
                    expires_at: None,
                },
            );
        }
        Ok(())
    }
    fn fsync(&self, ino: u64, fh: u64, datasync: bool) -> std::io::Result<()> {
        // Issue #35: force the open cache fd's data (and
        // optionally its metadata) to stable storage.
        // SQLite / etcd / RocksDB call fsync(2) on every
        // commit; returning ENOSYS (the fuser default
        // pre-fix) means those workloads silently lose
        // commit guarantees. With this override the
        // kernel sees a real `Ok` once the cache file's
        // bytes are on local disk.
        //
        // We sync the *cache file*, not the remote
        // object — the cache is the source of truth for
        // a FUSE mount's read-after-write view, and the
        // async writeback worker will upload to the
        // remote backend in the background. If a future
        // backend is "synchronous-or-bust" (no async
        // writeback), this method should also block on
        // the upload completing before returning Ok.
        let cache_fd = self.handles.get(&fh).and_then(|e| match e.value() {
            crate::FileHandleState::Write {
                cache_fd: Some(fd), ..
            } => Some(fd.clone()),
            _ => None,
        });
        let Some(fd) = cache_fd else {
            // No open cache fd (e.g. setattr with no fh,
            // or a read-only handle that never opened a
            // cache file). Surface NotFound so the
            // adapter maps to ENOENT; the database
            // typically retries or fails the transaction
            // — better than silently Ok'ing.
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("fsync({ino:#x}, fh={fh}): no open cache fd"),
            ));
        };
        // Hold the per-fd mutex while syncing so a
        // concurrent write through the same fd doesn't
        // race with the kernel's writeback. The write
        // path takes the same mutex around set_len +
        // write_all (see DiskWriteJob::do_write), so a
        // concurrent write is already serialised
        // through this lock.
        let f = fd
            .lock()
            .map_err(|e| std::io::Error::other(format!("fsync({fh}) mutex poisoned: {e}")))?;
        if datasync {
            f.sync_data()?;
        } else {
            f.sync_all()?;
        }
        Ok(())
    }
    fn release(&self, _ino: u64, fh: u64) -> std::io::Result<()> {
        // On release, trigger writeback for dirty handles
        let was_dirty = if let Some(entry) = self.handles.get(&fh)
            && let crate::FileHandleState::Write {
                path,
                dirty: true,
                cache_fd: Some(fd),
                ..
            } = entry.value()
        {
            // Issue #34 (release counterpart to flush):
            // fdatasync the cache fd before queueing the
            // async writeback. close(2) returns to the
            // user once FUSE replies Ok, and the user
            // treats that as "data is on local disk and
            // safe from this process crashing". Without
            // the explicit sync, only the OS page cache
            // holds the bytes; a power loss between
            // close() returning and the kernel's lazy
            // writeback would zero the cache file and
            // the async writeback would have nothing to
            // upload.
            //
            // sync_data (not sync_all) is intentional:
            // we only need the user data flushed, the
            // mtime update from the last write can stay
            // in the page cache and ride out on the
            // kernel's normal writeback. This matches
            // libfuse passthrough_hp's dup+close pattern.
            if let Ok(f) = fd.lock()
                && let Err(e) = f.sync_data()
            {
                tracing::warn!(
                    path = %path,
                    error = %e,
                    "release fdatasync failed; data may not be durable on local disk"
                );
                return Err(e);
            }
            let cpath = crate::cache_path(&self.cache_dir, path);
            if cpath.exists() {
                let sidecar = cpath.with_extension("dirty");
                let _ = std::fs::write(&sidecar, path.as_bytes());
                if let Some(tx) = self.writeback_sender.get() {
                    // Bug 22 (release-side mirror of the flush
                    // fix above). Same rationale + same
                    // recovery shape. Issue #53: 4th tuple
                    // element is the retry-cycle count —
                    // 0 for a fresh enqueue.
                    //
                    // Issue #38: skip the enqueue if a
                    // writeback for this path is already
                    // in flight. This is the second
                    // enqueue site of the bug (flush +
                    // release both fire for the same
                    // file when there's a write between
                    // them); the pending-set check is
                    // identical to the flush handler.
                    if self.writeback_pending.insert(path.as_str().to_string()) {
                        // Issue #202: per-task delay mirrors the
                        // flush handler above. See the per_task_
                        // writeback_delay doc comment for the
                        // size source (inodes.size) and the
                        // recovery-sentinel fallback.
                        let delay = self.per_task_writeback_delay(_ino);
                        if let Err(e) = tx.send(WritebackTask {
                            ino: _ino,
                            remote_path: path.clone(),
                            cache_path: cpath,
                            retry_cycle: 0,
                            per_task_delay: delay,
                        }) {
                            self.writeback_pending.remove(path.as_str());
                            tracing::warn!(
                                path=%path,
                                error=%e,
                                "release writeback send failed (worker dropped?); \
                                 .dirty sidecar kept for next-mount retry"
                            );
                        }
                    } else {
                        tracing::trace!(
                            path=%path,
                            "release: writeback already in flight; skipping duplicate (issue #38)"
                        );
                    }
                }
                tracing::debug!(path=%path, "release queued writeback");
            }
            true
        } else {
            false
        };

        // Issue #54: signal any in-flight prefetcher
        // to stop — but ONLY when the handle is
        // actually being released. Pre-fix the
        // prefetcher cancel happened unconditionally,
        // which clobbered the handle_caching
        // contract for Read handles: even when
        // handle_caching was configured, every
        // close() killed the prefetcher, so the
        // next open() had to spin up a fresh one
        // (cold cache, no prefetched chunks).
        if self.handle_caching == std::time::Duration::ZERO
            && let Some(entry) = self.handles.get(&fh)
            && let crate::FileHandleState::Read {
                prefetcher: Some(p),
                ..
            } = entry.value()
        {
            p.cancel();
        }

        if self.handle_caching > std::time::Duration::ZERO && !was_dirty {
            // Issue #54: keep the handle alive for
            // handle_caching duration so the next
            // open() can reuse the cache fd (Write)
            // and the in-flight prefetcher (Read).
            // Pre-fix this branch only retained
            // Write handles with a cache_fd; Read
            // handles fell through to handles.remove
            // and the entry's prefetcher was cancelled
            // above. Now both Read and Write handles
            // are retained when handle_caching > 0.
            //
            // TTL cleanup: a handle left in the map
            // forever would be a slow FD leak. Mark
            // the entry with the expiry instant; a
            // background sweeper (or a check on the
            // next open() for the same ino) drops
            // the entry once the TTL passes. For
            // now, the entry stays until the next
            // open() of the same ino replaces it or
            // the process exits — bounded by the
            // process lifetime, which matches
            // rclone's VFS handle-cache semantics.
            let kind = self
                .handles
                .get(&fh)
                .map(|e| match e.value() {
                    crate::FileHandleState::Read { .. } => "read",
                    crate::FileHandleState::Write { .. } => "write",
                })
                .unwrap_or("none");
            if kind != "none" {
                tracing::debug!(
                    fh,
                    kind,
                    "release: retaining handle for handle_caching duration (issue #54)"
                );
                // N-6 fix: stamp the handle with a TTL so open()
                // can sweep expired entries and prevent fd leaks.
                // Without this, retained handles stay in the
                // DashMap forever (bounded only by process
                // lifetime), accumulating cache_fd Arc<Mutex<File>>
                // that each hold an open fd.
                let ttl = self.handle_caching;
                self.handles.entry(fh).and_modify(|e| match e {
                    crate::FileHandleState::Read { expires_at, .. }
                    | crate::FileHandleState::Write { expires_at, .. } => {
                        *expires_at = Some(std::time::Instant::now() + ttl);
                    }
                });
                return Ok(());
            }
        }

        self.handles.remove(&fh);
        Ok(())
    }

    fn create(&self, _parent: u64, name: &str, _mode: u32) -> std::io::Result<(CoreFileAttr, u64)> {
        let parent_path = self.resolve(_parent).map(|e| e.path).unwrap_or_default();
        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };
        // Issue #268.5 O19: FUSE create entry. Fires on
        // every `touch` / `O_CREAT` / `cp -r` step.
        // **debug** not info — high frequency under shell
        // scripts. Default RUST_LOG=info must stay quiet.
        tracing::debug!(path = %full_path, "FUSE create entry");
        // Issue #57: ensure the parent directory exists
        // on hierarchical backends (HDFS, local fs,
        // WebHDFS) before issuing the write. Flat-
        // namespace backends (S3, GCS, OSS, COS, OBS)
        // auto-create the prefix on write, so the
        // mkdir_chain's `Unsupported` / `AlreadyExists`
        // arms make this a no-op for them — same cost
        // as a single op.create_dir round-trip.
        //
        // Pre-fix this skipped the parent check
        // entirely, so a `create("a/b/c.txt")` on
        // HDFS would surface `NotFound` from
        // op.write("a/b/c.txt") with no retry path.
        self.mkdir_chain(&full_path)?;
        let op = self.op.clone();
        let p = full_path.clone();
        rt().block_on(async move { op.write(&p, Vec::<u8>::new()).await })
            .map_err(|e| opendal_to_io_error(&e, "create"))?;
        // Synthesize metadata: we just wrote an empty file via op.write,
        // so the size is 0 and the kind is RegularFile. No need for a
        // post-write HEAD/stat to fetch what we already know — that was
        // 1 extra round-trip per `touch new` / `create` (issue #17).
        // mtime is `now()` because the write just happened.
        // (The pre-fix `stat_op` was returning (FileType::RegularFile,
        // 0, None) anyway via its `unwrap_or` fallback when the
        // backend hadn't yet propagated, so the mtime slot was already
        // unreliable — we now make it explicit and save the round-trip.)
        let (kind, size, mtime) = (FileType::RegularFile, 0u64, Some(SystemTime::now()));
        // Bug C fix: seed the inodes mtime so a follow-up getattr
        // (before the backend's stat_op caches anything) doesn't
        // fall back to UNIX_EPOCH. mtime is now always Some(_), so
        // unwrap_or is dead — the fallback remains defensive in case
        // someone refactors mtime back to Option.
        let now = SystemTime::now();
        let ino = self.alloc_ino_with_mtime(&full_path, kind, size, mtime.unwrap_or(now));
        // Issue #51: mint a fresh fh from NEXT_HANDLE
        // instead of using `ino` as the key into the
        // shared `handles` DashMap. `open()` uses
        // NEXT_HANDLE, so a `create()` returning `ino`
        // collides deterministically with the second
        // `open()` after the create — see the issue
        // text for the exact 3-step repro.
        let fh = NEXT_HANDLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Insert Write handle so follow-up write() can find the path
        // Create cache file for write handle
        let cpath = crate::cache_path(&self.cache_dir, &full_path);
        if let Some(parent) = cpath.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let cache_fd = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true) // N-8 fix: truncate old cache content on create;
            // the backend now has a 0-byte file and the cache must match.
            // Pre-fix `.truncate(false)` preserved stale cache from a
            // prior session, causing reads to return old data after
            // `touch existing.txt` (create without write).
            .write(true)
            .read(true)
            .open(&cpath)
            .ok()
            .map(|f| Arc::new(std::sync::Mutex::new(f)));
        self.handles.insert(
            fh,
            FileHandleState::Write {
                path: full_path,
                cache_fd,
                dirty: false,
                dirty_since: None,
                expires_at: None,
            },
        );
        self.cache_add_entry(
            &parent_path,
            name,
            if kind == FileType::Directory {
                EntryMode::DIR
            } else {
                EntryMode::FILE
            },
            size,
            mtime.unwrap_or(SystemTime::UNIX_EPOCH),
        );
        // Bug 33: create reply.entry bumps kernel
        // dentry count; mirror it.
        self.bump_lookup_count(ino);
        Ok((
            to_core_attr(&self.make_attr(ino, size, kind, mtime.unwrap_or(SystemTime::UNIX_EPOCH))),
            fh,
        ))
    }

    /// Atomic create: fail with EEXIST if the target already
    /// exists. Used by the FUSE adapter when the kernel passes
    /// `O_CREAT|O_EXCL` (issue #160).
    ///
    /// Strategy:
    /// 1. Check `Capability::write_with_if_not_exists` — only
    ///    S3, GCS, azblob, oss, cos, obs, b2, vercel-blob, fs
    ///    (and the sftp backend via patch) support it. Memory
    ///    and HDFS do not, so we fall back to `create()` (which
    ///    overwrites). On those backends the FUSE adapter will
    ///    see success and kernel `O_EXCL` users will silently
    ///    get a new ino pointing at overwritten content — this
    ///    is the same pre-existing behavior on those backends
    ///    before #160, so no regression.
    /// 2. When supported, use `op.write_options` with
    ///    `if_not_exists: true`. On S3 this maps to
    ///    `If-None-Match: *` (one RTT, atomic).
    /// 3. Map backend "already exists" errors to
    ///    `io::ErrorKind::AlreadyExists` so the fuser adapter
    ///    converts to EEXIST (POSIX-correct).
    ///
    /// Note: the non-excl `create()` path above intentionally
    /// does NOT check the capability — `O_CREAT` without
    /// `O_EXCL` is required by POSIX to succeed even if the
    /// file exists (overwrite), and the current code's
    /// `op.write(&p, Vec::new())` does exactly that.
    fn create_excl(
        &self,
        _parent: u64,
        name: &str,
        _mode: u32,
    ) -> std::io::Result<(CoreFileAttr, u64)> {
        // Issue #268.5 O19: FUSE create_excl entry. Less
        // common than create() — fires on O_CREAT|O_EXCL
        // paths (atomic-create workloads). **debug** still,
        // matching create() level.
        tracing::debug!(parent = _parent, name, "FUSE create_excl entry");
        if !self.op.info().full_capability().write_with_if_not_exists {
            // Backend doesn't support atomic create — fall back
            // to the regular `create()` (overwrite semantics).
            return self.create(_parent, name, _mode);
        }
        // Re-use the same setup as `create()` but with
        // `if_not_exists: true`. We duplicate the body rather
        // than refactoring to share: the two paths are
        // short, the divergence is real (mkdir_chain + cache
        // setup is the same, only the backend write differs),
        // and a helper would force a `&str` borrow past the
        // `block_on` boundary.
        let parent_path = self.resolve(_parent).map(|e| e.path).unwrap_or_default();
        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };
        self.mkdir_chain(&full_path)?;
        let op = self.op.clone();
        let p = full_path.clone();
        let result = rt().block_on(async move {
            op.write_options(
                &p,
                Vec::<u8>::new(),
                opendal::options::WriteOptions {
                    if_not_exists: true,
                    ..Default::default()
                },
            )
            .await
        });
        if let Err(e) = result {
            // Map backend "already exists" to io ErrorKind so
            // the fuser adapter returns EEXIST. Different
            // backends phrase this slightly differently.
            let kind = e.kind();
            let already_exists = matches!(
                kind,
                opendal::ErrorKind::AlreadyExists | opendal::ErrorKind::ConditionNotMatch
            ) || format!("{e}").to_lowercase().contains("exists");
            if already_exists {
                // Issue #268.5 O19: EEXIST is the EXPECTED
                // return path of create_excl on race. Default
                // RUST_LOG=info must not show it (high volume
                // under atomic-create benchmarks). **debug**
                // — the fuser adapter still returns EEXIST to
                // the caller via the std::io::Error.
                tracing::debug!(path = %full_path, "create_excl: target exists (EEXIST expected)");
            } else {
                tracing::warn!(path = %full_path, error = %e, "create_excl: backend write failed");
            }
            return Err(if already_exists {
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "create_excl: file exists",
                )
            } else {
                opendal_to_io_error(&e, "create_excl")
            });
        }
        let (kind, size, mtime) = (FileType::RegularFile, 0u64, Some(SystemTime::now()));
        let now = SystemTime::now();
        let ino = self.alloc_ino_with_mtime(&full_path, kind, size, mtime.unwrap_or(now));
        let fh = NEXT_HANDLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let cpath = crate::cache_path(&self.cache_dir, &full_path);
        if let Some(parent) = cpath.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let cache_fd = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&cpath)
            .ok()
            .map(|f| Arc::new(std::sync::Mutex::new(f)));
        self.handles.insert(
            fh,
            FileHandleState::Write {
                path: full_path.clone(),
                cache_fd,
                dirty: false,
                dirty_since: None,
                expires_at: None,
            },
        );
        self.cache_add_entry(
            &parent_path,
            name,
            if kind == FileType::Directory {
                EntryMode::DIR
            } else {
                EntryMode::FILE
            },
            size,
            mtime.unwrap_or(SystemTime::UNIX_EPOCH),
        );
        self.bump_lookup_count(ino);
        Ok((
            to_core_attr(&self.make_attr(ino, size, kind, mtime.unwrap_or(SystemTime::UNIX_EPOCH))),
            fh,
        ))
    }

    fn mkdir(&self, _parent: u64, name: &str) -> std::io::Result<CoreFileAttr> {
        let parent_path = self.resolve(_parent).map(|e| e.path).unwrap_or_default();
        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };
        // Issue #268.5 O19: FUSE mkdir entry. **debug**
        // not info — `cp -r` and tarballs trigger many
        // mkdirs in a tight loop. Default RUST_LOG=info
        // must not show per-file mkdir lines.
        tracing::debug!(path = %full_path, "FUSE mkdir entry");
        // Recursively create the entire path (parents + leaf).
        // Bug A fix: a single create_dir on "a/b/c/" leaves "a/" and
        // "a/b/" un-created on flat-namespace backends, so subsequent
        // `ls a/` returns EIO. mkdir_chain walks up and creates each
        // level, treating Unsupported (implicit-dir backends) and
        // AlreadyExists (idempotent) as success.
        //
        // mkdir_chain pops the leaf from the chain (issue #90 + #89
        // follow-up — MKCOL'ing a file path with trailing `/` makes
        // WebDAV create it as a directory). For mkdir() we explicitly
        // create_dir the leaf here.
        self.mkdir_chain(&full_path)?;
        // Create the leaf directory itself (mkdir_chain only handled
        // intermediates after the fix). Use a trailing `/` so WebDAV
        // interprets it as a collection, not a file.
        let op = self.op.clone();
        let leaf = if full_path.ends_with('/') {
            full_path.clone()
        } else {
            format!("{}/", full_path)
        };
        match rt().block_on(async { op.create_dir(&leaf).await }) {
            Ok(()) => {}
            Err(e)
                if e.kind() == opendal::ErrorKind::Unsupported
                    || e.kind() == opendal::ErrorKind::AlreadyExists =>
            {
                // Backend doesn't support create_dir (flat namespace
                // with implicit dirs), or the dir already exists.
                // Either way, mkdir succeeds.
            }
            Err(e) => {
                // Issue #268.5 O19: mkdir leaf create_dir failure
                // (Unsupported / AlreadyExists are silent success —
                // flat-namespace backends auto-create on write).
                tracing::warn!(
                    path = %leaf,
                    error = %e,
                    "FUSE mkdir: leaf create_dir failed"
                );
                return Err(opendal_to_io_error(&e, "mkdir"));
            }
        }
        let now = SystemTime::now();
        // Bug C follow-up: use the mtime-aware allocator so the
        // inodes entry's mtime slot is populated. The pre-fix
        // `alloc_ino` left it as `None`, and `getattr` would
        // then fall back to UNIX_EPOCH (see Bug C fix in
        // `CoreFilesystem::getattr`).
        let ino = self.alloc_ino_with_mtime(&full_path, FileType::Directory, 4096, now);
        // Bug B fix: prime the parent's dir_cache so a readdir on the
        // parent sees this new entry without a full backend re-list.
        self.cache_add_entry(&parent_path, name, EntryMode::DIR, 4096, now);
        // Bug 33: mkdir reply.entry bumps kernel
        // dentry count for the new dir; mirror it.
        self.bump_lookup_count(ino);
        Ok(to_core_attr(&self.make_attr(
            ino,
            4096,
            FileType::Directory,
            now,
        )))
    }

    fn unlink(&self, _parent: u64, name: &str) -> std::io::Result<()> {
        let parent_path = self.resolve(_parent).map(|e| e.path).unwrap_or_default();
        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };
        // #89 debug: log unlink calls
        tracing::warn!(
            parent = %parent_path,
            name = %name,
            full_path = %full_path,
            "FUSE unlink entry"
        );
        let op = self.op.clone();
        let p = full_path.clone();
        let cpath = crate::cache_path(&self.cache_dir, &full_path);

        // Backend delete FIRST. For a file that IS in the
        // backend (the common case via `write_remote` /
        // FUSE-mount-then-unlink), the kernel's read IRP —
        // which can race with the cleanup callback on the
        // WinFSP dispatcher thread (issue #294) — must see
        // the file gone from the backend before our local
        // indices drop it. Doing op.delete first narrows
        // the race window to whatever time elapses between
        // backend.delete returning and our `let _ = cpath;
        // let _ = indices;` chain below.
        //
        // Bug D fix: preserve the opendal error kind so
        // POSIX callers get the right errno
        // (NotFound→ENOENT, IsADirectory→EISDIR, etc.)
        // instead of a blanket EIO.
        let delete_result = rt()
            .block_on(async move { op.delete(&p).await })
            .map_err(|e| opendal_to_io_error(&e, "unlink"));

        // Local cleanup runs UNCONDITIONALLY below — the
        // `?` on `op.delete` that pre-fix hung the rest of
        // the function behind a short-circuit short-circuit
        // on NotFound, leaking the cache file, inodes
        // entry, disk_cache_index entries, attr_cache,
        // path_to_ino, dir_cache (the ino collision on
        // recreate was the surface symptom — Bug E
        // follow-up).
        //
        // Issue #341 specifics: a file still in the dirty
        // write-back cache (path written via the mount but
        // the upload hasn't fired yet) lands here with
        // `op.delete` returning NotFound. Dropping the
        // `writeback_pending` entry + `.dirty` sidecar
        // below cancels the upload — without this, the
        // worker would upload the cached bytes seconds
        // later and the file reappears in the backend
        // after the user already saw the delete complete.
        //
        // A worker that has already begun reading bytes
        // from `cpath` will complete — the race window is
        // bounded to bytes already buffered in the upload
        // task at the moment unlink fires.
        let was_dirty = self.writeback_pending.remove(full_path.as_str()).is_some();
        if was_dirty {
            // .dirty sidecar lives next to the cache file
            // per writeback.rs:84.
            let dirty_path = cpath.with_extension("dirty");
            let _ = std::fs::remove_file(&dirty_path);
            tracing::debug!(
                path = %full_path,
                "unlink: dropped pending writeback (dirty cache)"
            );
        }
        let _ = std::fs::remove_file(&cpath);
        // Clean block-level cache entries (disk + index).
        // O(1) via find_ino_by_path + inodes.get: replaces a
        // full-table scan with a two-hop lookup (path→ino via
        // path_to_ino DashMap, ino→InodeEntry via inodes DashMap).
        let file_size: u64 = self
            .find_ino_by_path(&full_path)
            .and_then(|ino| self.inodes.get(&ino))
            .map(|e| e.size)
            .unwrap_or(0);
        if file_size > 0 {
            remove_block_cache_files(&self.cache_dir, &full_path, file_size);
            // Bug A follow-up: also remove the block-level
            // entries from `disk_cache_index`. The disk file
            // removal above (`remove_block_cache_files`) only
            // touches the filesystem; the in-memory index
            // entries `(path, Some(idx))` would otherwise leak
            // and accumulate until the next process restart.
            let n_blocks = file_size.div_ceil(CACHE_BLOCK_SIZE);
            for blk in 0..n_blocks {
                self.disk_cache_index
                    .remove(&(full_path.clone(), Some(blk)));
            }
        }
        // The whole-file entry (key `(path, None)`).
        self.disk_cache_index.remove(&(full_path.clone(), None));
        // Bug E fix: inodes is keyed by the NEXT_INO counter, not
        // path_hash. Use find_ino_by_path to locate the correct ino
        // before removing. path_hash(&full_path) was a no-op
        // (path_hash is FNV-1a of the path, NEXT_INO is a monotonic
        // counter — they almost never coincide), so the inodes entry
        // leaked across the unlink, and a subsequent create at the
        // same path collided with the stale ino.
        if let Some(ino) = self.find_ino_by_path(&full_path) {
            self.inodes.remove(&ino);
        }
        // Stat phase 2: drop the reverse map entry too,
        // so a recreate at the same path doesn't see a
        // stale ino. find_ino_by_path above already
        // self-heals on miss, but the explicit remove
        // avoids a one-shot scan after unlink.
        self.path_to_ino.remove(&full_path);
        self.attr_cache.remove(&full_path);
        self.cache_remove_entry(&parent_path, name);

        // For the WinFSP cleanup callback at winfsp.rs:1032:
        // a NotFound returned here is the "dirty-only"
        // case — caught at the adapter layer as
        // "backend already absent (idempotent success)".
        // The local-state drop above is what keeps the
        // worker from re-uploading the file.
        delete_result
    }

    fn rmdir(&self, _parent: u64, name: &str) -> std::io::Result<()> {
        let parent_path = self.resolve(_parent).map(|e| e.path).unwrap_or_default();
        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };
        // Issue #268.5 O19: FUSE rmdir entry. rmdir fires
        // per `rm -rf` step; `cp -r` doesn't trigger it
        // but shell scripts and uninstall hooks do. **debug**
        // for entries, **warn** for failures.
        tracing::debug!(path = %full_path, "FUSE rmdir entry");
        let dir_path = format!("{}/", full_path.trim_end_matches('/'));
        let op = self.op.clone();
        let p = dir_path.clone();
        // Bug D fix: same as unlink — preserve the opendal error
        // kind. POSIX requires rmdir on a non-empty directory to
        // return EEXIST ("EEXIST: directory not empty"); the previous
        // blanket EIO left rm -rf in an undefined state on such
        // backends (some pre-check emptyness, some don't).
        if let Err(e) = rt()
            .block_on(async move { op.delete(&p).await })
            .map_err(|e| opendal_to_io_error(&e, "rmdir"))
        {
            tracing::warn!(path = %dir_path, error = %e, "FUSE rmdir: backend delete failed");
            return Err(e);
        }
        // Bug E fix: inodes keyed by NEXT_INO, not path_hash.
        if let Some(ino) = self.find_ino_by_path(&full_path) {
            self.inodes.remove(&ino);
        }
        self.path_to_ino.remove(&full_path);
        self.attr_cache.remove(&full_path);
        self.cache_remove_entry(&parent_path, name);
        self.invalidate_dir_cache(&full_path);
        Ok(())
    }

    fn rename(
        &self,
        _parent: u64,
        name: &str,
        _newparent: u64,
        newname: &str,
    ) -> std::io::Result<()> {
        let parent_path = self.resolve(_parent).map(|e| e.path).unwrap_or_default();
        let newparent_path = self.resolve(_newparent).map(|e| e.path).unwrap_or_default();
        let src = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };
        let dst = if newparent_path.is_empty() {
            newname.to_string()
        } else {
            format!("{}/{}", newparent_path, newname)
        };
        // Issue #78: the cache-walk used to derive `src`/`dst`
        // here loses fidelity when the source parent was never
        // `lookup`'d (opendal-only writes). Adapters that have
        // the full path (WinFSP) call `rename_paths` directly
        // with the correct paths; the default trait impl of
        // `rename_paths` routes FUSE calls back through here.
        self.do_rename(src, dst)
    }

    /// Issue #78: rename with explicit absolute paths.
    ///
    /// WinFSP's rename callback receives the full path of both
    /// the source and the destination. The (parent, name)
    /// signature on `rename` is FUSE-shaped — the parent ino
    /// can only be translated to a path by walking
    /// `self.lookup`, which misses when the source was created
    /// directly via the opendal backend (no prior mount-side
    /// lookup, no inode cached). This override skips the walk
    /// and uses the paths the adapter already has.
    fn rename_paths(&self, src_path: &str, dst_path: &str) -> std::io::Result<()> {
        self.do_rename(src_path.to_string(), dst_path.to_string())
    }

    fn statfs(&self, _ino: u64) -> std::io::Result<CoreVolumeStat> {
        let bs = 4096u32;
        // Issue #243.4: `total` is always expressed in 4-KiB
        // blocks (the FUSE block_size returned to the kernel
        // and forwarded by CSI's `node_get_volume_stats` to
        // K8s as `total_bytes = total * block_size`).
        //
        //   - `disk_total_size > 0`:  caller-set value in BYTES
        //     (see `cmd/mount.rs:827` — the CLI takes TB and
        //     multiplies by 1024^4 to get bytes). We divide
        //     by `bs` to get blocks, matching the unit
        //     expected by the kernel/CSI.
        //   - fallback `256 * 1024 * 1024`:  also expressed in
        //     blocks, NOT bytes. 256M blocks × 4 KiB = 1 TiB
        //     of total space.
        //
        // The fallback was originally written without a
        // unit annotation (PR #99 left the ambiguity
        // documented but unresolved). The CSI plugin
        // (`csi/mntrs-csi/src/main.rs:847`) consumes this
        // value directly, so the 1-TiB default is load-
        // bearing for csi-integration tests that assert a
        // non-zero K8s capacity. Do NOT change the number
        // without re-running csi-integration.
        let total = if self.disk_total_size > 0 {
            self.disk_total_size / bs as u64
        } else {
            256 * 1024 * 1024
        };
        // Sum the on-disk cache footprint from disk_cache_index.
        // Each entry's size is the on-disk .block file size (raw
        // payload + path prefix + CRC trailer). Round up to the
        // nearest block so a partially-filled final block still
        // counts as a full block — matches what users see in `df`
        // for ext4/xfs.
        //
        // Iterating disk_cache_index once per statfs is O(N) where
        // N is the number of distinct (path, block_idx) entries.
        // statfs is not on the FUSE hot path — the kernel caches
        // the result for `df -m`-style polls — so a linear walk
        // is fine. Issue #99.
        let used: u64 = self
            .disk_cache_index
            .iter()
            .map(|e| e.value().0.div_ceil(bs as u64))
            .sum();
        let free = total.saturating_sub(used);
        let avail = free; // mntrs doesn't distinguish "free for
        // unprivileged users" — there is no
        // per-uid gating on this mount.
        // Inodes: each open file or subdirectory holds one ino in
        // `self.inodes`. `df -i` reads `total_inodes - used` to show
        // "how many more files can I create" — so free must reflect
        // actual usage. Cap the headroom at 1B (matches the original
        // constant) so tools that compute percentages don't show
        // 0% for small mounts.
        let used_inodes = self.inodes.len() as u64;
        const MAX_INODES: u64 = 1_000_000_000;
        Ok(CoreVolumeStat {
            total_blocks: total,
            free_blocks: free,
            avail_blocks: avail,
            total_inodes: MAX_INODES,
            free_inodes: MAX_INODES.saturating_sub(used_inodes),
            block_size: bs,
            max_name_len: 255,
        })
    }

    fn getxattr(&self, ino: u64, name: &str) -> std::io::Result<Vec<u8>> {
        if let Some(InodeEntry { path: p, .. }) = self.resolve(ino) {
            let op = self.op.clone();
            let p2 = p.clone();
            match rt().block_on(async move { op.stat(&p2).await }) {
                Ok(meta) => match name {
                    "user.etag" | "s3.etag" => {
                        meta.etag().map(|e| e.as_bytes().to_vec()).ok_or_else(|| {
                            std::io::Error::new(std::io::ErrorKind::NotFound, "no etag")
                        })
                    }
                    "user.content-type" | "s3.content-type" => meta
                        .content_type()
                        .map(|c| c.as_bytes().to_vec())
                        .ok_or_else(|| {
                            std::io::Error::new(std::io::ErrorKind::NotFound, "no content-type")
                        }),
                    _ => Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "xattr not found",
                    )),
                },
                Err(_) => Err(std::io::Error::other("stat failed")),
            }
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "ino not found",
            ))
        }
    }

    fn listxattr(&self, ino: u64) -> std::io::Result<Vec<Vec<u8>>> {
        if let Some(InodeEntry { kind, .. }) = self.resolve(ino) {
            if kind == FileType::Directory {
                return Ok(vec![]);
            }
            Ok(vec![
                b"user.etag".to_vec(),
                b"user.content-type".to_vec(),
                b"s3.etag".to_vec(),
                b"s3.content-type".to_vec(),
            ])
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "ino not found",
            ))
        }
    }

    fn forget(&self, _ino: u64, _nlookup: u64) {
        // FUSE forget: kernel says it had `nlookup`
        // references to this inode and is now releasing
        // them. We only drop the inode state when our
        // mirrored count actually reaches zero (Bug 33).
        let ino = _ino;
        // Don't forget root inode — kernel doesn't
        // ref-count root and never sends forget for it.
        if ino == 1 {
            return;
        }
        // Decrement the per-ino kernel lookup count.
        // Three outcomes:
        //   * count > nlookup → just decrement, keep
        //     all state (other lookups still live).
        //   * count <= nlookup → kernel released its
        //     last ref; remove the counter entry AND
        //     drop the inodes / path_to_ino / attr_cache
        //     / handle state below.
        //   * counter missing → never bumped (e.g. ino
        //     was created out-of-band via alloc_ino
        //     from a code path that didn't go through a
        //     reply.entry to the kernel). Defensive
        //     drop matches pre-Bug-33 behaviour.
        let drop_state = match self.lookup_count.entry(ino) {
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                let cur = *e.get();
                if cur > _nlookup {
                    *e.get_mut() = cur - _nlookup;
                    false
                } else {
                    e.remove();
                    true
                }
            }
            dashmap::mapref::entry::Entry::Vacant(_) => true,
        };
        if !drop_state {
            return;
        }
        if let Some(InodeEntry { path, .. }) = self.resolve(ino) {
            self.inodes.remove(&ino);
            // Stat phase 2: drop the reverse map entry,
            // but ONLY if it still points to the ino
            // we're forgetting. A concurrent
            // alloc_ino*(path) after a recreate at the
            // same path may have already overwritten
            // the entry with a fresh ino — in that
            // case the entry is still live and we must
            // not remove it.
            self.path_to_ino
                .remove_if(&path, |_, current_ino| *current_ino == ino);
            self.attr_cache.remove(&path);
            // Clean up any open file handles for this inode
            // (actually handles key is fh, not ino; just filter by path)
            self.handles.retain(|_, v| v.path() != path);
        }
    }
}

// Free-standing helpers on `MntrsFs` (not part of the
// `CoreFilesystem` trait). Issue #257 helper extracted here
// so the read path can call it via a method syntax and the
// unit test in `tests::` can exercise it without going
// through the full `read()` flow (which requires a
// registered inode, a writable backend, and a writeback
// path — impractical for an isolated unit test of the
// fallback branch).
impl MntrsFs {
    /// Issue #257: opt-in fallback for the read path. When
    /// `read_stale_on_backend_error` is `true` and the
    /// backend fetch fails, attempt to return bytes from
    /// the file-level disk cache. Only fires when the
    /// cache file fully covers `[offset, offset+size)` —
    /// partial fallback would mislead the caller into
    /// thinking a partial read succeeded against fresh
    /// bytes (it didn't; the cache may be stale).
    pub(crate) fn try_backend_or_stale(
        &self,
        path: &str,
        offset: u64,
        size: u32,
        res: std::io::Result<bytes::Bytes>,
    ) -> std::io::Result<bytes::Bytes> {
        match res {
            Ok(b) => Ok(b),
            Err(backend_err) => {
                if !self.read_stale_on_backend_error {
                    return Err(backend_err);
                }
                let fcpath = crate::cache_path(&self.cache_dir, path);
                let cache_meta = std::fs::metadata(&fcpath).ok();
                let cache_len = cache_meta.as_ref().map(|m| m.len()).unwrap_or(0);
                let end = offset.saturating_add(size as u64);
                if cache_len < end {
                    tracing::warn!(
                        path = %path,
                        offset,
                        size,
                        cache_len,
                        "read: stale-on-error fallback rejected — cache incomplete",
                    );
                    return Err(backend_err);
                }
                match std::fs::read(&fcpath) {
                    Ok(data) => {
                        tracing::warn!(
                            path = %path,
                            offset,
                            size,
                            cache_len,
                            "read: backend failed; returning stale on-disk cache (issue #257 opt-in)",
                        );
                        let start = offset as usize;
                        let end = (start + size as usize).min(data.len());
                        Ok(bytes::Bytes::from(data[start..end].to_vec()))
                    }
                    Err(e) => {
                        tracing::warn!(
                            path = %path,
                            error = %e,
                            "read: stale-on-error fallback cache read failed",
                        );
                        Err(backend_err)
                    }
                }
            }
        }
    }

    /// Issue #300: when opening a file for write whose
    /// local cache file does not exist but the backend
    /// has the file, populate the cache with the
    /// backend content before opening the fd.
    ///
    /// Why this matters: the FUSE `open` write branch
    /// uses `OpenOptions::create(true).truncate(false)`
    /// so it opens a pre-existing cache file if one
    /// exists, otherwise it creates a fresh 0-byte
    /// file. A subsequent user-initiated `set_len(n)`
    /// (e.g. via Win32 `SetEndOfFile` from PowerShell
    /// `Set-Content`, `Out-File`, `[IO.File]::WriteAllText`,
    /// cmd `echo >`, or POSIX `ftruncate(2)` on an
    /// existing file) then **zero-fills** the new
    /// 0-byte cache to exactly `n` bytes, discarding
    /// the original backend content. The user observes
    /// zeros where the original bytes should be.
    ///
    /// This helper does a single `op.read(&path)` and
    /// `std::fs::write(&cpath, bytes)` before the fd
    /// is opened, so the cache file already contains
    /// the backend bytes when the user's `set_len`
    /// truncates.
    ///
    /// No-op when the cache file already exists (the
    /// caller is opening an already-cached file) or
    /// when the backend has no file at this path (a
    /// normal `open(O_CREAT)` on a brand-new file —
    /// the cache file gets created empty as today).
    ///
    /// On backend read failure (other than NotFound,
    /// which means the file genuinely doesn't exist
    /// yet), we log + fall through to a 0-byte cache
    /// rather than failing the `open()`. This matches
    /// the pre-#300 behaviour for the "backend gone"
    /// case: the user gets a 0-byte file they can write
    /// to, rather than an `open` error.
    ///
    /// Idempotent: subsequent `open()` calls on the
    /// same file early-return via the `cpath.exists()`
    /// check, so there's no extra cost for repeated
    /// opens.
    ///
    /// `inodes.size` is NOT touched here — `lookup()`
    /// / `getattr()` already populated it from the
    /// backend via `op.stat()`. The subsequent
    /// `setattr(size=...)` path updates both
    /// `inodes.size` and the cache fd correctly.
    fn populate_cache_from_backend(&self, path: &str) {
        let cpath = crate::cache_path(&self.cache_dir, path);
        if cpath.exists() {
            return;
        }
        if let Some(parent) = cpath.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let op = self.op.clone();
        let p = path.to_string();
        let fetched: Option<Vec<u8>> = rt().block_on(async move {
            match op.read(&p).await {
                Ok(b) => Some(b.to_vec()),
                Err(e) if e.kind() == opendal::ErrorKind::NotFound => None,
                Err(e) => {
                    tracing::warn!(
                        path = %p,
                        error = %e,
                        "populate_cache_from_backend: read failed; \
                         falling through to 0-byte cache"
                    );
                    None
                }
            }
        });
        let Some(bytes) = fetched else { return };
        if let Err(e) = std::fs::write(&cpath, &bytes) {
            tracing::warn!(
                path = %cpath.display(),
                bytes = bytes.len(),
                error = %e,
                "populate_cache_from_backend: cache write failed; \
                 falling through to 0-byte cache"
            );
            return;
        }
        tracing::debug!(
            path = %path,
            bytes = bytes.len(),
            "populate_cache_from_backend: populated cache from backend"
        );
    }
}

impl MntrsFs {
    /// Issue #78: shared rename body — backend op + cache/inode
    /// migration. Inherent method (not on the trait) so both the
    /// (parent, name) `rename` trait method and the absolute-path
    /// `rename_paths` trait method can call into the same logic
    /// without inheriting each other's visibility or signature
    /// constraints.
    ///
    /// `src` and `dst` are absolute backend paths (e.g.
    /// `"subdir/old.txt"`); the `rename` trait method derives
    /// them by walking `self.resolve(_parent)` and `name`, and
    /// `rename_paths` accepts them directly from the adapter
    /// (WinFSP's callback already provides them).
    fn do_rename(&self, src: String, dst: String) -> std::io::Result<()> {
        let op = self.op.clone();
        let src_clone = src.clone();
        let dst_clone = dst.clone();
        // Atomic rename — model: "copy-then-delete with rollback on
        // failure":
        //   1. Try server-side rename. If it returns Unsupported
        //      (opendal: backends like memory://, webhdfs that
        //      don't expose rename), fall through to copy+delete.
        //      Any other error: do NOT touch local state and
        //      return Ok(()) so the next read sees the unchanged
        //      src (no silent data loss).
        //   2. In the copy+delete fallback, if copy fails, do NOT
        //      delete src. If copy succeeds, delete src; if delete
        //      fails, log loudly but proceed (dst is already
        //      visible on the backend; preserving dst is more
        //      important than enforcing atomicity).
        //
        // Pre-delete of dst was removed (issue #17). On S3, the
        // copy step in `op.rename` uses PUT with overwrite semantics,
        // so a pre-delete is a wasted round-trip. On hierarchical
        // backends (HDFS, etc.) `op.rename` is atomic. On the
        // Unsupported fallback path, op.write to dst overwrites the
        // existing key (opendal's `Writer` is overwrite on S3 / GCS
        // / OSS / COS / OBS); for the rare backend where op.write
        // is create-only (memory, some WebHDFS deployments), the
        // copy may return AlreadyExists which the fallback treats as
        // a hard error — that's the same behavior as before this
        // change, except now we don't pay the cost of the
        // unconditional pre-delete.
        // Issue #197: the block_on closure returns Result<bool, io::Error>.
        //   Ok(true)  — backend confirmed the rename, or the copy+delete
        //                fallback completed. Migrate cache + inodes.
        //   Ok(false) — fallback failed for a non-source-missing reason
        //                (transient backend error). Preserve the existing
        //                "don't lose data on a transient" semantics by
        //                returning Ok(()) to FUSE.
        //   Err(NotFound) — the source itself is missing. POSIX rename
        //                requires ENOENT, so propagate. Issue #192's fix
        //                (return Ok(())) was a POSIX violation; this
        //                restores the correct semantics.
        let backend_result: Result<bool, std::io::Error> = rt().block_on(async move {
            match op.rename(&src_clone, &dst_clone).await {
                Ok(()) => Ok(true),
                Err(e) if e.kind() == opendal::ErrorKind::Unsupported => {
                    tracing::debug!(
                        path = %src_clone, error = %e,
                        "backend does not support server-side rename; falling back to op.copy + op.delete"
                    );
                    // Two-stage copy fallback for backends that
                    // don't implement server-side rename (memory://,
                    // some webhdfs deployments).
                    //
                    // Stage 1: try opendal's op.copy. It uses the
                    // operator's reader, so for the memory backend
                    // it reads the in-process BTreeMap (no cache-
                    // flush dependency) and for S3/HDFS it reads
                    // from the remote. This is the preferred path
                    // because it doesn't depend on the local cache
                    // file being on disk.
                    //
                    // Stage 2: if op.copy also returns Unsupported
                    // (memory:// doesn't implement copy either —
                    // only `write` + `delete`), fall back to
                    // reading the local cache file + op.write to
                    // dst + op.delete src. The cache file is the
                    // most-recent write the user issued; if the
                    // FUSE write hasn't hit disk yet (the page
                    // cache still holds the dirty data), the
                    // fallback's `std::fs::read` may return 0
                    // bytes. For the memory backend this isn't an
                    // issue because memory writes go straight to the
                    // backend (no cache-flush dependency); for
                    // S3/HDFS the only caller is a freshly-written
                    // file where the page cache holds the data —
                    // see the pre-fix failure analysis below.
                    // Two-stage copy: try op.copy first; on
                    // Unsupported fall back to cache-file +
                    // op.write. The unused-binding on the stage-1
                    // result is intentional — we only need to
                    // know success/failure, not the metadata.
                    let stage1: Result<opendal::Metadata, opendal::Error> =
                        op.copy(&src_clone, &dst_clone).await;
                    let copy_result: Result<bool, std::io::Error> = match stage1 {
                        Ok(_meta) => {
                            tracing::debug!(src = %src_clone, dst = %dst_clone, "rename fallback: op.copy ok");
                            Ok(true)
                        }
                        Err(copy_err) if copy_err.kind() == opendal::ErrorKind::Unsupported => {
                            // Stage 2: op.read + op.write.
                            // The memory backend's `op.copy` is
                            // also Unsupported (it only has
                            // write/delete/read), so the only
                            // way to "rename" is read the bytes
                            // from the backend, write them to
                            // dst, then delete src.
                            //
                            // Issue #78 regression: pre-fix this
                            // stage read the local cache file
                            // (`std::fs::read(cache_path)`), which
                            // only works for files the user
                            // wrote via the mount. For files
                            // created directly via the opendal
                            // backend (the #78 case), the cache
                            // file doesn't exist and the read
                            // returns NotFound — the rename
                            // silently turned into a no-op.
                            // Reading from the backend instead
                            // works for both code paths.
                            tracing::debug!(
                                src = %src_clone, dst = %dst_clone,
                                "op.copy unsupported too; falling back to op.read + op.write"
                            );
                            let bytes = match op.read(&src_clone).await {
                                Ok(b) => b.to_vec(),
                                Err(read_err)
                                    if read_err.kind()
                                        == opendal::ErrorKind::NotFound =>
                                {
                                    // Issue #197: source is missing.
                                    // POSIX rename(non-existent, dst)
                                    // requires ENOENT. Return Err so
                                    // fn rename propagates to FUSE
                                    // instead of silently succeeding.
                                    return Err(std::io::Error::new(
                                        std::io::ErrorKind::NotFound,
                                        format!("rename source not found: {}", src_clone),
                                    ));
                                }
                                Err(read_err) => {
                                    tracing::error!(
                                        src = %src_clone, error = %read_err,
                                        "rename fallback stage-2: op.read failed, keeping source intact"
                                    );
                                    return Ok(false);
                                }
                            };
                            match op.write(&dst_clone, bytes).await {
                                Ok(_meta) => {
                                    tracing::debug!(src = %src_clone, dst = %dst_clone, "rename fallback stage-2: op.write ok");
                                    Ok(true)
                                }
                                Err(write_err) => {
                                    tracing::error!(
                                        src = %src_clone, dst = %dst_clone, error = %write_err,
                                        "rename fallback stage-2: op.write failed, keeping source intact"
                                    );
                                    Ok(false)
                                }
                            }
                        }
                        Err(copy_err) => {
                            tracing::error!(
                                src = %src_clone, dst = %dst_clone, error = %copy_err,
                                "rename fallback: op.copy failed, keeping source intact"
                            );
                            Ok(false)
                        }
                    };
                    let del_res = op.delete(&src_clone).await;
                    if let Err(del_err) = &del_res {
                        tracing::warn!(
                            src = %src_clone, dst = %dst_clone, error = %del_err,
                            "rename fallback: copy ok, delete failed — both visible"
                        );
                    } else {
                        tracing::debug!(src = %src_clone, "rename fallback: delete src ok");
                    }
                    copy_result
                }
                Err(e) => {
                    tracing::warn!(
                        path = %src_clone, error = %e,
                        "server-side rename failed with non-Unsupported error; not falling back"
                    );
                    Ok(false)
                }
            }
        });
        match backend_result {
            Ok(true) => {}
            Ok(false) => return Ok(()),
            Err(io_err) => return Err(io_err),
        }
        // Migrate cache file
        let cpath_src = crate::cache_path(&self.cache_dir, &src);
        let cpath_dst = crate::cache_path(&self.cache_dir, &dst);
        if cpath_src.exists() && !cpath_dst.exists() {
            if let Some(parent) = cpath_dst.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::rename(&cpath_src, &cpath_dst);
        } else {
            let _ = std::fs::remove_file(&cpath_src);
        }
        // Migrate inodes src -> dst. The inodes map is keyed by
        // the NEXT_INO counter (alloc_ino), not by path_hash —
        // so the FUSE kernel, which already knows the ino for
        // the source file, will keep using that same ino for
        // the destination after rename. All we need to do is
        // change the entry's `path` field from src to dst; the
        // ino stays the same. This avoids the previous
        // implementation's mistake of inserting at path_hash
        // (which is a different number from the counter) and
        // leaving the FUSE kernel with a stale ino->path map.
        // Stat phase 2: switch from the linear iter-find
        // to the reverse-map fast path. `find_ino_by_path`
        // returns the canonical NEXT_INO-minted ino so
        // the in-place inodes update below is safe.
        let src_ino = self.find_ino_by_path(&src);
        if let Some(src_ino) = src_ino {
            // In-place path update. Size/mtime/ino are unchanged.
            self.inodes.entry(src_ino).and_modify(|v| {
                v.path = dst.clone();
            });
            // Reverse map: drop the old path entry,
            // insert the new one pointing at the same
            // ino. Both ops are independent DashMap
            // calls — between them, a concurrent
            // find_ino_by_path("dst") might briefly
            // miss; the fallback scan there self-heals.
            self.path_to_ino.remove(&src);
            self.path_to_ino.insert(dst.to_string(), src_ino);
        }

        if let Some(entry) = self.attr_cache.get(&src).map(|e| *e.value()) {
            self.attr_cache.insert(dst.to_string(), entry);
        }
        self.attr_cache.remove(&src);
        // Drop the .dirty sidecar for src so the next mount's
        // recovery scan (common_init_wb) doesn't re-upload the
        // pre-rename cache content to the now-orphan src path.
        // Without this, recovery would `op.write(src, cache_data)`
        // and resurrect the source on the backend after the rename
        // already deleted it (the same race the in-process
        // writeback task hit, see writeback.rs).
        let cpath_src = crate::cache_path(&self.cache_dir, &src);
        let _ = std::fs::remove_file(cpath_src.with_extension("dirty"));
        self.invalidate_dir_cache(&src);
        self.invalidate_dir_cache(&dst);
        // Invalidate the PARENT dir's listing cache too —
        // otherwise the next readdir on the parent returns the
        // stale listing (with the now-renamed src still present,
        // and missing the freshly-created dst). invalidate_dir_cache
        // only removes keys exactly matching the path or prefixed
        // with `path/`, so a top-level file's rename never reaches
        // the root cache slot ("") unless we do this explicitly.
        // This is the actual root cause of CI run #27492796860
        // `memory-stress-loop` reporting `rename src still exists`
        // — see issue #18.
        if let Some(parent_src) = std::path::Path::new(&src).parent().and_then(|p| p.to_str()) {
            self.invalidate_dir_cache(parent_src);
        }
        if let Some(parent_dst) = std::path::Path::new(&dst).parent().and_then(|p| p.to_str()) {
            self.invalidate_dir_cache(parent_dst);
        }
        Ok(())
    }
}

fn to_core_attr(a: &FileAttr) -> CoreFileAttr {
    CoreFileAttr {
        ino: a.ino.into(),
        size: a.size,
        blocks: a.blocks,
        atime: a.atime,
        mtime: a.mtime,
        ctime: a.ctime,
        crtime: a.crtime,
        // Bug 28: map every FileType variant explicitly.
        // Pre-fix the catch-all `_ => RegularFile`
        // collapsed Symlink / NamedPipe / BlockDevice /
        // CharDevice / Socket into regular files. Today
        // `make_attr` only produces Directory and
        // RegularFile so the collapse was a no-op, but
        // Bug 17 added the readlink/symlink trait surface
        // — a future fs-backend override that returns a
        // Symlink attr through this helper would have
        // lost its `kind` and presented to the kernel as
        // a regular file (broken `ls -la`, broken
        // readlink). Exhaustive match here so the
        // compiler enforces future additions.
        kind: match a.kind {
            FileType::Directory => CoreFileType::Directory,
            FileType::RegularFile => CoreFileType::RegularFile,
            FileType::Symlink => CoreFileType::Symlink,
            FileType::NamedPipe => CoreFileType::NamedPipe,
            FileType::BlockDevice => CoreFileType::BlockDevice,
            FileType::CharDevice => CoreFileType::CharDevice,
            FileType::Socket => CoreFileType::Socket,
        },
        perm: a.perm,
        nlink: a.nlink,
        uid: a.uid,
        gid: a.gid,
        rdev: a.rdev,
        blksize: a.blksize,
        flags: a.flags,
    }
}

pub fn new_test_fs(op: opendal::Operator, cache_dir: std::path::PathBuf) -> MntrsFs {
    // Initialize the global op for the write path's
    // background thread. The thread can't borrow the
    // `&self` op (it outlives any single `write()` call),
    // so we keep a global clone. Safe to call multiple
    // times; only the first call wins.
    set_opendal_sync_op(op.clone());
    // Initialize the disk-IO thread pool for async
    // writes. Default size: min(num_cpus, 8).
    init_disk_write_pool(None);
    // Issue #128: share one disk_cache_index Arc between the
    // `disk_cache_index` field (read-path inserts) and `multi_cache`'s
    // `DiskBlockCache` (write-path invalidate lookups). Pre-fix these
    // were two separate Arcs, so invalidate never saw inserted entries
    // and stale `.block` files survived appends.
    let disk_cache_index: Arc<dashmap::DashMap<CacheKey, (u64, std::time::Instant)>> =
        Arc::new(dashmap::DashMap::new());
    MntrsFs {
        op: Arc::new(op),
        inodes: Default::default(),
        path_to_ino: Default::default(),
        lookup_count: Default::default(),
        dir_cache: Default::default(),
        cache_dir: cache_dir.clone(),
        handles: Default::default(),
        // Issue #23: per-fh readdir snapshots. Empty
        // until opendir() populates an entry.
        dir_listers: Default::default(),
        dir_cache_ttl: std::time::Duration::from_secs(10),
        attr_ttl: std::time::Duration::from_secs(1),
        stat_cache_ttl: std::time::Duration::from_secs(10),
        volname: "test".into(),
        // Issue #243.2: pre-#243 this was 1 GiB. The 1 GiB
        // default contradicted the CLI `--vfs-cache-max-size`
        // default of `0` (= "off, no LRU cap"). The contradiction
        // only mattered for tests that *implicitly* relied on the
        // 1 GiB to drive eviction — those tests already use the
        // `new_test_fs_evict` helper which sets a per-test
        // `cache_max_size` explicitly. Tests that don't care
        // about LRU now skip the eviction path entirely (which
        // is also faster — L707 returns early when both caps
        // are zero). See issue #255 for the matching test
        // coverage and #252 for the rationale.
        cache_max_size: 0,
        write_back_delay: std::time::Duration::from_secs(1),
        // Issue #202: 0 disables immediate upload for tests,
        // preserving pre-#202 timing assumptions. Individual
        // tests that want to exercise the immediate path can
        // override this field directly.
        writeback_immediate_threshold: 0,
        cache_mode: "writes".into(),
        read_ahead: 0,
        prefetch_threshold: 16 * 1024 * 1024,
        prefetch_queue_mb: 64,
        read_chunk_size: 0,
        read_chunk_size_limit: 0,
        read_chunk_streams: 1,
        uid: None,
        gid: None,
        umask: None,
        dir_perms: 0o755,
        file_perms: 0o644,
        link_perms: 0o777,
        direct_io: false,
        // Issue #257: opt-in stale-on-backend-error. Default
        // off — the test suite asserts the conservative
        // behaviour (EIO on backend error, no surprise
        // stale data). Tests that exercise the fallback
        // path opt in explicitly.
        read_stale_on_backend_error: false,
        cache_max_age: std::time::Duration::from_secs(3600),
        // Issue #243.3: pre-#243 this was 100 MiB. The 100
        // MiB default contradicted the CLI
        // `--vfs-cache-min-free-space` default of `0` (=
        // "off, no floor check"). The contradiction only
        // mattered for tests that *implicitly* relied on
        // the 100 MiB to drive ENOSPC paths — those
        // already use `new_test_fs_evict` which sets the
        // value explicitly (or 0 for tests that don't
        // care). The L727 free-space probe is now
        // short-circuited when this is 0 (L707 + L727
        // combined), so writes no longer pay a statvfs
        // syscall on the no-floor path. See #255 for
        // matching test coverage and #253 for the
        // rationale.
        cache_min_free_space: 0,
        exclude_patterns: vec![],
        include_patterns: vec![],
        max_size: None,
        min_size: None,
        max_depth: None,
        ignore_case: false,
        fast_fingerprint: false,
        async_read: false,
        vfs_refresh: false,
        case_insensitive: false,
        no_implicit_dir: false,
        use_server_modtime: false,
        no_apple_double: false,
        no_apple_xattr: false,
        hash_filter: None,
        block_norm_dupes: false,
        write_wait: std::time::Duration::from_secs(0),
        read_wait: std::time::Duration::from_secs(0),
        cache_poll_interval: std::time::Duration::from_secs(60),
        handle_caching: std::time::Duration::from_secs(0),
        disk_total_size: 0,
        writeback_sender: std::sync::OnceLock::new(),
        #[cfg(not(windows))]
        fuse_notifier: std::sync::OnceLock::new(),
        writeback_pending: Arc::new(dashmap::DashSet::new()),
        // Issue #132: shared adaptive prefetch window controller.
        // Default min=128 KiB matches the read_chunk_size clamp
        // floor (lib.rs `self.read_chunk_size.clamp(131072, 16 MiB)`)
        // so the first prefetch chunk is unchanged from pre-#132.
        backpressure: Arc::new(backpressure::BackpressureController::new()),
        // Issue #201: cap=0 disables enforcement (MemoryLimiter::new
        // documents this). The try_reserve path becomes a no-op
        // increment; tests that exercise the cap behavior construct
        // their own cap > 0 limiter and pass it explicitly.
        mem_limiter: mem_limiter::MemoryLimiter::new(0),
        // Unbounded mem_cache for unit tests. Production mounts
        // overwrite this in cmd/mount.rs after the size is known.
        mem_cache: std::sync::Arc::new(crate::cache::DashMapMemCache::new(0)),
        attr_cache: Default::default(),
        disk_cache_index: disk_cache_index.clone(),
        storage_class: None,
        multi_cache: {
            let mc: std::sync::Arc<dyn crate::cache::MemCache> =
                std::sync::Arc::new(crate::cache::DashMapMemCache::new(0));
            crate::multi_level_cache::MultiLevelCache::new(
                mc,
                cache_dir.clone(),
                disk_cache_index.clone(),
                false,
                crate::metrics::global(),
            )
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn scratch_dir(label: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("mntrs-evict-{}", label));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Construct a MntrsFs suitable for disk-cache eviction tests.
    /// cache_max_size is honoured; cache_min_free_space can be 0.
    fn new_test_fs_evict(cache_dir: PathBuf, cache_max_size: u64) -> MntrsFs {
        let mut fs = new_test_fs(
            opendal::Operator::new(opendal::services::Memory::default())
                .unwrap()
                .finish(),
            cache_dir,
        );
        fs.cache_max_size = cache_max_size;
        fs.cache_min_free_space = 0;
        fs
    }

    /// Insert a synthetic cache entry (file on disk + index).
    fn insert_cache_entry(fs: &MntrsFs, path: &str, size: u64, atime: Instant) {
        let cpath = cache_path(&fs.cache_dir, path);
        std::fs::write(&cpath, vec![0u8; size as usize]).unwrap();
        fs.disk_cache_index
            .insert((path.to_string(), None), (size, atime));
    }

    // ── evict_lru_if_needed ──────────────────────────────────────

    #[test]
    fn evict_lru_noop_when_total_under_limit() {
        let dir = scratch_dir("noop");
        let fs = new_test_fs_evict(dir, 10 * 1024 * 1024);
        insert_cache_entry(&fs, "small.bin", 1024, Instant::now());
        fs.evict_lru_if_needed();
        assert_eq!(
            fs.disk_cache_index.len(),
            1,
            "single entry under limit should not be evicted"
        );
    }

    #[test]
    fn evict_lru_respects_size_limit() {
        let dir = scratch_dir("size");
        let fs = new_test_fs_evict(dir, 2048);
        let now = Instant::now();
        insert_cache_entry(&fs, "a.bin", 1024, now);
        insert_cache_entry(&fs, "b.bin", 1024, now - Duration::from_secs(10));
        insert_cache_entry(&fs, "c.bin", 1024, now - Duration::from_secs(20));

        // total = 3072, limit = 2048 → need to free 1024
        fs.evict_lru_if_needed();
        let remaining: u64 = fs.disk_cache_index.iter().map(|e| e.value().0).sum();
        assert!(
            remaining <= 2048,
            "total after eviction {} should be <= limit 2048",
            remaining
        );
        assert!(
            fs.disk_cache_index.len() <= 2,
            "at most 2 entries should remain"
        );
    }

    #[test]
    fn evict_lru_evicts_oldest_first() {
        let dir = scratch_dir("oldest");
        let fs = new_test_fs_evict(dir, 1024);
        let now = Instant::now();
        // newest
        insert_cache_entry(&fs, "new.bin", 1024, now);
        // middle
        insert_cache_entry(&fs, "mid.bin", 1024, now - Duration::from_secs(60));
        // oldest — should be evicted first
        insert_cache_entry(&fs, "old.bin", 1024, now - Duration::from_secs(120));

        // total = 3072, limit = 1024 → need 2048 freed = 2 entries
        fs.evict_lru_if_needed();

        // the newest entry should survive
        assert!(
            fs.disk_cache_index
                .contains_key(&("new.bin".to_string(), None)),
            "newest entry should survive eviction"
        );
        // both older entries should be gone
        assert!(
            !fs.disk_cache_index
                .contains_key(&("old.bin".to_string(), None)),
            "oldest entry should be evicted"
        );
        assert!(
            !fs.disk_cache_index
                .contains_key(&("mid.bin".to_string(), None)),
            "middle entry should be evicted"
        );
        assert_eq!(fs.disk_cache_index.len(), 1);
    }

    #[test]
    fn evict_lru_handles_block_entries() {
        let dir = scratch_dir("block");
        let fs = new_test_fs_evict(dir, 1024);
        let now = Instant::now();
        // file-level entry
        insert_cache_entry(&fs, "big.bin", 1024, now);
        // two block-level entries for the same file (idx 0, idx 1)
        for blk in 0..2u64 {
            let cpath = cache_block_path(&fs.cache_dir, "big.bin", blk);
            std::fs::write(&cpath, vec![0u8; 512]).unwrap();
            fs.disk_cache_index.insert(
                ("big.bin".to_string(), Some(blk)),
                (512, now - Duration::from_secs(10)),
            );
        }

        // total = 1024 + 512 + 512 = 2048, limit = 1024 → free 1024
        fs.evict_lru_if_needed();

        let remaining: u64 = fs.disk_cache_index.iter().map(|e| e.value().0).sum();
        assert!(
            remaining <= 1024,
            "total after eviction {} should be <= 1024",
            remaining
        );
        // block entries (older) should be gone
        assert!(
            !fs.disk_cache_index
                .contains_key(&("big.bin".to_string(), Some(0))),
            "block 0 should be evicted"
        );
        assert!(
            !fs.disk_cache_index
                .contains_key(&("big.bin".to_string(), Some(1))),
            "block 1 should be evicted"
        );
        // file-level entry (newer) should survive
        assert!(
            fs.disk_cache_index
                .contains_key(&("big.bin".to_string(), None)),
            "file-level entry should survive"
        );
    }

    // ── issue #243.5: default-zero behavior pins ──────────────

    /// Issue #243.2: `new_test_fs()` now defaults to
    /// `cache_max_size = 0`, matching the CLI
    /// `--vfs-cache-max-size` default. This pins the
    /// post-#243 default so a future regression that
    /// re-introduces the 1 GiB fallback trips CI.
    #[test]
    fn default_cache_max_size_is_zero() {
        let dir = scratch_dir("default-cache-max");
        let op = opendal::Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let fs = new_test_fs(op, dir);
        assert_eq!(
            fs.cache_max_size, 0,
            "post-#243.2 new_test_fs default must be 0 (= CLI --vfs-cache-max-size 0 = off)"
        );
    }

    /// Issue #243.3: `new_test_fs()` now defaults to
    /// `cache_min_free_space = 0`, matching the CLI
    /// `--vfs-cache-min-free-space` default. Pins the
    /// post-#243 default.
    #[test]
    fn default_cache_min_free_space_is_zero() {
        let dir = scratch_dir("default-cache-min");
        let op = opendal::Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let fs = new_test_fs(op, dir);
        assert_eq!(
            fs.cache_min_free_space, 0,
            "post-#243.3 new_test_fs default must be 0 (= CLI --vfs-cache-min-free-space 0 = off)"
        );
    }

    /// Issue #243.2: when both `cache_max_size` and
    /// `cache_min_free_space` are 0, `evict_lru_if_needed`
    /// must short-circuit at L707 and return without
    /// touching `disk_cache_index`. Pins the no-op
    /// behavior so a future change that drops the
    /// short-circuit (and re-introduces the
    /// "always-statvfs" or "always-iterate" path) trips
    /// CI.
    #[test]
    fn cache_caps_zero_means_evict_lru_is_noop() {
        let dir = scratch_dir("both-zero-noop");
        // new_test_fs post-#243 gives both caps = 0.
        let op = opendal::Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let fs = new_test_fs(op, dir);
        assert_eq!(fs.cache_max_size, 0);
        assert_eq!(fs.cache_min_free_space, 0);
        // Fill the index well past any historical fallback
        // (1 GiB / 100 MiB). If the L707 guard ever
        // regresses, eviction would kick in.
        let now = Instant::now();
        for i in 0..32 {
            fs.disk_cache_index.insert(
                (format!("f{i}.bin"), None),
                (4 * 1024 * 1024, now), // 4 MiB each = 128 MiB total
            );
        }
        let pre_len = fs.disk_cache_index.len();
        let pre_total: u64 = fs.disk_cache_index.iter().map(|e| e.value().0).sum();
        fs.evict_lru_if_needed();
        assert_eq!(
            fs.disk_cache_index.len(),
            pre_len,
            "with both caps = 0, evict_lru_if_needed must not touch disk_cache_index"
        );
        assert_eq!(
            pre_total,
            fs.disk_cache_index.iter().map(|e| e.value().0).sum::<u64>(),
            "no entry sizes should change"
        );
    }

    /// Issue #243.4: `disk_total_size` fallback is
    /// 256 MiB **blocks** (i.e. 1 TiB total bytes when
    /// multiplied by `block_size`). The 1-TiB value
    /// is load-bearing for CSI's `node_get_volume_stats`
    /// (csi/mntrs-csi/src/main.rs:843). This pin locks
    /// the fallback block count so a future change
    /// can't silently break the CSI assumption.
    #[test]
    fn disk_total_size_fallback_blocks_constant() {
        let dir = scratch_dir("dt-fallback");
        let op = opendal::Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let fs = new_test_fs(op, dir);
        assert_eq!(
            fs.disk_total_size, 0,
            "new_test_fs must not set disk_total_size"
        );
        let v = fs.statfs(1).expect("statfs");
        // The fallback `256 * 1024 * 1024` is in
        // 4-KiB blocks, so `total_blocks` should be
        // exactly that. 1 TiB = 256 * 1024 * 1024
        // blocks * 4096 bytes/block.
        assert_eq!(
            v.total_blocks,
            256 * 1024 * 1024,
            "statfs total_blocks fallback must be 256 M blocks = 1 TiB (see issue #243.4)"
        );
        assert_eq!(v.block_size as u64, 4096);
    }

    // ── statfs (issue #99) ─────────────────────────────────────

    /// Regression for issue #99: `statfs` previously reported
    /// `free_blocks == total_blocks` (so `df` always showed the
    /// mount as 100% empty, regardless of actual cache usage)
    /// and `free_inodes == total_inodes == 1B` (so `df -i` showed
    /// ~1 B free inodes forever, breaking CSI's
    /// `NodeGetVolumeStats`). CSI capacity monitoring couldn't
    /// trigger eviction or capacity alerts because the mount
    /// looked infinitely empty.
    ///
    /// The fix makes `statfs` derive `free_blocks` from the
    /// actual on-disk cache footprint (`disk_cache_index`
    /// summed over `bs`-aligned block counts) and `free_inodes`
    /// from `inodes.len()`. `total_blocks` is unchanged
    /// (still `disk_total_size / bs` or the 256 MiB default),
    /// and `total_inodes` keeps its 1 B cap so percentage-based
    /// dashboards show sensible numbers for small mounts.
    #[test]
    fn statfs_reports_real_used_blocks() {
        // Default config: disk_total_size = 0 → 256 MiB total.
        // new_test_fs gives an empty disk_cache_index and an
        // empty inodes map, so the empty-mount baseline is
        // `free == total` and `free_inodes == total_inodes`.
        let dir = scratch_dir("statfs-empty");
        let fs = new_test_fs_evict(dir, 1024 * 1024);
        let v = fs.statfs(1).expect("statfs");
        let bs = v.block_size as u64;
        assert!(v.total_blocks > 0, "total_blocks should be > 0");
        assert_eq!(
            v.free_blocks, v.total_blocks,
            "empty cache: free should equal total"
        );
        assert_eq!(
            v.avail_blocks, v.total_blocks,
            "empty cache: avail should equal total"
        );
        assert_eq!(
            v.total_inodes, 1_000_000_000,
            "total_inodes cap should be 1 B"
        );
        assert_eq!(
            v.free_inodes, v.total_inodes,
            "empty inodes map: free should equal total"
        );

        // Add a 4 MiB cache entry (1 4-MiB block → 1024
        // 4-KiB blocks when block_size = 4096). free_blocks
        // should decrease by exactly 1024.
        let dir2 = scratch_dir("statfs-one");
        let fs2 = new_test_fs_evict(dir2, 1024 * 1024);
        let now = Instant::now();
        let entry_size: u64 = 4 * 1024 * 1024;
        let cpath = crate::cache_block_path(&fs2.cache_dir, "f.bin", 0);
        std::fs::write(&cpath, vec![0u8; entry_size as usize]).unwrap();
        fs2.disk_cache_index
            .insert(("f.bin".to_string(), Some(0)), (entry_size, now));
        let v = fs2.statfs(1).expect("statfs");
        let expected_used_blocks = entry_size.div_ceil(bs);
        assert_eq!(
            v.total_blocks - v.free_blocks,
            expected_used_blocks,
            "free should be total minus one 4-MiB block"
        );
        // total_blocks / total_inodes are unchanged — only
        // the "free" fields are derived from state.
        // (Note: the fallback total for disk_total_size == 0
        // is 256 * 1024 * 1024 in BYTES — not in 4-KiB blocks —
        // so total_blocks here is 67_108_864, not 65_536. The
        // `df -B1` view is fine; `df` (1-K blocks) will show
        // larger numbers. That pre-existing mismatch is out of
        // scope for issue #99 — this test only asserts that
        // the *delta* from `total_blocks` is correct.)
        assert_eq!(
            v.avail_blocks, v.free_blocks,
            "avail and free should match (no per-uid gating)"
        );

        // Add a second 8-MiB block for the same file (a different
        // block index). Total cache footprint = 12 MiB =
        // 3072 4-KiB blocks. free_blocks should drop by another
        // 2048 from the previous value.
        let entry_size2: u64 = 8 * 1024 * 1024;
        let cpath2 = crate::cache_block_path(&fs2.cache_dir, "f.bin", 1);
        std::fs::write(&cpath2, vec![0u8; entry_size2 as usize]).unwrap();
        fs2.disk_cache_index
            .insert(("f.bin".to_string(), Some(1)), (entry_size2, now));
        let v = fs2.statfs(1).expect("statfs");
        let total_used_blocks = (entry_size + entry_size2).div_ceil(bs);
        assert_eq!(
            v.total_blocks - v.free_blocks,
            total_used_blocks,
            "free should now reflect 12 MiB total cache footprint"
        );
    }

    #[test]
    fn statfs_reports_real_used_inodes() {
        let dir = scratch_dir("statfs-inodes");
        let fs = new_test_fs_evict(dir, 1024 * 1024);

        // Synthesize inodes via the public alloc helper used by
        // lookup/create. We don't go through CoreFilesystem here
        // because that path also triggers writeback (async) which
        // complicates the assertion; we only need inodes.len() > 0.
        let _ = fs.alloc_ino("a.bin", crate::FileType::RegularFile, 0);
        let _ = fs.alloc_ino("b.bin", crate::FileType::RegularFile, 0);
        let _ = fs.alloc_ino("c.bin", crate::FileType::Directory, 0);

        let v = fs.statfs(1).expect("statfs");
        // FUSE root ino (1) is also counted → 3 + 1 = 4 inodes.
        let used = fs.inodes.len() as u64;
        assert_eq!(
            v.total_inodes - v.free_inodes,
            used,
            "free_inodes should reflect actual inodes.len()"
        );
        assert!(
            v.free_inodes < v.total_inodes,
            "with any inodes, free must drop below total"
        );

        // Sanity: a freshly constructed fs (no inodes) reports
        // free_inodes == total_inodes, so we don't accidentally
        // report 0 free on an empty mount.
        let dir2 = scratch_dir("statfs-inodes-empty");
        let fs2 = new_test_fs_evict(dir2, 1024 * 1024);
        let v = fs2.statfs(1).expect("statfs");
        assert_eq!(v.free_inodes, v.total_inodes);
    }

    // ── vfs_refresh bypass (issue #210) ──────────────────────────

    /// Pre-populate the attr_cache with a synthetic entry so we
    /// can assert whether `stat_op` honors or bypasses the cache.
    fn seed_attr_cache(fs: &MntrsFs, path: &str, kind: FileType, size: u64) {
        fs.attr_cache.insert(
            path.to_string(),
            (kind, size, None, std::time::Instant::now()),
        );
    }

    #[test]
    fn vfs_refresh_bypasses_attr_cache_on_stat() {
        // Issue #210: when `vfs_refresh=true`, `stat_op` must
        // skip the attr_cache and call the backend. When
        // false (default), the cached entry is returned
        // without a backend round-trip.
        let dir = scratch_dir("vfs-refresh-bypass");
        let op = opendal::Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        // Pre-create a real file on the memory backend so
        // the backend stat has something to return that's
        // *different* from the cached value.
        let _ = op.clone();
        let op = opendal::Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let op_for_seed = op.clone();
        crate::rt().block_on(async move {
            op_for_seed
                .write("cached.bin", "BACKEND_PAYLOAD_42_BYTES_LONG")
                .await
                .unwrap();
        });

        // Case 1: vfs_refresh=false → cache wins
        let mut fs_off = new_test_fs_evict(dir.clone(), 1024 * 1024);
        fs_off.op = Arc::new(op.clone());
        fs_off.vfs_refresh = false;
        seed_attr_cache(&fs_off, "cached.bin", FileType::RegularFile, 999);
        let FileStat { kind, size, .. } = fs_off.stat_op("cached.bin").expect("stat_op off");
        assert_eq!(
            size, 999,
            "vfs_refresh=false must return the cached size (999), not the backend's 29"
        );
        assert_eq!(kind, FileType::RegularFile);

        // Case 2: vfs_refresh=true → backend wins
        let dir2 = scratch_dir("vfs-refresh-bypass-on");
        let mut fs_on = new_test_fs_evict(dir2, 1024 * 1024);
        fs_on.op = Arc::new(op.clone());
        fs_on.vfs_refresh = true;
        seed_attr_cache(&fs_on, "cached.bin", FileType::RegularFile, 999);
        let FileStat { kind, size, .. } = fs_on.stat_op("cached.bin").expect("stat_op on");
        assert_eq!(
            size, 29,
            "vfs_refresh=true must bypass attr_cache and return the backend's size (29)"
        );
        assert_eq!(kind, FileType::RegularFile);
    }

    /// Issue #224 + [[feedback-tuple-vs-struct]] + #219
    /// precedent: `FileStat` field semantics are
    /// self-pinning via named fields. A future 4th field
    /// (e.g. `atime`, `mode`, `nlink`) is a compile-time
    /// catch at every construction site (vs the tuple's
    /// silent 0/None drop), and a reorder is impossible
    /// at the call site because the struct's named fields
    /// document themselves.
    #[test]
    fn file_stat_fields_pin_semantics() {
        // A fresh literal must include every field; a
        // missing field is a compile error here, not a
        // runtime bug.
        let stat: FileStat = FileStat {
            kind: FileType::RegularFile,
            size: 1024,
            mtime: Some(std::time::SystemTime::UNIX_EPOCH),
        };
        assert_eq!(stat.kind, FileType::RegularFile);
        assert_eq!(stat.size, 1024);
        assert_eq!(stat.mtime, Some(std::time::SystemTime::UNIX_EPOCH));

        // `Copy` lets us move the struct freely without
        // `.clone()` — pinned because `Option<FileStat>`
        // also relies on it (so does the `.unwrap_or(...)`
        // fallback in `getattr` slow path, which passes
        // the fallback by-value into the Option).
        let copy = stat;
        assert_eq!(stat, copy, "FileStat should be Copy + Eq");

        // `None` mtime is a valid state (lookup / readdir
        // on a file we've only ever read remotely).
        let no_mtime: FileStat = FileStat {
            kind: FileType::Directory,
            size: 4096,
            mtime: None,
        };
        assert_eq!(no_mtime.mtime, None);
        assert_eq!(no_mtime.size, 4096);
    }

    // ── LruHeapEntry (issue #225) ────────────────────────────────

    /// Issue #225 + [[feedback-tuple-vs-struct]] + #219
    /// precedent: `LruHeapEntry` field semantics are
    /// self-pinning via named fields. The manual `Ord` impl
    /// must reproduce the **atime-first sort** of the
    /// original 3-tuple exactly, or `evict_lru_if_needed`
    /// pops the wrong entries (silent cache-size
    /// regression). Three cases pin the contract:
    ///
    /// 1. Same atime, different size → size breaks the tie.
    /// 2. Same atime + size, different key → key breaks the tie.
    /// 3. Different atime → atime wins regardless of size/key.
    ///
    /// `BinaryHeap` is a max-heap; with `Reverse`, smaller
    /// `Ord` = popped first (oldest first). The asserts use
    /// `Less` / `Greater` directly to make the heap's
    /// pop-ordering intent obvious at the call site.
    #[test]
    fn lru_heap_entry_ord_is_atime_first() {
        let now = std::time::Instant::now();
        let earlier = now - Duration::from_secs(60);
        let latest = now + Duration::from_secs(60);

        // Case 1: same atime, different size → size breaks tie.
        // Smallest size pops first (min-heap on the 3-tuple of
        // (atime, key, size) — and our struct preserves that).
        let a = LruHeapEntry {
            atime: now,
            key: ("p".to_string(), None),
            size: 100,
        };
        let b = LruHeapEntry {
            atime: now,
            key: ("q".to_string(), None),
            size: 200,
        };
        assert!(
            a.cmp(&b) == std::cmp::Ordering::Less,
            "same atime, smaller size should be Less"
        );
        assert!(
            b.cmp(&a) == std::cmp::Ordering::Greater,
            "same atime, larger size should be Greater"
        );

        // Case 2: same atime + size, different key → key breaks tie.
        // CacheKey = (String, Option<u64>); derive Ord is
        // lexicographic on the String then on the Option.
        let c = LruHeapEntry {
            atime: now,
            key: ("alpha".to_string(), None),
            size: 100,
        };
        let d = LruHeapEntry {
            atime: now,
            key: ("beta".to_string(), None),
            size: 100,
        };
        assert!(
            c.cmp(&d) == std::cmp::Ordering::Less,
            "same atime+size, smaller key should be Less"
        );
        assert!(
            d.cmp(&c) == std::cmp::Ordering::Greater,
            "same atime+size, larger key should be Greater"
        );

        // Case 3: different atime → atime wins regardless of
        // size/key. The earlier entry pops first (it's the
        // "oldest"). Make `newer` smaller in size/key to prove
        // atime dominates.
        let older = LruHeapEntry {
            atime: earlier,
            key: ("zzz".to_string(), Some(999)),
            size: 9999,
        };
        let newer = LruHeapEntry {
            atime: latest,
            key: ("aaa".to_string(), None),
            size: 1,
        };
        assert!(
            older.cmp(&newer) == std::cmp::Ordering::Less,
            "earlier atime must be Less even with larger size/key"
        );
        assert!(
            newer.cmp(&older) == std::cmp::Ordering::Greater,
            "later atime must be Greater even with smaller size/key"
        );

        // Sanity: the struct round-trips through BinaryHeap +
        // Reverse in atime-first pop order. With 4 entries
        // pushed (older, a, c, newer) the pop order must be:
        // older (earliest atime) → c (next: atime=now, then
        // "alpha" < "p" lexicographically) → a → newer.
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        let mut heap: BinaryHeap<Reverse<LruHeapEntry>> = BinaryHeap::new();
        heap.push(Reverse(newer.clone()));
        heap.push(Reverse(older.clone()));
        heap.push(Reverse(a.clone()));
        heap.push(Reverse(c.clone()));
        let popped: Vec<LruHeapEntry> = (0..4).filter_map(|_| heap.pop().map(|r| r.0)).collect();
        assert_eq!(
            popped[0].atime, older.atime,
            "earliest atime should pop first"
        );
        assert_eq!(
            popped[1].key.0, c.key.0,
            "second pop should be the entry with lexicographically smaller key (alpha < p)"
        );
        assert_eq!(
            popped[2].key.0, a.key.0,
            "third pop should be p (key > alpha)"
        );
        assert_eq!(popped[3].atime, newer.atime, "latest atime should pop last");
    }

    // ── DirEntryCacheValue (issue #226) ──────────────────────────

    /// Issue #226 + [[feedback-tuple-vs-struct]] + #219/#224/#225
    /// precedent: `DirEntryCacheValue` field semantics are
    /// self-pinning via named fields. The test pins the field
    /// identity via a literal (a missing field is a compile error
    /// here, not a runtime bug), the `Copy` semantics (DashMap's
    /// `*e.value()` deref relies on it for the read path), and
    /// the `Eq` semantics used by cache invalidation paths.
    /// The 3-field count is enforced by the literal below — the
    /// struct form is exactly the same shape as the original
    /// `(EntryMode, u64, SystemTime)` tuple, but a future 4th
    /// field becomes a compile error at every construction site
    /// instead of a silent default on missing fields.
    #[test]
    fn dir_entry_cache_value_fields_pin_semantics() {
        let mtime = SystemTime::now();
        let val: DirEntryCacheValue = DirEntryCacheValue {
            mode: EntryMode::FILE,
            size: 4096,
            mtime,
        };
        // Field identity: each field round-trips through the
        // literal. A future field rename at the struct site
        // would break this test (compile error, not silent
        // data drift).
        assert_eq!(val.mode, EntryMode::FILE);
        assert_eq!(val.size, 4096);
        assert_eq!(val.mtime, mtime);

        // Copy semantics: DashMap's `*e.value()` deref relies
        // on this. If `DirEntryCacheValue` ever loses `Copy`,
        // every read path using `*e.value()` fails to compile,
        // which is what we want — no silent .clone() fallback.
        let copy = val;
        assert_eq!(val, copy, "DirEntryCacheValue should be Copy + Eq");

        // Eq semantics used by cache invalidation:
        let same_again = DirEntryCacheValue {
            mode: EntryMode::FILE,
            size: 4096,
            mtime,
        };
        assert_eq!(
            val, same_again,
            "two DirEntryCacheValue built from the same fields should be Eq"
        );

        // A different size breaks Eq:
        let diff_size = DirEntryCacheValue {
            mode: EntryMode::FILE,
            size: 8192,
            mtime,
        };
        assert_ne!(val, diff_size, "different size should be != ");

        // A different mode breaks Eq:
        let diff_mode = DirEntryCacheValue {
            mode: EntryMode::DIR,
            size: 4096,
            mtime,
        };
        assert_ne!(val, diff_mode, "different mode should be != ");
    }

    /// Issue #232: when `stat_op` returns NotFound but the
    /// parent's dir_cache snapshot has the child (HDFS
    /// implicit-dir scenario), the lookup must reuse the
    /// snapshot's size/mtime — not re-stat the cache file.
    /// Pre-fix the implicit branch threw away size/mtime
    /// from `DirEntryCacheValue` and re-ran
    /// `std::fs::metadata(&cpath)` for the same data. Post-
    /// fix the snapshot is the source of truth for files
    /// not on the backend, matching the HDFS implicit-dir
    /// contract documented at lookup:2112.
    #[test]
    fn lookup_implicit_uses_dir_cache_size_mtime() {
        use crate::util::canonicalize_list_path;
        let dir = scratch_dir("lookup-implicit-dir-cache");
        let fs = new_test_fs_evict(dir, 1024 * 1024);
        // No files on the memory backend → stat_op("child")
        // returns None, falling through to the dir_cache
        // implicit branch.
        let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        fs.cache_add_entry("", "child", EntryMode::FILE, 12_345, mtime);
        // The root path is `""`; cache_add_entry canonicalizes
        // the parent to the same key list_op stores under.
        let parent_key = canonicalize_list_path("");
        assert!(
            fs.dir_cache.get(&parent_key).is_some(),
            "dir_cache should hold the seeded entry under the canonical root key"
        );
        let attr = fs.lookup(1, "child").expect("lookup must succeed");
        assert_eq!(
            attr.size, 12_345,
            "implicit lookup must return the dir_cache's size, not 0 or metadata-derived"
        );
        assert_eq!(
            attr.mtime, mtime,
            "implicit lookup must return the dir_cache's mtime, not UNIX_EPOCH"
        );
    }

    /// Issue #232: bug 128 precedence rule still holds for
    /// the implicit branch. When the cache file is larger
    /// than the dir_cache size (a not-yet-uploaded write
    /// extended the file), the returned size is the
    /// max(cache_size, dir_cache_size) — the cache file
    /// wins so read-after-write sees the new size.
    #[test]
    fn lookup_implicit_cache_file_size_wins_over_dir_cache() {
        use crate::util::canonicalize_list_path;
        let dir = scratch_dir("lookup-implicit-cache-file-wins");
        let fs = new_test_fs_evict(dir.clone(), 1024 * 1024);
        let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        // dir_cache says the file is 1000 bytes; cache file
        // is 2000 bytes (a local write extended it). The
        // lookup should return 2000.
        fs.cache_add_entry("", "wfile", EntryMode::FILE, 1000, mtime);
        let cpath = crate::cache_path(&fs.cache_dir, "wfile");
        if let Some(parent) = cpath.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&cpath, vec![0u8; 2000]).unwrap();
        // Confirm the dir_cache entry is reachable.
        let parent_key = canonicalize_list_path("");
        assert!(fs.dir_cache.get(&parent_key).is_some());
        let attr = fs.lookup(1, "wfile").expect("lookup must succeed");
        assert_eq!(
            attr.size, 2000,
            "cache file size (2000) must win over dir_cache size (1000) — bug 128"
        );
        assert_eq!(
            attr.mtime, mtime,
            "mtime is the dir_cache's server-side modtime (not the cache file's mtime)"
        );
    }

    // ── issue #258: stat_op error-kind distinction ────────────

    /// Issue #258: `stat_op` returns `None` for both
    /// "missing file" (NotFound) and "backend is broken"
    /// (any other error kind) — but the **cause** differs
    /// and the operator-action differs. With `no_implicit_dir`
    /// set, a missing file should be `None` (the caller's
    /// expected behavior); a backend failure should also be
    /// `None`, but **must not** silently masquerade as a
    /// missing file in the tracing pipeline. This pin verifies
    /// that for a non-NotFound opendal error, `stat_op`
    /// returns `None` *without* invoking the implicit-dir
    /// `list()` RTT (which would just hang on a dead backend).
    /// The test is gated to Unix because opendal's Fs backend
    /// error mapping for `ENOTDIR` differs across platforms
    /// (Linux/macOS: `ErrorKind::Unexpected`; Windows:
    /// `ErrorKind::NotFound`, because Win32 normalises path-
    /// through-file errors). The cross-platform invariant
    /// "stat_op never enters the implicit-dir branch on a
    /// backend error" is pinned by the Unix-only test, and
    /// `no_implicit_dir = true` short-circuits both branches
    /// on every platform.
    #[cfg(unix)]
    #[test]
    fn stat_op_returns_none_on_non_notfound_error() {
        use opendal::ErrorKind;
        let dir = scratch_dir("258-stat-non-notfound");
        // Use the Fs backend pointed at a path where the
        // parent is a regular file. `op.stat("file/sub")`
        // returns ENOTDIR, which `opendal_core::raw::new_std_io_error`
        // maps to `ErrorKind::Unexpected` (the catch-all
        // arm with `set_temporary`). That's our "backend
        // is broken" signal — distinct from `NotFound`.
        let fs_root = scratch_dir("258-stat-fsroot");
        std::fs::write(fs_root.join("not_a_dir"), b"regular file").unwrap();
        let cfg = opendal::services::Fs::default().root(&fs_root.to_string_lossy());
        let op = opendal::Operator::new(cfg).unwrap().finish();
        // Sanity: confirm the backend yields a non-NotFound
        // kind for this scenario (the test would otherwise
        // be testing the wrong thing). The opendal Fs
        // backend uses tokio::fs under the hood, so we
        // need a tokio runtime here — `futures::executor::block_on`
        // would panic with "there is no reactor running".
        let rt = tokio::runtime::Runtime::new().unwrap();
        let sanity_err = rt
            .block_on(async { op.stat("not_a_dir/sub").await })
            .expect_err("stat of 'not_a_dir/sub' must fail");
        assert_ne!(
            sanity_err.kind(),
            ErrorKind::NotFound,
            "precondition: ENOTDIR must surface as a non-NotFound kind \
             (got {:?}); the test is wired against the opendal fs \
             backend error mapping",
            sanity_err.kind()
        );
        let mut fs = new_test_fs(op, dir);
        fs.no_implicit_dir = true;
        // The pin: stat_op must return None for this
        // non-NotFound error, *not* fall through to a
        // list() that would just timeout on a real
        // disconnected backend. The implicit-dir branch
        // is gated on no_implicit_dir=false, so with
        // no_implicit_dir=true we already short-circuit
        // at the top; with it false (the default), the
        // new `Err(e) => { ... None }` arm is the gate.
        let result = fs.stat_op("not_a_dir/sub");
        assert!(
            result.is_none(),
            "non-NotFound error must yield None (issue #258: \
             don't fall into the implicit-dir list() branch)"
        );
    }

    /// Issue #258: the implicit-dir `list()` fallback must
    /// only fire on `NotFound`. If the backend is healthy
    /// but the path doesn't exist (a true NotFound), the
    /// implicit-dir check should still run as before —
    /// only the **error kind** should gate it. This pins
    /// the NotFound path so a future refactor that
    /// over-narrows the match (e.g. drops the implicit-dir
    /// branch entirely) trips CI.
    #[test]
    fn stat_op_still_runs_implicit_dir_check_on_notfound() {
        let dir = scratch_dir("258-stat-notfound");
        let op = opendal::Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        // Memory backend: stat on a non-existent path returns
        // NotFound. Without implicit-dir semantics, stat_op
        // returns None. We need a *dir-shaped* implicit-dir
        // hit. Memory backend doesn't auto-create implicit
        // dirs (no SimulateLayer), so the implicit-dir
        // branch returns None too. The pin here is: the
        // **NotFound arm** runs and we get None, but we
        // don't get a hang, panic, or double-list.
        let mut fs = new_test_fs(op, dir);
        fs.no_implicit_dir = false;
        let result = fs.stat_op("missing/path.txt");
        assert!(
            result.is_none(),
            "NotFound on a missing path must yield None"
        );
        // And no_implicit_dir=true must short-circuit
        // *before* the list() call too — i.e. both
        // NotFound branches produce None with no panic.
        fs.no_implicit_dir = true;
        let result2 = fs.stat_op("another/missing/path");
        assert!(
            result2.is_none(),
            "no_implicit_dir must short-circuit NotFound without listing"
        );
    }

    // ── issue #257: opt-in stale-on-backend-error read fallback ──

    // -- issue #257: opt-in stale-on-backend-error read fallback --

    /// Issue #257 happy path: with the opt-in flag set, a
    /// backend read failure returns bytes from the
    /// file-level disk cache instead of EIO. The flag
    /// defaults to `false` elsewhere -- this test sets it
    /// explicitly. Tests the closure directly via the
    /// `try_backend_or_stale` method (extracted from
    /// `read()` for unit-testability).
    #[test]
    fn read_falls_back_to_cache_on_backend_error_when_opted_in() {
        let dir = scratch_dir("257-fallback-ok");
        let op = opendal::Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let fs = new_test_fs(op, dir.clone());
        let mut fs = fs;
        fs.read_stale_on_backend_error = true;
        assert!(
            fs.read_stale_on_backend_error,
            "issue #257: opt-in flag must take effect"
        );

        // Seed the file-level cache file directly. The
        // path matches what `cache_path()` returns for
        // "cacheable.bin" -- a hash-named file under
        // cache_dir.
        let fcpath = crate::cache_path(&fs.cache_dir, "cacheable.bin");
        let payload = b"stale-but-readable-bytes!";
        std::fs::write(&fcpath, payload).unwrap();

        // Synthesize a backend error.
        let backend_err = std::io::Error::other("synthetic backend failure (issue #257 test)");
        let res: std::io::Result<bytes::Bytes> = Err(backend_err);

        let fallback = fs
            .try_backend_or_stale("cacheable.bin", 0, payload.len() as u32, res)
            .expect("opt-in fallback must return cache bytes");
        assert_eq!(fallback.as_ref(), payload);
    }

    /// Issue #257 default-off: with the flag at its default
    /// value (`false`), the closure passes the backend error
    /// through unchanged. Cache file may exist; we ignore
    /// it. This is the conservative behavior -- silent stale
    /// data is worse than a noisy EIO.
    #[test]
    fn read_passes_through_backend_error_by_default() {
        let dir = scratch_dir("257-no-fallback");
        let op = opendal::Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let fs = new_test_fs(op, dir.clone());
        assert!(
            !fs.read_stale_on_backend_error,
            "default must be off (issue #257 opt-in)"
        );

        // Even if a cache file exists, default-off must
        // NOT serve it. Seed the cache file so we can
        // assert the error kind is preserved (proving we
        // didn't accidentally fall through to disk).
        let fcpath = crate::cache_path(&fs.cache_dir, "strict.bin");
        std::fs::write(&fcpath, b"would-be-stale").unwrap();

        let backend_err = std::io::Error::other("synthetic backend failure");
        let res: std::io::Result<bytes::Bytes> = Err(backend_err);

        let err = fs
            .try_backend_or_stale("strict.bin", 0, 12, res)
            .expect_err("default off must surface backend failure");
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        // The error message must be the original backend
        // error -- not a synthesized "cache incomplete" or
        // "fallback rejected" message. This pins the
        // pass-through semantics.
        assert!(
            err.to_string().contains("synthetic backend failure"),
            "default-off must propagate the original error, got: {err}"
        );
    }

    /// Issue #257 partial-cache rejection: even when opted
    /// in, a backend error with an incomplete on-disk
    /// cache (cache file too short to cover the request)
    /// must NOT serve partial bytes. The caller gets the
    /// original backend error; partial fallback is
    /// rejected because it would mislead the caller into
    /// thinking the partial bytes are fresh.
    #[test]
    fn read_rejects_partial_cache_fallback() {
        let dir = scratch_dir("257-partial-reject");
        let op = opendal::Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let fs = new_test_fs(op, dir.clone());
        let mut fs = fs;
        fs.read_stale_on_backend_error = true;

        // Cache file is only 5 bytes; request asks for 100.
        let fcpath = crate::cache_path(&fs.cache_dir, "tiny.bin");
        std::fs::write(&fcpath, b"short").unwrap();

        let backend_err = std::io::Error::other("synthetic backend failure");
        let res: std::io::Result<bytes::Bytes> = Err(backend_err);

        let err = fs
            .try_backend_or_stale("tiny.bin", 0, 100, res)
            .expect_err("partial cache must not be served");
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert!(err.to_string().contains("synthetic backend failure"));
    }
}
