// Extracted pure utility functions from lib.rs.
//
// This module contains stateless (or process-stable-state) helper
// functions that have no dependency on `MntrsFs`, the tokio runtime,
// or any FUSE-adapter types.  Every item is re-exported from `lib.rs`
// via `pub use util::*` so existing `crate::` paths are unaffected.

use std::path::{Path, PathBuf};

// ── Path hashing ────────────────────────────────────────────────────

pub fn path_hash(path: &str) -> u64 {
    use std::sync::OnceLock;
    // Issue #58: a fixed, process-stable salt. Pre-fix
    // this used a per-process random salt (SystemTime
    // + ASLR'd address) which made every restart
    // produce different hash values for the same
    // path — and since disk cache file names are
    // derived from `path_hash`, all cached files
    // became unreachable after a process restart.
    // For production daemon / CSI deployments
    // (config reload, OOM-kill recovery, host
    // reboot) this turns a 100 GiB warm cache into
    // 100 GiB of cold reads against the backend.
    //
    // Collision analysis: FNV-1a has ~50% collision
    // probability at 2^32 entries. The mntrs
    // collision check (filename + content CRC) on
    // every cache hit would catch any actual
    // collision before it could serve wrong data,
    // so the worst case is a cache miss — same as
    // a restart. For a CSI mount with 100M files
    // the collision probability is < 0.001%, well
    // below the cost of a cold restart.
    //
    // Operators who want a stronger mixing (at the
    // cost of cold-cache-after-restart) can set
    // `MNTRS_PATH_HASH_SALT=random` (re-randomize
    // per restart) or `MNTRS_PATH_HASH_SALT=<u64>`
    // (explicit value). The default below is the
    // golden-ratio constant — a popular FNV-1a
    // tweak.
    static SALT: OnceLock<u64> = OnceLock::new();
    let salt = SALT.get_or_init(|| {
        std::env::var("MNTRS_PATH_HASH_SALT")
            .ok()
            .and_then(|v| {
                if v == "random" {
                    // Re-randomize via the
                    // pre-fix behaviour.
                    use std::time::SystemTime;
                    let t = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .unwrap_or(0);
                    let addr = (&t as *const _) as usize as u64;
                    Some(t ^ addr.rotate_left(17) ^ 0x9E3779B97F4A7C15)
                } else {
                    v.parse::<u64>().ok()
                }
            })
            .unwrap_or(0x9E3779B97F4A7C15) // golden ratio; stable across restarts
    });
    let mut h: u64 = 0x811c9dc5 ^ *salt;
    for b in path.bytes() {
        h = h.wrapping_mul(0x01000193) ^ b as u64;
    }
    (h & 0x7FFFFFFFFFFFFFFF).max(2)
}

// ── Glob matching ───────────────────────────────────────────────────

pub fn fnmatch(pattern: &str, name: &str, ignore_case: bool) -> bool {
    let (p, n): (Vec<char>, Vec<char>) = if ignore_case {
        (
            pattern.to_lowercase().chars().collect(),
            name.to_lowercase().chars().collect(),
        )
    } else {
        (pattern.chars().collect(), name.chars().collect())
    };
    let (pl, nl) = (p.len(), n.len());
    let mut pi = 0;
    let mut ni = 0;
    let mut star = None;
    let mut match_start = 0;
    while ni < nl {
        if pi < pl && p[pi] == '*' {
            star = Some(pi);
            match_start = ni;
            pi += 1;
        } else if pi < pl && (p[pi] == '?' || p[pi] == n[ni]) {
            pi += 1;
            ni += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            match_start += 1;
            ni = match_start;
        } else {
            return false;
        }
    }
    while pi < pl && p[pi] == '*' {
        pi += 1;
    }
    pi == pl
}

// ── Path canonicalization ───────────────────────────────────────────

/// Bug 34: canonical form for any path used as a
/// `dir_cache` key OR passed to opendal's `lister`.
///
/// Why one form: pre-fix, `list_op` stored entries
/// under `"foo/"` (with trailing slash, formatted by
/// the caller), while `cache_add_entry` (called from
/// create/mkdir) stored entries under `"foo"` (no
/// trailing slash). A subsequent `list_op("foo/")`
/// would miss the just-added entry because the keys
/// disagreed. The fix centralizes path normalization
/// in this helper so every dir_cache touch agrees on
/// the canonical shape.
///
/// Rules (idempotent):
///   * Strip leading slashes (opendal S3/GCS/etc.
///     reject leading `/`).
///   * Collapse interior `//` runs to a single `/`
///     (defensive vs caller-side string concatenation
///     that may leave double slashes).
///   * Non-empty paths always end with a single `/`.
///     (opendal listers signal "list contents of dir
///     X" via the trailing `/`; without it, some
///     backends return the entry for X itself
///     instead.)
///   * Empty input → empty output (the root of the
///     bucket / mount).
///
/// Examples:
///   * `""`           → `""`
///   * `"/"`          → `""`
///   * `"foo"`        → `"foo/"`
///   * `"foo/"`       → `"foo/"`
///   * `"/foo/"`      → `"foo/"`
///   * `"//foo//bar//"` → `"foo/bar/"`
pub(crate) fn canonicalize_list_path(raw: &str) -> String {
    let segments: Vec<&str> = raw.split('/').filter(|s| !s.is_empty()).collect();
    if segments.is_empty() {
        String::new()
    } else {
        let mut out = segments.join("/");
        out.push('/');
        out
    }
}

// ── Mkdir chain builder ─────────────────────────────────────────────

/// Build the list of intermediate-directory paths that `mkdir_chain`
/// must `op.create_dir(...)` before writing `full_path`. The leaf is
/// excluded — it's the file or directory the caller will create
/// explicitly (`op.write` for `create()`, `op.create_dir` with trailing
/// `/` for `mkdir()`).
///
/// Walking order: leaf-first. For `full_path = "a/b/c.txt"` we push
/// `"a/b/c.txt/"`, then walk up via `rfind('/')` and push `"a/b/"`,
/// then `"a/"`. After the loop the chain is
/// `["a/b/c.txt/", "a/b/", "a/"]` — leaf at index 0, topmost parent
/// at the end.
///
/// Output: the chain with the leaf removed, in top-down order so the
/// concurrent `join_all` PUTs go parent-first:
///
/// ```text
/// full_path            intermediates (return value)
/// "a/b/c.txt"          ["a/b/", "a/"]          (3 segments → 2 intermediates)
/// "a/b/c"              ["a/b/", "a/"]
/// "a/b"                ["a/b/"]                (2 segments → 1)
/// "a"                  []                       (1 segment → 0)
/// ""                   []
/// "a/b/c.txt/"         ["a/b/", "a/"]           (trailing / trimmed)
/// ```
///
/// Note that intermediates are FULL parent paths with trailing `/`,
/// not just the last segment. `build_mkdir_chain("a/b/c.txt")` returns
/// `["a/b/", "a/"]`, NOT `["b/", "a/"]`. The trailing `/` is what
/// makes WebDAV/MinIO/etc. treat the path as a collection rather than
/// a file when it's later passed to `op.create_dir`.
///
/// Issue #91 regression note: an earlier version called `chain.pop()`
/// on this output (popping the LAST element) and incorrectly left the
/// leaf in the chain. The leaf then went into `op.create_dir(...)`
/// which on Apache mod_dav MKCOL'd `/path/to/file/`, turning the file
/// into a directory. The fix is `reverse → pop → reverse`, which
/// drops the leaf (now at index 0 after the first reverse) and keeps
/// intermediates in top-down order.
///
/// `pub` so unit tests in this crate can probe the chain shape
/// directly. Integration tests in `tests/` can also reach it via
/// `mntrs::build_mkdir_chain`.
pub fn build_mkdir_chain(full_path: &str) -> Vec<String> {
    let mut chain: Vec<String> = Vec::new();
    let mut cur = full_path.trim_end_matches('/').to_string();
    while !cur.is_empty() {
        chain.push(format!("{}/", cur));
        match cur.rfind('/') {
            Some(pos) => cur.truncate(pos),
            None => cur.clear(),
        }
    }
    // Walk order was leaf-first. Reverse → leaf is now LAST. Pop it.
    // Reverse again → intermediates are back to top-down order.
    chain.reverse();
    chain.pop();
    chain.reverse();
    chain
}

// ── Cache path builders ─────────────────────────────────────────────

pub fn cache_path(cache_dir: &Path, path: &str) -> PathBuf {
    cache_path_block(cache_dir, path, 0)
}

/// Key for the disk-cache LRU index. `None` block_idx means
/// the whole-file cache (`cache_path`); `Some(idx)` is the
/// per-block cache (`cache_block_path`). The tuple is
/// `Hash + Eq` out of the box (the String and the u64 are
/// both `Hash + Eq`), so we don't need a custom newtype.
///
/// `CacheKey` is the source of truth for *what* a cache
/// entry is. The corresponding on-disk path is rebuilt
/// from the components (`cache_path` for `None`,
/// `cache_block_path` for `Some`), so the index and the
/// file system can't drift as long as both helpers
/// produce deterministic names.
pub type CacheKey = (String, Option<u64>);

/// Refresh the in-memory `last_access_instant` for a
/// cache entry. The on-disk atime is unreliable on
/// `relatime` (the Linux default since 2.6.30) and is
/// not consulted by the LRU sweeper — the sweeper sorts
/// by the in-memory `Instant` recorded here. So every
/// read-path cache hit must call this, otherwise the LRU
/// degrades to FIFO (the insert time, never bumped).
///
/// Cost: one `DashMap::entry().and_modify()` per cache
/// hit, which is a per-shard lock + a relaxed write. In
/// the hot path that's a few ns.
pub(crate) fn bump_in_memory_atime(
    index: &dashmap::DashMap<CacheKey, (u64, std::time::Instant)>,
    key: &CacheKey,
) {
    index
        .entry(key.clone())
        .and_modify(|(_sz, t)| *t = std::time::Instant::now());
}

/// Block-level cache path. block_index=0 means whole file (backward compatible).
pub fn cache_path_block(cache_dir: &Path, path: &str, block_index: u64) -> PathBuf {
    let base = format!("{:020x}", path_hash(path));
    if block_index == 0 {
        cache_dir.join(&base)
    } else {
        cache_dir.join(format!("{}_{:04x}", base, block_index))
    }
}

/// Cache file path for a specific block. Encodes block_idx for restart recovery.
pub fn cache_block_path(cache_dir: &Path, path: &str, block_idx: u64) -> PathBuf {
    cache_dir.join(format!("{:020x}_{:010x}.block", path_hash(path), block_idx))
}

// ── OpenDAL error conversion ────────────────────────────────────────

/// Map an `opendal::Error` into a `std::io::Error` with the closest
/// `io::ErrorKind`. Used by the FUSE-adapter error paths (Bug D fix)
/// and the CoreFilesystem impls.
pub fn opendal_to_io_error(e: &opendal::Error, op: &str) -> std::io::Error {
    use opendal::ErrorKind;
    use std::io::ErrorKind as IoKind;
    let kind = match e.kind() {
        ErrorKind::NotFound => IoKind::NotFound,
        ErrorKind::AlreadyExists => IoKind::AlreadyExists,
        ErrorKind::PermissionDenied => IoKind::PermissionDenied,
        ErrorKind::IsADirectory => IoKind::IsADirectory,
        ErrorKind::NotADirectory => IoKind::NotADirectory,
        ErrorKind::Unsupported => IoKind::Unsupported,
        // New: map rate limiting and auth errors explicitly
        ErrorKind::RateLimited => IoKind::WouldBlock,
        _ => IoKind::Other,
    };
    std::io::Error::new(kind, format!("{op} failed: {e}"))
}

/// Convert OpenDAL Timestamp to std::time::SystemTime, clamped to UNIX_EPOCH.
pub(crate) fn opendal_timestamp_to_system_time(
    ts: impl Into<std::time::SystemTime>,
) -> std::time::SystemTime {
    let st: std::time::SystemTime = ts.into();
    if st < std::time::UNIX_EPOCH {
        std::time::UNIX_EPOCH
    } else {
        st
    }
}

// ── Panic logger ────────────────────────────────────────────────────

/// Install a panic hook that logs to a file before crashing.
/// Useful in container/CSI environments where stderr may be lost.
pub fn install_panic_logger() {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let msg = format!("panic: {info}");
        let location = info.location().map(|l| l.to_string()).unwrap_or_default();
        let backtrace = std::backtrace::Backtrace::force_capture();
        let report = format!("{msg}\n  at {location}\n  backtrace:\n{backtrace}\n");
        // Always write to stderr
        // Also write to file
        if let Ok(path) = std::env::var("MNTRS_PANIC_LOG") {
            let _ = std::fs::write(&path, &report);
        } else {
            let default_path = format!("/tmp/mntrs-panic.{}.log", std::process::id());
            let _ = std::fs::write(default_path, &report);
        }
        prev(info);
    }));
}

// ── Cgroup memory detection ─────────────────────────────────────────

/// Detect cgroup v1 memory limit (bytes). Returns None if not in a cgroup.
/// Reads /sys/fs/cgroup/memory/memory.limit_in_bytes.
/// Falls back to /proc/self/cgroup for container-specific path.
pub fn detect_cgroup_memory_limit() -> Option<u64> {
    // Try cgroup v1 first (most common in K8s)
    let cgroup_paths = ["/sys/fs/cgroup/memory/memory.limit_in_bytes"];
    for path in &cgroup_paths {
        if let Ok(content) = std::fs::read_to_string(path) {
            let val: u64 = content.trim().parse().ok()?;
            if val > 0 && val < u64::MAX {
                return Some(val);
            }
        }
    }
    // Try cgroup v2
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let trimmed = content.trim();
        if trimmed != "max"
            && let Ok(val) = trimmed.parse::<u64>()
            && val > 0
        {
            return Some(val);
        }
    }
    None
}
