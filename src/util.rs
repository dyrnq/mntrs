// Extracted pure utility functions from lib.rs.
//
// This module contains stateless (or process-stable-state) helper
// functions that have no dependency on `MntrsFs`, the tokio runtime,
// or any FUSE-adapter types.  Every item is re-exported from `lib.rs`
// via `pub use util::*` so existing `crate::` paths are unaffected.

use std::path::{Path, PathBuf};

/// Issue #261.4: XDG Base Directory Specification helpers.
/// Three functions used by `mount`, `unmount`, `install` to derive
/// standard per-user paths consistently. Behavior:
/// - `data_dir()` honors `$XDG_DATA_HOME` or falls back to
///   `$HOME/.local/share`. **Errors** if HOME is unset (was
///   silently falling back to `/tmp` — the bug we're fixing).
/// - `config_dir()` honors `$XDG_CONFIG_HOME` or falls back to
///   `$HOME/.config`. Same HOME-unset error behavior.
/// - `runtime_dir()` honors `$XDG_RUNTIME_DIR` only. The XDG
///   spec requires it (typically `/run/user/<uid>`); there is
///   no HOME fallback. Errors if unset — falling back to `/tmp`
///   here would re-introduce the multi-user collision bug.
///
/// Errors return `anyhow::Result` because callers want user-
/// visible error messages, not panic-on-None.
pub fn data_dir() -> anyhow::Result<PathBuf> {
    if let Ok(v) = std::env::var("XDG_DATA_HOME")
        && !v.is_empty()
    {
        return Ok(PathBuf::from(v));
    }
    let home = std::env::var("HOME").map_err(|_| {
        anyhow::anyhow!(
            "cannot determine data directory: $XDG_DATA_HOME and $HOME are both unset. \
             Set $XDG_DATA_HOME (or $HOME) to a writable user-owned path."
        )
    })?;
    Ok(PathBuf::from(home).join(".local").join("share"))
}

/// $XDG_CONFIG_HOME or $HOME/.config. Errors if HOME unset.
pub fn config_dir() -> anyhow::Result<PathBuf> {
    if let Ok(v) = std::env::var("XDG_CONFIG_HOME")
        && !v.is_empty()
    {
        return Ok(PathBuf::from(v));
    }
    let home = std::env::var("HOME").map_err(|_| {
        anyhow::anyhow!(
            "cannot determine config directory: $XDG_CONFIG_HOME and $HOME are both unset. \
             Set $XDG_CONFIG_HOME (or $HOME) to a writable user-owned path."
        )
    })?;
    Ok(PathBuf::from(home).join(".config"))
}

/// $XDG_RUNTIME_DIR only (no HOME fallback per XDG spec).
/// Errors if unset — runtime state cannot go to /tmp (collision risk).
pub fn runtime_dir() -> anyhow::Result<PathBuf> {
    let v = std::env::var("XDG_RUNTIME_DIR").map_err(|_| {
        anyhow::anyhow!(
            "cannot determine runtime directory: $XDG_RUNTIME_DIR is unset. \
             XDG spec requires it; if running outside a desktop session, \
             set XDG_RUNTIME_DIR=/run/user/$(id -u)."
        )
    })?;
    if v.is_empty() {
        return Err(anyhow::anyhow!(
            "$XDG_RUNTIME_DIR is set to empty string — refusing to use it"
        ));
    }
    Ok(PathBuf::from(v))
}

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

/// Resolve a mountpoint to its canonical form so that
/// `record_mount` / `remove_mount` / `unmount`'s `mounts.txt`
/// cleanup all see the same string regardless of how the user
/// typed the symlinked prefix. Issue #384.
///
/// On macOS `/tmp` is a symlink to `/private/tmp`, so `/tmp/foo`
/// and `/private/tmp/foo` are byte-different but refer to the same
/// directory. Without canonicalization, `mounts.txt` can accumulate
/// stale entries after a normal mount/unmount round-trip where the
/// user passes the canonical form on mount and the symlinked form
/// on unmount (or vice versa): the recorded row is filtered out
/// by string-equality against the *other* form, the line survives,
/// and `mntrs list` shows a ghost mount that's already gone.
///
/// On Linux this is effectively a no-op for the common case — the
/// `/tmp` directory is a real directory, and `fs::canonicalize`
/// returns the input unchanged. macOS is where the symlink
/// resolution actually matters.
///
/// This helper does NOT lowercase. Linux filesystems are case-
/// sensitive (ext4/xfs/btrfs) and lowercasing would silently merge
/// genuinely-different files into the same canonical key. macOS
/// APFS case-insensitivity is a separate, larger concern; see the
/// open issue thread for that direction. This fix only resolves
/// symlinks.
///
/// Fallback: if `fs::canonicalize` fails (e.g. the mountpoint
/// directory was already deleted between mount and unmount), the
/// raw string is returned. `umount(8)` and the `mounts.txt` filter
/// will surface their own ENOENT-style diagnostic in that case.
pub(crate) fn canonicalize_mountpoint(mp: &str) -> String {
    std::fs::canonicalize(mp)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| mp.to_string())
}

/// Return `s` with any `userinfo` (the `KEY:SECRET@host` form per
/// RFC 1738) replaced by `***`. The userinfo form
/// `s3://AKIA...:wJal...@s3.amazonaws.com/bucket` is accepted by
/// `url::Url::parse` and used to leak the secret through
/// `tracing::info!(storage_url = ...)` and through opendal error
/// `Display` chains. Stripping it before any log line keeps the
/// secret out of MNTRS_DAEMON_LOG (mode 0600 — good), stderr
/// (mode 0644 — bad — captured by macOS unified log via launchd),
/// shell history, and Sentry-style log aggregators.
pub(crate) fn redact_storage_url(s: &str) -> String {
    match url::Url::parse(s) {
        Ok(u) if !u.username().is_empty() || u.password().is_some() => {
            let mut sanitized = url::Url::parse("s3://x@x/").unwrap_or_else(|_| u.clone());
            if let Some(host) = u.host_str() {
                let _ = sanitized.set_host(Some(host));
            }
            sanitized.set_path(u.path());
            sanitized.set_username("***").ok();
            sanitized.set_password(None).ok();
            sanitized.to_string()
        }
        _ => s.to_string(),
    }
}

// ── test-only env-mutex ─────────────────────────────────────────
//
// Tests in `util::tests` and `cmd::mount::tests_261_2` both mutate
// process-wide env vars (`HOME`, `XDG_DATA_HOME`, ...) via
// `std::env::set_var` / `remove_var`. Cargo runs tests in parallel,
// so two env-mutating tests can race on the same var. We serialize
// them through a single shared mutex so the env state one test sees
// is the env state the next test observes.
//
// Issue #289 closed the in-`util` race; Issue #384 extended the
// requirement to the record/remove roundtrip tests in `mount.rs`
// which also touch `HOME`. The same mutex is shared.
//
// The `cfg(test)` gate keeps this out of release builds.
#[cfg(test)]
pub(crate) static TESTS_ENV_MUTEX: std::sync::OnceLock<std::sync::Mutex<()>> =
    std::sync::OnceLock::new();

/// Acquire the shared test env-mutex. Callers MUST drop the guard
/// before returning from the test body — holding it across an
/// assertion will deadlock the rest of the suite.
#[cfg(test)]
pub(crate) fn tests_env_mutex() -> std::sync::MutexGuard<'static, ()> {
    TESTS_ENV_MUTEX
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

// ── Unicode NFC normalization (Issue #307) ─────────────────────────

/// Canonicalize a file-name to NFC (Unicode precomposed form).
/// Idempotent: NFC of NFC is NFC. Cheap on pure-ASCII input
/// (the `unicode-normalization` crate's fast path skips the
/// per-char pass when the input contains no combining marks).
///
/// Why: macOS HFS+/APFS uses NFD (decomposed: `e` + U+0301
/// combining acute) while Windows / Linux use NFC (precomposed:
/// `é` U+00E9). Object stores preserve whatever the uploader
/// sent, so cross-OS access (`macOS FUSE → S3 → WinFSP mount`
/// or vice versa) misses the backend lookup if the adapter
/// doesn't normalize to a canonical form. We pick NFC because
/// it's what NTFS stores internally and what Linux's VFS layer
/// hands to FUSE.
///
/// Scope: called from `core_fs::winfsp.rs` and `core_fs::fuser.rs`
/// at every callback that decodes a kernel-supplied `file_name`.
/// This ensures the trait methods always see NFC names; the
/// existing `block_norm_dupes`-side NFC usage in `list_op` then
/// has consistent keys to dedup against.
pub fn nfc(s: &str) -> String {
    use unicode_normalization::UnicodeNormalization;
    s.nfc().collect()
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
            // Issue #261.4: prefer $XDG_RUNTIME_DIR (XDG spec) or
            // $TMPDIR (BSD/macOS convention), fall back to
            // std::env::temp_dir() (Rust stdlib — typically
            // /tmp on Linux but respects TMPDIR override).
            // No more bare /tmp hardcode that would collide across
            // users/pods and disappear on tmpfs reboot.
            let default_dir = std::env::var("XDG_RUNTIME_DIR")
                .ok()
                .or_else(|| std::env::var("TMPDIR").ok())
                .map(std::path::PathBuf::from)
                .unwrap_or_else(std::env::temp_dir);
            let default_path = default_dir.join(format!("mntrs-panic.{}.log", std::process::id()));
            let _ = std::fs::write(&default_path, &report);
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

#[cfg(test)]
mod redact_tests {
    use super::*;
    #[test]
    fn plain_url_unchanged() {
        assert_eq!(redact_storage_url("s3://bucket/path"), "s3://bucket/path");
    }
    #[test]
    fn userinfo_replaced() {
        let r = redact_storage_url("s3://AKIA:secret@s3.amazonaws.com/bucket");
        assert!(!r.contains("AKIA"));
        assert!(!r.contains("secret"));
        assert!(r.contains("***"));
        assert!(r.contains("s3.amazonaws.com"));
        assert!(r.contains("bucket"));
    }
    #[test]
    fn password_only_replaced() {
        let r = redact_storage_url("s3://bucket:mysecret@host.example/path/key");
        assert!(!r.contains("mysecret"));
        assert!(r.contains("***"));
        assert!(r.contains("host.example"));
    }
    #[test]
    fn garbage_input_passes_through() {
        assert_eq!(redact_storage_url("not a url"), "not a url");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    // ── env-mutation serialization (Issue #289) ─────────────────────
    //
    // The XDG-helper tests below mutate process-wide env vars
    // (`XDG_DATA_HOME`, `XDG_CONFIG_HOME`, `XDG_RUNTIME_DIR`,
    // `HOME`). Cargo runs tests in parallel by default, so two
    // tests touching the same var can race — e.g. `data_dir_xdg_data_home_wins`
    // setting `XDG_DATA_HOME=/custom/data` and `data_dir_no_env_errors`
    // removing it concurrently. CI hit the race in run 28496095276
    // (Ok('/custom/data') leaked across the boundary). The local
    // pre-commit run usually misses the race because the parallel
    // scheduler happens not to overlap them.
    //
    // We don't want to force `--test-threads=1` (slow), so we
    // serialize the env-mutating tests via a single shared
    // `OnceLock<Mutex>` that's locked at the top of every
    // test body that calls `set_var` / `remove_var`.
    // Issue #384: this same mutex is now `pub(crate)` (see
    // `TESTS_ENV_MUTEX` above) so tests in other modules can
    // serialize against the env mutations this module's tests
    // perform. Alias here for the in-module callers — locking the
    // shared static is cheaper than re-declaring a second
    // `OnceLock`.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        super::tests_env_mutex()
    }

    // ── fnmatch ────────────────────────────────────────────────────

    #[test]
    fn fnmatch_exact() {
        assert!(fnmatch("hello.txt", "hello.txt", false));
        assert!(!fnmatch("hello.txt", "hello.tx", false));
    }

    #[test]
    fn fnmatch_star_prefix() {
        assert!(fnmatch("*.txt", "hello.txt", false));
        assert!(!fnmatch("*.txt", "hello.bin", false));
    }

    #[test]
    fn fnmatch_star_suffix() {
        assert!(fnmatch("hello.*", "hello.txt", false));
        assert!(fnmatch("hello.*", "hello.", false));
        // * is greedy — "hello.*" matches "hello.txt.bak" because
        // * consumes "txt.bak" (any sequence of characters).
        // To assert single-extension, use a different pattern.
        assert!(fnmatch("hello.*", "hello.txt.bak", false));
    }

    #[test]
    fn fnmatch_star_middle() {
        assert!(fnmatch("a*c", "abc", false));
        assert!(fnmatch("a*c", "ac", false));
        assert!(fnmatch("a*c", "axxxxxc", false));
        assert!(!fnmatch("a*c", "acb", false));
    }

    #[test]
    fn fnmatch_multi_star() {
        assert!(fnmatch("a*b*c", "axbyc", false));
        assert!(fnmatch("*a*b*", "xyzabq", false));
    }

    #[test]
    fn fnmatch_question_mark() {
        assert!(fnmatch("h?llo", "hello", false));
        assert!(fnmatch("h??lo", "hello", false));
        assert!(!fnmatch("h?llo", "hllo", false));
        assert!(!fnmatch("h?llo", "heello", false));
    }

    #[test]
    fn fnmatch_star_and_question() {
        assert!(fnmatch("*.?xt", "hello.txt", false));
        assert!(!fnmatch("*.?xt", "hello.tx", false));
    }

    #[test]
    fn fnmatch_empty_pattern() {
        assert!(fnmatch("", "", false));
        assert!(!fnmatch("", "a", false));
    }

    #[test]
    fn fnmatch_empty_name() {
        assert!(!fnmatch("a", "", false));
        assert!(fnmatch("*", "", false));
    }

    #[test]
    fn fnmatch_case_insensitive() {
        assert!(fnmatch("HELLO", "hello", true));
        assert!(fnmatch("*.TXT", "hello.txt", true));
        assert!(fnmatch("A?C", "abc", true));
        assert!(!fnmatch("A?C", "abd", true));
    }

    #[test]
    fn fnmatch_no_star_exact_required() {
        assert!(!fnmatch("abc", "abcd", false));
        assert!(!fnmatch("abc", "ab", false));
        assert!(fnmatch("abc", "abc", false));
    }

    // ── canonicalize_list_path ──────────────────────────────────────

    #[test]
    fn canonicalize_empty() {
        assert_eq!(canonicalize_list_path(""), "");
        assert_eq!(canonicalize_list_path("/"), "");
    }

    #[test]
    fn canonicalize_simple_dir() {
        assert_eq!(canonicalize_list_path("foo"), "foo/");
        assert_eq!(canonicalize_list_path("foo/"), "foo/");
    }

    #[test]
    fn canonicalize_nested_dir() {
        assert_eq!(canonicalize_list_path("foo/bar"), "foo/bar/");
        assert_eq!(canonicalize_list_path("foo/bar/"), "foo/bar/");
    }

    #[test]
    fn canonicalize_strip_leading_slash() {
        assert_eq!(canonicalize_list_path("/foo"), "foo/");
        assert_eq!(canonicalize_list_path("/foo/"), "foo/");
        assert_eq!(canonicalize_list_path("/foo/bar"), "foo/bar/");
    }

    #[test]
    fn canonicalize_collapse_double_slash() {
        assert_eq!(canonicalize_list_path("//foo//bar//"), "foo/bar/");
        assert_eq!(canonicalize_list_path("foo//bar"), "foo/bar/");
    }

    #[test]
    fn canonicalize_idempotent() {
        let a = canonicalize_list_path("foo/bar");
        let b = canonicalize_list_path(&a);
        assert_eq!(a, b);
    }

    // ── canonicalize_mountpoint (Issue #384) ────────────────────────
    //
    // On macOS `/tmp` is a symlink to `/private/tmp` — the helper
    // must collapse those forms so `record_mount` and `remove_mount`
    // agree byte-for-byte. On Linux the input is typically a real
    // directory and the helper returns the input unchanged.
    //
    // We don't test `/tmp` ↔ `/private/tmp` directly because that
    // pair is macOS-specific; instead we construct a symlink under
    // a `tempfile::tempdir()` and assert the helper resolves it.
    // That way the test passes on every unix platform and still
    // exercises the symlink-resolution branch.

    // `std::os::unix::fs::symlink` only resolves on unix. Gate the
    // whole test — the same behavior is implicitly verified by the
    // live macOS smoke test in `PR mount/umount roundtrip` (the
    // helper is exercised through `record_mount` / `remove_mount`
    // on Linux + macOS; the unit test only adds the synthetic
    // symlink case).
    #[cfg(unix)]
    #[test]
    fn canonicalize_mountpoint_resolves_symlink() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let link = dir.path().join("link");
        symlink(&real, &link).unwrap();

        let resolved = canonicalize_mountpoint(link.to_str().unwrap());
        // Both forms must point to the same canonical directory.
        assert_eq!(
            resolved,
            std::fs::canonicalize(&real)
                .unwrap()
                .to_string_lossy()
                .to_string()
        );
    }

    #[test]
    fn canonicalize_mountpoint_no_symlink_is_identity() {
        // A real directory with no symlinks in the path is
        // returned in canonical form (which on Linux is the
        // input; on macOS may differ if the path sits under a
        // symlinked prefix — we only assert the round-trip
        // agrees).
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let a = canonicalize_mountpoint(real.to_str().unwrap());
        let b = canonicalize_mountpoint(&a);
        assert_eq!(a, b);
    }

    #[test]
    fn canonicalize_mountpoint_missing_path_returns_raw() {
        // fs::canonicalize on a non-existent path returns an
        // Err. The helper must fall back to the raw string so
        // record/remove callers don't see a surprising empty
        // string or panic. The mountpoint in the issue
        // reproduction could have been deleted between mount
        // and unmount — that path must still behave
        // sensibly.
        let missing = "/this/path/definitely/does/not/exist/mntrs-test";
        let got = canonicalize_mountpoint(missing);
        assert_eq!(got, missing);
    }

    #[test]
    fn canonicalize_mountpoint_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let first = canonicalize_mountpoint(real.to_str().unwrap());
        let second = canonicalize_mountpoint(&first);
        // canonicalize(canonicalize(x)) == canonicalize(x)
        assert_eq!(first, second);
    }

    // ── opendal_to_io_error ─────────────────────────────────────────

    #[test]
    fn opendal_error_kind_mapping() {
        use opendal::ErrorKind;
        use std::io::ErrorKind as IoKind;

        let cases: &[(ErrorKind, IoKind)] = &[
            (ErrorKind::NotFound, IoKind::NotFound),
            (ErrorKind::AlreadyExists, IoKind::AlreadyExists),
            (ErrorKind::PermissionDenied, IoKind::PermissionDenied),
            (ErrorKind::IsADirectory, IoKind::IsADirectory),
            (ErrorKind::NotADirectory, IoKind::NotADirectory),
            (ErrorKind::Unsupported, IoKind::Unsupported),
            (ErrorKind::RateLimited, IoKind::WouldBlock),
        ];
        for (ek, expected_io) in cases {
            let err = opendal::Error::new(*ek, "test error");
            let io_err = opendal_to_io_error(&err, "test_op");
            assert_eq!(
                io_err.kind(),
                *expected_io,
                "{ek:?} should map to {expected_io:?}"
            );
        }
    }

    #[test]
    fn opendal_error_unknown_maps_to_other() {
        let err = opendal::Error::new(opendal::ErrorKind::Unexpected, "boom");
        let io_err = opendal_to_io_error(&err, "do_something");
        assert_eq!(io_err.kind(), std::io::ErrorKind::Other);
        assert!(io_err.to_string().contains("do_something failed"));
    }

    #[test]
    fn opendal_error_message_includes_op_name() {
        let err = opendal::Error::new(opendal::ErrorKind::NotFound, "missing file");
        let io_err = opendal_to_io_error(&err, "unlink");
        assert!(io_err.to_string().contains("unlink failed"));
    }

    // ── opendal_timestamp_to_system_time ────────────────────────────

    #[test]
    fn timestamp_modern_is_unchanged() {
        use std::time::{Duration, SystemTime};
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let clamped = opendal_timestamp_to_system_time(t);
        assert_eq!(clamped, t);
    }

    #[test]
    fn timestamp_pre_epoch_clamped() {
        use std::time::{Duration, SystemTime};
        let t = SystemTime::UNIX_EPOCH - Duration::from_secs(86400);
        let clamped = opendal_timestamp_to_system_time(t);
        assert_eq!(clamped, SystemTime::UNIX_EPOCH);
    }

    #[test]
    fn timestamp_exactly_epoch_is_ok() {
        use std::time::SystemTime;
        let clamped = opendal_timestamp_to_system_time(SystemTime::UNIX_EPOCH);
        assert_eq!(clamped, SystemTime::UNIX_EPOCH);
    }

    // ── cache_path_block / cache_block_path ─────────────────────────

    #[test]
    fn cache_path_block_zero_is_whole_file() {
        let dir = Path::new("/tmp/cache");
        let p = cache_path_block(dir, "hello.txt", 0);
        assert!(p.starts_with(dir));
        let filename = p.file_name().unwrap().to_str().unwrap();
        assert!(
            !filename.contains('_'),
            "block_index=0 should have no _ suffix, got {filename}"
        );
        assert_eq!(filename.len(), 20);
    }

    #[test]
    fn cache_path_block_nonzero_has_suffix() {
        let dir = Path::new("/tmp/cache");
        let p = cache_path_block(dir, "hello.txt", 3);
        let filename = p.file_name().unwrap().to_str().unwrap();
        assert!(
            filename.contains("_0003"),
            "block_index=3 should have _0003, got {filename}"
        );
    }

    #[test]
    fn cache_block_path_format() {
        let dir = Path::new("/tmp/cache");
        let p = cache_block_path(dir, "hello.txt", 7);
        let filename = p.file_name().unwrap().to_str().unwrap();
        assert!(filename.ends_with(".block"));
        assert!(filename.contains("_0000000007"));
    }

    #[test]
    fn cache_path_same_hash_for_same_path() {
        let dir = Path::new("/tmp/cache");
        let p1 = cache_path_block(dir, "hello.txt", 0);
        let p2 = cache_path_block(dir, "hello.txt", 0);
        assert_eq!(p1, p2);
    }

    #[test]
    fn cache_path_different_for_different_paths() {
        let dir = Path::new("/tmp/cache");
        let p1 = cache_path_block(dir, "hello.txt", 0);
        let p2 = cache_path_block(dir, "world.txt", 0);
        assert_ne!(p1, p2);
    }

    // ── bump_in_memory_atime ────────────────────────────────────────

    #[test]
    fn bump_in_memory_atime_updates_instant() {
        let idx = dashmap::DashMap::new();
        let key: CacheKey = ("test/path".to_string(), None);
        let old = std::time::Instant::now();
        idx.insert(key.clone(), (4096, old));
        std::thread::sleep(std::time::Duration::from_millis(5));
        bump_in_memory_atime(&idx, &key);
        let (_sz, new_instant) = *idx.get(&key).unwrap().value();
        assert!(new_instant > old, "bumped atime should be more recent");
    }

    #[test]
    fn bump_in_memory_atime_noop_on_missing_key() {
        let idx = dashmap::DashMap::new();
        let key: CacheKey = ("absent".to_string(), None);
        bump_in_memory_atime(&idx, &key);
        assert!(idx.is_empty());
    }

    // ── detect_cgroup_memory_limit ──────────────────────────────────

    #[test]
    fn detect_cgroup_memory_limit_returns_none_or_value() {
        let result = detect_cgroup_memory_limit();
        if let Some(val) = result {
            assert!(val > 0);
            assert!(val < u64::MAX);
        }
    }

    // ── path_hash determinism ───────────────────────────────────────

    #[test]
    fn path_hash_deterministic_within_process() {
        let h1 = path_hash("hello.txt");
        let h2 = path_hash("hello.txt");
        assert_eq!(h1, h2);
    }

    #[test]
    fn path_hash_different_for_different_paths() {
        let h1 = path_hash("hello.txt");
        let h2 = path_hash("world.txt");
        assert_ne!(h1, h2);
    }

    #[test]
    fn path_hash_returns_value_gt_1() {
        assert!(path_hash("") >= 2);
        assert!(path_hash("anything") >= 2);
    }

    #[test]
    fn path_hash_shorter_than_u64_max() {
        let h = path_hash("test");
        assert!(h < (1u64 << 63));
    }

    // ── XDG helpers (Issue #261.4) ───────────────────────────────

    /// `data_dir()` honors `$XDG_DATA_HOME` when set, even if HOME
    /// is also set (env precedence).
    #[test]
    fn data_dir_xdg_data_home_wins() {
        let _g = env_lock();
        // SAFETY: serialized via `env_lock` against other
        // env-mutating tests in this module (Issue #289).
        unsafe {
            std::env::set_var("XDG_DATA_HOME", "/custom/data");
            std::env::set_var("HOME", "/home/test");
        }
        let dir = data_dir().unwrap();
        assert_eq!(dir, PathBuf::from("/custom/data"));
        unsafe {
            std::env::remove_var("XDG_DATA_HOME");
        }
    }

    /// `data_dir()` falls back to `$HOME/.local/share` when
    /// `$XDG_DATA_HOME` is unset.
    #[test]
    fn data_dir_home_fallback() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("XDG_DATA_HOME");
            std::env::set_var("HOME", "/home/test");
        }
        let dir = data_dir().unwrap();
        assert_eq!(dir, PathBuf::from("/home/test/.local/share"));
    }

    /// `data_dir()` errors when both XDG_DATA_HOME and HOME are
    /// unset — the bug fix: was silently falling back to /tmp.
    #[test]
    fn data_dir_no_env_errors() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("XDG_DATA_HOME");
            std::env::remove_var("HOME");
        }
        let err = data_dir().unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("XDG_DATA_HOME") && msg.contains("HOME"),
            "expected error to mention both vars, got: {msg}"
        );
    }

    /// Empty XDG_DATA_HOME is treated as unset (XDG spec edge case).
    #[test]
    fn data_dir_empty_xdg_falls_back() {
        let _g = env_lock();
        unsafe {
            std::env::set_var("XDG_DATA_HOME", "");
            std::env::set_var("HOME", "/home/test");
        }
        let dir = data_dir().unwrap();
        assert_eq!(dir, PathBuf::from("/home/test/.local/share"));
        unsafe {
            std::env::remove_var("XDG_DATA_HOME");
        }
    }

    /// `config_dir()` mirrors data_dir but for XDG_CONFIG_HOME.
    #[test]
    fn config_dir_xdg_config_home_wins() {
        let _g = env_lock();
        unsafe {
            std::env::set_var("XDG_CONFIG_HOME", "/custom/config");
            std::env::set_var("HOME", "/home/test");
        }
        let dir = config_dir().unwrap();
        assert_eq!(dir, PathBuf::from("/custom/config"));
        unsafe {
            std::env::remove_var("XDG_CONFIG_HOME");
        }
    }

    #[test]
    fn config_dir_home_fallback() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("XDG_CONFIG_HOME");
            std::env::set_var("HOME", "/home/test");
        }
        let dir = config_dir().unwrap();
        assert_eq!(dir, PathBuf::from("/home/test/.config"));
    }

    /// `runtime_dir()` has NO HOME fallback per XDG spec.
    /// Errors if unset — the fix.
    #[test]
    fn runtime_dir_errors_without_xdg() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("XDG_RUNTIME_DIR");
            std::env::set_var("HOME", "/home/test");
        }
        let err = runtime_dir().unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("XDG_RUNTIME_DIR"),
            "expected error to mention XDG_RUNTIME_DIR, got: {msg}"
        );
    }

    #[test]
    fn runtime_dir_xdg_set() {
        let _g = env_lock();
        unsafe {
            std::env::set_var("XDG_RUNTIME_DIR", "/run/user/1000");
        }
        let dir = runtime_dir().unwrap();
        assert_eq!(dir, PathBuf::from("/run/user/1000"));
    }

    #[test]
    fn runtime_dir_empty_errors() {
        let _g = env_lock();
        unsafe {
            std::env::set_var("XDG_RUNTIME_DIR", "");
        }
        let err = runtime_dir().unwrap_err();
        assert!(format!("{err}").contains("empty"));
    }

    // ── nfc (Issue #307) ─────────────────────────────────────────────

    /// Pure ASCII is unchanged by NFC normalization.
    #[test]
    fn nfc_ascii_unchanged() {
        assert_eq!(nfc("hello.txt"), "hello.txt");
        assert_eq!(nfc("foo/bar/baz"), "foo/bar/baz");
    }

    /// Input already in NFC passes through unchanged.
    #[test]
    fn nfc_nfc_pass_through() {
        // "café.txt" — the source literal is NFC (precomposed é, U+00E9).
        let nfc_in = "café.txt";
        assert_eq!(nfc(nfc_in), "café.txt");
    }

    /// Input in NFD is decomposed via `.nfd()` and then re-composed
    /// by `nfc()` back to the canonical NFC form. This is the exact
    /// round-trip the WinFSP / FUSE adapters perform at every
    /// callback entry point.
    #[test]
    fn nfc_nfd_to_nfc() {
        use unicode_normalization::UnicodeNormalization as _;
        // Construct NFD explicitly: e (U+0065) + combining acute (U+0301).
        let nfd: String = "café.txt".nfd().collect();
        assert_eq!(nfd, "cafe\u{0301}.txt");
        assert_eq!(nfc(&nfd), "café.txt");
    }

    /// `nfc(nfc(x)) == nfc(x)` — the operation must be idempotent so
    /// that adapter callers don't accidentally double-normalize or
    /// pay the cost twice on already-canonical input.
    #[test]
    fn nfc_idempotent() {
        use unicode_normalization::UnicodeNormalization as _;
        let once = nfc("café.txt");
        let twice = nfc(&once);
        assert_eq!(once, twice);
        let nfd: String = "café.txt".nfd().collect();
        let once = nfc(&nfd);
        let twice = nfc(&once);
        assert_eq!(once, twice);
    }

    /// Empty input is a no-op.
    #[test]
    fn nfc_empty() {
        assert_eq!(nfc(""), "");
    }
}

// ---------------------------------------------------------------------------
// Issue #420: centralize the mutex poison-recovery idiom.
//
// `Mutex::lock()` returns `Err(PoisonError)` if a previous holder of
// the lock panicked while holding it. The default `unwrap()` panics
// the new caller, crashing FUSE workers on every subsequent read
// even though the inner state is still consistent (we never
// `panic!()` while holding these locks — we recover specifically so a
// single bad read doesn't propagate). Before this helper, the
// recovery idiom was copy-pasted at 9 sites in `prefetcher.rs` and 8
// sites in `core_fs/winfsp.rs`, with at least one audit (PR #405 /
// commit `d78dd45`) missing a site because the copy-paste drifted.
// A single trait method makes the recovery uniform and trivially
// auditable.

use std::sync::{Mutex, MutexGuard};

/// Poison-safe lock helper. Use this in place of `.lock().unwrap()`
/// on any `Mutex<T>` whose lock holders do not corrupt inner state
/// on panic (the prefetch queue and the WinFSP getattr cache both
/// satisfy this invariant — verified by reading the lock-holding
/// blocks).
///
/// `tracing::warn!` is emitted on recovery so a poisoned mutex is
/// visible in production logs (the existing copy-pasted sites used
/// the same message string; consolidating it here keeps the wire
/// format identical).
pub trait LockOrRecover<T> {
    fn lock_or_recover(&self) -> MutexGuard<'_, T>;
}

impl<T> LockOrRecover<T> for Mutex<T> {
    fn lock_or_recover(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|p| {
            tracing::warn!("mutex poisoned; recovering");
            p.into_inner()
        })
    }
}

#[cfg(test)]
mod lock_or_recover_tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn lock_or_recover_returns_guard_on_unpoisoned_mutex() {
        let m: Arc<Mutex<u64>> = Arc::new(Mutex::new(42));
        let g = m.lock_or_recover();
        assert_eq!(*g, 42);
    }

    /// Poison the mutex by panicking while holding the lock, then
    /// confirm `lock_or_recover` returns a usable guard instead of
    /// propagating the poison error. This is the regression scenario
    /// the helper exists for — the old `.lock().unwrap()` would crash
    /// here.
    #[test]
    fn lock_or_recover_survives_poison() {
        let m: Arc<Mutex<u64>> = Arc::new(Mutex::new(7));
        let m2 = m.clone();
        let _ = std::thread::spawn(move || {
            let _g = m2.lock().unwrap();
            panic!("intentional poison for test");
        })
        .join();
        // m is now poisoned. lock_or_recover must still hand back a
        // guard with the original value intact.
        let g = m.lock_or_recover();
        assert_eq!(*g, 7);
    }
}
