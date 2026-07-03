use anyhow::{Context, Result};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::SystemTime;

/// Row describing one `.dirty` sidecar found in a cache dir.
///
/// Issue #395 fix #1: gives operators an `ls`-like surface for pending
/// uploads they otherwise can't see. Fields:
/// - `remote`: backend path read from the sidecar (the line the daemon
///   would `op.write` to). Trimmed of trailing whitespace.
/// - `cache_path`: on-disk cache file the sidecar points at (path minus
///   the `.dirty` extension). May not exist — orphan sidecar.
pub struct DirtyRow {
    pub sidecar_path: std::path::PathBuf,
    pub remote: String,
    pub cache_path: std::path::PathBuf,
    pub cache_exists: bool,
    pub cache_size: u64,
    pub sidecar_mtime: SystemTime,
}

/// Walk `cache_dir` once and return one row per `.dirty` sidecar.
///
/// Stops on per-file IO errors (logs `eprintln` and continues) so a
/// single bad sidecar doesn't blacklist the whole dir. Empty / missing
/// dir returns `Ok(vec![])` rather than erroring — `list-dirty` is a
/// read-only diagnostic, fail open.
pub fn scan_dirty_sidecars(cache_dir: &Path) -> Result<Vec<DirtyRow>> {
    let mut rows = Vec::new();
    let entries = match fs::read_dir(cache_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(rows),
        Err(e) => {
            return Err(e).with_context(|| {
                format!("list-dirty: failed to read_dir({})", cache_dir.display())
            });
        }
    };
    for entry in entries.flatten() {
        let p = entry.path();
        // Only `.dirty` extensions count — anything else in the cache
        // dir (cache files, .block shards, attr_cache) is noise.
        if p.extension().is_none_or(|ext| ext != "dirty") {
            continue;
        }
        let cache_path = p.with_extension("");
        let (cache_exists, cache_size) = match fs::metadata(&cache_path) {
            Ok(m) => (true, m.len()),
            Err(_) => (false, 0),
        };
        let sidecar_mtime = entry
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);

        // Read the sidecar to get the remote path. If we can't, still
        // emit a row with empty remote so the operator sees the sidecar.
        let remote = read_remote_path(&p).unwrap_or_default();

        rows.push(DirtyRow {
            sidecar_path: p,
            remote,
            cache_path,
            cache_exists,
            cache_size,
            sidecar_mtime,
        });
    }
    // Stable order so repeated invocations produce diff-able output.
    rows.sort_by(|a, b| a.sidecar_path.cmp(&b.sidecar_path));
    Ok(rows)
}

fn read_remote_path(sidecar: &Path) -> Result<String> {
    let f =
        File::open(sidecar).with_context(|| format!("list-dirty: open({})", sidecar.display()))?;
    let reader = BufReader::new(f);
    let first_line = reader.lines().next();
    let first = match first_line {
        Some(Ok(s)) => s,
        Some(Err(e)) => {
            return Err(anyhow::Error::new(e))
                .with_context(|| format!("list-dirty: read({})", sidecar.display()));
        }
        None => String::new(),
    };
    Ok(first.trim().to_string())
}

/// `mntrs list-dirty <cache-dir>` main entry. Prints a human-readable
/// table for an operator to grep / cron / tail. Exit code 0 even if
/// the dir is empty — no sidecars is a healthy state.
pub fn list_dirty(cache_dir: &Path) -> Result<()> {
    let rows = scan_dirty_sidecars(cache_dir)?;

    if rows.is_empty() {
        println!("no pending .dirty sidecars in {}", cache_dir.display());
        return Ok(());
    }

    println!(
        "{} pending .dirty sidecar(s) in {}:",
        rows.len(),
        cache_dir.display()
    );
    // Fixed-width columns, like `mntrs list`. Operators can pipe to
    // grep / awk without parsing JSON. Cache-size field is right-padded
    // bytes (so it stays aligned for both "12 B" and "12345678 B").
    // Header: clippy prefers literal segments inside the format string
    // when the arg IS a literal — fold "SIDECAR" into the format string
    // and align the four variable column labels.
    println!(
        "{:<40} {:>11} {:>20} {:>5}  SIDECAR",
        "REMOTE PATH", "SIZE", "LAST-MOD", "ORPH"
    );
    println!("{}", "-".repeat(120));
    for r in &rows {
        let remote_display = if r.remote.is_empty() { "?" } else { &r.remote };
        let orphan = if r.cache_exists { "no" } else { "YES" };
        let mtime = format_system_time(r.sidecar_mtime);
        println!(
            "{:40} {:>9} B {:>20} {:>5}  {}",
            truncate(remote_display, 40),
            r.cache_size,
            mtime,
            orphan,
            r.sidecar_path.display()
        );
    }
    Ok(())
}

fn format_system_time(t: SystemTime) -> String {
    // 20-char YYYY-MM-DD HH:MM:SS UTC for column alignment.
    // We deliberately use UTC so output is locale-independent
    // (operators in global fleets can compare rows).
    use std::time::UNIX_EPOCH;
    let dur = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs();
    // Convert seconds to Y-M-D H:M:S via a simple algorithm.
    // (chrono would be cleaner but adds a dep; this is enough for ops).
    let (year, month, day, hour, min, sec) = epoch_to_ymdhms(secs);
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        year, month, day, hour, min, sec
    )
}

/// Gregorial epoch → (Y, M, D, h, m, s). Valid for 1970-2100.
/// Algorithm: Hinnant's date library (public domain reference impl).
fn epoch_to_ymdhms(secs: u64) -> (i32, u32, u32, u32, u32, u32) {
    let days = (secs / 86_400) as i64;
    let rem = (secs % 86_400) as u32;
    let hour = rem / 3600;
    let min = (rem % 3600) / 60;
    let sec = rem % 60;

    // Hinnant's days_from_civil inverse.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d, hour, min, sec)
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        // Keep first (max-1) chars + ellipsis. Byte-unsafe at non-ASCII
        // boundaries; chars() iter is safe.
        let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_basics() {
        // 0 = 1970-01-01 00:00:00
        assert_eq!(epoch_to_ymdhms(0), (1970, 1, 1, 0, 0, 0));
        // 2026-07-02 04:01:25 UTC = epoch 1782964885
        assert_eq!(epoch_to_ymdhms(1782964885), (2026, 7, 2, 4, 1, 25));
    }

    #[test]
    fn truncate_short_unchanged() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn truncate_long_clamped() {
        let s = "a".repeat(50);
        let out = truncate(&s, 10);
        assert_eq!(out.chars().count(), 10);
        assert!(out.ends_with('…'));
    }

    #[test]
    fn empty_dir_returns_no_rows() {
        let dir = std::env::temp_dir().join(format!(
            "mntrs-list-dirty-empty-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let rows = scan_dirty_sidecars(&dir).unwrap();
        assert!(rows.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_dir_returns_empty_not_err() {
        let dir = std::env::temp_dir().join(format!(
            "mntrs-list-dirty-missing-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        // Don't create the dir.
        let rows = scan_dirty_sidecars(&dir).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn sidecar_without_cache_file_is_orphan() {
        let dir = std::env::temp_dir().join(format!(
            "mntrs-list-dirty-orphan-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        // Write a sidecar whose cache file doesn't exist.
        let sidecar = dir.join("abc123.dirty");
        std::fs::write(&sidecar, "hello.txt\n").unwrap();

        let rows = scan_dirty_sidecars(&dir).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].remote, "hello.txt");
        assert!(!rows[0].cache_exists);
        assert_eq!(rows[0].cache_size, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
