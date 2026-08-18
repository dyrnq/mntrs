//! CLI default value drift guard (audit #364 pattern 3, issue #446).
//!
//! Prevents README ↔ `--help` default value drift. Audit found 3 historical
//! drifts: #102 (`--vfs-cache-mode`), #125 (`--vfs-read-wait`), #296 (WinFSP
//! test count). Pure 2-line reactive fixes; this test makes drift fail CI
//! instead of staying silent.
//!
//! Strategy (plan C from issue #446 ROI analysis):
//!   - Subprocess: `target/debug/mntrs mount --help`
//!   - File read: `README.md` flag-table rows (column 2 = CLI default)
//!   - Explicit list of 3 known-drift flags; expand as new drifts surface
//!   - No general Markdown / clap parser — pure substring + normalize
//!
//! Adding a new flag to this test: append to `FLAGS`, no parser change needed.

use std::process::Command;

/// (flag, help-extractor, readme-extractor, normalizer)
///
/// Each entry knows how to extract the default from both sources. The
/// normalizer makes `--vfs-read-wait` `1` (raw seconds in `--help`) equal
/// to `1s` (human-readable suffix in README).
struct FlagSpec {
    flag: &'static str,
    /// Return the default value as printed by `mntrs mount --help`.
    /// Return `None` if no `[default: ...]` token is present (bool flag).
    help_default: fn(&str) -> Option<String>,
    /// Return the default value from the README flag-table row.
    readme_default: fn(&str) -> String,
    /// Normalize both sides to a canonical representation.
    /// Defaults to identity (string equality).
    normalize: fn(&str) -> String,
}

/// `--vfs-cache-mode`: help prints `[default: off]`, README prints `off`.
fn help_default_after_desc(help: &str, flag: &str) -> Option<String> {
    // clap emits the flag line, then a wrapped description block (the
    // description often contains a human "(default: X)" string), then a
    // blank line, then the canonical `[default: VALUE]` form on its own
    // line. We collect lines starting from the flag line until we see
    // `[default: ...]` — and we STOP if we hit the next `--some-flag`
    // line, so each flag's default is read from its own block (the
    // `--help` order may not match the FLAGS array order).
    //
    // Edge: descriptions sometimes mention "(default: X)" mid-line — we
    // only honour the bracketed `[default: X]` form that clap itself
    // emits at the end of the help text.
    let mut collecting = false;
    for line in help.lines() {
        let trimmed = line.trim_start();
        if !collecting {
            if trimmed.starts_with(&format!("--{flag}")) || trimmed.contains(&format!("--{flag} "))
            {
                collecting = true;
            } else {
                continue;
            }
        }
        // Once we're in a flag's section, the next flag's "--xxx" line
        // (start of a new block) means we've gone too far.
        if trimmed.starts_with("--") && !trimmed.starts_with(&format!("--{flag}")) {
            return None;
        }
        if let Some(start) = trimmed.find("[default: ") {
            let rest = &trimmed[start + "[default: ".len()..];
            let v = rest.split(']').next().unwrap_or("").trim().to_string();
            if !v.is_empty() {
                return Some(v);
            }
        }
    }
    None
}

fn help_default_vfs_cache_mode(help: &str) -> Option<String> {
    help_default_after_desc(help, "vfs-cache-mode")
}

fn help_default_vfs_read_wait(help: &str) -> Option<String> {
    help_default_after_desc(help, "vfs-read-wait")
}

fn help_default_vfs_write_wait(help: &str) -> Option<String> {
    help_default_after_desc(help, "vfs-write-wait")
}

fn help_default_vfs_read_chunk_size(help: &str) -> Option<String> {
    help_default_after_desc(help, "vfs-read-chunk-size")
}

fn help_default_vfs_read_chunk_size_limit(help: &str) -> Option<String> {
    help_default_after_desc(help, "vfs-read-chunk-size-limit")
}

fn help_default_vfs_read_ahead(help: &str) -> Option<String> {
    help_default_after_desc(help, "vfs-read-ahead")
}

/// `--write-back-cache` is a bool flag; clap does not emit `[default: ...]`
/// (implicit default is `false`). Return `Some("false")` so we can compare
/// against the README value.
fn help_default_write_back_cache(help: &str) -> Option<String> {
    let _ = help;
    Some("false".to_string())
}

/// README flag-table row matcher. Returns the second column (CLI default).
fn readme_default(readme: &str, flag: &str) -> String {
    let needle = format!("`--{flag}`");
    for line in readme.lines() {
        if line.starts_with('|') && line.contains(&needle) {
            let cells: Vec<&str> = line.split('|').collect();
            // Split produces N+1 cells for N pipes; cells[0] is empty
            // (leading pipe), cells[1] is the flag, cells[2] is CLI default.
            if cells.len() >= 3 {
                return cells[2].trim().to_string();
            }
        }
    }
    panic!("flag `--{flag}` not found in README flag table");
}

fn readme_default_vfs_cache_mode(readme: &str) -> String {
    readme_default(readme, "vfs-cache-mode")
}

fn readme_default_vfs_read_wait(readme: &str) -> String {
    readme_default(readme, "vfs-read-wait")
}

fn readme_default_vfs_write_wait(readme: &str) -> String {
    readme_default(readme, "vfs-write-wait")
}

fn readme_default_vfs_read_chunk_size(readme: &str) -> String {
    readme_default(readme, "vfs-read-chunk-size")
}

fn readme_default_vfs_read_chunk_size_limit(readme: &str) -> String {
    readme_default(readme, "vfs-read-chunk-size-limit")
}

fn readme_default_vfs_read_ahead(readme: &str) -> String {
    readme_default(readme, "vfs-read-ahead")
}

fn readme_default_write_back_cache(readme: &str) -> String {
    readme_default(readme, "write-back-cache")
}

/// `--vfs-read-wait` clap stores raw seconds (`u64`); README shows `1s`.
/// Normalize `1` → `1s`, `5` → `5s`. Other values pass through unchanged
/// (so a future `Duration` type that prints `5s` still matches).
fn normalize_seconds(s: &str) -> String {
    if s.chars().all(|c| c.is_ascii_digit()) && !s.is_empty() {
        format!("{s}s")
    } else {
        s.to_string()
    }
}

/// Strip surrounding backticks (README wraps values in `` ` ``).
fn strip_backticks(s: &str) -> String {
    s.trim().trim_matches('`').to_string()
}

fn normalize_default(s: &str) -> String {
    let s = strip_backticks(s);
    // If the value is a bare number of seconds, add the unit.
    normalize_seconds(&s)
}

/// `--vfs-read-chunk-size` clap prints the raw byte count
/// (`134217728`); README prints the human form
/// (`128 MiB (134217728)`). Normalize BOTH sides to the same
/// canonical byte count so they compare equal.
///
/// Accepts `128MiB`, `128 MiB`, `128Mi`, `128 M`, `128 MB`, `134217728`,
/// `128 kib` (case-insensitive), and any trailing parenthetical is
/// dropped (`128 MiB (134217728)` → `134217728`). Returns the original
/// string unchanged if no unit / digits parse (so a future change
/// surfaces as drift instead of silently passing).
fn normalize_bytes(s: &str) -> String {
    let s = strip_backticks(s);
    // Drop any trailing parenthetical: "128 MiB (134217728)" → "128 MiB"
    let s = match s.find(" (") {
        Some(i) => s[..i].to_string(),
        None => s,
    };
    let s = s.trim();
    // Split off the unit suffix (case-insensitive). Look for the first
    // non-digit / non-'.'' non-' '-' char.
    let mut split = s.len();
    for (i, c) in s.char_indices() {
        if !c.is_ascii_digit() && c != '.' && c != ' ' {
            split = i;
            break;
        }
    }
    let (num_str, unit_str) = s.split_at(split);
    let num_str = num_str.trim();
    let unit_str = unit_str.trim();
    let n: f64 = match num_str.parse::<f64>() {
        Ok(n) => n,
        Err(_) => return s.to_string(),
    };
    // Bare digits = raw bytes (no unit): pass through.
    if unit_str.is_empty() {
        return num_str.to_string();
    }
    let unit = unit_str.to_ascii_uppercase();
    // Strip trailing 'B' so "M", "MB", "MiB" all match.
    let unit = unit.strip_suffix('B').unwrap_or(&unit);
    let mult: f64 = match unit {
        "" => 1.0,
        "K" | "KI" => 1024.0,
        "M" | "MI" => 1024.0 * 1024.0,
        "G" | "GI" => 1024.0 * 1024.0 * 1024.0,
        _ => return s.to_string(),
    };
    let bytes = (n * mult) as u64;
    bytes.to_string()
}

/// `--vfs-read-ahead` / `--vfs-read-chunk-size-limit` clap prints
/// `0 = off` / `0 = unlimited`; README prints `0` / `0 (off)`. Both
/// mean "0" — collapse to the bare `0`.
fn normalize_zero(s: &str) -> String {
    let s = strip_backticks(s);
    if s == "0" {
        return "0".to_string();
    }
    if let Some(rest) = s.strip_prefix("0 ") {
        // "0 = off" / "0 = unlimited" / "0 (off)" — drop the suffix.
        if rest.starts_with('=') || rest.starts_with('(') {
            return "0".to_string();
        }
    }
    s
}

const FLAGS: &[FlagSpec] = &[
    FlagSpec {
        flag: "vfs-cache-mode",
        help_default: help_default_vfs_cache_mode,
        readme_default: readme_default_vfs_cache_mode,
        normalize: normalize_default,
    },
    FlagSpec {
        flag: "vfs-read-wait",
        help_default: help_default_vfs_read_wait,
        readme_default: readme_default_vfs_read_wait,
        normalize: normalize_default,
    },
    FlagSpec {
        flag: "vfs-write-wait",
        help_default: help_default_vfs_write_wait,
        readme_default: readme_default_vfs_write_wait,
        // help prints `1`, README prints `1s` — `normalize_seconds`
        // already handles this.
        normalize: normalize_default,
    },
    FlagSpec {
        flag: "vfs-read-chunk-size",
        help_default: help_default_vfs_read_chunk_size,
        readme_default: readme_default_vfs_read_chunk_size,
        // help prints `128MiB`, README prints `128 MiB (134217728)`.
        normalize: normalize_bytes,
    },
    FlagSpec {
        flag: "vfs-read-chunk-size-limit",
        help_default: help_default_vfs_read_chunk_size_limit,
        readme_default: readme_default_vfs_read_chunk_size_limit,
        // help prints `0 = unlimited`, README prints `0 (off)`.
        normalize: normalize_zero,
    },
    FlagSpec {
        flag: "vfs-read-ahead",
        help_default: help_default_vfs_read_ahead,
        readme_default: readme_default_vfs_read_ahead,
        // help prints `0 = off`, README prints `0`.
        normalize: normalize_zero,
    },
    FlagSpec {
        flag: "write-back-cache",
        help_default: help_default_write_back_cache,
        readme_default: readme_default_write_back_cache,
        normalize: normalize_default,
    },
];

/// Locate the freshly built `mntrs` binary. `cargo test` sets
/// `CARGO_BIN_EXE_<name>` for each `[[bin]]` in the workspace; we use it
/// to avoid PATH / target-dir guessing.
fn mntrs_bin() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_mntrs"))
}

/// Walk up from CARGO_MANIFEST_DIR until we find README.md (the test
/// binary is in `target/debug/deps/cli_defaults-XXX`, so we anchor at the
/// crate root which is the repo root).
fn readme_path() -> std::path::PathBuf {
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut p = manifest.as_path();
    loop {
        let candidate = p.join("README.md");
        if candidate.exists() {
            return candidate;
        }
        match p.parent() {
            Some(parent) => p = parent,
            None => panic!("README.md not found above CARGO_MANIFEST_DIR"),
        }
    }
}

#[test]
fn cli_defaults_match_readme() {
    let bin = mntrs_bin();
    let help_out = Command::new(&bin)
        .args(["mount", "--help"])
        .output()
        .expect("spawn `mntrs mount --help`");
    assert!(
        help_out.status.success(),
        "`mntrs mount --help` failed: stderr={}",
        String::from_utf8_lossy(&help_out.stderr)
    );
    let help = String::from_utf8_lossy(&help_out.stdout).into_owned();

    let readme = std::fs::read_to_string(readme_path()).expect("read README.md");

    for spec in FLAGS {
        let help_val = (spec.help_default)(&help).unwrap_or_else(|| {
            panic!(
                "--{}: no [default: ...] found in `mntrs mount --help`",
                spec.flag
            )
        });
        let readme_val = (spec.readme_default)(&readme);

        let help_norm = (spec.normalize)(&help_val);
        let readme_norm = (spec.normalize)(&readme_val);

        assert_eq!(
            help_norm, readme_norm,
            "--{} default drifted: --help says `{}`, README says `{}`",
            spec.flag, help_val, readme_val
        );
    }
}
