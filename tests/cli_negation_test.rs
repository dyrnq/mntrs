//! CLI negation flag parse guard for default-true bool flags (issue #474).
//!
//! Prevents regressions in the clap `--no-*` pattern used for
//! `--slow-statfs` and `--finder-local`. The negation flag is a
//! presence-only `ArgAction::SetFalse` with `default_value_t = true`,
//! so:
//!
//!   * without the flag on the CLI: bool field stays true (default fires)
//!   * with `--no-foo` on the CLI: bool field flips to false (negation fires)
//!
//! We verify this end-to-end by spawning the binary with various arg
//! shapes. The daemon would normally run forever, so we send SIGTERM
//! after 1.5s and check that stderr never contained a clap parse
//! error (`unexpected argument`, `value is required`, etc.).
//!
//! The end-to-end mount-option behavior (`-o local` actually pushed or
//! not) is covered by manual mount smoke tests run during development;
//! this test only locks the CLI surface so future clap upgrades can't
//! silently break negation.

use std::process::Command;
use std::time::Duration;

/// Locate the freshly built `mntrs` binary.
fn mntrs_bin() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_mntrs"))
}

/// Run `mntrs mount memory:/// <tmpdir> <args>` for ~1.5s, then SIGTERM.
/// Returns the captured stderr. The presence of a clap parse error
/// (substring match) on stderr is what we assert against.
fn run_mount_capture_stderr(args: &[&str]) -> String {
    let tmp = std::env::temp_dir().join(format!(
        "mntrs-cli-neg-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&tmp);
    let mp = tmp.join("mp");
    let _ = std::fs::create_dir_all(&mp);

    let mut full_args = vec!["mount", "memory:///", mp.to_str().unwrap()];
    full_args.extend_from_slice(args);

    let mut child = Command::new(mntrs_bin())
        .args(&full_args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn mntrs mount");

    // Daemon runs forever; wait a short window then kill it. If args
    // are malformed, clap fails fast before mount starts and the
    // process exits on its own -- `wait()` would then return Err.
    std::thread::sleep(Duration::from_millis(1500));
    let _ = child.kill();
    let output = child.wait_with_output().expect("wait_with_output");

    // Best-effort mountpoint cleanup so leftover test mounts don't
    // pile up across `cargo test` invocations.
    let _ = Command::new("umount").arg(&mp).output();
    let _ = std::fs::remove_dir_all(&tmp);

    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn has_clap_parse_error(stderr: &str) -> bool {
    // The exact clap error messages we want to flag. These are the
    // user-facing strings `clap::error::Error` formats to stderr.
    stderr.contains("unexpected argument")
        || stderr.contains("a value is required")
        || stderr.contains("cannot find")
        || stderr.contains("invalid value")
}

#[test]
fn default_args_parse_clean() {
    let stderr = run_mount_capture_stderr(&[]);
    assert!(
        !has_clap_parse_error(&stderr),
        "default args should parse without clap errors; stderr=\n{stderr}"
    );
}

#[test]
fn no_finder_local_parses() {
    let stderr = run_mount_capture_stderr(&["--no-finder-local"]);
    assert!(
        !has_clap_parse_error(&stderr),
        "--no-finder-local should parse; stderr=\n{stderr}"
    );
}

#[test]
fn no_slow_statfs_parses() {
    let stderr = run_mount_capture_stderr(&["--no-slow-statfs"]);
    assert!(
        !has_clap_parse_error(&stderr),
        "--no-slow-statfs should parse; stderr=\n{stderr}"
    );
}

#[test]
fn both_negation_flags_parse() {
    let stderr = run_mount_capture_stderr(&["--no-finder-local", "--no-slow-statfs"]);
    assert!(
        !has_clap_parse_error(&stderr),
        "both --no-* flags should parse together; stderr=\n{stderr}"
    );
}

#[test]
fn help_lists_both_polarity_flags() {
    // Lock the CLI surface so a future clap refactor doesn't silently
    // drop the negation flag from --help output.
    let out = Command::new(mntrs_bin())
        .args(["mount", "--help"])
        .output()
        .expect("spawn `mntrs mount --help`");
    assert!(
        out.status.success(),
        "`mntrs mount --help` failed: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let help = String::from_utf8_lossy(&out.stdout);
    assert!(
        help.contains("--finder-local"),
        "--finder-local missing from --help"
    );
    assert!(
        help.contains("--no-finder-local"),
        "--no-finder-local missing from --help"
    );
    assert!(
        help.contains("--slow-statfs"),
        "--slow-statfs missing from --help"
    );
    assert!(
        help.contains("--no-slow-statfs"),
        "--no-slow-statfs missing from --help"
    );
    assert!(
        help.contains("--storage-class"),
        "--storage-class missing from --help"
    );
}

// ── --storage-class wiring (issue #219 follow-up) ────────────────
//
// `--storage-class` was previously a "shadow flag" — clap accepted it,
// the daemon emitted a "no effect" warn, and the value was dropped on
// the floor. As of the fix it propagates to opendal's S3 builder via
// the `default_storage_class` setter. These tests pin the contract:
// the flag stays in `--help`, it parses without error, and the
// shadow warn no longer fires for it.

/// `mntrs mount --help` must list `--storage-class`. The flag has a
/// `value_parser` enum (STANDARD, GLACIER, …); clap emits both the
/// flag line and the possible-values list. Lock both so a future
/// clap refactor can't silently drop the surface.
#[test]
fn help_lists_storage_class_flag() {
    let out = Command::new(mntrs_bin())
        .args(["mount", "--help"])
        .output()
        .expect("spawn `mntrs mount --help`");
    assert!(
        out.status.success(),
        "`mntrs mount --help` failed: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let help = String::from_utf8_lossy(&out.stdout);
    assert!(
        help.contains("--storage-class"),
        "--storage-class flag line missing from --help"
    );
    // value_parser enum: a few canonical names should be in the
    // help text (clap renders them either in the possible-values
    // block or in the doc-comment if a value_parser is used).
    for v in &["STANDARD", "GLACIER", "DEEP_ARCHIVE"] {
        assert!(
            help.contains(v),
            "expected value `{v}` listed in --storage-class help"
        );
    }
}

/// Without `--storage-class` on the CLI, the daemon must NOT emit
/// the shadow-flag warn mentioning `--storage-class`. (Other shadow
/// flags may still warn if the user set them off-default; this
/// assertion is scoped to `--storage-class` only.)
#[test]
fn default_args_no_storage_class_shadow() {
    let stderr = run_mount_capture_stderr(&[]);
    assert!(
        !stderr.contains("--storage-class"),
        "without --storage-class on CLI, shadow warn must not mention it; stderr=\n{stderr}"
    );
}

/// `--storage-class=GLACIER` parses cleanly AND must NOT trigger
/// the shadow warn — the flag is now wired through to the opendal
/// S3 builder, so the warn would be a regression.
#[test]
fn storage_class_glacier_parses_and_no_shadow_warn() {
    let stderr = run_mount_capture_stderr(&["--storage-class=GLACIER"]);
    assert!(
        !has_clap_parse_error(&stderr),
        "--storage-class=GLACIER should parse without clap error; stderr=\n{stderr}"
    );
    assert!(
        !stderr.contains("shadow") || !stderr.contains("--storage-class"),
        "--storage-class is now wired; shadow warn must not fire; stderr=\n{stderr}"
    );
}

/// `--storage-class=BOGUS` is rejected at startup by clap's
/// value_parser. The error message must surface the invalid value
/// so users can see what's wrong without consulting docs.
#[test]
fn storage_class_invalid_value_rejected() {
    let stderr = run_mount_capture_stderr(&["--storage-class=BOGUS"]);
    assert!(
        has_clap_parse_error(&stderr),
        "--storage-class=BOGUS should fail clap value_parser; stderr=\n{stderr}"
    );
    assert!(
        stderr.contains("BOGUS"),
        "clap error should mention the invalid value BOGUS; stderr=\n{stderr}"
    );
}

// ── --vfs-cache-max-age wiring (issue #507) ────────────────────────
//
// `--vfs-cache-max-age` was previously a "shadow flag" — clap
// accepted it, the daemon emitted a "no effect" warn, and the value
// was dropped on the floor. As of the fix it propagates to
// `MultiLevelCache::new` as the L2 TTL (absolute age via filesystem
// mtime). These tests pin the contract: the flag stays in `--help`,
// it parses without error, and the shadow warn no longer fires.

/// Without `--vfs-cache-max-age` on the CLI, the daemon must NOT
/// mention it in stderr. Pre-fix, the shadow-warn list included
/// the flag unconditionally; this locks the warn is gone.
#[test]
fn default_args_no_vfs_cache_max_age_shadow() {
    let stderr = run_mount_capture_stderr(&[]);
    assert!(
        !stderr.contains("--vfs-cache-max-age"),
        "without --vfs-cache-max-age on CLI, stderr must not mention it; stderr=\n{stderr}"
    );
}

/// `--vfs-cache-max-age 0` (disable) must not log the shadow warn,
/// even though 0 is off the default 3600. Pre-fix this was a
/// spurious warn.
#[test]
fn vfs_cache_max_age_zero_no_shadow_warn() {
    let stderr = run_mount_capture_stderr(&["--vfs-cache-max-age", "0"]);
    assert!(
        !has_clap_parse_error(&stderr),
        "--vfs-cache-max-age 0 should parse; stderr=\n{stderr}"
    );
    assert!(
        !stderr.contains("--vfs-cache-max-age"),
        "--vfs-cache-max-age=0 is wired; shadow warn must not fire; stderr=\n{stderr}"
    );
}

/// `--vfs-cache-max-age 60` must not log the shadow warn.
#[test]
fn vfs_cache_max_age_nonzero_no_shadow_warn() {
    let stderr = run_mount_capture_stderr(&["--vfs-cache-max-age", "60"]);
    assert!(
        !has_clap_parse_error(&stderr),
        "--vfs-cache-max-age 60 should parse; stderr=\n{stderr}"
    );
    assert!(
        !stderr.contains("--vfs-cache-max-age"),
        "--vfs-cache-max-age=60 is wired; shadow warn must not fire; stderr=\n{stderr}"
    );
}

/// Clap must parse a range of `--vfs-cache-max-age` values without
/// error. Includes the default (3600), a typical short-TTL (60),
/// 1 day, and `0` (disabled).
#[test]
fn vfs_cache_max_age_parses_various_values() {
    for v in ["0", "1", "60", "3600", "86400"] {
        let stderr = run_mount_capture_stderr(&["--vfs-cache-max-age", v]);
        assert!(
            !has_clap_parse_error(&stderr),
            "value {v} must parse cleanly; stderr=\n{stderr}"
        );
    }
}

// ── --no-modtime wiring (issue #509) ──────────────────────────
//
// `--no-modtime` was previously a "shadow flag" — clap accepted
// it, the README advertised it, the help text listed it, but the
// value was dropped on the floor at `_no_modtime: bool` (compiler-
// enforced dead). As of the fix it propagates to `MntrsFs::no_modtime`
// and gates both `stat_op` and `list_op` mtime paths. These tests
// pin the contract: the flag stays in `--help`, parses without
// error, and the shadow-warn no longer fires.

/// Without `--no-modtime` on the CLI, the daemon must NOT
/// mention it in stderr. Pre-fix, the shadow-warn path was
/// unaffected for `--no-modtime` (the warn was gated on
/// non-default values and `_no_modtime` was already a no-op),
/// but this locks the flag is recognized as wired.
#[test]
fn default_args_no_no_modtime_shadow() {
    let stderr = run_mount_capture_stderr(&[]);
    assert!(
        !stderr.contains("--no-modtime"),
        "without --no-modtime on CLI, shadow warn must not mention it; stderr=\n{stderr}"
    );
}

/// `--no-modtime` parses cleanly and must NOT trigger the shadow
/// warn — the flag is now wired through to `MntrsFs::no_modtime`
/// (gates both `stat_op` and `list_op` mtime paths).
#[test]
fn no_modtime_flag_parses_and_no_shadow_warn() {
    let stderr = run_mount_capture_stderr(&["--no-modtime"]);
    assert!(
        !has_clap_parse_error(&stderr),
        "--no-modtime should parse without clap error; stderr=\n{stderr}"
    );
    assert!(
        !stderr.contains("shadow") && !stderr.contains("--no-modtime"),
        "--no-modtime is now wired; shadow warn must not fire; stderr=\n{stderr}"
    );
}

/// `mntrs mount --help` must list `--no-modtime`. The flag was
/// always in the CLI surface (clap accepted it), but a future
/// refactor that accidentally drops the field would silently
/// leave users searching for it. Lock the help-line is there.
#[test]
fn help_lists_no_modtime_flag() {
    let out = Command::new(mntrs_bin())
        .args(["mount", "--help"])
        .output()
        .expect("spawn `mntrs mount --help`");
    assert!(
        out.status.success(),
        "`mntrs mount --help` failed: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let help = String::from_utf8_lossy(&out.stdout);
    assert!(
        help.contains("--no-modtime"),
        "--no-modtime flag line missing from --help"
    );
}
