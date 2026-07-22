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
}
