#[cfg(target_os = "linux")]
use anyhow::Context;
use anyhow::Result;

#[cfg(target_os = "linux")]
pub fn systemd() -> Result<()> {
    // Issue: install hardening (audit #3 pass).
    //
    // Previously this function:
    //   1. Compiled on every platform (wrote the unit on macOS/Windows too)
    //   2. Opened the destination with `fs::write` → followed any
    //      pre-existing symlink (TOCTOU) and inherited umask (0o644)
    //   3. Hardcoded `/usr/bin/mntrs` and `/usr/bin/fusermount3`
    //      (silent non-functional unit on common installs)
    //   4. Restart=always hot-looped on misconfigured instances
    //
    // Fixes:
    //   * `#[cfg(target_os = "linux")]` gates the function so non-Linux
    //     builds get a clear "unsupported on this platform" error.
    //   * `OpenOptions::create_new(true).mode(0o600)` — fails if a
    //     symlink (or any file) is already at the destination path;
    //     mode is owner-only regardless of umask.
    //   * `std::env::current_exe()` resolves the mntrs binary path
    //     at install time; `which fusermount3 || which fusermount`
    //     falls back gracefully on Debian vs Arch.
    //   * `Restart=on-failure` + `StartLimitBurst=5` /
    //     `StartLimitIntervalSec=60s` to bound restart storms from
    //     broken %i instances.
    //   * Idempotency: read the existing file (if any) and bail with a
    //     clear "use --force to overwrite" hint if it differs from the
    //     template we would write.
    //   * Emit `tracing::info!` for log-aggregator visibility.
    let config_dir = crate::util::config_dir().context("systemd install failed")?;
    let service_dir = config_dir.join("systemd").join("user");
    let service_file = service_dir.join("mntrs-mount@.service");

    let mntrs_path = resolve_mntrs_path()?;
    let mntrs_path_display = mntrs_path.display().to_string();
    let fusermount = resolve_fusermount();

    let template = format!(
        r#"[Unit]
Description=mntrs FUSE mount for %i
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={mntrs_path_display} mount %i /mnt/%i
ExecStop={fusermount} -u /mnt/%i
ExecStopPost={fusermount} -uz /mnt/%i
Restart=on-failure
RestartSec=5s
StartLimitBurst=5
StartLimitIntervalSec=60s
Environment=HOME=%h
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=default.target
"#
    );

    // Idempotency: if a unit file already exists and differs from the
    // template, fail loudly rather than silently destroying the operator's
    // customizations (extra `Environment=` lines, different `ExecStart=`,
    // etc.). The CLI does not yet expose a `--force` flag; the operator
    // can edit the file manually or remove it before re-running.
    if service_file.exists() {
        let existing = std::fs::read_to_string(&service_file)
            .map_err(|e| anyhow::Error::new(e).context("read existing service file"))?;
        if existing == template {
            tracing::info!(
                service_file = %service_file.display(),
                "mntrs install: service file already up to date; nothing to do"
            );
            println!(
                "Service file already up to date: {}",
                service_file.display()
            );
            return Ok(());
        }
        anyhow::bail!(
            "service file already exists at {} with different content; \
             back it up or remove it before re-running `mntrs install`",
            service_file.display()
        );
    }

    std::fs::create_dir_all(&service_dir)
        .map_err(|e| anyhow::Error::new(e).context("create service dir"))?;

    // Symlink-safe write: `create_new(true)` fails with EEXIST if anything
    // (file, directory, symlink, FIFO, …) is already at the destination.
    // The race window between `exists()` above and this `open` is closed
    // by `create_new` — if a TOCTOU attacker plants a symlink in that
    // window, the open returns EEXIST and the operator sees a clear error.
    let mut f = write_open_secure(&service_file)
        .map_err(|e| anyhow::Error::new(e).context("open service file for write"))?;
    use std::io::Write as _;
    f.write_all(template.as_bytes())
        .map_err(|e| anyhow::Error::new(e).context("write service file"))?;
    f.sync_all()
        .map_err(|e| anyhow::Error::new(e).context("fsync service file"))?;

    tracing::info!(
        service_file = %service_file.display(),
        mntrs_path = %mntrs_path.display(),
        fusermount = %fusermount,
        "wrote systemd user service template"
    );

    println!(
        "Wrote systemd user service template: {}",
        service_file.display()
    );
    println!();
    println!("Usage:");
    println!("  systemctl --user daemon-reload");
    println!("  systemctl --user enable --now mntrs-mount@s3://bucket.service");
    println!("  systemctl --user status mntrs-mount@s3://bucket.service");

    Ok(())
}

/// Fallback stub for non-Linux targets. The CLI subcommand is also
/// gated on `cfg(target_os = "linux")` in main.rs, so this should
/// rarely be hit — but keeping the symbol present lets the binary
/// link cleanly even if the gate moves in the future.
#[cfg(not(target_os = "linux"))]
pub fn systemd() -> Result<()> {
    anyhow::bail!(
        "`mntrs install systemd` is only supported on Linux; this build \
         targets a non-Linux OS"
    )
}

/// Resolve the path to the running mntrs binary. Uses `current_exe()`
/// so the generated unit points at the binary the operator actually
/// invoked, regardless of install location (Homebrew, cargo install,
/// Nix profile, /usr/bin, ~/.local/bin, …).
#[cfg(target_os = "linux")]
fn resolve_mntrs_path() -> Result<std::path::PathBuf> {
    let p = std::env::current_exe()
        .map_err(|e| anyhow::Error::new(e).context("locate running mntrs binary"))?;
    if !p.exists() {
        anyhow::bail!(
            "current_exe() returned {} which does not exist on disk",
            p.display()
        );
    }
    Ok(p)
}

/// Find fusermount3 with a fallback to fusermount. The setuid binary
/// ships under different names on Debian (`/bin/fusermount3`) vs
/// Arch (`/usr/bin/fusermount3`) vs older installs (`fusermount`).
#[cfg(target_os = "linux")]
fn resolve_fusermount() -> String {
    for candidate in ["fusermount3", "fusermount"] {
        if let Ok(p) = which(candidate) {
            return p.to_string_lossy().into_owned();
        }
    }
    // Last-resort fallback: let systemd's PATH search find it. If
    // neither binary is on PATH the unit will fail at start time
    // with a clear "Failed to locate executable" error rather than
    // silently no-op'ing.
    "fusermount3".to_string()
}

/// Minimal `which(1)` implementation — walks PATH for an executable
/// matching `name`. Avoids pulling in the `which` crate for one call.
#[cfg(target_os = "linux")]
fn which(name: &str) -> Result<std::path::PathBuf> {
    let path_var = std::env::var_os("PATH").ok_or_else(|| anyhow::anyhow!("PATH is not set"))?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(name);
        if is_executable(&candidate) {
            return Ok(candidate);
        }
    }
    anyhow::bail!("`{}` not found on PATH", name)
}

#[cfg(target_os = "linux")]
fn is_executable(p: &std::path::Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    match std::fs::metadata(p) {
        Ok(m) => m.is_file() && (m.permissions().mode() & 0o111) != 0,
        Err(_) => false,
    }
}

/// Open a path for writing, refusing to overwrite an existing entry
/// (file, symlink, FIFO, …), with mode 0o600 (owner read/write only).
/// Returns the open file handle; the caller writes content + syncs.
#[cfg(target_os = "linux")]
fn write_open_secure(p: &std::path::Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(p)
}

#[cfg(test)]
#[cfg(target_os = "linux")]
mod tests {
    use super::*;
    use std::io::Write;

    /// Build a fresh temp dir; cleanup is best-effort.
    fn tmpdir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mntrs-install-test-{}-{}-{}",
            tag,
            std::process::id(),
            // nanosecond suffix to avoid collisions between tests
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn is_executable_true_for_sh() {
        let sh = which("sh").expect("sh should be on PATH");
        assert!(is_executable(&sh));
    }

    #[test]
    fn is_executable_false_for_nonexistent() {
        assert!(!is_executable(std::path::Path::new(
            "/definitely/not/here/please/xyzzy"
        )));
    }

    #[test]
    fn which_finds_sh() {
        let p = which("sh").unwrap();
        assert!(p.ends_with("sh"));
    }

    #[test]
    fn which_misses_nonexistent() {
        assert!(which("mntrs-no-such-binary-xyzzy-12345").is_err());
    }

    #[test]
    fn resolve_mntrs_path_returns_existing_exe() {
        let p = resolve_mntrs_path().unwrap();
        assert!(p.exists(), "{:?} should exist", p);
    }

    #[test]
    fn resolve_fusermount_returns_nonempty() {
        // On every reasonable Linux test host at least one of the
        // fusermount* binaries is on PATH; if not, the function falls
        // back to the literal "fusermount3" string.
        let s = resolve_fusermount();
        assert!(!s.is_empty());
    }

    #[test]
    fn write_open_secure_rejects_existing_file() {
        let dir = tmpdir("sec");
        let p = dir.join("unit.service");
        std::fs::write(&p, "placeholder").unwrap();
        // Pre-existing file → create_new(true) must fail.
        assert!(write_open_secure(&p).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_open_secure_rejects_symlink() {
        let dir = tmpdir("sym");
        let real = dir.join("real.txt");
        std::fs::write(&real, "target").unwrap();
        let link = dir.join("link.txt");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        // Symlink at destination must NOT be followed — create_new fails.
        assert!(write_open_secure(&link).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_open_secure_creates_with_mode_0o600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tmpdir("new");
        let p = dir.join("fresh.service");
        let mut f = write_open_secure(&p).unwrap();
        f.write_all(b"hello").unwrap();
        f.sync_all().unwrap();
        drop(f);
        let meta = std::fs::metadata(&p).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
