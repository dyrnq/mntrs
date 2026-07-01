use anyhow::{Result, anyhow};
use std::fs;
use std::path::Path;
#[cfg(unix)]
use std::process::Command;

pub fn unmount(target: &str) -> Result<()> {
    tracing::debug!(target, "unmount: entered");
    let mounts = crate::cmd::mount::read_mounts();
    tracing::debug!(
        target,
        mounts_count = mounts.len(),
        "unmount: read mounts db"
    );

    if target == "all" || target == "-a" {
        if mounts.is_empty() {
            return Err(anyhow!("no mntrs mounts found"));
        }
        // Issue #315: collect per-mount errors instead of swallowing
        // them into tracing::debug!. Pre-fix, a half-failed `unmount
        // all` (e.g. 1 stale entry + 1 live one) returned Ok(()) —
        // scripts that piped the output to a state-check thought
        // everything succeeded and proceeded to remount, hitting
        // "device busy" downstream. Now: each failure is appended
        // with its mountpoint + error message, all are eprintln'd at
        // the end, and the function returns Err if any failed.
        let mut failures: Vec<String> = Vec::new();
        for m in &mounts {
            let mountpoint = &m.mountpoint;
            eprintln!("unmounting {mountpoint}");
            tracing::debug!(mountpoint, "unmount: 'all' branch -> fuse_unmount");
            if let Err(e) = fuse_unmount(mountpoint) {
                tracing::debug!(error=%e, mountpoint, "unmount all per-mount failed");
                failures.push(format!("{mountpoint}: {e}"));
            }
        }
        // Issue #261.4: same path as mount.rs uses for the db.
        let db = crate::cmd::mount::mounts_db_path();
        tracing::debug!(db, "unmount: 'all' branch -> remove mounts db");
        if let Err(e) = fs::remove_file(&db) {
            tracing::debug!(error=%e, db, "unmount all db remove failed");
            // db removal failure is a separate concern from per-mount
            // failures — surface it as its own message so the user
            // doesn't think their mounts are still tracked after the
            // explicit `unmount all` removed them.
            failures.push(format!("mounts db {db}: {e}"));
        }
        if !failures.is_empty() {
            for f in &failures {
                eprintln!("error: {f}");
            }
            return Err(anyhow!(
                "unmount all: {} mount(s) failed: {}",
                failures.len(),
                failures.join("; ")
            ));
        }
        return Ok(());
    }

    tracing::debug!(target, "unmount: dispatching target");
    // Windows: "V:" alone makes `Path::exists()` call `GetFileAttributesW("V:")`
    // which blocks on WinFSP's volume ready-handshake (observed in #249 e2e:
    // hangs for ~60s then returns false even when V: is mounted). Detect the
    // drive-letter form FIRST so we never pay that cost.
    //
    // Issue #315: idempotent contract — if the target is not a known
    // mountpoint (drive letter absent, path doesn't exist, and no entry
    // in the mounts db for the storage URL), treat that as Ok with a
    // debug log instead of Err. This mirrors the existing in-fuse_unmount
    // handling for "drive letter already absent" (line 219-221 area) and
    // the inner ERROR_FILE_NOT_FOUND path on Windows. Scripts can chain
    // `mntrs unmount X && mount-another X` without first having to verify
    // X is currently mounted — matches `umount` on Linux / `diskpart
    // remove` on Windows in spirit (both succeed when the target is
    // already gone). Pre-fix, `mntrs unmount /nonexistent` returned
    // Err("mount point '...' does not exist") which broke any `unmount
    // && remount` script in idempotency-sensitive contexts.
    #[cfg(windows)]
    let mountpoint: String = {
        if is_drive_letter(target) {
            tracing::debug!(
                target,
                "unmount: windows drive-letter shortcut (skip Path::exists)"
            );
            target.to_string()
        } else if Path::new(target).exists() {
            target.to_string()
        } else if let Some(m) = mounts.iter().find(|m| m.storage == target) {
            m.mountpoint.clone()
        } else {
            tracing::debug!(
                target,
                "unmount: target not a drive letter, not on disk, not in mounts db; idempotent Ok"
            );
            return Ok(());
        }
    };
    #[cfg(not(windows))]
    let mountpoint: String = {
        if Path::new(target).exists() {
            target.to_string()
        } else if let Some(m) = mounts.iter().find(|m| m.storage == target) {
            m.mountpoint.clone()
        } else {
            tracing::debug!(
                target,
                "unmount: target not on disk, not in mounts db; idempotent Ok"
            );
            return Ok(());
        }
    };

    fuse_unmount(&mountpoint)?;

    // remove from db
    let db = crate::cmd::mount::mounts_db_path();
    if let Ok(content) = fs::read_to_string(&db) {
        let filtered: Vec<&str> = content
            .lines()
            .filter(|l| l.split('\0').nth(1) != Some(mountpoint.as_str()))
            .collect();
        if let Err(e) = fs::write(&db, filtered.join("\n")) {
            tracing::debug!(error=%e, "unmount db cleanup failed");
        }
    }
    Ok(())
}

/// POSIX: shell out to `fusermount3` (fallback `fusermount`).
///
/// Issue #371: macOS has no `fusermount(-3)` binary; vanilla
/// installs return `Err(NotFound)` for both names and the
/// previous single-path implementation always failed there. The
/// macOS branch (see `fuse_unmount_macos_with_umount`) additionally
/// falls through to the platform `umount(8)` so the unmount signal
/// reaches macFUSE's `mount_macfuse` helper.
///
/// Platform split (matches the `#[cfg(unix)]` outer guard that
/// keeps the Windows branch in a sibling `fuse_unmount`):
/// - macOS-only path:  `cfg(target_os = "macos")` → umount fallback.
/// - Linux / *BSD / Solaris: `cfg(all(unix, not(target_os = "macos")))`
///   → original fusermount-only chain, **byte-identical** to the
///   pre-fix implementation, so a change to the macOS branch
///   cannot accidentally regress a Linux CI run.
#[cfg(unix)]
fn fuse_unmount(mountpoint: &str) -> Result<()> {
    #[cfg(not(target_os = "macos"))]
    {
        fuse_unmount_via_fusermount(mountpoint)
    }

    #[cfg(target_os = "macos")]
    {
        fuse_unmount_macos_with_umount(mountpoint)
    }
}

/// Linux + non-macOS unix: the original `fusermount3` →
/// `fusermount` chain, preserved verbatim from the pre-fix
/// implementation. macOS is dispatched to
/// `fuse_unmount_macos_with_umount` instead so this function
/// compiles only when `target_os` is anything **except** macOS.
///
/// Gated by `cfg(all(unix, not(target_os = "macos")))` rather than
/// just `cfg(unix)` so that the cfg predicate on its own is
/// enough to prove the Linux path is unaffected by changes to
/// `fuse_unmount_macos_with_umount`.
///
/// Visibility: `pub(crate)` so that `src/cmd/mount.rs` can reuse
/// this helper at the cleanup / signal-watcher sites without
/// duplicating the shell-out (Issue #374). The function's cfg
/// predicate (not the visibility) is what matters for guaranteeing
/// Linux behaviour is unaffected by changes to the macOS branch.
#[cfg(all(unix, not(target_os = "macos")))]
pub(crate) fn fuse_unmount_via_fusermount(mountpoint: &str) -> Result<()> {
    let result = Command::new("fusermount3")
        .arg("-u")
        .arg(mountpoint)
        .status()
        .or_else(|_| {
            Command::new("fusermount")
                .arg("-u")
                .arg(mountpoint)
                .status()
        });

    match result {
        Ok(status) if status.success() => {
            eprintln!("unmounted {mountpoint}");
            Ok(())
        }
        Ok(status) => Err(anyhow!(
            "fusermount failed with exit code {}",
            status.code().unwrap_or(-1)
        )),
        Err(e) => Err(anyhow!("failed to run fusermount: {e}")),
    }
}

/// macOS-only unmount path (Issue #371).
///
/// Tries the same `fusermount3` → `fusermount` chain as the Linux
/// path first — a user who installs libfuse via Homebrew gets the
/// same primary code path — then, when both names return
/// `Err(NotFound)` (the macOS-vanilla case), defers to `umount(8)`.
///
/// Why umount(8) works on macOS: macFUSE's `mount_macfuse` helper
/// (installed at
/// `/Library/Filesystems/macfuse.fs/Contents/Resources/mount_macfuse`
/// and registered with the kernel via the FSKit extension
/// `io.macfuse.app.fsmodule`) implements the unmount protocol
/// for `macfuse` filesystem entries listed by `mount(8)`. A plain
/// `umount <mountpoint>` against one of those entries routes
/// through that helper, so we don't need a macOS-specific
/// unmount binary.
///
/// Non-NotFound errors from `fusermount` (EACCES, ENOENT of the
/// mountpoint path itself, etc.) are surfaced verbatim — the same
/// as on Linux.
///
/// Visibility: `pub(crate)` so that `src/cmd/mount.rs` can reuse
/// this helper at the cleanup / signal-watcher sites
/// (Issue #374). On macOS they get the umount(8) fallback; on any
/// other unix, this function's body is not even compiled.
#[cfg(target_os = "macos")]
pub(crate) fn fuse_unmount_macos_with_umount(mountpoint: &str) -> Result<()> {
    let result = Command::new("fusermount3")
        .arg("-u")
        .arg(mountpoint)
        .status()
        .or_else(|_| {
            Command::new("fusermount")
                .arg("-u")
                .arg(mountpoint)
                .status()
        });

    match result {
        Ok(status) if status.success() => {
            eprintln!("unmounted {mountpoint}");
            Ok(())
        }
        Ok(status) => Err(anyhow!(
            "fusermount failed with exit code {}",
            status.code().unwrap_or(-1)
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Vanilla macOS install: no fusermount(-3) binary on
            // PATH. Defer to umount(8) so the unmount signal
            // reaches macFUSE's mount_macfuse helper. (Issue #371.)
            tracing::debug!(
                mountpoint,
                "fuse_unmount(macos): fusermount(-3) not on PATH; falling back to umount(8) (Issue #371)"
            );
            // Issue #379: macOS `umount(8)` (BSD-derived) does not
            // canonicalize before matching the mount table — pass
            // the canonical path so a user-supplied `/tmp/foo`
            // (which symlinks to `/private/tmp/foo` on macOS) matches
            // the entry listed by `mount(8)`. On canonicalize
            // failure (e.g. the mountpoint dir was deleted between
            // mount and unmount), fall back to the raw string;
            // `umount(8)` will return its own "not currently
            // mounted" / ENOENT-style diagnostic.
            let canonical = std::fs::canonicalize(mountpoint)
                .unwrap_or_else(|_| std::path::PathBuf::from(mountpoint));
            let umount_status = Command::new("umount").arg(&canonical).status();
            match umount_status {
                Ok(s) if s.success() => {
                    eprintln!("unmounted {mountpoint}");
                    Ok(())
                }
                Ok(s) => Err(anyhow!(
                    "umount failed with exit code {}",
                    s.code().unwrap_or(-1)
                )),
                Err(e2) => Err(anyhow!(
                    "failed to run umount (fusermount also missing): {e2}"
                )),
            }
        }
        Err(e) => Err(anyhow!("failed to run fusermount: {e}")),
    }
}

/// Windows: tear down the WinFSP volume via Win32 (#249).
///
/// Two APIs, one per mountpoint form:
///   - `DefineDosDeviceW(DDD_REMOVE_DEFINITION, "X:", NULL)` removes
///     the symbolic DOS-device link the WinFSP kernel filter
///     registered for drive-letter mounts. **No** trailing backslash.
///   - `DeleteVolumeMountPointW("C:\\mnt\\foo\\")` removes the
///     reparse-point volume mount from an NTFS directory. Trailing
///     backslash required.
///
/// Win32 error codes that mean "already gone" are surfaced as
/// `tracing::debug!` and Ok (signal/exit may have torn it down
/// between our stat and our API call — not a real failure).
///
/// **Caveat (R1):** the DOS device is owned by the *mount* process.
/// `mntrs unmount` from a different process will get
/// `ERROR_ACCESS_DENIED` until that mount process is stopped. The
/// cross-process unmount fix (mount process listens for an unmount
/// signal and calls `FspFileSystemRemoveMountPoint` itself) is
/// tracked separately.
///
/// Issue #315: the R1 access-denied path is now treated as a USER
/// ERROR rather than silent success. Pre-fix we logged
/// `tracing::debug!` and returned Ok(()) — scripts that chained
/// `mntrs unmount V: && mount-another V:` raced silently: the
/// `&&` branch ran because unmount succeeded, but V: was still
/// owned by the original mount process, so the second mount failed
/// downstream with no actionable diagnostic. Now we eprintln! a
/// warning naming the owning PID (looked up from the mounts db) and
/// return Err so the script's `&&` short-circuits correctly.
#[cfg(windows)]
fn fuse_unmount(mountpoint: &str) -> Result<()> {
    use windows::Win32::Storage::FileSystem::{
        DDD_REMOVE_DEFINITION, DefineDosDeviceW, DeleteVolumeMountPointW,
    };
    use windows::core::PCWSTR;

    if is_drive_letter(mountpoint) {
        // "X:" or "X:\" → normalise to "X:" (no trailing slash for
        // DefineDosDeviceW).
        let name: String = mountpoint[..2].to_ascii_uppercase();
        let wname: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
        // SAFETY: wname is null-terminated UTF-16; DDD_REMOVE_DEFINITION
        // takes no target (NULL). The Win32 call returns
        // Result<(), windows_result::Error> in the windows 0.61 crate
        // (the legacy BOOL wrapper was removed); `e.code().0` gives
        // the raw WIN32_ERROR u32.
        tracing::debug!(mountpoint, dos_name=%name, "fuse_unmount(windows): drive-letter branch -> DefineDosDeviceW(DDD_REMOVE_DEFINITION)");
        let res = unsafe {
            DefineDosDeviceW(
                DDD_REMOVE_DEFINITION,
                PCWSTR(wname.as_ptr()),
                PCWSTR::null(),
            )
        };
        tracing::debug!(
            mountpoint,
            ok = res.is_ok(),
            "fuse_unmount(windows): DefineDosDeviceW returned"
        );
        match res {
            Ok(()) => {
                eprintln!("unmounted {mountpoint}");
                return Ok(());
            }
            Err(e) => {
                let code = e.code().0;
                // 2 = ERROR_FILE_NOT_FOUND (drive letter isn't
                // registered — already gone — idempotent Ok).
                if code == 2 {
                    tracing::debug!(mountpoint, "drive letter already absent");
                    return Ok(());
                }
                // 5 / 0x80070005 = ERROR_ACCESS_DENIED (R1: another
                // mntrs process owns the DOS device). Surface to the
                // user as an actionable error rather than silently
                // succeeding — pre-fix this was tracing::debug! +
                // Ok(()) which made scripts that chained
                // `unmount V: && mount-another V:` race silently.
                if code == 5 || code == (0x80070005_u32 as i32) {
                    // Look up the owning PID from the mounts db —
                    // the entry was written by the mount process with
                    // std::process::id() at record_mount time. Empty
                    // PID means the writer crashed before capturing
                    // it (Bug 23 path); fall back to a PID-less
                    // warning rather than fabricating one.
                    let owner_pid = crate::cmd::mount::read_mounts()
                        .iter()
                        .find(|m| m.mountpoint.eq_ignore_ascii_case(&name))
                        .map(|m| m.pid.clone())
                        .unwrap_or_default();
                    if owner_pid.is_empty() {
                        eprintln!(
                            "warning: {mountpoint} is owned by another mntrs process; \
                             stop it and retry (no PID recorded in mounts db)"
                        );
                    } else {
                        eprintln!(
                            "warning: {mountpoint} is owned by another mntrs process (pid {owner_pid}); \
                             stop it (e.g. `taskkill /F /PID {owner_pid}`) and retry"
                        );
                    }
                    return Err(anyhow!(
                        "{mountpoint} is owned by another mntrs process (pid {})",
                        if owner_pid.is_empty() {
                            "<unknown>".to_string()
                        } else {
                            owner_pid
                        }
                    ));
                }
                return Err(anyhow!(
                    "DefineDosDeviceW failed for {mountpoint} (win32 err = {code})"
                ));
            }
        }
    }

    // NTFS directory mount: ensure trailing backslash per MSDN.
    let name: String = if mountpoint.ends_with('\\') {
        mountpoint.to_string()
    } else {
        format!("{mountpoint}\\")
    };
    let wname: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
    tracing::debug!(mountpoint, win32_path=%name, "fuse_unmount(windows): NTFS-path branch -> DeleteVolumeMountPointW");
    // SAFETY: wname is null-terminated UTF-16.
    let res = unsafe { DeleteVolumeMountPointW(PCWSTR(wname.as_ptr())) };
    tracing::debug!(
        mountpoint,
        ok = res.is_ok(),
        "fuse_unmount(windows): DeleteVolumeMountPointW returned"
    );
    match res {
        Ok(()) => {
            eprintln!("unmounted {mountpoint}");
            Ok(())
        }
        Err(e) => {
            let code = e.code().0;
            // 2 = ERROR_FILE_NOT_FOUND; 0x80071126 =
            // ERROR_NOT_A_REPARSE_POINT — the directory isn't
            // currently mounted (race with signal/exit cleanup).
            if code == 2 || code == (0x80071126_u32 as i32) {
                tracing::debug!(mountpoint, win32_err = code, "mount point already absent");
                Ok(())
            } else {
                Err(anyhow!(
                    "DeleteVolumeMountPointW failed for {mountpoint} (win32 err = {code})"
                ))
            }
        }
    }
}

/// True iff `s` is a Windows drive-letter mountpoint: 1 ASCII letter + colon,
/// optional trailing backslash. Anything longer (path-like, UNC, etc.)
/// returns false so the caller routes to the NTFS branch.
///
/// `cfg(windows)` only — only meaningful when the Win32 branch exists.
/// Test coverage stays on Windows where the fix itself matters; the helper
/// is too small to need a cross-platform test matrix.
#[cfg(windows)]
fn is_drive_letter(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() < 2 || bytes.len() > 3 {
        return false;
    }
    if !bytes[0].is_ascii_alphabetic() || bytes[1] != b':' {
        return false;
    }
    if bytes.len() == 3 && bytes[2] != b'\\' {
        return false;
    }
    true
}

#[cfg(all(windows, test))]
mod tests {
    use super::is_drive_letter;

    #[test]
    fn is_drive_letter_accepts_canonical_forms() {
        assert!(is_drive_letter("X:"));
        assert!(is_drive_letter("x:"));
        assert!(is_drive_letter("X:\\"));
        assert!(is_drive_letter("Z:\\"));
        assert!(is_drive_letter("C:"));
    }

    #[test]
    fn is_drive_letter_rejects_paths_and_garbage() {
        assert!(!is_drive_letter(""));
        assert!(!is_drive_letter("X"));
        assert!(!is_drive_letter("XX:"));
        assert!(!is_drive_letter("1:"));
        assert!(!is_drive_letter(":X"));
        assert!(!is_drive_letter("X:\\foo"));
        assert!(!is_drive_letter("C:\\mnt\\s3"));
        assert!(!is_drive_letter("X:\\\\"));
        assert!(!is_drive_letter("X:/"));
    }

    /// #315 idempotent contract: `unmount` on a target that is
    /// not a drive letter, not on disk, and not in the mounts db
    /// must return Ok (matches `umount` on Linux / `diskpart remove`
    /// on Windows). Pre-fix this returned Err("mount point ... does
    /// not exist"), breaking any `unmount && remount` script.
    ///
    /// The unique 39-char prefix `/_mntrs_idem_315_test_unique_X`
    /// makes the test independent of the developer's real mounts db:
    /// it's a path that's never a drive letter, never on disk, never
    /// the storage URL of a real mount, but also unique enough to
    /// survive a stray leftover from a prior failed test run.
    #[test]
    fn unmount_nonexistent_target_is_idempotent_ok() {
        let target = "Z:\\__mntrs_idem_315_test_unique_unlikely_path__\\foo";
        // Drive-letter shortcut branch: Z:\foo\... is NOT a drive
        // letter (it has more than 3 chars), so we fall through to
        // Path::exists -> mounts.db lookup -> Ok. Verify Ok.
        let r = super::unmount(target);
        assert!(
            r.is_ok(),
            "expected idempotent Ok for nonexistent target, got: {r:?}"
        );
    }
}
