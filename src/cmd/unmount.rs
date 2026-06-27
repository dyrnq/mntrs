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
        for m in &mounts {
            let mountpoint = &m.mountpoint;
            eprintln!("unmounting {mountpoint}");
            tracing::debug!(mountpoint, "unmount: 'all' branch -> fuse_unmount");
            if let Err(e) = fuse_unmount(mountpoint) {
                tracing::debug!(error=%e, mountpoint, "unmount all skip failed");
            }
        }
        // Issue #261.4: same path as mount.rs uses for the db.
        let db = crate::cmd::mount::mounts_db_path();
        tracing::debug!(db, "unmount: 'all' branch -> remove mounts db");
        if let Err(e) = fs::remove_file(&db) {
            tracing::debug!(error=%e, db, "unmount all db remove failed");
        }
        return Ok(());
    }

    tracing::debug!(target, "unmount: dispatching target");
    // Windows: "V:" alone makes `Path::exists()` call `GetFileAttributesW("V:")`
    // which blocks on WinFSP's volume ready-handshake (observed in #249 e2e:
    // hangs for ~60s then returns false even when V: is mounted). Detect the
    // drive-letter form FIRST so we never pay that cost.
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
            return Err(anyhow!("mount point '{}' does not exist", target));
        }
    };
    #[cfg(not(windows))]
    let mountpoint: String = {
        if Path::new(target).exists() {
            target.to_string()
        } else if let Some(m) = mounts.iter().find(|m| m.storage == target) {
            m.mountpoint.clone()
        } else {
            return Err(anyhow!("mount point '{}' does not exist", target));
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
#[cfg(unix)]
fn fuse_unmount(mountpoint: &str) -> Result<()> {
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
/// `tracing::debug!` (signal/exit may have torn it down between our
/// stat and our API call — not a real failure). `ERROR_ACCESS_DENIED`
/// (5) is logged as a debug event too: see R1 caveat below.
///
/// **Caveat (R1):** the DOS device is owned by the *mount* process.
/// `mntrs unmount` from a different process will get
/// `ERROR_ACCESS_DENIED` until that mount process is stopped. The
/// cross-process unmount fix (mount process listens for an unmount
/// signal and calls `FspFileSystemRemoveMountPoint` itself) is
/// tracked separately.
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
                // registered — already gone); 5 / 0x80070005 =
                // ERROR_ACCESS_DENIED (R1: mount process still owns
                // the DOS device — caller must stop it before
                // retrying).
                if code == 2 || code == 5 || code == (0x80070005_u32 as i32) {
                    if code == 2 {
                        tracing::debug!(mountpoint, "drive letter already absent");
                    } else {
                        tracing::debug!(
                            mountpoint,
                            win32_err = code,
                            "drive letter owned by another process; stop the mntrs mount process and retry"
                        );
                    }
                    return Ok(());
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
}
