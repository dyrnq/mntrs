use anyhow::{Result, anyhow};
use std::path::Path;
use std::process::Command;
use std::fs;

pub fn unmount(target: &str) -> Result<()> {
    let mounts = crate::cmd::mount::read_mounts();

    if target == "all" || target == "-a" {
        if mounts.is_empty() {
            return Err(anyhow!("no mntrs mounts found"));
        }
        for m in &mounts { let mountpoint = &m.mountpoint;
            eprintln!("unmounting {mountpoint}");
            if let Err(e) = fuse_unmount(mountpoint) { tracing::debug!(error=%e, mountpoint, "unmount all skip failed"); }
        }
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
        if let Err(e) = fs::remove_file(format!("{}/.local/share/mntrs/mounts.txt", home)) { tracing::debug!(error=%e, "unmount all db remove failed"); }
        return Ok(());
    }

    let mountpoint = if Path::new(target).exists() {
        target.to_string()
    } else {
        // try to match by storage URL
        if let Some(m) = mounts.iter().find(|m| m.storage == target) {
            m.mountpoint.clone()
        } else {
            return Err(anyhow!("mount point '{}' does not exist", target));
        }
    };

    fuse_unmount(&mountpoint)?;

    // remove from db
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    let db = format!("{}/.local/share/mntrs/mounts.txt", home);
    if let Ok(content) = fs::read_to_string(&db) {
        let filtered: Vec<&str> = content.lines()
            .filter(|l| !l.contains(&mountpoint))
            .collect();
        if let Err(e) = fs::write(&db, filtered.join("\n")) { tracing::debug!(error=%e, "unmount db cleanup failed"); }
    }
    Ok(())
}

fn fuse_unmount(mountpoint: &str) -> Result<()> {
    let result = Command::new("fusermount3")
        .arg("-u")
        .arg(mountpoint)
        .status()
        .or_else(|_| {
            Command::new("fusermount").arg("-u").arg(mountpoint).status()
        });

    match result {
        Ok(status) if status.success() => {
            eprintln!("unmounted {mountpoint}");
            Ok(())
        }
        Ok(status) => {
            Err(anyhow!("fusermount failed with exit code {}", status.code().unwrap_or(-1)))
        }
        Err(e) => Err(anyhow!("failed to run fusermount: {e}")),
    }
}
