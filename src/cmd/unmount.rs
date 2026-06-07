use anyhow::{Result, anyhow};
use std::path::Path;
use std::process::Command;
use std::fs;

pub fn unmount(target: &str) -> Result<()> {
    if target == "all" || target == "-a" {
        let content = fs::read_to_string("/tmp/mntrs-mounts.txt").unwrap_or_default();
        if content.trim().is_empty() {
            return Err(anyhow!("no mntrs mounts found"));
        }
        for line in content.lines() {
            if let Some(idx) = line.find(' ') {
                let mountpoint = &line[idx+1..];
                eprintln!("unmounting {mountpoint}");
                let _ = fuse_unmount(mountpoint);
            }
        }
        let _ = fs::remove_file("/tmp/mntrs-mounts.txt");
        return Ok(());
    }

    let path = Path::new(target);
    if !path.exists() {
        return Err(anyhow!("mount point '{}' does not exist", target));
    }
    fuse_unmount(target)?;

    // remove from mounts db
    if let Ok(content) = fs::read_to_string("/tmp/mntrs-mounts.txt") {
        let filtered: Vec<&str> = content.lines()
            .filter(|l| !l.ends_with(target))
            .collect();
        let _ = fs::write("/tmp/mntrs-mounts.txt", filtered.join("\n"));
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
