use anyhow::{Result, anyhow};
use std::path::Path;
use std::process::Command;

pub fn unmount(target: &str) -> Result<()> {
    if target == "all" || target == "-a" {
        let output = Command::new("mount")
            .output()
            .map_err(|e| anyhow!("failed to list mounts: {e}"))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut found = false;
        for line in stdout.lines() {
            if !line.contains(" fuse.") { continue; }
            if let Some(idx) = line.find(" on ") {
                let rest = &line[idx + 4..];
                if let Some(idx2) = rest.find(" type ") {
                    let mp = rest[..idx2].to_string();
                    eprintln!("unmounting {mp}");
                    let _ = fuse_unmount(&mp);
                    found = true;
                }
            }
        }
        if !found { return Err(anyhow!("no active fuse mounts found")); }
        return Ok(());
    }

    let path = Path::new(target);
    if !path.exists() {
        return Err(anyhow!("mount point '{}' does not exist", target));
    }
    fuse_unmount(target)
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
