use anyhow::{Result, anyhow};
use std::process::Command;

pub fn list() -> Result<()> {
    let output = Command::new("mount")
        .output()
        .map_err(|e| anyhow!("failed to list mounts: {e}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut found = false;

    for line in stdout.lines() {
        // match our mntrs processes: look for our binary name in mount source
        if !line.contains(" type fuse") { continue; }
        found = true;
        if let Some(idx) = line.find(" on ") {
            let rest = &line[idx + 4..];
            if let Some(idx2) = rest.find(" type ") {
                let mp = rest[..idx2].to_string();
                let storage = line.split(' ').next().unwrap_or("?");
                println!("{:40} {}", storage, mp);
            }
        }
    }

    if !found {
        println!("no active fuse mounts");
    }
    Ok(())
}
