use anyhow::{Result, anyhow};
use std::fs;

pub fn list() -> Result<()> {
    let content = fs::read_to_string("/tmp/mntrs-mounts.txt").unwrap_or_default();
    if content.trim().is_empty() {
        println!("no active mntrs mounts");
        return Ok(());
    }
    for line in content.lines() {
        if let Some(idx) = line.find(' ') {
            let storage = &line[..idx];
            let mountpoint = &line[idx+1..];
            println!("{:40} {}", storage, mountpoint);
        }
    }
    Ok(())
}
