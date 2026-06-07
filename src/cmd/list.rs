use anyhow::Result;

pub fn list() -> Result<()> {
    let mounts = crate::cmd::mount::read_mounts();
    if mounts.is_empty() {
        println!("no active mntrs mounts");
        return Ok(());
    }
    for (storage, mountpoint) in &mounts {
        println!("{:40} {}", storage, mountpoint);
    }
    Ok(())
}
