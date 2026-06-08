use anyhow::Result;

pub fn list() -> Result<()> {
    let mounts = crate::cmd::mount::read_mounts();
    if mounts.is_empty() {
        println!("no active mntrs mounts");
        return Ok(());
    }
    println!(
        "{:40} {:30} {:>8} {:10} {:4} {:8}",
        "Storage", "Mountpoint", "PID", "User", "Mode", "Type"
    );
    println!("{}", "-".repeat(105));
    for m in &mounts {
        println!(
            "{:40} {:30} {:>8} {:10} {:4} {:8}",
            m.storage,
            m.mountpoint,
            m.pid,
            m.user,
            if m.read_only { "ro" } else { "rw" },
            m.backend
        );
    }
    Ok(())
}
