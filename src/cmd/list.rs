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
        // Bug 23: defensive display fallback in case a
        // future writer ever lands a MountInfo with an
        // empty pid past read_mounts's filter (or a
        // refactor drops the filter). "?" keeps the
        // table column visually present so an operator
        // notices the missing data instead of seeing
        // empty space.
        let pid_display = if m.pid.is_empty() { "?" } else { &m.pid };
        let user_display = if m.user.is_empty() { "?" } else { &m.user };
        println!(
            "{:40} {:30} {:>8} {:10} {:4} {:8}",
            m.storage,
            m.mountpoint,
            pid_display,
            user_display,
            if m.read_only { "ro" } else { "rw" },
            m.backend
        );
    }
    Ok(())
}
