use anyhow::Result;

pub fn systemd() -> Result<()> {
    // Issue #261.4: use XDG config helper. If XDG_CONFIG_HOME/HOME
    // both unset, fail with a clear error instead of writing to /tmp
    // (which would silently collide across users/pods).
    let config_dir =
        crate::util::config_dir().map_err(|e| anyhow::anyhow!("systemd install failed: {e}"))?;
    let service_dir = config_dir.join("systemd").join("user");
    let service_file = service_dir.join("mntrs-mount@.service");

    let template = r#"[Unit]
Description=mntrs FUSE mount for %i
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/mntrs mount %i /mnt/%i
ExecStop=/usr/bin/fusermount3 -u /mnt/%i
ExecStopPost=/usr/bin/fusermount3 -uz /mnt/%i
Restart=always
RestartSec=5s
Environment=HOME=%h
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=default.target
"#;

    std::fs::create_dir_all(&service_dir)?;
    std::fs::write(&service_file, template)?;

    println!(
        "Wrote systemd user service template: {}",
        service_file.display()
    );
    println!();
    println!("Usage:");
    println!("  systemctl --user daemon-reload");
    println!("  systemctl --user enable --now mntrs-mount@s3://bucket.service");
    println!("  systemctl --user status mntrs-mount@s3://bucket.service");

    Ok(())
}
