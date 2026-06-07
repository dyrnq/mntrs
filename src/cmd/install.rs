use anyhow::Result;

pub fn systemd() -> Result<()> {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    let service_dir = format!("{}/.config/systemd/user", home);
    let service_file = format!("{}/mntrs-mount@.service", service_dir);
    
    let template = r#"[Unit]
Description=mntrs FUSE mount for %i
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/mntrs mount %i /mnt/%i
ExecStop=/usr/bin/fusermount3 -u /mnt/%i
Restart=no
Environment=HOME=%h
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=default.target
"#;

    std::fs::create_dir_all(&service_dir)?;
    std::fs::write(&service_file, template)?;
    
    println!("Wrote systemd user service template: {service_file}");
    println!();
    println!("Usage:");
    println!("  systemctl --user daemon-reload");
    println!("  systemctl --user enable --now mntrs-mount@s3://bucket.service");
    println!("  systemctl --user status mntrs-mount@s3://bucket.service");
    
    Ok(())
}
