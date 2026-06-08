fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set PROTOC explicitly for tonic_build
    let protoc = if let Ok(p) = std::env::var("PROTOC") {
        p
    } else if std::path::Path::new("/tmp/protoc/bin/protoc").exists() {
        "/tmp/protoc/bin/protoc".to_string()
    } else {
        "protoc".to_string()
    };
    std::env::set_var("PROTOC", &protoc);
    eprintln!("mntrs-csi build.rs: using protoc={protoc}");

    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    eprintln!("mntrs-csi build.rs: OUT_DIR={}", out_dir.display());

    // Run protoc manually to verify
    let status = std::process::Command::new(&protoc)
        .arg("--version")
        .status()
        .expect("failed to run protoc");
    eprintln!("mntrs-csi build.rs: protoc --version exit={status}");

    tonic_build::compile_protos("../proto/csi.proto")?;
    eprintln!("mntrs-csi build.rs: compile_protos done");

    // List output files
    if let Ok(entries) = std::fs::read_dir(&out_dir) {
        for e in entries.flatten() {
            eprintln!("mntrs-csi build.rs: out: {}", e.path().display());
        }
    }

    Ok(())
}
