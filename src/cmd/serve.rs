//! mntrs serve — expose storage as local HTTP server (read-only).

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;

use anyhow::Result;

/// Start a read-only HTTP server exposing the given storage backend.
pub fn serve(storage_url: &str, opts: &HashMap<String, String>, port: u16) -> Result<()> {
    let op = Arc::new(crate::cmd::mount::build_operator_sync(storage_url, opts)?);

    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr)?;
    println!("mntrs serve listening on http://{addr}");
    println!("Storage: {storage_url}");

    for stream in listener.incoming() {
        let op = op.clone();
        std::thread::spawn(move || {
            let mut stream = stream.unwrap();
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let req = String::from_utf8_lossy(&buf);

            // Parse GET /path HTTP/1.1
            let path = req
                .lines()
                .next()
                .and_then(|l| l.split_whitespace().nth(1))
                .unwrap_or("/");
            let path = path.trim_start_matches('/');

            let rt = tokio::runtime::Runtime::new().unwrap();
            match rt.block_on(async { op.read(path).await }) {
                Ok(data) => {
                    let len = data.len();
                    let ct = "application/octet-stream";
                    let header = format!(
                        "HTTP/1.0 200 OK\r\nContent-Type: {ct}\r\nContent-Length: {len}\r\nAccess-Control-Allow-Origin: *\r\n\r\n"
                    );
                    let _ = stream.write_all(header.as_bytes());
                    let _ = stream.write_all(&data.to_bytes());
                }
                Err(_) => {
                    let _ = stream.write_all(b"HTTP/1.0 404 Not Found\r\n\r\n");
                }
            }
        });
    }
    Ok(())
}
