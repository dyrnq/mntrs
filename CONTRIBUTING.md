# Contributing to mntrs

## Quick Start

```bash
git clone https://github.com/user/mntrs
cd mntrs
cargo build
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

## Project Structure

```
mntrs/
├── src/
│   ├── lib.rs          # FUSE filesystem (MntrsFs), 775 lines
│   ├── main.rs         # CLI entry point (clap)
│   ├── cmd/
│   │   ├── mount.rs    # mount logic, backend builders
│   │   ├── unmount.rs  # unmount logic
│   │   ├── list.rs     # list active mounts
│   │   └── install.rs  # systemd service generator
│   ├── core_fs/        # Platform-independent FS abstraction (WIP)
│   └── path.rs         # Platform path normalization
├── csi/mntrs-csi/      # Kubernetes CSI driver
├── tests/              # Unit tests (29 total)
├── bench/              # Benchmark scripts
├── COMPARE.md          # rclone parameter parity tracker
├── TODO.md             # Completed feature checklist
└── TODO2.md            # Reference project improvement tracker
```

## Testing

```bash
cargo test --workspace                     # All 29 tests
cargo test -p mntrs                        # Core tests only
cargo test -p mntrs-csi                    # CSI tests only
```

## Benchmark

```bash
# Requires rclone mount and MinIO backend
MNTRS_BIN=./target/release/mntrs \
  ENDPOINT=http://localhost:9000 \
  ACCESS_KEY=minioadmin \
  SECRET_KEY=minioadmin \
  BUCKET=test-bucket \
  RCLONE_MNT=/tmp/rclone-mnt \
  bash bench/run_all.sh
```

## Code Style

- `cargo fmt` before every commit
- `cargo clippy --workspace --all-targets -- -D warnings` must pass
- No `unsafe` unless absolutely necessary (prefer `rustix`)
- Use `tracing::debug!` for best-effort error paths, `tracing::warn!` for critical
- `let _ =` only allowed in signal handlers and atexit cleanup

## CI

Three platform matrix on every push:
- Linux: build + test + clippy + fmt + FUSE
- macOS: build + test + clippy + fmt
- Windows: build + test + clippy + fmt

Weekly benchmark against rclone via MinIO service container.

## License

MIT OR Apache-2.0
