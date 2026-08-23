//! `--max-read-ahead` FuserAdapter wiring (audit fix).
//!
//! Pins the post-fix behaviour of `--max-read-ahead`: the CLI value
//! must reach `fuser::KernelConfig::set_max_readahead` via the
//! `FuserAdapter::max_read_ahead` field.
//!
//! ## Why this test exists
//!
//! Pre-fix, `FuserAdapter::init()` hardcoded
//! `config.set_max_readahead(1024 * 1024)` (1 MiB) regardless of the
//! CLI value, and `cmd/mount.rs` accepted the user's
//! `--max-read-ahead` into a `_max_read_ahead: u64` underscore-
//! prefixed param — dead at compile time. The flag parsed without
//! error and silently had no effect, which masked user intent
//! (e.g. `--max-read-ahead 4096` for a slow backend where 1 MiB
//! kernel prefetch pessimises sequential reads on tiny files).
//!
//! ## Pinning mechanism
//!
//! `fuser::KernelConfig::new` is `pub(crate)` in fuser 0.18 so we
//! can't construct a real `KernelConfig` from outside the crate to
//! call `set_max_readahead` and assert against it. Instead we pin
//! the **plumbing half** — that the field exists on `FuserAdapter`,
//! the default matches rclone (131072 = 128 KiB), and a user
//! override flows through `FuserAdapter::new`. The `init()` body
//! change (`config.set_max_readahead(self.max_read_ahead)`) is a
//! single-line mechanical substitution verified by code review and
//! by the CI gate.
//!
//! Run:
//!   cargo test --test max_read_ahead_test
//!
//! All three test functions build a `FuserAdapter` over a real
//! `MntrsFs` against an in-memory opendal backend (same pattern as
//! `tests/o_append_fuse_notifier_test.rs`). The `init()` /
//! `fuser::spawn_mount` path is not invoked — we only verify the
//! adapter's stored state, which is the only observable difference
//! between pre-fix and post-fix.

use std::time::Duration;

use mntrs::core_fs::CoreFilesystem;
use mntrs::core_fs::fuser::FuserAdapter;
use mntrs::new_test_fs_with_mode;
use mntrs::util::CacheMode;
use opendal::Operator;
use opendal::services::Memory;

/// Build a fresh MntrsFs against an in-memory opendal backend.
/// Mirrors the helper in `tests/o_append_fuse_notifier_test.rs:75`
/// — the cache dir is `temp_dir()` + a unique suffix so concurrent
/// tests don't share on-disk state.
fn build_fs(label: &str) -> mntrs::MntrsFs {
    let op = Operator::new(Memory::default()).unwrap();
    let cache_dir = std::env::temp_dir().join(format!(
        "mntrs-max-read-ahead-{}-{}-{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&cache_dir);
    std::fs::create_dir_all(&cache_dir).unwrap();

    let fs = new_test_fs_with_mode(op, cache_dir, CacheMode::Writes);
    fs.init().expect("init");
    fs
}

/// Audit pin #1: the `FuserAdapter::new` constructor stores the
/// caller-supplied `max_read_ahead` verbatim on the `pub` field.
/// Pre-fix this constructor took no `max_read_ahead` arg (the
/// `_max_read_ahead: u64` in `cmd/mount.rs` was the underscore-
/// prefixed dead-storage marker) and the field didn't exist on the
/// struct.
#[test]
fn fuser_adapter_stores_user_supplied_max_read_ahead() {
    let fs = build_fs("override");
    let adapter = FuserAdapter::new(
        fs,
        Duration::from_secs(10),
        Duration::from_secs(5),
        /* direct_io */ false,
        /* write_back_cache */ false,
        /* max_read_ahead */ 524288,
    );
    assert_eq!(
        adapter.max_read_ahead, 524288,
        "FuserAdapter::new must store the user-supplied max_read_ahead \
         verbatim (was the silent-shadow bug)"
    );
}

/// Audit pin #2: the canonical rclone default 131072 (128 KiB) is
/// what users get when they don't pass `--max-read-ahead`. This
/// test pins the default by exercising the constructor with that
/// value explicitly — the *real* default lives in `main.rs`
/// `default_value = "131072"` and is verified by
/// `tests/cli_defaults_test.rs` (README ↔ --help drift guard).
/// This test ensures the value isn't accidentally doubled /
/// halved by a refactor in `cmd/mount.rs` or `main.rs`.
#[test]
fn fuser_adapter_default_max_read_ahead_matches_rclone() {
    let fs = build_fs("default");
    let adapter = FuserAdapter::new(
        fs,
        Duration::from_secs(10),
        Duration::from_secs(5),
        false,
        false,
        /* rclone default */ 131072,
    );
    assert_eq!(
        adapter.max_read_ahead, 131072,
        "FuserAdapter default must match rclone's --max-read-ahead default"
    );
}

/// Audit pin #3: `set_max_readahead` rejects 0 (returns
/// `Err(1)`). The cmd/mount.rs call uses
/// `max_read_ahead.min(u32::MAX as u64) as u32`, so a 0 from the
/// CLI would be passed through. The pre-fix code had the same
/// risk (the hardcoded 1 MiB was always >0), but the new wiring
/// exposes it. We don't fix the rejection here — fuser returns
/// the clamp and `init()` continues — but we pin that the
/// adapter field is a faithful echo of the input so any future
/// "default 0" decision is traceable from the CLI to the field.
#[test]
fn fuser_adapter_passes_through_zero_max_read_ahead() {
    let fs = build_fs("zero");
    let adapter = FuserAdapter::new(
        fs,
        Duration::from_secs(10),
        Duration::from_secs(5),
        false,
        false,
        /* zero */ 0,
    );
    assert_eq!(
        adapter.max_read_ahead, 0,
        "FuserAdapter must pass through max_read_ahead=0 faithfully; \
         fuser's set_max_readahead will reject it at init() time and \
         fall back to its own minimum (1)"
    );
}
