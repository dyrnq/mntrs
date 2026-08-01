//! Issue #485 — tests for the `symlinks_lower` case-insensitive
//! secondary index on `MntrsFs`.
//!
//! The three case-insensitive scan sites
//! (`batch_lookup_from_dir_cache`, `fn lookup`, `fn unlink`)
//! delegate to two private helpers:
//!
//! - `lookup_symlink_key_case_insensitive(full_path) -> Option<String>`
//! - `lookup_symlink_entry_case_insensitive(full_path) -> Option<(String, PathBuf)>`
//!
//! Each helper tries the O(1) `symlinks_lower.get(...)` first
//! and falls back to a linear `symlinks` scan that **also
//! populates the index** (self-healing). A
//! `#[cfg(test)] symlink_scan_count: AtomicUsize` on `MntrsFs`
//! is incremented on every fallback-loop iteration; tests
//! drive the helpers via the `__symlink_index_diag` shim and
//! the public `CoreFilesystem` API.
//!
//! These tests run on Unix only — the `__symlink_index_diag`
//! shim is `#[cfg(test)]` and the index exists on every
//! platform, but the WinFSP symlink code path is platform-
//! specific. Tests that exercise the public API use the
//! `CoreFilesystem::symlink` / `unlink` trait methods, which
//! on Unix run the fuser adapter path that exercises the same
//! helpers.

use mntrs::core_fs::CoreFilesystem;
use mntrs::new_test_fs;
use opendal::services::Memory;

fn make_fs() -> mntrs::MntrsFs {
    let op = opendal::Operator::new(Memory::default()).unwrap();
    let dir =
        std::env::temp_dir().join(format!("mntrs-symlinks-index-test-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    new_test_fs(op, dir)
}

/// `full_path` for a top-level symlink (parent = root, ino 1).
fn root_full_path(name: &str) -> String {
    name.to_string()
}

/// `lookup_symlink_key_case_insensitive` with a `full_path`
/// that doesn't exist returns `None` and does NOT touch the
/// fallback loop (counter stays 0).
#[test]
fn symlink_index_miss_returns_none_without_scan() {
    let fs = make_fs();
    let (_symlinks, _lower, count) = fs.__symlink_index_diag();
    assert_eq!(count, 0, "fresh fs starts with zero scan count");
    let result = fs.__symlink_lookup_key("nope");
    assert!(result.is_none());
    let (_, _, count_after) = fs.__symlink_index_diag();
    assert_eq!(
        count_after, 0,
        "miss must not increment the scan-fallback counter"
    );
}

/// Insert via `CoreFilesystem::symlink` (the public API path
/// that includes the `symlinks_lower` mirror), then verify the
/// helper finds the entry on **both** the exact-case key and
/// a different-case key. Fast path expected: counter stays 0.
#[test]
fn symlink_index_lowercase_stored_uppercase_lookup() {
    let fs = make_fs();
    // parent = root (ino 1). Create a symlink stored as
    // lowercase "link" pointing at a relative target.
    fs.symlink(1, "link", std::path::Path::new("target"))
        .expect("symlink create should succeed");

    // Exact-case lookup: still O(1) — the secondary index has
    // the lowercased key but we ask for it directly.
    let exact = fs.__symlink_lookup_key(&root_full_path("link"));
    assert_eq!(exact.as_deref(), Some("link"));

    // Mixed-case lookup (uppercase "Link"): the helper
    // lowercases the query and hits the secondary index.
    let mixed = fs.__symlink_lookup_key(&root_full_path("Link"));
    assert_eq!(
        mixed.as_deref(),
        Some("link"),
        "case-insensitive lookup must resolve to the canonical stored key"
    );

    // Different-case query that mixes basename + path:
    // "LINK" → "link".
    let upper = fs.__symlink_lookup_key(&root_full_path("LINK"));
    assert_eq!(upper.as_deref(), Some("link"));

    // The exact-case entry version (with target) also works.
    let entry = fs.__symlink_lookup_entry(&root_full_path("Link"));
    let (stored, target) = entry.expect("entry helper must find the symlink");
    assert_eq!(stored, "link");
    assert_eq!(target, std::path::PathBuf::from("target"));

    // Counter must stay at 0 — every lookup was a fast-path
    // hit on `symlinks_lower`.
    let (_symlinks, lower, count) = fs.__symlink_index_diag();
    assert_eq!(
        count, 0,
        "fast-path hit must not increment scan-fallback counter (lower={lower})"
    );
    assert_eq!(
        lower, 1,
        "exactly one secondary-index entry for the one symlink"
    );
}

/// Inverse direction: the canonical stored key has uppercase
/// characters. `symlink` stores whatever case the caller
/// passes — there is no automatic lowercase normalisation.
/// A lookup in different case must still resolve via the
/// secondary index. Same O(1) fast-path; same counter
/// assertion.
#[test]
fn symlink_index_mixed_case_stored() {
    let fs = make_fs();
    fs.symlink(1, "MixedCase", std::path::Path::new("target"))
        .expect("symlink create");

    // All case variants must resolve to the canonical stored
    // key verbatim (the secondary index stores the lowercase
    // query and the canonical stored value).
    for case in ["MixedCase", "mixedcase", "MIXEDCASE", "MiXeDcAsE"] {
        let r = fs.__symlink_lookup_key(&root_full_path(case));
        assert_eq!(
            r.as_deref(),
            Some("MixedCase"),
            "lookup {case:?} must resolve to canonical stored key"
        );
    }

    let (_, _, count) = fs.__symlink_index_diag();
    assert_eq!(
        count, 0,
        "fast-path hit for every case variant — no fallback"
    );
}

/// Mixed-case PARENT path: store `SubDir/link`, lookup with
/// `SUBDIR/LINK`. Confirms `to_lowercase()` runs over the
/// entire path, not just the basename.
#[test]
fn symlink_index_mixed_case_parent_path() {
    let fs = make_fs();
    // Create the parent dir with mixed case (mkdir + lookup
    // round-trip through the public API so the parent ino
    // exists for symlink to attach to).
    let parent_attr = fs.mkdir(1, "SubDir").expect("mkdir SubDir");
    let parent_ino = parent_attr.ino;
    assert!(parent_ino > 1, "subdir gets a non-root ino");

    // Create the symlink under it.
    fs.symlink(parent_ino, "LinkName", std::path::Path::new("target"))
        .expect("symlink create");

    // Lookup via the helper with a different-case parent path.
    // Helper lowercases the full string, so this hits the
    // `SubDir/LinkName` secondary-index key.
    let mixed_parent = fs.__symlink_lookup_key("SUBDIR/LINKNAME");
    assert_eq!(
        mixed_parent.as_deref(),
        Some("SubDir/LinkName"),
        "case-insensitive lookup over mixed-case parent must resolve"
    );

    let (_s, _l, count) = fs.__symlink_index_diag();
    assert_eq!(count, 0);
}

/// Self-healing fallback: drop the secondary-index entry
/// directly (simulating a future maintainer who adds an
/// insert site without mirroring), then verify the helper
/// still finds the entry via the linear scan AND populates
/// the secondary index as a side effect.
#[test]
fn symlink_index_self_healing_fallback() {
    let fs = make_fs();
    fs.symlink(1, "link", std::path::Path::new("target"))
        .expect("symlink create");

    // Sanity: secondary index has the entry, counter is 0.
    let (_s, lower_before, count_before) = fs.__symlink_index_diag();
    assert_eq!(lower_before, 1);
    assert_eq!(count_before, 0);

    // Simulate drift: remove the secondary-index entry but
    // keep `symlinks` populated.
    fs.__symlinks_lower_remove_for_test(&"link".to_lowercase());
    let (_s, lower_after_remove, _) = fs.__symlink_index_diag();
    assert_eq!(lower_after_remove, 0);

    // Helper now falls back to linear scan, finds the entry,
    // and re-populates the secondary index.
    let result = fs.__symlink_lookup_key("Link");
    assert_eq!(result.as_deref(), Some("link"));
    let (_s, lower_after_heal, count_after) = fs.__symlink_index_diag();
    assert_eq!(
        lower_after_heal, 1,
        "self-healing re-populates the secondary index"
    );
    assert_eq!(
        count_after, 1,
        "fallback loop ran exactly once (single matching entry)"
    );

    // Subsequent lookups are O(1) again — counter stays at 1.
    let _ = fs.__symlink_lookup_key("LINK");
    let (_, _, count_repeat) = fs.__symlink_index_diag();
    assert_eq!(
        count_repeat, 1,
        "second lookup uses the re-populated fast path"
    );
}

/// Insert / remove mirroring: after `CoreFilesystem::unlink`,
/// both `symlinks` AND `symlinks_lower` are empty.
/// Mirrors the dual-branch remove contract at `MntrsFs::unlink`.
#[test]
fn symlink_index_unlink_clears_mirror() {
    let fs = make_fs();
    fs.symlink(1, "MixedCase", std::path::Path::new("target"))
        .expect("symlink create");

    // Pre-unlink: both maps populated.
    let (s_before, l_before, _) = fs.__symlink_index_diag();
    assert_eq!(s_before, 1);
    assert_eq!(l_before, 1);

    // Unlink with a different-case name to exercise the
    // dual-branch remove (the lowercased-name /
    // uppercased-kernel pair).
    fs.unlink(1, "MIXEDCASE").expect("unlink should succeed");

    // Post-unlink: both maps empty.
    let (s_after, l_after, _) = fs.__symlink_index_diag();
    assert_eq!(s_after, 0, "symlinks drained by unlink");
    assert_eq!(
        l_after, 0,
        "symlinks_lower mirrored by the dual-branch remove"
    );

    // A subsequent lookup confirms the entry is gone.
    let r = fs.__symlink_lookup_key("mixedcase");
    assert!(r.is_none());
}
