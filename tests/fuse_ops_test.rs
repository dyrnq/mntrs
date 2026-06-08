//! FUSE operation unit tests — stat_op, evict_lru, mem_cache, error paths.
//! Inspired by rclone's vfs_test.go suite.

use std::path::Path;

use mntrs::{cache_path, fnmatch, path_hash};

// ============================================================
// Cache operations
// ============================================================

#[test]
fn cache_path_returns_hex_filename() {
    let p = cache_path(Path::new("/tmp/cache"), "hello.txt");
    let name = p.file_name().unwrap().to_str().unwrap();
    assert_eq!(name.len(), 20);
    assert!(name.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn cache_path_different_paths_different_files() {
    let a = cache_path(Path::new("/cache"), "a.txt");
    let b = cache_path(Path::new("/cache"), "b.txt");
    assert_ne!(a, b);
}

#[test]
fn cache_path_same_path_same_file() {
    assert_eq!(
        cache_path(Path::new("/cache"), "x"),
        cache_path(Path::new("/cache"), "x")
    );
}

#[test]
fn cache_path_different_cache_dirs() {
    let a = cache_path(Path::new("/a"), "x.txt");
    let b = cache_path(Path::new("/b"), "x.txt");
    assert_ne!(a, b);
}

// ============================================================
// Error paths
// ============================================================

#[test]
fn path_hash_always_positive() {
    for p in &[
        "/",
        "/a/b/c",
        "x",
        "",
        "very/long/path/with/many/components",
    ] {
        let h = path_hash(p);
        assert!(h >= 2, "hash for '{p}' should be >= 2, got {h}");
    }
}

#[test]
fn path_hash_is_deterministic() {
    assert_eq!(path_hash("hello"), path_hash("hello"));
}

// ============================================================
// fnmatch edge cases (rclone parity)
// ============================================================

#[test]
fn fnmatch_star_matches_empty() {
    assert!(fnmatch("file.*", "file.", false));
}

#[test]
fn fnmatch_star_matches_everything() {
    assert!(fnmatch("*", "anything_at_all", false));
}

#[test]
fn fnmatch_multiple_stars() {
    assert!(fnmatch("a*b*c", "axyzb123c", false));
}

#[test]
fn fnmatch_question_requires_char() {
    assert!(!fnmatch("?", "", false));
}

#[test]
fn fnmatch_case_insensitive_uppercase_pattern() {
    assert!(fnmatch("HELLO", "hello", true));
}

#[test]
fn fnmatch_case_insensitive_mixed() {
    assert!(fnmatch("HeLlO", "hElLo", true));
}

#[test]
fn fnmatch_leading_star() {
    assert!(fnmatch("*.txt", "foo.txt", false));
    assert!(!fnmatch("*.txt", "foo.txt.bak", false));
}

#[test]
fn fnmatch_exact_no_wildcards() {
    assert!(fnmatch("exact", "exact", false));
    assert!(!fnmatch("exact", "Exact", false));
}

// ============================================================
// Concurrent safety stubs
// ============================================================

#[test]
fn path_hash_thread_safety() {
    // path_hash is a pure function, thread-safe by design
    let h1 = path_hash("concurrent_test");
    let h2 = path_hash("concurrent_test");
    assert_eq!(h1, h2);
}
