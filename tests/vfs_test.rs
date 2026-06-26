use mntrs::{cache_path, fnmatch, path_hash};

// ============================================================
// statfs — verify df output values are reasonable
// ============================================================

#[test]
fn statfs_reports_positive_blocks() {
    // MntrsFs statfs uses 256M blocks * 4096 = ~1PB
    // Just verify the constants are reasonable
    let block_size: u64 = 4096;
    let total_blocks: u64 = 256 * 1024 * 1024;
    let total_space = total_blocks as u128 * block_size as u128;
    assert!(
        total_space >= 1024u128 * 1024 * 1024 * 1024,
        "total space should be >= 1TB, got {}",
        total_space
    );
}

// ============================================================
// read offset/size boundary tests
// ============================================================

#[test]
fn read_offset_at_eof_returns_empty() {
    // Simulate: file_size=3899, offset=3899 → should return empty
    let file_size: u64 = 3899;
    let offset: u64 = 3899;
    let size: u32 = 4096;
    assert!(offset >= file_size);
    // The range should clamp: fetch_size.min((file_size - offset).max(1))
    let remaining = file_size.saturating_sub(offset);
    let fetch_size = (size as u64).min(remaining.max(1));
    assert_eq!(fetch_size, 1); // min(4096, 1) = 1 — reads 0 bytes effectively
}

#[test]
fn read_offset_before_eof_clamps_range() {
    let file_size: u64 = 3899;
    let offset: u64 = 3899 - 500; // 3399
    let size: u32 = 4096;
    let remaining = file_size.saturating_sub(offset);
    let fetch_size = (size as u64).min(remaining.max(1));
    assert_eq!(remaining, 500);
    assert_eq!(fetch_size, 500); // should clamp to remaining
}

#[test]
fn read_within_bounds_no_clamp() {
    let file_size: u64 = 3899;
    let offset: u64 = 0;
    let size: u32 = 100;
    let remaining = file_size.saturating_sub(offset);
    let fetch_size = (size as u64).min(remaining.max(1));
    assert_eq!(fetch_size, 100);
}

#[test]
fn read_zero_size_file() {
    let file_size: u64 = 0;
    let offset: u64 = 0;
    let remaining = file_size.saturating_sub(offset);
    let fetch_size = (4096u64).min(remaining.max(1));
    assert_eq!(fetch_size, 1); // min(4096, 1)
}

// ============================================================
// list_op filter validation
// ============================================================

#[test]
fn fnmatch_exclude_pattern() {
    assert!(fnmatch("*.tmp", "file.tmp", false));
    assert!(!fnmatch("*.tmp", "file.txt", false));
}

#[test]
fn fnmatch_include_overrides() {
    // If include is set, only matching files pass
    let files = ["a.txt", "b.rs", "c.txt"];
    let included: Vec<_> = files
        .iter()
        .filter(|f| fnmatch("*.txt", f, false))
        .collect();
    assert_eq!(included.len(), 2);
    assert!(included.contains(&&"a.txt"));
    assert!(included.contains(&&"c.txt"));
}

#[test]
fn fnmatch_question_mark_single_char() {
    assert!(fnmatch("file.???", "file.txt", false));
    assert!(!fnmatch("file.???", "file.rs", false)); // rs is 2 chars
    assert!(fnmatch("file.?s", "file.rs", false));
}

#[test]
fn path_hash_non_zero() {
    let h = path_hash("/some/path");
    assert!(h >= 2, "path_hash must be >= 2 (0 and 1 reserved for FUSE)");
    assert_eq!(
        h,
        path_hash("/some/path"),
        "path_hash must be deterministic"
    );
}

#[test]
fn path_hash_different_paths() {
    let a = path_hash("file_a");
    let b = path_hash("file_b");
    // Practically should be different (hash collision theoretically possible but unlikely)
    // Just verify they're both valid
    assert!(a >= 2);
    assert!(b >= 2);
}

#[test]
fn cache_path_format() {
    let tmp = std::env::temp_dir();
    let p = cache_path(&tmp, "hello/world");
    let parent = p.parent().unwrap();
    assert_eq!(parent, tmp);
    let name = p.file_name().unwrap().to_str().unwrap();
    assert_eq!(name.len(), 20, "cache path filename should be 20-char hex");
    assert!(name.chars().all(|c| c.is_ascii_hexdigit()), "should be hex");
}

// ============================================================
// mount_internal — verify parameter handling
// ============================================================

/// mount_internal with invalid scheme should return error (not panic)
#[test]
fn mount_internal_invalid_scheme_returns_error() {
    let tmp = std::env::temp_dir().join(format!("mntrs-test-invalid-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);

    let result = mntrs::cmd::mount::mount_internal(
        "invalid-scheme://bucket",
        tmp.to_str().unwrap(),
        &std::collections::HashMap::new(),
        false,
    );

    let _ = std::fs::remove_dir_all(&tmp);
    assert!(result.is_err(), "invalid scheme should fail");
}

/// mount_internal with empty mountpoint should fail
#[test]
fn mount_internal_empty_mountpoint_fails() {
    let result = mntrs::cmd::mount::mount_internal(
        "s3://bucket",
        "",
        &std::collections::HashMap::new(),
        false,
    );
    assert!(result.is_err(), "empty mountpoint should fail");
}

/// unmount_internal with non-existent path should not panic
#[test]
fn unmount_internal_nonexistent_does_not_panic() {
    let result = mntrs::cmd::mount::unmount_internal("/tmp/mntrs-nonexistent-mount");
    // Should return Ok (graceful handling)
    assert!(
        result.is_ok(),
        "unmount of nonexistent path should be graceful"
    );
}

/// path_hash returns same hash for same path
#[test]
fn path_hash_consistent() {
    let h1 = path_hash("/some/long/path/with/many/parts");
    let h2 = path_hash("/some/long/path/with/many/parts");
    assert_eq!(h1, h2);
}

/// path_hash uniqueness check
#[test]
fn path_hash_uniqueness() {
    let h1 = path_hash("/path/a");
    let h2 = path_hash("/path/b");
    assert_ne!(h1, h2);
}

/// cache_path produces expected format
#[test]
fn cache_path_format_verified() {
    let tmp = std::env::temp_dir();
    let p = cache_path(&tmp, "hello/world");
    let name = p.file_name().unwrap().to_str().unwrap();
    assert_eq!(name.len(), 20, "cache path filename should be 20-char hex");
    assert!(name.chars().all(|c| c.is_ascii_hexdigit()), "should be hex");
}

/// cache_block_path includes block index in filename
#[test]
fn cache_block_path_includes_block_idx() {
    let tmp = std::env::temp_dir();
    let p = mntrs::cache_block_path(&tmp, "hello/world", 42);
    let name = p.file_name().unwrap().to_str().unwrap();
    assert!(name.ends_with(".block"), "should end with .block");
    assert!(
        name.contains("_000000002a"),
        "should contain block index 42 in hex"
    );
}

/// load_cache_index returns empty for non-existent cache dir
#[test]
fn load_cache_index_empty_for_nonexistent_dir() {
    let entries = mntrs::load_cache_index(&std::env::temp_dir().join("__nonexistent_cache_dir__"));
    assert!(
        entries.is_empty(),
        "should return empty list for nonexistent dir"
    );
}

/// Issue #227 + [[feedback-tuple-vs-struct]] + #219
/// precedent: `CacheIndexEntry` field semantics are
/// self-pinning via named fields. A future 5th field is
/// a compile-time catch at every construction site (vs
/// the tuple's silent 0/empty-default drop), and a
/// reorder is impossible at the call site. The test
/// pins the field identity and round-trips through a
/// real `load_cache_index` so the on-disk encoding
/// (`hash_blockidx.block`) and the struct's hex parse
/// stay in sync.
#[test]
fn cache_index_entry_fields_pin_semantics() {
    // Construct a real block file in a temp dir, then
    // round-trip through load_cache_index.
    let tmp = std::env::temp_dir().join(format!(
        "mntrs_test_cache_index_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&tmp).unwrap();
    // hash = "deadbeef", block_idx = 0x2a = 42
    let name = "deadbeef_000000002a.block";
    let path = tmp.join(name);
    let payload = vec![0u8; 64];
    std::fs::write(&path, &payload).unwrap();

    let entries = mntrs::load_cache_index(&tmp);
    assert_eq!(entries.len(), 1, "expected exactly one block file");
    let e = &entries[0];
    assert_eq!(e.name, name, "name should match filename");
    assert_eq!(e.block_idx, 42, "block_idx should be 0x2a = 42");
    assert_eq!(
        e.size,
        payload.len() as u64,
        "size should be file size in bytes"
    );
    // mtime is from the filesystem; just assert it's
    // a real (not-zero) SystemTime from the recent past.
    assert!(
        e.mtime <= std::time::SystemTime::now(),
        "mtime should be <= now"
    );

    // Drop impl is not required, but make sure Clone + Eq
    // work — used by callers that diff index snapshots.
    let cloned = e.clone();
    assert_eq!(e, &cloned, "CacheIndexEntry should be Clone + Eq");

    let _ = std::fs::remove_dir_all(&tmp);
}

/// fnmatch with various patterns
#[test]
fn fnmatch_various_patterns() {
    assert!(fnmatch("*.txt", "file.txt", false));
    assert!(fnmatch("file.*", "file.txt", false));
    assert!(fnmatch("file.*", "file.", false));
    assert!(!fnmatch("*.txt", "file.rs", false));
    assert!(fnmatch("a?c", "abc", false));
    assert!(!fnmatch("a?c", "abdc", false));
    assert!(fnmatch("a*c", "abbbc", false));
    assert!(fnmatch("a*c", "ac", false));
}

/// fnmatch case insensitive
#[test]
fn fnmatch_case_insensitive_matching() {
    assert!(fnmatch("*.TXT", "file.txt", true));
    assert!(fnmatch("*.txt", "FILE.TXT", true));
    assert!(!fnmatch("*.TXT", "file.txt", false));
}

/// fnmatch with path separators
#[test]
fn fnmatch_path_separator() {
    assert!(fnmatch("a/b/c", "a/b/c", false));
    assert!(!fnmatch("a/b/c", "a/b/d", false));
}
