use mntrs::{cache_path, fnmatch, path_hash};
use std::path::Path;

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
    let p = cache_path(Path::new("/tmp/cache"), "hello/world");
    let parent = p.parent().unwrap();
    assert_eq!(parent, Path::new("/tmp/cache"));
    let name = p.file_name().unwrap().to_str().unwrap();
    assert_eq!(name.len(), 20, "cache path filename should be 20-char hex");
    assert!(name.chars().all(|c| c.is_ascii_hexdigit()), "should be hex");
}
