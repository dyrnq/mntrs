use mntrs::{fnmatch, path_hash};

#[test]
fn path_hash_is_stable() {
    assert_eq!(path_hash("/foo/bar"), path_hash("/foo/bar"));
    assert_eq!(path_hash(""), path_hash(""));
    // Different paths should produce different hashes (practically)
    assert_ne!(path_hash("a"), path_hash("b"));
}

#[test]
fn path_hash_always_positive() {
    for p in &["/", "/a", "/very/long/path/here", "x", ""] {
        let h = path_hash(p);
        assert!(h >= 2, "hash for '{p}' should be >= 2, got {h}");
    }
}

#[test]
fn fnmatch_exact() {
    assert!(fnmatch("hello", "hello", false));
    assert!(!fnmatch("hello", "world", false));
}

#[test]
fn fnmatch_star() {
    assert!(fnmatch("*.txt", "foo.txt", false));
    assert!(fnmatch("*.txt", "bar.txt", false));
    assert!(!fnmatch("*.txt", "foo.rs", false));
    assert!(fnmatch("a*c", "abc", false));
    assert!(fnmatch("a*c", "ac", false));
    assert!(fnmatch("a*c", "axyzc", false));
}

#[test]
fn fnmatch_question() {
    assert!(fnmatch("a?c", "abc", false));
    assert!(fnmatch("a?c", "axc", false));
    assert!(!fnmatch("a?c", "ac", false));
    assert!(!fnmatch("a?c", "abbc", false));
}

#[test]
fn fnmatch_case_insensitive() {
    assert!(fnmatch("Hello", "hello", true));
    assert!(fnmatch("*.TXT", "foo.txt", true));
    assert!(!fnmatch("Hello", "hello", false));
}

#[test]
fn fnmatch_edge_cases() {
    assert!(fnmatch("*", "anything", false));
    assert!(fnmatch("?", "x", false));
    assert!(!fnmatch("?", "xx", false));
    assert!(fnmatch("file.*", "file.", false));
}

#[test]
fn cache_path_is_stable() {
    use mntrs::cache_path;
        let tmp = std::env::temp_dir();
    let p = cache_path(&tmp, "hello/world");
    let name = p.file_name().unwrap().to_str().unwrap();
    assert_eq!(name.len(), 20);
    assert!(name.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn cache_path_deterministic() {
    use mntrs::cache_path;
        assert_eq!(
        cache_path(std::path::Path::new("/a"), "x"),
        cache_path(std::path::Path::new("/a"), "x")
    );
    assert_ne!(
        cache_path(std::path::Path::new("/a"), "x"),
        cache_path(std::path::Path::new("/b"), "x")
    );
}
