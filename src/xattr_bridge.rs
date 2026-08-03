//! Pure helpers for the xattr metadata pipeline.
//!
//! Issue #500: the read path (`getxattr` / `listxattr`) lives in
//! `src/lib.rs` and was extracted in PR #496. The write path
//! (`setxattr` / `removexattr`) needs the same name parsing +
//! well-known filtering logic, plus a single source of truth for
//! the `user.<key>` → `key` normalization rule that both paths
//! apply. This module owns that logic so:
//!
//! * `src/lib.rs` keeps using the same normalizer for read + write
//!   (a single definition, no risk of drift).
//! * `src/core_fs/winfsp.rs` can `use crate::xattr_bridge::*` for
//!   the WinFSP write path (#501) without re-implementing the
//!   filtering — see that issue's "Out of scope" section.
//!
//! Free functions (not associated methods, not a trait impl)
//! because they're not tied to any single `CoreFilesystem`
//! implementation — they take `&str` and return pure values,
//! easy to unit-test in isolation.

// Issue #500: xattrs that the rclone `--metadata` parity set
// surfaces but are *derived* values. They're populated by the
// backend on `stat()` and must not be overwritten by
// user-driven `setxattr`. Trying to set or remove these via
// the FUSE setxattr path returns `Unsupported` (mapped to
// `ENOSYS` by the fuser adapter) — the equivalent of trying
// to `chmod` a mode the backend doesn't track.
///
/// Legacy S3 spellings (`s3.etag`, `s3.content-type`) are
/// listed too: pre-#465 callers scripted against them; we
/// accept them on `getxattr` as aliases and reject them on
/// the write path the same way.
pub const WELL_KNOWN_XATTRS: &[&str] = &[
    "user.etag",
    "user.mime_type",
    "user.content-type",
    "user.mtime",
    "user.content_length",
    "s3.etag",
    "s3.content-type",
];

/// True if `name` is a well-known derived xattr. The caller
/// is responsible for treating this as non-writable: the read
/// path serves these from `Metadata::etag()` / `content_type()`
/// / etc., not from `user_metadata`, so they don't even have
/// a backing entry in the user-metadata map.
pub fn is_well_known_xattr(name: &str) -> bool {
    WELL_KNOWN_XATTRS.contains(&name)
}

/// If `name` is `user.<key>` *and not* a well-known derived
/// name, returns the normalized key (`lowercase + dots→underscores`).
/// Returns `None` for:
///
/// * any name that doesn't start with `user.` (we don't
///   surface other namespaces; matches rclone `--metadata`
///   scope);
/// * the empty `user.` form (no key);
/// * well-known names like `user.etag` (derived values, not
///   user metadata).
///
/// This is the only function the write path needs for
/// validation; the read path uses `is_user_xattr_name` (looser)
/// so legacy spellings like `s3.etag` keep working on `getxattr`.
pub fn parse_user_xattr_key(name: &str) -> Option<String> {
    if is_well_known_xattr(name) {
        return None;
    }
    if !name.starts_with("user.") {
        return None;
    }
    let raw = &name[5..];
    if raw.is_empty() {
        return None;
    }
    Some(normalize_user_meta_key(raw))
}

/// True if `name` is the canonical `user.*` namespace (any
/// key, including well-known). Used by the read path to decide
/// whether to look in `Metadata::user_metadata()` — a broader
/// check than `parse_user_xattr_key` because `getxattr
/// user.etag` is a legitimate call that just resolves to a
/// derived value.
pub fn is_user_xattr_name(name: &str) -> bool {
    name.starts_with("user.") && name.len() > 5
}

/// Normalize a custom user-metadata key per rclone's mapping
/// rules (lowercase + dots→underscores). S3 user metadata
/// keys are case-insensitive at the HTTP header level (the
/// `x-amz-meta-*` prefix gets normalized by AWS). Lowercase +
/// dots→underscores matches the canonical xattr naming: dots
/// are illegal in xattr names on most platforms.
///
/// Single source of truth — both `xattr_value_for` /
/// `xattr_names_for` (read path) and `setxattr` (write path)
/// route through this.
pub fn normalize_user_meta_key(s: &str) -> String {
    s.to_lowercase().replace('.', "_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_user_xattr_key_roundtrip() {
        assert_eq!(parse_user_xattr_key("user.foo"), Some("foo".to_string()));
        assert_eq!(
            parse_user_xattr_key("user.My-Key"),
            Some("my-key".to_string())
        );
        assert_eq!(
            parse_user_xattr_key("user.My.Key"),
            Some("my_key".to_string())
        );
    }

    #[test]
    fn parse_user_xattr_key_rejects_wellknown() {
        for name in WELL_KNOWN_XATTRS {
            assert!(
                parse_user_xattr_key(name).is_none(),
                "{name} should be rejected"
            );
        }
    }

    #[test]
    fn parse_user_xattr_key_rejects_non_user() {
        assert!(parse_user_xattr_key("system.posix_acl_access").is_none());
        assert!(parse_user_xattr_key("").is_none());
        assert!(parse_user_xattr_key("foo").is_none());
    }

    #[test]
    fn parse_user_xattr_key_rejects_empty_key() {
        assert!(parse_user_xattr_key("user.").is_none());
    }

    #[test]
    fn normalize_lowercase_and_dots_to_underscores() {
        assert_eq!(normalize_user_meta_key("Foo"), "foo");
        assert_eq!(normalize_user_meta_key("My.Key"), "my_key");
        assert_eq!(normalize_user_meta_key("ALREADY_ok"), "already_ok");
    }
}
