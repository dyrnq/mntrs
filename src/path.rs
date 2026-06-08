//! 平台无关的路径归一化。
//!
//! Windows 路径 `\foo\bar` ↔ Linux 路径 `/foo/bar`，
//! 内部统一存 POSIX 形式。
//!
//! ```rust
//! use mntrs::path::normalize;
//! assert_eq!(normalize(r"\foo\bar"), "/foo/bar");
//! assert_eq!(normalize("/foo/bar"), "/foo/bar");
//! ```

/// 将路径归一化为 POSIX 风格（`/` 分隔符）。
pub fn normalize(p: &str) -> String {
    p.replace('\\', "/")
}

/// 将 POSIX 路径转换为当前平台的 native 格式。
#[cfg(windows)]
pub fn to_native(p: &str) -> String {
    p.replace('/', "\\")
}

#[cfg(not(windows))]
pub fn to_native(p: &str) -> String {
    p.to_string()
}

/// 将 Windows 盘符（如 `X:`）解析为 mount 目标。
/// 返回 `DriveLetter('X')` 或 `NtfsDirectory(path)`。
#[cfg(windows)]
pub fn parse_windows_target(target: &str) -> std::io::Result<winfsp::host::MountPoint> {
    use winfsp::host::MountPoint;
    let t = target.trim();
    if t.len() == 2 && t.as_bytes()[1] == b':' {
        let letter = t.as_bytes()[0];
        if letter.is_ascii_alphabetic() {
            return Ok(MountPoint::DriveLetter(letter as char));
        }
    }
    if t == "*" {
        return Ok(MountPoint::DriveLetterAuto);
    }
    Ok(MountPoint::NtfsDirectory(std::path::PathBuf::from(t)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_unix() {
        assert_eq!(normalize("/a/b/c"), "/a/b/c");
        assert_eq!(normalize("a/b"), "a/b");
    }

    #[test]
    fn test_normalize_windows() {
        assert_eq!(normalize(r"\a\b\c"), "/a/b/c");
        assert_eq!(normalize(r"a\b"), "a/b");
    }

    #[test]
    fn test_normalize_mixed() {
        assert_eq!(normalize(r"/a\b/c"), "/a/b/c");
    }

    #[test]
    fn test_to_native_unix() {
        let n = to_native("/a/b/c");
        if cfg!(windows) {
            assert_eq!(n, r"\a\b\c");
        } else {
            assert_eq!(n, "/a/b/c");
        }
    }
}
