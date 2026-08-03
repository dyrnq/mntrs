//! Parsers for the WinFSP `FILE_FULL_EA_INFORMATION` buffer format.
//!
//! Issue #501: WinFSP's `get_extended_attributes` /
//! `set_extended_attributes` callbacks receive and return raw byte
//! buffers in the Microsoft `FILE_FULL_EA_INFORMATION` layout
//! (see <https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/wdm/ns-wdm-_file_full_ea_information>).
//! This module owns the encode / decode logic so the WinFSP
//! adapter (`src/core_fs/winfsp.rs`) doesn't have to inline the
//! pointer-arithmetic style of the Windows struct.
//!
//! Layout of each entry:
//! ```text
//! offset  size  field
//! 0       4     NextEntryOffset (u32 LE; 0 = last entry)
//! 4       1     Flags           (u8)
//! 5       1     EaNameLength    (u8; bytes, not including any NUL)
//! 6       2     EaValueLength   (u16 LE)
//! 8       N     EaName          (NOT NUL-terminated in spec, but
//!                                NTFS / SetEaFile callers traditionally
//!                                include the NUL — `parse_ea_entries`
//!                                tolerates either)
//! 8+N     M     EaValue         (immediately follows the name)
//! ```
//!
//! Each entry's `NextEntryOffset` is relative to the start of
//! that entry, so the parser advances by `NextEntryOffset`
//! (or, when 0, terminates).
//!
//! cfg(windows): the Microsoft layout is meaningless on Unix.
//! The fuser adapter (`src/core_fs/fuser.rs`) doesn't need this
//! module — Linux/macOS go through POSIX `getxattr`/`setxattr`
//! with the `&OsStr` name + `&[u8]` value args directly.
#![cfg(windows)]

/// One decoded EA entry. Borrows from the input buffer; the
/// `name` is interpreted as UTF-8 lossy because the EA buffer
/// is byte-oriented (NTFS doesn't enforce a charset).
///
/// We use `from_utf8` (strict) because mntrs only ever stores
/// UTF-8 in xattrs (`MntrsFs::setxattr` rejects non-UTF-8 with
/// `InvalidInput` per the issue #500 contract). A future
/// backend that stored non-UTF-8 would need a different
/// reader — but today every byte that lands in the EA buffer
/// via `set_extended_attributes` came from a POSIX `setxattr`
/// on a `user.*` name (ASCII letters + dots + underscores +
/// hyphens), so this is safe.
#[derive(Debug, PartialEq)]
pub struct EaEntry<'a> {
    /// EA name (e.g. `user.foo`). Borrowed from the input buffer.
    pub name: &'a str,
    /// EA value bytes. Borrowed from the input buffer.
    pub value: &'a [u8],
    /// EA flags byte. NTFS uses bit 0 (`FILE_ATTRIBUTE_EA_INHERITED`)
    /// for "this EA was inherited"; mntrs ignores flags on write
    /// (opendal user_metadata is a flat map) and reports 0 on read.
    pub flags: u8,
}

/// Iterate `FILE_FULL_EA_INFORMATION` entries in `buffer`.
///
/// The iterator stops cleanly at `NextEntryOffset == 0` (the
/// last entry) without reading past `buffer.len()`. Malformed
/// entries that point past the buffer end also terminate the
/// iterator (no panic, no error — the caller already checked
/// `buffer.len()` against the documented WinFSP contract).
///
/// This is a free function (not a method on a struct) because
/// WinFSP hands the adapter a single `&mut [u8]` per callback
/// invocation; there's no parser state to keep around between
/// calls.
pub fn parse_ea_entries(buffer: &[u8]) -> impl Iterator<Item = EaEntry<'_>> {
    let mut offset = 0usize;
    std::iter::from_fn(move || {
        // Fixed header is 8 bytes. If we have fewer, the buffer
        // is truncated — stop without error.
        if offset + 8 > buffer.len() {
            return None;
        }
        // SAFETY: bounds check above + the buffer is a plain
        // &[u8] (no implicit alignment requirement). The four
        // header fields are read with `from_le_bytes` to avoid
        // any host-endian assumption (WinFSP itself runs on
        // little-endian x86_64, but the kernel structures are
        // always LE per the Microsoft spec).
        let next_entry_offset = u32::from_le_bytes(
            buffer[offset..offset + 4]
                .try_into()
                .expect("checked above"),
        ) as usize;
        let flags = buffer[offset + 4];
        let name_len = buffer[offset + 5] as usize;
        let value_len = u16::from_le_bytes(
            buffer[offset + 6..offset + 8]
                .try_into()
                .expect("checked above"),
        ) as usize;
        // Variable part: name (name_len bytes) + value
        // (value_len bytes). Defensive bound check — a malformed
        // entry could point past the buffer end.
        let name_start = offset + 8;
        let value_start = name_start + name_len;
        let value_end = value_start + value_len;
        if value_end > buffer.len() {
            return None;
        }
        let name_bytes = &buffer[name_start..value_start];
        // NTFS doesn't enforce a charset, but mntrs only writes
        // UTF-8 (see type doc). Use strict UTF-8 and panic-free
        // lossy as a belt-and-braces fallback for malformed
        // names that some other process may have set.
        let name = std::str::from_utf8(name_bytes).unwrap_or("");
        let value = &buffer[value_start..value_end];
        let entry = EaEntry { name, value, flags };
        // Advance: NextEntryOffset is relative to start of
        // THIS entry. 0 means "this was the last entry".
        offset = if next_entry_offset == 0 {
            // Park offset past the buffer end so the next
            // call's bounds check returns None.
            buffer.len() + 1
        } else {
            offset + next_entry_offset
        };
        Some(entry)
    })
}

/// Encode one EA entry into a `FILE_FULL_EA_INFORMATION`
/// buffer. The caller passes the buffer to append into and
/// the current write offset; this writes the entry and
/// advances the cursor to the entry's end (rounded up to a
/// 4-byte boundary for the next-entry alignment).
///
/// `name` is the EA name (e.g. `"user.foo"`). `value` is the
/// EA value bytes. `flags` is the flags byte (typically 0).
///
/// Padding to 4-byte alignment: per Microsoft, each
/// `FILE_FULL_EA_INFORMATION` entry's `NextEntryOffset` must be
/// 4-byte aligned relative to the start of the buffer. The
/// entry's variable part (name + value) is laid out
/// contiguously with no padding, but the next entry starts at
/// the aligned offset. We honor that here.
pub fn write_ea_entry(
    buffer: &mut [u8],
    cursor: &mut u32,
    name: &str,
    value: &[u8],
    flags: u8,
) -> Result<(), EaWriteError> {
    let name_len = name.len();
    if name_len > u8::MAX as usize {
        return Err(EaWriteError::NameTooLong);
    }
    if value.len() > u16::MAX as usize {
        return Err(EaWriteError::ValueTooLong);
    }
    // Compute the entry's total size (header 8 + name + value)
    // and round up to a 4-byte multiple for the next-entry
    // offset. The padding bytes themselves are zero-filled
    // (NTFS doesn't read them, but we zero them for hygiene).
    let entry_size = 8 + name_len + value.len();
    let padded_size = (entry_size + 3) & !3;
    let next_offset = (*cursor) + padded_size as u32;
    // Buffer overrun check: the cursor at the start of this
    // entry plus the padded size must fit.
    let required = next_offset as usize;
    if required > buffer.len() {
        return Err(EaWriteError::BufferTooSmall);
    }
    let offset = *cursor as usize;
    buffer[offset..offset + 4].copy_from_slice(&next_offset.to_le_bytes());
    buffer[offset + 4] = flags;
    buffer[offset + 5] = name_len as u8;
    buffer[offset + 6..offset + 8].copy_from_slice(&(value.len() as u16).to_le_bytes());
    buffer[offset + 8..offset + 8 + name_len].copy_from_slice(name.as_bytes());
    buffer[offset + 8 + name_len..offset + 8 + name_len + value.len()].copy_from_slice(value);
    // Zero the tail padding (if any) so partial-write debugging
    // is easier.
    if padded_size > entry_size {
        let pad_start = offset + entry_size;
        let pad_end = offset + padded_size;
        buffer[pad_start..pad_end].fill(0);
    }
    *cursor = next_offset;
    Ok(())
}

/// Encode the terminator (a single zeroed `ULONG` meaning
/// "NextEntryOffset = 0, no more entries"). Per Microsoft,
/// the buffer must be terminated by an entry whose
/// `NextEntryOffset` is 0.
pub fn write_ea_terminator(buffer: &mut [u8], cursor: &mut u32) -> Result<(), EaWriteError> {
    if (*cursor as usize) + 4 > buffer.len() {
        return Err(EaWriteError::BufferTooSmall);
    }
    let off = *cursor as usize;
    buffer[off..off + 4].copy_from_slice(&[0u8; 4]);
    *cursor += 4;
    Ok(())
}

/// Compute the total padded size of an entry (for caller-side
/// pre-sizing). Useful when the caller wants to allocate the
/// right-sized buffer up front rather than grow incrementally.
#[allow(dead_code)]
pub fn ea_entry_padded_size(name: &str, value: &[u8]) -> usize {
    let entry_size = 8 + name.len() + value.len();
    (entry_size + 3) & !3
}

/// Errors specific to writing into an EA buffer. WinFSP
/// surfaces these as STATUS_INVALID_PARAMETER (the kernel
/// has no richer error code for "EA buffer too small" — it
/// just truncates and the caller re-queries with a larger
/// buffer per `IoCheckEaBufferValidity`).
#[derive(Debug, PartialEq)]
pub enum EaWriteError {
    NameTooLong,
    ValueTooLong,
    BufferTooSmall,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode one entry and round-trip-decode it.
    #[test]
    fn roundtrip_one_entry() {
        let mut buf = [0u8; 64];
        let mut cursor = 0u32;
        write_ea_entry(&mut buf, &mut cursor, "user.foo", b"bar", 0).unwrap();
        write_ea_terminator(&mut buf, &mut cursor).unwrap();
        let entries: Vec<_> = parse_ea_entries(&buf[..cursor as usize]).collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "user.foo");
        assert_eq!(entries[0].value, b"bar");
        assert_eq!(entries[0].flags, 0);
    }

    /// Two entries, second one's NextEntryOffset must point
    /// past the first (padded) entry.
    #[test]
    fn roundtrip_two_entries() {
        let mut buf = [0u8; 128];
        let mut cursor = 0u32;
        write_ea_entry(&mut buf, &mut cursor, "user.a", b"1", 0).unwrap();
        write_ea_entry(&mut buf, &mut cursor, "user.bb", b"22", 0).unwrap();
        write_ea_terminator(&mut buf, &mut cursor).unwrap();
        let entries: Vec<_> = parse_ea_entries(&buf[..cursor as usize]).collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "user.a");
        assert_eq!(entries[0].value, b"1");
        assert_eq!(entries[1].name, "user.bb");
        assert_eq!(entries[1].value, b"22");
    }

    /// Entry with non-UTF-8 name bytes — parse must not panic
    /// (the EA buffer is byte-oriented; NTFS doesn't enforce
    /// a charset). The lossy conversion yields an empty
    /// name; the parser should yield exactly one entry.
    #[test]
    fn parse_non_utf8_name_does_not_panic() {
        let mut buf = [0u8; 64];
        // Build a single entry with a 1-byte name (0xff).
        buf[0..4].copy_from_slice(&16u32.to_le_bytes()); // NextEntryOffset=16
        buf[4] = 0; // Flags
        buf[5] = 1; // EaNameLength
        buf[6..8].copy_from_slice(&0u16.to_le_bytes()); // EaValueLength
        buf[8] = 0xff; // EaName (invalid UTF-8)
        // Padded size = 8 + 1 = 9, rounded up to 12; terminator at offset 12.
        buf[12..16].copy_from_slice(&0u32.to_le_bytes());
        let entries: Vec<_> = parse_ea_entries(&buf[..16]).collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, ""); // lossy → empty
        assert_eq!(entries[0].value, b"");
    }

    /// Entry size alignment: 8 + name_len + value_len must
    /// round up to a 4-byte multiple.
    #[test]
    fn entry_padded_size_aligns_4() {
        assert_eq!(ea_entry_padded_size("a", b""), 12); // 8+1 → pad 11 → 12
        assert_eq!(ea_entry_padded_size("ab", b""), 12); // 8+2 → 10 → 12
        assert_eq!(ea_entry_padded_size("abc", b""), 12); // 8+3 → 11 → 12
        assert_eq!(ea_entry_padded_size("abcd", b""), 12); // 8+4 → 12
        assert_eq!(ea_entry_padded_size("abcde", b""), 16); // 8+5 → 13 → 16
    }

    /// Value at the 16-bit u16 limit (the protocol cap).
    /// Anything larger must reject with ValueTooLong.
    #[test]
    fn rejects_oversized_value() {
        let mut buf = vec![0u8; 70_000];
        let mut cursor = 0u32;
        let huge = vec![0u8; 70_000];
        let err = write_ea_entry(&mut buf, &mut cursor, "user.big", &huge, 0)
            .expect_err("should reject value > u16::MAX");
        assert_eq!(err, EaWriteError::ValueTooLong);
    }

    #[test]
    fn rejects_oversized_name() {
        let mut buf = vec![0u8; 300];
        let mut cursor = 0u32;
        let long_name: String = "x".repeat(300);
        let err = write_ea_entry(&mut buf, &mut cursor, &long_name, b"v", 0)
            .expect_err("should reject name > u8::MAX");
        assert_eq!(err, EaWriteError::NameTooLong);
    }

    #[test]
    fn buffer_too_small_returns_error() {
        let mut buf = [0u8; 8];
        let mut cursor = 0u32;
        // 8+1+1=10 bytes for the entry; buf only has 8.
        let err = write_ea_entry(&mut buf, &mut cursor, "a", b"v", 0)
            .expect_err("should reject when buffer < padded entry size");
        assert_eq!(err, EaWriteError::BufferTooSmall);
    }
}
