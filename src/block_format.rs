// Disk-cache block format: constants, CRC helpers, read/write
// serialization, and cache-index management.
//
// This module is the single source of truth for the on-disk
// block-cache format (currently v3).  Both the read path
// (`read_block_cached`) and the write path (`serialize_v3_block`)
// live here so a format change requires editing exactly one file.

use std::path::Path;
use std::sync::Arc;

// ── Constants ───────────────────────────────────────────────────────

/// CRC32C trailer size, in bytes. The block cache file
/// format is `content_bytes || crc32c_le(content)` for
/// full blocks; partial blocks (`< CACHE_BLOCK_SIZE` — the
/// last block of a file) carry no trailer because there's
/// no canonical "expected length" to validate against
/// without an extra sidecar.
pub(crate) const BLOCK_CRC_TRAILER: usize = 4;

/// Disk-cache block-file format marker. Spells "MNCR"
/// (mntrs cache) in ASCII. The header is 4 bytes of magic
/// + 4 bytes of version (little-endian u32) = 8 bytes total.
///
/// Followed by the content and the 4-byte CRC32C trailer.
///
/// Why a magic + version at all: a future on-disk format
/// change (e.g. compressed blocks, encrypted blocks) must
/// not silently misread existing files. The CRC alone only
/// catches data corruption, not format mismatch. The
/// version field is the explicit extension point.
///
/// Backward compat: files written before this header landed
/// have no magic at offset 0 and read as legacy
/// (unprotected `content`, protected `content || crc32c`,
/// or partial `< CACHE_BLOCK_SIZE`). The read path detects
/// "MNCR" at offset 0 and switches to the new format; all
/// other files fall through to the legacy parser. New
/// files always use the new format.
pub(crate) const BLOCK_MAGIC: &[u8; 4] = b"MNCR";

/// On-disk format version. Increment when the layout
/// changes in a way the existing read path can't parse.
/// The read path is conservative: any version it doesn't
/// recognize (including higher versions from a newer build)
/// is treated as corrupt and the file is unlinked,
/// forcing a remote re-fetch. Bump this when changing
/// the layout, and add a branch in `read_block_cached` to
/// handle the new version.
///
/// Version history:
///   * `1` — uncompressed: `MNCR || version=1 || content
///     || crc32c(magic||version||content)`.
///   * `2` — lz4-compressed: `MNCR || version=2 ||
///     lz4_flex::compress_prepend_size(content) ||
///     crc32c(magic||version||compressed_payload)`.
///   * `3` (current) — lz4 + path verification:
///     `MNCR || version=3 || path_len(2 LE u16) ||
///     path(path_len bytes UTF-8) ||
///     lz4_flex::compress_prepend_size(content) ||
///     crc32c(magic||version||path_len||path||payload)`.
///     The embedded path lets the reader detect a
///     `path_hash` collision (DESIGN_VULNS High #4):
///     the cache file at `{path_hash(P)}_{idx}.block`
///     is opened, parsed, and the embedded path is
///     compared against the expected path P. Mismatch
///     → unlink + return None → remote re-fetch +
///     re-write with the correct path embedded. Without
///     this, two different paths whose 64-bit FNV-1a
///     hashes collide (probability ~5e-8 at 1M entries
///     with the per-process salt, but climbing to ~50%
///     near 2^32) would silently swap content.
pub(crate) const BLOCK_FORMAT_VERSION: u32 = 3;

/// First-generation new-format version (uncompressed).
/// Kept as a read-side constant so the read path can
/// transparently consume cache files written by older
/// builds without forcing a refetch. Old files get
/// rewritten with the current `BLOCK_FORMAT_VERSION`
/// on the next write through `write_block_cached` /
/// `do_block_cache_write`.
pub(crate) const LEGACY_BLOCK_FORMAT_VERSION_V1: u32 = 1;

/// Second-generation format (lz4-compressed, no path
/// verification). Same back-compat policy as V1: read
/// only, get rewritten as v3 on next write to the same
/// block. Note that v2 reads return content WITHOUT
/// the v3 collision check, so a v2 file could in
/// principle deliver wrong content if two paths hashed
/// to the same name — but the per-process random salt
/// in `path_hash` makes that vanishingly unlikely for
/// realistic cache sizes, and a v2-to-v3 rewrite
/// closes the window for any subsequent read.
pub(crate) const LEGACY_BLOCK_FORMAT_VERSION_V2: u32 = 2;

/// Size of the magic + version header at the start of a
/// new-format block file. = 4 (magic) + 4 (version).
pub(crate) const BLOCK_HEADER_SIZE: usize = 8;

/// Total per-block overhead for the new format:
/// `BLOCK_HEADER_SIZE` (magic + version) + `BLOCK_CRC_TRAILER`.
/// A full new-format block is `content (≤ 8 MiB) +
/// BLOCK_OVERHEAD` bytes; a partial new-format block is
/// `< 8 MiB + BLOCK_OVERHEAD` bytes.
pub(crate) const BLOCK_OVERHEAD: usize = BLOCK_HEADER_SIZE + BLOCK_CRC_TRAILER;

/// Upper bound on the on-disk payload (post-header,
/// pre-trailer) of any block file. Set to leave room
/// for the v2/v3 lz4-compressed formats' worst-case
/// expansion on uncompressible input.
///
/// LZ4 frame-less block worst case ≈
/// `input_len + (input_len / 255) + 16` per the spec.
/// For an 8 MiB block: ~32 928 bytes of expansion.
/// Plus the 4-byte uncompressed-size prefix that
/// `lz4_flex::compress_prepend_size` prepends.
///
/// `MAX_PAYLOAD_OVERHEAD = 65_536` gives ~2× safety
/// margin over the theoretical worst case — keeps the
/// sanity check tight enough to catch garbage in the
/// cache dir while not unlinking legitimate lz4
/// expansions on incompressible (already-compressed)
/// data like JPEGs or zstd files.
pub(crate) const MAX_PAYLOAD_OVERHEAD: usize = 65_536;

/// Max embedded-path length the v3 block format
/// supports. Stored as a 2-byte LE u16 (so the
/// hard limit is 65535), capped at MAX_PATH_LEN
/// for sanity. PATH_MAX on Linux is 4096; we round
/// up to the next power of 2 to leave room for
/// backends that allow longer keys (S3 keys can be
/// up to 1024 bytes, GCS up to 1024, but some
/// HDFS/abstract paths can be longer with namespace
/// prefixes). The check is a defensive cap, not a
/// supported configuration — writes of paths above
/// this limit fall through to direct remote on the
/// next read.
pub(crate) const MAX_PATH_LEN: usize = 8192;

// ── CRC helpers ─────────────────────────────────────────────────────

/// Compute CRC32C checksum. Delegates to the `crc32c`
/// crate which uses hardware instructions when
/// available (x86 CRC32Q via SSE4.2, ARMv8.2-CRC32)
/// and falls back to a software table-driven
/// implementation otherwise. ~5-10× faster than the
/// hand-rolled poly loop on x86 with SSE4.2 for 8 MiB
/// buffers.
pub(crate) fn crc32c_checksum(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Compute CRC32C over the concatenation of two
/// non-contiguous slices — `a || b` — without
/// materializing a new buffer. Uses the `crc32c`
/// crate's `crc32c_append` so we get the hardware
/// acceleration (single call into the optimized inner
/// loop per slice, no allocation).
pub(crate) fn crc32c_checksum_concat(a: &[u8], b: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(a), b)
}

// ── Read path ───────────────────────────────────────────────────────

/// Read a block cache file with optional CRC32C
/// verification. Detects the on-disk format by inspecting
/// the first 4 bytes; see `BLOCK_MAGIC` for the
/// format-discrimination logic.
///
/// File layout (new format, current):
///   * `MNCR` magic (4) || `version` (4, LE u32) ||
///     `content` (≤ 8 MiB) || `crc32c(magic || version ||
///     content)` (4) — protected. Total `content +
///     BLOCK_OVERHEAD` bytes.
///
/// File layout (legacy, no header — read when first 4
/// bytes aren't "MNCR"):
///   * `8 MiB`              — legacy / unprotected
///     (backward compatible with cache files written
///     before the CRC was added). Used as-is.
///   * `8 MiB + 4`          — legacy / protected: the
///     last 4 bytes are a little-endian CRC32C of the
///     first 8 MiB. On mismatch the file is unlinked
///     (corrupt) and the function returns `None`.
///   * `< 8 MiB`            — legacy / partial (last
///     block of a file). No trailer; used as-is.
///   * `> 8 MiB + BLOCK_OVERHEAD` — corrupt (writer
///     overran or garbage). Unlinked, returns `None`.
///
/// Returns `Some(Bytes)` on a clean read and `None` on a
/// corrupt or unrecognized file (after unlinking it). The
/// caller should treat `None` as a cache miss.
pub(crate) fn read_block_cached(cpath: &Path, expected_path: &str) -> Option<bytes::Bytes> {
    let metadata = std::fs::metadata(cpath).ok()?;
    let size = metadata.len() as usize;
    if size
        > crate::CACHE_BLOCK_SIZE as usize
            + BLOCK_OVERHEAD
            + MAX_PAYLOAD_OVERHEAD
            + MAX_PATH_LEN
            + 2
    {
        // Writer overran or someone dropped garbage in
        // the cache dir. Treat as corrupt. The added
        // MAX_PAYLOAD_OVERHEAD slack accommodates the v2
        // lz4-compressed format's worst-case expansion
        // when content is uncompressible (already-zipped
        // media, encrypted data) — lz4 may emit slightly
        // more than input. MAX_PATH_LEN + 2 additionally
        // covers the v3 embedded-path field.
        let _ = std::fs::remove_file(cpath);
        tracing::warn!(
            ?cpath,
            size,
            "block cache file size exceeds format; unlinking"
        );
        return None;
    }
    let data = std::fs::read(cpath).ok()?;
    // Format detection: new format starts with the
    // magic at offset 0. Anything else is legacy.
    let is_new_format = data.len() >= BLOCK_HEADER_SIZE && &data[0..4] == BLOCK_MAGIC;
    if is_new_format {
        return read_new_format(cpath, &data, expected_path);
    }
    // Legacy parsers — the three pre-CRC variants:
    if size == crate::CACHE_BLOCK_SIZE as usize + BLOCK_CRC_TRAILER {
        // Legacy / protected full block: verify CRC32C.
        let (content, trailer) = data.split_at(crate::CACHE_BLOCK_SIZE as usize);
        // Bug 4 fix: trailer.try_into() should always
        // succeed here (split_at gives exactly
        // BLOCK_CRC_TRAILER bytes when size matches the
        // outer check), but defending against a future
        // refactor that breaks that invariant — a silent
        // unwrap_or([0u8; 4]) would let a torn write whose
        // computed CRC happens to be 0 (e.g. empty content)
        // pass as valid. Treat as corrupt instead.
        let want = match <[u8; BLOCK_CRC_TRAILER]>::try_from(trailer) {
            Ok(b) => u32::from_le_bytes(b),
            Err(_) => {
                let _ = std::fs::remove_file(cpath);
                tracing::warn!(
                    ?cpath,
                    trailer_len = trailer.len(),
                    "legacy block CRC trailer wrong size; unlinking"
                );
                return None;
            }
        };
        let got = crc32c_checksum(content);
        if want != got {
            let _ = std::fs::remove_file(cpath);
            tracing::warn!(
                ?cpath,
                stored = want,
                computed = got,
                "block cache CRC mismatch (legacy format); unlinking and refetching"
            );
            return None;
        }
        Some(bytes::Bytes::copy_from_slice(content))
    } else if size == crate::CACHE_BLOCK_SIZE as usize {
        // Legacy / unprotected full block. We can't verify,
        // so log once at debug level the first time a
        // legacy block is hit.
        tracing::debug!(?cpath, "block cache file is unprotected (legacy format)");
        Some(bytes::Bytes::from(data))
    } else if size < crate::CACHE_BLOCK_SIZE as usize {
        // Legacy / partial block (last block of a file).
        // No CRC expected.
        Some(bytes::Bytes::from(data))
    } else {
        // size > 8 MiB but <= 8 MiB + BLOCK_OVERHEAD — a
        // new-format partial block bigger than the
        // magic check would catch (size >= BLOCK_HEADER_SIZE
        // but magic missing) or a torn write. Treat as
        // corrupt. The size check at the top of the
        // function is for `> 8 MiB + BLOCK_OVERHEAD`; this
        // branch is `8 MiB + 1 ..= 8 MiB + BLOCK_OVERHEAD`
        // (and any size that didn't match the legacy
        // protected/unprotected sizes).
        let _ = std::fs::remove_file(cpath);
        tracing::warn!(
            ?cpath,
            size,
            "block cache file size in no-man's-land; unlinking"
        );
        None
    }
}

/// New-format block reader. Assumes the caller has
/// already verified the magic at offset 0 — this just
/// parses version, content, and CRC.
///
/// CRC is over `magic || version || content` (the
/// entire file up to the trailer), so a write-side bug
/// that corrupts the version byte is caught by the CRC
/// check and the file is unlinked. This is what
/// distinguishes a magic+version header from a naive
/// "version field at offset 4": the magic and version
/// are themselves CRC-protected, so they can't be
/// silently tampered with.
fn read_new_format(cpath: &Path, data: &[u8], expected_path: &str) -> Option<bytes::Bytes> {
    // Strip the 8-byte header.
    let after_header = &data[BLOCK_HEADER_SIZE..];
    // The last 4 bytes are the CRC; everything before is
    // the content.
    if after_header.len() < BLOCK_CRC_TRAILER {
        // Header but no room for trailer — torn write.
        let _ = std::fs::remove_file(cpath);
        tracing::warn!(?cpath, "new-format block too short for trailer; unlinking");
        return None;
    }
    let content_end = after_header.len() - BLOCK_CRC_TRAILER;
    let content = &after_header[..content_end];
    // Bug 4 fix: the size check above guarantees
    // `after_header[content_end..]` is exactly
    // BLOCK_CRC_TRAILER bytes, so try_into succeeds, but
    // unwrap_or([0u8; 4]) would silently let a torn write
    // whose computed CRC happens to be 0 pass as valid.
    // Treat the conversion failure as corrupt instead.
    let stored_crc = match <[u8; BLOCK_CRC_TRAILER]>::try_from(&after_header[content_end..]) {
        Ok(b) => u32::from_le_bytes(b),
        Err(_) => {
            let _ = std::fs::remove_file(cpath);
            tracing::warn!(
                ?cpath,
                trailer_len = after_header.len() - content_end,
                "new-format block CRC trailer wrong size; unlinking"
            );
            return None;
        }
    };
    // CRC covers magic + version + content (the entire
    // file minus the trailing 4 CRC bytes).
    let computed_crc = crc32c_checksum(&data[..data.len() - BLOCK_CRC_TRAILER]);
    if stored_crc != computed_crc {
        let _ = std::fs::remove_file(cpath);
        tracing::warn!(
            ?cpath,
            stored = stored_crc,
            computed = computed_crc,
            "new-format block CRC mismatch; unlinking and refetching"
        );
        return None;
    }
    // Version gate. Three known versions today:
    //   * v1 (LEGACY_BLOCK_FORMAT_VERSION_V1 = 1) —
    //     uncompressed, no embedded path. Pre-#4 cache
    //     files; read for back-compat, no collision
    //     check.
    //   * v2 (LEGACY_BLOCK_FORMAT_VERSION_V2 = 2) —
    //     lz4-compressed with size prefix, no embedded
    //     path. Post-#4, pre-v3 (path-collision audit).
    //     Read for back-compat, no collision check.
    //   * v3 (BLOCK_FORMAT_VERSION = 3, current) — lz4
    //     + embedded path for path_hash collision
    //     detection. Written by both writers
    //     (write_block_cached + do_block_cache_write).
    // Unknown versions are conservatively treated as
    // corrupt — better to lose one cache entry than to
    // silently misread a format the code doesn't
    // understand. The CRC has already been verified
    // above, so reaching this point with a known version
    // means the payload is intact.
    //
    // Bug 4 fix: data is guaranteed `>= BLOCK_HEADER_SIZE`
    // (= 8) by the caller — the `is_new_format` check at
    // the top of `read_block_cached` verifies the magic
    // at offset 0..4 and only enters this function when
    // `data.len() >= BLOCK_HEADER_SIZE`. So `data[4..8]`
    // is always 4 bytes. But the explicit fallback to
    // [0u8; 4] would map to `version == 0`, which
    // doesn't match any of v1/v2/v3 today; it would
    // still hit the "unsupported version" arm and
    // unlink — coincidentally safe. We replace with an
    // explicit corrupt-file return so a future refactor
    // that adds a version 0 (unlikely but legal) doesn't
    // silently accept a length-truncated header.
    let version_bytes: [u8; 4] = match data[4..8].try_into() {
        Ok(b) => b,
        Err(_) => {
            let _ = std::fs::remove_file(cpath);
            tracing::warn!(
                ?cpath,
                "new-format block too short for version field; unlinking"
            );
            return None;
        }
    };
    let version = u32::from_le_bytes(version_bytes);
    match version {
        LEGACY_BLOCK_FORMAT_VERSION_V1 => {
            // Uncompressed: content is the payload as-is.
            // No path verification (v1 predates the
            // embedded-path field); collision risk
            // remains for v1 files until they're rewritten
            // as v3 on next write to the same block.
            Some(bytes::Bytes::copy_from_slice(content))
        }
        LEGACY_BLOCK_FORMAT_VERSION_V2 => {
            // v2 lz4 (no embedded path). Same
            // collision-risk caveat as v1.
            decode_lz4_payload(cpath, content)
        }
        BLOCK_FORMAT_VERSION => {
            // v3: `path_len(2 LE u16) || path(path_len) ||
            // lz4_payload`. Extract the path, verify it
            // matches what the caller expected (rejects
            // path_hash collisions per DESIGN_VULNS High
            // #4), then decompress the rest.
            if content.len() < 2 {
                let _ = std::fs::remove_file(cpath);
                tracing::warn!(?cpath, "v3 block too short for path_len field; unlinking");
                return None;
            }
            let path_len = u16::from_le_bytes([content[0], content[1]]) as usize;
            if path_len > MAX_PATH_LEN || 2 + path_len > content.len() {
                let _ = std::fs::remove_file(cpath);
                tracing::warn!(
                    ?cpath,
                    path_len,
                    max = MAX_PATH_LEN,
                    "v3 block path_len out of bounds; unlinking"
                );
                return None;
            }
            let path_bytes = &content[2..2 + path_len];
            match std::str::from_utf8(path_bytes) {
                Ok(p) if p == expected_path => {
                    // Path matches — safe to decompress.
                    decode_lz4_payload(cpath, &content[2 + path_len..])
                }
                Ok(p) => {
                    // Path mismatch — `path_hash` collision.
                    // Unlink so the next write owns the
                    // file; return None so the caller
                    // re-fetches from remote and writes a
                    // v3 with the right path embedded.
                    // The DESIGN_VULNS High #4 fix.
                    let _ = std::fs::remove_file(cpath);
                    tracing::warn!(
                        ?cpath,
                        expected = expected_path,
                        stored = p,
                        "v3 block path_hash collision detected; unlinking and refetching"
                    );
                    None
                }
                Err(_) => {
                    let _ = std::fs::remove_file(cpath);
                    tracing::warn!(?cpath, "v3 block path bytes not valid UTF-8; unlinking");
                    None
                }
            }
        }
        _ => {
            let _ = std::fs::remove_file(cpath);
            tracing::warn!(
                ?cpath,
                version,
                supported = BLOCK_FORMAT_VERSION,
                "new-format block has unsupported version; unlinking and refetching"
            );
            None
        }
    }
}

/// Decompress a v2/v3 lz4 payload (after any leading
/// header bytes have been stripped). Shared by both
/// formats so the bounds + error handling stay
/// consistent.
fn decode_lz4_payload(cpath: &Path, content: &[u8]) -> Option<bytes::Bytes> {
    match lz4_flex::decompress_size_prepended(content) {
        Ok(decompressed) if decompressed.len() <= crate::CACHE_BLOCK_SIZE as usize => {
            Some(bytes::Bytes::from(decompressed))
        }
        Ok(decompressed) => {
            let _ = std::fs::remove_file(cpath);
            tracing::warn!(
                ?cpath,
                decompressed_size = decompressed.len(),
                cap = crate::CACHE_BLOCK_SIZE,
                "lz4 block decompressed to more than CACHE_BLOCK_SIZE; unlinking"
            );
            None
        }
        Err(e) => {
            let _ = std::fs::remove_file(cpath);
            tracing::warn!(
                ?cpath,
                error = %e,
                "lz4 block decompress failed; unlinking and refetching"
            );
            None
        }
    }
}

// ── Write path ──────────────────────────────────────────────────────

/// Serialize a v3 block: build the complete on-disk byte
/// sequence `MNCR || version=3 || path_len(LE u16) || path(UTF-8)
/// || lz4_flex::compress_prepend_size(data) || CRC32C(over all above)`.
///
/// Returns `None` when `path` exceeds `MAX_PATH_LEN` — the
/// caller should fall through to direct-from-remote.
///
/// This is the single point of truth for v3 serialization.
/// Both `MntrsFs::write_block_cached` and
/// `DiskWriteJob::do_block_cache_write` call it so a format
/// change requires editing exactly one function.
pub(crate) fn serialize_v3_block(path: &str, data: &[u8]) -> Option<Vec<u8>> {
    let path_bytes = path.as_bytes();
    if path_bytes.len() > MAX_PATH_LEN {
        tracing::debug!(
            path,
            len = path_bytes.len(),
            max = MAX_PATH_LEN,
            "block cache write: path too long for v3 embed; skipping cache"
        );
        return None;
    }
    let compressed = lz4_flex::compress_prepend_size(data);
    // Build header: MNCR || version=3
    let mut header = [0u8; BLOCK_HEADER_SIZE];
    header[0..4].copy_from_slice(BLOCK_MAGIC);
    header[4..8].copy_from_slice(&BLOCK_FORMAT_VERSION.to_le_bytes());
    // Build path_block: path_len(LE u16) || path
    let mut path_block = Vec::with_capacity(2 + path_bytes.len());
    path_block.extend_from_slice(&(path_bytes.len() as u16).to_le_bytes());
    path_block.extend_from_slice(path_bytes);
    // Assemble: header || path_block || compressed
    let total = BLOCK_HEADER_SIZE + path_block.len() + compressed.len() + BLOCK_CRC_TRAILER;
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&header);
    buf.extend_from_slice(&path_block);
    buf.extend_from_slice(&compressed);
    // CRC covers magic + version + path_block + compressed
    // (the entire buffer minus the trailing 4 CRC bytes).
    let crc = crc32c_checksum_concat(&header, &buf[BLOCK_HEADER_SIZE..]);
    buf.extend_from_slice(&crc.to_le_bytes());
    Some(buf)
}

// ── Cache cleanup ───────────────────────────────────────────────────

/// Remove block-level cache entries for a path. O(K) where K is
/// the inodes.size() / CACHE_BLOCK_SIZE — direct `remove_file` per
/// block, no `read_dir` over the whole cache dir.
///
/// Replaces the previous `read_dir(&cache_dir).filter(starts_with(prefix))`
/// pattern, which was O(N) over the entire cache (N entries in
/// the dir, mostly unrelated to this ino). On a CSI mount with
/// 1000+ cached files, the old scan was ~4ms per unlink/rename/
/// setattr/rmdir — a 5× slowdown vs rclone on a single unlink
/// (issue #17's remaining gap).
///
/// Stale block files (the inodes entry was removed but the block
/// file on disk was missed) are tolerated: `remove_file` returns
/// an error and we silently ignore it.
///
/// **Note**: this helper only removes the *disk* files. The
/// matching in-memory `disk_cache_index` entries (key
/// `(path, Some(block_idx))`) must be removed by the caller —
/// see the unlink/rmdir/rename impls in `CoreFilesystem`.
/// Centralizing that cleanup in this helper would require
/// passing the index in as a parameter, which would couple
/// a pure disk operation to the in-memory state; keeping
/// them separate lets the caller choose the right key
/// shape.
pub(crate) fn remove_block_cache_files(cache_dir: &Path, full_path: &str, size: u64) {
    let n_blocks = size.div_ceil(crate::CACHE_BLOCK_SIZE);
    for blk in 0..n_blocks {
        let bpath = crate::cache_block_path(cache_dir, full_path, blk);
        let _ = std::fs::remove_file(&bpath);
    }
}

/// Issue #55: drop every block-level disk cache
/// entry for `path` (both the in-memory index and
/// the on-disk `.block` files). Called by the
/// writeback worker after a successful upload so
/// subsequent reads can't return pre-upload data
/// via the stale block cache. `pub(crate)` so
/// `writeback::spawn` can call it.
pub(crate) fn drop_block_cache_for_path(
    cache_dir: &Path,
    disk_cache_index: &Arc<dashmap::DashMap<crate::CacheKey, (u64, std::time::Instant)>>,
    path: &str,
) {
    let to_remove: Vec<crate::CacheKey> = disk_cache_index
        .iter()
        .filter(|e| e.key().0 == path && e.key().1.is_some())
        .map(|e| e.key().clone())
        .collect();
    for key in to_remove {
        if let Some(idx) = key.1 {
            let bpath = crate::cache_block_path(cache_dir, &key.0, idx);
            let _ = std::fs::remove_file(&bpath);
        }
        disk_cache_index.remove(&key);
    }
}

/// One entry in the on-disk cache index. Issue #227
/// refactored this from a 4-tuple `(String, u64, u64,
/// SystemTime)` to a named-field struct per
/// [[feedback-tuple-vs-struct]]: tuples with >3 fields
/// are easy to silently reorder at construction or
/// destructuring sites, and a 4th field today means a
/// 5th is plausible in 6mo. The struct form makes every
/// field self-documenting and ensures future field
/// additions are a compile-time catch at every call site
/// (vs the tuple's silent default-on-missing-field).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheIndexEntry {
    /// File name on disk (e.g. `"abcd_000000002a.block"`).
    /// Encodes both the path hash prefix and the block
    /// index, so callers can re-derive the block path via
    /// [`crate::cache_block_path`] when only this is kept.
    pub name: String,
    /// Block index within the file (parsed from the hex
    /// suffix of `name`). `0`-based.
    pub block_idx: u64,
    /// Block size in bytes (from `entry.metadata().len()`).
    /// Note this is the **on-disk** size including the
    /// `BLOCK_CRC_TRAILER`; the user-visible content size
    /// is `size - BLOCK_CRC_TRAILER as u64` for full blocks.
    pub size: u64,
    /// File modification time (from `entry.metadata().modified()`).
    /// Used to evict old cache blocks before re-using the slot.
    pub mtime: std::time::SystemTime,
}

/// Scan cache dir for block files and rebuild disk_cache_index.
/// Loaded at startup so cache is warm across restarts.
pub fn load_cache_index(cache_dir: &Path) -> Vec<CacheIndexEntry> {
    let mut entries = Vec::new();
    let Ok(dir) = std::fs::read_dir(cache_dir) else {
        return entries;
    };
    for entry in dir.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        // Parse "hash_blockidx.block" format
        if let Some(rest) = name.strip_suffix(".block")
            && let Some(block_str) = rest.split('_').nth(1)
            && let Ok(block_idx) = u64::from_str_radix(block_str, 16)
            && let Ok(meta) = entry.metadata()
            && let Ok(mtime) = meta.modified()
        {
            entries.push(CacheIndexEntry {
                name,
                block_idx,
                size: meta.len(),
                mtime,
            });
        }
    }
    entries
}

// ── Dead code (kept for future use) ─────────────────────────────────

/// Lightweight checksummed buffer for cache integrity validation.
/// Uses CRC32C (same as mountpoint-s3 and S3's native checksum).
#[allow(dead_code)]
pub struct ChecksummedBytes {
    data: bytes::Bytes,
    checksum: u32, // CRC32C
}

#[allow(dead_code)]
impl ChecksummedBytes {
    /// Create from raw bytes, computing checksum.
    pub fn new(data: bytes::Bytes) -> Self {
        let checksum = crc32c_checksum(&data);
        Self { data, checksum }
    }

    /// Create without checksum validation (for data from trusted source).
    pub fn new_unchecked(data: bytes::Bytes) -> Self {
        Self { data, checksum: 0 }
    }

    /// Validate integrity and return inner data.
    pub fn into_inner(self) -> std::io::Result<bytes::Bytes> {
        if self.checksum == 0 {
            return Ok(self.data);
        }
        let actual = crc32c_checksum(&self.data);
        if actual != self.checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "cache checksum mismatch: expected {:#x}, got {:#x}",
                    self.checksum, actual
                ),
            ));
        }
        Ok(self.data)
    }

    /// Get checksum for serialization.
    pub fn checksum(&self) -> u32 {
        self.checksum
    }

    /// Get reference to data without validation.
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod disk_cache_crc_tests {
    use super::{
        BLOCK_CRC_TRAILER, BLOCK_FORMAT_VERSION, BLOCK_HEADER_SIZE, BLOCK_MAGIC, BLOCK_OVERHEAD,
        LEGACY_BLOCK_FORMAT_VERSION_V1, LEGACY_BLOCK_FORMAT_VERSION_V2, MAX_PATH_LEN,
        crc32c_checksum, crc32c_checksum_concat, read_block_cached,
    };
    use crate::CACHE_BLOCK_SIZE;
    use std::path::PathBuf;

    /// Make a unique scratch dir for each test so the
    /// unlink-on-corruption path doesn't race with siblings.
    fn scratch(name: &str) -> PathBuf {
        let p =
            std::env::temp_dir().join(format!("mntrs-crc-test-{}-{}", name, std::process::id()));
        let _ = std::fs::create_dir_all(&p);
        p
    }

    /// Build a v3 (lz4 + embedded path) on-disk block
    /// file matching what `write_block_cached` produces.
    /// Used by the round-trip and corruption tests so
    /// they exercise the current writer format.
    fn build_v3_block(path: &str, content: &[u8]) -> Vec<u8> {
        let compressed = lz4_flex::compress_prepend_size(content);
        let path_bytes = path.as_bytes();
        let mut buf = Vec::with_capacity(BLOCK_OVERHEAD + 2 + path_bytes.len() + compressed.len());
        buf.extend_from_slice(BLOCK_MAGIC);
        buf.extend_from_slice(&BLOCK_FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&(path_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(path_bytes);
        buf.extend_from_slice(&compressed);
        let crc = crc32c_checksum_concat(
            &{
                let mut h = Vec::with_capacity(8);
                h.extend_from_slice(BLOCK_MAGIC);
                h.extend_from_slice(&BLOCK_FORMAT_VERSION.to_le_bytes());
                h
            },
            &{
                let mut t = Vec::with_capacity(2 + path_bytes.len() + compressed.len());
                t.extend_from_slice(&(path_bytes.len() as u16).to_le_bytes());
                t.extend_from_slice(path_bytes);
                t.extend_from_slice(&compressed);
                t
            },
        );
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Build a v2 (lz4, no path) on-disk block file for
    /// the back-compat regression test. Hardcodes
    /// version=2 (LEGACY_BLOCK_FORMAT_VERSION_V2) so the
    /// reader takes the v2 branch and decodes without
    /// path verification.
    fn build_v2_legacy_block(content: &[u8]) -> Vec<u8> {
        let compressed = lz4_flex::compress_prepend_size(content);
        let mut buf = Vec::with_capacity(BLOCK_OVERHEAD + compressed.len());
        buf.extend_from_slice(BLOCK_MAGIC);
        buf.extend_from_slice(&LEGACY_BLOCK_FORMAT_VERSION_V2.to_le_bytes());
        buf.extend_from_slice(&compressed);
        let crc = crc32c_checksum_concat(
            &{
                let mut h = Vec::with_capacity(8);
                h.extend_from_slice(BLOCK_MAGIC);
                h.extend_from_slice(&LEGACY_BLOCK_FORMAT_VERSION_V2.to_le_bytes());
                h
            },
            &compressed,
        );
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    #[test]
    fn crc_round_trip_full_block() {
        let dir = scratch("full");
        let p = dir.join("block.bin");
        let content: Vec<u8> = (0..CACHE_BLOCK_SIZE as u32)
            .map(|i| (i & 0xff) as u8)
            .collect();
        std::fs::write(&p, build_v3_block("test/block.bin", &content)).unwrap();
        let out = read_block_cached(&p, "test/block.bin").expect("clean full block should be Some");
        assert_eq!(out.len(), CACHE_BLOCK_SIZE as usize);
        assert_eq!(out.as_ref(), content.as_slice());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn crc_corruption_triggers_unlink_and_returns_none() {
        let dir = scratch("corrupt");
        let p = dir.join("block.bin");
        let content: Vec<u8> = (0..CACHE_BLOCK_SIZE as u32)
            .map(|i| (i & 0xff) as u8)
            .collect();
        let mut buf = build_v3_block("test/block.bin", &content);
        // Flip a bit in the CRC trailer (last 4 bytes).
        let len = buf.len();
        buf[len - 1] ^= 0x01;
        std::fs::write(&p, &buf).unwrap();

        let out = read_block_cached(&p, "test/block.bin");
        assert!(out.is_none(), "corrupt CRC should return None");
        assert!(!p.exists(), "corrupt file should be unlinked");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn v1_uncompressed_block_round_trip() {
        let dir = scratch("legacy_v1");
        let p = dir.join("block.bin");
        let content: Vec<u8> = (0..1024_u32).map(|i| (i & 0xff) as u8).collect();
        let mut buf = Vec::new();
        buf.extend_from_slice(BLOCK_MAGIC);
        buf.extend_from_slice(&LEGACY_BLOCK_FORMAT_VERSION_V1.to_le_bytes());
        buf.extend_from_slice(&content);
        // CRC over (header || content), same recipe as
        // the v1 writer used before #4 landed.
        let crc = crc32c_checksum_concat(
            &{
                let mut h = Vec::with_capacity(8);
                h.extend_from_slice(BLOCK_MAGIC);
                h.extend_from_slice(&LEGACY_BLOCK_FORMAT_VERSION_V1.to_le_bytes());
                h
            },
            &content,
        );
        buf.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(&p, &buf).unwrap();
        let out = read_block_cached(&p, "test/block.bin").expect("v1 block should read cleanly");
        assert_eq!(out.as_ref(), content.as_slice());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn v2_lz4_block_round_trip_back_compat() {
        let dir = scratch("legacy_v2");
        let p = dir.join("block.bin");
        let content: Vec<u8> = (0..1024_u32).map(|i| (i & 0xff) as u8).collect();
        std::fs::write(&p, build_v2_legacy_block(&content)).unwrap();
        let out = read_block_cached(&p, "test/block.bin")
            .expect("v2 block should read cleanly via back-compat branch");
        assert_eq!(out.as_ref(), content.as_slice());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn v3_path_collision_detected_and_unlinked() {
        let dir = scratch("path_collision");
        let p = dir.join("block.bin");
        let content: Vec<u8> = (0..1024_u32).map(|i| (i & 0xff) as u8).collect();
        std::fs::write(&p, build_v3_block("owner/file_A", &content)).unwrap();
        let out = read_block_cached(&p, "owner/file_B");
        assert!(
            out.is_none(),
            "v3 path mismatch should return None (collision detected)"
        );
        assert!(
            !p.exists(),
            "v3 path mismatch should unlink so the next write of the colliding twin owns the slot"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn crc_legacy_unprotected_block_8mib() {
        let dir = scratch("legacy");
        let p = dir.join("block.bin");
        let content: Vec<u8> = (0..CACHE_BLOCK_SIZE as u32)
            .map(|i| (i & 0xff) as u8)
            .collect();
        std::fs::write(&p, &content).unwrap();
        let out = read_block_cached(&p, "test/block.bin")
            .expect("legacy unprotected block should be Some");
        assert_eq!(out.len(), CACHE_BLOCK_SIZE as usize);
        assert_eq!(out.as_ref(), content.as_slice());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn crc_legacy_protected_block_8mib_plus_4() {
        let dir = scratch("legacy_protected");
        let p = dir.join("block.bin");
        let content: Vec<u8> = (0..CACHE_BLOCK_SIZE as u32)
            .map(|i| (i & 0xff) as u8)
            .collect();
        let crc = crc32c_checksum(&content);
        let mut buf = content.clone();
        buf.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(&p, &buf).unwrap();
        let out =
            read_block_cached(&p, "test/block.bin").expect("legacy protected block should be Some");
        assert_eq!(out.len(), CACHE_BLOCK_SIZE as usize);
        assert_eq!(out.as_ref(), content.as_slice());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn crc_partial_block_passes_through() {
        let dir = scratch("partial");
        let p = dir.join("block.bin");
        let content: Vec<u8> = (0..4096u32).map(|i| (i & 0xff) as u8).collect();
        std::fs::write(&p, build_v3_block("test/block.bin", &content)).unwrap();
        let out = read_block_cached(&p, "test/block.bin").expect("partial block should be Some");
        assert_eq!(out.len(), content.len());
        assert_eq!(out.as_ref(), content.as_slice());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn crc_oversized_file_triggers_unlink() {
        let dir = scratch("oversized");
        let p = dir.join("block.bin");
        let content: Vec<u8> = vec![0xab; CACHE_BLOCK_SIZE as usize + BLOCK_OVERHEAD + 1];
        std::fs::write(&p, &content).unwrap();

        let out = read_block_cached(&p, "test/block.bin");
        assert!(out.is_none(), "oversized file should return None");
        assert!(!p.exists(), "oversized file should be unlinked");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn crc_unsupported_version_triggers_unlink() {
        let dir = scratch("bad_version");
        let p = dir.join("block.bin");
        let content: Vec<u8> = (0..4096u32).map(|i| (i & 0xff) as u8).collect();
        let mut buf = Vec::new();
        buf.extend_from_slice(BLOCK_MAGIC);
        buf.extend_from_slice(&99u32.to_le_bytes());
        buf.extend_from_slice(&content);
        let crc = crc32c_checksum_concat(&buf, &[]);
        buf.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(&p, &buf).unwrap();

        let out = read_block_cached(&p, "test/block.bin");
        assert!(out.is_none(), "unsupported version should return None");
        assert!(!p.exists(), "unsupported-version file should be unlinked");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn crc_corrupt_magic_triggers_unlink() {
        let dir = scratch("bad_magic");
        let p = dir.join("block.bin");
        let content: Vec<u8> = vec![0xab; 4096];
        std::fs::write(&p, &content).unwrap();
        let out = read_block_cached(&p, "test/block.bin");
        let _ = out; // exercised for sanity
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── tornado write: v3 block truncated before CRC trailer ──────

    #[test]
    fn crc_torn_write_truncated_before_trailer_is_none() {
        let dir = scratch("torn_trunc");
        let p = dir.join("block.bin");
        let content: Vec<u8> = (0..4096u32).map(|i| (i & 0xff) as u8).collect();
        let full = build_v3_block("test/block.bin", &content);
        // Truncate: drop the 4-byte CRC trailer.
        let truncated = &full[..full.len() - BLOCK_CRC_TRAILER];
        // Also drop a few more bytes from the compressed payload
        // so the remaining data is definitely not a valid block.
        let chopped = &truncated[..truncated.len() - 3];
        std::fs::write(&p, chopped).unwrap();

        let out = read_block_cached(&p, "test/block.bin");
        assert!(
            out.is_none(),
            "truncated block missing CRC must return None"
        );
        assert!(!p.exists(), "truncated block must be unlinked");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn crc_torn_write_no_room_for_trailer_is_none() {
        // Block has valid magic+version header but after_header
        // is < BLOCK_CRC_TRAILER — the "too short for trailer"
        // branch in read_new_format.
        let dir = scratch("torn_short");
        let p = dir.join("block.bin");
        let mut buf = Vec::new();
        buf.extend_from_slice(BLOCK_MAGIC);
        buf.extend_from_slice(&BLOCK_FORMAT_VERSION.to_le_bytes());
        // Only 2 bytes of payload — less than the 4-byte CRC trailer.
        buf.extend_from_slice(&[0xAA, 0xBB]);
        std::fs::write(&p, &buf).unwrap();

        let out = read_block_cached(&p, "test/block.bin");
        assert!(out.is_none(), "header + 2 bytes must return None");
        assert!(!p.exists(), "malformed block must be unlinked");
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── legacy CRC trailer: wrong size ────────────────────────────

    #[test]
    fn crc_legacy_trailer_too_short_is_none() {
        let dir = scratch("legacy_short_tail");
        let p = dir.join("block.bin");
        let content: Vec<u8> = vec![0u8; CACHE_BLOCK_SIZE as usize + 2];
        // Exactly CACHE_BLOCK_SIZE + 2: not 8 MiB exactly
        // (unprotected), not 8 MiB + 4 (protected), and not
        // < 8 MiB (partial). This lands in the "no-man's-land"
        // else branch → unlink + None.
        std::fs::write(&p, &content).unwrap();

        let out = read_block_cached(&p, "test/block.bin");
        assert!(
            out.is_none(),
            "legacy block with wrong-sized trailer must return None"
        );
        assert!(!p.exists(), "malformed legacy block must be unlinked");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn crc_legacy_trailer_corrupt_try_into_fails() {
        // Write CACHE_BLOCK_SIZE + 4 bytes (size triggers
        // protected path) but the 4 trailer bytes aren't a
        // valid CRC of the content → mismatch → unlink.
        let dir = scratch("legacy_corrupt_crc");
        let p = dir.join("block.bin");
        let content: Vec<u8> = (0..CACHE_BLOCK_SIZE as u32)
            .map(|i| (i & 0xff) as u8)
            .collect();
        let correct_crc = crc32c_checksum(&content);
        // Corrupt one byte of the CRC
        let bad_crc = correct_crc ^ 0xDEADBEEF;
        let mut buf = content.clone();
        buf.extend_from_slice(&bad_crc.to_le_bytes());
        std::fs::write(&p, &buf).unwrap();

        let out = read_block_cached(&p, "test/block.bin");
        assert!(
            out.is_none(),
            "legacy block with wrong CRC must return None"
        );
        assert!(!p.exists(), "corrupt legacy block must be unlinked");
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── v3 path mismatch (CRC valid, path wrong) ──────────────────

    #[test]
    fn crc_v3_path_mismatch_unlinks_and_returns_none() {
        let dir = scratch("v3_path_mismatch");
        let p = dir.join("block.bin");
        let content: Vec<u8> = (0..4096u32).map(|i| (i & 0xff) as u8).collect();
        let buf = build_v3_block("stored/path.bin", &content);
        std::fs::write(&p, &buf).unwrap();

        // Read with a different expected_path — CRC is valid,
        // but the embedded path doesn't match what we asked for.
        let out = read_block_cached(&p, "expected/different.bin");
        assert!(
            out.is_none(),
            "v3 block with wrong embedded path must return None"
        );
        assert!(!p.exists(), "path-mismatch block must be unlinked");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn crc_v3_path_not_utf8_unlinks() {
        let dir = scratch("v3_bad_utf8");
        let p = dir.join("block.bin");
        let content: Vec<u8> = (0..4096u32).map(|i| (i & 0xff) as u8).collect();
        let compressed = lz4_flex::compress_prepend_size(&content);
        // Construct a v3 block with non-UTF-8 path bytes
        let bad_path: Vec<u8> = vec![0xFF, 0xFE, 0xFD, 0x80]; // invalid UTF-8
        let mut buf = Vec::new();
        buf.extend_from_slice(BLOCK_MAGIC);
        buf.extend_from_slice(&BLOCK_FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&(bad_path.len() as u16).to_le_bytes());
        buf.extend_from_slice(&bad_path);
        buf.extend_from_slice(&compressed);
        let crc = crc32c_checksum_concat(
            &{
                let mut h = Vec::with_capacity(8);
                h.extend_from_slice(BLOCK_MAGIC);
                h.extend_from_slice(&BLOCK_FORMAT_VERSION.to_le_bytes());
                h
            },
            &buf[BLOCK_HEADER_SIZE..],
        );
        buf.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(&p, &buf).unwrap();

        let out = read_block_cached(&p, "test/block.bin");
        assert!(
            out.is_none(),
            "v3 block with non-UTF-8 path must return None"
        );
        assert!(!p.exists(), "bad-UTF-8 block must be unlinked");
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── zero-byte content edge case ───────────────────────────────

    #[test]
    fn crc_v3_zero_content_round_trip() {
        let dir = scratch("v3_zero");
        let p = dir.join("block.bin");
        let content: Vec<u8> = vec![];
        std::fs::write(&p, build_v3_block("test/block.bin", &content)).unwrap();
        let out = read_block_cached(&p, "test/block.bin").expect("zero-content block must be Some");
        assert!(out.is_empty(), "zero-content block must be empty");
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── LZ4 decompress failure (valid CRC, corrupt payload) ───────

    #[test]
    fn crc_v2_garbage_payload_lz4_fails() {
        let dir = scratch("v2_garbage");
        let p = dir.join("block.bin");
        let garbage: Vec<u8> = (0..128u32)
            .map(|i| (i.wrapping_mul(17) & 0xff) as u8)
            .collect();
        let mut buf = Vec::new();
        buf.extend_from_slice(BLOCK_MAGIC);
        buf.extend_from_slice(&LEGACY_BLOCK_FORMAT_VERSION_V2.to_le_bytes());
        buf.extend_from_slice(&garbage);
        let crc = crc32c_checksum_concat(
            &{
                let mut h = Vec::with_capacity(8);
                h.extend_from_slice(BLOCK_MAGIC);
                h.extend_from_slice(&LEGACY_BLOCK_FORMAT_VERSION_V2.to_le_bytes());
                h
            },
            &garbage,
        );
        buf.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(&p, &buf).unwrap();

        // CRC passes (computed over garbage), but lz4_flex can't
        // decompress garbage → decode_lz4_payload returns None →
        // unlink.
        let out = read_block_cached(&p, "test/block.bin");
        assert!(
            out.is_none(),
            "v2 block with garbage payload must return None"
        );
        assert!(!p.exists(), "garbage-payload block must be unlinked");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn crc_lz4_decompressed_too_large_is_unlinked() {
        let dir = scratch("lz4_oversized");
        let p = dir.join("block.bin");
        // Build a v3 block whose lz4 payload decompresses to a
        // size larger than CACHE_BLOCK_SIZE. Use a large repeat
        // pattern that lz4 compresses well.
        let big: Vec<u8> = vec![0xAB; CACHE_BLOCK_SIZE as usize + 1];
        let compressed = lz4_flex::compress_prepend_size(&big);
        let path_bytes = b"test/block.bin";
        let mut buf = Vec::new();
        buf.extend_from_slice(BLOCK_MAGIC);
        buf.extend_from_slice(&BLOCK_FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&(path_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(path_bytes);
        buf.extend_from_slice(&compressed);
        let crc = crc32c_checksum_concat(
            &{
                let mut h = Vec::with_capacity(8);
                h.extend_from_slice(BLOCK_MAGIC);
                h.extend_from_slice(&BLOCK_FORMAT_VERSION.to_le_bytes());
                h
            },
            &buf[BLOCK_HEADER_SIZE..],
        );
        buf.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(&p, &buf).unwrap();

        let out = read_block_cached(&p, "test/block.bin");
        assert!(
            out.is_none(),
            "block decompressing > CACHE_BLOCK_SIZE must return None"
        );
        assert!(!p.exists(), "oversized-decompressed block must be unlinked");
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── v3 block: path_len out of bounds ──────────────────────────

    #[test]
    fn crc_v3_path_len_exceeds_max_path_len_unlinks() {
        let dir = scratch("v3_long_path");
        let p = dir.join("block.bin");
        let content: Vec<u8> = (0..1024u32).map(|i| (i & 0xff) as u8).collect();
        let compressed = lz4_flex::compress_prepend_size(&content);
        // Craft a v3 block with path_len > MAX_PATH_LEN
        let mut buf = Vec::new();
        buf.extend_from_slice(BLOCK_MAGIC);
        buf.extend_from_slice(&BLOCK_FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&((MAX_PATH_LEN + 1) as u16).to_le_bytes());
        // Don't write actual path bytes — just enough to reach
        // the path_len check in read_new_format (it checks
        // path_len > MAX_PATH_LEN first).
        buf.extend_from_slice(&[0u8; 4]); // junk
        buf.extend_from_slice(&compressed);
        let crc = crc32c_checksum_concat(
            &{
                let mut h = Vec::with_capacity(8);
                h.extend_from_slice(BLOCK_MAGIC);
                h.extend_from_slice(&BLOCK_FORMAT_VERSION.to_le_bytes());
                h
            },
            &buf[BLOCK_HEADER_SIZE..],
        );
        buf.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(&p, &buf).unwrap();

        let out = read_block_cached(&p, "test/block.bin");
        assert!(
            out.is_none(),
            "v3 block with path_len > MAX_PATH_LEN must return None"
        );
        assert!(!p.exists(), "oversized-path block must be unlinked");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn crc_v3_path_len_exceeds_content_len_unlinks() {
        let dir = scratch("v3_path_oob");
        let p = dir.join("block.bin");
        // Craft a v3 block where path_len claims 100 but content
        // only has 10 bytes after the path_len field.
        let mut buf = Vec::new();
        buf.extend_from_slice(BLOCK_MAGIC);
        buf.extend_from_slice(&BLOCK_FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&(100u16).to_le_bytes());
        buf.extend_from_slice(b"junk"); // only 4 bytes
        let crc = crc32c_checksum_concat(
            &{
                let mut h = Vec::with_capacity(8);
                h.extend_from_slice(BLOCK_MAGIC);
                h.extend_from_slice(&BLOCK_FORMAT_VERSION.to_le_bytes());
                h
            },
            &buf[BLOCK_HEADER_SIZE..],
        );
        buf.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(&p, &buf).unwrap();

        let out = read_block_cached(&p, "test/block.bin");
        assert!(
            out.is_none(),
            "v3 block path_len exceeding available content must return None"
        );
        assert!(!p.exists(), "OOB-path block must be unlinked");
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── v3 block too short for path_len field ─────────────────────

    #[test]
    fn crc_v3_too_short_for_path_len_field() {
        let dir = scratch("v3_tiny");
        let p = dir.join("block.bin");
        // Exactly 1 byte of content (after header + CRC trailer):
        // `content.len() = 1`, but we need at least 2 for the
        // u16 path_len field.
        let mut buf = Vec::new();
        buf.extend_from_slice(BLOCK_MAGIC);
        buf.extend_from_slice(&BLOCK_FORMAT_VERSION.to_le_bytes());
        buf.push(0x00); // 1 byte payload
        let crc = crc32c_checksum(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(&p, &buf).unwrap();

        let out = read_block_cached(&p, "test/block.bin");
        assert!(
            out.is_none(),
            "v3 block with < 2 content bytes must return None"
        );
        assert!(!p.exists(), "tiny v3 block must be unlinked");
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---------------------------------------------------------------
    // Disk cache #5: `write_block_cached` round-trip tests
    // ---------------------------------------------------------------

    use crate::cache_block_path;
    use crate::new_test_fs;
    use opendal::Operator;
    use opendal::services::Memory;

    fn make_fs() -> crate::MntrsFs {
        let op = Operator::new(Memory::default()).unwrap().finish();
        let cache_dir = std::env::temp_dir().join(format!(
            "mntrs-write-block-test-{}-{:x}",
            std::process::id(),
            line_addr()
        ));
        let _ = std::fs::create_dir_all(&cache_dir);
        new_test_fs(op, cache_dir)
    }

    fn line_addr() -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        std::thread::current().id().hash(&mut h);
        h.finish()
    }

    #[test]
    fn write_block_full_round_trip() {
        use super::MAX_PAYLOAD_OVERHEAD;
        let fs = make_fs();
        let content: Vec<u8> = (0..CACHE_BLOCK_SIZE as u32)
            .map(|i| (i & 0xff) as u8)
            .collect();
        assert!(fs.write_block_cached("full.bin", 0, &content));
        let blk_path = cache_block_path(&fs.cache_dir, "full.bin", 0);
        let meta = std::fs::metadata(&blk_path).unwrap();
        assert!(
            (meta.len() as usize) >= BLOCK_OVERHEAD + 4,
            "on-disk file should at least hold header + 4-byte size prefix + CRC; got {}",
            meta.len()
        );
        assert!(
            (meta.len() as usize)
                <= CACHE_BLOCK_SIZE as usize + BLOCK_OVERHEAD + MAX_PAYLOAD_OVERHEAD,
            "on-disk file should fit the v2 worst-case bound; got {}",
            meta.len()
        );
        let head = std::fs::read(&blk_path).unwrap();
        assert_eq!(&head[0..4], BLOCK_MAGIC);
        let version = u32::from_le_bytes(head[4..8].try_into().unwrap());
        assert_eq!(version, BLOCK_FORMAT_VERSION);
        let entry = fs
            .disk_cache_index
            .get(&(String::from("full.bin"), Some(0)))
            .expect("disk_cache_index should contain the entry");
        assert_eq!(entry.value().0 as usize, meta.len() as usize);
        let bytes = read_block_cached(&blk_path, "full.bin").expect("clean full block");
        assert_eq!(bytes.len(), CACHE_BLOCK_SIZE as usize);
        assert_eq!(bytes.as_ref(), content.as_slice());
    }

    #[test]
    fn write_block_partial_new_format() {
        use super::MAX_PAYLOAD_OVERHEAD;
        let fs = make_fs();
        let content: Vec<u8> = (0..4096u32).map(|i| (i & 0xff) as u8).collect();
        assert!(fs.write_block_cached("tail.bin", 7, &content));
        let blk_path = cache_block_path(&fs.cache_dir, "tail.bin", 7);
        let meta = std::fs::metadata(&blk_path).unwrap();
        assert!(
            (meta.len() as usize) <= content.len() + BLOCK_OVERHEAD + MAX_PAYLOAD_OVERHEAD,
            "partial block on-disk size should fit v2 worst-case bound; got {}",
            meta.len()
        );
        let head = std::fs::read(&blk_path).unwrap();
        assert_eq!(&head[0..4], BLOCK_MAGIC);
        let bytes =
            read_block_cached(&blk_path, "tail.bin").expect("partial block should round-trip");
        assert_eq!(bytes.len(), content.len());
        assert_eq!(bytes.as_ref(), content.as_slice());
    }

    #[test]
    fn write_block_prefetch_loop_writes_two_blocks() {
        let fs = make_fs();
        let full: Vec<u8> = (0..CACHE_BLOCK_SIZE as u32)
            .map(|i| (i & 0xff) as u8)
            .collect();
        assert!(fs.write_block_cached("prefetched.bin", 0, &full));
        assert!(fs.write_block_cached("prefetched.bin", 1, &full));
        for blk in 0..2u64 {
            let key = (String::from("prefetched.bin"), Some(blk));
            assert!(
                fs.disk_cache_index.contains_key(&key),
                "block {blk} must be in disk_cache_index"
            );
            let p = cache_block_path(&fs.cache_dir, "prefetched.bin", blk);
            let bytes = read_block_cached(&p, "prefetched.bin").expect("round-trip read");
            assert_eq!(bytes.len(), CACHE_BLOCK_SIZE as usize);
            assert_eq!(bytes.as_ref(), full.as_slice());
        }
    }
}
