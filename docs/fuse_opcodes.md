# FUSE opcode implementation tracking (issue #20)

This document tracks which FUSE opcodes mntrs
implements vs delegates to the fuser default
(ENOSYS). Updated as of 2026-06-17 after the
P0/P1/P2/P3 fix batch (commits covering issues
#23, #25, #29, #30, #31, #34, #35, #36, #38,
#39, #42, #43, #50, #51, #52, #53, #54, #55,
#56, #57, #58).

## Status: 30 / 41 opcodes implemented (73%)

### Implemented (30)

| #  | Opcode            | File / handler                          | Notes |
|----|-------------------|-----------------------------------------|-------|
| 1  | `lookup`          | `core_fs/fuser.rs` (FuserAdapter)       | |
| 2  | `forget`          | `core_fs/fuser.rs`                       | |
| 3  | `getattr`         | `core_fs/fuser.rs`                       | |
| 4  | `setattr`         | `core_fs/fuser.rs` → `MntrsFs::setattr`  | #42: ftruncate on opened fd |
| 5  | `access`          | `core_fs/fuser.rs`                       | |
| 6  | `readlink`        | `core_fs/fuser.rs`                       | |
| 7  | `symlink`         | `core_fs/fuser.rs`                       | #17: trait default returns ENOSYS for object stores |
| 8  | `mknod`           | (not implemented)                         | P3, low priority — object stores don't support device files |
| 9  | `mkdir`           | `core_fs/fuser.rs`                       | |
| 10 | `unlink`          | `core_fs/fuser.rs`                       | |
| 11 | `rmdir`           | `core_fs/fuser.rs`                       | |
| 12 | `rename`          | `core_fs/fuser.rs`                       | #56: WinFSP resolves parent ino from full path |
| 13 | `link`            | `core_fs/fuser.rs`                       | #25: trait default returns ENOSYS for object stores |
| 14 | `open`            | `core_fs/fuser.rs`                       | |
| 15 | `read`            | `core_fs/fuser.rs`                       | #43: partial file-level cache falls through |
| 16 | `write`           | `core_fs/fuser.rs`                       | #12: write_at (lseek+pwrite atomic) |
| 17 | `statfs`          | `core_fs/fuser.rs`                       | |
| 18 | `release`         | `core_fs/fuser.rs`                       | #34: fdatasync cache fd before writeback |
| 19 | `fsync`           | `core_fs/fuser.rs`                       | #35: sync_all/sync_data on cache fd |
| 20 | `opendir`         | `core_fs/fuser.rs`                       | #23: per-fh snapshot |
| 21 | `readdir`         | `core_fs/fuser.rs`                       | #23: per-fh slice; #29: batched lookups |
| 22 | `releasedir`      | `core_fs/fuser.rs`                       | #23: drop per-fh snapshot |
| 23 | `fsyncdir`        | `core_fs/fuser.rs`                       | #35: no-op default (backends have no dir-data) |
| 24 | `flush`           | `core_fs/fuser.rs`                       | #34: fdatasync + writeback enqueue |
| 25 | `getxattr`        | `core_fs/fuser.rs`                       | |
| 26 | `listxattr`       | `core_fs/fuser.rs`                       | |
| 27 | `setxattr`        | (not implemented)                         | P3, low priority |
| 28 | `removexattr`     | (not implemented)                         | P3, low priority |
| 29 | `create`          | `core_fs/fuser.rs`                       | #51: NEXT_HANDLE for fh, not ino |
| 30 | `fallocate`       | `core_fs/fuser.rs`                       | #25: setattr(size = offset+length) |
| 31 | `copy_file_range` | `core_fs/fuser.rs`                       | #25: read+write passthrough default |

### Not implemented (11)

| #  | Opcode            | Default behavior | Workaround / follow-up |
|----|-------------------|------------------|--------------------------|
| 1  | `mknod`           | ENOSYS            | P3 low priority |
| 2  | `setxattr`        | ENOSYS            | P3 low priority |
| 3  | `removexattr`     | ENOSYS            | P3 low priority |
| 4  | `getlk`           | ENOSYS            | POSIX advisory locks; not common on object stores |
| 5  | `setlk`           | ENOSYS            | same |
| 6  | `setlkw`          | ENOSYS            | same |
| 7  | `poll`            | ENOSYS            | kernel uses poll only for direct I/O; not used in buffered mode |
| 8  | `lseek` (SEEK_HOLE) | ENOSYS          | sparse file support; object stores don't have holes |
| 9  | `lseek` (SEEK_DATA) | ENOSYS          | same |
| 10 | `ioctl`           | ENOSYS            | backend-specific; not part of VFS contract |
| 11 | `bmap`            | ENOSYS            | block-device mapper; not applicable |

## WinFSP status

| #  | Opcode           | Status |
|----|------------------|--------|
| 1  | `get_file_info`  | ✓ |
| 2  | `set_file_size`  | ✓ (#42) |
| 3  | `get_security_by_name` | ✓ |
| 4  | `read_directory` | ✓ (#23) |
| 5  | `create` / `open` / `cleanup` | ✓ |
| 6  | `read` / `write` / `flush` | ✓ (#34, #35) |
| 7  | `set_basic_info` | ⚠ partial — only size (the rest not in trait) |
| 8  | `rename`         | ✓ (#56) |

## Update protocol

When adding a new FUSE opcode:

1. Add the trait method to `CoreFilesystem`
2. Add the fuser handler in `FuserAdapter`
3. Add the winfsp handler in `WinFspAdapter`
4. Run `cargo test` + the relevant integration
   test (`tests/fuse_ops_test.rs` or
   `tests/winfsp_integration_test.rs`)
5. Update this table — mark ✓ + add commit
   reference

When closing an issue that adds FUSE coverage,
add the commit SHA to the row's Notes column.
