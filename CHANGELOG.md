# Changelog

All notable changes to mntrs are documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/),
and this project does **not** yet follow Semantic Versioning. Dates
use the YYYY-MM-DD form.

## Unreleased

### Breaking changes

- **`--vfs-cache-mode` is now a real per-mode selector.** The flag
  was previously accepted but stored as a `String` with no
  behavioral effect (a "shadow field"). It is now backed by a
  typed `crate::util::CacheMode` enum and dispatched at the
  relevant sites (`open`, `create`, `create_excl`, `write`,
  `read`, `fsync`, `flush`, `release`).

  | Mode       | Write buffer          | Read cache          | Crash safety                |
  |------------|-----------------------|---------------------|------------------------------|
  | `off`      | in-memory `Vec<u8>`   | L1 mem_cache only   | dirty bytes lost on crash    |
  | `writes`   | disk file + fdatasync | L1 mem_cache only   | dirty bytes durable          |
  | `full`     | disk file + fdatasync | L1 + L2 `.block`    | dirty bytes durable          |
  | `minimal`  | disk file + fdatasync, unlinked after upload | L1 mem_cache only | dirty bytes durable across the upload window, no on-disk footprint between writes |

  **Default changed from `writes` to `off`.** Users who relied on
  the implicit crash safety of the previous default must now pass
  `--vfs-cache-mode=writes` (or `--vfs-cache-mode=full`)
  explicitly. CSI deployments in particular should set this in
  the mount options — pod reschedule is a "soft crash" and loses
  un-uploaded bytes only in `off` mode.

  See `docs/durability.md` for the per-mode guarantees and the
  `src/util.rs::CacheMode` doc comment for the dispatch
  predicates. Implementation: issue #583.

- **`MntrsFs::cache_mode` field type changed from `String` to
  `crate::util::CacheMode`.** This is a struct-internal type
  change; it does not affect the CLI surface (the flag still
  accepts the same four string values) and does not affect the
  on-disk format (the cache directory layout is unchanged).
  Any external code that reached into `MntrsFs::cache_mode`
  directly will fail to compile and need to switch to pattern
  matching on the enum (or use `CacheMode::parse` to convert
  from a string).

### Additions

- **`--vfs-cache-mode=minimal` is now a distinct mode.** Previously
  it parsed to `CacheMode::Off` (silent alias). It is now backed
  by a new `CacheMode::Minimal` variant with the following
  semantics:

  - Write buffer: on-disk cache file with `fdatasync` (same as
    `writes` / `full`).
  - Read cache: L1 mem_cache only — the on-disk file is
    `remove_file`'d after a successful upload, so it does not
    survive between writes.
  - Crash safety: dirty bytes are `fdatasync`'d during the upload
    window and re-enqueued by the recovery scan if the daemon
    crashes; once an upload completes, the file is removed and
    there is no on-disk footprint.

  This is the mode to use when you want disk-backed write
  durability (your bytes survive a daemon crash or a power
  loss) without retaining a permanent local cache file. The
  `WritebackTask` struct gained a `delete_cache_on_success: bool`
  field to thread the unlink decision from the enqueue site
  (`src/lib.rs`) into the worker (`src/writeback.rs`).

  Implementation: issue #T2-N. The 13 `== CacheMode::Off` dispatch
  sites in `src/lib.rs` were also refactored to use the
  `disk_write_buffer()` / `disk_read_cache()` /
  `delete_cache_on_success()` predicates so a future cache-mode
  variant cannot silently mis-route. No CLI change — `minimal`
  already parsed before #T2-N; only its behavior changed.

- **`--vfs-read-ahead` is now wired (issue #588).** The flag was
  previously accepted but stored and discarded (a "shadow
  flag"); it is now consumed by `MntrsFs::maybe_create_prefetcher`
  under `cache-mode=full` only.

  Effect: with `cache-mode=full`, the prefetcher's queue cap
  becomes `prefetch_queue_mb + read_ahead` MiB. Off / Writes /
  Minimal modes silently ignore the value (no L2 block cache to
  amortize against, so an extra queue just wastes memory). The
  formula is extracted into `compute_prefetch_queue_bytes` and
  pinned by a unit test covering all four cache modes.

  Default 0 (matches rclone). No CLI change. The SHADOW warning
  in the mount log for `--vfs-read-ahead != 0` is gone.

- **`--vfs-write-wait` is now wired (issue #T2-N+1).** The flag
  was previously accepted but stored and discarded (a "shadow
  flag"); it is now consumed by `per_task_writeback_delay` for
  large files (above `--writeback-immediate-threshold`).

  Effect: a writeback task for a large file is enqueued with
  `per_task_delay = write_wait - elapsed_since_last_write`
  instead of the uniform `--write-back` delay. The intent is
  coalescing — a follow-up write+close inside the window
  lands in a single upload rather than triggering a wasted
  upload of a still-warming file. Small files (below the
  threshold) are unaffected — they still upload immediately.
  The delay is capped at `--write-back` so the periodic queue
  (which fires every `--write-back` seconds) will still pick
  up the task even if the `write_wait` window is longer than
  the batch period.

  Per-handle `FileHandleState::Write` gained a
  `last_write_at: Option<Instant>` field; the `Clone` impl
  forwards it. The function signature is now
  `per_task_writeback_delay(&self, ino: u64, fh: u64)`.
  When the handle is gone (recovery scan, flush-without-fh,
  etc.) `per_task_writeback_delay` falls back to
  `--write-back` — no coalescing info available.

  Default 1 s (matches rclone). No CLI change. The SHADOW
  warning in the mount log for `--vfs-write-wait != 1` is
  gone.

- **`--vfs-refresh` periodic mode is now wired (issue #592).**
  The flag was previously accepted only as a boolean one-shot
  toggle (issue #210, "skip attr_cache on every stat"). A new
  `--vfs-refresh <secs>` form takes a duration and spawns a
  background tokio task that clears `dir_cache` and
  `attr_cache` every N seconds so the next readdir / stat
  refetches from the remote.

  Effect: with `--vfs-refresh 60`, the readdir / stat caches
  are dropped on a fixed 60-second schedule. `inodes` is left
  alone (the FUSE kernel holds `ino` references that would
  dangle if we removed the entries); `disk_cache_index` is
  left alone (individual file contents are still valid until
  `--vfs-cache-max-age` expires them).

  Default `0` (disabled) — deliberately more conservative
  than rclone's `5m` default. mntrs's existing
  `--dir-cache-ttl` and `--attr-cache-ttl` already provide
  per-cache-class freshness on the lazy read path; the eager
  periodic clear is opt-in for users who need tighter
  visibility into out-of-band remote changes.

  `MntrsFs::refresh_interval: Duration` (default
  `Duration::ZERO`) was added; `spawn_refresh_worker` is
  invoked from `common_init_wb` after the writeback and
  concurrent_delete workers. No CLI change beyond the new
  `--vfs-refresh <secs>` form.

### Migration

- Add `--vfs-cache-mode=writes` (or `full`) to existing mount
  invocations if you depended on the pre-#583 default.
- No code changes required for CLI users.
- Library users pinning the old field type need to switch to the
  enum.

## Past releases

Prior changes were tracked in git history (commit messages) and
in GitHub releases. This file starts tracking at the change
introduced by issue #583.