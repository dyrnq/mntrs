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

### Fixes

- **`--max-read-ahead` is now wired (was silently shadowed).**
  Pre-fix, `FuserAdapter::init()` hardcoded
  `config.set_max_readahead(1024 * 1024)` (1 MiB) regardless of
  the CLI value, and `cmd/mount.rs` accepted `--max-read-ahead`
  into a `_max_read_ahead: u64` underscore-prefixed param — dead
  at compile time. The flag parsed without error and silently had
  no effect. Post-fix:

  - `FuserAdapter` gains a `pub max_read_ahead: u32` field.
  - `cmd/mount.rs` declares `max_read_ahead: u64` (no leading
    underscore) and forwards the value through `FuserAdapter::new`.
  - `init()` calls `config.set_max_readahead(self.max_read_ahead)`
    instead of the hardcoded literal.

  Default remains 131072 (128 KiB, matches rclone). Behavioural
  change is observable only for users who set the flag explicitly
  (pre-fix their value was discarded silently). Pin test:
  `tests/max_read_ahead_test.rs` (3 cases: user override, rclone
  default, zero passthrough). CLI drift guard:
  `tests/cli_defaults_test.rs` now includes `--max-read-ahead`.

- **Cleaned stale `--vfs-cache-mode` "no effect" docstring and
  SHADOW-warn entry.** Three pieces of stale code were lingering
  after the flag was wired in #583 / #T2-N:

  - `src/main.rs` L150-156 docstring still read "**No effect in
    mntrs** — this is a deprecation alias for the four-knob
    composition `--attr-cache-ttl 0 --dir-cache-ttl 0
    --cache-max-size 0 --writeback-immediate`". Rewritten to
    describe the actual `CacheMode` dispatch (off / writes / full
    / minimal) and point at `docs/durability.md#cache-mode-summary`.
  - `src/main.rs` SHADOW-warn entry
    (`if vfs_cache_mode != "off" { shadow.push("--vfs-cache-mode"); }`)
    fired the spurious warn on every mount using a non-default
    mode (`writes`, `full`, `minimal`). Deleted.
  - `docs/vfs-cache-flags.md` top + L76 still classified
    `--vfs-cache-mode` as SHADOW; updated to WIRED and pointed at
    the four-mode semantics doc.
  - `README.md` listed `--vfs-cache-mode` and `--vfs-refresh` in
    the "not yet implemented" group; both are now wired, removed
    from that group.

- **Fixed octal-vs-decimal bug in `--link-perms` SHADOW-warn
  comparison.** The flag's clap default is `default_value = "777"`
  (parsed as decimal u32 = 777), but the warn check was
  `link_perms != 0o777` (octal 511). The two values were never
  equal, so the warn fired on EVERY mount using the default —
  i.e. every mount that did not explicitly pass `--link-perms`.
  Changed the comparison to `!= 777` (decimal) to match the
  actual default. `--link-perms=755` (real shadow) still fires
  the warn correctly.

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

  Fixed in the same change: the `write()` integration path was
  enqueuing the writeback BEFORE updating the inodes entry's
  size, so `per_task_writeback_delay` always saw `size == 0`
  and always routed to the small-file `Duration::ZERO`
  immediate-upload branch — making the entire
  `--vfs-write-wait` flag dead code on `write()`. The inode
  size update now runs before the enqueue. Pinned by
  `tests/write_wait_microbench.rs`.

- **`--vfs-write-wait` microbench (issue #T2-N+1).**
  `tests/write_wait_microbench.rs` pins the three behavioral
  claims of the coalescing window via
  `mntrs::writeback::pending_count()` timing assertions:

  - `write_wait=0` → upload drains inside 200 ms.
  - `write_wait=2s` (capped at `--write-back`) → upload is
    still pending at 200 ms, drains at ~1 s.
  - Two back-to-back `write()+release()` cycles inside the
    coalescing window produce ONE task, not two (the
    `writeback_pending.insert()` skip-if-already-present check
    holds).

  Run with
  `cargo test --release --test write_wait_microbench -- --test-threads=1`.

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

- **`--vfs-buffer-size <bytes>` wired into opendal upload
  chunk (issue #595).** Default `16777216` (16 MiB),
  matching rclone. The value is forwarded to opendal's
  `OpWriter::chunk` on every writeback / upload path
  (multipart for files >200 MiB, one-shot otherwise) plus
  the 2 FUSE-thread xattr full-object rewrites at
  `setxattr` / `removexattr`. 0 keeps opendal's default
  (8 MiB) — pre-#595 behavior. Read / stat / list /
  mkdir / delete are unaffected. See
  `docs/vfs-cache-flags.md` for service-floor caveats
  (S3 multipart enforces a 5 MiB minimum part size).

- **Fixed `--vfs-refresh` (issue #592 / PR #593) silently doing
  nothing at runtime.** The refresh worker's `dir_cache.clear()`
  and `attr_cache.clear()` were operating on a *deep-copy* clone
  of the maps: `DashMap::clone` clones every entry into a new
  map, so the spawned task was clearing its own private copy and
  the main maps — what FUSE callbacks see — survived forever.
  Both fields are now wrapped in `Arc<DashMap<...>>`; the worker's
  `clone()` is now a refcount bump that shares backing storage,
  and `clear()` actually drains the live maps. A new integration
  test (`tests/refresh_interval_test.rs`) pins the three
  behavioral claims: zero-interval fast-paths the worker out,
  nonzero interval drains both caches within two ticks, and
  `inodes` is left untouched.

- **Pinned `--vfs-buffer-size` chunk_size floor (issue #595).**
  The multipart branch's `if buffer_size >= 5 MiB { buffer_size } else { 5 MiB }`
  logic is now extracted into a pure `pub const fn multipart_chunk_size(u64) -> usize`
  helper in `src/writeback.rs`, alongside a `pub const S3_MIN_MULTIPART_PART: u64 = 5 MiB`.
  Three new unit tests in `writeback::tests` pin the boundary semantics:
  below-floor falls back to 5 MiB, at-floor `buffer_size` wins (>=, not >),
  above-floor `buffer_size` wins. A fourth test pins the one-shot path's
  *non*-floor semantics: `buffer_size=0` is a valid value that delegates to
  opendal's 8 MiB default — a future refactor that copy-pastes the multipart
  floor here would silently bump the one-shot default above 8 MiB, and this
  test catches that.

- **Fixed `--vfs-refresh` write-path notifier never reached the
  kernel (audit alongside issue #89 / #93).** The `write()`
  handler called `self.fuse_notifier.get()` to fetch the FUSE
  kernel notifier, but the `fuse_notifier` field is initialized
  to `OnceLock::new()` and `set_fuse_notifier()` (called from the
  mount command path) writes to a process-static `FUSE_NOTIFIER`
  cell instead. The check was therefore always `None` and the
  notifier side-effect was dead code. The `O_APPEND` write-offset
  bug described in #89 (kernel uses stale cached size) would
  therefore have resurfaced as soon as the test suite ran in a
  setup that populated the notifier. The fix reads from
  `FUSE_NOTIFIER.get()` (same place the setter writes). Pinned
  by `tests/o_append_fuse_notifier_test.rs` via a process-static
  `INVAL_INODE_COUNT` counter exposed through
  `mntrs::__inval_inode_count_for_test()`.

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