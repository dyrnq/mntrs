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
  | `minimal`  | maps to `off`         | L1 mem_cache only   | dirty bytes lost on crash    |

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