# readdir streaming refactor (issue #23)

## Problem

`MntrsFs::readdir` (in `src/core_fs/fuser.rs`) returns the full
directory list (capped at `MAX_LIST_ENTRIES = 1M`) from
`list_op`/`dir_cache`. The FUSE adapter paginates this list
to the kernel using array-index cookies `(i + 1) as u64`.

When the kernel's reply buffer is full (`reply.add` returns
`true` mid-page), it re-issues `readdir(offset=N)` where `N`
is the index of the last entry we delivered. The adapter then
slices the materialized list at `start = N`.

If a concurrent mutation (create/unlink) happens between the
two readdir calls, the materialized list at `dir_cache` could
be different — but the `dir_cache` TTL is 300s by default, so
the second page reads the same cached Vec. That means the bug
is mostly latent in the current code: the materialized list
is stable for the duration of pagination.

The real risk surfaces if `dir_cache` is invalidated between
pages (e.g. by a `write` that triggers `cache_remove`), in
which case the second `readdir` re-materializes and may
return a different list at the same `start` offset — leading
to **skipped or duplicate entries** to user-space.

## Current behavior (Bug 32, fixed in ece4391)

`entries.iter().enumerate().skip(start)` was replaced with
`entries[start..].iter().enumerate()`. This is an O(1) per page
fix for the inner loop (was O(offset) due to skip's per-step
iterator advance). It does **not** fix the cookie-stability
problem.

## Proposed fix (issue #23)

Replace the materialized `Vec` with a per-handle streaming
lister, so the cookie is a stable cursor into a single
ongoing iteration.

### Trait change

Replace

```rust
fn readdir(&self, ino: u64) -> std::io::Result<Vec<CoreDirEntry>>;
```

with

```rust
/// Open a streaming readdir handle.
/// Returns a CoreFileHandle whose Read variant holds the
/// backend lister mid-iteration.
fn opendir(&self, ino: u64) -> std::io::Result<u64>;
/// Pull the next page (up to N entries) from a readdir
/// handle. Returns Ok(empty) at EOF.
fn readdir(
    &self, ino: u64, fh: u64, offset: u64, max: usize,
) -> std::io::Result<Vec<CoreDirEntry>>;
/// Close the readdir handle, releasing the lister.
fn releasedir(&self, ino: u64, fh: u64) -> std::io::Result<()>;
```

`offset` becomes either:
- A no-op (lister state is held in `FileHandleState`)
- A sanity check / debug aid

### FileHandleState change

```rust
enum FileHandleState {
    Read { ... },
    Write { ... },
    DirList { lister: opendal::Lister, dir_path: String },
}
```

The lister's lifetime is tied to the `fh`. The FUSE adapter
calls `opendir` on first readdir (offset=0), stashes the
`fh` in the kernel's `FileHandle`, and `releasedir` on
`release(ino, fh)`.

### dir_cache change

`dir_cache` currently stores `Vec<(String, EntryMeta)>`.
Change to:
- Keep materialized list for `lookup`/`getattr` speed
  (those need the full set, no pagination)
- Add a separate lister cache keyed by `ino` that holds
  an `opendal::Lister` mid-iteration; entry TTL is bounded
  by `releasedir`

OR drop the materialized list and have `lookup` re-scan
`Lister` (slower per-entry but constant memory).

### Cross-backend continuation

opendal `Lister::next()` returns `Option<Entry>` where each
`Entry` has a `Metadata::content_length`, `mode`, etc. The
native lister is the right primitive — it handles S3
NextContinuationToken, HDFS startAfter, fs::ReadDir cursor
internally. No per-backend continuation logic needed in
mntrs.

The one cross-backend subtlety: opendal's `Lister` is
**not `Send`** (holds a stream), so the `DirList` variant
of `FileHandleState` must only be touched on the FUSE
worker thread. `fuser` runs each request on the worker
thread that started the session, so this is fine for the
fuser backend. For winfsp (Windows), the adapter may
need to spawn a per-fh task; left as TODO.

## Estimated effort

- Trait change: 1 file (`core_fs/mod.rs`)
- `MntrsFs` impl: 1 file (`src/lib.rs` readdir area)
- fuser adapter: 1 file (`src/core_fs/fuser.rs`)
- winfsp adapter: 1 file (`src/core_fs/winfsp.rs`) — add
  Releasedir mapping
- dir_cache value type: 1 file (`src/lib.rs`)
- Test for pagination stability under mutation: 1 file
  (extend `tests/bug_regression_test.rs`)

Roughly 250-400 LoC. Half-day to one-day focused work.

## Status

- Documented limitation in code: ece4391 (Bug 32)
- This design doc: pending review
- Implementation: not started

## Related

- issue #23: original report
- commit ece4391: slice-indexing micro-fix; same comment
  block above documents the true-streaming gap
- issue #31: cat 1K.bin 23x slower (separate concern, but
  parallel-chunk prefetch in the read path would compose
  well with the per-fh lister state infrastructure)
