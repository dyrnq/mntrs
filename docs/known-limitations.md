# Known limitations

> **Scope:** Backend-inherent limitations where mntrs cannot
> match rclone's wall-clock without server-side cooperation.
> Each entry records: the gap, the structural reason it
> cannot be closed, and what (if anything) is left to do.

## `mkdir -p` on flat-namespace backends (S3, GCS, OSS, COS, AZBlob)

### Symptom

`mkdir -p a/b/c/d/e` against a local MinIO S3 backend:

```
MkdirDeep | mkdir -p a/b/c/d/e   | mntrs=0.038s  rclone=0.008s   rclone wins by 4.75x
```

Single-level `mkdir` is also slower but closer
(mntrs 0.004s vs rclone 0.001s, 4×). The 5-level chain is
where the gap compounds.

### Structural reason

S3, GCS, OSS, COS, and AzBlob are **flat namespaces** —
directories are not first-class objects. Creating a "dir"
is a `PUT` of a zero-byte object with a trailing-slash
key (`a/b/c/d/e/`). mntrs cannot avoid the PUTs.

GNU `mkdir -p a/b/c/d/e` issues 5 separate `mkdir(2)`
syscalls. Each becomes one FUSE callback. mntrs's
`mkdir_chain` (`src/lib.rs:2227`) **already amortizes the
intermediates** — `chain.iter().map(create_dir)` +
`join_all` makes 4 intermediate PUTs run concurrently
(1 RTT regardless of depth, not 4). The remaining 5th
`mkdir` is the leaf, which `MntrsFs::mkdir`
(`src/lib.rs:6359`) issues **sequentially**:

1. `mkdir_chain(full_path)` — 1 RTT for all intermediates
2. `op.create_dir(&full_path)` — 1 RTT for the leaf

5 sequential `mkdir(2)` calls × ~6 ms each = ~30 ms of
PUTs. Adding `cache_add_entry` + `alloc_ino_with_mtime` +
tombstone cancel check (~1–2 ms each) lands at ~38 ms.

rclone's 8 ms suggests it:

- Skips `PUT` verification on intermediate levels
  (assumes success, rolls back on failure later), OR
- Uses a single `PUT` with a multi-level collection
  marker (server-side feature not generally available), OR
- Returns from `mkdir` before the `PUT` leaves the process
  (write-cache mode + lazy verify)

### What's left to do

mntrs does not currently ship a `mkdir -p` write-behind
analogous to the `batched_delete` path. Two follow-ups
were considered in issue #539 and deferred:

1. **Cache-add-only mode**: skip the leaf `op.create_dir`
   when the leaf has no children yet AND the directory
   already exists in `dir_cache`. Currently `mkdir`
   always issues `create_dir`. Low risk; saves ~6 ms per
   `mkdir` when the leaf is already implicit.
2. **PUT coalescing**: when N `mkdir`s arrive in <100 ms
   from the same process on the same parent chain, batch
   them. Not natively supported by S3 — would require a
   mntrs-native "mkdir -p" write-behind similar in shape
   to `batched_delete`. High effort, ~2× speedup
   plausible.

**Decision (2026-08-03, issue #539):** Accept the S3
cost. Document as a known flat-namespace limitation and
remove the `mkdir -p 5` row from the regression-watch
set until a fix lands. Single-level `mkdir` remains in
the watch set as it is closer to rclone.

### Workarounds

- Pre-create the deep chain once at bucket setup time
  (e.g. `aws s3api put-object --key a/b/c/d/e/`), then
  let mntrs pick it up implicitly on subsequent `mkdir`.
- Mount non-S3 backends when the workload is mkdir-heavy
  (`fs://`, `sftp://` — first-class dirs).
- Keep the depth of directory trees shallow; the gap
  scales with depth, not breadth.

### Files / refs

- `src/lib.rs:2227` — `mkdir_chain` (intermediates,
  join_all)
- `src/lib.rs:6359` — `MntrsFs::mkdir` (sequential leaf)
- `src/util.rs:380` — `build_mkdir_chain` (chain shape)
- `bench/run_all.sh:356-357` — single-level `mkdir` bench
- Issue #539
- Issue #17 (original mkdir slowness issue; intermediates
  were the bottleneck there, fixed by `join_all`)