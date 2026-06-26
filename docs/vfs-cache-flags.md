# rclone `vfs-cache-*` flags in mntrs

> **Scope:** Catalog of the rclone-compatible `--vfs-*` flags
> that mntrs accepts on the CLI but **does not dispatch**.
> Each is parsed and stored, then ignored at runtime. This
> doc explains why each one is a no-op and points to the
> mntrs-native knob that does the equivalent job (if any).
>
> Supersedes the older "per cache mode" framing in
> [`durability.md`](durability.md). The rclone-compat
> `vfs_cache_mode` flag is **not** a mode selector — it
> is one of nine shadow fields that look like knobs but
> have no effect.

## Background: mntrs has 5 independent cache layers

rclone's `--vfs-cache-*` family controls a single VFS
layer — the only cache between the local view and the
remote. mntrs is a FUSE daemon with **five independent
caches**, each with its own TTL/policy knob:

| # | Layer | Knob |
|---|---|---|
| 1 | `attr_cache` (per-inode backend metadata) | `--attr-cache-ttl` |
| 2 | `dir_cache` (readdir snapshot) | `--dir-cache-ttl` + `--vfs-cache-poll-interval` |
| 3 | `disk_cache_index` + on-disk blocks (LRU) | `--cache-max-size` + `--cache-min-free-space` |
| 4 | `mem_cache` (in-memory blocks, bounded) | `--mem-limit` |
| 5 | `multi_cache` (combines mem + disk) | (composite; see [`mntrs-cache-knobs.md`](mntrs-cache-knobs.md)) |

A single `--vfs-cache-mode=off|full|minimal` switch is
meaningless in a 5-layer system — each layer needs its
own bypass. See the **`vfs-cache-mode` semantics** section
below for the composition recipe.

## Inventory of shadow flags (verified)

| CLI flag | MntrsFs field | Status |
|---|---|---|
| `--vfs-cache-mode` | `cache_mode: String` | SHADOW |
| `--vfs-cache-max-age` | `cache_max_age: Duration` | SHADOW |
| `--vfs-read-ahead` | `read_ahead: u64` | SHADOW |
| `--vfs-fast-fingerprint` | `fast_fingerprint: bool` | SHADOW |
| `--vfs-case-insensitive` | `case_insensitive: bool` | SHADOW |
| `--vfs-links` | `links: bool` | SHADOW |
| `--vfs-used-is-size` | `_vfs_used_is_size: bool` | UNUSED (`_` prefix) |
| `--vfs-metadata-extension` | `_vfs_metadata_extension: Option<String>` | UNUSED |
| `--vfs-no-modtime` | `_no_modtime: bool` | UNUSED |

The first six are rclone-shaped knobs we kept for
backward compat with rclone scripts. The last three
(underscore-prefixed) are placeholders for features that
were never built; the `_` prefix is the marker that says
"we know this is dead."

## Per-flag rationale

### `--vfs-cache-mode` (SHADOW)

rclone's `off | writes | full | minimal` toggles the
single VFS layer. mntrs has five independent caches
controlled by individual knobs — there is no single
"mode" selector. See the **`vfs-cache-mode` semantics**
section below for the four-knob composition that maps
to "no cache" intent.

### `--vfs-cache-max-age` (SHADOW)

rclone's flag governs the single file-level cache TTL.
mntrs's TTLs are per-layer (`--attr-cache-ttl` for
stat-cache, `--dir-cache-ttl` for readdir). There is no
single "max age" — set the per-layer TTLs.

### `--vfs-read-ahead` (SHADOW)

rclone sets kernel-level read-ahead via `O_DIRECT` /
`fadvise`. mntrs controls read-ahead through
`--vfs-prefetch-threshold` + `--vfs-prefetch-queue-mb`
(the prefetcher path, issue #201/#222) — a more
aggressive model that issues the next chunk in the
background while the kernel reads the current one.

### `--vfs-fast-fingerprint` (SHADOW)

rclone toggles a faster-but-less-secure hash for dedup
checks. mntrs's `--hash-filter K/N` knob (issue #205)
is the equivalent sharding primitive — the same
trade-off (correctness vs speed) lives there.

### `--vfs-case-insensitive` (SHADOW)

Not implemented. The platform filesystem governs
case-sensitivity (`mount_case_insensitive` is a FUSE
hint, but the backing storage's case semantics are the
real authority).

### `--vfs-links` (SHADOW)

Symlink support is governed by `--link-perms` (always
allowed unless restricted). The `vfs_links` flag has
no effect — passing it does not change symlink
behavior.

### `--vfs-used-is-size` (UNUSED, `_` prefix)

rclone uses `st_size` as the "used" stat. mntrs
reports `--vfs-disk-space-total-size` (configurable,
default off) in `statfs`. The flag was added before
`disk_space_total_size` existed and was never wired.

### `--vfs-metadata-extension` (UNUSED, `_` prefix)

rclone stores VFS metadata in `<name>{ext}` sidecars.
mntrs uses `.dirty` sidecars (writeback queue) plus the
in-memory `inodes` DashMap (stat cache). The metadata
extension concept does not apply.

### `--vfs-no-modtime` (UNUSED, `_` prefix)

The inverse of `--vfs-use-server-modtime`, which IS
wired (lib.rs:~1333, the inverse of the server-modtime
flag). This field is a leftover; the "no modtime"
behavior is already covered by `no_modtime` (the
non-prefixed alias at L184 of main.rs).

## `vfs-cache-mode` semantics (current best interpretation)

The user-facing question "what does `--vfs-cache-mode=off`
mean in mntrs?" has three reasonable interpretations,
documented in [#230](../../issues/230):

| Interpretation | Use case | Risk |
|---|---|---|
| 1. Read-through only (recommended) | Latency-sensitive S3 | Low |
| 2. No local write at all | Streaming workloads | High — recovery path breaks |
| 3. No disk, but keep mem | tmpfs-style | Medium |

**Interpretation 1 (read-through only)** is the current
best interpretation. To compose "no cache" semantics from
existing mntrs knobs:

```bash
mntrs mount s3://bucket /mnt \
    --attr-cache-ttl 0 \          # bypass attr_cache
    --dir-cache-ttl 0 \          # bypass dir_cache
    --cache-max-size 0 \         # bypass disk_cache_index
    --mem-limit <existing> \     # mem_cache is bounded; keep as-is
    --writeback-immediate \      # every write uploads on close
```

That's four existing knobs that compose into the
"minimal caching" semantic — no new flag is needed.
The `--vfs-cache-mode=off` flag becomes a deprecation
alias pointing to this four-knob combination.

This is the **current recommended interpretation**;
the user has not yet signed off on which interpretation
is canonical. See [#230](../../issues/230) for the
decision matrix and open questions.

## Operational impact

Passing any of the shadow flags on the CLI is silently
ignored. mntrs surfaces this with a single
`tracing::warn!` line at mount time, listing the
shadow flags the user explicitly set:

```
mount_internal: --vfs-cache-mode is a no-op in mntrs (see docs/vfs-cache-flags.md);
    also: --vfs-fast-fingerprint, --vfs-case-insensitive, ...
```

The warning is consolidated (one line per mount, not
nine), so the log noise stays bounded.

## How to verify a shadow flag is being ignored

Pass any shadow flag and check the log:

```bash
RUST_LOG=warn mntrs mount s3://bucket /mnt --vfs-cache-mode=off --vfs-fast-fingerprint
# WARN mount_internal: --vfs-cache-mode is a no-op in mntrs (...);
#      also: --vfs-fast-fingerprint (...)
```

The lack of dispatch is also verifiable in code:
`grep -n "cache_mode\b" src/lib.rs` returns zero hits
inside the read/write/list paths — the field is
constructed and never read.

## Adding a real implementation

If the user needs an actual knob that maps to a
shadow-flag semantic, the path is:

1. Open a new issue describing the use case (link to
   the relevant shadow flag).
2. Implement the dispatch (the field becomes
   self-documenting; clippy enforces the use).
3. Remove the shadow entry from this doc and the
   consolidated-warn list.

The 9 fields stay in `MntrsFs` until step 3 — dropping
the field breaks the CLI surface (unknown flag).

## Related

- [#228](../../issues/228) — Sprint 8 design reframe (tracker)
- [#229](../../issues/229) — Sprint 8.1 (this doc + help text + warn)
- [#230](../../issues/230) — Sprint 8.2 (`vfs-cache-mode` interpretation)
- [#231](../../issues/231) — Sprint 8.3 (mntrs-specific knobs — see
  [`mntrs-cache-knobs.md`](mntrs-cache-knobs.md))
- [#142](../../issues/142) — parent shadow-field gap (merged)
- [`durability.md`](durability.md) — actual writeback/durability model
- [[`feedback-rclone-params-keep-and-document`](../) — keep-and-document rule