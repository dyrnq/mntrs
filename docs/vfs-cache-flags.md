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
| `--vfs-cache-max-age` | `cache_max_age: Duration` | WIRED (issue #507) |
| `--vfs-read-ahead` | `read_ahead: u64` | SHADOW |
| `--vfs-fast-fingerprint` | `fast_fingerprint: bool` | SHADOW |
| `--vfs-case-insensitive` | `case_insensitive: bool` | SHADOW |
| `--vfs-links` | `links: bool` | SHADOW |
| `--vfs-used-is-size` | `_vfs_used_is_size: bool` | UNUSED (`_` prefix) |
| `--vfs-metadata-extension` | `_vfs_metadata_extension: Option<String>` | UNUSED |
| `--vfs-no-modtime` | `_no_modtime: bool` | UNUSED |

The first five are rclone-shaped knobs we kept for
backward compat with rclone scripts. The last three
(underscore-prefixed) are placeholders for features that
were never built; the `_` prefix is the marker that says
"we know this is dead." (`--vfs-cache-max-age` was
previously in this list but was wired in issue #507.)

The "SHADOW" rows below are currently the 5
rclone-shaped knobs that remain accepted-but-unused.
The "UNUSED" rows are placeholders the compiler
guarantees never reach a read site (`_` prefix).

## Per-flag rationale

### `--vfs-cache-mode` (SHADOW)

rclone's `off | writes | full | minimal` toggles the
single VFS layer. mntrs has five independent caches
controlled by individual knobs — there is no single
"mode" selector. See the **`vfs-cache-mode` semantics**
section below for the four-knob composition that maps
to "no cache" intent.

### `--vfs-cache-max-age` (WIRED, issue #507)

rclone's flag governs the single file-level cache TTL —
absolute age from cache write time ("objects older than
this"), not idle TTL. The implementation matches:

- **L2 `.block` cache**: `DiskBlockCache::get_block`
  checks filesystem mtime on every L2 hit. Expired
  entries return `None` and the on-disk `.block` file is
  removed so the next read refetches from remote. The
  `MultiLevelCache` caller sees the expired entry as a
  plain L2 miss (metric counts it as `l2_miss`).
- **Whole-file cache**: `MntrsFs::read` step 4 checks
  `cache_path` mtime before serving. Expired whole-file
  cache files are dropped and the read falls through to
  block cache + remote fetch.
- **Background sweep**: `evict_if_needed` (was
  `evict_lru_if_needed`) runs an age sweep on the write
  path that removes age-expired entries even when no
  capacity pressure exists. Required for users who set
  only `--vfs-cache-max-age` (no `--cache-max-size`).
- **`.dirty` sidecar guard**: whole-file cache files
  with a sibling `{cache}.dirty` are **never** swept by
  the age check — `writeback` is the only legitimate
  cleaner (`src/writeback.rs:299, 434`).
- **`0` disables** the check (matches the CLI help
  `"0 to disable"`). The helper
  `util::is_cache_file_expired` short-circuits without
  a syscall so the hot path stays free when TTL is off.
- **L1 (`mem_cache`) is NOT age-evicted**. The
  `MemCache` trait has no age API and changing it would
  touch DashMap / Moka / Foyer impls. L1 remains under
  `--mem-limit` pressure eviction.

The implementation uses filesystem mtime, not the
in-memory `disk_cache_index` `Instant` (which is atime
for LRU). Our writers never `set_len`-bump mtime on read,
so on-disk mtime is the genuine cache write time — the
same anchor rclone uses.

mntrs's per-layer TTLs (`--attr-cache-ttl`,
`--dir-cache-ttl`) are still the right knobs for those
specific caches; `--vfs-cache-max-age` covers the data
cache (whole-file + block).

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
default 0 = off) in `statfs`. When off, statfs reports
a fallback of **256 M 4-KiB blocks = 1 TiB** total (see
issue #243.4 for the unit note). The CSI plugin
consumes this value via `node_get_volume_stats` —
do not change the fallback without re-running
csi-integration. The flag was added before
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

## `vfs-cache-mode` semantics (canonical: Interpretation 1)

The user-facing question "what does `--vfs-cache-mode=off`
mean in mntrs?" has three reasonable interpretations,
documented in [#230](../../issues/230):

| Interpretation | Use case | Risk |
|---|---|---|
| 1. Read-through only (**canonical**) | Latency-sensitive S3 | Low |
| 2. No local write at all | Streaming workloads | 🔴 High — recovery path breaks |
| 3. No disk, but keep mem | tmpfs-style | Medium |

**Interpretation 1 (read-through only)** is the
**user-confirmed canonical semantic** (signed off
2026-06-26). It composes "no cache" from existing
mntrs knobs:

```bash
mntrs mount s3://bucket /mnt \
    --attr-cache-ttl 0 \          # bypass attr_cache
    --dir-cache-ttl 0 \          # bypass dir_cache
    --cache-max-size 0 \         # bypass disk_cache_index
    --mem-limit <existing> \     # mem_cache is bounded; keep as-is
    --writeback-immediate \      # every write uploads on close
```

That's four existing knobs that compose into the
"minimal caching" semantic — **no new code, no new
flag**. The `--vfs-cache-mode=off` flag is a
**deprecation alias** that points users to this
four-knob combination (Q4 = option A).

### Why not Interpretation 2

Interpretation 2 (no local write at all) was rejected
on silent-data-loss risk: the `.dirty` sidecar
recovery path in `mount_internal` would still find
files left over from a previous mount under a
different mode, and silently skip them. The recovery
loop at `src/cmd/mount.rs:76` only runs on mount
startup; if `cache_mode=2` is set, the loop is
correctly bypassed, but the leftover sidecars from
mode=1 mounts persist on disk and never upload. This
is the kind of silent-failure mode
[[`feedback-re-evaluate-risk-vs-issue`](../../)]
explicitly warns against.

### Why not Interpretation 3

Interpretation 3 (no disk, keep mem) is workload-
dependent and offers no clear win — `mem_cache` is
already bounded by `--mem-limit`, and large working
sets churn the L1 (evict on pressure, refill from
backend) without the multi-tier knob to hint "hot"
vs "cold" (candidate #2 in [#231](../../issues/231),
DEFER status).

### Q3 — per-mount vs CLI flag

The CLI flag stays as the user-facing entry point.
Per-mount / per-volume overrides (e.g. CSI mode
where one tenant wants no-cache and another wants
full-cache on the same backend) are achieved by
the CSI driver setting the four underlying knobs
directly in `mount_internal` — no new mechanism
needed.

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

## `--write-back-cache` (opt-in FUSE kernel writeback)

`--write-back-cache` is **NOT** one of the 9 rclone-compat shadow
fields above — it is a real, wired flag (`src/main.rs:57-59`,
`src/cmd/mount.rs:660`, `src/core_fs/fuser.rs:init()`). Its
default is **`false`** (opt-in) since 2026-06-30.

When `false`: the daemon's `write()` handler runs per writeback
segment, the kernel doesn't buffer, the `.dirty` sidecar shows
up on close, and tests 01-06 (which assume this mode) pass
cleanly.

When `true`: the kernel buffers multi-page writes in its page
cache and only delivers setattr(size) on close. The daemon's
`write()` is never called for the body. This restores rclone's
default rcd's small-write optimization but interferes with
mntrs's per-message observability — hence the opt-in.

### Why the default is off

The flag was added (PR #79/80/81, commit `a53571c`, 2026-06-18)
and **unconditional** in init. Over the following 12 days the
trade proved net-negative for mntrs:

- **3 cache-poisoning bugs** — `#331` (mem_cache read at
  lib.rs:3050), `#334` (prefetch path at lib.rs:2966-2975),
  `#337` (remote-fetch L1 populate at lib.rs:3333). All three
  were races where the kernel cache hid real write timing and
  the daemon's `write()` (which guarded the cache write) was
  never called. Fixed in PRs #333 and #339.
- **Bench regression** — PR #339 noted 26/69 vs 43/69 wins;
  some workloads became slower.
- **stress 01 + 05 architecturally fail** under WRITEBACK_CACHE:
  - `01-large-dir` — daemon sees 80-90% of file creates
    because the kernel buffers create/setattr pairs
  - `05-crash-recovery` — 0 cache files for 4 MiB / 2 MiB
    multi-page bodies because daemon's `write()` never fires

The PR gates `InitFlags::FUSE_WRITEBACK_CACHE` behind the flag.
CSI drivers (Pod multi-tenancy) keep the default `false` —
they want per-FS-message observability.

### Lock-in test

`tests/stress/07-writeback-cache-optin.sh` mounts with
`--write-back-cache` and verifies the contract:

1. Multi-page writes do NOT call daemon's write() until close.
2. After close + daemon SIGKILL, the data is still served
   correctly.
3. A new daemon mounts over the same cache dir and sees the
   cache file with non-empty content.

This locks in the opt-in behavior so a future regression that
flips the default back to `true` will fail CI loudly.

### Re-enabling the flag

If you want rclone-style small-write behavior:

```bash
mntrs mount s3://bucket /mnt --write-back-cache
```

The kernel-side mount option `writeback_cache` at `mount.rs:1227`
is gated on the same flag, so the FUSE INIT and libfuse mount
option stay in sync.

## `--storage-class` (wired — S3 backend only)

`--storage-class` is **NOT** a shadow flag. It was previously in
the consolidated-warn list but has since been wired through to
opendal's `default_storage_class` setter on the S3 backend
(`src/cmd/mount.rs:build_s3`, `opendal-service-s3` 0.58
`Backend::default_storage_class`). The setter writes the value
into `config.default_storage_class`; the backend then sends
the `x-amz-storage-class` header on PUT / Copy / Multipart.

Valid values (clap `value_parser` enforces at startup):

- `STANDARD` — default for most buckets
- `STANDARD_IA` — infrequent access, ms retrieval, 30-day min
- `ONEZONE_IA` — single-AZ IA, cheaper than STANDARD_IA
- `INTELLIGENT_TIERING` — AWS auto-tiers by access pattern
- `GLACIER_IR` — archive with ms retrieval, 90-day min
- `GLACIER` — archive, minute-to-hour retrieval, 90-day min
- `DEEP_ARCHIVE` — cheapest, 12+ hour retrieval, 180-day min
- `OUTPOSTS` — AWS Outposts buckets only
- `REDUCED_REDUNDANCY` — legacy; AWS no longer supports new buckets

Example:

```bash
mntrs mount s3://my-bucket /mnt \
    --storage-class=GLACIER_IR
```

Limitations:

- **S3 backend only.** OSS / COS / OBS / Azblob / GCS backends
  silently ignore the value (their respective headers differ;
  opendal exposes no equivalent setter for those). For those
  backends, no mntrs flag exists today.
- **Mount-time only.** The value is set on the opendal builder
  at startup and applies to all uploads in that mount. To
  change per-object, use a backend lifecycle policy.
- **Objects uploaded before mount started are not affected.**
  Re-uploading the same key overwrites with the new class;
  deleting the old object is up to the user.
- **Min storage duration charges** apply for IA / GLACIER
  classes — deleting or overwriting to a cheaper class before
  the minimum duration incurs a prorated charge from AWS.

### Why this was a shadow and isn't anymore

The flag was originally accepted for rclone compat but never
wired (issue #455 audit identified it). The fix added:

1. `src/main.rs` — clap `value_parser` enum to fail bad values
   at startup (vs waiting for AWS 400 `InvalidStorageClass`).
2. `src/main.rs` — inject the value into the `opts` HashMap
   next to other backend-config keys (`endpoint`, `region`,
   etc.).
3. `src/cmd/mount.rs::build_s3` — call
   `builder.default_storage_class(v)` on the opendal S3 builder.

The flag was removed from the consolidated-warn list (which
still fires for the remaining `--vfs-*` shadow flags).

### Related

- #81 — original PR for unconditional WRITEBACK_CACHE
- #331, #334, #337 — three cache-poisoning bugs
- #339 — fix PR for #337 (also covers #334)
- #354 — obsolete (closed in the fix PR); the "04 missing
  files" report was a misframing under WRITEBACK_CACHE.
- [`fuse-writeback-opt-out.md`](../) — memory file documenting
  the rationale.
- [`durability.md`](durability.md) — see "Three writeback
  concepts" above for the other two writeback concepts.
