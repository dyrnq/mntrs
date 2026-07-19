# OpenDAL upstream tracking

> **Scope:** Apache OpenDAL upstream issues that affect mntrs
> and any workarounds we currently carry (typically via a
> `[patch.crates-io]` block in `Cargo.toml`). Supersedes the
> GitHub issue tracker reference — when an opendal release is
> cut, scan this document and bump the "resolved in" column.

## Current pinned versions

| Crate | Version | Source | Reason |
|-------|---------|--------|--------|
| `opendal` | `0.57` | crates.io | `0.58.0` yanked 2026-07-17 for macOS dyld bug ([apache/opendal#7923](https://github.com/apache/opendal/issues/7923), fix in [#7925](https://github.com/apache/opendal/pull/7925), not yet released). Stay on last-safe `0.57.0`. |
| `hdfs-native` | `0.13.5` + fork patch | `dyrnq/hdfs-native` git rev `419cd0e` | See [hdfs-native fork workaround](#hdfs-native-fork-dfs-client-use-datanode-hostname) below. Drop the patch when opendal releases with [PR #7910](https://github.com/apache/opendal/pull/7910). |

---

## 🔴 High priority — affects write durability or rename semantics

### opendal#7400 — WriteGenerator buffered data loss on write failure

- **Tracking:** <https://github.com/apache/opendal/pull/7400>
- **Status:** PR open, not merged.
- **Impact on mntrs:** mntrs calls `op.write()` (and `op.write_with()` for streaming) for the cold-cache write path. The current opendal WriteGenerator drops buffered bytes on remote-write failure (network timeout, 5xx). mntrs cannot recover those bytes — they were never sent to the backend. The local `.dirty` sidecar lingers until the next mount restart.
- **Workaround (current):** `mntrs list-dirty <cache-dir>` (PR #401) surfaces the orphan sidecar; `writeback_recovery` reattempts on next mount. We accept silent-loss-of-buffered-bytes on remote failure as a documented opendal limitation.
- **Action:** When #7400 merges and ships in an opendal release we can bump to, drop the `list-dirty` workaround (no longer needed because the buffered bytes will retry).

### opendal#7607 — S3 conditional `DeleteObject` (`If-Match`)

- **Tracking:** <https://github.com/apache/opendal/pull/7607>
- **Status:** PR open, not merged.
- **Impact on mntrs:** Tied to issue [#145](../issues/145) (rename/unlink S3 semantics). Today mntrs sends unconditional `DeleteObject` on `unlink`; if the object was modified between `lookup` and `unlink` (or by an external writer on a shared bucket), we silently delete the newer version. `If-Match` would give us CAS semantics — safe-delete against the version we stat'd.
- **Workaround (current):** None. Document the race in the user-facing `mount` help; advise against concurrent writers on shared buckets.
- **Action:** When #7607 merges and ships, wire `op.delete_with(...).with_version(...)` (or whatever the upstream API lands) into the FUSE `unlink` handler at `src/lib.rs:5154`.

---

## 🟡 Medium priority — affects specific backends or path handling

### opendal#7801 — `normalize_path` whitespace trim removal

- **Tracking:** <https://github.com/apache/opendal/pull/7801>
- **Status:** WIP.
- **Impact on mntrs:** opendal's `normalize_path` currently trims leading/trailing whitespace from object keys. If upstream removes the trim, filenames containing whitespace (e.g. `" foo.txt"`) would round-trip identically to before. mntrs has its own `canonicalize_list_path` (issue #78 follow-up area) but opendal-internal paths used in `op.list()`, `op.stat()` etc. could see subtle behavior changes.
- **Workaround (current):** None — current trim behavior is what mntrs and its tests already assume.
- **Action:** When #7801 lands, re-run the full mount test matrix (memory + S3 + HDFS backends) with the new opendal. If any test fails on a path-with-whitespace, file an issue.

### opendal#7705 — recursive `list` non-trailing-slash path fix

- **Tracking:** <https://github.com/apache/opendal/pull/7705>
- **Status:** Merged.
- **Impact on mntrs:** Improves list-path slash handling correctness; included automatically the next time we bump opendal past the merge commit.
- **Action:** None — we'll pick this up for free on the next opendal bump.

### opendal#4256 — WebDAV `list` implementation incorrect

- **Tracking:** <https://github.com/apache/opendal/issues/4256>
- **Status:** Open (no upstream fix yet).
- **Impact on mntrs:** Users on the WebDAV backend may see `ls` return wrong results (missing entries, duplicates, or stale listings). The HDFS+Kerberos + WebDAV paths in `tests/e2e/csi/` would surface this if exercised.
- **Workaround (current):** None. Document in user-facing docs that WebDAV `list` is best-effort.
- **Action:** Track upstream; if/when a fix lands, run the WebDAV CI matrix.

---

## 🟢 Low priority — small surface area or rare triggers

| Issue | Title | Tracking | Impact |
|-------|-------|----------|--------|
| opendal#7629 | `list_with_glob` via `GlobLayer` | <https://github.com/apache/opendal/pull/7629> | mntrs has no glob-aware list path; would only matter if we expose `--include`/`--exclude` patterns. |
| opendal#6577 | Trailing-whitespace filename can't be read | <https://github.com/apache/opendal/issues/6577> | Edge case; mntrs-side `canonicalize_list_path` may mask some, but read-after-write on whitespace-suffixed keys is fragile. |

---

## hdfs-native fork: `dfs.client.use.datanode.hostname`

This is the only `Cargo.toml [patch.crates-io]` block we currently carry. It exists to make the k8s HDFS path work end-to-end; full background is in [issue #148](https://github.com/dyrnq/mntrs/issues/148).

### Why a fork (not upstream)

`opendal-service-hdfs-native` is pinned to `hdfs-native ^0.13` in opendal `0.57.0` and `0.58.0` (yanked). The fork `dyrnq/hdfs-native` (rev `419cd0e`) adds the missing `dfs.client.use.datanode.hostname` config knob so the client connects to DataNodes via their k8s Service DNS host_name instead of the in-container loopback `ip_addr=127.0.0.1`.

### Upstream status

- Upstream merged the equivalent in [Kimahriman/hdfs-native#303](https://github.com/Kimahriman/hdfs-native/pull/303) (commit `e339b560`, 2026-06-23).
- Released in `hdfs-native v0.14.1` (2026-07-11); current is `v0.14.2`.
- opendal side: [apache/opendal#7910](https://github.com/apache/opendal/pull/7910) merges `hdfs-native → 0.14` into opendal `master` (2026-07-15). **Not yet in any opendal release.**

### Drop-fork readiness checklist

All four conditions must be true before deleting the `[patch.crates-io]` block:

1. ✅ Upstream `hdfs-native` release with #303 — `v0.14.1` (2026-07-11) onwards.
2. ⏳ opendal release containing [PR #7910](https://github.com/apache/opendal/pull/7910) — pending (next `0.58.x` or `0.59.x`).
3. ⏳ That opendal release must also be installable (i.e., not yanked for the macOS dyld bug #7923) — pending `0.58.1` (with #7925 fix) or `0.59.0` (with #7925 + #7910).
4. ⏳ `Cargo.toml` `opendal` constraint bumped to that version and `cargo update -p opendal` succeeds without breaking any `cargo test` / `cargo build`.

The block in `Cargo.toml` ([lines ~52-80](../../Cargo.toml)) already comments all of this; this section exists as the human-readable companion.

---

## Update policy

When bumping `opendal` (or any related crate) in `Cargo.toml`:

1. Run `cargo update -p opendal` and note the new version.
2. Scan this document for any "Status: Merged" → "Status: Released in opendal X.Y.Z" entries. Update the row.
3. For each entry that says "Action: When #N ships, do X", either land X in the same PR as the version bump, or open a follow-up issue linking to it.
4. If a `[patch.crates-io]` block becomes obsolete (e.g., the hdfs-native fork), delete the block and add a one-line commit message referencing this document and the upstream issue that made the patch unnecessary.

If you find an upstream issue that affects mntrs and isn't listed here, **add it** — this document is the single source of truth for the upgrade-impact matrix.
