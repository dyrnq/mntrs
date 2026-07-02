# macOS benchmark (issue #304 / PR #381)

> **Status:** Initial run captured on macOS 15.6
> (darwin/amd64) with macFUSE 5.1.3 kext loaded and a
> local MinIO server (Go 1.24.6, `localhost:9000`).
> 96 mntrs-only tests, 0 failed. With the rclone
> comparison column enabled (auto-detect picks up the
> official binary from `rclone.org/downloads/` if it's
> in `$PATH`), the suite grows to 177 paired tests
> (~30s wall-clock on the same machine).

## Summary

| Field | Value |
|-------|-------|
| Date | 2026-07-02 |
| Host | macOS 15.6, darwin/amd64 (Apple Silicon works too) |
| macFUSE | 5.1.3 (`io.macfuse.filesystems.macfuse.23`) |
| Backend | s3 (MinIO `localhost:9000`, bucket `bench-bucket`) |
| mntrs binary | `./target/release/mntrs` |
| Data points | 96 mntrs tests + 81 rclone tests (with `MAC_BENCH_INCLUDE_RCLONE=auto`) |
| rclone (opt-in) | `~/.local/bin/rclone v1.74.3` (official build, not Homebrew) |
| Wall-clock | ~25s (mntrs-only) / ~32s (with rclone) |

## Why a separate macOS bench

The Linux bench `bench/run_all.sh` is portable POSIX bash,
but it relies on GNU-only toolchain idioms:

* `fusermount3 -u` — replaced with `umount -f`
* `mountpoint -q` — replaced with `mount | grep`
* `stat -c%s` — replaced with BSD `stat -f%z`
* `date -Iseconds` — replaced with `date +%FT%TZ`
* `getfattr -d` — replaced with `xattr -l`
* `head -c1K` — replaced with `head -c $((1 * 1024))`
  (BSD `head` rejects the K/M suffix)

The script also has to canonicalize `/tmp` → `/private/tmp`
because macFUSE registers the canonical path in `mount`
output even when the user passed the symlinked path.

Rather than scattering `#ifdef __APPLE__` blocks across the
shared script (and re-testing them on every Linux change),
the macOS variant is its own file. The structure mirrors
the Linux bench one section at a time so the two stay
in sync visually; differences are called out inline.

## Quick start

```bash
# Build mntrs (debug is fine for quick smoke, release for the real run)
cargo build --release

# Start MinIO + create the bucket
docker run -d --rm -p 9000:9000 -p 9001:9001 \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio server /data --console-address ":9001"

# Run the bench (mntrs-only by default — fast path)
bash bench/run_all_mac.sh

# Compare against rclone (auto-detects the official binary;
# falls back to mntrs-only if `rclone mount` isn't supported)
MAC_BENCH_INCLUDE_RCLONE=auto bash bench/run_all_mac.sh

# Force mntrs-only (skip the rclone probe)
MAC_BENCH_INCLUDE_RCLONE=0 bash bench/run_all_mac.sh

# Skip the S3 backend (memory-only, fast CI-style smoke)
MAC_BENCH_SKIP_S3=1 bash bench/run_all_mac.sh
```

The result table is rendered to stdout AND copied to:

* `/tmp/mntrs-bench-mac-result.txt` — raw `bench()` output
* `/tmp/mntrs-bench-mac-result.md` — rendered markdown table

## rclone on macOS — caveat

**Brew-installed rclone does NOT support `mount` on darwin.**
The rclone docs call this out explicitly; only the official
binary from [rclone.org/downloads](https://rclone.org/downloads/)
can layer FUSE on macOS. The bench script auto-probes for
this on startup:

* If `which rclone` points at a binary that **can** mount
  → comparison column is enabled, header shows
  `rclone: enabled (/path/to/rclone)`
* If the probe fails (most commonly: brew install) → header
  shows `rclone: disabled (mntrs-only)`, bench continues
* You can force either mode via `MAC_BENCH_INCLUDE_RCLONE=0|1`
  regardless of probe result

The probe mounts a temp bucket, sleeps 3s, checks `mount`
output, then unmounts. ~5s overhead at startup.

## GitHub Actions

There is **no** macOS bench workflow. See
[issue #304](https://github.com/dyrnq/mntrs/issues/304) —
the GH-hosted `macos-latest` runner cannot load the
macFUSE kext (the runner image is read-only and the
kext isn't pre-approved), so a real mount e2e is not
possible in CI today.

Until that lands, the macOS bench is **manual only**:
run it on a developer Mac, paste the rendered table
into a PR or issue. The Linux bench workflow
(`.github/workflows/bench.yml`) continues to run on
every PR against `main`; the macOS suite is the
local-only parallel that catches regressions that
don't reproduce on the Linux runner.

## What's different from `bench/run_all.sh`

| Section | Linux | macOS | Reason |
|---------|-------|-------|--------|
| FUSE unmount | `fusermount3 -u` | `umount -f` | macFUSE kext |
| Mount-point check | `mountpoint -q` | `mount \| grep` | no `mountpoint` on macOS |
| File size | `stat -c%s` | `stat -f%z` | BSD stat |
| ISO8601 timestamps | `date -Iseconds` | `date +%FT%TZ` | GNU date extension |
| xattr | `getfattr -d` | `xattr -l` | no `getfattr` on macOS |
| `head -cN{K,M}` | `head -c1K` | `head -c $((N*1024))` | BSD head rejects K/M |
| Path canonicalization | not needed | `/tmp` → `/private/tmp` | macFUSE registers the canonicalized path |
| `rclone mount` | works on brew | **broken on brew** | only official binary works |

The 38 test sections are otherwise the same so it's
straightforward to spot regressions between the two
bench outputs during a code review.

## Result-tmp schema

Each row in `/tmp/mntrs-bench-mac-result.txt` is:

```
<duration>|<test-name>|<target>|<category>
```

`<target>` is either `mntrs` or `rclone` (or `mem` for
the in-process memory backend baseline). The renderer
`bench/render_table.py` consumes this file directly.

## See also

* `bench/run_all.sh` — Linux mntrs-vs-rclone bench
* `bench/baseline.txt` — Linux baseline (mntrs=43 rclone=32 tie=5 80 tests; the historical reference, kept from before the no-static-baseline rule)
* `tests/e2e/bench/check-regression.sh` — POSIX-bash regression gate (consumes a freshly-rendered result, not a checked-in file)
* `docs/benchmark_cat_head_tail.md` — earlier multi-backend scan
* `docs/vfs-cache-flags.md` — `--vfs-cache-mode` shadow-flag audit
* [issue #304](https://github.com/dyrnq/mntrs/issues/304) — GH runner macFUSE limitation

Note: per-platform baseline files (`bench/baseline-*.txt`) are runtime
output and not source — they belong to the same `target/`-class of
files as build artifacts. The regression gate re-renders the bench
and diffs against a previously-checked-in snapshot via
`tests/e2e/bench/check-regression.sh`, so the working baseline for
this script is whatever the most recent successful run produced on
the same hardware, not a file in the repo.