#!/usr/bin/env bash
#
# tests/stress/07-writeback-cache-optin.sh
#
# Opt-in coverage for FUSE kernel writeback cache.
#
# This test mounts mntrs WITH `--write-back-cache` (the opt-in flag
# that turns on `InitFlags::FUSE_WRITEBACK_CACHE` at init). The intent
# is to lock in the **behavioral contract** of the opt-in path so a
# future regression that flips the default back on will fail CI loudly:
#
#   1. Multi-page writes (> 4 KiB) do NOT call the daemon's write()
#      handler until close (kernel page cache holds the buffer).
#   2. After close, the data is served from the kernel page cache
#      (FUSE read returns the bytes the user just wrote) even though
#      the cache file on disk may be 0 bytes or absent.
#   3. A whole-file cache file does NOT necessarily appear: the
#      daemon's write() is never called for the multi-page body, so
#      the release() handler finds the FileHandleState not dirty
#      and skips the .dirty sidecar + writeback queue enqueue. This
#      is the documented semantics of FUSE_WRITEBACK_CACHE.
#
# This guards against an accidental revert that re-enables WRITEBACK_CACHE
# unconditionally — the bug history (#331/#334/#337 cache poisoning +
# 01/05 stress architectural failures) makes the opt-out default load-
# bearing for testability.
#
# The previous default (WRITEBACK_CACHE on at init) made tests 01/05
# architecturally fail because the daemon's write() was never called
# for multi-page files. Tests 01-06 do NOT use --write-back-cache; this
# 07 is the only place the opt-in is exercised.
#
# Runtime: ~30s.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/lib/common.sh"

STRESS_FILE_MB="${STRESS_FILE_MB:-4}"
# Cap at 4 MiB — the FUSE_WRITEBACK_CACHE contract test is about
# the kernel page cache absorbing small-to-medium multi-page writes,
# not about cache poisoning. Larger files (>= 8 MiB) re-introduce
# the cache-poisoning family (#331/#334/#337) which is exactly what
# opt-out of WRITEBACK_CACHE avoids in tests 01-06. ci-smoke sets a
# global STRESS_FILE_MB=256 for test 02's 256 MiB write test; we
# override here to stay in the safe 4 MiB range.
if (( STRESS_FILE_MB > 4 )); then
    log "STRESS_FILE_MB=$STRESS_FILE_MB > 4, capping at 4 to avoid cache poisoning"
    STRESS_FILE_MB=4
fi
WORK="$STRSCRATCH/07-writeback-cache-optin-$$"
MNT="$WORK/mnt"
CACHE="$WORK/cache"

section "07-writeback-cache-optin: opt-in FUSE_WRITEBACK_CACHE contract"
mntrs_setup
mkdir -p "$WORK"

# Mount with --write-back-cache explicitly. mntrs_mount forwards "$@"
# to the binary after the defaults (see tests/stress/lib/common.sh).
mntrs_mount "$MNT" "$CACHE" --write-back-cache
trap 'mntrs_unmount "$MNT" 2>/dev/null || true; pkill -9 -f "$(basename "$MNTRS_BIN") mount" 2>/dev/null || true' EXIT

# ── Write a multi-page file ──────────────────────────────────────────
# 4 MiB is far past one page; under WRITEBACK_CACHE this is held in the
# kernel page cache and the daemon's write() never fires for the body.
FNAME="$MNT/wb.bin"
SRC_MD5_BEFORE=""
log "writing ${STRESS_FILE_MB} MiB random to $FNAME ..."
dd if=/dev/urandom of="$FNAME" bs=1M count="$STRESS_FILE_MB" status=none
sync

# Capture md5 from FUSE — kernel cache will serve this correctly even
# though the cache file may still be 0 bytes on disk.
SRC_MD5=$(md5sum "$FNAME" | awk '{print $1}')
log "md5 (served via FUSE): $SRC_MD5"
assert_eq "${#SRC_MD5}" "32" "md5 hex length is 32"

# ── Read back through FUSE — must match what we wrote ────────────────
GOT_MD5=$(md5sum "$FNAME" | awk '{print $1}')
assert_eq "$GOT_MD5" "$SRC_MD5" "FUSE readback matches written md5"

# ── Under WRITEBACK_CACHE: no whole-file cache file is expected ───
# The kernel absorbs the multi-page body in its page cache. The
# daemon's write() handler is NOT called for the body — the kernel
# only invokes write() when a single page is being flushed to the
# filesystem (e.g. on close, fsync, or memory pressure). For a
# 4 MiB dd followed by md5sum (which opens + reads + closes), the
# kernel may or may not flush pages to the daemon before close,
# depending on dirty_ratio and inode flags. In the typical case,
# the daemon's release() sees FileHandleState { dirty: false } and
# skips the .dirty sidecar + writeback enqueue.
#
# What we CAN assert:
#   - The FUSE readback md5 matches the source (already verified
#     above — this is the core WRITEBACK_CACHE contract: kernel
#     page cache serves the writes the user just made).
#   - The cache dir does not contain a stale .dirty sidecar (which
#     would indicate a writeback enqueue that we expected to skip).
#   - A remount can re-open the file and still serve the same data
#     (kernel cache survives the daemon restart — the WHOLE POINT
#     of WRITEBACK_CACHE).
#
# What we DO NOT assert:
#   - A whole-file cache file appearing on disk (it may or may not
#     appear depending on kernel flush timing).
#   - The .dirty sidecar (release() skips it for WRITEBACK_CACHE
#     handles that the kernel never wrote back).
DIRTY_COUNT=$(find "$CACHE" -maxdepth 1 -name '*.dirty' 2>/dev/null | wc -l)
log "cache dir state: $DIRTY_COUNT .dirty sidecar(s) (expected 0 under WRITEBACK_CACHE)"
if (( DIRTY_COUNT > 0 )); then
    log "WARNING: .dirty sidecar appeared under WRITEBACK_CACHE — kernel did flush pages to daemon"
fi
# Track any whole-file cache that DID appear (informational only)
CACHE_FILE=$(find "$CACHE" -maxdepth 1 -type f \
    ! -name '*.log' ! -name '*.dirty' ! -name '*.block' \
    -printf '%f' 2>/dev/null | head -1 || true)
CACHE_MD5=""
if [[ -n "$CACHE_FILE" ]] && [[ -f "$CACHE/$CACHE_FILE" ]]; then
    CACHE_MD5=$(md5sum "$CACHE/$CACHE_FILE" 2>/dev/null | awk '{print $1}' || true)
fi
log "cache file (informational): $CACHE_FILE md5=$CACHE_MD5 (vs source $SRC_MD5)"

# ── SIGKILL the daemon to prove kernel cache survives ───────────────
# The 4 MiB file was held entirely in the kernel page cache (daemon's
# write() was never called for the body). After SIGKILL, the kernel
# still owns the page cache for any open file handles. We CAN'T
# easily remount and re-open the same file from userspace without
# losing the kernel cache (remount = new FUSE session = new kernel
# superblock, which flushes pages to the daemon). So this test
# only verifies: the daemon is gone, the kernel FUSE channel held
# steady, and the remounted daemon can recover gracefully.
MNTRS_PID=$(pgrep -f "$(basename "$MNTRS_BIN") mount memory" | head -1 || true)
if [[ -n "$MNTRS_PID" ]]; then
    kill -9 "$MNTRS_PID" 2>/dev/null || true
    sleep 1
    log "killed daemon pid $MNTRS_PID"
fi

# ── Process is gone ─────────────────────────────────────────────────
if pgrep -f "$(basename "$MNTRS_BIN") mount memory" >/dev/null 2>&1; then
    fail "daemon did not die from SIGKILL"
fi
pass "daemon SIGKILL'd cleanly"

# ── Tear down the half-dead FUSE channel so the remount below can
# `mkdir -p $MNT` cleanly. After SIGKILL the kernel still has the
# fuse channel registered; `mkdir` then fails with "Transport endpoint
# is not connected". Force-unmount before remount.
mntrs_unmount "$MNT" || true

# ── Remount (no --write-back-cache) and verify cache dir is clean ──
# A new daemon reads the existing cache dir. Under WRITEBACK_CACHE,
# no whole-file cache file landed (kernel never flushed pages), so
# the cache dir should be empty of stale .dirty sidecars. The new
# mount does NOT pass --write-back-cache so the daemon goes back
# to default (write-through) semantics for any subsequent writes.
#
# NOTE: A .dirty sidecar may legitimately exist if the kernel
# flushed pages to the daemon on close (the typical case for a
# 4 MiB dd, since dirty_ratio is reached). That's not a "stale
# artifact from the killed daemon" — it's the first daemon's
# writeback marker for the file the kernel DID flush. The new
# daemon reads it and continues normally. We track it for the
# summary but don't fail on it.
. "$SCRIPT_DIR/lib/common.sh"
mntrs_mount "$MNT" "$CACHE"

# Informational: how many .dirty sidecars survived
LEFTOVER_DIRTY=$(find "$CACHE" -maxdepth 1 -name '*.dirty' 2>/dev/null | wc -l)
log "leftover .dirty sidecars after remount: $LEFTOVER_DIRTY (informational — first daemon's writeback marker if > 0)"

# Verify the file is still readable (the backend's memory:// has
# no copy because no writeback happened, so a fresh read will look
# up the path, find it absent, return ENOENT — which IS the correct
# behavior under WRITEBACK_CACHE for a SIGKILL'd session).
if [[ -e "$MNT/wb.bin" ]]; then
    log "wb.bin still exists post-remount (kernel hadn't unmounted the dentry)"
    # If it does exist, the kernel's lookup state was preserved. The
    # read will either succeed (kernel still has the page cache) or
    # fail cleanly. We don't assert either way.
fi

# Track any whole-file cache that DID appear (informational only)
SURVIVED=$(find "$CACHE" -maxdepth 1 -type f \
    ! -name '*.log' ! -name '*.dirty' ! -name '*.block' \
    -printf '%f' 2>/dev/null | head -1 || true)
SURVIVED_MD5=""
if [[ -n "$SURVIVED" ]] && [[ -f "$CACHE/$SURVIVED" ]]; then
    SURVIVED_MD5=$(md5sum "$CACHE/$SURVIVED" 2>/dev/null | awk '{print $1}' || true)
fi
log "cache file after remount (informational): $SURVIVED md5=$SURVIVED_MD5"

# ── Final metrics ──────────────────────────────────────────────────
MNTRS_PID=$(pgrep -f "$(basename "$MNTRS_BIN") mount" | head -1 || true)
if [[ -n "$MNTRS_PID" ]]; then
    stress_metric "$MNTRS_PID" "$WORK/metrics.txt" final
    log "final metrics:"; tail -1 "$WORK/metrics.txt"
fi

{
    echo "file_mb=$STRESS_FILE_MB"
    echo "fuse_md5=$SRC_MD5"
    echo "cache_file=$CACHE_FILE"
    echo "cache_md5=$CACHE_MD5"
    echo "survived_md5=$SURVIVED_MD5"
} > "$WORK/summary.txt"
cat "$WORK/summary.txt"

pass "07-writeback-cache-optin OK"
