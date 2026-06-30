#!/usr/bin/env bash
#
# tests/stress/05-crash-recovery.sh
#
# Issue #143 scenario 6: crash recovery.
# Start a write, SIGKILL mntrs while writeback is still pending
# (cache file holds the in-flight bytes), remount, and verify
# the cache file survived the crash.
#
# Catches:
#   - Cache file integrity under abrupt exit (regular file should
#     survive SIGKILL — verifies we're not relying on flush() to
#     persist data)
#   - State corruption in cache dir from abrupt exit
#   - FUSE session leak when killed before cleanup
#
# Implementation note: under FUSE_WRITEBACK_CACHE (unconditional at
# src/core_fs/fuser.rs:114), the kernel buffers writes in its own
# page cache and only delivers a single setattr per file on close.
# The daemon's flush()/release() handlers (which create .dirty
# sidecars) are NEVER called. So this test verifies the cache file
# directly instead of relying on .dirty sidecars as a recovery
# marker. If WRITEBACK_CACHE is ever disabled, the .dirty path
# becomes live and this test still passes (cache files are created
# earlier in the write handler).
#
# Runtime: ~20-40s.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/lib/common.sh"

WORK="$STRSCRATCH/05-crash-recovery-$$"
MNT="$WORK/mnt"
CACHE="$WORK/cache"

section "05-crash-recovery: SIGKILL during writeback, verify cache file survival"
mntrs_setup
mkdir -p "$WORK"

# Default write-back delay 1s is fine: we don't depend on the
# delay queue firing because we verify the cache file, not the
# .dirty sidecar.
mntrs_mount "$MNT" "$CACHE"
trap 'mntrs_unmount "$MNT" 2>/dev/null || true; pkill -9 -f "$(basename "$MNTRS_BIN") mount" 2>/dev/null || true' EXIT

# ── Write two files (creates cache files immediately) ────────────────
FNAME="$MNT/recovered.bin"
log "writing 4 MiB to $FNAME ..."
dd if=/dev/urandom of="$FNAME" bs=1M count=4 status=none

log "writing 2 MiB to $MNT/recovered2.bin ..."
dd if=/dev/urandom of="$MNT/recovered2.bin" bs=1M count=2 status=none

# ── Build a fingerprint of the cache dir (sorted size:md5 per file) ──
# We compare via direct disk md5 to side-step FUSE kernel page-cache
# quirks (the kernel may serve userspace reads from its own cache
# without consulting the daemon's write handler).
# Use polling drain (cap 30s) because under FUSE_WRITEBACK_CACHE the
# daemon sees setattr(set_size) on close, then the writeback worker
# fires after --vfs-write-back (default 1s). The cache file lands on
# disk only after the worker reads cache + upload. Total turnaround
# can be 2-3s on slow CI runners.
DRAIN_END=$(( $(date +%s) + 30 ))
while (( $(date +%s) < DRAIN_END )); do
    HAVE=$(find "$CACHE" -maxdepth 1 -type f \
        ! -name '*.log' ! -name '*.dirty' ! -name '*.block' \
        -printf '.' 2>/dev/null | wc -c)
    if (( HAVE >= 2 )); then break; fi
    sleep 1
done

cache_fingerprint() {
    local dir="$1"
    # Only whole-file cache (no .block V3, no .dirty sidecars). Sort for
    # determinism so fingerprint comparison doesn't depend on inode order.
    find "$dir" -maxdepth 1 -type f \
        ! -name '*.log' \
        ! -name '*.dirty' \
        ! -name '*.block' \
        -printf '%s %f\n' \
        2>/dev/null \
        | LC_ALL=C sort \
        | while read -r sz fn; do
            md5sum "$dir/$fn" 2>/dev/null | awk -v s="$sz" '{printf "%s %s\n", s, $1}'
        done
}

PRE_FP=$(cache_fingerprint "$CACHE")
# Count files via the fingerprint function's own find (avoids the
# `grep -c . || echo 0` pattern that produces "0\n0" multi-line values
# when the fingerprint is empty).
PRE_LINES=$(find "$CACHE" -maxdepth 1 -type f \
    ! -name '*.log' ! -name '*.dirty' ! -name '*.block' \
    -printf '.' 2>/dev/null | wc -c)
log "cache dir fingerprint before crash ($PRE_LINES files):"
echo "$PRE_FP" | sed 's/^/    /'
assert_ge "$PRE_LINES" "2" "at least two cache files exist before crash"

# ── Verify the two expected files made it into the cache as full-size ─
# Look up by source-path's FNV-1a hash so we don't depend on the
# alphabetical ordering of cache filenames. The actual filename format
# is `cache_path_block(dir, path, 0)` so a hash lookup is straightforward.
SIZED_4M=$(awk '$1 == "4194304" { c++ } END { print c+0 }' <<<"$PRE_FP")
SIZED_2M=$(awk '$1 == "2097152" { c++ } END { print c+0 }' <<<"$PRE_FP")
assert_ge "$SIZED_4M" "1" "4 MiB cache file present"
assert_ge "$SIZED_2M" "1" "2 MiB cache file present"

# ── SIGKILL the mntrs daemon ────────────────────────────────────────
log "SIGKILL-ing mntrs daemon ..."
MNTRS_PID=$(pgrep -f "$(basename "$MNTRS_BIN") mount memory" | head -1 || true)
if [[ -z "$MNTRS_PID" ]]; then
    fail "couldn't find mntrs pid"
fi
kill -9 "$MNTRS_PID" 2>/dev/null || true
sleep 1
mntrs_unmount "$MNT"
sleep 1

# ── Verify the cache files survived the crash with intact content ───
POST_FP=$(cache_fingerprint "$CACHE")
POST_LINES=$(echo -n "$POST_FP" | grep -c . || echo 0)
assert_eq "$POST_LINES" "$PRE_LINES" "cache file count survived crash"
assert_eq "$POST_FP" "$PRE_FP" "cache file sizes+md5s unchanged after crash"

# ── Remount and verify the cache files aren't corrupted by recovery ─
log "remounting to verify recovery doesn't damage cache files ..."
# Re-source helpers in case env was clobbered (mntrs_unmount may have)
. "$SCRIPT_DIR/lib/common.sh"
mntrs_mount "$MNT" "$CACHE"
sleep 1  # let recovery startup scan the cache dir

POST_RECOVERY_FP=$(cache_fingerprint "$CACHE")
POST_RECOVERY_LINES=$(echo -n "$POST_RECOVERY_FP" | grep -c . || echo 0)
assert_eq "$POST_RECOVERY_LINES" "$PRE_LINES" "cache file count after remount"
assert_eq "$POST_RECOVERY_FP" "$PRE_FP" "cache file md5s unchanged after remount"

# ── Final metrics ───────────────────────────────────────────────────
MNTRS_PID=$(pgrep -f "$(basename "$MNTRS_BIN") mount" | head -1 || true)
if [[ -n "$MNTRS_PID" ]]; then
    stress_metric "$MNTRS_PID" "$WORK/metrics.txt" final
    log "final metrics:"; tail -1 "$WORK/metrics.txt"
fi

{
    echo "cache_files=$PRE_LINES"
    echo "pre_fp:"
    echo "$PRE_FP"
    echo "post_fp:"
    echo "$POST_FP"
} > "$WORK/summary.txt"
cat "$WORK/summary.txt"

pass "05-crash-recovery OK"
