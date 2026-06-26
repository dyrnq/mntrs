#!/usr/bin/env bash
#
# tests/stress/05-crash-recovery.sh
#
# Issue #143 scenario 6: crash recovery.
# Start a write, SIGKILL mntrs while writeback is still pending
# (.dirty sidecar present, upload incomplete), remount, and verify
# the .dirty sidecar is recovered on next mount.
#
# Catches:
#   - Writeback persistence across crashes (the .dirty sidecar recovery
#     path that opens on startup)
#   - State corruption in cache dir from abrupt exit
#   - FUSE session leak when killed before cleanup
#
# Runtime: ~10-30s.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/lib/common.sh"

WORK="$STRSCRATCH/05-crash-recovery-$$"
MNT="$WORK/mnt"
CACHE="$WORK/cache"

section "05-crash-recovery: SIGKILL during writeback, verify .dirty recovery"
mntrs_setup
mkdir -p "$WORK"

# Mount with a 30s write-back delay so we have a wide window to kill
# the daemon while the upload is still pending. (default 1s would race
# with the file-creation latency.)
mntrs_mount "$MNT" "$CACHE" --vfs-write-back 30
trap 'mntrs_unmount "$MNT" 2>/dev/null || true; pkill -9 -f "target/debug/mntrs mount" 2>/dev/null || true' EXIT

# ── Write a file (creates .dirty sidecar immediately) ──────────────
FNAME="$MNT/recovered.bin"
log "writing 4 MiB to $FNAME ..."
dd if=/dev/urandom of="$FNAME" bs=1M count=4 status=none

# Write another so we have >1 to recover
dd if=/dev/urandom of="$MNT/recovered2.bin" bs=1M count=2 status=none

# ── Confirm .dirty sidecars exist (writeback not yet drained) ───────
sleep 0.2
DIRTY_FILES=$(ls "$CACHE"/*.dirty 2>/dev/null | wc -l)
assert_ge "$DIRTY_FILES" "1" "at least one .dirty sidecar exists before crash"
log "before crash: $DIRTY_FILES .dirty sidecars"

# Snapshot the md5 of the in-flight data
EXPECTED_MD5_1=$(md5sum "$FNAME" | awk '{print $1}')
EXPECTED_MD5_2=$(md5sum "$MNT/recovered2.bin" | awk '{print $1}')

# ── SIGKILL the mntrs daemon ────────────────────────────────────────
log "SIGKILL-ing mntrs daemon ..."
MNTRS_PID=$(pgrep -f "target/debug/mntrs mount memory" | head -1 || true)
if [[ -z "$MNTRS_PID" ]]; then
    fail "couldn't find mntrs pid"
fi
kill -9 "$MNTRS_PID" 2>/dev/null || true
sleep 1
# Best-effort unmount; may fail if kernel hasn't released the FUSE fd.
fusermount3 -u "$MNT" 2>/dev/null || fusermount -u "$MNT" 2>/dev/null || true
sleep 1

# ── Verify the .dirty sidecars survived the crash ───────────────────
DIRTY_AFTER=$(ls "$CACHE"/*.dirty 2>/dev/null | wc -l)
assert_eq "$DIRTY_AFTER" "$DIRTY_FILES" "all .dirty sidecars survived crash"

# ── Remount and verify the recovery path picked them up ─────────────
log "remounting to trigger recovery ..."
mntrs_mount "$MNT" "$CACHE" --vfs-write-back 30

# Allow recovery to upload the dirty files (write-back delay = 30s).
log "waiting for recovery upload ..."
WAIT_T=0
while [[ -n "$(ls "$CACHE"/*.dirty 2>/dev/null)" ]]; do
    sleep 1
    WAIT_T=$((WAIT_T + 1))
    if (( WAIT_T > 90 )); then
        log "still dirty after 90s; mount log tail:"
        tail -20 "$CACHE/mount.log"
        fail "recovery didn't drain within 90s"
    fi
done
log "recovery complete in ~${WAIT_T}s"

# ── Verify the recovered files are accessible via FUSE ──────────────
log "verifying recovered files ..."
for f in recovered.bin recovered2.bin; do
    if [[ ! -f "$MNT/$f" ]]; then
        fail "recovered file missing: $f"
    fi
done

GOT_MD5_1=$(md5sum "$MNT/recovered.bin" | awk '{print $1}')
GOT_MD5_2=$(md5sum "$MNT/recovered2.bin" | awk '{print $1}')
assert_eq "$GOT_MD5_1" "$EXPECTED_MD5_1" "recovered.bin md5"
assert_eq "$GOT_MD5_2" "$EXPECTED_MD5_2" "recovered2.bin md5"

# ── Final metrics ───────────────────────────────────────────────────
MNTRS_PID=$(pgrep -f "target/debug/mntrs mount" | head -1 || true)
if [[ -n "$MNTRS_PID" ]]; then
    stress_metric "$MNTRS_PID" "$WORK/metrics.txt" final
    log "final metrics:"; tail -1 "$WORK/metrics.txt"
fi

{
    echo "dirty_before_crash=$DIRTY_FILES dirty_after_crash=$DIRTY_AFTER"
    echo "recovery_s=$WAIT_T"
    echo "expected_md5_1=$EXPECTED_MD5_1"
    echo "got_md5_1=$GOT_MD5_1"
} > "$WORK/summary.txt"
cat "$WORK/summary.txt"

pass "05-crash-recovery OK"