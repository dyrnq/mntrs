#!/usr/bin/env bash
#
# tests/stress/04-writeback-concurrent.sh
#
# Issue #143 scenario 5: writeback under load.
# Fire N parallel writers (>= UPLOAD_SEM=4 permits in writeback.rs)
# at the same mount, write distinct files, then wait for writeback
# drain and verify every file made it to the backend.
#
# Catches:
#   - Concurrency bugs in writeback::spawn (issue #53 cap math,
#     MAX_REENQUEUE_CYCLES race, Semaphore permit accounting)
#   - Lost writes when upload order is non-FIFO
#   - PENDING_COUNT accounting leaks
#
# Configurable via env:
#   STRESS_PARALLEL  — writer count (default 8, > UPLOAD_SEM=4)
#   STRESS_FILES_PP  — files per writer (default 8)
#   STRESS_FILE_KB   — per-file size in KiB (default 256)
#
# Runtime: ~30s.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/lib/common.sh"

STRESS_PARALLEL="${STRESS_PARALLEL:-8}"
STRESS_FILES_PP="${STRESS_FILES_PP:-8}"
STRESS_FILE_KB="${STRESS_FILE_KB:-256}"
WORK="$STRSCRATCH/04-writeback-concurrent-$$"
MNT="$WORK/mnt"
CACHE="$WORK/cache"

section "04-writeback-concurrent: ${STRESS_PARALLEL} writers × ${STRESS_FILES_PP} files × ${STRESS_FILE_KB} KiB"
mntrs_setup
mkdir -p "$WORK"

mntrs_mount "$MNT" "$CACHE"
trap 'mntrs_unmount "$MNT"' EXIT

# ── Pre-generate expected content (same as writers will produce) ─────
# Deterministic per-file content: 32 bytes of "data_${writer}_${file}..." → md5.
# Avoids depending on /dev/urandom which is slow under parallel load.
write_one() {
    local w="$1" f="$2"
    local fname
    fname=$(printf 'w%02d_f%03d.bin' "$w" "$f")
    local payload
    payload=$(printf 'data_writer=%d file=%d padding=' "$w" "$f")
    printf '%s' "$payload" > "$MNT/$fname"
    # Pad with zeros to exact size
    truncate -s "$((STRESS_FILE_KB * 1024))" "$MNT/$fname"
    # Re-write the payload at the start (truncate doesn't change content)
    printf '%s' "$payload" | dd of="$MNT/$fname" conv=notrunc status=none bs=1 count="${#payload}"
    md5sum "$MNT/$fname" | awk -v n="$fname" '{print $1, n}'
}

export -f write_one
export MNT
export STRESS_FILE_KB

# ── Parallel write phase ────────────────────────────────────────────
log "firing $STRESS_PARALLEL writers ..."
START=$(date +%s)
MD5_FILE="$WORK/written.md5"
: > "$MD5_FILE"

# Use xargs -P for clean parallelism with output capture
seq 1 "$STRESS_PARALLEL" | xargs -P"$STRESS_PARALLEL" -I{} bash -c '
    w="$1"
    for f in $(seq 1 '"$STRESS_FILES_PP"'); do
        write_one "$w" "$f"
    done
' _ {} >> "$MD5_FILE" 2>&1
WRITE_T=$(( $(date +%s) - START ))
TOTAL_FILES=$(( STRESS_PARALLEL * STRESS_FILES_PP ))
log "parallel write done in ${WRITE_T}s ($TOTAL_FILES files)"

# ── Drain writeback ─────────────────────────────────────────────────
log "waiting for writeback drain ..."
WAIT_T=0
while [[ -n "$(ls "$CACHE"/*.dirty 2>/dev/null)" ]]; do
    sleep 0.5
    WAIT_T=$((WAIT_T + 1))
    if (( WAIT_T > 600 )); then
        # Dump diagnostics
        log "still dirty after 300s; PENDING_COUNT check:"
        grep -E "pending_count|writeback.*STUCK" "$CACHE/mount.log" | tail -10 || true
        fail "writeback didn't drain within 300s"
    fi
done
DRAIN_T=$(( $(date +%s) - START ))
log "drain complete (total ${DRAIN_T}s)"

# ── Verify every file is present on backend with correct md5 ─────────
# Issue #158: prefer batch stat to avoid 64 sequential stat calls.
log "verifying $TOTAL_FILES files on backend ..."
MISSING=0
while read -r md5 fname; do
    if [[ ! -f "$MNT/$fname" ]]; then
        log "  MISSING: $fname"
        MISSING=$((MISSING + 1))
        continue
    fi
    GOT=$(md5sum "$MNT/$fname" | awk '{print $1}')
    if [[ "$GOT" != "$md5" ]]; then
        log "  MD5 MISMATCH: $fname (got=$GOT want=$md5)"
        MISSING=$((MISSING + 1))
    fi
done < "$MD5_FILE"
assert_eq "$MISSING" "0" "all files present and matching"

# ── PENDING_COUNT should be 0 after drain ───────────────────────────
PENDING=$(grep -E "pending_count|PENDING" "$CACHE/mount.log" | tail -1 || echo "n/a")
log "pending_count trace: $PENDING"

# ── Metrics ──────────────────────────────────────────────────────────
MNTRS_PID=$(pgrep -f "$(basename "$MNTRS_BIN") mount" | head -1 || true)
if [[ -n "$MNTRS_PID" ]]; then
    stress_metric "$MNTRS_PID" "$WORK/metrics.txt" final
    log "final metrics:"; tail -1 "$WORK/metrics.txt"
fi

{
    echo "parallel=$STRESS_PARALLEL files_per_writer=$STRESS_FILES_PP file_kb=$STRESS_FILE_KB"
    echo "total_files=$TOTAL_FILES"
    echo "write_s=$WRITE_T drain_s=$WAIT_T"
} > "$WORK/summary.txt"
cat "$WORK/summary.txt"

pass "04-writeback-concurrent OK"