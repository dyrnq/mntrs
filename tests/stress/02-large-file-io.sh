#!/usr/bin/env bash
#
# tests/stress/02-large-file-io.sh
#
# Issue #143 scenario 2: large file sequential I/O.
# Write a 1 GiB file, read it back, verify md5 matches.
# Catches:
#   - writeback buffer overflow / truncation under large writes
#   - multipart upload threshold (issue #46: >200 MiB) end-to-end
#   - read prefetch correctness (issue #132)
#   - .dirty sidecar lifecycle (issue #53)
#
# Configurable via env:
#   STRESS_FILE_MB  — file size in MiB (default 1024 = 1 GiB)
#
# Runtime: ~2-4 min depending on disk speed.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/lib/common.sh"

STRESS_FILE_MB="${STRESS_FILE_MB:-1024}"
WORK="$STRSCRATCH/02-large-file-io-$$"
MNT="$WORK/mnt"
CACHE="$WORK/cache"

section "02-large-file-io: ${STRESS_FILE_MB} MiB sequential write+read+md5"
mntrs_setup
mkdir -p "$WORK"

mntrs_mount "$MNT" "$CACHE"
trap 'mntrs_unmount "$MNT"' EXIT

SRC="$WORK/source.bin"
DST="$MNT/big.bin"
READBACK="$WORK/readback.bin"

# ── Source: write locally first to know expected md5 ─────────────────
log "creating ${STRESS_FILE_MB} MiB source locally (this is the reference) ..."
START=$(date +%s)
dd if=/dev/urandom of="$SRC" bs=1M count="$STRESS_FILE_MB" status=none
SRC_T=$(( $(date +%s) - START ))
log "source done in ${SRC_T}s"
SRC_MD5=$(md5sum "$SRC" | awk '{print $1}')
log "source md5: $SRC_MD5"

# ── Copy to mount (exercises write+writeback) ────────────────────────
log "copying to mount (write + writeback upload) ..."
START=$(date +%s)
cp -f "$SRC" "$DST"
# Issue #53: wait for writeback upload to complete (the .dirty sidecar
# is removed only after a successful upload).
DIRTY="$CACHE/big.bin.dirty"
WAIT_T=0
while [[ -f "$DIRTY" ]]; do
    sleep 0.5
    WAIT_T=$((WAIT_T + 1))
    if (( WAIT_T > 600 )); then
        fail "writeback didn't drain within 300s — .dirty sidecar still present"
    fi
done
COPY_T=$(( $(date +%s) - START ))
log "copy + drain done in ${COPY_T}s ($(awk -v n="$STRESS_FILE_MB" -v t="$COPY_T" 'BEGIN{printf "%.1f", n/t}') MiB/s)"

# ── Read back through FUSE (exercises read path + prefetch) ──────────
log "reading back through mount ..."
START=$(date +%s)
cp -f "$DST" "$READBACK"
READ_T=$(( $(date +%s) - START ))
log "read done in ${READ_T}s ($(awk -v n="$STRESS_FILE_MB" -v t="$READ_T" 'BEGIN{printf "%.1f", n/t}') MiB/s)"

# ── Verify ───────────────────────────────────────────────────────────
READ_MD5_VAL=$(md5sum "$READBACK" | awk '{print $1}')
assert_eq "$READ_MD5_VAL" "$SRC_MD5" "read-back md5 matches source"

# ── Metrics ──────────────────────────────────────────────────────────
MNTRS_PID=$(pgrep -f "$(basename "$MNTRS_BIN") mount" | head -1 || true)
if [[ -n "$MNTRS_PID" ]]; then
    stress_metric "$MNTRS_PID" "$WORK/metrics.txt" final
    log "final metrics:"; tail -1 "$WORK/metrics.txt"
fi

{
    echo "file_mb=$STRESS_FILE_MB src_md5=$SRC_MD5"
    echo "src_s=$SRC_T copy_s=$COPY_T read_s=$READ_T"
    echo "src_mibps=$(awk -v n="$STRESS_FILE_MB" -v t="$SRC_T" 'BEGIN{printf "%.1f", n/t}')"
    echo "copy_mibps=$(awk -v n="$STRESS_FILE_MB" -v t="$COPY_T" 'BEGIN{printf "%.1f", n/t}')"
    echo "read_mibps=$(awk -v n="$STRESS_FILE_MB" -v t="$READ_T" 'BEGIN{printf "%.1f", n/t}')"
} > "$WORK/summary.txt"
cat "$WORK/summary.txt"

pass "02-large-file-io OK"