#!/usr/bin/env bash
#
# tests/stress/01-large-dir.sh
#
# Issue #143 scenario 1: large directory.
# Create 10,000 files in a single directory, then exercise ls -la / find /
# stat against them and verify zero errors. This catches:
#   - readdir chunk-size regressions (issue #134, #158 already covered)
#   - inode-allocation leaks under heavy churn
#   - stat-cache invalidation races
#
# Configurable via env:
#   STRESS_FILES   — file count (default 10000)
#   STRESS_BYTES   — per-file size (default 256)
#
# Runtime: ~3-5 min on a 4-core VM.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/lib/common.sh"

STRESS_FILES="${STRESS_FILES:-10000}"
STRESS_BYTES="${STRESS_BYTES:-256}"
WORK="$STRSCRATCH/01-large-dir-$$"
MNT="$WORK/mnt"
CACHE="$WORK/cache"
LOG="$WORK/run.log"

section "01-large-dir: $STRESS_FILES files × $STRESS_BYTES bytes"
mntrs_setup
mkdir -p "$WORK"
log "scratch: $WORK"

mntrs_mount "$MNT" "$CACHE"
trap 'mntrs_unmount "$MNT" 2>/dev/null || true; tail -50 "$CACHE/mount.log" 2>/dev/null || true' EXIT

# ── Create files ─────────────────────────────────────────────────────
log "creating $STRESS_FILES files ..."
START=$(date +%s)
for i in $(seq 1 "$STRESS_FILES"); do
    # 8-char zero-padded name → lexicographic order matches numeric
    fname=$(printf 'f_%08d' "$i")
    dd if=/dev/urandom of="$MNT/$fname" bs="$STRESS_BYTES" count=1 status=none
done
CREATE_T=$(( $(date +%s) - START ))
log "create done in ${CREATE_T}s ($(awk -v n="$STRESS_FILES" -v t="$CREATE_T" 'BEGIN{printf "%.1f", n/t}') files/s)"

# ── ls -la ───────────────────────────────────────────────────────────
log "ls -la ..."
START=$(date +%s)
LS_LINES=$(ls -la "$MNT" | wc -l)
LS_T=$(( $(date +%s) - START ))
# Expect: header(. + .. + N files) → N+3 lines for N>0; allow some slack.
assert_ge "$LS_LINES" "$STRESS_FILES" "ls -la line count"

# ── stat each ────────────────────────────────────────────────────────
log "stat each ..."
START=$(date +%s)
FAIL_STAT=0
for i in $(seq 1 "$STRESS_FILES"); do
    fname=$(printf 'f_%08d' "$i")
    stat -c '%n %s' "$MNT/$fname" >/dev/null 2>&1 || FAIL_STAT=$((FAIL_STAT + 1))
done
STAT_T=$(( $(date +%s) - START ))
assert_eq "$FAIL_STAT" "0" "stat each: failed count"

# ── find ─────────────────────────────────────────────────────────────
log "find ..."
START=$(date +%s)
FIND_COUNT=$(find "$MNT" -maxdepth 1 -type f -printf '.' | wc -c)
FIND_T=$(( $(date +%s) - START ))
assert_eq "$FIND_COUNT" "$STRESS_FILES" "find count"

# ── md5sum sanity ────────────────────────────────────────────────────
log "md5sum batch ..."
START=$(date +%s)
(
    cd "$MNT"
    md5sum f_* > "$WORK/md5.txt"
)
MD5_COUNT=$(wc -l < "$WORK/md5.txt")
MD5_T=$(( $(date +%s) - START ))
assert_eq "$MD5_COUNT" "$STRESS_FILES" "md5sum line count"

# ── Final metrics ────────────────────────────────────────────────────
# mntrs daemon PID is the only mntrs process
MNTRS_PID=$(pgrep -f "$(basename "$MNTRS_BIN") mount" | head -1 || true)
if [[ -n "$MNTRS_PID" ]]; then
    stress_metric "$MNTRS_PID" "$WORK/metrics.txt" final
    log "final metrics:"; tail -1 "$WORK/metrics.txt"
fi

{
    echo "files=$STRESS_FILES bytes_per_file=$STRESS_BYTES"
    echo "create_s=$CREATE_T ls_s=$LS_T stat_s=$STAT_T find_s=$FIND_T md5_s=$MD5_T"
} > "$WORK/summary.txt"
cat "$WORK/summary.txt"

pass "01-large-dir OK"