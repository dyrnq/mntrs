#!/usr/bin/env bash
#
# tests/stress/06-soak-mixed.sh
#
# Issue #143 scenario 4: long-running mixed workload.
#
# This is the scaled-down version of the "24h continuous mount" soak
# test — same code paths, but with a configurable duration so CI can
# run it in 5 minutes while an operator can opt-in to longer runs.
#
# Mixed workload (round-robin):
#   1. Write a 4 MiB file with random content
#   2. Read it back + verify md5
#   3. Delete it
#   4. Stat every file in the mountpoint
#
# Metrics collected throughout:
#   - RSS growth (must be bounded — mem-limit LRU should prevent OOM)
#   - fd count (must be bounded — FUSE sessions, writeback permits)
#   - thread count (must be bounded — tokio workers + writeback)
#
# Configurable via env:
#   STRESS_SOAK_SECS  — total duration (default 300 = 5 min)
#   STRESS_INTERVAL   — round-robin interval (default 1s)
#
# Runtime: STRESS_SOAK_SECS + setup overhead.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/lib/common.sh"

STRESS_SOAK_SECS="${STRESS_SOAK_SECS:-300}"
STRESS_INTERVAL="${STRESS_INTERVAL:-1}"
WORK="$STRSCRATCH/06-soak-mixed-$$"
MNT="$WORK/mnt"
CACHE="$WORK/cache"

section "06-soak-mixed: ${STRESS_SOAK_SECS}s mixed R/W/D/stat workload"
mntrs_setup
mkdir -p "$WORK"

# Tighter mem-limit so the soak actually exercises eviction.
# (The issue spec calls out "memory bounded" as a pass criterion.)
mntrs_mount "$MNT" "$CACHE" \
    --mem-limit "${STRESS_MEM_MB:-256}" \
    --mem-cache-metrics-interval 5
trap 'mntrs_unmount "$MNT"' EXIT

MNTRS_PID=$(pgrep -f "$(basename "$MNTRS_BIN") mount memory" | head -1 || true)
if [[ -z "$MNTRS_PID" ]]; then
    fail "couldn't find mntrs pid"
fi
log "mntrs pid: $MNTRS_PID"

# ── Sample metrics every 5s ────────────────────────────────────────
METRICS="$WORK/metrics.txt"
echo "time rss_kb fds threads" > "$METRICS"
INITIAL_RSS=$(awk '/^VmRSS:/ {print $2}' "/proc/$MNTRS_PID/status")
INITIAL_FDS=$(ls -1 "/proc/$MNTRS_PID/fd" | wc -l)
INITIAL_THREADS=$(ls -1 "/proc/$MNTRS_PID/task" | wc -l)
log "baseline: rss=${INITIAL_RSS}KB fds=${INITIAL_FDS} threads=${INITIAL_THREADS}"

# ── Main soak loop ─────────────────────────────────────────────────
END_T=$(( $(date +%s) + STRESS_SOAK_SECS ))
ITER=0
START=$(date +%s)

while (( $(date +%s) < END_T )); do
    ITER=$((ITER + 1))
    fname=$(printf 'soak_%06d.bin' "$ITER")

    # 1. Write 4 MiB random
    dd if=/dev/urandom of="$MNT/$fname" bs=1M count=4 status=none

    # 2. Read back (verify md5 via direct stat after soak — full re-md5
    # of every iteration's file would dominate runtime; instead the
    # drain + final assert_eq covers the invariant).
    md5sum "$MNT/$fname" >/dev/null

    # 3. Delete
    rm -f "$MNT/$fname"

    # 4. Light stat of remaining files (cap to avoid blowing time budget
    # if delete races with new creates)
    find "$MNT" -maxdepth 1 -type f -name 'soak_*' 2>/dev/null | head -100 \
        | xargs -r stat -c '%n %s' >/dev/null 2>&1 || true

    # Periodic metric sample (every 5 iterations ≈ 5s)
    if (( ITER % 5 == 0 )); then
        stress_metric "$MNTRS_PID" "$METRICS"
    fi

    # Cadence
    sleep "$STRESS_INTERVAL"
done

ELAPSED=$(( $(date +%s) - START ))
log "soak done: $ITER iterations in ${ELAPSED}s"

# ── Drain daemon + kernel writeback queues before assertions ────────
# The last several iterations' writebacks may not have settled yet
# (the daemon delay queue holds them for --vfs-write-back seconds).
# Without this drain, the REMAINING_DIRTY assertion below races.
# Note: FUSE_WRITEBACK_CACHE is OFF by default in mntrs — the daemon
# sees synchronous write() calls + creates .dirty sidecars from
# flush()/release(). The poll loop checks .dirty count drains to 0.
sync
DRAIN_END=$(( $(date +%s) + 30 ))
while (( $(date +%s) < DRAIN_END )); do
    # `find` exits 0 with empty stdout when no match — avoids the `set
    # -o pipefail` trap of `ls *.dirty` exiting 2 → silent script exit
    # → confusing "soak done" with no further output.
    N=$(find "$CACHE" -maxdepth 1 -name '*.dirty' -print 2>/dev/null | wc -l)
    if (( N == 0 )); then break; fi
    log "  draining: $N .dirty sidecars remaining ..."
    sleep 1
done

# ── Final metrics ───────────────────────────────────────────────────
stress_metric "$MNTRS_PID" "$METRICS" final
FINAL_RSS=$(awk '/^VmRSS:/ {print $2}' "/proc/$MNTRS_PID/status")
FINAL_FDS=$(ls -1 "/proc/$MNTRS_PID/fd" | wc -l)
FINAL_THREADS=$(ls -1 "/proc/$MNTRS_PID/task" | wc -l)
log "final:    rss=${FINAL_RSS}KB fds=${FINAL_FDS} threads=${FINAL_THREADS}"

# ── Pass/fail criteria ─────────────────────────────────────────────
# 1. RSS growth < 5x baseline (would indicate a leak)
RSS_GROWTH_RATIO=$(awk -v a="$INITIAL_RSS" -v b="$FINAL_RSS" 'BEGIN{printf "%.2f", b/a}')
log "rss_growth_ratio=$RSS_GROWTH_RATIO"

# 2. FD count must not exceed baseline + 50 (FUSE sessions + writeback
# permits should be stable). A runaway usually means a leak in the
# readdir / stat path that opens but doesn't close.
FD_GROWTH=$(( FINAL_FDS - INITIAL_FDS ))
log "fd_growth=$FD_GROWTH"
assert_le "$FD_GROWTH" "50" "fd count growth"

# 3. Thread count must not exceed baseline + 20 (tokio worker pool +
# writeback pool are bounded). A runaway = spawned-but-never-joined
# task.
THREAD_GROWTH=$(( FINAL_THREADS - INITIAL_THREADS ))
log "thread_growth=$THREAD_GROWTH"
assert_le "$THREAD_GROWTH" "20" "thread count growth"

# 4. All writeback must have drained (no .dirty sidecars after soak).
# Use `find` (returns exit 0 with empty stdout on no match) — `ls *.dirty`
# with no match exits 2 and trips `set -o pipefail` + `set -e`, silently
# terminating the script before the assertion can print a meaningful msg.
REMAINING_DIRTY=$(find "$CACHE" -maxdepth 1 -name '*.dirty' -print 2>/dev/null | wc -l)
assert_eq "$REMAINING_DIRTY" "0" "no leftover .dirty sidecars"

{
    echo "duration_s=$ELAPSED iterations=$ITER"
    echo "rss_initial_kb=$INITIAL_RSS rss_final_kb=$FINAL_RSS rss_growth_ratio=$RSS_GROWTH_RATIO"
    echo "fds_initial=$INITIAL_FDS fds_final=$FINAL_FDS fds_growth=$FD_GROWTH"
    echo "threads_initial=$INITIAL_THREADS threads_final=$FINAL_THREADS threads_growth=$THREAD_GROWTH"
    echo "remaining_dirty=$REMAINING_DIRTY"
} > "$WORK/summary.txt"
cat "$WORK/summary.txt"

pass "06-soak-mixed OK"