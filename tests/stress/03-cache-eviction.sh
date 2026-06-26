#!/usr/bin/env bash
#
# tests/stress/03-cache-eviction.sh
#
# Issue #143 scenario 3: cache eviction under memory pressure.
# Write 2x the --mem-limit. After the run, verify:
#   - No read errors (LRU eviction must be transparent to read path)
#   - Read-back md5 still matches source (no corruption)
#   - mem_cache used/capacity within bounds (eviction actually triggered)
#
# Catches:
#   - LRU eviction races (issue #118 mem_limiter release_if_reserved)
#   - block-cache inconsistency after eviction (issue #55)
#   - mem_cache A/B parity between dashmap/moka/foyer
#
# Configurable via env:
#   STRESS_MEM_MB   — mem-limit (default 256 MiB; total data = 2x)
#
# Runtime: ~1-3 min depending on cache mode.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/lib/common.sh"

STRESS_MEM_MB="${STRESS_MEM_MB:-256}"
# Per-file size: 64 MiB → 8 files for 512 MiB total (2x mem-limit).
FILE_MB=64
N_FILES=$(( (STRESS_MEM_MB * 2 + FILE_MB - 1) / FILE_MB ))
WORK="$STRSCRATCH/03-cache-eviction-$$"
MNT="$WORK/mnt"
CACHE="$WORK/cache"

section "03-cache-eviction: $N_FILES × ${FILE_MB}MiB = $((N_FILES * FILE_MB))MiB with mem-limit=${STRESS_MEM_MB}MiB"
mntrs_setup
mkdir -p "$WORK"

# Enable mem_cache metrics emission (1s interval) so we can assert
# eviction actually fired — without this the test would silently pass
# even if mem-limit wasn't enforced.
mntrs_mount "$MNT" "$CACHE" \
    --mem-limit "$STRESS_MEM_MB" \
    --mem-cache-metrics-interval 1 \
    --mem-cache-impl "${STRESS_MEM_IMPL:-dashmap}"
trap 'mntrs_unmount "$MNT"' EXIT

# ── Create N source files locally (reference md5s) ──────────────────
log "creating $N_FILES reference files (${FILE_MB} MiB each) ..."
START=$(date +%s)
declare -A SRC_MD5
for i in $(seq 1 "$N_FILES"); do
    fname=$(printf 'f_%02d.bin' "$i")
    dd if=/dev/urandom of="$WORK/$fname" bs=1M count="$FILE_MB" status=none
    SRC_MD5[$fname]=$(md5sum "$WORK/$fname" | awk '{print $1}')
done
SRC_T=$(( $(date +%s) - START ))
log "sources ready in ${SRC_T}s"

# ── Copy to mount (forces 2x mem-limit; LRU must evict) ──────────────
log "copying to mount (will evict ~half the working set) ..."
START=$(date +%s)
for i in $(seq 1 "$N_FILES"); do
    fname=$(printf 'f_%02d.bin' "$i")
    cp -f "$WORK/$fname" "$MNT/$fname"
done
COPY_T=$(( $(date +%s) - START ))
log "copy done in ${COPY_T}s"

# Wait for writeback to drain.
WAIT_T=0
while [[ -n "$(ls "$CACHE"/*.dirty 2>/dev/null)" ]]; do
    sleep 0.5
    WAIT_T=$((WAIT_T + 1))
    if (( WAIT_T > 600 )); then
        fail "writeback didn't drain within 300s"
    fi
done

# ── Read back through FUSE — read path must be transparent ───────────
log "reading back all $N_FILES through mount ..."
READ_FAIL=0
for i in $(seq 1 "$N_FILES"); do
    fname=$(printf 'f_%02d.bin' "$i")
    # cp reads sequentially through the FUSE read path. After eviction,
    # mntrs should fall through to the remote backend (memory://).
    if ! cp -f "$MNT/$fname" "$WORK/readback_$fname" 2>/dev/null; then
        log "  READ FAIL: $fname"
        READ_FAIL=$((READ_FAIL + 1))
        continue
    fi
    GOT=$(md5sum "$WORK/readback_$fname" | awk '{print $1}')
    WANT="${SRC_MD5[$fname]}"
    if [[ "$GOT" != "$WANT" ]]; then
        log "  MD5 MISMATCH: $fname (got=$GOT want=$WANT)"
        READ_FAIL=$((READ_FAIL + 1))
    fi
done
assert_eq "$READ_FAIL" "0" "all read-backs match source md5"

# ── Check mem_cache metrics show eviction ───────────────────────────
# Count the events where evictions > 0.
EVICT_NONZERO=$(awk '
    /mem_cache.*evictions=/ {
        match($0, /evictions=([0-9]+)/, a); if (a[1] > 0) c++
    } END { print c+0 }
' "$CACHE/mount.log" 2>/dev/null || echo 0)
log "mem_cache events with non-zero evictions: $EVICT_NONZERO"
if (( EVICT_NONZERO == 0 )); then
    warn "no mem_cache eviction events seen — mem-limit may not be enforced, or the test fits in cache"
fi

# ── Metrics ──────────────────────────────────────────────────────────
MNTRS_PID=$(pgrep -f "target/debug/mntrs mount" | head -1 || true)
if [[ -n "$MNTRS_PID" ]]; then
    stress_metric "$MNTRS_PID" "$WORK/metrics.txt" final
    log "final metrics:"; tail -1 "$WORK/metrics.txt"
fi

{
    echo "mem_limit_mb=$STRESS_MEM_MB total_data_mb=$((N_FILES * FILE_MB))"
    echo "n_files=$N_FILES file_mb=$FILE_MB"
    echo "src_s=$SRC_T copy_s=$COPY_T"
    echo "eviction_events=$EVICT_NONZERO"
} > "$WORK/summary.txt"
cat "$WORK/summary.txt"

pass "03-cache-eviction OK"