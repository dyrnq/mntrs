#!/usr/bin/env bash
#
# A/B benchmark for mem_cache: dashmap (FIFO) vs moka (TinyLFU).
#
# Runs an identical mixed IO workload against the memory backend
# with each impl wired up, captures the periodic
# `mem_cache_stats` tracing events, and prints a side-by-side
# comparison.
#
# The workload is shaped to make TinyLFU shine:
#
#   Phase 1 — cold:  write 64 small files, read each once
#   Phase 2 — hot:   re-read the same 64 files 5 times in a row
#                     (the working set fits in 256 MiB mem_cache
#                      with room to spare; both impls should
#                      hit 100%, but we'll see if they don't)
#   Phase 3 — churn: overwrite the first 32 files with new
#                     content, then re-read all 64 (moka's
#                     TinyLFU should keep the still-hot 32
#                     and evict the now-cold 32; DashMap's
#                     FIFO will evict the *oldest* inserted,
#                     which after a full re-read is roughly
#                     the same — but the transient churn
#                     behavior can differ)
#   Phase 4 — random: pick 200 random (file, offset) reads
#                     (forces misses regardless of policy)
#
# The benchmark runs 3 iterations per impl to average out
# mount-time noise (cold first-call cost). Each iteration is
# a fresh mount, so the read path's in-process state can't
# leak across runs.
#
# Run locally:
#   ./tests/e2e/mem_cache_ab_bench.sh [iterations]
#   ./tests/e2e/mem_cache_ab_bench.sh 3
#
# Requires:
#   * mntrs release binary built (target/release/mntrs)
#   * fusermount3 / fusermount
#   * a temp mountpoint dir

set -u

ITERATIONS="${1:-3}"
BIN="${BIN:-/data/work/mntrs/target/release/mntrs}"
MP_BASE="${MP_BASE:-/tmp/mntrs-ab-test}"
CACHE_BASE="${CACHE_BASE:-/tmp/mntrs-ab-cache}"

if [ ! -x "$BIN" ]; then
    echo "=== Build mntrs ==="
    cargo build --release 2>&1 | tail -3
fi

# Per-impl scratch dirs.
declare -A LOG_DIR
for impl in dashmap moka; do
    LOG_DIR[$impl]="/tmp/mntrs-ab-${impl}-logs"
    rm -rf "${LOG_DIR[$impl]}"
    mkdir -p "${LOG_DIR[$impl]}"
done

# ---- Per-impl / per-iter workhorse ----
run_one() {
    local impl="$1"
    local iter="$2"
    local MP="${MP_BASE}-${impl}-${iter}"
    local CACHE="${CACHE_BASE}-${impl}-${iter}"
    local LOG="${LOG_DIR[$impl]}/iter-${iter}.log"
    local FAIL=0

    rm -rf "$MP" "$CACHE"
    mkdir -p "$MP" "$CACHE"

    # Mount with metrics logger at 0.5s tick — fine enough
    # to capture the per-phase transitions, coarse enough
    # to keep log volume readable.
    RUST_LOG=info \
    "$BIN" mount "memory:///" "$MP" \
        --cache-dir "$CACHE" \
        --mem-cache-impl "$impl" \
        --mem-cache-metrics-interval 1 \
        > "$LOG" 2>&1 &
    local MPID=$!
    sleep 2

    # 60s readiness probe (matches the CI workflow).
    local READY=0
    for i in $(seq 1 60); do
        if mount | grep -q "$MP" && ls "$MP/" >/dev/null 2>&1; then
            READY=1
            break
        fi
        sleep 1
    done
    if [ $READY -eq 0 ]; then
        echo "  [$impl iter $iter] MOUNT FAILED"
        cat "$LOG"
        fusermount3 -u "$MP" 2>/dev/null
        return 1
    fi

    # Phase 1: cold writes
    for i in $(seq 1 64); do
        dd if=/dev/urandom of="$MP/file_$(printf %03d $i).dat" \
           bs=4096 count=4 2>/dev/null || FAIL=1
    done

    # Phase 2: hot re-reads (5 iterations of the full set)
    for round in 1 2 3 4 5; do
        for i in $(seq 1 64); do
            cat "$MP/file_$(printf %03d $i).dat" >/dev/null 2>&1 || FAIL=1
        done
    done

    # Phase 3: churn (overwrite first 32 with bigger content)
    for i in $(seq 1 32); do
        dd if=/dev/urandom of="$MP/file_$(printf %03d $i).dat" \
           bs=4096 count=16 2>/dev/null || FAIL=1
        # Read it once to populate the cache with the new size
        cat "$MP/file_$(printf %03d $i).dat" >/dev/null 2>&1
    done

    # Phase 4: random access (200 random reads)
    for i in $(seq 1 200); do
        local idx=$((RANDOM % 64 + 1))
        cat "$MP/file_$(printf %03d $idx).dat" >/dev/null 2>&1
    done

    # Cleanup
    rm -f "$MP"/file_*.dat 2>/dev/null
    sleep 1  # let the final metrics tick fire

    fusermount3 -u "$MP" 2>/dev/null
    for _ in 1 2 3 4 5; do
        mount | grep -q " $MP " || break
        sleep 0.5
    done

    if [ $FAIL -eq 0 ]; then
        echo "  [$impl iter $iter] OK"
    else
        echo "  [$impl iter $iter] FAILED (sub-step)"
    fi
}

echo "=== mem_cache A/B benchmark ==="
echo "iterations: $ITERATIONS"
echo "binary:     $BIN"
echo

# Run the workload for each impl, N iterations each.
for impl in dashmap moka; do
    echo "--- impl=$impl ---"
    for i in $(seq 1 "$ITERATIONS"); do
        run_one "$impl" "$i"
    done
done

echo
echo "=== results ==="

# Extract the LAST `mem_cache_stats` line from each log (the
# terminal snapshot of the cache state) and format as a
# table.
#
# The tracing subscriber emits ANSI escape codes between the
# field name and the value (for terminal coloring); the
# regex strips them by matching the ANSI CSI sequence
# `\<ESC\>\[[0-9;]*m` greedily between the field name and
# the digit run.
extract_field() {
    # $1=log path, $2=field name (e.g. "hits"). For
    # `hit_rate_pct` the value is a float (e.g. "46.38"),
    # and the value is wrapped in quotes — so we need a
    # pattern that handles both. We always strip the quotes
    # if present so the caller gets a clean number.
    grep "mem_cache_stats" "$1" 2>/dev/null \
        | tail -1 \
        | sed -E 's/\x1b\[[0-9;]*m//g' \
        | grep -oE "${2}=(\"[0-9.]+\"|[0-9]+)" \
        | head -1 \
        | sed -E "s/${2}=(\"|)//; s/\"$//"
}

printf "%-12s %-5s %-8s %-8s %-12s %-9s %-10s %-9s %-9s %-10s\n" \
    "impl" "iter" "hits" "misses" "hit_rate_pct" "inserts" "evictions" "entries" "used_B" "cap_B"
printf "%-12s %-5s %-8s %-8s %-12s %-9s %-10s %-9s %-9s %-10s\n" \
    "----" "----" "----" "------" "------------" "--------" "---------" "-------" "------" "-----"

for impl in dashmap moka; do
    for i in $(seq 1 "$ITERATIONS"); do
        log="${LOG_DIR[$impl]}/iter-${i}.log"
        if ! grep -q "mem_cache_stats" "$log" 2>/dev/null; then
            printf "%-12s %-5s %s\n" "$impl" "$i" "(no stats line)"
            continue
        fi
        # Extract structured fields from the tracing line.
        hits=$(extract_field "$log" "hits")
        misses=$(extract_field "$log" "misses")
        hit_rate=$(extract_field "$log" "hit_rate_pct")
        inserts=$(extract_field "$log" "inserts")
        evictions=$(extract_field "$log" "evictions")
        entries=$(extract_field "$log" "entries")
        used=$(extract_field "$log" "used_bytes")
        cap=$(extract_field "$log" "capacity_bytes")
        printf "%-12s %-5s %-8s %-8s %-12s %-9s %-10s %-9s %-9s %-10s\n" \
            "$impl" "$i" "$hits" "$misses" "${hit_rate}%" "$inserts" "$evictions" "$entries" "$used" "$cap"
    done
done

echo
echo "=== summary ==="

# Per-impl mean of the last-tick hit_rate. Both impls should
# converge to ~100% under the Phase 2 hot-re-read loop (the
# working set fits the cache). The interesting metric is
# `misses` — fewer misses under Phase 3 churn means the
# eviction policy kept the still-hot blocks.
for impl in dashmap moka; do
    total_hits=0
    total_misses=0
    total_inserts=0
    total_evictions=0
    for i in $(seq 1 "$ITERATIONS"); do
        log="${LOG_DIR[$impl]}/iter-${i}.log"
        if ! grep -q "mem_cache_stats" "$log" 2>/dev/null; then continue; fi
        h=$(extract_field "$log" "hits")
        m=$(extract_field "$log" "misses")
        ins=$(extract_field "$log" "inserts")
        ev=$(extract_field "$log" "evictions")
        total_hits=$((total_hits + h))
        total_misses=$((total_misses + m))
        total_inserts=$((total_inserts + ins))
        total_evictions=$((total_evictions + ev))
    done
    if [ $total_hits -gt 0 ] || [ $total_misses -gt 0 ]; then
        total=$((total_hits + total_misses))
        rate=$(( total > 0 ? total_hits * 100 / total : 0 ))
        printf "%-12s avg over %d iters: hits=%d misses=%d hit_rate=%d%% inserts=%d evictions=%d\n" \
            "$impl" "$ITERATIONS" "$total_hits" "$total_misses" "$rate" "$total_inserts" "$total_evictions"
    fi
done

echo
echo "=== raw logs (last 3 stats lines per impl per iter) ==="
for impl in dashmap moka; do
    echo
    echo "--- $impl ---"
    for i in $(seq 1 "$ITERATIONS"); do
        echo "  iter $i:"
        grep "mem_cache_stats" "${LOG_DIR[$impl]}/iter-${i}.log" 2>/dev/null | tail -3 | sed 's/^/    /'
    done
done
