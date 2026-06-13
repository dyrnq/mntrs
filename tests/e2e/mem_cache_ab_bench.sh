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
#   Phase 4 — Zipfian hot reads: 200 reads with a 80/20
#                     access skew (80% of reads go to 20% of
#                     files) — the textbook production
#                     workload. moka's TinyLFU should retain
#                     the hot 20% across the cold 80%
#                     churn; DashMap's FIFO evicts the
#                     oldest-inserted regardless of access
#                     recency.
#   Phase 5 — sequential scan: read all 64 files in order
#                     (the `find`/`tar`/backup pattern). Tests
#                     whether the cache survives a forward
#                     iteration that touches every key once.
#                     FIFO is disadvantaged here (it must
#                     keep the previous block while reading
#                     the next); TinyLFU's tiny ghost sketch
#                     remembers the recent pattern.
#   Phase 6 — mixed read-write with temporal locality: 60
#                     cycles of "write small file → read
#                     it back" on a small set of files
#                     (editor / build-tool pattern). The
#                     write path triggers `invalidate_ino`;
#                     the read path then refills the cache.
#                     This is the workload where FIFO hurts
#                     the most: the most-recently-written
#                     block is also the most-recently-read
#                     block, but FIFO evicts on insert
#                     order, not on access recency.
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
# Comma-separated list of backends to benchmark. Each
# value maps to a `mntrs mount` URL with appropriate
# `--opt` flags. Defaults to "memory" (hermetic, no
# external services required). Add "s3" or "hdfs" to run
# the same workload against a real backend.
BACKENDS="${BACKENDS:-memory}"
MP_BASE="${MP_BASE:-/tmp/mntrs-ab-test}"
CACHE_BASE="${CACHE_BASE:-/tmp/mntrs-ab-cache}"

if [ ! -x "$BIN" ]; then
    echo "=== Build mntrs ==="
    cargo build --release 2>&1 | tail -3
fi

# Per-(backend, impl) scratch dirs. Keyed by both because
# the same `(impl, iter)` index is reused across backends.
declare -A LOG_DIR
for backend in $BACKENDS; do
    for impl in dashmap moka; do
        LOG_DIR["${backend}/${impl}"]="/tmp/mntrs-ab-${backend}-${impl}-logs"
        rm -rf "${LOG_DIR[${backend}/${impl}]}"
        mkdir -p "${LOG_DIR[${backend}/${impl}]}"
    done
done

# ---- Backend URL builder ----
#
# Returns the `mntrs mount <URL> [opts...]` argument
# list for the requested backend. The URL is the first
# line; subsequent lines are --opt key=value pairs.
build_mount_args() {
    case "$1" in
        memory)
            echo "memory:///"
            ;;
        s3)
            # Local MinIO at minio-test:9000. The bucket
            # `bench-memcache` is created by the prerequisite
            # setup. Credentials match the docker run line in
            # `.github/workflows/integration.yml`.
            echo "s3://bench-memcache"
            echo "--opt"
            echo "endpoint=http://localhost:9000"
            echo "--opt"
            echo "access-key=minioadmin"
            echo "--opt"
            echo "secret-key=minioadmin"
            echo "--opt"
            echo "region=us-east-1"
            ;;
        hdfs)
            # Local HDFS simple-auth container, nameservice
            # aliased to 127.0.0.1 via `--add-host
            # nameservice:127.0.0.1`. Same setup the CI
            # workflow uses (integration.yml).
            echo "hdfs://localhost:8020/"
            ;;
        *)
            echo "ERROR: unknown backend '$1'" >&2
            return 1
            ;;
    esac
}

# ---- Per-impl / per-iter workhorse ----
run_one() {
    local backend="$1"
    local impl="$2"
    local iter="$3"
    local MP="${MP_BASE}-${backend}-${impl}-${iter}"
    local CACHE="${CACHE_BASE}-${backend}-${impl}-${iter}"
    local LOG="${LOG_DIR[${backend}/${impl}]}/iter-${iter}.log"
    local FAIL=0

    rm -rf "$MP" "$CACHE"
    mkdir -p "$MP" "$CACHE"

    # Build the mount URL + opts for this backend.
    local mount_args
    mount_args=$(build_mount_args "$backend") || return 1
    local url=$(echo "$mount_args" | head -1)
    local opts=()
    while IFS= read -r line; do
        opts+=("$line")
    done < <(echo "$mount_args" | tail -n +2)

    # Mount with metrics logger at 1s tick — fine enough
    # to capture the per-phase transitions, coarse enough
    # to keep log volume readable.
    #
    # MEM_LIMIT (env) overrides the default 256 MiB cache
    # cap. Set to a small value (e.g. 16) to force the
    # eviction policy to actually run on every workload,
    # exposing the impl difference. With the default cap,
    # our working set fits in cache and both impls
    # converge to 100% on the hot set.
    #
    # CACHE_MODE (env) defaults to whatever the binary
    # defaults to ("writes"). Set to "off" to bypass the
    # on-disk cache layer entirely — this makes
    # `mem_cache` the only cache in front of the backend,
    # which is the right setting to stress-test the
    # mem_cache eviction policy. With the disk cache
    # enabled, every working-set entry has a hot disk
    # shadow and mem_cache rarely has to evict.
    local extra_mount_args=()
    if [ -n "${MEM_LIMIT:-}" ]; then
        extra_mount_args+=(--mem-limit "$MEM_LIMIT")
    fi
    if [ -n "${CACHE_MODE:-}" ]; then
        extra_mount_args+=(--vfs-cache-mode "$CACHE_MODE")
    fi
    RUST_LOG=info \
    "$BIN" mount "$url" "$MP" \
        --cache-dir "$CACHE" \
        "${opts[@]}" \
        --mem-cache-impl "$impl" \
        "${extra_mount_args[@]}" \
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
        echo "  [$backend/$impl iter $iter] MOUNT FAILED"
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

    # Phase 4: Zipfian hot reads (200 reads, 80/20 skew).
    # We approximate a Zipfian distribution by partitioning
    # the 64 files into a "hot 20%" set (indices 1..13,
    # approx 12.5 files) and a "cold 80%" set, then
    # drawing 80% of reads from the hot set and 20% from
    # the cold set. Real Zipfian would be a power law, but
    # the binary split is a sharper test of the eviction
    # policy — DashMap's FIFO evicts the cold set as
    # they're touched (the hot set was inserted first, so
    # they come out first), while TinyLFU's admission
    # filter locks the hot set in.
    for i in $(seq 1 200); do
        if [ $((RANDOM % 100)) -lt 80 ]; then
            local idx=$((RANDOM % 13 + 1))   # hot set
        else
            local idx=$((RANDOM % 51 + 14))  # cold set (14..64)
        fi
        cat "$MP/file_$(printf %03d $idx).dat" >/dev/null 2>&1
    done

    # Phase 5: sequential scan. Read all 64 files in
    # order, twice. The first pass warms the cache; the
    # second pass is the test (will it all still hit after
    # a full rotation?).
    for round in 1 2; do
        for i in $(seq -f "%03g" 1 64); do
            cat "$MP/file_${i}.dat" >/dev/null 2>&1
        done
    done

    # Phase 6: mixed read-write with temporal locality.
    # 60 cycles of (write, read) on 5 files. This is the
    # editor/build-tool pattern: open file, write a small
    # change, read it back. The write triggers
    # `invalidate_ino`; the read refills the cache. The
    # critical question: does the cache *survive* the
    # write-then-read pattern? FIFO evicts on insert
    # (the most-recent write), so the immediately-following
    # read is a guaranteed miss. TinyLFU's admission
    # filter sees the read right after the insert and
    # promotes the entry to the protected segment.
    for cycle in $(seq 1 60); do
        local idx=$((cycle % 5 + 1))  # round-robin over 5 files
        local file="$MP/file_$(printf %03d $idx).dat"
        echo "edit_$cycle" >> "$file" 2>/dev/null
        cat "$file" >/dev/null 2>&1
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
        echo "  [$backend/$impl iter $iter] OK"
    else
        echo "  [$backend/$impl iter $iter] FAILED (sub-step)"
    fi
}

echo "=== mem_cache A/B benchmark ==="
echo "iterations: $ITERATIONS"
echo "binary:     $BIN"
echo "backends:   $BACKENDS"
echo

# Run the workload for each (backend, impl) combination, N
# iterations each. The `LOG_DIR` map is keyed by impl only
# (each backend reuses the same per-impl log dir; the
# raw-logs dump at the bottom prints one section per
# backend's last log line).
for backend in $BACKENDS; do
    for impl in dashmap moka; do
        echo "--- backend=$backend impl=$impl ---"
        for i in $(seq 1 "$ITERATIONS"); do
            run_one "$backend" "$impl" "$i"
        done
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

printf "%-10s %-8s %-5s %-8s %-8s %-12s %-9s %-10s %-9s %-9s %-10s\n" \
    "backend" "impl" "iter" "hits" "misses" "hit_rate_pct" "inserts" "evictions" "entries" "used_B" "cap_B"
printf "%-10s %-8s %-5s %-8s %-8s %-12s %-9s %-10s %-9s %-9s %-10s\n" \
    "-------" "----" "----" "----" "------" "------------" "--------" "---------" "-------" "------" "-----"

for backend in $BACKENDS; do
    for impl in dashmap moka; do
        for i in $(seq 1 "$ITERATIONS"); do
            log="${LOG_DIR[${backend}/${impl}]}/iter-${i}.log"
            if ! grep -q "mem_cache_stats" "$log" 2>/dev/null; then
                printf "%-10s %-8s %-5s %s\n" "$backend" "$impl" "$i" "(no stats line)"
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
            printf "%-10s %-8s %-5s %-8s %-8s %-12s %-9s %-10s %-9s %-9s %-10s\n" \
                "$backend" "$impl" "$i" "$hits" "$misses" "${hit_rate}%" "$inserts" "$evictions" "$entries" "$used" "$cap"
        done
    done
done

echo
echo "=== summary ==="

# Per-(backend, impl) mean of the last-tick hit_rate.
# Both impls should converge to ~100% under the Phase 2
# hot-re-read loop (the working set fits the cache). The
# interesting metric is `misses` — fewer misses under
# Phase 3 churn means the eviction policy kept the
# still-hot blocks.
for backend in $BACKENDS; do
    echo "--- backend=$backend ---"
    for impl in dashmap moka; do
        total_hits=0
        total_misses=0
        total_inserts=0
        total_evictions=0
        for i in $(seq 1 "$ITERATIONS"); do
            log="${LOG_DIR[${backend}/${impl}]}/iter-${i}.log"
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
            printf "  %-8s avg over %d iters: hits=%d misses=%d hit_rate=%d%% inserts=%d evictions=%d\n" \
                "$impl" "$ITERATIONS" "$total_hits" "$total_misses" "$rate" "$total_inserts" "$total_evictions"
        fi
    done
done

echo
echo "=== raw logs (last 3 stats lines per (backend, impl, iter)) ==="
for backend in $BACKENDS; do
    for impl in dashmap moka; do
        echo
        echo "--- $backend / $impl ---"
        for i in $(seq 1 "$ITERATIONS"); do
            echo "  iter $i:"
            grep "mem_cache_stats" "${LOG_DIR[${backend}/${impl}]}/iter-${i}.log" 2>/dev/null | tail -3 | sed 's/^/    /'
        done
    done
done
