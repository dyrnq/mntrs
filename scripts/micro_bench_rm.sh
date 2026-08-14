#!/usr/bin/env bash
# Micro-bench for the S3 deleter paths.
#
# Runs a focused subset of `bench/unlink_ab.sh` against a local MinIO:
#   - rm single file  (10 iterations)
#   - rm -rf 10 files
#   - rm -rf 100 files
#   - rm -rf 500 files
#
# Compares the two surviving modes by remounting between runs:
#   1. unbatched  : MNTRS_UNLINK_BATCH=0 (strict op.delete path)
#   2. concurrent : MNTRS_UNLINK_BATCH=1 (N concurrent single-DELETE,
#                                        default 8 workers per
#                                        MNTRS_DELETE_WORKER_COUNT)
#
# Issue #572 follow-up: the historical "batched-current" /
# "batched+fast-flush" comparison modes (which relied on
# MNTRS_BATCH_SIZE / MNTRS_BATCH_FLUSH_DELAY_MS /
# MNTRS_BATCH_FAST_FLUSH_THRESHOLD) were retired in issue #568
# stage 6 and #570; those knobs no longer exist. Only the two
# modes above remain.
#
# Pre-reqs:
#   - MinIO listening on http://127.0.0.1:9000 (minioadmin/minioadmin)
#   - buckets "bench-bucket" and "bench-bucket-batch" already created
#   - mntrs debug binary at ./target/debug/mntrs
#
# Usage: scripts/micro_bench_rm.sh
# Output: prints a markdown table matching the historical format.

set -euo pipefail

MNTRS_BIN="${MNTRS_BIN:-./target/debug/mntrs}"
BUCKET_UNBATCHED=bench-bucket
BUCKET_BATCHED=bench-bucket-batch
ENDPOINT="http://127.0.0.1:9000"
ACCESS_KEY=minioadmin
SECRET_KEY=minioadmin
REGION=us-east-1

MNTRS_MNT=/tmp/micro-bench-unbatched
MNTRS_BATCH_MNT=/tmp/micro-bench-batched

ITERATIONS_SMALL=10      # for rm single file
CACHE_DIR=/tmp/micro-bench-cache

# Pre-flight checks
[[ -x $MNTRS_BIN ]] || { echo "mntrs binary not found: $MNTRS_BIN"; exit 1; }
# MinIO returns 403 on unauthenticated GET /, which `curl -sf` flags as
# failure. Probe the health endpoint instead, which always 200s.
curl -sf "${ENDPOINT}/minio/health/ready" -o /dev/null \
    || { echo "MinIO not reachable at $ENDPOINT"; exit 1; }

mkdir -p "$MNTRS_MNT" "$MNTRS_BATCH_MNT" "$CACHE_DIR"
rm -rf "${CACHE_DIR:?}/"*

# Function definitions must come before they're invoked. The
# pre-flight `cleanup_mounts` call below catches leftover
# mounts from a previous aborted run; without it the next
# mountpoint fails with "Resource busy" because macFUSE holds
# the dirent open.

cleanup_mounts() {
    # macOS doesn't ship `mountpoint`; detect via `mount | grep`
    # and fall back to `diskutil unmount` if `umount` rejects.
    for m in "$MNTRS_MNT" "$MNTRS_BATCH_MNT"; do
        if mount | grep -q " on $m "; then
            umount "$m" 2>/dev/null || diskutil unmount force "$m" 2>/dev/null || true
        fi
        # /private/tmp is macOS's resolved form of /tmp — clean both
        if mount | grep -q " on /private$m "; then
            umount "/private$m" 2>/dev/null || diskutil unmount force "/private$m" 2>/dev/null || true
        fi
        rm -rf "$m"
    done
}

cleanup_mounts

# --- mode runner -----------------------------------------------------------
#
# Each mode:
#   1. umount any leftover mntrs mount
#   2. spawn mntrs with the right env vars
#   3. populate the bucket with files
#   4. run the rm workloads, time each
#   5. unmount, kill daemon
#
# Output: append "<label> <test> <seconds>" lines to $RESULTS.

RESULTS=$(mktemp)
CURRENT_WORKDIR=""
CURRENT_WORKDIR_SEED=""
trap 'cleanup_mounts; rm -rf "${CURRENT_WORKDIR:-}" "${CURRENT_WORKDIR_SEED:-}" 2>/dev/null; rm -f "$RESULTS"' EXIT

mount_unbatched() {
    mkdir -p "$MNTRS_MNT"
    # NOTE: deliberately NO --vfs-cache-mode=writes. With write
    # cache, writes are deferred (5s write-back) and rm -rf of
    # files that haven't been written to S3 yet is a local
    # cache eviction (~3ms) — measures local perf, not S3.
    # Without write cache, every rm -rf triggers a real backend
    # delete which is what we want to A/B.
    MNTRS_UNLINK_BATCH=0 \
    RUST_LOG=info,mntrs::concurrent_delete=info \
    "$MNTRS_BIN" mount "s3://$BUCKET_UNBATCHED" "$MNTRS_MNT" \
        --opt "endpoint=$ENDPOINT" --opt "access-key=$ACCESS_KEY" \
        --opt "secret-key=$SECRET_KEY" --opt "region=$REGION" \
        --cache-dir "$CACHE_DIR/unbatched" \
        --daemon --daemon-wait --daemon-timeout=15
}

mount_batched() {
    local extra_env="$1"
    mkdir -p "$MNTRS_BATCH_MNT"
    # shellcheck disable=SC2086  # extra_env is a space-separated KEY=VAL list passed straight through
    env $extra_env \
    MNTRS_UNLINK_BATCH=1 \
    MNTRS_DAEMON_LOG=/tmp/micro-bench-batched.log \
    RUST_LOG=info,mntrs::concurrent_delete=info \
    "$MNTRS_BIN" mount "s3://$BUCKET_BATCHED" "$MNTRS_BATCH_MNT" \
        --opt "endpoint=$ENDPOINT" --opt "access-key=$ACCESS_KEY" \
        --opt "secret-key=$SECRET_KEY" --opt "region=$REGION" \
        --cache-dir "$CACHE_DIR/batched" \
        --daemon --daemon-wait --daemon-timeout=15
}

run_workloads() {
    local label="$1"  # "unbatched" | "batched-current" | "batched-fast"
    local mp="$2"
    local workdir
    workdir=$(mktemp -d)
    # Cleanup deferred to script-level EXIT trap (it reads the
    # current $workdir via the closure below).
    CURRENT_WORKDIR=$workdir

    # rm single file: pick a pre-populated file, rm it, re-populate,
    # repeat ITERATIONS_SMALL times. Each iteration touches a
    # distinct filename so the dirent cache doesn't return stale
    # ENOENT for the same path on re-list.
    local single_total=0
    for ((i = 0; i < ITERATIONS_SMALL; i++)); do
        local f="$mp/rmtest_single_$i.txt"
        # Re-populate from S3 via the mounted bucket. Use the
        # existing mount to do the write so the bucket contents
        # match what's expected (no extra aws-cli calls).
        echo "s$i" > "$f"
        local t e
        t=$(date +%s.%N)
        rm -f "$f"
        e=$(date +%s.%N)
        single_total=$(awk -v s="$single_total" -v b="$t" -v a="$e" 'BEGIN{print s + (a - b)}')
    done
    local single_avg
    single_avg=$(awk -v t="$single_total" -v n="$ITERATIONS_SMALL" 'BEGIN{print t / n}')
    printf '%s | rm single file | %.3f\n' "$label" "$single_avg" >> "$RESULTS"

    # rm -rf 10 / 100 / 500 files. Single round each — the
    # numbers are large enough (10–500 files × S3 round trip)
    # that one sample is meaningful. Re-populating between
    # rounds via `aws s3 sync` would add minutes.
    run_rm_rf() {
        local subdir="$1"  # rmtest_10 / rmtest_100 / rmtest_500
        local target="$mp/$subdir"
        local t e
        t=$(date +%s.%N)
        rm -rf "$target"
        e=$(date +%s.%N)
        local dur
        dur=$(awk -v b="$t" -v a="$e" 'BEGIN{print a - b}')
        printf '%s | rm -rf %s | %.3f\n' "$label" "$subdir" "$dur" >> "$RESULTS"
    }
    run_rm_rf rmtest_10
    run_rm_rf rmtest_100
    run_rm_rf rmtest_500
}

# --- populate buckets ------------------------------------------------------
#
# Strategy: build a /tmp/seed directory tree with all the test
# files locally, then `aws s3 sync` (which uses multiple
# concurrent connections in parallel) once per bucket.
# Per-file `aws s3 cp` would take 5+ minutes because each
# invocation pays Python startup; sync does it in ~5 seconds.

populate_buckets() {
    echo "==[0/3 populate buckets via local seed + s3 sync]=="
    local seed
    seed=$(mktemp -d)
    CURRENT_WORKDIR_SEED=$seed
    export AWS_ACCESS_KEY_ID=$ACCESS_KEY AWS_SECRET_ACCESS_KEY=$SECRET_KEY AWS_DEFAULT_REGION=$REGION
    for n in 10 100 500; do
        mkdir -p "$seed/rmtest_$n"
        case $n in
            10)  range=$(seq 1 10)  ;;
            100) range=$(seq 1 100) ;;
            500) range=$(seq 1 500) ;;
        esac
        for i in $range; do
            printf 'f%d' "$i" > "$seed/rmtest_$n/f_$(printf '%04d' "$i").txt"
        done
    done
    for bucket in "$BUCKET_UNBATCHED" "$BUCKET_BATCHED"; do
        # Drop + recreate bucket for a clean slate.
        aws --endpoint-url "$ENDPOINT" s3 rm "s3://$bucket" --recursive --quiet 2>/dev/null || true
        aws --endpoint-url "$ENDPOINT" s3 sync "$seed/" "s3://$bucket/" --quiet --no-progress
    done
    rm -rf "$seed"
    CURRENT_WORKDIR_SEED=""
    echo "populate done"
}

populate_buckets

# --- run all three modes ---------------------------------------------------
#
# Each mode mounts a different bucket (unbatched vs batched),
# so the underlying state is independent. But within one
# bucket, `rm -rf` empties the test dirs and the next mode
# that mounts the same bucket sees empty dirs (≈0ms). To keep
# numbers meaningful we re-populate between modes.

repopulate_for_mode() {
    local bucket="$1"
    local seed
    seed=$(mktemp -d)
    CURRENT_WORKDIR_SEED=$seed
    for n in 10 100 500; do
        mkdir -p "$seed/rmtest_$n"
        case $n in
            10)  range=$(seq 1 10)  ;;
            100) range=$(seq 1 100) ;;
            500) range=$(seq 1 500) ;;
        esac
        for i in $range; do
            printf 'f%d' "$i" > "$seed/rmtest_$n/f_$(printf '%04d' "$i").txt"
        done
    done
    export AWS_ACCESS_KEY_ID=$ACCESS_KEY AWS_SECRET_ACCESS_KEY=$SECRET_KEY AWS_DEFAULT_REGION=$REGION
    aws --endpoint-url "$ENDPOINT" s3 sync "$seed/" "s3://$bucket/" --quiet --no-progress --delete
    rm -rf "$seed"
    CURRENT_WORKDIR_SEED=""
}

cleanup_mounts
echo "==[1/2 unbatched]=="
repopulate_for_mode "$BUCKET_UNBATCHED"
mount_unbatched
run_workloads unbatched "$MNTRS_MNT"
cleanup_mounts

echo "==[2/2 concurrent (N=8 default)]=="
repopulate_for_mode "$BUCKET_BATCHED"
mount_batched ""
run_workloads concurrent "$MNTRS_BATCH_MNT"
cleanup_mounts

# --- render ----------------------------------------------------------------

echo
echo "## Micro-bench results (local MinIO, debug build)"
echo
printf '%-20s | %-22s | %8s | %14s | %14s\n' \
    "mode" "test" "sec" "vs unbatched" "vs batched-current"
echo "---------------------+------------------------+----------+----------------+----------------"

awk -F'|' '
    {
        gsub(/^ +| +$/, "", $1); gsub(/^ +| +$/, "", $2); gsub(/^ +| +$/, "", $3)
        key = $2
        sec[key ":" $1] = $3 + 0
    }
    END {
        n = split("rm single file|rm -rf rmtest_10|rm -rf rmtest_100|rm -rf rmtest_500", tests, "|")
        for (i = 1; i <= n; i++) {
            t = tests[i]
            u = sec[t ":unbatched"]
            bc = sec[t ":batched-current"]
            bf = sec[t ":batched-fast"]
            vbu = (u > 0) ? sprintf("%6.2fx", bf / u) : "n/a"
            vbc = (bc > 0) ? sprintf("%6.2fx", bf / bc) : "n/a"
            printf "%-20s | %-22s | %8.3f | %14s | %14s\n", "batched+fast-flush", t, bf, vbu, vbc
            vbu = (u > 0) ? sprintf("%6.2fx", bc / u) : "n/a"
            printf "%-20s | %-22s | %8.3f | %14s | %14s\n", "batched-current", t, bc, vbu, "1.00x"
            printf "%-20s | %-22s | %8.3f | %14s | %14s\n", "unbatched", t, u, "1.00x", "n/a"
            print "---------------------+------------------------+----------+----------------+----------------"
        }
    }
' "$RESULTS"