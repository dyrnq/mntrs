#!/bin/bash
# Plan #64 stage A: focused A/B micro-bench for rm workloads.
# Runs only the rm/rmdir/rm-rf tests against local MinIO, three
# impls: mntrs-unbatched / mntrs-batched / rclone. Outputs CSV
# in render_table.py format and a summary for the decision.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

ENDPOINT="${ENDPOINT:-http://localhost:9000}"
AK="${AK:-minioadmin}"
SK="${SK:-minioadmin}"
REGION="${REGION:-us-east-1}"
MNTRS_BIN="${MNTRS_BIN:-./target/release/mntrs}"
RCLONE_BIN="${RCLONE_BIN:-/tmp/rclone}"

# Three isolated buckets so deleting through one impl doesn't
# race another's readdir caching.
BUCKET_BASE="${BUCKET_BASE:-stagea-$$-$(date +%s)}"
BUCKET_UNB="$BUCKET_BASE-unbatched"
BUCKET_BAT="$BUCKET_BASE-batched"
BUCKET_RCL="$BUCKET_BASE-rclone"

MNTRS_MNT="/tmp/stagea-unbatched"
MNTRS_BAT_MNT="/tmp/stagea-batched"
RCLONE_MNT="/tmp/stagea-rclone"

RESULT_TMP="$(mktemp /tmp/stagea-results-XXXXXX)"
DAEMON_UNB="/tmp/stagea-daemon-unbatched.log"
DAEMON_BAT="/tmp/stagea-daemon-batched.log"

export AWS_ACCESS_KEY_ID="$AK"
export AWS_SECRET_ACCESS_KEY="$SK"

cleanup() {
    fusermount3 -u "$MNTRS_MNT" 2>/dev/null || true
    fusermount3 -u "$MNTRS_BAT_MNT" 2>/dev/null || true
    fusermount3 -u "$RCLONE_MNT" 2>/dev/null || true
    /tmp/mc rb --force "local/$BUCKET_UNB" 2>/dev/null || true
    /tmp/mc rb --force "local/$BUCKET_BAT" 2>/dev/null || true
    /tmp/mc rb --force "local/$BUCKET_RCL" 2>/dev/null || true
}
trap cleanup EXIT

bench() {
    local name="$1"; shift
    local tag="$1"; shift
    # Robust timing: TIMEFORMAT + subshell. `time` inside
    # `{ ...; }` is not honored as a reserved word under all
    # bash versions; a subshell is portable.
    local t
    TIMEFORMAT='%3R'
    t=$( { time "$@" >/dev/null 2>&1; } 2>&1 )
    if [ $? -ne 0 ]; then
        echo "FAIL|$name|$tag|RmRf" >> "$RESULT_TMP"
        return
    fi
    # TIMEFORMAT=%3R prints e.g. "0.337". Convert to mNs format
    # for render_table.py (which expects e.g. "0m0.337s").
    local secs="$t"
    local mins=$(awk -v s="$secs" 'BEGIN { printf "%d", s/60 }')
    local rems=$(awk -v s="$secs" 'BEGIN { printf "%.3f", s-'"$mins"'*60 }')
    echo "${mins}m${rems}s|$name|$tag|RmRf" >> "$RESULT_TMP"
}

mount_mntrs() {
    local mp="$1"; local bucket="$2"; local log="$3"
    rm -rf "$mp" && mkdir -p "$mp"
    # NO --vfs-cache-mode=writes for stage A. With write cache,
    # writes are deferred (5s write-back) and rm -rf of files
    # that haven't been written to S3 yet is a local cache
    # eviction (~3ms) — measures local perf, not S3. Without
    # write cache, every rm -rf triggers a real backend delete
    # which is what we want to A/B.
    # MNTRS_DAEMON_LOG: capture the daemon CHILD's stdout/stderr
    # (mount.rs:1489 redirects fds 1/2 inside the forked daemon).
    # Without this, only the parent's startup lines are captured.
    # RUST_LOG=info so batched_delete flush lines land in the log.
    rm -f "$log"
    MNTRS_DAEMON_LOG="$log" \
    RUST_LOG=info,mntrs::batched_delete=info \
    "$MNTRS_BIN" mount "s3://$bucket" "$mp" \
        --opt "endpoint=$ENDPOINT" --opt "access-key=$AK" \
        --opt "secret-key=$SK" --opt "region=$REGION" \
        --vfs-read-ahead=134217728 --async-read \
        --mem-cache-impl=dashmap \
        --daemon --daemon-wait --daemon-timeout=15 \
        >/dev/null 2>&1
    sleep 1
    for i in $(seq 1 20); do
        mountpoint -q "$mp" && return 0
        sleep 0.3
    done
    return 1
}

mount_rclone() {
    local mp="$1"; local bucket="$2"
    rm -rf "$mp" && mkdir -p "$mp"
    # No --vfs-cache-mode=writes — see comment in mount_mntrs.
    "$RCLONE_BIN" mount "bench:$bucket" "$mp" --daemon \
        --log-file /tmp/stagea-rclone.log --log-level INFO \
        >/dev/null 2>&1
    sleep 2
    mountpoint -q "$mp" && return 0
    for i in $(seq 1 20); do
        mountpoint -q "$mp" && return 0
        sleep 0.3
    done
    return 1
}

echo "=========================================="
echo " Plan #64 Stage A: rm A/B micro-bench"
echo " endpoint: $ENDPOINT"
echo " bucket:   $BUCKET_BASE"
echo "=========================================="

# Create the three buckets.
/tmp/mc mb --ignore-existing "local/$BUCKET_UNB" 2>&1 | head -1
/tmp/mc mb --ignore-existing "local/$BUCKET_BAT" 2>&1 | head -1
/tmp/mc mb --ignore-existing "local/$BUCKET_RCL" 2>&1 | head -1

# rclone config.
"$RCLONE_BIN" config create bench s3 provider Minio \
    access_key_id "$AK" secret_access_key "$SK" \
    endpoint "$ENDPOINT" region "$REGION" --no-check-certificate 2>/dev/null

# ---- Mount all three ----
echo "--- mounting three impls ---"
mount_mntrs "$MNTRS_MNT" "$BUCKET_UNB" "$DAEMON_UNB" || { echo "FAIL: unbatched mount"; exit 1; }
echo "  mntrs-unbatched: OK"
# Batched mount must inherit MNTRS_UNLINK_BATCH=1 in its env.
# Stage B: MNTRS_BATCH_FLUSH_DELAY_MS=10 lowers the deadline-driven
# flush window from 50ms to 10ms — eliminates the small-workload
# overhead discovered in stage A (single-file was 0.50x).
MNTRS_UNLINK_BATCH=1 MNTRS_BATCH_FLUSH_DELAY_MS=10 \
    mount_mntrs "$MNTRS_BAT_MNT" "$BUCKET_BAT" "$DAEMON_BAT" || { echo "FAIL: batched mount"; exit 1; }
echo "  mntrs-batched:   OK (MNTRS_UNLINK_BATCH=1, MNTRS_BATCH_FLUSH_DELAY_MS=10)"
mount_rclone "$RCLONE_MNT" "$BUCKET_RCL" || { echo "FAIL: rclone mount"; exit 1; }
echo "  rclone:          OK"

# ---- Populate rmtest data on each ----
_recreate() {
    local U="$MNTRS_MNT"; local B="$MNTRS_BAT_MNT"; local R="$RCLONE_MNT"
    case "$1" in
        small_10)
            for tag in "$U" "$B" "$R"; do mkdir -p "$tag/rmtest_small_10"; done
            for i in $(seq 1 10); do
                echo "s$i" > "$U/rmtest_small_10/f_$i.txt"
                echo "s$i" > "$B/rmtest_small_10/f_$i.txt"
                echo "s$i" > "$R/rmtest_small_10/f_$i.txt"
            done ;;
        shallow_100)
            for tag in "$U" "$B" "$R"; do mkdir -p "$tag/rmtest_shallow_100"; done
            for i in $(seq 1 100); do
                echo "sh100_$i" > "$U/rmtest_shallow_100/f_$(printf '%04d' "$i").txt"
                echo "sh100_$i" > "$B/rmtest_shallow_100/f_$(printf '%04d' "$i").txt"
                echo "sh100_$i" > "$R/rmtest_shallow_100/f_$(printf '%04d' "$i").txt"
            done ;;
        shallow_500)
            for tag in "$U" "$B" "$R"; do mkdir -p "$tag/rmtest_shallow_500"; done
            for i in $(seq 1 500); do
                echo "sh500_$i" > "$U/rmtest_shallow_500/f_$(printf '%04d' "$i").txt"
                echo "sh500_$i" > "$B/rmtest_shallow_500/f_$(printf '%04d' "$i").txt"
                echo "sh500_$i" > "$R/rmtest_shallow_500/f_$(printf '%04d' "$i").txt"
            done ;;
        deep)
            for tag in "$U" "$B" "$R"; do
                mkdir -p "$tag/rmtest_deep_3/a/b/c" "$tag/rmtest_deep_3/d/e/f"
                for sub in "$tag/rmtest_deep_3/a" "$tag/rmtest_deep_3/a/b" "$tag/rmtest_deep_3/a/b/c" \
                           "$tag/rmtest_deep_3/d" "$tag/rmtest_deep_3/d/e" "$tag/rmtest_deep_3/d/e/f"; do
                    for j in $(seq 1 10); do echo "deep_$j" > "$sub/f_$j.txt"; done
                done
            done ;;
        mixed)
            for tag in "$U" "$B" "$R"; do
                mkdir -p "$tag/rmtest_mixed"
                for i in $(seq 1 50); do dd if=/dev/urandom of="$tag/rmtest_mixed/s_$(printf '%04d' "$i").bin" bs=4K count=1 2>/dev/null; done
                dd if=/dev/urandom of="$tag/rmtest_mixed/large_1.bin" bs=1M count=10 2>/dev/null
                dd if=/dev/urandom of="$tag/rmtest_mixed/large_2.bin" bs=1M count=5 2>/dev/null
            done ;;
    esac
    sync
    sleep 1
}

echo "--- running A/B rm benchmarks ---"
_recreate small_10
bench "rm -rf 10 files"   "mntrs"          rm -rf "$MNTRS_MNT/rmtest_small_10"
bench "rm -rf 10 files"   "mntrs-batched"  rm -rf "$MNTRS_BAT_MNT/rmtest_small_10"
bench "rm -rf 10 files"   "rclone"         rm -rf "$RCLONE_MNT/rmtest_small_10"

_recreate shallow_100
bench "rm -rf 100 files"  "mntrs"          rm -rf "$MNTRS_MNT/rmtest_shallow_100"
bench "rm -rf 100 files"  "mntrs-batched"  rm -rf "$MNTRS_BAT_MNT/rmtest_shallow_100"
bench "rm -rf 100 files"  "rclone"         rm -rf "$RCLONE_MNT/rmtest_shallow_100"

_recreate shallow_500
echo "DEBUG: S3 counts before rm -rf 500:" >&2
echo "  unb: $(/tmp/mc ls local/$BUCKET_UNB/rmtest_shallow_500/ 2>/dev/null | wc -l)" >&2
echo "  bat: $(/tmp/mc ls local/$BUCKET_BAT/rmtest_shallow_500/ 2>/dev/null | wc -l)" >&2
echo "  rcl: $(/tmp/mc ls local/$BUCKET_RCL/rmtest_shallow_500/ 2>/dev/null | wc -l)" >&2
bench "rm -rf 500 files"  "mntrs"          rm -rf "$MNTRS_MNT/rmtest_shallow_500"
bench "rm -rf 500 files"  "mntrs-batched"  rm -rf "$MNTRS_BAT_MNT/rmtest_shallow_500"
bench "rm -rf 500 files"  "rclone"         rm -rf "$RCLONE_MNT/rmtest_shallow_500"

_recreate deep
bench "rm -rf deep tree (60)" "mntrs"          rm -rf "$MNTRS_MNT/rmtest_deep_3"
bench "rm -rf deep tree (60)" "mntrs-batched"  rm -rf "$MNTRS_BAT_MNT/rmtest_deep_3"
bench "rm -rf deep tree (60)" "rclone"         rm -rf "$RCLONE_MNT/rmtest_deep_3"

_recreate mixed
bench "rm -rf mixed (52 files 15M)" "mntrs"          rm -rf "$MNTRS_MNT/rmtest_mixed"
bench "rm -rf mixed (52 files 15M)" "mntrs-batched"  rm -rf "$MNTRS_BAT_MNT/rmtest_mixed"
bench "rm -rf mixed (52 files 15M)" "rclone"         rm -rf "$RCLONE_MNT/rmtest_mixed"

# Single-file case.
echo "single" > "$MNTRS_MNT/rmtest_single.txt"
echo "single" > "$MNTRS_BAT_MNT/rmtest_single.txt"
echo "single" > "$RCLONE_MNT/rmtest_single.txt"
sync; sleep 1
bench "rm single file" "mntrs"          rm -f "$MNTRS_MNT/rmtest_single.txt"
bench "rm single file" "mntrs-batched"  rm -f "$MNTRS_BAT_MNT/rmtest_single.txt"
bench "rm single file" "rclone"         rm -f "$RCLONE_MNT/rmtest_single.txt"

# Empty dir case.
mkdir -p "$MNTRS_MNT/rmtest_empty" "$MNTRS_BAT_MNT/rmtest_empty" "$RCLONE_MNT/rmtest_empty"
bench "rmdir empty" "mntrs"          rmdir "$MNTRS_MNT/rmtest_empty"
bench "rmdir empty" "mntrs-batched"  rmdir "$MNTRS_BAT_MNT/rmtest_empty"
bench "rmdir empty" "rclone"         rmdir "$RCLONE_MNT/rmtest_empty"

echo ""
echo "=========================================="
echo " Raw results (render_table.py input):"
echo "=========================================="
cat "$RESULT_TMP"
echo ""
echo "=========================================="
echo " Three-way summary:"
echo "=========================================="
python3 "$SCRIPT_DIR/render_table.py" "$RESULT_TMP"

echo ""
echo "=========================================="
echo " Batch metrics from $DAEMON_BAT:"
echo "=========================================="
if [ -f "$DAEMON_BAT" ]; then
    FLUSH_LINES=$(grep -c 'batched_delete: flush' "$DAEMON_BAT" 2>/dev/null | head -1)
    FLUSH_LINES=${FLUSH_LINES:-0}
    MULTI=$(grep 'batched_delete: flush' "$DAEMON_BAT" | grep -cE 'batch_size=([2-9]|[1-9][0-9]+)' 2>/dev/null | head -1)
    MULTI=${MULTI:-0}
    if [ "$FLUSH_LINES" -gt 0 ] 2>/dev/null; then
        MAX_BS=$(grep -oE 'batch_size=[0-9]+' "$DAEMON_BAT" | awk -F= '{print $2}' | sort -n | tail -1)
        MEAN_BS=$(grep -oE 'batch_size=[0-9]+' "$DAEMON_BAT" | awk -F= '{sum+=$2; n++} END {if(n>0) printf "%.1f", sum/n; else print 0}')
        SINGLE=$(grep -oE 'batch_size=[0-9]+' "$DAEMON_BAT" | awk -F= '$2==1 {n++} END {print n+0}')
        FAIL_K=$(grep -cE 'batched_delete.*failed=[1-9]' "$DAEMON_BAT" 2>/dev/null | head -1)
        FAIL_K=${FAIL_K:-0}
        RETRY=$(grep -cE 'batched_delete.*retries=[1-9]' "$DAEMON_BAT" 2>/dev/null | head -1)
        RETRY=${RETRY:-0}
        echo "  flushes:               $FLUSH_LINES"
        echo "  multi-key (>=2):       $MULTI"
        echo "  max batch_size:        $MAX_BS"
        echo "  mean batch_size:       $MEAN_BS"
        echo "  single-key batches:    $SINGLE"
        echo "  flushes w/ failed>=1:  $FAIL_K"
        echo "  flushes w/ retries>=1: $RETRY"
    else
        echo "  no flush log lines found — write-behind not engaging"
    fi
    echo ""
    echo "  --- sample flush lines ---"
    grep 'batched_delete: flush' "$DAEMON_BAT" | head -5
fi

echo ""
echo "Result file: $RESULT_TMP"