#!/bin/bash
# mntrs vs rclone benchmark
# Usage: MNTRS_BIN=./target/release/mntrs RCLONE_MNT=/opt/maven-repo ./bench/run_all.sh
set -e

ENDPOINT="${ENDPOINT:-http://localhost:9000}"
ACCESS_KEY="${ACCESS_KEY:-u5SybesIDVX9b6Pk}"
SECRET_KEY="${SECRET_KEY:-lOpH1v7kdM6H8NkPu1H2R6gLc9jcsmWM}"
BUCKET="${BUCKET:-maven-repo}"
REGION="${REGION:-us-east-1}"
MNTRS_BIN="${MNTRS_BIN:-./target/release/mntrs}"
MNTRS_MNT="${MNTRS_MNT:-/tmp/mntrs-bench}"
RCLONE_MNT="${RCLONE_MNT:-/opt/maven-repo}"

cleanup() { fusermount3 -u "$MNTRS_MNT" 2>/dev/null || true; }
trap cleanup EXIT
mkdir -p "$MNTRS_MNT"

echo "=== mntrs benchmark $(date -Iseconds) ==="
echo "  binary: $MNTRS_BIN"
echo "  MinIO:  $ENDPOINT/$BUCKET"
echo ""

# Start mntrs
timeout 20 "$MNTRS_BIN" mount "s3://$BUCKET" "$MNTRS_MNT" \
    --opt "endpoint=$ENDPOINT" \
    --opt "access-key=$ACCESS_KEY" \
    --opt "secret-key=$SECRET_KEY" \
    --opt "region=$REGION" \
    --read-only --use-server-modtime 2>/dev/null &
sleep 5

if ! mount | grep -q "$MNTRS_MNT"; then
    echo "FATAL: mntrs mount failed"
    exit 1
fi

# Check rclone mount
if ! mount | grep -q "$RCLONE_MNT"; then
    echo "FATAL: rclone mount not found at $RCLONE_MNT"
    exit 1
fi

# CSV header
printf "%-20s | %10s | %10s | %s\n" "test" "mntrs" "rclone" "winner"
printf "%-20s-+-%10s-+-%10s-+-%s\n" "--------------------" "----------" "----------" "------"

run_bench() {
    local name="$1"; shift
    # Run on mntrs mount
    local mntrs_t=$( { time "$@" "$MNTRS_MNT" >/dev/null 2>&1; } 2>&1 | grep real | awk '{print $2}')
    # Run on rclone mount
    local rclone_t=$( { time "$@" "$RCLONE_MNT" >/dev/null 2>&1; } 2>&1 | grep real | awk '{print $2}')
    
    # Determine winner
    local mntrs_s=$(echo "$mntrs_t" | sed 's/0m//;s/s//' | awk -F. '{print $1*1000 + $2}')
    local rclone_s=$(echo "$rclone_t" | sed 's/0m//;s/s//' | awk -F. '{print $1*1000 + $2}')
    local winner="-"
    if [ -n "$mntrs_s" ] && [ -n "$rclone_s" ]; then
        if [ "$mntrs_s" -lt "$rclone_s" ]; then
            winner="🏆 mntrs"
        elif [ "$rclone_s" -lt "$mntrs_s" ]; then
            winner="rclone"
        else
            winner="tie"
        fi
    fi
    printf "%-20s | %10s | %10s | %s\n" "$name" "$mntrs_t" "$rclone_t" "$winner"
}

# Warmup
ls "$MNTRS_MNT"/ >/dev/null 2>&1
ls "$RCLONE_MNT"/ >/dev/null 2>&1

run_bench "ls_warm" ls
run_bench "ls_la_warm" ls -la
run_bench "find_depth2" find -maxdepth 2

# cat x100 — both mounts point to same MinIO, files should match
F=$(ls "$MNTRS_MNT"/ 2>/dev/null | head -1)
if [ -n "$F" ]; then
    mntrs_cat=$( { time for i in $(seq 1 100); do cat "$MNTRS_MNT/$F" >/dev/null 2>&1; done; } 2>&1 | grep real | awk '{print $2}')
    rclone_cat=$( { time for i in $(seq 1 100); do cat "$RCLONE_MNT/$F" >/dev/null 2>&1; done; } 2>&1 | grep real | awk '{print $2}')
    printf "%-20s | %10s | %10s | %s\n" "cat_100x" "$mntrs_cat" "$rclone_cat" "see above"
fi

# stat 100
mntrs_stat=$( { time for f in $(ls "$MNTRS_MNT"/ | head -100); do stat "$MNTRS_MNT/$f" >/dev/null 2>&1; done; } 2>&1 | grep real | awk '{print $2}')
rclone_stat=$( { time for f in $(ls "$RCLONE_MNT"/ | head -100); do stat "$RCLONE_MNT/$f" >/dev/null 2>&1; done; } 2>&1 | grep real | awk '{print $2}')
printf "%-20s | %10s | %10s | %s\n" "stat_100" "$mntrs_stat" "$rclone_stat" "see above"

echo ""
echo "=== done ==="
