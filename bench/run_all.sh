#!/bin/bash
# mntrs benchmark — compares against rclone mount on same MinIO backend
set -e

# Config (from /root/.config/rclone/rclone.conf minio-1)
ENDPOINT="http://192.168.6.130:19000"
ACCESS_KEY="u5SybesIDVX9b6Pk"
SECRET_KEY="lOpH1v7kdM6H8NkPu1H2R6gLc9jcsmWM"
BUCKET="maven-repo"
REGION="us-east-1"
MNTRS_BIN="${MNTRS_BIN:-./target/release/mntrs}"
MNTRS_MNT="/tmp/mntrs-bench"
RCLONE_MNT="/opt/maven-repo"

cleanup() {
    fusermount3 -u "$MNTRS_MNT" 2>/dev/null || true
}
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

bench() {
    local name="$1"; shift
    local mntrs_time rclone_time
    mntrs_time=$({ time "$@" "$MNTRS_MNT" >/dev/null 2>&1; } 2>&1 | grep real | awk '{print $2}')
    rclone_time=$({ time "$@" "$RCLONE_MNT" >/dev/null 2>&1; } 2>&1 | grep real | awk '{print $2}')
    echo "$name|$mntrs_time|$rclone_time"
}

echo "test|mntrs|rclone"
echo "-----|------|------"

# 1. ls warm
ls "$MNTRS_MNT"/ >/dev/null 2>&1  # warmup
bench "ls_warm" ls

# 2. ls -la warm
bench "ls_la_warm" ls -la

# 3. find -maxdepth 2
bench "find_depth2" find -maxdepth 2

# 4. cat small file x100
F=$(ls "$MNTRS_MNT"/ 2>/dev/null | head -1)
F2=$(ls "$RCLONE_MNT"/ 2>/dev/null | head -1)
bench "cat_100x" sh -c "for i in \$(seq 1 100); do cat \"$MNTRS_MNT/$F\" >/dev/null 2>&1; done"

# Need special handling for cross-mount paths
mntrs_cat_time=$({ time for i in $(seq 1 100); do cat "$MNTRS_MNT/$F" >/dev/null 2>&1; done; } 2>&1 | grep real | awk '{print $2}')
rclone_cat_time=$({ time for i in $(seq 1 100); do cat "$RCLONE_MNT/$F2" >/dev/null 2>&1; done; } 2>&1 | grep real | awk '{print $2}')
echo "cat_100x|$mntrs_cat_time|$rclone_cat_time"

# 5. stat 100 files
mntrs_stat_time=$({ time for f in $(ls "$MNTRS_MNT"/ | head -100); do stat "$MNTRS_MNT/$f" >/dev/null 2>&1; done; } 2>&1 | grep real | awk '{print $2}')
rclone_stat_time=$({ time for f in $(ls "$RCLONE_MNT"/ | head -100); do stat "$RCLONE_MNT/$f" >/dev/null 2>&1; done; } 2>&1 | grep real | awk '{print $2}')
echo "stat_100|$mntrs_stat_time|$rclone_stat_time"

echo ""
echo "=== done ==="
