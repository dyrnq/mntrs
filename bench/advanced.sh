#!/bin/bash
# mntrs advanced benchmarks — large files, random read, concurrent, write, large dirs
set -e
ENDPOINT="${ENDPOINT:-http://192.168.6.130:19000}"
ACCESS_KEY="${ACCESS_KEY:-u5SybesIDVX9b6Pk}"
SECRET_KEY="${SECRET_KEY:-lOpH1v7kdM6H8NkPu1H2R6gLc9jcsmWM}"
BUCKET="${BUCKET:-maven-repo}"
REGION="${REGION:-us-east-1}"
MNTRS_BIN="${MNTRS_BIN:-./target/release/mntrs}"
MNTRS_MNT="${MNTRS_MNT:-/tmp/mntrs-adv}"
RCLONE_MNT="${RCLONE_MNT:-/opt/maven-repo}"

cleanup() { fusermount3 -u "$MNTRS_MNT" 2>/dev/null || true; }
trap cleanup EXIT
mkdir -p "$MNTRS_MNT"

# Find a large file (>100KB) or use the largest available
LARGE_FILE=$(ls -S "$RCLONE_MNT"/* 2>/dev/null | head -1)
if [ -z "$LARGE_FILE" ]; then
    echo "No files found for benchmark, creating test data"
    dd if=/dev/urandom of=/tmp/bench_1mb.bin bs=1M count=1 2>/dev/null
    # Upload would need write access — skip for read-only bench
    LARGE_FILE="/tmp/bench_1mb.bin"
fi

echo "=== mntrs advanced benchmark $(date -Iseconds) ==="
echo "Large file: $LARGE_FILE ($(stat -c%s "$LARGE_FILE" 2>/dev/null || echo '?'))"
echo ""

# Start mntrs
timeout 30 "$MNTRS_BIN" mount "s3://$BUCKET" "$MNTRS_MNT" \
    --opt "endpoint=$ENDPOINT" --opt "access-key=$ACCESS_KEY" \
    --opt "secret-key=$SECRET_KEY" --opt "region=$REGION" \
    --read-only --use-server-modtime --vfs-read-chunk-streams 4 2>/dev/null &
sleep 5

echo "=== 1. Large file sequential read ==="
F=$(ls "$MNTRS_MNT"/ | head -1)
[ -n "$F" ] && {
    echo -n "  mntrs dd 64KB: "
    time dd if="$MNTRS_MNT/$F" bs=65536 count=1 of=/dev/null 2>&1 | grep -v records
    echo -n "  rclone dd 64KB:"
    time dd if="$RCLONE_MNT/$F" bs=65536 count=1 of=/dev/null 2>&1 | grep -v records
}

echo ""
echo "=== 2. Random seek read ==="
[ -n "$F" ] && {
    echo -n "  mntrs random (10 seeks): "
    SIZE=$(stat -c%s "$RCLONE_MNT/$F" 2>/dev/null || echo 4096)
    time for i in 1 2 3 4 5 6 7 8 9 10; do
        OFF=$((RANDOM % SIZE))
        dd if="$MNTRS_MNT/$F" bs=1 count=1 skip=$OFF of=/dev/null 2>/dev/null
    done
    echo -n "  rclone random (10 seeks):"
    time for i in 1 2 3 4 5 6 7 8 9 10; do
        OFF=$((RANDOM % SIZE))
        dd if="$RCLONE_MNT/$F" bs=1 count=1 skip=$OFF of=/dev/null 2>/dev/null
    done
}

echo ""
echo "=== 3. Directory listing (100 entries) ==="
echo -n "  mntrs ls 100: "
time ls "$MNTRS_MNT"/ | head -100 >/dev/null
echo -n "  rclone ls 100:"
time ls "$RCLONE_MNT"/ | head -100 >/dev/null

echo ""
echo "=== 4. Concurrent read (4 streams via dd) ==="
[ -n "$F" ] && {
    echo -n "  mntrs 4x concurrent: "
    time (
        for i in 1 2 3 4; do
            dd if="$MNTRS_MNT/$F" bs=4096 count=10 of=/dev/null 2>/dev/null &
        done
        wait
    )
    echo -n "  rclone 4x concurrent:"
    time (
        for i in 1 2 3 4; do
            dd if="$RCLONE_MNT/$F" bs=4096 count=10 of=/dev/null 2>/dev/null &
        done
        wait
    )
}

echo ""
echo "=== 5. memory:// backend (zero-network) ==="
MNTRS_MEM="/tmp/mntrs-mem"
mkdir -p "$MNTRS_MEM"
timeout 10 "$MNTRS_BIN" mount "memory://" "$MNTRS_MEM" --read-only 2>/dev/null &
sleep 3
echo -n "  memory stat: "
time stat "$MNTRS_MEM" >/dev/null 2>&1 || echo "N/A"
fusermount3 -u "$MNTRS_MEM" 2>/dev/null

echo ""
echo "=== done ==="
