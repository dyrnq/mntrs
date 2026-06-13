#!/usr/bin/env bash
#
# Mount/unmount lifecycle stress test with FD leak detection.
#
# Verifies that repeated mount/write/unmount cycles do not leak
# processes, file descriptors, or FUSE mounts. Catches regressions
# in session.join(), FUSE_SESSION cleanup, and signal handling.
#
# Usage:
#   ./tests/e2e/mount/lifecycle_stress.sh [ITERATIONS] [BINARY] [MOUNTPOINT]
#
# Defaults: 30 iterations, target/release/mntrs, /tmp/mntrs-lifecycle
#
# Exit code: 0 on success, 1 on any failure.

set -u

ITERATIONS="${1:-30}"
BIN="${2:-target/release/mntrs}"
MP="${3:-/tmp/mntrs-lifecycle}"

# Support S3 backend via env vars
S3_URL="${LIFECYCLE_S3_URL:-}"
S3_OPTS="${LIFECYCLE_S3_OPTS:-}"

if [ ! -x "$BIN" ]; then
    echo "=== Build mntrs ==="
    cargo build --release -p mntrs 2>&1 | tail -3
fi
BIN="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")"

echo "=== lifecycle_stress ==="
echo "iterations: $ITERATIONS"
echo "binary:     $BIN"
echo "mountpoint: $MP"
echo "backend:    ${S3_URL:-memory://}"
echo

PASS=0
FAIL=0
LEAK=0

# Baseline FD count
BASE_FD=$(cat /proc/sys/fs/file-nr | awk '{print $1}')

cleanup() {
    fusermount3 -u "$MP" 2>/dev/null || fusermount -u "$MP" 2>/dev/null || true
    # Kill any leaked processes
    for pid in $(pgrep -u "$UID" -f "mntrs mount.*${MP}" 2>/dev/null || true); do
        kill -9 "$pid" 2>/dev/null || true
    done
    rm -rf "$MP"
}

for i in $(seq 1 "$ITERATIONS"); do
    cleanup
    mkdir -p "$MP"

    # Mount
    if [ -n "$S3_URL" ]; then
        "$BIN" mount "$S3_URL" "$MP" $S3_OPTS > /dev/null 2>&1 &
    else
        "$BIN" mount "memory:///" "$MP" > /dev/null 2>&1 &
    fi
    MPID=$!

    # Wait for mount
    READY=0
    for w in $(seq 1 30); do
        mount | grep -q " $MP " && READY=1 && break
        sleep 0.2
    done
    if [ $READY -eq 0 ]; then
        echo "✗ iter $i: mount not ready"
        FAIL=$((FAIL + 1))
        kill -9 $MPID 2>/dev/null
        continue
    fi

    # Write + read
    echo "lifecycle-$i" > "$MP/probe.txt" 2>/dev/null
    GOT=$(cat "$MP/probe.txt" 2>/dev/null)

    # Unmount
    fusermount3 -u "$MP" 2>/dev/null || fusermount -u "$MP" 2>/dev/null || true

    # Wait for process exit (up to 5s)
    for w in $(seq 1 50); do
        kill -0 $MPID 2>/dev/null || break
        sleep 0.1
    done

    # Check process leaked
    if kill -0 $MPID 2>/dev/null; then
        echo "✗ iter $i: process $MPID leaked!"
        kill -9 $MPID 2>/dev/null
        LEAK=$((LEAK + 1))
        FAIL=$((FAIL + 1))
        continue
    fi

    # Check mount cleaned
    if mount | grep -q " $MP "; then
        echo "✗ iter $i: mount persists after unmount"
        FAIL=$((FAIL + 1))
        continue
    fi

    PASS=$((PASS + 1))
    if (( i % 10 == 0 )); then
        CUR_FD=$(cat /proc/sys/fs/file-nr | awk '{print $1}')
        echo "  ... $i/$ITERATIONS  pass=$PASS fail=$FAIL fd_delta=$((CUR_FD - BASE_FD))"
    fi
done

# Final FD check
sleep 1
FINAL_FD=$(cat /proc/sys/fs/file-nr | awk '{print $1}')
FD_DELTA=$((FINAL_FD - BASE_FD))

echo
echo "=== Results ==="
echo "  Mount/write/unmount: $PASS/$ITERATIONS passed"
echo "  Process leaks:       $LEAK"
echo "  FD delta:            $FD_DELTA"

cleanup

if [ $FAIL -eq 0 ] && [ $FD_DELTA -lt 50 ]; then
    echo "  ✅ lifecycle stress PASSED"
    exit 0
else
    echo "  ❌ lifecycle stress FAILED"
    exit 1
fi
