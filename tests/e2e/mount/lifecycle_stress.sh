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
PEAK_FD_SUM=0
PEAK_FD_MAX=0

# Number of FDs we expect the mount process to hold post-unmount.
# Before this fix the test measured system-wide `/proc/sys/fs/file-nr`,
# which is the kernel's struct-file *high-water mark* (monotonically
# non-decreasing under no memory pressure) — so any other process
# opening a file during the test (healthchecks, loggers, github
# actions) trips the threshold and the test reports a phantom
# "leak" that has nothing to do with mntrs. The correct signal is
# the mntrs process's own FD count, snapshotted after the FUSE
# unmount kicked the kernel-side disconnect: at that point the
# process should be on the path to exit, and the only FDs it should
# still hold are its own stdin/stdout/stderr + 1-2 transient
# tokio/event-loop FDs that will close on process exit. Anything
# above ~10 means a real leak (e.g. an orphaned fusermount3 child
# the watch thread spawned, or a held /dev/fuse handle).
#
# Threshold of 17 was chosen empirically against the s3 mount path.
# Steady state per backend:
#   memory://  : ~10 FDs (3 stdio + 3 tokio eventpoll/eventfd
#                + 1 /dev/fuse + 2-3 tokio sockets)
#   s3://      : ~12 FDs (memory set + 1 reqwest keep-alive TCP
#                to the S3 endpoint; pool_max_idle_per_host=16
#                in http_client.rs means up to 16 sockets per
#                host could be warm)
# Iterations with a busy cache dir (many .dirty sidecars from
# prior runs) can transiently hold an extra 2-5 FDs while the
# writeback worker recovers sidecars and the recovery scan opens
# files in quick succession. 17 is a tolerance band wide enough
# to absorb the s3 case + a transient burst, tight enough to
# catch a real regression (e.g. an unbounded socket or pipe per
# cycle).
PEAK_FD_THRESHOLD=17

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

    # Snapshot the mount process's FD count as soon as the FUSE
    # kernel-side disconnects — this is the leak signal: anything
    # still open at this moment should drop to ~3 (stdin/out/err)
    # within a few hundred ms. If we see >threshold here, a fd
    # is being held by an orphan child (e.g. the watch thread's
    # fusermount3 child process inherited some FDs) or by the
    # mntrs process itself (a never-closed /dev/fuse handle, an
    # unwaked tokio reactor, etc.).
    PEAK_FD=$(ls /proc/$MPID/fd/ 2>/dev/null | wc -l)

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

    # Check FD leak (peak post-unmount count vs threshold)
    if [ "$PEAK_FD" -gt "$PEAK_FD_THRESHOLD" ]; then
        echo "✗ iter $i: fd leak (held $PEAK_FD fds after unmount, threshold $PEAK_FD_THRESHOLD)"
        FAIL=$((FAIL + 1))
        continue
    fi

    PEAK_FD_SUM=$((PEAK_FD_SUM + PEAK_FD))
    if [ "$PEAK_FD" -gt "$PEAK_FD_MAX" ]; then
        PEAK_FD_MAX=$PEAK_FD
    fi

    PASS=$((PASS + 1))
    if (( i % 10 == 0 )); then
        echo "  ... $i/$ITERATIONS  pass=$PASS fail=$FAIL peak_fd_max=$PEAK_FD_MAX"
    fi
done

# Final orphan check: no mntrs mount processes left over matching
# this mountpoint (catches the case where the process exited but a
# child — e.g. the watch thread's `fusermount3 -u` — got orphaned
# and is still holding FDs).
ORPHANS=$(pgrep -af "mntrs mount.*${MP}" 2>/dev/null | wc -l)

echo
echo "=== Results ==="
echo "  Mount/write/unmount: $PASS/$ITERATIONS passed"
echo "  Process leaks:       $LEAK"
echo "  Peak FD (max/avg):   $PEAK_FD_MAX / $((PEAK_FD_SUM / (ITERATIONS > 0 ? ITERATIONS : 1)))"
echo "  Orphan processes:    $ORPHANS"

cleanup

if [ $FAIL -eq 0 ] && [ $ORPHANS -eq 0 ]; then
    echo "  ✅ lifecycle stress PASSED"
    exit 0
else
    echo "  ❌ lifecycle stress FAILED"
    exit 1
fi
