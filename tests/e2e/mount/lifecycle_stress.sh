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
# Threshold of 32 was recalibrated after PR #616 introduced
# io::sync (src/io/sync.rs), an isolated multi_thread tokio
# runtime that hosts concurrent_delete + writeback workers.
#
# PR #616 was reviewed and merged without realizing that the
# new runtime adds a non-trivial fixed FD cost to the daemon
# snapshot — s3-lifecycle-stress (which calls `echo > probe.txt`,
# triggering the writeback path which initializes io::sync)
# started failing the FD-leak detector at threshold=20.
#
# Empirical post-#616 measurements (local 8-core + GHA
# ubuntu-latest 2-core give the same daemon post-mount peak):
#
#   memory backend, 30 iter: max=14 FDs (PASSED at threshold 20,
#       passes at 32 with headroom)
#   s3 backend, 3 iter:      max=20 FDs (legitimate baseline,
#       not a leak — reqwest keep-alive pool warm)
#
# Steady state per backend AFTER PR #616:
#   memory://  : ~16 FDs base
#                (3 stdio + 2 tokio eventpoll/eventfd on
#                crate::rt() + 1 /dev/fuse + 1-2 tokio sockets
#                + ~6 from io::sync: 1 shared epoll + 1 eventfd
#                on the multi_thread runtime + bridge std::thread +
#                wakeup pipe + internal notify)
#   s3://      : ~20 FDs base
#                (+ ~4 from reqwest keep-alive TCP to the S3
#                endpoint; pool_max_idle_per_host=16 in
#                http_client.rs means up to 16 sockets per host
#                COULD be warm, but empirical ~3-4 are warm in
#                a 30-iter test)
#   +4-8 transient: busy cache dir (.dirty sidecar recovery
#                opens files in quick succession)
#
# 32 = 20 (s3 base) + 8 (transient burst) + 4 (defensive headroom
# for reqwest pool expansion during the first WRITE path).
# Still tight enough to catch an unbounded regression: a per-iter
# FD that doesn't drop trends 32+ as iterations accumulate and
# the avgs/maxes diverge.
#
# Why not "lazy init io::sync on first delete/write/prefetch" to
# avoid the cost entirely? Because the lifecycle_stress test
# itself invokes writes (`echo > probe.txt`), which triggers
# writeback → io::sync init. The init happens during the first
# iteration regardless of laziness. The only way to avoid the
# cost in this test is the threshold recalibration above.
# Read-only production mounts (--read-only flag, no writes) DO
# benefit from a future lazy-init follow-up — see plan note.
#
# RULE: do not silently raise this further. If a future PR
# legitimately needs more FDs, the right answer is to MOVE the
# new FDs to a separate process / subprocess / off-thread runtime
# so the daemon's post-unmount snapshot stays bounded. Raising
# the threshold indefinitely defeats the detector.
PEAK_FD_THRESHOLD=32

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
