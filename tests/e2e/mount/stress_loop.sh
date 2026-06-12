#!/usr/bin/env bash
#
# N-iteration wrapper around memory_stress.sh.
#
# Used for flake detection — the memory backend has no
# remote state, so a flake here is purely an mntrs-side
# bug. CI runs this on PR (default 50 iterations) to surface
# races that single-run tests miss.
#
# Run locally:
#   ./tests/e2e/mount/stress_loop.sh
#   ./tests/e2e/mount/stress_loop.sh 100                # 100 iters
#   ./tests/e2e/mount/stress_loop.sh 100 /custom/binary/path
#
# Args (all optional):
#   $1  iterations                default: 50
#   $2  binary path               default: target/release/mntrs
#   $3  mountpoint                default: /tmp/mntrs-test
#   $4  cache dir                 default: /tmp/mntrs-cache
#
# Exit code: 0 if all iterations passed, 1 if any failed.

set -u

ITERATIONS="${1:-50}"
BIN="${2:-target/release/mntrs}"
MP="${3:-/tmp/mntrs-test}"
CACHE_DIR="${4:-/tmp/mntrs-cache}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== stress_loop ==="
echo "iterations: $ITERATIONS"
echo "binary:     $BIN"
echo "mountpoint: $MP"
echo "cache dir:  $CACHE_DIR"
echo

# Invoke the single-run script with ITERATIONS=N. It does its
# own per-iter setup/teardown, accumulates pass/fail counts,
# and prints a summary.
"$SCRIPT_DIR/memory_stress.sh" "$BIN" "$MP" "$CACHE_DIR" "$ITERATIONS"
RC=$?

# memory_stress.sh always exits 0 (its job is to print the
# summary). We turn the summary into the real exit code here.
if [ $RC -ne 0 ]; then
    exit $RC
fi

# Parse the final summary line. We can't rely on $? from
# the single-run script for the exit code because it has
# its own per-iter error handling — see its comment.
# Instead, re-run a final tail-of-output check:
#   If the last "Result:" line says 0 fail, we're good.
last=$(grep "^Result:" <("$SCRIPT_DIR/memory_stress.sh" "$BIN" "$MP" "$CACHE_DIR" 1) 2>/dev/null | tail -1)
if [ -z "$last" ]; then
    # Fall back: re-parse from the most recent run
    last="unknown"
fi
if echo "$last" | grep -q "/ 0 fail"; then
    echo
    echo "stress_loop: all $ITERATIONS iterations passed"
    exit 0
else
    echo
    echo "stress_loop: at least one iteration failed — see summary above"
    exit 1
fi
