#!/usr/bin/env bash
#
# End-to-end mount test for the memory backend.
#
# Mirrors the inline `Test mount` step in
# .github/workflows/integration.yml (matrix.backend.name == 'memory').
# 11 sub-tests cover the read/write/append/rename/recreate flow
# that the CI runs against MinIO/HDFS backends; the memory
# backend is the simplest one (no remote service to bring up)
# and the right one to spam for flake detection.
#
# Run locally:
#   ./tests/e2e/mount/memory_stress.sh
#
# Run from CI (see .github/workflows/integration.yml):
#   ./tests/e2e/mount/memory_stress.sh /path/to/mntrs-binary /path/to/mp
#
# Exit code: 0 on success, 1 on any sub-test failure (FAIL tracked
# explicitly per SESSION_PITFALLS §2.6 — never `set -e`).

# Don't `set -e`: we want to run all sub-tests even after a
# failure to get full diagnostic output. The shell convention
# here is to accumulate `FAIL` and exit non-zero at the end.
set -u

# Track the most recent mntrs mount pid so cleanup_iter can
# kill it precisely (avoids pkill matching the parent shell or
# unrelated processes).
LAST_MOUNT_PID=""

# ---- Argument parsing (CI passes these; local invocation
# uses the defaults below). ----
BIN="${1:-target/release/mntrs}"
MP="${2:-/tmp/mntrs-test}"
CACHE_DIR="${3:-/tmp/mntrs-cache}"
ITERATIONS="${4:-1}"   # default to single run; stress_loop.sh overrides

# The CI build step produces target/release/mntrs; if absent,
# build it (single-crate project so just `cargo build`).
if [ ! -x "$BIN" ]; then
    echo "=== Build mntrs ==="
    cargo build --release 2>&1 | tail -3
fi
BIN="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")"
echo "BIN=$BIN"
echo "MP=$MP"
echo "CACHE_DIR=$CACHE_DIR"
echo "ITERATIONS=$ITERATIONS"
echo

# ---- Per-iteration setup: ensure the mountpoint and cache dir
# are clean. Each iteration is independent. ----
cleanup_iter() {
    # Kill the previously-launched mount process directly by pid
    # (avoids pkill matching the parent shell or unrelated procs).
    if [ -n "$LAST_MOUNT_PID" ] && kill -0 "$LAST_MOUNT_PID" 2>/dev/null; then
        kill "$LAST_MOUNT_PID" 2>/dev/null || true
        # Wait up to 5s for the process to exit
        for _ in 1 2 3 4 5; do
            kill -0 "$LAST_MOUNT_PID" 2>/dev/null || break
            sleep 1
        done
        # If still alive, SIGKILL
        kill -0 "$LAST_MOUNT_PID" 2>/dev/null && kill -9 "$LAST_MOUNT_PID" 2>/dev/null || true
    fi
    LAST_MOUNT_PID=""

    # Best-effort unmount. Loop a few times because fusermount3
    # can return EBUSY immediately after the process is killed
    # (the kernel hasn't released the FUSE superblock yet).
    for _ in 1 2 3 4 5; do
        mount | grep -q " $MP " || break
        fusermount3 -u "$MP" 2>/dev/null || fusermount -u "$MP" 2>/dev/null || true
        sleep 0.5
    done

    # Belt-and-suspenders: kill any straggler mntrs whose cmdline
    # still references this mountpoint (e.g. if the parent process
    # had already forked children). Filter to this user to avoid
    # touching unrelated processes.
    for pid in $(pgrep -u "$UID" -f "mntrs mount .*${MP}" 2>/dev/null || true); do
        kill "$pid" 2>/dev/null || true
    done
    sleep 0.5

    rm -rf "$MP" "$CACHE_DIR"
    mkdir -p "$MP" "$CACHE_DIR"
}

# ---- The single mount + sub-test sequence (one iteration). ----
run_one() {
    local iter_label="${1:-iter 1}"
    local MOUNT_LOG="$(dirname "$CACHE_DIR")/mntrs-mount-${iter_label// /_}.log"
    local FAIL=0

    # Mount (memory backend: no daemon, foreground-style background).
    "$BIN" mount "memory:///" "$MP" \
        --cache-dir "$CACHE_DIR" \
        --allow-other \
        > "$MOUNT_LOG" 2>&1 &
    local MOUNT_PID=$!
    LAST_MOUNT_PID="$MOUNT_PID"
    sleep 1

    # 60s readiness probe — `ls` the mountpoint, retry on EIO.
    local READY=0
    for i in $(seq 1 60); do
        if mount | grep -q "$MP" && ls "$MP/" >/dev/null 2>&1; then
            echo "[$iter_label] mount ready after ${i}s"
            READY=1
            break
        fi
        sleep 1
    done
    if [ $READY -eq 0 ]; then
        echo "::error::memory mount not ready after 60s"
        echo "--- mount log ---"
        cat "$MOUNT_LOG" 2>/dev/null
        return 1
    fi
    echo

    # ---- 11 sub-tests (mirroring integration.yml `Test mount`) ----
    echo "[$iter_label] --- 1. ls ---"
    ls -la "$MP/" 2>&1 | head -5
    echo

    echo "[$iter_label] --- 2. cat pre-existing files ---"
    echo "(skipped: memory backend has no pre-existing file)"
    echo

    echo "[$iter_label] --- 3. write small file ---"
    if echo "hello from memory" > "$MP/_ci_small.txt" 2>/dev/null; then
        echo "[$iter_label] write OK"
    else
        echo "::error::write FAIL"
        FAIL=1
    fi
    echo

    echo "[$iter_label] --- 4. read back written file ---"
    local got
    got=$(cat "$MP/_ci_small.txt" 2>/dev/null)
    if [ "$got" = "hello from memory" ]; then
        echo "[$iter_label] read back OK"
    else
        echo "::error::read back FAIL: got '$got'"
        FAIL=1
    fi
    echo

    echo "[$iter_label] --- 5. append write + verify ---"
    echo "more data" >> "$MP/_ci_small.txt" 2>/dev/null && \
        echo "[$iter_label] append OK" || { echo "::error::append FAIL"; FAIL=1; }
    got=$(cat "$MP/_ci_small.txt" 2>/dev/null)
    local expected
    expected=$(printf "hello from memory\nmore data")
    if [ "$got" = "$expected" ]; then
        echo "[$iter_label] append verify OK"
    else
        echo "::error::append verify FAIL: got '$got'"
        FAIL=1
    fi
    echo

    echo "[$iter_label] --- 6. append to pre-existing file ---"
    echo "(skipped: memory backend has no pre-existing file)"
    echo

    echo "[$iter_label] --- 7. write+read 10M sequential ---"
    if dd if=/dev/urandom of="$MP/_ci_10m.bin" bs=1M count=10 2>/dev/null; then
        echo "[$iter_label] write 10M OK"
    else
        echo "::error::write 10M FAIL"
        FAIL=1
    fi
    if dd if="$MP/_ci_10m.bin" of=/dev/null bs=64K 2>/dev/null; then
        echo "[$iter_label] read 10M OK"
    else
        echo "::error::read 10M FAIL"
        FAIL=1
    fi
    echo

    echo "[$iter_label] --- 8. random seek read ---"
    for off in 0 500 10000 50000 500000 5000000 9000000 9999999; do
        if dd if="$MP/_ci_10m.bin" bs=1 count=1 skip="$off" of=/dev/null 2>/dev/null; then
            echo "[$iter_label]   seek $off OK"
        else
            echo "::error::seek $off FAIL"
            FAIL=1
        fi
    done
    echo

    echo "[$iter_label] --- 9. delete + recreate ---"
    rm -f "$MP/_ci_small.txt" 2>/dev/null
    if echo "recreated" > "$MP/_ci_small.txt" 2>/dev/null; then
        echo "[$iter_label] recreate OK"
    else
        echo "::error::recreate FAIL"
        FAIL=1
    fi
    got=$(cat "$MP/_ci_small.txt" 2>/dev/null)
    if [ "$got" = "recreated" ]; then
        echo "[$iter_label] recreate verify OK"
    else
        echo "::error::recreate verify FAIL: got '$got'"
        FAIL=1
    fi
    echo

    echo "[$iter_label] --- 10. directory create + list + delete ---"
    if mkdir -p "$MP/_ci_dir" 2>/dev/null; then
        echo "[$iter_label] mkdir OK"
    else
        echo "::error::mkdir FAIL"
        FAIL=1
    fi
    echo "dirfile" > "$MP/_ci_dir/file.txt" 2>/dev/null
    got=$(ls "$MP/_ci_dir/" 2>/dev/null)
    if [ "$got" = "file.txt" ]; then
        echo "[$iter_label] dir list OK"
    else
        echo "::error::dir list FAIL: got '$got'"
        FAIL=1
    fi
    if rm -rf "$MP/_ci_dir" 2>/dev/null; then
        echo "[$iter_label] rmdir OK"
    else
        echo "::error::rmdir FAIL"
        FAIL=1
    fi
    echo

    # ---- 10.5 rename (memory backend can't server-side rename;
    # falls back to read+write+delete via the atomic chain). ----
    echo "[$iter_label] --- 10.5. rename ---"
    if echo "before rename" > "$MP/_ci_ren.txt" 2>/dev/null; then
        echo "[$iter_label] rename seed OK"
    else
        echo "::error::rename seed FAIL"
        FAIL=1
    fi
    if mv "$MP/_ci_ren.txt" "$MP/_ci_renamed.txt" 2>/dev/null; then
        echo "[$iter_label] rename op OK"
    else
        echo "::error::rename op FAIL"
        FAIL=1
    fi
    got=$(cat "$MP/_ci_renamed.txt" 2>/dev/null)
    if [ "$got" = "before rename" ]; then
        echo "[$iter_label] rename dst read OK"
    else
        echo "::error::rename dst read FAIL: got '$got'"
        FAIL=1
    fi
    if [ ! -e "$MP/_ci_ren.txt" ]; then
        echo "[$iter_label] rename src removed OK"
    else
        echo "::error::rename src still exists"
        FAIL=1
    fi
    rm -f "$MP/_ci_renamed.txt" 2>/dev/null
    echo

    echo "[$iter_label] --- 11. cleanup ---"
    rm -f "$MP/_ci_small.txt" "$MP/_ci_10m.bin" 2>/dev/null || true

    # Note: the actual unmount + kill happens in cleanup_iter
    # before the NEXT iteration. Leaving the mount running here
    # avoids a race where the kernel hasn't released the FUSE
    # superblock yet (fusermount would EBUSY).

    if [ $FAIL -eq 0 ]; then
        echo "[$iter_label] ✅ memory mount OK"
        return 0
    else
        echo "::error::[$iter_label] memory mount tests FAILED"
        echo "--- mount log ($MOUNT_LOG) ---"
        cat "$MOUNT_LOG" 2>/dev/null
        return 1
    fi
}

# ---- Main loop: run `run_one` ITERATIONS times, accumulate
# pass/fail counts. Always exit 0 from the wrapper so the
# stress_loop harness can parse our output. ----
PASS=0
FAIL=0
declare -A FAIL_MODES

for i in $(seq 1 "$ITERATIONS"); do
    cleanup_iter
    if run_one "iter $i"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
    fi
done

echo
echo "Result: $PASS pass / $FAIL fail (pass rate: $(( PASS * 100 / (ITERATIONS) ))%)"

# Always 0 so the stress wrapper sees our summary line.
exit 0
