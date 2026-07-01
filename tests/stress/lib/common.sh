#!/usr/bin/env bash
#
# tests/stress/lib/common.sh
#
# Shared helpers for the #143 stress/stability test suite.
#
# Sources the script from the test file like:
#   . "$(dirname "$0")/lib/common.sh"
#
# Conventions (matching tests/e2e/common/hdfs-prep.sh):
#   - 4-space indent
#   - `name() { ... }` form (no `function` keyword)
#   - `set -euo pipefail` only at the direct-invocation branch
#
# Public API:
#   mntrs_setup       — build mntrs binary, create scratch dirs
#   mntrs_mount       — start a daemon-mounted memory backend, wait ready
#   mntrs_unmount     — fusermount -u + cleanup
#   stress_metric     — sample RSS/fd/thread metrics to <log>.metrics
#   assert_eq         — fail-fast equality check with diff
#   assert_le         — fail-fast "<=" check (memory-bound assertions)
#   assert_ge         — fail-fast ">=" check
#   pass / fail / log — green/red status helpers

# shellcheck shell=bash

if [[ -n "${__STRESS_COMMON_LOADED:-}" ]]; then
    return 0 2>/dev/null || true
fi
__STRESS_COMMON_LOADED=1

# ── Paths ────────────────────────────────────────────────────────────
# Per-suite scratch dir: $STRSCRATCH/<test-name>-<pid>/
STRSCRATCH="${STRSCRATCH:-/tmp/mntrs-stress}"
MNTRS_BIN="${MNTRS_BIN:-$STRSCRATCH/mntrs}"
REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"

mkdir -p "$STRSCRATCH"

# ── Logging ──────────────────────────────────────────────────────────
log()  { printf '\033[1;36m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
pass() { printf '\033[1;32m  PASS\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m  FAIL\033[0m %s\n' "$*" >&2; exit 1; }
warn() { printf '\033[1;33m  WARN\033[0m %s\n' "$*" >&2; }
section() { printf '\n\033[1;35m━━━ %s ━━━\033[0m\n' "$*"; }

# ── Assertions ───────────────────────────────────────────────────────
assert_eq() {
    local got="$1" want="$2" msg="${3:-assert_eq}"
    if [[ "$got" != "$want" ]]; then
        fail "$msg: got '$got', want '$want'"
    fi
    pass "$msg ($got)"
}
assert_ge() {
    local got="$1" want="$2" msg="${3:-assert_ge}"
    if (( got < want )); then
        fail "$msg: $got < $want"
    fi
    pass "$msg ($got >= $want)"
}
assert_le() {
    local got="$1" want="$2" msg="${3:-assert_le}"
    if (( got > want )); then
        fail "$msg: $got > $want"
    fi
    pass "$msg ($got <= $want)"
}

# ── Build mntrs (debug build — has line numbers in stack traces) ──────
mntrs_setup() {
    if [[ ! -x "$MNTRS_BIN" ]] || [[ "$REPO_ROOT/src" -nt "$MNTRS_BIN" ]]; then
        log "Building mntrs (debug) ..."
        (cd "$REPO_ROOT" && cargo build --bin mntrs) || fail "cargo build failed"
        # debug binary path: target/debug/mntrs; copy to STRSCRATCH so the
        # binary path is stable regardless of cargo target-dir config.
        cp "$REPO_ROOT/target/debug/mntrs" "$MNTRS_BIN"
    fi
    log "mntrs binary: $MNTRS_BIN"
}

# ── Mount / unmount ──────────────────────────────────────────────────
# Usage: mntrs_mount <mountpoint> <cache_dir> [extra mntrs args...]
#
# Uses --daemon + --daemon-wait so the binary returns once the FUSE
# mount is live (avoids racing with shell I/O through the mountpoint).
#
# NOTE: FUSE kernel writeback (`--write-back-cache`) is OFF by default
# in mntrs (see `FuserAdapter::write_back_cache` + CLI flag's bool
# default). Tests that want the old behavior (kernel buffering writes,
# daemon's write() skipped for multi-page files) must opt in explicitly
# via `"$@"`. Most stress tests rely on the default — daemon-side
# write() is the contract they exercise.
mntrs_mount() {
    local mnt="$1"
    local cache_dir="$2"
    shift 2

    mkdir -p "$mnt" "$cache_dir"

    # Always allow_other so stress scripts run as root or any user.
    # --vfs-cache-mode full: writeback enabled (so tests 04/05 actually exercise upload).
    # --vfs-write-back: overridable via env (default 1s; 05-crash-recovery sets 30).
    #   Tests must NOT pass --vfs-write-back in "$@" again (clap rejects duplicate).
    #
    # RUST_LOG=debug is injected by default so daemon's write: entry /
    # register_dirty traces land in mount.log. Distinguishes "kernel
    # absorbed the write" (no trace) from "daemon's write() fired but
    # cache file was slow to materialize" (trace exists, file appears
    # shortly). Override via STRESS_RUST_LOG=info for tests that
    # exercise a high-volume writeback loop and don't need the noise.
    #
    # MNTRS_DAEMON_LOG redirects the re-exec'd daemon child's stdio to
    # the same mount.log. Without this, the daemon detaches its stdio
    # to /dev/null (per cmd/mount.rs:1157), so the only thing in
    # mount.log would be the parent process's startup lines.
    # Setting MNTRS_DAEMON_LOG preserves the daemon's tracing output
    # alongside the parent's, making post-mortem debugging possible.
    MNTRS_DAEMON_LOG="$cache_dir/mount.log" \
    RUST_LOG="${STRESS_RUST_LOG:-debug}" "$MNTRS_BIN" mount \
        "memory:///" \
        "$mnt" \
        --daemon --daemon-wait \
        --allow-other \
        --cache-dir "$cache_dir" \
        --vfs-cache-mode full \
        --vfs-write-back "${STRESS_VFS_WRITE_BACK:-1}" \
        "$@" \
        > "$cache_dir/mount.log" 2>&1 \
        || { cat "$cache_dir/mount.log"; fail "mntrs mount failed for $mnt"; }

    # Wait for the FUSE mount to actually register with the kernel.
    # `stat -f $mnt` was previously used here, but it succeeds for any
    # directory (regular or FUSE-backed) so it can pass BEFORE the FUSE
    # mount registers in /proc/self/mounts (typically 0.5-2s later on
    # Debian 13 / kernel 6.12.94). When that happened, the next test
    # dd/write hit the local empty $mnt/ directory instead of going
    # through FUSE, and the daemon never saw any CREATE/WRITE events —
    # test 01/04/05's "0 cache files" failure mode. See memory
    # stress-2026-07-01-mount-registration-race.md.
    #
    # The grep below checks for a mountpoint string in /proc/self/mounts
    # with a leading space (to avoid substring matches on /tmp/foo/mnt
    # matching /tmp/foo/mnt2) and a trailing space (to avoid matching
    # longer paths that share the prefix). 5s budget matches the old
    # stat-f budget; on a healthy kernel the mount appears within ~1s.
    local i ready=0
    # shellcheck disable=SC2034  # loop counter
    for i in $(seq 1 50); do
        if grep -F " $mnt " /proc/self/mounts >/dev/null 2>&1; then ready=1; break; fi
        sleep 0.1
    done
    if (( ready == 0 )); then
        log "mount.log tail after 5s mount-registration timeout:"
        tail -30 "$cache_dir/mount.log" || true
        fail "FUSE mount for $mnt never registered in /proc/self/mounts (see $cache_dir/mount.log)"
    fi
    # Even if stat succeeded, the daemon could have died immediately
    # after the kernel accepted the mount (e.g. FUSE session abort).
    # Check via pgrep on the basename so we don't depend on build path.
    if ! pgrep -f "$(basename "$MNTRS_BIN") mount memory" >/dev/null 2>&1; then
        log "mount.log tail after stat-success-without-daemon:"
        tail -30 "$cache_dir/mount.log" || true
        fail "mntrs daemon died after mount (stat succeeded but pid gone)"
    fi
}

mntrs_unmount() {
    local mnt="$1"
    # Try `mntrs unmount` first (it drains writeback cleanly via the
    # documented path). Fall back to fusermount3 directly if the
    # daemon's mtab record is stale or the mntrs binary is gone
    # (e.g. CI cleanup race). Both paths exit 0 if the mount is gone.
    "$MNTRS_BIN" unmount "$mnt" >/dev/null 2>&1 \
        || fusermount3 -u "$mnt" 2>/dev/null \
        || fusermount -u "$mnt" 2>/dev/null \
        || true
    sleep 0.5
    # Final best-effort cleanup of any zombie mount entry
    fusermount3 -qzu "$mnt" 2>/dev/null || true
}

# ── Cache dir preservation ───────────────────────────────────────────
# Usage: stress_preserve_cache <cache_dir> <label>
#
# Move <cache_dir> to <cache_dir>-debug-<timestamp> so post-mortem
# inspection is possible after a test failure. The trap's
# `mntrs_unmount` would otherwise delete the cache dir via
# `remove_dir_all(cache_dir)` inside cmd::mount, leaving the
# artifact empty. This helper is meant to be called by tests' EXIT
# traps BEFORE mntrs_unmount when a failure has been observed.
#
# Idempotent: if the dir is already gone, it's a no-op.
stress_preserve_cache() {
    local cache_dir="$1"
    local label="${2:-debug}"
    if [[ -d "$cache_dir" ]]; then
        local stamp
        stamp=$(date +%H%M%S)
        local preserved="${cache_dir}-${label}-${stamp}"
        mv "$cache_dir" "$preserved" 2>/dev/null || true
        log "preserved cache for post-mortem: $preserved"
    fi
}

# ── Metrics ──────────────────────────────────────────────────────────
# Sample RSS (KB), fd count, thread count for a PID.
# Usage: stress_metric <pid> <out_file> [label]
stress_metric() {
    # shellcheck disable=SC2034  # label reserved for caller-side tagging
    local pid="$1" out="$2" label="${3:-}"
    {
        local rss_kb=0 fd_count=0 thread_count=0
        if [[ -d "/proc/$pid" ]]; then
            rss_kb=$(awk '/^VmRSS:/ {print $2}' "/proc/$pid/status" 2>/dev/null || echo 0)
            fd_count=$(ls -1 "/proc/$pid/fd" 2>/dev/null | wc -l)
            thread_count=$(ls -1 "/proc/$pid/task" 2>/dev/null | wc -l)
        fi
        printf '%s rss_kb=%s fds=%s threads=%s\n' \
            "$(date +%H:%M:%S)" "$rss_kb" "$fd_count" "$thread_count"
    } >> "$out"
}

# ── Test registration ────────────────────────────────────────────────
# Track pass/fail counts for the run-all entry point.
# shellcheck disable=SC2034  # reserved for cross-script tally
STRESS_PASS=0
# shellcheck disable=SC2034  # reserved for cross-script tally
STRESS_FAIL=0