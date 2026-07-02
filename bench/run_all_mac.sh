#!/usr/bin/env bash
#
# bench/run_all_mac.sh — macOS mntrs benchmark (memory:// + s3:// backends).
#
# Mirrors bench/run_all.sh (the Linux mntrs-vs-rclone bench) but is
# macOS-native:
#
#   - Uses macFUSE umount semantics (`umount -f`) instead of
#     `fusermount3 -u`.
#   - Uses BSD `stat -f%z` instead of GNU `stat -c%s`.
#   - Uses `date +%FT%TZ` instead of GNU `date -Iseconds`.
#   - Uses BSD `xattr -l` instead of GNU `getfattr -d`.
#   - `head -c` uses bytes (BSD doesn't accept K/M suffixes).
#   - `/tmp` is canonicalized to `/private/tmp` in mount-table checks
#     (macFUSE registers the canonicalized path).
#
# rclone comparison: opt-in via MAC_BENCH_INCLUDE_RCLONE=1. The script
# probes `rclone mount` capability (brew-installed rclone on darwin
# explicitly does not support mount — only the official binary from
# rclone.org/downloads/ works). Auto-detect is on by default: if rclone
# is in $PATH AND a brief probe mount succeeds, the comparison is
# enabled. Set MAC_BENCH_INCLUDE_RCLONE=0 to force mntrs-only.
#
# Quick start:
#   bash bench/run_all_mac.sh
# Force mntrs-only (skip the rclone probe):
#   MAC_BENCH_INCLUDE_RCLONE=0 bash bench/run_all_mac.sh
# Skip the S3 backend (memory-only, fast CI-style smoke):
#   MAC_BENCH_SKIP_S3=1 bash bench/run_all_mac.sh
#
# This file is intentionally ~280 lines (vs the 562-line Linux bench)
# because the rclone side is conditional. When RCLONE_OPT_IN=0 the
# script is effectively mntrs-only; when RCLONE_OPT_IN=1 the comparison
# runs as 1:1 with the Linux bench's mntrs side.
set -uo pipefail

# ── Paths ────────────────────────────────────────────────────────────
ENDPOINT="${ENDPOINT:-http://localhost:9000}"
ACCESS_KEY="${ACCESS_KEY:-minioadmin}"
SECRET_KEY="${SECRET_KEY:-minioadmin}"
BUCKET="${BUCKET:-bench-bucket}"
REGION="${REGION:-us-east-1}"
MNTRS_BIN="${MNTRS_BIN:-./target/release/mntrs}"
MNTRS_MNT="${MNTRS_MNT:-/tmp/mntrs-bench}"
RCLONE_MNT="${RCLONE_MNT:-/tmp/rclone-bench}"
MEM_MNT="/tmp/mntrs-mem-bench"
DATA_DIR="/tmp/mntrs-bench-data"
RESULT_TMP="$(mktemp /tmp/bench-mac-results-XXXXXX)"

# Result-tmp is preserved at /tmp/mntrs-bench-mac-result.txt for
# baseline seeding + post-run inspection. The rendered markdown table
# is printed to stdout and appended to /tmp/mntrs-bench-mac-result.md.
RESULT_COPY="/tmp/mntrs-bench-mac-result.txt"
RESULT_MD="/tmp/mntrs-bench-mac-result.md"

# macOS-specific: /tmp is a symlink to /private/tmp. Canonicalize at
# mount-time so FUSE writeback markers land in the path the user asked
# for, and the mount-table grep below can match the kernel's view.
canonicalize_mnt() {
    case "$1" in
        /tmp*) echo "/private${1}" ;;
        *)     echo "$1"      ;;
    esac
}

# ── Cleanup ──────────────────────────────────────────────────────────
cleanup() {
    if [[ -n "${MNTRS_BIN:-}" ]] && [[ -x "$MNTRS_BIN" ]]; then
        "$MNTRS_BIN" unmount "$MNTRS_MNT" >/dev/null 2>&1 || true
        "$MNTRS_BIN" unmount "$MEM_MNT"     >/dev/null 2>&1 || true
    fi
    if [[ "$RCLONE_OPT_IN" == "1" ]] && command -v rclone >/dev/null 2>&1; then
        rclone unmount "$RCLONE_MNT" >/dev/null 2>&1 || true
    fi
    umount -f "$MNTRS_MNT" >/dev/null 2>&1 || true
    umount -f "$RCLONE_MNT" >/dev/null 2>&1 || true
    umount -f "$MEM_MNT"   >/dev/null 2>&1 || true
    cp "$RESULT_TMP" "$RESULT_COPY" 2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "$MNTRS_MNT" "$MEM_MNT"

# ── rclone auto-detect ──────────────────────────────────────────────
# Brew-installed rclone on darwin cannot mount. The official binary
# (rclone.org/downloads/) does. We can't statically tell the two apart
# from `which rclone` alone (both are 80MB Mach-O binaries), so we
# probe with a brief mount.
RCLONE_OPT_IN=0
RC_BIN="$(command -v rclone 2>/dev/null || true)"
[[ "$RCLONE_OPT_IN" == "1" ]] && mkdir -p "$RCLONE_MNT"
case "${MAC_BENCH_INCLUDE_RCLONE:-auto}" in
    1|true|yes)  RCLONE_OPT_IN=1 ;;
    0|false|no)  RCLONE_OPT_IN=0 ;;
    auto|"")
        if [[ -x "$RC_BIN" ]]; then
            PROBE_DIR=$(mktemp -d /tmp/.rclone-probe.XXXXXX)
            # rclone reads HTTP_PROXY/HTTPS_PROXY from the environment
            # to reach MinIO through a corporate egress. We pass the
            # env through unchanged rather than baking in a default —
            # operators set the proxy in their shell rc when needed.
            # `${HTTP_PROXY:-}` returns "" when unset, satisfying `set -u`.
            HTTP_PROXY="${HTTP_PROXY:-}" \
            HTTPS_PROXY="${HTTPS_PROXY:-}" \
                "$RC_BIN" mount bench:bench-bucket "$PROBE_DIR" \
                    --daemon --allow-non-empty --vfs-cache-mode=writes \
                    --log-file "$PROBE_DIR.log" --log-level ERROR 2>/dev/null
            # macFUSE kext handshake can take 15-20s on busy hosts.
            # Mirror the main-mount polling loop (see "Mount rclone"
            # below) instead of a fixed 3s sleep, otherwise we
            # mis-classify a working rclone binary as "brew-installed"
            # and silently fall back to mntrs-only.
            RC_PROBE_OK=0
            for _ in $(seq 1 30); do
                sleep 1
                if mount | grep -qF " on $(canonicalize_mnt "$PROBE_DIR") ("; then
                    RC_PROBE_OK=1
                    break
                fi
            done
            if [[ "$RC_PROBE_OK" == "1" ]]; then
                RCLONE_OPT_IN=1
                echo "[mac-bench] rclone auto-detected at $RC_BIN (mount test OK)"
            else
                echo "[mac-bench] rclone at $RC_BIN cannot mount (likely brew); mntrs-only"
            fi
            umount -f "$PROBE_DIR" 2>/dev/null || true
            "$RC_BIN" unmount "$PROBE_DIR" 2>/dev/null || true
            pkill -f "rclone mount bench:bench-bucket $PROBE_DIR" 2>/dev/null || true
            rm -rf "$PROBE_DIR" "$PROBE_DIR.log" 2>/dev/null || true
        else
            echo "[mac-bench] rclone not in PATH; mntrs-only"
        fi
        ;;
esac

# ── bench() helper (same contract as bench/run_all.sh) ───────────────
PASS=0; FAIL=0; TOTAL=0
bench() {
    local name="$1" target="$2"
    shift 2
    local t
    local out
    TOTAL=$((TOTAL + 1))
    out=$({ time "$@" >/dev/null 2>&1; } 2>&1) || {
        printf "  %-35s | %10s | FAIL\n" "$name" "$target"
        echo "FAIL|$name|$target|$CATEGORY" >> "$RESULT_TMP"
        FAIL=$((FAIL + 1))
        return
    }
    t=$(echo "$out" | grep real | awk '{print $2}')
    printf "  %-35s | %10s | %s\n" "$name" "$target" "$t"
    echo "$t|$name|$target|$CATEGORY" >> "$RESULT_TMP"
    PASS=$((PASS + 1))
}

# bench_mr — convenience: bench a name on both mntrs and (opt-in) rclone.
#   bench_mr "name" mntrs_cmd... || rclone_cmd...
# Args before the literal sentinel "||" form the mntrs invocation,
# args after form the rclone invocation. This avoids using "|" (shell
# pipeline) as a separator inside a single function call.
bench_mr() {
    local name="$1"
    shift
    local mntrs_args=() rclone_args=()
    local mode="m"
    for arg in "$@"; do
        if [[ "$arg" == "||" ]]; then mode="r"; continue; fi
        if [[ "$mode" == "m" ]]; then mntrs_args+=("$arg"); else rclone_args+=("$arg"); fi
    done
    bench "$name" "mntrs" "${mntrs_args[@]}"
    if [[ "$RCLONE_OPT_IN" == "1" ]]; then
        bench "$name" "rclone" "${rclone_args[@]}"
    fi
}

# ── Header ───────────────────────────────────────────────────────────
START_TIME=$(date +%s)
echo "============================================"
echo " mntrs macOS benchmark"
echo " started: $(date +%FT%TZ)"
echo " endpoint: $ENDPOINT"
echo " bucket: $BUCKET"
echo " rclone: $([[ $RCLONE_OPT_IN == 1 ]] && echo "enabled ($RC_BIN)" || echo "disabled (mntrs-only)")"
echo "============================================"
echo ""

# ── Test data prep ───────────────────────────────────────────────────
echo "--- Preparing test data ---"
mkdir -p "$DATA_DIR"
dd if=/dev/urandom of="$DATA_DIR/1K.bin"   bs=1K  count=1 2>/dev/null
dd if=/dev/urandom of="$DATA_DIR/4K.bin"   bs=4K  count=1 2>/dev/null
dd if=/dev/urandom of="$DATA_DIR/64K.bin"  bs=64K count=1 2>/dev/null
dd if=/dev/urandom of="$DATA_DIR/1M.bin"   bs=1M  count=1 2>/dev/null
dd if=/dev/urandom of="$DATA_DIR/10M.bin"  bs=1M  count=10 2>/dev/null
dd if=/dev/urandom of="$DATA_DIR/100M.bin" bs=1M  count=100 2>/dev/null
mkdir -p "$DATA_DIR/many"
for i in $(seq 1 500); do
    echo "$i" > "$DATA_DIR/many/file_$(printf '%04d' "$i").txt"
done
echo "  data prepared: $(du -sh "$DATA_DIR" | awk '{print $1}')"

# ── Mount mntrs (S3 backend) ─────────────────────────────────────────
echo ""
echo "--- Mounting ---"
echo "  $(date +%FT%TZ): starting mntrs S3 mount..."
if [[ "${MAC_BENCH_SKIP_S3:-0}" != "1" ]]; then
    "$MNTRS_BIN" mount "s3://$BUCKET" "$MNTRS_MNT" \
        --opt "endpoint=$ENDPOINT" --opt "access-key=$ACCESS_KEY" \
        --opt "secret-key=$SECRET_KEY" --opt "region=$REGION" \
        --vfs-cache-mode=writes --vfs-write-back=5 \
        --vfs-read-ahead=134217728 --async-read \
        --mem-cache-impl="${MEM_CACHE_IMPL:-dashmap}" \
        --daemon --daemon-wait --daemon-timeout=15 2>&1
fi
echo "  $(date +%FT%TZ): mntrs mount returned (exit=$?)"
sleep 3

# ── Mount rclone (opt-in) ────────────────────────────────────────────
if [[ "$RCLONE_OPT_IN" == "1" ]]; then
    echo "  $(date +%FT%TZ): starting rclone mount..."
    # --allow-non-empty: the mount point can have leftover FUSE write-
    # back markers (e.g. .DS_Store or previous test files) from prior
    # runs — without this flag rclone refuses to mount, even though
    # the kernel would happily layer macFUSE on top.
    # Proxy: pass through the operator's env (rclone respects
    # HTTP_PROXY/HTTPS_PROXY for the S3 backend). Don't bake in
    # defaults — operators set this in their shell rc when needed.
    # `${HTTP_PROXY:-}` returns "" when unset, satisfying `set -u`.
    HTTP_PROXY="${HTTP_PROXY:-}" \
    HTTPS_PROXY="${HTTPS_PROXY:-}" \
        "$RC_BIN" mount "bench:$BUCKET" "$RCLONE_MNT" \
            --daemon --allow-non-empty \
            --vfs-cache-mode=writes --vfs-write-back=5s \
            --log-file /tmp/rclone-bench.log --log-level INFO 2>&1
    echo "  $(date +%FT%TZ): rclone mount returned (exit=$?)"
    # rclone's macFUSE mount races with the daemon's exit; poll for
    # up to 30s before declaring failure. Without this delay the
    # mount-table check below hits a transient FAILED message even
    # though the rclone tests later all pass (the kext lands
    # asynchronously after the daemon process is forked). In some
    # environments the kext handshake can take 15-20s when many
    # macFUSE mounts are already active.
    for _ in $(seq 1 30); do
        sleep 1
        if mount | grep -qF " on $(canonicalize_mnt "$RCLONE_MNT") ("; then
            break
        fi
    done
fi

# Mount-table check (uses canonicalize_mnt for /tmp → /private/tmp)
echo "  $(date +%FT%TZ): checking mounts..."
CANON_MNTRS=$(canonicalize_mnt "$MNTRS_MNT")
if mount | grep -q " on $CANON_MNTRS ("; then
    echo "  mntrs mount: OK"
else
    echo "  mntrs mount: FAILED — mount table:"
    mount | grep mntrs || true
    echo "  mntrs mount: FAILED (check errors above)"
fi
if [[ "$RCLONE_OPT_IN" == "1" ]]; then
    CANON_RCLONE=$(canonicalize_mnt "$RCLONE_MNT")
    if mount | grep -q " on $CANON_RCLONE ("; then
        echo "  rclone mount: OK"
    else
        echo "  rclone mount: FAILED — mount table:"
        mount | grep rclone || true
        echo "  rclone mount: FAILED"
    fi
fi
echo "  mounts ready"
echo ""

# ── Upload test data via S3 (skip when MAC_BENCH_SKIP_S3=1) ──────────
if [[ "${MAC_BENCH_SKIP_S3:-0}" != "1" ]]; then
    echo "--- Uploading test data ---"
    # Try to install awscli if missing. Without this precondition, the
    # subsequent `aws s3 mb/sync` calls fail silently and the bench
    # runs against an empty bucket — producing 0-IO "PASS" results
    # that look valid but don't exercise anything.
    if ! command -v aws >/dev/null 2>&1; then
        if command -v uv >/dev/null 2>&1; then
            uv tool install awscli 2>&1 | tail -5 || true
        elif command -v pip3 >/dev/null 2>&1; then
            pip3 install --user awscli 2>&1 | tail -5 || true
        fi
    fi
    if ! command -v aws >/dev/null 2>&1; then
        echo "ERROR: awscli not found and could not be installed." >&2
        echo "       Install via 'brew install awscli' or 'uv tool install awscli'," >&2
        echo "       or set MAC_BENCH_SKIP_S3=1 to skip the S3 backend." >&2
        exit 1
    fi
    AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
        aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3 mb "s3://$BUCKET" 2>/dev/null || true
    rm -rf /tmp/bench-upload; mkdir -p /tmp/bench-upload
    cp "$DATA_DIR"/1K.bin "$DATA_DIR"/4K.bin "$DATA_DIR"/64K.bin \
       "$DATA_DIR"/1M.bin "$DATA_DIR"/10M.bin "$DATA_DIR"/100M.bin \
       /tmp/bench-upload/
    mkdir -p /tmp/bench-upload/many && cp -r "$DATA_DIR"/many/* /tmp/bench-upload/many/
    AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
        aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3 sync /tmp/bench-upload/ "s3://$BUCKET/" 2>&1
    echo "  upload done"
    echo ""
fi

# ── Warmup ───────────────────────────────────────────────────────────
echo "--- Warmup ---"
timeout 10 ls "$MNTRS_MNT"/ >/dev/null 2>&1 || echo "  warmup: mntrs ls hung"
timeout 10 cat "$MNTRS_MNT/1K.bin" >/dev/null 2>&1 || echo "  warmup: mntrs cat hung"
if [[ "$RCLONE_OPT_IN" == "1" ]]; then
    timeout 10 ls "$RCLONE_MNT"/ >/dev/null 2>&1 || echo "  warmup: rclone ls hung"
    timeout 10 cat "$RCLONE_MNT/1K.bin" >/dev/null 2>&1 || echo "  warmup: rclone cat hung"
fi

# ── 1. Directory listing ─────────────────────────────────────────────
echo ""; echo "=== 1. Directory listing ==="; CATEGORY="DirList"
for d in "" "many"; do
    bench_mr "ls $d" ls "$MNTRS_MNT/$d" || ls "$RCLONE_MNT/$d"
    bench_mr "ls -la $d" ls -la "$MNTRS_MNT/$d" || ls -la "$RCLONE_MNT/$d"
done
for d in "" "many"; do
    bench_mr "find $d maxdepth1" find "$MNTRS_MNT/$d" -maxdepth 1 \
        | find "$RCLONE_MNT/$d" -maxdepth 1
done

# ── 2. Stat ──────────────────────────────────────────────────────────
echo ""; echo "=== 2. Stat ==="; CATEGORY="Stat"
for f in 1K.bin 4K.bin 64K.bin 1M.bin 10M.bin 100M.bin; do
    bench_mr "stat $f" stat "$MNTRS_MNT/$f" || stat "$RCLONE_MNT/$f"
done
for f in 1K.bin 4K.bin; do
    for i in 1 10 100; do
        bench_mr "stat ${f}x${i}" \
            bash -c "for n in \$(seq 1 $i); do stat '$MNTRS_MNT/$f' >/dev/null; done" \
            || \
            bash -c "for n in \$(seq 1 $i); do stat '$RCLONE_MNT/$f' >/dev/null; done"
    done
done

# ── 3. Sequential read ───────────────────────────────────────────────
echo ""; echo "=== 3. Sequential read ==="; CATEGORY="SeqRead"
for f in 1K.bin 4K.bin 64K.bin 1M.bin 10M.bin 100M.bin; do
    bench_mr "cat $f" cat "$MNTRS_MNT/$f" || cat "$RCLONE_MNT/$f"
done
for f in 1K.bin 4K.bin; do
    for i in 10 100; do
        bench_mr "cat ${f}x${i}" \
            bash -c "for n in \$(seq 1 $i); do cat '$MNTRS_MNT/$f' >/dev/null; done" \
            || \
            bash -c "for n in \$(seq 1 $i); do cat '$RCLONE_MNT/$f' >/dev/null; done"
    done
done

# ── 4. Read via dd ───────────────────────────────────────────────────
echo ""; echo "=== 4. Read via dd (block size variation) ==="; CATEGORY="ddRead"
for bs in 512 4096 65536 1048576; do
    for f in 1M.bin 10M.bin; do
        bench_mr "dd bs=${bs} $f" \
            dd if="$MNTRS_MNT/$f" bs=$bs of=/dev/null 2>/dev/null \
            || \
            dd if="$RCLONE_MNT/$f" bs=$bs of=/dev/null 2>/dev/null
    done
done

# ── 5. Random read (BSD stat: -f%z) ─────────────────────────────────
echo ""; echo "=== 5. Random read ==="; CATEGORY="RandRead"
for f in 1M.bin 10M.bin; do
    for seeks in 10 50; do
        bench_mr "random ${seeks}x $f" \
            bash -c "sz=\$(stat -f%z '$MNTRS_MNT/$f' 2>/dev/null || echo 1048576); for n in \$(seq 1 $seeks); do off=\$((RANDOM % sz)); dd if='$MNTRS_MNT/$f' bs=1 count=1 skip=\$off of=/dev/null 2>/dev/null; done" \
            || \
            bash -c "sz=\$(stat -f%z '$RCLONE_MNT/$f' 2>/dev/null || echo 1048576); for n in \$(seq 1 $seeks); do off=\$((RANDOM % sz)); dd if='$RCLONE_MNT/$f' bs=1 count=1 skip=\$off of=/dev/null 2>/dev/null; done"
    done
done

# ── 6. Write ─────────────────────────────────────────────────────────
echo ""; echo "=== 6. Write ==="; CATEGORY="Write"
for sz in 1K 4K 64K 1M; do
    bench_mr "write $sz new" \
        cp "$DATA_DIR/${sz}.bin" "$MNTRS_MNT/bench-write-${sz}.bin" \
        || \
        cp "$DATA_DIR/${sz}.bin" "$RCLONE_MNT/bench-write-${sz}.bin"
done
for sz in 1K 4K; do
    bench_mr "write $sz overwrite" \
        cp "$DATA_DIR/${sz}.bin" "$MNTRS_MNT/1K.bin" \
        || \
        cp "$DATA_DIR/${sz}.bin" "$RCLONE_MNT/1K.bin"
done

# ── 7. Dir/File ops ──────────────────────────────────────────────────
echo ""; echo "=== 7. Dir/File ops ==="; CATEGORY="DirOps"
bench_mr "mkdir"    mkdir -p "$MNTRS_MNT/bench-dir" || mkdir -p "$RCLONE_MNT/bench-dir"
bench_mr "rmdir"    rmdir "$MNTRS_MNT/bench-dir"    || rmdir "$RCLONE_MNT/bench-dir"
bench_mr "unlink"   rm -f "$MNTRS_MNT/bench-unlink-test" || rm -f "$RCLONE_MNT/bench-unlink-test"
touch "$MNTRS_MNT/bench-unlink-test" 2>/dev/null
if [[ "$RCLONE_OPT_IN" == "1" ]]; then
    touch "$RCLONE_MNT/bench-unlink-test" 2>/dev/null
fi
bench_mr "unlink exist" rm -f "$MNTRS_MNT/bench-unlink-test" || rm -f "$RCLONE_MNT/bench-unlink-test"

# ── 8. Rename ────────────────────────────────────────────────────────
echo ""; echo "=== 8. Rename ==="; CATEGORY="Rename"
cp "$DATA_DIR/1K.bin" "$MNTRS_MNT/bench-rename-src" 2>/dev/null
if [[ "$RCLONE_OPT_IN" == "1" ]]; then
    cp "$DATA_DIR/1K.bin" "$RCLONE_MNT/bench-rename-src" 2>/dev/null
fi
bench_mr "rename" \
    mv "$MNTRS_MNT/bench-rename-src" "$MNTRS_MNT/bench-rename-dst" \
    || \
    mv "$RCLONE_MNT/bench-rename-src" "$RCLONE_MNT/bench-rename-dst"

# ── 9. Truncate ──────────────────────────────────────────────────────
echo ""; echo "=== 9. Truncate ==="; CATEGORY="Truncate"
cp "$DATA_DIR/10M.bin" "$MNTRS_MNT/bench-trunc" 2>/dev/null
if [[ "$RCLONE_OPT_IN" == "1" ]]; then
    cp "$DATA_DIR/10M.bin" "$RCLONE_MNT/bench-trunc" 2>/dev/null
fi
bench_mr "truncate 0"  truncate -s 0  "$MNTRS_MNT/bench-trunc" || truncate -s 0  "$RCLONE_MNT/bench-trunc"
bench_mr "truncate 1M" truncate -s 1M "$MNTRS_MNT/bench-trunc" || truncate -s 1M "$RCLONE_MNT/bench-trunc"

# ── 10. Xattr (BSD: xattr -l replaces getfattr) ──────────────────────
echo ""; echo "=== 10. Xattr ==="; CATEGORY="Xattr"
bench_mr "xattr" xattr -l "$MNTRS_MNT/1K.bin" || xattr -l "$RCLONE_MNT/1K.bin"

# ── 11. Memory backend baseline (zero network) ───────────────────────
echo ""; echo "=== 11. Memory backend (zero network) ==="; CATEGORY="Memory"
mkdir -p "$MEM_MNT"
"$MNTRS_BIN" mount "memory://" "$MEM_MNT" \
    --daemon --daemon-wait --daemon-timeout=10 2>/dev/null
sleep 2
bench "stat mem" "mntrs" stat "$MEM_MNT"
echo "  (memory backend — for reference only)"

# ── 12. Head/tail reads (BSD head -c needs bytes, not K) ────────────
echo ""; echo "=== 12. Head/tail reads ==="; CATEGORY="HeadTail"
for f in 10M.bin 100M.bin; do
    for n in 1 10 100; do
        bench_mr "head -c${n}K $f" \
            head -c $((n * 1024)) "$MNTRS_MNT/$f" \
            || \
            head -c $((n * 1024)) "$RCLONE_MNT/$f"
    done
done

# ── 13. Checksum ─────────────────────────────────────────────────────
echo ""; echo "=== 13. Checksum ==="; CATEGORY="Checksum"
for f in 1K.bin 4K.bin 64K.bin 1M.bin; do
    bench_mr "md5sum $f" md5sum "$MNTRS_MNT/$f"  || md5sum "$RCLONE_MNT/$f"
    bench_mr "shasum $f" shasum "$MNTRS_MNT/$f"  || shasum "$RCLONE_MNT/$f"
done

# ── 14. Touch ────────────────────────────────────────────────────────
echo ""; echo "=== 14. Touch ==="; CATEGORY="Touch"
bench_mr "touch new"   touch "$MNTRS_MNT/bench-touch-new" || touch "$RCLONE_MNT/bench-touch-new"
bench_mr "touch exist" touch "$MNTRS_MNT/1K.bin"          || touch "$RCLONE_MNT/1K.bin"

# ── 15. Chmod ────────────────────────────────────────────────────────
echo ""; echo "=== 15. Chmod ==="; CATEGORY="Chmod"
bench_mr "chmod" chmod 0644 "$MNTRS_MNT/1K.bin" 2>/dev/null || \
    chmod 0644 "$RCLONE_MNT/1K.bin" 2>/dev/null || true

# ── 16. Symlink ──────────────────────────────────────────────────────
echo ""; echo "=== 16. Symlink ==="; CATEGORY="Symlink"
ln -sf "$MNTRS_MNT/1K.bin" "$MNTRS_MNT/bench-link" 2>/dev/null || true
if [[ "$RCLONE_OPT_IN" == "1" ]]; then
    ln -sf "$RCLONE_MNT/1K.bin" "$RCLONE_MNT/bench-link" 2>/dev/null || true
fi
bench_mr "readlink" readlink "$MNTRS_MNT/bench-link" 2>/dev/null || \
    readlink "$RCLONE_MNT/bench-link" 2>/dev/null || true

# ── 17. Large dir ops ────────────────────────────────────────────────
echo ""; echo "=== 17. Large dir ops ==="; CATEGORY="LargeDir"
bench_mr "ls -f many" ls "$MNTRS_MNT/many" 2>/dev/null || ls "$RCLONE_MNT/many" 2>/dev/null
bench_mr "find many"  find "$MNTRS_MNT/many" 2>/dev/null || find "$RCLONE_MNT/many" 2>/dev/null
bench_mr "rm many 10" \
    bash -c "cd '$MNTRS_MNT/many' && ls | head -10 | xargs rm -f" 2>/dev/null || true \
    || \
    bash -c "cd '$RCLONE_MNT/many' && ls | head -10 | xargs rm -f" 2>/dev/null || true

# ── 18. Concurrent reads ─────────────────────────────────────────────
echo ""; echo "=== 18. Concurrent reads ==="; CATEGORY="Concurrent"
for threads in 2 4 8; do
    for f in 1M.bin 10M.bin; do
        bench_mr "concurrent ${threads}x $f" \
            bash -c "for n in \$(seq 1 $threads); do dd if='$MNTRS_MNT/$f' bs=64K count=16 of=/dev/null 2>/dev/null & done; wait" \
            || \
            bash -c "for n in \$(seq 1 $threads); do dd if='$RCLONE_MNT/$f' bs=64K count=16 of=/dev/null 2>/dev/null & done; wait"
    done
done

# ── 19. Fstat ────────────────────────────────────────────────────────
echo ""; echo "=== 19. Fstat ==="; CATEGORY="Fstat"
bench_mr "fstat 1K" \
    bash -c "exec 3<'$MNTRS_MNT/1K.bin'; fstat \$3 2>/dev/null; exec 3>&-" \
    || \
    bash -c "exec 3<'$RCLONE_MNT/1K.bin'; fstat \$3 2>/dev/null; exec 3>&-"

# ── 20. Lseek ────────────────────────────────────────────────────────
echo ""; echo "=== 20. Lseek ==="; CATEGORY="Lseek"
bench_mr "lseek 100M" \
    bash -c "exec 3<'$MNTRS_MNT/100M.bin'; dd bs=1 seek=1000 count=0 of=/dev/null 2>/dev/null <&3; exec 3>&-" \
    || \
    bash -c "exec 3<'$RCLONE_MNT/100M.bin'; dd bs=1 seek=1000 count=0 of=/dev/null 2>/dev/null <&3; exec 3>&-"

# ── Issue 134 coverage (mntrs-only — no rclone pair adds value) ─────
# These sections test mntrs-specific behavior (writeback, fsync, append
# semantics) where a rclone comparison wouldn't add signal — the
# benchmarks are about catching mntrs regressions, not beating rclone.

echo ""; echo "=== 28. fsync ==="; CATEGORY="Fsync"
echo "test" > "$MNTRS_MNT/fsync-test.txt" 2>/dev/null
bench "dd conv=fsync 4K" "mntrs" dd if=/dev/zero of="$MNTRS_MNT/fsync-test.txt" bs=4K count=1 conv=fsync 2>/dev/null
rm -f "$MNTRS_MNT/fsync-test.txt" 2>/dev/null

echo ""; echo "=== 29. read-after-write same fd ==="; CATEGORY="ReadAfterWrite"
bench "write+read same fd" "mntrs" bash -c "exec 3<>'$MNTRS_MNT/raw-test.bin'; printf 'hello world' >&3; dd bs=11 count=1 <&3 2>/dev/null; exec 3>&-; rm -f '$MNTRS_MNT/raw-test.bin'"

echo ""; echo "=== 30. O_APPEND append ==="; CATEGORY="Append"
bench "append x3" "mntrs" bash -c "echo line1 > '$MNTRS_MNT/append-x3.txt'; echo line2 >> '$MNTRS_MNT/append-x3.txt'; echo line3 >> '$MNTRS_MNT/append-x3.txt'; wc -l < '$MNTRS_MNT/append-x3.txt' | tr -d ' ' | grep -q '^3\$'; rm -f '$MNTRS_MNT/append-x3.txt'"

echo ""; echo "=== 31. handle_caching: open/read/close loop ==="; CATEGORY="HandleCaching"
dd if=/dev/urandom of="$MNTRS_MNT/hc-file.bin" bs=64K count=1 2>/dev/null
bench "open/read/close x50" "mntrs" bash -c "for i in \$(seq 1 50); do dd if='$MNTRS_MNT/hc-file.bin' bs=4K count=1 of=/dev/null 2>/dev/null; done"
rm -f "$MNTRS_MNT/hc-file.bin" 2>/dev/null

echo ""; echo "=== 32. openat / fd reuse ==="; CATEGORY="FdReuse"
echo "seeded" > "$MNTRS_MNT/fd-test.txt" 2>/dev/null
bench "openat write+cat" "mntrs" bash -c "exec 3<>'$MNTRS_MNT/fd-test.txt'; echo appended >&3; exec 3>&-; cat '$MNTRS_MNT/fd-test.txt' >/dev/null; rm -f '$MNTRS_MNT/fd-test.txt'"

echo ""; echo "=== 33. Concurrent writes (4 threads, 4 files) ==="; CATEGORY="ConcurrentWrite"
bench "concurrent write x4" "mntrs" bash -c "for n in 1 2 3 4; do dd if=/dev/zero of='$MNTRS_MNT/cw-\$n.bin' bs=64K count=16 2>/dev/null & done; wait; for n in 1 2 3 4; do rm -f '$MNTRS_MNT/cw-\$n.bin'; done"

echo ""; echo "=== 34. writeback persistence (write → unmount → remount → read) ==="; CATEGORY="WritebackPersistence"
echo "persistence-test-data-$(date +%s)" > "$MNTRS_MNT/wb-persist.txt" 2>/dev/null
EXPECTED=$(cat "$MNTRS_MNT/wb-persist.txt" 2>/dev/null)
sync; sleep 2
umount -f "$MNTRS_MNT" 2>/dev/null; sleep 1
"$MNTRS_BIN" mount "s3://$BUCKET" "$MNTRS_MNT" \
    --opt "endpoint=$ENDPOINT" --opt "access-key=$ACCESS_KEY" \
    --opt "secret-key=$SECRET_KEY" --opt "region=$REGION" \
    --vfs-cache-mode=writes --vfs-write-back=5 \
    --daemon --daemon-wait --daemon-timeout=15 2>/dev/null
sleep 3
ACTUAL=$(cat "$MNTRS_MNT/wb-persist.txt" 2>/dev/null)
TOTAL=$((TOTAL + 1))
if [[ "$EXPECTED" == "$ACTUAL" ]] && [[ -n "$EXPECTED" ]]; then
    echo "  wb persist mntrs: OK (data survives remount)"
    PASS=$((PASS + 1))
    echo "wb-persist-OK|wb persist|mntrs|WritebackPersistence" >> "$RESULT_TMP"
else
    echo "  wb persist mntrs: FAIL (expected='$EXPECTED' actual='$ACTUAL')"
    FAIL=$((FAIL + 1))
    echo "FAIL|wb persist|mntrs|WritebackPersistence" >> "$RESULT_TMP"
fi
rm -f "$MNTRS_MNT/wb-persist.txt" 2>/dev/null

echo ""; echo "=== 35. Prefetcher warm cache (read 10M twice) ==="; CATEGORY="Prefetcher"
dd if=/dev/urandom of="$MNTRS_MNT/prefetch-10M.bin" bs=1M count=10 2>/dev/null
sync; sleep 2
bench "prefetch 1st read" "mntrs" dd if="$MNTRS_MNT/prefetch-10M.bin" bs=64K of=/dev/null 2>/dev/null
bench "prefetch 2nd read" "mntrs" dd if="$MNTRS_MNT/prefetch-10M.bin" bs=64K of=/dev/null 2>/dev/null
rm -f "$MNTRS_MNT/prefetch-10M.bin" 2>/dev/null

echo ""; echo "=== 36. mkdir -p deep 5 levels ==="; CATEGORY="MkdirDeep"
bench "mkdir -p a/b/c/d/e" "mntrs" mkdir -p "$MNTRS_MNT/mkdirp-deep/a/b/c/d/e" 2>/dev/null
rm -rf "$MNTRS_MNT/mkdirp-deep" 2>/dev/null

echo ""; echo "=== 37. Bulk stat (100 files) ==="; CATEGORY="BulkStat"
mkdir -p "$MNTRS_MNT/bulkstat" 2>/dev/null
for i in $(seq 1 100); do echo "bs_$i" > "$MNTRS_MNT/bulkstat/f_$i.txt"; done
sync; sleep 1
bench "stat x100" "mntrs" bash -c "for f in '$MNTRS_MNT'/bulkstat/f_*.txt; do stat \$f >/dev/null 2>&1; done"
rm -rf "$MNTRS_MNT/bulkstat" 2>/dev/null

echo ""; echo "=== 38. Prefetcher adaptive (issue #132) ==="; CATEGORY="PrefetcherAdaptive"
dd if=/dev/urandom of="$MNTRS_MNT/adaptive-100M.bin" bs=1M count=100 2>/dev/null
sync; sleep 2
bench "cat 100M cold"       "mntrs" cat "$MNTRS_MNT/adaptive-100M.bin" >/dev/null
bench "cat 100M warm"       "mntrs" cat "$MNTRS_MNT/adaptive-100M.bin" >/dev/null
bench "dd bs=1M 100M cold"  "mntrs" dd if="$MNTRS_MNT/adaptive-100M.bin" bs=1M of=/dev/null 2>/dev/null
rm -f "$MNTRS_MNT/adaptive-100M.bin" 2>/dev/null

# ── Render result table ──────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
echo ""
echo "=== FINAL TABLE ==="
# python3 is required for the markdown table renderer. On macOS 12.3+
# it's not preinstalled; this gate prevents silent result corruption
# (a missing python3 leaves the .md file empty and the user sees a
# "PASS=96" line with no per-test breakdown).
if command -v python3 >/dev/null 2>&1; then
    python3 "$REPO_ROOT/bench/render_table.py" "$RESULT_TMP" 2>&1 | tee -a "$RESULT_MD"
else
    echo "WARN: python3 not found; the rendered markdown table is unavailable." >&2
    echo "      Install via 'brew install python@3.12' or set PATH to include it." >&2
    echo "      Raw per-test data is preserved at: $RESULT_COPY" >&2
fi

# ── Final summary ────────────────────────────────────────────────────
ELAPSED=$(( $(date +%s) - START_TIME ))
echo "============================================"
printf " %d tests: %d passed, %d failed (%ds)\n" "$TOTAL" "$PASS" "$FAIL" "$ELAPSED"
echo "  result_tmp preserved at: $RESULT_COPY"
echo "  markdown table:          $RESULT_MD"
echo "============================================"
