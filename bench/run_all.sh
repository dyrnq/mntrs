#!/bin/bash
# mntrs vs rclone comprehensive benchmark (100+ tests, ~5 min)
# Usage: ./bench/run_all.sh
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

ENDPOINT="${ENDPOINT:-http://localhost:9000}"
ACCESS_KEY="${ACCESS_KEY:-minioadmin}"
SECRET_KEY="${SECRET_KEY:-minioadmin}"
BUCKET="${BUCKET:-bench-bucket}"
REGION="${REGION:-us-east-1}"
MNTRS_BIN="${MNTRS_BIN:-./target/release/mntrs}"
MNTRS_MNT="${MNTRS_MNT:-/tmp/mntrs-bench}"
RCLONE_MNT="${RCLONE_MNT:-/tmp/rclone-bench}"
MEM_MNT="/tmp/mntrs-mem-bench"
RESULT_TMP="$(mktemp /tmp/bench-results-XXXXXX)"

PASS=0
FAIL=0
TOTAL=0
START_TIME=$(date +%s)

cleanup() {
    fusermount3 -u "$MNTRS_MNT" 2>/dev/null || true
    fusermount3 -u "$RCLONE_MNT" 2>/dev/null || true
    fusermount3 -u "$MEM_MNT" 2>/dev/null || true
}
trap cleanup EXIT

bench() {
    local name="$1"; shift
    local mnt="$1"; shift
    TOTAL=$((TOTAL + 1))
    local out
    out=$({ time "$@" >/dev/null 2>&1; } 2>&1) || {
        printf "  %-35s | %15s | FAIL\n" "$name" "$mnt"
        echo "FAIL|$name|$mnt|$CATEGORY" >> "$RESULT_TMP"
        FAIL=$((FAIL + 1))
        return
    }
    local t=$(echo "$out" | grep real | awk '{print $2}')
    printf "  %-35s | %15s | %s\n" "$name" "$t" "OK"
    echo "$t|$name|$mnt|$CATEGORY" >> "$RESULT_TMP"
    PASS=$((PASS + 1))
}

echo "============================================"
echo " mntrs vs rclone benchmark"
echo " started: $(date -Iseconds)"
echo " endpoint: $ENDPOINT"
echo " bucket: $BUCKET"
echo "============================================"
echo ""

# ---- Prepare test data ----
echo "--- Preparing test data ---"
DATA_DIR="/tmp/mntrs-bench-data"
mkdir -p "$DATA_DIR"
dd if=/dev/urandom of="$DATA_DIR/1K.bin" bs=1K count=1 2>/dev/null
dd if=/dev/urandom of="$DATA_DIR/4K.bin" bs=4K count=1 2>/dev/null
dd if=/dev/urandom of="$DATA_DIR/64K.bin" bs=64K count=1 2>/dev/null
dd if=/dev/urandom of="$DATA_DIR/1M.bin" bs=1M count=1 2>/dev/null
dd if=/dev/urandom of="$DATA_DIR/10M.bin" bs=1M count=10 2>/dev/null
dd if=/dev/urandom of="$DATA_DIR/100M.bin" bs=1M count=100 2>/dev/null
# Many small files for dir listing test
mkdir -p "$DATA_DIR/many"
for i in $(seq 1 500); do
    echo "$i" > "$DATA_DIR/many/file_$(printf '%04d' "$i").txt"
done
echo "  data prepared: $(du -sh $DATA_DIR | awk '{print $1}')"

# ---- Mount mntrs + rclone ----
echo ""
echo "--- Mounting ---"
echo "  $(date -Iseconds): preparing mount dirs..."
mkdir -p "$MNTRS_MNT" "$RCLONE_MNT"

MEM_CACHE_IMPL="${MEM_CACHE_IMPL:-dashmap}"
# Write mount for mntrs (default: writes cache mode)
echo "  $(date -Iseconds): starting mntrs mount (mem-cache-impl=$MEM_CACHE_IMPL)..."
"$MNTRS_BIN" mount "s3://$BUCKET" "$MNTRS_MNT" \
    --opt "endpoint=$ENDPOINT" --opt "access-key=$ACCESS_KEY" \
    --opt "secret-key=$SECRET_KEY" --opt "region=$REGION" \
    --vfs-cache-mode=writes --vfs-write-back=5 \
    --vfs-read-ahead=134217728 --async-read \
    --mem-cache-impl="$MEM_CACHE_IMPL" \
    --daemon --daemon-wait --daemon-timeout=15 2>&1
echo "  $(date -Iseconds): mntrs mount returned (exit=$?)"

# rclone mount (writes cache mode for fair comparison)
echo "  $(date -Iseconds): setting up rclone config..."
rclone config create bench s3 provider Minio \
    access_key_id $ACCESS_KEY secret_access_key $SECRET_KEY \
    endpoint $ENDPOINT region $REGION 2>/dev/null
echo "  $(date -Iseconds): starting rclone mount..."
rclone mount bench:$BUCKET "$RCLONE_MNT" --daemon --vfs-cache-mode=writes --vfs-write-back=5s --log-file /tmp/rclone-bench.log --log-level INFO 2>&1
echo "  $(date -Iseconds): rclone mount returned (exit=$?)"

sleep 3
echo "  $(date -Iseconds): checking mounts..."
if mountpoint -q "$MNTRS_MNT"; then
  echo "  mntrs mount: OK"
else
  echo "  mntrs mount: FAILED — mount table:"
  mount | grep mntrs || true
  echo "  mntrs mount: FAILED (check errors above)"
fi
if mountpoint -q "$RCLONE_MNT" || mount | grep -q "$RCLONE_MNT"; then
  echo "  rclone mount: OK"
else
  echo "  rclone mount: FAILED — mount table:"
  mount | grep rclone || true
  echo "  rclone mount: FAILED"
fi
echo "  mounts ready"
echo ""

# ---- Upload test data via S3 API (same protocol as mntrs/rclone) ----
echo "--- Uploading test data ---"
uv tool install awscli 2>/dev/null || pip3 install awscli 2>/dev/null || true
AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY"   aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3 mb "s3://$BUCKET" 2>/dev/null || true
rm -rf /tmp/bench-upload
mkdir -p /tmp/bench-upload
cp "$DATA_DIR"/1K.bin "$DATA_DIR"/4K.bin "$DATA_DIR"/64K.bin "$DATA_DIR"/1M.bin "$DATA_DIR"/10M.bin "$DATA_DIR"/100M.bin /tmp/bench-upload/
mkdir -p /tmp/bench-upload/many && cp -r "$DATA_DIR"/many/* /tmp/bench-upload/many/
AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY"   aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3 sync /tmp/bench-upload/ "s3://$BUCKET/" 2>&1
echo "  upload done"
echo ""

# ---- Warmup (with timeout, S3 mount may hang) ----
timeout 10 ls "$MNTRS_MNT"/ >/dev/null 2>&1 || echo "  warmup: mntrs ls hung"
timeout 10 ls "$RCLONE_MNT"/ >/dev/null 2>&1 || echo "  warmup: rclone ls hung"
timeout 10 cat "$MNTRS_MNT/1K.bin" >/dev/null 2>&1 || echo "  warmup: mntrs cat hung"
timeout 10 cat "$RCLONE_MNT/1K.bin" >/dev/null 2>&1 || echo "  warmup: rclone cat hung"

# ============================================================
# Test categories
# ============================================================

# Dir listing
echo "=== 1. Directory listing ==="
CATEGORY="DirList"
for d in "" "many"; do
    bench "ls $d" "mntrs" ls "$MNTRS_MNT/$d"
    bench "ls $d" "rclone" ls "$RCLONE_MNT/$d"
    bench "ls -la $d" "mntrs" ls -la "$MNTRS_MNT/$d"
    bench "ls -la $d" "rclone" ls -la "$RCLONE_MNT/$d"
done
for d in "" "many"; do
    bench "find $d maxdepth1" "mntrs" find "$MNTRS_MNT/$d" -maxdepth 1
    bench "find $d maxdepth1" "rclone" find "$RCLONE_MNT/$d" -maxdepth 1
done

# Stat
echo ""
echo "=== 2. Stat ==="
CATEGORY="Stat"
for f in 1K.bin 4K.bin 64K.bin 1M.bin 10M.bin 100M.bin; do
    bench "stat $f" "mntrs" stat "$MNTRS_MNT/$f"
    bench "stat $f" "rclone" stat "$RCLONE_MNT/$f"
done
for f in 1K.bin 4K.bin; do
    for i in 1 10 100; do
        bench "stat ${f}x${i}" "mntrs" bash -c "for n in \$(seq 1 $i); do stat '$MNTRS_MNT/$f' >/dev/null; done"
        bench "stat ${f}x${i}" "rclone" bash -c "for n in \$(seq 1 $i); do stat '$RCLONE_MNT/$f' >/dev/null; done"
    done
done

# Read
echo ""
echo "=== 3. Sequential read ==="
CATEGORY="SeqRead"
for f in 1K.bin 4K.bin 64K.bin 1M.bin 10M.bin 100M.bin; do
    bench "cat $f" "mntrs" cat "$MNTRS_MNT/$f"
    bench "cat $f" "rclone" cat "$RCLONE_MNT/$f"
done
for f in 1K.bin 4K.bin; do
    for i in 10 100; do
        bench "cat ${f}x${i}" "mntrs" bash -c "for n in \$(seq 1 $i); do cat '$MNTRS_MNT/$f' >/dev/null; done"
        bench "cat ${f}x${i}" "rclone" bash -c "for n in \$(seq 1 $i); do cat '$RCLONE_MNT/$f' >/dev/null; done"
    done
done

# Read with different block sizes (dd)
echo ""
echo "=== 4. Read via dd (block size variation) ==="
CATEGORY="ddRead"
for bs in 512 4096 65536 1048576; do
    for f in 1M.bin 10M.bin; do
        bench "dd bs=${bs} $f" "mntrs" dd if="$MNTRS_MNT/$f" bs=$bs of=/dev/null 2>/dev/null
        bench "dd bs=${bs} $f" "rclone" dd if="$RCLONE_MNT/$f" bs=$bs of=/dev/null 2>/dev/null
    done
done

# Random read
echo ""
echo "=== 5. Random read ==="
CATEGORY="RandRead"
for f in 1M.bin 10M.bin; do
    for seeks in 10 50; do
        bench "random ${seeks}x $f" "mntrs" bash -c "
            sz=\$(stat -c%s '$MNTRS_MNT/$f' 2>/dev/null || echo 1048576)
            for n in \$(seq 1 $seeks); do
                off=\$((RANDOM % sz))
                dd if='$MNTRS_MNT/$f' bs=1 count=1 skip=\$off of=/dev/null 2>/dev/null
            done
        "
        bench "random ${seeks}x $f" "rclone" bash -c "
            sz=\$(stat -c%s '$RCLONE_MNT/$f' 2>/dev/null || echo 1048576)
            for n in \$(seq 1 $seeks); do
                off=\$((RANDOM % sz))
                dd if='$RCLONE_MNT/$f' bs=1 count=1 skip=\$off of=/dev/null 2>/dev/null
            done
        "
    done
done

# Write
echo ""
echo "=== 6. Write ==="
CATEGORY="Write"
for sz in 1K 4K 64K 1M; do
    src="$DATA_DIR/${sz}.bin"
    bench "write $sz new" "mntrs" cp "$src" "$MNTRS_MNT/bench-write-${sz}.bin"
    bench "write $sz new" "rclone" cp "$src" "$RCLONE_MNT/bench-write-${sz}.bin"
done
# Overwrite existing
for sz in 1K 4K; do
    bench "write $sz overwrite" "mntrs" cp "$DATA_DIR/${sz}.bin" "$MNTRS_MNT/1K.bin"
    bench "write $sz overwrite" "rclone" cp "$DATA_DIR/${sz}.bin" "$RCLONE_MNT/1K.bin"
done

# Mkdir / Rmdir / Unlink
echo ""
echo "=== 7. Dir/File ops ==="
CATEGORY="DirOps"
bench "mkdir" "mntrs" mkdir -p "$MNTRS_MNT/bench-dir"
bench "mkdir" "rclone" mkdir -p "$RCLONE_MNT/bench-dir"
bench "rmdir" "mntrs" rmdir "$MNTRS_MNT/bench-dir"
bench "rmdir" "rclone" rmdir "$RCLONE_MNT/bench-dir"
bench "unlink" "mntrs" rm -f "$MNTRS_MNT/bench-unlink-test"
bench "unlink" "rclone" rm -f "$RCLONE_MNT/bench-unlink-test"
touch "$MNTRS_MNT/bench-unlink-test" "$RCLONE_MNT/bench-unlink-test" 2>/dev/null
bench "unlink exist" "mntrs" rm -f "$MNTRS_MNT/bench-unlink-test"
bench "unlink exist" "rclone" rm -f "$RCLONE_MNT/bench-unlink-test"

# Rename
echo ""
echo "=== 8. Rename ==="
CATEGORY="Rename"
cp "$DATA_DIR/1K.bin" "$MNTRS_MNT/bench-rename-src" 2>/dev/null
cp "$DATA_DIR/1K.bin" "$RCLONE_MNT/bench-rename-src" 2>/dev/null
bench "rename" "mntrs" mv "$MNTRS_MNT/bench-rename-src" "$MNTRS_MNT/bench-rename-dst"
bench "rename" "rclone" mv "$RCLONE_MNT/bench-rename-src" "$RCLONE_MNT/bench-rename-dst"

# Truncate
echo ""
echo "=== 9. Truncate ==="
CATEGORY="Truncate"
cp "$DATA_DIR/10M.bin" "$MNTRS_MNT/bench-trunc" 2>/dev/null
cp "$DATA_DIR/10M.bin" "$RCLONE_MNT/bench-trunc" 2>/dev/null
bench "truncate 0" "mntrs" truncate -s 0 "$MNTRS_MNT/bench-trunc"
bench "truncate 0" "rclone" truncate -s 0 "$RCLONE_MNT/bench-trunc"
bench "truncate 1M" "mntrs" truncate -s 1M "$MNTRS_MNT/bench-trunc"
bench "truncate 1M" "rclone" truncate -s 1M "$RCLONE_MNT/bench-trunc"

# Xattr
echo ""
echo "=== 10. Xattr ==="
CATEGORY="Xattr"
bench "getfattr" "mntrs" getfattr -d "$MNTRS_MNT/1K.bin" 2>/dev/null || true
bench "getfattr" "rclone" getfattr -d "$RCLONE_MNT/1K.bin" 2>/dev/null || true

# memory backend baseline (zero network, single mount)
echo ""
echo "=== 11. Memory backend (zero network) ==="
CATEGORY="Memory"
mkdir -p "$MEM_MNT"
"$MNTRS_BIN" mount "memory://" "$MEM_MNT" \
    --daemon --daemon-wait --daemon-timeout=10 2>/dev/null
sleep 2
bench "stat mem" "mem" stat "$MEM_MNT"
echo "  (memory backend — for reference only)"

# Additional read tests (included in comparison table)
echo ""
echo "=== 12. Head/tail reads ==="
CATEGORY="HeadTail"
for f in 10M.bin 100M.bin; do
    for n in 1 10 100; do
        bench "head -c${n}K $f" "mntrs" head -c "${n}K" "$MNTRS_MNT/$f"
        bench "head -c${n}K $f" "rclone" head -c "${n}K" "$RCLONE_MNT/$f"
    done
done

# Md5sum / sha
echo ""
echo "=== 13. Checksum ==="
CATEGORY="Checksum"
for f in 1K.bin 4K.bin 64K.bin 1M.bin; do
    bench "md5sum $f" "mntrs" md5sum "$MNTRS_MNT/$f"
    bench "md5sum $f" "rclone" md5sum "$RCLONE_MNT/$f"
    bench "sha1sum $f" "mntrs" sha1sum "$MNTRS_MNT/$f"
    bench "sha1sum $f" "rclone" sha1sum "$RCLONE_MNT/$f"
done

# Touch (create empty, update mtime)
echo ""
echo "=== 14. Touch ==="
CATEGORY="Touch"
bench "touch new" "mntrs" touch "$MNTRS_MNT/bench-touch-new"
bench "touch new" "rclone" touch "$RCLONE_MNT/bench-touch-new"
bench "touch exist" "mntrs" touch "$MNTRS_MNT/1K.bin"
bench "touch exist" "rclone" touch "$RCLONE_MNT/1K.bin"

# Chmod (where supported)
echo ""
echo "=== 15. Chmod ==="
CATEGORY="Chmod"
bench "chmod" "mntrs" chmod 0644 "$MNTRS_MNT/1K.bin" 2>/dev/null || true
bench "chmod" "rclone" chmod 0644 "$RCLONE_MNT/1K.bin" 2>/dev/null || true

# Hardlink / Symlink (if supported)
echo ""
echo "=== 16. Symlink ==="
CATEGORY="Symlink"
ln -sf "$MNTRS_MNT/1K.bin" "$MNTRS_MNT/bench-link" 2>/dev/null || true
ln -sf "$RCLONE_MNT/1K.bin" "$RCLONE_MNT/bench-link" 2>/dev/null || true
bench "readlink" "mntrs" readlink "$MNTRS_MNT/bench-link" 2>/dev/null || true
bench "readlink" "rclone" readlink "$RCLONE_MNT/bench-link" 2>/dev/null || true

# Dir with 500 files
echo ""
echo "=== 17. Large dir ops ==="
CATEGORY="LargeDir"
bench "ls -f many" "mntrs" ls "$MNTRS_MNT/many" 2>/dev/null
bench "ls -f many" "rclone" ls "$RCLONE_MNT/many" 2>/dev/null
bench "find many" "mntrs" find "$MNTRS_MNT/many" 2>/dev/null
bench "find many" "rclone" find "$RCLONE_MNT/many" 2>/dev/null
bench "rm many 10" "mntrs" bash -c "cd '$MNTRS_MNT/many' && ls | head -10 | xargs rm -f" 2>/dev/null || true
bench "rm many 10" "rclone" bash -c "cd '$RCLONE_MNT/many' && ls | head -10 | xargs rm -f" 2>/dev/null || true

# Concurrent reads
echo ""
echo "=== 18. Concurrent reads ==="
CATEGORY="Concurrent"
for threads in 2 4 8; do
    for f in 1M.bin 10M.bin; do
        bench "concurrent ${threads}x $f" "mntrs" bash -c "
            for n in \$(seq 1 $threads); do
                dd if='$MNTRS_MNT/$f' bs=64K count=16 of=/dev/null 2>/dev/null &
            done
            wait
        "
        bench "concurrent ${threads}x $f" "rclone" bash -c "
            for n in \$(seq 1 $threads); do
                dd if='$RCLONE_MNT/$f' bs=64K count=16 of=/dev/null 2>/dev/null &
            done
            wait
        "
    done
done

# Fstat (stat by fd)
echo ""
echo "=== 19. Fstat ==="
CATEGORY="Fstat"
bench "fstat 1K" "mntrs" bash -c "exec 3<'$MNTRS_MNT/1K.bin'; fstat \$3 2>/dev/null; exec 3>&-"
bench "fstat 1K" "rclone" bash -c "exec 3<'$RCLONE_MNT/1K.bin'; fstat \$3 2>/dev/null; exec 3>&-"

# Lseek
echo ""
echo "=== 20. Lseek ==="
CATEGORY="Lseek"
bench "lseek 100M" "mntrs" bash -c "exec 3<'$MNTRS_MNT/100M.bin'; dd bs=1 seek=1000 count=0 of=/dev/null 2>/dev/null <&3; exec 3>&-"
bench "lseek 100M" "rclone" bash -c "exec 3<'$RCLONE_MNT/100M.bin'; dd bs=1 seek=1000 count=0 of=/dev/null 2>/dev/null <&3; exec 3>&-"

# ---- Table ----
python3 "$SCRIPT_DIR/render_table.py" "$RESULT_TMP"
if [ -f /tmp/rclone-bench.log ]; then echo "=== rclone log ===" >> "$RESULT_TMP"; cat /tmp/rclone-bench.log >> "$RESULT_TMP"; fi
rm -f "$RESULT_TMP"

# ---- Summary ----
echo ""
ELAPSED=$(( $(date +%s) - START_TIME ))
echo "============================================"
printf " %d tests: %d passed, %d failed (%ds)\n" "$TOTAL" "$PASS" "$FAIL" "$ELAPSED"
echo "============================================"

# ============================================================
# rm -rf multi-dimensional benchmarks (issue #134)
# ============================================================
_recreate_rm_data() {
    local label="$1"
    case "$label" in
        single)
            echo "single" > "$MNTRS_MNT/rmtest_single.txt" 2>/dev/null
            echo "single" > "$RCLONE_MNT/rmtest_single.txt" 2>/dev/null ;;
        empty_dir)
            rmdir "$MNTRS_MNT/rmtest_empty" 2>/dev/null; mkdir -p "$MNTRS_MNT/rmtest_empty" 2>/dev/null
            rmdir "$RCLONE_MNT/rmtest_empty" 2>/dev/null; mkdir -p "$RCLONE_MNT/rmtest_empty" 2>/dev/null ;;
        small_10)
            rm -rf "$MNTRS_MNT/rmtest_small_10" 2>/dev/null; mkdir -p "$MNTRS_MNT/rmtest_small_10"; rm -rf "$RCLONE_MNT/rmtest_small_10" 2>/dev/null; mkdir -p "$RCLONE_MNT/rmtest_small_10"; for i in $(seq 1 10); do echo "s$i" > "$MNTRS_MNT/rmtest_small_10/f_$i.txt"; echo "s$i" > "$RCLONE_MNT/rmtest_small_10/f_$i.txt"; done ;;
        shallow_100)
            rm -rf "$MNTRS_MNT/rmtest_shallow_100" 2>/dev/null; mkdir -p "$MNTRS_MNT/rmtest_shallow_100"; rm -rf "$RCLONE_MNT/rmtest_shallow_100" 2>/dev/null; mkdir -p "$RCLONE_MNT/rmtest_shallow_100"; for i in $(seq 1 100); do echo "sh100_$i" > "$MNTRS_MNT/rmtest_shallow_100/f_$(printf '%04d' "$i").txt"; echo "sh100_$i" > "$RCLONE_MNT/rmtest_shallow_100/f_$(printf '%04d' "$i").txt"; done ;;
        shallow_500)
            rm -rf "$MNTRS_MNT/rmtest_shallow_500" 2>/dev/null; mkdir -p "$MNTRS_MNT/rmtest_shallow_500"; rm -rf "$RCLONE_MNT/rmtest_shallow_500" 2>/dev/null; mkdir -p "$RCLONE_MNT/rmtest_shallow_500"; for i in $(seq 1 500); do echo "sh500_$i" > "$MNTRS_MNT/rmtest_shallow_500/f_$(printf '%04d' "$i").txt"; echo "sh500_$i" > "$RCLONE_MNT/rmtest_shallow_500/f_$(printf '%04d' "$i").txt"; done ;;
        deep)
            rm -rf "$MNTRS_MNT/rmtest_deep_3" 2>/dev/null; rm -rf "$RCLONE_MNT/rmtest_deep_3" 2>/dev/null
            D="$MNTRS_MNT/rmtest_deep_3"; R="$RCLONE_MNT/rmtest_deep_3"
            mkdir -p "$D/a/b/c" "$D/d/e/f" "$R/a/b/c" "$R/d/e/f" 2>/dev/null
            for sub in "$D/a" "$D/a/b" "$D/a/b/c" "$D/d" "$D/d/e" "$D/d/e/f"; do for j in $(seq 1 10); do echo "deep_$j" > "$sub/f_$j.txt"; done; done
            for sub in "$R/a" "$R/a/b" "$R/a/b/c" "$R/d" "$R/d/e" "$R/d/e/f"; do for j in $(seq 1 10); do echo "deep_$j" > "$sub/f_$j.txt"; done; done ;;
        mixed)
            rm -rf "$MNTRS_MNT/rmtest_mixed" 2>/dev/null; rm -rf "$RCLONE_MNT/rmtest_mixed" 2>/dev/null
            mkdir -p "$MNTRS_MNT/rmtest_mixed" "$RCLONE_MNT/rmtest_mixed" 2>/dev/null
            for i in $(seq 1 50); do dd if=/dev/urandom of="$MNTRS_MNT/rmtest_mixed/s_$(printf '%04d' "$i").bin" bs=4K count=1 2>/dev/null; dd if=/dev/urandom of="$RCLONE_MNT/rmtest_mixed/s_$(printf '%04d' "$i").bin" bs=4K count=1 2>/dev/null; done
            dd if=/dev/urandom of="$MNTRS_MNT/rmtest_mixed/large_1.bin" bs=1M count=10 2>/dev/null; dd if=/dev/urandom of="$MNTRS_MNT/rmtest_mixed/large_2.bin" bs=1M count=5 2>/dev/null
            dd if=/dev/urandom of="$RCLONE_MNT/rmtest_mixed/large_1.bin" bs=1M count=10 2>/dev/null; dd if=/dev/urandom of="$RCLONE_MNT/rmtest_mixed/large_2.bin" bs=1M count=5 2>/dev/null ;;
        nested_empty)
            rm -rf "$MNTRS_MNT/rmtest_nested_empty" 2>/dev/null; rm -rf "$RCLONE_MNT/rmtest_nested_empty" 2>/dev/null
            mkdir -p "$MNTRS_MNT/rmtest_nested_empty/a/b/c" "$RCLONE_MNT/rmtest_nested_empty/a/b/c" ;;
    esac; sync; sleep 1
}

echo ""; echo "=== 21. rm -rf: unlink baseline ==="; CATEGORY="RmRf"
_recreate_rm_data single
bench "rm single file" "mntrs" rm -f "$MNTRS_MNT/rmtest_single.txt"
bench "rm single file" "rclone" rm -f "$RCLONE_MNT/rmtest_single.txt"

echo ""; echo "=== 22. rm -rf: empty directory ==="; CATEGORY="RmRf"
_recreate_rm_data empty_dir
bench "rmdir empty" "mntrs" rmdir "$MNTRS_MNT/rmtest_empty"
bench "rmdir empty" "rclone" rmdir "$RCLONE_MNT/rmtest_empty"
_recreate_rm_data nested_empty
bench "rm -rf nested_empty" "mntrs" rm -rf "$MNTRS_MNT/rmtest_nested_empty"
bench "rm -rf nested_empty" "rclone" rm -rf "$RCLONE_MNT/rmtest_nested_empty"

echo ""; echo "=== 23. rm -rf: small batch (10 files) ==="; CATEGORY="RmRf"
_recreate_rm_data small_10
bench "rm -rf 10 files" "mntrs" rm -rf "$MNTRS_MNT/rmtest_small_10"
bench "rm -rf 10 files" "rclone" rm -rf "$RCLONE_MNT/rmtest_small_10"

echo ""; echo "=== 24. rm -rf: shallow 100 files ==="; CATEGORY="RmRf"
_recreate_rm_data shallow_100
bench "rm -rf 100 files" "mntrs" rm -rf "$MNTRS_MNT/rmtest_shallow_100"
bench "rm -rf 100 files" "rclone" rm -rf "$RCLONE_MNT/rmtest_shallow_100"

echo ""; echo "=== 25. rm -rf: shallow 500 files ==="; CATEGORY="RmRf"
_recreate_rm_data shallow_500
bench "rm -rf 500 files" "mntrs" rm -rf "$MNTRS_MNT/rmtest_shallow_500"
bench "rm -rf 500 files" "rclone" rm -rf "$RCLONE_MNT/rmtest_shallow_500"

echo ""; echo "=== 26. rm -rf: 3-level deep directory tree ==="; CATEGORY="RmRf"
_recreate_rm_data deep
bench "rm -rf deep tree (60 files)" "mntrs" rm -rf "$MNTRS_MNT/rmtest_deep_3"
bench "rm -rf deep tree (60 files)" "rclone" rm -rf "$RCLONE_MNT/rmtest_deep_3"

echo ""; echo "=== 27. rm -rf: mixed small+large files (52 files, ~15M) ==="; CATEGORY="RmRf"
_recreate_rm_data mixed
bench "rm -rf mixed (52 files 15M)" "mntrs" rm -rf "$MNTRS_MNT/rmtest_mixed"
bench "rm -rf mixed (52 files 15M)" "rclone" rm -rf "$RCLONE_MNT/rmtest_mixed"

# ============================================================
# Issue #134: coverage gaps (fsync, append, read-after-write, etc.)
# ============================================================

echo ""; echo "=== 28. fsync / fsyncdir ==="; CATEGORY="Fsync"
echo "test" > "$MNTRS_MNT/fsync-test.txt" 2>/dev/null; echo "test" > "$RCLONE_MNT/fsync-test.txt" 2>/dev/null
bench "dd conv=fsync 4K" "mntrs" dd if=/dev/zero of="$MNTRS_MNT/fsync-test.txt" bs=4K count=1 conv=fsync 2>/dev/null
bench "dd conv=fsync 4K" "rclone" dd if=/dev/zero of="$RCLONE_MNT/fsync-test.txt" bs=4K count=1 conv=fsync 2>/dev/null
rm -f "$MNTRS_MNT/fsync-test.txt" "$RCLONE_MNT/fsync-test.txt" 2>/dev/null

echo ""; echo "=== 29. Read-after-write same fd ==="; CATEGORY="ReadAfterWrite"
bench "write+read same fd" "mntrs" bash -c "exec 3<>'$MNTRS_MNT/raw-test.bin'; printf 'hello world' >&3; dd bs=11 count=1 <&3 2>/dev/null; exec 3>&-; rm -f '$MNTRS_MNT/raw-test.bin'"
bench "write+read same fd" "rclone" bash -c "exec 3<>'$RCLONE_MNT/raw-test.bin'; printf 'hello world' >&3; dd bs=11 count=1 <&3 2>/dev/null; exec 3>&-; rm -f '$RCLONE_MNT/raw-test.bin'"
echo "hello" > "$MNTRS_MNT/raw-sep.txt" 2>/dev/null; bench "echo+cat sep" "mntrs" cat "$MNTRS_MNT/raw-sep.txt" >/dev/null; rm -f "$MNTRS_MNT/raw-sep.txt"
echo "hello" > "$RCLONE_MNT/raw-sep.txt" 2>/dev/null; bench "echo+cat sep" "rclone" cat "$RCLONE_MNT/raw-sep.txt" >/dev/null; rm -f "$RCLONE_MNT/raw-sep.txt"

echo ""; echo "=== 30. O_APPEND append ==="; CATEGORY="Append"
bench "append x3 mntrs" "mntrs" bash -c "echo line1 > '$MNTRS_MNT/append-x3.txt'; echo line2 >> '$MNTRS_MNT/append-x3.txt'; echo line3 >> '$MNTRS_MNT/append-x3.txt'; wc -l < '$MNTRS_MNT/append-x3.txt' | grep -q 3; rm -f '$MNTRS_MNT/append-x3.txt'"
bench "append x3 rclone" "rclone" bash -c "echo line1 > '$RCLONE_MNT/append-x3.txt'; echo line2 >> '$RCLONE_MNT/append-x3.txt'; echo line3 >> '$RCLONE_MNT/append-x3.txt'; wc -l < '$RCLONE_MNT/append-x3.txt' | grep -q 3; rm -f '$RCLONE_MNT/append-x3.txt'"

echo ""; echo "=== 31. handle_caching: open/read/close loop ==="; CATEGORY="HandleCaching"
dd if=/dev/urandom of="$MNTRS_MNT/hc-file.bin" bs=64K count=1 2>/dev/null; dd if=/dev/urandom of="$RCLONE_MNT/hc-file.bin" bs=64K count=1 2>/dev/null
bench "open/read/close x50" "mntrs" bash -c "for i in \$(seq 1 50); do dd if='$MNTRS_MNT/hc-file.bin' bs=4K count=1 of=/dev/null 2>/dev/null; done"
bench "open/read/close x50" "rclone" bash -c "for i in \$(seq 1 50); do dd if='$RCLONE_MNT/hc-file.bin' bs=4K count=1 of=/dev/null 2>/dev/null; done"
rm -f "$MNTRS_MNT/hc-file.bin" "$RCLONE_MNT/hc-file.bin" 2>/dev/null

echo ""; echo "=== 32. openat / fd reuse ==="; CATEGORY="FdReuse"
echo "seeded" > "$MNTRS_MNT/fd-test.txt" 2>/dev/null; echo "seeded" > "$RCLONE_MNT/fd-test.txt" 2>/dev/null
bench "openat write+cat" "mntrs" bash -c "exec 3<>'$MNTRS_MNT/fd-test.txt'; echo appended >&3; exec 3>&-; cat '$MNTRS_MNT/fd-test.txt' >/dev/null; rm -f '$MNTRS_MNT/fd-test.txt'"
bench "openat write+cat" "rclone" bash -c "exec 3<>'$RCLONE_MNT/fd-test.txt'; echo appended >&3; exec 3>&-; cat '$RCLONE_MNT/fd-test.txt' >/dev/null; rm -f '$RCLONE_MNT/fd-test.txt'"

echo ""; echo "=== 33. Concurrent writes (4 threads, 4 files) ==="; CATEGORY="ConcurrentWrite"
bench "concurrent write x4" "mntrs" bash -c "for n in 1 2 3 4; do dd if=/dev/zero of='$MNTRS_MNT/cw-\$n.bin' bs=64K count=16 2>/dev/null & done; wait; for n in 1 2 3 4; do rm -f '$MNTRS_MNT/cw-\$n.bin'; done"
bench "concurrent write x4" "rclone" bash -c "for n in 1 2 3 4; do dd if=/dev/zero of='$RCLONE_MNT/cw-\$n.bin' bs=64K count=16 2>/dev/null & done; wait; for n in 1 2 3 4; do rm -f '$RCLONE_MNT/cw-\$n.bin'; done"

echo ""; echo "=== 34. Writeback persistence (write -> unmount -> remount -> read) ==="; CATEGORY="WritebackPersistence"
echo "persistence-test-data-$(date +%s)" > "$MNTRS_MNT/wb-persist.txt" 2>/dev/null
EXPECTED=$(cat "$MNTRS_MNT/wb-persist.txt" 2>/dev/null)
sync; sleep 2
fusermount3 -u "$MNTRS_MNT" 2>/dev/null; sleep 1
"$MNTRS_BIN" mount "s3://$BUCKET" "$MNTRS_MNT" --opt "endpoint=$ENDPOINT" --opt "access-key=$ACCESS_KEY" --opt "secret-key=$SECRET_KEY" --opt "region=$REGION" --vfs-cache-mode=writes --vfs-write-back=5 --daemon --daemon-wait --daemon-timeout=15 2>/dev/null
sleep 3
ACTUAL=$(cat "$MNTRS_MNT/wb-persist.txt" 2>/dev/null)
if [ "$EXPECTED" = "$ACTUAL" ] && [ -n "$EXPECTED" ]; then bench "wb persist mntrs" "mntrs" true
else echo "  wb persist mntrs: FAIL (expected='$EXPECTED' actual='$ACTUAL')"; echo "FAIL|wb persist mntrs|mntrs|WritebackPersistence" >> "$RESULT_TMP"; FAIL=$((FAIL + 1)); TOTAL=$((TOTAL + 1)); fi
rm -f "$MNTRS_MNT/wb-persist.txt" 2>/dev/null

echo ""; echo "=== 35. Prefetcher warm cache (read 10M twice) ==="; CATEGORY="Prefetcher"
dd if=/dev/urandom of="$MNTRS_MNT/prefetch-10M.bin" bs=1M count=10 2>/dev/null; dd if=/dev/urandom of="$RCLONE_MNT/prefetch-10M.bin" bs=1M count=10 2>/dev/null; sync; sleep 2
bench "prefetch 1st read" "mntrs" dd if="$MNTRS_MNT/prefetch-10M.bin" bs=64K of=/dev/null 2>/dev/null
bench "prefetch 1st read" "rclone" dd if="$RCLONE_MNT/prefetch-10M.bin" bs=64K of=/dev/null 2>/dev/null
bench "prefetch 2nd read" "mntrs" dd if="$MNTRS_MNT/prefetch-10M.bin" bs=64K of=/dev/null 2>/dev/null
bench "prefetch 2nd read" "rclone" dd if="$RCLONE_MNT/prefetch-10M.bin" bs=64K of=/dev/null 2>/dev/null
rm -f "$MNTRS_MNT/prefetch-10M.bin" "$RCLONE_MNT/prefetch-10M.bin" 2>/dev/null

echo ""; echo "=== 36. mkdir -p deep 5 levels ==="; CATEGORY="MkdirDeep"
bench "mkdir -p a/b/c/d/e" "mntrs" mkdir -p "$MNTRS_MNT/mkdirp-deep/a/b/c/d/e" 2>/dev/null
bench "mkdir -p a/b/c/d/e" "rclone" mkdir -p "$RCLONE_MNT/mkdirp-deep/a/b/c/d/e" 2>/dev/null
rm -rf "$MNTRS_MNT/mkdirp-deep" "$RCLONE_MNT/mkdirp-deep" 2>/dev/null

echo ""; echo "=== 37. Bulk stat (100 files) ==="; CATEGORY="BulkStat"
mkdir -p "$MNTRS_MNT/bulkstat" "$RCLONE_MNT/bulkstat" 2>/dev/null
for i in $(seq 1 100); do echo "bs_$i" > "$MNTRS_MNT/bulkstat/f_$i.txt"; echo "bs_$i" > "$RCLONE_MNT/bulkstat/f_$i.txt"; done
sync; sleep 1
bench "stat x100" "mntrs" bash -c "for f in '$MNTRS_MNT'/bulkstat/f_*.txt; do stat \$f >/dev/null 2>&1; done"
bench "stat x100" "rclone" bash -c "for f in '$RCLONE_MNT'/bulkstat/f_*.txt; do stat \$f >/dev/null 2>&1; done"
rm -rf "$MNTRS_MNT/bulkstat" "$RCLONE_MNT/bulkstat" 2>/dev/null
