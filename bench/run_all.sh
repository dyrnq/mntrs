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
        FAIL=$((FAIL + 1))
        return
    }
    local t=$(echo "$out" | grep real | awk '{print $2}')
    printf "  %-35s | %15s | %s\n" "$name" "$t" "OK"
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
mkdir -p "$MNTRS_MNT" "$RCLONE_MNT"

# Write mount for mntrs (default: writes cache mode)
"$MNTRS_BIN" mount "s3://$BUCKET" "$MNTRS_MNT" \
    --opt "endpoint=$ENDPOINT" --opt "access-key=$ACCESS_KEY" \
    --opt "secret-key=$SECRET_KEY" --opt "region=$REGION" \
    --vfs-cache-mode=writes --vfs-write-back=5 \
    --daemon --daemon-wait --daemon-timeout=15

# rclone mount (writes cache mode for fair comparison)
rclone mount --daemon --vfs-cache-mode=writes --vfs-write-back=5s \
    :s3,provider=Minio,access_key_id=$ACCESS_KEY,secret_access_key=$SECRET_KEY,endpoint=$ENDPOINT,region=$REGION:$BUCKET \
    "$RCLONE_MNT" 2>/dev/null

sleep 3
if mountpoint -q "$MNTRS_MNT" 2>/dev/null; then
  echo "  mntrs mount: OK"
else
  echo "  mntrs mount: FAILED (check errors above)"
fi
if mountpoint -q "$RCLONE_MNT" 2>/dev/null; then
  echo "  rclone mount: OK"
else
  echo "  rclone mount: FAILED"
fi
echo "  mounts ready"
echo ""

# ---- Upload test data (skip if data already in bucket) ----
echo "--- Checking test data ---"
# Quick check: if 1K.bin already exists, skip upload (CI may have pre-uploaded)
if curl -sfI "$ENDPOINT/$BUCKET/1K.bin" >/dev/null 2>&1; then
    echo "  data already in bucket, skipping upload"
else
    echo "  uploading test data..."
    pip3 install awscli 2>/dev/null || true
    mkdir -p /tmp/bench-upload
    cp "$DATA_DIR"/1K.bin "$DATA_DIR"/4K.bin "$DATA_DIR"/64K.bin "$DATA_DIR"/1M.bin "$DATA_DIR"/10M.bin "$DATA_DIR"/100M.bin /tmp/bench-upload/
    cp -r "$DATA_DIR"/many /tmp/bench-upload/ 2>/dev/null || true
    AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY"         aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3 sync /tmp/bench-upload/ "s3://$BUCKET/" --quiet 2>/dev/null || {
        echo "  awscli failed, trying curl fallback..."
        for f in 1K.bin 4K.bin 64K.bin 1M.bin 10M.bin 100M.bin; do
            curl -sf -X PUT "$ENDPOINT/$BUCKET/$f" --data-binary @"$DATA_DIR/$f" 2>/dev/null || true
        done
        for f in "$DATA_DIR/many/"*; do
            name=$(basename "$f")
            curl -sf -X PUT "$ENDPOINT/$BUCKET/many/$name" --data-binary @"$f" 2>/dev/null || true
        done
    }
    echo "  upload done"
fi
echo ""

# ---- Warmup ----
ls "$MNTRS_MNT"/ >/dev/null 2>&1
ls "$RCLONE_MNT"/ >/dev/null 2>&1
cat "$MNTRS_MNT/1K.bin" >/dev/null 2>&1 || true
cat "$RCLONE_MNT/1K.bin" >/dev/null 2>&1 || true

# ============================================================
# Test categories
# ============================================================

# Dir listing
echo "=== 1. Directory listing ===
CATEGORY="DirList""
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
echo "=== 2. Stat ===
CATEGORY="Stat""
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
echo "=== 3. Sequential read ===
CATEGORY="SeqRead""
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
echo "=== 4. Read via dd (block size variation) ===
CATEGORY="ddRead""
for bs in 512 4096 65536 1048576; do
    for f in 1M.bin 10M.bin; do
        bench "dd bs=${bs} $f" "mntrs" dd if="$MNTRS_MNT/$f" bs=$bs of=/dev/null 2>/dev/null
        bench "dd bs=${bs} $f" "rclone" dd if="$RCLONE_MNT/$f" bs=$bs of=/dev/null 2>/dev/null
    done
done

# Random read
echo ""
echo "=== 5. Random read ===
CATEGORY="RandRead""
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
echo "=== 6. Write ===
CATEGORY="Write""
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
echo "=== 7. Dir/File ops ===
CATEGORY="DirOps""
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
echo "=== 8. Rename ===
CATEGORY="Rename""
cp "$DATA_DIR/1K.bin" "$MNTRS_MNT/bench-rename-src" 2>/dev/null
cp "$DATA_DIR/1K.bin" "$RCLONE_MNT/bench-rename-src" 2>/dev/null
bench "rename" "mntrs" mv "$MNTRS_MNT/bench-rename-src" "$MNTRS_MNT/bench-rename-dst"
bench "rename" "rclone" mv "$RCLONE_MNT/bench-rename-src" "$RCLONE_MNT/bench-rename-dst"

# Truncate
echo ""
echo "=== 9. Truncate ===
CATEGORY="Truncate""
cp "$DATA_DIR/10M.bin" "$MNTRS_MNT/bench-trunc" 2>/dev/null
cp "$DATA_DIR/10M.bin" "$RCLONE_MNT/bench-trunc" 2>/dev/null
bench "truncate 0" "mntrs" truncate -s 0 "$MNTRS_MNT/bench-trunc"
bench "truncate 0" "rclone" truncate -s 0 "$RCLONE_MNT/bench-trunc"
bench "truncate 1M" "mntrs" truncate -s 1M "$MNTRS_MNT/bench-trunc"
bench "truncate 1M" "rclone" truncate -s 1M "$RCLONE_MNT/bench-trunc"

# Xattr
echo ""
echo "=== 10. Xattr ===
CATEGORY="Xattr""
bench "getfattr" "mntrs" getfattr -d "$MNTRS_MNT/1K.bin" 2>/dev/null || true
bench "getfattr" "rclone" getfattr -d "$RCLONE_MNT/1K.bin" 2>/dev/null || true

# memory backend baseline (zero network, single mount)
echo ""
echo "=== 11. Memory backend (zero network) ===
CATEGORY="Memory""
mkdir -p "$MEM_MNT"
"$MNTRS_BIN" mount "memory://" "$MEM_MNT" \
    --daemon --daemon-wait --daemon-timeout=10 2>/dev/null
sleep 2
bench "stat mem" "mem" stat "$MEM_MNT"
echo "  (memory backend — for reference only)"

# ---- Table ----
python3 "$SCRIPT_DIR/render_table.py" "$RESULT_TMP"
rm -f "$RESULT_TMP"

# ---- Summary ----
echo ""
ELAPSED=$(( $(date +%s) - START_TIME ))
echo "============================================"
printf " %d tests: %d passed, %d failed (%ds)\n" "$TOTAL" "$PASS" "$FAIL" "$ELAPSED"
echo "============================================"

# Additional read tests
echo ""
echo "=== 12. Head/tail reads ===
CATEGORY="HeadTail""
for f in 10M.bin 100M.bin; do
    for n in 1 10 100; do
        bench "head -c${n}K $f" "mntrs" head -c "${n}K" "$MNTRS_MNT/$f"
        bench "head -c${n}K $f" "rclone" head -c "${n}K" "$RCLONE_MNT/$f"
    done
done

# Md5sum / sha
echo ""
echo "=== 13. Checksum ===
CATEGORY="Checksum""
for f in 1K.bin 4K.bin 64K.bin 1M.bin; do
    bench "md5sum $f" "mntrs" md5sum "$MNTRS_MNT/$f"
    bench "md5sum $f" "rclone" md5sum "$RCLONE_MNT/$f"
    bench "sha1sum $f" "mntrs" sha1sum "$MNTRS_MNT/$f"
    bench "sha1sum $f" "rclone" sha1sum "$RCLONE_MNT/$f"
done

# Touch (create empty, update mtime)
echo ""
echo "=== 14. Touch ===
CATEGORY="Touch""
bench "touch new" "mntrs" touch "$MNTRS_MNT/bench-touch-new"
bench "touch new" "rclone" touch "$RCLONE_MNT/bench-touch-new"
bench "touch exist" "mntrs" touch "$MNTRS_MNT/1K.bin"
bench "touch exist" "rclone" touch "$RCLONE_MNT/1K.bin"

# Chmod (where supported)
echo ""
echo "=== 15. Chmod ===
CATEGORY="Chmod""
bench "chmod" "mntrs" chmod 0644 "$MNTRS_MNT/1K.bin" 2>/dev/null || true
bench "chmod" "rclone" chmod 0644 "$RCLONE_MNT/1K.bin" 2>/dev/null || true

# Hardlink / Symlink (if supported)
echo ""
echo "=== 16. Symlink ===
CATEGORY="Symlink""
ln -sf "$MNTRS_MNT/1K.bin" "$MNTRS_MNT/bench-link" 2>/dev/null || true
ln -sf "$RCLONE_MNT/1K.bin" "$RCLONE_MNT/bench-link" 2>/dev/null || true
bench "readlink" "mntrs" readlink "$MNTRS_MNT/bench-link" 2>/dev/null || true
bench "readlink" "rclone" readlink "$RCLONE_MNT/bench-link" 2>/dev/null || true

# Dir with 500 files
echo ""
echo "=== 17. Large dir ops ===
CATEGORY="LargeDir""
bench "ls -f many" "mntrs" ls "$MNTRS_MNT/many" 2>/dev/null
bench "ls -f many" "rclone" ls "$RCLONE_MNT/many" 2>/dev/null
bench "find many" "mntrs" find "$MNTRS_MNT/many" 2>/dev/null
bench "find many" "rclone" find "$RCLONE_MNT/many" 2>/dev/null
bench "rm many 10" "mntrs" bash -c "cd '$MNTRS_MNT/many' && ls | head -10 | xargs rm -f" 2>/dev/null || true
bench "rm many 10" "rclone" bash -c "cd '$RCLONE_MNT/many' && ls | head -10 | xargs rm -f" 2>/dev/null || true

# Concurrent reads
echo ""
echo "=== 18. Concurrent reads ===
CATEGORY="Concurrent""
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
echo "=== 19. Fstat ===
CATEGORY="Fstat""
bench "fstat 1K" "mntrs" bash -c "exec 3<'$MNTRS_MNT/1K.bin'; fstat \$3 2>/dev/null; exec 3>&-"
bench "fstat 1K" "rclone" bash -c "exec 3<'$RCLONE_MNT/1K.bin'; fstat \$3 2>/dev/null; exec 3>&-"

# Lseek
echo ""
echo "=== 20. Lseek ===
CATEGORY="Lseek""
bench "lseek 100M" "mntrs" bash -c "exec 3<'$MNTRS_MNT/100M.bin'; dd bs=1 seek=1000 count=0 of=/dev/null 2>/dev/null <&3; exec 3>&-"
bench "lseek 100M" "rclone" bash -c "exec 3<'$RCLONE_MNT/100M.bin'; dd bs=1 seek=1000 count=0 of=/dev/null 2>/dev/null <&3; exec 3>&-"
