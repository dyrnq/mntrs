#!/usr/bin/env bash
#
# mntrs 多维压测：cat / head / tail × 多文件大小 × memory / s3 / hdfs
# 每个组合跑 100 次，输出 CSV + 汇总统计。
#
set -uo pipefail

MNTRS_BIN="./target/release/mntrs"
BASE_MP="/tmp/mntrs-bench-cht"
CACHE_DIR="/tmp/mntrs-bench-cht-cache"
DATA_DIR="/tmp/mntrs-bench-cht-data"
RESULTS_CSV="/tmp/mntrs-bench-cht-results.csv"
ITER="${ITER:-100}"

# S3 (MinIO)
S3_ENDPOINT="http://localhost:9000"
S3_ACCESS_KEY="minioadmin"
S3_SECRET_KEY="minioadmin"
S3_BUCKET="${S3_BUCKET:-bench-cht-1k}"

# HDFS
HDFS_NAMENODE="172.17.0.6:8020"

# kill residuals
pkill -f "mntrs.*bench-cht" 2>/dev/null || true
sleep 1

# ---- Cleanup ----
cleanup_all() {
    for mp in "$BASE_MP"-memory "$BASE_MP"-s3 "$BASE_MP"-hdfs; do
        fusermount3 -u "$mp" 2>/dev/null || fusermount -u "$mp" 2>/dev/null || true
    done
    rm -rf "$CACHE_DIR" 2>/dev/null || true
}
trap cleanup_all EXIT

mkdir -p "$BASE_MP"-memory "$BASE_MP"-s3 "$BASE_MP"-hdfs
mkdir -p "$CACHE_DIR" "$DATA_DIR"

# ---- Generate test data ----
echo "=== Generating test data ==="
for size in 1K 100K 1M 10M; do
    dd if=/dev/urandom of="$DATA_DIR/file-${size}.bin" bs=${size} count=1 2>/dev/null
done
ls -lh "$DATA_DIR/"

# ---- Upload to S3 ----
echo "=== Uploading to S3 (MinIO) ==="
mc mb "local/$S3_BUCKET" 2>/dev/null || true
for size in 1K 100K 1M 10M; do
    mc cp "$DATA_DIR/file-${size}.bin" "local/$S3_BUCKET/file-${size}.bin" 2>/dev/null
done

# ---- Upload to HDFS ----
echo "=== Uploading to HDFS ==="
docker exec hdfs /opt/hadoop/bin/hdfs dfs -mkdir -p /user/mntrs 2>/dev/null || true
for size in 1K 100K 1M 10M; do
    docker exec hdfs /opt/hadoop/bin/hdfs dfs -put -f /tmp/file-${size}.bin /user/mntrs/file-${size}.bin 2>/dev/null || (
        docker cp "$DATA_DIR/file-${size}.bin" hdfs:/tmp/file-${size}.bin
        docker exec hdfs /opt/hadoop/bin/hdfs dfs -put -f /tmp/file-${size}.bin /user/mntrs/file-${size}.bin
    ) 2>/dev/null
done
docker exec hdfs /opt/hadoop/bin/hdfs dfs -ls /user/mntrs/ 2>/dev/null

# ---- Mount ----
echo "=== Mounting backends ==="

echo "  Mounting memory..."
"$MNTRS_BIN" mount "memory://" "$BASE_MP"-memory \
    --cache-dir "$CACHE_DIR/memory" \
    --daemon --daemon-wait --daemon-timeout=15 2>/dev/null

echo "  Mounting S3..."
"$MNTRS_BIN" mount "s3://$S3_BUCKET" "$BASE_MP"-s3 \
    --opt "endpoint=$S3_ENDPOINT" --opt "access-key=$S3_ACCESS_KEY" \
    --opt "secret-key=$S3_SECRET_KEY" --opt "region=us-east-1" \
    --cache-dir "$CACHE_DIR/s3" --use-server-modtime \
    --vfs-read-chunk-streams 4 \
    --daemon --daemon-wait --daemon-timeout=15 2>/dev/null

echo "  Mounting HDFS..."
"$MNTRS_BIN" mount "hdfs://$HDFS_NAMENODE/user/mntrs" "$BASE_MP"-hdfs \
    --cache-dir "$CACHE_DIR/hdfs" --use-server-modtime \
    --vfs-read-chunk-streams 4 \
    --daemon --daemon-wait --daemon-timeout=15 2>/dev/null

sleep 5
echo ""
mount | grep mntrs-bench-cht || echo "  WARNING: no mounts found"

# ---- Sanity check ----
echo "=== Sanity check ==="
for backend in memory s3 hdfs; do
    echo -n "  $backend: "
    ls "$BASE_MP-$backend/" 2>/dev/null | wc -l
done

# ---- Warmup ----
echo "=== Warmup ==="
for backend in memory s3 hdfs; do
    mp="$BASE_MP-$backend"
    for size in 1K 100K 1M 10M; do
        cat "$mp/file-${size}.bin" > /dev/null 2>&1 || true
    done
done

# ---- Benchmark ----
echo ""
echo "=== Benchmark: ${ITER} iterations each ==="
echo "backend,size,op,iter,real_us" > "$RESULTS_CSV"

TOTAL=$(( 3 * 4 * 7 * ITER ))
COUNT=0

for backend in memory s3 hdfs; do
    mp="$BASE_MP-$backend"

    for size in 1K 100K 1M 10M; do
        for op_info in \
            "cat:cat" \
            "head-1K:head -c 1K" \
            "head-10K:head -c 10K" \
            "head-1M:head -c 1M" \
            "tail-1K:tail -c 1K" \
            "tail-10K:tail -c 10K" \
            "tail-1M:tail -c 1M"; do

            op_name="${op_info%%:*}"
            op_cmd="${op_info##*:}"

            for i in $(seq 1 $ITER); do
                start=$(date +%s%6N)
                $op_cmd "$mp/file-${size}.bin" > /dev/null 2>&1 || true
                end=$(date +%s%6N)
                elapsed=$(( end - start ))
                echo "$backend,$size,$op_name,$i,$elapsed" >> "$RESULTS_CSV"
            done

            COUNT=$(( COUNT + ITER ))
            echo "  [$backend] file-${size}.bin $op_name ($COUNT / $TOTAL)"
        done
    done
done

echo ""
echo "=== Raw data saved to: $RESULTS_CSV ==="
echo ""

# ---- Summary stats ----
python3 - "$RESULTS_CSV" << 'PYEOF'
import csv, sys, math
from collections import defaultdict

rows = []
with open(sys.argv[1]) as f:
    reader = csv.DictReader(f)
    for r in reader:
        r['real_us'] = int(r['real_us'])
        rows.append(r)

groups = defaultdict(list)
for r in rows:
    key = (r['backend'], r['size'], r['op'])
    groups[key].append(r['real_us'])

print(f"{'Backend':<10} {'Size':<8} {'Op':<12} {'P50(ms)':<10} {'P95(ms)':<10} {'P99(ms)':<10} {'Avg(ms)':<10} {'Min(ms)':<10} {'Max(ms)':<10} {'StdDev':<8}")
print("-" * 100)

for key in sorted(groups.keys()):
    vals = sorted(groups[key])
    n = len(vals)
    p50 = vals[n // 2] / 1000.0
    p95 = vals[int(n * 0.95)] / 1000.0
    p99 = vals[int(n * 0.99)] / 1000.0
    avg = sum(vals) / n / 1000.0
    mn = vals[0] / 1000.0
    mx = vals[-1] / 1000.0
    mean = sum(vals) / n
    std = math.sqrt(sum((v - mean)**2 for v in vals) / n) / 1000.0
    b, s, o = key
    print(f"{b:<10} {s:<8} {o:<12} {p50:<10.2f} {p95:<10.2f} {p99:<10.2f} {avg:<10.2f} {mn:<10.2f} {mx:<10.2f} {std:<8.2f}")

print()
print(f"Total data points: {len(rows)}")
print(f"File: {sys.argv[1]}")
PYEOF

echo ""
echo "=== Done ==="
