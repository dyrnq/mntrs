#!/bin/bash
# PoC: S3 DeleteObjects batching microbenchmark.
#
# Question: when 500 FUSE unlink calls arrive in a burst (rm -rf 500),
# each op.delete() in opendal triggers a single S3 DELETE
# (ServiceOperation::DeleteObject). The bundled BatchDeleter<S3Deleter>
# exists with max_batch_size=1000 but is only used when you keep the
# deleter open across calls — which FUSE unlink can't.
#
# Hypothesis: 500 × single DELETE = ~500ms (1ms/op).
# 500 × S3 DeleteObjects = ~50ms (1 HTTP roundtrip).
#
# We use aws cli (already in the bench workflow) to test both paths,
# comparing:
#   1. 500 × `aws s3 rm`            (single DELETE per call — same as op.delete)
#   2. 1  × `aws s3api delete-objects` with 500 keys (S3 DeleteObjects)
#   3. 5  × `aws s3api delete-objects` with 100 keys each (chunked batching)
set -uo pipefail

ENDPOINT="${ENDPOINT:-http://localhost:9000}"
ACCESS_KEY="${ACCESS_KEY:-minioadmin}"
SECRET_KEY="${SECRET_KEY:-minioadmin}"
BUCKET="${BUCKET:-mntrs-delete-poc}"
N=500
PREFIX="poc"

export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"

bench() {
    local name="$1"
    shift
    local out
    out=$({ time "$@" >/dev/null 2>&1; } 2>&1) || {
        printf "  %-45s | FAIL\n" "$name"
        return
    }
    local t=$(echo "$out" | grep real | awk '{print $2}')
    printf "  %-45s | %s\n" "$name" "$t"
}

echo "=== S3 DeleteObjects batching PoC ==="
echo "  endpoint:   $ENDPOINT"
echo "  bucket:     $BUCKET"
echo "  n files:    $N"
echo ""

# Ensure bucket exists
aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3 mb "s3://$BUCKET" 2>/dev/null || true

# ---- Setup: upload N files ----
echo "--- Setup: upload $N files via aws s3 cp (one PUT per file) ---"
upload_start=$(date +%s%N)
mkdir -p /tmp/poc_upload
rm -rf /tmp/poc_upload/*
for i in $(seq 1 $N); do
    echo "content-$i" > "/tmp/poc_upload/file_$(printf '%04d' "$i").txt"
done
aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3 sync /tmp/poc_upload/ "s3://$BUCKET/$PREFIX/t1/" >/dev/null 2>&1
upload_end=$(date +%s%N)
echo "  uploaded $N files in $(( (upload_end - upload_start) / 1000000 ))ms"
echo ""

# Copy to t2, t3, t4 so each test starts with its own files
echo "--- Setup: copy to t2/t3/t4 keys ---"
aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3 cp --recursive \
    "s3://$BUCKET/$PREFIX/t1/" "s3://$BUCKET/$PREFIX/t2/" >/dev/null 2>&1
aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3 cp --recursive \
    "s3://$BUCKET/$PREFIX/t1/" "s3://$BUCKET/$PREFIX/t3/" >/dev/null 2>&1
aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3 cp --recursive \
    "s3://$BUCKET/$PREFIX/t1/" "s3://$BUCKET/$PREFIX/t4/" >/dev/null 2>&1
echo "  copy done"
echo ""

# ---- Test 1: 500 × aws s3 rm (single DELETE per call) ----
echo "=== Test 1: 500 × aws s3 rm (single S3 DELETE per call) ==="
bench "500 × aws s3 rm prefix=" bash -c "
    for i in \$(seq 1 $N); do
        aws --endpoint-url '$ENDPOINT' --no-verify-ssl s3 rm 's3://$BUCKET/$PREFIX/t1/file_\$(printf %04d \$i).txt' >/dev/null 2>&1
    done
"
echo ""

# ---- Test 2: 1 × S3 DeleteObjects with 500 keys ----
echo "=== Test 2: 1 × aws s3api delete-objects (500 keys in 1 req) ==="
build_delete_objects_input() {
    local prefix="$1"
    python3 -c "
import json
keys = [{'Key': f'$prefix/file_{i:04d}.txt'} for i in range(1, $N+1)]
print(json.dumps({'Delete': {'Objects': keys, 'Quiet': True}}))
" > /tmp/delete_objects_input.json
}
build_delete_objects_input "$PREFIX/t2"
bench "1 × delete-objects (500 keys)" \
    aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3api delete-objects \
    --bucket "$BUCKET" --delete "file:///tmp/delete_objects_input.json"
echo ""

# ---- Test 3: 5 × S3 DeleteObjects with 100 keys each (chunked) ----
echo "=== Test 3: 5 × aws s3api delete-objects (100 keys each) ==="
chunked_delete_objects() {
    local prefix="$1"
    local chunk_size="$2"
    local n_chunks=$(( (N + chunk_size - 1) / chunk_size ))
    for c in $(seq 0 $((n_chunks - 1))); do
        local start=$((c * chunk_size + 1))
        local end=$((start + chunk_size - 1))
        if [ $end -gt $N ]; then end=$N; fi
        python3 -c "
import json
keys = [{'Key': f'$prefix/file_{i:04d}.txt'} for i in range($start, $end + 1)]
print(json.dumps({'Delete': {'Objects': keys, 'Quiet': True}}))
" > /tmp/delete_objects_chunk_${c}.json
        aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3api delete-objects \
            --bucket "$BUCKET" --delete "file:///tmp/delete_objects_chunk_${c}.json" \
            >/dev/null 2>&1
    done
}
bench "5 × delete-objects (100 keys each)" \
    bash -c "chunked_delete_objects '$PREFIX/t3' 100"
echo ""

# ---- Test 4: 10 × S3 DeleteObjects with 50 keys each (more chunks) ----
echo "=== Test 4: 10 × aws s3api delete-objects (50 keys each) ==="
bench "10 × delete-objects (50 keys each)" \
    bash -c "chunked_delete_objects '$PREFIX/t4' 50"
echo ""

# ---- Summary ----
echo "=== Summary ==="
echo "baseline mntrs:  ~574ms for 500 unlinks  (Probe A)"
echo "baseline rclone: ~136ms for 500 unlinks  (bench)"
echo ""
echo "If Test 2 is ~50ms → batching saves ~90% of wall."
echo "If Test 3 is ~80ms → chunked batching still wins big."
echo "If Test 4 is ~100ms → chunk size sensitivity matters."
