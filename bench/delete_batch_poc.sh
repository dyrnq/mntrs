#!/bin/bash
# PoC v3: S3 DeleteObjects batching microbenchmark.
#
# Question: 500 FUSE unlink calls hit 1 S3 DELETE each = ~574ms.
#          500 batched into 1 S3 DeleteObjects = ~50ms?
#          What's the right batch size?
#
# v2 silently failed because the aws s3api delete-objects call
# produced a non-zero exit but the `run_batched_delete` function
# muted it. v3 adds visible error reporting.
set -uo pipefail

ENDPOINT="${ENDPOINT:-http://localhost:9000}"
ACCESS_KEY="${ACCESS_KEY:-minioadmin}"
SECRET_KEY="${SECRET_KEY:-minioadmin}"
BUCKET="${BUCKET:-mntrs-delete-poc}"
N=500
PREFIX="poc"

export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"

echo "=== S3 DeleteObjects batching PoC v3 ==="
echo "  endpoint:   $ENDPOINT"
echo "  bucket:     $BUCKET"
echo "  n files:    $N"
echo ""

# Ensure bucket exists
aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3 mb "s3://$BUCKET" 2>&1 | head -1 || true

# ---- Setup: upload N files to 4 prefixes ----
echo "--- Setup: upload $N files × 4 prefixes via aws s3 sync ---"
mkdir -p /tmp/poc_upload
rm -rf /tmp/poc_upload/*
for i in $(seq 1 $N); do
    echo "content-$i" > "/tmp/poc_upload/file_$(printf '%04d' "$i").txt"
done
upload_start=$(date +%s%N)
for tag in t1 t2 t3 t4; do
    aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3 sync /tmp/poc_upload/ \
        "s3://$BUCKET/$PREFIX/$tag/" >/dev/null 2>&1
done
upload_end=$(date +%s%N)
echo "  uploaded $((N * 4)) files in $(( (upload_end - upload_start) / 1000000 ))ms"
echo ""

# First let me verify a single delete-objects call works
echo "--- Sanity: single delete-objects with 1 key ---"
# aws s3api --delete parses the JSON as the BODY of the Delete
# element, not the envelope. So we send {"Objects":[...],"Quiet":true}
# not {"Delete":{"Objects":[...],"Quiet":true}}.
echo '{"Objects":[{"Key":"poc/t1/file_0001.txt"}],"Quiet":true}' > /tmp/sanity.json
out=$(aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3api delete-objects \
    --bucket "$BUCKET" --delete "file:///tmp/sanity.json" 2>&1)
echo "  output: $out"
echo ""

run_batched_delete() {
    local chunk_size="$1"
    local test_name="$2"
    local n_chunks=$(( (N + chunk_size - 1) / chunk_size ))
    local start_ns=$(date +%s%N)
    local failed=0
    for c in $(seq 0 $((n_chunks - 1))); do
        local begin=$((c * chunk_size + 1))
        local end=$((begin + chunk_size - 1))
        if [ $end -gt $N ]; then end=$N; fi
        python3 -c "
import json
keys = [{'Key': f'$PREFIX/${test_name}/file_{i:04d}.txt'} for i in range($begin, $end + 1)]
# aws s3api --delete strips the Delete wrapper, so the JSON must be
# the body of Delete (Objects + Quiet), not the envelope.
print(json.dumps({'Objects': keys, 'Quiet': True}))
" > /tmp/do_input.json
        # Print first chunk to verify format
        if [ $c -eq 0 ]; then
            echo "  [debug] chunk 0 file size: $(wc -c < /tmp/do_input.json) bytes"
            echo "  [debug] chunk 0 first 200 chars: $(head -c 200 /tmp/do_input.json)"
        fi
        local err
        err=$(aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3api delete-objects \
            --bucket "$BUCKET" --delete "file:///tmp/do_input.json" 2>&1)
        local rc=$?
        if [ $rc -ne 0 ]; then
            echo "  [error] chunk $c failed: $err"
            failed=1
            break
        fi
    done
    local end_ns=$(date +%s%N)
    local total_ms=$(( (end_ns - start_ns) / 1000000 ))
    if [ $failed -eq 0 ]; then
        printf "  %-45s | %dms (%d batches × chunk=%d)\n" \
            "$test_name" "$total_ms" "$n_chunks" "$chunk_size"
    else
        printf "  %-45s | FAIL\n" "$test_name"
    fi
}

echo "=== Test 1: 1 × delete-objects (500 keys in 1 req) ==="
run_batched_delete 500 "t1"
echo ""

echo "=== Test 2: 5 × delete-objects (100 keys each) ==="
run_batched_delete 100 "t2"
echo ""

echo "=== Test 3: 10 × delete-objects (50 keys each) ==="
run_batched_delete 50 "t3"
echo ""

echo "=== Test 4: 50 × delete-objects (10 keys each) ==="
run_batched_delete 10 "t4"
echo ""

echo "=== Summary ==="
echo "baseline mntrs:  ~574ms for 500 unlinks (Probe A 1.06ms/op)"
echo "baseline rclone: ~136ms for 500 unlinks (bench)"
echo ""
echo "If Test 1 ~50ms: 1 batch collapses 500 roundtrips."
echo "If Test 4 ~250ms: even granular batches beat per-call DELETE."
