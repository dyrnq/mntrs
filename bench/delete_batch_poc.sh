#!/bin/bash
# PoC v2: S3 DeleteObjects batching microbenchmark.
#
# Question: when 500 FUSE unlink calls arrive in a burst (rm -rf 500),
# each op.delete() in opendal triggers a single S3 DELETE
# (ServiceOperation::DeleteObject). The bundled BatchDeleter<S3Deleter>
# exists with max_batch_size=1000 but is only used when you keep the
# deleter open across calls — which FUSE unlink can't.
#
# Hypothesis: 500 × single DELETE = ~500ms (1ms/op).
# 500 × S3 DeleteObjects (1 batch) = ~50ms (1 HTTP roundtrip).
#
# v1 used `aws s3 rm` in a loop, but each `aws` invocation spawns a
# Python VM (~500ms startup), so 500 × rm = 4 minutes of cli overhead
# instead of the actual S3 latency. v2 uses `aws s3api delete-objects`
# with --cli-input-json to batch in 1 roundtrip, AND keeps a tight
# shell loop only for the batched tests (1 aws invocation per batch,
# not per file).
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

run_batched_delete() {
    # Deletes files in $BUCKET/$PREFIX/$TEST using only S3 DeleteObjects,
    # batched in chunks of $1 keys per request. $2 = test name for output.
    local chunk_size="$1"
    local test_name="$2"
    local n_chunks=$(( (N + chunk_size - 1) / chunk_size ))
    local start_ns=$(date +%s%N)
    for c in $(seq 0 $((n_chunks - 1))); do
        local begin=$((c * chunk_size + 1))
        local end=$((begin + chunk_size - 1))
        if [ $end -gt $N ]; then end=$N; fi
        # Build the JSON input file
        python3 -c "
import json
keys = [{'Key': f'$PREFIX/${test_name}/file_{i:04d}.txt'} for i in range($begin, $end + 1)]
print(json.dumps({'Delete': {'Objects': keys, 'Quiet': True}}))
" > /tmp/do_input.json
        # The file:// URI must work; some awscli versions need a real path
        aws --endpoint-url "$ENDPOINT" --no-verify-ssl \
            s3api delete-objects --bucket "$BUCKET" \
            --delete "file:///tmp/do_input.json" >/dev/null 2>&1 || return 1
    done
    local end_ns=$(date +%s%N)
    local total_ms=$(( (end_ns - start_ns) / 1000000 ))
    printf "  %-45s | %dms (%d batches × chunk=%d)\n" \
        "$test_name" "$total_ms" "$n_chunks" "$chunk_size"
}

echo "=== S3 DeleteObjects batching PoC (v2) ==="
echo "  endpoint:   $ENDPOINT"
echo "  bucket:     $BUCKET"
echo "  n files:    $N"
echo ""

# Ensure bucket exists
aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3 mb "s3://$BUCKET" 2>/dev/null || true

# ---- Setup: upload N files to 4 prefixes ----
echo "--- Setup: upload $N files × 4 prefixes via aws s3 sync ---"
upload_start=$(date +%s%N)
mkdir -p /tmp/poc_upload
rm -rf /tmp/poc_upload/*
for i in $(seq 1 $N); do
    echo "content-$i" > "/tmp/poc_upload/file_$(printf '%04d' "$i").txt"
done
for tag in t1 t2 t3 t4; do
    aws --endpoint-url "$ENDPOINT" --no-verify-ssl s3 sync /tmp/poc_upload/ \
        "s3://$BUCKET/$PREFIX/$tag/" >/dev/null 2>&1
done
upload_end=$(date +%s%N)
echo "  uploaded $((N * 4)) files in $(( (upload_end - upload_start) / 1000000 ))ms"
echo ""

# ---- Test 1: 1 × S3 DeleteObjects with 500 keys (single huge batch) ----
echo "=== Test 1: 1 × delete-objects (500 keys in 1 req) ==="
run_batched_delete 500 "t1"
echo ""

# ---- Test 2: 5 × DeleteObjects with 100 keys each ----
echo "=== Test 2: 5 × delete-objects (100 keys each) ==="
run_batched_delete 100 "t2"
echo ""

# ---- Test 3: 10 × DeleteObjects with 50 keys each ----
echo "=== Test 3: 10 × delete-objects (50 keys each) ==="
run_batched_delete 50 "t3"
echo ""

# ---- Test 4: 50 × DeleteObjects with 10 keys each (granular) ----
echo "=== Test 4: 50 × delete-objects (10 keys each) ==="
run_batched_delete 10 "t4"
echo ""

# ---- Summary ----
echo "=== Summary ==="
echo "baseline mntrs:  ~574ms for 500 unlinks (Probe A 1.06ms/op)"
echo "baseline rclone: ~136ms for 500 unlinks (bench)"
echo ""
echo "If Test 1 is ~50ms: 1-batch DeleteObjects collapses 500 roundtrips."
echo "If Test 4 is ~250ms: even granular batches beat per-call DELETE."
echo "The sweet spot (test 1 vs 4) tells us the batch size to use."
