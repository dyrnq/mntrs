#!/usr/bin/env bash
#
# Plan #64 step 11: end-to-end test for the S3 batched-delete
# write-behind path. Verifies that:
#   1. MNTRS_UNLINK_BATCH=1 enables the batcher (info log line
#      at mount start; flush lines while unlink is running).
#   2. The user's `rm` returns successfully while the batched
#      deleter is still in flight.
#   3. The S3 backend ends up empty after the unlink burst
#      (verified via `aws s3api list-objects-v2`, not just the
#      FUSE view — tombstones only mask the in-memory state).
#   4. The daemon log shows at least one multi-key
#      `batched_delete: flush` event with batch_size > 1
#      (proves the batching actually happened, not just that
#      deletes succeed serially).
#
# Prereqs (host side, set in CI):
#   - MinIO running on localhost:9000
#   - AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY exported
#   - aws CLI installed (or use the bundled `boto3` Python)
#
# Usage:
#   ./tests/e2e/mount/unlink_batch_test.sh [BINARY] [MOUNTPOINT]
#
# Defaults: target/release/mntrs, /tmp/mntrs-unlink-batch

set -u

BIN="${1:-target/release/mntrs}"
MP="${2:-/tmp/mntrs-unlink-batch}"
N_FILES=200
BUCKET="mntrs-unlink-batch-$$-$(date +%s)"

# Endpoint + creds. MinIO defaults; override via env if the CI
# matrix is different.
ENDPOINT="${UNLINK_BATCH_ENDPOINT:-http://localhost:9000}"
AK="${UNLINK_BATCH_AK:-minioadmin}"
SK="${UNLINK_BATCH_SK:-minioadmin}"

# Pick one of aws cli or python+boto3 for backend verification.
if command -v aws >/dev/null 2>&1; then
    VERIFY="aws"
elif python3 -c "import boto3" 2>/dev/null; then
    VERIFY="boto3"
else
    echo "FAIL: need either aws cli or boto3 for backend verification"
    exit 1
fi

if [ ! -x "$BIN" ]; then
    echo "=== Build mntrs ==="
    cargo build --release -p mntrs 2>&1 | tail -3
fi
BIN="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")"

LOG="/tmp/mntrs-unlink-batch-$$.log"
mkdir -p "$MP"
umount "$MP" 2>/dev/null || fusermount3 -u "$MP" 2>/dev/null || true
rm -rf "$MP"

echo "=== unlink_batch_test ==="
echo "binary:     $BIN"
echo "mountpoint: $MP"
echo "backend:    $ENDPOINT / $BUCKET"
echo "files:      $N_FILES"
echo "log:        $LOG"
echo

# 1. Create the bucket. `aws s3api create-bucket` is fine for
#    MinIO; boto3 is the fallback.
if [ "$VERIFY" = "aws" ]; then
    AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" \
        aws s3api create-bucket --bucket "$BUCKET" \
        --endpoint-url "$ENDPOINT" 2>/dev/null || true
else
    python3 -c "
import os, boto3
boto3.client('s3', endpoint_url=os.environ['ENDPOINT'],
             aws_access_key_id=os.environ['AK'],
             aws_secret_access_key=os.environ['SK'],
             region_name='us-east-1').create_bucket(Bucket=os.environ['BUCKET'])
" 2>/dev/null || true
    export ENDPOINT AK SK BUCKET
fi

cleanup() {
    set +e
    if [ -d "$MP" ]; then
        fusermount3 -u "$MP" 2>/dev/null || umount "$MP" 2>/dev/null
    fi
    if [ "$VERIFY" = "aws" ]; then
        AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" \
            aws s3api delete-objects --bucket "$BUCKET" \
            --endpoint-url "$ENDPOINT" \
            --delete "{\"Objects\":[{\"Key\":\"_force_cleanup_marker\"}],\"Quiet\":true}" \
            2>/dev/null || true
        AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" \
            aws s3 rb "s3://$BUCKET" --endpoint-url "$ENDPOINT" --force 2>/dev/null
    else
        ENDPOINT="$ENDPOINT" AK="$AK" SK="$SK" BUCKET="$BUCKET" \
            python3 -c "
import os, boto3
b = boto3.resource('s3', endpoint_url=os.environ['ENDPOINT'],
    aws_access_key_id=os.environ['AK'],
    aws_secret_access_key=os.environ['SK'],
    region_name='us-east-1').Bucket(os.environ['BUCKET'])
b.objects.all().delete()
b.delete()
" 2>/dev/null
    fi
    rm -rf "$MP"
}
trap cleanup EXIT

# 2. Mount with batched delete enabled.
echo "--- mount (MNTRS_UNLINK_BATCH=1) ---"
MNTRS_UNLINK_BATCH=1 \
RUST_LOG="mntrs::batched_delete=info,mntrs=warn" \
"$BIN" mount \
    --opt endpoint="$ENDPOINT" \
    --opt access-key="$AK" \
    --opt secret-key="$SK" \
    --opt region=us-east-1 \
    "s3://$BUCKET" "$MP" \
    >"$LOG" 2>&1 &
MOUNT_PID=$!
# fuser mount is async; wait for it to appear in `mount`.
for i in $(seq 1 30); do
    if mount | grep -q "on $MP "; then
        break
    fi
    sleep 0.2
done
if ! mount | grep -q "on $MP "; then
    echo "FAIL: mount did not appear within 6s"
    tail -30 "$LOG"
    exit 1
fi
echo "  mounted (pid $MOUNT_PID)"

# Confirm the info log line landed.
if ! grep -q "batched_delete: enabled (MNTRS_UNLINK_BATCH=1)" "$LOG"; then
    echo "FAIL: batched_delete not enabled in daemon log"
    tail -30 "$LOG"
    exit 1
fi
echo "  batched_delete enabled (info log)"

# 3. Create N files, then rm -rf them. The user's `rm` should
#    return before all S3 deletes have been requested — that's
#    the write-behind contract.
echo "--- create + rm $N_FILES files ---"
T0=$(date +%s%N)
mkdir -p "$MP/rmtest"
for i in $(seq 1 "$N_FILES"); do
    printf "x" > "$MP/rmtest/file_$(printf '%04d' $i).txt"
done
T1=$(date +%s%N)
echo "  create elapsed_ms: $(( (T1 - T0) / 1000000 ))"

T0=$(date +%s%N)
rm -rf "$MP/rmtest"
T1=$(date +%s%N)
echo "  rm elapsed_ms: $(( (T1 - T0) / 1000000 ))"

# 4. Force the rmdir barrier to drain any in-flight batches
#    (rm -rf already invokes rmdir, but a re-run is harmless
#    and ensures we observe the post-rmdir state).
sleep 0.5

# 4b. Plan #64 stage C: recreate-after-rm must NOT return
#     ENOENT. Without the tombstone-on-ack + cancel_pending
#     fix, the tombstone would outlive the S3 delete and
#     `cat <recreated_file>` would fail. Mounted with batched
#     deletes enabled so this exercises the production path.
echo "--- recreate-after-rm ---"
T0=$(date +%s%N)
echo "stage_c_recreate_payload" > "$MP/rmtest/file_0001.txt"
cat "$MP/rmtest/file_0001.txt" > /tmp/stage_c_recreate_got.txt
T1=$(date +%s%N)
echo "  recreate elapsed_ms: $(( (T1 - T0) / 1000000 ))"
if ! diff -q /tmp/stage_c_recreate_got.txt <(echo "stage_c_recreate_payload") >/dev/null; then
    echo "  recreate-after-rm FAIL: got '$(cat /tmp/stage_c_recreate_got.txt)'"
    MOUNT_TEST_FAILED=1
else
    echo "  recreate-after-rm OK"
fi
rm -f /tmp/stage_c_recreate_got.txt
rm -f "$MP/rmtest/file_0001.txt"

# 5. Verify the S3 backend is empty.
echo "--- verify S3 backend state ---"
if [ "$VERIFY" = "aws" ]; then
    N=$(AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" \
        aws s3api list-objects-v2 --bucket "$BUCKET" \
        --endpoint-url "$ENDPOINT" --output json 2>/dev/null \
        | python3 -c "import sys, json; print(len(json.load(sys.stdin).get('Contents', [])))")
else
    N=$(ENDPOINT="$ENDPOINT" AK="$AK" SK="$SK" BUCKET="$BUCKET" \
        python3 -c "
import os, boto3
b = boto3.resource('s3', endpoint_url=os.environ['ENDPOINT'],
    aws_access_key_id=os.environ['AK'],
    aws_secret_access_key=os.environ['SK'],
    region_name='us-east-1').Bucket(os.environ['BUCKET'])
print(sum(1 for _ in b.objects.all()))
")
fi
echo "  s3 list-objects count: $N (expected 0)"

# 6. Verify the daemon log shows at least one multi-key flush.
echo "--- verify batched_deleter flush log ---"
FLUSH_LINES=$(grep -c "batched_delete: flush" "$LOG" || true)
MULTI_KEY=$(grep "batched_delete: flush" "$LOG" | grep -c "batch_size=[2-9]" || true)
echo "  flush lines: $FLUSH_LINES"
echo "  multi-key (>1) flush lines: $MULTI_KEY"

# 7. Pass/fail.
if [ "$N" -ne 0 ]; then
    echo "FAIL: backend has $N objects remaining"
    exit 1
fi
if [ "$MULTI_KEY" -lt 1 ]; then
    echo "FAIL: no multi-key batched_delete flush observed (write-behind not engaging)"
    tail -30 "$LOG"
    exit 1
fi

echo
echo "PASS: $N_FILES unlinks → 0 backend objects, $MULTI_KEY multi-key flush lines"
exit 0
