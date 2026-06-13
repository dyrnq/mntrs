#!/usr/bin/env bash
#
# Writeback crash recovery test.
#
# Verifies that dirty cache files (.dirty sidecars) are properly
# recovered after a crash — the data is uploaded to the remote
# and .dirty is cleaned up only AFTER upload succeeds.
#
# This is a regression test for the common_init_wb() bug where
# .dirty was deleted before upload, causing permanent data loss.
#
# Requires: S3 backend (MinIO). Set via env or defaults to localhost:9000.
#
# Usage:
#   ./tests/e2e/mount/writeback_recovery.sh [BINARY] [CACHE_DIR]
#
# Exit code: 0 on success, 1 on failure.

set -u

BIN="${1:-target/release/mntrs}"
CACHE_DIR="${2:-/tmp/mntrs-wb-recovery-cache}"

S3_URL="${WB_S3_URL:-s3://mntrs-wb-recovery-test}"
S3_ENDPOINT="${WB_S3_ENDPOINT:-http://localhost:9000}"
S3_ACCESS="${WB_S3_ACCESS:-minioadmin}"
S3_SECRET="${WB_S3_SECRET:-minioadmin}"
MP="/tmp/mntrs-wb-recovery"

if [ ! -x "$BIN" ]; then
    echo "=== Build mntrs ==="
    cargo build --release -p mntrs 2>&1 | tail -3
fi
BIN="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")"

# Check S3 connectivity
if ! curl -sf "$S3_ENDPOINT/minio/health/live" > /dev/null 2>&1; then
    echo "⚠ MinIO not available at $S3_ENDPOINT — skipping writeback recovery test"
    exit 0
fi

echo "=== writeback_recovery ==="

FAIL=0
OPTS="--opt endpoint=$S3_ENDPOINT --opt access-key=$S3_ACCESS --opt secret-key=$S3_SECRET --opt region=us-east-1"

# Create bucket (try mc, then awscli, then raw curl)
BUCKET="${S3_URL#s3://}"
if command -v mc &>/dev/null; then
    mc alias set local "$S3_ENDPOINT" "$S3_ACCESS" "$S3_SECRET" 2>/dev/null
    mc mb "local/$BUCKET" 2>/dev/null || true
    mc rm --recursive --force "local/$BUCKET/" 2>/dev/null || true
elif command -v aws &>/dev/null; then
    AWS_ACCESS_KEY_ID="$S3_ACCESS" AWS_SECRET_ACCESS_KEY="$S3_SECRET" \
        aws --endpoint-url "$S3_ENDPOINT" s3 mb "s3://$BUCKET" 2>/dev/null || true
else
    # Fallback: curl PUT (works with MinIO)
    curl -sf -X PUT "${S3_ENDPOINT}/${BUCKET}" 2>/dev/null || true
fi

# Cleanup
fusermount3 -u "$MP" 2>/dev/null || true
rm -rf "$MP" "$CACHE_DIR"
mkdir -p "$MP" "$CACHE_DIR"

# ---- Step 1: Simulate crash state ----
echo "--- Step 1: Create cache + .dirty sidecar (simulating crash) ---"
echo "RECOVERY-TEST-DATA-$(date +%s)" > "$CACHE_DIR/deadbeef01"
echo "recovery-probe.txt" > "$CACHE_DIR/deadbeef01.dirty"
echo "  cache: $(cat "$CACHE_DIR/deadbeef01")"
echo "  dirty: $(cat "$CACHE_DIR/deadbeef01.dirty")"

# ---- Step 2: Mount (triggers recovery) ----
echo "--- Step 2: Mount (triggers common_init_wb recovery) ---"
"$BIN" mount "$S3_URL" "$MP" $OPTS --cache-dir "$CACHE_DIR" > /dev/null 2>&1 &
MPID=$!

READY=0
for i in $(seq 1 30); do
    mount | grep -q " $MP " && READY=1 && break
    sleep 0.5
done
if [ $READY -eq 0 ]; then
    echo "::error::mount not ready after 15s"
    FAIL=1
fi

# Wait for upload to complete
sleep 5

# ---- Step 3: Verify ----
echo "--- Step 3: Verify recovery ---"

# Check .dirty was cleaned (upload completed)
if [ -f "$CACHE_DIR/deadbeef01.dirty" ]; then
    echo "✗ .dirty still exists — upload may have failed or wasn't triggered"
    FAIL=1
else
    echo "✓ .dirty removed (upload completed)"
fi

# Check data in S3 (try mc, then awscli, then curl)
BUCKET="${S3_URL#s3://}"
EXPECTED_PREFIX="RECOVERY-TEST-DATA"
S3_OK=0
if command -v mc &>/dev/null; then
    S3_DATA=$(mc cat "local/$BUCKET/recovery-probe.txt" 2>/dev/null)
    if echo "$S3_DATA" | grep -q "$EXPECTED_PREFIX"; then
        echo "✓ Data uploaded to S3: $S3_DATA"
        S3_OK=1
    fi
elif command -v aws &>/dev/null; then
    S3_DATA=$(AWS_ACCESS_KEY_ID="$S3_ACCESS" AWS_SECRET_ACCESS_KEY="$S3_SECRET" \
        aws --endpoint-url "$S3_ENDPOINT" s3 cp "s3://$BUCKET/recovery-probe.txt" - 2>/dev/null)
    if echo "$S3_DATA" | grep -q "$EXPECTED_PREFIX"; then
        echo "✓ Data uploaded to S3: $S3_DATA"
        S3_OK=1
    fi
else
    S3_DATA=$(curl -sf "${S3_ENDPOINT}/${BUCKET}/recovery-probe.txt" 2>/dev/null)
    if echo "$S3_DATA" | grep -q "$EXPECTED_PREFIX"; then
        echo "✓ Data uploaded to S3: $S3_DATA"
        S3_OK=1
    fi
fi
if [ $S3_OK -eq 0 ]; then
    echo "✗ Data NOT in S3 — recovery failed, data lost!"
    FAIL=1
fi

# ---- Cleanup ----
fusermount3 -u "$MP" 2>/dev/null || true
sleep 1
kill -9 $MPID 2>/dev/null || true
BUCKET="${S3_URL#s3://}"
if command -v mc &>/dev/null; then
    mc rm --recursive --force "local/$BUCKET/" 2>/dev/null || true
elif command -v aws &>/dev/null; then
    AWS_ACCESS_KEY_ID="$S3_ACCESS" AWS_SECRET_ACCESS_KEY="$S3_SECRET" \
        aws --endpoint-url "$S3_ENDPOINT" s3 rm "s3://$BUCKET/" --recursive 2>/dev/null || true
fi
rm -rf "$MP" "$CACHE_DIR"

if [ $FAIL -eq 0 ]; then
    echo "  ✅ writeback recovery PASSED"
    exit 0
else
    echo "  ❌ writeback recovery FAILED"
    exit 1
fi
