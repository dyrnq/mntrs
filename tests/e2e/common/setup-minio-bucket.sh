#!/usr/bin/env bash
#
# tests/e2e/common/setup-minio-bucket.sh
#
# Create a MinIO bucket and optionally upload a seed file. Used by
# the s3-backend matrix entries in integration.yml, csi-integration.yml,
# and bench.yml (which only creates the bucket — bench uses local files).
#
# Usage:
#   . tests/e2e/common/setup-minio-bucket.sh
#   setup_minio_bucket [bucket] [endpoint] [seed_text] [seed_s3_path]
#
#   defaults: bucket=test-bucket, endpoint=http://localhost:9000,
#             seed_text="hello s3",
#             seed_s3_path=s3://${bucket}/s3-test.txt
#
# To skip the seed upload (bench case), pass seed_text="" — the
# caller then writes its own /tmp/hello.txt for its local-FUSE tests.
#
# Three callers in current use:
#   - integration.yml (matrix.backend.name == 's3')
#   - csi-integration.yml (matrix.test.name == 's3')
#   - bench.yml (no matrix filter — bench always needs a bucket)
#
# `pip3 install awscli` runs unconditionally so the script is
# self-contained; awscli is small and already-cached installs are
# <1s on re-runs.

# Guard against double-include.
if [[ -n "${__SETUP_MINIO_BUCKET_LOADED:-}" ]]; then
    return 0 2>/dev/null || true
fi
__SETUP_MINIO_BUCKET_LOADED=1

setup_minio_bucket() {
    local bucket="${1:-test-bucket}"
    local endpoint="${2:-http://localhost:9000}"
    local seed_text="${3:-hello s3}"
    local seed_s3_path="${4:-s3://${bucket}/s3-test.txt}"
    local access_key="${MINIO_ACCESS_KEY:-minioadmin}"
    local secret_key="${MINIO_SECRET_KEY:-minioadmin}"

    pip3 install awscli

    AWS_ACCESS_KEY_ID="$access_key" AWS_SECRET_ACCESS_KEY="$secret_key" \
        aws --endpoint-url "$endpoint" --no-verify-ssl \
        s3 mb "s3://${bucket}" 2>/dev/null || true

    if [ -n "$seed_text" ]; then
        # Derive the local seed-file path from the s3 path's basename
        # — integration.yml used /tmp/s3-test.txt; csi-integration uses
        # the same. bench.yml skips seed_text to keep its existing
        # /tmp/hello.txt behavior.
        local seed_basename
        seed_basename=$(basename "$seed_s3_path")
        echo "$seed_text" > "/tmp/${seed_basename}"
        AWS_ACCESS_KEY_ID="$access_key" AWS_SECRET_ACCESS_KEY="$secret_key" \
            aws --endpoint-url "$endpoint" --no-verify-ssl \
            s3 cp "/tmp/${seed_basename}" "$seed_s3_path"
    fi
}

# Direct invocation dispatch (when not sourced).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
    setup_minio_bucket "$@"
fi