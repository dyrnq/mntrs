#!/usr/bin/env python3
# PoC: opendal vs Go SDK (boto3) DELETE comparison.
#
# Insight from the bash PoC (v4): 500 single DELETEs ≈ 500ms
# locally on MinIO, and 1 batched DeleteObjects(500 keys) is also
# ≈ 576ms. The per-batch overhead is constant (~500ms), so
# batching doesn't help on the localhost MinIO. The real question
# is how much of mntrs's 1ms/op is opendal+reqsign (Rust) vs Go
# SDK — boto3 uses the same Go SDK as rclone, so a boto3
# delete-object 500 times is the "rclone-equivalent" baseline.
#
# Install in CI: pip install boto3 awscli (or use vendored).
import os
import sys
import time
import boto3
from botocore.config import Config

ENDPOINT = os.environ.get("ENDPOINT", "http://localhost:9000")
BUCKET = os.environ.get("BUCKET", "mntrs-delete-poc")
N = 500
PREFIX = "poc"

# s3 client with the same settings rclone uses
s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1",
    config=Config(
        signature_version="s3v4",
        retries={"max_attempts": 1, "mode": "standard"},
        connect_timeout=5,
        read_timeout=30,
    ),
)


def upload_files(prefix):
    """Upload N files. Each file is a tiny PUT."""
    print(f"  uploading {N} files to {prefix}/...")
    body = b"x"
    keys = [f"{prefix}/file_{i:04d}.txt" for i in range(1, N + 1)]
    t0 = time.perf_counter()
    for k in keys:
        s3.put_object(Bucket=BUCKET, Key=k, Body=body)
    elapsed = time.perf_counter() - t0
    print(f"  uploaded {N} files in {elapsed*1000:.0f}ms ({elapsed*1e6/N:.0f}us/op)")
    return keys


def test_single_delete(prefix):
    """500 × boto3 delete_object. Same path as rclone's DELETE."""
    keys = [f"{prefix}/file_{i:04d}.txt" for i in range(1, N + 1)]
    # warm-up: 1 delete to amortize connection setup
    s3.delete_object(Bucket=BUCKET, Key=keys[0])
    keys = keys[1:]
    t0 = time.perf_counter()
    for k in keys:
        s3.delete_object(Bucket=BUCKET, Key=k)
    elapsed = time.perf_counter() - t0
    print(
        f"  test 1 single: {elapsed*1000:.0f}ms total = "
        f"{elapsed*1e6/len(keys):.0f}us/call"
    )


def test_batched(prefix, chunk_size):
    """chunked delete_objects with chunk_size keys per call."""
    keys = [f"{prefix}/file_{i:04d}.txt" for i in range(1, N + 1)]
    # warm-up
    s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": keys[0]}], "Quiet": True})
    keys = keys[1:]
    t0 = time.perf_counter()
    n_batches = 0
    for c in range(0, len(keys), chunk_size):
        batch = keys[c : c + chunk_size]
        s3.delete_objects(
            Bucket=BUCKET,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )
        n_batches += 1
    elapsed = time.perf_counter() - t0
    print(
        f"  test chunk={chunk_size}: {elapsed*1000:.0f}ms total "
        f"({n_batches} batches, {elapsed*1e6/n_batches:.0f}us/batch)"
    )


def main():
    # `global` must precede any reference to `s3` in this function
    # (Python SyntaxError otherwise). Declare first, then use.
    global s3

    print(f"=== S3 DELETE: boto3 (Go SDK) vs opendal (Rust) ===")
    print(f"  endpoint: {ENDPOINT}")
    print(f"  bucket:   {BUCKET}")
    print(f"  n files:  {N}")
    print()

    # Ensure bucket exists. boto3 raises many specific exception
    # classes (BucketAlreadyOwnedByYou, BucketAlreadyExists, etc.) —
    # catch broad Exception and swallow on first call.
    try:
        s3.create_bucket(Bucket=BUCKET)
    except Exception:
        pass

    # Rebuild the client — gives us a clean connection pool
    # (the create_bucket call may have left state on the first
    # client we used for setup).
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 1, "mode": "standard"},
            connect_timeout=5,
            read_timeout=30,
        ),
    )

    # 4 prefixes, each with N files
    upload_files(f"{PREFIX}/t1")
    upload_files(f"{PREFIX}/t2")
    upload_files(f"{PREFIX}/t3")
    upload_files(f"{PREFIX}/t4")
    print()

    print(f"=== Test 1: {N} × boto3 delete_object (Go SDK = rclone-equiv) ===")
    test_single_delete(f"{PREFIX}/t1")
    print()

    print(f"=== Test 2: {N} keys in 1 delete_objects (1 batch) ===")
    test_batched(f"{PREFIX}/t2", N)
    print()

    print(f"=== Test 3: chunk=100 delete_objects ({N//100} batches) ===")
    test_batched(f"{PREFIX}/t3", 100)
    print()

    print(f"=== Test 4: chunk=50 delete_objects ({N//50} batches) ===")
    test_batched(f"{PREFIX}/t4", 50)
    print()

    print(f"=== Summary ===")
    print(f"baseline mntrs ({N} × opendal op.delete):  ~574ms (Probe A)")
    print(f"baseline rclone ({N} × Go SDK delete_object): ~136ms (bench)")
    print(f"")
    print(f"Test 1 (Go SDK):  ms total × us/call")
    print(f"  If ~136ms, 0.27ms/op → the 1ms/op mntrs is opendal overhead")
    print(f"  If ~574ms, 1.15ms/op → MinIO per-DELETE is the floor")
    print(f"Test 2/3/4 (batching): ms total")
    print(f"  If Test 2 < Test 1, batching saves the roundtrip")
    print(f"  If Test 2 ≈ Test 1, MinIO CPU-bound on per-DELETE processing")


if __name__ == "__main__":
    main()
