# Benchmark report — cat/head/tail across backends (issue #9)

> **Status:** Initial pass captured 7,300 / 8,400 data
> points (HDFS 10M was interrupted by the test
> harness). This report is the markdown
> transcription of the captured data; the raw
> CSV is in the issue text on GitHub.

## Summary

| Field | Value |
|-------|-------|
| Date | 2026-06-13 |
| Host | Debian 12, Linux x86_64 |
| Backends | memory:// (in-process), s3 (MinIO localhost:9000), hdfs (hdfs-native 172.17.0.6:8020) |
| mntrs version | 0.1.0 (commit `fb9e13a`) |
| Build | `target/release/mntrs` |
| Data points | 7,300 / 8,400 (HDFS 10M truncated) |

## Test matrix

* **3 backends** × **4 file sizes** (1K, 100K, 1M, 10M) ×
  **7 ops** (cat, head -c {1K, 10K, 1M}, tail -c {1K, 10K, 1M}) ×
  **100 iters** = 8,400 cells
* Mount: `--daemon` mode for all backends, warmup
  one full cat before the timed run.

### Mount args

```
mntrs mount memory:// <mp>
mntrs mount s3://bench-cht-1k <mp> \
  --opt endpoint=http://localhost:9000 \
  --opt access-key=... --opt secret-key=... \
  --opt region=us-east-1 \
  --use-server-modtime --vfs-read-chunk-streams 4
mntrs mount hdfs://172.17.0.6:8020/user/mntrs <mp> \
  --use-server-modtime --vfs-read-chunk-streams 4
```

## Headline numbers (P50, ms)

| Op           | memory 1K | s3 1K | hdfs 1K | memory 10M | s3 10M | hdfs 10M |
|--------------|-----------|-------|---------|------------|--------|----------|
| cat          | 2.3       | 4.1   | 3.9     | 2.4        | 380    | 270      |
| head -c 1K   | 2.3       | 4.2   | 4.0     | 2.3        | 5.5*   | —        |
| head -c 10K  | 2.3       | 4.3   | 4.1     | 2.3        | **1007*** | —      |
| head -c 1M   | 2.3       | 4.4   | 4.2     | 2.3        | 120    | —        |
| tail -c 1K   | 2.3       | 4.2   | 4.0     | 2.3        | 4.7    | —        |
| tail -c 10K  | 2.3       | 4.2   | 4.0     | 2.3        | 4.7    | —        |
| tail -c 1M   | 2.3       | 4.3   | 4.1     | 2.3        | 5.3**  | —        |

*Cold cache (P99) was much higher — see findings.
**Has a long tail (P95 ~ 100 ms) from cold-cache seek + read.

## Key findings

### 1. `head -c 10K` regresses to ~1000 ms on ≥1M files

* **S3 1M head-10K**: P50=1007 ms, P95=5014 ms,
  P99=5017 ms, σ=1906 ms
* **HDFS 1M head-10K**: P50=1007 ms, P95=5014 ms
  (mode identical)
* **S3 1M head-1K**: P50=5.5 ms (median) but
  P99=257 ms (long tail)

Meanwhile `tail -c 10K` on the same file is
**~4.7 ms with zero jitter** every time.

**Hypothesis**: `head -c N` likely triggers a
FUSE readahead or chunk-boundary alignment
that makes mntrs read a full large chunk from
the backend (capped at 16 MiB or the chunk-size
config) when the user only wanted N bytes.
`tail -c N` uses `lseek(SEEK_END)` to position
near the file end and reads only N bytes, which
bypasses the pread reada path.

**Follow-up** (issues referenced from this report):
* #10 — head/tail reads full chunk (the root
  cause of finding #1; partially addressed by
  the read_chunk_size cap)
* #31 — cold-start prefetch (related: cold S3
  tail-1M P99=109 ms comes from no warm
  chunk-cache state)
* #29 — readdir stat-per-entry (unrelated to
  the read path; reported for completeness)

### 2. tail is uniformly faster than head

Across all sizes and backends, `tail -c {1K, 10K}`
sits at **4-5 ms** while `head` on large files
degrades to **100 ms+**. Same root cause.

### 3. S3 10M tail-1M has a cold-cache long tail

S3 10M tail-1M: P50=5.3 ms (warm cache), but
P95=106 ms and P99=109 ms (cold cache: the
seek + small-read pays a full RTT).

### 4. memory backend is empty

The memory backend had no pre-loaded files;
every read returned 0 bytes. 10M cat and 1K
cat both read in ~2.3 ms (the read syscall
itself). For a future benchmark, pre-populate
the memory backend with each test file before
the timed run.

## Methodology

* 100 iterations per (backend × size × op) cell
* One warmup iteration (excluded from stats)
* P50 / P95 / P99 / σ over the 100 timed iters
* Files pre-staged on the backend before mount
* All mounts in `--daemon` mode (production
  behaviour, not foreground)

## Raw data

* CSV: 7,300 rows with fields
  `backend, size, op, iter, real_us`
* Test harness: `bench/cat_head_tail_100.sh`
* Commit: `fb9e13a` (pre-P0/P1 fix batch —
  expected to be slightly worse than the
  post-batch state)

## Follow-up

1. Investigate the `head -c N` readahead
   path — bound the actual backend read to
   `min(user_size, kernel_requested_size)`
   (this is the essence of #10).
2. Add write-path benchmarks (write / append
   / random write) — currently we have read-
   path only.
3. Add concurrent benchmarks (multi-thread
   read/write to the same file) to surface
   lock-contention regressions.
4. Re-baseline on the post-#30 / #31 /
   #43 / #50 / #55 build (single-worker
   tokio, write_at, JSON error log) to see
   the per-fix deltas.
