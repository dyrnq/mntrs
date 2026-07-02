# Benchmark report — Windows WinFSP (issue #378)

> **Status:** Pipeline shipped. Initial baseline not yet seeded —
> see "Seeding the baseline" below.

## Summary

| Field | Value |
|-------|-------|
| Issue | #378 |
| Runner | `windows-latest` (GitHub-hosted Windows VM) |
| Backend | `memory://` (in-process, no S3 deps) |
| Adapter | WinFSP 2.1.25156 (choco pin) |
| mntrs build | `target/release/mntrs.exe` |
| Workloads | ~25 tests across 6 categories |
| Regression threshold | 20% per critical test |
| Cadence | weekly Mon 02:00 UTC + on PR + on `main` push + `workflow_dispatch` |

## Test matrix

* **6 categories** × **25 tests** total = 25 cells (no rclone column)
* Categories: SeqRead, RandRead, concurrent, Write, Copy/Move, ReadDir
* Sizes: 1K / 4K / 64K / 1M / 10M / 100M (matches bash bench)
* Mount: `mntrs mount memory:// V:` in background, wait-for-ready loop
* Pre-stage data via `[IO.File]::WriteAllBytes` against the live mount

### Mount

```pwsh
mntrs mount memory:// V:
```

Cleanup is automatic on EXIT (success/failure/Ctrl+C) via the script's
internal trap. The workflow's `Cleanup mount (always)` step is a
belt-and-suspenders fallback for cases where the trap didn't fire
(e.g., cargo test panic mid-workload).

## Output format

A pipe-separated markdown table mirrors `bench/baseline.txt` (Linux
version), with the rclone column dropped since this PR is mntrs-only
on Windows:

```
  ==========================================================
    BENCHMARK SUMMARY: mntrs (Windows WinFSP / memory://)
  ==========================================================
    Category         | Test                      |   mntrs
    -----------------+---------------------------+---------
    SeqRead          | Get-Content 1K.bin        |  0m0.005s
    ...
    -----------------+---------------------------+---------
    Result: mntrs=25  tests=25  (25 total)
  ==========================================================
```

The `Result:` footer line is the anchor for `check-regression.ps1`'s
winner-count parse (currently just test-count, since no rclone column
exists yet — a future PR with rclone comparison will gate on it
directly).

## Critical tests for regression check

These five test names are hardcoded in
`bench/check-regression.ps1` and must match what
`bench/run_all.ps1` prints in the table exactly:

| Test name | What it guards |
|---|---|
| `Get-Content 100M.bin` | Large sequential read perf (IRP_MJ_READ + writeback) |
| `Random-Read 50x 10M.bin` | Random seek + small read (matches bash "random 50x 1M.bin") |
| `Concurrent 4x 10M.bin` | FUSE-reentrancy / WinFSP dispatcher contention |
| `Write-New 10M.bin` | Write path (writeback + flush + backend) |
| `Get-ChildItem 500` | readdir perf (gates #306 from regressing) |

If any of these regresses >20% vs the baseline, the `Check
regressions` step fails with `::error::` annotations on the PR.

## Seeding the baseline

The shipped `bench/baseline-windows.txt` is an empty placeholder.
To seed it with real numbers:

1. Merge this PR to `main`.
2. On GitHub, go to **Actions → Benchmark (Windows) → Run workflow**
   (use `main` branch). Wait for the run to complete (~5 min).
3. Download the `bench-result-windows` artifact.
4. Copy the file to `bench/baseline-windows.txt` in a small follow-up
   PR titled `windows bench: seed initial baseline`. The diff is
   a single file with the md table + `Result:` footer.
5. Subsequent PRs and weekly cron runs gate on this baseline.

If a future change intentionally regresses one of the critical
tests (e.g., a deliberate writeback rewrite), regenerate the
baseline from a known-good commit using the same workflow_dispatch
procedure.

## Out of scope

- **rclone comparison on Windows.** rclone.exe works fine on
  windows-latest but the env setup (download, config create,
  mount, wait-for-ready) is ~30 LoC of pure ceremony. Deferred
  to a follow-up PR; would add a rclone column to the result
  table and gate on winner-count regression.
- **S3 backend on Windows runner.** MinIO setup is the existing
  `ci-windows.yml` S3 e2e block (single-binary minio.exe +
  mc.exe inlined to avoid cross-step process termination).
  Reusing that block here would add ~80 LoC; deferred.
- **Cross-OS comparable baseline.** `bench/run_all.sh` and
  `bench/run_all.ps1` produce the same output shape, so a future
  PR could consolidate them into one `bench/run_common.sh` +
  per-OS wrapper and produce side-by-side artifacts. Not in
  scope for first PR.

## Validation

* PR triggers the `bench-windows` job on `windows-latest`. With
  no baseline yet, it passes with `::warning::check_regression:
  baseline file not found ... Skipping regression check ...`.
* The `windows` job (ci-windows.yml) still passes — confirms no
  shared infra regression from this PR.
* Local: `pwsh bench/run_all.ps1` mounts V:, runs ~25 tests in
  <3 min on a local Windows machine, prints markdown table,
  tears down V: on EXIT.

## References

- Issue #378 — Windows bench pipeline
- `bench/run_all.sh` — the Linux bench (the structure being mirrored)
- `bench/baseline.txt` — the Linux baseline (format being matched)
- `tests/e2e/bench/check-regression.sh` — the Linux regression check (porting source)
- `tests/microbench.rs` — platform-agnostic microbench tests (also run as sanity gate)
- Memory: `mntrs-winfsp-ci-dll-path` (DLL PATH workaround pattern)
- Memory: `mntrs-dev-conventions` (LF line endings, Co-Authored-By trailer, etc.)