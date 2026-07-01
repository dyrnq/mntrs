# Stress test report — Windows WinFSP (issue #388)

> **Status:** Pipeline shipped. All 6 Linux stress scenarios ported to
> PowerShell. Each scenario that cannot run on Windows emits `  SKIP:`
> and is treated as non-fail by `run-all.ps1`; as of this writing all
> 6 run cleanly on `windows-latest`.

## Summary

| Field | Value |
|-------|-------|
| Issue | #388 |
| Runner | `windows-latest` (GitHub-hosted Windows VM) |
| Backend | `memory://` (in-process, no S3 deps) |
| Adapter | WinFSP 2.1.25156 (choco pin) |
| mntrs build | `target/debug/mntrs.exe` (debug — line numbers in stack traces) |
| Workloads | 6 scenarios (full port of `tests/stress/0[1-6]-*.sh`) |
| Cadence | daily 04:00 UTC + `workflow_dispatch` |

## Test matrix

* **6 scenarios** mirrored 1:1 from the Linux stress suite:
  * `01-large-dir` — N small files + Get-ChildItem / Get-Item / Get-FileHash
  * `02-large-file-io` — `STRESS_FILE_MB` MiB sequential write/read/md5 + writeback drain
  * `03-cache-eviction` — 2× mem-limit write + read-back md5 + mem_cache eviction log scan
  * `04-writeback-concurrent` — N parallel writers (Start-Process) + writeback drain + per-file md5 verify
  * `05-crash-recovery` — `Stop-Process -Force` during writeback + remount cache-fingerprint verify
  * `06-soak-mixed` — long-running R/W/D/Get-ChildItem loop + fd/thread/RSS growth asserts

* **CI smoke sizes** (set by `tests/stress/ci-smoke.ps1`, mirrors the
  Linux `ci-smoke.sh`):

  | Var | CI smoke | Default | What it controls |
  |---|---|---|---|
  | `STRESS_FILES` | 1000 | 10000 | `01-large-dir` file count |
  | `STRESS_BYTES` | 64 | 256 | `01-large-dir` per-file size |
  | `STRESS_FILE_MB` | 256 | 1024 | `02-large-file-io` file size |
  | `STRESS_MEM_MB` | 128 | 256 | `03-cache-eviction` mem-limit |
  | `STRESS_PARALLEL` | 4 | 8 | `04-writeback-concurrent` writers |
  | `STRESS_FILES_PP` | 4 | 8 | `04-writeback-concurrent` files/writer |
  | `STRESS_SOAK_SECS` | 60 | 300 | `06-soak-mixed` duration |
  | `STRESS_INTERVAL` | 1 | 1 | `06-soak-mixed` round-robin interval |

  Total runtime on `windows-latest`: ~4-5 min.

## Entry points

- **`tests/stress/run-all.ps1`** — full-size scenarios; for operator
  soak runs and "I want it to actually exercise the failure modes"
  sessions.
- **`tests/stress/ci-smoke.ps1`** — conservative-size entry point
  for the nightly workflow.

Both accept an optional scenario list as positional args:

```pwsh
pwsh tests/stress/ci-smoke.ps1                       # all 6
pwsh tests/stress/ci-smoke.ps1 01-large-dir          # one
pwsh tests/stress/ci-smoke.ps1 02-large-file-io 06-soak-mixed
```

## SKIP semantics

If a scenario cannot run on the current host (e.g. WinFSP feature not
wired, memory backend rejected, environment limitation), call
`Write-Skip "<reason>"` from `lib/common.ps1`. It emits a
`  SKIP <reason>` line and exits with code **77** (autotools
convention, mirrors bash `common.sh`'s `exit 77` pattern).
`run-all.ps1` classifies exit code 77 as SKIP (not FAIL). The suite
passes if 0 FAILs, regardless of SKIP count. The final summary line
lists skipped scenarios explicitly:

```
stress suite: 5 passed, 0 failed, 1 skipped
skipped: 03-cache-eviction
```

Use SKIP sparingly — only for scenarios that genuinely cannot run on
the host. A scenario that runs to completion but reports a degraded
result should FAIL, not SKIP.

Exit-code classification (not log-grep) is used so Write-Host output
(to the Information stream) doesn't need to round-trip through a file
just to be parsed. Only stdout is redirected to the per-scenario log
file; classification reads `$proc.ExitCode` directly.

## Platform differences vs Linux

| Linux bash | Windows PowerShell | Why |
|---|---|---|
| `--daemon --daemon-wait` | (none) — process is foreground | `cfg(not(windows))` in `src/cmd/mount.rs` — WinFSP session held by parent process |
| `--allow-other` | (none) | `cfg(not(windows))` — uses `/etc/fuse.conf`; not applicable on WinFSP |
| Mount ready check: `grep " $mnt " /proc/self/mounts` | `Test-Path V:\` polling | Win32 equivalent: kernel-mode drive query |
| `dd if=/dev/urandom of=...` | `[IO.File]::WriteAllBytes(...)` with `[System.Security.Cryptography.RandomNumberGenerator]::Fill(buf)` | Avoids /dev/urandom |
| `md5sum` | `Get-FileHash -Algorithm MD5` | Native .NET |
| `kill -9 $PID` | `Stop-Process -Id $PID -Force` | Native Win32 |
| `find ... -printf '.' \| wc -c` | `(Get-ChildItem ...).Count` | Native |
| `awk '/VmRSS:/ {print $2}' /proc/$PID/status` | `(Get-Process -Id $PID).WorkingSet64 / 1024` | Native .NET |
| `ls /proc/$PID/fd \| wc -l` | `(Get-Process -Id $PID).HandleCount` | Native .NET |
| `xargs -P N bash -c '...'` | `Start-Process pwsh -File writer.ps1` × N | Native parallelism |
| `sync; sleep 2` (drain kernel page cache) | (skipped) | WinFSP isn't backed by a Linux-style page cache; the daemon sees writes synchronously |
| `STRESS_VFS_WRITE_BACK=1` default | `--vfs-write-back 1` | Flag is platform-agnostic (clap) |

## Mount lifecycle

Each per-scenario PS1 script calls `Mount-StressDrive` to bring up
`V:\stress-NN` from `memory://` (subdir so concurrent scenarios
don't collide on the same drive letter). `Register-StressCleanup`
hooks a `PowerShell.Exiting` engine event so on EXIT (success,
failure, Ctrl+C) the cache dir is preserved (if requested) and the
mount is dismounted.

The workflow's `Cleanup mount (always)` step is a belt-and-suspenders
fallback for cases where the trap didn't fire (e.g. cargo build panic
mid-scenario, scenario crashed before trap registration).

## Workflow

`.github/workflows/stress-nightly-windows.yml`:

```yaml
on:
  schedule: [{cron: "0 4 * * *"}]
  workflow_dispatch:
    inputs:
      scenarios:
        description: "Space-separated scenarios (default: all)"
        type: string
```

Steps:

1. `actions/checkout@v4`
2. `dtolnay/rust-toolchain@stable`
3. `Swatinem/rust-cache@v2` (`save-if: main`)
4. `Install WinFSP + protoc` — exact copy of `ci-windows.yml`
   (WinFSP choco pin 2.1.25156 + protoc).
5. `Search winfsp-x64.dll + PATH` — exact copy of `ci-windows.yml`
   (DLL PATH workaround).
6. `Build mntrs (debug)` — `cargo build --bin mntrs`.
7. `Run stress suite` — `pwsh ci-smoke.ps1 <scenarios>` with
   `*>&1 | Tee-Object -FilePath stress-result.txt` (all-streams
   redirect — Write-Host emits to Information stream, not stdout).
8. `Upload stress result` (always) — `stress-result.txt` artifact.
9. `Upload scratch logs on failure` — `${{ runner.temp }}/mntrs-stress/**`
   artifact (7-day retention).
10. `Cleanup mount (always)` — `Stop-Process mntrs -Force` +
    `mntrs.exe unmount V:\stress-NN` + clear `mounts.txt`.

## What each scenario catches

Mapping to issue #388's expected failure modes (matches the Linux
`tests/stress/README.md` mapping):

- **01-large-dir** — readdir chunk-size regressions, inode leak under
  churn, stat-cache invalidation races.
- **02-large-file-io** — writeback buffer overflow/truncation, the
  >200 MiB multipart upload path end-to-end (issue #46), `.dirty`
  sidecar lifecycle (issue #53), read prefetch correctness (issue
  #132).
- **03-cache-eviction** — LRU races (`mem_limiter::release_if_reserved`,
  issue #118), block-cache inconsistency after eviction (issue #55),
  mem_cache A/B parity.
- **04-writeback-concurrent** — concurrency bugs in `writeback::spawn`
  (issue #53 cap math, `MAX_REENQUEUE_CYCLES` race, Semaphore
  accounting), lost writes under non-FIFO upload order, `PENDING_COUNT`
  accounting leaks.
- **05-crash-recovery** — `.dirty` sidecar recovery across crashes,
  cache-dir state corruption under abrupt exit, WinFSP session leak
  when killed before cleanup.
- **06-soak-mixed** — RSS/fd/thread leaks under sustained churn,
  unbounded growth of `DelayQueue`, tokio task join leaks.

## Out of scope

- **macOS port** — per repo policy, no macOS code changes.
- **`07-writeback-cache-optin`** — Linux-only (requires explicit
  `--write-back-cache` opt-in; Windows default already matches the
  opt-in behavior, see `src/cmd/mount.rs:510`).
- **S3 backend on Windows runner** — MinIO setup is heavy and not
  needed for stress coverage of the WinFSP adapter. Memory backend
  exercises the same code paths (writeback upload, read prefetch,
  cache eviction) without the S3 dependency.
- **Cross-OS comparable failures** — the scenarios match the Linux
  test matrix but the timings are not directly comparable (WinFSP
  vs FUSE dispatcher overhead differs by ~2× on the same hardware).

## Validation

**Local (before push):**
1. `cargo fmt --all -- --check && cargo fmt --all && cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo build` per CLAUDE.md.
2. `pwsh tests/stress/ci-smoke.ps1` end-to-end on local WinFSP — expect 6/6 pass in ~5min.
3. `grep -c $'\r' tests/stress/*.ps1 tests/stress/lib/common.ps1 .github/workflows/stress-nightly-windows.yml docs/stress_windows.md` — expect 0 (LF line endings per CLAUDE.md).

**CI (after push):**
1. PR triggers `stress-nightly-windows` job — should pass.
2. Existing `ci-windows.yml`, `bench-windows.yml` jobs still pass — confirms no shared infra regression.
3. Manual `workflow_dispatch` with single-scenario input works (e.g. `01-large-dir`).
4. Failure path uploads scratch logs (manually verify by introducing a deliberate failure).

## References

- Issue #388 — Windows stress pipeline
- `tests/stress/{01..06}*.sh` + `lib/common.sh` — porting sources
- `bench/run_all.ps1` + `bench-windows.yml` — PowerShell + workflow
  patterns being reused (Format-Time / Mount-WinFsp / DLL PATH /
  cleanup trap)
- Memory: `mntrs-winfsp-ci-dll-path` — DLL PATH workaround pattern
- Memory: `mntrs-dev-conventions` — LF line endings, Co-Authored-By
  trailer, dev workflow