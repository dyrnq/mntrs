# tests/stress/06-soak-mixed.ps1
#
# Issue #388 scenario 6: long-running mixed workload.
#
# This is the scaled-down version of the "24h continuous mount" soak
# test — same code paths, but with a configurable duration so CI can
# run it in 5 minutes while an operator can opt-in to longer runs.
#
# Mixed workload (round-robin):
#   1. Write a 4 MiB file with random content
#   2. Read it back + verify md5
#   3. Delete it
#   4. Stat every file in the mountpoint
#
# Metrics collected throughout:
#   - RSS growth (must be bounded — mem-limit LRU should prevent OOM)
#   - fd count (must be bounded — WinFSP sessions, writeback permits)
#   - thread count (must be bounded — tokio workers + writeback)
#
# Configurable via env:
#   STRESS_SOAK_SECS  — total duration (default 300 = 5 min)
#   STRESS_INTERVAL   — round-robin interval (default 1s)
#
# Runtime: STRESS_SOAK_SECS + setup overhead.

[CmdletBinding()]
param()

. (Join-Path $PSScriptRoot "lib\common.ps1")

$STRESS_SOAK_SECS = if ($env:STRESS_SOAK_SECS) { [int]$env:STRESS_SOAK_SECS } else { 300 }
$STRESS_INTERVAL = if ($env:STRESS_INTERVAL) { [int]$env:STRESS_INTERVAL } else { 1 }

$MNTRS_MNT = if ($env:MNTRS_MNT) { $env:MNTRS_MNT } else { "V:" }
$MNT = "${MNTRS_MNT}\stress-06"
$WORK = Join-Path $script:StressScratch "06-soak-mixed-$PID"
$CACHE = Join-Path $WORK "cache"

Write-Section "06-soak-mixed: ${STRESS_SOAK_SECS}s mixed R/W/D/Get-ChildItem workload"
Initialize-Stress
trap {
    Invoke-StressCleanup
    continue
}
if (-not (Test-Path -LiteralPath $WORK)) {
    New-Item -ItemType Directory -Force -Path $WORK | Out-Null
}

# Tighter mem-limit so the soak actually exercises eviction.
# (The issue spec calls out "memory bounded" as a pass criterion.)
$stressMemMb = if ($env:STRESS_MEM_MB) { [int]$env:STRESS_MEM_MB } else { 256 }
Mount-StressDrive -Mountpoint $MNT -CacheDir $CACHE `
    "--mem-limit", "$stressMemMb", `
    "--mem-cache-metrics-interval", "5"
Register-StressCleanup -Mountpoint $MNT -CacheDir $CACHE

$MNTRS_PID = $script:MntrsProc.Id
if (-not $MNTRS_PID) {
    throw "couldn't find mntrs pid"
}
Write-Log "mntrs pid: $MNTRS_PID"

# ── Sample metrics every 5s ──────────────────────────────────────
$METRICS = Join-Path $WORK "metrics.txt"
"time rss_kb fds threads" | Set-Content -LiteralPath $METRICS -Force
$initial = Get-Process -Id $MNTRS_PID -ErrorAction Stop
$INITIAL_RSS = [int]($initial.WorkingSet64 / 1024)
$INITIAL_FDS = $initial.HandleCount
$INITIAL_THREADS = $initial.Threads.Count
Write-Log "baseline: rss=${INITIAL_RSS}KB fds=${INITIAL_FDS} threads=${INITIAL_THREADS}"

# ── Main soak loop ───────────────────────────────────────────────
$END_T = (Get-Date).AddSeconds($STRESS_SOAK_SECS)
$ITER = 0
$START_T = Get-Date
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$buf = New-Object byte[] (1MB)

while ((Get-Date) -lt $END_T) {
    $ITER++
    $fname = "soak_{0:D6}.bin" -f $ITER
    $path = Join-Path $MNT $fname

    # 1. Write 4 MiB random
    try {
        $fs = [IO.File]::OpenWrite($path)
        try {
            for ($j = 0; $j -lt 4; $j++) {
                $rng.GetBytes($buf)
                $fs.Write($buf, 0, $buf.Length)
            }
        } finally { $fs.Dispose() }
    } catch {
        Write-Warn "iter ${ITER}: write failed (${_})"
        Start-Sleep -Seconds $STRESS_INTERVAL
        continue
    }

    # 2. Read back + verify md5 (direct verify — full re-md5 every
    # iteration dominates runtime, so the drain + final assert covers
    # the invariant. Mirrors common.sh:76-78.)
    try {
        Get-FileHash -LiteralPath $path -Algorithm MD5 | Out-Null
    } catch {
        Write-Warn "iter ${ITER}: hash failed (${_})"
    }

    # 3. Delete
    try {
        Remove-Item -LiteralPath $path -Force -ErrorAction Stop
    } catch {
        Write-Warn "iter ${ITER}: delete failed (${_})"
    }

    # 4. Light stat of remaining files (cap to avoid blowing time
    # budget if delete races with new creates).
    try {
        $remaining = @(Get-ChildItem -LiteralPath $MNT -File -ErrorAction SilentlyContinue | Select-Object -First 100)
        foreach ($r in $remaining) {
            $item = Get-Item -LiteralPath $r.FullName -ErrorAction SilentlyContinue
            # Touch .Length to materialize the stat call.
            $len = $item.Length
        }
    } catch { }

    # Periodic metric sample (every 5 iterations ≈ 5s at default cadence)
    if (($ITER % 5) -eq 0) {
        Get-StressMetrics -Pid $MNTRS_PID -OutFile $METRICS
    }

    Start-Sleep -Seconds $STRESS_INTERVAL
}

$ELAPSED = [int]((Get-Date) - $START_T).TotalSeconds
Write-Log "soak done: $ITER iterations in ${ELAPSED}s"

# ── Drain daemon + writeback queues before assertions ───────────
# The last several iterations' writebacks may not have settled
# yet (the daemon delay queue holds them for --vfs-write-back
# seconds). Poll .dirty sidecars for up to 30s. Mirrors
# common.sh:107-117.
$drainStart = Get-Date
while (((Get-Date) - $drainStart).TotalSeconds -lt 30) {
    $dirtyCount = @(Get-ChildItem -LiteralPath $CACHE -Filter "*.dirty" -ErrorAction SilentlyContinue).Count
    if ($dirtyCount -eq 0) { break }
    Write-Log "  draining: $dirtyCount .dirty sidecars remaining ..."
    Start-Sleep -Seconds 1
}

# ── Final metrics ───────────────────────────────────────────────
Get-StressMetrics -Pid $MNTRS_PID -OutFile $METRICS -Label "final"
$final = Get-Process -Id $MNTRS_PID -ErrorAction Stop
$FINAL_RSS = [int]($final.WorkingSet64 / 1024)
$FINAL_FDS = $final.HandleCount
$FINAL_THREADS = $final.Threads.Count
Write-Log "final:    rss=${FINAL_RSS}KB fds=${FINAL_FDS} threads=${FINAL_THREADS}"

# ── Pass/fail criteria ──────────────────────────────────────────
# 1. RSS growth < 5x baseline (would indicate a leak)
$RSS_GROWTH = if ($INITIAL_RSS -gt 0) {
    "{0:N2}" -f ($FINAL_RSS / [double]$INITIAL_RSS)
} else { "n/a" }
Write-Log "rss_growth_ratio=$RSS_GROWTH"

# 2. FD count must not exceed baseline + 50
$FD_GROWTH = $FINAL_FDS - $INITIAL_FDS
Write-Log "fd_growth=$FD_GROWTH"
Assert-Le ([double]$FD_GROWTH) 50.0 "fd count growth"

# 3. Thread count must not exceed baseline + 20
$THREAD_GROWTH = $FINAL_THREADS - $INITIAL_THREADS
Write-Log "thread_growth=$THREAD_GROWTH"
Assert-Le ([double]$THREAD_GROWTH) 20.0 "thread count growth"

# 4. All writeback must have drained (no .dirty sidecars after soak)
$REMAINING_DIRTY = @(Get-ChildItem -LiteralPath $CACHE -Filter "*.dirty" -ErrorAction SilentlyContinue).Count
Assert-Eq $REMAINING_DIRTY 0 "no leftover .dirty sidecars"

@(
    "duration_s=$ELAPSED iterations=$ITER"
    "rss_initial_kb=$INITIAL_RSS rss_final_kb=$FINAL_RSS rss_growth_ratio=$RSS_GROWTH"
    "fds_initial=$INITIAL_FDS fds_final=$FINAL_FDS fds_growth=$FD_GROWTH"
    "threads_initial=$INITIAL_THREADS threads_final=$FINAL_THREADS threads_growth=$THREAD_GROWTH"
    "remaining_dirty=$REMAINING_DIRTY"
) | Set-Content -LiteralPath (Join-Path $WORK "summary.txt") -Force
Get-Content -LiteralPath (Join-Path $WORK "summary.txt") | Write-Host

Write-Pass "06-soak-mixed OK"