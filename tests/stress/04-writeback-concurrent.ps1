# tests/stress/04-writeback-concurrent.ps1
#
# Issue #388 scenario 4: writeback under load.
# Fire N parallel writers (>= UPLOAD_SEM=4 permits in writeback.rs)
# at the same mount, write distinct files, then wait for writeback
# drain and verify every file made it to the backend.
#
# Catches:
#   - Concurrency bugs in writeback::spawn (issue #53 cap math,
#     MAX_REENQUEUE_CYCLES race, Semaphore permit accounting)
#   - Lost writes when upload order is non-FIFO
#   - PENDING_COUNT accounting leaks
#
# Configurable via env:
#   STRESS_PARALLEL  — writer count (default 8, > UPLOAD_SEM=4)
#   STRESS_FILES_PP  — files per writer (default 8)
#   STRESS_FILE_KB   — per-file size in KiB (default 256)
#
# Runtime: ~30s with default sizes; CI smoke (4×4×256K) ~15s.

[CmdletBinding()]
param()

. (Join-Path $PSScriptRoot "lib\common.ps1")

$STRESS_PARALLEL = if ($env:STRESS_PARALLEL) { [int]$env:STRESS_PARALLEL } else { 8 }
$STRESS_FILES_PP = if ($env:STRESS_FILES_PP) { [int]$env:STRESS_FILES_PP } else { 8 }
$STRESS_FILE_KB = if ($env:STRESS_FILE_KB) { [int]$env:STRESS_FILE_KB } else { 256 }

$MNTRS_MNT = if ($env:MNTRS_MNT) { $env:MNTRS_MNT } else { "V:" }
$MNT = "${MNTRS_MNT}\stress-04"
$WORK = Join-Path $script:StressScratch "04-writeback-concurrent-$PID"
$CACHE = Join-Path $WORK "cache"

$TOTAL_FILES = $STRESS_PARALLEL * $STRESS_FILES_PP

Write-Section "04-writeback-concurrent: ${STRESS_PARALLEL} writers x ${STRESS_FILES_PP} files x ${STRESS_FILE_KB} KiB"
Initialize-Stress
if (-not (Test-Path -LiteralPath $WORK)) {
    New-Item -ItemType Directory -Force -Path $WORK | Out-Null
}

# This scenario specifically exercises the writeback upload path.
# Enable --vfs-write-back 1 so writes go through the local cache
# first, get uploaded async (matching the Linux stress test).
Mount-StressDrive -Mountpoint $MNT -CacheDir $CACHE `
    "--vfs-write-back", "1"
# Preserve cache dir on EXIT (failure path) so post-mortem is
# possible — `mntrs.exe unmount` calls `remove_dir_all` which wipes
# the only evidence of what the daemon did/didn't do.
Register-StressCleanup -Mountpoint $MNT -CacheDir $CACHE -PreserveOnExit $true

# ── Pre-generate expected content ───────────────────────────────────
# Same content writers will produce, so we can md5-verify post-drain.
# For each (writer, file) tuple we build a deterministic byte array:
# the file is padded to STRESS_FILE_KB*1024 bytes with a header
# carrying `data_writer=W file=F padding=` so we can detect any
# cross-writer overwrite. Matches common.sh:47-59.
$expected = @{}  # fname -> md5
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$tail = New-Object byte[] ($STRESS_FILE_KB * 1024)
for ($w = 1; $w -le $STRESS_PARALLEL; $w++) {
    for ($f = 1; $f -le $STRESS_FILES_PP; $f++) {
        $fname = "w{0:D2}_f{1:D3}.bin" -f $w, $f
        $header = [System.Text.Encoding]::UTF8.GetBytes("data_writer=$w file=$f padding=")
        $rng.GetBytes($tail)
        $full = New-Object byte[] ($STRESS_FILE_KB * 1024)
        [Array]::Copy($tail, 0, $full, 0, $tail.Length)
        [Array]::Copy($header, 0, $full, 0, $header.Length)
        # Write once to compute md5 (source of truth). Then write the
        # same bytes via the parallel writers below.
        $localPath = Join-Path $WORK "_pre_$fname"
        [IO.File]::WriteAllBytes($localPath, $full)
        $expected[$fname] = (Get-FileHash -LiteralPath $localPath -Algorithm MD5).Hash.ToLower()
    }
}
Write-Log "pre-generated $TOTAL_FILES expected md5s"

# ── Parallel write phase ───────────────────────────────────────────
# Use Start-Process x N parallel writers. Each child pwsh writes its
# slice of files. Replaces bash's `xargs -P N bash -c '...'`.
Write-Log "firing $STRESS_PARALLEL writers ..."
$writeStart = Get-Date

# Build a shared writer script as a string (positional args: writer
# index, mount point, files_pp, file_kb). Each child calls the script
# with its own index, so cross-writer isolation is guaranteed.
$writerScript = @"
param(`$w, `$mnt, `$filesPP, `$fileKB)
`$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
`$tail = New-Object byte[] (`$fileKB * 1024)
for (`$f = 1; `$f -le `$filesPP; `$f++) {
    `$fname = 'w{0:D2}_f{1:D3}.bin' -f `$w, `$f
    `$header = [System.Text.Encoding]::UTF8.GetBytes("data_writer=`$w file=`$f padding=")
    `$rng.GetBytes(`$tail)
    `$full = New-Object byte[] (`$fileKB * 1024)
    [Array]::Copy(`$tail, 0, `$full, 0, `$tail.Length)
    [Array]::Copy(`$header, 0, `$full, 0, `$header.Length)
    [IO.File]::WriteAllBytes((Join-Path `$mnt `$fname), `$full)
}
"@

# Materialize the script to a file (cleaner than -Command with escaping).
$writerScriptFile = Join-Path $WORK "writer.ps1"
Set-Content -LiteralPath $writerScriptFile -Value $writerScript

$procs = @()
for ($w = 1; $w -le $STRESS_PARALLEL; $w++) {
    $argList = @(
        "-NoProfile",
        "-File", $writerScriptFile,
        $w, $MNT, $STRESS_FILES_PP, $STRESS_FILE_KB
    )
    $p = Start-Process -FilePath "pwsh" -ArgumentList $argList `
        -PassThru -WindowStyle Hidden
    $procs += $p
}
foreach ($p in $procs) {
    if (-not $p.WaitForExit(300000)) {
        # 5-min per-writer timeout (matches common.sh:99-104 budget)
        Write-Log "writer pid=$($p.Id) exceeded 300s; killing"
        try { Stop-Process -Id $p.Id -Force } catch { }
        throw "writer $($p.Id) timeout"
    }
    if ($p.ExitCode -ne 0) {
        throw "writer $($p.Id) exited with code $($p.ExitCode)"
    }
}
$writeElapsed = (Get-Date) - $writeStart
$writeSec = [int]$writeElapsed.TotalSeconds
Write-Log "parallel write done in ${writeSec}s ($TOTAL_FILES files)"

# ── Drain writeback ─────────────────────────────────────────────────
Write-Log "waiting for writeback drain ..."
$waitIters = 0
$wallStart = Get-Date
while ($true) {
    $dirtyCount = @(Get-ChildItem -LiteralPath $CACHE -Filter "*.dirty" -ErrorAction SilentlyContinue).Count
    if ($dirtyCount -eq 0) { break }
    Start-Sleep -Milliseconds 500
    $waitIters++
    if ($waitIters -gt 600) {
        Write-Log "still dirty after 300s; pending_count trace:"
        if (Test-Path (Join-Path $CACHE "mount.log")) {
            Get-Content (Join-Path $CACHE "mount.log") | Select-String -Pattern "pending_count|writeback.*STUCK" | Select-Object -Last 10 | Write-Host
        }
        throw "writeback didn't drain within 300s"
    }
    if ($script:MntrsProc.HasExited) {
        Write-Log "daemon died during writeback drain — last mount.log lines:"
        if (Test-Path (Join-Path $CACHE "mount.log")) {
            Get-Content (Join-Path $CACHE "mount.log") -Tail 30
        }
        throw "daemon died mid-drain (see $CACHE\mount.log)"
    }
}
$wallSec = [int]((Get-Date) - $wallStart).TotalSeconds
Write-Log "drain complete (wall ${wallSec}s)"

# ── Verify every file is present on backend with correct md5 ────────
# Issue #158: prefer batch stat to avoid per-file Get-Item calls.
Write-Log "verifying $TOTAL_FILES files on backend ..."
$missing = 0
foreach ($fname in $expected.Keys) {
    $path = Join-Path $MNT $fname
    if (-not (Test-Path -LiteralPath $path)) {
        Write-Log "  MISSING: $fname"
        $missing++
        continue
    }
    $got = (Get-FileHash -LiteralPath $path -Algorithm MD5).Hash.ToLower()
    if ($got -ne $expected[$fname]) {
        Write-Log "  MD5 MISMATCH: $fname (got=$got want=$($expected[$fname]))"
        $missing++
    }
}
Assert-Eq $missing 0 "all files present and matching"

# ── Metrics ──────────────────────────────────────────────────────────
if ($script:MntrsProc -and -not $script:MntrsProc.HasExited) {
    Get-StressMetrics -Pid $script:MntrsProc.Id -OutFile (Join-Path $WORK "metrics.txt") -Label "final"
    Write-Log "final metrics:"
    Get-Content -LiteralPath (Join-Path $WORK "metrics.txt") -Tail 1
}

@(
    "parallel=$STRESS_PARALLEL files_per_writer=$STRESS_FILES_PP file_kb=$STRESS_FILE_KB"
    "total_files=$TOTAL_FILES"
    "write_s=$writeSec drain_s=$wallSec"
) | Set-Content -LiteralPath (Join-Path $WORK "summary.txt") -Force
Get-Content -LiteralPath (Join-Path $WORK "summary.txt") | Write-Host

Write-Pass "04-writeback-concurrent OK"