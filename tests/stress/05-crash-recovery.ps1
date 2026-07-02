# tests/stress/05-crash-recovery.ps1
#
# Issue #388 scenario 5: crash recovery.
# Start a write, Stop-Process -Force mntrs while writeback is still
# pending (cache file holds the in-flight bytes), remount, and verify
# the cache file survived the crash.
#
# Catches:
#   - Cache file integrity under abrupt exit (regular file should
#     survive Stop-Process -Force — verifies we're not relying on
#     flush() to persist data)
#   - State corruption in cache dir from abrupt exit
#   - WinFSP session leak when killed before cleanup
#
# Mirrors common.sh:05-crash-recovery.sh. Stop-Process -Id $pid -Force
# is the Win32 equivalent of `kill -9 $PID`. Cache file integrity
# is what we verify — not .dirty sidecar lifecycle (that's a Linux
# FUSE-WRITEBACK_CACHE artifact, not present in mntrs).
#
# Runtime: ~20-40s.

[CmdletBinding()]
param()

. (Join-Path $PSScriptRoot "lib\common.ps1")

$MNTRS_MNT = if ($env:MNTRS_MNT) { $env:MNTRS_MNT } else { "V:" }
$MNT = "${MNTRS_MNT}\stress-05"
$WORK = Join-Path $script:StressScratch "05-crash-recovery-$PID"
$CACHE = Join-Path $WORK "cache"

Write-Section "05-crash-recovery: Stop-Process -Force during writeback, verify cache file survival"
Initialize-Stress
if (-not (Test-Path -LiteralPath $WORK)) {
    New-Item -ItemType Directory -Force -Path $WORK | Out-Null
}

# This scenario exercises the writeback cache (cache file
# verification after a hard kill). Enable --vfs-write-back 1
# so writes go through the local cache first; the post-crash
# test verifies the cache file (not the .dirty sidecar).
Mount-StressDrive -Mountpoint $MNT -CacheDir $CACHE `
    "--vfs-write-back", "1"
# Preserve cache dir on EXIT (failure path) so post-mortem is
# possible — `mntrs.exe unmount` calls `remove_dir_all` which wipes
# the only evidence of what the daemon did/didn't do.
Register-StressCleanup -Mountpoint $MNT -CacheDir $CACHE -PreserveOnExit $true

# ── Write two files (creates cache files immediately) ──────────────
$F1 = Join-Path $MNT "recovered.bin"
$F2 = Join-Path $MNT "recovered2.bin"
Write-Log "writing 4 MiB to $F1 ..."
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$buf = New-Object byte[] (1MB)
$fs = [IO.File]::OpenWrite($F1)
try {
    for ($i = 0; $i -lt 4; $i++) {
        $rng.GetBytes($buf)
        $fs.Write($buf, 0, $buf.Length)
    }
} finally { $fs.Dispose() }

Write-Log "writing 2 MiB to $F2 ..."
$fs = [IO.File]::OpenWrite($F2)
try {
    for ($i = 0; $i -lt 2; $i++) {
        $rng.GetBytes($buf)
        $fs.Write($buf, 0, $buf.Length)
    }
} finally { $fs.Dispose() }

# ── Build a fingerprint of the cache dir (sorted size:md5 per file) ─
# Direct disk md5 (not FUSE) so the kernel page cache can't serve
# different bytes than what we wrote.
# Drain budget raised to 60s (was 30s in common.sh) for CI's higher-
# latency ubuntu-24.04 kernel. On Windows we don't have a page
# cache race, but the writeback upload worker may still be running
# (default --vfs-write-back 1s delay). Mirrors common.sh:74-89.
$drainStart = Get-Date
$initialLines = 0
while (((Get-Date) - $drainStart).TotalSeconds -lt 60) {
    $files = @(Get-ChildItem -LiteralPath $CACHE -File `
        | Where-Object { $_.Name -notmatch '\.log$' -and $_.Name -notmatch '\.dirty$' -and $_.Name -notmatch '\.block$' })
    $initialLines = $files.Count
    if ($initialLines -ge 2) { break }
    if ($script:MntrsProc.HasExited) {
        Write-Log "daemon died during 60s drain — bailing"
        if (Test-Path (Join-Path $CACHE "mount.log")) {
            Get-Content (Join-Path $CACHE "mount.log") -Tail 30
        }
        break
    }
    Start-Sleep -Seconds 1
}

# Fingerprint function: sorted "size md5" pairs of every non-meta
# file in the cache dir. Mirrors common.sh:91-105.
function Get-CacheFingerprint {
    param([string] $Dir)
    $lines = @(Get-ChildItem -LiteralPath $Dir -File `
        | Where-Object { $_.Name -notmatch '\.log$' -and $_.Name -notmatch '\.dirty$' -and $_.Name -notmatch '\.block$' } `
        | Sort-Object Name `
        | ForEach-Object {
            $h = (Get-FileHash -LiteralPath $_.FullName -Algorithm MD5).Hash.ToLower()
            "{0} {1}" -f $_.Length, $h
        })
    return $lines
}

$PRE_FP = @(Get-CacheFingerprint -Dir $CACHE)
$PRE_LINES = $PRE_FP.Count
Write-Log "cache dir fingerprint before crash ($PRE_LINES files):"
$PRE_FP | ForEach-Object { Write-Host "    $_" }
Assert-Ge $PRE_LINES 2 "at least two cache files exist before crash"

# ── Verify the two expected files made it into the cache as full-size ─
$sized4M = @($PRE_FP | Where-Object { $_.StartsWith("4194304 ") }).Count
$sized2M = @($PRE_FP | Where-Object { $_.StartsWith("2097152 ") }).Count
Assert-Ge $sized4M 1 "4 MiB cache file present"
Assert-Ge $sized2M 1 "2 MiB cache file present"

# ── Stop-Process -Force the mntrs daemon ────────────────────────────
Write-Log "Stop-Process -Force mntrs daemon (pid=$($script:MntrsProc.Id)) ..."
try { Stop-Process -Id $script:MntrsProc.Id -Force -ErrorAction Stop }
catch { Write-Warn "Stop-Process failed: $_" }
Start-Sleep -Seconds 1
# mntrs.exe unmount (clean path); ignore failure since we just killed it.
try { & $script:MntrsBin unmount $MNT 2>&1 | Out-Null } catch { }
$script:MntrsProc = $null  # so cleanup doesn't try to Stop-Process again
Start-Sleep -Seconds 1

# ── Verify the cache files survived the crash with intact content ──
$POST_FP = @(Get-CacheFingerprint -Dir $CACHE)
$POST_LINES = $POST_FP.Count
Assert-Eq $POST_LINES $PRE_LINES "cache file count survived crash"
Assert-Eq ($POST_FP -join "`n") ($PRE_FP -join "`n") "cache file sizes+md5s unchanged after crash"

# ── Remount and verify the cache files aren't corrupted by recovery ─
Write-Log "remounting to verify recovery doesn't damage cache files ..."
# Re-source helpers in case env was clobbered.
. (Join-Path $PSScriptRoot "lib\common.ps1")
Mount-StressDrive -Mountpoint $MNT -CacheDir $CACHE `
    "--vfs-write-back", "1"
Start-Sleep -Seconds 1  # let recovery startup scan the cache dir

$POST_RECOVERY_FP = @(Get-CacheFingerprint -Dir $CACHE)
$POST_RECOVERY_LINES = $POST_RECOVERY_FP.Count
Assert-Eq $POST_RECOVERY_LINES $PRE_LINES "cache file count after remount"
Assert-Eq ($POST_RECOVERY_FP -join "`n") ($PRE_FP -join "`n") "cache file md5s unchanged after remount"

# ── Final metrics ──────────────────────────────────────────────────
# Re-fetch the proc after remount (Mount-StressDrive updates
# $script:MntrsProc).
if ($script:MntrsProc -and -not $script:MntrsProc.HasExited) {
    Get-StressMetrics -Pid $script:MntrsProc.Id -OutFile (Join-Path $WORK "metrics.txt") -Label "final"
    Write-Log "final metrics:"
    Get-Content -LiteralPath (Join-Path $WORK "metrics.txt") -Tail 1
}

@(
    "cache_files=$PRE_LINES"
    "pre_fp:"
    $PRE_FP
    "post_fp:"
    $POST_FP
) | Set-Content -LiteralPath (Join-Path $WORK "summary.txt") -Force
Get-Content -LiteralPath (Join-Path $WORK "summary.txt") | Write-Host

Write-Pass "05-crash-recovery OK"