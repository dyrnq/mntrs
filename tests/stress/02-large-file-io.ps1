# tests/stress/02-large-file-io.ps1
#
# Issue #388 scenario 2: large file sequential I/O.
# Write STRESS_FILE_MB MiB file, read it back, verify md5 matches.
#
# Catches:
#   - writeback buffer overflow / truncation under large writes
#   - multipart upload threshold (issue #46: >200 MiB) end-to-end
#   - read prefetch correctness (issue #132)
#   - .dirty sidecar lifecycle (issue #53)
#
# Configurable via env:
#   STRESS_FILE_MB  — file size in MiB (default 1024 = 1 GiB)
#
# Runtime: ~2-4 min depending on disk speed.

[CmdletBinding()]
param()

. (Join-Path $PSScriptRoot "lib\common.ps1")

$STRESS_FILE_MB = if ($env:STRESS_FILE_MB) { [int]$env:STRESS_FILE_MB } else { 1024 }

$MNTRS_MNT = if ($env:MNTRS_MNT) { $env:MNTRS_MNT } else { "V:" }
$MNT = "${MNTRS_MNT}\stress-02"
$WORK = Join-Path $script:StressScratch "02-large-file-io-$PID"
$CACHE = Join-Path $WORK "cache"

Write-Section "02-large-file-io: ${STRESS_FILE_MB} MiB sequential write+read+md5"
Initialize-Stress
trap {
    Invoke-StressCleanup
    continue
}
if (-not (Test-Path -LiteralPath $WORK)) {
    New-Item -ItemType Directory -Force -Path $WORK | Out-Null
}

Mount-StressDrive -Mountpoint $MNT -CacheDir $CACHE
Register-StressCleanup -Mountpoint $MNT -CacheDir $CACHE

# ── Source: write locally first to know expected md5 ─────────────────
$SRC = Join-Path $WORK "source.bin"
$DST = Join-Path $MNT "big.bin"
$READBACK = Join-Path $WORK "readback.bin"

Write-Log "creating ${STRESS_FILE_MB} MiB source locally (this is the reference) ..."
$srcStart = Get-Date
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$srcBuf = New-Object byte[] (1MB)
$srcFs = [IO.File]::OpenWrite($SRC)
try {
    for ($i = 0; $i -lt $STRESS_FILE_MB; $i++) {
        $rng.GetBytes($srcBuf)
        $srcFs.Write($srcBuf, 0, $srcBuf.Length)
    }
} finally { $srcFs.Dispose() }
$srcElapsed = (Get-Date) - $srcStart
$srcSec = [int]$srcElapsed.TotalSeconds
if ($srcSec -le 0) { $srcSec = 1 }
Write-Log "source done in ${srcSec}s"

$SRC_MD5 = (Get-FileHash -LiteralPath $SRC -Algorithm MD5).Hash.ToLower()
Write-Log "source md5: $SRC_MD5"

# ── Copy to mount (exercises write + writeback) ────────────────────
Write-Log "copying to mount (write + writeback upload) ..."
$copyStart = Get-Date
Copy-Item -LiteralPath $SRC -Destination $DST -Force

# Issue #53: wait for writeback upload to complete (the .dirty sidecar
# is removed only after a successful upload). Poll the cache dir for
# the .dirty file. Match common.sh:54-61.
$dirty = Join-Path $CACHE "big.bin.dirty"
$waitIters = 0
while (Test-Path -LiteralPath $dirty) {
    Start-Sleep -Milliseconds 500
    $waitIters++
    if ($waitIters -gt 600) {
        # 300s budget (matches common.sh:58-60)
        throw "writeback didn't drain within 300s — .dirty sidecar still present"
    }
}
$copyElapsed = (Get-Date) - $copyStart
$copySec = [int]$copyElapsed.TotalSeconds
if ($copySec -le 0) { $copySec = 1 }
$copyRate = "{0:N1}" -f ($STRESS_FILE_MB / $copySec)
Write-Log "copy + drain done in ${copySec}s (${copyRate} MiB/s)"

# ── Read back through FUSE (exercises read path + prefetch) ──────────
Write-Log "reading back through mount ..."
$readStart = Get-Date
Copy-Item -LiteralPath $DST -Destination $READBACK -Force
$readElapsed = (Get-Date) - $readStart
$readSec = [int]$readElapsed.TotalSeconds
if ($readSec -le 0) { $readSec = 1 }
$readRate = "{0:N1}" -f ($STRESS_FILE_MB / $readSec)
Write-Log "read done in ${readSec}s (${readRate} MiB/s)"

# ── Verify ───────────────────────────────────────────────────────────
$READ_MD5 = (Get-FileHash -LiteralPath $READBACK -Algorithm MD5).Hash.ToLower()
Assert-Eq $READ_MD5 $SRC_MD5 "read-back md5 matches source"

# ── Metrics ──────────────────────────────────────────────────────────
if ($script:MntrsProc -and -not $script:MntrsProc.HasExited) {
    Get-StressMetrics -Pid $script:MntrsProc.Id -OutFile (Join-Path $WORK "metrics.txt") -Label "final"
    Write-Log "final metrics:"
    Get-Content -LiteralPath (Join-Path $WORK "metrics.txt") -Tail 1
}

$summary = @(
    "file_mb=$STRESS_FILE_MB src_md5=$SRC_MD5"
    "src_s=$srcSec copy_s=$copySec read_s=$readSec"
    "src_mibps=$(if ($srcSec -gt 0) { "{0:N1}" -f ($STRESS_FILE_MB / $srcSec) } else { 'n/a' })"
    "copy_mibps=$copyRate"
    "read_mibps=$readRate"
)
$summary | Set-Content -LiteralPath (Join-Path $WORK "summary.txt") -Force
Get-Content -LiteralPath (Join-Path $WORK "summary.txt") | Write-Host

Write-Pass "02-large-file-io OK"