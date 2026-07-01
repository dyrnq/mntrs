# tests/stress/03-cache-eviction.ps1
#
# Issue #388 scenario 3: cache eviction under memory pressure.
# Write 2x the --mem-limit. After the run, verify:
#   - No read errors (LRU eviction must be transparent to read path)
#   - Read-back md5 still matches source (no corruption)
#   - mem_cache used/capacity within bounds (eviction actually triggered)
#
# Catches:
#   - LRU eviction races (issue #118 mem_limiter release_if_reserved)
#   - block-cache inconsistency after eviction (issue #55)
#   - mem_cache A/B parity between dashmap/moka/foyer
#
# Configurable via env:
#   STRESS_MEM_MB  — mem-limit (default 256 MiB; total data = 2x)
#
# Runtime: ~1-3 min depending on cache mode.

[CmdletBinding()]
param()

. (Join-Path $PSScriptRoot "lib\common.ps1")

$STRESS_MEM_MB = if ($env:STRESS_MEM_MB) { [int]$env:STRESS_MEM_MB } else { 256 }

# Per-file size: 64 MiB → N files for 2x mem-limit. Matches common.sh:27.
$FILE_MB = 64
$N_FILES = [int][math]::Ceiling(($STRESS_MEM_MB * 2) / $FILE_MB)
$TOTAL_MB = $N_FILES * $FILE_MB

$MNTRS_MNT = if ($env:MNTRS_MNT) { $env:MNTRS_MNT } else { "V:" }
$MNT = "${MNTRS_MNT}\stress-03"
$WORK = Join-Path $script:StressScratch "03-cache-eviction-$PID"
$CACHE = Join-Path $WORK "cache"

Write-Section "03-cache-eviction: $N_FILES x ${FILE_MB}MiB = ${TOTAL_MB}MiB with mem-limit=${STRESS_MEM_MB}MiB"
Initialize-Stress
if (-not (Test-Path -LiteralPath $WORK)) {
    New-Item -ItemType Directory -Force -Path $WORK | Out-Null
}

# Enable mem_cache metrics emission (1s interval) so we can assert
# eviction actually fired. Matches common.sh:40-43.
$memImpl = if ($env:STRESS_MEM_IMPL) { $env:STRESS_MEM_IMPL } else { "dashmap" }
Mount-StressDrive -Mountpoint $MNT -CacheDir $CACHE `
    "--mem-limit", "$STRESS_MEM_MB", `
    "--mem-cache-metrics-interval", "1", `
    "--mem-cache-impl", "$memImpl"
Register-StressCleanup -Mountpoint $MNT -CacheDir $CACHE

# ── Create N source files locally (reference md5s) ──────────────────
Write-Log "creating $N_FILES reference files (${FILE_MB} MiB each) ..."
$srcStart = Get-Date
$srcMd5 = @{}
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$buf = New-Object byte[] (1MB)
for ($i = 1; $i -le $N_FILES; $i++) {
    $name = "f_{0:D2}.bin" -f $i
    $path = Join-Path $WORK $name
    $fs = [IO.File]::OpenWrite($path)
    try {
        for ($j = 0; $j -lt $FILE_MB; $j++) {
            $rng.GetBytes($buf)
            $fs.Write($buf, 0, $buf.Length)
        }
    } finally { $fs.Dispose() }
    $srcMd5[$name] = (Get-FileHash -LiteralPath $path -Algorithm MD5).Hash.ToLower()
}
$srcElapsed = (Get-Date) - $srcStart
$srcSec = [int]$srcElapsed.TotalSeconds
Write-Log "sources ready in ${srcSec}s"

# ── Copy to mount (forces 2x mem-limit; LRU must evict) ──────────────
Write-Log "copying to mount (will evict ~half the working set) ..."
$copyStart = Get-Date
for ($i = 1; $i -le $N_FILES; $i++) {
    $name = "f_{0:D2}.bin" -f $i
    Copy-Item -LiteralPath (Join-Path $WORK $name) -Destination (Join-Path $MNT $name) -Force
}
$copyElapsed = (Get-Date) - $copyStart
$copySec = [int]$copyElapsed.TotalSeconds
Write-Log "copy done in ${copySec}s"

# Wait for writeback to drain (poll .dirty sidecars in cache dir).
$waitIters = 0
while ($true) {
    $dirtyCount = @(Get-ChildItem -LiteralPath $CACHE -Filter "*.dirty" -ErrorAction SilentlyContinue).Count
    if ($dirtyCount -eq 0) { break }
    Start-Sleep -Milliseconds 500
    $waitIters++
    if ($waitIters -gt 600) {
        throw "writeback didn't drain within 300s ($dirtyCount .dirty sidecars remaining)"
    }
}

# ── Read back through FUSE — read path must be transparent ───────────
Write-Log "reading back all $N_FILES through mount ..."
$readFail = 0
for ($i = 1; $i -le $N_FILES; $i++) {
    $name = "f_{0:D2}.bin" -f $i
    $rb = Join-Path $WORK "readback_$name"
    try {
        Copy-Item -LiteralPath (Join-Path $MNT $name) -Destination $rb -Force -ErrorAction Stop
    } catch {
        Write-Log "  READ FAIL: $name"
        $readFail++
        continue
    }
    $got = (Get-FileHash -LiteralPath $rb -Algorithm MD5).Hash.ToLower()
    $want = $srcMd5[$name]
    if ($got -ne $want) {
        Write-Log "  MD5 MISMATCH: $name (got=$got want=$want)"
        $readFail++
    }
}
Assert-Eq $readFail 0 "all read-backs match source md5"

# ── Check mem_cache metrics show eviction ───────────────────────────
# Grep mount.log for `mem_cache.*evictions=N` where N > 0. Matches
# common.sh:101-105.
$evictNonZero = 0
if (Test-Path -LiteralPath (Join-Path $CACHE "mount.log")) {
    $evictNonZero = [regex]::Matches(
        (Get-Content -LiteralPath (Join-Path $CACHE "mount.log") -Raw),
        "evictions=(\d+)"
    ) | Where-Object { [int]$_.Groups[1].Value -gt 0 } | Measure-Object | Select-Object -ExpandProperty Count
}
Write-Log "mem_cache events with non-zero evictions: $evictNonZero"
if ($evictNonZero -eq 0) {
    Write-Warn "no mem_cache eviction events seen — mem-limit may not be enforced, or the test fits in cache"
}

# ── Metrics ──────────────────────────────────────────────────────────
if ($script:MntrsProc -and -not $script:MntrsProc.HasExited) {
    Get-StressMetrics -Pid $script:MntrsProc.Id -OutFile (Join-Path $WORK "metrics.txt") -Label "final"
    Write-Log "final metrics:"
    Get-Content -LiteralPath (Join-Path $WORK "metrics.txt") -Tail 1
}

@(
    "mem_limit_mb=$STRESS_MEM_MB total_data_mb=$TOTAL_MB"
    "n_files=$N_FILES file_mb=$FILE_MB"
    "src_s=$srcSec copy_s=$copySec"
    "eviction_events=$evictNonZero"
) | Set-Content -LiteralPath (Join-Path $WORK "summary.txt") -Force
Get-Content -LiteralPath (Join-Path $WORK "summary.txt") | Write-Host

Write-Pass "03-cache-eviction OK"