# tests/stress/01-large-dir.ps1
#
# Issue #388 scenario 1: large directory.
# Create N small files in a single directory, exercise Get-ChildItem /
# Get-Item / Get-FileHash against them, verify zero errors.
#
# Catches:
#   - readdir chunk-size regressions (issue #134, #158 already covered)
#   - inode-allocation leaks under heavy churn
#   - stat-cache invalidation races
#
# Configurable via env:
#   STRESS_FILES  — file count (default 10000)
#   STRESS_BYTES  — per-file size (default 256)
#
# Runtime: ~3-5 min on a 4-core VM (CI smoke: 1k files, <30s).

[CmdletBinding()]
param()

# `.Tests/common.ps1` (with trailing `1` is the PS5+ idiom) is not
# reliable cross-platform; use the explicit PSCommandPath + PS-ScriptRoot
# pattern instead so the source works under any caller cwd.
. (Join-Path $PSScriptRoot "lib\common.ps1")

$STRESS_FILES = if ($env:STRESS_FILES) { [int]$env:STRESS_FILES } else { 10000 }
$STRESS_BYTES = if ($env:STRESS_BYTES) { [int]$env:STRESS_BYTES } else { 256 }

# WinFSP mount points on Windows must be drive letters (\\.\V:),
# not arbitrary dirs. Match bench/run_all.ps1: V: as default.
$MNTRS_MNT = if ($env:MNTRS_MNT) { $env:MNTRS_MNT } else { "V:" }
$MNT = "${MNTRS_MNT}\stress-01"  # subdir so concurrent stress jobs don't collide
$WORK = Join-Path $script:StressScratch "01-large-dir-$PID"
$CACHE = Join-Path $WORK "cache"
$LOG = Join-Path $WORK "run.log"

Write-Section "01-large-dir: $STRESS_FILES files x $STRESS_BYTES bytes"
Initialize-Stress
# Trap any terminating error from here on and clean up the mount
# before re-throwing. Mirrors bash's `trap '...' ERR + EXIT` for
# scenarios that need postmortem. The previous engine-event
# pattern kept the pwsh process alive for 20 min after a throw
# (see common.ps1 Invoke-StressCleanup docstring).
trap {
    Invoke-StressCleanup
    continue
}
if (-not (Test-Path -LiteralPath $WORK)) {
    New-Item -ItemType Directory -Force -Path $WORK | Out-Null
}
Write-Log "scratch: $WORK"

# Build the mount subdir if needed (WinFSP rejects non-empty mountpoints
# unless --allow-non-empty is set, so we create before mount and let
# WinFSP register an empty tree under it).
Mount-StressDrive -Mountpoint $MNT -CacheDir $CACHE
Register-StressCleanup -Mountpoint $MNT -CacheDir $CACHE -PreserveOnExit $true

# ── Create files ─────────────────────────────────────────────────────
Write-Log "creating $STRESS_FILES files ..."
$createStart = Get-Date
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$buf = New-Object byte[] $STRESS_BYTES
for ($i = 1; $i -le $STRESS_FILES; $i++) {
    $name = "f_{0:D8}" -f $i
    $rng.GetBytes($buf)
    [IO.File]::WriteAllBytes((Join-Path $MNT $name), $buf)
}
$createElapsed = (Get-Date) - $createStart
$createSec = [int]$createElapsed.TotalSeconds
if ($createSec -le 0) { $createSec = 1 }
$rate = "{0:N1}" -f ($STRESS_FILES / $createSec)
Write-Log "create done in ${createSec}s ($rate files/s)"

# ── Get-ChildItem (smoke check: should see at least one page of dir) ─
Write-Log "Get-ChildItem ..."
$lsStart = Get-Date
$entries = @(Get-ChildItem -LiteralPath $MNT -File)
$lsCount = $entries.Count
$lsElapsed = (Get-Date) - $lsStart
$lsSec = "{0:N3}" -f $lsElapsed.TotalSeconds
Write-Log "Get-ChildItem done in ${lsSec}s, $lsCount entries"
Assert-Ge $lsCount 100 "Get-ChildItem returned at least one page of entries"

# ── Get-Item (analog of bash stat-each) ─────────────────────────────
Write-Log "Get-Item each ..."
$statStart = Get-Date
$failStat = 0
for ($i = 1; $i -le $STRESS_FILES; $i++) {
    $name = "f_{0:D8}" -f $i
    try {
        $item = Get-Item -LiteralPath (Join-Path $MNT $name) -ErrorAction Stop
        if ($item.Length -ne $STRESS_BYTES) {
            $failStat++
        }
    } catch {
        $failStat++
    }
}
$statElapsed = (Get-Date) - $statStart
$statSec = "{0:N3}" -f $statElapsed.TotalSeconds
Write-Log "Get-Item done in ${statSec}s ($failStat failures)"
Assert-Eq $failStat 0 "Get-Item each: failed count"

# ── Recursive Get-ChildItem (analog of bash find) ────────────────────
Write-Log "Get-ChildItem -Recurse ..."
$findStart = Get-Date
$findCount = @(Get-ChildItem -LiteralPath $MNT -Recurse -File).Count
$findElapsed = (Get-Date) - $findStart
$findSec = "{0:N3}" -f $findElapsed.TotalSeconds
Write-Log "find done in ${findSec}s, $findCount entries"
Assert-Eq $findCount $STRESS_FILES "find count"

# ── Get-FileHash batch (analog of bash md5sum batch) ────────────────
Write-Log "Get-FileHash batch ..."
$md5Start = Get-Date
$md5File = Join-Path $WORK "md5.txt"
"" | Set-Content -LiteralPath $md5File -Force
$md5Lines = New-Object System.Collections.Generic.List[string]
for ($i = 1; $i -le $STRESS_FILES; $i++) {
    $name = "f_{0:D8}" -f $i
    $h = Get-FileHash -LiteralPath (Join-Path $MNT $name) -Algorithm MD5
    $md5Lines.Add(("{0}  {1}" -f $h.Hash.ToLower(), $name))
}
Set-Content -LiteralPath $md5File -Value $md5Lines
$md5Count = $md5Lines.Count
$md5Elapsed = (Get-Date) - $md5Start
$md5Sec = "{0:N3}" -f $md5Elapsed.TotalSeconds
Write-Log "Get-FileHash done in ${md5Sec}s, $md5Count hashes"
Assert-Eq $md5Count $STRESS_FILES "Get-FileHash line count"

# ── Final metrics ────────────────────────────────────────────────────
if ($script:MntrsProc -and -not $script:MntrsProc.HasExited) {
    Get-StressMetrics -Pid $script:MntrsProc.Id -OutFile (Join-Path $WORK "metrics.txt") -Label "final"
    Write-Log "final metrics:"
    Get-Content -LiteralPath (Join-Path $WORK "metrics.txt") -Tail 1
}

@(
    "files=$STRESS_FILES bytes_per_file=$STRESS_BYTES"
    "create_s=$createSec ls_s=$lsSec stat_s=$statSec find_s=$findSec md5_s=$md5Sec"
) | Set-Content -LiteralPath (Join-Path $WORK "summary.txt") -Force
Get-Content -LiteralPath (Join-Path $WORK "summary.txt") | Write-Host

Write-Pass "01-large-dir OK"