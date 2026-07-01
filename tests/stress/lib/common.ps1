# tests/stress/lib/common.ps1
#
# Shared helpers for the #388 Windows stress/stability test suite.
# Sibling of tests/stress/lib/common.sh (Linux bash version).
#
# Sourced from per-scenario scripts like:
#   . "$PSScriptRoot/lib/common.ps1"
#
# Conventions (matching bench/run_all.ps1):
#   - 4-space indent
#   - PascalCase Verb-Noun function names (PowerShell convention)
#   - [CmdletBinding()] at script-top + $ErrorActionPreference = "Stop"
#   - LF line endings
#
# Public API:
#   Initialize-Stress           — resolve paths, build mntrs binary
#   Mount-StressDrive           — start mntrs + WinFSP mount, wait ready
#   Dismount-StressDrive        — stop mntrs + clear drive letter
#   Preserve-StressCache        — move cache dir aside for post-mortem
#   Get-StressMetrics           — sample RSS / fd / thread counts
#   Assert-Eq / Assert-Le / Assert-Ge
#   Write-Log / Write-Pass / Write-Fail / Write-Warn / Write-Section

# shellcheck shell=pwsh
# Idempotency guard (mirrors common.sh's __STRESS_COMMON_LOADED).
if ($global:__STRESS_COMMON_PS1_LOADED) {
    return
}
$global:__STRESS_COMMON_PS1_LOADED = $true

# ── Paths ────────────────────────────────────────────────────────────
# Per-suite scratch dir: $STRSCRATCH/<test-name>-<pid>/
# Default: $env:RUNNER_TEMP/mntrs-stress on GH; /tmp/mntrs-stress elsewhere.
$script:StressScratch = $env:STRSCRATCH
if (-not $script:StressScratch) {
    if ($env:RUNNER_TEMP) {
        $script:StressScratch = Join-Path $env:RUNNER_TEMP "mntrs-stress"
    } else {
        $script:StressScratch = Join-Path $env:TEMP "mntrs-stress"
    }
}

# MNTRS_BIN override; default to a stable copy under STRSCRATCH so
# the binary path doesn't depend on cargo target-dir config. Mirrors
# common.sh:35.
$script:MntrsBin = $env:MNTRS_BIN
if (-not $script:MntrsBin) {
    $script:MntrsBin = Join-Path $script:StressScratch "mntrs.exe"
}

# REPO_ROOT: parent-of-parent-of-this-script (tests/stress/lib/ → repo root).
# Mirrors common.sh:36.
$script:RepoRoot = $env:REPO_ROOT
if (-not $script:RepoRoot) {
    $script:RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
}

if (-not (Test-Path -LiteralPath $script:StressScratch)) {
    New-Item -ItemType Directory -Force -Path $script:StressScratch | Out-Null
}

# ── Logging ──────────────────────────────────────────────────────────
# Mirrors common.sh's log/pass/fail/warn/section with PowerShell-
# native ANSI escape sequences. Write-Host (Information stream) is
# captured by the workflow's `*>&1 | Tee-Object` (see bench-windows.yml).
function Write-Log {
    param([string] $msg)
    $ts = Get-Date -Format "HH:mm:ss"
    Write-Host "[$ts] $msg" -ForegroundColor Cyan
}
function Write-Pass {
    param([string] $msg)
    Write-Host "  PASS $msg" -ForegroundColor Green
}
function Write-Warn {
    param([string] $msg)
    Write-Host "  WARN $msg" -ForegroundColor Yellow
}
function Write-Fail {
    param([string] $msg)
    Write-Host "  FAIL $msg" -ForegroundColor Red
    throw "stress: $msg"
}
function Write-Skip {
    # Exit code 77 is the autotools "skip" convention; run-all.ps1
    # classifies it as SKIP rather than FAIL. Mirrors bash common.sh's
    # `exit 77` pattern in scenarios like 03-cache-eviction.sh when
    # the mem-limit backend isn't available.
    param([string] $msg)
    Write-Host "  SKIP $msg" -ForegroundColor Yellow
    exit 77
}
function Write-Section {
    param([string] $msg)
    Write-Host ""
    Write-Host "━━━ $msg ━━━" -ForegroundColor Magenta
}

# ── Assertions ───────────────────────────────────────────────────────
function Assert-Eq {
    param(
        [Parameter(Mandatory = $true)] $Got,
        [Parameter(Mandatory = $true)] $Want,
        [Parameter(Mandatory = $true)] [string] $Msg
    )
    if ($Got -ne $Want) {
        Write-Fail "$Msg : got '$Got', want '$Want'"
    }
    Write-Pass "$Msg ($Got)"
}
function Assert-Le {
    param(
        [Parameter(Mandatory = $true)] [double] $Got,
        [Parameter(Mandatory = $true)] [double] $Want,
        [Parameter(Mandatory = $true)] [string] $Msg
    )
    if ($Got -gt $Want) {
        Write-Fail "$Msg : $Got > $Want"
    }
    Write-Pass "$Msg ($Got <= $Want)"
}
function Assert-Ge {
    param(
        [Parameter(Mandatory = $true)] [double] $Got,
        [Parameter(Mandatory = $true)] [double] $Want,
        [Parameter(Mandatory = $true)] [string] $Msg
    )
    if ($Got -lt $Want) {
        Write-Fail "$Msg : $Got < $Want"
    }
    Write-Pass "$Msg ($Got >= $Want)"
}

# ── Build mntrs (debug build — has line numbers in stack traces) ───
# Mirrors common.sh:mntrs_setup. When MNTRS_BIN is already set in the
# env (or defaults to the cargo target dir), use it as-is and skip the
# copy — Copy-Item of a file onto itself fails on Windows.
function Initialize-Stress {
    $canonical = (Resolve-Path -LiteralPath $script:MntrsBin -ErrorAction SilentlyContinue).Path
    $targetDebug = Join-Path $script:RepoRoot "target\debug\mntrs.exe"
    $targetDebugCanonical = if (Test-Path -LiteralPath $targetDebug) {
        (Resolve-Path -LiteralPath $targetDebug).Path
    } else { "" }
    $isCargoTarget = ($canonical -eq $targetDebugCanonical)
    if ($isCargoTarget) {
        # Caller pointed us at the cargo build artifact; build it
        # in place if stale and skip the Copy-Item that would fail.
        if (-not (Test-Path -LiteralPath $script:MntrsBin) -or
            (Get-Item -LiteralPath "$script:RepoRoot\src").LastWriteTime -gt
            (Get-Item -LiteralPath $script:MntrsBin).LastWriteTime) {
            Write-Log "Building mntrs (debug) ..."
            Push-Location $script:RepoRoot
            try {
                & cargo build --bin mntrs 2>&1 | Out-Null
                if ($LASTEXITCODE -ne 0) {
                    throw "cargo build failed (exit $LASTEXITCODE)"
                }
            } finally { Pop-Location }
        }
    } elseif (-not (Test-Path -LiteralPath $script:MntrsBin) -or
        (Get-Item -LiteralPath "$script:RepoRoot\src").LastWriteTime -gt
        (Get-Item -LiteralPath $script:MntrsBin).LastWriteTime) {
        Write-Log "Building mntrs (debug) ..."
        Push-Location $script:RepoRoot
        try {
            & cargo build --bin mntrs 2>&1 | Out-Null
            if ($LASTEXITCODE -ne 0) {
                throw "cargo build failed (exit $LASTEXITCODE)"
            }
        } finally { Pop-Location }
        Copy-Item -LiteralPath "$script:RepoRoot\target\debug\mntrs.exe" `
                  -Destination $script:MntrsBin -Force
    }
    Write-Log "mntrs binary: $script:MntrsBin"
}

# ── Mount / unmount ───────────────────────────────────────────────
# Usage: Mount-StressDrive <mountpoint> <cache_dir> [extra mntrs args...]
#
# Mirrors common.sh:mntrs_mount. Notable Windows differences:
#   - No --daemon / --daemon-wait: Windows mount is foreground
#     (cfg(not(windows)) in src/cmd/mount.rs). The parent process
#     holds the WinFSP session directly.
#   - No --allow-other: cfg(not(windows)) (uses /etc/fuse.conf).
#   - Readiness probe: poll Test-Path "<MNT>\" (trailing backslash
#     forces kernel-mode drive query; bare "V:" can resolve via
#     cwd parser without verifying the drive exists). Mirrors the
#     proven pattern in tests/e2e/common/mount-test.ps1:325 and
#     bench/run_all.ps1:208-215.
#   - WinFsp.Launcher service must be running; auto-start if needed
#     (mirrors bench/run_all.ps1:164-174).
#   - PATH += WinFsp\bin so winfsp-x64.dll resolves at mntrs.exe
#     process start (mirrors bench/run_all.ps1:181-193).
#   - --vfs-cache-mode full + --vfs-write-back: same defaults as
#     common.sh so tests 04/05 actually exercise writeback upload.
#   - RUST_LOG=debug by default for mem_cache eviction trace
#     visibility (mirrors common.sh STRESS_RUST_LOG behavior).
#   - <mountpoint> accepts "V:" (drive letter only) OR "V:\subdir"
#     (drive + subdir). The drive letter is what gets passed to
#     mntrs mount (WinFSP rejects paths under drive letters for
#     the mountpoint arg). The subdir is created after the mount
#     is up. Per-scenario subdirs under V: prevent cross-scenario
#     file collisions without colliding on the drive letter.
$script:MntrsProc = $null

function Mount-StressDrive {
    param(
        [Parameter(Mandatory = $true)] [string] $Mountpoint,
        [Parameter(Mandatory = $true)] [string] $CacheDir,
        [Parameter(ValueFromRemainingArguments = $true)] [string[]] $ExtraArgs
    )

    # Split mountpoint into drive letter + (optional) subdir.
    # "V:"     -> drive="V:", subdir=""
    # "V:\foo" -> drive="V:", subdir="\foo"
    if ($Mountpoint -notmatch '^([A-Za-z]):(.*)$') {
        throw "mountpoint must be a drive letter (e.g. V:) or V:\subdir, got: $Mountpoint"
    }
    $driveLetter = $Matches[1].ToUpper() + ":"
    $subdir = $Matches[2]

    if (-not (Test-Path -LiteralPath $CacheDir)) {
        New-Item -ItemType Directory -Force -Path $CacheDir | Out-Null
    }

    # WinFsp.Launcher pre-flight (matches bench/run_all.ps1:164-174).
    $svc = Get-Service WinFsp.Launcher -ErrorAction SilentlyContinue
    if ($null -eq $svc) {
        throw "WinFsp.Launcher service not registered — install WinFSP first"
    }
    if ($svc.Status -ne 'Running') {
        Write-Log "WinFsp.Launcher status=$($svc.Status), starting..."
        Start-Service WinFsp.Launcher -ErrorAction Stop
        Start-Sleep -Seconds 2
    }

    # PATH += WinFsp\bin (matches bench/run_all.ps1:181-193).
    $winFspBin = 'C:\Program Files\WinFsp\bin'
    if (-not (Test-Path $winFspBin)) {
        $winFspBin = 'C:\Program Files (x86)\WinFsp\bin'
    }
    if ((Test-Path $winFspBin) -and ($env:PATH -notlike "*$winFspBin*")) {
        $env:PATH = "$winFspBin;$env:PATH"
    }

    # Build argument list (mirrors common.sh:119-130, minus --daemon /
    # --allow-other which are cfg-gated out on Windows). Mount on the
    # drive letter only — WinFSP rejects subpath mountpoints.
    $vfsWriteBack = if ($env:STRESS_VFS_WRITE_BACK) { $env:STRESS_VFS_WRITE_BACK } else { "1" }
    $argList = @(
        "mount", "memory:///", $driveLetter,
        "--cache-dir", $CacheDir,
        "--vfs-cache-mode", "full",
        "--vfs-write-back", $vfsWriteBack
    )
    if ($ExtraArgs.Count -gt 0) {
        $argList += $ExtraArgs
    }

    $logFile = Join-Path $CacheDir "mount.log"
    Write-Log "mount: $driveLetter (subdir=$subdir, log: $logFile)"

    # Use Start-Process with -PassThru so we can poll + Stop-Process
    # later. Capture daemon stdout only to log file; let stderr go
    # to the parent terminal so mount errors are visible to the
    # caller (the workflow's `*>&1 | Tee-Object` upstream of this
    # captures everything). Matches bench/run_all.ps1:197-201.
    $proc = Start-Process -FilePath $script:MntrsBin `
        -ArgumentList $argList `
        -RedirectStandardOutput $logFile `
        -PassThru -NoNewWindow
    $script:MntrsProc = $proc

    # Wait for the drive letter to register (120 × 500ms = 60s budget,
    # matching bench/run_all.ps1:208-215).
    $ready = $false
    for ($i = 1; $i -le 120; $i++) {
        if ($proc.HasExited) {
            Write-Log "mntrs exited prematurely — log tail:"
            if (Test-Path $logFile) { Get-Content $logFile -Tail 30 }
            throw "mntrs exited (code=$($proc.ExitCode)) before mount registered"
        }
        if (Test-Path "${driveLetter}\") {
            $ready = $true
            break
        }
        Start-Sleep -Milliseconds 500
    }
    if (-not $ready) {
        Write-Log "mount.log tail after 60s timeout:"
        if (Test-Path $logFile) { Get-Content $logFile -Tail 30 }
        throw "$driveLetter did not become ready in 60s"
    }
    Write-Log "$driveLetter ready (pid=$($proc.Id))"

    # Create per-scenario subdir on the mounted drive (served by FUSE
    # so this exercises mkdir / getattr paths through mntrs).
    if ($subdir) {
        $fullSubdir = "${driveLetter}${subdir}"
        if (-not (Test-Path -LiteralPath $fullSubdir)) {
            New-Item -ItemType Directory -Force -Path $fullSubdir | Out-Null
            Write-Log "created $fullSubdir on mount"
        }
    }
}

# Usage: Dismount-StressDrive <mountpoint>
# Mirrors common.sh:mntrs_unmount. Strips any subdir component to
# extract the drive letter for mntrs.exe unmount (which only accepts
# drive letters, not paths). Tries mntrs.exe unmount first (clean
# path: drains writeback + releases session), falls back to
# Stop-Process -Force if the daemon is unresponsive.
function Dismount-StressDrive {
    param([Parameter(Mandatory = $true)] [string] $Mountpoint)

    # Extract drive letter for unmount invocation.
    if ($Mountpoint -notmatch '^([A-Za-z]):') {
        throw "mountpoint must start with drive letter, got: $Mountpoint"
    }
    $driveLetter = $Matches[1].ToUpper() + ":"

    # Clean unmount first (mirrors common.sh:174-177: prefer native
    # unmount, fall back to fusermount).
    try {
        & $script:MntrsBin unmount $driveLetter 2>&1 | Out-Null
    } catch { }

    # Belt-and-suspenders: if mntrs is still running (clean unmount
    # raced), force-stop it.
    if ($script:MntrsProc -and -not $script:MntrsProc.HasExited) {
        try { Stop-Process -Id $script:MntrsProc.Id -Force -ErrorAction Stop }
        catch { Write-Warn "  unmount: Stop-Process failed: $_" }
    }

    # Final wait so the drive letter clears before the next scenario.
    Start-Sleep -Milliseconds 500
}

# ── Cache dir preservation ──────────────────────────────────────────
# Usage: Preserve-StressCache <cache_dir> <label>
# Mirrors common.sh:stress_preserve_cache. Move cache dir aside for
# post-mortem inspection; idempotent.
function Preserve-StressCache {
    param(
        [Parameter(Mandatory = $true)] [string] $CacheDir,
        [Parameter(Mandatory = $false)] [string] $Label = "debug"
    )
    if (Test-Path -LiteralPath $CacheDir) {
        $stamp = Get-Date -Format "HHmmss"
        $preserved = "${CacheDir}-${Label}-${stamp}"
        try {
            Move-Item -LiteralPath $CacheDir -Destination $preserved -Force
            Write-Log "preserved cache for post-mortem: $preserved"
        } catch { Write-Warn "preserve failed: $_" }
    }
}

# ── Metrics ─────────────────────────────────────────────────────────
# Usage: Get-StressMetrics <pid> <out_file> [label]
# Mirrors common.sh:stress_metric. Uses Get-Process to sample
# WorkingSet64 (KB), HandleCount, ThreadsCount for a PID.
function Get-StressMetrics {
    param(
        [Parameter(Mandatory = $true)] [int] $Pid,
        [Parameter(Mandatory = $true)] [string] $OutFile,
        [Parameter(Mandatory = $false)] [string] $Label = ""
    )
    $ts = Get-Date -Format "HH:mm:ss"
    $rss_kb = 0; $fds = 0; $threads = 0
    try {
        $p = Get-Process -Id $Pid -ErrorAction Stop
        $rss_kb = [int]($p.WorkingSet64 / 1024)
        $fds = $p.HandleCount
        $threads = $p.Threads.Count
    } catch { }
    Add-Content -LiteralPath $OutFile -Value "$ts rss_kb=$rss_kb fds=$fds threads=$threads"
}

# ── Convenience: trap cleanup on EXIT ───────────────────────────────
# Usage:
#   . tests/stress/lib/common.ps1
#   Mount-StressDrive $mnt $cache @extra-args
#   Register-StressCleanup -Mountpoint $mnt -CacheDir $cache
#
# Mirrors the trap pattern in 04-writeback-concurrent.sh:42 and
# 05-crash-recovery.sh:48: on EXIT, preserve cache + dismount (cache
# preservation is conditional on the script-level $script:PreserveOnExit
# flag, which scenarios that want post-mortem can set to $true before
# any failure).
$script:PreserveOnExit = $false
$script:CleanupMountpoint = $null
$script:CleanupCacheDir = $null

function Register-StressCleanup {
    param(
        [Parameter(Mandatory = $true)] [string] $Mountpoint,
        [Parameter(Mandatory = $true)] [string] $CacheDir,
        [Parameter(Mandatory = $false)] [bool] $PreserveOnExit = $false
    )
    $script:CleanupMountpoint = $Mountpoint
    $script:CleanupCacheDir = $CacheDir
    $script:PreserveOnExit = $PreserveOnExit
}

# Wire up the cleanup trap exactly once. PowerShell's Register-EngineEvent
# PowerShell.Exiting fires on script exit (success, exception, Ctrl+C)
# — matches bash's `trap '...' EXIT`.
if (-not $global:__STRESS_CLEANUP_REGISTERED) {
    $global:__STRESS_CLEANUP_REGISTERED = $true
    Register-EngineEvent -SourceIdentifier PowerShell.Exiting -SupportEvent -Action {
        if ($script:CleanupMountpoint) {
            try {
                if ($script:PreserveOnExit) {
                    Preserve-StressCache -CacheDir $script:CleanupCacheDir -Label "fail"
                }
                Dismount-StressDrive -Mountpoint $script:CleanupMountpoint
            } catch { }
        }
    } | Out-Null
}