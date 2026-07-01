# bench/run_all.ps1
#
# mntrs Windows bench workload (Issue #378). Mirror of
# bench/run_all.sh (the Linux mntrs-vs-rclone script), minus rclone
# for the first PR. Runs ~25 tests across 6 categories against a
# WinFSP mount of memory://.
#
# Usage:
#   pwsh bench/run_all.ps1
#
# Env overrides (all optional):
#   MNTRS_BIN   — path to mntrs.exe (default: ./target/release/mntrs.exe)
#   MNTRS_MNT   — drive letter for the mount (default: V:)
#   DATA_DIR    — directory under MNTRS_MNT for staged data
#                 (default: ${MNTRS_MNT}\data)
#   BACKEND     — mntrs backend URI (default: memory://)
#   RESULT_FILE — output file (default: bench-result.txt)
#
# Output: a pipe-separated markdown table on stdout, mirrored to
# bench-result.txt. The `Result:` footer line follows the same
# regex shape bench/check-regression.ps1 expects.
#
# Notes on timing:
#   - Measure-Command { ... } returns a TimeSpan; we extract
#     .TotalSeconds and format as "0m0.073s" so the regression
#     script's Parse-Time regex works unchanged.
#   - Each test gets one warmup iteration that is excluded from
#     timing (matches bench/run_all.sh warmup convention).

[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

# ── Defaults / env ────────────────────────────────────────────────────

$MNTRS_BIN   = $env:MNTRS_BIN
if (-not $MNTRS_BIN) { $MNTRS_BIN = "./target/release/mntrs.exe" }
$MNTRS_BNT   = $env:MNTRS_MNT
if (-not $MNTRS_BNT) { $MNTRS_BNT = "V:" }
$BACKEND     = $env:BACKEND
if (-not $BACKEND) { $BACKEND = "memory://" }
$RESULT_FILE = $env:RESULT_FILE
if (-not $RESULT_FILE) { $RESULT_FILE = "bench-result.txt" }

# String concat (not Join-Path) on purpose: Join-Path validates that
# the root drive exists. At script-top-level the mount hasn't happened
# yet, so Join-Path "V:" "data" throws "Cannot find drive V" and
# triggers the trap before the function table is fully populated.
# Workload functions below use Join-Path freely — by then V: exists.
$DATA_DIR    = "${MNTRS_BNT}\data"
$MANY_DIR    = "${DATA_DIR}\many"

# Trims trailing colon for the case where MNTRS_MNT is "V:"
$MNTRS_MNT_NO_COLON = $MNTRS_BNT.TrimEnd(':')

# ── Result accumulator ───────────────────────────────────────────────

# Each row: "[Category] | [TestName] | [Time]"
$script:Rows = New-Object System.Collections.Generic.List[string]

function Add-Row {
    param([string] $Category, [string] $Test, [string] $Time)
    $line = "    {0,-15} | {1,-25} | {2,9}" -f $Category, $Test, $Time
    $script:Rows.Add($line)
}

# ── Helpers ──────────────────────────────────────────────────────────

# Format-Time: TimeSpan -> "0m0.073s" string.
# Mirrors bash's `time` output shape so the regression script's
# Parse-Time regex (parse_time() in check-regression.sh, Parse-Time
# in check-regression.ps1) can handle both pipelines uniformly.
function Format-Time {
    param([TimeSpan] $ts)
    $total = [int]$ts.TotalSeconds
    $frac = $ts.TotalSeconds - $total
    # bash printf "%dm%.3fs" — 3-decimal fractional part
    return ("{0}m{1:N3}s" -f $total, $frac)
}

# Bench: run a scriptblock, time it, record a row.
function Bench {
    param(
        [string] $Category,
        [string] $Test,
        [scriptblock] $Action
    )
    # Warmup iteration (excluded from timing).
    try {
        & $Action | Out-Null
    } catch {
        Add-Row -Category $Category -Test $Test -Time "FAIL"
        Write-Warning "  $Test FAIL (warmup): $_"
        return
    }
    # Timed iteration.
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        & $Action | Out-Null
    } catch {
        $sw.Stop()
        Add-Row -Category $Category -Test $Test -Time "FAIL"
        Write-Warning "  $Test FAIL: $_"
        return
    }
    $sw.Stop()
    $fmt = Format-Time -ts $sw.Elapsed
    Add-Row -Category $Category -Test $Test -Time $fmt
    Write-Host ("  {0,-25} | {1,9}" -f $Test, $fmt)
}

# Pre-Stage-Data: write the file sizes the bench reads from.
# Random bytes via [System.Random] (avoids depending on /dev/urandom).
function Pre-Stage-Data {
    Write-Host "--- Pre-staging data under $DATA_DIR ---"
    New-Item -ItemType Directory -Force -Path $DATA_DIR | Out-Null

    $rng = [System.Random]::new(0xDEADBEEF)
    $sizes = @(
        @{ Name = "1K.bin";   Bytes = 1KB     },
        @{ Name = "4K.bin";   Bytes = 4KB     },
        @{ Name = "64K.bin";  Bytes = 64KB    },
        @{ Name = "1M.bin";   Bytes = 1MB     },
        @{ Name = "10M.bin";  Bytes = 10MB    },
        @{ Name = "100M.bin"; Bytes = 100MB   }
    )
    foreach ($s in $sizes) {
        $path = Join-Path $DATA_DIR $s.Name
        $buf = New-Object byte[] $s.Bytes
        $rng.NextBytes($buf)
        [IO.File]::WriteAllBytes($path, $buf)
        Write-Host ("  staged {0} ({1} bytes)" -f $s.Name, $s.Bytes)
    }

    # Many small files for the readdir test.
    Write-Host "--- Pre-staging $MANY_DIR (500 files) ---"
    New-Item -ItemType Directory -Force -Path $MANY_DIR | Out-Null
    1..500 | ForEach-Object {
        $p = Join-Path $MANY_DIR ("file_{0:D4}.txt" -f $_)
        [IO.File]::WriteAllText($p, "content $_`n")
    }
}

# ── Mount lifecycle ──────────────────────────────────────────────────

$script:MntrsProc = $null

function Mount-WinFsp {
    Write-Host "--- Mounting $BACKEND at $MNTRS_BNT ---"
    # Mirrors tests/e2e/common/mount-test.ps1:308-313 invocation
    # exactly: -RedirectStandardOutput/Error + -PassThru + -NoNewWindow.
    # Without -NoNewWindow, on windows-latest runners the spawned
    # mntrs exits silently within ~60s (no stdout/stderr, just gone)
    # before the WinFSP kernel-mode attach completes.
    $script:MntrsProc = Start-Process -FilePath $MNTRS_BIN `
        -ArgumentList @("mount", $BACKEND, $MNTRS_BNT) `
        -RedirectStandardOutput "mntrs-bench.stdout.log" `
        -RedirectStandardError  "mntrs-bench.stderr.log" `
        -PassThru -NoNewWindow
    Write-Host "  started mntrs pid=$($script:MntrsProc.Id)"
    # Wait for the drive letter to appear. Mirrors the proven
    # readiness probe in tests/e2e/common/mount-test.ps1:325:
    # 120 × 500ms = 60s budget, Test-Path with trailing backslash
    # to force a kernel-mode drive query (bare "V:" can resolve
    # via the cwd parser without verifying the drive exists).
    $ready = $false
    for ($i = 1; $i -le 120; $i++) {
        if (Test-Path "${MNTRS_BNT}\") {
            $ready = $true
            break
        }
        Start-Sleep -Milliseconds 500
    }
    if (-not $ready) {
        Write-Host "--- mntrs-bench.stdout.log (last 40 lines) ---"
        if (Test-Path "mntrs-bench.stdout.log") { Get-Content "mntrs-bench.stdout.log" -Tail 40 }
        Write-Host "--- mntrs-bench.stderr.log (last 40 lines) ---"
        if (Test-Path "mntrs-bench.stderr.log") { Get-Content "mntrs-bench.stderr.log" -Tail 40 }
        Write-Host "--- mntrs processes ---"
        Get-Process mntrs -ErrorAction SilentlyContinue | Select-Object Id, Name | Format-Table | Out-String | Write-Host
        throw "mntrs mount did not become ready in 60s"
    }
    Write-Host "  $MNTRS_BNT ready"
}

function Unmount-WinFsp {
    if ($script:MntrsProc -and -not $script:MntrsProc.HasExited) {
        try { Stop-Process -Id $script:MntrsProc.Id -Force -ErrorAction Stop }
        catch { Write-Warning "  unmount: Stop-Process failed: $_" }
    }
    # Best-effort DOS-device clear (handles the case where the
    # process exited on its own but the drive letter lingers).
    try {
        if (Get-Command $MNTRS_BIN -ErrorAction SilentlyContinue) {
            & $MNTRS_BIN unmount $MNTRS_BNT 2>&1 | Out-Null
        }
    } catch { }
}

# Register cleanup on EXIT (success, failure, Ctrl+C).
# Only the script-level trap is needed — the try/finally below also
# covers normal cleanup. (Module.OnRemove was tried as a third
# belt-and-suspenders hook but SessionState.Module is $null for .ps1
# files invoked via `&`, so that line throws and the trap fires
# before Unmount-WinFsp is defined.)
trap {
    Unmount-WinFsp
    break
}

# ── Workload definitions ─────────────────────────────────────────────

function Run-SeqRead {
    Write-Host ""
    Write-Host "=== SeqRead ==="
    Bench -Category "SeqRead" -Test "Get-Content 1K.bin" -Action {
        Get-Content -LiteralPath (Join-Path $DATA_DIR "1K.bin") | Out-Null
    }
    Bench -Category "SeqRead" -Test "Get-Content 64K.bin" -Action {
        Get-Content -LiteralPath (Join-Path $DATA_DIR "64K.bin") | Out-Null
    }
    Bench -Category "SeqRead" -Test "Get-Content 1M.bin" -Action {
        Get-Content -LiteralPath (Join-Path $DATA_DIR "1M.bin") | Out-Null
    }
    Bench -Category "SeqRead" -Test "Get-Content 10M.bin" -Action {
        Get-Content -LiteralPath (Join-Path $DATA_DIR "10M.bin") | Out-Null
    }
    Bench -Category "SeqRead" -Test "ReadAllBytes 4K.bin" -Action {
        [IO.File]::ReadAllBytes((Join-Path $DATA_DIR "4K.bin")) | Out-Null
    }
    Bench -Category "SeqRead" -Test "ReadAllBytes 1M.bin" -Action {
        [IO.File]::ReadAllBytes((Join-Path $DATA_DIR "1M.bin")) | Out-Null
    }
    Bench -Category "SeqRead" -Test "ReadAllBytes 10M.bin" -Action {
        [IO.File]::ReadAllBytes((Join-Path $DATA_DIR "10M.bin")) | Out-Null
    }
    Bench -Category "SeqRead" -Test "Get-Content 100M.bin" -Action {
        Get-Content -LiteralPath (Join-Path $DATA_DIR "100M.bin") | Out-Null
    }
}

function Run-RandRead {
    Write-Host ""
    Write-Host "=== RandRead ==="
    $rng = [System.Random]::new(0xCAFEBABE)
    # 50 seeks of 64 KiB each from a 1 MiB file.
    Bench -Category "RandRead" -Test "Random-Read 50x 1M.bin" -Action {
        $path = Join-Path $DATA_DIR "1M.bin"
        $fs = [IO.File]::OpenRead($path)
        try {
            for ($k = 0; $k -lt 50; $k++) {
                $off = $rng.Next(0, [int](1MB - 64KB))
                $fs.Position = $off
                $buf = New-Object byte[] 64KB
                $fs.Read($buf, 0, $buf.Length) | Out-Null
            }
        } finally { $fs.Dispose() }
    }
    # 50 seeks of 64 KiB each from a 10 MiB file.
    Bench -Category "RandRead" -Test "Random-Read 50x 10M.bin" -Action {
        $path = Join-Path $DATA_DIR "10M.bin"
        $fs = [IO.File]::OpenRead($path)
        try {
            for ($k = 0; $k -lt 50; $k++) {
                $off = $rng.Next(0, [int](10MB - 64KB))
                $fs.Position = $off
                $buf = New-Object byte[] 64KB
                $fs.Read($buf, 0, $buf.Length) | Out-Null
            }
        } finally { $fs.Dispose() }
    }
}

function Run-Concurrent {
    Write-Host ""
    Write-Host "=== Concurrent ==="
    Bench -Category "Concurrent" -Test "Concurrent 4x 10M.bin" -Action {
        $target = Join-Path $DATA_DIR "10M.bin"
        $procs = @()
        for ($i = 0; $i -lt 4; $i++) {
            $p = Start-Process -FilePath "powershell" `
                -ArgumentList @("-NoProfile", "-Command", "Get-Content -LiteralPath '$target' | Out-Null") `
                -PassThru `
                -WindowStyle Hidden
            $procs += $p
        }
        foreach ($p in $procs) { $p.WaitForExit() | Out-Null }
    }
}

function Run-Write {
    Write-Host ""
    Write-Host "=== Write ==="
    $rng = [System.Random]::new(0xBEEFCAFE)
    Bench -Category "Write" -Test "Write-New 1K.bin" -Action {
        $buf = New-Object byte[] 1KB
        $rng.NextBytes($buf)
        [IO.File]::WriteAllBytes((Join-Path $DATA_DIR "_w_1K.bin"), $buf)
    }
    Bench -Category "Write" -Test "Write-New 1M.bin" -Action {
        $buf = New-Object byte[] 1MB
        $rng.NextBytes($buf)
        [IO.File]::WriteAllBytes((Join-Path $DATA_DIR "_w_1M.bin"), $buf)
    }
    Bench -Category "Write" -Test "Write-New 10M.bin" -Action {
        $buf = New-Object byte[] 10MB
        $rng.NextBytes($buf)
        [IO.File]::WriteAllBytes((Join-Path $DATA_DIR "_w_10M.bin"), $buf)
    }
    Bench -Category "Write" -Test "Set-Content 1K" -Action {
        Set-Content -LiteralPath (Join-Path $DATA_DIR "_sc_1K.txt") -Value ("x" * 1024) -NoNewline
    }
}

function Run-CopyMove {
    Write-Host ""
    Write-Host "=== Copy/Move ==="
    Bench -Category "Copy" -Test "Copy-Item 1M" -Action {
        Copy-Item -LiteralPath (Join-Path $DATA_DIR "1M.bin") (Join-Path $DATA_DIR "_c_1M.bin") -Force
    }
    Bench -Category "Copy" -Test "Copy-Item 10M" -Action {
        Copy-Item -LiteralPath (Join-Path $DATA_DIR "10M.bin") (Join-Path $DATA_DIR "_c_10M.bin") -Force
    }
    Bench -Category "Move" -Test "Move-Item 1M" -Action {
        # Re-create source so this test is repeatable.
        Copy-Item -LiteralPath (Join-Path $DATA_DIR "_c_1M.bin") (Join-Path $DATA_DIR "_mv_src.bin") -Force
        Move-Item -LiteralPath (Join-Path $DATA_DIR "_mv_src.bin") (Join-Path $DATA_DIR "_mv_dst.bin") -Force
    }
}

function Run-ReadDir {
    Write-Host ""
    Write-Host "=== ReadDir ==="
    Bench -Category "ReadDir" -Test "Get-ChildItem 500" -Action {
        Get-ChildItem -LiteralPath $MANY_DIR -File | Out-Null
    }
}

function Run-Delete {
    Write-Host ""
    Write-Host "=== Delete ==="
    Bench -Category "Delete" -Test "Remove-Item 1M" -Action {
        # Re-create so the test is repeatable.
        Copy-Item -LiteralPath (Join-Path $DATA_DIR "1M.bin") (Join-Path $DATA_DIR "_rm_1M.bin") -Force
        Remove-Item -LiteralPath (Join-Path $DATA_DIR "_rm_1M.bin") -Force
    }
}

# ── Render & emit ────────────────────────────────────────────────────

function Render-Result {
    $total = $script:Rows.Count
    $separator = "    ---------------+---------------------------+---------"
    $lines = @()
    $lines += ""
    $lines += "  =========================================================="
    $lines += "    BENCHMARK SUMMARY: mntrs (Windows WinFSP / $BACKEND)"
    $lines += "  =========================================================="
    $lines += "    Category         | Test                      |   mntrs"
    $lines += $separator
    foreach ($r in $script:Rows) { $lines += $r }
    $lines += $separator
    $lines += "    Result: mntrs=$total  tests=$total  ($total total)"
    $lines += "  =========================================================="
    return $lines
}

# ── Main ─────────────────────────────────────────────────────────────

try {
    Write-Host "============================================"
    Write-Host " mntrs Windows bench (Issue #378)"
    Write-Host " mntrs: $MNTRS_BIN"
    Write-Host " mount: $MNTRS_BNT ($BACKEND)"
    Write-Host " data:  $DATA_DIR"
    Write-Host "============================================"

    if (-not (Test-Path -LiteralPath $MNTRS_BIN)) {
        throw "mntrs binary not found at $MNTRS_BIN. Run 'cargo build --release' first."
    }

    Mount-WinFsp
    Pre-Stage-Data

    Run-SeqRead
    Run-RandRead
    Run-Concurrent
    Run-Write
    Run-CopyMove
    Run-ReadDir
    Run-Delete

    $lines = Render-Result
    $lines | Tee-Object -FilePath $RESULT_FILE
    Write-Host ""
    Write-Host "Result written to $RESULT_FILE"
    exit 0
} catch {
    Write-Error "::error::run_all: $_"
    exit 1
} finally {
    Unmount-WinFsp
}