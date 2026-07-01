# bench/check-regression.ps1
#
# Detect Windows bench regressions vs a baseline. Sibling of
# tests/e2e/bench/check-regression.sh (the Linux version). Same
# contract: parse the pipe-separated table that bench/run_all.ps1
# emits, compare critical tests against bench/baseline-windows.txt,
# fail if any regress > threshold.
#
# Usage (direct invocation):
#   pwsh bench/check-regression.ps1 `
#       -Current bench-result.txt `
#       -Baseline bench/baseline-windows.txt `
#       -Threshold 0.20
#
# Usage (dot-source):
#   . bench/check-regression.ps1
#   Check-Regression -Current bench-result.txt -Baseline bench/baseline-windows.txt -Threshold 0.20
#
# Returns:
#   exit 0  — no regression
#   exit N>0 — N failures (caller can use as step exit code or to
#              emit GHA ::error:: annotations)

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)] [string] $Current = "bench-result.txt",
    [Parameter(Mandatory = $false)] [string] $Baseline = "bench/baseline-windows.txt",
    [Parameter(Mandatory = $false)] [double] $Threshold = 0.20
)

# Guard against double-include (when dot-sourced).
if ($global:__CHECK_REGRESSION_PS1_LOADED) {
    return
}
$global:__CHECK_REGRESSION_PS1_LOADED = $true

# ── Helpers ───────────────────────────────────────────────────────────

# Parse-Time: convert "0m0.073s" or "1m23.5s" to seconds (float).
# Mirrors the bash parse_time regex (same input shape — bash bench
# format is reused so the two pipelines can share tooling later).
function Parse-Time {
    param([string] $t)
    if ([string]::IsNullOrWhiteSpace($t)) { return 0.0 }
    $m_part, $s_part = $t -split 'm', 2
    $s_part = $s_part.TrimEnd('s')
    return [double]$m_part * 60.0 + [double]$s_part
}

# Parse-Winners: extract "Result: mntrs=25  tests=25  (25 total)"
# line from a bench result file. Sets globals Mntrs, Tests via
# the script scope. No rclone column in Windows format (first PR
# is mntrs-only).
function Parse-Winners {
    param([string] $file)
    $script:Mntrs = 0
    $script:Tests = 0
    if (-not (Test-Path -LiteralPath $file)) { return }
    $line = Select-String -LiteralPath $file -Pattern '^\s*Result:\s*mntrs=' -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $line) {
        Write-Warning "::warning::check_regression: no 'Result:' line in $file"
        return
    }
    if ($line -match 'mntrs=(\d+)') { [void]($script:Mntrs = [int]$Matches[1]) }
    if ($line -match 'tests=(\d+)') { [void]($script:Tests = [int]$Matches[1]) }
}

# Get-Mntrs-Time: extract the mntrs column time for a given test name.
# Returns empty string if not found. The pipe-separated table format
# is shared with bench/baseline.txt so this works for both.
function Get-Mntrs-Time {
    param(
        [string] $file,
        [string] $test
    )
    if (-not (Test-Path -LiteralPath $file)) { return "" }
    $allLines = Get-Content -LiteralPath $file
    foreach ($l in $allLines) {
        # Match: `| <test> | <time> [|]` with optional whitespace and
        # optional trailing pipe. The trailing pipe is required by
        # the bash 5-column format (cat | test | mntrs | rclone |
        # winner) but the Windows 3-column format (cat | test |
        # mntrs) omits it. Making it optional lets one regression
        # script parse both.
        # Anchored to `|` boundaries to avoid partial-name false hits.
        # Single capture group `(\S+)` for the time column — return
        # $Matches[1].
        $pat = "\|\s*$([regex]::Escape($test))\s*\|\s*(\S+)\s*\|?"
        if ($l -match $pat) {
            return $Matches[1]
        }
    }
    return ""
}

# ── Main check ────────────────────────────────────────────────────────

function Check-Regression {
    param(
        [string] $Current,
        [string] $Baseline,
        [double] $Threshold
    )

    if (-not (Test-Path -LiteralPath $Current)) {
        Write-Error "::error::check_regression: current result file not found: $Current"
        return 1
    }
    if (-not (Test-Path -LiteralPath $Baseline)) {
        Write-Warning "::warning::check_regression: baseline file not found: $Baseline"
        Write-Warning "  Skipping regression check (no baseline to compare against)."
        Write-Warning "  Run a known-good bench, copy it to $Baseline, and commit it."
        return 0
    }

    $failures = 0
    $warns = 0

    $thresholdPct = [int]($Threshold * 100)
    Write-Host "==> Regression check: $Current vs $Baseline (threshold: ${thresholdPct}%)"

    # --- Check 1: overall test counts (no winner count on Windows yet) ---
    # Future PR with rclone comparison will add a winner-count gate.
    # For now: if the current run dropped a test category entirely,
    # warn (don't fail).
    Parse-Winners -file $Current
    $cur_m = $script:Mntrs; $cur_n = $script:Tests
    Parse-Winners -file $Baseline
    $base_m = $script:Mntrs; $base_n = $script:Tests

    if ($base_n -gt 0) {
        if ($cur_n -lt $base_n) {
            Write-Warning "::warning::fewer tests in current run ($cur_n) than baseline ($base_n)"
            $warns++
        } else {
            Write-Host "  test count OK: current=$cur_n, baseline=$base_n"
        }
    }

    # --- Check 2: per-test time on critical tests ---
    # High-signal workloads where a regression is most likely a real
    # bug. Must match the test names exactly as bench/run_all.ps1
    # prints them in the table.
    $critical_tests = @(
        "Get-Content 100M.bin",
        "Random-Read 50x 10M.bin",
        "Concurrent 4x 10M.bin",
        "Write-New 10M.bin",
        "Get-ChildItem 500"
    )

    foreach ($test in $critical_tests) {
        $cur_time = Get-Mntrs-Time -file $Current -test $test
        $base_time = Get-Mntrs-Time -file $Baseline -test $test

        if ([string]::IsNullOrEmpty($cur_time) -or [string]::IsNullOrEmpty($base_time)) {
            Write-Warning "  ::warning:::: '$test' not in both files (cur='$cur_time' base='$base_time'), skipping"
            $warns++
            continue
        }

        $cur_sec = Parse-Time -t $cur_time
        $base_sec = Parse-Time -t $base_time
        if ($base_sec -eq 0) { continue }  # divide-by-zero guard

        # Noise floor: skip tests whose baseline is below 0.1s.
        # At sub-100ms, single-digit-ms run-to-run jitter translates
        # to 20%+ percentage swings, so the percentage threshold
        # produces false positives. The bash version's critical
        # tests (cat 100M, random 50x 1M, concurrent 4x 10M) all
        # have baselines in the 0.5-3s range so they don't trip
        # this; the PS version adds Random-Read 50x 10M.bin (~7ms)
        # and Get-ChildItem 500 (~60ms, marginal) which do.
        if ($base_sec -lt 0.1) {
            Write-Warning "  ::warning:::: '$test' baseline ${base_time} is below 0.1s noise floor (cur='$cur_time'), skipping regression check"
            $warns++
            continue
        }

        $slow_pct = ($cur_sec - $base_sec) / $base_sec

        if ($slow_pct -gt $Threshold) {
            $slowPctStr = "{0:N1}" -f ($slow_pct * 100)
            Write-Error "::error::$test : ${cur_time} vs baseline ${base_time} (${slowPctStr}% slower, threshold ${thresholdPct}%)"
            $failures++
        } else {
            Write-Host "  $test OK: ${cur_time} (baseline ${base_time})"
        }
    }

    Write-Host ""
    Write-Host "==> Regression check complete: $failures failure(s), $warns warning(s)"
    return $failures
}

# ── Entry point (when run directly, not dot-sourced) ─────────────────

if ($MyInvocation.InvocationName -ne '.') {
    $exitCode = Check-Regression -Current $Current -Baseline $Baseline -Threshold $Threshold
    exit $exitCode
}