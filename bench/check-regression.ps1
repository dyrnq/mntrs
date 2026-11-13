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
#
# ── Why the extra guards (Issue #687) ─────────────────────────────────
#
# This script shipped with a 20%-threshold gate that never gated
# anything: bench/baseline-windows.txt was a comment-only placeholder,
# so every critical test was skipped for lack of a baseline and
# $failures was always 0. Once the baseline is seeded for real, three
# more problems show up, and all three are handled here:
#
# 1. A failed workload prints the literal "FAIL" in the time column.
#    The original Parse-Time did `"FAIL" -split 'm', 2` → $s_part is
#    $null → `$s_part.TrimEnd('s')` threw "You cannot call a method on
#    a null-valued expression". That exception is non-terminating, so
#    the loop carried on with $cur_sec = $null, `$null -gt 0.20`
#    evaluated to false, and the FAILED workload was reported as "OK"
#    with exit code 0 — a broken critical workload passed the gate.
#    Parse-Time now rejects non-time strings instead of throwing, and
#    a critical workload that reports FAIL fails the gate: not
#    completing is a functional breakage, not a perf regression.
#
# 2. Percentage-only thresholds are meaningless on very fast tests.
#    Measured over 6 Benchmark (Windows) runs on main (2026-10-29 …
#    2026-11-09), the critical tests vary between runs like this:
#
#       Get-Content 100M.bin    6.60 … 7.64s   (spread 15%)
#       Concurrent 4x 10M.bin   1.80 … 2.06s   (spread 13%)
#       Write-New 10M.bin       0.052 … 0.066s (spread 25%)
#       Get-ChildItem 500       0.071 … 0.100s (spread 34%)
#       Random-Read 50x 10M.bin 0.006 … 0.010s (spread 50%)
#
#    A 20% threshold on an 8ms test is a 1.6ms tolerance — pure noise.
#    Hence two guards:
#
#      -MinBaselineSeconds (default 0.1): a baseline shorter than this
#      is not measurable reliably, so the test is skipped with a
#      warning instead of producing false positives.
#
#      -MinDeltaSeconds (default 0.1): a regression only counts when
#      the slowdown exceeds BOTH the percentage threshold and this
#      absolute floor. The slowest observed inter-run jitter is
#      ~30ms, so 0.1s sits above the noise with ~3x margin while
#      staying far below the smallest meaningful signal (20% of
#      Concurrent 4x 10M.bin = 0.4s).
#
#    With both guards the gate keeps the two tests whose spread is
#    well under the threshold (Get-Content 100M.bin, Concurrent 4x
#    10M.bin) and explicitly skips the three that cannot be measured
#    on a shared CI runner.
#
# 3. The baseline itself must be a real result, and it must come from
#    the same kind of run. bench/baseline-windows.txt holds the
#    BENCHMARK SUMMARY table of an actual push/schedule run and
#    bench/baseline-windows-pr.txt holds one of an actual
#    pull_request run — GitHub Actions lands the two event types on
#    measurably different runner hardware (up to 40% apart on the
#    same day), so comparing a pull_request run against the
#    push/schedule baseline would flag every PR as a ~35% regression.
#    The workflow picks the file that matches the event; see the
#    provenance headers in both files.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)] [string] $Current = "bench-result.txt",
    [Parameter(Mandatory = $false)] [string] $Baseline = "bench/baseline-windows.txt",
    [Parameter(Mandatory = $false)] [double] $Threshold = 0.20,
    # Baselines shorter than this (seconds) are noise-dominated and
    # are skipped rather than judged. See note 2 in the header.
    [Parameter(Mandatory = $false)] [double] $MinBaselineSeconds = 0.1,
    # A regression must exceed this absolute slowdown (seconds) in
    # addition to the percentage threshold. See note 2 in the header.
    [Parameter(Mandatory = $false)] [double] $MinDeltaSeconds = 0.1
)

# Guard against double-include (when dot-sourced).
if ($global:__CHECK_REGRESSION_PS1_LOADED) {
    return
}
$global:__CHECK_REGRESSION_PS1_LOADED = $true

# ── Helpers ───────────────────────────────────────────────────────────

# Write-GhaAnnotation: emit a GitHub Actions workflow command.
#
# This deliberately uses Write-Host rather than Write-Error /
# Write-Warning. PowerShell renders those as a decorated block — ANSI
# colour codes, a source-line excerpt and the marker on a line of its
# own — which pushes "::error::" off the start of the line. The
# Actions runner only turns a line into an annotation when the marker
# is the first thing on it, so a Write-Error annotation is silently
# dropped and the log shows a PowerShell error dump instead of the
# message. Write-Host writes the marker verbatim to stdout, which is
# also the convention used by tests/e2e/common/mount-test.ps1.
function Write-GhaAnnotation {
    param(
        [ValidateSet('error', 'warning', 'notice')]
        [string] $Level,
        [string] $Message
    )
    Write-Host "::$Level::$Message"
}

# Test-TimeString: true when $t is a bench time cell, i.e. "0m0.073s"
# or "1m23.5s". A failed workload prints the literal "FAIL" instead,
# which must never reach the arithmetic below.
function Test-TimeString {
    param([string] $t)
    if ([string]::IsNullOrWhiteSpace($t)) { return $false }
    return ($t -match '^\d+m\d+(\.\d+)?s$')
}

# Parse-Time: convert "0m0.073s" or "1m23.5s" to seconds (float).
# Mirrors the bash parse_time regex (same input shape — bash bench
# format is reused so the two pipelines can share tooling later).
# Returns $null for anything that is not a time string (e.g. "FAIL"
# or an empty cell) instead of throwing on a null split component.
function Parse-Time {
    param([string] $t)
    if (-not (Test-TimeString -t $t)) { return $null }
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
        Write-GhaAnnotation -Level warning -Message "check_regression: no 'Result:' line in $file"
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
        [double] $Threshold,
        [double] $MinBaselineSeconds,
        [double] $MinDeltaSeconds
    )

    if (-not (Test-Path -LiteralPath $Current)) {
        Write-GhaAnnotation -Level error -Message "check_regression: current result file not found: $Current"
        return 1
    }
    if (-not (Test-Path -LiteralPath $Baseline)) {
        Write-GhaAnnotation -Level warning -Message "check_regression: baseline file not found: $Baseline"
        Write-Host "  Skipping regression check (no baseline to compare against)."
        Write-Host "  Run a known-good bench, copy it to $Baseline, and commit it."
        return 0
    }

    $failures = 0
    $warns = 0

    $thresholdPct = [int]($Threshold * 100)
    Write-Host "==> Regression check: $Current vs $Baseline (threshold: ${thresholdPct}%, noise floor: ${MinBaselineSeconds}s, min delta: ${MinDeltaSeconds}s)"

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
            Write-GhaAnnotation -Level warning -Message "fewer tests in current run ($cur_n) than baseline ($base_n)"
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

        if ([string]::IsNullOrEmpty($cur_time)) {
            Write-GhaAnnotation -Level warning -Message "'$test' not in current run ($Current), skipping"
            $warns++
            continue
        }

        # A workload that reports FAIL did not complete. That is a
        # functional breakage and must fail the gate — it is not a
        # perf comparison, and swallowing it would hide a real bug.
        if (-not (Test-TimeString -t $cur_time)) {
            Write-GhaAnnotation -Level error -Message "$test : workload did not complete (time column = '$cur_time')"
            $failures++
            continue
        }

        if ([string]::IsNullOrEmpty($base_time)) {
            Write-GhaAnnotation -Level warning -Message "'$test' not in baseline ($Baseline), skipping"
            $warns++
            continue
        }
        if (-not (Test-TimeString -t $base_time)) {
            Write-GhaAnnotation -Level warning -Message "'$test' baseline entry is not a time ('$base_time'), skipping"
            $warns++
            continue
        }

        $cur_sec = Parse-Time -t $cur_time
        $base_sec = Parse-Time -t $base_time
        if ($null -eq $cur_sec -or $null -eq $base_sec) {
            Write-GhaAnnotation -Level warning -Message "'$test' could not be parsed (cur='$cur_time' base='$base_time'), skipping"
            $warns++
            continue
        }
        if ($base_sec -eq 0) { continue }  # divide-by-zero guard

        # Noise floor: a baseline this short varies too much between
        # runs for a percentage threshold to mean anything.
        if ($base_sec -lt $MinBaselineSeconds) {
            Write-GhaAnnotation -Level warning -Message "'$test' baseline ${base_time} is below the ${MinBaselineSeconds}s noise floor (cur='${cur_time}'), skipping"
            $warns++
            continue
        }

        $slow_pct = ($cur_sec - $base_sec) / $base_sec
        $slow_abs = $cur_sec - $base_sec

        # Both conditions must hold: the percentage threshold AND an
        # absolute slowdown floor, so that runner jitter on fast
        # workloads cannot trip the gate.
        if (($slow_pct -gt $Threshold) -and ($slow_abs -gt $MinDeltaSeconds)) {
            $slowPctStr = "{0:N1}" -f ($slow_pct * 100)
            Write-GhaAnnotation -Level error -Message "$test : ${cur_time} vs baseline ${base_time} (${slowPctStr}% slower, threshold ${thresholdPct}%)"
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
    $exitCode = Check-Regression -Current $Current -Baseline $Baseline -Threshold $Threshold `
                            -MinBaselineSeconds $MinBaselineSeconds -MinDeltaSeconds $MinDeltaSeconds
    exit $exitCode
}
