# tests/stress/run-all.ps1
#
# Entry point for the #388 Windows stress test suite.
#
# Runs each scenario in order; failure of any one fails the whole suite
# (skip = exit 0 + SKIP log, counted as pass). Each scenario is gated
# by env vars (STRESS_*_SECS / STRESS_FILES / etc.) so the same scripts
# can be scaled for nightly CI (small) vs operator soak runs (large)
# without code changes.
#
# Usage:
#   pwsh tests/stress/run-all.ps1           # all 6 in order
#   pwsh tests/stress/run-all.ps1 01-large-dir
#   pwsh tests/stress/run-all.ps1 02-large-file-io 06-soak-mixed
#
# Override per-scenario with env:
#   $env:STRESS_FILES = 1000; pwsh tests/stress/run-all.ps1 01-large-dir
#   $env:STRESS_SOAK_SECS = 60; pwsh tests/stress/run-all.ps1 06-soak-mixed
#   $env:STRESS_FILE_MB = 512; pwsh tests/stress/run-all.ps1 02-large-file-io
#
# For nightly CI: tests/stress/ci-smoke.ps1 picks conservative sizes.

[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$scriptDir = $PSScriptRoot
$suiteStart = Get-Date

$ALL = @(
    "01-large-dir",
    "02-large-file-io",
    "03-cache-eviction",
    "04-writeback-concurrent",
    "05-crash-recovery",
    "06-soak-mixed"
)

if ($args.Count -gt 0) {
    $selected = @($args)
} else {
    $selected = $ALL
}

Write-Host "stress suite: $($selected -join ' ')"
Write-Host ""

$pass = 0
$fail = 0
$skipped = 0
$failedScenarios = @()
$skippedScenarios = @()

foreach ($s in $selected) {
    $scriptPath = Join-Path $scriptDir "$s.ps1"
    Write-Host "━━━ running $s ━━━"
    if (-not (Test-Path -LiteralPath $scriptPath)) {
        Write-Host "  SKIP: $s (script not found at $scriptPath)"
        $skipped++
        $skippedScenarios += $s
        Write-Host ""
        continue
    }

    # Run scenario in a child pwsh so a scenario's terminating error
    # doesn't poison the suite. Classify by exit code (autotools
    # convention, mirrors bash common.sh):
    #   0   = PASS
    #   77  = SKIP (Write-Skip helper exits 77 in common.ps1)
    #   *   = FAIL
    # Only redirect stdout (NOT stderr) — Start-Process rejects two
    # redirects pointing at the same file. Scenario stderr inherits
    # the parent's stderr handle, so it still flows into the
    # workflow's `*>&1 | Tee-Object` capture upstream. The log file
    # is for postmortem only; classification reads $proc.ExitCode.
    $outFile = Join-Path $env:TEMP "stress-$s-$PID.log"
    $proc = Start-Process -FilePath "pwsh" `
        -ArgumentList @("-NoProfile", "-File", $scriptPath) `
        -RedirectStandardOutput $outFile `
        -PassThru -NoNewWindow -Wait

    if (Test-Path -LiteralPath $outFile) {
        Get-Content -LiteralPath $outFile -Raw | Write-Host
    }

    $exitCode = $proc.ExitCode
    if ($exitCode -eq 0) {
        Write-Host "  → PASS ($s)"
        $pass++
    } elseif ($exitCode -eq 77) {
        Write-Host "  → SKIP ($s)"
        $skipped++
        $skippedScenarios += $s
    } else {
        Write-Host "  → FAIL ($s, exit=$exitCode)"
        $fail++
        $failedScenarios += $s
    }
    Write-Host ""
}

$elapsed = [int]((Get-Date) - $suiteStart).TotalSeconds
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
Write-Host "stress suite: $pass passed, $fail failed, $skipped skipped"
if ($fail -gt 0) {
    Write-Host "failed: $($failedScenarios -join ' ')"
}
if ($skipped -gt 0) {
    Write-Host "skipped: $($skippedScenarios -join ' ')"
}
if ($fail -gt 0) {
    exit 1
}
exit 0