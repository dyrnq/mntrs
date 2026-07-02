# tests/stress/ci-smoke.ps1
#
# Conservative-size variant of run-all.ps1 for the nightly CI workflow.
# Picks env-var overrides that keep each scenario under ~5 min total
# while still exercising the failure modes:
#   - large dir: 1k files (vs 10k default)
#   - large file: 256 MiB (vs 1 GiB default; still > 200 MiB multipart threshold)
#   - cache eviction: 128 MiB mem-limit
#   - writeback concurrent: 4 writers × 4 files (vs 8 × 8)
#   - crash recovery: unchanged (already fast)
#   - soak mixed: 60s (vs 5 min default)
#
# Total: ~4-5 min on windows-latest.

[CmdletBinding()]
param()

$env:STRESS_FILES = "1000"
$env:STRESS_BYTES = "64"
$env:STRESS_FILE_MB = "256"
$env:STRESS_MEM_MB = "128"
$env:STRESS_PARALLEL = "4"
$env:STRESS_FILES_PP = "4"
$env:STRESS_SOAK_SECS = "60"
$env:STRESS_INTERVAL = "1"

# Forward any positional args (subset of scenarios) through to run-all.
# Use @() instead of @args so an empty arg list doesn't splat $null
# (which PowerShell rejects with "A positional parameter cannot be
# found that accepts argument '$null'"). If $args has items, splat
# them; otherwise call without args (= all scenarios).
if ($args.Count -gt 0) {
    & (Join-Path $PSScriptRoot "run-all.ps1") @args
} else {
    & (Join-Path $PSScriptRoot "run-all.ps1")
}
exit $LASTEXITCODE