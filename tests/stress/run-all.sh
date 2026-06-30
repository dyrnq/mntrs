#!/usr/bin/env bash
#
# tests/stress/run-all.sh
#
# Entry point for the #143 stress test suite.
#
# Runs each scenario in order; failure of any one fails the whole suite.
# Each scenario is gated by env vars (STRESS_*_SECS / STRESS_FILES / etc.)
# so the same scripts can be scaled for nightly CI (small) vs
# operator soak runs (large) without code changes.
#
# Usage:
#   tests/stress/run-all.sh [scenario ...]
#   # no args → run all 6 in order
#
# Override per-scenario with env:
#   STRESS_FILES=1000       tests/stress/run-all.sh 01-large-dir
#   STRESS_SOAK_SECS=60     tests/stress/run-all.sh 06-soak-mixed
#   STRESS_FILE_MB=512      tests/stress/run-all.sh 02-large-file-io
#
# For nightly CI: tests/stress/ci-smoke.sh picks conservative sizes.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SUITE_START=$(date +%s)

ALL=(01-large-dir 02-large-file-io 03-cache-eviction 04-writeback-concurrent 05-crash-recovery 06-soak-mixed 07-writeback-cache-optin)

if [[ $# -gt 0 ]]; then
    SELECTED=("$@")
else
    SELECTED=("${ALL[@]}")
fi

echo "stress suite: ${SELECTED[*]}"
echo

PASS=0
FAIL=0
declare -a FAILED_SCENARIOS=()

for s in "${SELECTED[@]}"; do
    if [[ ! -x "$SCRIPT_DIR/$s.sh" ]]; then
        echo "  SKIP: $s (script not found or not executable)"
        continue
    fi
    echo "━━━ running $s ━━━"
    if "$SCRIPT_DIR/$s.sh"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        FAILED_SCENARIOS+=("$s")
    fi
    echo
done

ELAPSED=$(( $(date +%s) - SUITE_START ))
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "stress suite: $PASS passed, $FAIL failed in ${ELAPSED}s"
if (( FAIL > 0 )); then
    echo "failed: ${FAILED_SCENARIOS[*]}"
    exit 1
fi
exit 0