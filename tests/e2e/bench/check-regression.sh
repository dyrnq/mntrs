#!/usr/bin/env bash
#
# tests/e2e/bench/check-regression.sh
#
# Detect bench regressions vs a baseline. Used by bench.yml's
# "Check regressions" step, which used to just `cat bench-result.txt`
# — i.e. the step always succeeded regardless of how bad the run was.
#
# Usage:
#   . tests/e2e/bench/check-regression.sh
#   check_regression <current.txt> <baseline.txt> [threshold]
#
#   current:    output of `bench/run_all.sh` (the markdown table from
#               bench/render_table.py, captured by bench.yml's
#               `tee bench-result.txt`)
#   baseline:   a previously-good copy of the same format
#   threshold:  fractional regression to fail on, e.g. 0.20 = 20%.
#               Default 0.20. Both per-test time and overall winner
#               count are checked against this threshold.
#
# What it checks:
#   1. Overall winner counts: if `mntrs=43` drops to `mntrs=38` (>20%
#      fewer wins), fail.
#   2. Critical test times: a hardcoded set of high-signal tests
#      ("100M.bin", "random 50x 1M.bin", etc.). If the current mntrs
#      time is >threshold% slower than baseline, fail.
#   3. The run is allowed to have FEWER tests than baseline (e.g. a
#      new test added to bench.sh) but not MORE — if the current
#      run has tests the baseline doesn't, those are skipped (warned,
#      not failed).
#
# Returns:
#   0 if no regression detected
#   N (1, 2, 3...) where N is the number of failures (caller can
#     use this as a step exit code, or as input to a GHA ::error::)

# Guard against double-include.
if [[ -n "${__CHECK_REGRESSION_LOADED:-}" ]]; then
    return 0 2>/dev/null || true
fi
__CHECK_REGRESSION_LOADED=1

# parse_time: convert "0m0.073s" or "1m23.5s" to seconds (float).
parse_time() {
    local t="$1"
    # Strip 'm' and trailing 's', split.
    local m_part s_part
    m_part="${t%m*}"
    s_part="${t#*m}"
    s_part="${s_part%s}"
    # awk for float math (handles decimals like "0.073").
    awk -v m="$m_part" -v s="$s_part" 'BEGIN { printf "%.4f", m * 60 + s }'
}

# parse_winners: extract "Result: mntrs=43  rclone=32  tie=5  (N tests)"
# line from a bench result file. Sets globals M, R, T, N.
parse_winners() {
    local file="$1"
    local line
    line=$(grep -E '^[[:space:]]*Result:[[:space:]]*mntrs=' "$file" | head -1 || true)
    if [ -z "$line" ]; then
        echo "::warning::check_regression: no 'Result:' line in $file"
        M=0; R=0; T=0; N=0
        return 0
    fi
    # Parse with sed + awk — bash regex doesn't handle this well.
    M=$(echo "$line" | sed -nE 's/.*mntrs=([0-9]+).*/\1/p')
    R=$(echo "$line" | sed -nE 's/.*rclone=([0-9]+).*/\1/p')
    T=$(echo "$line" | sed -nE 's/.*tie=([0-9]+).*/\1/p')
    N=$(echo "$line" | sed -nE 's/.*\(([0-9]+)[[:space:]]*tests\).*/\1/p')
}

# get_mntrs_time: extract mntrs column time for a test name. Returns
# empty string if test not found.
get_mntrs_time() {
    local file="$1"
    local test="$2"
    # Match lines with `| <test> | <time> | <time> | <winner>`. The
    # test column is bounded by '|' and padded with spaces. We use
    # awk for column extraction (more reliable than sed for this).
    awk -F'|' -v t="$test" '
        $0 ~ "[[:space:]]\\|[[:space:]]*" t "[[:space:]]*\\|" {
            gsub(/^ +| +$/, "", $3)
            print $3
            exit
        }
    ' "$file"
}

check_regression() {
    local current="$1"
    local baseline="${2:-bench/baseline.txt}"
    local threshold="${3:-0.20}"

    if [ ! -f "$current" ]; then
        echo "::error::check_regression: current result file not found: $current"
        return 1
    fi
    if [ ! -f "$baseline" ]; then
        echo "::warning::check_regression: baseline file not found: $baseline"
        echo "  Skipping regression check (no baseline to compare against)."
        echo "  Run a known-good bench, copy it to $baseline, and commit it."
        return 0
    fi

    local failures=0
    local warns=0

    echo "==> Regression check: $current vs $baseline (threshold: $(awk "BEGIN{printf \"%.0f%%\", $threshold * 100}"))"

    # --- Check 1: overall winner counts ---
    parse_winners "$current"
    local cur_m=$M cur_r=$R cur_t=$T cur_n=$N
    parse_winners "$baseline"
    local base_m=$M base_r=$R base_t=$T base_n=$N

    if [ "$base_m" -gt 0 ] 2>/dev/null; then
        local m_drop
        m_drop=$(awk -v cur="$cur_m" -v base="$base_m" \
            'BEGIN {
                if (base == 0) { print "0"; exit }
                printf "%.4f", (base - cur) / base
            }')
        # Compare with awk (bash can't do float comparisons portably).
        local m_regressed
        m_regressed=$(awk -v d="$m_drop" -v t="$threshold" \
            'BEGIN { print (d > t) ? 1 : 0 }')
        if [ "$m_regressed" = "1" ]; then
            echo "::error::winner count regression: mntrs wins $cur_m vs baseline $base_m ($(awk "BEGIN{printf \"%.1f\", $m_drop * 100}")% drop, threshold $(awk "BEGIN{printf \"%.0f\", $threshold * 100}")%)"
            failures=$((failures + 1))
        else
            echo "  winner count OK: mntrs=$cur_m (baseline=$base_m)"
        fi
    fi

    # --- Check 2: per-test time on critical tests ---
    # The set of "critical" tests: high-signal workloads where a
    # regression is most likely to be a real bug (large payloads
    # stress the read+write path; the random/concurrent ones stress
    # FUSE reentrancy and locking). If you add a new critical test,
    # add it here.
    local critical_tests=(
        "cat 100M.bin"
        "random 50x 1M.bin"
        "concurrent 4x 10M.bin"
    )

    for test in "${critical_tests[@]}"; do
        local cur_time base_time
        cur_time=$(get_mntrs_time "$current" "$test")
        base_time=$(get_mntrs_time "$baseline" "$test")

        if [ -z "$cur_time" ] || [ -z "$base_time" ]; then
            echo "  ::warning:::: '$test' not in both files (cur='$cur_time' base='$base_time'), skipping"
            warns=$((warns + 1))
            continue
        fi

        local cur_sec base_sec slow_pct regressed
        cur_sec=$(parse_time "$cur_time")
        base_sec=$(parse_time "$base_time")
        # 0 baseline = divide-by-zero risk; skip.
        if [ "$(awk -v b="$base_sec" 'BEGIN { print (b == 0) ? 1 : 0 }')" = "1" ]; then
            continue
        fi
        slow_pct=$(awk -v c="$cur_sec" -v b="$base_sec" \
            'BEGIN { printf "%.4f", (c - b) / b }')
        regressed=$(awk -v s="$slow_pct" -v t="$threshold" \
            'BEGIN { print (s > t) ? 1 : 0 }')

        if [ "$regressed" = "1" ]; then
            echo "::error::$test: ${cur_time} vs baseline ${base_time} ($(awk "BEGIN{printf \"%.1f\", $slow_pct * 100}")% slower, threshold $(awk "BEGIN{printf \"%.0f\", $threshold * 100}")%)"
            failures=$((failures + 1))
        else
            echo "  $test OK: ${cur_time} (baseline ${base_time})"
        fi
    done

    echo ""
    echo "==> Regression check complete: $failures failure(s), $warns warning(s)"
    return "$failures"
}

# Direct invocation dispatch (when not sourced).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
    check_regression "$@"
fi
