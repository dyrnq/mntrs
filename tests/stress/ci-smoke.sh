#!/usr/bin/env bash
#
# tests/stress/ci-smoke.sh
#
# Conservative-size variant of run-all.sh for the nightly CI workflow.
# Picks env-var overrides that keep each scenario under ~5 min total
# while still exercising the failure modes:
#   - large dir: 1k files (vs 10k default)
#   - large file: 256 MiB (vs 1 GiB default; still > 200 MiB multipart threshold)
#   - cache eviction: 128 MiB mem-limit
#   - writeback concurrent: 4 writers × 4 files (vs 8 × 8)
#   - crash recovery: unchanged (already fast)
#   - soak mixed: 60s (vs 5 min default)
#
# Total: ~4-5 min on a 4-core VM.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export STRESS_FILES=1000
export STRESS_BYTES=64
export STRESS_FILE_MB=256
export STRESS_MEM_MB=128
export STRESS_PARALLEL=4
export STRESS_FILES_PP=4
export STRESS_SOAK_SECS=60
export STRESS_INTERVAL=1

exec "$SCRIPT_DIR/run-all.sh" "$@"