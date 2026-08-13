# Issue #562 Stage 5 — ThresholdCalibrator design placeholder

This file is a placeholder for issue #566 (Stage 5 Calibrator). The actual
implementation will live in `src/batched_delete.rs` and will supersede this
document. See the linked GitHub issue for the full design.

## TL;DR

- Memory-only task spawned from `batched_delete::spawn()`.
- Reads `CounterSnapshot` + `BurstObserver` every 60s.
- Outputs `tracing::info!` recommendations (never auto-applies).
- Cold-start silent (no recommendations for first 1h / 100 flushes).
- Hysteresis: at most 1 recommendation per 10-min window.

See: <https://github.com/dyrnq/mntrs/issues/566>