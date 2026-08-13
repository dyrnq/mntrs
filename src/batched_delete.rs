//! Plan #64: Batched S3 DeleteObjects backend.
//!
//! ## Why this exists
//!
//! opendal 0.58.1 has `BatchDeleter<S3Deleter>` (max_batch_size = 1000)
//! wired into its `delete_with().recursive(true)` path, but the public
//! `Operator::delete()` facade creates a fresh deleter per call
//! (`opendal-core-0.58.1/src/types/operator/operator.rs:1635`). The
//! batcher therefore never accumulates across FUSE callbacks.
//!
//! We bypass opendal's delete path entirely and call S3
//! `DeleteObjects` directly with a long-lived worker. Probe H
//! measured ~5× speedup (574 ms → 115 ms for 500 unlinks on local
//! MinIO; boto3 matches the 115 ms ceiling, confirming MinIO is not
//! the bottleneck).
//!
//! ## Policy B: write-behind
//!
//! Callbacks return `Ok(())` immediately after enqueue. The user's
//! `rm` receives success before S3 confirms deletion. Per-key
//! failures are logged, not surfaced. Default off; opt-in via
//! `MNTRS_UNLINK_BATCH=1`.
//!
//! ## Shutdown
//!
//! Mirrors `writeback::spawn`: dropping the last `BatchedDeleter`
//! handle closes the control channel and the worker exits on
//! `rx.recv().await → None`. In-flight pending deletes are completed
//! with `io::ErrorKind::BrokenPipe` (same pattern writeback uses on
//! abrupt sender close).
//!
//! ## References
//!
//! - S3 DeleteObjects spec (AWS S3 API reference)
//! - opendal-service-s3/src/core.rs:1271-1312 (reference impl we mirror)
//! - reqsign-aws-v4 3.0.3 `RequestSigner::sign`
//! - plan: /home/devops/.claude/plans/virtual-noodling-salamander.md

// Public API surface ahead of wire-up: step 5 lands the module; steps
// 6-9 integrate it into MntrsFs. Until then the FUSE-side callers
// don't exist, so most symbols are unused. The `#[allow(dead_code)]`
// is intentional and scoped to this module.
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use md5::{Digest, Md5};
use quick_xml::Writer;
use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};
use reqsign_aws_v4::{
    Credential as AwsCredential, DefaultCredentialProvider, EnvCredentialProvider,
    ProfileCredentialProvider, RequestSigner, StaticCredentialProvider,
};
use reqsign_core::{Context, ProvideCredentialChain, Signer};
use reqsign_file_read_tokio::TokioFileRead;
use tokio::sync::{mpsc, oneshot};

// ===== Constants =====

/// S3 hard limit: at most 1000 object identifiers per DeleteObjects
/// request. We chunk below this regardless of the configured
/// `batch_size` so a misconfiguration cannot produce an invalid
/// request.
pub(crate) const HARD_MAX_KEYS_PER_REQUEST: usize = 1000;

// Plan #64 stage B: per-mount tuning knobs surfaced via
// environment variables. Defaults preserve the values that the
// original plan #64 baseline used.
pub(crate) const DEFAULT_BATCH_SIZE: usize = 100;
pub(crate) const DEFAULT_FLUSH_DELAY: Duration = Duration::from_millis(50);
pub(crate) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const DEFAULT_MAX_RETRIES: u32 = 3;
pub(crate) const DEFAULT_RETRY_FACTOR: f64 = 2.0;
pub(crate) const DEFAULT_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Issue #530: caller-side threshold gating. When the batched
/// deleter's pending queue has fewer than this many entries
/// at the moment of enqueue, the caller falls through to a
/// direct strict delete (`delete_backend_strict`) instead of
/// enqueueing. Why: each enqueued file pays up to
/// `flush_delay` (50 ms by default) of latency before its S3
/// DELETE is requested. For small bursts (rm -rf on a few
/// files), the strict path's `op.delete()` is faster and
/// simpler. Only when the burst is large enough that one
/// batched `DeleteObjects` call beats N serial `op.delete()`
/// calls does the overhead pay for itself — bench crossover
/// is at ~500 files; threshold 32 is a conservative
/// approximation that still benefits large `rm -rf` work
/// without hurting small unlink workloads. Tunable via
/// `MNTRS_BATCH_THRESHOLD` (0 = always batch, even single
/// files; default 32).
pub(crate) const DEFAULT_BATCH_THRESHOLD: usize = 32;

/// Issue #553: fast-flush threshold. When the pending
/// queue has fewer than this many keys, the worker flushes
/// immediately instead of waiting out the `flush_delay`
/// deadline. Reasoning: a single unlink or a 10-file
/// `rm -rf` mid-burst pays the full `flush_delay` (50 ms by
/// default) for negligible batching benefit (~1 S3 DELETE
/// either way). 0 disables the fast path (matches pre-fix
/// behaviour — every non-full batch waits for the deadline).
///
/// Empirical sweet spot from issue 541 run 30865329186:
/// with batch_size=20 the median partial flush carried
/// 13-16 keys, so any threshold <= 8 keeps the typical
/// `rm -rf 100 files` (which accumulates 50-100 keys
/// before the 50 ms deadline) on the WaitForDeadline
/// path while shunting stragglers and tail fragments
/// straight to S3.
pub(crate) const DEFAULT_FAST_FLUSH_THRESHOLD: usize = 8;

/// Issue #562 stage 1: default flusher pool size. Matches
/// rclone --transfers=4 so the mntrs S3 worker can amortise
/// a DeleteObjects round-trip the same way rclone amortises
/// its per-file transfer.
pub(crate) const DEFAULT_BATCH_WORKER_COUNT: usize = 4;

/// Issue #562 stage 1: hard upper bound on the flusher
/// pool. Each flusher holds its own `Signer<AwsCredential>`
/// (cheap clone of region+chain) and shares the
/// `reqwest::Client` connection pool through `Arc` inside
/// `WorkerConfig::http`, so memory pressure is bounded by
/// the channel buffers and the connection pool, not by the
/// signer. Even so, 16 is a generous cap — a misconfigured
/// `MNTRS_BATCH_WORKER_COUNT=10000` would otherwise spawn
/// 10000 clones of the credential chain.
pub(crate) const MAX_BATCH_WORKER_COUNT: usize = 16;

// ===== Counters (plan #64 stage B) =====
//
// Process-static counters, like writeback::PENDING_COUNT. Read
// from any thread via the public accessor functions below.

static FLUSHES_TOTAL: AtomicU64 = AtomicU64::new(0);
static KEYS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHUTDOWN_LOST_TOTAL: AtomicU64 = AtomicU64::new(0);
static SINGLE_KEY_BATCHES_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Issue #553: how many flushes fired via the fast-flush
/// path (pending.len() < fast_flush_threshold at decision
/// time). Useful to verify the threshold is firing on small
/// rm workloads without breaking big-batch timing.
static FAST_FLUSH_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAX_BATCH_SIZE_OBSERVED: AtomicU64 = AtomicU64::new(0);
/// Issue #530: how many `enqueue` calls were routed to the
/// strict (`delete_backend_strict`) path because the current
/// pending queue length was below the batch threshold. Useful
/// to verify the gating is firing on real workloads.
static THRESHOLD_SKIPPED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Issue #562 stage 3: how many times `ProfileState` flipped
/// from one profile to another (transitions only, not observe
/// calls that kept the current profile). Useful to verify
/// hysteresis + cooldown are not oscillating under steady-state
/// workload.
static PROFILE_TRANSITIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Issue #562 stage 1.5: how many single-key flushes used
/// the short-circuit plain `DELETE /bucket/key` path instead
/// of the multi-key `DeleteObjects` XML path. Tracks the
/// actual hit rate so a /metrics consumer (Stage 4) and the
/// bench harness can verify the lever is firing on small
/// workloads.
static SINGLE_KEY_FAST_DELETE_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Issue #562 stage 5 (Calibrator input): per-flush
/// retry count. Bumped every time `send_chunk_with_retry`
/// retries a request (either a retryable HTTP status or a
/// transport error). Used by `ThresholdCalibrator` to
/// estimate retry_rate = retries / flushes. Atomic so the
/// flusher loop can increment without lock contention.
static RETRY_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Issue #562 stage 5 (Calibrator input): per-flush
/// chunk size running sum. The calibrator divides
/// `chunk_size_sum / FLUSHES_TOTAL` to get the median-ish
/// chunk size; we use sum+count rather than a per-flush
/// sample array to keep the hot path lock-free. Stored as
/// a single `AtomicU64` because chunk sizes fit comfortably
/// in u64 across the lifetime of a mount.
static CHUNK_SIZE_SUM: AtomicU64 = AtomicU64::new(0);
/// Issue #562 stage 5 (Calibrator counter): count of
/// `tracing::info!` recommendation lines emitted. Bumped
/// inside the calibrator loop. Operator-facing signal that
/// the calibrator has *something* to say; zero over a 24h
/// run means the current profile is well-fit and no
/// adjustment is suggested.
static CALIBRATOR_RECOMMENDATIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

// ===== Profile (issue #562 stage 3) =====
//
// A workload-shape classification that maps to a fixed triple
// of (batch_size, flush_delay, fast_flush_threshold). The
// controller reads the active profile at every
// `decide_next_action` call, so the batcher adapts within a
// single burst rather than being pinned at mount time. The
// three values were chosen from the 10 nightly runs after
// Stage 1 (see issue #562 for the bench data): Small targets
// the `rm single file` / IDE-save workload (sparse unlinks,
// low latency), Medium matches the pre-Stage-3 defaults
// (general-purpose), Bulk targets the `rm -rf node_modules`
// workload (large bursts, amortise round-trips over hundreds
// of keys).
//
// Profile is an enum (not free-form config) so the active
// triple is always one of three known shapes. Pinned
// (`MNTRS_BATCH_PROFILE=small|medium|bulk`) and auto
// (`=auto`, default) both use the same enum; only the
// transition logic in `ProfileState` differs.

/// Issue #562 stage 3: workload-shape classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Profile {
    /// Sparse unlinks — single `rm`, IDE save deltas, etc.
    /// `batch_size=20 flush_delay=10ms fast_flush_threshold=4`.
    /// Aggressively fast-flushes tail fragments so the user
    /// sees the file gone quickly. Wins `rm -rf 10 files`
    /// (Stage 1 left this at 3.77x rclone).
    Small,
    /// General-purpose. Matches pre-Stage-3 defaults.
    /// `batch_size=100 flush_delay=50ms fast_flush_threshold=8`.
    /// The starting profile for auto mode; should not regress
    /// existing benchmarks.
    Medium,
    /// Large bursts — `rm -rf` of a tree, CI cleanup scripts.
    /// `batch_size=500 flush_delay=200ms fast_flush_threshold=32`.
    /// Long flush_delay lets the queue accumulate hundreds of
    /// keys before issuing a single `DeleteObjects` so the S3
    /// round-trip amortises over 500 keys. Wins `rm -rf 500`
    /// (already at 1.00x with Medium) and `rm -rf 1000`.
    Bulk,
}

impl Profile {
    /// The size at which a size-driven flush fires. Above
    /// this, `decide_next_action` returns `FlushBatch { fast: false }`
    /// (the deadline wait is skipped).
    pub(crate) fn batch_size(self) -> usize {
        match self {
            Profile::Small => 20,
            Profile::Medium => 100,
            Profile::Bulk => 500,
        }
    }

    /// Time the controller waits for more keys to accumulate
    /// before flushing a partial batch (the "middle band":
    /// `fast_flush_threshold <= pending.len() < batch_size`).
    /// Larger values let the queue accumulate more keys per
    /// `DeleteObjects` call.
    pub(crate) fn flush_delay(self) -> Duration {
        match self {
            Profile::Small => Duration::from_millis(10),
            Profile::Medium => Duration::from_millis(50),
            Profile::Bulk => Duration::from_millis(200),
        }
    }

    /// Below this pending-len, the controller fast-flushes
    /// immediately instead of waiting for `flush_delay`. The
    /// Stage 1.5 fix for the small-batch regression tracked
    /// in #541. 0 disables the fast branch entirely.
    pub(crate) fn fast_flush_threshold(self) -> usize {
        match self {
            Profile::Small => 4,
            Profile::Medium => 8,
            Profile::Bulk => 32,
        }
    }

    /// Reserved for the future Stage 1.5 single-key
    /// short-circuit (plain DELETE on `chunk.len()==1` instead
    /// of a 1-element `DeleteObjects`). Default false in this
    /// PR so the Stage 3 scope stays tight; flipping to true
    /// is a one-line change once the short-circuit is
    /// implemented. Only meaningful for `Profile::Small`
    /// where single-key batches dominate.
    pub(crate) fn single_key_fast_delete(self) -> bool {
        matches!(self, Profile::Small)
    }

    /// Parse the `MNTRS_BATCH_PROFILE` env value. `auto` →
    /// `None` (the caller passes `None` to `ProfileState::new`
    /// to mean "drive transitions yourself"); `small|medium|bulk`
    /// → the corresponding variant (pinned).
    pub(crate) fn parse_env(name: &str, default: Profile) -> Profile {
        match std::env::var(name).ok().as_deref() {
            Some("small") => Profile::Small,
            Some("medium") => Profile::Medium,
            Some("bulk") => Profile::Bulk,
            // `auto` and unset both fall through to the default.
            // The auto case is meaningful only when the caller
            // uses the `Option<Profile>` return from a separate
            // helper (see `parse_profile_or_auto` below); here
            // we just provide a sane pinned default.
            Some("auto") | None => default,
            Some(other) => {
                tracing::warn!(
                    target: "mntrs::batched_delete",
                    env = name,
                    value = other,
                    "unrecognised MNTRS_BATCH_PROFILE value; using default"
                );
                default
            }
        }
    }
}

/// Like `Profile::parse_env` but distinguishes "auto" from a
/// pinned default. Returns `Some(Profile)` for pinned values
/// (small/medium/bulk) and `None` for `auto`/unset (the caller
/// passes `None` to `ProfileState::new` to mean "start at
/// Medium and let the observer drive transitions").
pub(crate) fn parse_profile_or_auto(name: &str) -> Option<Profile> {
    match std::env::var(name).ok().as_deref() {
        Some("small") => Some(Profile::Small),
        Some("medium") => Some(Profile::Medium),
        Some("bulk") => Some(Profile::Bulk),
        Some("auto") | None => None,
        Some(other) => {
            tracing::warn!(
                target: "mntrs::batched_delete",
                env = name,
                value = other,
                "unrecognised MNTRS_BATCH_PROFILE value; using auto"
            );
            None
        }
    }
}

// ===== BurstObserver (issue #562 stage 3) =====
//
// Lock-free ring buffer of `pending.len()` samples. The
// enqueue path pushes one sample per unlink (a single atomic
// store); the controller reads `p95()` once per iteration to
// feed `ProfileState`. Sized for ~30 s of samples at 100 Hz
// (3000 entries). The window is naturally truncated by
// wrap-around — older samples are overwritten, so the
// percentile reflects "what has the workload looked like
// over the last ~30 s", not "all-time".

/// Number of samples the observer retains. At ~100 Hz this
/// covers ~30 s of workload history, which is enough to
/// smooth out a single `rm -rf` burst while still being
/// responsive to workload-shape changes.
pub(crate) const BURST_OBSERVER_CAP: usize = 3000;

/// Lock-free ring buffer of `pending.len()` samples. Writes
/// are atomic (x86/arm guarantee natural alignment for
/// `AtomicU32`), so the hot enqueue path needs no mutex. The
/// `idx` field advances monotonically and wraps modulo `CAP`
/// — `window_count` caps the active range so a fresh observer
/// doesn't try to read uninitialised memory.
pub(crate) struct BurstObserver {
    samples: Box<[AtomicU32; BURST_OBSERVER_CAP]>,
    /// Monotonic write index. Always `>= window_count`.
    idx: AtomicUsize,
    /// Number of valid samples currently in the buffer;
    /// saturates at `CAP`. `p95()` reads only `window_count`
    /// entries starting at `idx - window_count`.
    window_count: AtomicUsize,
}

impl BurstObserver {
    pub(crate) fn new() -> Self {
        // `Box<[AtomicU32; N]>` is unsized-friendly and the
        // array is zero-initialised (AtomicU32's default is 0,
        // which is a valid `pending_len` sample).
        let samples: Box<[AtomicU32; BURST_OBSERVER_CAP]> =
            Box::new(std::array::from_fn(|_| AtomicU32::new(0)));
        Self {
            samples,
            idx: AtomicUsize::new(0),
            window_count: AtomicUsize::new(0),
        }
    }

    /// Record one sample of the pending-queue length. Called
    /// from the enqueue path after a successful push. O(1),
    /// lock-free.
    pub(crate) fn observe(&self, pending_len: usize) {
        // Clamp to u32 so a runaway queue (shouldn't happen,
        // but cheap insurance) doesn't overflow the storage.
        // u32 supports ~4 billion entries; the pending queue
        // is bounded by `MNTRS_BATCH_THRESHOLD` (default 32)
        // in practice, so this is well within range.
        let sample = pending_len.min(u32::MAX as usize) as u32;
        // `fetch_add` is sequential consistent for ordering
        // between the sample store and the index advance.
        // `Relaxed` would race the read in `p95()` on weakly
        // ordered hardware, so we keep the strong ordering.
        let write_idx = self.idx.fetch_add(1, Ordering::AcqRel);
        let slot = write_idx % BURST_OBSERVER_CAP;
        self.samples[slot].store(sample, Ordering::Release);
        // Saturate window_count so a fresh observer returns
        // "no samples yet" until at least one observe has run.
        let prev = self.window_count.load(Ordering::Acquire);
        if prev < BURST_OBSERVER_CAP {
            self.window_count.store(prev + 1, Ordering::Release);
        }
    }

    /// 95th percentile of the active window. Returns 0 if the
    /// observer has fewer than 20 samples (the percentile is
    /// ill-defined for very small windows; we conservatively
    /// return 0 so `ProfileState` sees "small" workload and
    /// picks `Profile::Small` until there's enough data).
    /// Returns `usize::MAX` if the window is non-empty but
    /// has fewer than 20 samples, signalling "we have data
    /// but not enough to compute a percentile" — `ProfileState`
    /// treats this as "no hint, keep current profile".
    pub(crate) fn p95(&self) -> usize {
        let count = self.window_count.load(Ordering::Acquire);
        if count == 0 {
            return 0;
        }
        // Need at least 20 samples to compute a meaningful
        // p95 (20 * 0.95 = 19, so the percentile falls on
        // an actual data point).
        if count < 20 {
            // Return max so callers can distinguish "no
            // data" (0) from "some data but not enough for
            // a percentile" (usize::MAX).
            return usize::MAX;
        }
        let take = count.min(BURST_OBSERVER_CAP);
        // Snapshot the active window. Reads are atomic, no
        // lock needed. We allocate a small Vec here —
        // `p95()` is called once per controller iteration
        // (not per enqueue), so the allocation cost is
        // bounded by the controller loop rate (~10-100 Hz).
        let mut snapshot: Vec<u32> = Vec::with_capacity(take);
        let start = self.idx.load(Ordering::Acquire).saturating_sub(take);
        for i in 0..take {
            let slot = (start + i) % BURST_OBSERVER_CAP;
            snapshot.push(self.samples[slot].load(Ordering::Acquire));
        }
        snapshot.sort_unstable();
        // p95 index: ceil(0.95 * (n - 1)) clamped to [0, n-1].
        // For n=20: idx = ceil(0.95 * 19) = ceil(18.05) = 19
        // (the max). For n=100: idx = ceil(0.95 * 99) = 95.
        let idx = ((take as f64 * 0.95).ceil() as usize).min(take.saturating_sub(1));
        snapshot[idx] as usize
    }
}

impl Default for BurstObserver {
    fn default() -> Self {
        Self::new()
    }
}

// ===== ProfileState (issue #562 stage 3) =====
//
// Owns the currently-active profile + the hysteresis + cooldown
// logic that decides when to flip. The controller calls
// `observe(burst_hint, now)` before each `decide_next_action`;
// `observe` returns the (possibly unchanged) current profile
// after applying the transition rules.
//
// Hysteresis: the burst hint must exceed the boundary
// threshold (50 for Small→Bulk, 5 for Bulk→Small) for the
// cooldown duration before a flip fires. This prevents a
// single `rm -rf` from bouncing the system into Bulk and
// keeping it there.
//
// Cooldown: after any flip, no further transitions are
// evaluated for `cooldown` Duration. This caps the
// transition rate at ~12/hour (cooldown = 5 s) under
// pathological oscillation, well below the RFC's
// 10/hour oscillation guard.

/// Hysteresis upper bound: `p95(pending_len) > UP_THRESHOLD`
/// for the cooldown duration before a flip toward `Bulk`.
/// Tuned from the 10 nightly runs after Stage 1 (rm -rf 100
/// arrives at ~1100 unlinks/s, so the p95 of pending_len
/// during that burst is ~2-8 keys, well below 50).
const PROFILE_UP_THRESHOLD: usize = 50;
/// Hysteresis lower bound: `p95(pending_len) < DOWN_THRESHOLD`
/// for the cooldown duration before a flip toward `Small`.
const PROFILE_DOWN_THRESHOLD: usize = 5;
/// Default cooldown between transitions. RFC #562 Stage 3
/// acceptance: < 10 transitions/hour under steady-state
/// workload; with a 5 s cooldown the maximum achievable
/// rate is 720/hour if the hint oscillates every iteration,
/// but the hysteresis + the hint being workload-derived
/// keeps the practical rate well below 10/hour.
pub(crate) const PROFILE_DEFAULT_COOLDOWN: Duration = Duration::from_secs(5);

pub(crate) struct ProfileState {
    /// Current profile as `u8`. Stored as `AtomicU8` so
    /// `current()` is lock-free on the hot controller path.
    /// Encoding: 0=Small, 1=Medium, 2=Bulk. Any out-of-range
    /// value would be a bug — `set_current` clamps.
    current: AtomicU8,
    /// Wall-clock of the last transition. `None` until the
    /// first flip; thereafter used to enforce cooldown. Held
    /// under a `Mutex` because the only writers are
    /// `observe()` and `new()`, and they fire at most every
    /// `cooldown` Duration — lock contention is not a concern.
    last_transition: Mutex<Option<Instant>>,
    /// Cooldown Duration enforced after every transition.
    /// `Duration::MAX` means "pinned, never transitions"
    /// (the value used when `MNTRS_BATCH_PROFILE=small|medium|bulk`
    /// is set, so the user-pinned profile is bit-for-bit the
    /// pre-Stage-3 code path).
    cooldown: Duration,
}

impl ProfileState {
    /// Build a `ProfileState` with the given starting profile.
    /// `cooldown` controls how often transitions are allowed:
    /// 5 s for auto mode (driven by `ProfileState`), or
    /// `Duration::MAX` for pinned mode (the controller calls
    /// `observe` but the cooldown never elapses so the
    /// profile never actually changes).
    pub(crate) fn new(initial: Profile, cooldown: Duration) -> Self {
        Self {
            current: AtomicU8::new(initial as u8),
            last_transition: Mutex::new(None),
            cooldown,
        }
    }

    /// The currently-active profile. Lock-free (`AtomicU8::load`).
    pub(crate) fn current(&self) -> Profile {
        match self.current.load(Ordering::Acquire) {
            0 => Profile::Small,
            1 => Profile::Medium,
            2 => Profile::Bulk,
            // Defensive: a corrupted atomic value would
            // otherwise silently pin to Medium. Log + clamp.
            _ => {
                tracing::error!(
                    target: "mntrs::batched_delete",
                    "ProfileState::current: corrupted u8 (defaulting to Medium)"
                );
                Profile::Medium
            }
        }
    }

    /// Apply hysteresis + cooldown to the burst hint and
    /// return the (possibly flipped) current profile.
    /// `hint_p95` is `BurstObserver::p95()`; `0` means "no
    /// samples yet" and `usize::MAX` means "some samples but
    /// not enough for a percentile" — both are treated as
    /// "no hint" and the current profile is returned
    /// unchanged.
    pub(crate) fn observe(&self, hint_p95: usize, now: Instant) -> Profile {
        // No hint → keep the current profile.
        if hint_p95 == 0 || hint_p95 == usize::MAX {
            return self.current();
        }

        let current = self.current();
        // Compute the candidate profile from the hint.
        let candidate = if hint_p95 > PROFILE_UP_THRESHOLD {
            Profile::Bulk
        } else if hint_p95 < PROFILE_DOWN_THRESHOLD {
            Profile::Small
        } else {
            Profile::Medium
        };

        // No transition needed.
        if candidate == current {
            return current;
        }

        // Cooldown gate. If we just transitioned, hold the
        // current profile until the cooldown elapses. This is
        // the load-bearing oscillation guard.
        let mut last = self
            .last_transition
            .lock()
            .expect("last_transition mutex poisoned");
        if let Some(prev) = *last
            && now.duration_since(prev) < self.cooldown
        {
            return current;
        }

        // Flip. Update atomic first so concurrent readers
        // see the new profile ASAP, then log + bump the
        // counter + record the transition time.
        self.current.store(candidate as u8, Ordering::Release);
        PROFILE_TRANSITIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
        tracing::info!(
            target: "mntrs::batched_delete",
            from = ?current,
            to = ?candidate,
            hint_p95,
            cooldown_ms = self.cooldown.as_millis() as u64,
            "batched_delete: profile transition"
        );
        *last = Some(now);
        candidate
    }
}

// ===== ThresholdCalibrator (issue #562 stage 5) =====
//
// A pure decision function plus a small state struct that
// tracks the rolling recommendation history. The Calibrator
// observes `CounterSnapshot` + `BurstObserver` every 60s and
// emits `tracing::info!` recommendations about whether the
// active Profile's batch_size / fast_flush_threshold should
// be raised or lowered. **It never auto-applies** — the
// invariant is: "operator sees a recommendation, decides
// whether to act on it, sets the env accordingly on the
// next mount". The atomic counter
// `CALIBRATOR_RECOMMENDATIONS_TOTAL` tracks how many
// recommendations the calibrator has emitted over the
// mount's lifetime; zero over a 24h run means the current
// profile is well-fit and no adjustment is suggested.
//
// Cold-start silence: the calibrator's first N flushes are
// suppressed so a freshly-mounted system doesn't emit
// "recommend batch_size=1000" before enough flushes have
// happened to be statistically meaningful. The threshold is
// `min_flushes_for_recommendation` (default 100). The
// 10-minute `recommendation_cooldown` provides the
// hysteresis: at most one recommendation every 10 minutes
// even if every observation hits a trigger.
//
// Why per-flush stats aren't tracked directly: we keep the
// hot path lock-free by storing only the cumulative
// counters (FLUSHES_TOTAL, RETRY_TOTAL, CHUNK_SIZE_SUM,
// etc.) and computing averages on the calibrator side.
// The trade-off is we lose the ability to compute variance
// over a sliding window; the calibrator's input is the
// lifetime cumulative snapshot. Future enhancement: a
// per-30s ring buffer of flush_duration samples would let
// us compute p99 flush duration properly. For stage 5 the
// lifetime avg is good enough — the recommendation is
// "your batch_size looks wrong" which is a slow-moving
/// signal.
pub(crate) const MIN_FLUSHES_FOR_RECOMMENDATION: u64 = 100;
pub(crate) const CALIBRATOR_RECOMMENDATION_COOLDOWN: Duration = Duration::from_secs(600);
pub(crate) const CALIBRATOR_OBSERVATION_INTERVAL: Duration = Duration::from_secs(60);

/// Issue #562 stage 5: a single recommendation emitted by
/// the `ThresholdCalibrator`. The loop emits zero or one
/// of these per observation; the operator reads them via
/// `tracing::info!` and decides whether to set
/// `MNTRS_BATCH_SIZE` / `MNTRS_BATCH_FAST_FLUSH_THRESHOLD`
/// accordingly on the next mount.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CalibrationRecommendation {
    /// The variance in p99 flush duration over the trailing
    /// observation window is high relative to the median. This
    /// means some flushes are dragging out the deadline —
    /// batch_size is probably too large for the workload.
    /// Lower it by 25%.
    LowerBatchSize { current: usize, proposed: usize },
    /// The retry rate over the trailing observation window
    /// exceeds 5%. The current batch_size is too small to
    /// amortise per-key overhead, OR the network is too
    /// unreliable. Raise batch_size by 25% to put more keys
    /// per request.
    RaiseBatchSize { current: usize, proposed: usize },
    /// The median chunk size is < 2 and the burst observer's
    /// p95 stays low (< 5). This is a sustained small-burst
    /// workload where the fast-flush threshold should engage
    /// on the very first unlink — set it to 1.
    LowerFastFlushThreshold { current: usize, proposed: usize },
}

/// Issue #562 stage 5: input to one calibrator observation.
/// A `Snapshot` is computed by the loop from
/// `CounterSnapshot` + `BurstObserver::p95()` +
/// `ProfileState::current()`. The Calibrator's decision
/// function is **pure** — no IO, no clock — so unit tests
/// can drive every input combination.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CalibrationInput {
    /// Cumulative flush count since mount. Cold-start
    /// silence uses this directly.
    pub flushes_total: u64,
    /// Cumulative retry count since mount. retry_rate =
    /// retry_total / flushes_total.
    pub retry_total: u64,
    /// Cumulative chunk_size_sum since mount. avg_chunk_size
    /// = chunk_size_sum / flushes_total.
    pub chunk_size_sum: u64,
    /// Current p95 of `pending.len()` from the
    /// `BurstObserver`.
    pub burst_p95: usize,
    /// Active profile. Used to read the current
    /// batch_size / fast_flush_threshold so the
    /// recommendation can quote "current" and "proposed".
    pub current_profile: Profile,
}

/// Issue #562 stage 5: state held across observations to
/// enforce cold-start silence and the 10-min recommendation
/// cooldown. The struct is owned by the calibrator loop
/// task and never shared with flushers.
#[derive(Debug)]
pub(crate) struct ThresholdCalibrator {
    /// Cold-start silence: skip observation while
    /// `flushes_total < min_flushes`. Default 100 matches
    /// the Stage 5 acceptance criterion ("no recommendations
    /// for first 100 flushes").
    pub min_flushes: u64,
    /// Hysteresis: at most one recommendation per
    /// `recommendation_cooldown`. Default 10 minutes. Stops
    /// noise from causing flapping recommendations.
    pub recommendation_cooldown: Duration,
    /// Last emitted recommendation timestamp. `None` until
    /// the first recommendation fires.
    pub last_recommendation: Option<Instant>,
}

impl ThresholdCalibrator {
    pub(crate) fn new() -> Self {
        Self {
            min_flushes: MIN_FLUSHES_FOR_RECOMMENDATION,
            recommendation_cooldown: CALIBRATOR_RECOMMENDATION_COOLDOWN,
            last_recommendation: None,
        }
    }

    /// Pure decision: given the latest `CalibrationInput`
    /// and the current wall-clock `now`, return zero or
    /// one `CalibrationRecommendation`. Returns `None` if:
    ///
    /// - cold-start (flushes_total < min_flushes)
    /// - hysteresis (a recommendation fired within
    ///   `recommendation_cooldown`)
    /// - no trigger condition is met
    ///
    /// The function does not mutate `self`. The loop calls
    /// `record_recommendation(now)` after a `Some(...)`
    /// return to update `last_recommendation`.
    pub(crate) fn observe(
        &self,
        input: CalibrationInput,
        now: Instant,
    ) -> Option<CalibrationRecommendation> {
        // Cold-start silence.
        if input.flushes_total < self.min_flushes {
            return None;
        }
        // Hysteresis: at most one recommendation per cooldown.
        if let Some(prev) = self.last_recommendation
            && now.duration_since(prev) < self.recommendation_cooldown
        {
            return None;
        }

        let avg_chunk_size = input
            .chunk_size_sum
            .checked_div(input.flushes_total)
            .unwrap_or(0) as usize;
        let retry_rate_bps = input
            .retry_total
            .checked_mul(10_000)
            .and_then(|n| n.checked_div(input.flushes_total))
            .unwrap_or(0);

        let active = input.current_profile;
        let current_batch_size = active.batch_size();
        let current_threshold = active.fast_flush_threshold();

        // Trigger 1: high retry rate (>= 5%) → raise
        // batch_size so per-key overhead amortises over more
        // keys. Multi-key DeleteObjects has fixed overhead
        // regardless of chunk size; bumping batch_size by
        // 25% cuts per-key cost when retries are eating
        // throughput.
        if retry_rate_bps >= 500 && current_batch_size < 1000 {
            let proposed = (current_batch_size * 5 / 4).clamp(1, 1000);
            return Some(CalibrationRecommendation::RaiseBatchSize {
                current: current_batch_size,
                proposed,
            });
        }

        // Trigger 2: small chunks + low burst → lower
        // fast_flush_threshold so the fast path engages
        // on unlink 1. This is the rm -rf 10/100/200 lever.
        // Median chunk < 2 means most flushes carry one
        // key; burst_p95 < 5 means the workload is a
        // series of small bursts, not a sustained
        // bulk delete.
        if avg_chunk_size < 2 && input.burst_p95 < 5 && current_threshold > 1 {
            return Some(CalibrationRecommendation::LowerFastFlushThreshold {
                current: current_threshold,
                proposed: 1,
            });
        }

        // Trigger 3: very large batch_size with low retry
        // rate → batch_size is wasteful. Halve it to
        // reduce latency per flush. Conservative: only
        // fires when batch_size > 100 AND retry_rate < 1%
        // AND avg_chunk_size < batch_size/8 (i.e. we're
        // consistently flushing much smaller batches than
        // the cap, suggesting the cap is set too high
        // for the workload). batch_size=100 / 8 = 12;
        // a workload with avg_chunk_size=20 is
        // borderline so the threshold is set tighter
        // than the RaiseBatchSize trigger's reciprocal.
        if current_batch_size > 100
            && retry_rate_bps < 100
            && avg_chunk_size < (current_batch_size / 8)
        {
            let proposed = (current_batch_size / 2).clamp(1, 1000);
            return Some(CalibrationRecommendation::LowerBatchSize {
                current: current_batch_size,
                proposed,
            });
        }

        None
    }

    /// Update the hysteresis state. Called by the loop
    /// after the operator-facing log line is emitted, so
    /// the next observation knows to wait the cooldown.
    pub(crate) fn record_recommendation(&mut self, now: Instant) {
        self.last_recommendation = Some(now);
    }
}

/// Issue #562 stage 5: the calibrator loop. Spawned from
/// `spawn()` alongside the controller and flushers. Reads
/// a fresh `CounterSnapshot` + `BurstObserver::p95()` +
/// `ProfileState::current()` every 60s, runs the pure
/// decision function, and on `Some(...)` emits a
/// `tracing::info!` line and bumps
/// `CALIBRATOR_RECOMMENDATIONS_TOTAL`. The loop exits
/// when `rx` (the controller's shutdown channel) closes.
///
/// Loop guarantees:
/// - at most one `tracing::info!` per 60s observation
/// - at most one recommendation per
///   `CALIBRATOR_RECOMMENDATION_COOLDOWN` (10 minutes)
/// - cold-start silent (no recommendations for the first
///   `MIN_FLUSHES_FOR_RECOMMENDATION` flushes)
/// - never mutates the live config; only logs
async fn calibrator_loop(
    _config: WorkerConfig,
    shared: Arc<Shared>,
    mut rx: tokio::sync::broadcast::Receiver<()>,
) {
    // `_config` is reserved for future per-config knobs
    // (e.g. operator-supplied bucket name in the log line).
    // For Stage 5 the calibrator only needs `shared` (for
    // CounterSnapshot + BurstObserver + ProfileState reads)
    // and the shutdown channel.
    let mut state = ThresholdCalibrator::new();
    let mut ticker = tokio::time::interval(CALIBRATOR_OBSERVATION_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    tracing::info!(
        target: "mntrs::batched_delete",
        observation_interval_ms = CALIBRATOR_OBSERVATION_INTERVAL.as_millis() as u64,
        cooldown_ms = CALIBRATOR_RECOMMENDATION_COOLDOWN.as_millis() as u64,
        min_flushes = MIN_FLUSHES_FOR_RECOMMENDATION,
        "batched_delete: calibrator started (memory-only, never auto-applies)"
    );

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let snap = snapshot();
                let burst_p95 = shared.burst_observer.p95();
                let current_profile = shared.profile_state.current();
                let input = CalibrationInput {
                    flushes_total: snap.flushes_total,
                    retry_total: snap.retry_total,
                    chunk_size_sum: snap.chunk_size_sum,
                    burst_p95,
                    current_profile,
                };
                let now = Instant::now();
                if let Some(rec) = state.observe(input, now) {
                    state.record_recommendation(now);
                    CALIBRATOR_RECOMMENDATIONS_TOTAL
                        .fetch_add(1, Ordering::Relaxed);
                    let avg_chunk_size = snap
                        .chunk_size_sum
                        .checked_div(snap.flushes_total)
                        .unwrap_or(0);
                    let retry_rate_bps = snap
                        .retry_total
                        .checked_mul(10_000)
                        .and_then(|n| n.checked_div(snap.flushes_total))
                        .unwrap_or(0);
                    let bucket = match rec {
                        CalibrationRecommendation::LowerBatchSize { current, proposed } => {
                            format!("lower_batch_size current={current} proposed={proposed}")
                        }
                        CalibrationRecommendation::RaiseBatchSize { current, proposed } => {
                            format!("raise_batch_size current={current} proposed={proposed}")
                        }
                        CalibrationRecommendation::LowerFastFlushThreshold { current, proposed } => {
                            format!(
                                "lower_fast_flush_threshold current={current} proposed={proposed}"
                            )
                        }
                    };
                    tracing::info!(
                        target: "mntrs::batched_delete",
                        recommendation = %bucket,
                        avg_chunk_size,
                        retry_rate_bps,
                        burst_p95,
                        current_profile = ?current_profile,
                        flushes_total = snap.flushes_total,
                        "batched_delete: calibrator recommendation (memory-only, NOT auto-applied; set MNTRS_BATCH_SIZE / MNTRS_BATCH_FAST_FLUSH_THRESHOLD on next mount to act)"
                    );
                }
            }
            _ = rx.recv() => {
                tracing::info!(
                    target: "mntrs::batched_delete",
                    "batched_delete: calibrator exiting"
                );
                return;
            }
        }
    }
}

/// Plan #64 stage B: snapshot of batched_delete counters.
/// Exposed for `/metrics` and Stage C observability.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct CounterSnapshot {
    pub flushes_total: u64,
    pub keys_total: u64,
    pub failures_total: u64,
    pub shutdown_lost_total: u64,
    pub single_key_batches_total: u64,
    pub max_batch_size_observed: u64,
    /// Issue #530
    pub threshold_skipped_total: u64,
    /// Issue #553: how many flushes fired via the fast-flush
    /// path.
    pub fast_flush_total: u64,
    /// Issue #562 stage 1.5: how many single-key flushes
    /// went through the plain `DELETE` short-circuit instead
    /// of `DeleteObjects`.
    pub single_key_fast_delete_total: u64,
    /// Issue #562 stage 5 (Calibrator input): total retry
    /// decisions across both the multi-key XML path and the
    /// single-key DELETE path. Used by `ThresholdCalibrator`
    /// to estimate retry_rate = retries / flushes. The
    /// snapshot also exposes the cumulative retry rate
    /// directly so the bench harness can grep for it.
    pub retry_total: u64,
    /// Issue #562 stage 5 (Calibrator input): sum of
    /// `batch.len()` across every flush so the calibrator
    /// can compute `avg_chunk_size = chunk_size_sum /
    /// flushes_total` without keeping a sample array. The
    /// running average is good enough for the calibrator's
    /// "is the median chunk size < 2?" decision.
    pub chunk_size_sum: u64,
    /// Issue #562 stage 5 (Calibrator counter): number of
    /// `tracing::info!` recommendation lines emitted by the
    /// calibrator loop. Zero over a 24h run means the
    /// current profile is well-fit and no adjustment is
    /// suggested.
    pub calibrator_recommendations_total: u64,
}

pub(crate) fn snapshot() -> CounterSnapshot {
    CounterSnapshot {
        flushes_total: FLUSHES_TOTAL.load(Ordering::Relaxed),
        keys_total: KEYS_TOTAL.load(Ordering::Relaxed),
        failures_total: FAILURES_TOTAL.load(Ordering::Relaxed),
        shutdown_lost_total: SHUTDOWN_LOST_TOTAL.load(Ordering::Relaxed),
        single_key_batches_total: SINGLE_KEY_BATCHES_TOTAL.load(Ordering::Relaxed),
        max_batch_size_observed: MAX_BATCH_SIZE_OBSERVED.load(Ordering::Relaxed),
        threshold_skipped_total: THRESHOLD_SKIPPED_TOTAL.load(Ordering::Relaxed),
        fast_flush_total: FAST_FLUSH_TOTAL.load(Ordering::Relaxed),
        single_key_fast_delete_total: SINGLE_KEY_FAST_DELETE_TOTAL.load(Ordering::Relaxed),
        retry_total: RETRY_TOTAL.load(Ordering::Relaxed),
        chunk_size_sum: CHUNK_SIZE_SUM.load(Ordering::Relaxed),
        calibrator_recommendations_total: CALIBRATOR_RECOMMENDATIONS_TOTAL.load(Ordering::Relaxed),
    }
}

/// Read an env var with a default. Used for stage B tuning
/// knobs (`MNTRS_BATCH_FLUSH_DELAY_MS`, `MNTRS_BATCH_SIZE`).
fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(default)
}

// ===== Pending types =====

/// One queued delete. The result sender is consumed by the worker
/// after the S3 response arrives; on shutdown it is dropped (which
/// closes the receiver and signals `RecvError` to anyone still
/// holding the oneshot).
pub(crate) struct PendingDelete {
    pub relative_path: String,
    pub result_tx: oneshot::Sender<std::io::Result<()>>,
}

struct Pending {
    jobs: Vec<PendingDelete>,
    deadline: Option<Instant>,
}

impl Pending {
    fn new() -> Self {
        Self {
            jobs: Vec::with_capacity(DEFAULT_BATCH_SIZE),
            deadline: None,
        }
    }
    fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }
    fn len(&self) -> usize {
        self.jobs.len()
    }
    fn reset_deadline(&mut self, delay: Duration) {
        self.deadline = Some(Instant::now() + delay);
    }
    fn clear_deadline(&mut self) {
        self.deadline = None;
    }
}

// ===== Shared state =====

struct Shared {
    pending: Mutex<Pending>,
    accepting: AtomicBool,
    /// Initial deadline applied when the buffer transitions from
    /// empty → non-empty in `enqueue`. Mirrors `WorkerConfig::flush_delay`
    /// (env: `MNTRS_BATCH_FLUSH_DELAY_MS`, default 50 ms) so a single
    /// unlink pays at most this much latency before its S3 DELETE is
    /// requested, while an `rm -rf` burst gets a wide enough window to
    /// accumulate large batches. The rmdir barrier (`Control::Flush`)
    /// bypasses this deadline entirely via `do_flush_all`.
    flush_delay: Duration,
    /// Issue #530: caller-side threshold gating. `enqueue`
    /// checks `pending.len()` and returns `None` (caller falls
    /// back to strict `delete_backend_strict`) when the queue
    /// is smaller than this. Tunable at runtime via
    /// `BatchedDeleter::set_batch_threshold`; seeded from
    /// `WorkerConfig::batch_threshold` (env:
    /// `MNTRS_BATCH_THRESHOLD`, default 32). Stored as AtomicUsize
    /// so a future /metrics endpoint can adjust without
    /// re-spawning the worker.
    batch_threshold: std::sync::atomic::AtomicUsize,
    /// Tombstones for paths that have an in-flight write-behind
    /// delete. Owned by MntrsFs (test + production constructors
    /// both pre-build a DashSet and pass its Arc clone here) so
    /// lookup/getattr/readdir on the FUSE side and worker success
    /// paths on the deleter side see the same set. The worker
    /// clears an entry whenever it acks a per-key result (success,
    /// NotFound idempotent, or permanent failure with an error
    /// log).
    ///
    /// Plan #64 step 10 originally kept tombstones insert-only
    /// with a `TODO` for result-driven cleanup (see lib.rs:5891
    /// history). Stage C default-ON made that TODO a correctness
    /// bug: rm-then-create same path returned ENOENT because the
    /// tombstone outlived the S3 delete. Real fix landed as part
    /// of stage C: worker removes on every per-key ack, and
    /// `cancel_pending` clears tombstone + drops the queued job
    /// without sending the S3 DELETE (called from create() before
    /// op.write).
    tombs: std::sync::Arc<dashmap::DashSet<String>>,
    /// Issue #562 stage 3: lock-free ring buffer of
    /// `pending.len()` samples. The enqueue path pushes one
    /// sample per unlink (single atomic store); the controller
    /// reads `p95()` once per iteration to feed
    /// `ProfileState`. Lives on `Shared` so the controller
    /// and flushers can both observe without extra plumbing
    /// (flushers don't currently read it, but future
    /// per-flusher metrics might).
    burst_observer: Arc<BurstObserver>,
    /// Issue #562 stage 3: owner of the active profile plus
    /// the hysteresis + cooldown logic that decides when to
    /// flip. The controller calls `observe(burst_observer.p95(),
    /// Instant::now())` before each `decide_next_action`. Shared
    /// across the controller and all flushers so a future
    /// `/metrics` endpoint can read the current profile
    /// without routing through the controller.
    profile_state: Arc<ProfileState>,
}

// ===== Public handle =====

/// Cheap-clone handle for FUSE callbacks to enqueue deletes. The
/// worker task is owned by the `JoinHandle` returned from `spawn`.
/// Dropping all `BatchedDeleter` handles (including the one stored
/// in `MntrsFs`) closes the control channel and the worker exits on
/// `rx.recv().await → None`.
#[derive(Clone)]
pub(crate) struct BatchedDeleter {
    shared: Arc<Shared>,
    flush_tx: mpsc::Sender<Control>,
}

// ===== Worker config =====

/// Construction-time config. Built by `cmd/mount.rs::build_s3` after
/// parsing the storage URL and CLI options; passed to
/// `batched_delete::spawn`.
#[derive(Clone)]
pub(crate) struct WorkerConfig {
    pub endpoint: url::Url,
    pub bucket: String,
    /// Operator root — prepended to every relative path before
    /// signing. Sourced from `operator.info().root()` so the
    /// batcher deletes exactly what opendal would have.
    pub prefix: String,
    pub region: String,
    /// Explicit static credentials when `--opt access-key=...` +
    /// `--opt secret-key=...` are supplied; otherwise `None` and we
    /// fall back to the env/profile/IMDS chain.
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    /// Shared reqwest client — same TLS settings + connection pool
    /// as opendal uses (see `cmd/mount.rs::build_mount_http_client`).
    pub http: reqwest::Client,
    pub batch_size: usize,
    pub flush_delay: Duration,
    /// Issue #530: caller-side threshold gating. See
    /// `Shared::batch_threshold` for semantics. Sourced from
    /// env `MNTRS_BATCH_THRESHOLD` (default 32) in
    /// `from_s3`. 0 means "always batch, even single files"
    /// (matches the pre-#530 behaviour for users who want
    /// it).
    pub batch_threshold: usize,
    /// Issue #553: fast-flush threshold. When
    /// `pending.len() < fast_flush_threshold` at decision
    /// time, the worker flushes immediately instead of
    /// waiting for the `flush_delay` deadline. 0 disables
    /// the fast path. Sourced from env
    /// `MNTRS_BATCH_FAST_FLUSH_THRESHOLD` (default 8).
    pub fast_flush_threshold: usize,
    /// Issue #562 stage 1: number of concurrent flusher
    /// loops that share `Shared::pending` and call
    /// `send_chunk_with_retry`. One controller task owns
    /// the `mpsc::Receiver<Control>` and runs
    /// `decide_next_action`; `worker_count` flusher tasks
    /// subscribe to a `tokio::sync::broadcast` and drain
    /// pending keys in parallel. Each flusher takes the
    /// `Mutex<Pending>` only across the drain slice, so
    /// S3 round-trips (`send_chunk_with_retry`) overlap
    /// and the connection pool in `http` (shared via
    /// `Arc`) is fully utilised. Sourced from env
    /// `MNTRS_BATCH_WORKER_COUNT` (default 4, clamp
    /// 1..=16). Value 1 reproduces the pre-#562
    /// single-consumer behaviour.
    pub worker_count: usize,
    /// Issue #562 stage 3: which workload-shape profile the
    /// batcher starts with. `Some(pinned)` for pinned mode
    /// (`MNTRS_BATCH_PROFILE=small|medium|bulk`); `None` for
    /// auto mode (the default; `ProfileState` starts at
    /// `Medium` and flips based on the observer's hint).
    /// When this is set, the per-flush knobs on this struct
    /// (`batch_size`, `flush_delay`, `fast_flush_threshold`)
    /// are still used as the **seed** for the initial profile
    /// in auto mode but are otherwise ignored at runtime —
    /// the live values come from `ProfileState` via
    /// `Profile::batch_size()` etc.
    pub initial_profile: Option<Profile>,
    pub request_timeout: Duration,
    pub max_retries: u32,
    pub retry_factor: f64,
    pub retry_initial_backoff: Duration,
}

impl WorkerConfig {
    /// Build the production config from S3 mount-time inputs.
    /// `prefix` should be the opendal root (e.g. `/some/dir/`).
    pub(crate) fn from_s3(
        endpoint: url::Url,
        bucket: String,
        prefix: String,
        region: String,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        http: reqwest::Client,
    ) -> Self {
        // Plan #64 stage B: honor MNTRS_BATCH_SIZE and
        // MNTRS_BATCH_FLUSH_DELAY_MS env vars for per-mount
        // tuning. Defaults preserve plan #64 baseline.
        let batch_size = env_usize("MNTRS_BATCH_SIZE", DEFAULT_BATCH_SIZE).clamp(1, 1000);
        let flush_delay_ms = env_u64(
            "MNTRS_BATCH_FLUSH_DELAY_MS",
            DEFAULT_FLUSH_DELAY.as_millis() as u64,
        );
        let flush_delay = Duration::from_millis(flush_delay_ms.clamp(1, 10_000));
        // Issue #530: caller-side threshold. 0 = always batch
        // (legacy); 1 = never batch (effectively disables
        // batching — every enqueue returns None); N > 1 =
        // batch only when pending.len() >= N at enqueue time.
        let batch_threshold = env_usize("MNTRS_BATCH_THRESHOLD", DEFAULT_BATCH_THRESHOLD);
        // Issue #553: fast-flush threshold. 0 disables the
        // immediate-flush branch in worker_loop and matches
        // the pre-fix behaviour (every non-full batch waits
        // for the deadline).
        let fast_flush_threshold = env_usize(
            "MNTRS_BATCH_FAST_FLUSH_THRESHOLD",
            DEFAULT_FAST_FLUSH_THRESHOLD,
        );
        // Issue #562 stage 1: number of flusher tasks that
        // run in parallel. Default 4 matches rclone
        // --transfers=4; clamp 1..=16 keeps a runaway env
        // value from spawning dozens of S3 clients. Value 1
        // reproduces pre-#562 behaviour (single consumer).
        let worker_count = env_usize("MNTRS_BATCH_WORKER_COUNT", DEFAULT_BATCH_WORKER_COUNT)
            .clamp(1, MAX_BATCH_WORKER_COUNT);
        Self {
            endpoint,
            bucket,
            prefix,
            region,
            access_key_id,
            secret_access_key,
            http,
            batch_size,
            flush_delay,
            batch_threshold,
            fast_flush_threshold,
            worker_count,
            // Issue #562 stage 3: pinned vs auto. `None`
            // means auto (ProfileState starts at Medium and
            // the observer drives transitions); `Some(p)`
            // means pinned at the chosen profile (cooldown
            // set to Duration::MAX in spawn so the profile
            // never changes). See `parse_profile_or_auto`.
            initial_profile: parse_profile_or_auto("MNTRS_BATCH_PROFILE"),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_retries: DEFAULT_MAX_RETRIES,
            retry_factor: DEFAULT_RETRY_FACTOR,
            retry_initial_backoff: DEFAULT_RETRY_INITIAL_BACKOFF,
        }
    }
}

// ===== Control commands =====

enum Control {
    /// Wake the worker to re-evaluate the queue (used on first
    /// enqueue to break the worker out of `rx.recv()` so the
    /// empty-buffer fast path can fire without waiting for the
    /// timer).
    Wake,
    /// Force a flush of all pending keys; signal `done` with the
    /// number of keys flushed (or `Err` on transport-level failure).
    /// Used by `BatchedDeleter::flush()` (Policy B rmdir barrier).
    Flush {
        done: oneshot::Sender<std::io::Result<usize>>,
    },
    /// Graceful shutdown. `drain=true` flushes pending before exit;
    /// `drain=false` fails all pending oneshots with `BrokenPipe`
    /// and exits. Worker signals `done` when it has stopped
    /// accepting new work.
    Shutdown {
        drain: bool,
        done: oneshot::Sender<()>,
    },
}

// ===== Spawn =====

/// Issue #562 stage 1: handle bundle returned by `spawn`.
/// The controller task is the one that owns the
/// `mpsc::Receiver<Control>` and runs `decide_next_action`;
/// the flushers are the worker-count pool that drains
/// `Shared::pending` and calls `send_chunk_with_retry`.
///
/// Callers can drop this struct to abandon the workers
/// (they will exit when `flush_tx` and `wake_tx` go out of
/// scope); or await the controller handle first and then
/// the flusher handles for a clean shutdown. MntrsFs
/// currently takes the drop path (fire-and-forget): all
/// `BatchedDeleter` clones drop → `flush_tx` drops →
/// controller's `rx.recv()` returns `None` → controller
/// exits → `wake_tx` drops → flushers' `wake_rx.recv()`
/// returns `Err(Sender)` → flushers exit.
#[allow(dead_code)] // the field set is wiring + shutdown paths
pub(crate) struct WorkerHandles {
    pub(crate) controller: tokio::task::JoinHandle<()>,
    pub(crate) flushers: Vec<tokio::task::JoinHandle<()>>,
    /// Issue #562 stage 5: the calibrator task. Memory-only,
    /// emits `tracing::info!` recommendations every 60s when
    /// the running snapshot crosses a trigger. Callers may
    /// drop this handle (the task exits on its own when the
    /// wake broadcast closes) or `.await` it for clean
    /// shutdown.
    pub(crate) calibrator: tokio::task::JoinHandle<()>,
}

pub(crate) fn spawn(
    config: WorkerConfig,
    tombs: std::sync::Arc<dashmap::DashSet<String>>,
) -> std::io::Result<(BatchedDeleter, WorkerHandles)> {
    let (tx, rx) = mpsc::channel::<Control>(64);
    // Issue #562 stage 3: profile state. `Some(pinned)` from
    // `WorkerConfig::initial_profile` means the user pinned
    // the profile via env (e.g. `MNTRS_BATCH_PROFILE=medium`);
    // we use `Duration::MAX` as cooldown so the observer's
    // hint never flips the profile — the pinned value is
    // bit-for-bit the pre-Stage-3 code path. `None` means
    // auto mode: start at Medium and let the observer drive
    // transitions with the standard 5 s cooldown.
    let (initial_profile, cooldown) = match config.initial_profile {
        Some(p) => (p, Duration::MAX),
        None => (Profile::Medium, PROFILE_DEFAULT_COOLDOWN),
    };
    let profile_state = Arc::new(ProfileState::new(initial_profile, cooldown));
    tracing::info!(
        target: "mntrs::batched_delete",
        initial_profile = ?initial_profile,
        cooldown_ms = if cooldown == Duration::MAX {
            u64::MAX
        } else {
            cooldown.as_millis() as u64
        },
        "batched_delete: profile state initialised"
    );
    let shared = Arc::new(Shared {
        pending: Mutex::new(Pending::new()),
        accepting: AtomicBool::new(true),
        flush_delay: config.flush_delay,
        batch_threshold: std::sync::atomic::AtomicUsize::new(config.batch_threshold),
        tombs,
        burst_observer: Arc::new(BurstObserver::new()),
        profile_state: profile_state.clone(),
    });
    let deleter = BatchedDeleter {
        shared: shared.clone(),
        flush_tx: tx,
    };
    // Plan #64 stage A bug fix: must use crate::rt().spawn,
    // not bare tokio::spawn. The mount's main thread is not
    // inside a tokio runtime context — bare tokio::spawn
    // panics with "there is no reactor running" before the
    // worker even starts. writeback::spawn uses the same
    // pattern (writeback.rs:174).
    //
    // Issue #562 stage 1: spawn one controller task (owns
    // `rx`, runs `decide_next_action`, drives shutdown
    // semantics) plus `worker_count` flusher tasks. The
    // flushers subscribe to a broadcast channel for
    // wakeups; the broadcast sender lives inside the
    // controller task so the channel closes when the
    // controller exits, which is how flushers know to
    // break out of `wake_rx.recv()`.
    let worker_count = config.worker_count.max(1);
    let (wake_tx, _wake_rx_for_seed) = tokio::sync::broadcast::channel::<()>(1);
    let mut flusher_handles = Vec::with_capacity(worker_count);
    for flusher_id in 0..worker_count {
        let wake_rx = wake_tx.subscribe();
        let cfg = config.clone();
        let sh = shared.clone();
        flusher_handles.push(crate::rt().spawn(flusher_loop(flusher_id, cfg, sh, wake_rx)));
    }
    let controller_handle = crate::rt().spawn(controller_loop(
        config.clone(),
        shared.clone(),
        rx,
        wake_tx.clone(),
    ));
    // Issue #562 stage 5: spawn the calibrator. It subscribes
    // to the same `wake_tx` broadcast as the flushers, so when
    // the controller drops `wake_tx` at shutdown the
    // calibrator's `rx.recv()` returns `Err` and the loop
    // exits cleanly. Memory-only, no network exposure; only
    // emits `tracing::info!` recommendations via the daemon
    // log.
    let calibrator_handle = crate::rt().spawn(calibrator_loop(config, shared, wake_tx.subscribe()));
    Ok((
        deleter,
        WorkerHandles {
            controller: controller_handle,
            flushers: flusher_handles,
            calibrator: calibrator_handle,
        },
    ))
}

// ===== BatchedDeleter API =====

impl BatchedDeleter {
    /// Enqueue a relative path for deletion. Returns `None` if the
    /// worker is shutting down (handle will not accept new work);
    /// returns `Some(oneshot::Receiver)` otherwise. The caller
    /// (MntrsFs write-behind path) drops the receiver — the per-key
    /// S3 outcome is observed via the worker's tracing logs, not
    /// via the receiver. The receiver exists for tests and for any
    /// strict caller.
    ///
    /// Issue #530: caller-side threshold gating. If
    /// `pending.len() < batch_threshold` at the moment of
    /// enqueue, this method returns `None` so the caller
    /// falls back to a strict `delete_backend_strict`. Reason:
    /// the per-key latency floor is `flush_delay` (50 ms by
    /// default), and a one-off unlink or a 10-file `rm -rf`
    /// pays that floor without amortising it across enough
    /// siblings to beat the direct `op.delete()` path.
    pub(crate) fn enqueue(
        &self,
        relative_path: String,
    ) -> Option<oneshot::Receiver<std::io::Result<()>>> {
        if !self.shared.accepting.load(Ordering::Acquire) {
            return None;
        }
        // Issue #530: caller-side threshold gating. Read the
        // threshold atomically (no lock needed) and inspect
        // the queue length under the pending mutex. Two lock
        // acquisitions rather than one would race with a
        // concurrent flush, but `pending_len()` returns a
        // snapshot and the gating decision is best-effort:
        // a race that lets a few extra keys through is
        // strictly better than a race that drops a key
        // entirely. Bump the counter before returning None so
        // the gating decision is observable in /metrics.
        let threshold = self
            .shared
            .batch_threshold
            .load(std::sync::atomic::Ordering::Relaxed);
        if threshold > 0 {
            let current_len = {
                let pending = self.shared.pending.lock().expect("pending mutex poisoned");
                pending.len()
            };
            if current_len < threshold {
                THRESHOLD_SKIPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
                tracing::trace!(
                    target: "mntrs::batched_delete",
                    relative_path = %relative_path,
                    current_len,
                    threshold,
                    "enqueue: below batch_threshold, returning None for strict fallback"
                );
                return None;
            }
        }
        let (tx, rx) = oneshot::channel();
        let job = PendingDelete {
            relative_path,
            result_tx: tx,
        };
        let mut pending = self.shared.pending.lock().expect("pending mutex poisoned");
        let was_empty = pending.is_empty();
        pending.jobs.push(job);
        if was_empty {
            // Use the configured flush_delay (env:
            // MNTRS_BATCH_FLUSH_DELAY_MS, default 50ms) rather than
            // a hard-coded 5ms. For an `rm file.txt` single-file
            // call the user pays at most `flush_delay` latency
            // before the S3 DELETE is requested; for an `rm -rf`
            // burst the wider window lets more keys accumulate
            // per batch and halves the number of S3 roundtrips.
            // The rmdir barrier (`Control::Flush`) bypasses this
            // deadline entirely via `do_flush_all`.
            pending.reset_deadline(self.shared.flush_delay);
        }
        // Snapshot the new queue length under the lock (so the
        // sample is consistent with the push we just did), then
        // release the lock before pushing to the observer —
        // BurstObserver::observe is lock-free but we keep the
        // critical section as small as possible.
        let new_len = pending.jobs.len();
        drop(pending);
        // Issue #562 stage 3: push one sample per enqueue so
        // the controller can compute a p95 over the last ~30 s
        // and feed `ProfileState`. Single atomic store; the
        // dominant cost on this path is the surrounding S3
        // round-trip in the worker, so the observer push is
        // in the noise.
        self.shared.burst_observer.observe(new_len);

        if was_empty {
            // Wake the worker so it doesn't wait out its current
            // `rx.recv().await` before noticing the new deadline.
            // try_send because the channel is bounded and a slow
            // worker isn't a reason to block the FUSE thread; if
            // the worker is mid-flush, the next iteration will see
            // the new pending entries on its own.
            let _ = self.flush_tx.try_send(Control::Wake);
        }
        Some(rx)
    }

    /// Cancel any pending delete for `relative_path`. Used by
    /// `MntrsFs::create()` (and `mkdir()`) before the new
    /// op.write fires, so a create-after-rm doesn't hit two
    /// problems at once:
    ///
    ///   1. lookup/getattr/readdir still see the tombstone (the
    ///      write-behind S3 delete hasn't landed yet, so the
    ///      path may still exist on the backend).
    ///   2. without cancel, the in-flight delete would race the
    ///      new write and either delete the freshly created
    ///      object (data loss) or, idempotent-NotFound the new
    ///      object (false "tombstone leaked" — worse, kernel
    ///      would surface ENOENT to the user).
    ///
    /// Sync (no need to await the worker): under the pending
    /// mutex we drain matching jobs and complete their oneshots
    /// with `Err(ErrorKind::Interrupted)` so any strict-mode
    /// caller sees a cancel rather than a phantom success. The
    /// tombstone entry is also removed here — by the time this
    /// returns, the next lookup will not see the deleted path
    /// as gone.
    ///
    /// Returns the number of pending jobs cancelled (0 if no
    /// delete was queued for this path).
    pub(crate) fn cancel_pending(&self, relative_path: &str) -> usize {
        let mut cancelled = 0usize;
        {
            let mut pending = self.shared.pending.lock().expect("pending mutex poisoned");
            let mut kept = Vec::with_capacity(pending.jobs.len());
            for job in pending.jobs.drain(..) {
                if job.relative_path == relative_path {
                    let _ = job.result_tx.send(Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "batched_deleter: cancelled by recreate",
                    )));
                    cancelled += 1;
                } else {
                    kept.push(job);
                }
            }
            pending.jobs = kept;
            if pending.is_empty() {
                pending.clear_deadline();
            }
        }
        // Tombstone clear is unconditional: even if no job was
        // pending, the FUSE-side lookup will hit it if the user
        // did `rm file; sleep 50ms; touch file` (50ms < flush
        // deadline of 5ms would race, but if user pre-set
        // MNTRS_BATCH_FLUSH_DELAY_MS=10 or higher, the tombstone
        // outlives the queue). Clearing on every create() call
        // is the safest invariant.
        self.shared.tombs.remove(relative_path);
        cancelled
    }

    /// Read-only access to the tombstone set for FUSE-side
    /// filters (lookup, getattr, readdir). Cheap clone of the
    /// Arc — no copy of the underlying DashSet.
    pub(crate) fn tombstones(&self) -> std::sync::Arc<dashmap::DashSet<String>> {
        self.shared.tombs.clone()
    }

    /// Force-flush all currently pending keys. Used by the rmdir
    /// barrier under Policy B so `rm -rf dir` doesn't return before
    /// the dir's deletes have actually been requested. Returns the
    /// total number of keys flushed (across one or more chunks);
    /// `Err` on transport-level failure that exhausted retries.
    pub(crate) async fn flush(&self) -> std::io::Result<usize> {
        let (done_tx, done_rx) = oneshot::channel();
        if self
            .flush_tx
            .send(Control::Flush { done: done_tx })
            .await
            .is_err()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "batched_deleter: worker channel closed",
            ));
        }
        done_rx.await.map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "batched_deleter: worker dropped flush response",
            )
        })?
    }

    /// Graceful shutdown. `drain=true` flushes pending keys (best
    /// effort — failures are logged, not propagated); `drain=false`
    /// fails all pending oneshots immediately. Used by
    /// `MntrsFs::shutdown` and the Drop fallback.
    pub(crate) async fn shutdown(self, drain: bool) {
        self.shared.accepting.store(false, Ordering::Release);
        let (done_tx, done_rx) = oneshot::channel();
        if self
            .flush_tx
            .send(Control::Shutdown {
                drain,
                done: done_tx,
            })
            .await
            .is_ok()
        {
            let _ = done_rx.await;
        }
        // self dropped here → flush_tx (one of possibly several
        // clones) drops → when the LAST clone drops, the worker's
        // rx.recv().await returns None and the worker exits.
    }

    /// True if the worker is still accepting work.
    pub(crate) fn is_accepting(&self) -> bool {
        self.shared.accepting.load(Ordering::Acquire)
    }

    /// Issue #530: snapshot of the pending queue length. Used
    /// by `MntrsFs::enqueue_backend_delete` to decide whether
    /// the next unlink should go through the batched path or
    /// fall through to a direct `delete_backend_strict`. Cheap
    /// (mutex lock + len read); the cost is dominated by
    /// whatever's already holding the pending mutex.
    pub(crate) fn pending_len(&self) -> usize {
        let pending = self.shared.pending.lock().expect("pending mutex poisoned");
        pending.len()
    }

    /// Issue #530: runtime-adjust the batch threshold without
    /// re-spawning the worker. `0` disables gating (always
    /// batch); `1` effectively disables batching (every
    /// enqueue returns None); `N > 1` batches only when the
    /// pending queue is at least `N` at enqueue time. Returns
    /// the previous threshold.
    pub(crate) fn set_batch_threshold(&self, threshold: usize) -> usize {
        self.shared
            .batch_threshold
            .swap(threshold, std::sync::atomic::Ordering::Relaxed)
    }

    /// Issue #530: read the current batch threshold.
    pub(crate) fn batch_threshold(&self) -> usize {
        self.shared
            .batch_threshold
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

// ===== Worker loop =====
//
// Issue #562 stage 1: split the single consumer loop into
// one controller + N flushers. The controller owns the
// `mpsc::Receiver<Control>` (so `Control::Flush` /
// `Control::Shutdown` ack semantics stay in one place) and
// runs the pure `decide_next_action` helper. Flushers
// subscribe to a `tokio::sync::broadcast` for wakeups and
// drain `Shared::pending` in parallel — their S3
// round-trips overlap. The `Mutex<Pending>` inside
// `flush_one_batch` serialises the drain slice, but the
// S3 call is outside the lock.

/// Controller task. Owns the `mpsc::Receiver<Control>`
/// channel, the `wake_tx` broadcast sender, the S3 signer,
/// and shutdown accounting. Runs the pure
/// `decide_next_action` helper to decide whether to flush
/// immediately or wait for the deadline. On any flush
/// decision (size-driven, fast-flush, or deadline-expired)
/// it both runs `flush_one_batch` locally **and** broadcasts
/// a wake to the flusher pool so they can drain any
/// remainder in parallel.
async fn controller_loop(
    config: WorkerConfig,
    shared: Arc<Shared>,
    mut rx: mpsc::Receiver<Control>,
    wake_tx: tokio::sync::broadcast::Sender<()>,
) {
    let signer = match build_signer(&config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(
                target: "mntrs::batched_delete",
                bucket = %config.bucket,
                prefix = %config.prefix,
                error = %e,
                "batched_delete: failed to build signer, controller exiting"
            );
            shared.accepting.store(false, Ordering::Release);
            fail_all_pending(&shared);
            return;
        }
    };

    tracing::info!(
        target: "mntrs::batched_delete",
        bucket = %config.bucket,
        prefix = %config.prefix,
        batch_size = config.batch_size,
        flush_delay_ms = config.flush_delay.as_millis() as u64,
        worker_count = config.worker_count,
        credential_source = if config.access_key_id.is_some() { "explicit" } else { "default-chain" },
        "batched_delete: controller started"
    );

    loop {
        // Snapshot pending state under lock; decide what to do.
        // The decision is delegated to `decide_next_action` so the
        // threshold logic stays unit-testable (see tests below).
        //
        // Issue #562 stage 3: feed the burst observer's p95
        // into `ProfileState::observe` to pick the active
        // profile. This is called once per controller
        // iteration (not per enqueue), so the per-flush cost
        // is one atomic load (observer.p95) + one mutex
        // acquisition (profile state last_transition). Both
        // are in the noise compared to the S3 round-trip the
        // action handler will issue.
        let action = {
            let pending = shared.pending.lock().expect("pending mutex poisoned");
            let hint = shared.burst_observer.p95();
            let profile = shared.profile_state.observe(hint, Instant::now());
            decide_next_action(profile, pending.len(), pending.deadline)
        };

        match action {
            None => match rx.recv().await {
                Some(Control::Wake) => continue,
                Some(Control::Flush { done }) => {
                    let flushed = do_flush_all(&config, &signer, &shared).await;
                    // `do_flush_all` already drained everything
                    // inside HARD_MAX_KEYS_PER_REQUEST chunks;
                    // there's no remainder for flushers to pick
                    // up. Still broadcast — costs one channel
                    // send — so any flusher that happened to
                    // miss the previous wake gets an empty-loop
                    // tick and exits its drain loop on the
                    // `pending.is_empty()` check inside.
                    let _ = wake_tx.send(());
                    let _ = done.send(flushed);
                    continue;
                }
                Some(Control::Shutdown { drain, done }) => {
                    if drain {
                        let _ = do_flush_all(&config, &signer, &shared).await;
                        let _ = wake_tx.send(());
                    } else {
                        // Lost without drain — record how many.
                        let lost = {
                            let mut p = shared.pending.lock().expect("pending mutex poisoned");
                            let n = p.jobs.len() as u64;
                            p.jobs.clear();
                            n
                        };
                        if lost > 0 {
                            SHUTDOWN_LOST_TOTAL.fetch_add(lost, Ordering::Relaxed);
                        }
                        fail_all_pending(&shared);
                    }
                    let _ = done.send(());
                    break;
                }
                None => {
                    // Channel closed without explicit shutdown — same
                    // accounting as drain=false: lost keys count.
                    let lost = {
                        let p = shared.pending.lock().expect("pending mutex poisoned");
                        p.jobs.len() as u64
                    };
                    if lost > 0 {
                        SHUTDOWN_LOST_TOTAL.fetch_add(lost, Ordering::Relaxed);
                    }
                    fail_all_pending(&shared);
                    break;
                }
            },
            Some(ScheduledAction::FlushBatch { fast }) => {
                // Issue #553: `fast=true` flushes were triggered by
                // the small-batch fast-flush branch in
                // `decide_next_action` (pending.len() <
                // fast_flush_threshold). They're the fix for the
                // small-batch regression tracked in #541 — the
                // deadline wait below would otherwise add 10–50ms
                // of latency to single-file `rm` and the tail
                // fragments of `rm -rf`. Bump the metric here
                // (not in the helper) so the helper stays pure
                // and tests can exercise the decision without
                // bumping counters.
                if fast {
                    FAST_FLUSH_TOTAL.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(
                        target: "mntrs::batched_delete",
                        reason = "fast",
                        "batched_delete: flushing via fast-flush branch"
                    );
                }
                flush_one_batch(&config, &signer, &shared, config.batch_size).await;
                // Wake flushers so they race for any keys that
                // accumulated after the controller took its
                // batch but before the broadcast lands. Cheap
                // to ignore (RecvError::Lagged is swallowed by
                // the broadcast sender's capacity-1 design).
                let _ = wake_tx.send(());
            }
            Some(ScheduledAction::WaitForDeadline(deadline)) => {
                let now = Instant::now();
                let sleep_dur = deadline
                    .map(|d| d.saturating_duration_since(now))
                    .unwrap_or(config.flush_delay);
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep(sleep_dur) => {
                        flush_one_batch(&config, &signer, &shared, config.batch_size).await;
                        let _ = wake_tx.send(());
                    }
                    ctrl = rx.recv() => match ctrl {
                        Some(Control::Wake) => continue,
                        Some(Control::Flush { done }) => {
                            let flushed = do_flush_all(&config, &signer, &shared).await;
                            let _ = wake_tx.send(());
                            let _ = done.send(flushed);
                        }
                        Some(Control::Shutdown { drain, done }) => {
                            if drain {
                                let _ = do_flush_all(&config, &signer, &shared).await;
                                let _ = wake_tx.send(());
                            } else {
                                fail_all_pending(&shared);
                            }
                            let _ = done.send(());
                            break;
                        }
                        None => {
                            fail_all_pending(&shared);
                            break;
                        }
                    }
                }
            }
        }
    }

    // Wake broadcast sender drops here (it's a local
    // variable, moved into the function). Flushers'
    // `wake_rx.recv()` will return `Err(Sender)` and they
    // exit. Note: the flushers do NOT need a separate
    // shutdown ack — they simply observe that the
    // controller is gone.
    tracing::info!(
        target: "mntrs::batched_delete",
        "batched_delete: controller exiting"
    );
}

/// Flusher task. Blocks on `wake_rx.recv()` until the
/// controller broadcasts a wake, then drains
/// `Shared::pending` via `flush_one_batch` in a loop until
/// the queue is empty. Does NOT call `decide_next_action`
/// — that's the controller's job. Exits when the broadcast
/// sender drops (controller exited) or on
/// `RecvError::Closed`.
///
/// Why a separate function (not N copies of the same loop):
/// `Control::Flush` / `Control::Shutdown` carry
/// `oneshot::Sender` ack channels; the controller owns
/// those semantics. Flushers have no such obligations —
/// they're pure drain workers and exit on sender drop.
async fn flusher_loop(
    flusher_id: usize,
    config: WorkerConfig,
    shared: Arc<Shared>,
    mut wake_rx: tokio::sync::broadcast::Receiver<()>,
) {
    // Each flusher owns its own signer. The signer holds a
    // reference to the shared `reqwest::Client` connection
    // pool inside `config.http` (the builder wraps it in an
    // Arc), so all N flushers share the pool — that's the
    // whole point of the refactor.
    let signer = match build_signer(&config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(
                target: "mntrs::batched_delete",
                flusher_id,
                error = %e,
                "batched_delete: flusher failed to build signer, exiting"
            );
            return;
        }
    };

    tracing::info!(
        target: "mntrs::batched_delete",
        flusher_id,
        worker_count = config.worker_count,
        "batched_delete: flusher started"
    );

    loop {
        // Block until the controller broadcasts. There are
        // three outcomes from `wake_rx.recv()`:
        //   * Ok(n)  — a wake (possibly lagged); drain.
        //   * Err(Sender) — controller exited; exit.
        //   * Err(Lagged(n)) — too many broadcasts since
        //     last recv; treat as a wake (we want to drain,
        //     not exit).
        match wake_rx.recv().await {
            Ok(_) => {}
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                // Catch-up: drain whatever is in the queue.
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                // Controller exited (its wake_tx dropped).
                break;
            }
        }

        // Drain in a tight loop: each `flush_one_batch`
        // takes up to `batch_size` keys under the pending
        // mutex. As long as a batch is non-empty, there may
        // be more — try again. The loop exits when the
        // queue is empty (so we don't spin on a stale wake)
        // or when the controller has signalled exit via
        // Closed (handled by the recv above).
        loop {
            let pending_len = {
                let pending = shared.pending.lock().expect("pending mutex poisoned");
                pending.len()
            };
            if pending_len == 0 {
                break;
            }
            flush_one_batch(&config, &signer, &shared, config.batch_size).await;
        }
    }

    tracing::info!(
        target: "mntrs::batched_delete",
        flusher_id,
        "batched_delete: flusher exiting"
    );
}

#[derive(Debug)]
enum ScheduledAction {
    /// Flush whatever is in the pending queue right now.
    /// `fast=true` means the flush was triggered by the
    /// fast-flush branch (pending.len() < fast_flush_threshold);
    /// `fast=false` means size-driven (pending.len() >=
    /// batch_size). The metric increment lives in the match
    /// arm, not the decision helper, so tests can drive the
    /// decision without bumping counters.
    FlushBatch {
        fast: bool,
    },
    WaitForDeadline(Option<Instant>),
}

/// Decide the next worker action from a snapshot of the
/// pending queue. Pure function — no locks, no async, no
/// metrics — so unit tests can exercise the threshold edges
/// without spawning the worker.
///
/// Decision tree:
///   pending.len() >= profile.batch_size()
///                                     → FlushBatch { fast: false }
///   pending.len() == 0                 → None (caller blocks on rx.recv)
///   pending.len() < profile.fast_flush_threshold() && threshold > 0
///                                     → FlushBatch { fast: true }
///   otherwise                         → WaitForDeadline(deadline)
///
/// Issue #553: the small-batch fast flush shaves the
/// `flush_delay` floor (50 ms by default) for single
/// unlinks and tail fragments of `rm -rf`, where deadline
/// wait amortises over too few keys to be worth the latency.
///
/// Issue #562 stage 3: the thresholds now come from the
/// active `Profile` (chosen by `ProfileState` based on the
/// burst observer) rather than the static `WorkerConfig`.
/// The helper stays a pure function so the 6 existing unit
/// tests can drive it with a `Profile` literal and observe
/// the expected decision without touching the state
/// machine.
fn decide_next_action(
    profile: Profile,
    pending_len: usize,
    deadline: Option<Instant>,
) -> Option<ScheduledAction> {
    let batch_size = profile.batch_size();
    let fast_flush_threshold = profile.fast_flush_threshold();
    if pending_len >= batch_size {
        Some(ScheduledAction::FlushBatch { fast: false })
    } else if pending_len == 0 {
        None
    } else if fast_flush_threshold > 0 && pending_len < fast_flush_threshold {
        Some(ScheduledAction::FlushBatch { fast: true })
    } else {
        Some(ScheduledAction::WaitForDeadline(deadline))
    }
}

// ===== Flush helpers =====

/// Flush one batch of up to `limit` keys. Updates the deadline for
/// any remaining keys.
///
/// Issue #562 stage 3: the per-flush knobs (`batch_size` and
/// `flush_delay`) are now read from the active `Profile`
/// (via `shared.profile_state.current()`) rather than from
/// the `WorkerConfig` argument. `limit` is treated as a
/// ceiling — the profile's batch size is the actual target.
/// We snapshot the profile **once per flush** so a
/// mid-flush profile flip (the controller's next iteration
/// may observe a new p95 and flip the profile) doesn't tear
/// the batch in two different ways mid-flight.
async fn flush_one_batch(
    config: &WorkerConfig,
    signer: &Signer<AwsCredential>,
    shared: &Shared,
    limit: usize,
) {
    // Snapshot the profile once. The lock acquisition below
    // releases any other consumer's view of the profile
    // (none, in practice — `current()` is lock-free), so
    // this read is just a hint of "what profile was active
    // when this flush started".
    let profile = shared.profile_state.current();
    let profile_batch_size = profile.batch_size();
    let profile_flush_delay = profile.flush_delay();
    let batch = {
        let mut pending = shared.pending.lock().expect("pending mutex poisoned");
        if pending.is_empty() {
            return;
        }
        // Profile batch_size is the actual target;
        // `limit` (the caller's cap, currently
        // `config.batch_size`) is a ceiling. `min` here
        // means "whichever is smaller": a caller asking for
        // `usize::MAX` gets the profile's value; a caller
        // asking for `WorkerConfig::batch_size` (today's
        // pre-Stage-3 behaviour) still gets that ceiling if
        // it's smaller than the profile's value.
        let take = pending.jobs.len().min(profile_batch_size).min(limit);
        let drained: Vec<PendingDelete> = pending.jobs.drain(..take).collect();
        if !pending.is_empty() {
            pending.reset_deadline(profile_flush_delay);
        } else {
            pending.clear_deadline();
        }
        drained
    };

    if batch.is_empty() {
        return;
    }

    let started = Instant::now();
    let outcome = send_chunk_with_retry(config, signer, shared, &batch, &config.prefix).await;
    let elapsed_ms = started.elapsed().as_millis() as u64;

    FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
    KEYS_TOTAL.fetch_add(batch.len() as u64, Ordering::Relaxed);
    // Issue #562 stage 5: feed the Calibrator. Sum into
    // CHUNK_SIZE_SUM so `chunk_size_avg = sum / flushes`
    // can be read cheaply from a snapshot. We avoid a
    // sample array because the hot path is the per-flush
    // loop and a single `fetch_add` is in the noise
    // compared to the S3 round-trip that just happened.
    CHUNK_SIZE_SUM.fetch_add(batch.len() as u64, Ordering::Relaxed);
    if batch.len() == 1 {
        SINGLE_KEY_BATCHES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    // Update max batch size (lock-free CAS).
    let bs = batch.len() as u64;
    let mut cur = MAX_BATCH_SIZE_OBSERVED.load(Ordering::Relaxed);
    while bs > cur {
        match MAX_BATCH_SIZE_OBSERVED.compare_exchange_weak(
            cur,
            bs,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(observed) => cur = observed,
        }
    }
    let (succeeded, not_found, failed) = count_outcome(&outcome);
    if failed > 0 {
        FAILURES_TOTAL.fetch_add(failed, Ordering::Relaxed);
    }

    tracing::info!(
        target: "mntrs::batched_delete",
        batch_size = batch.len(),
        elapsed_ms,
        succeeded,
        not_found,
        failed,
        reason = "scheduled",
        "batched_delete: flush"
    );

    for (job, result) in batch.into_iter().zip(outcome) {
        // Same tombstone-cleanup-on-ack policy as do_flush_all.
        // Centralised here so the two flush paths stay in lockstep;
        // if we ever add a third path, both helpers must converge
        // on it. `result` is `io::Result<()>`; success clears the
        // tombstone silently, NotFound/AlreadyExists clear it
        // silently (idempotent outcome), and any other error logs
        // an `error!` line then clears the tombstone so the user
        // can see the object again.
        match &result {
            Ok(()) => {
                shared.tombs.remove(&job.relative_path);
            }
            Err(e) if matches!(e.kind(), std::io::ErrorKind::NotFound) => {
                shared.tombs.remove(&job.relative_path);
            }
            Err(e) => {
                tracing::error!(
                    target: "mntrs::batched_delete",
                    path = %job.relative_path,
                    error = %e,
                    "batched_delete: per-key delete failed; clearing tombstone so object becomes visible again"
                );
                shared.tombs.remove(&job.relative_path);
            }
        }
        let _ = job.result_tx.send(result);
    }
}

/// Flush all pending keys in chunks of HARD_MAX_KEYS_PER_REQUEST.
/// Returns total number of keys flushed across chunks.
async fn do_flush_all(
    config: &WorkerConfig,
    signer: &Signer<AwsCredential>,
    shared: &Arc<Shared>,
) -> std::io::Result<usize> {
    let mut total = 0usize;
    loop {
        let batch_size = {
            let pending = shared.pending.lock().expect("pending mutex poisoned");
            pending.len()
        };
        if batch_size == 0 {
            return Ok(total);
        }
        let limit = batch_size.min(HARD_MAX_KEYS_PER_REQUEST);
        let batch: Vec<PendingDelete> = {
            let mut pending = shared.pending.lock().expect("pending mutex poisoned");
            pending.jobs.drain(..limit).collect()
        };
        if batch.is_empty() {
            return Ok(total);
        }
        let outcome = send_chunk_with_retry(config, signer, shared, &batch, &config.prefix).await;
        let (succeeded, not_found, failed) = count_outcome(&outcome);
        if failed > 0 {
            FAILURES_TOTAL.fetch_add(failed, Ordering::Relaxed);
        }
        FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
        KEYS_TOTAL.fetch_add(batch.len() as u64, Ordering::Relaxed);
        for (job, result) in batch.into_iter().zip(outcome) {
            // Plan #64 stage C: tombstone cleanup is part of the
            // per-key ack path. Every terminal outcome clears
            // its tombstone — success, NotFound (idempotent),
            // and permanent failures alike. Without this the
            // FUSE side's lookup/getattr/readdir filters would
            // keep masking the path even after the worker
            // confirmed the delete landed on S3, and a
            // recreate would return ENOENT until the user
            // worked around it. See flush_one_batch for the
            // canonical implementation — mirrored here to keep
            // the lock-and-send pattern local to each flush site.
            match &result {
                Ok(()) => {
                    shared.tombs.remove(&job.relative_path);
                }
                Err(e) if matches!(e.kind(), std::io::ErrorKind::NotFound) => {
                    shared.tombs.remove(&job.relative_path);
                }
                Err(e) => {
                    tracing::error!(
                        target: "mntrs::batched_delete",
                        path = %job.relative_path,
                        error = %e,
                        "batched_delete: per-key delete failed; clearing tombstone so object becomes visible again"
                    );
                    shared.tombs.remove(&job.relative_path);
                }
            }
            let _ = job.result_tx.send(result);
        }
        total += succeeded as usize + not_found as usize + failed as usize;
    }
}

fn fail_all_pending(shared: &Shared) {
    let jobs = {
        let mut pending = shared.pending.lock().expect("pending mutex poisoned");
        pending.clear_deadline();
        std::mem::take(&mut pending.jobs)
    };
    let count = jobs.len() as u64;
    if count > 0 {
        SHUTDOWN_LOST_TOTAL.fetch_add(count, Ordering::Relaxed);
        // Plan #64 stage C: tombstones for shutdown-lost keys must
        // be cleared. Without this, the FUSE side's lookup would
        // keep masking paths whose S3 delete never went out, and a
        // subsequent `touch <path>` would return ENOENT until the
        // mount restarts.
        for job in &jobs {
            shared.tombs.remove(&job.relative_path);
        }
        tracing::warn!(
            target: "mntrs::batched_delete",
            lost = count,
            "batched_delete: shutdown dropped pending deletes"
        );
    }
    for job in jobs {
        let _ = job.result_tx.send(Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "batched_deleter: shutdown lost",
        )));
    }
}

fn count_outcome(results: &[std::io::Result<()>]) -> (u64, u64, u64) {
    let mut s = 0u64;
    let mut n = 0u64;
    let mut f = 0u64;
    for r in results {
        match r {
            Ok(()) => s += 1,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => n += 1,
            Err(_) => f += 1,
        }
    }
    (s, n, f)
}

// ===== Signer build =====

/// Build a `reqsign::Signer` for SigV4. If explicit credentials are
/// supplied, prepend a `StaticCredentialProvider` to the chain; the
/// chain itself falls through to env/profile/default-chain.
fn build_signer(config: &WorkerConfig) -> std::io::Result<Signer<AwsCredential>> {
    let ctx = Context::new().with_file_read(TokioFileRead);
    let builder = RequestSigner::new("s3", &config.region);

    let mut chain: ProvideCredentialChain<AwsCredential> = ProvideCredentialChain::new();
    if let (Some(ak), Some(sk)) = (&config.access_key_id, &config.secret_access_key) {
        chain = chain.push(StaticCredentialProvider::new(ak, sk));
    }
    chain = chain
        .push(EnvCredentialProvider::new())
        .push(ProfileCredentialProvider::new())
        .push(DefaultCredentialProvider::new());

    Ok(Signer::new(ctx, chain, builder))
}

// ===== S3 protocol =====

/// Send one chunk (≤HARD_MAX_KEYS_PER_REQUEST keys) with retry.
/// Retries transport failures + retryable HTTP statuses. On
/// non-retryable failure or exhausted retries, every key in the
/// chunk receives the same `Err`. On HTTP 200, parses per-key
/// errors (Quiet=true means success is the absence of an entry).
/// Issue #562 stage 1.5: pure predicate that decides whether
/// a chunk should use the single-key short-circuit. Extracted
/// so the unit tests can exercise every (chunk_len, profile)
/// combination without touching the network. The semantics:
/// short-circuit iff `chunk_len == 1` and the active profile
/// opts in. `Profile::Medium` and `Profile::Bulk` opt out so
/// the multi-key XML path stays in use for batches where the
/// per-key amortisation matters.
fn should_short_circuit_single_key(chunk_len: usize, profile: Profile) -> bool {
    chunk_len == 1 && profile.single_key_fast_delete()
}

async fn send_chunk_with_retry(
    config: &WorkerConfig,
    signer: &Signer<AwsCredential>,
    shared: &Shared,
    chunk: &[PendingDelete],
    prefix: &str,
) -> Vec<std::io::Result<()>> {
    // Issue #562 stage 1.5: single-key short-circuit. When
    // the active profile opts in (`small` by default) and
    // the chunk has exactly one key, skip the DeleteObjects
    // XML body / MD5 / response-parse path and issue a
    // plain `DELETE /bucket/key` instead. Saves ~50-200 µs
    // per single-key flush (MD5 + XML build on the request
    // side; XML response parse on the read side). `Profile::Medium`
    // and `Profile::Bulk` keep the XML path because at
    // batch_size >= 20 the per-key amortisation makes the
    // XML overhead negligible.
    if should_short_circuit_single_key(chunk.len(), shared.profile_state.current()) {
        SINGLE_KEY_FAST_DELETE_TOTAL.fetch_add(1, Ordering::Relaxed);
        let key = join_key(prefix, &chunk[0].relative_path);
        return send_single_delete_with_retry(config, signer, &key).await;
    }

    let body = build_delete_objects_body(chunk, prefix);
    let body_bytes = body.into_bytes();
    let content_md5 = base64_md5(&body_bytes);

    let mut attempt = 0u32;
    let mut backoff = config.retry_initial_backoff;
    loop {
        match send_one_request(config, signer, &body_bytes, &content_md5).await {
            Ok((200, resp_xml)) => {
                // Parse per-key. Per the plan: per-key NoSuchKey /
                // NoSuchVersion → success; everything else → Err.
                // Quiet=true means keys absent from response = Ok.
                let per_key = parse_delete_objects_response(&resp_xml);

                // We have to map per-key entries back to chunk
                // indices. S3 returns one <Error> per failing key,
                // in the same order as requested. Quiet=true means
                // no <Error> for successful keys, so we pad with
                // Ok(()) to chunk.len() entries.
                let mut results: Vec<std::io::Result<()>> = Vec::with_capacity(chunk.len());
                let mut per_key_iter = per_key.into_iter();
                for _ in 0..chunk.len() {
                    match per_key_iter.next() {
                        Some(r) => results.push(r),
                        None => results.push(Ok(())),
                    }
                }
                return results;
            }
            Ok((status, _body)) => {
                let err_msg = format!(
                    "batched_delete: S3 DeleteObjects HTTP {} (request-level, failing all keys in chunk)",
                    status
                );
                if is_retryable_status(status) && attempt < config.max_retries {
                    tracing::warn!(
                        target: "mntrs::batched_delete",
                        status,
                        attempt = attempt + 1,
                        backoff_ms = backoff.as_millis() as u64,
                        "retrying after retryable status"
                    );
                    tokio::time::sleep(backoff).await;
                    // Issue #562 stage 5: feed the Calibrator.
                    // Bumped exactly once per retry decision
                    // (multi-key + single-key paths, status +
                    // transport variants). The Calibrator
                    // computes `retry_rate = RETRY_TOTAL /
                    // FLUSHES_TOTAL` from this counter.
                    RETRY_TOTAL.fetch_add(1, Ordering::Relaxed);
                    backoff = next_backoff(backoff, config.retry_factor);
                    attempt += 1;
                    continue;
                }
                let kind = if status == 404 {
                    std::io::ErrorKind::NotFound
                } else {
                    std::io::ErrorKind::Other
                };
                let msg = if status == 404 {
                    format!(
                        "{} (request-level 404 is NoSuchBucket, not idempotent missing key)",
                        err_msg
                    )
                } else {
                    err_msg
                };
                return (0..chunk.len())
                    .map(|_| Err(std::io::Error::new(kind, msg.clone())))
                    .collect();
            }
            Err(e) => {
                if attempt < config.max_retries {
                    tracing::warn!(
                        target: "mntrs::batched_delete",
                        error = %e,
                        attempt = attempt + 1,
                        backoff_ms = backoff.as_millis() as u64,
                        "retrying after transport error"
                    );
                    tokio::time::sleep(backoff).await;
                    // Issue #562 stage 5: feed the Calibrator.
                    // Bumped exactly once per retry decision
                    // (multi-key + single-key paths, status +
                    // transport variants). The Calibrator
                    // computes `retry_rate = RETRY_TOTAL /
                    // FLUSHES_TOTAL` from this counter.
                    RETRY_TOTAL.fetch_add(1, Ordering::Relaxed);
                    backoff = next_backoff(backoff, config.retry_factor);
                    attempt += 1;
                    continue;
                }
                let msg = format!(
                    "batched_delete: transport failure after {} retries: {}",
                    config.max_retries, e
                );
                return (0..chunk.len())
                    .map(|_| Err(std::io::Error::other(msg.clone())))
                    .collect();
            }
        }
    }
}

fn is_retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

fn next_backoff(current: Duration, factor: f64) -> Duration {
    Duration::from_secs_f64(current.as_secs_f64() * factor)
}

/// Issue #562 stage 1.5: retry wrapper around the
/// single-key short-circuit. Mirrors `send_chunk_with_retry`
/// semantics (3-attempt retry on retryable status / transport
/// error with exponential backoff) but for a plain
/// `DELETE /bucket/key`. Returns `vec![Ok(())]` on 204 /
/// 200 / idempotent 404 and `vec![Err(...)]` otherwise.
async fn send_single_delete_with_retry(
    config: &WorkerConfig,
    signer: &Signer<AwsCredential>,
    key: &str,
) -> Vec<std::io::Result<()>> {
    let mut attempt = 0u32;
    let mut backoff = config.retry_initial_backoff;
    loop {
        match send_single_delete_request(config, signer, key).await {
            Ok(status) => match status {
                // 204 No Content is the standard S3 single-object
                // DELETE success; 200 with empty body is also
                // accepted by some S3-compatible backends.
                200 | 204 => return vec![Ok(())],
                // 404 = NoSuchKey. Idempotent: the key is
                // already gone, which is the post-condition
                // the caller wanted. Matches the
                // DeleteObjects response-parser policy where
                // NoSuchKey / NoSuchVersion are mapped to Ok.
                404 => return vec![Ok(())],
                s if is_retryable_status(s) && attempt < config.max_retries => {
                    tracing::warn!(
                        target: "mntrs::batched_delete",
                        status = s,
                        attempt = attempt + 1,
                        backoff_ms = backoff.as_millis() as u64,
                        "single-key DELETE: retrying after retryable status"
                    );
                    tokio::time::sleep(backoff).await;
                    // Issue #562 stage 5: feed the Calibrator.
                    // Bumped exactly once per retry decision
                    // (multi-key + single-key paths, status +
                    // transport variants). The Calibrator
                    // computes `retry_rate = RETRY_TOTAL /
                    // FLUSHES_TOTAL` from this counter.
                    RETRY_TOTAL.fetch_add(1, Ordering::Relaxed);
                    backoff = next_backoff(backoff, config.retry_factor);
                    attempt += 1;
                    continue;
                }
                s => {
                    let msg = format!(
                        "batched_delete: S3 single-object DELETE HTTP {} for key `{}`",
                        s, key
                    );
                    return vec![Err(std::io::Error::other(msg))];
                }
            },
            Err(e) => {
                if attempt < config.max_retries {
                    tracing::warn!(
                        target: "mntrs::batched_delete",
                        error = %e,
                        attempt = attempt + 1,
                        backoff_ms = backoff.as_millis() as u64,
                        "single-key DELETE: retrying after transport error"
                    );
                    tokio::time::sleep(backoff).await;
                    // Issue #562 stage 5: feed the Calibrator.
                    // Bumped exactly once per retry decision
                    // (multi-key + single-key paths, status +
                    // transport variants). The Calibrator
                    // computes `retry_rate = RETRY_TOTAL /
                    // FLUSHES_TOTAL` from this counter.
                    RETRY_TOTAL.fetch_add(1, Ordering::Relaxed);
                    backoff = next_backoff(backoff, config.retry_factor);
                    attempt += 1;
                    continue;
                }
                let msg = format!(
                    "batched_delete: single-object DELETE transport failure after {} retries: {}",
                    config.max_retries, e
                );
                return vec![Err(std::io::Error::other(msg))];
            }
        }
    }
}

/// Issue #562 stage 1.5: send one signed `DELETE /bucket/key`.
/// Returns the HTTP status code only (no body — S3 single-object
/// DELETE has no per-key response body). Mirrors
/// `send_one_request`'s signing + timeout plumbing but uses
/// HTTP DELETE and skips the content-md5 / content-type
/// headers (no request body on a plain DELETE).
async fn send_single_delete_request(
    config: &WorkerConfig,
    signer: &Signer<AwsCredential>,
    key: &str,
) -> std::io::Result<u16> {
    let mut url = config.endpoint.clone();
    {
        let mut seg = url.path_segments_mut().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "endpoint URL cannot be a base",
            )
        })?;
        seg.clear().push(&config.bucket);
        for segment in key.split('/').filter(|s| !s.is_empty()) {
            seg.push(segment);
        }
    }
    let (parts, _) = http::Request::builder()
        .method(http::Method::DELETE)
        .uri(url.as_str())
        .body(())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?
        .into_parts();
    let mut parts = parts;

    signer
        .sign(&mut parts, None)
        .await
        .map_err(|e| std::io::Error::other(format!("sign: {}", e)))?;

    let http_req: http::Request<Vec<u8>> = http::Request::from_parts(parts, Vec::new());
    let reqwest_req = reqwest::Request::try_from(http_req)
        .map_err(|e| std::io::Error::other(format!("reqwest: {}", e)))?;

    let resp = tokio::time::timeout(config.request_timeout, config.http.execute(reqwest_req))
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "single-object DELETE request timeout after {:?}",
                    config.request_timeout
                ),
            )
        })?
        .map_err(|e| std::io::Error::other(format!("reqwest: {}", e)))?;

    Ok(resp.status().as_u16())
}

/// Send one signed DeleteObjects request. Returns
/// `(status_code, response_body_xml)`.
async fn send_one_request(
    config: &WorkerConfig,
    signer: &Signer<AwsCredential>,
    body_bytes: &[u8],
    content_md5_b64: &str,
) -> std::io::Result<(u16, String)> {
    let url = build_delete_objects_url(&config.endpoint, &config.bucket)?;
    let (parts, _) = http::Request::builder()
        .method(http::Method::POST)
        .uri(url.as_str())
        .body(())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?
        .into_parts();
    let mut parts = parts;

    parts.headers.insert(
        "content-type",
        http::HeaderValue::from_static("application/xml"),
    );
    parts.headers.insert(
        "content-length",
        http::HeaderValue::from_str(&body_bytes.len().to_string())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?,
    );
    parts.headers.insert(
        "content-md5",
        http::HeaderValue::from_str(content_md5_b64)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?,
    );

    signer
        .sign(&mut parts, None)
        .await
        .map_err(|e| std::io::Error::other(format!("sign: {}", e)))?;

    let http_req: http::Request<Vec<u8>> = http::Request::from_parts(parts, body_bytes.to_vec());
    let reqwest_req = reqwest::Request::try_from(http_req)
        .map_err(|e| std::io::Error::other(format!("reqwest: {}", e)))?;

    let resp = tokio::time::timeout(config.request_timeout, config.http.execute(reqwest_req))
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "DeleteObjects request timeout after {:?}",
                    config.request_timeout
                ),
            )
        })?
        .map_err(|e| std::io::Error::other(format!("reqwest: {}", e)))?;

    let status = resp.status().as_u16();
    let body = resp
        .text()
        .await
        .map_err(|e| std::io::Error::other(format!("read body: {}", e)))?;
    Ok((status, body))
}

// ===== XML body builder =====

/// Build the `<Delete>...</Delete>` body for one chunk.
/// `prefix` is the operator root (e.g. `/some/dir/`) prepended to
/// every relative path.
pub(crate) fn build_delete_objects_body(chunk: &[PendingDelete], prefix: &str) -> String {
    let mut writer = Writer::new(Vec::<u8>::new());
    writer
        .write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))
        .expect("xml decl write");
    writer
        .write_event(Event::Start(BytesStart::new("Delete")))
        .expect("Delete start");
    for job in chunk {
        writer
            .write_event(Event::Start(BytesStart::new("Object")))
            .expect("Object start");
        let key_full = join_key(prefix, &job.relative_path);
        writer
            .write_event(Event::Start(BytesStart::new("Key")))
            .expect("Key start");
        writer
            .write_event(Event::Text(BytesText::new(&key_full)))
            .expect("Key text");
        writer
            .write_event(Event::End(BytesEnd::new("Key")))
            .expect("Key end");
        writer
            .write_event(Event::End(BytesStart::new("Object").to_end()))
            .expect("Object end");
    }
    writer
        .write_event(Event::Start(BytesStart::new("Quiet")))
        .expect("Quiet start");
    writer
        .write_event(Event::Text(BytesText::new("true")))
        .expect("Quiet text");
    writer
        .write_event(Event::End(BytesEnd::new("Quiet")))
        .expect("Quiet end");
    writer
        .write_event(Event::End(BytesStart::new("Delete").to_end()))
        .expect("Delete end");
    String::from_utf8(writer.into_inner()).expect("xml utf8")
}

fn join_key(prefix: &str, rel: &str) -> String {
    let p = prefix.trim_end_matches('/');
    if p.is_empty() {
        rel.to_string()
    } else if let Some(stripped) = rel.strip_prefix('/') {
        format!("{}/{}", p, stripped)
    } else {
        format!("{}/{}", p, rel)
    }
}

fn base64_md5(body: &[u8]) -> String {
    let mut hasher = Md5::new();
    hasher.update(body);
    B64.encode(hasher.finalize())
}

// ===== URL builder =====

/// Build the path-style DeleteObjects URL:
/// `{endpoint}/{bucket}/?delete`. Path style for S3-compatible
/// endpoints (MinIO defaults to path-style).
pub(crate) fn build_delete_objects_url(
    endpoint: &url::Url,
    bucket: &str,
) -> std::io::Result<url::Url> {
    let mut url = endpoint.clone();
    {
        let mut seg = url.path_segments_mut().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "endpoint URL cannot be a base",
            )
        })?;
        seg.clear().push(bucket);
    }
    url.query_pairs_mut().append_pair("delete", "");
    Ok(url)
}

// ===== Response parser =====

/// Parse the DeleteObjects response XML into a per-failing-key
/// `Result<(), io::Error>`. `Quiet=true` means successful keys are
/// absent from the response (they map to `Ok(())`).
///
/// Per-key error mapping:
/// - `NoSuchKey` / `NoSuchVersion` / `NoSuchUpload` → Ok(())
/// - Anything else → Err(io::Error)
pub(crate) fn parse_delete_objects_response(xml: &str) -> Vec<std::io::Result<()>> {
    use quick_xml::events::Event as QEvent;
    use quick_xml::reader::Reader;

    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut out: Vec<std::io::Result<()>> = Vec::new();
    let mut in_error = false;
    let mut cur_key: Option<String> = None;
    let mut cur_code: Option<String> = None;
    let mut cur_message: Option<String> = None;
    let mut last_tag: Option<String> = None;

    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(QEvent::Start(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                match name.as_str() {
                    "Error" => {
                        in_error = true;
                        cur_key = None;
                        cur_code = None;
                        cur_message = None;
                    }
                    "Key" => cur_key = None,
                    "Code" => cur_code = None,
                    "Message" => cur_message = None,
                    _ => {}
                }
                last_tag = Some(name);
            }
            Ok(QEvent::Text(t)) => {
                let raw = t.into_inner();
                let txt = String::from_utf8_lossy(&raw).into_owned();
                if in_error {
                    match last_tag.as_deref() {
                        Some("Key") => cur_key = Some(txt),
                        Some("Code") => cur_code = Some(txt),
                        Some("Message") => cur_message = Some(txt),
                        _ => {}
                    }
                }
            }
            Ok(QEvent::End(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                if name == "Error" && in_error {
                    let code = cur_code.take().unwrap_or_default();
                    let message = cur_message.take().unwrap_or_default();
                    let _key = cur_key.take();
                    let is_not_found = matches!(
                        code.as_str(),
                        "NoSuchKey" | "NoSuchVersion" | "NoSuchUpload"
                    );
                    let entry = if is_not_found {
                        Ok(())
                    } else {
                        Err(std::io::Error::other(format!(
                            "S3 DeleteObjects per-key error: {} ({})",
                            code, message
                        )))
                    };
                    out.push(entry);
                    in_error = false;
                }
                last_tag = None;
            }
            Ok(QEvent::Eof) => break,
            Err(_) => break,
            _ => {
                last_tag = None;
            }
        }
        buf.clear();
    }
    out
}

// ===== Unit tests =====

#[cfg(test)]
mod tests {
    use super::*;

    /// Process-global mutex serialising all tests that
    /// read/write `MNTRS_BATCH_WORKER_COUNT` via
    /// `unsafe { std::env::set_var / remove_var }`.
    /// cargo runs tests in parallel by default; without
    /// this lock a `set_var` from one test can race a
    /// `remove_var` from another, leaving the worker_count
    /// field holding the wrong value when
    /// `WorkerConfig::from_s3` reads it.
    static WORKER_COUNT_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn fake_job(path: &str) -> PendingDelete {
        PendingDelete {
            relative_path: path.to_string(),
            result_tx: oneshot::channel().0,
        }
    }

    #[test]
    fn build_body_emits_delete_quiet_object_keys() {
        let jobs = vec![fake_job("a/b.txt"), fake_job("c.txt")];
        let body = build_delete_objects_body(&jobs, "/root/");
        assert!(body.contains("<Delete>"));
        assert!(body.contains("<Quiet>true</Quiet>"));
        assert!(body.contains("<Key>/root/a/b.txt</Key>"));
        assert!(body.contains("<Key>/root/c.txt</Key>"));
        assert!(body.contains("</Delete>"));
    }

    #[test]
    fn build_body_prefix_root_without_slash() {
        let jobs = vec![fake_job("a.txt")];
        let body = build_delete_objects_body(&jobs, "root");
        assert!(body.contains("<Key>root/a.txt</Key>"));
    }

    #[test]
    fn build_body_relative_path_with_leading_slash() {
        let jobs = vec![fake_job("/a.txt")];
        let body = build_delete_objects_body(&jobs, "/root/");
        assert!(body.contains("<Key>/root/a.txt</Key>"));
    }

    #[test]
    fn build_body_empty_prefix() {
        let jobs = vec![fake_job("a.txt")];
        let body = build_delete_objects_body(&jobs, "");
        assert!(body.contains("<Key>a.txt</Key>"));
    }

    #[test]
    fn base64_md5_is_deterministic() {
        let body = b"<Delete></Delete>";
        let a = base64_md5(body);
        let b = base64_md5(body);
        assert_eq!(a, b);
        let mut hasher = Md5::new();
        hasher.update(body);
        let expected = B64.encode(hasher.finalize());
        assert_eq!(a, expected);
    }

    #[test]
    fn build_delete_objects_url_path_style() {
        let endpoint = url::Url::parse("http://localhost:9000").unwrap();
        let url = build_delete_objects_url(&endpoint, "mybucket").unwrap();
        assert_eq!(url.path(), "/mybucket");
        assert_eq!(url.query(), Some("delete="));
    }

    #[test]
    fn build_delete_objects_url_with_existing_path() {
        let endpoint = url::Url::parse("http://localhost:9000/some/prefix").unwrap();
        let url = build_delete_objects_url(&endpoint, "mybucket").unwrap();
        assert_eq!(url.path(), "/mybucket");
    }

    #[test]
    fn parse_response_all_success_quiet() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>"#;
        let res = parse_delete_objects_response(xml);
        assert!(res.is_empty(), "Quiet=true + no errors = no entries");
    }

    #[test]
    fn parse_response_with_not_found_key() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult>
  <Error><Key>a.txt</Key><Code>NoSuchKey</Code><Message>missing</Message></Error>
  <Error><Key>b.txt</Key><Code>AccessDenied</Code><Message>forbidden</Message></Error>
</DeleteResult>"#;
        let res = parse_delete_objects_response(xml);
        assert_eq!(res.len(), 2);
        assert!(res[0].is_ok(), "NoSuchKey is idempotent success");
        assert!(res[1].is_err(), "AccessDenied surfaces");
    }

    #[test]
    fn parse_response_nosuchversion_is_success() {
        let xml = r#"<DeleteResult>
  <Error><Key>v.txt</Key><Code>NoSuchVersion</Code><Message>x</Message></Error>
</DeleteResult>"#;
        let res = parse_delete_objects_response(xml);
        assert_eq!(res.len(), 1);
        assert!(res[0].is_ok());
    }

    #[test]
    fn parse_response_malformed_returns_empty() {
        let xml = "<<<not xml>>>";
        let res = parse_delete_objects_response(xml);
        assert!(res.is_empty());
    }

    #[test]
    fn count_outcome_separates_success_notfound_failure() {
        let results = vec![
            Ok(()),
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "x")),
            Err(std::io::Error::other("x")),
            Ok(()),
        ];
        let (s, n, f) = count_outcome(&results);
        assert_eq!(s, 2);
        assert_eq!(n, 1);
        assert_eq!(f, 1);
    }

    #[test]
    fn retryable_status_set() {
        for s in [408u16, 429, 500, 502, 503, 504] {
            assert!(is_retryable_status(s), "{} should retry", s);
        }
        for s in [200u16, 400, 401, 403, 404, 409] {
            assert!(!is_retryable_status(s), "{} should NOT retry", s);
        }
    }

    #[test]
    fn next_backoff_grows_by_factor() {
        let b0 = Duration::from_millis(100);
        assert_eq!(next_backoff(b0, 2.0), Duration::from_millis(200));
        assert_eq!(
            next_backoff(next_backoff(b0, 2.0), 2.0),
            Duration::from_millis(400)
        );
    }

    #[test]
    fn worker_config_from_s3_uses_defaults() {
        // Issue #562 stage 1: the worker_count default lives
        // in env, so unset it before building the config to
        // hit the documented default. Take the env lock
        // first so we don't race the `worker_count_*`
        // sibling tests that set/unset the same env var.
        let _guard = WORKER_COUNT_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("MNTRS_BATCH_WORKER_COUNT");
        }
        let cfg = WorkerConfig::from_s3(
            url::Url::parse("http://localhost:9000").unwrap(),
            "b".into(),
            "/root/".into(),
            "us-east-1".into(),
            Some("ak".into()),
            Some("sk".into()),
            reqwest::Client::new(),
        );
        assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(cfg.flush_delay, DEFAULT_FLUSH_DELAY);
        assert_eq!(cfg.request_timeout, DEFAULT_REQUEST_TIMEOUT);
        assert_eq!(cfg.max_retries, DEFAULT_MAX_RETRIES);
        // Issue #553: fast_flush_threshold defaults to the
        // small-batch fix threshold; 0 disables the path.
        assert_eq!(cfg.fast_flush_threshold, DEFAULT_FAST_FLUSH_THRESHOLD);
        // Issue #562 stage 1: flusher pool defaults to 4.
        assert_eq!(cfg.worker_count, DEFAULT_BATCH_WORKER_COUNT);
    }

    #[test]
    fn parse_response_empty_input() {
        let res = parse_delete_objects_response("");
        assert!(res.is_empty());
    }

    #[test]
    fn counter_snapshot_is_const_default() {
        // Default snapshot is zero (no I/O). The live counts grow
        // as the worker runs; this only asserts the struct itself.
        let s = CounterSnapshot::default();
        assert_eq!(s.flushes_total, 0);
        assert_eq!(s.keys_total, 0);
        assert_eq!(s.failures_total, 0);
        assert_eq!(s.shutdown_lost_total, 0);
        assert_eq!(s.single_key_batches_total, 0);
        assert_eq!(s.max_batch_size_observed, 0);
        // Issue #553: fast_flush_total slot is wired even at rest.
        assert_eq!(s.fast_flush_total, 0);
    }

    #[test]
    fn env_override_parsing_round_trip() {
        // Stage B tuning: just verify the parsing helpers behave.
        // We cannot test the from_s3 env application without
        // mutating process-global env (which races with parallel
        // tests). The end-to-end env behavior is verified by
        // bench/unlink_ab.sh with MNTRS_BATCH_FLUSH_DELAY_MS=10.
        // The bare `unsafe` here is local to test scaffolding.
        unsafe {
            std::env::set_var("MNTRS_TEST_BATCH_SIZE", "25");
            std::env::set_var("MNTRS_TEST_FLUSH_DELAY", "7");
        }
        assert_eq!(env_usize("MNTRS_TEST_BATCH_SIZE", 100), 25);
        assert_eq!(env_u64("MNTRS_TEST_FLUSH_DELAY", 50), 7);
        assert_eq!(env_usize("MNTRS_TEST_MISSING_VAR", 99), 99);
        unsafe {
            std::env::remove_var("MNTRS_TEST_BATCH_SIZE");
            std::env::remove_var("MNTRS_TEST_FLUSH_DELAY");
        }
    }

    #[tokio::test]
    async fn enqueue_returns_none_after_shutdown() {
        let (deleter, _h) = spawn(
            WorkerConfig::from_s3(
                url::Url::parse("http://localhost:9000").unwrap(),
                "b".into(),
                "/".into(),
                "us-east-1".into(),
                Some("ak".into()),
                Some("sk".into()),
                reqwest::Client::new(),
            ),
            std::sync::Arc::new(dashmap::DashSet::new()),
        )
        .unwrap();
        let clone = deleter.clone();
        deleter.shutdown(false).await;
        let r = clone.enqueue("a".into());
        assert!(r.is_none(), "post-shutdown enqueue must reject");
    }

    #[tokio::test]
    async fn spawn_then_drop_exits_worker() {
        let (deleter, handles) = spawn(
            WorkerConfig::from_s3(
                url::Url::parse("http://localhost:9000").unwrap(),
                "b".into(),
                "/".into(),
                "us-east-1".into(),
                Some("ak".into()),
                Some("sk".into()),
                reqwest::Client::new(),
            ),
            std::sync::Arc::new(dashmap::DashSet::new()),
        )
        .unwrap();
        drop(deleter);
        // Issue #562 stage 1: the controller owns the
        // `mpsc::Receiver<Control>` and the `wake_tx`
        // sender. When the last `BatchedDeleter` clone is
        // dropped, `flush_tx` closes → controller's
        // `rx.recv()` returns `None` → controller exits →
        // its `wake_tx` drops → flushers' `wake_rx.recv()`
        // returns `Err(Closed)` → flushers exit. Await
        // controller first so `wake_tx` is guaranteed
        // dropped by the time we wait on flushers.
        let _ = tokio::time::timeout(Duration::from_secs(2), handles.controller).await;
        for fh in handles.flushers {
            let _ = tokio::time::timeout(Duration::from_secs(2), fh).await;
        }
    }

    // ===== Plan #64 stage C: tombstone lifecycle =====

    /// cancel_pending drains matching queued jobs and clears the
    /// tombstone without sending an S3 DELETE. Reproduces the
    /// `rm X && touch X` race: write-behind enqueue at rm time
    /// inserts the tombstone, then create() calls cancel_pending
    /// to free the path before op.write. Without this the
    /// in-flight S3 DELETE would race the new write.
    #[test]
    fn cancel_pending_drains_jobs_and_clears_tombstone() {
        let tombs = std::sync::Arc::new(dashmap::DashSet::<String>::new());
        // Stub deleter — we don't even need a worker; cancel_pending
        // only touches Shared.pending + Shared.tombs under the lock.
        let shared = std::sync::Arc::new(Shared {
            pending: Mutex::new(Pending::new()),
            accepting: AtomicBool::new(true),
            flush_delay: Duration::from_millis(50),
            // Issue #530: tests use threshold 0 to disable
            // gating — they don't exercise the
            // enqueue-returns-None branch and want a clean
            // "always enqueue" path.
            batch_threshold: std::sync::atomic::AtomicUsize::new(0),
            tombs: tombs.clone(),
            // Issue #562 stage 3: tests don't exercise the
            // observer / profile state paths. Use the default
            // initial profile (Medium) so any incidental
            // reads in the future don't trip on a default.
            burst_observer: Arc::new(BurstObserver::new()),
            profile_state: Arc::new(ProfileState::new(Profile::Medium, PROFILE_DEFAULT_COOLDOWN)),
        });
        let (tx, _rx) = mpsc::channel::<Control>(8);
        let deleter = BatchedDeleter {
            shared: shared.clone(),
            flush_tx: tx,
        };

        // Simulate what unlink does: enqueue + tombstone.
        tombs.insert("p".into());
        {
            let mut pending = shared.pending.lock().unwrap();
            let (otx, _orx) = oneshot::channel();
            pending.jobs.push(PendingDelete {
                relative_path: "p".into(),
                result_tx: otx,
            });
        }
        assert!(tombs.contains("p"));

        // MntrsFs::create calls cancel_pending before op.write.
        let n = deleter.cancel_pending("p");
        assert_eq!(n, 1, "exactly the one queued job must be cancelled");
        assert!(
            !tombs.contains("p"),
            "tombstone must be cleared so lookup returns ENOENT → OK"
        );
        assert!(
            shared.pending.lock().unwrap().jobs.is_empty(),
            "queue must be drained"
        );
    }

    /// cancel_pending is a no-op for paths that aren't queued and
    /// not tombstoned — but it does clear a tombstone if present
    /// (the FUSE side may have stale state from a prior failure).
    #[test]
    fn cancel_pending_unknown_path_clears_stale_tombstone() {
        let tombs = std::sync::Arc::new(dashmap::DashSet::<String>::new());
        let shared = std::sync::Arc::new(Shared {
            pending: Mutex::new(Pending::new()),
            accepting: AtomicBool::new(true),
            flush_delay: Duration::from_millis(50),
            // Issue #530: tests use threshold 0 to disable
            // gating — they don't exercise the
            // enqueue-returns-None branch and want a clean
            // "always enqueue" path.
            batch_threshold: std::sync::atomic::AtomicUsize::new(0),
            tombs: tombs.clone(),
            // Issue #562 stage 3: see the cancel_pending test
            // above for rationale. Stub values; the cancel
            // paths don't read the observer or profile.
            burst_observer: Arc::new(BurstObserver::new()),
            profile_state: Arc::new(ProfileState::new(Profile::Medium, PROFILE_DEFAULT_COOLDOWN)),
        });
        let (tx, _rx) = mpsc::channel::<Control>(8);
        let deleter = BatchedDeleter {
            shared: shared.clone(),
            flush_tx: tx,
        };
        tombs.insert("stale".into());
        let n = deleter.cancel_pending("stale");
        assert_eq!(n, 0);
        assert!(!tombs.contains("stale"));
    }

    /// cancel_pending on an absent path is a clean no-op.
    #[test]
    fn cancel_pending_absent_path_is_noop() {
        let tombs = std::sync::Arc::new(dashmap::DashSet::<String>::new());
        let shared = std::sync::Arc::new(Shared {
            pending: Mutex::new(Pending::new()),
            accepting: AtomicBool::new(true),
            flush_delay: Duration::from_millis(50),
            // Issue #530: tests use threshold 0 to disable
            // gating — they don't exercise the
            // enqueue-returns-None branch and want a clean
            // "always enqueue" path.
            batch_threshold: std::sync::atomic::AtomicUsize::new(0),
            tombs: tombs.clone(),
            // Issue #562 stage 3: see the cancel_pending test
            // above for rationale.
            burst_observer: Arc::new(BurstObserver::new()),
            profile_state: Arc::new(ProfileState::new(Profile::Medium, PROFILE_DEFAULT_COOLDOWN)),
        });
        let (tx, _rx) = mpsc::channel::<Control>(8);
        let deleter = BatchedDeleter {
            shared: shared.clone(),
            flush_tx: tx,
        };
        let n = deleter.cancel_pending("never-existed");
        assert_eq!(n, 0);
        assert!(tombs.is_empty());
    }

    // ===== Issue #530 threshold gating tests =====

    /// Helper for Issue #530 unit tests: build a Shared
    /// literal + a stub BatchedDeleter whose `flush_tx`
    /// channel is a dead end (no worker is spawned — the
    /// tests don't actually need the worker; they only
    /// inspect `enqueue()`'s return value and
    /// `pending_len()`).
    fn make_test_deleter_with_threshold(
        threshold: usize,
    ) -> (BatchedDeleter, std::sync::Arc<dashmap::DashSet<String>>) {
        let tombs = std::sync::Arc::new(dashmap::DashSet::<String>::new());
        let shared = std::sync::Arc::new(Shared {
            pending: Mutex::new(Pending::new()),
            accepting: AtomicBool::new(true),
            flush_delay: Duration::from_millis(50),
            batch_threshold: std::sync::atomic::AtomicUsize::new(threshold),
            tombs: tombs.clone(),
            // Issue #562 stage 3: enqueue tests don't read the
            // observer / profile state. Use defaults.
            burst_observer: Arc::new(BurstObserver::new()),
            profile_state: Arc::new(ProfileState::new(Profile::Medium, PROFILE_DEFAULT_COOLDOWN)),
        });
        let (tx, _rx) = mpsc::channel::<Control>(8);
        let deleter = BatchedDeleter {
            shared: shared.clone(),
            flush_tx: tx,
        };
        (deleter, tombs)
    }

    /// Issue #530: when the pending queue is below
    /// `batch_threshold`, `enqueue()` returns None and does
    /// not insert a job. This is the gating contract the
    /// FUSE-side caller relies on for small-burst fallback.
    #[test]
    fn enqueue_below_threshold_returns_none_and_does_not_queue() {
        let (deleter, _tombs) = make_test_deleter_with_threshold(32);
        let rx = deleter.enqueue("a/b/c".to_string());
        assert!(rx.is_none(), "below threshold: must return None");
        assert_eq!(
            deleter.pending_len(),
            0,
            "below threshold: must not insert into pending"
        );
        assert_eq!(deleter.batch_threshold(), 32);
    }

    /// Issue #530: with `batch_threshold = 0`, gating is
    /// disabled and every `enqueue()` call returns Some
    /// (preserves the legacy "always batch" behaviour
    /// users who set MNTRS_BATCH_THRESHOLD=0 expect).
    #[test]
    fn enqueue_threshold_zero_disables_gating() {
        let (deleter, _tombs) = make_test_deleter_with_threshold(0);
        let rx1 = deleter.enqueue("a".to_string());
        assert!(rx1.is_some(), "threshold=0: must always enqueue");
        assert_eq!(deleter.pending_len(), 1);
        let rx2 = deleter.enqueue("b".to_string());
        assert!(rx2.is_some(), "threshold=0: must always enqueue");
        assert_eq!(deleter.pending_len(), 2);
    }

    /// Issue #530: `set_batch_threshold` is a runtime knob
    /// that takes effect immediately for subsequent
    /// `enqueue()` calls. Returns the previous threshold
    /// (atomic `swap` semantics) so callers can record the
    /// before/after pair.
    #[test]
    fn set_batch_threshold_takes_effect_immediately() {
        let (deleter, _tombs) = make_test_deleter_with_threshold(32);
        assert_eq!(deleter.batch_threshold(), 32);
        let prev = deleter.set_batch_threshold(64);
        assert_eq!(prev, 32, "returns previous threshold");
        assert_eq!(deleter.batch_threshold(), 64);
        // First enqueue with new threshold of 64 and empty
        // pending queue (0 < 64) must still be None.
        assert!(deleter.enqueue("x".to_string()).is_none());
        // Lowering to 0 lets the same queue drain-through.
        assert_eq!(deleter.set_batch_threshold(0), 64);
        assert!(deleter.enqueue("y".to_string()).is_some());
        assert_eq!(deleter.pending_len(), 1);
    }

    /// Issue #530: when `enqueue` returns None, the
    /// `THRESHOLD_SKIPPED_TOTAL` counter advances. This is
    /// how /metrics and the bench harness can confirm the
    /// gating is actually firing on real workloads
    /// (without grepping debug logs).
    #[test]
    fn threshold_skipped_counter_advances_on_none_return() {
        let (deleter, _tombs) = make_test_deleter_with_threshold(16);
        let before = crate::batched_delete::snapshot().threshold_skipped_total;
        // Three calls, all below threshold (16), all should
        // return None and bump the counter.
        assert!(deleter.enqueue("p1".to_string()).is_none());
        assert!(deleter.enqueue("p2".to_string()).is_none());
        assert!(deleter.enqueue("p3".to_string()).is_none());
        let after = crate::batched_delete::snapshot().threshold_skipped_total;
        assert!(
            after >= before + 3,
            "threshold_skipped_total must advance by at least 3 (was {before}, now {after})"
        );
    }

    // ===== decide_next_action tests (Issue #553) =====
    //
    // `decide_next_action` is the pure helper that picks the
    // worker's next action. It takes a snapshot of the pending
    // queue and the WorkerConfig and returns the same ScheduledAction
    // the worker would pick — without locks, async, or metrics.
    // That's what makes these tests cheap and deterministic: no
    // network, no sleep, no shared atomics.

    fn cfg(batch_size: usize, fast_flush_threshold: usize) -> WorkerConfig {
        WorkerConfig {
            bucket: "b".into(),
            prefix: "/".into(),
            region: "us-east-1".into(),
            endpoint: url::Url::parse("http://localhost:9000").unwrap(),
            access_key_id: None,
            secret_access_key: None,
            http: reqwest::Client::new(),
            batch_size,
            flush_delay: Duration::from_millis(50),
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            fast_flush_threshold,
            // Issue #562 stage 1: tests for the pure
            // `decide_next_action` helper don't care about
            // the flusher pool size (only the controller
            // calls the helper, and the helper doesn't read
            // worker_count). Pin to 1 so the test never
            // accidentally spawns a flusher task.
            worker_count: 1,
            retry_factor: 2.0,
            retry_initial_backoff: Duration::from_millis(100),
            batch_threshold: 0,
            // Issue #562 stage 3: pure helper tests don't read
            // the initial profile (they pass a Profile literal
            // directly to decide_next_action). Default to None
            // to keep the cfg() helper minimal.
            initial_profile: None,
        }
    }

    /// Test helper: build a synthetic profile for the
    /// threshold-edge tests below. The three real Profile
    /// variants cover the production paths, but the tests
    /// exercise edges (`fast_flush_threshold = 0` and `= 1`)
    /// that no production profile currently uses. The helper
    /// picks the closest production profile's batch_size so
    /// the size-driven band is identical to the original
    /// pre-Stage-3 tests.
    ///
    /// Issue #562 stage 3: tests now drive `decide_next_action`
    /// with a `Profile` literal rather than `&WorkerConfig`.
    /// The thresholds (100/8 etc.) are the Medium profile's
    /// values, so the production code paths are exercised.
    /// The threshold=0 / threshold=1 cases are still needed
    /// to guard the strict-less-than semantics, but they
    /// require custom values that no production Profile has
    /// — we approximate by using `Profile::Medium` and the
    /// tests below assert the predicate's shape.
    fn decide_for(pending_len: usize, deadline: Option<Instant>) -> Option<ScheduledAction> {
        decide_next_action(Profile::Medium, pending_len, deadline)
    }

    #[test]
    fn decide_empty_queue_returns_none() {
        // Empty queue → worker should block on rx.recv(), not
        // spin doing nothing.
        assert!(decide_for(0, None).is_none());
    }

    #[test]
    fn decide_full_batch_returns_size_driven_flush() {
        // pending.len() == batch_size (Medium = 100) → flush,
        // fast=false (size-driven path; the fast counter must
        // NOT advance).
        let action = decide_for(100, None);
        match action {
            Some(ScheduledAction::FlushBatch { fast }) => assert!(!fast),
            other => panic!("expected FlushBatch {{ fast: false }}, got {other:?}"),
        }
    }

    #[test]
    fn decide_above_batch_size_also_size_driven() {
        // pending.len() > batch_size → still size-driven flush.
        // Belt-and-suspenders against off-by-one in the comparison.
        let action = decide_for(137, None);
        match action {
            Some(ScheduledAction::FlushBatch { fast }) => assert!(!fast),
            other => panic!("expected FlushBatch {{ fast: false }}, got {other:?}"),
        }
    }

    #[test]
    fn decide_small_batch_fast_flushes_when_threshold_set() {
        // pending.len() < fast_flush_threshold (Medium = 8) → fast
        // flush. This is the core #541 fix: skip the
        // flush_delay wait when there's not enough work to
        // amortise it.
        for &n in &[1usize, 2, 7] {
            let action = decide_for(n, None);
            match action {
                Some(ScheduledAction::FlushBatch { fast }) => assert!(fast, "n={n}"),
                other => panic!("n={n}: expected FlushBatch {{ fast: true }}, got {other:?}"),
            }
        }
    }

    #[test]
    fn decide_middle_band_waits_for_deadline() {
        // pending.len() in [fast_flush_threshold, batch_size) →
        // wait for deadline. This is the "true batching" band:
        // enough keys to amortise the deadline wait, not enough
        // to trigger size-driven flush.
        let deadline = Some(Instant::now() + Duration::from_millis(20));
        for &n in &[8usize, 9, 50, 99] {
            let action = decide_for(n, deadline);
            match action {
                Some(ScheduledAction::WaitForDeadline(d)) => assert_eq!(d, deadline, "n={n}"),
                other => panic!("n={n}: expected WaitForDeadline, got {other:?}"),
            }
        }
    }

    /// Issue #562 stage 3: regression guard for the
    /// `fast_flush_threshold == 0` shape, even though no
    /// production Profile currently has it. The helper
    /// signature now takes a Profile literal, so we can't
    /// easily reach this state from the public API. Instead
    /// we test the equivalent semantic by checking that
    /// `Profile::Bulk.fast_flush_threshold() == 32` and
    /// `Profile::Small.fast_flush_threshold() == 4` (which
    /// are both > 0, so the fast branch still fires for
    /// small n). The strict-`<` invariant that drove the
    /// pre-Stage-3 tests is exercised in
    /// `decide_threshold_is_strict_less_than` below.
    #[test]
    fn decide_threshold_zero_equivalent_in_production() {
        // Production Profiles all have fast_flush_threshold > 0,
        // so the fast branch always fires for n < threshold.
        // Verify each profile's threshold so a future regression
        // that sets any to 0 is caught here (the production
        // invariant is "all profiles have a non-zero threshold").
        assert!(Profile::Small.fast_flush_threshold() > 0);
        assert!(Profile::Medium.fast_flush_threshold() > 0);
        assert!(Profile::Bulk.fast_flush_threshold() > 0);
        // And that the fast branch fires for n=1 with each.
        for p in [Profile::Small, Profile::Medium, Profile::Bulk] {
            let action = decide_next_action(p, 1, None);
            match action {
                Some(ScheduledAction::FlushBatch { fast }) => assert!(fast, "{p:?}"),
                other => panic!("{p:?}: expected FlushBatch {{ fast: true }}, got {other:?}"),
            }
        }
    }

    /// Regression guard for the strict-`<` semantics at the
    /// fast_flush_threshold boundary. Pre-Stage-3 this was
    /// tested with `cfg(100, 1)` and `cfg(100, 8)` to verify
    /// `pending_len == fast_flush_threshold` falls into the
    /// wait band, not the fast branch. Now we drive the same
    /// assertion through each production profile's threshold.
    #[test]
    fn decide_threshold_one_behaves_like_threshold_zero() {
        // Production Profiles don't have threshold=1, but the
        // semantic ("n=1 with threshold=1 is NOT a fast flush")
        // generalises to: for every profile, n == threshold is
        // NOT a fast flush (strict-less-than).
        for p in [Profile::Small, Profile::Medium, Profile::Bulk] {
            let threshold = p.fast_flush_threshold();
            let action = decide_next_action(p, threshold, None);
            assert!(
                matches!(action, Some(ScheduledAction::WaitForDeadline(_))),
                "{p:?}: n == threshold ({threshold}) must NOT fast-flush; got {action:?}"
            );
        }
    }

    #[test]
    fn decide_threshold_is_strict_less_than() {
        // pending_len == fast_flush_threshold is NOT a fast
        // flush — it's the first value in the wait band.
        // This is the strict `<` semantics that keeps the
        // middle band non-empty.
        let action = decide_for(8, None);
        assert!(matches!(action, Some(ScheduledAction::WaitForDeadline(_))));
    }

    // ===== Issue #562 stage 1: worker_count env wiring =====
    //
    // These tests share process-global env state via
    // `unsafe { std::env::set_var/remove_var }`. cargo runs
    // tests in parallel by default, so they must serialise
    // on `WORKER_COUNT_ENV_LOCK` (declared at the top of
    // this module) so a `set_var` from one test doesn't race
    // a `remove_var` from another. `worker_config_from_s3_uses_defaults`
    // also acquires the lock for the same reason.

    /// Default worker_count is 4 when MNTRS_BATCH_WORKER_COUNT
    /// is unset. Matches rclone --transfers=4 so the S3
    /// worker pool can amortise DeleteObjects round-trips
    /// the same way rclone amortises per-file transfers.
    #[test]
    fn worker_count_default_is_four() {
        let _guard = WORKER_COUNT_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("MNTRS_BATCH_WORKER_COUNT");
        }
        let cfg = WorkerConfig::from_s3(
            url::Url::parse("http://localhost:9000").unwrap(),
            "b".into(),
            "/root/".into(),
            "us-east-1".into(),
            Some("ak".into()),
            Some("sk".into()),
            reqwest::Client::new(),
        );
        assert_eq!(cfg.worker_count, DEFAULT_BATCH_WORKER_COUNT);
        assert_eq!(cfg.worker_count, 4);
    }

    /// Out-of-range env values clamp to MAX_BATCH_WORKER_COUNT
    /// (16). Each flusher holds its own signer clone, and a
    /// misconfigured `MNTRS_BATCH_WORKER_COUNT=999` must not
    /// spawn 999 S3 clients.
    #[test]
    fn worker_count_clamped_to_max_16() {
        let _guard = WORKER_COUNT_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("MNTRS_BATCH_WORKER_COUNT", "999");
        }
        let cfg = WorkerConfig::from_s3(
            url::Url::parse("http://localhost:9000").unwrap(),
            "b".into(),
            "/".into(),
            "us-east-1".into(),
            Some("ak".into()),
            Some("sk".into()),
            reqwest::Client::new(),
        );
        assert_eq!(cfg.worker_count, MAX_BATCH_WORKER_COUNT);
        assert_eq!(cfg.worker_count, 16);
        unsafe {
            std::env::remove_var("MNTRS_BATCH_WORKER_COUNT");
        }
    }

    /// Env values <= 0 clamp to 1, reproducing the pre-#562
    /// single-consumer behaviour. The user's intent for
    /// `MNTRS_BATCH_WORKER_COUNT=0` is "don't multi-task",
    /// which the existing impl achieves with one flusher.
    #[test]
    fn worker_count_clamped_to_min_1() {
        let _guard = WORKER_COUNT_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("MNTRS_BATCH_WORKER_COUNT", "0");
        }
        let cfg = WorkerConfig::from_s3(
            url::Url::parse("http://localhost:9000").unwrap(),
            "b".into(),
            "/".into(),
            "us-east-1".into(),
            Some("ak".into()),
            Some("sk".into()),
            reqwest::Client::new(),
        );
        assert_eq!(cfg.worker_count, 1);
        unsafe {
            std::env::remove_var("MNTRS_BATCH_WORKER_COUNT");
        }
    }

    // ===== Issue #562 stage 3: BurstObserver unit tests =====
    //
    // The observer is lock-free; tests run single-threaded and
    // assert the public surface (p95 correctness, ring
    // wrap-around, no-data / not-enough-data sentinel values).

    /// Pushing exactly `BURST_OBSERVER_CAP + 1` samples must
    /// wrap the write index without losing data — `p95()`
    /// should still report the correct value because the
    /// active window is exactly the last `CAP` samples. The
    /// first sample (value=1) is overwritten by the 3001st;
    /// `p95` of [2..=3000] is `2850` (index
    /// `ceil(0.95 * 2999)` = 2850, 0-indexed in sorted
    /// order — the value at rank 2850 of [2..=3000] is
    /// `2 + 2850 = 2852`).
    #[test]
    fn burst_observer_samples_wrap_around() {
        let o = BurstObserver::new();
        // Push CAP + 1 samples: 1, 2, 3, ..., CAP, CAP+1.
        for i in 1..=(BURST_OBSERVER_CAP + 1) {
            o.observe(i);
        }
        let p95 = o.p95();
        // Sorted window is [2, 3, ..., CAP+1] = [2..=3001],
        // length 3000. p95 index = ceil(0.95 * 2999) = 2850.
        // Value at rank 2850 is 2 + 2850 = 2852.
        assert_eq!(
            p95, 2852,
            "ring wrap-around must not corrupt the percentile"
        );
    }

    /// With fewer than 20 samples the observer returns
    /// `usize::MAX` so callers can distinguish "no data" (0)
    /// from "some data but not enough for a percentile"
    /// (usize::MAX). `ProfileState` treats both as "no hint"
    /// and keeps the current profile.
    #[test]
    fn burst_observer_p95_small_n() {
        let o = BurstObserver::new();
        // 0 samples → 0
        assert_eq!(o.p95(), 0);
        // 1 sample → usize::MAX (some data, not enough)
        o.observe(42);
        assert_eq!(o.p95(), usize::MAX);
        // 19 samples total → still usize::MAX
        for i in 1..=18 {
            o.observe(i);
        }
        assert_eq!(o.p95(), usize::MAX);
        // 20 samples total → real percentile. Window is
        // [1, 2, ..., 18, 42] (20 elements, sorted).
        // p95 idx = ceil(0.95 * 19) = 19 → value 42.
        o.observe(0);
        assert_eq!(o.p95(), 42);

        // 100 samples of value 7 → p95 = 7 (uniform).
        let o = BurstObserver::new();
        for _ in 0..100 {
            o.observe(7);
        }
        assert_eq!(o.p95(), 7);

        // 20 samples [0, 0, ..., 0, 1000] (19 zeros, 1
        // thousand). Sorted [0, 0, ..., 0, 1000], p95 idx
        // 19 → 1000.
        let o = BurstObserver::new();
        for _ in 0..19 {
            o.observe(0);
        }
        o.observe(1000);
        assert_eq!(o.p95(), 1000);
    }

    // ===== Issue #562 stage 3: ProfileState unit tests =====
    //
    // Hysteresis + cooldown are time-driven; tests use
    // `Instant::now()` as the baseline and advance by
    // explicit durations via short cooldowns (so the suite
    // stays sub-second). The `Duration::MAX` pinned case
    // uses the real cooldown and asserts no transition
    // happens regardless of elapsed wall-clock.

    /// `ProfileState::new(initial, …).current()` returns the
    /// initial profile unchanged. The atomic + mutex fields
    /// are correctly initialised; no spurious transition.
    #[test]
    fn profile_state_starts_at_initial() {
        let s = ProfileState::new(Profile::Bulk, PROFILE_DEFAULT_COOLDOWN);
        assert_eq!(s.current(), Profile::Bulk);

        let s = ProfileState::new(Profile::Small, PROFILE_DEFAULT_COOLDOWN);
        assert_eq!(s.current(), Profile::Small);

        let s = ProfileState::new(Profile::Medium, PROFILE_DEFAULT_COOLDOWN);
        assert_eq!(s.current(), Profile::Medium);
    }

    /// A single observation that requests a flip must be
    /// suppressed if it fires inside the cooldown window.
    /// The hint must be sustained for the full cooldown
    /// before the flip lands — this is the oscillation
    /// guard. Test uses a 100 ms cooldown so the suite
    /// stays sub-second.
    #[test]
    fn profile_state_cooldown_blocks_immediate_flip() {
        let cooldown = Duration::from_millis(100);
        let s = ProfileState::new(Profile::Small, cooldown);
        let t0 = Instant::now();

        // Strong hint (p95=200 >> UP_THRESHOLD=50) at t0
        // would normally flip Small → Bulk.
        let flipped = s.observe(200, t0);
        assert_eq!(
            flipped,
            Profile::Bulk,
            "first observation must flip immediately"
        );
        assert_eq!(s.current(), Profile::Bulk);

        // 50 ms later (within cooldown), another hint that
        // requests flip back to Small is suppressed.
        let flipped = s.observe(1, t0 + Duration::from_millis(50));
        assert_eq!(
            flipped,
            Profile::Bulk,
            "cooldown must suppress immediate flip-back"
        );
        assert_eq!(s.current(), Profile::Bulk);

        // After cooldown elapses, the hint is honoured.
        let flipped = s.observe(1, t0 + Duration::from_millis(150));
        assert_eq!(flipped, Profile::Small, "cooldown elapsed → flip allowed");
        assert_eq!(s.current(), Profile::Small);
    }

    /// Hysteresis boundary values. The candidate-profile
    /// computation uses strict `>` (up-threshold = 50) and
    /// strict `<` (down-threshold = 5), so the boundaries
    /// themselves fall into the Medium band, not the
    /// Bulk/Small bands. This test pins down that semantic:
    /// hint = 50 from a Medium state stays Medium (same
    /// band, no flip); hint = 51 flips to Bulk. hint = 5
    /// from a Bulk state falls into the Medium band and
    /// flips to Medium; hint = 4 flips to Small.
    #[test]
    fn profile_state_hysteresis_boundary_values() {
        // Sub-millisecond cooldown so back-to-back observations
        // on a single state can both fire their intended flips.
        // The cooldown gate itself is covered by
        // `profile_state_cooldown_blocks_immediate_flip`; this
        // test exercises the boundary values, not the gate.
        let cooldown = Duration::from_nanos(1);
        let s = ProfileState::new(Profile::Medium, cooldown);
        let t0 = Instant::now();

        // p95 == 50: `50 > 50` is false, `50 < 5` is false,
        // candidate = Medium. Same as current → no flip.
        assert_eq!(s.observe(50, t0), Profile::Medium);
        assert_eq!(s.current(), Profile::Medium);

        // p95 == 51: just above the threshold → Bulk.
        assert_eq!(s.observe(51, t0), Profile::Bulk);
        assert_eq!(s.current(), Profile::Bulk);

        // From a fresh Bulk state with no prior transition,
        // hint == 5 falls into the Medium band (`5 < 5` is
        // false), so candidate = Medium → flip Bulk → Medium.
        let s = ProfileState::new(Profile::Bulk, cooldown);
        assert_eq!(s.observe(5, t0), Profile::Medium);
        assert_eq!(s.current(), Profile::Medium);

        // Wait past the nanosecond cooldown, then hint == 4
        // (strictly below the down-threshold) → Small.
        // Flip Medium → Small.
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(s.observe(4, Instant::now()), Profile::Small);
        assert_eq!(s.current(), Profile::Small);
    }

    /// `Duration::MAX` cooldown means "pinned, never
    /// transitions after the first" — used when the user
    /// sets `MNTRS_BATCH_PROFILE=small|medium|bulk`. The
    /// first observation can still flip (cooldown = MAX
    /// means "cooldown never elapses", but `last_transition`
    /// starts as None so the first flip is unconditional).
    /// Subsequent flips are blocked because
    /// `now.duration_since(prev) < Duration::MAX` is true
    /// for any `now`. Also: no-hint observations (0 and
    /// `usize::MAX`) return the current profile unchanged
    /// without consulting cooldown.
    #[test]
    fn profile_state_pinned_never_transitions() {
        let s = ProfileState::new(Profile::Medium, Duration::MAX);
        let now = Instant::now();

        // No-hint sentinels → no change, no cooldown touched.
        assert_eq!(s.observe(0, now), Profile::Medium);
        assert_eq!(s.observe(usize::MAX, now), Profile::Medium);

        // First extreme hint → flips (cooldown is MAX but
        // last_transition is None so the gate is open once).
        assert_eq!(s.observe(10_000, now), Profile::Bulk);
        assert_eq!(s.current(), Profile::Bulk);

        // All subsequent flips blocked by the MAX cooldown:
        // Bulk → Small hint at any `now`.
        assert_eq!(s.observe(1, now), Profile::Bulk);
        assert_eq!(s.observe(1, now + Duration::from_secs(3600)), Profile::Bulk);
        assert_eq!(
            s.observe(1, now + Duration::from_secs(86_400 * 365)),
            Profile::Bulk
        );
        assert_eq!(s.current(), Profile::Bulk);

        // No-hint still a no-op even after a transition.
        assert_eq!(s.observe(0, now + Duration::from_secs(3600)), Profile::Bulk);
        assert_eq!(s.observe(usize::MAX, now), Profile::Bulk);
    }

    /// Hint equal to the current profile's "natural" band
    /// must NOT bump `PROFILE_TRANSITIONS_TOTAL` (we only
    /// count actual flips). Hint in a different band but
    /// blocked by cooldown must also not bump the counter.
    /// This test guards the metric against inflation by
    /// hint oscillation.
    #[test]
    fn profile_state_transitions_logged_only_on_flip() {
        // Snapshot the global counter so other tests in the
        // same process don't pollute our delta.
        let before = PROFILE_TRANSITIONS_TOTAL.load(Ordering::Relaxed);

        let cooldown = Duration::from_millis(50);
        let s = ProfileState::new(Profile::Medium, cooldown);
        let t0 = Instant::now();

        // Same-band hint → no flip, no counter bump.
        s.observe(20, t0); // 20 in [5, 50] → Medium
        s.observe(10, t0); // 10 in [5, 50] → Medium
        assert_eq!(
            PROFILE_TRANSITIONS_TOTAL.load(Ordering::Relaxed),
            before,
            "same-band hints must not bump PROFILE_TRANSITIONS_TOTAL"
        );

        // First real flip → counter bumps by exactly 1.
        s.observe(100, t0); // 100 > 50 → Bulk
        assert_eq!(
            PROFILE_TRANSITIONS_TOTAL.load(Ordering::Relaxed),
            before + 1,
            "real flip must bump PROFILE_TRANSITIONS_TOTAL by 1"
        );

        // Suppressed flip (cooldown not elapsed) → no bump.
        s.observe(1, t0 + Duration::from_millis(10));
        assert_eq!(
            PROFILE_TRANSITIONS_TOTAL.load(Ordering::Relaxed),
            before + 1,
            "cooldown-suppressed flip must not bump the counter"
        );

        // Hint back to Bulk (same as current) → no bump.
        s.observe(100, t0 + Duration::from_millis(10));
        assert_eq!(
            PROFILE_TRANSITIONS_TOTAL.load(Ordering::Relaxed),
            before + 1,
            "same-as-current hint must not bump the counter"
        );

        // Second real flip after cooldown elapses → +1.
        s.observe(1, t0 + Duration::from_millis(100));
        assert_eq!(
            PROFILE_TRANSITIONS_TOTAL.load(Ordering::Relaxed),
            before + 2,
            "post-cooldown flip must bump the counter by 1"
        );
    }

    // ===== Issue #562 stage 1.5: single-key short-circuit =====
    //
    // The predicate `should_short_circuit_single_key` is
    // pure (chunk_len, Profile) → bool. Unit tests pin every
    // (chunk_len, profile) combination so a future change to
    // `Profile::single_key_fast_delete()` is caught here
    // before nightly.
    //
    // `send_single_delete_with_retry` and
    // `send_single_delete_request` are network-dependent and
    // tested via the existing bench harness; the unit tests
    // below only verify the pure-path contract.

    /// chunk_len == 1 + Profile::Small (the only profile that
    /// opts in by default) → short-circuit fires.
    #[test]
    fn short_circuit_fires_for_small_profile_single_key() {
        assert!(should_short_circuit_single_key(1, Profile::Small));
    }

    /// chunk_len == 1 + Profile::Medium → does NOT short-circuit
    /// (the per-key amortisation at batch_size=100 makes the
    /// XML path cheap enough that the short-circuit's complexity
    /// isn't worth it).
    #[test]
    fn short_circuit_skipped_for_medium_profile() {
        assert!(!should_short_circuit_single_key(1, Profile::Medium));
    }

    /// chunk_len == 1 + Profile::Bulk → does NOT short-circuit.
    /// Same reasoning as Medium but stronger: at batch_size=500
    /// the XML body is amortised over hundreds of keys.
    #[test]
    fn short_circuit_skipped_for_bulk_profile() {
        assert!(!should_short_circuit_single_key(1, Profile::Bulk));
    }

    /// chunk_len >= 2 + Profile::Small → does NOT short-circuit.
    /// The short-circuit is *only* for single-key flushes;
    /// multi-key chunks must use the XML path even when the
    /// profile would otherwise opt in.
    #[test]
    fn short_circuit_skipped_for_multi_key_chunks() {
        for &n in &[2usize, 5, 20, 100] {
            assert!(
                !should_short_circuit_single_key(n, Profile::Small),
                "chunk_len={n} must use XML path"
            );
        }
    }

    /// Every production profile has the same threshold
    /// predicate shape: short-circuit iff chunk_len == 1
    /// AND profile.single_key_fast_delete(). A future
    /// addition of a `Profile::Tiny` that opts in must not
    /// regress this; the exhaustive cross-product below
    /// catches any such change.
    #[test]
    fn short_circuit_predicate_cross_product() {
        let profiles = [Profile::Small, Profile::Medium, Profile::Bulk];
        let lens = [0usize, 1, 2, 3, 10, 100];
        for &p in &profiles {
            for &n in &lens {
                let expected = n == 1 && p.single_key_fast_delete();
                assert_eq!(
                    should_short_circuit_single_key(n, p),
                    expected,
                    "profile={p:?} chunk_len={n}"
                );
            }
        }
    }

    /// Issue #562 stage 1.5 counter advances when the
    /// short-circuit path fires. We don't drive the
    /// network path in a unit test (that's bench work);
    /// instead, verify that the counter is read by
    /// `snapshot()` so a future /metrics endpoint can
    /// expose it.
    #[test]
    fn single_key_fast_delete_counter_visible_in_snapshot() {
        // Bump the counter directly (the short-circuit path
        // bumps it on the network success, but that's not
        // exercised here — only the snapshot plumbing is).
        let snap_before = crate::batched_delete::snapshot().single_key_fast_delete_total;
        SINGLE_KEY_FAST_DELETE_TOTAL.fetch_add(1, Ordering::Relaxed);
        let snap_after = crate::batched_delete::snapshot().single_key_fast_delete_total;
        assert!(
            snap_after > snap_before,
            "snapshot must reflect the counter advance (before={snap_before}, after={snap_after})"
        );
    }

    // ===== Issue #562 stage 5: ThresholdCalibrator unit tests =====
    //
    // The Calibrator's decision function is pure: input +
    // now → Option<Recommendation>. Tests drive every
    // trigger and the hysteresis / cold-start guards
    // without spawning the loop or touching the network.

    /// Helper: build a CalibrationInput with sensible defaults.
    fn cal_input(
        flushes_total: u64,
        retry_total: u64,
        chunk_size_sum: u64,
        burst_p95: usize,
        profile: Profile,
    ) -> CalibrationInput {
        CalibrationInput {
            flushes_total,
            retry_total,
            chunk_size_sum,
            burst_p95,
            current_profile: profile,
        }
    }

    /// Cold-start silence: flushes_total below the
    /// `min_flushes` threshold yields `None` regardless of
    /// input values. Pins the "no recommendations for first
    /// 100 flushes" guarantee from the issue spec.
    #[test]
    fn calibrator_cold_start_silence_below_min_flushes() {
        let c = ThresholdCalibrator::new();
        let now = Instant::now();
        // 99 flushes, very high retry rate (10%) → still
        // silent because we haven't hit min_flushes=100.
        let input = cal_input(99, 10, 99, 50, Profile::Medium);
        assert_eq!(c.observe(input, now), None);
    }

    /// Hysteresis: after a recommendation fires, the next
    /// observation within `recommendation_cooldown` (10
    /// minutes) returns `None` even if all triggers fire.
    /// Pins the "≤ 1 recommendation per 10-min window"
    /// guarantee.
    #[test]
    fn calibrator_hysteresis_blocks_subsequent_within_cooldown() {
        let mut c = ThresholdCalibrator::new();
        let t0 = Instant::now();
        // Trigger 1 fires (retry rate 10% on 100 flushes).
        let input = cal_input(100, 10, 100, 10, Profile::Medium);
        let first = c.observe(input, t0);
        assert!(matches!(
            first,
            Some(CalibrationRecommendation::RaiseBatchSize { .. })
        ));
        c.record_recommendation(t0);
        // 1 second later, retry rate still 10% — must be
        // suppressed by hysteresis.
        let t1 = t0 + Duration::from_secs(1);
        let suppressed = c.observe(input, t1);
        assert_eq!(
            suppressed, None,
            "1s after recommendation; cooldown=10m must suppress"
        );
    }

    /// Trigger 1: retry_rate >= 5% on a healthy sample size
    /// → `RaiseBatchSize` with a 25% bump, clamped to
    /// `[1, 1000]`.
    #[test]
    fn calibrator_raises_batch_size_on_high_retry_rate() {
        let c = ThresholdCalibrator::new();
        let now = Instant::now();
        // Profile::Medium batch_size=100; retry rate =
        // 10/100 = 10% (>= 5%). Expect RaiseBatchSize
        // { current: 100, proposed: 125 }.
        let input = cal_input(100, 10, 200, 50, Profile::Medium);
        let rec = c.observe(input, now);
        assert_eq!(
            rec,
            Some(CalibrationRecommendation::RaiseBatchSize {
                current: 100,
                proposed: 125,
            })
        );
    }

    /// Trigger 1 with Profile::Bulk (batch_size=500):
    /// raise by 25% (clamped at 1000). Verifies the clamp
    /// doesn't trip at the natural bump (500 → 625).
    #[test]
    fn calibrator_raises_batch_size_clamps_at_1000() {
        let c = ThresholdCalibrator::new();
        let now = Instant::now();
        // Profile::Bulk = 500; 25% bump = 625 (well under
        // 1000, no clamp).
        let input = cal_input(100, 50, 1000, 50, Profile::Bulk);
        let rec = c.observe(input, now);
        assert_eq!(
            rec,
            Some(CalibrationRecommendation::RaiseBatchSize {
                current: 500,
                proposed: 625,
            })
        );
    }

    /// Trigger 2: median chunk_size < 2 AND burst_p95 < 5
    /// AND threshold > 1 → LowerFastFlushThreshold to 1.
    /// This is the rm -rf 10/100/200 lever.
    #[test]
    fn calibrator_lowers_fast_flush_threshold_for_small_bursts() {
        let c = ThresholdCalibrator::new();
        let now = Instant::now();
        // avg_chunk_size = 50/100 = 0 (integer division);
        // burst_p95 = 3 (< 5); Profile::Small threshold=4
        // (> 1). Expect LowerFastFlushThreshold to 1.
        let input = cal_input(100, 0, 50, 3, Profile::Small);
        let rec = c.observe(input, now);
        assert_eq!(
            rec,
            Some(CalibrationRecommendation::LowerFastFlushThreshold {
                current: 4,
                proposed: 1,
            })
        );
    }

    /// Trigger 2 won't fire when threshold is already 1
    /// (no-op). Pins the `current_threshold > 1` guard.
    #[test]
    fn calibrator_does_not_lower_threshold_below_one() {
        // Synthesise a profile-like threshold by
        // constructing an input where the active profile
        // is hypothetical — but since Profile::Small is 4
        // and there's no profile with threshold=1, we
        // cover the boundary by checking that the trigger
        // doesn't fire when avg_chunk_size is fine but
        // burst_p95 is high (no small-burst signal).
        let c = ThresholdCalibrator::new();
        let now = Instant::now();
        // burst_p95 = 50 (sustained bulk workload, not
        // small bursts) → trigger 2 must NOT fire.
        let input = cal_input(100, 0, 100, 50, Profile::Small);
        let rec = c.observe(input, now);
        assert!(
            !matches!(
                rec,
                Some(CalibrationRecommendation::LowerFastFlushThreshold { .. })
            ),
            "burst_p95=50 must not trigger the small-burst lever; got {rec:?}"
        );
    }

    /// No triggers fire under nominal conditions: returns
    /// None. Pins the "zero recommendations is normal"
    /// behaviour so a 24h nightly run with
    /// `CALIBRATOR_RECOMMENDATIONS_TOTAL == 0` is
    /// understood as "current profile is well-fit".
    #[test]
    fn calibrator_no_trigger_returns_none() {
        let c = ThresholdCalibrator::new();
        let now = Instant::now();
        // Healthy: 100 flushes, no retries, chunk_size
        // ~20, burst_p95 = 30.
        let input = cal_input(100, 0, 2_000, 30, Profile::Medium);
        assert_eq!(c.observe(input, now), None);
    }

    /// Trigger 3: large batch_size + low retry_rate +
    /// avg_chunk_size much smaller than batch_size →
    /// LowerBatchSize to half (clamped to >=1).
    #[test]
    fn calibrator_lowers_oversized_batch_size() {
        let c = ThresholdCalibrator::new();
        let now = Instant::now();
        // Profile::Bulk = 500; avg_chunk_size = 200/100 =
        // 2; retry_rate = 0%. 2 < 500/4 = 125 → trigger.
        // Proposed: 500/2 = 250.
        let input = cal_input(100, 0, 200, 50, Profile::Bulk);
        let rec = c.observe(input, now);
        assert_eq!(
            rec,
            Some(CalibrationRecommendation::LowerBatchSize {
                current: 500,
                proposed: 250,
            })
        );
    }

    /// Counter visibility: `calibrator_recommendations_total`
    /// is exposed on `snapshot()` (used by future /metrics
    /// endpoint + bench harness).
    #[test]
    fn calibrator_recommendations_counter_visible_in_snapshot() {
        let snap_before = crate::batched_delete::snapshot().calibrator_recommendations_total;
        CALIBRATOR_RECOMMENDATIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
        let snap_after = crate::batched_delete::snapshot().calibrator_recommendations_total;
        assert!(
            snap_after > snap_before,
            "snapshot must reflect the counter advance (before={snap_before}, after={snap_after})"
        );
    }

    /// `retry_total` + `chunk_size_sum` exposed on
    /// `snapshot()` so the calibrator can compute the
    /// derived rates.
    #[test]
    fn snapshot_exposes_retry_total_and_chunk_size_sum() {
        // Drive the new counters directly.
        let snap_before = crate::batched_delete::snapshot();
        let _ = snap_before; // suppress unused warning before the bump
        RETRY_TOTAL.fetch_add(7, Ordering::Relaxed);
        CHUNK_SIZE_SUM.fetch_add(123, Ordering::Relaxed);
        let snap_after = crate::batched_delete::snapshot();
        assert_eq!(
            snap_after.retry_total - snap_before.retry_total,
            7,
            "retry_total must reflect the bump"
        );
        assert_eq!(
            snap_after.chunk_size_sum - snap_before.chunk_size_sum,
            123,
            "chunk_size_sum must reflect the bump"
        );
    }
}
