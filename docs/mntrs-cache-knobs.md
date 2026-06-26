# mntrs-specific cache knobs (design doc)

> **Scope:** Candidate cache-control knobs that mntrs may
> need because of its architecture (FUSE daemon, multi-
> tier cache, writeback worker, CSI multi-tenant mode).
> rclone has no analog for these — they are mntrs-native
> ideas evaluated against the **demand + smallest + testable**
> filter.
>
> This is a **design doc**, not a 1-shot PR. Each
> candidate that passes the filter becomes a follow-up
> sub-issue with concrete flag name, default, test plan,
> and migration impact.

## Background: why mntrs may need more knobs

mntrs is a FUSE daemon, not a single-process CLI. The
architecture creates cache-control needs rclone doesn't
have:

- **Multi-process / multi-tenant** (CSI mode): N pods on
  N nodes may share a backend. A single mount's cache
  can't be globally coherent; the question is what
  cache knobs to expose per-volume.
- **Background services** (writeback worker, prefetcher,
  MemoryLimiter): each runs continuously; their knobs
  are operational, not user-facing.
- **Multi-tier cache** (mem + disk): the L1/L2 boundary
  is mntrs-specific.
- **5-layer architecture** (see
  [`vfs-cache-flags.md`](vfs-cache-flags.md)): per-layer
  knobs exist, but cross-layer coordination is not.

## Evaluation framework

For each candidate, three questions must answer YES:

1. **Demand**: is there evidence (issue, ops report,
   customer request) that users want this knob?
2. **Smallest**: can the smallest possible implementation
   solve the use case without redesign?
3. **Testable**: can the behavior be unit-tested or
   integration-tested without an unrealistic harness?

If a candidate fails any of the three, drop it from
the design doc.

## Candidates (5)

### 1. Writeback throttling per-inode / per-volume — DROP

**Use case:** a write-heavy workload (database, log)
shouldn't monopolize the writeback queue. A bursty
workload (CI build) should flush fast.

**rclone analog:** none (rclone's writeback is
single-process).

**mntrs-specific:** `--writeback-per-ino-budget-bytes-per-sec`
(u64, 0 = no limit), or `--writeback-class database|bursty`
(preset).

**Filter result:**
- **Demand:** 🔴 NO. No issue, no ops report, no
  customer request. Speculative.
- **Smallest:** 🟡 MAYBE. A per-inode token bucket adds
  significant plumbing (token state per inode + global
  scheduler). The "class" preset is simpler but still
  new code.
- **Testable:** 🟢 YES (token bucket is unit-testable).

**Decision: DROP** — fails the demand test. If demand
appears later (an issue surfaces), reopen as a fresh
sub-issue.

### 2. Multi-tier cache hint — DEFER (potential)

**Use case:** user knows a file is "hot" and wants it
kept in `mem_cache` specifically, or "cold" and wants
it disk-only. Useful for hybrid workloads (some files
are read many times, others once).

**rclone analog:** none.

**mntrs-specific:** `--cache-tier mem|disk|both` (per-mount
default) or `user.cache_tier` xattr (per-file override).
Or `--disk-cache-policy bypass|retain|auto` (default:
auto — current behavior).

**Filter result:**
- **Demand:** 🟡 WEAK. One operator asked informally
  about L1-only mode for latency-critical reads. Not
  documented anywhere yet.
- **Smallest:** 🟢 YES. A simple per-mount flag +
  bypass in `populate()` covers the "disk-cache-off"
  case. The "mem-only" case is harder because
  `mem_cache` is already bounded by `--mem-limit` and
  blocks are auto-promoted to disk on pressure.
- **Testable:** 🟢 YES. Unit-testable in
  `multi_level_cache.rs` integration tests.

**Decision: DEFER** — weakest of the three. If a
concrete issue surfaces, the smallest implementation
is `--disk-cache-policy bypass|retain` (two values,
default retain). Add to the next-sprint backlog as a
speculative sub-issue.

### 3. Per-tenant cap (CSI mode) — DROP (CSI has its own)

**Use case:** an operator wants each CSI volume to have
a hard cap on `--cache-max-size` regardless of what
the user passes.

**rclone analog:** none.

**mntrs-specific:** `tenant_override_cache_max_size`
(u64), set by the CSI driver in `mount_internal`, not
the user.

**Filter result:**
- **Demand:** 🟡 WEAK. The CSI driver already has its
  own quota model (StorageClass → resource limits). A
  `tenant_override_*` knob inside mntrs duplicates that.
- **Smallest:** 🟡 MAYBE. A new field on `MountOptions`
  is cheap, but the CSI driver already controls this
  via its own quota enforcement.
- **Testable:** 🟡 MAYBE. Would require a CSI test
  harness to validate end-to-end.

**Decision: DROP** — duplicates CSI's existing quota
model. If a real gap appears, the fix is in the CSI
driver, not mntrs.

### 4. Background-work priority — DROP

**Use case:** a workload running in CI vs production
has different tolerance for writeback/prefetch latency.
A "production" workload wants fast writeback; a "CI"
workload can tolerate slower writeback.

**rclone analog:** none.

**mntrs-specific:** `--mntrs-priority production|interactive|background`
(affects writeback thread priority, prefetch aggressiveness).

**Filter result:**
- **Demand:** 🔴 NO. Speculative. Operators typically
  just tune the existing knobs (`--vfs-write-back`,
  `--writeback-immediate-threshold`).
- **Smallest:** 🔴 NO. Priority is a knob on at least
  three subsystems (writeback, prefetcher, MemoryLimiter);
  coordinating them is a redesign.
- **Testable:** 🟡 MAYBE. The behavior is observable
  but the test surface is non-trivial.

**Decision: DROP** — fails demand + smallest.

### 5. Disk-cache LRU hint — DROP (covered by existing knobs)

**Use case:** a workload that reads once and discards
(build, cache-miss) doesn't need disk-cache. A workload
that reads many times (data analysis) does.

**rclone analog:** `--vfs-cache-mode=full` (closest, but
conflates with no-cache).

**mntrs-specific:** `--disk-cache-policy bypass|retain`
(default: retain).

**Filter result:**
- **Demand:** 🟡 WEAK. Implicit in candidate 2 above.
- **Smallest:** 🟢 YES. Same as candidate 2's smallest
  impl.
- **Testable:** 🟢 YES. Same as candidate 2.

**Decision: DROP** — subsumed by candidate 2's
`--disk-cache-policy bypass|retain|auto` (if candidate
2 is ever picked up). Don't open a separate issue.

## Summary

| # | Candidate | Demand | Smallest | Testable | Decision |
|---|---|---|---|---|---|
| 1 | Writeback throttling per-ino | 🔴 | 🟡 | 🟢 | DROP |
| 2 | Multi-tier cache hint | 🟡 | 🟢 | 🟢 | DEFER (potential) |
| 3 | Per-tenant cap | 🟡 | 🟡 | 🟡 | DROP (CSI quota) |
| 4 | Background-work priority | 🔴 | 🔴 | 🟡 | DROP |
| 5 | Disk-cache LRU hint | 🟡 | 🟢 | 🟢 | DROP (subsumed by #2) |

**Net: 1 candidate survives (DEFER status).** No new
follow-up issues are opened at this time. If concrete
demand surfaces for #2, a new issue will be filed
linking to this design doc.

## Related

- [#228](../../issues/228) — Sprint 8 design reframe (tracker)
- [#231](../../issues/231) — Sprint 8.3 (this doc)
- [`vfs-cache-flags.md`](vfs-cache-flags.md) — sibling shadow-flag doc
- [#201](../../issues/201), [#202](../../issues/202), [#222](../../issues/222)
  — recent mntrs-specific knobs (precedent for adding
  non-rclone params)