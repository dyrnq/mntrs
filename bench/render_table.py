#!/usr/bin/env python3
"""Render benchmark results as a comparison table.

Plan #64 step 12: render three-way A/B when results include
`mntrs`, `mntrs-batched`, and `rclone`. The batched row is only
present for the `RmRf` workloads (rm/rmdir/rm -rf) where the
batched-delete code path is engaged. For workloads that don't
have a batched measurement, the column shows '—' (em-dash,
same as a missing entry) and is ignored by the win counter.
"""
import sys

rows = []
m = {}
b = {}
r = {}

input_file = sys.argv[1] if len(sys.argv) > 1 else '/dev/stdin'

with open(input_file) as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        parts = line.split('|', 3)
        if len(parts) < 4:
            continue
        time_val, test, target, cat = parts
        key = cat + '|' + test
        if target == 'mntrs':
            m[key] = time_val
        elif target == 'mntrs-batched':
            b[key] = time_val
        else:
            r[key] = time_val
        if (cat, test) not in rows:
            rows.append((cat, test))


def to_sec(t):
    if t in ('FAIL', 'SKIP', '—'):
        return None
    try:
        parts = t.replace('s', '').split('m')
        return float(parts[0]) * 60 + float(parts[1])
    except Exception:
        return None


# Issue #523: SKIP is treated identically to FAIL/— in the win
# accounting. A skipped row is environment-side (e.g. getfattr
# missing on the runner, rclone mount not surfacing symlinks on
# object storage) — not a win for either side, and not a
# regression signal.
NON_COMPARABLE = ('FAIL', 'SKIP', '—')

table_rows = []
mntrs_vs_rclone_wins = {'mntrs': 0, 'rclone': 0, 'tie': 0, 'skip': 0}
batched_delta = []  # (cat, test, mb, mu, speedup)

for cat, test in rows:
    key = cat + '|' + test
    mv = m.get(key, '—')
    bv = b.get(key, '—')
    rv = r.get(key, '—')

    # mntrs vs rclone
    w = '—'
    if mv == 'FAIL' and rv not in NON_COMPARABLE:
        w = 'rclone'
        mntrs_vs_rclone_wins['rclone'] += 1
    elif rv == 'FAIL' and mv not in NON_COMPARABLE:
        w = 'mntrs'
        mntrs_vs_rclone_wins['mntrs'] += 1
    elif mv not in NON_COMPARABLE and rv not in NON_COMPARABLE:
        ms = to_sec(mv)
        rs = to_sec(rv)
        if ms is not None and rs is not None:
            if ms < rs:
                w = f'mntrs  ({rs - ms:.3f}s)'
                mntrs_vs_rclone_wins['mntrs'] += 1
            elif rs < ms:
                w = f'rclone  ({ms - rs:.3f}s)'
                mntrs_vs_rclone_wins['rclone'] += 1
            else:
                w = 'tie'
                mntrs_vs_rclone_wins['tie'] += 1
        else:
            mntrs_vs_rclone_wins['skip'] += 1
    else:
        mntrs_vs_rclone_wins['skip'] += 1

    # mntrs-batched vs mntrs-unbatched delta (only meaningful
    # when both rows are present and not FAIL/SKIP).
    if bv not in NON_COMPARABLE and mv not in NON_COMPARABLE:
        ms_b = to_sec(bv)
        ms_u = to_sec(mv)
        if ms_b is not None and ms_u is not None and ms_u > 0:
            batched_delta.append((cat, test, bv, mv, ms_u / ms_b))

    table_rows.append((cat, test, mv, bv, rv, w))

# Print
print()
print('=' * 110)
print('  BENCHMARK SUMMARY: mntrs vs mntrs-batched vs rclone')
print('=' * 110)
header = (
    f'  {"Category":<12} | {"Test":<26} | '
    f'{"mntrs":>9} | {"mntrs-batch":>11} | {"rclone":>9} | {"Winner":>20}'
)
print(header)
print(
    f'  {"-"*12}-+-{"-"*26}-+-{"-"*9}-+-{"-"*11}-+-{"-"*9}-+-{"-"*20}'
)

for cat, test, mv, bv, rv, w in table_rows:
    print(
        f'  {cat:<12} | {test:<26} | '
        f'{mv:>9} | {bv:>11} | {rv:>9} | {w:>20}'
    )

print(
    f'  {"-"*12}-+-{"-"*26}-+-{"-"*9}-+-{"-"*11}-+-{"-"*9}-+-{"-"*20}'
)
mw = mntrs_vs_rclone_wins
print(
    f'  Result (mntrs vs rclone): '
    f'mntrs={mw["mntrs"]}  rclone={mw["rclone"]}  '
    f'tie={mw["tie"]}  skip={mw["skip"]}  ({len(rows)} tests)'
)
print('=' * 110)

# Batched-vs-unbatched delta table (Plan #64 step 13 input)
if batched_delta:
    print()
    print('=' * 78)
    print('  batched-delete A/B (mntrs-unbatched / mntrs-batched)')
    print('=' * 78)
    print(f'  {"Category":<12} | {"Test":<26} | {"unbatched":>10} | {"batched":>10} | {"speedup":>9}')
    print(f'  {"-"*12}-+-{"-"*26}-+-{"-"*10}-+-{"-"*10}-+-{"-"*9}')
    total_speedup = 0.0
    for cat, test, bv, mv, speedup in batched_delta:
        total_speedup += speedup
        print(
            f'  {cat:<12} | {test:<26} | {mv:>10} | {bv:>10} | {speedup:>7.2f}x'
        )
    print(f'  {"-"*12}-+-{"-"*26}-+-{"-"*10}-+-{"-"*10}-+-{"-"*9}')
    avg = total_speedup / len(batched_delta)
    print(f'  geomean speedup across {len(batched_delta)} batched tests: {avg:.2f}x')
    print('=' * 78)
print()