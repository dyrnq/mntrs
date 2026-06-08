#!/usr/bin/env python3
"""Render benchmark results as a comparison table."""
import sys

rows = []
m = {}
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
        else:
            r[key] = time_val
        if (cat, test) not in rows:
            rows.append((cat, test))

# Count wins
mwins = 0
rwins = 0
ties = 0

def to_sec(t):
    try:
        parts = t.replace('s', '').split('m')
        return float(parts[0]) * 60 + float(parts[1])
    except Exception:
        return None

table_rows = []
for cat, test in rows:
    key = cat + '|' + test
    mv = m.get(key, '\u2014')
    rv = r.get(key, '\u2014')
    w = '\u2014'
    if mv == 'FAIL' and rv not in ('FAIL', '\u2014'):
        w = 'rclone'
        rwins += 1
    elif rv == 'FAIL' and mv not in ('FAIL', '\u2014'):
        w = 'mntrs'
        mwins += 1
    elif mv not in ('FAIL', '\u2014') and rv not in ('FAIL', '\u2014'):
        ms = to_sec(mv)
        rs = to_sec(rv)
        if ms is not None and rs is not None:
            if ms < rs:
                w = f'mntrs  ({rs - ms:.3f}s)'
                mwins += 1
            elif rs < ms:
                w = f'rclone  ({ms - rs:.3f}s)'
                rwins += 1
            else:
                w = 'tie'
                ties += 1
    table_rows.append((cat, test, mv, rv, w))

# Print
print()
print('=' * 82)
print('  BENCHMARK SUMMARY: mntrs vs rclone')
print('=' * 82)
print(f'  {"Category":<16} | {"Test":<26} | {"mntrs":>8} | {"rclone":>8} | {"Winner":>20}')
print(f'  {"-"*16}-+-{"-"*26}-+-{"-"*8}-+-{"-"*8}-+-{"-"*20}')

for cat, test, mv, rv, w in table_rows:
    print(f'  {cat:<16} | {test:<26} | {mv:>8} | {rv:>8} | {w:>20}')

print(f'  {"-"*16}-+-{"-"*26}-+-{"-"*8}-+-{"-"*8}-+-{"-"*20}')
print(f'  Result: mntrs={mwins}  rclone={rwins}  tie={ties}  ({len(rows)} tests)')
print('=' * 82)
print()
