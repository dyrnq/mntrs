//! Microbench for #16 (find_ino_by_path) and #17 (write hot path)
//! optimizations.
//!
//! These are NOT criterion-style benches — they're focused
//! timing comparisons of the algorithmic shape before/after
//! each fix. The MntrsFs::find_ino_by_path and FileHandleState
//! aren't part of the public API, so we replicate their data
//! shape inline. The comparison is between (a) the pre-fix
//! algorithm and (b) the post-fix algorithm, run on identical
//! synthetic state. This isolates the change from the rest of
//! the FUSE stack (which is the source of bench-script noise).
//!
//! Run: `cargo test --test microbench --release -- --nocapture`
//! The `--release` is important; debug builds add 10-100x of
//! their own overhead and obscure the ratio.
//!
//! Output is `println!`'d to stdout so the harness's
//! `--nocapture` flag shows the numbers. The tests assert
//! "new path is at least N× faster than old path" with a
//! conservative N — fails the test if the gap collapses
//! (regression guard).

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use dashmap::DashMap;

// ─── #16: find_ino_by_path ──────────────────────────────────────────

/// Pre-fix: linear scan of the inodes DashMap. O(N) per call.
fn find_ino_linear_scan(
    inodes: &DashMap<u64, (String, (), u64, Option<()>)>,
    target: &str,
) -> Option<u64> {
    for entry in inodes.iter() {
        if entry.value().0 == target {
            return Some(*entry.key());
        }
    }
    None
}

/// Post-fix: reverse map lookup with inodes re-check. O(1) per
/// call (one DashMap.get + one DashMap.get to verify).
fn find_ino_reverse_map(
    path_to_ino: &DashMap<String, u64>,
    inodes: &DashMap<u64, (String, (), u64, Option<()>)>,
    target: &str,
) -> Option<u64> {
    if let Some(ino) = path_to_ino.get(target).map(|r| *r.value())
        && let Some(entry) = inodes.get(&ino)
        && entry.value().0 == target
    {
        return Some(ino);
    }
    None
}

#[test]
fn microbench_find_ino_by_path_500_entries() {
    // 500 entries — matches the bench script's `many/`
    // directory size, where the regression hurt most.
    const N_ENTRIES: usize = 500;
    const N_LOOKUPS: usize = 100_000;
    // Target is in the middle of the inodes table so the
    // linear scan does ~250 comparisons on average per
    // lookup (the worst case for a random hit).
    let target_index = N_ENTRIES / 2;
    let target_path = format!("file_{target_index}");

    let inodes: DashMap<u64, (String, (), u64, Option<()>)> = DashMap::new();
    let path_to_ino: DashMap<String, u64> = DashMap::new();
    for i in 0..N_ENTRIES {
        let ino = (i + 100) as u64;
        let path = format!("file_{i}");
        inodes.insert(ino, (path.clone(), (), 4096, None));
        path_to_ino.insert(path, ino);
    }

    // Old path: linear scan.
    let t0 = Instant::now();
    let mut hits = 0u64;
    for _ in 0..N_LOOKUPS {
        if find_ino_linear_scan(&inodes, &target_path).is_some() {
            hits += 1;
        }
    }
    let linear_dur = t0.elapsed();
    assert_eq!(
        hits as usize, N_LOOKUPS,
        "linear scan should hit every call"
    );

    // New path: reverse map.
    let t0 = Instant::now();
    let mut hits = 0u64;
    for _ in 0..N_LOOKUPS {
        if find_ino_reverse_map(&path_to_ino, &inodes, &target_path).is_some() {
            hits += 1;
        }
    }
    let reverse_dur = t0.elapsed();
    assert_eq!(
        hits as usize, N_LOOKUPS,
        "reverse lookup should hit every call"
    );

    let linear_ns_per = linear_dur.as_nanos() / N_LOOKUPS as u128;
    let reverse_ns_per = reverse_dur.as_nanos() / N_LOOKUPS as u128;
    let speedup = linear_ns_per as f64 / reverse_ns_per.max(1) as f64;

    println!("\n#16 find_ino_by_path microbench (N_ENTRIES={N_ENTRIES}, N_LOOKUPS={N_LOOKUPS})");
    println!("  linear scan  : {linear_ns_per:>6} ns/call ({linear_dur:?} total)");
    println!("  reverse map  : {reverse_ns_per:>6} ns/call ({reverse_dur:?} total)");
    println!("  speedup      : {speedup:.1}×");

    // Conservative regression guard: reverse map should be
    // at least 5× faster on a 500-entry table. In practice
    // we expect 50-500× depending on CPU/cache.
    assert!(
        speedup >= 5.0,
        "reverse map should be ≥5× faster than linear scan (got {speedup:.1}×)"
    );
}

// ─── #17: write hot path ────────────────────────────────────────────

/// Replica of FileHandleState (private to lib) shaped to
/// match the DashMap stored in MntrsFs.handles. The size +
/// allocation pattern is what matters for the bench, not the
/// exact field semantics.
#[derive(Clone)]
#[allow(dead_code)]
enum FakeHandle {
    Write {
        path: String,
        cache_fd: Option<Arc<Mutex<()>>>,
        dirty: bool,
        dirty_since: Option<Instant>,
    },
    Read {
        path: String,
    },
}

/// Pre-fix: two handles.get calls + full handles.insert that
/// rebuilds the enum. Mirrors the old write() code path.
fn write_old_pattern(handles: &DashMap<u64, FakeHandle>, fh: u64, _data: &[u8]) {
    // First get: extract path.
    let path = handles
        .get(&fh)
        .map(|r| match r.value() {
            FakeHandle::Write { path, .. } => path.clone(),
            FakeHandle::Read { path } => path.clone(),
        })
        .unwrap();
    // Second get: extract cache_fd.
    let cache_fd = handles.get(&fh).and_then(|e| {
        if let FakeHandle::Write {
            cache_fd: Some(fd), ..
        } = e.value()
        {
            Some(fd.clone())
        } else {
            None
        }
    });
    // Trailing insert: full rebuild.
    handles.insert(
        fh,
        FakeHandle::Write {
            path,
            cache_fd,
            dirty: true,
            dirty_since: Some(Instant::now()),
        },
    );
}

/// Post-fix: one combined match + and_modify in-place update.
fn write_new_pattern(handles: &DashMap<u64, FakeHandle>, fh: u64, _data: &[u8]) {
    // Single get with pattern match.
    let (_path, _cache_fd) = match handles.get(&fh) {
        Some(entry) => match entry.value() {
            FakeHandle::Write { path, cache_fd, .. } => (path.clone(), cache_fd.clone()),
            FakeHandle::Read { path } => (path.clone(), None),
        },
        None => return,
    };
    // In-place mutation.
    handles.entry(fh).and_modify(|h| {
        if let FakeHandle::Write {
            dirty, dirty_since, ..
        } = h
        {
            *dirty = true;
            *dirty_since = Some(Instant::now());
        }
    });
}

#[test]
fn microbench_write_hot_path() {
    // Single open file handle, many sequential writes —
    // matches the bench script's `write 1K/4K/.../1M new`
    // workload pattern at the FUSE-worker layer.
    const N_WRITES: usize = 100_000;
    let handles: DashMap<u64, FakeHandle> = DashMap::new();
    let fd = Arc::new(Mutex::new(()));
    handles.insert(
        42,
        FakeHandle::Write {
            path: "bench.bin".to_string(),
            cache_fd: Some(fd),
            dirty: false,
            dirty_since: None,
        },
    );
    let data = vec![0u8; 4096]; // 4 KiB write payload

    // Warm-up — populate any allocator pools so the timing
    // doesn't include first-time allocations.
    for _ in 0..1000 {
        write_old_pattern(&handles, 42, &data);
        write_new_pattern(&handles, 42, &data);
    }

    // Old pattern.
    let t0 = Instant::now();
    for _ in 0..N_WRITES {
        write_old_pattern(&handles, 42, &data);
    }
    let old_dur = t0.elapsed();

    // New pattern.
    let t0 = Instant::now();
    for _ in 0..N_WRITES {
        write_new_pattern(&handles, 42, &data);
    }
    let new_dur = t0.elapsed();

    let old_ns_per = old_dur.as_nanos() / N_WRITES as u128;
    let new_ns_per = new_dur.as_nanos() / N_WRITES as u128;
    let speedup = old_ns_per as f64 / new_ns_per.max(1) as f64;

    println!("\n#17 write hot path microbench (N_WRITES={N_WRITES})");
    println!("  old pattern  : {old_ns_per:>6} ns/call ({old_dur:?} total)");
    println!("  new pattern  : {new_ns_per:>6} ns/call ({new_dur:?} total)");
    println!("  speedup      : {speedup:.2}×");

    // Conservative regression guard. The new path saves
    // one shard lock + a full enum rebuild — typical
    // savings are 30-80 % depending on allocator. Anything
    // ≥10 % faster confirms the optimization is alive;
    // anything significantly slower is a regression.
    assert!(
        speedup >= 1.05,
        "new pattern should be ≥1.05× faster than old (got {speedup:.2}×)"
    );
}
