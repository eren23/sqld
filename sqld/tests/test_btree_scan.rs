use std::path::PathBuf;
use std::sync::Arc;

use sqld::storage::btree::{
    default_compare, encode_i64_key, decode_i64_key, BPlusTree, ScanDirection,
};
use sqld::storage::{BufferPoolManager, DiskManager, Tid};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_dir(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("sqld_test_btscan_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

fn make_tree(name: &str) -> (PathBuf, BPlusTree) {
    let dir = test_dir(name);
    let dm = Arc::new(DiskManager::new(&dir).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(256, 2, dm));
    let tree = BPlusTree::new(bpm, false, Box::new(default_compare));
    (dir, tree)
}

fn key(val: i64) -> Vec<u8> {
    encode_i64_key(val).to_vec()
}

fn tid(page: u32, slot: u16) -> Tid {
    Tid::new(page, slot)
}

/// Populate a tree with keys [0..n) and return it.
fn populated_tree(name: &str, n: i64) -> (PathBuf, BPlusTree) {
    let (dir, tree) = make_tree(name);
    for i in 0..n {
        tree.insert(&key(i), tid(1, i as u16)).unwrap();
    }
    (dir, tree)
}

// ---------------------------------------------------------------------------
// Forward scan — full range (unbounded)
// ---------------------------------------------------------------------------

#[test]
fn forward_scan_unbounded() {
    let (dir, tree) = populated_tree("fwd_unbound", 100);

    let iter = tree
        .range_scan(None, None, ScanDirection::Forward)
        .unwrap();
    let results: Vec<_> = iter.map(|r| r.unwrap()).collect();

    assert_eq!(results.len(), 100);
    for (i, (k, _t)) in results.iter().enumerate() {
        assert_eq!(decode_i64_key(k), i as i64);
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Backward scan — full range (unbounded)
// ---------------------------------------------------------------------------

#[test]
fn backward_scan_unbounded() {
    let (dir, tree) = populated_tree("bwd_unbound", 100);

    let iter = tree
        .range_scan(None, None, ScanDirection::Backward)
        .unwrap();
    let results: Vec<_> = iter.map(|r| r.unwrap()).collect();

    assert_eq!(results.len(), 100);
    // Should be in descending order.
    for (i, (k, _t)) in results.iter().enumerate() {
        assert_eq!(decode_i64_key(k), 99 - i as i64);
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Forward scan — inclusive start, inclusive end
// ---------------------------------------------------------------------------

#[test]
fn forward_inclusive_inclusive() {
    let (dir, tree) = populated_tree("fwd_ii", 100);

    let iter = tree
        .range_scan(
            Some((&key(10), true)),
            Some((&key(20), true)),
            ScanDirection::Forward,
        )
        .unwrap();

    let results: Vec<_> = iter.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 11); // 10..=20
    assert_eq!(decode_i64_key(&results[0].0), 10);
    assert_eq!(decode_i64_key(&results[10].0), 20);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Forward scan — exclusive start, inclusive end
// ---------------------------------------------------------------------------

#[test]
fn forward_exclusive_inclusive() {
    let (dir, tree) = populated_tree("fwd_ei", 100);

    let iter = tree
        .range_scan(
            Some((&key(10), false)),
            Some((&key(20), true)),
            ScanDirection::Forward,
        )
        .unwrap();

    let results: Vec<_> = iter.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 10); // 11..=20
    assert_eq!(decode_i64_key(&results[0].0), 11);
    assert_eq!(decode_i64_key(&results[9].0), 20);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Forward scan — inclusive start, exclusive end
// ---------------------------------------------------------------------------

#[test]
fn forward_inclusive_exclusive() {
    let (dir, tree) = populated_tree("fwd_ie", 100);

    let iter = tree
        .range_scan(
            Some((&key(10), true)),
            Some((&key(20), false)),
            ScanDirection::Forward,
        )
        .unwrap();

    let results: Vec<_> = iter.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 10); // 10..20
    assert_eq!(decode_i64_key(&results[0].0), 10);
    assert_eq!(decode_i64_key(&results[9].0), 19);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Forward scan — exclusive start, exclusive end
// ---------------------------------------------------------------------------

#[test]
fn forward_exclusive_exclusive() {
    let (dir, tree) = populated_tree("fwd_ee", 100);

    let iter = tree
        .range_scan(
            Some((&key(10), false)),
            Some((&key(20), false)),
            ScanDirection::Forward,
        )
        .unwrap();

    let results: Vec<_> = iter.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 9); // 11..20
    assert_eq!(decode_i64_key(&results[0].0), 11);
    assert_eq!(decode_i64_key(&results[8].0), 19);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Backward scan — with bounds
// ---------------------------------------------------------------------------

#[test]
fn backward_inclusive_inclusive() {
    let (dir, tree) = populated_tree("bwd_ii", 100);

    let iter = tree
        .range_scan(
            Some((&key(10), true)),
            Some((&key(20), true)),
            ScanDirection::Backward,
        )
        .unwrap();

    let results: Vec<_> = iter.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 11); // 20..=10, descending
    assert_eq!(decode_i64_key(&results[0].0), 20);
    assert_eq!(decode_i64_key(&results[10].0), 10);

    cleanup(&dir);
}

#[test]
fn backward_exclusive_exclusive() {
    let (dir, tree) = populated_tree("bwd_ee", 100);

    let iter = tree
        .range_scan(
            Some((&key(10), false)),
            Some((&key(20), false)),
            ScanDirection::Backward,
        )
        .unwrap();

    let results: Vec<_> = iter.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 9); // 19..11, descending
    assert_eq!(decode_i64_key(&results[0].0), 19);
    assert_eq!(decode_i64_key(&results[8].0), 11);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Scan with only start bound (open end)
// ---------------------------------------------------------------------------

#[test]
fn forward_start_bound_only() {
    let (dir, tree) = populated_tree("fwd_start_only", 50);

    let iter = tree
        .range_scan(
            Some((&key(40), true)),
            None,
            ScanDirection::Forward,
        )
        .unwrap();

    let results: Vec<_> = iter.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 10); // 40..50
    assert_eq!(decode_i64_key(&results[0].0), 40);
    assert_eq!(decode_i64_key(&results[9].0), 49);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Scan with only end bound (open start)
// ---------------------------------------------------------------------------

#[test]
fn forward_end_bound_only() {
    let (dir, tree) = populated_tree("fwd_end_only", 50);

    let iter = tree
        .range_scan(
            None,
            Some((&key(10), false)),
            ScanDirection::Forward,
        )
        .unwrap();

    let results: Vec<_> = iter.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 10); // 0..10
    assert_eq!(decode_i64_key(&results[0].0), 0);
    assert_eq!(decode_i64_key(&results[9].0), 9);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Empty scan (range with no matching keys)
// ---------------------------------------------------------------------------

#[test]
fn empty_scan_range() {
    let (dir, tree) = populated_tree("empty_scan", 50);

    // Range where no keys exist.
    let iter = tree
        .range_scan(
            Some((&key(100), true)),
            Some((&key(200), true)),
            ScanDirection::Forward,
        )
        .unwrap();

    let results: Vec<_> = iter.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 0);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Scan on empty tree
// ---------------------------------------------------------------------------

#[test]
fn scan_empty_tree() {
    let (dir, tree) = make_tree("scan_empty");

    let iter = tree
        .range_scan(None, None, ScanDirection::Forward)
        .unwrap();
    let results: Vec<_> = iter.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 0);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Scan after deletes (skips deleted entries)
// ---------------------------------------------------------------------------

#[test]
fn scan_skips_deleted() {
    let (dir, tree) = populated_tree("scan_deleted", 20);

    // Delete even keys.
    for i in (0..20).step_by(2) {
        tree.delete(&key(i)).unwrap();
    }

    let iter = tree
        .range_scan(None, None, ScanDirection::Forward)
        .unwrap();
    let results: Vec<_> = iter.map(|r| r.unwrap()).collect();

    assert_eq!(results.len(), 10); // Only odd keys remain.
    for (idx, (k, _)) in results.iter().enumerate() {
        let v = decode_i64_key(k);
        assert_eq!(v, (idx * 2 + 1) as i64);
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Large range scan crossing multiple leaves
// ---------------------------------------------------------------------------

#[test]
fn large_range_scan_across_leaves() {
    let dir = test_dir("large_scan");
    let dm = Arc::new(DiskManager::new(&dir).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(1024, 2, dm));
    let tree = BPlusTree::new(bpm, false, Box::new(default_compare));

    for i in 0..5000i64 {
        tree.insert(&key(i), tid(1, (i % 65536) as u16)).unwrap();
    }

    // Scan [1000, 3000).
    let iter = tree
        .range_scan(
            Some((&key(1000), true)),
            Some((&key(3000), false)),
            ScanDirection::Forward,
        )
        .unwrap();

    let results: Vec<_> = iter.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 2000);
    assert_eq!(decode_i64_key(&results[0].0), 1000);
    assert_eq!(decode_i64_key(&results[1999].0), 2999);

    cleanup(&dir);
}
