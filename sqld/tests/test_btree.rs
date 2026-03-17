use std::path::PathBuf;
use std::sync::Arc;

use sqld::storage::btree::{
    default_compare, encode_composite_key, encode_i64_key, decode_i64_key,
    reverse_compare, BPlusTree,
};
use sqld::storage::{BufferPoolManager, DiskManager, Tid};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sqld_test_btree_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

fn make_tree(name: &str, unique: bool) -> (PathBuf, BPlusTree) {
    let dir = test_dir(name);
    let dm = Arc::new(DiskManager::new(&dir).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(256, 2, dm));
    let tree = BPlusTree::new(bpm, unique, Box::new(default_compare));
    (dir, tree)
}

fn make_tree_reverse(name: &str, unique: bool) -> (PathBuf, BPlusTree) {
    let dir = test_dir(name);
    let dm = Arc::new(DiskManager::new(&dir).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(256, 2, dm));
    let tree = BPlusTree::new(bpm, unique, Box::new(reverse_compare));
    (dir, tree)
}

fn tid(page: u32, slot: u16) -> Tid {
    Tid::new(page, slot)
}

fn key(val: i64) -> Vec<u8> {
    encode_i64_key(val).to_vec()
}

// ---------------------------------------------------------------------------
// Ascending key insertion
// ---------------------------------------------------------------------------

#[test]
fn ascending_insert_and_search() {
    let (dir, tree) = make_tree("asc_insert", false);

    for i in 0..500 {
        tree.insert(&key(i), tid(1, i as u16)).unwrap();
    }
    for i in 0..500 {
        let result = tree.search(&key(i)).unwrap();
        assert_eq!(result, Some(tid(1, i as u16)), "key {i} not found");
    }
    // Non-existent key.
    assert_eq!(tree.search(&key(9999)).unwrap(), None);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Descending key insertion
// ---------------------------------------------------------------------------

#[test]
fn descending_insert_and_search() {
    let (dir, tree) = make_tree("desc_insert", false);

    for i in (0..500).rev() {
        tree.insert(&key(i), tid(1, i as u16)).unwrap();
    }
    for i in 0..500 {
        let result = tree.search(&key(i)).unwrap();
        assert_eq!(result, Some(tid(1, i as u16)), "key {i} not found");
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Random key insertion
// ---------------------------------------------------------------------------

#[test]
fn random_insert_and_search() {
    let (dir, tree) = make_tree("rand_insert", false);

    // Simple LCG for deterministic pseudo-random order.
    let mut rng: u64 = 42;
    let mut keys: Vec<i64> = Vec::new();
    for _ in 0..1000 {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let v = (rng >> 33) as i64;
        keys.push(v);
    }
    keys.sort();
    keys.dedup();

    // Shuffle with Fisher-Yates using same LCG.
    rng = 123;
    let n = keys.len();
    for i in (1..n).rev() {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let j = (rng >> 33) as usize % (i + 1);
        keys.swap(i, j);
    }

    for (idx, &v) in keys.iter().enumerate() {
        tree.insert(&key(v), tid(1, idx as u16)).unwrap();
    }
    for (idx, &v) in keys.iter().enumerate() {
        let result = tree.search(&key(v)).unwrap();
        assert_eq!(result, Some(tid(1, idx as u16)), "key {v} not found");
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 100k keys — height <= 3
// ---------------------------------------------------------------------------

#[test]
fn hundred_k_height() {
    let dir = test_dir("100k_height");
    let dm = Arc::new(DiskManager::new(&dir).unwrap());
    // Large pool to hold all pages.
    let bpm = Arc::new(BufferPoolManager::new(4096, 2, dm));
    let tree = BPlusTree::new(bpm, false, Box::new(default_compare));

    for i in 0..100_000i64 {
        tree.insert(&key(i), tid(1, (i % 65536) as u16)).unwrap();
    }

    let h = tree.height().unwrap();
    assert!(h <= 3, "expected height <= 3, got {h}");

    // Spot-check some lookups.
    for &v in &[0i64, 1, 999, 50_000, 99_999] {
        assert!(
            tree.search(&key(v)).unwrap().is_some(),
            "key {v} not found"
        );
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Delete + search
// ---------------------------------------------------------------------------

#[test]
fn delete_and_search() {
    let (dir, tree) = make_tree("del_search", false);

    for i in 0..200 {
        tree.insert(&key(i), tid(1, i as u16)).unwrap();
    }

    // Delete even keys.
    for i in (0..200).step_by(2) {
        let ok = tree.delete(&key(i)).unwrap();
        assert!(ok, "delete of key {i} should succeed");
    }

    // Verify: even keys gone, odd keys still present.
    for i in 0..200 {
        let result = tree.search(&key(i)).unwrap();
        if i % 2 == 0 {
            assert_eq!(result, None, "key {i} should be deleted");
        } else {
            assert_eq!(result, Some(tid(1, i as u16)), "key {i} should exist");
        }
    }

    // Delete non-existent key returns false.
    assert!(!tree.delete(&key(9999)).unwrap());

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Split correctness
// ---------------------------------------------------------------------------

#[test]
fn split_preserves_all_keys() {
    let (dir, tree) = make_tree("split_correct", false);

    // Insert enough keys to force multiple splits.
    let count = 2000;
    for i in 0..count {
        tree.insert(&key(i), tid(1, i as u16)).unwrap();
    }

    // All keys must be findable after splits.
    for i in 0..count {
        let result = tree.search(&key(i)).unwrap();
        assert!(result.is_some(), "key {i} missing after splits");
    }

    // Tree should have height >= 2 (at least one split occurred).
    let h = tree.height().unwrap();
    assert!(h >= 2, "expected height >= 2 after 2000 inserts, got {h}");

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Unique constraint rejection
// ---------------------------------------------------------------------------

#[test]
fn unique_rejects_duplicate() {
    let (dir, tree) = make_tree("unique_rej", true);

    tree.insert(&key(42), tid(1, 0)).unwrap();
    let result = tree.insert(&key(42), tid(1, 1));
    assert!(result.is_err(), "duplicate insert should fail");

    // Distinct key succeeds.
    tree.insert(&key(43), tid(1, 1)).unwrap();

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Descending index (reverse comparator)
// ---------------------------------------------------------------------------

#[test]
fn descending_index() {
    let (dir, tree) = make_tree_reverse("desc_index", false);

    for i in 0..100 {
        tree.insert(&key(i), tid(1, i as u16)).unwrap();
    }

    // All keys should be findable.
    for i in 0..100 {
        assert!(tree.search(&key(i)).unwrap().is_some(), "key {i} not found");
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Composite key — full and prefix search
// ---------------------------------------------------------------------------

#[test]
fn composite_key_full_search() {
    let (dir, tree) = make_tree("comp_full", false);

    // Composite key: (col_a: i64, col_b: i64)
    for a in 0..10i64 {
        for b in 0..10i64 {
            let k = encode_composite_key(&[&encode_i64_key(a), &encode_i64_key(b)]);
            tree.insert(&k, tid(a as u32, b as u16)).unwrap();
        }
    }

    // Full composite key search.
    for a in 0..10i64 {
        for b in 0..10i64 {
            let k = encode_composite_key(&[&encode_i64_key(a), &encode_i64_key(b)]);
            let result = tree.search(&k).unwrap();
            assert_eq!(
                result,
                Some(tid(a as u32, b as u16)),
                "composite ({a},{b}) not found"
            );
        }
    }

    cleanup(&dir);
}

#[test]
fn composite_key_prefix_scan() {
    use sqld::storage::btree::ScanDirection;

    let (dir, tree) = make_tree("comp_prefix", false);

    for a in 0..5i64 {
        for b in 0..10i64 {
            let k = encode_composite_key(&[&encode_i64_key(a), &encode_i64_key(b)]);
            tree.insert(&k, tid(a as u32, b as u16)).unwrap();
        }
    }

    // Prefix scan for a = 2: scan from (2, MIN) to (2, MAX).
    let start = encode_composite_key(&[&encode_i64_key(2), &encode_i64_key(i64::MIN)]);
    let end = encode_composite_key(&[&encode_i64_key(2), &encode_i64_key(i64::MAX)]);

    let iter = tree
        .range_scan(
            Some((&start, true)),
            Some((&end, true)),
            ScanDirection::Forward,
        )
        .unwrap();

    let results: Vec<_> = iter.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 10, "prefix scan should return 10 entries");

    // Verify all belong to a = 2.
    for (k, t) in &results {
        let a = decode_i64_key(&k[0..8]);
        assert_eq!(a, 2, "expected a=2, got {a}");
        assert_eq!(t.page_id, 2);
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// VACUUM merges under-full leaves
// ---------------------------------------------------------------------------

#[test]
fn vacuum_merges_leaves() {
    let (dir, tree) = make_tree("vacuum", false);

    for i in 0..500 {
        tree.insert(&key(i), tid(1, i as u16)).unwrap();
    }

    // Delete most keys to make leaves very sparse.
    for i in 0..480 {
        tree.delete(&key(i)).unwrap();
    }

    let merges = tree.vacuum().unwrap();
    // Some merges should have occurred since leaves are < 40% full.
    assert!(merges > 0, "expected at least 1 merge, got {merges}");

    // Remaining keys still accessible.
    for i in 480..500 {
        assert!(
            tree.search(&key(i)).unwrap().is_some(),
            "key {i} missing after vacuum"
        );
    }

    cleanup(&dir);
}
