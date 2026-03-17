use sqld::storage::free_space_map::FreeSpaceMap;
use sqld::storage::page::PageId;

// ---------------------------------------------------------------------------
// bytes_to_category / needed_to_category
// ---------------------------------------------------------------------------

#[test]
fn bytes_to_category_granularity() {
    // 0 free bytes → category 0 (full)
    assert_eq!(FreeSpaceMap::bytes_to_category(0), 0);
    // 31 bytes → 0 (< 32)
    assert_eq!(FreeSpaceMap::bytes_to_category(31), 0);
    // 32 bytes → 1
    assert_eq!(FreeSpaceMap::bytes_to_category(32), 1);
    // 64 bytes → 2
    assert_eq!(FreeSpaceMap::bytes_to_category(64), 2);
    // 8160 bytes → 255
    assert_eq!(FreeSpaceMap::bytes_to_category(8160), 255);
    // Values larger than 8160 cap at 255.
    assert_eq!(FreeSpaceMap::bytes_to_category(10000), 255);
}

#[test]
fn needed_to_category_rounds_up() {
    // Need 1 byte → category 1 (need at least 32-byte slot)
    assert_eq!(FreeSpaceMap::needed_to_category(1), 1);
    // Need 32 bytes → category 1
    assert_eq!(FreeSpaceMap::needed_to_category(32), 1);
    // Need 33 bytes → category 2
    assert_eq!(FreeSpaceMap::needed_to_category(33), 2);
    // Need 0 bytes → category 0
    assert_eq!(FreeSpaceMap::needed_to_category(0), 0);
}

// ---------------------------------------------------------------------------
// Basic update and find
// ---------------------------------------------------------------------------

#[test]
fn update_and_find_page() {
    let page_ids: Vec<PageId> = vec![10, 20, 30];
    let mut fsm = FreeSpaceMap::new();

    // Mark pages with decreasing free space.
    fsm.update(0, 8000); // page 10: ~8000 bytes free
    fsm.update(1, 100);  // page 20: ~100 bytes free
    fsm.update(2, 4000); // page 30: ~4000 bytes free

    // Need 200 bytes → should find page 10 (first with enough room).
    assert_eq!(fsm.find_page(&page_ids, 200), Some(10));

    // Need 5000 bytes → should find page 10 (only one with enough).
    assert_eq!(fsm.find_page(&page_ids, 5000), Some(10));

    // Need 9000 bytes → none have that much.
    assert_eq!(fsm.find_page(&page_ids, 9000), None);
}

#[test]
fn find_prefers_first_fit() {
    let page_ids: Vec<PageId> = vec![1, 2, 3];
    let mut fsm = FreeSpaceMap::new();

    fsm.update(0, 500);
    fsm.update(1, 500);
    fsm.update(2, 500);

    // All three have enough room; should return the first.
    assert_eq!(fsm.find_page(&page_ids, 100), Some(1));
}

// ---------------------------------------------------------------------------
// with_capacity
// ---------------------------------------------------------------------------

#[test]
fn with_capacity_marks_all_empty() {
    let fsm = FreeSpaceMap::with_capacity(5);
    assert_eq!(fsm.len(), 5);
    for i in 0..5 {
        assert_eq!(fsm.get_category(i), Some(255));
    }
}

// ---------------------------------------------------------------------------
// update grows entries
// ---------------------------------------------------------------------------

#[test]
fn update_auto_grows() {
    let mut fsm = FreeSpaceMap::new();
    assert_eq!(fsm.len(), 0);

    fsm.update(5, 1000);
    assert_eq!(fsm.len(), 6); // indices 0..5 filled with 255 (empty), index 5 set

    // Indices 0-4 should default to 255 (empty).
    for i in 0..5 {
        assert_eq!(fsm.get_category(i), Some(255));
    }
    assert_eq!(fsm.get_category(5), Some(FreeSpaceMap::bytes_to_category(1000)));
}

// ---------------------------------------------------------------------------
// update_page
// ---------------------------------------------------------------------------

#[test]
fn update_page_by_id() {
    let page_ids: Vec<PageId> = vec![100, 200, 300];
    let mut fsm = FreeSpaceMap::with_capacity(3);

    fsm.update_page(&page_ids, 200, 500);
    assert_eq!(fsm.get_category(1), Some(FreeSpaceMap::bytes_to_category(500)));

    // Unknown page id is a no-op.
    fsm.update_page(&page_ids, 999, 500);
    assert_eq!(fsm.len(), 3);
}

// ---------------------------------------------------------------------------
// get_free_bytes
// ---------------------------------------------------------------------------

#[test]
fn get_free_bytes_approximation() {
    let mut fsm = FreeSpaceMap::new();
    fsm.update(0, 1000);

    // Stored as category 31 (1000/32 = 31), approximation = 31*32 = 992
    let approx = fsm.get_free_bytes(0).unwrap();
    assert!(approx <= 1000);
    assert!(approx >= 960); // within one granularity

    assert_eq!(fsm.get_free_bytes(99), None);
}

// ---------------------------------------------------------------------------
// Simulated FSM lifecycle with heap operations
// ---------------------------------------------------------------------------

#[test]
fn fsm_lifecycle_simulation() {
    let page_ids: Vec<PageId> = vec![1, 2, 3];
    let mut fsm = FreeSpaceMap::with_capacity(3);

    // Initially all empty (8168 bytes usable on an 8KB page).
    for i in 0..3 {
        fsm.update(i, 8168);
    }

    // After inserting into page 1, free space drops.
    fsm.update(0, 4000);
    // After filling page 2, mark it full.
    fsm.update(1, 0);

    // Need 100 bytes → should find page 1 (index 0).
    assert_eq!(fsm.find_page(&page_ids, 100), Some(1));

    // Need 5000 bytes → skip page 1 (only 4000), page 2 (full), find page 3.
    assert_eq!(fsm.find_page(&page_ids, 5000), Some(3));

    // After vacuum on page 2 (index 1), space is reclaimed.
    fsm.update(1, 8168);
    // Index 0 has only 4000 bytes (category 125 < needed 157), so index 1
    // (page_id 2) is the first fit.
    assert_eq!(fsm.find_page(&page_ids, 5000), Some(2));
}
