use std::path::PathBuf;

use sqld::wal::recovery::{MemoryPageStore, PageStore, RecoveryManager};
use sqld::wal::{WalEntry, WalManager, WalRecord};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sqld_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

/// Build a sequence of WalEntry values with monotonically increasing LSNs.
///
/// Each entry is assigned an LSN equal to its 1-based index (1, 2, 3, ...)
/// and a prev_lsn of 0 (sufficient for redo-only tests where the undo chain
/// is not exercised).
fn make_entries(records: Vec<WalRecord>) -> Vec<WalEntry> {
    records
        .into_iter()
        .enumerate()
        .map(|(i, rec)| WalEntry::new((i + 1) as u64, 0, rec))
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Basic redo: begin txn, insert a tuple, commit.
/// After recovery the page store should contain the inserted data.
#[test]
fn test_basic_redo() {
    let dir = test_dir("basic_redo");
    let wal = WalManager::open(&dir).unwrap();

    let txn_id = 1;
    let data = vec![10, 20, 30];

    wal.append(WalRecord::Begin { txn_id }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id,
        page_id: 1,
        slot_index: 0,
        data: data.clone(),
    })
    .unwrap();
    wal.commit(txn_id).unwrap();

    let mut store = MemoryPageStore::new();
    let _state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // The inserted tuple should be present at page 1, slot 0.
    let slot_data = store.get_slot(1, 0).expect("slot 0 on page 1 should exist");
    assert_eq!(slot_data, &data, "slot data should match the inserted tuple");

    cleanup(&dir);
}

/// Redo idempotency: if the page LSN is already >= the record LSN the redo
/// phase must skip that record.
///
/// We achieve this by first applying the records through redo to populate
/// the store, then running redo a second time against the same entries.
/// We verify the store state is identical (not double-applied) by checking
/// the slot data remains unchanged.  Additionally, we artificially bump the
/// page LSN above all record LSNs and verify redo produces no changes.
#[test]
fn test_redo_idempotency() {
    let entries = make_entries(vec![
        WalRecord::Begin { txn_id: 1 },
        WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 1,
            slot_index: 0,
            data: vec![0xAA, 0xBB],
        },
        WalRecord::Commit { txn_id: 1 },
    ]);

    let state = RecoveryManager::analysis(&entries, 0);
    let mut store = MemoryPageStore::new();

    // First redo — populates the store.
    RecoveryManager::redo(&entries, &state, &mut store);
    assert_eq!(
        store.get_slot(1, 0).unwrap(),
        &vec![0xAA, 0xBB],
        "first redo should insert the tuple"
    );

    // Second redo — page_lsn is already >= every record LSN, so nothing
    // should change.
    RecoveryManager::redo(&entries, &state, &mut store);
    assert_eq!(
        store.get_slot(1, 0).unwrap(),
        &vec![0xAA, 0xBB],
        "second redo should be a no-op (idempotent)"
    );

    // Now create a fresh store and pre-set its page LSN above all record
    // LSNs by applying a dummy insert at a very high LSN.
    let mut store2 = MemoryPageStore::new();
    store2.redo_record(
        1,
        9999,
        &WalRecord::InsertTuple {
            txn_id: 0,
            page_id: 1,
            slot_index: 5,
            data: vec![0xFF],
        },
    );
    // Page 1 now has page_lsn = 9999, with slot 5 = [0xFF].

    // Redo should skip all records for page 1 because page_lsn (9999) >= every
    // record LSN (1, 2, 3).
    RecoveryManager::redo(&entries, &state, &mut store2);
    assert!(
        store2.get_slot(1, 0).is_none(),
        "slot 0 should NOT be created — redo was skipped due to high page_lsn"
    );
    assert_eq!(
        store2.get_slot(1, 5).unwrap(),
        &vec![0xFF],
        "pre-existing slot 5 should remain untouched"
    );
}

/// Redo with multiple pages: insert into page 1, insert into page 2,
/// delete from page 1, update page 2 — verify the final state.
#[test]
fn test_redo_multiple_pages() {
    let entries = make_entries(vec![
        WalRecord::Begin { txn_id: 1 },
        // Insert into page 1 slot 0
        WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 1,
            slot_index: 0,
            data: vec![1, 2, 3],
        },
        // Insert into page 2 slot 0
        WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 2,
            slot_index: 0,
            data: vec![4, 5, 6],
        },
        // Delete page 1 slot 0
        WalRecord::DeleteTuple {
            txn_id: 1,
            page_id: 1,
            slot_index: 0,
            data: vec![1, 2, 3],
        },
        // Update page 2 slot 0
        WalRecord::UpdateTuple {
            txn_id: 1,
            page_id: 2,
            slot_index: 0,
            old_data: vec![4, 5, 6],
            new_data: vec![7, 8, 9],
        },
        WalRecord::Commit { txn_id: 1 },
    ]);

    let state = RecoveryManager::analysis(&entries, 0);
    let mut store = MemoryPageStore::new();
    RecoveryManager::redo(&entries, &state, &mut store);

    // Page 1 slot 0 was inserted then deleted — should be gone.
    assert!(
        store.get_slot(1, 0).is_none(),
        "page 1 slot 0 should be deleted"
    );

    // Page 2 slot 0 was inserted then updated to [7,8,9].
    let p2_data = store.get_slot(2, 0).expect("page 2 slot 0 should exist");
    assert_eq!(p2_data, &vec![7, 8, 9], "page 2 slot 0 should have updated data");
}

/// Redo skips non-dirty pages: if a page is removed from the dirty page
/// table before redo, its records should not be applied.
#[test]
fn test_redo_skips_non_dirty_pages() {
    let entries = make_entries(vec![
        WalRecord::Begin { txn_id: 1 },
        WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 10,
            slot_index: 0,
            data: vec![0xDE, 0xAD],
        },
        WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 20,
            slot_index: 0,
            data: vec![0xBE, 0xEF],
        },
        WalRecord::Commit { txn_id: 1 },
    ]);

    let mut state = RecoveryManager::analysis(&entries, 0);

    // Both page 10 and page 20 should be in the dirty page table after
    // analysis.
    assert!(
        state.dirty_page_table.contains_key(&10),
        "page 10 should be dirty after analysis"
    );
    assert!(
        state.dirty_page_table.contains_key(&20),
        "page 20 should be dirty after analysis"
    );

    // Remove page 10 from the dirty page table before running redo.
    state.dirty_page_table.remove(&10);

    let mut store = MemoryPageStore::new();
    RecoveryManager::redo(&entries, &state, &mut store);

    // Page 10 should NOT have been touched.
    assert!(
        store.get_slot(10, 0).is_none(),
        "page 10 should not be redone because it was removed from the dirty page table"
    );

    // Page 20 should have been redone normally.
    let p20_data = store
        .get_slot(20, 0)
        .expect("page 20 slot 0 should exist after redo");
    assert_eq!(p20_data, &vec![0xBE, 0xEF]);
}

/// Redo of update: begin, insert, update the same slot, commit.
/// After recovery the slot should contain the final (updated) data.
#[test]
fn test_redo_of_update() {
    let dir = test_dir("redo_update");
    let wal = WalManager::open(&dir).unwrap();

    let txn_id = 42;
    let initial = vec![1, 1, 1, 1];
    let updated = vec![2, 2, 2, 2];

    wal.append(WalRecord::Begin { txn_id }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id,
        page_id: 5,
        slot_index: 3,
        data: initial.clone(),
    })
    .unwrap();
    wal.append(WalRecord::UpdateTuple {
        txn_id,
        page_id: 5,
        slot_index: 3,
        old_data: initial.clone(),
        new_data: updated.clone(),
    })
    .unwrap();
    wal.commit(txn_id).unwrap();

    let mut store = MemoryPageStore::new();
    let _state = RecoveryManager::recover(&wal, &mut store).unwrap();

    let slot_data = store
        .get_slot(5, 3)
        .expect("page 5 slot 3 should exist after recovery");
    assert_eq!(
        slot_data, &updated,
        "slot should contain the final updated data, not the initial insert data"
    );

    cleanup(&dir);
}
