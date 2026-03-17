use sqld::wal::{MemoryPageStore, RecoveryManager, TxnStatus, WalManager, WalRecord};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("sqld_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cleanup(dir: &std::path::PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

// ---------------------------------------------------------------------------
// 1. Multiple concurrent uncommitted transactions — only the committed one
//    survives recovery.
// ---------------------------------------------------------------------------

#[test]
fn multiple_concurrent_uncommitted_transactions() {
    let dir = test_dir("multi_concurrent_uncommitted");
    let wal = WalManager::open(&dir).unwrap();

    // Txn 1: insert into page 1 slot 0
    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 1,
        slot_index: 0,
        data: vec![10, 20, 30],
    })
    .unwrap();

    // Txn 2: insert into page 2 slot 0
    wal.append(WalRecord::Begin { txn_id: 2 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 2,
        page_id: 2,
        slot_index: 0,
        data: vec![40, 50, 60],
    })
    .unwrap();

    // Txn 3: insert into page 3 slot 0
    wal.append(WalRecord::Begin { txn_id: 3 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 3,
        page_id: 3,
        slot_index: 0,
        data: vec![70, 80, 90],
    })
    .unwrap();

    // Only txn 1 commits
    wal.commit(1).unwrap();

    // Recovery
    let mut store = MemoryPageStore::new();
    let state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // Txn 1's data should persist on page 1
    assert_eq!(store.get_slot(1, 0), Some(&vec![10, 20, 30]));

    // Txn 2 and 3 were uncommitted — their inserts must be undone
    assert_eq!(store.get_slot(2, 0), None);
    assert_eq!(store.get_slot(3, 0), None);

    // Txn 1 committed, txns 2 and 3 should be aborted after recovery
    assert_eq!(
        state.active_txn_table.get(&1).unwrap().status,
        TxnStatus::Committed
    );
    assert_eq!(
        state.active_txn_table.get(&2).unwrap().status,
        TxnStatus::Aborted
    );
    assert_eq!(
        state.active_txn_table.get(&3).unwrap().status,
        TxnStatus::Aborted
    );

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 2. Undo ordering — multiple operations by one uncommitted transaction are
//    undone in reverse order, leaving both slots empty.
// ---------------------------------------------------------------------------

#[test]
fn undo_ordering_reverse_order() {
    let dir = test_dir("undo_ordering");
    let wal = WalManager::open(&dir).unwrap();

    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();

    // Operation 1: insert slot 0
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 1,
        slot_index: 0,
        data: vec![1, 2, 3],
    })
    .unwrap();

    // Operation 2: insert slot 1
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 1,
        slot_index: 1,
        data: vec![4, 5, 6],
    })
    .unwrap();

    // Operation 3: update slot 0 (old_data matches original insert)
    wal.append(WalRecord::UpdateTuple {
        txn_id: 1,
        page_id: 1,
        slot_index: 0,
        old_data: vec![1, 2, 3],
        new_data: vec![7, 8, 9],
    })
    .unwrap();

    // NO commit — simulate crash
    wal.flush().unwrap();

    let mut store = MemoryPageStore::new();
    let state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // All three operations should be undone in reverse order:
    //   undo update slot 0 (restore to [1,2,3])
    //   undo insert slot 1 (remove it)
    //   undo insert slot 0 (remove it)
    // Both slots should be empty after full undo.
    assert_eq!(store.get_slot(1, 0), None);
    assert_eq!(store.get_slot(1, 1), None);

    // Transaction should be marked aborted
    assert_eq!(
        state.active_txn_table.get(&1).unwrap().status,
        TxnStatus::Aborted
    );

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 3. Index operations recovery — committed index insert persists, uncommitted
//    index insert is undone.
// ---------------------------------------------------------------------------

#[test]
fn index_operations_recovery() {
    let dir = test_dir("index_ops_recovery");
    let wal = WalManager::open(&dir).unwrap();

    // Txn 1: index insert, then commit
    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::IndexInsert {
        txn_id: 1,
        index_page_id: 10,
        key: vec![0xAA, 0xBB],
        tid_page: 100,
        tid_slot: 5,
    })
    .unwrap();
    wal.commit(1).unwrap();

    // Txn 2: index insert, NO commit
    wal.append(WalRecord::Begin { txn_id: 2 }).unwrap();
    wal.append(WalRecord::IndexInsert {
        txn_id: 2,
        index_page_id: 20,
        key: vec![0xCC, 0xDD],
        tid_page: 200,
        tid_slot: 7,
    })
    .unwrap();
    wal.flush().unwrap();

    let mut store = MemoryPageStore::new();
    let state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // Txn 1's index entry should persist: key + tid_page(LE) + tid_slot(LE)
    let expected_val = {
        let mut v = vec![0xAA, 0xBB];
        v.extend_from_slice(&100u32.to_le_bytes());
        v.extend_from_slice(&5u16.to_le_bytes());
        v
    };
    assert_eq!(store.get_slot(10, 0), Some(&expected_val));

    // Txn 2's index entry should be undone
    assert_eq!(store.get_slot(20, 0), None);

    assert_eq!(
        state.active_txn_table.get(&1).unwrap().status,
        TxnStatus::Committed
    );
    assert_eq!(
        state.active_txn_table.get(&2).unwrap().status,
        TxnStatus::Aborted
    );

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 4. Interleaved operations — txn 1 commits (ops redo'd), txn 2 does not
//    commit (ops undone).
// ---------------------------------------------------------------------------

#[test]
fn interleaved_operations_recovery() {
    let dir = test_dir("interleaved_ops");
    let wal = WalManager::open(&dir).unwrap();

    // Begin both transactions
    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::Begin { txn_id: 2 }).unwrap();

    // Txn 1: insert page 1 slot 0
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 1,
        slot_index: 0,
        data: vec![0xA1],
    })
    .unwrap();

    // Txn 2: insert page 2 slot 0
    wal.append(WalRecord::InsertTuple {
        txn_id: 2,
        page_id: 2,
        slot_index: 0,
        data: vec![0xB1],
    })
    .unwrap();

    // Txn 1: delete page 1 slot 0 (carries undo data for re-insert)
    wal.append(WalRecord::DeleteTuple {
        txn_id: 1,
        page_id: 1,
        slot_index: 0,
        data: vec![0xA1],
    })
    .unwrap();

    // Txn 2: update page 2 slot 0
    wal.append(WalRecord::UpdateTuple {
        txn_id: 2,
        page_id: 2,
        slot_index: 0,
        old_data: vec![0xB1],
        new_data: vec![0xB2],
    })
    .unwrap();

    // Txn 1 commits
    wal.commit(1).unwrap();
    // Txn 2 does NOT commit

    let mut store = MemoryPageStore::new();
    let state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // Txn 1 committed: insert then delete -> slot 0 on page 1 should be gone
    assert_eq!(store.get_slot(1, 0), None);

    // Txn 2 uncommitted: insert then update are both undone -> page 2 slot 0 empty
    assert_eq!(store.get_slot(2, 0), None);

    assert_eq!(
        state.active_txn_table.get(&1).unwrap().status,
        TxnStatus::Committed
    );
    assert_eq!(
        state.active_txn_table.get(&2).unwrap().status,
        TxnStatus::Aborted
    );

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 5. Already-aborted transaction — recovery recognises the explicit abort
//    and does not attempt to undo it again.
// ---------------------------------------------------------------------------

#[test]
fn already_aborted_transaction_not_undone_twice() {
    let dir = test_dir("already_aborted");
    let wal = WalManager::open(&dir).unwrap();

    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 1,
        slot_index: 0,
        data: vec![99],
    })
    .unwrap();

    // Explicitly abort — this writes an Abort record and flushes
    wal.abort(1).unwrap();

    // After the explicit abort the WAL already contains Begin, InsertTuple,
    // Abort for txn 1. Recovery's analysis phase should mark txn 1 as Aborted
    // and the undo phase should skip it entirely (no double undo).
    let mut store = MemoryPageStore::new();
    let state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // The insert was redo'd during the redo phase (it is part of the WAL and
    // the page LSN starts at 0), but the undo phase should NOT run for an
    // already-aborted transaction. The net effect depends on whether the abort
    // was a clean runtime abort (data already cleaned up) vs crash abort. In
    // this scenario the abort was explicit, so recovery treats it as already
    // handled. The key assertion: the txn is Aborted in the recovery state.
    assert_eq!(
        state.active_txn_table.get(&1).unwrap().status,
        TxnStatus::Aborted
    );

    // Since the undo phase skips already-aborted transactions, the redo'd
    // insert remains in the page store. This is expected because in a real
    // system the runtime abort would have already cleaned up the page before
    // writing the Abort record. The redo phase faithfully replays what was on
    // disk.
    //
    // The critical invariant: the undo phase did NOT attempt to undo txn 1
    // again (which could cause errors or double-undo corruption). We verify
    // this indirectly by checking that no extra CLR or Abort records were
    // written for txn 1 beyond what was already in the WAL.
    let entries = wal.read_all_entries().unwrap();
    let abort_count = entries
        .iter()
        .filter(|e| matches!(e.record, WalRecord::Abort { txn_id: 1 }))
        .count();
    // Exactly one Abort record: the explicit one we wrote. Recovery should not
    // have appended a second one.
    assert_eq!(abort_count, 1, "recovery must not write a duplicate Abort");

    let clr_count = entries
        .iter()
        .filter(|e| matches!(e.record, WalRecord::Clr { txn_id: 1, .. }))
        .count();
    assert_eq!(
        clr_count, 0,
        "recovery must not write CLRs for an already-aborted txn"
    );

    cleanup(&dir);
}
