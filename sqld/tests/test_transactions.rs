use sqld::transaction::{IsolationLevel, TransactionManager, TransactionStatus};
use sqld::transaction::transaction::{ReadEntry, WriteEntry};

// ===========================================================================
// 1. Begin transaction creates active transaction
// ===========================================================================

#[test]
fn begin_creates_active_transaction_read_committed() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin(IsolationLevel::ReadCommitted);

    assert!(txn.is_active());
    assert_eq!(txn.status, TransactionStatus::Active);
    assert_eq!(txn.isolation_level, IsolationLevel::ReadCommitted);
    assert!(txn.write_set.is_empty());
    assert!(txn.read_set.is_empty());
    assert!(txn.savepoints.is_empty());
    assert_eq!(txn.command_id, 0);
}

#[test]
fn begin_creates_active_transaction_repeatable_read() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin(IsolationLevel::RepeatableRead);

    assert!(txn.is_active());
    assert_eq!(txn.isolation_level, IsolationLevel::RepeatableRead);
}

#[test]
fn begin_creates_active_transaction_serializable() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin(IsolationLevel::Serializable);

    assert!(txn.is_active());
    assert_eq!(txn.isolation_level, IsolationLevel::Serializable);
}

// ===========================================================================
// 2. Transaction gets unique incrementing IDs
// ===========================================================================

#[test]
fn txn_ids_monotonically_increasing() {
    let mgr = TransactionManager::new();

    let txn1 = mgr.begin(IsolationLevel::ReadCommitted);
    let txn2 = mgr.begin(IsolationLevel::ReadCommitted);
    let txn3 = mgr.begin(IsolationLevel::ReadCommitted);

    assert!(txn1.txn_id < txn2.txn_id);
    assert!(txn2.txn_id < txn3.txn_id);
}

#[test]
fn txn_ids_are_unique() {
    let mgr = TransactionManager::new();
    let ids: Vec<u64> = (0..100)
        .map(|_| mgr.begin(IsolationLevel::ReadCommitted).txn_id)
        .collect();

    for i in 0..ids.len() {
        for j in (i + 1)..ids.len() {
            assert_ne!(ids[i], ids[j], "duplicate txn_id found: {}", ids[i]);
        }
    }
}

// ===========================================================================
// 3. Commit changes status to Committed
// ===========================================================================

#[test]
fn commit_changes_status_to_committed() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);
    let txn_id = txn.txn_id;

    assert!(mgr.is_active(txn_id));
    assert!(!mgr.is_committed(txn_id));

    mgr.commit(&mut txn).unwrap();

    assert_eq!(txn.status, TransactionStatus::Committed);
    assert!(!txn.is_active());
    assert!(!mgr.is_active(txn_id));
    assert!(mgr.is_committed(txn_id));
}

#[test]
fn commit_sets_commit_time() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    assert!(txn.commit_time.is_none());

    mgr.commit(&mut txn).unwrap();

    assert!(txn.commit_time.is_some());
}

// ===========================================================================
// 4. Abort changes status to Aborted
// ===========================================================================

#[test]
fn abort_changes_status_to_aborted() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);
    let txn_id = txn.txn_id;

    assert!(mgr.is_active(txn_id));

    mgr.abort(&mut txn);

    assert_eq!(txn.status, TransactionStatus::Aborted);
    assert!(!txn.is_active());
    assert!(!mgr.is_active(txn_id));
    assert!(!mgr.is_committed(txn_id));
}

// ===========================================================================
// 5. Cannot commit non-active transaction (returns Err)
// ===========================================================================

#[test]
fn double_commit_returns_error() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    mgr.commit(&mut txn).unwrap();

    let result = mgr.commit(&mut txn);
    assert!(result.is_err());
}

#[test]
fn commit_after_abort_returns_error() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    mgr.abort(&mut txn);

    let result = mgr.commit(&mut txn);
    assert!(result.is_err());
}

// ===========================================================================
// 6. Transaction tracks writes via add_write
// ===========================================================================

#[test]
fn write_set_initially_empty() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin(IsolationLevel::ReadCommitted);

    assert_eq!(txn.write_count(), 0);
    assert!(txn.write_set.is_empty());
}

#[test]
fn add_write_increments_write_count() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 10);
    assert_eq!(txn.write_count(), 1);

    txn.add_write(1, 20);
    assert_eq!(txn.write_count(), 2);

    txn.add_write(2, 10);
    assert_eq!(txn.write_count(), 3);
}

#[test]
fn add_write_records_correct_entries() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 10);
    txn.add_write(2, 20);

    assert_eq!(
        txn.write_set,
        vec![
            WriteEntry { table_id: 1, tuple_id: 10 },
            WriteEntry { table_id: 2, tuple_id: 20 },
        ]
    );
}

// ===========================================================================
// 7. Transaction tracks reads via add_read
// ===========================================================================

#[test]
fn read_set_initially_empty() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin(IsolationLevel::ReadCommitted);

    assert!(txn.read_set.is_empty());
}

#[test]
fn add_read_records_correct_entries() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_read(1, 100);
    txn.add_read(1, 200);
    txn.add_read(3, 50);

    assert_eq!(txn.read_set.len(), 3);
    assert_eq!(
        txn.read_set,
        vec![
            ReadEntry { table_id: 1, tuple_id: 100 },
            ReadEntry { table_id: 1, tuple_id: 200 },
            ReadEntry { table_id: 3, tuple_id: 50 },
        ]
    );
}

#[test]
fn mixed_read_write_tracking() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_read(1, 10);
    txn.add_write(1, 10);
    txn.add_read(2, 20);

    assert_eq!(txn.read_set.len(), 2);
    assert_eq!(txn.write_count(), 1);
}

// ===========================================================================
// 8. write_count returns correct count
// ===========================================================================

#[test]
fn write_count_zero_for_new_transaction() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin(IsolationLevel::ReadCommitted);

    assert_eq!(txn.write_count(), 0);
}

#[test]
fn write_count_matches_number_of_add_write_calls() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    for i in 0..50u64 {
        txn.add_write(i, i * 10);
        assert_eq!(txn.write_count(), (i + 1) as usize);
    }
}

// ===========================================================================
// 9. Command ID increments with next_command_id
// ===========================================================================

#[test]
fn command_id_starts_at_zero() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin(IsolationLevel::ReadCommitted);

    assert_eq!(txn.command_id, 0);
}

#[test]
fn next_command_id_returns_current_then_increments() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    assert_eq!(txn.next_command_id(), 0);
    assert_eq!(txn.next_command_id(), 1);
    assert_eq!(txn.next_command_id(), 2);
    assert_eq!(txn.command_id, 3);
}

#[test]
fn command_id_increments_many_times() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    for i in 0..1000u32 {
        assert_eq!(txn.next_command_id(), i);
    }
    assert_eq!(txn.command_id, 1000);
}

// ===========================================================================
// 10. TransactionManager tracks active set correctly
// ===========================================================================

#[test]
fn active_transaction_ids_empty_initially() {
    let mgr = TransactionManager::new();

    assert!(mgr.active_transaction_ids().is_empty());
}

#[test]
fn active_transaction_ids_tracks_begun_transactions() {
    let mgr = TransactionManager::new();

    let txn1 = mgr.begin(IsolationLevel::ReadCommitted);
    let txn2 = mgr.begin(IsolationLevel::ReadCommitted);

    let active = mgr.active_transaction_ids();
    assert_eq!(active.len(), 2);
    assert!(active.contains(&txn1.txn_id));
    assert!(active.contains(&txn2.txn_id));
}

#[test]
fn active_transaction_ids_removes_aborted() {
    let mgr = TransactionManager::new();

    let mut txn1 = mgr.begin(IsolationLevel::ReadCommitted);
    let txn2 = mgr.begin(IsolationLevel::ReadCommitted);
    let id1 = txn1.txn_id;
    let id2 = txn2.txn_id;

    mgr.abort(&mut txn1);

    let active = mgr.active_transaction_ids();
    assert_eq!(active.len(), 1);
    assert!(!active.contains(&id1));
    assert!(active.contains(&id2));
}

// ===========================================================================
// 11. TransactionManager tracks committed set correctly
// ===========================================================================

#[test]
fn committed_set_empty_initially() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin(IsolationLevel::ReadCommitted);

    assert!(!mgr.is_committed(txn.txn_id));
}

#[test]
fn committed_set_contains_committed_transaction() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);
    let id = txn.txn_id;

    mgr.commit(&mut txn).unwrap();

    assert!(mgr.is_committed(id));
}

#[test]
fn committed_set_does_not_contain_aborted_transaction() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);
    let id = txn.txn_id;

    mgr.abort(&mut txn);

    assert!(!mgr.is_committed(id));
}

#[test]
fn committed_set_tracks_multiple_commits() {
    let mgr = TransactionManager::new();
    let mut txn1 = mgr.begin(IsolationLevel::ReadCommitted);
    let mut txn2 = mgr.begin(IsolationLevel::ReadCommitted);
    let mut txn3 = mgr.begin(IsolationLevel::ReadCommitted);
    let id1 = txn1.txn_id;
    let id2 = txn2.txn_id;
    let id3 = txn3.txn_id;

    mgr.commit(&mut txn1).unwrap();
    mgr.commit(&mut txn2).unwrap();
    mgr.abort(&mut txn3);

    assert!(mgr.is_committed(id1));
    assert!(mgr.is_committed(id2));
    assert!(!mgr.is_committed(id3));
}

// ===========================================================================
// 12. After commit, txn is not in active set
// ===========================================================================

#[test]
fn active_transaction_ids_removes_committed() {
    let mgr = TransactionManager::new();

    let mut txn1 = mgr.begin(IsolationLevel::ReadCommitted);
    let txn2 = mgr.begin(IsolationLevel::ReadCommitted);
    let id1 = txn1.txn_id;
    let id2 = txn2.txn_id;

    mgr.commit(&mut txn1).unwrap();

    let active = mgr.active_transaction_ids();
    assert_eq!(active.len(), 1);
    assert!(!active.contains(&id1));
    assert!(active.contains(&id2));
}

// ===========================================================================
// 13. After abort, txn is not in active set
// ===========================================================================

#[test]
fn after_abort_txn_not_in_active_set() {
    let mgr = TransactionManager::new();

    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);
    let id = txn.txn_id;

    assert!(mgr.is_active(id));

    mgr.abort(&mut txn);

    assert!(!mgr.is_active(id));
    assert!(!mgr.active_transaction_ids().contains(&id));
}

#[test]
fn active_transaction_ids_empty_after_all_finished() {
    let mgr = TransactionManager::new();

    let mut txn1 = mgr.begin(IsolationLevel::ReadCommitted);
    let mut txn2 = mgr.begin(IsolationLevel::ReadCommitted);
    let mut txn3 = mgr.begin(IsolationLevel::ReadCommitted);

    mgr.commit(&mut txn1).unwrap();
    mgr.abort(&mut txn2);
    mgr.commit(&mut txn3).unwrap();

    assert!(mgr.active_transaction_ids().is_empty());
}

// ===========================================================================
// 14. Multiple concurrent transactions
// ===========================================================================

#[test]
fn multiple_concurrent_transactions_independent() {
    let mgr = TransactionManager::new();

    let mut txn1 = mgr.begin(IsolationLevel::ReadCommitted);
    let mut txn2 = mgr.begin(IsolationLevel::RepeatableRead);
    let mut txn3 = mgr.begin(IsolationLevel::Serializable);

    // Each has its own write set.
    txn1.add_write(1, 10);
    txn2.add_write(2, 20);
    txn2.add_write(2, 30);
    txn3.add_write(3, 40);

    assert_eq!(txn1.write_count(), 1);
    assert_eq!(txn2.write_count(), 2);
    assert_eq!(txn3.write_count(), 1);

    // All three are active.
    assert!(mgr.is_active(txn1.txn_id));
    assert!(mgr.is_active(txn2.txn_id));
    assert!(mgr.is_active(txn3.txn_id));

    // Commit one, abort another, leave one active.
    mgr.commit(&mut txn1).unwrap();
    mgr.abort(&mut txn2);

    assert!(!mgr.is_active(txn1.txn_id));
    assert!(mgr.is_committed(txn1.txn_id));

    assert!(!mgr.is_active(txn2.txn_id));
    assert!(!mgr.is_committed(txn2.txn_id));

    assert!(mgr.is_active(txn3.txn_id));
    assert!(!mgr.is_committed(txn3.txn_id));
}

#[test]
fn concurrent_transactions_different_isolation_levels() {
    let mgr = TransactionManager::new();

    let txn_rc = mgr.begin(IsolationLevel::ReadCommitted);
    let txn_rr = mgr.begin(IsolationLevel::RepeatableRead);
    let txn_si = mgr.begin(IsolationLevel::Serializable);

    assert_eq!(txn_rc.isolation_level, IsolationLevel::ReadCommitted);
    assert_eq!(txn_rr.isolation_level, IsolationLevel::RepeatableRead);
    assert_eq!(txn_si.isolation_level, IsolationLevel::Serializable);
}

#[test]
fn commit_one_does_not_affect_others() {
    let mgr = TransactionManager::new();

    let mut txn1 = mgr.begin(IsolationLevel::ReadCommitted);
    let mut txn2 = mgr.begin(IsolationLevel::ReadCommitted);

    txn1.add_write(1, 100);
    txn2.add_write(2, 200);

    mgr.commit(&mut txn1).unwrap();

    // txn2 should still be active and retain its write set.
    assert!(txn2.is_active());
    assert_eq!(txn2.write_count(), 1);
    assert_eq!(txn2.write_set[0], WriteEntry { table_id: 2, tuple_id: 200 });

    mgr.commit(&mut txn2).unwrap();
    assert!(mgr.is_committed(txn2.txn_id));
}

// ===========================================================================
// 15. pick_deadlock_victim returns smallest txn_id
// ===========================================================================

#[test]
fn pick_deadlock_victim_returns_smallest_id() {
    let mgr = TransactionManager::new();

    let result = mgr.pick_deadlock_victim(&[10, 5, 20, 3, 15]);
    assert_eq!(result, Some(3));
}

#[test]
fn pick_deadlock_victim_single_candidate() {
    let mgr = TransactionManager::new();

    let result = mgr.pick_deadlock_victim(&[42]);
    assert_eq!(result, Some(42));
}

#[test]
fn pick_deadlock_victim_empty_candidates() {
    let mgr = TransactionManager::new();

    let result = mgr.pick_deadlock_victim(&[]);
    assert_eq!(result, None);
}

#[test]
fn pick_deadlock_victim_duplicate_ids() {
    let mgr = TransactionManager::new();

    let result = mgr.pick_deadlock_victim(&[7, 7, 7]);
    assert_eq!(result, Some(7));
}

#[test]
fn pick_deadlock_victim_two_candidates() {
    let mgr = TransactionManager::new();

    let result = mgr.pick_deadlock_victim(&[100, 50]);
    assert_eq!(result, Some(50));
}

// ===========================================================================
// 16. Begin with each isolation level works
// ===========================================================================

#[test]
fn begin_with_read_committed_isolation() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin(IsolationLevel::ReadCommitted);

    assert!(txn.is_active());
    assert_eq!(txn.status, TransactionStatus::Active);
    assert_eq!(txn.isolation_level, IsolationLevel::ReadCommitted);
}

#[test]
fn begin_with_repeatable_read_isolation() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin(IsolationLevel::RepeatableRead);

    assert!(txn.is_active());
    assert_eq!(txn.status, TransactionStatus::Active);
    assert_eq!(txn.isolation_level, IsolationLevel::RepeatableRead);
}

#[test]
fn begin_with_serializable_isolation() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin(IsolationLevel::Serializable);

    assert!(txn.is_active());
    assert_eq!(txn.status, TransactionStatus::Active);
    assert_eq!(txn.isolation_level, IsolationLevel::Serializable);
}

// ===========================================================================
// Savepoints (bonus coverage for the Transaction API)
// ===========================================================================

#[test]
fn savepoint_create_and_rollback() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 10);
    txn.add_read(1, 10);
    txn.create_savepoint("sp1".to_string());

    txn.add_write(1, 20);
    txn.add_write(1, 30);
    txn.add_read(2, 50);
    assert_eq!(txn.write_count(), 3);
    assert_eq!(txn.read_set.len(), 2);

    txn.rollback_to_savepoint("sp1").unwrap();

    assert_eq!(txn.write_count(), 1);
    assert_eq!(txn.read_set.len(), 1);
    assert_eq!(txn.write_set[0], WriteEntry { table_id: 1, tuple_id: 10 });
}

#[test]
fn savepoint_release_removes_it() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());
    assert_eq!(txn.savepoints.len(), 1);

    txn.release_savepoint("sp1").unwrap();
    assert!(txn.savepoints.is_empty());
}

#[test]
fn rollback_to_nonexistent_savepoint_returns_error() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    let result = txn.rollback_to_savepoint("no_such_savepoint");
    assert!(result.is_err());
}

#[test]
fn release_nonexistent_savepoint_returns_error() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    let result = txn.release_savepoint("no_such_savepoint");
    assert!(result.is_err());
}

// ===========================================================================
// Snapshot populated on begin
// ===========================================================================

#[test]
fn snapshot_reflects_active_transactions_at_begin() {
    let mgr = TransactionManager::new();

    let txn1 = mgr.begin(IsolationLevel::ReadCommitted);
    let txn2 = mgr.begin(IsolationLevel::ReadCommitted);

    // txn2's snapshot should include txn1 as active.
    assert!(txn2.snapshot.active_txns.contains(&txn1.txn_id));
}

#[test]
fn snapshot_xmax_is_beyond_own_id() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin(IsolationLevel::ReadCommitted);

    // xmax should be >= txn_id + 1 (the next id to be assigned).
    assert!(txn.snapshot.xmax > txn.txn_id);
}
