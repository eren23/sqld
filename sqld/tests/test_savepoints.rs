use sqld::transaction::{IsolationLevel, TransactionManager};
use sqld::transaction::transaction::{ReadEntry, WriteEntry};

// ===========================================================================
// 1. Create savepoint records position
// ===========================================================================

#[test]
fn create_savepoint_records_write_set_position() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 10);
    txn.add_write(1, 20);
    txn.create_savepoint("sp1".to_string());

    assert_eq!(txn.savepoints.len(), 1);
    assert_eq!(txn.savepoints[0].name, "sp1");
    assert_eq!(txn.savepoints[0].write_set_position, 2);
}

#[test]
fn create_savepoint_records_read_set_position() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_read(1, 10);
    txn.add_read(1, 20);
    txn.add_read(2, 30);
    txn.create_savepoint("sp1".to_string());

    assert_eq!(txn.savepoints[0].read_set_position, 3);
}

#[test]
fn create_savepoint_at_empty_sets_records_zero() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());

    assert_eq!(txn.savepoints[0].write_set_position, 0);
    assert_eq!(txn.savepoints[0].read_set_position, 0);
}

#[test]
fn create_savepoint_records_both_positions_accurately() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 10);
    txn.add_read(1, 100);
    txn.add_write(2, 20);
    txn.add_read(2, 200);
    txn.add_read(3, 300);
    txn.create_savepoint("sp1".to_string());

    assert_eq!(txn.savepoints[0].write_set_position, 2);
    assert_eq!(txn.savepoints[0].read_set_position, 3);
}

// ===========================================================================
// 2. Rollback to savepoint truncates write_set
// ===========================================================================

#[test]
fn rollback_to_savepoint_truncates_write_set() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 10);
    txn.create_savepoint("sp1".to_string());

    txn.add_write(1, 20);
    txn.add_write(1, 30);
    assert_eq!(txn.write_count(), 3);

    txn.rollback_to_savepoint("sp1").unwrap();

    assert_eq!(txn.write_count(), 1);
    assert_eq!(
        txn.write_set,
        vec![WriteEntry { table_id: 1, tuple_id: 10 }]
    );
}

#[test]
fn rollback_to_savepoint_at_zero_clears_write_set() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());

    txn.add_write(1, 10);
    txn.add_write(1, 20);
    assert_eq!(txn.write_count(), 2);

    txn.rollback_to_savepoint("sp1").unwrap();

    assert_eq!(txn.write_count(), 0);
    assert!(txn.write_set.is_empty());
}

// ===========================================================================
// 3. Rollback to savepoint truncates read_set
// ===========================================================================

#[test]
fn rollback_to_savepoint_truncates_read_set() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_read(1, 10);
    txn.add_read(1, 20);
    txn.create_savepoint("sp1".to_string());

    txn.add_read(2, 30);
    txn.add_read(2, 40);
    txn.add_read(3, 50);
    assert_eq!(txn.read_set.len(), 5);

    txn.rollback_to_savepoint("sp1").unwrap();

    assert_eq!(txn.read_set.len(), 2);
    assert_eq!(
        txn.read_set,
        vec![
            ReadEntry { table_id: 1, tuple_id: 10 },
            ReadEntry { table_id: 1, tuple_id: 20 },
        ]
    );
}

#[test]
fn rollback_to_savepoint_at_zero_clears_read_set() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());

    txn.add_read(1, 10);
    assert_eq!(txn.read_set.len(), 1);

    txn.rollback_to_savepoint("sp1").unwrap();

    assert!(txn.read_set.is_empty());
}

#[test]
fn rollback_truncates_both_write_and_read_sets() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 10);
    txn.add_read(1, 100);
    txn.create_savepoint("sp1".to_string());

    txn.add_write(2, 20);
    txn.add_read(2, 200);
    txn.add_write(3, 30);
    txn.add_read(3, 300);

    txn.rollback_to_savepoint("sp1").unwrap();

    assert_eq!(txn.write_count(), 1);
    assert_eq!(txn.read_set.len(), 1);
    assert_eq!(txn.write_set[0], WriteEntry { table_id: 1, tuple_id: 10 });
    assert_eq!(txn.read_set[0], ReadEntry { table_id: 1, tuple_id: 100 });
}

// ===========================================================================
// 4. Rollback to savepoint removes nested savepoints created after it
// ===========================================================================

#[test]
fn rollback_removes_nested_savepoints_after_target() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());
    txn.add_write(1, 10);
    txn.create_savepoint("sp2".to_string());
    txn.add_write(1, 20);
    txn.create_savepoint("sp3".to_string());
    txn.add_write(1, 30);

    assert_eq!(txn.savepoints.len(), 3);

    txn.rollback_to_savepoint("sp1").unwrap();

    // sp2 and sp3 should be gone; sp1 should remain.
    assert_eq!(txn.savepoints.len(), 1);
    assert_eq!(txn.savepoints[0].name, "sp1");
}

#[test]
fn rollback_to_middle_removes_only_later_savepoints() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());
    txn.add_write(1, 10);
    txn.create_savepoint("sp2".to_string());
    txn.add_write(1, 20);
    txn.create_savepoint("sp3".to_string());
    txn.add_write(1, 30);
    txn.create_savepoint("sp4".to_string());
    txn.add_write(1, 40);

    txn.rollback_to_savepoint("sp2").unwrap();

    // sp1 and sp2 should remain; sp3 and sp4 removed.
    assert_eq!(txn.savepoints.len(), 2);
    assert_eq!(txn.savepoints[0].name, "sp1");
    assert_eq!(txn.savepoints[1].name, "sp2");
}

// ===========================================================================
// 5. Rollback to savepoint keeps the savepoint itself (can rollback again)
// ===========================================================================

#[test]
fn rollback_keeps_savepoint_for_reuse() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());

    // First batch of writes.
    txn.add_write(1, 10);
    txn.add_write(1, 20);
    txn.rollback_to_savepoint("sp1").unwrap();
    assert_eq!(txn.write_count(), 0);

    // Second batch of writes, then rollback again.
    txn.add_write(2, 30);
    txn.add_write(2, 40);
    txn.add_write(2, 50);
    assert_eq!(txn.write_count(), 3);

    txn.rollback_to_savepoint("sp1").unwrap();
    assert_eq!(txn.write_count(), 0);

    // Savepoint should still exist.
    assert_eq!(txn.savepoints.len(), 1);
    assert_eq!(txn.savepoints[0].name, "sp1");
}

#[test]
fn rollback_keeps_savepoint_read_set_reuse() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_read(1, 100);
    txn.create_savepoint("sp1".to_string());

    txn.add_read(2, 200);
    txn.rollback_to_savepoint("sp1").unwrap();
    assert_eq!(txn.read_set.len(), 1);

    txn.add_read(3, 300);
    txn.add_read(4, 400);
    txn.rollback_to_savepoint("sp1").unwrap();
    assert_eq!(txn.read_set.len(), 1);
    assert_eq!(txn.read_set[0], ReadEntry { table_id: 1, tuple_id: 100 });
}

// ===========================================================================
// 6. Release savepoint removes the marker
// ===========================================================================

#[test]
fn release_savepoint_removes_marker() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 10);
    txn.create_savepoint("sp1".to_string());
    txn.add_write(1, 20);

    assert_eq!(txn.savepoints.len(), 1);

    txn.release_savepoint("sp1").unwrap();

    assert!(txn.savepoints.is_empty());
    // Release does NOT truncate write/read sets.
    assert_eq!(txn.write_count(), 2);
}

#[test]
fn release_savepoint_does_not_affect_data() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_read(1, 10);
    txn.add_write(1, 10);
    txn.create_savepoint("sp1".to_string());
    txn.add_read(2, 20);
    txn.add_write(2, 20);

    txn.release_savepoint("sp1").unwrap();

    // All data remains intact after release.
    assert_eq!(txn.write_count(), 2);
    assert_eq!(txn.read_set.len(), 2);
}

#[test]
fn release_already_released_savepoint_returns_error() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());
    txn.release_savepoint("sp1").unwrap();

    let result = txn.release_savepoint("sp1");
    assert!(result.is_err());
}

// ===========================================================================
// 7. Release savepoint removes nested savepoints after it
// ===========================================================================

#[test]
fn release_savepoint_removes_nested_after_it() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());
    txn.create_savepoint("sp2".to_string());
    txn.create_savepoint("sp3".to_string());

    assert_eq!(txn.savepoints.len(), 3);

    txn.release_savepoint("sp1").unwrap();

    // sp1, sp2, sp3 all removed (truncate to idx of sp1).
    assert!(txn.savepoints.is_empty());
}

#[test]
fn release_middle_savepoint_removes_it_and_later_ones() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());
    txn.create_savepoint("sp2".to_string());
    txn.create_savepoint("sp3".to_string());
    txn.create_savepoint("sp4".to_string());

    assert_eq!(txn.savepoints.len(), 4);

    txn.release_savepoint("sp2").unwrap();

    // sp1 should remain; sp2, sp3, sp4 should be gone.
    assert_eq!(txn.savepoints.len(), 1);
    assert_eq!(txn.savepoints[0].name, "sp1");
}

#[test]
fn release_innermost_savepoint_keeps_outer_ones() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());
    txn.create_savepoint("sp2".to_string());
    txn.create_savepoint("sp3".to_string());

    txn.release_savepoint("sp3").unwrap();

    assert_eq!(txn.savepoints.len(), 2);
    assert_eq!(txn.savepoints[0].name, "sp1");
    assert_eq!(txn.savepoints[1].name, "sp2");
}

#[test]
fn release_savepoint_preserves_all_data() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 10);
    txn.create_savepoint("sp1".to_string());
    txn.add_write(2, 20);
    txn.create_savepoint("sp2".to_string());
    txn.add_write(3, 30);

    txn.release_savepoint("sp1").unwrap();

    // All writes preserved, all savepoint markers removed.
    assert_eq!(txn.write_count(), 3);
    assert!(txn.savepoints.is_empty());
}

// ===========================================================================
// 8. Nested savepoints: sp1 -> writes -> sp2 -> writes -> rollback sp2
//    -> only sp2 writes removed
// ===========================================================================

#[test]
fn nested_rollback_removes_only_inner_writes() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());
    txn.add_write(1, 10);
    txn.add_write(1, 20);

    txn.create_savepoint("sp2".to_string());
    txn.add_write(2, 30);
    txn.add_write(2, 40);

    assert_eq!(txn.write_count(), 4);

    txn.rollback_to_savepoint("sp2").unwrap();

    // Only sp2's writes (table 2) should be removed.
    assert_eq!(txn.write_count(), 2);
    assert_eq!(
        txn.write_set,
        vec![
            WriteEntry { table_id: 1, tuple_id: 10 },
            WriteEntry { table_id: 1, tuple_id: 20 },
        ]
    );

    // sp1 and sp2 should both remain.
    assert_eq!(txn.savepoints.len(), 2);
    assert_eq!(txn.savepoints[0].name, "sp1");
    assert_eq!(txn.savepoints[1].name, "sp2");
}

#[test]
fn nested_rollback_removes_only_inner_reads() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());
    txn.add_read(1, 10);

    txn.create_savepoint("sp2".to_string());
    txn.add_read(2, 20);
    txn.add_read(2, 30);

    assert_eq!(txn.read_set.len(), 3);

    txn.rollback_to_savepoint("sp2").unwrap();

    assert_eq!(txn.read_set.len(), 1);
    assert_eq!(
        txn.read_set,
        vec![ReadEntry { table_id: 1, tuple_id: 10 }]
    );
}

#[test]
fn nested_rollback_sp2_then_continue_and_rollback_sp1() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 1); // before sp1
    txn.create_savepoint("sp1".to_string());

    txn.add_write(1, 2); // between sp1 and sp2
    txn.create_savepoint("sp2".to_string());

    txn.add_write(1, 3); // after sp2

    // Rollback sp2: removes write(1,3).
    txn.rollback_to_savepoint("sp2").unwrap();
    assert_eq!(txn.write_count(), 2);

    // Add more work after rollback.
    txn.add_write(1, 4);
    txn.add_write(1, 5);
    assert_eq!(txn.write_count(), 4);

    // Rollback sp1: removes writes(1,2), (1,4), (1,5).
    txn.rollback_to_savepoint("sp1").unwrap();
    assert_eq!(txn.write_count(), 1);
    assert_eq!(txn.write_set[0], WriteEntry { table_id: 1, tuple_id: 1 });
}

// ===========================================================================
// 9. Deeply nested: sp1 -> sp2 -> sp3 -> rollback sp1 -> all writes after
//    sp1 gone
// ===========================================================================

#[test]
fn deeply_nested_rollback_to_earliest_removes_all_later_work() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(10, 1); // before any savepoint
    txn.add_read(10, 1);

    txn.create_savepoint("sp1".to_string());
    txn.add_write(10, 2);
    txn.add_read(10, 2);

    txn.create_savepoint("sp2".to_string());
    txn.add_write(10, 3);
    txn.add_read(10, 3);

    txn.create_savepoint("sp3".to_string());
    txn.add_write(10, 4);
    txn.add_write(10, 5);
    txn.add_read(10, 4);
    txn.add_read(10, 5);

    assert_eq!(txn.write_count(), 5);
    assert_eq!(txn.read_set.len(), 5);
    assert_eq!(txn.savepoints.len(), 3);

    txn.rollback_to_savepoint("sp1").unwrap();

    // Only the write/read before sp1 should remain.
    assert_eq!(txn.write_count(), 1);
    assert_eq!(txn.write_set[0], WriteEntry { table_id: 10, tuple_id: 1 });
    assert_eq!(txn.read_set.len(), 1);
    assert_eq!(txn.read_set[0], ReadEntry { table_id: 10, tuple_id: 1 });

    // sp2 and sp3 removed; sp1 kept.
    assert_eq!(txn.savepoints.len(), 1);
    assert_eq!(txn.savepoints[0].name, "sp1");
}

#[test]
fn deeply_nested_rollback_with_many_writes_per_level() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    // 5 writes before sp1.
    for i in 0..5 {
        txn.add_write(1, i);
    }
    txn.create_savepoint("sp1".to_string());

    // 3 writes between sp1 and sp2.
    for i in 10..13 {
        txn.add_write(2, i);
    }
    txn.create_savepoint("sp2".to_string());

    // 7 writes between sp2 and sp3.
    for i in 20..27 {
        txn.add_write(3, i);
    }
    txn.create_savepoint("sp3".to_string());

    // 2 more writes after sp3.
    txn.add_write(4, 100);
    txn.add_write(4, 101);
    assert_eq!(txn.write_count(), 17);

    // Rollback all the way to sp1.
    txn.rollback_to_savepoint("sp1").unwrap();

    assert_eq!(txn.write_count(), 5);
    for i in 0..5u64 {
        assert_eq!(txn.write_set[i as usize], WriteEntry { table_id: 1, tuple_id: i });
    }

    assert_eq!(txn.savepoints.len(), 1);
    assert_eq!(txn.savepoints[0].name, "sp1");
}

// ===========================================================================
// 10. Rollback to non-existent savepoint returns error
// ===========================================================================

#[test]
fn rollback_to_nonexistent_savepoint_returns_error() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    let result = txn.rollback_to_savepoint("does_not_exist");
    assert!(result.is_err());
}

#[test]
fn rollback_to_nonexistent_savepoint_with_others_present() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());
    txn.add_write(1, 10);

    let result = txn.rollback_to_savepoint("sp_other");
    assert!(result.is_err());

    // Existing savepoint and data should be untouched.
    assert_eq!(txn.savepoints.len(), 1);
    assert_eq!(txn.savepoints[0].name, "sp1");
    assert_eq!(txn.write_count(), 1);
}

#[test]
fn rollback_to_nonexistent_on_empty_savepoint_stack() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 10);
    txn.add_read(1, 100);

    let result = txn.rollback_to_savepoint("anything");
    assert!(result.is_err());

    // Data should be untouched.
    assert_eq!(txn.write_count(), 1);
    assert_eq!(txn.read_set.len(), 1);
}

// ===========================================================================
// 11. Release non-existent savepoint returns error
// ===========================================================================

#[test]
fn release_nonexistent_savepoint_returns_error() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    let result = txn.release_savepoint("no_such");
    assert!(result.is_err());
}

#[test]
fn release_nonexistent_savepoint_with_others_present() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());

    let result = txn.release_savepoint("sp_other");
    assert!(result.is_err());

    // Existing savepoint should be untouched.
    assert_eq!(txn.savepoints.len(), 1);
}

#[test]
fn release_nonexistent_on_empty_savepoint_stack() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    let result = txn.release_savepoint("anything");
    assert!(result.is_err());
}

// ===========================================================================
// 12. Double rollback to same savepoint (second rollback is valid, no-op
//     on data)
// ===========================================================================

#[test]
fn double_rollback_to_same_savepoint_is_valid() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 1);
    txn.create_savepoint("sp1".to_string());

    txn.add_write(1, 2);
    txn.add_write(1, 3);

    // First rollback: removes writes after sp1.
    txn.rollback_to_savepoint("sp1").unwrap();
    assert_eq!(txn.write_count(), 1);

    // Second rollback: savepoint still exists, no additional data to remove.
    txn.rollback_to_savepoint("sp1").unwrap();
    assert_eq!(txn.write_count(), 1);
    assert_eq!(txn.write_set[0], WriteEntry { table_id: 1, tuple_id: 1 });

    // Savepoint is still there.
    assert_eq!(txn.savepoints.len(), 1);
    assert_eq!(txn.savepoints[0].name, "sp1");
}

#[test]
fn double_rollback_with_intermediate_writes() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());

    txn.add_write(1, 10);
    txn.rollback_to_savepoint("sp1").unwrap();
    assert_eq!(txn.write_count(), 0);

    // Add more writes between rollbacks.
    txn.add_write(2, 20);
    txn.add_write(2, 30);
    txn.rollback_to_savepoint("sp1").unwrap();
    assert_eq!(txn.write_count(), 0);
}

#[test]
fn triple_rollback_to_same_savepoint() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 10);
    txn.add_read(1, 100);
    txn.create_savepoint("sp1".to_string());

    // Batch 1.
    txn.add_write(2, 20);
    txn.add_read(2, 200);
    txn.rollback_to_savepoint("sp1").unwrap();
    assert_eq!(txn.write_count(), 1);
    assert_eq!(txn.read_set.len(), 1);

    // Batch 2.
    txn.add_write(3, 30);
    txn.add_write(4, 40);
    txn.add_read(3, 300);
    txn.rollback_to_savepoint("sp1").unwrap();
    assert_eq!(txn.write_count(), 1);
    assert_eq!(txn.read_set.len(), 1);

    // Batch 3.
    txn.add_write(5, 50);
    txn.rollback_to_savepoint("sp1").unwrap();
    assert_eq!(txn.write_count(), 1);
    assert_eq!(txn.read_set.len(), 1);

    // The original data before sp1 is always preserved.
    assert_eq!(txn.write_set[0], WriteEntry { table_id: 1, tuple_id: 10 });
    assert_eq!(txn.read_set[0], ReadEntry { table_id: 1, tuple_id: 100 });

    // Savepoint persists.
    assert_eq!(txn.savepoints.len(), 1);
    assert_eq!(txn.savepoints[0].name, "sp1");
}

// ===========================================================================
// 13. Savepoint after rollback: create new savepoint after rolling back
// ===========================================================================

#[test]
fn create_savepoint_after_rollback() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 10);
    txn.create_savepoint("sp1".to_string());

    txn.add_write(1, 20);
    txn.rollback_to_savepoint("sp1").unwrap();

    // Now create a new savepoint after rolling back.
    txn.add_write(2, 30);
    txn.create_savepoint("sp2".to_string());
    txn.add_write(2, 40);

    assert_eq!(txn.write_count(), 3); // write(1,10), write(2,30), write(2,40)
    assert_eq!(txn.savepoints.len(), 2); // sp1 and sp2

    // Rollback to sp2 should only remove write(2,40).
    txn.rollback_to_savepoint("sp2").unwrap();
    assert_eq!(txn.write_count(), 2);
    assert_eq!(
        txn.write_set,
        vec![
            WriteEntry { table_id: 1, tuple_id: 10 },
            WriteEntry { table_id: 2, tuple_id: 30 },
        ]
    );
}

#[test]
fn create_savepoint_after_rollback_records_correct_position() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 1);
    txn.add_write(1, 2);
    txn.add_write(1, 3);
    txn.create_savepoint("sp1".to_string());

    txn.add_write(1, 4);
    txn.add_write(1, 5);
    txn.rollback_to_savepoint("sp1").unwrap();

    assert_eq!(txn.write_count(), 3);

    // New savepoint should record position 3 (current write_set length).
    txn.create_savepoint("sp_new".to_string());
    assert_eq!(txn.savepoints.last().unwrap().write_set_position, 3);
}

#[test]
fn create_savepoint_after_full_rollback_to_outer() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());
    txn.add_write(1, 10);
    txn.create_savepoint("sp2".to_string());
    txn.add_write(2, 20);
    txn.create_savepoint("sp3".to_string());
    txn.add_write(3, 30);

    // Roll all the way back to sp1.
    txn.rollback_to_savepoint("sp1").unwrap();
    assert_eq!(txn.write_count(), 0);
    assert_eq!(txn.savepoints.len(), 1);

    // Create new work and new savepoint.
    txn.add_write(10, 100);
    txn.create_savepoint("sp_new".to_string());
    txn.add_write(11, 110);

    assert_eq!(txn.savepoints.len(), 2);
    assert_eq!(txn.savepoints[1].name, "sp_new");
    assert_eq!(txn.savepoints[1].write_set_position, 1);

    txn.rollback_to_savepoint("sp_new").unwrap();
    assert_eq!(txn.write_count(), 1);
    assert_eq!(txn.write_set[0], WriteEntry { table_id: 10, tuple_id: 100 });
}

// ===========================================================================
// 14. Savepoint with commit: savepoints don't affect commit
// ===========================================================================

#[test]
fn savepoints_do_not_affect_commit() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 10);
    txn.create_savepoint("sp1".to_string());
    txn.add_write(1, 20);
    txn.create_savepoint("sp2".to_string());
    txn.add_write(1, 30);

    // Commit with active savepoints -- should succeed.
    mgr.commit(&mut txn).unwrap();

    assert!(mgr.is_committed(txn.txn_id));
    assert!(!txn.is_active());
    // Write set preserved (commit does not clear it).
    assert_eq!(txn.write_count(), 3);
}

#[test]
fn commit_after_rollback_preserves_remaining_writes() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 10);
    txn.create_savepoint("sp1".to_string());
    txn.add_write(1, 20);
    txn.add_write(1, 30);

    txn.rollback_to_savepoint("sp1").unwrap();
    assert_eq!(txn.write_count(), 1);

    mgr.commit(&mut txn).unwrap();

    assert!(mgr.is_committed(txn.txn_id));
    assert_eq!(txn.write_count(), 1);
    assert_eq!(txn.write_set[0], WriteEntry { table_id: 1, tuple_id: 10 });
}

#[test]
fn commit_after_release_succeeds() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.create_savepoint("sp1".to_string());
    txn.add_write(1, 10);
    txn.release_savepoint("sp1").unwrap();

    mgr.commit(&mut txn).unwrap();

    assert!(mgr.is_committed(txn.txn_id));
    assert_eq!(txn.write_count(), 1);
}

#[test]
fn commit_after_multiple_savepoint_operations() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    txn.add_write(1, 10);
    txn.create_savepoint("sp1".to_string());
    txn.add_write(2, 20);
    txn.create_savepoint("sp2".to_string());
    txn.add_write(3, 30);

    // Rollback sp2, release sp1.
    txn.rollback_to_savepoint("sp2").unwrap();
    txn.release_savepoint("sp1").unwrap();

    txn.add_write(4, 40);

    mgr.commit(&mut txn).unwrap();

    assert!(mgr.is_committed(txn.txn_id));
    assert_eq!(txn.write_count(), 3);
    assert_eq!(
        txn.write_set,
        vec![
            WriteEntry { table_id: 1, tuple_id: 10 },
            WriteEntry { table_id: 2, tuple_id: 20 },
            WriteEntry { table_id: 4, tuple_id: 40 },
        ]
    );
}

// ===========================================================================
// 15. Full lifecycle: begin -> savepoint -> writes -> savepoint -> writes
//     -> rollback -> commit
// ===========================================================================

#[test]
fn full_lifecycle_begin_savepoint_writes_rollback_commit() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);
    let txn_id = txn.txn_id;

    assert!(txn.is_active());
    assert!(mgr.is_active(txn_id));

    // Phase 1: initial writes.
    txn.add_write(1, 100);
    txn.add_read(1, 100);

    // Phase 2: first savepoint.
    txn.create_savepoint("before_batch".to_string());

    txn.add_write(1, 200);
    txn.add_write(1, 300);
    txn.add_read(1, 200);
    txn.add_read(1, 300);

    // Phase 3: nested savepoint.
    txn.create_savepoint("inner".to_string());

    txn.add_write(2, 400);
    txn.add_write(2, 500);
    txn.add_read(2, 400);

    assert_eq!(txn.write_count(), 5);
    assert_eq!(txn.read_set.len(), 4);
    assert_eq!(txn.savepoints.len(), 2);

    // Phase 4: rollback to first savepoint, discarding both batches after it.
    txn.rollback_to_savepoint("before_batch").unwrap();

    assert_eq!(txn.write_count(), 1);
    assert_eq!(txn.write_set[0], WriteEntry { table_id: 1, tuple_id: 100 });
    assert_eq!(txn.read_set.len(), 1);
    assert_eq!(txn.read_set[0], ReadEntry { table_id: 1, tuple_id: 100 });

    // Inner savepoint removed; before_batch kept.
    assert_eq!(txn.savepoints.len(), 1);
    assert_eq!(txn.savepoints[0].name, "before_batch");

    // Phase 5: do some final work and commit.
    txn.add_write(3, 600);
    txn.add_read(3, 600);

    mgr.commit(&mut txn).unwrap();

    assert!(!txn.is_active());
    assert!(mgr.is_committed(txn_id));
    assert!(!mgr.is_active(txn_id));

    assert_eq!(txn.write_count(), 2);
    assert_eq!(
        txn.write_set,
        vec![
            WriteEntry { table_id: 1, tuple_id: 100 },
            WriteEntry { table_id: 3, tuple_id: 600 },
        ]
    );
    assert_eq!(
        txn.read_set,
        vec![
            ReadEntry { table_id: 1, tuple_id: 100 },
            ReadEntry { table_id: 3, tuple_id: 600 },
        ]
    );
}

#[test]
fn full_lifecycle_multiple_rollbacks_and_new_savepoints() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    // Step 1: write, savepoint, write, rollback.
    txn.add_write(1, 1);
    txn.create_savepoint("sp1".to_string());
    txn.add_write(1, 2);
    txn.rollback_to_savepoint("sp1").unwrap();
    assert_eq!(txn.write_count(), 1);

    // Step 2: new savepoint, write, another savepoint, write, rollback inner.
    txn.add_write(1, 3);
    txn.create_savepoint("sp2".to_string());
    txn.add_write(1, 4);
    txn.create_savepoint("sp3".to_string());
    txn.add_write(1, 5);

    txn.rollback_to_savepoint("sp3").unwrap();
    assert_eq!(txn.write_count(), 3); // writes 1, 3, 4

    // Step 3: rollback to sp2.
    txn.rollback_to_savepoint("sp2").unwrap();
    assert_eq!(txn.write_count(), 2); // writes 1, 3

    // Step 4: release sp1 and sp2, then commit.
    txn.release_savepoint("sp1").unwrap();
    // sp1 and sp2 both gone now (release sp1 truncates to its index, removing
    // everything from sp1 onwards).
    assert!(txn.savepoints.is_empty());

    txn.add_write(1, 6);
    mgr.commit(&mut txn).unwrap();

    assert!(mgr.is_committed(txn.txn_id));
    assert_eq!(txn.write_count(), 3);
    assert_eq!(
        txn.write_set,
        vec![
            WriteEntry { table_id: 1, tuple_id: 1 },
            WriteEntry { table_id: 1, tuple_id: 3 },
            WriteEntry { table_id: 1, tuple_id: 6 },
        ]
    );
}

#[test]
fn full_lifecycle_with_abort_after_savepoint_work() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);
    let txn_id = txn.txn_id;

    txn.add_write(1, 10);
    txn.create_savepoint("sp1".to_string());
    txn.add_write(1, 20);

    // Abort instead of commit.
    mgr.abort(&mut txn);

    assert!(!txn.is_active());
    assert!(!mgr.is_active(txn_id));
    assert!(!mgr.is_committed(txn_id));
}

#[test]
fn full_lifecycle_interleaved_reads_writes_savepoints() {
    let mgr = TransactionManager::new();
    let mut txn = mgr.begin(IsolationLevel::ReadCommitted);

    // Initial work.
    txn.add_read(1, 1);
    txn.add_write(1, 1);
    txn.add_read(1, 2);

    // sp1
    txn.create_savepoint("sp1".to_string());
    txn.add_write(2, 10);
    txn.add_read(2, 10);

    // sp2
    txn.create_savepoint("sp2".to_string());
    txn.add_write(3, 20);
    txn.add_write(3, 21);
    txn.add_read(3, 20);

    assert_eq!(txn.write_count(), 4);
    assert_eq!(txn.read_set.len(), 4);

    // Rollback sp2: undo writes/reads after sp2.
    txn.rollback_to_savepoint("sp2").unwrap();
    assert_eq!(txn.write_count(), 2);
    assert_eq!(txn.read_set.len(), 3);

    // New work after rollback.
    txn.add_write(4, 30);
    txn.add_read(4, 30);

    // Commit.
    mgr.commit(&mut txn).unwrap();

    assert!(mgr.is_committed(txn.txn_id));
    assert_eq!(txn.write_count(), 3);
    assert_eq!(txn.read_set.len(), 4);

    assert_eq!(
        txn.write_set,
        vec![
            WriteEntry { table_id: 1, tuple_id: 1 },
            WriteEntry { table_id: 2, tuple_id: 10 },
            WriteEntry { table_id: 4, tuple_id: 30 },
        ]
    );
    assert_eq!(
        txn.read_set,
        vec![
            ReadEntry { table_id: 1, tuple_id: 1 },
            ReadEntry { table_id: 1, tuple_id: 2 },
            ReadEntry { table_id: 2, tuple_id: 10 },
            ReadEntry { table_id: 4, tuple_id: 30 },
        ]
    );
}
