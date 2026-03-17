use sqld::transaction::{LockManager, LockMode, LockTarget};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn row(table_id: u64, tuple_id: u64) -> LockTarget {
    LockTarget::Row { table_id, tuple_id }
}

fn table(table_id: u64) -> LockTarget {
    LockTarget::Table(table_id)
}

// ===========================================================================
// 1. Multiple shared locks on same row from different txns -> all granted
// ===========================================================================

#[test]
fn shared_shared_compatibility_two_txns() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Shared).unwrap());
    assert!(lm.acquire(2, target.clone(), LockMode::Shared).unwrap());

    // Both transactions should hold the lock.
    assert_eq!(lm.locks_held_by(1).len(), 1);
    assert_eq!(lm.locks_held_by(2).len(), 1);
}

#[test]
fn shared_shared_compatibility_many_txns() {
    let lm = LockManager::new();
    let target = row(1, 1);
    for txn_id in 1..=10 {
        assert!(
            lm.acquire(txn_id, target.clone(), LockMode::Shared).unwrap(),
            "txn {txn_id} should acquire shared lock"
        );
    }
    for txn_id in 1..=10 {
        assert_eq!(lm.locks_held_by(txn_id).len(), 1);
    }
}

// ===========================================================================
// 2. Exclusive lock blocks subsequent shared lock -> queued
// ===========================================================================

#[test]
fn exclusive_then_shared_blocks() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    // Shared request should be queued.
    assert!(!lm.acquire(2, target.clone(), LockMode::Shared).unwrap());
    assert!(lm.is_waiting(2));
}

// ===========================================================================
// 3. Shared lock blocks subsequent exclusive lock -> queued
// ===========================================================================

#[test]
fn shared_then_exclusive_blocks() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Shared).unwrap());
    // Exclusive request should be queued (returns false).
    assert!(!lm.acquire(2, target.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.is_waiting(2));
}

// ===========================================================================
// 4. Exclusive lock blocks subsequent exclusive lock -> queued
// ===========================================================================

#[test]
fn exclusive_exclusive_conflict() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, target.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.is_waiting(2));
    assert!(!lm.is_waiting(1));
}

#[test]
fn exclusive_exclusive_different_rows_no_conflict() {
    let lm = LockManager::new();
    assert!(lm.acquire(1, row(1, 1), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, row(1, 2), LockMode::Exclusive).unwrap());
    assert!(!lm.is_waiting(1));
    assert!(!lm.is_waiting(2));
}

// ===========================================================================
// 5. SIRead never blocks (even with exclusive held)
// ===========================================================================

#[test]
fn siread_does_not_block_exclusive() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, target.clone(), LockMode::SIRead).unwrap());
    assert!(!lm.is_waiting(2));
}

#[test]
fn exclusive_does_not_block_on_siread() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::SIRead).unwrap());
    // Exclusive should be granted because SIRead never conflicts.
    assert!(lm.acquire(2, target.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.is_waiting(2));
}

// ===========================================================================
// 6. SIRead coexists with all lock modes
// ===========================================================================

#[test]
fn siread_with_siread_compatible() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::SIRead).unwrap());
    assert!(lm.acquire(2, target.clone(), LockMode::SIRead).unwrap());
}

#[test]
fn siread_with_shared_compatible() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::SIRead).unwrap());
    assert!(lm.acquire(2, target.clone(), LockMode::Shared).unwrap());
}

#[test]
fn siread_with_intention_shared_compatible() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::SIRead).unwrap());
    assert!(lm.acquire(2, target.clone(), LockMode::IntentionShared).unwrap());
}

#[test]
fn siread_with_intention_exclusive_compatible() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::SIRead).unwrap());
    assert!(lm.acquire(2, target.clone(), LockMode::IntentionExclusive).unwrap());
}

#[test]
fn siread_with_exclusive_compatible() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::SIRead).unwrap());
    assert!(lm.acquire(2, target.clone(), LockMode::Exclusive).unwrap());
}

// ===========================================================================
// 7. Lock upgrade: S -> X when no other holders -> granted
// ===========================================================================

#[test]
fn lock_upgrade_shared_to_exclusive_no_other_holders() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Shared).unwrap());
    // Upgrade: same txn, shared -> exclusive, no contention.
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());

    let held = lm.locks_held_by(1);
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].1, LockMode::Exclusive);
}

// ===========================================================================
// 8. Lock upgrade: S -> X when other S holders -> queued
// ===========================================================================

#[test]
fn lock_upgrade_blocked_when_other_shared_holder() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Shared).unwrap());
    assert!(lm.acquire(2, target.clone(), LockMode::Shared).unwrap());
    // Txn 1 tries to upgrade to exclusive, but txn 2 holds shared.
    assert!(!lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.is_waiting(1));
}

#[test]
fn lock_upgrade_granted_after_other_releases() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Shared).unwrap());
    assert!(lm.acquire(2, target.clone(), LockMode::Shared).unwrap());
    // Txn 1 tries upgrade -> queued.
    assert!(!lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());

    // Release txn 2 -> txn 1 should be promoted.
    lm.release_all(2);

    let held = lm.locks_held_by(1);
    let modes: Vec<LockMode> = held.iter().map(|(_, m)| *m).collect();
    assert!(
        modes.contains(&LockMode::Exclusive),
        "txn 1 should now hold exclusive lock"
    );
    assert!(!lm.is_waiting(1));
}

// ===========================================================================
// 9. Idempotent acquire: same lock mode twice -> no duplicate
// ===========================================================================

#[test]
fn idempotent_shared_acquire() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Shared).unwrap());
    assert!(lm.acquire(1, target.clone(), LockMode::Shared).unwrap());
    assert_eq!(lm.locks_held_by(1).len(), 1);
}

#[test]
fn idempotent_exclusive_acquire() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    assert_eq!(lm.locks_held_by(1).len(), 1);
}

#[test]
fn shared_after_exclusive_is_idempotent() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    // Exclusive subsumes shared, so re-acquiring shared should be idempotent.
    assert!(lm.acquire(1, target.clone(), LockMode::Shared).unwrap());
    assert_eq!(lm.locks_held_by(1).len(), 1);
    assert_eq!(lm.locks_held_by(1)[0].1, LockMode::Exclusive);
}

// ===========================================================================
// 10. Release specific lock unblocks waiters (FIFO)
// ===========================================================================

#[test]
fn release_specific_lock() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);
    assert!(lm.acquire(1, r1.clone(), LockMode::Shared).unwrap());
    assert!(lm.acquire(1, r2.clone(), LockMode::Shared).unwrap());

    // Release only r1.
    lm.release(1, &r1);

    let held = lm.locks_held_by(1);
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].0, r2);
}

#[test]
fn release_specific_lock_grants_waiter() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, target.clone(), LockMode::Shared).unwrap());

    // Release specific lock for txn 1.
    lm.release(1, &target);

    assert_eq!(lm.locks_held_by(2).len(), 1);
    assert!(!lm.is_waiting(2));
}

#[test]
fn release_specific_lock_nonexistent_is_noop() {
    let lm = LockManager::new();
    let target = row(1, 1);
    // Releasing a lock that was never acquired should not panic.
    lm.release(1, &target);
}

#[test]
fn release_specific_lock_fifo_order() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    // Queue two exclusive waiters.
    assert!(!lm.acquire(2, target.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(3, target.clone(), LockMode::Exclusive).unwrap());

    // Release txn 1's specific lock -> first waiter (txn 2) should be granted.
    lm.release(1, &target);

    assert_eq!(lm.locks_held_by(2).len(), 1);
    assert!(!lm.is_waiting(2));
    assert!(lm.is_waiting(3));
}

// ===========================================================================
// 11. Release all locks for a transaction
// ===========================================================================

#[test]
fn release_all_clears_all_locks() {
    let lm = LockManager::new();
    for i in 1..=5 {
        assert!(lm.acquire(1, row(1, i), LockMode::Exclusive).unwrap());
    }
    assert_eq!(lm.locks_held_by(1).len(), 5);

    lm.release_all(1);

    assert_eq!(lm.locks_held_by(1).len(), 0);
}

#[test]
fn release_all_removes_from_wait_queue() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, target.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.is_waiting(2));

    // Release all for txn 2 should remove it from the wait queue.
    lm.release_all(2);
    assert!(!lm.is_waiting(2));
    assert_eq!(lm.locks_held_by(2).len(), 0);
}

#[test]
fn release_all_grants_next_waiter_in_queue() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, target.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(3, target.clone(), LockMode::Shared).unwrap());

    lm.release_all(1);

    // Txn 2 (exclusive) should be granted, txn 3 should wait.
    assert_eq!(lm.locks_held_by(2).len(), 1);
    assert!(lm.is_waiting(3));
}

#[test]
fn release_all_for_unknown_txn_is_noop() {
    let lm = LockManager::new();
    // Should not panic.
    lm.release_all(999);
}

// ===========================================================================
// 12. Intention locks: IS + IS compatible
// ===========================================================================

#[test]
fn intention_shared_compatible_with_intention_shared() {
    let lm = LockManager::new();
    let target = table(1);
    assert!(lm.acquire(1, target.clone(), LockMode::IntentionShared).unwrap());
    assert!(lm.acquire(2, target.clone(), LockMode::IntentionShared).unwrap());
}

// ===========================================================================
// 13. Intention locks: IS + IX compatible
// ===========================================================================

#[test]
fn intention_shared_compatible_with_intention_exclusive() {
    let lm = LockManager::new();
    let target = table(1);
    assert!(lm.acquire(1, target.clone(), LockMode::IntentionShared).unwrap());
    assert!(lm.acquire(2, target.clone(), LockMode::IntentionExclusive).unwrap());
}

#[test]
fn intention_exclusive_compatible_with_intention_shared() {
    let lm = LockManager::new();
    let target = table(1);
    assert!(lm.acquire(1, target.clone(), LockMode::IntentionExclusive).unwrap());
    assert!(lm.acquire(2, target.clone(), LockMode::IntentionShared).unwrap());
}

// ===========================================================================
// 14. Intention locks: IX + IX compatible
// ===========================================================================

#[test]
fn intention_exclusive_compatible_with_intention_exclusive() {
    let lm = LockManager::new();
    let target = table(1);
    assert!(lm.acquire(1, target.clone(), LockMode::IntentionExclusive).unwrap());
    assert!(lm.acquire(2, target.clone(), LockMode::IntentionExclusive).unwrap());
}

// ===========================================================================
// 15. Intention locks: IS + X conflict
// ===========================================================================

#[test]
fn intention_shared_blocks_exclusive() {
    let lm = LockManager::new();
    let target = table(1);
    assert!(lm.acquire(1, target.clone(), LockMode::IntentionShared).unwrap());
    // Exclusive should conflict with IntentionShared.
    assert!(!lm.acquire(2, target.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.is_waiting(2));
}

#[test]
fn exclusive_blocks_intention_shared() {
    let lm = LockManager::new();
    let target = table(1);
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, target.clone(), LockMode::IntentionShared).unwrap());
    assert!(lm.is_waiting(2));
}

#[test]
fn intention_exclusive_blocks_exclusive() {
    let lm = LockManager::new();
    let target = table(1);
    assert!(lm.acquire(1, target.clone(), LockMode::IntentionExclusive).unwrap());
    assert!(!lm.acquire(2, target.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.is_waiting(2));
}

#[test]
fn intention_shared_compatible_with_shared() {
    let lm = LockManager::new();
    let target = table(1);
    assert!(lm.acquire(1, target.clone(), LockMode::IntentionShared).unwrap());
    assert!(lm.acquire(2, target.clone(), LockMode::Shared).unwrap());
}

// ===========================================================================
// 16. Table-level and row-level locks are independent targets
// ===========================================================================

#[test]
fn table_lock_independent_of_row_lock() {
    let lm = LockManager::new();
    // Table lock and row lock on the same table_id are different targets.
    assert!(lm.acquire(1, table(1), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, row(1, 1), LockMode::Exclusive).unwrap());
    // No conflict because they are different lock targets.
    assert!(!lm.is_waiting(1));
    assert!(!lm.is_waiting(2));
}

#[test]
fn different_tables_no_conflict() {
    let lm = LockManager::new();
    assert!(lm.acquire(1, table(1), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, table(2), LockMode::Exclusive).unwrap());
}

#[test]
fn table_level_exclusive_blocks_same_table() {
    let lm = LockManager::new();
    let target = table(1);
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, target.clone(), LockMode::Shared).unwrap());
    assert!(lm.is_waiting(2));
}

#[test]
fn table_level_shared_compatible() {
    let lm = LockManager::new();
    let target = table(1);
    assert!(lm.acquire(1, target.clone(), LockMode::Shared).unwrap());
    assert!(lm.acquire(2, target.clone(), LockMode::Shared).unwrap());
}

// ===========================================================================
// 17. get_siread_locks returns all SIRead locks
// ===========================================================================

#[test]
fn siread_appears_in_get_siread_locks() {
    let lm = LockManager::new();
    let target = row(1, 1);
    lm.acquire(1, target.clone(), LockMode::SIRead).unwrap();
    lm.acquire(2, target.clone(), LockMode::SIRead).unwrap();

    let siread_locks = lm.get_siread_locks();
    assert_eq!(siread_locks.len(), 2);

    let txn_ids: Vec<u64> = siread_locks.iter().map(|(_, t)| *t).collect();
    assert!(txn_ids.contains(&1));
    assert!(txn_ids.contains(&2));
}

#[test]
fn get_siread_locks_excludes_non_siread() {
    let lm = LockManager::new();
    let target = row(1, 1);
    lm.acquire(1, target.clone(), LockMode::Shared).unwrap();
    lm.acquire(2, target.clone(), LockMode::SIRead).unwrap();

    let siread_locks = lm.get_siread_locks();
    assert_eq!(siread_locks.len(), 1);
    assert_eq!(siread_locks[0].1, 2);
}

#[test]
fn get_siread_locks_empty_when_none() {
    let lm = LockManager::new();
    let target = row(1, 1);
    lm.acquire(1, target.clone(), LockMode::Shared).unwrap();
    lm.acquire(2, target.clone(), LockMode::Shared).unwrap();

    let siread_locks = lm.get_siread_locks();
    assert!(siread_locks.is_empty());
}

#[test]
fn get_siread_locks_multiple_targets() {
    let lm = LockManager::new();
    lm.acquire(1, row(1, 1), LockMode::SIRead).unwrap();
    lm.acquire(2, row(1, 2), LockMode::SIRead).unwrap();
    lm.acquire(3, table(1), LockMode::SIRead).unwrap();

    let siread_locks = lm.get_siread_locks();
    assert_eq!(siread_locks.len(), 3);

    let txn_ids: Vec<u64> = siread_locks.iter().map(|(_, t)| *t).collect();
    assert!(txn_ids.contains(&1));
    assert!(txn_ids.contains(&2));
    assert!(txn_ids.contains(&3));
}

// ===========================================================================
// 18. locks_held_by returns correct locks for txn
// ===========================================================================

#[test]
fn locks_held_by_returns_correct_locks() {
    let lm = LockManager::new();
    lm.acquire(1, row(1, 1), LockMode::Shared).unwrap();
    lm.acquire(1, row(1, 2), LockMode::Exclusive).unwrap();
    lm.acquire(1, table(1), LockMode::IntentionShared).unwrap();

    let held = lm.locks_held_by(1);
    assert_eq!(held.len(), 3);

    let targets: Vec<&LockTarget> = held.iter().map(|(t, _)| t).collect();
    assert!(targets.contains(&&row(1, 1)));
    assert!(targets.contains(&&row(1, 2)));
    assert!(targets.contains(&&table(1)));
}

#[test]
fn locks_held_by_returns_empty_for_unknown_txn() {
    let lm = LockManager::new();
    assert!(lm.locks_held_by(999).is_empty());
}

#[test]
fn locks_held_by_does_not_include_other_txn_locks() {
    let lm = LockManager::new();
    let target = row(1, 1);
    lm.acquire(1, target.clone(), LockMode::Shared).unwrap();
    lm.acquire(2, target.clone(), LockMode::Shared).unwrap();

    let held_by_1 = lm.locks_held_by(1);
    assert_eq!(held_by_1.len(), 1);
    let held_by_2 = lm.locks_held_by(2);
    assert_eq!(held_by_2.len(), 1);
}

#[test]
fn locks_held_by_does_not_include_queued_locks() {
    let lm = LockManager::new();
    let target = row(1, 1);
    lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap();
    lm.acquire(2, target.clone(), LockMode::Exclusive).unwrap(); // queued

    // Txn 2 is waiting, not holding.
    assert_eq!(lm.locks_held_by(2).len(), 0);
    assert_eq!(lm.locks_held_by(1).len(), 1);
}

// ===========================================================================
// 19. is_waiting returns true for queued txn
// ===========================================================================

#[test]
fn is_waiting_returns_true_for_queued_txn() {
    let lm = LockManager::new();
    let target = row(1, 1);
    lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap();
    lm.acquire(2, target.clone(), LockMode::Shared).unwrap();

    assert!(lm.is_waiting(2));
    assert!(!lm.is_waiting(1));
}

#[test]
fn is_waiting_returns_false_for_granted_txn() {
    let lm = LockManager::new();
    let target = row(1, 1);
    lm.acquire(1, target.clone(), LockMode::Shared).unwrap();

    assert!(!lm.is_waiting(1));
}

#[test]
fn is_waiting_returns_false_for_unknown_txn() {
    let lm = LockManager::new();
    assert!(!lm.is_waiting(999));
}

#[test]
fn is_waiting_becomes_false_after_grant() {
    let lm = LockManager::new();
    let target = row(1, 1);
    lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap();
    lm.acquire(2, target.clone(), LockMode::Shared).unwrap();
    assert!(lm.is_waiting(2));

    // Release holder -> waiter gets granted.
    lm.release_all(1);
    assert!(!lm.is_waiting(2));
}

// ===========================================================================
// 20. Wait-for graph construction
// ===========================================================================

#[test]
fn wait_for_graph_empty_when_no_waiters() {
    let lm = LockManager::new();
    let target = row(1, 1);
    lm.acquire(1, target.clone(), LockMode::Shared).unwrap();
    lm.acquire(2, target.clone(), LockMode::Shared).unwrap();

    let graph = lm.build_wait_for_graph();
    assert!(graph.is_empty());
}

#[test]
fn wait_for_graph_single_edge() {
    let lm = LockManager::new();
    let target = row(1, 1);
    lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap();
    lm.acquire(2, target.clone(), LockMode::Exclusive).unwrap(); // queued

    let graph = lm.build_wait_for_graph();
    assert!(graph.contains_key(&2));
    assert!(graph[&2].contains(&1));
    assert!(!graph.contains_key(&1));
}

#[test]
fn wait_for_graph_multiple_holders() {
    let lm = LockManager::new();
    let target = row(1, 1);
    lm.acquire(1, target.clone(), LockMode::Shared).unwrap();
    lm.acquire(2, target.clone(), LockMode::Shared).unwrap();
    // Txn 3 wants exclusive -> waits on both 1 and 2.
    lm.acquire(3, target.clone(), LockMode::Exclusive).unwrap();

    let graph = lm.build_wait_for_graph();
    assert!(graph.contains_key(&3));
    assert!(graph[&3].contains(&1));
    assert!(graph[&3].contains(&2));
}

#[test]
fn wait_for_graph_deadlock_cycle() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);
    lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap();
    lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap();
    lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap(); // txn 1 waits on txn 2
    lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap(); // txn 2 waits on txn 1

    let graph = lm.build_wait_for_graph();
    assert!(graph[&1].contains(&2));
    assert!(graph[&2].contains(&1));

    // Deadlock detection should find a cycle.
    let cycles = lm.detect_deadlocks();
    assert!(!cycles.is_empty());
    // The cycle should contain both txn 1 and txn 2.
    let cycle = &cycles[0];
    assert!(cycle.contains(&1));
    assert!(cycle.contains(&2));
}

#[test]
fn wait_for_graph_siread_does_not_create_edges() {
    let lm = LockManager::new();
    let target = row(1, 1);
    lm.acquire(1, target.clone(), LockMode::SIRead).unwrap();
    // SIRead never blocks, so exclusive should be granted immediately.
    lm.acquire(2, target.clone(), LockMode::Exclusive).unwrap();

    let graph = lm.build_wait_for_graph();
    assert!(graph.is_empty());
}

#[test]
fn wait_for_graph_multiple_waiters_on_same_resource() {
    let lm = LockManager::new();
    let target = row(1, 1);
    lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap();
    lm.acquire(2, target.clone(), LockMode::Exclusive).unwrap();
    lm.acquire(3, target.clone(), LockMode::Shared).unwrap();

    let graph = lm.build_wait_for_graph();
    // Both txn 2 and txn 3 should wait on txn 1.
    assert!(graph.contains_key(&2));
    assert!(graph[&2].contains(&1));
    assert!(graph.contains_key(&3));
    assert!(graph[&3].contains(&1));
}

#[test]
fn no_deadlock_when_no_cycles() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);
    lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap();
    lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap();
    // Only txn 1 waits on txn 2 (no reverse edge).
    lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap();

    let cycles = lm.detect_deadlocks();
    assert!(cycles.is_empty());
}

#[test]
fn three_way_deadlock() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);
    let r3 = row(1, 3);

    // Txn 1 holds r1, txn 2 holds r2, txn 3 holds r3.
    lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap();
    lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap();
    lm.acquire(3, r3.clone(), LockMode::Exclusive).unwrap();

    // Txn 1 waits on r2 (held by 2), txn 2 waits on r3 (held by 3),
    // txn 3 waits on r1 (held by 1) => cycle 1->2->3->1.
    lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap();
    lm.acquire(2, r3.clone(), LockMode::Exclusive).unwrap();
    lm.acquire(3, r1.clone(), LockMode::Exclusive).unwrap();

    let cycles = lm.detect_deadlocks();
    assert!(!cycles.is_empty(), "should detect a three-way deadlock cycle");
}

// ===========================================================================
// 21. FIFO ordering: first waiter gets granted first after release
// ===========================================================================

#[test]
fn fifo_ordering_first_waiter_granted_first() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, target.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(3, target.clone(), LockMode::Exclusive).unwrap());

    // Release txn 1 -> txn 2 should be granted (FIFO), txn 3 remains waiting.
    lm.release_all(1);

    assert!(!lm.is_waiting(2));
    assert!(lm.is_waiting(3));
    assert_eq!(lm.locks_held_by(2).len(), 1);
    assert_eq!(lm.locks_held_by(3).len(), 0);
}

#[test]
fn fifo_ordering_grants_multiple_compatible_waiters() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    // Three shared waiters.
    assert!(!lm.acquire(2, target.clone(), LockMode::Shared).unwrap());
    assert!(!lm.acquire(3, target.clone(), LockMode::Shared).unwrap());
    assert!(!lm.acquire(4, target.clone(), LockMode::Shared).unwrap());

    // Release exclusive -> all shared waiters should be granted.
    lm.release_all(1);

    assert_eq!(lm.locks_held_by(2).len(), 1);
    assert_eq!(lm.locks_held_by(3).len(), 1);
    assert_eq!(lm.locks_held_by(4).len(), 1);
    assert!(!lm.is_waiting(2));
    assert!(!lm.is_waiting(3));
    assert!(!lm.is_waiting(4));
}

#[test]
fn fifo_ordering_shared_granted_exclusive_stays_waiting() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    // Shared waiter then exclusive waiter.
    assert!(!lm.acquire(2, target.clone(), LockMode::Shared).unwrap());
    assert!(!lm.acquire(3, target.clone(), LockMode::Exclusive).unwrap());

    lm.release_all(1);

    // Shared txn 2 should be granted; exclusive txn 3 conflicts with shared.
    assert_eq!(lm.locks_held_by(2).len(), 1);
    assert!(lm.is_waiting(3));
}

#[test]
fn fifo_ordering_chain_of_exclusive_waiters() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, target.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(3, target.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(4, target.clone(), LockMode::Exclusive).unwrap());

    // Release txn 1 -> txn 2 granted.
    lm.release_all(1);
    assert_eq!(lm.locks_held_by(2).len(), 1);
    assert!(lm.is_waiting(3));
    assert!(lm.is_waiting(4));

    // Release txn 2 -> txn 3 granted.
    lm.release_all(2);
    assert_eq!(lm.locks_held_by(3).len(), 1);
    assert!(lm.is_waiting(4));

    // Release txn 3 -> txn 4 granted.
    lm.release_all(3);
    assert_eq!(lm.locks_held_by(4).len(), 1);
    assert!(!lm.is_waiting(4));
}

#[test]
fn fifo_ordering_mixed_shared_exclusive_queue() {
    let lm = LockManager::new();
    let target = row(1, 1);
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());

    // Queue: shared(2), shared(3), exclusive(4), shared(5)
    assert!(!lm.acquire(2, target.clone(), LockMode::Shared).unwrap());
    assert!(!lm.acquire(3, target.clone(), LockMode::Shared).unwrap());
    assert!(!lm.acquire(4, target.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(5, target.clone(), LockMode::Shared).unwrap());

    // Release txn 1 -> process_wait_queue grants compatible locks.
    lm.release_all(1);

    // Txns 2 and 3 (shared) should be granted.
    assert_eq!(lm.locks_held_by(2).len(), 1);
    assert_eq!(lm.locks_held_by(3).len(), 1);
    // Txn 4 (exclusive) conflicts with shared holders 2 and 3 -> wait.
    assert!(lm.is_waiting(4));
    // Txn 5 (shared) does not conflict with shared holders, so it can be granted.
    // The implementation iterates all waiters and grants any that can_grant against holders.
    assert_eq!(lm.locks_held_by(5).len(), 1);
}

// ===========================================================================
// Additional edge cases and exhaustive validation
// ===========================================================================

#[test]
fn conflict_matrix_exhaustive() {
    use LockMode::*;

    let modes = [Shared, Exclusive, SIRead, IntentionShared, IntentionExclusive];

    let expected: Vec<(LockMode, LockMode, bool)> = vec![
        (Shared, Shared, false),
        (Shared, Exclusive, true),
        (Shared, SIRead, false),
        (Shared, IntentionShared, false),
        (Shared, IntentionExclusive, false),
        (Exclusive, Shared, true),
        (Exclusive, Exclusive, true),
        (Exclusive, SIRead, false),
        (Exclusive, IntentionShared, true),
        (Exclusive, IntentionExclusive, true),
        (SIRead, Shared, false),
        (SIRead, Exclusive, false),
        (SIRead, SIRead, false),
        (SIRead, IntentionShared, false),
        (SIRead, IntentionExclusive, false),
        (IntentionShared, Shared, false),
        (IntentionShared, Exclusive, true),
        (IntentionShared, SIRead, false),
        (IntentionShared, IntentionShared, false),
        (IntentionShared, IntentionExclusive, false),
        (IntentionExclusive, Shared, false),
        (IntentionExclusive, Exclusive, true),
        (IntentionExclusive, SIRead, false),
        (IntentionExclusive, IntentionShared, false),
        (IntentionExclusive, IntentionExclusive, false),
    ];

    for (a, b, should_conflict) in &expected {
        assert_eq!(
            a.conflicts_with(b),
            *should_conflict,
            "{:?} conflicts_with {:?} should be {}, got {}",
            a,
            b,
            should_conflict,
            a.conflicts_with(b)
        );
    }

    // Verify symmetry.
    for a in &modes {
        for b in &modes {
            assert_eq!(
                a.conflicts_with(b),
                b.conflicts_with(a),
                "conflict matrix should be symmetric: {:?} vs {:?}",
                a,
                b
            );
        }
    }
}

#[test]
fn conflict_matrix_acquire_validation() {
    use LockMode::*;

    let compatible_pairs = vec![
        (Shared, Shared),
        (Shared, IntentionShared),
        (IntentionShared, IntentionShared),
        (IntentionExclusive, IntentionExclusive),
        (IntentionShared, IntentionExclusive),
    ];

    for (mode_a, mode_b) in &compatible_pairs {
        let lm = LockManager::new();
        let target = row(99, 99);
        assert!(
            lm.acquire(1, target.clone(), *mode_a).unwrap(),
            "txn 1 should acquire {:?}",
            mode_a
        );
        assert!(
            lm.acquire(2, target.clone(), *mode_b).unwrap(),
            "{:?} and {:?} should be compatible",
            mode_a,
            mode_b
        );
    }

    let conflicting_pairs = vec![
        (Shared, Exclusive),
        (Exclusive, Shared),
        (Exclusive, Exclusive),
        (Exclusive, IntentionShared),
        (IntentionShared, Exclusive),
        (Exclusive, IntentionExclusive),
        (IntentionExclusive, Exclusive),
    ];

    for (mode_a, mode_b) in &conflicting_pairs {
        let lm = LockManager::new();
        let target = row(99, 99);
        assert!(
            lm.acquire(1, target.clone(), *mode_a).unwrap(),
            "txn 1 should acquire {:?}",
            mode_a
        );
        assert!(
            !lm.acquire(2, target.clone(), *mode_b).unwrap(),
            "{:?} and {:?} should conflict",
            mode_a,
            mode_b
        );
    }
}

#[test]
fn many_locks_on_different_resources() {
    let lm = LockManager::new();
    for i in 0..100 {
        assert!(lm.acquire(1, row(1, i), LockMode::Exclusive).unwrap());
    }
    assert_eq!(lm.locks_held_by(1).len(), 100);

    lm.release_all(1);
    assert_eq!(lm.locks_held_by(1).len(), 0);
}
