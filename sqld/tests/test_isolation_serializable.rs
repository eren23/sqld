use std::collections::HashSet;

use sqld::transaction::lock_manager::{LockManager, LockMode, LockTarget};
use sqld::transaction::{IsolationLevel, Snapshot, SsiManager, TransactionManager};

// ===========================================================================
// Helper: build a snapshot with given active set
// ===========================================================================

fn make_snapshot(active: &[u64], xmin: u64, xmax: u64) -> Snapshot {
    let active_set: HashSet<u64> = active.iter().copied().collect();
    Snapshot::new(xmin, xmax, active_set)
}

// ===========================================================================
// 1. SSI manager registers snapshots
// ===========================================================================

#[test]
fn register_snapshot_and_commit_check_passes_without_deps() {
    let mut ssi = SsiManager::new();

    ssi.register(1, make_snapshot(&[], 1, 4));
    ssi.register(2, make_snapshot(&[1], 1, 4));
    ssi.register(3, make_snapshot(&[1, 2], 1, 4));

    // No rw-dependencies -- all should pass.
    assert!(ssi.pre_commit_check(1).is_ok());
    assert!(ssi.pre_commit_check(2).is_ok());
    assert!(ssi.pre_commit_check(3).is_ok());
}

#[test]
fn register_with_transaction_manager_snapshots() {
    let mgr = TransactionManager::new();
    let mut ssi = SsiManager::new();

    let t1 = mgr.begin(IsolationLevel::Serializable);
    let t2 = mgr.begin(IsolationLevel::Serializable);

    ssi.register(t1.txn_id, t1.snapshot.clone());
    ssi.register(t2.txn_id, t2.snapshot.clone());

    // t2's snapshot should include t1 as active.
    assert!(t2.snapshot.active_txns.contains(&t1.txn_id));

    // No deps -- both pass.
    assert!(ssi.pre_commit_check(t1.txn_id).is_ok());
    assert!(ssi.pre_commit_check(t2.txn_id).is_ok());
}

// ===========================================================================
// 2. SIRead locks acquired on reads (via lock_manager.acquire with SIRead)
// ===========================================================================

#[test]
fn siread_lock_always_granted_even_with_exclusive() {
    let lm = LockManager::new();
    let target = LockTarget::Row { table_id: 1, tuple_id: 42 };

    // Exclusive lock held by txn 1.
    assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());

    // SIRead by txn 2 should still be granted (SIRead never blocks).
    assert!(
        lm.acquire(2, target.clone(), LockMode::SIRead).unwrap(),
        "SIRead lock must be granted even when an exclusive lock is held"
    );
}

#[test]
fn siread_lock_appears_in_siread_locks_list() {
    let lm = LockManager::new();
    let target = LockTarget::Row { table_id: 1, tuple_id: 42 };

    lm.acquire(10, target.clone(), LockMode::SIRead).unwrap();

    let siread_locks = lm.get_siread_locks();
    assert_eq!(siread_locks.len(), 1);
    assert_eq!(siread_locks[0].0, target);
    assert_eq!(siread_locks[0].1, 10);
}

// ===========================================================================
// 3. rw-dependency tracking
// ===========================================================================

#[test]
fn add_rw_dependency_tracked_in_count() {
    let mut ssi = SsiManager::new();

    ssi.add_rw_dependency(1, 2);
    assert_eq!(ssi.dependency_count(), 1);

    ssi.add_rw_dependency(2, 3);
    assert_eq!(ssi.dependency_count(), 2);

    ssi.add_rw_dependency(1, 3);
    assert_eq!(ssi.dependency_count(), 3);
}

#[test]
fn self_dependency_ignored() {
    let mut ssi = SsiManager::new();

    ssi.add_rw_dependency(1, 1);
    assert_eq!(
        ssi.dependency_count(),
        0,
        "Self-dependency should be ignored"
    );
}

#[test]
fn record_write_over_siread_creates_dependency() {
    let lm = LockManager::new();
    let mut ssi = SsiManager::new();

    let target = LockTarget::Row { table_id: 1, tuple_id: 100 };

    // Txn 10 acquires an SIRead lock (it read the row).
    lm.acquire(10, target.clone(), LockMode::SIRead).unwrap();

    // Txn 20 writes over that row -- this should create rw-dependency 10 -> 20.
    ssi.record_write_over_siread(20, &target, &lm);

    assert_eq!(
        ssi.dependency_count(),
        1,
        "record_write_over_siread should create an rw-dependency"
    );
}

// ===========================================================================
// 4. Dangerous structure detection (T1->T2->T3, T1 concurrent with T3)
// ===========================================================================

#[test]
fn dangerous_structure_three_concurrent_txns() {
    let mut ssi = SsiManager::new();

    // T1, T2, T3 all concurrent.
    ssi.register(1, make_snapshot(&[2, 3], 1, 4));
    ssi.register(2, make_snapshot(&[1, 3], 1, 4));
    ssi.register(3, make_snapshot(&[1, 2], 1, 4));

    ssi.add_rw_dependency(1, 2);
    ssi.add_rw_dependency(2, 3);

    let result = ssi.pre_commit_check(2);
    assert!(
        result.is_err(),
        "Dangerous structure T1->T2->T3 (all concurrent) must be detected"
    );
}

// ===========================================================================
// 5. Write-skew detection (2-txn cycle: T1->T2->T1)
// ===========================================================================

#[test]
fn write_skew_two_txn_cycle_detected() {
    let mut ssi = SsiManager::new();

    ssi.register(1, make_snapshot(&[2], 1, 3));
    ssi.register(2, make_snapshot(&[1], 1, 3));

    // Mutual rw-dependencies: T1->T2 and T2->T1.
    ssi.add_rw_dependency(1, 2);
    ssi.add_rw_dependency(2, 1);

    // T2 commits: chain is T1->T2->T1 (T1==T3), T1 and T2 concurrent.
    let result = ssi.pre_commit_check(2);
    assert!(
        result.is_err(),
        "Write-skew (T1->T2->T1 with concurrent txns) must be detected"
    );
}

#[test]
fn write_skew_detected_on_either_committer() {
    let mut ssi = SsiManager::new();

    ssi.register(1, make_snapshot(&[2], 1, 3));
    ssi.register(2, make_snapshot(&[1], 1, 3));

    ssi.add_rw_dependency(1, 2);
    ssi.add_rw_dependency(2, 1);

    // T1 tries to commit first: chain is T2->T1->T2, T1 and T2 concurrent.
    let result = ssi.pre_commit_check(1);
    assert!(
        result.is_err(),
        "Write-skew should be detected regardless of which txn commits first"
    );
}

// ===========================================================================
// 6. Non-concurrent T1,T3 passes
// ===========================================================================

#[test]
fn non_concurrent_t1_t3_passes() {
    let mut ssi = SsiManager::new();

    // T1 committed before T3 started.
    ssi.register(1, make_snapshot(&[], 1, 2));
    ssi.register(2, make_snapshot(&[1], 1, 3));
    ssi.register(3, make_snapshot(&[], 1, 4));
    ssi.mark_committed(1);

    ssi.add_rw_dependency(1, 2);
    ssi.add_rw_dependency(2, 3);

    // T1 and T3 are NOT concurrent.
    let result = ssi.pre_commit_check(2);
    assert!(
        result.is_ok(),
        "Non-concurrent T1 and T3 should not trigger a dangerous structure"
    );
}

// ===========================================================================
// 7. Aborted txn breaks dangerous structure
// ===========================================================================

#[test]
fn aborted_t1_breaks_dangerous_structure() {
    let mut ssi = SsiManager::new();

    ssi.register(1, make_snapshot(&[2, 3], 1, 4));
    ssi.register(2, make_snapshot(&[1, 3], 1, 4));
    ssi.register(3, make_snapshot(&[1, 2], 1, 4));

    ssi.add_rw_dependency(1, 2);
    ssi.add_rw_dependency(2, 3);

    // Abort T1 -- removes edges involving T1.
    ssi.mark_aborted(1);

    let result = ssi.pre_commit_check(2);
    assert!(
        result.is_ok(),
        "Aborting T1 should break the dangerous structure"
    );
}

#[test]
fn aborted_t3_breaks_dangerous_structure() {
    let mut ssi = SsiManager::new();

    ssi.register(1, make_snapshot(&[2, 3], 1, 4));
    ssi.register(2, make_snapshot(&[1, 3], 1, 4));
    ssi.register(3, make_snapshot(&[1, 2], 1, 4));

    ssi.add_rw_dependency(1, 2);
    ssi.add_rw_dependency(2, 3);

    ssi.mark_aborted(3);

    let result = ssi.pre_commit_check(2);
    assert!(
        result.is_ok(),
        "Aborting T3 should break the dangerous structure"
    );
}

// ===========================================================================
// 8. pre_commit_check returns error for serialization failure
// ===========================================================================

#[test]
fn pre_commit_check_error_is_serialization_failure() {
    let mut ssi = SsiManager::new();

    ssi.register(1, make_snapshot(&[2, 3], 1, 4));
    ssi.register(2, make_snapshot(&[1, 3], 1, 4));
    ssi.register(3, make_snapshot(&[1, 2], 1, 4));

    ssi.add_rw_dependency(1, 2);
    ssi.add_rw_dependency(2, 3);

    let result = ssi.pre_commit_check(2);
    assert!(result.is_err());

    // Verify the error message mentions "serialization failure".
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("serialization failure"),
        "Error should mention serialization failure, got: {}",
        err_msg
    );
}

#[test]
fn pre_commit_check_write_skew_error_contains_txn_id() {
    let mut ssi = SsiManager::new();

    ssi.register(1, make_snapshot(&[2], 1, 3));
    ssi.register(2, make_snapshot(&[1], 1, 3));

    ssi.add_rw_dependency(1, 2);
    ssi.add_rw_dependency(2, 1);

    let result = ssi.pre_commit_check(2);
    assert!(result.is_err());

    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("2"),
        "Error should reference the committing txn_id (2), got: {}",
        err_msg
    );
}
