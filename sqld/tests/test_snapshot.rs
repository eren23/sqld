use std::collections::HashSet;

use sqld::transaction::{IsolationLevel, Snapshot, TransactionManager};

// ===========================================================================
// 1. Snapshot captures active transactions at creation time
// ===========================================================================

#[test]
fn snapshot_captures_active_transactions_at_creation() {
    let tm = TransactionManager::new();

    let t1 = tm.begin(IsolationLevel::ReadCommitted);
    let t2 = tm.begin(IsolationLevel::RepeatableRead);

    // Create a snapshot on behalf of a hypothetical txn_id.
    // Both t1 and t2 are still active, so both should appear.
    let snap = tm.create_snapshot(99);

    assert!(
        snap.active_txns.contains(&t1.txn_id),
        "snapshot should capture t1 as active"
    );
    assert!(
        snap.active_txns.contains(&t2.txn_id),
        "snapshot should capture t2 as active"
    );
}

#[test]
fn snapshot_does_not_include_txns_created_after() {
    let tm = TransactionManager::new();

    let t1 = tm.begin(IsolationLevel::ReadCommitted);
    // t1's snapshot was taken at begin-time, before t2 exists.
    let t2 = tm.begin(IsolationLevel::ReadCommitted);

    assert!(
        !t1.snapshot.active_txns.contains(&t2.txn_id),
        "t1's snapshot should not include t2, which was created later"
    );
}

#[test]
fn snapshot_on_begin_includes_all_prior_active_txns() {
    let tm = TransactionManager::new();

    let t1 = tm.begin(IsolationLevel::ReadCommitted);
    let t2 = tm.begin(IsolationLevel::ReadCommitted);
    let t3 = tm.begin(IsolationLevel::ReadCommitted);

    // t3's snapshot should capture both t1 and t2.
    assert!(t3.snapshot.active_txns.contains(&t1.txn_id));
    assert!(t3.snapshot.active_txns.contains(&t2.txn_id));
}

// ===========================================================================
// 2. Snapshot xmin is lowest active txn id
// ===========================================================================

#[test]
fn snapshot_xmin_is_lowest_active_txn_id() {
    let tm = TransactionManager::new();

    let t1 = tm.begin(IsolationLevel::ReadCommitted);
    let _t2 = tm.begin(IsolationLevel::ReadCommitted);
    let _t3 = tm.begin(IsolationLevel::ReadCommitted);

    // All three are active. The snapshot for a new observer should have
    // xmin == t1.txn_id (the lowest active).
    let snap = tm.create_snapshot(99);
    assert_eq!(
        snap.xmin, t1.txn_id,
        "xmin should be the lowest active txn id"
    );
}

#[test]
fn snapshot_xmin_advances_when_lowest_commits() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let t2 = tm.begin(IsolationLevel::ReadCommitted);
    let _t3 = tm.begin(IsolationLevel::ReadCommitted);

    // Commit t1 (the lowest active).
    tm.commit(&mut t1).unwrap();

    // New snapshot's xmin should now be t2's id (the new lowest active).
    let snap = tm.create_snapshot(99);
    assert_eq!(
        snap.xmin, t2.txn_id,
        "xmin should advance to t2 after t1 commits"
    );
}

#[test]
fn snapshot_xmin_when_no_active_txns_is_own_id() {
    let tm = TransactionManager::new();

    // No transactions are active, so xmin falls back to the supplied txn_id.
    let snap = tm.create_snapshot(42);
    assert_eq!(
        snap.xmin, 42,
        "xmin should default to own txn_id when no active transactions"
    );
}

// ===========================================================================
// 3. Snapshot xmax is next txn id to be assigned
// ===========================================================================

#[test]
fn snapshot_xmax_is_next_txn_id() {
    let tm = TransactionManager::new();

    let _t1 = tm.begin(IsolationLevel::ReadCommitted);
    let t2 = tm.begin(IsolationLevel::ReadCommitted);

    // After creating t2, the next_txn_id should be t2.txn_id + 1.
    // t2's snapshot.xmax was taken at begin time of t2, so it should
    // be at least t2.txn_id + 1 (the next id to assign).
    assert!(
        t2.snapshot.xmax > t2.txn_id,
        "xmax should be beyond the transaction's own id"
    );

    // A fresh snapshot should have xmax == next id to be assigned.
    let snap = tm.create_snapshot(99);
    // The next txn_id is t2.txn_id + 1 (since t2 was the last begun).
    assert_eq!(
        snap.xmax,
        t2.txn_id + 1,
        "xmax should equal the next txn_id to be assigned"
    );
}

#[test]
fn snapshot_xmax_increases_with_new_transactions() {
    let tm = TransactionManager::new();

    let _t1 = tm.begin(IsolationLevel::ReadCommitted);
    let snap1 = tm.create_snapshot(90);

    let _t2 = tm.begin(IsolationLevel::ReadCommitted);
    let snap2 = tm.create_snapshot(91);

    assert!(
        snap2.xmax > snap1.xmax,
        "xmax should increase as new transactions are created"
    );
}

#[test]
fn snapshot_xmax_unchanged_by_commits() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let snap_before = tm.create_snapshot(90);
    tm.commit(&mut t1).unwrap();
    let snap_after = tm.create_snapshot(91);

    // Committing doesn't create new txn ids, so xmax stays the same.
    assert_eq!(
        snap_before.xmax, snap_after.xmax,
        "xmax should not change merely from committing (no new txn ids assigned)"
    );
}

// ===========================================================================
// 4. ReadCommitted: refresh_snapshot updates snapshot to see newly committed
// ===========================================================================

#[test]
fn read_committed_refresh_sees_newly_committed() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let mut t2 = tm.begin(IsolationLevel::ReadCommitted);

    let t2_id = t2.txn_id;

    // t1's initial snapshot should not see t2 as committed.
    let initially_visible = t1.snapshot.is_visible(t2_id);
    assert!(
        !initially_visible,
        "t2 should not be visible before refresh (still active or beyond xmax)"
    );

    // Commit t2.
    tm.commit(&mut t2).unwrap();

    // Refresh t1's snapshot (READ COMMITTED allows this).
    tm.refresh_snapshot(&mut t1);

    // After refresh, t2 should be visible.
    let after_refresh_visible = t1.snapshot.is_visible(t2_id);
    assert!(
        after_refresh_visible,
        "t2 should be visible after refresh in READ COMMITTED"
    );
}

#[test]
fn read_committed_refresh_updates_xmax() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let original_xmax = t1.snapshot.xmax;

    // Start another transaction so next_txn_id advances.
    let _t2 = tm.begin(IsolationLevel::ReadCommitted);

    tm.refresh_snapshot(&mut t1);
    assert!(
        t1.snapshot.xmax > original_xmax,
        "xmax should advance after refresh when new txns have started"
    );
}

#[test]
fn read_committed_refresh_updates_active_set() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let mut t2 = tm.begin(IsolationLevel::ReadCommitted);
    let t2_id = t2.txn_id;

    // t1's snapshot should include t2 as active (or t2 is beyond xmax).
    // Either way, t2 is not visible.

    // Commit t2.
    tm.commit(&mut t2).unwrap();

    // Start a new transaction so it shows up in the refreshed active set.
    let t3 = tm.begin(IsolationLevel::ReadCommitted);

    tm.refresh_snapshot(&mut t1);

    // After refresh, t2 should no longer be in the active set.
    assert!(
        !t1.snapshot.active_txns.contains(&t2_id),
        "committed t2 should not be in refreshed active set"
    );
    // t3 should be in the active set.
    assert!(
        t1.snapshot.active_txns.contains(&t3.txn_id),
        "new active t3 should be in refreshed active set"
    );
}

#[test]
fn read_committed_multiple_refreshes() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);

    // First batch: create and commit t2.
    let mut t2 = tm.begin(IsolationLevel::ReadCommitted);
    let t2_id = t2.txn_id;
    tm.commit(&mut t2).unwrap();

    tm.refresh_snapshot(&mut t1);
    assert!(
        t1.snapshot.is_visible(t2_id),
        "t2 should be visible after first refresh"
    );

    // Second batch: create and commit t3.
    let mut t3 = tm.begin(IsolationLevel::ReadCommitted);
    let t3_id = t3.txn_id;
    tm.commit(&mut t3).unwrap();

    tm.refresh_snapshot(&mut t1);
    assert!(
        t1.snapshot.is_visible(t3_id),
        "t3 should be visible after second refresh"
    );
}

// ===========================================================================
// 5. RepeatableRead: refresh_snapshot does NOT change snapshot
// ===========================================================================

#[test]
fn repeatable_read_refresh_does_not_change_snapshot() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::RepeatableRead);
    let original_xmin = t1.snapshot.xmin;
    let original_xmax = t1.snapshot.xmax;
    let original_active = t1.snapshot.active_txns.clone();

    // Start and commit another transaction.
    let mut t2 = tm.begin(IsolationLevel::ReadCommitted);
    let t2_id = t2.txn_id;
    tm.commit(&mut t2).unwrap();

    // Attempt refresh — should be a no-op.
    tm.refresh_snapshot(&mut t1);

    assert_eq!(
        t1.snapshot.xmin, original_xmin,
        "REPEATABLE READ snapshot xmin should not change on refresh"
    );
    assert_eq!(
        t1.snapshot.xmax, original_xmax,
        "REPEATABLE READ snapshot xmax should not change on refresh"
    );
    assert_eq!(
        t1.snapshot.active_txns, original_active,
        "REPEATABLE READ snapshot active set should not change on refresh"
    );
    // Committed t2 should still not be visible.
    assert!(
        !t1.snapshot.is_visible(t2_id),
        "REPEATABLE READ should not see t2 committed after snapshot was taken"
    );
}

#[test]
fn repeatable_read_refresh_after_many_commits() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::RepeatableRead);
    let original_xmax = t1.snapshot.xmax;

    // Create and commit several transactions.
    for _ in 0..5 {
        let mut tx = tm.begin(IsolationLevel::ReadCommitted);
        tm.commit(&mut tx).unwrap();
    }

    tm.refresh_snapshot(&mut t1);
    assert_eq!(
        t1.snapshot.xmax, original_xmax,
        "REPEATABLE READ xmax must remain frozen despite new commits"
    );
}

// ===========================================================================
// 6. Serializable: refresh_snapshot does NOT change snapshot
// ===========================================================================

#[test]
fn serializable_refresh_does_not_change_snapshot() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::Serializable);
    let original_xmin = t1.snapshot.xmin;
    let original_xmax = t1.snapshot.xmax;
    let original_active = t1.snapshot.active_txns.clone();

    // Start and commit another transaction.
    let mut t2 = tm.begin(IsolationLevel::ReadCommitted);
    let t2_id = t2.txn_id;
    tm.commit(&mut t2).unwrap();

    // Attempt refresh — should be a no-op.
    tm.refresh_snapshot(&mut t1);

    assert_eq!(
        t1.snapshot.xmin, original_xmin,
        "SERIALIZABLE snapshot xmin should not change on refresh"
    );
    assert_eq!(
        t1.snapshot.xmax, original_xmax,
        "SERIALIZABLE snapshot xmax should not change on refresh"
    );
    assert_eq!(
        t1.snapshot.active_txns, original_active,
        "SERIALIZABLE snapshot active set should not change on refresh"
    );
    assert!(
        !t1.snapshot.is_visible(t2_id),
        "SERIALIZABLE should not see t2 committed after snapshot was taken"
    );
}

#[test]
fn serializable_refresh_after_many_commits() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::Serializable);
    let original_xmax = t1.snapshot.xmax;

    for _ in 0..5 {
        let mut tx = tm.begin(IsolationLevel::ReadCommitted);
        tm.commit(&mut tx).unwrap();
    }

    tm.refresh_snapshot(&mut t1);
    assert_eq!(
        t1.snapshot.xmax, original_xmax,
        "SERIALIZABLE xmax must remain frozen despite new commits"
    );
}

// ===========================================================================
// 7. Multiple concurrent transactions all appear in each other's active sets
// ===========================================================================

#[test]
fn concurrent_txns_appear_in_each_others_active_sets() {
    let tm = TransactionManager::new();

    let t1 = tm.begin(IsolationLevel::RepeatableRead);
    let t2 = tm.begin(IsolationLevel::RepeatableRead);
    let t3 = tm.begin(IsolationLevel::RepeatableRead);
    let t4 = tm.begin(IsolationLevel::RepeatableRead);

    // t2 should see t1 in its active set.
    assert!(t2.snapshot.active_txns.contains(&t1.txn_id));

    // t3 should see t1 and t2.
    assert!(t3.snapshot.active_txns.contains(&t1.txn_id));
    assert!(t3.snapshot.active_txns.contains(&t2.txn_id));

    // t4 should see t1, t2, and t3.
    assert!(t4.snapshot.active_txns.contains(&t1.txn_id));
    assert!(t4.snapshot.active_txns.contains(&t2.txn_id));
    assert!(t4.snapshot.active_txns.contains(&t3.txn_id));

    // t1 should NOT see t2, t3, or t4 (they did not exist when t1 began).
    assert!(!t1.snapshot.active_txns.contains(&t2.txn_id));
    assert!(!t1.snapshot.active_txns.contains(&t3.txn_id));
    assert!(!t1.snapshot.active_txns.contains(&t4.txn_id));
}

#[test]
fn all_concurrent_txns_invisible_to_each_other() {
    let tm = TransactionManager::new();

    let t1 = tm.begin(IsolationLevel::RepeatableRead);
    let t2 = tm.begin(IsolationLevel::RepeatableRead);
    let t3 = tm.begin(IsolationLevel::RepeatableRead);

    // No transaction should see any concurrent active transaction as visible.
    assert!(
        !t2.snapshot.is_visible(t1.txn_id),
        "t2 should not see active t1"
    );
    assert!(
        !t3.snapshot.is_visible(t1.txn_id),
        "t3 should not see active t1"
    );
    assert!(
        !t3.snapshot.is_visible(t2.txn_id),
        "t3 should not see active t2"
    );
}

#[test]
fn five_concurrent_txns_snapshot_consistency() {
    let tm = TransactionManager::new();

    let txns: Vec<_> = (0..5)
        .map(|_| tm.begin(IsolationLevel::RepeatableRead))
        .collect();

    // The last transaction should have all previous txns in its active set.
    let last = &txns[4];
    for earlier in &txns[..4] {
        assert!(
            last.snapshot.active_txns.contains(&earlier.txn_id),
            "txn {} should be in txn {}'s active set",
            earlier.txn_id,
            last.txn_id
        );
    }

    // The first transaction should have none of the later ones.
    let first = &txns[0];
    for later in &txns[1..] {
        assert!(
            !first.snapshot.active_txns.contains(&later.txn_id),
            "txn {} should NOT be in txn {}'s active set",
            later.txn_id,
            first.txn_id
        );
    }
}

// ===========================================================================
// 8. Committed transaction no longer appears in new snapshots
// ===========================================================================

#[test]
fn committed_txn_not_in_new_snapshot_active_set() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let t1_id = t1.txn_id;
    tm.commit(&mut t1).unwrap();

    // A snapshot created after t1 commits should not include t1 in active set.
    let snap = tm.create_snapshot(99);
    assert!(
        !snap.active_txns.contains(&t1_id),
        "committed t1 should not appear in new snapshot's active set"
    );
}

#[test]
fn committed_txn_visible_in_new_snapshot() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let t1_id = t1.txn_id;
    tm.commit(&mut t1).unwrap();

    // A new transaction's snapshot should see t1 as visible (committed and
    // within [xmin, xmax) and not active).
    let t2 = tm.begin(IsolationLevel::RepeatableRead);
    assert!(
        t2.snapshot.is_visible(t1_id),
        "committed t1 should be visible in t2's snapshot"
    );
}

#[test]
fn committed_txn_not_in_concurrent_new_snapshot() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let t2 = tm.begin(IsolationLevel::ReadCommitted);
    let t1_id = t1.txn_id;
    let t2_id = t2.txn_id;

    // Commit t1 while t2 is still active.
    tm.commit(&mut t1).unwrap();

    // A fresh snapshot should have t2 in active, but not t1.
    let snap = tm.create_snapshot(99);
    assert!(
        !snap.active_txns.contains(&t1_id),
        "committed t1 should not be in new snapshot"
    );
    assert!(
        snap.active_txns.contains(&t2_id),
        "still-active t2 should be in new snapshot"
    );
}

// ===========================================================================
// 9. Aborted transaction no longer appears in new snapshots
// ===========================================================================

#[test]
fn aborted_txn_not_in_new_snapshot_active_set() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let t1_id = t1.txn_id;
    tm.abort(&mut t1);

    // A snapshot created after t1 aborts should not include t1 as active.
    let snap = tm.create_snapshot(99);
    assert!(
        !snap.active_txns.contains(&t1_id),
        "aborted t1 should not appear in new snapshot's active set"
    );
}

#[test]
fn aborted_txn_removed_from_active_tracking() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let t1_id = t1.txn_id;

    assert!(tm.is_active(t1_id));
    tm.abort(&mut t1);
    assert!(
        !tm.is_active(t1_id),
        "aborted txn should be removed from active tracking"
    );
    assert!(
        !tm.is_committed(t1_id),
        "aborted txn should not be in committed set"
    );
}

#[test]
fn aborted_txn_not_in_subsequent_begin_snapshot() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let t1_id = t1.txn_id;
    tm.abort(&mut t1);

    // A transaction begun after the abort should not have t1 in its active set.
    let t2 = tm.begin(IsolationLevel::RepeatableRead);
    assert!(
        !t2.snapshot.active_txns.contains(&t1_id),
        "aborted t1 should not appear in t2's snapshot active set"
    );
}

// ===========================================================================
// 10. Snapshot with no other active txns has empty active set (except self)
// ===========================================================================

#[test]
fn snapshot_with_no_active_txns_empty_active_set() {
    let tm = TransactionManager::new();

    // No transactions have been started, so create_snapshot with an arbitrary
    // id should yield an empty active set.
    let snap = tm.create_snapshot(1);
    assert!(
        snap.active_txns.is_empty(),
        "active set should be empty when no transactions are active"
    );
}

#[test]
fn sole_transaction_snapshot_does_not_contain_self() {
    let tm = TransactionManager::new();

    let t1 = tm.begin(IsolationLevel::RepeatableRead);

    // The snapshot is taken before the txn is registered as active,
    // so t1 should NOT appear in its own active set.
    assert!(
        !t1.snapshot.active_txns.contains(&t1.txn_id),
        "sole transaction should not see itself in its own active set"
    );
    assert!(
        t1.snapshot.active_txns.is_empty(),
        "sole transaction's active set should be empty"
    );
}

#[test]
fn snapshot_after_all_others_committed_has_empty_active_set() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let mut t2 = tm.begin(IsolationLevel::ReadCommitted);
    tm.commit(&mut t1).unwrap();
    tm.commit(&mut t2).unwrap();

    // A new transaction begun after all others have committed.
    // Its snapshot is taken before it is registered as active,
    // so the active set should be empty.
    let t3 = tm.begin(IsolationLevel::RepeatableRead);

    assert!(
        !t3.snapshot.active_txns.contains(&t1.txn_id),
        "committed t1 should not be in t3's active set"
    );
    assert!(
        !t3.snapshot.active_txns.contains(&t2.txn_id),
        "committed t2 should not be in t3's active set"
    );
    assert!(
        t3.snapshot.active_txns.is_empty(),
        "active set should be empty when all prior txns have committed"
    );
}

// ===========================================================================
// 11. Sequential transactions get incrementing txn_ids
// ===========================================================================

#[test]
fn sequential_txn_ids_increment_by_one() {
    let tm = TransactionManager::new();

    let t1 = tm.begin(IsolationLevel::ReadCommitted);
    let t2 = tm.begin(IsolationLevel::ReadCommitted);
    let t3 = tm.begin(IsolationLevel::RepeatableRead);
    let t4 = tm.begin(IsolationLevel::Serializable);

    assert_eq!(t2.txn_id, t1.txn_id + 1);
    assert_eq!(t3.txn_id, t2.txn_id + 1);
    assert_eq!(t4.txn_id, t3.txn_id + 1);
}

#[test]
fn txn_ids_continue_incrementing_after_commit() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let t1_id = t1.txn_id;
    tm.commit(&mut t1).unwrap();

    let t2 = tm.begin(IsolationLevel::ReadCommitted);
    assert_eq!(
        t2.txn_id,
        t1_id + 1,
        "txn id should continue incrementing after commit"
    );
}

#[test]
fn txn_ids_continue_incrementing_after_abort() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let t1_id = t1.txn_id;
    tm.abort(&mut t1);

    let t2 = tm.begin(IsolationLevel::ReadCommitted);
    assert_eq!(
        t2.txn_id,
        t1_id + 1,
        "txn id should continue incrementing after abort"
    );
}

#[test]
fn hundred_sequential_txns_have_sequential_ids() {
    let tm = TransactionManager::new();
    let first = tm.begin(IsolationLevel::ReadCommitted);
    let base_id = first.txn_id;

    for i in 1..100u64 {
        let t = tm.begin(IsolationLevel::ReadCommitted);
        assert_eq!(
            t.txn_id,
            base_id + i,
            "txn_id at iteration {i} should be base + {i}"
        );
    }
}

// ===========================================================================
// 12. Snapshot isolation: later snapshot sees committed txn that earlier didn't
// ===========================================================================

#[test]
fn later_snapshot_sees_committed_txn_earlier_did_not() {
    let tm = TransactionManager::new();

    // Start t1 and t2 concurrently.
    let t1 = tm.begin(IsolationLevel::RepeatableRead);
    let mut t2 = tm.begin(IsolationLevel::ReadCommitted);
    let t2_id = t2.txn_id;

    // t1's snapshot was taken before t2 committed, so t2 is not visible to t1.
    assert!(
        !t1.snapshot.is_visible(t2_id),
        "t1's snapshot should not see active t2"
    );

    // Now commit t2.
    tm.commit(&mut t2).unwrap();

    // Start a new transaction t3 — its snapshot is taken AFTER t2 committed.
    let t3 = tm.begin(IsolationLevel::RepeatableRead);

    // t3 should see t2 as visible.
    assert!(
        t3.snapshot.is_visible(t2_id),
        "t3's snapshot should see t2, which committed before t3 began"
    );

    // t1's original snapshot should STILL not see t2 (REPEATABLE READ is frozen).
    assert!(
        !t1.snapshot.is_visible(t2_id),
        "t1's REPEATABLE READ snapshot should remain frozen and not see t2"
    );
}

#[test]
fn read_committed_can_see_concurrent_commit_after_refresh() {
    let tm = TransactionManager::new();

    let mut observer = tm.begin(IsolationLevel::ReadCommitted);
    let mut writer = tm.begin(IsolationLevel::ReadCommitted);
    let writer_id = writer.txn_id;

    // Observer cannot see writer initially.
    assert!(!observer.snapshot.is_visible(writer_id));

    // Writer commits.
    tm.commit(&mut writer).unwrap();

    // Observer refreshes and can now see the writer.
    tm.refresh_snapshot(&mut observer);
    assert!(
        observer.snapshot.is_visible(writer_id),
        "READ COMMITTED observer should see writer after refresh"
    );
}

#[test]
fn repeatable_read_cannot_see_concurrent_commit_even_after_refresh() {
    let tm = TransactionManager::new();

    let mut observer = tm.begin(IsolationLevel::RepeatableRead);
    let mut writer = tm.begin(IsolationLevel::ReadCommitted);
    let writer_id = writer.txn_id;

    // Observer cannot see writer initially.
    assert!(!observer.snapshot.is_visible(writer_id));

    // Writer commits.
    tm.commit(&mut writer).unwrap();

    // Observer tries to refresh — no effect for REPEATABLE READ.
    tm.refresh_snapshot(&mut observer);
    assert!(
        !observer.snapshot.is_visible(writer_id),
        "REPEATABLE READ observer should NOT see writer even after refresh attempt"
    );
}

#[test]
fn serializable_cannot_see_concurrent_commit_even_after_refresh() {
    let tm = TransactionManager::new();

    let mut observer = tm.begin(IsolationLevel::Serializable);
    let mut writer = tm.begin(IsolationLevel::ReadCommitted);
    let writer_id = writer.txn_id;

    assert!(!observer.snapshot.is_visible(writer_id));

    tm.commit(&mut writer).unwrap();

    tm.refresh_snapshot(&mut observer);
    assert!(
        !observer.snapshot.is_visible(writer_id),
        "SERIALIZABLE observer should NOT see writer even after refresh attempt"
    );
}

#[test]
fn snapshot_isolation_chain_of_commits() {
    let tm = TransactionManager::new();

    // Create a chain: t1 commits, t2 sees t1 then commits, t3 sees both.
    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let t1_id = t1.txn_id;
    tm.commit(&mut t1).unwrap();

    let mut t2 = tm.begin(IsolationLevel::ReadCommitted);
    let t2_id = t2.txn_id;
    assert!(
        t2.snapshot.is_visible(t1_id),
        "t2 should see committed t1"
    );
    tm.commit(&mut t2).unwrap();

    let t3 = tm.begin(IsolationLevel::RepeatableRead);
    assert!(
        t3.snapshot.is_visible(t1_id),
        "t3 should see committed t1"
    );
    assert!(
        t3.snapshot.is_visible(t2_id),
        "t3 should see committed t2"
    );
}

// ===========================================================================
// Additional edge cases
// ===========================================================================

#[test]
fn snapshot_xmin_boundary_visible() {
    // A txn_id exactly equal to xmin should be visible (it is < xmax and not active).
    let snap = Snapshot::new(5, 20, HashSet::new());
    assert!(snap.is_visible(5), "txn at xmin should be visible");
}

#[test]
fn snapshot_below_xmin_visible() {
    let snap = Snapshot::new(5, 20, HashSet::new());
    assert!(snap.is_visible(1), "txn below xmin should be visible");
}

#[test]
fn snapshot_xmax_boundary_not_visible() {
    // A txn_id equal to xmax is NOT visible (xmax is exclusive upper bound).
    let snap = Snapshot::new(5, 20, HashSet::new());
    assert!(
        !snap.is_visible(20),
        "txn at xmax should not be visible (exclusive)"
    );
}

#[test]
fn snapshot_above_xmax_not_visible() {
    let snap = Snapshot::new(5, 20, HashSet::new());
    assert!(!snap.is_visible(21), "txn above xmax should not be visible");
    assert!(
        !snap.is_visible(100),
        "txn far above xmax should not be visible"
    );
}

#[test]
fn snapshot_just_below_xmax_visible() {
    let snap = Snapshot::new(5, 20, HashSet::new());
    assert!(
        snap.is_visible(19),
        "txn just below xmax should be visible"
    );
}

#[test]
fn active_txn_not_visible_in_snapshot() {
    let mut active = HashSet::new();
    active.insert(7u64);
    active.insert(12u64);
    let snap = Snapshot::new(5, 20, active);

    assert!(
        !snap.is_visible(7),
        "active txn 7 should not be visible in snapshot"
    );
    assert!(
        !snap.is_visible(12),
        "active txn 12 should not be visible in snapshot"
    );
    // Non-active txn in range should still be visible.
    assert!(snap.is_visible(8), "non-active txn 8 should be visible");
    assert!(snap.is_visible(5), "non-active txn 5 should be visible");
}

#[test]
fn multiple_active_txns_all_excluded() {
    let active: HashSet<u64> = vec![3, 5, 9, 14].into_iter().collect();
    let snap = Snapshot::new(1, 20, active);

    for id in &[3, 5, 9, 14] {
        assert!(
            !snap.is_visible(*id),
            "active txn {id} should not be visible"
        );
    }
    // Others in range should be visible.
    for id in &[1, 2, 4, 6, 7, 8, 10, 11, 12, 13, 15, 19] {
        assert!(snap.is_visible(*id), "txn {id} should be visible");
    }
}

#[test]
fn txn_started_after_snapshot_not_visible() {
    let snap = Snapshot::new(1, 10, HashSet::new());

    assert!(
        !snap.is_visible(10),
        "txn 10 (at xmax) started after snapshot"
    );
    assert!(
        !snap.is_visible(11),
        "txn 11 (above xmax) started after snapshot"
    );
    assert!(
        !snap.is_visible(1000),
        "txn 1000 started long after snapshot"
    );
}
