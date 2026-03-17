use std::collections::HashSet;

use sqld::transaction::mvcc::{Snapshot, TxnStatusLookup, VisibilityCheck};
use sqld::transaction::TransactionStatus;
use sqld::types::tuple::MvccHeader;

// ===========================================================================
// Mock status lookup
// ===========================================================================

struct MockStatus {
    committed: HashSet<u64>,
    aborted: HashSet<u64>,
    active: HashSet<u64>,
}

impl MockStatus {
    fn new() -> Self {
        Self {
            committed: HashSet::new(),
            aborted: HashSet::new(),
            active: HashSet::new(),
        }
    }

    fn with_committed(mut self, txn_id: u64) -> Self {
        self.committed.insert(txn_id);
        self
    }

    fn with_aborted(mut self, txn_id: u64) -> Self {
        self.aborted.insert(txn_id);
        self
    }

    fn with_active(mut self, txn_id: u64) -> Self {
        self.active.insert(txn_id);
        self
    }
}

impl TxnStatusLookup for MockStatus {
    fn status_of(&self, txn_id: u64) -> Option<TransactionStatus> {
        if self.committed.contains(&txn_id) {
            Some(TransactionStatus::Committed)
        } else if self.aborted.contains(&txn_id) {
            Some(TransactionStatus::Aborted)
        } else if self.active.contains(&txn_id) {
            Some(TransactionStatus::Active)
        } else {
            None
        }
    }
}

// ===========================================================================
// 1. Own insert is visible
// ===========================================================================

#[test]
fn own_insert_is_visible() {
    // Txn 10 inserts a tuple (xmin=10, xmax=0). Txn 10 should see it.
    let snap = Snapshot::new(10, 20, HashSet::new());
    let header = MvccHeader::new_insert(10, 0);
    let status = MockStatus::new();
    assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

#[test]
fn own_insert_visible_regardless_of_status_lookup() {
    // Even if the status lookup has no entry for our txn, we see our own row.
    let snap = Snapshot::new(10, 20, HashSet::new());
    let header = MvccHeader::new_insert(10, 1);
    let status = MockStatus::new(); // no entries at all
    assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

#[test]
fn own_insert_visible_with_nonzero_cid() {
    // cid varies but the tuple is still our own insert.
    let snap = Snapshot::new(10, 20, HashSet::new());
    let header = MvccHeader::new_insert(10, 5);
    let status = MockStatus::new();
    assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

// ===========================================================================
// 2. Own delete is not visible
// ===========================================================================

#[test]
fn own_delete_not_visible() {
    // Txn 10 reads a tuple created by committed txn 5, then txn 10 deletes
    // it (xmax=10). Txn 10 should NOT see the tuple.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new(5, 10, 0);
    let status = MockStatus::new().with_committed(5);
    assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

// ===========================================================================
// 3. Committed insert in snapshot is visible
// ===========================================================================

#[test]
fn committed_insert_in_snapshot_visible() {
    // Txn 5 committed before the snapshot was taken. Txn 10 should see the tuple.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new_insert(5, 0);
    let status = MockStatus::new().with_committed(5);
    assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

#[test]
fn committed_insert_at_snapshot_boundary_visible() {
    // xmin is just below snapshot xmax -> still in snapshot.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new_insert(19, 0);
    let status = MockStatus::new().with_committed(19);
    assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

#[test]
fn committed_insert_visible_not_in_active_set() {
    // Txn 5 committed, not in the active set -> visible.
    let mut active = HashSet::new();
    active.insert(3u64);
    let snap = Snapshot::new(1, 20, active);
    let header = MvccHeader::new_insert(5, 0);
    let status = MockStatus::new().with_committed(5);
    assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

// ===========================================================================
// 4. Uncommitted (active) insert is not visible
// ===========================================================================

#[test]
fn uncommitted_insert_not_visible() {
    // Txn 5 is still active. Txn 10 should not see the tuple.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new_insert(5, 0);
    let status = MockStatus::new().with_active(5);
    assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

// ===========================================================================
// 5. Aborted insert is not visible
// ===========================================================================

#[test]
fn aborted_insert_not_visible() {
    // Txn 5 aborted. Its inserts should be invisible to everyone.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new_insert(5, 0);
    let status = MockStatus::new().with_aborted(5);
    assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

#[test]
fn aborted_insert_with_xmax_set_not_visible() {
    // Even if someone set xmax on an aborted insert, the row never existed.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new(5, 7, 0);
    let status = MockStatus::new().with_aborted(5).with_committed(7);
    assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

// ===========================================================================
// 6. Insert by unknown txn is not visible
// ===========================================================================

#[test]
fn unknown_xmin_status_not_visible() {
    // If the status lookup returns None for xmin, the tuple is not visible.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new_insert(5, 0);
    let status = MockStatus::new(); // no entry for txn 5
    assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

// ===========================================================================
// 7. Committed delete in snapshot hides tuple
// ===========================================================================

#[test]
fn committed_delete_in_snapshot_hides_tuple() {
    // Txn 5 inserted, txn 7 deleted. Both committed and in the snapshot.
    // The tuple should not be visible because the delete is committed and
    // visible within the snapshot.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new(5, 7, 0);
    let status = MockStatus::new().with_committed(5).with_committed(7);
    assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

#[test]
fn committed_delete_at_snapshot_boundary_hides_tuple() {
    // xmax = 19 which is < snapshot xmax (20) and not in active set -> in snapshot.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new(5, 19, 0);
    let status = MockStatus::new().with_committed(5).with_committed(19);
    assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

// ===========================================================================
// 8. Committed delete NOT in snapshot still shows tuple
//    (the deleting txn committed after our snapshot was taken)
// ===========================================================================

#[test]
fn committed_delete_after_snapshot_still_visible() {
    // Txn 5 inserted, txn 25 deleted. Snapshot xmax is 20, so txn 25 is
    // >= snapshot xmax and therefore not in the snapshot. Tuple is visible.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new(5, 25, 0);
    let status = MockStatus::new().with_committed(5).with_committed(25);
    assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

#[test]
fn committed_delete_was_active_at_snapshot_time_still_visible() {
    // Txn 7 committed after snapshot but was in the active set at snapshot
    // time. Snapshot does not see txn 7's changes -> tuple remains visible.
    let mut active = HashSet::new();
    active.insert(7u64);
    let snap = Snapshot::new(1, 20, active);
    let header = MvccHeader::new(5, 7, 0);
    let status = MockStatus::new().with_committed(5).with_committed(7);
    assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

// ===========================================================================
// 9. Aborted delete still shows tuple (deletion rolled back)
// ===========================================================================

#[test]
fn aborted_delete_tuple_still_visible() {
    // Txn 5 committed the insert, txn 7 tried to delete but aborted.
    // The tuple should still be visible because the delete was rolled back.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new(5, 7, 0);
    let status = MockStatus::new().with_committed(5).with_aborted(7);
    assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

#[test]
fn aborted_delete_on_own_insert_still_visible() {
    // We inserted (xmin=10), some other txn tried to delete (xmax=7) and aborted.
    let snap = Snapshot::new(10, 20, HashSet::new());
    let header = MvccHeader::new(10, 7, 0);
    let status = MockStatus::new().with_aborted(7);
    assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

// ===========================================================================
// 10. Active deleter doesn't hide tuple from other txns
// ===========================================================================

#[test]
fn active_deleter_does_not_hide_tuple() {
    // Txn 5 committed insert, txn 7 is still active and marked xmax.
    // Txn 10 should still see the tuple because 7 has not committed the delete.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new(5, 7, 0);
    let status = MockStatus::new().with_committed(5).with_active(7);
    assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

#[test]
fn unknown_deleter_status_does_not_hide_tuple() {
    // If the deleter's status is unknown (None), treat conservatively -> visible.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new(5, 7, 0);
    let status = MockStatus::new().with_committed(5); // no entry for 7
    assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

// ===========================================================================
// 11. Own insert then own delete -> not visible
// ===========================================================================

#[test]
fn own_insert_then_own_delete_not_visible() {
    // Txn 10 both inserts and deletes the same tuple in the same transaction.
    // xmin == txn_id (our insert), xmax == txn_id (our delete) -> not visible.
    let snap = Snapshot::new(10, 20, HashSet::new());
    let header = MvccHeader::new(10, 10, 0);
    let status = MockStatus::new();
    assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

// ===========================================================================
// 12. Write-write conflict detection: committed xmax -> conflict
// ===========================================================================

#[test]
fn write_conflict_committed_xmax() {
    // Another committed txn already set xmax -> conflict exists.
    let header = MvccHeader::new(5, 7, 0);
    let status = MockStatus::new().with_committed(7);
    assert!(VisibilityCheck::check_write_conflict(&header, 10, &status));
}

// ===========================================================================
// 13. Write-write conflict detection: active xmax -> conflict
// ===========================================================================

#[test]
fn write_conflict_active_xmax() {
    // Another active txn holds xmax -> conflict (must wait or abort).
    let header = MvccHeader::new(5, 7, 0);
    let status = MockStatus::new().with_active(7);
    assert!(VisibilityCheck::check_write_conflict(&header, 10, &status));
}

// ===========================================================================
// 14. Write-write conflict detection: aborted xmax -> no conflict
// ===========================================================================

#[test]
fn no_write_conflict_aborted_xmax() {
    // The txn that set xmax aborted, so the delete is rolled back -> no conflict.
    let header = MvccHeader::new(5, 7, 0);
    let status = MockStatus::new().with_aborted(7);
    assert!(!VisibilityCheck::check_write_conflict(&header, 10, &status));
}

// ===========================================================================
// 15. Write-write conflict detection: no xmax -> no conflict
// ===========================================================================

#[test]
fn no_write_conflict_no_xmax() {
    // xmax=0 means nobody has deleted/updated the tuple -> no conflict.
    let header = MvccHeader::new_insert(5, 0);
    let status = MockStatus::new();
    assert!(!VisibilityCheck::check_write_conflict(&header, 10, &status));
}

#[test]
fn no_write_conflict_own_xmax() {
    // We already set xmax ourselves -> no conflict with ourselves.
    let header = MvccHeader::new(5, 10, 0);
    let status = MockStatus::new().with_active(10);
    assert!(!VisibilityCheck::check_write_conflict(&header, 10, &status));
}

#[test]
fn no_write_conflict_unknown_xmax_status() {
    // Unknown status (None) for the deleter -> treated as no conflict.
    let header = MvccHeader::new(5, 7, 0);
    let status = MockStatus::new(); // no entry for 7
    assert!(!VisibilityCheck::check_write_conflict(&header, 10, &status));
}

// ===========================================================================
// 16. Tuple with xmin in active set of snapshot -> not visible
// ===========================================================================

#[test]
fn xmin_in_active_set_not_visible() {
    // Txn 5 committed after the snapshot was taken, but was in the active set
    // at snapshot time. The tuple should NOT be visible to txn 10.
    let mut active = HashSet::new();
    active.insert(5u64);
    let snap = Snapshot::new(1, 20, active);
    let header = MvccHeader::new_insert(5, 0);
    let status = MockStatus::new().with_committed(5);
    assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

#[test]
fn xmin_in_active_set_among_multiple_not_visible() {
    // Multiple active txns at snapshot time; txn 5 is one of them.
    let mut active = HashSet::new();
    active.insert(3u64);
    active.insert(5u64);
    active.insert(7u64);
    let snap = Snapshot::new(1, 20, active);
    let header = MvccHeader::new_insert(5, 0);
    let status = MockStatus::new().with_committed(5);
    assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

// ===========================================================================
// 17. Tuple with xmin >= xmax of snapshot -> not visible (future txn)
// ===========================================================================

#[test]
fn future_xmin_not_visible() {
    // xmin = 25 which is >= snapshot xmax (20). Even if committed, snapshot
    // does not include it.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new_insert(25, 0);
    let status = MockStatus::new().with_committed(25);
    assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

#[test]
fn xmin_equal_to_snapshot_xmax_not_visible() {
    // xmin == snapshot xmax is the boundary case: not visible.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new_insert(20, 0);
    let status = MockStatus::new().with_committed(20);
    assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

#[test]
fn far_future_xmin_not_visible() {
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new_insert(1000, 0);
    let status = MockStatus::new().with_committed(1000);
    assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

// ===========================================================================
// Snapshot::is_visible unit tests
// ===========================================================================

#[test]
fn snapshot_visibility_below_xmax_not_active_is_visible() {
    let mut active = HashSet::new();
    active.insert(3u64);
    let snap = Snapshot::new(1, 10, active);

    // Below xmax and not in active set -> visible.
    assert!(snap.is_visible(1));
    assert!(snap.is_visible(2));
    assert!(snap.is_visible(5));
    assert!(snap.is_visible(9));

    // In active set -> not visible.
    assert!(!snap.is_visible(3));

    // >= xmax -> not visible.
    assert!(!snap.is_visible(10));
    assert!(!snap.is_visible(100));
}

#[test]
fn snapshot_empty_active_set() {
    let snap = Snapshot::new(1, 10, HashSet::new());
    assert!(snap.is_visible(1));
    assert!(snap.is_visible(9));
    assert!(!snap.is_visible(10));
}

#[test]
fn snapshot_all_active() {
    let active: HashSet<u64> = (1..10).collect();
    let snap = Snapshot::new(1, 10, active);
    for i in 1..10 {
        assert!(!snap.is_visible(i), "txn {} should not be visible", i);
    }
    assert!(!snap.is_visible(10));
}

// ===========================================================================
// Combined / edge-case scenarios
// ===========================================================================

#[test]
fn committed_insert_deleted_by_aborted_txn_visible() {
    // A committed row whose deletion was aborted is visible.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new(3, 8, 0);
    let status = MockStatus::new().with_committed(3).with_aborted(8);
    assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

#[test]
fn committed_insert_deleted_by_committed_txn_not_visible() {
    // A committed row deleted by a committed txn (both in snapshot) is not visible.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new(3, 8, 0);
    let status = MockStatus::new().with_committed(3).with_committed(8);
    assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

#[test]
fn own_insert_with_xmax_zero_visible() {
    // Our own insert with no deletion is visible.
    let snap = Snapshot::new(10, 20, HashSet::new());
    let header = MvccHeader::new(10, 0, 0);
    let status = MockStatus::new();
    assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
}

#[test]
fn visibility_and_no_conflict_are_complementary() {
    // A tuple that is visible and has no conflict (xmax=0).
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new_insert(5, 0);
    let status = MockStatus::new().with_committed(5);
    assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
    assert!(!VisibilityCheck::check_write_conflict(&header, 10, &status));
}

#[test]
fn visible_tuple_can_have_write_conflict() {
    // A tuple can be visible to us yet have a write conflict.
    // xmin=5 committed, xmax=7 active (not us) -> visible, but conflict.
    let snap = Snapshot::new(1, 20, HashSet::new());
    let header = MvccHeader::new(5, 7, 0);
    let status = MockStatus::new().with_committed(5).with_active(7);
    assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
    assert!(VisibilityCheck::check_write_conflict(&header, 10, &status));
}

#[test]
fn multiple_active_txns_only_non_active_committed_visible() {
    // Several txns active at snapshot time -- only non-active committed txns
    // produce visible tuples.
    let mut active = HashSet::new();
    active.insert(3u64);
    active.insert(5u64);
    active.insert(7u64);
    let snap = Snapshot::new(1, 20, active);

    // Txn 4 committed and not in active set -> visible.
    let header4 = MvccHeader::new_insert(4, 0);
    let status = MockStatus::new()
        .with_committed(4)
        .with_active(3)
        .with_active(5)
        .with_active(7);
    assert!(VisibilityCheck::is_visible(&header4, 10, &snap, &status));

    // Txn 5 committed but was in active set at snapshot time -> not visible.
    let header5 = MvccHeader::new_insert(5, 0);
    let status5 = MockStatus::new().with_committed(5);
    assert!(!VisibilityCheck::is_visible(&header5, 10, &snap, &status5));
}
