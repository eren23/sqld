use std::collections::{HashMap, HashSet};

use crate::utils::error::{Error, TransactionError};

use super::lock_manager::{LockManager, LockTarget};
use super::mvcc::Snapshot;

// ---------------------------------------------------------------------------
// RW-dependency edge
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RwDependency {
    /// The reader transaction.
    pub reader: u64,
    /// The writer transaction.
    pub writer: u64,
}

// ---------------------------------------------------------------------------
// SSI Manager
// ---------------------------------------------------------------------------

/// Tracks rw-dependencies between serializable transactions and detects
/// dangerous structures (T1 →rw→ T2 →rw→ T3 where T1 and T3 overlap).
pub struct SsiManager {
    /// rw-dependency edges: reader → set of writers.
    rw_out: HashMap<u64, HashSet<u64>>,
    /// Reverse index: writer → set of readers.
    rw_in: HashMap<u64, HashSet<u64>>,
    /// Snapshots for committed transactions (kept until safe to discard).
    snapshots: HashMap<u64, Snapshot>,
    /// Set of committed txn_ids managed by SSI.
    committed: HashSet<u64>,
    /// Set of aborted txn_ids.
    aborted: HashSet<u64>,
}

impl SsiManager {
    pub fn new() -> Self {
        Self {
            rw_out: HashMap::new(),
            rw_in: HashMap::new(),
            snapshots: HashMap::new(),
            committed: HashSet::new(),
            aborted: HashSet::new(),
        }
    }

    /// Register a transaction's snapshot for SSI tracking.
    pub fn register(&mut self, txn_id: u64, snapshot: Snapshot) {
        self.snapshots.insert(txn_id, snapshot);
    }

    /// Record that `reader` read data that `writer` later wrote (or will write).
    /// This creates an rw-dependency edge: reader →rw→ writer.
    pub fn add_rw_dependency(&mut self, reader: u64, writer: u64) {
        if reader == writer {
            return;
        }
        self.rw_out.entry(reader).or_default().insert(writer);
        self.rw_in.entry(writer).or_default().insert(reader);
    }

    /// Record that `writer` wrote data that `reader` had previously read
    /// (detected via SIRead locks). This is the typical runtime detection:
    /// when a transaction writes a row, check if any other transaction holds
    /// a SIRead lock on that row.
    pub fn record_write_over_siread(
        &mut self,
        writer: u64,
        target: &LockTarget,
        lock_manager: &LockManager,
    ) {
        let siread_locks = lock_manager.get_siread_locks();
        for (lock_target, reader) in &siread_locks {
            if lock_target == target && *reader != writer {
                // reader →rw→ writer (reader read it, writer is overwriting).
                self.add_rw_dependency(*reader, writer);
            }
        }
    }

    /// Check for dangerous structure on commit of `txn_id`.
    ///
    /// Dangerous structure: T1 →rw→ T2 →rw→ T3 where T1 and T3 are
    /// concurrent (their snapshots overlap — i.e., T3 started before T1
    /// committed, or equivalently T1 was active when T3's snapshot was taken).
    ///
    /// Special case: when T1 == T3 (2-transaction write-skew), the structure
    /// T1 →rw→ T2 →rw→ T1 is dangerous if T1 and T2 are concurrent.
    ///
    /// Returns Err if the committing transaction should be aborted.
    pub fn pre_commit_check(&self, txn_id: u64) -> Result<(), Error> {
        // txn_id is T2 in the dangerous structure.
        // Check: exists T1 →rw→ txn_id →rw→ T3 where T1 ∥ T3.

        let in_edges = self.rw_in.get(&txn_id); // T1s → txn_id
        let out_edges = self.rw_out.get(&txn_id); // txn_id → T3s

        if in_edges.is_none() || out_edges.is_none() {
            return Ok(());
        }

        let t1_set = in_edges.unwrap();
        let t3_set = out_edges.unwrap();

        for &t1 in t1_set {
            if self.aborted.contains(&t1) {
                continue;
            }
            for &t3 in t3_set {
                if self.aborted.contains(&t3) {
                    continue;
                }
                if t1 == t3 {
                    // 2-transaction write-skew: T1 →rw→ T2 →rw→ T1.
                    // Dangerous if T1 and T2 (the committing txn) are concurrent.
                    if self.are_concurrent(t1, txn_id) {
                        return Err(
                            TransactionError::SerializationFailure { txn_id }.into(),
                        );
                    }
                    continue;
                }
                if self.are_concurrent(t1, t3) {
                    return Err(TransactionError::SerializationFailure { txn_id }.into());
                }
            }
        }

        Ok(())
    }

    /// Mark a transaction as committed in SSI tracking.
    pub fn mark_committed(&mut self, txn_id: u64) {
        self.committed.insert(txn_id);
    }

    /// Mark a transaction as aborted in SSI tracking.
    pub fn mark_aborted(&mut self, txn_id: u64) {
        self.aborted.insert(txn_id);
        // Clean up edges involving this txn.
        self.rw_out.remove(&txn_id);
        self.rw_in.remove(&txn_id);
        // Also remove from other transactions' edge sets.
        for set in self.rw_out.values_mut() {
            set.remove(&txn_id);
        }
        for set in self.rw_in.values_mut() {
            set.remove(&txn_id);
        }
        self.snapshots.remove(&txn_id);
    }

    /// Two transactions are concurrent if each started before the other
    /// committed. With snapshots: T3's snapshot contains T1 as active
    /// (T1 hadn't committed when T3 started), OR T1's snapshot contains T3.
    fn are_concurrent(&self, t1: u64, t3: u64) -> bool {
        // Check if T1 was in T3's active set (T1 hadn't committed when T3 started).
        if let Some(snap_t3) = self.snapshots.get(&t3) {
            if snap_t3.active_txns.contains(&t1) {
                return true;
            }
        }

        // Check if T3 was in T1's active set (T3 hadn't committed when T1 started).
        if let Some(snap_t1) = self.snapshots.get(&t1) {
            if snap_t1.active_txns.contains(&t3) {
                return true;
            }
        }

        // Fallback: if both are still active (neither committed), they're concurrent.
        if !self.committed.contains(&t1) && !self.committed.contains(&t3) {
            return true;
        }

        false
    }

    /// Clean up state for transactions that are no longer needed.
    pub fn cleanup(&mut self, txn_id: u64) {
        self.rw_out.remove(&txn_id);
        self.rw_in.remove(&txn_id);
        self.snapshots.remove(&txn_id);
        self.committed.remove(&txn_id);
        self.aborted.remove(&txn_id);
    }

    /// Get the number of rw-dependencies tracked.
    pub fn dependency_count(&self) -> usize {
        self.rw_out.values().map(|s| s.len()).sum()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_snapshot(active: &[u64], xmin: u64, xmax: u64) -> Snapshot {
        let active_set: HashSet<u64> = active.iter().copied().collect();
        Snapshot::new(xmin, xmax, active_set)
    }

    #[test]
    fn no_dependencies_passes() {
        let ssi = SsiManager::new();
        assert!(ssi.pre_commit_check(1).is_ok());
    }

    #[test]
    fn single_dependency_passes() {
        let mut ssi = SsiManager::new();
        ssi.register(1, make_snapshot(&[], 1, 3));
        ssi.register(2, make_snapshot(&[1], 1, 3));
        ssi.add_rw_dependency(1, 2);
        // T2 committing: only one edge, no dangerous structure.
        assert!(ssi.pre_commit_check(2).is_ok());
    }

    #[test]
    fn dangerous_structure_detected() {
        let mut ssi = SsiManager::new();
        // T1, T2, T3 all concurrent.
        ssi.register(1, make_snapshot(&[2, 3], 1, 4));
        ssi.register(2, make_snapshot(&[1, 3], 1, 4));
        ssi.register(3, make_snapshot(&[1, 2], 1, 4));

        // T1 →rw→ T2 →rw→ T3
        ssi.add_rw_dependency(1, 2);
        ssi.add_rw_dependency(2, 3);

        // T2 tries to commit: T1 and T3 are concurrent → dangerous.
        let result = ssi.pre_commit_check(2);
        assert!(result.is_err());
    }

    #[test]
    fn aborted_transaction_breaks_structure() {
        let mut ssi = SsiManager::new();
        ssi.register(1, make_snapshot(&[2, 3], 1, 4));
        ssi.register(2, make_snapshot(&[1, 3], 1, 4));
        ssi.register(3, make_snapshot(&[1, 2], 1, 4));

        ssi.add_rw_dependency(1, 2);
        ssi.add_rw_dependency(2, 3);

        // Abort T1 — breaks the chain.
        ssi.mark_aborted(1);

        assert!(ssi.pre_commit_check(2).is_ok());
    }

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

        // T1 and T3 are NOT concurrent (T1 committed, T3 doesn't see T1 as active).
        assert!(ssi.pre_commit_check(2).is_ok());
    }

    #[test]
    fn write_skew_scenario() {
        // Classic write-skew: two concurrent transactions each read what the
        // other writes. T1 reads X (which T2 writes), T2 reads Y (which T1 writes).
        let mut ssi = SsiManager::new();
        ssi.register(1, make_snapshot(&[2], 1, 3));
        ssi.register(2, make_snapshot(&[1], 1, 3));

        // T1 →rw→ T2 (T1 read X, T2 wrote X)
        // T2 →rw→ T1 (T2 read Y, T1 wrote Y)
        ssi.add_rw_dependency(1, 2);
        ssi.add_rw_dependency(2, 1);

        assert_eq!(ssi.dependency_count(), 2);

        // When T2 tries to commit: T1 →rw→ T2 →rw→ T1.
        // T1 == T3, so the 2-txn write-skew path fires: T1 and T2 are
        // concurrent → serialization failure.
        let result = ssi.pre_commit_check(2);
        assert!(result.is_err());
    }

    #[test]
    fn cleanup_removes_state() {
        let mut ssi = SsiManager::new();
        ssi.register(1, make_snapshot(&[], 1, 2));
        ssi.add_rw_dependency(1, 2);
        ssi.mark_committed(1);

        ssi.cleanup(1);
        assert_eq!(ssi.dependency_count(), 0);
    }
}
