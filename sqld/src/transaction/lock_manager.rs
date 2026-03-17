use std::collections::{HashMap, HashSet, VecDeque};

use crate::utils::error::Error;

// ---------------------------------------------------------------------------
// Lock modes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Shared,
    Exclusive,
    /// Serializable-isolation read lock (advisory — never blocks).
    SIRead,
    /// Intention shared (for DDL protection).
    IntentionShared,
    /// Intention exclusive (for DDL protection).
    IntentionExclusive,
}

impl LockMode {
    /// Returns true if `self` conflicts with `other`.
    pub fn conflicts_with(&self, other: &LockMode) -> bool {
        use LockMode::*;
        // SIRead never blocks.
        if *self == SIRead || *other == SIRead {
            return false;
        }
        matches!(
            (self, other),
            (Exclusive, Exclusive)
                | (Exclusive, Shared)
                | (Shared, Exclusive)
                | (Exclusive, IntentionShared)
                | (IntentionShared, Exclusive)
                | (Exclusive, IntentionExclusive)
                | (IntentionExclusive, Exclusive)
        )
    }
}

// ---------------------------------------------------------------------------
// Lock target
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockTarget {
    Row { table_id: u64, tuple_id: u64 },
    Table(u64),
}

// ---------------------------------------------------------------------------
// Lock entry
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct LockEntry {
    /// Set of (txn_id, mode) pairs currently holding the lock.
    pub holders: Vec<(u64, LockMode)>,
    /// FIFO queue of transactions waiting to acquire this lock.
    pub wait_queue: VecDeque<(u64, LockMode)>,
}

impl LockEntry {
    fn new() -> Self {
        Self {
            holders: Vec::new(),
            wait_queue: VecDeque::new(),
        }
    }

    /// Check if `mode` can be granted given current holders, ignoring `txn_id`
    /// (a txn doesn't conflict with itself for upgrade purposes).
    fn can_grant(&self, txn_id: u64, mode: LockMode) -> bool {
        for &(holder_txn, holder_mode) in &self.holders {
            if holder_txn == txn_id {
                continue;
            }
            if mode.conflicts_with(&holder_mode) {
                return false;
            }
        }
        true
    }

    /// Check if txn already holds this lock at this mode or stronger.
    fn already_holds(&self, txn_id: u64, mode: LockMode) -> bool {
        for &(holder_txn, holder_mode) in &self.holders {
            if holder_txn == txn_id {
                if holder_mode == mode {
                    return true;
                }
                // Exclusive subsumes Shared.
                if holder_mode == LockMode::Exclusive && mode == LockMode::Shared {
                    return true;
                }
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Lock manager
// ---------------------------------------------------------------------------

pub struct LockManager {
    locks: std::sync::Mutex<HashMap<LockTarget, LockEntry>>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Attempt to acquire a lock. Returns Ok(true) if granted, Ok(false) if
    /// the txn has been queued (would block), Err on error.
    pub fn acquire(
        &self,
        txn_id: u64,
        target: LockTarget,
        mode: LockMode,
    ) -> Result<bool, Error> {
        let mut locks = self.locks.lock().unwrap();
        let entry = locks.entry(target).or_insert_with(LockEntry::new);

        // Already hold it?
        if entry.already_holds(txn_id, mode) {
            return Ok(true);
        }

        // SIRead locks are advisory — always grant immediately.
        if mode == LockMode::SIRead {
            entry.holders.push((txn_id, mode));
            return Ok(true);
        }

        // Can we grant right now?
        if entry.wait_queue.is_empty() && entry.can_grant(txn_id, mode) {
            // If upgrading from Shared to Exclusive, remove old Shared.
            if mode == LockMode::Exclusive {
                entry.holders.retain(|&(t, _)| t != txn_id);
            }
            entry.holders.push((txn_id, mode));
            Ok(true)
        } else {
            // Enqueue.
            entry.wait_queue.push_back((txn_id, mode));
            Ok(false)
        }
    }

    /// Release all locks held by a transaction and try to wake up waiters.
    pub fn release_all(&self, txn_id: u64) {
        let mut locks = self.locks.lock().unwrap();
        let targets: Vec<LockTarget> = locks.keys().cloned().collect();

        for target in targets {
            if let Some(entry) = locks.get_mut(&target) {
                entry.holders.retain(|&(t, _)| t != txn_id);
                entry.wait_queue.retain(|&(t, _)| t != txn_id);
                // Try to grant queued requests.
                Self::process_wait_queue(entry);
            }
        }

        // Clean up empty entries.
        locks.retain(|_, entry| !entry.holders.is_empty() || !entry.wait_queue.is_empty());
    }

    /// Release a specific lock for a transaction.
    pub fn release(
        &self,
        txn_id: u64,
        target: &LockTarget,
    ) {
        let mut locks = self.locks.lock().unwrap();
        if let Some(entry) = locks.get_mut(target) {
            entry.holders.retain(|&(t, _)| t != txn_id);
            Self::process_wait_queue(entry);
            if entry.holders.is_empty() && entry.wait_queue.is_empty() {
                locks.remove(target);
            }
        }
    }

    /// Build wait-for graph: returns adjacency list `waiter -> set of holders it waits on`.
    pub fn build_wait_for_graph(&self) -> HashMap<u64, HashSet<u64>> {
        let locks = self.locks.lock().unwrap();
        let mut graph: HashMap<u64, HashSet<u64>> = HashMap::new();

        for entry in locks.values() {
            for &(waiter_txn, waiter_mode) in &entry.wait_queue {
                for &(holder_txn, holder_mode) in &entry.holders {
                    if waiter_txn != holder_txn && waiter_mode.conflicts_with(&holder_mode) {
                        graph.entry(waiter_txn).or_default().insert(holder_txn);
                    }
                }
            }
        }

        graph
    }

    /// Detect deadlocks via DFS cycle detection on the wait-for graph.
    /// Returns a list of cycles found (each cycle is a vec of txn_ids).
    pub fn detect_deadlocks(&self) -> Vec<Vec<u64>> {
        let graph = self.build_wait_for_graph();
        let mut cycles = Vec::new();
        let mut visited: HashSet<u64> = HashSet::new();
        let mut on_stack: HashSet<u64> = HashSet::new();

        for &node in graph.keys() {
            if !visited.contains(&node) {
                let mut path = Vec::new();
                Self::dfs_find_cycles(
                    node, &graph, &mut visited, &mut on_stack, &mut path, &mut cycles,
                );
            }
        }

        cycles
    }

    fn dfs_find_cycles(
        node: u64,
        graph: &HashMap<u64, HashSet<u64>>,
        visited: &mut HashSet<u64>,
        on_stack: &mut HashSet<u64>,
        path: &mut Vec<u64>,
        cycles: &mut Vec<Vec<u64>>,
    ) {
        visited.insert(node);
        on_stack.insert(node);
        path.push(node);

        if let Some(neighbors) = graph.get(&node) {
            for &next in neighbors {
                if !visited.contains(&next) {
                    Self::dfs_find_cycles(next, graph, visited, on_stack, path, cycles);
                } else if on_stack.contains(&next) {
                    // Found a cycle — extract it.
                    let cycle_start = path.iter().position(|&n| n == next).unwrap();
                    let cycle: Vec<u64> = path[cycle_start..].to_vec();
                    cycles.push(cycle);
                }
            }
        }

        path.pop();
        on_stack.remove(&node);
    }

    /// Get all locks held by a transaction.
    pub fn locks_held_by(&self, txn_id: u64) -> Vec<(LockTarget, LockMode)> {
        let locks = self.locks.lock().unwrap();
        let mut result = Vec::new();
        for (target, entry) in locks.iter() {
            for &(t, mode) in &entry.holders {
                if t == txn_id {
                    result.push((target.clone(), mode));
                }
            }
        }
        result
    }

    /// Get all SIRead locks (used by SSI).
    pub fn get_siread_locks(&self) -> Vec<(LockTarget, u64)> {
        let locks = self.locks.lock().unwrap();
        let mut result = Vec::new();
        for (target, entry) in locks.iter() {
            for &(txn_id, mode) in &entry.holders {
                if mode == LockMode::SIRead {
                    result.push((target.clone(), txn_id));
                }
            }
        }
        result
    }

    fn process_wait_queue(entry: &mut LockEntry) {
        let mut granted = Vec::new();
        let mut i = 0;
        while i < entry.wait_queue.len() {
            let (txn_id, mode) = entry.wait_queue[i];
            if entry.can_grant(txn_id, mode) {
                entry.holders.push((txn_id, mode));
                granted.push(i);
            }
            i += 1;
        }
        // Remove granted entries from back to front to preserve indices.
        for idx in granted.into_iter().rev() {
            entry.wait_queue.remove(idx);
        }
    }

    /// Check if a transaction is waiting for any lock.
    pub fn is_waiting(&self, txn_id: u64) -> bool {
        let locks = self.locks.lock().unwrap();
        for entry in locks.values() {
            for &(t, _) in &entry.wait_queue {
                if t == txn_id {
                    return true;
                }
            }
        }
        false
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_locks_compatible() {
        let lm = LockManager::new();
        let target = LockTarget::Row { table_id: 1, tuple_id: 1 };
        assert!(lm.acquire(1, target.clone(), LockMode::Shared).unwrap());
        assert!(lm.acquire(2, target.clone(), LockMode::Shared).unwrap());
    }

    #[test]
    fn exclusive_blocks_shared() {
        let lm = LockManager::new();
        let target = LockTarget::Row { table_id: 1, tuple_id: 1 };
        assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
        // Txn 2 should be queued.
        assert!(!lm.acquire(2, target.clone(), LockMode::Shared).unwrap());
    }

    #[test]
    fn release_grants_waiters() {
        let lm = LockManager::new();
        let target = LockTarget::Row { table_id: 1, tuple_id: 1 };
        assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
        assert!(!lm.acquire(2, target.clone(), LockMode::Shared).unwrap());

        // Release txn 1 — txn 2 should be granted.
        lm.release_all(1);

        let locks = lm.locks_held_by(2);
        assert_eq!(locks.len(), 1);
    }

    #[test]
    fn siread_never_blocks() {
        let lm = LockManager::new();
        let target = LockTarget::Row { table_id: 1, tuple_id: 1 };
        assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
        // SIRead should succeed despite exclusive lock.
        assert!(lm.acquire(2, target.clone(), LockMode::SIRead).unwrap());
    }

    #[test]
    fn deadlock_detection_simple_cycle() {
        let lm = LockManager::new();
        let r1 = LockTarget::Row { table_id: 1, tuple_id: 1 };
        let r2 = LockTarget::Row { table_id: 1, tuple_id: 2 };

        // Txn 1 holds r1, Txn 2 holds r2.
        assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
        assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());

        // Txn 1 waits for r2, Txn 2 waits for r1 → cycle.
        assert!(!lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap());
        assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());

        let cycles = lm.detect_deadlocks();
        assert!(!cycles.is_empty());
    }

    #[test]
    fn lock_upgrade() {
        let lm = LockManager::new();
        let target = LockTarget::Row { table_id: 1, tuple_id: 1 };
        assert!(lm.acquire(1, target.clone(), LockMode::Shared).unwrap());
        // Upgrade to exclusive (no other holders).
        assert!(lm.acquire(1, target.clone(), LockMode::Exclusive).unwrap());
        let held = lm.locks_held_by(1);
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].1, LockMode::Exclusive);
    }

    #[test]
    fn idempotent_acquire() {
        let lm = LockManager::new();
        let target = LockTarget::Row { table_id: 1, tuple_id: 1 };
        assert!(lm.acquire(1, target.clone(), LockMode::Shared).unwrap());
        assert!(lm.acquire(1, target.clone(), LockMode::Shared).unwrap());
        let held = lm.locks_held_by(1);
        // Should still be just one lock.
        assert_eq!(held.len(), 1);
    }
}
