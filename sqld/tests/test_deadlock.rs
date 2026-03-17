use std::collections::HashSet;

use sqld::transaction::lock_manager::{LockManager, LockMode, LockTarget};

// ===========================================================================
// Helper functions
// ===========================================================================

fn row(table_id: u64, tuple_id: u64) -> LockTarget {
    LockTarget::Row { table_id, tuple_id }
}

fn table(table_id: u64) -> LockTarget {
    LockTarget::Table(table_id)
}

/// Checks whether `cycles` contains a cycle whose txn-id set matches `expected`.
fn contains_cycle_with(cycles: &[Vec<u64>], expected: &[u64]) -> bool {
    let expected_set: HashSet<u64> = expected.iter().copied().collect();
    cycles.iter().any(|cycle| {
        let cycle_set: HashSet<u64> = cycle.iter().copied().collect();
        cycle_set == expected_set
    })
}

// ===========================================================================
// 1. No deadlock with no waits
// ===========================================================================

#[test]
fn no_deadlock_empty_lock_manager() {
    let lm = LockManager::new();
    let cycles = lm.detect_deadlocks();
    assert!(cycles.is_empty(), "no deadlock expected on empty lock manager");
}

#[test]
fn no_deadlock_single_lock() {
    let lm = LockManager::new();
    assert!(lm.acquire(1, row(1, 1), LockMode::Exclusive).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(cycles.is_empty(), "single held lock should not produce a deadlock");
}

#[test]
fn no_deadlock_multiple_non_conflicting_locks() {
    let lm = LockManager::new();
    // Multiple transactions each holding exclusive locks on different rows.
    assert!(lm.acquire(1, row(1, 1), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, row(1, 2), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(3, row(1, 3), LockMode::Exclusive).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(cycles.is_empty());
}

#[test]
fn no_deadlock_shared_locks_on_same_row() {
    let lm = LockManager::new();
    // Shared locks are compatible -- no waits at all.
    assert!(lm.acquire(1, row(1, 1), LockMode::Shared).unwrap());
    assert!(lm.acquire(2, row(1, 1), LockMode::Shared).unwrap());
    assert!(lm.acquire(3, row(1, 1), LockMode::Shared).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(cycles.is_empty());
}

// ===========================================================================
// 2. No deadlock with non-circular waits (chain A -> B -> C)
// ===========================================================================

#[test]
fn no_deadlock_linear_wait_chain() {
    let lm = LockManager::new();

    // T1 holds r1, T2 holds r2, T3 holds r3.
    assert!(lm.acquire(1, row(1, 1), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, row(1, 2), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(3, row(1, 3), LockMode::Exclusive).unwrap());

    // T3 waits for r2 (held by T2) -- edge 3 -> 2
    assert!(!lm.acquire(3, row(1, 2), LockMode::Exclusive).unwrap());
    // T2 waits for r1 (held by T1) -- edge 2 -> 1
    assert!(!lm.acquire(2, row(1, 1), LockMode::Exclusive).unwrap());

    // Chain: 3 -> 2 -> 1, no cycle.
    let cycles = lm.detect_deadlocks();
    assert!(
        cycles.is_empty(),
        "linear wait chain should not be a deadlock, but found cycles: {:?}",
        cycles
    );
}

#[test]
fn no_deadlock_divergent_waits() {
    let lm = LockManager::new();

    // T1 holds r1.
    assert!(lm.acquire(1, row(1, 1), LockMode::Exclusive).unwrap());

    // T2 and T3 both wait for r1 -- fan-in waits, no cycle.
    assert!(!lm.acquire(2, row(1, 1), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(3, row(1, 1), LockMode::Exclusive).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(cycles.is_empty());
}

#[test]
fn no_false_positive_linear_wait() {
    let lm = LockManager::new();

    // T1 holds exclusive lock on row A.
    assert!(lm.acquire(1, row(1, 1), LockMode::Exclusive).unwrap());
    // T2 waits for row A (held by T1) -- linear wait, not a cycle.
    assert!(!lm.acquire(2, row(1, 1), LockMode::Exclusive).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(
        cycles.is_empty(),
        "expected no deadlock (linear wait only), but found: {:?}",
        cycles,
    );
}

// ===========================================================================
// 3. Simple 2-txn deadlock (A holds r1, B holds r2, A waits r2, B waits r1)
// ===========================================================================

#[test]
fn deadlock_two_transactions() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);

    // T1 holds r1, T2 holds r2.
    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());

    // T1 waits for r2 (held by T2).
    assert!(!lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap());
    // T2 waits for r1 (held by T1).
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(!cycles.is_empty(), "expected a 2-txn deadlock cycle");
    assert!(
        contains_cycle_with(&cycles, &[1, 2]),
        "cycle should contain txns 1 and 2, got: {:?}",
        cycles
    );
}

#[test]
fn deadlock_two_transactions_on_table_locks() {
    let lm = LockManager::new();
    let t1 = table(1);
    let t2 = table(2);

    assert!(lm.acquire(10, t1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(20, t2.clone(), LockMode::Exclusive).unwrap());

    assert!(!lm.acquire(10, t2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(20, t1.clone(), LockMode::Exclusive).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(!cycles.is_empty(), "table-level deadlock should be detected");
    assert!(contains_cycle_with(&cycles, &[10, 20]));
}

// ===========================================================================
// 4. 3-txn deadlock cycle (A -> B -> C -> A)
// ===========================================================================

#[test]
fn deadlock_three_transactions_cycle() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);
    let r3 = row(1, 3);

    // Each txn holds one row.
    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(3, r3.clone(), LockMode::Exclusive).unwrap());

    // Create circular wait: 1 -> 2 -> 3 -> 1
    assert!(!lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r3.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(3, r1.clone(), LockMode::Exclusive).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(!cycles.is_empty(), "expected a 3-txn deadlock cycle");
    assert!(
        contains_cycle_with(&cycles, &[1, 2, 3]),
        "cycle should contain txns 1, 2, 3, got: {:?}",
        cycles
    );
}

#[test]
fn deadlock_four_transactions_cycle() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);
    let r3 = row(1, 3);
    let r4 = row(1, 4);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(3, r3.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(4, r4.clone(), LockMode::Exclusive).unwrap());

    // 1 -> 2 -> 3 -> 4 -> 1
    assert!(!lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r3.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(3, r4.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(4, r1.clone(), LockMode::Exclusive).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(!cycles.is_empty(), "expected a 4-txn deadlock cycle");
    assert!(contains_cycle_with(&cycles, &[1, 2, 3, 4]));
}

// ===========================================================================
// 5. Deadlock resolved by aborting victim (release_all breaks cycle)
// ===========================================================================

#[test]
fn deadlock_resolved_by_release_all_two_txn() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());

    // Confirm deadlock exists.
    assert!(!lm.detect_deadlocks().is_empty());

    // Abort T2 (the victim).
    lm.release_all(2);

    // Cycle should be broken.
    let cycles = lm.detect_deadlocks();
    assert!(
        cycles.is_empty(),
        "deadlock should be resolved after release_all, but found: {:?}",
        cycles
    );

    // T1 should now hold at least its original lock (r2 may also be granted).
    let held = lm.locks_held_by(1);
    assert!(
        held.len() >= 1,
        "txn 1 should hold at least its original lock after victim is released"
    );
}

#[test]
fn deadlock_resolved_by_releasing_first_txn() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());

    assert!(!lm.detect_deadlocks().is_empty());

    // Break deadlock by releasing T1 instead.
    lm.release_all(1);

    let cycles = lm.detect_deadlocks();
    assert!(
        cycles.is_empty(),
        "deadlock should be resolved after releasing T1's locks, but found: {:?}",
        cycles
    );
}

#[test]
fn deadlock_resolved_three_txn_by_releasing_one() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);
    let r3 = row(1, 3);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(3, r3.clone(), LockMode::Exclusive).unwrap());

    // 1 -> 2 -> 3 -> 1
    assert!(!lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r3.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(3, r1.clone(), LockMode::Exclusive).unwrap());

    assert!(!lm.detect_deadlocks().is_empty());

    // Abort T2 to break the cycle.
    lm.release_all(2);

    let cycles = lm.detect_deadlocks();
    assert!(
        cycles.is_empty(),
        "3-txn deadlock should be resolved after aborting T2, found: {:?}",
        cycles
    );
}

#[test]
fn release_all_clears_all_locks_for_aborted_txn() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());

    lm.release_all(2);

    // T2 should hold no locks and not be waiting.
    let held = lm.locks_held_by(2);
    assert!(held.is_empty(), "aborted txn should hold no locks");
    assert!(!lm.is_waiting(2), "aborted txn should not be waiting");
}

// ===========================================================================
// 6. Wait-for graph correctness (verify edges)
// ===========================================================================

#[test]
fn wait_for_graph_empty_when_no_waits() {
    let lm = LockManager::new();
    assert!(lm.acquire(1, row(1, 1), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, row(1, 2), LockMode::Exclusive).unwrap());

    let graph = lm.build_wait_for_graph();
    assert!(
        graph.is_empty(),
        "no waits means empty wait-for graph, got: {:?}",
        graph
    );
}

#[test]
fn wait_for_graph_single_edge() {
    let lm = LockManager::new();
    let r1 = row(1, 1);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());

    let graph = lm.build_wait_for_graph();
    // T2 waits for T1.
    assert!(graph.contains_key(&2), "T2 should appear as waiter");
    assert!(
        graph[&2].contains(&1),
        "T2 should be waiting on T1"
    );
    assert!(
        !graph.contains_key(&1),
        "T1 should not be waiting on anyone"
    );
}

#[test]
fn wait_for_graph_two_waiters_for_same_holder() {
    let lm = LockManager::new();
    let r1 = row(1, 1);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(3, r1.clone(), LockMode::Exclusive).unwrap());

    let graph = lm.build_wait_for_graph();
    assert!(graph[&2].contains(&1));
    assert!(graph[&3].contains(&1));
    assert_eq!(graph.len(), 2, "only T2 and T3 should be waiting");
}

#[test]
fn wait_for_graph_mutual_edges_for_deadlock() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());

    let graph = lm.build_wait_for_graph();
    // 1 waits for 2, 2 waits for 1.
    assert!(graph[&1].contains(&2), "1 -> 2 edge expected");
    assert!(graph[&2].contains(&1), "2 -> 1 edge expected");
}

#[test]
fn wait_for_graph_three_txn_cycle_edges() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);
    let r3 = row(1, 3);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(3, r3.clone(), LockMode::Exclusive).unwrap());

    assert!(!lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r3.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(3, r1.clone(), LockMode::Exclusive).unwrap());

    let graph = lm.build_wait_for_graph();
    assert!(graph[&1].contains(&2), "1 -> 2 edge");
    assert!(graph[&2].contains(&3), "2 -> 3 edge");
    assert!(graph[&3].contains(&1), "3 -> 1 edge");
    assert_eq!(graph.len(), 3);
}

#[test]
fn wait_for_graph_edges_removed_after_release() {
    let lm = LockManager::new();
    let r1 = row(1, 1);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());

    // Before release: edge 2 -> 1 exists.
    let graph = lm.build_wait_for_graph();
    assert!(graph.contains_key(&2));

    // Release T1 -- T2 should be promoted to holder.
    lm.release_all(1);

    // After release: no waiters, graph should be empty.
    let graph = lm.build_wait_for_graph();
    assert!(
        graph.is_empty(),
        "graph should be empty after releasing holder, got: {:?}",
        graph
    );
}

#[test]
fn wait_for_graph_no_self_edges() {
    let lm = LockManager::new();
    let r1 = row(1, 1);

    // A transaction should never have a wait-for edge to itself.
    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    // Re-acquiring the same lock in the same mode is idempotent.
    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());

    let graph = lm.build_wait_for_graph();
    for (waiter, holders) in &graph {
        assert!(
            !holders.contains(waiter),
            "self-edge found for txn {}",
            waiter
        );
    }
}

// ===========================================================================
// 7. Multiple independent deadlocks
// ===========================================================================

#[test]
fn multiple_independent_deadlocks() {
    let lm = LockManager::new();

    // Deadlock 1: T1 and T2 on rows (1,1) and (1,2).
    let r1 = row(1, 1);
    let r2 = row(1, 2);
    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());

    // Deadlock 2: T3 and T4 on rows (2,1) and (2,2).
    let r3 = row(2, 1);
    let r4 = row(2, 2);
    assert!(lm.acquire(3, r3.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(4, r4.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(3, r4.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(4, r3.clone(), LockMode::Exclusive).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(
        cycles.len() >= 2,
        "expected at least 2 independent deadlock cycles, found: {:?}",
        cycles
    );
    assert!(contains_cycle_with(&cycles, &[1, 2]));
    assert!(contains_cycle_with(&cycles, &[3, 4]));
}

#[test]
fn resolve_one_deadlock_leaves_other_intact() {
    let lm = LockManager::new();

    // Deadlock 1: T1 <-> T2 on r1, r2.
    let r1 = row(1, 1);
    let r2 = row(1, 2);
    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());

    // Deadlock 2: T3 <-> T4 on r3, r4.
    let r3 = row(2, 1);
    let r4 = row(2, 2);
    assert!(lm.acquire(3, r3.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(4, r4.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(3, r4.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(4, r3.clone(), LockMode::Exclusive).unwrap());

    // Resolve deadlock 1 by aborting T2.
    lm.release_all(2);

    let cycles = lm.detect_deadlocks();
    // Deadlock 2 should still be present.
    assert!(!cycles.is_empty(), "deadlock 2 should still exist");
    assert!(contains_cycle_with(&cycles, &[3, 4]));
    // Deadlock 1 should be gone.
    assert!(
        !contains_cycle_with(&cycles, &[1, 2]),
        "deadlock 1 should be resolved"
    );
}

#[test]
fn three_independent_deadlocks() {
    let lm = LockManager::new();

    // Deadlock 1: T1 <-> T2
    assert!(lm.acquire(1, row(1, 1), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, row(1, 2), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(1, row(1, 2), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, row(1, 1), LockMode::Exclusive).unwrap());

    // Deadlock 2: T3 <-> T4
    assert!(lm.acquire(3, row(2, 1), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(4, row(2, 2), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(3, row(2, 2), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(4, row(2, 1), LockMode::Exclusive).unwrap());

    // Deadlock 3: T5 <-> T6
    assert!(lm.acquire(5, row(3, 1), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(6, row(3, 2), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(5, row(3, 2), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(6, row(3, 1), LockMode::Exclusive).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(
        cycles.len() >= 3,
        "expected at least 3 deadlock cycles, found {}: {:?}",
        cycles.len(),
        cycles
    );
    assert!(contains_cycle_with(&cycles, &[1, 2]));
    assert!(contains_cycle_with(&cycles, &[3, 4]));
    assert!(contains_cycle_with(&cycles, &[5, 6]));
}

// ===========================================================================
// 8. Deadlock with shared/exclusive mixed locks
// ===========================================================================

#[test]
fn deadlock_shared_holder_vs_exclusive_waiter() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);

    // T1 holds Shared on r1, T2 holds Exclusive on r2.
    assert!(lm.acquire(1, r1.clone(), LockMode::Shared).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());

    // T1 waits for Exclusive on r2 (conflicts with T2's Exclusive).
    assert!(!lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap());
    // T2 waits for Exclusive on r1 (conflicts with T1's Shared).
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(
        !cycles.is_empty(),
        "shared/exclusive deadlock expected"
    );
    assert!(contains_cycle_with(&cycles, &[1, 2]));
}

#[test]
fn deadlock_both_shared_then_upgrade_cross() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);

    // Both hold Shared on different rows.
    assert!(lm.acquire(1, r1.clone(), LockMode::Shared).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Shared).unwrap());

    // T1 wants Exclusive on r2 (conflicts with T2's Shared).
    assert!(!lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap());
    // T2 wants Exclusive on r1 (conflicts with T1's Shared).
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(
        !cycles.is_empty(),
        "expected deadlock from exclusive waiters vs shared holders"
    );
    assert!(contains_cycle_with(&cycles, &[1, 2]));
}

#[test]
fn no_deadlock_shared_vs_shared_no_conflict() {
    let lm = LockManager::new();
    let r1 = row(1, 1);

    // Both hold Shared on same row -- no conflict.
    assert!(lm.acquire(1, r1.clone(), LockMode::Shared).unwrap());
    assert!(lm.acquire(2, r1.clone(), LockMode::Shared).unwrap());

    // T3 wants Exclusive -- waits, but no cycle.
    assert!(!lm.acquire(3, r1.clone(), LockMode::Exclusive).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(
        cycles.is_empty(),
        "shared holders with one exclusive waiter is not a deadlock"
    );
}

#[test]
fn deadlock_intention_exclusive_vs_exclusive() {
    let lm = LockManager::new();
    let t1 = table(1);
    let t2 = table(2);

    // T1 holds Exclusive on table 1, T2 holds Exclusive on table 2.
    assert!(lm.acquire(1, t1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, t2.clone(), LockMode::Exclusive).unwrap());

    // Cross-wait with IntentionExclusive (conflicts with Exclusive).
    assert!(!lm.acquire(1, t2.clone(), LockMode::IntentionExclusive).unwrap());
    assert!(!lm.acquire(2, t1.clone(), LockMode::IntentionExclusive).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(
        !cycles.is_empty(),
        "IntentionExclusive vs Exclusive deadlock expected"
    );
    assert!(contains_cycle_with(&cycles, &[1, 2]));
}

#[test]
fn deadlock_intention_shared_vs_exclusive() {
    let lm = LockManager::new();
    let t1 = table(1);
    let t2 = table(2);

    // T1 holds Exclusive on table 1, T2 holds Exclusive on table 2.
    assert!(lm.acquire(1, t1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, t2.clone(), LockMode::Exclusive).unwrap());

    // IntentionShared conflicts with Exclusive.
    assert!(!lm.acquire(1, t2.clone(), LockMode::IntentionShared).unwrap());
    assert!(!lm.acquire(2, t1.clone(), LockMode::IntentionShared).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(
        !cycles.is_empty(),
        "IntentionShared vs Exclusive deadlock expected"
    );
    assert!(contains_cycle_with(&cycles, &[1, 2]));
}

// ===========================================================================
// 9. SIRead locks don't cause deadlocks
// ===========================================================================

#[test]
fn siread_does_not_block_exclusive() {
    let lm = LockManager::new();
    let r1 = row(1, 1);

    // SIRead is advisory -- even with an Exclusive held, SIRead succeeds.
    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(
        lm.acquire(2, r1.clone(), LockMode::SIRead).unwrap(),
        "SIRead should always be granted immediately"
    );

    let cycles = lm.detect_deadlocks();
    assert!(cycles.is_empty());
}

#[test]
fn siread_does_not_create_wait_for_edges() {
    let lm = LockManager::new();
    let r1 = row(1, 1);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, r1.clone(), LockMode::SIRead).unwrap());

    let graph = lm.build_wait_for_graph();
    assert!(
        graph.is_empty(),
        "SIRead should not create any wait-for edges, got: {:?}",
        graph
    );
}

#[test]
fn siread_cross_locks_no_deadlock() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);

    // T1 holds Exclusive on r1, T2 holds Exclusive on r2.
    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());

    // SIRead across -- both succeed immediately, no waiting.
    assert!(lm.acquire(1, r2.clone(), LockMode::SIRead).unwrap());
    assert!(lm.acquire(2, r1.clone(), LockMode::SIRead).unwrap());

    let cycles = lm.detect_deadlocks();
    assert!(
        cycles.is_empty(),
        "SIRead cross-locks should never cause deadlocks"
    );
}

#[test]
fn siread_multiple_transactions_no_deadlock() {
    let lm = LockManager::new();
    let r1 = row(1, 1);

    // Many transactions all grab SIRead on the same row.
    for txn_id in 1..=10 {
        assert!(lm.acquire(txn_id, r1.clone(), LockMode::SIRead).unwrap());
    }

    let cycles = lm.detect_deadlocks();
    assert!(cycles.is_empty());
    let graph = lm.build_wait_for_graph();
    assert!(graph.is_empty());
}

#[test]
fn siread_does_not_block_and_not_blocked_by_exclusive() {
    let lm = LockManager::new();
    let r1 = row(1, 1);

    // SIRead first, then Exclusive by another txn -- SIRead never conflicts.
    assert!(lm.acquire(1, r1.clone(), LockMode::SIRead).unwrap());
    assert!(
        lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap(),
        "Exclusive should be granted even with SIRead holder"
    );

    let cycles = lm.detect_deadlocks();
    assert!(cycles.is_empty());
}

// ===========================================================================
// 10. is_waiting correctly reports waiting state
// ===========================================================================

#[test]
fn is_waiting_false_when_not_waiting() {
    let lm = LockManager::new();
    assert!(
        !lm.is_waiting(1),
        "txn 1 should not be waiting (never acquired anything)"
    );
}

#[test]
fn is_waiting_false_when_lock_granted() {
    let lm = LockManager::new();
    assert!(lm.acquire(1, row(1, 1), LockMode::Exclusive).unwrap());
    assert!(
        !lm.is_waiting(1),
        "txn 1 holds the lock, should not be waiting"
    );
}

#[test]
fn is_waiting_true_when_blocked() {
    let lm = LockManager::new();
    let r1 = row(1, 1);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());

    assert!(
        !lm.is_waiting(1),
        "txn 1 holds the lock, not waiting"
    );
    assert!(
        lm.is_waiting(2),
        "txn 2 is blocked, should be waiting"
    );
}

#[test]
fn is_waiting_becomes_false_after_holder_releases() {
    let lm = LockManager::new();
    let r1 = row(1, 1);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.is_waiting(2));

    // Release T1 -- T2 gets promoted from wait queue to holder.
    lm.release_all(1);

    assert!(
        !lm.is_waiting(2),
        "after release_all(1), txn 2 should no longer be waiting"
    );
}

#[test]
fn is_waiting_false_after_own_release_all() {
    let lm = LockManager::new();
    let r1 = row(1, 1);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.is_waiting(2));

    // Abort the waiter itself.
    lm.release_all(2);

    assert!(
        !lm.is_waiting(2),
        "after release_all(2), txn 2 should be removed from wait queues"
    );
}

#[test]
fn is_waiting_multiple_waiters() {
    let lm = LockManager::new();
    let r1 = row(1, 1);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(3, r1.clone(), LockMode::Exclusive).unwrap());

    assert!(!lm.is_waiting(1));
    assert!(lm.is_waiting(2));
    assert!(lm.is_waiting(3));
}

#[test]
fn is_waiting_on_multiple_resources() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());

    // T3 waits on two different resources.
    assert!(!lm.acquire(3, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(3, r2.clone(), LockMode::Exclusive).unwrap());

    assert!(lm.is_waiting(3), "T3 should be waiting on at least one resource");

    // Release r1's holder -- T3 may still be waiting on r2.
    lm.release_all(1);

    // T3 may or may not still be waiting depending on whether it got r1.
    // But after releasing all holders, T3 should eventually not be waiting.
    lm.release_all(2);
    assert!(
        !lm.is_waiting(3),
        "T3 should not be waiting after all holders released"
    );
}

// ===========================================================================
// Additional edge cases
// ===========================================================================

#[test]
fn locks_held_by_empty_for_unknown_txn() {
    let lm = LockManager::new();
    let held = lm.locks_held_by(999);
    assert!(held.is_empty());
}

#[test]
fn locks_held_by_reflects_granted_locks() {
    let lm = LockManager::new();
    assert!(lm.acquire(1, row(1, 1), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(1, row(1, 2), LockMode::Shared).unwrap());

    let held = lm.locks_held_by(1);
    assert_eq!(held.len(), 2);

    let modes: Vec<LockMode> = held.iter().map(|(_, m)| *m).collect();
    assert!(modes.contains(&LockMode::Exclusive));
    assert!(modes.contains(&LockMode::Shared));
}

#[test]
fn detect_deadlocks_returns_empty_after_all_released() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());

    assert!(!lm.detect_deadlocks().is_empty());

    // Release both.
    lm.release_all(1);
    lm.release_all(2);

    assert!(lm.detect_deadlocks().is_empty());
    assert!(lm.build_wait_for_graph().is_empty());
}

#[test]
fn detect_deadlocks_is_idempotent() {
    let lm = LockManager::new();
    let r1 = row(1, 1);
    let r2 = row(1, 2);

    assert!(lm.acquire(1, r1.clone(), LockMode::Exclusive).unwrap());
    assert!(lm.acquire(2, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(1, r2.clone(), LockMode::Exclusive).unwrap());
    assert!(!lm.acquire(2, r1.clone(), LockMode::Exclusive).unwrap());

    // Calling detect_deadlocks multiple times should give consistent results
    // and not mutate any internal state.
    let cycles1 = lm.detect_deadlocks();
    let cycles2 = lm.detect_deadlocks();
    let cycles3 = lm.detect_deadlocks();
    assert_eq!(cycles1.len(), cycles2.len());
    assert_eq!(cycles2.len(), cycles3.len());
    assert!(!cycles1.is_empty());
}

#[test]
fn diamond_pattern_no_deadlock() {
    let lm = LockManager::new();

    // T1 and T2 both hold shared locks on row A.
    assert!(lm.acquire(1, row(1, 1), LockMode::Shared).unwrap());
    assert!(lm.acquire(2, row(1, 1), LockMode::Shared).unwrap());

    // T3 wants exclusive lock on row A -- queued because shared holders exist.
    assert!(!lm.acquire(3, row(1, 1), LockMode::Exclusive).unwrap());

    // Not a deadlock: T3 simply waits for T1 and T2.
    let cycles = lm.detect_deadlocks();
    assert!(
        cycles.is_empty(),
        "diamond pattern should not be a deadlock, found: {:?}",
        cycles,
    );
}
