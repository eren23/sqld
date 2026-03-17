use std::sync::Arc;

use crate::planner::physical_plan::{KeyRange, PhysicalPlan};
use crate::types::{Datum, MvccHeader, Schema, Tuple};
use crate::utils::error::Result;

// ---------------------------------------------------------------------------
// Executor trait — pull-based Volcano model
// ---------------------------------------------------------------------------

/// Every physical operator implements this trait.
///
/// Life-cycle: `init()` → `next()` × N → `close()`.
///
/// `next()` returns `Ok(None)` when the operator is exhausted.
pub trait Executor {
    fn init(&mut self) -> Result<()>;
    fn next(&mut self) -> Result<Option<Tuple>>;
    fn close(&mut self) -> Result<()>;
    fn schema(&self) -> &Schema;
}

// ---------------------------------------------------------------------------
// Catalog provider — abstracts storage access
// ---------------------------------------------------------------------------

/// Trait for accessing table/index data.  Tests supply in-memory
/// implementations; production code wires in the real storage layer.
pub trait CatalogProvider: Send + Sync {
    /// Return the schema of the named table.
    fn table_schema(&self, table: &str) -> Result<Schema>;

    /// Full sequential scan — returns all visible tuples.
    fn scan_table(&self, table: &str) -> Result<Vec<Tuple>>;

    /// Index range scan — returns tuples matching the key ranges.
    fn scan_index(
        &self,
        table: &str,
        index: &str,
        ranges: &[KeyRange],
    ) -> Result<Vec<Tuple>>;

    /// Insert a single tuple, returning the inserted tuple (for RETURNING).
    fn insert_tuple(&self, table: &str, values: Vec<Datum>) -> Result<Tuple>;

    /// Delete a tuple identified by its data, returning the deleted tuple.
    fn delete_tuple(&self, table: &str, tuple: &Tuple) -> Result<Tuple>;

    /// Update a tuple, returning the new version.
    fn update_tuple(
        &self,
        table: &str,
        old_tuple: &Tuple,
        new_values: Vec<Datum>,
    ) -> Result<Tuple>;
}

// ---------------------------------------------------------------------------
// Executor context — shared state for all operators in a plan
// ---------------------------------------------------------------------------

pub const DEFAULT_WORK_MEM: usize = 4 * 1024 * 1024; // 4 MiB

pub struct ExecutorContext {
    pub catalog: Arc<dyn CatalogProvider>,
    pub work_mem: usize,
}

impl ExecutorContext {
    pub fn new(catalog: Arc<dyn CatalogProvider>) -> Self {
        Self {
            catalog,
            work_mem: DEFAULT_WORK_MEM,
        }
    }

    pub fn with_work_mem(mut self, bytes: usize) -> Self {
        self.work_mem = bytes;
        self
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a tuple with a dummy MVCC header (for intermediate results).
pub fn intermediate_tuple(data: Vec<Datum>) -> Tuple {
    Tuple::new(MvccHeader::new(0, 0, 0), data)
}

/// Null-pad or extend a datum vector to `len` columns (used by outer joins).
pub fn null_padded(values: &[Datum], len: usize) -> Vec<Datum> {
    let mut v = values.to_vec();
    v.resize(len, Datum::Null);
    v
}

// ---------------------------------------------------------------------------
// Build executor tree from physical plan
// ---------------------------------------------------------------------------

pub fn build_executor(
    plan: PhysicalPlan,
    ctx: Arc<ExecutorContext>,
) -> Box<dyn Executor> {
    match plan {
        PhysicalPlan::SeqScan {
            table,
            schema,
            predicate,
            ..
        } => Box::new(super::seq_scan::SeqScanExecutor::new(
            ctx, table, schema, predicate,
        )),

        PhysicalPlan::IndexScan {
            table,
            index_name,
            schema,
            key_ranges,
            predicate,
            ..
        } => Box::new(super::index_scan::IndexScanExecutor::new(
            ctx, table, index_name, schema, key_ranges, predicate,
        )),

        PhysicalPlan::Filter { predicate, input } => {
            let child = build_executor(*input, ctx);
            Box::new(super::filter::FilterExecutor::new(child, predicate))
        }

        PhysicalPlan::Project { expressions, input } => {
            let child = build_executor(*input, ctx);
            Box::new(super::project::ProjectExecutor::new(child, expressions))
        }

        PhysicalPlan::Values { rows, schema } => {
            Box::new(super::values::ValuesExecutor::new(rows, schema))
        }

        PhysicalPlan::Empty { schema } => {
            Box::new(super::values::ValuesExecutor::new(vec![], schema))
        }

        PhysicalPlan::Limit {
            count,
            offset,
            input,
        } => {
            let child = build_executor(*input, ctx);
            Box::new(super::limit::LimitExecutor::new(child, count, offset))
        }

        PhysicalPlan::ExternalSort { order_by, input } => {
            let work_mem = ctx.work_mem;
            let child = build_executor(*input, ctx);
            Box::new(super::sort::SortExecutor::new(child, order_by, work_mem))
        }

        PhysicalPlan::HashJoin {
            join_type,
            left_keys,
            right_keys,
            condition,
            left,
            right,
            schema,
        } => {
            let left_exec = build_executor(*left, ctx.clone());
            let right_exec = build_executor(*right, ctx.clone());
            let work_mem = ctx.work_mem;
            Box::new(super::hash_join::HashJoinExecutor::new(
                left_exec, right_exec, join_type, left_keys, right_keys,
                condition, schema, work_mem,
            ))
        }

        PhysicalPlan::SortMergeJoin {
            join_type,
            left_keys,
            right_keys,
            condition,
            left,
            right,
            schema,
        } => {
            let left_exec = build_executor(*left, ctx.clone());
            let right_exec = build_executor(*right, ctx);
            Box::new(super::sort_merge_join::SortMergeJoinExecutor::new(
                left_exec, right_exec, join_type, left_keys, right_keys,
                condition, schema,
            ))
        }

        PhysicalPlan::NestedLoopJoin {
            join_type,
            condition,
            left,
            right,
            schema,
        } => {
            let left_exec = build_executor(*left, ctx.clone());
            let right_exec = build_executor(*right, ctx);
            Box::new(super::nested_loop_join::NestedLoopJoinExecutor::new(
                left_exec, right_exec, join_type, condition, schema,
            ))
        }

        PhysicalPlan::HashAggregate {
            group_by,
            aggregates,
            input,
            schema,
        } => {
            let child = build_executor(*input, ctx);
            Box::new(super::hash_aggregate::HashAggregateExecutor::new(
                child, group_by, aggregates, schema,
            ))
        }

        PhysicalPlan::SortAggregate {
            group_by,
            aggregates,
            input,
            schema,
        } => {
            let child = build_executor(*input, ctx);
            Box::new(super::sort_aggregate::SortAggregateExecutor::new(
                child, group_by, aggregates, schema,
            ))
        }

        PhysicalPlan::HashDistinct { input } => {
            let child = build_executor(*input, ctx);
            Box::new(super::distinct::HashDistinctExecutor::new(child))
        }

        PhysicalPlan::SortDistinct { input } => {
            let child = build_executor(*input, ctx);
            Box::new(super::distinct::SortDistinctExecutor::new(child))
        }

        PhysicalPlan::Union { all, left, right } => {
            let left_exec = build_executor(*left, ctx.clone());
            let right_exec = build_executor(*right, ctx);
            Box::new(super::set_ops::UnionExecutor::new(left_exec, right_exec, all))
        }

        PhysicalPlan::Intersect { all, left, right } => {
            let left_exec = build_executor(*left, ctx.clone());
            let right_exec = build_executor(*right, ctx);
            Box::new(super::set_ops::IntersectExecutor::new(
                left_exec, right_exec, all,
            ))
        }

        PhysicalPlan::Except { all, left, right } => {
            let left_exec = build_executor(*left, ctx.clone());
            let right_exec = build_executor(*right, ctx);
            Box::new(super::set_ops::ExceptExecutor::new(
                left_exec, right_exec, all,
            ))
        }

        PhysicalPlan::Insert {
            table,
            columns,
            input,
        } => {
            let child = build_executor(*input, ctx.clone());
            Box::new(super::modify::ModifyExecutor::new_insert(
                ctx, child, table, columns,
            ))
        }

        PhysicalPlan::Update {
            table,
            assignments,
            input,
        } => {
            let child = build_executor(*input, ctx.clone());
            Box::new(super::modify::ModifyExecutor::new_update(
                ctx, child, table, assignments,
            ))
        }

        PhysicalPlan::Delete { table, input } => {
            let child = build_executor(*input, ctx.clone());
            Box::new(super::modify::ModifyExecutor::new_delete(
                ctx, child, table,
            ))
        }
    }
}
