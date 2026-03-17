use crate::sql::ast::{Expr, JoinType};
use crate::types::Schema;

use super::logical_plan::SortExpr;

// ---------------------------------------------------------------------------
// Key range for index scans
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum Bound {
    Unbounded,
    Inclusive(Expr),
    Exclusive(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeyRange {
    pub low: Bound,
    pub high: Bound,
}

impl KeyRange {
    pub fn full() -> Self {
        KeyRange {
            low: Bound::Unbounded,
            high: Bound::Unbounded,
        }
    }

    pub fn eq(val: Expr) -> Self {
        KeyRange {
            low: Bound::Inclusive(val.clone()),
            high: Bound::Inclusive(val),
        }
    }
}

// ---------------------------------------------------------------------------
// Physical plan
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    SeqScan {
        table: String,
        alias: Option<String>,
        schema: Schema,
        predicate: Option<Expr>,
    },

    IndexScan {
        table: String,
        alias: Option<String>,
        index_name: String,
        schema: Schema,
        key_ranges: Vec<KeyRange>,
        predicate: Option<Expr>,
    },

    HashJoin {
        join_type: JoinType,
        left_keys: Vec<Expr>,
        right_keys: Vec<Expr>,
        condition: Option<Expr>,
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        schema: Schema,
    },

    SortMergeJoin {
        join_type: JoinType,
        left_keys: Vec<Expr>,
        right_keys: Vec<Expr>,
        condition: Option<Expr>,
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        schema: Schema,
    },

    NestedLoopJoin {
        join_type: JoinType,
        condition: Option<Expr>,
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        schema: Schema,
    },

    HashAggregate {
        group_by: Vec<Expr>,
        aggregates: Vec<super::logical_plan::AggregateExpr>,
        input: Box<PhysicalPlan>,
        schema: Schema,
    },

    SortAggregate {
        group_by: Vec<Expr>,
        aggregates: Vec<super::logical_plan::AggregateExpr>,
        input: Box<PhysicalPlan>,
        schema: Schema,
    },

    ExternalSort {
        order_by: Vec<SortExpr>,
        input: Box<PhysicalPlan>,
    },

    HashDistinct {
        input: Box<PhysicalPlan>,
    },

    SortDistinct {
        input: Box<PhysicalPlan>,
    },

    Project {
        expressions: Vec<super::logical_plan::ProjectionExpr>,
        input: Box<PhysicalPlan>,
    },

    Filter {
        predicate: Expr,
        input: Box<PhysicalPlan>,
    },

    Limit {
        count: Option<usize>,
        offset: usize,
        input: Box<PhysicalPlan>,
    },

    Union {
        all: bool,
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
    },

    Intersect {
        all: bool,
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
    },

    Except {
        all: bool,
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
    },

    Insert {
        table: String,
        columns: Vec<String>,
        input: Box<PhysicalPlan>,
    },

    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        input: Box<PhysicalPlan>,
    },

    Delete {
        table: String,
        input: Box<PhysicalPlan>,
    },

    Values {
        rows: Vec<Vec<Expr>>,
        schema: Schema,
    },

    Empty {
        schema: Schema,
    },
}

impl PhysicalPlan {
    pub fn schema(&self) -> Schema {
        match self {
            PhysicalPlan::SeqScan { schema, .. }
            | PhysicalPlan::IndexScan { schema, .. }
            | PhysicalPlan::HashJoin { schema, .. }
            | PhysicalPlan::SortMergeJoin { schema, .. }
            | PhysicalPlan::NestedLoopJoin { schema, .. }
            | PhysicalPlan::HashAggregate { schema, .. }
            | PhysicalPlan::SortAggregate { schema, .. }
            | PhysicalPlan::Values { schema, .. }
            | PhysicalPlan::Empty { schema, .. } => schema.clone(),

            PhysicalPlan::ExternalSort { input, .. }
            | PhysicalPlan::HashDistinct { input }
            | PhysicalPlan::SortDistinct { input }
            | PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Insert { input, .. }
            | PhysicalPlan::Update { input, .. }
            | PhysicalPlan::Delete { input, .. } => input.schema(),

            PhysicalPlan::Project { expressions, .. } => {
                use crate::types::Column;
                use super::logical_plan::LogicalPlan;
                let cols = expressions
                    .iter()
                    .map(|pe| {
                        let dt = LogicalPlan::infer_expr_type(&pe.expr);
                        Column::new(pe.alias.clone(), dt, true)
                    })
                    .collect();
                Schema::new(cols)
            }

            PhysicalPlan::Union { left, .. }
            | PhysicalPlan::Intersect { left, .. }
            | PhysicalPlan::Except { left, .. } => left.schema(),
        }
    }

    pub fn node_name(&self) -> &'static str {
        match self {
            PhysicalPlan::SeqScan { .. } => "SeqScan",
            PhysicalPlan::IndexScan { .. } => "IndexScan",
            PhysicalPlan::HashJoin { .. } => "HashJoin",
            PhysicalPlan::SortMergeJoin { .. } => "SortMergeJoin",
            PhysicalPlan::NestedLoopJoin { .. } => "NestedLoopJoin",
            PhysicalPlan::HashAggregate { .. } => "HashAggregate",
            PhysicalPlan::SortAggregate { .. } => "SortAggregate",
            PhysicalPlan::ExternalSort { .. } => "ExternalSort",
            PhysicalPlan::HashDistinct { .. } => "HashDistinct",
            PhysicalPlan::SortDistinct { .. } => "SortDistinct",
            PhysicalPlan::Project { .. } => "Project",
            PhysicalPlan::Filter { .. } => "Filter",
            PhysicalPlan::Limit { .. } => "Limit",
            PhysicalPlan::Union { .. } => "Union",
            PhysicalPlan::Intersect { .. } => "Intersect",
            PhysicalPlan::Except { .. } => "Except",
            PhysicalPlan::Insert { .. } => "Insert",
            PhysicalPlan::Update { .. } => "Update",
            PhysicalPlan::Delete { .. } => "Delete",
            PhysicalPlan::Values { .. } => "Values",
            PhysicalPlan::Empty { .. } => "Empty",
        }
    }

    pub fn children(&self) -> Vec<&PhysicalPlan> {
        match self {
            PhysicalPlan::SeqScan { .. }
            | PhysicalPlan::IndexScan { .. }
            | PhysicalPlan::Values { .. }
            | PhysicalPlan::Empty { .. } => vec![],

            PhysicalPlan::ExternalSort { input, .. }
            | PhysicalPlan::HashDistinct { input }
            | PhysicalPlan::SortDistinct { input }
            | PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::SortAggregate { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Insert { input, .. }
            | PhysicalPlan::Update { input, .. }
            | PhysicalPlan::Delete { input, .. } => vec![input],

            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::SortMergeJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::Union { left, right, .. }
            | PhysicalPlan::Intersect { left, right, .. }
            | PhysicalPlan::Except { left, right, .. } => vec![left, right],
        }
    }
}
