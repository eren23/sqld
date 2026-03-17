pub mod constant_folding;
pub mod dead_column_elimination;
pub mod join_elimination;
pub mod join_reorder;
pub mod predicate_pushdown;
pub mod projection_pushdown;
pub mod simplification;
pub mod subquery_decorrelation;
pub mod view_merging;

use super::logical_plan::LogicalPlan;

/// Trait for a logical optimization rule.
pub trait OptimizationRule {
    fn name(&self) -> &'static str;
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan;
}
