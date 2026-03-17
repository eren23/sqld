use super::logical_plan::LogicalPlan;
use super::rules::constant_folding::ConstantFolding;
use super::rules::dead_column_elimination::DeadColumnElimination;
use super::rules::join_elimination::JoinElimination;
use super::rules::predicate_pushdown::PredicatePushdown;
use super::rules::projection_pushdown::ProjectionPushdown;
use super::rules::simplification::Simplification;
use super::rules::subquery_decorrelation::SubqueryDecorrelation;
use super::rules::join_reorder::JoinReorder;
use super::rules::view_merging::ViewMerging;
use super::rules::OptimizationRule;
use super::Catalog;

/// The logical optimizer applies a sequence of rewrite rules to transform
/// a logical plan into an equivalent but more efficient form.
pub struct Optimizer {
    rules: Vec<Box<dyn OptimizationRule>>,
}

impl Optimizer {
    /// Create an optimizer with the default rule set.
    pub fn new(catalog: &Catalog) -> Self {
        Self {
            rules: vec![
                // Phase 1: Normalize and simplify expressions
                Box::new(ConstantFolding),
                Box::new(Simplification),
                // Phase 2: Decorrelate subqueries and merge views
                Box::new(SubqueryDecorrelation),
                Box::new(ViewMerging),
                // Phase 3: Push operations down
                Box::new(PredicatePushdown),
                Box::new(ProjectionPushdown),
                // Phase 4: Eliminate dead work
                Box::new(DeadColumnElimination),
                Box::new(JoinElimination::new(catalog.clone())),
                // Phase 5: Join ordering
                Box::new(JoinReorder::new(catalog.clone())),
            ],
        }
    }

    /// Create an optimizer with a custom rule set.
    pub fn with_rules(rules: Vec<Box<dyn OptimizationRule>>) -> Self {
        Self { rules }
    }

    /// Apply all optimization rules to the plan.
    pub fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        let mut current = plan;
        for rule in &self.rules {
            current = rule.apply(current);
        }
        current
    }

    /// Apply all optimization rules, returning both the plan and a trace of
    /// which rules were applied.
    pub fn optimize_with_trace(&self, plan: LogicalPlan) -> (LogicalPlan, Vec<&'static str>) {
        let mut current = plan;
        let mut trace = Vec::new();
        for rule in &self.rules {
            trace.push(rule.name());
            current = rule.apply(current);
        }
        (current, trace)
    }
}
