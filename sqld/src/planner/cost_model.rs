use super::physical_plan::PhysicalPlan;
use super::Catalog;

/// Configurable cost constants for the optimizer.
#[derive(Debug, Clone)]
pub struct CostConstants {
    /// Cost per sequential page read.
    pub seq_page_cost: f64,
    /// Cost per random page read (index scan).
    pub random_page_cost: f64,
    /// CPU cost per tuple processed.
    pub cpu_tuple_cost: f64,
    /// CPU cost per index tuple.
    pub cpu_index_tuple_cost: f64,
    /// CPU cost per operator evaluation.
    pub cpu_operator_cost: f64,
    /// Cost to build one entry in a hash table.
    pub hash_build_cost: f64,
    /// Sort cost multiplier (applied to n * log(n)).
    pub sort_cost_factor: f64,
}

impl Default for CostConstants {
    fn default() -> Self {
        Self {
            seq_page_cost: 1.0,
            random_page_cost: 4.0,
            cpu_tuple_cost: 0.01,
            cpu_index_tuple_cost: 0.005,
            cpu_operator_cost: 0.0025,
            hash_build_cost: 0.02,
            sort_cost_factor: 1.0,
        }
    }
}

/// Estimates the cost of executing a physical plan.
pub struct CostModel<'a> {
    constants: CostConstants,
    catalog: &'a Catalog,
}

impl<'a> CostModel<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self {
            constants: CostConstants::default(),
            catalog,
        }
    }

    pub fn with_constants(catalog: &'a Catalog, constants: CostConstants) -> Self {
        Self { constants, catalog }
    }

    /// Estimate the total cost of a physical plan.
    pub fn estimate_cost(&self, plan: &PhysicalPlan) -> f64 {
        match plan {
            PhysicalPlan::SeqScan { table, .. } => {
                let stats = self.catalog.get_stats(table);
                let page_cost = self.constants.seq_page_cost * stats.page_count;
                let tuple_cost = self.constants.cpu_tuple_cost * stats.row_count;
                page_cost + tuple_cost
            }

            PhysicalPlan::IndexScan {
                table,
                key_ranges,
                ..
            } => {
                let stats = self.catalog.get_stats(table);
                // Estimate selectivity from number of ranges
                let sel = if key_ranges.is_empty() {
                    1.0
                } else {
                    0.1 * key_ranges.len() as f64
                }
                .min(1.0);
                let rows = stats.row_count * sel;
                let pages = (stats.page_count * sel).max(1.0);
                let page_cost = self.constants.random_page_cost * pages;
                let tuple_cost = self.constants.cpu_index_tuple_cost * rows;
                page_cost + tuple_cost
            }

            PhysicalPlan::HashJoin { left, right, .. } => {
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                let left_rows = self.estimate_physical_rows(left);
                let right_rows = self.estimate_physical_rows(right);
                // Build hash on right (smaller), probe with left
                let build_cost = self.constants.hash_build_cost * right_rows;
                let probe_cost = self.constants.cpu_tuple_cost * left_rows;
                left_cost + right_cost + build_cost + probe_cost
            }

            PhysicalPlan::SortMergeJoin { left, right, .. } => {
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                let left_rows = self.estimate_physical_rows(left);
                let right_rows = self.estimate_physical_rows(right);
                // Sort both sides + merge
                let sort_left = self.sort_cost(left_rows);
                let sort_right = self.sort_cost(right_rows);
                let merge_cost = self.constants.cpu_tuple_cost * (left_rows + right_rows);
                left_cost + right_cost + sort_left + sort_right + merge_cost
            }

            PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                let left_rows = self.estimate_physical_rows(left);
                // For each left row, scan the entire right side
                left_cost + left_rows * right_cost
            }

            PhysicalPlan::HashAggregate { input, .. } => {
                let input_cost = self.estimate_cost(input);
                let rows = self.estimate_physical_rows(input);
                let hash_cost = self.constants.hash_build_cost * rows;
                input_cost + hash_cost
            }

            PhysicalPlan::SortAggregate { input, .. } => {
                let input_cost = self.estimate_cost(input);
                let rows = self.estimate_physical_rows(input);
                let sort_cost = self.sort_cost(rows);
                let agg_cost = self.constants.cpu_tuple_cost * rows;
                input_cost + sort_cost + agg_cost
            }

            PhysicalPlan::ExternalSort { input, .. } => {
                let input_cost = self.estimate_cost(input);
                let rows = self.estimate_physical_rows(input);
                input_cost + self.sort_cost(rows)
            }

            PhysicalPlan::HashDistinct { input } => {
                let input_cost = self.estimate_cost(input);
                let rows = self.estimate_physical_rows(input);
                input_cost + self.constants.hash_build_cost * rows
            }

            PhysicalPlan::SortDistinct { input } => {
                let input_cost = self.estimate_cost(input);
                let rows = self.estimate_physical_rows(input);
                input_cost + self.sort_cost(rows)
            }

            PhysicalPlan::Project { input, .. } => {
                let input_cost = self.estimate_cost(input);
                let rows = self.estimate_physical_rows(input);
                input_cost + self.constants.cpu_operator_cost * rows
            }

            PhysicalPlan::Filter { input, .. } => {
                let input_cost = self.estimate_cost(input);
                let rows = self.estimate_physical_rows(input);
                input_cost + self.constants.cpu_operator_cost * rows
            }

            PhysicalPlan::Limit { input, .. } => self.estimate_cost(input),

            PhysicalPlan::Union { left, right, .. } => {
                self.estimate_cost(left) + self.estimate_cost(right)
            }

            PhysicalPlan::Intersect { left, right, .. } => {
                self.estimate_cost(left) + self.estimate_cost(right)
            }

            PhysicalPlan::Except { left, right, .. } => {
                self.estimate_cost(left) + self.estimate_cost(right)
            }

            PhysicalPlan::Insert { input, .. }
            | PhysicalPlan::Update { input, .. }
            | PhysicalPlan::Delete { input, .. } => self.estimate_cost(input),

            PhysicalPlan::Values { rows, .. } => rows.len() as f64 * self.constants.cpu_tuple_cost,

            PhysicalPlan::Empty { .. } => 0.0,
        }
    }

    /// Estimate cost of sorting n rows.
    fn sort_cost(&self, n: f64) -> f64 {
        if n <= 1.0 {
            return 0.0;
        }
        self.constants.sort_cost_factor
            * self.constants.cpu_tuple_cost
            * n
            * n.log2()
    }

    /// Estimate the row count output of a physical plan.
    fn estimate_physical_rows(&self, plan: &PhysicalPlan) -> f64 {
        match plan {
            PhysicalPlan::SeqScan { table, .. } => {
                self.catalog.get_stats(table).row_count
            }
            PhysicalPlan::IndexScan {
                table, key_ranges, ..
            } => {
                let stats = self.catalog.get_stats(table);
                let sel = if key_ranges.is_empty() {
                    1.0
                } else {
                    0.1 * key_ranges.len() as f64
                }
                .min(1.0);
                stats.row_count * sel
            }
            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::SortMergeJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                let l = self.estimate_physical_rows(left);
                let r = self.estimate_physical_rows(right);
                // Assume equi-join selectivity
                (l * r * 0.1).max(1.0)
            }
            PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::SortAggregate { input, .. } => {
                // Estimate group count as sqrt of input
                self.estimate_physical_rows(input).sqrt().max(1.0)
            }
            PhysicalPlan::ExternalSort { input, .. } => self.estimate_physical_rows(input),
            PhysicalPlan::HashDistinct { input }
            | PhysicalPlan::SortDistinct { input } => {
                self.estimate_physical_rows(input) * 0.8
            }
            PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Filter { input, .. } => self.estimate_physical_rows(input),
            PhysicalPlan::Limit {
                count,
                offset,
                input,
            } => {
                let rows = self.estimate_physical_rows(input);
                let available = (rows - *offset as f64).max(0.0);
                match count {
                    Some(c) => available.min(*c as f64),
                    None => available,
                }
            }
            PhysicalPlan::Union { left, right, all } => {
                let l = self.estimate_physical_rows(left);
                let r = self.estimate_physical_rows(right);
                if *all { l + r } else { (l + r) * 0.8 }
            }
            PhysicalPlan::Intersect { left, right, .. } => {
                let l = self.estimate_physical_rows(left);
                let r = self.estimate_physical_rows(right);
                l.min(r) * 0.5
            }
            PhysicalPlan::Except { left, right, .. } => {
                let l = self.estimate_physical_rows(left);
                let r = self.estimate_physical_rows(right);
                (l - r * 0.5).max(1.0)
            }
            PhysicalPlan::Values { rows, .. } => rows.len() as f64,
            PhysicalPlan::Empty { .. } => 0.0,
            PhysicalPlan::Insert { input, .. }
            | PhysicalPlan::Update { input, .. }
            | PhysicalPlan::Delete { input, .. } => self.estimate_physical_rows(input),
        }
    }
}
