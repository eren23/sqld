# Query Engine

The query engine transforms parsed SQL ASTs into optimized executable plans. It consists of three stages: plan building, optimization, and physical planning.

Source files:
- `src/planner/plan_builder.rs` -- AST to LogicalPlan
- `src/planner/logical_plan.rs` -- Logical plan node types
- `src/planner/optimizer.rs` -- Rule-based optimizer
- `src/planner/rules/` -- 9 optimization rules
- `src/planner/physical_planner.rs` -- LogicalPlan to PhysicalPlan
- `src/planner/physical_plan.rs` -- Physical plan node types
- `src/planner/cost_model.rs` -- Cost estimation
- `src/planner/cardinality.rs` -- Cardinality estimation

## Plan Builder

The `PlanBuilder` (`src/planner/plan_builder.rs`) converts AST statements into `LogicalPlan` trees. It takes a `Catalog` reference for schema lookup.

### SELECT Translation

A `SELECT` statement is built bottom-up in this order:

1. **FROM** -- Each table reference becomes a `Scan` node. Joins are nested left-to-right, with `USING` clauses converted to `ON` equality conditions.
2. **WHERE** -- A `Filter` node wraps the FROM plan.
3. **GROUP BY / Aggregates** -- An `Aggregate` node is created if the query has `GROUP BY` or aggregate functions in the SELECT list. Aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STRING_AGG`, `ARRAY_AGG`, `BOOL_AND`, `BOOL_OR`) are extracted from the SELECT expressions.
4. **HAVING** -- Another `Filter` node after aggregation.
5. **ORDER BY** -- A `Sort` node is placed *before* projection (a post-swarm bug fix -- originally projection came first, which lost columns needed for ordering).
6. **SELECT list** -- A `Project` node with the output expressions.
7. **DISTINCT** -- A `Distinct` node.
8. **LIMIT/OFFSET** -- A `Limit` node.
9. **Set operations** -- `Union`, `Intersect`, or `Except` node combining two plans.

### DML Translation

- **INSERT** -- `Values` or sub-SELECT plan feeds into an `Insert` node.
- **UPDATE** -- `Scan` (optionally with `Filter`) feeds into an `Update` node carrying assignment expressions.
- **DELETE** -- `Scan` (optionally with `Filter`) feeds into a `Delete` node.

## Logical Plan Nodes

The `LogicalPlan` enum (`src/planner/logical_plan.rs`) has 16 variants:

| Node | Description |
|------|-------------|
| `Scan` | Table scan with table name, alias, and schema |
| `Filter` | Predicate filter over a child plan |
| `Project` | Column projection / expression evaluation |
| `Join` | Join of two relations (all join types) |
| `Aggregate` | Grouping with aggregate functions |
| `Sort` | Order-preserving sort |
| `Limit` | Row limit with optional offset |
| `Distinct` | Duplicate elimination |
| `Union` | Set union (ALL or DISTINCT) |
| `Intersect` | Set intersection |
| `Except` | Set difference |
| `Insert` | INSERT target |
| `Update` | UPDATE with assignments |
| `Delete` | DELETE from table |
| `Values` | Literal row set |
| `Empty` | Zero-row relation (no FROM clause) |

Each node implements `schema()` to return its output schema and `children()` to return child nodes, enabling plan traversal.

### Expression Helpers

The logical plan module provides utility functions for expression analysis:

- `collect_columns(expr)` -- Extracts all column references from an expression
- `expr_references_only(expr, tables)` -- Checks if an expression only references columns from specific tables
- `split_conjunction(expr)` -- Splits an AND chain into individual predicates
- `combine_conjunction(preds)` -- Combines predicates with AND
- `referenced_tables(expr)` -- Gets the set of table names referenced in an expression

## Optimizer

The optimizer (`src/planner/optimizer.rs`) applies 9 rewrite rules in sequence, organized into 5 phases:

### Phase 1: Normalize and Simplify

1. **Constant Folding** (`rules/constant_folding.rs`) -- Evaluates constant expressions at planning time. For example, `1 + 2` becomes `3`, and `WHERE true AND x > 5` becomes `WHERE x > 5`.

2. **Simplification** (`rules/simplification.rs`) -- Applies algebraic simplifications such as removing double negation (`NOT NOT x` becomes `x`), simplifying comparisons with NULL, and removing identity operations.

### Phase 2: Decorrelate and Merge

3. **Subquery Decorrelation** (`rules/subquery_decorrelation.rs`) -- Transforms correlated subqueries into joins when possible, eliminating the need for nested-loop evaluation of subqueries.

4. **View Merging** (`rules/view_merging.rs`) -- Inlines view definitions, merging the view's query tree into the main query plan to enable further optimization across the view boundary.

### Phase 3: Push Operations Down

5. **Predicate Pushdown** (`rules/predicate_pushdown.rs`) -- Pushes filter predicates as close to the data source as possible. Predicates that reference only one side of a join are pushed below the join. This reduces the number of rows flowing through the plan.

6. **Projection Pushdown** (`rules/projection_pushdown.rs`) -- Eliminates unnecessary columns from intermediate plan nodes by pushing projections down. Note: a post-swarm bug fix ensures that scan schemas are not narrowed, because the executor returns full tuples from storage regardless of the plan's schema.

### Phase 4: Eliminate Dead Work

7. **Dead Column Elimination** (`rules/dead_column_elimination.rs`) -- Removes columns that are computed but never used by any downstream operator.

8. **Join Elimination** (`rules/join_elimination.rs`) -- Removes joins that provably produce the same result as one of their inputs (e.g., a join on a unique key that is not referenced in the output).

### Phase 5: Join Ordering

9. **Join Reorder** (`rules/join_reorder.rs`) -- Reorders multi-way joins to minimize estimated cost. Uses the catalog's cardinality statistics and the cost model to evaluate different join orderings.

### Rule Interface

All rules implement the `OptimizationRule` trait:

```rust
pub trait OptimizationRule {
    fn name(&self) -> &'static str;
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan;
}
```

The optimizer applies each rule in order, passing the output of one rule as input to the next. The `optimize_with_trace()` method also records which rules were applied, useful for `EXPLAIN` output.

## Cost Model

The cost model (`src/planner/cost_model.rs`) estimates the total cost of executing a physical plan using PostgreSQL-style cost constants:

| Constant | Default | Description |
|----------|---------|-------------|
| `seq_page_cost` | 1.0 | Cost per sequential page read |
| `random_page_cost` | 4.0 | Cost per random page read (index scan) |
| `cpu_tuple_cost` | 0.01 | CPU cost per tuple processed |
| `cpu_index_tuple_cost` | 0.005 | CPU cost per index tuple |
| `cpu_operator_cost` | 0.0025 | CPU cost per operator evaluation |
| `hash_build_cost` | 0.02 | Cost to build one hash table entry |
| `sort_cost_factor` | 1.0 | Multiplier for sort cost (n * log(n)) |

The cost model estimates costs for each physical operator:
- **SeqScan**: `seq_page_cost * pages + cpu_tuple_cost * rows`
- **IndexScan**: `random_page_cost * (pages * selectivity) + cpu_index_tuple_cost * (rows * selectivity)`
- **HashJoin**: `left_cost + right_cost + hash_build_cost * right_rows + cpu_tuple_cost * left_rows`
- **SortMergeJoin**: `left_cost + right_cost + sort(left) + sort(right) + merge_cost`
- **NestedLoopJoin**: `left_cost + left_rows * right_cost`

## Cardinality Estimation

The cardinality estimator (`src/planner/cardinality.rs`) estimates output row counts using selectivity formulas:

| Predicate | Selectivity |
|-----------|------------|
| `col = literal` | `1 / ndv` (number of distinct values) |
| `col != literal` | `1 - 1/ndv` |
| Range (`<`, `>`, `<=`, `>=`) | `(val - min) / (max - min)` if stats available, else 0.33 |
| `AND` | `sel(A) * sel(B)` |
| `OR` | `sel(A) + sel(B) - sel(A) * sel(B)` |
| `NOT` | `1 - sel` |
| `BETWEEN` | `(high - low) / (max - min)` if stats available, else 0.25 |
| `LIKE` | 0.1 (default) |
| `IS NULL` | `null_fraction` if stats available, else 0.02 |
| `IN (list)` | `min(list_len / ndv, 1)` |

Join cardinality for equi-joins uses: `|L| * |R| / max(ndv_L, ndv_R)`.

Group count for GROUP BY is estimated as the product of distinct value counts for each group-by column, capped at the input cardinality.

## Physical Planner

The physical planner (`src/planner/physical_planner.rs`) converts an optimized logical plan into a physical plan by choosing concrete algorithms.

### Scan Selection

For `Scan` nodes (optionally with a pushed-down `Filter`):

1. Check available indexes for the table
2. Try to extract key ranges from the predicate for each index
3. Estimate costs for both IndexScan and SeqScan
4. Choose the cheaper option

Key range extraction handles: `col = val`, `col > val`, `col >= val`, `col < val`, `col <= val`, `col BETWEEN low AND high`, and `col IN (v1, v2, ...)`.

### Join Algorithm Selection

For `Join` nodes, the planner extracts equi-join keys from the condition:

- **If equi-keys exist**: Compare `HashJoin` vs `SortMergeJoin` by cost, pick the cheaper one
- **If no equi-keys**: Use `NestedLoopJoin`

### Aggregate Algorithm Selection

- **Scalar aggregate** (no GROUP BY): Always uses `HashAggregate`
- **Grouped aggregate**: Compare `HashAggregate` vs `SortAggregate` (with preceding sort) by cost

### Distinct Algorithm Selection

Compare `HashDistinct` vs `SortDistinct` (with preceding sort) by cost.

### Physical Plan Nodes

The `PhysicalPlan` enum has 21 variants:

| Category | Nodes |
|----------|-------|
| Scans | `SeqScan`, `IndexScan` |
| Joins | `HashJoin`, `SortMergeJoin`, `NestedLoopJoin` |
| Aggregation | `HashAggregate`, `SortAggregate` |
| Sort | `ExternalSort` |
| Distinct | `HashDistinct`, `SortDistinct` |
| Relational | `Project`, `Filter`, `Limit` |
| Set operations | `Union`, `Intersect`, `Except` |
| DML | `Insert`, `Update`, `Delete` |
| Sources | `Values`, `Empty` |
