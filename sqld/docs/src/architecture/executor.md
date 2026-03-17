# Executor

The executor implements a Volcano-style pull-based execution model. A physical plan tree is converted into a tree of executor operators, and the root operator is pulled for tuples one at a time.

Source files:
- `src/executor/executor.rs` -- Executor trait, CatalogProvider trait, executor tree builder
- `src/executor/expr_eval.rs` -- Stack-based expression evaluator (compiler + VM)
- `src/executor/scalar_functions.rs` -- 39 scalar function implementations
- `src/executor/seq_scan.rs` -- Sequential scan
- `src/executor/index_scan.rs` -- Index scan
- `src/executor/filter.rs` -- Predicate filter
- `src/executor/project.rs` -- Projection
- `src/executor/hash_join.rs` -- Hash join
- `src/executor/sort_merge_join.rs` -- Sort-merge join
- `src/executor/nested_loop_join.rs` -- Nested loop join
- `src/executor/hash_aggregate.rs` -- Hash-based aggregation
- `src/executor/sort_aggregate.rs` -- Sort-based aggregation
- `src/executor/sort.rs` -- External sort
- `src/executor/limit.rs` -- Limit/offset
- `src/executor/distinct.rs` -- Hash and sort distinct
- `src/executor/set_ops.rs` -- Union, Intersect, Except
- `src/executor/values.rs` -- Literal row set
- `src/executor/modify.rs` -- INSERT, UPDATE, DELETE

## Volcano Pull-Based Model

Every physical operator implements the `Executor` trait:

```rust
pub trait Executor {
    fn init(&mut self) -> Result<()>;
    fn next(&mut self) -> Result<Option<Tuple>>;
    fn close(&mut self) -> Result<()>;
    fn schema(&self) -> &Schema;
}
```

The lifecycle is:

1. **`init()`** -- Prepare the operator for execution. For scans, this loads data from the catalog provider. For joins, this may build a hash table (hash join) or sort inputs (sort-merge join).
2. **`next()`** -- Return the next tuple, or `None` when exhausted. Each call pulls tuples from child operators as needed.
3. **`close()`** -- Release resources.

The `build_executor()` function recursively converts a `PhysicalPlan` tree into a tree of boxed `Executor` trait objects. An `ExecutorContext` provides shared state (catalog provider, work memory limit).

## CatalogProvider

The `CatalogProvider` trait abstracts data access:

```rust
pub trait CatalogProvider: Send + Sync {
    fn table_schema(&self, table: &str) -> Result<Schema>;
    fn scan_table(&self, table: &str) -> Result<Vec<Tuple>>;
    fn scan_index(&self, table: &str, index: &str, ranges: &[KeyRange]) -> Result<Vec<Tuple>>;
    fn insert_tuple(&self, table: &str, values: Vec<Datum>) -> Result<Tuple>;
    fn delete_tuple(&self, table: &str, tuple: &Tuple) -> Result<Tuple>;
    fn update_tuple(&self, table: &str, old_tuple: &Tuple, new_values: Vec<Datum>) -> Result<Tuple>;
}
```

Currently the server uses `MemoryCatalogProvider` (in-memory `HashMap` storage). The real storage engine would implement this trait to provide buffer-pool-backed page I/O.

## Operator Implementations

### Scan Operators

**SeqScan** -- Fetches all tuples from a table via `catalog.scan_table()` during `init()`, then iterates through them in `next()`. If a predicate is present (pushed down from the physical planner), tuples are filtered during iteration.

**IndexScan** -- Fetches tuples matching key ranges via `catalog.scan_index()` during `init()`. Applies a residual predicate filter if present.

### Filter

**Filter** -- Pulls tuples from its child operator and evaluates a predicate expression using the compiled expression evaluator. Only tuples where the predicate evaluates to `true` are passed through.

### Project

**Project** -- Evaluates a list of projection expressions against each input tuple, producing a new tuple with the projected columns. Supports arbitrary expressions (not just column references).

### Join Operators

**HashJoin** -- Builds a hash table from the right (build) side during `init()`, keyed on the equi-join keys. During `next()`, probes the hash table with each left (probe) tuple. Supports all join types: Inner, Left, Right, Full, Cross. For outer joins, tracks which build-side tuples were matched and emits unmatched tuples padded with NULLs.

**SortMergeJoin** -- Sorts both inputs on the join keys, then merges them in sorted order. Both sides are materialized during `init()`. Handles all join types including outer joins.

**NestedLoopJoin** -- The simplest join: for each tuple from the left side, scans the entire right side and evaluates the join condition. Used as a fallback when no equi-join keys are available (e.g., cross joins, theta joins).

### Aggregation Operators

**HashAggregate** -- Builds a hash table keyed on the GROUP BY expressions during `init()`. Each entry maintains accumulator state for all aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STRING_AGG`, `ARRAY_AGG`, `BOOL_AND`, `BOOL_OR`). Supports `DISTINCT` aggregates. After consuming all input, `next()` iterates over the hash table entries producing result tuples.

**SortAggregate** -- Assumes input is sorted on the GROUP BY columns. Reads tuples and accumulates aggregates until the group key changes, then emits the completed group.

### Sort

**Sort (ExternalSort)** -- Materializes all input tuples during `init()`, then sorts them using the specified sort expressions (column, ascending/descending, nulls first/last). The sort comparator handles multi-key ordering with NULL handling. Work memory limit is configurable (default 4 MB) via `ExecutorContext`.

### Limit

**Limit** -- Skips `offset` tuples from its child, then returns up to `count` tuples. Short-circuits by not pulling further tuples after the limit is reached.

### Distinct

**HashDistinct** -- Maintains a hash set of seen tuples. Only passes through tuples not previously seen.

**SortDistinct** -- Assumes input is sorted. Compares each tuple to the previous one and only emits when the value changes.

### Set Operations

**Union** -- Concatenates tuples from left and right children. If `ALL` is false, deduplicates using a hash set.

**Intersect** -- Collects all tuples from the right side into a hash set during `init()`. During `next()`, only emits left-side tuples that appear in the right set.

**Except** -- Collects all tuples from the right side into a hash set during `init()`. During `next()`, only emits left-side tuples that do NOT appear in the right set.

### Values

**Values** -- Evaluates literal row expressions from `INSERT ... VALUES (...)` or inline table expressions. Each row's expressions are evaluated once during `init()`.

### Modify (DML)

**Modify** -- A single operator that handles INSERT, UPDATE, and DELETE:

- **INSERT** -- Pulls tuples from its child (Values or sub-SELECT) and calls `catalog.insert_tuple()` for each one.
- **UPDATE** -- Pulls tuples from its child (scan + filter), evaluates assignment expressions to compute new values, and calls `catalog.update_tuple()`.
- **DELETE** -- Pulls tuples from its child (scan + filter) and calls `catalog.delete_tuple()`.

All three modes return the affected tuples (supporting the `RETURNING` clause).

## Expression Evaluator

The expression evaluator (`src/executor/expr_eval.rs`) uses a two-phase approach:

### Phase 1: Compilation

`compile_expr(expr, schema)` compiles an AST `Expr` into a flat list of `ExprOp` bytecode instructions. Column references are resolved to ordinal positions in the schema at compile time.

The `ExprOp` instruction set includes:

| Category | Instructions |
|----------|-------------|
| Push | `PushLiteral(Datum)`, `PushColumn(usize)` |
| Arithmetic | `Add`, `Sub`, `Mul`, `Div`, `Mod`, `Exp`, `Concat`, `Neg` |
| Comparison | `Eq`, `NotEq`, `Lt`, `Gt`, `LtEq`, `GtEq` |
| Logical | `And`, `Or`, `Not` |
| Null | `IsNull`, `IsNotNull` |
| Type | `Cast(DataType)` |
| Functions | `CallScalar { name, arity }` |
| Special | `Coalesce(n)`, `Nullif`, `Greatest(n)`, `Least(n)` |
| Control | `Case { when_count, has_else, has_operand }` |
| Patterns | `Between { negated }`, `InList { len, negated }`, `Like { negated, case_insensitive }` |

### Phase 2: Evaluation

`evaluate_expr(ops, tuple)` executes the bytecode against a tuple using a stack-based VM. Values are pushed and popped from a `Vec<Datum>` stack. The final stack value is the result.

SQL NULL semantics are fully implemented:
- Three-valued logic for AND/OR (e.g., `FALSE AND NULL` is `FALSE`, `TRUE AND NULL` is `NULL`)
- NULL propagation through arithmetic and comparisons
- IN with NULL elements returns NULL when no match is found but a NULL element exists

### LIKE Pattern Matching

The `like_match()` function implements SQL LIKE with `%` (any sequence) and `_` (any single character) using dynamic programming (O(n*m) time, O(m) space). ILIKE support is provided by lowercasing both the value and pattern before matching.

## Scalar Functions

The scalar function dispatcher (`src/executor/scalar_functions.rs`) supports 39 functions across 5 categories:

### String Functions (17)

`length`/`char_length`, `upper`, `lower`, `trim`/`btrim`, `ltrim`, `rtrim`, `substring`/`substr`, `position`/`strpos`, `replace`, `concat`, `left`, `right`, `reverse`, `lpad`, `rpad`, `repeat`, `split_part`

### Math Functions (13)

`abs`, `ceil`/`ceiling`, `floor`, `round`, `trunc`/`truncate`, `sqrt`, `power`/`pow`, `mod`, `ln`, `log`/`log10`, `exp`, `sign`, `random`

### Date/Time Functions (5)

`now`/`current_timestamp`, `extract`, `date_trunc`, `age`, `to_char`

### Type Functions (2)

`cast`, `typeof`/`pg_typeof`

### Null Functions (2)

`coalesce`, `nullif`

The `random()` function uses a simple xorshift64 PRNG (no external dependency), seeded from an atomic counter.
