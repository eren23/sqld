# Swarm Goal

SQL Database Engine from Scratch — Rust

Build a fully functional SQL database engine implementing a significant
subset of SQL, with a cost-based query optimizer, MVCC transaction
isolation, B+ tree storage, write-ahead logging with crash recovery,
and a wire-compatible PostgreSQL protocol server. Implemented entirely
in Rust with zero external database dependencies. A comprehensive test
suite validates every layer from individual token parsing up through
multi-client transactional correctness under concurrent load.

---

## 0) Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│                     Client Layer                         │
│  PostgreSQL Wire Protocol (v3) — TCP listener            │
│  Connection pool — Session state — Auth (trust/password)  │
├──────────────────────────────────────────────────────────┤
│                     SQL Frontend                         │
│  Lexer → Parser → AST → Semantic Analyzer (Binder)       │
├──────────────────────────────────────────────────────────┤
│                     Query Engine                         │
│  Logical Planner → Cost-Based Optimizer → Physical Plan   │
├──────────────────────────────────────────────────────────┤
│                     Executor                             │
│  Volcano-model pull-based iterators                      │
│  SeqScan | IndexScan | Filter | Project | HashJoin |     │
│  SortMergeJoin | NestedLoopJoin | Sort | Aggregate |     │
│  Limit | HashAggregate | Union | Subquery                │
├──────────────────────────────────────────────────────────┤
│                     Transaction Manager                  │
│  MVCC — Snapshot Isolation + Serializable (SSI)          │
│  Lock Manager (row-level) — Deadlock Detection           │
├──────────────────────────────────────────────────────────┤
│                     Storage Engine                       │
│  Buffer Pool (LRU-K) — Page Manager — B+ Tree            │
│  Heap Files — Tuple Layout — TOAST (large values)        │
├──────────────────────────────────────────────────────────┤
│                     Recovery                             │
│  Write-Ahead Log (WAL) — ARIES-style recovery            │
│  Checkpointing — Log truncation                          │
├──────────────────────────────────────────────────────────┤
│                     Catalog                              │
│  System tables — Schema management — Statistics          │
└──────────────────────────────────────────────────────────┘
```

Each layer depends only on the layer(s) directly below it. No circular
dependencies. Every layer exposes a clean Rust trait interface that can
be tested with mock implementations of its dependencies.

---

## 1) SQL Language Support

### Data Types
```
INTEGER     — 32-bit signed integer
BIGINT      — 64-bit signed integer
FLOAT       — 64-bit IEEE 754 double
BOOLEAN     — true / false / NULL
VARCHAR(n)  — variable-length string, max n bytes (default 255)
TEXT        — unbounded string (TOAST for > 2KB)
TIMESTAMP   — microsecond precision, UTC
DATE        — calendar date
DECIMAL(p,s)— exact decimal (up to 38 digits precision)
BLOB        — binary large object (TOAST)
NULL        — the null type, coercible to any type
```

### Type Coercion Rules
Implicit widening follows a lattice:
`INTEGER → BIGINT → DECIMAL → FLOAT`
`VARCHAR(n) → TEXT`
`DATE → TIMESTAMP`
No implicit narrowing. Explicit `CAST(expr AS type)` required.
Boolean participates in no implicit coercions.

### DDL Statements
```sql
CREATE TABLE name (
    col1 type [NOT NULL] [DEFAULT expr] [PRIMARY KEY],
    col2 type [UNIQUE] [CHECK (expr)] [REFERENCES other_table(col)],
    ...
    [PRIMARY KEY (col1, col2, ...)],
    [UNIQUE (col1, col2, ...)],
    [CHECK (expr)],
    [FOREIGN KEY (col) REFERENCES other_table(col)
        [ON DELETE CASCADE|SET NULL|RESTRICT]
        [ON UPDATE CASCADE|SET NULL|RESTRICT]]
);

DROP TABLE [IF EXISTS] name [CASCADE];

ALTER TABLE name
    ADD COLUMN col type [constraints]
  | DROP COLUMN col
  | RENAME COLUMN old TO new
  | ADD CONSTRAINT name constraint_def
  | DROP CONSTRAINT name;

CREATE INDEX [UNIQUE] name ON table (col1 [ASC|DESC], col2 [ASC|DESC], ...);
CREATE INDEX name ON table USING HASH (col);    -- hash index
DROP INDEX [IF EXISTS] name;

CREATE VIEW name AS select_statement;
DROP VIEW [IF EXISTS] name;
```

### DML Statements
```sql
-- INSERT
INSERT INTO table [(col1, col2, ...)]
    VALUES (expr, expr, ...) [, (expr, expr, ...), ...];
INSERT INTO table [(col1, col2, ...)]
    SELECT ...;

-- UPDATE
UPDATE table SET col1 = expr [, col2 = expr, ...]
    [WHERE condition]
    [RETURNING col1, col2, ...];

-- DELETE
DELETE FROM table
    [WHERE condition]
    [RETURNING col1, col2, ...];

-- SELECT (full support)
SELECT [DISTINCT] select_list
    FROM table_references
    [WHERE condition]
    [GROUP BY expr [, expr, ...]]
    [HAVING condition]
    [ORDER BY expr [ASC|DESC] [NULLS FIRST|LAST] [, ...]]
    [LIMIT count]
    [OFFSET count];

-- Table references
table_name [[AS] alias]
subquery [AS] alias
join_type JOIN table_ref ON condition
join_type JOIN table_ref USING (col1, col2, ...)
CROSS JOIN table_ref
table_ref NATURAL JOIN table_ref

-- Join types: INNER | LEFT [OUTER] | RIGHT [OUTER] | FULL [OUTER]

-- Set operations
select UNION [ALL] select
select INTERSECT [ALL] select
select EXCEPT [ALL] select

-- Subqueries
WHERE col IN (SELECT ...)
WHERE col = (SELECT ...)           -- scalar subquery
WHERE EXISTS (SELECT ...)
WHERE col > ALL (SELECT ...)
WHERE col > ANY (SELECT ...)
SELECT *, (SELECT ...) AS computed -- correlated scalar subquery in select list
FROM (SELECT ...) AS derived       -- derived tables
```

### Expressions
```sql
-- Arithmetic: +, -, *, /, % (integer modulo), ^ (power)
-- Comparison: =, <>, <, >, <=, >=
-- Logical: AND, OR, NOT
-- IS NULL, IS NOT NULL
-- BETWEEN x AND y
-- LIKE pattern (%, _ wildcards)
-- CASE WHEN cond THEN expr [WHEN ...] [ELSE expr] END
-- COALESCE(expr, expr, ...)
-- NULLIF(expr, expr)
-- CAST(expr AS type)
-- IN (value_list)
-- IN (subquery)
-- EXISTS (subquery)
-- scalar subquery as expression
```

### Aggregate Functions
```sql
COUNT(*), COUNT(expr), COUNT(DISTINCT expr)
SUM(expr), SUM(DISTINCT expr)
AVG(expr), AVG(DISTINCT expr)
MIN(expr), MAX(expr)
STRING_AGG(expr, delimiter [ORDER BY ...])
ARRAY_AGG(expr [ORDER BY ...])
BOOL_AND(expr), BOOL_OR(expr)
```

### Scalar Functions
```sql
-- String: LENGTH, UPPER, LOWER, TRIM, LTRIM, RTRIM, SUBSTRING,
--         POSITION, REPLACE, CONCAT, LEFT, RIGHT, REVERSE,
--         LPAD, RPAD, REPEAT, SPLIT_PART
-- Math: ABS, CEIL, FLOOR, ROUND, TRUNC, SQRT, POWER, MOD,
--       LN, LOG, EXP, SIGN, RANDOM()
-- Date/Time: NOW(), CURRENT_DATE, CURRENT_TIMESTAMP,
--            EXTRACT(field FROM timestamp),
--            DATE_TRUNC(field, timestamp),
--            AGE(timestamp, timestamp),
--            timestamp + INTERVAL 'n units',
--            TO_CHAR(timestamp, format)
-- Type: CAST, TYPEOF
-- Null: COALESCE, NULLIF
```

### Transaction Control
```sql
BEGIN [ISOLATION LEVEL level];
COMMIT;
ROLLBACK;
SAVEPOINT name;
ROLLBACK TO SAVEPOINT name;
RELEASE SAVEPOINT name;

-- Isolation levels:
-- READ COMMITTED (default)
-- REPEATABLE READ (snapshot isolation)
-- SERIALIZABLE (SSI — serializable snapshot isolation)
```

### Utility
```sql
EXPLAIN [ANALYZE] select_statement;  -- show query plan [with execution stats]
SHOW TABLES;
SHOW COLUMNS FROM table;
ANALYZE table;                       -- update table statistics for optimizer
VACUUM table;                        -- reclaim dead tuple space
COPY table FROM 'path' [DELIMITER 'c'] [HEADER]; -- bulk CSV import
COPY table TO 'path' [DELIMITER 'c'] [HEADER];   -- bulk CSV export
```

---

## 2) Lexer

Converts SQL source text into a token stream.

### Token Types
```rust
enum TokenKind {
    // Literals
    IntegerLiteral(i64),
    FloatLiteral(f64),
    StringLiteral(String),       // 'single-quoted'
    BooleanLiteral(bool),
    NullLiteral,

    // Identifiers
    Identifier(String),          // unquoted
    QuotedIdentifier(String),    // "double-quoted" (case-sensitive)

    // Keywords (reserved)
    // SELECT, FROM, WHERE, JOIN, ON, INSERT, UPDATE, DELETE, CREATE,
    // DROP, ALTER, TABLE, INDEX, VIEW, INTO, VALUES, SET, AS, AND, OR,
    // NOT, IN, EXISTS, BETWEEN, LIKE, IS, NULL, TRUE, FALSE, CASE,
    // WHEN, THEN, ELSE, END, BEGIN, COMMIT, ROLLBACK, SAVEPOINT,
    // PRIMARY, KEY, FOREIGN, REFERENCES, UNIQUE, CHECK, DEFAULT,
    // CASCADE, RESTRICT, DISTINCT, ALL, UNION, INTERSECT, EXCEPT,
    // ORDER, BY, ASC, DESC, LIMIT, OFFSET, GROUP, HAVING, INNER,
    // LEFT, RIGHT, FULL, OUTER, CROSS, NATURAL, USING, RETURNING,
    // EXPLAIN, ANALYZE, SHOW, VACUUM, COPY, INTERVAL, NULLS, FIRST,
    // LAST, IF, ADD, COLUMN, RENAME, TO, CONSTRAINT, ON_DELETE,
    // ON_UPDATE, SET_NULL, WITH, RECURSIVE, OVER, PARTITION,
    // ROWS, RANGE, UNBOUNDED, PRECEDING, FOLLOWING, CURRENT, ROW,
    // ... (full list in lexer module)

    // Operators
    Plus, Minus, Star, Slash, Percent, Caret,
    Eq, NotEq, Lt, Gt, LtEq, GtEq,
    Concat,          // ||

    // Punctuation
    LeftParen, RightParen,
    Comma, Semicolon, Dot,
    ColonColon,      // :: (type cast shorthand)

    // Special
    Placeholder(u32), // $1, $2 (prepared statement params)
    Eof,
}

struct Token {
    kind: TokenKind,
    span: Span,       // byte offset range in source
    line: u32,
    col: u32,
}
```

### Lexer Rules
- Case-insensitive keywords: `SELECT` = `select` = `Select`.
- Identifiers: `[a-zA-Z_][a-zA-Z0-9_]*`. Unquoted identifiers are
  lowercased. Quoted identifiers (`"MyTable"`) preserve case.
- String literals: single-quoted, with `''` as escape for embedded quote.
  Support `E'...'` for C-style escapes (`\n`, `\t`, `\\`, `\'`).
- Numeric literals: `123`, `123.456`, `1.23e10`, `0x1A` (hex integer).
- Single-line comments: `-- ...` to end of line.
- Block comments: `/* ... */`, nestable.
- Whitespace: spaces, tabs, newlines — skipped, not tokenized.
- Operators: longest-match (`<>` before `<`, `::` before `:`, `||` before `|`).
- Error tokens: unrecognized characters produce a diagnostic token with
  source location.

### Error Reporting
Every lexer error includes: source text, line number, column number,
byte offset, and a human-readable message. Multiple errors collected
(not fail-fast) up to a configurable limit (default 50).

---

## 3) Parser

Pratt parser (top-down operator precedence) for expressions. Recursive
descent for statements. Produces a typed AST.

### Precedence Table (low → high)
```
1.  OR
2.  AND
3.  NOT (prefix)
4.  IS [NOT] NULL, IS [NOT] TRUE, IS [NOT] FALSE
5.  comparison: =, <>, <, >, <=, >=
6.  BETWEEN, IN, LIKE, EXISTS
7.  addition: +, -
8.  multiplication: *, /, %
9.  exponentiation: ^
10. concatenation: ||
11. unary: +, - (prefix)
12. type cast: ::
13. field access: .
14. function call, subscript
```

### AST Node Types (core selection)
```rust
enum Statement {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    AlterTable(AlterTableStmt),
    CreateIndex(CreateIndexStmt),
    DropIndex(DropIndexStmt),
    CreateView(CreateViewStmt),
    Begin(BeginStmt),
    Commit,
    Rollback(Option<String>),   // optional savepoint name
    Savepoint(String),
    Explain(ExplainStmt),
    Analyze(String),            // table name
    Vacuum(String),
    Copy(CopyStmt),
    ShowTables,
    ShowColumns(String),
}

struct SelectStmt {
    distinct: bool,
    columns: Vec<SelectItem>,         // expr [AS alias] | *
    from: Vec<TableRef>,
    joins: Vec<JoinClause>,
    where_clause: Option<Expr>,
    group_by: Vec<Expr>,
    having: Option<Expr>,
    order_by: Vec<OrderByItem>,
    limit: Option<Expr>,
    offset: Option<Expr>,
    set_op: Option<Box<SetOperation>>,
}

enum Expr {
    Literal(Literal),
    ColumnRef { table: Option<String>, column: String },
    BinaryOp { left: Box<Expr>, op: BinaryOp, right: Box<Expr> },
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
    FunctionCall { name: String, args: Vec<Expr>, distinct: bool },
    AggregateCall { func: AggFunc, arg: Box<Expr>, distinct: bool },
    Cast { expr: Box<Expr>, target_type: DataType },
    Case { operand: Option<Box<Expr>>, whens: Vec<WhenClause>, else_expr: Option<Box<Expr>> },
    InList { expr: Box<Expr>, list: Vec<Expr>, negated: bool },
    InSubquery { expr: Box<Expr>, subquery: Box<SelectStmt>, negated: bool },
    Exists { subquery: Box<SelectStmt>, negated: bool },
    Subquery(Box<SelectStmt>),       // scalar subquery
    Between { expr: Box<Expr>, low: Box<Expr>, high: Box<Expr>, negated: bool },
    Like { expr: Box<Expr>, pattern: Box<Expr>, negated: bool },
    IsNull { expr: Box<Expr>, negated: bool },
    Placeholder(u32),
    TypeCast { expr: Box<Expr>, target: DataType },
    Nested(Box<Expr>),                // parenthesized
}
```

### Error Recovery
- On parse error: skip tokens until next statement boundary (`;` or
  keyword that starts a new statement).
- Collect all errors, report with source location + expected tokens.
- Partial AST: successfully parsed statements returned alongside errors.
- Common mistake detection: missing comma after column, `WHERE` before
  `FROM`, `=` instead of `==` (suggest `=` is correct in SQL).

---

## 4) Semantic Analyzer (Binder)

Transforms raw AST into a bound/resolved AST by resolving all names
and checking types.

### Binding Phases
1. **Name resolution**: resolve table names → catalog entries, column
   names → (table_id, column_ordinal). Disambiguate `col` when multiple
   tables have it (error if ambiguous, unless qualified).
2. **Type checking**: infer expression types bottom-up. Apply coercion
   rules. Validate operator type compatibility. Check aggregate vs
   non-aggregate context in SELECT/HAVING.
3. **Scope analysis**: subquery scoping — inner queries can reference
   outer columns (correlated subqueries). Detect correlation depth.
4. **Constraint checking**: NOT NULL columns must have values in INSERT.
   CHECK constraints validated at compile time where possible (constant
   expressions). Foreign key targets must exist.
5. **View expansion**: replace view references with their stored
   SELECT AST, then re-bind.
6. **Wildcard expansion**: `SELECT *` → explicit column list.
   `SELECT t.*` → columns from table `t` only.

### Bound AST
```rust
struct BoundSelect {
    columns: Vec<BoundExpr>,        // with resolved types
    from: Vec<BoundTableRef>,       // with table_id, schema info
    joins: Vec<BoundJoin>,
    where_clause: Option<BoundExpr>,
    group_by: Vec<BoundExpr>,
    having: Option<BoundExpr>,
    order_by: Vec<BoundOrderBy>,
    limit: Option<u64>,
    offset: Option<u64>,
    output_schema: Schema,          // column names + types of result set
}

struct BoundExpr {
    expr: BoundExprKind,
    data_type: DataType,
    nullable: bool,
}
```

### Errors
- Unknown table / column / function.
- Ambiguous column reference.
- Type mismatch (e.g., `'abc' + 5`).
- Aggregate in WHERE clause (must be in HAVING).
- Non-aggregated column in SELECT with GROUP BY.
- Subquery returns more than one column for scalar subquery context.
- Correlated subquery references unknown outer column.
- Circular view definition.

---

## 5) Query Planner

Converts bound AST into a logical plan tree, then optimizes.

### Logical Plan Nodes
```rust
enum LogicalPlan {
    Scan { table_id: TableId, alias: String },
    Filter { predicate: BoundExpr, child: Box<LogicalPlan> },
    Project { exprs: Vec<BoundExpr>, child: Box<LogicalPlan> },
    Join {
        join_type: JoinType,
        condition: BoundExpr,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },
    Aggregate {
        group_by: Vec<BoundExpr>,
        aggregates: Vec<AggregateExpr>,
        child: Box<LogicalPlan>,
    },
    Sort { order_by: Vec<BoundOrderBy>, child: Box<LogicalPlan> },
    Limit { count: u64, offset: u64, child: Box<LogicalPlan> },
    Distinct { child: Box<LogicalPlan> },
    Union { all: bool, left: Box<LogicalPlan>, right: Box<LogicalPlan> },
    Intersect { all: bool, left: Box<LogicalPlan>, right: Box<LogicalPlan> },
    Except { all: bool, left: Box<LogicalPlan>, right: Box<LogicalPlan> },
    Insert { table_id: TableId, columns: Vec<ColumnId>, source: Box<LogicalPlan> },
    Update { table_id: TableId, assignments: Vec<(ColumnId, BoundExpr)>, filter: Option<BoundExpr> },
    Delete { table_id: TableId, filter: Option<BoundExpr> },
    Values { rows: Vec<Vec<BoundExpr>>, schema: Schema },
    Empty,
}
```

### Plan Construction Rules
- `FROM` clause → `Scan` nodes, joined by `CrossJoin` then filtered.
- `JOIN ... ON` → `Join` with condition.
- `WHERE` → `Filter` above joins.
- `GROUP BY` + aggregates → `Aggregate`.
- `HAVING` → `Filter` above `Aggregate`.
- `SELECT` expressions → `Project`.
- `DISTINCT` → `Distinct`.
- `ORDER BY` → `Sort`.
- `LIMIT/OFFSET` → `Limit`.
- Subqueries: correlated → `DependentJoin` (decorrelated during optimization),
  uncorrelated → executed once and materialized.

---

## 6) Cost-Based Optimizer

Transforms logical plans into cheaper-equivalent logical plans, then
selects physical implementations.

### Table Statistics (maintained by ANALYZE)
```rust
struct TableStats {
    row_count: u64,
    total_pages: u64,
    per_column: HashMap<ColumnId, ColumnStats>,
}

struct ColumnStats {
    null_count: u64,
    distinct_count: u64,           // number of distinct non-null values
    min_value: Option<Datum>,
    max_value: Option<Datum>,
    avg_width_bytes: u32,
    histogram: Histogram,           // equi-depth, 100 buckets
    most_common_values: Vec<(Datum, f64)>,  // top 10 values + frequencies
}
```

### Cardinality Estimation
- Base table: `row_count` from stats.
- Filter selectivity:
  - `col = literal`: `1 / distinct_count` (or MCV frequency if in top-10).
  - `col < literal`: histogram interpolation.
  - `col BETWEEN a AND b`: `sel(col < b) - sel(col < a)`.
  - `col LIKE 'prefix%'`: range estimate on string ordering.
  - `col IS NULL`: `null_count / row_count`.
  - `AND`: `sel(A) × sel(B)` (independence assumption).
  - `OR`: `sel(A) + sel(B) - sel(A) × sel(B)`.
  - `NOT`: `1 - sel(expr)`.
  - `IN (list)`: `min(list.len × sel(=), 1.0)`.
  - `IN (subquery)`: `min(subquery_rows / distinct_count, 1.0)`.
- Join: `|R| × |S| / max(distinct(R.col), distinct(S.col))`.
- Aggregate: `min(group_by_distinct_product, child_rows)`.
- Unknown predicate: default selectivity 0.1.

### Optimization Rules (rule-based + cost-based)

**Logical rewrites (always applied):**
1. **Predicate pushdown**: push filters below joins, projections.
2. **Projection pushdown**: push column pruning to scans.
3. **Constant folding**: `1 + 2` → `3`, `true AND x` → `x`.
4. **Dead column elimination**: remove unused columns early.
5. **Subquery decorrelation**: convert correlated subquery to join.
6. **Simplification**: `x = x` → `true` (when NOT NULL),
   `x AND true` → `x`, `x OR false` → `x`.
7. **IN-list to join**: `WHERE x IN (SELECT ...)` → semi-join.
8. **EXISTS to semi-join**: `WHERE EXISTS (SELECT ...)` → semi-join.
9. **View merging**: inline simple views into the query.
10. **Join elimination**: remove inner join to table whose columns are
    unused and join is on a unique key.

**Physical plan selection (cost-based):**
For each logical node, choose among physical implementations:

| Logical | Physical Options |
|---|---|
| Scan | SeqScan, IndexScan (if applicable index exists) |
| Filter | applied within scan (index condition) or above scan |
| Join | HashJoin, SortMergeJoin, NestedLoopJoin, IndexNestedLoopJoin |
| Aggregate | HashAggregate, SortAggregate |
| Sort | ExternalSort (in-memory if fits, else disk-spill) |
| Distinct | HashDistinct, SortDistinct |

### Cost Model
```
cost(SeqScan)        = pages × seq_page_cost + rows × cpu_tuple_cost
cost(IndexScan)      = index_pages × random_page_cost + rows × (cpu_index_cost + cpu_tuple_cost)
cost(HashJoin)        = cost(build_child) + cost(probe_child) + build_rows × cpu_hash_cost + probe_rows × cpu_hash_check_cost
cost(SortMergeJoin)  = cost(sort_left) + cost(sort_right) + (left_rows + right_rows) × cpu_merge_cost
cost(NestedLoopJoin) = cost(outer) + outer_rows × cost(inner)
cost(HashAggregate)  = child_cost + rows × cpu_hash_cost
cost(Sort)           = child_cost + rows × log2(rows) × cpu_comparison_cost [+ disk_sort_cost if spills]
```

Cost constants (configurable):
```toml
[optimizer]
seq_page_cost = 1.0
random_page_cost = 4.0
cpu_tuple_cost = 0.01
cpu_index_cost = 0.005
cpu_hash_cost = 0.02
cpu_hash_check_cost = 0.01
cpu_comparison_cost = 0.005
cpu_merge_cost = 0.003
effective_cache_size_pages = 16384
work_mem_bytes = 4194304     # 4MB sort/hash memory before disk spill
```

### Join Ordering
- For ≤ 6 tables: exhaustive dynamic programming (DPccp — connected
  subgraph complement pairs).
- For 7–12 tables: greedy heuristic (always join the cheapest available pair).
- For > 12 tables: left-deep greedy.
- Cross joins pushed to last.

### Index Selection
For each filter predicate, check if a matching index exists:
- Equality on indexed column(s): prefer hash index (O(1)) or B+ tree.
- Range predicate: B+ tree only.
- Composite index: usable if predicate covers a prefix of indexed columns.
- Index-only scan: if all needed columns are in the index, skip heap fetch.
- Covering index detection.

### EXPLAIN Output
```
HashJoin (cost=1245.00..3892.50 rows=5000)
  Join Cond: orders.customer_id = customers.id
  -> SeqScan on orders (cost=0.00..845.00 rows=50000)
  -> Hash
     -> IndexScan on customers using customers_pkey (cost=0.00..400.50 rows=10000)
        Filter: customers.active = true
```

With `ANALYZE`: adds actual rows, actual time, loops, buffer hits/reads.

---

## 7) Executor

Pull-based Volcano model. Each physical operator implements:
```rust
trait Executor {
    fn init(&mut self, ctx: &mut ExecContext) -> Result<()>;
    fn next(&mut self, ctx: &mut ExecContext) -> Result<Option<Tuple>>;
    fn close(&mut self) -> Result<()>;
    fn schema(&self) -> &Schema;
}
```

### Physical Operators

**SeqScan**: iterate all tuples in heap file, applying visibility check
(MVCC). Optional pushed-down filter predicate.

**IndexScan**: walk B+ tree for qualifying keys, fetch heap tuples by TID.
Supports: exact match, range scan, prefix scan (composite index).
Optional index-only scan (returns values from index leaf directly).

**Filter**: calls `child.next()`, evaluates predicate, skips non-matching.

**Project**: calls `child.next()`, evaluates projection expressions,
returns new tuple with projected schema.

**HashJoin**: Phase 1 (build): consume inner child into hash table on
join key. Phase 2 (probe): for each outer tuple, probe hash table.
Supports INNER, LEFT, RIGHT, FULL OUTER, SEMI, ANTI.
Spills to disk if hash table exceeds `work_mem`.

**SortMergeJoin**: sorts both inputs (or exploits existing sort order),
then merges. Efficient for pre-sorted inputs and range joins.

**NestedLoopJoin**: for each outer tuple, scan entire inner child.
Used when inner side is very small or for cross joins. Supports
parameterized inner (index nested loop join).

**Sort**: in-memory quicksort if data fits in `work_mem`, else external
merge sort with disk-spill runs. Supports multi-key sort with
ASC/DESC/NULLS FIRST/LAST per key.

**HashAggregate**: build hash table keyed by GROUP BY values. For each
input tuple, update aggregation accumulators. On completion, emit one
tuple per group.

**SortAggregate**: assumes sorted input on GROUP BY keys. Emits group
result when key changes. Lower memory than HashAggregate for pre-sorted data.

**Limit**: passes through first N tuples from child, then closes.

**Distinct**: HashDistinct (hash-based dedup) or SortDistinct (skip
consecutive duplicates on sorted input).

**Union/Intersect/Except**: Append children (UNION ALL), or hash-based
dedup (UNION), or hash-based set operations.

**Values**: emit literal rows from INSERT ... VALUES.

**Modify (Insert/Update/Delete)**: pulls tuples from child, applies
mutation to heap file, maintains indexes, checks constraints, writes WAL.

### Expression Evaluation
Expressions are compiled to a simple stack-based bytecode for evaluation:
```rust
enum ExprOp {
    PushColumn(usize),       // push tuple field by ordinal
    PushLiteral(Datum),
    PushNull,
    Add, Sub, Mul, Div, Mod, Pow, Concat,
    Eq, NotEq, Lt, Gt, LtEq, GtEq,
    And, Or, Not,
    IsNull, IsNotNull,
    Cast(DataType),
    Call(ScalarFnId, u8),    // function id + arity
    CaseStart, When, Then, Else, CaseEnd,
    Like,
    InList(u16),             // number of values to pop
}
```
Compiled once during plan creation, evaluated per tuple.

---

## 8) Storage Engine

### Page Layout
Fixed 8KB pages. Every page has a 24-byte header:
```
┌──────────────────────────────────────────┐
│ page_id: u64          (8 bytes)          │
│ page_type: u8         (1 byte)           │
│ free_space_offset: u16 (2 bytes)         │
│ tuple_count: u16      (2 bytes)          │
│ flags: u8             (1 byte)           │
│ lsn: u64              (8 bytes) — WAL    │
│ checksum: u16         (2 bytes)          │
├──────────────────────────────────────────┤
│ Slot array (grows downward from header)  │
│ slot[0]: (offset: u16, length: u16)      │
│ slot[1]: ...                             │
│ ...                                      │
├──────────── free space ──────────────────┤
│ Tuple data (grows upward from page end)  │
│ [tuple N] [tuple N-1] ... [tuple 0]      │
└──────────────────────────────────────────┘
```

Page types:
- `HEAP_DATA` — table data
- `BTREE_INTERNAL` — B+ tree internal node
- `BTREE_LEAF` — B+ tree leaf node
- `HASH_BUCKET` — hash index bucket
- `OVERFLOW` — TOAST / overflow data
- `FREE_SPACE_MAP` — tracks free space per page

### Tuple Layout
```
┌──────────────────────────────────────────┐
│ Header (variable size):                  │
│   xmin: u64    — creating transaction id │
│   xmax: u64    — deleting transaction id │
│   cid: u32     — command id within txn   │
│   flags: u8    — null bitmap follows?    │
│   null_bitmap: [u8; ceil(ncols/8)]       │
├──────────────────────────────────────────┤
│ Fixed-length columns (in schema order):  │
│   INTEGER: 4 bytes, BIGINT: 8 bytes,     │
│   FLOAT: 8 bytes, BOOLEAN: 1 byte,       │
│   DATE: 4 bytes, TIMESTAMP: 8 bytes,     │
│   DECIMAL: 16 bytes                      │
├──────────────────────────────────────────┤
│ Variable-length columns:                 │
│   (offset: u16, length: u16) in header   │
│   actual bytes at referenced offset      │
│   VARCHAR/TEXT > 2KB: TOAST pointer       │
└──────────────────────────────────────────┘
```

### Tuple Identifier (TID)
`(page_id: u64, slot_index: u16)` — uniquely locates a tuple.
Indexes store TIDs as pointers to heap tuples.

### Heap File
Each table stored as a sequence of pages. New tuples inserted into
the first page with sufficient free space (tracked by free space map).

Operations:
- `insert(tuple) → TID`
- `delete(tid)` → marks xmax on tuple (no physical removal until VACUUM)
- `update(tid, new_tuple) → new_TID` — delete old + insert new (no in-place update)
- `scan() → Iterator<Tuple>` — sequential scan through all pages
- `fetch(tid) → Tuple` — random access by TID

### B+ Tree Index
```
Order: ceil((page_size - header) / (key_size + ptr_size))
Typical: ~200 keys per internal node, ~150 key-TID pairs per leaf.

Internal node: [ptr₀, key₁, ptr₁, key₂, ptr₂, ..., keyₙ, ptrₙ]
Leaf node:     [key₁, tid₁, key₂, tid₂, ...] + next_leaf_ptr + prev_leaf_ptr

Supports:
- Point lookup: O(log_B N)
- Range scan: O(log_B N + result_size / B)
- Insert with node splitting (right-split, push median up)
- Delete with optional merge/redistribution (lazy: just mark deleted,
  merge on VACUUM when page < 40% full)
- Composite keys: compare tuple-wise, left-to-right
- Descending index support: reverse comparator
- Unique constraint enforcement at insert time
- Concurrent access via latch crabbing (top-down, release parent
  latch once child is safe)
```

### Hash Index
```
Extendible hashing:
- Directory: array of 2^d pointers (d = global depth)
- Buckets: pages of key-TID pairs
- Split on overflow: increment local depth, redistribute
- Supports: exact-match lookup O(1), no range scans
- Concurrent access: page-level latches
```

### Buffer Pool Manager
```rust
struct BufferPool {
    frames: Vec<Frame>,          // fixed-size array of page frames
    page_table: HashMap<PageId, FrameId>,
    replacer: LruKReplacer,      // K=2
    disk_manager: DiskManager,
    dirty_flags: BitVec,
}

struct Frame {
    page: Page,                  // 8KB
    pin_count: AtomicU32,
    is_dirty: AtomicBool,
    page_id: PageId,
}
```

- **Pin/Unpin protocol**: operators pin pages during access, unpin when done.
  Pinned pages cannot be evicted.
- **LRU-K replacement** (K=2): track last K access timestamps per frame.
  Evict the frame with the oldest K-th-last access. Handles sequential
  scan flooding better than plain LRU.
- **Dirty page writeback**: dirty pages written to disk on eviction or
  during checkpoint. WAL must be flushed before dirty page write (WAL
  protocol: page LSN ≤ flushed LSN).
- **Page size**: 8KB.
- **Default pool size**: 256MB (32768 frames). Configurable.
- **Pre-fetching**: sequential scan hints trigger async read-ahead of
  next 32 pages.

### TOAST (The Oversized-Attribute Storage Technique)
Values > 2KB stored out-of-line in overflow pages. Heap tuple contains
a TOAST pointer `(toast_table_id, chunk_id, total_length)`. Chunks are
2KB each, stored in a separate B+ tree keyed by `(chunk_id, sequence_no)`.

### Free Space Map
Per-table, tracks approximate free space per heap page. 1 byte per page
(free space in 32-byte granularity: 0 = full, 255 = empty). Used by
INSERT to find a page with room. Updated on insert/delete/vacuum.

---

## 9) Transaction Manager (MVCC)

### Transaction State
```rust
struct Transaction {
    txn_id: u64,                  // monotonically increasing
    status: TxnStatus,           // Active | Committed | Aborted
    isolation_level: IsolationLevel,
    snapshot: Snapshot,           // set of visible txn_ids
    write_set: Vec<WriteRecord>, // all modifications for rollback
    read_set: Vec<ReadRecord>,   // for SSI conflict detection
    savepoints: Vec<Savepoint>,
    start_ts: u64,               // logical timestamp
    commit_ts: Option<u64>,
}

struct Snapshot {
    xmin: u64,           // all txns < xmin are visible
    xmax: u64,           // all txns >= xmax are invisible
    active_txns: Vec<u64>, // txns in [xmin, xmax) that are still active (invisible)
}
```

### Visibility Rules (per tuple)
A tuple is visible to transaction T with snapshot S if:
1. `tuple.xmin` is committed AND `tuple.xmin` is in S (i.e., `< S.xmax`
   and not in `S.active_txns`).
2. AND either:
   a. `tuple.xmax` is invalid (not deleted), OR
   b. `tuple.xmax` is aborted, OR
   c. `tuple.xmax` is not in S (deleter started after snapshot).
3. Special case: tuple created by T itself (xmin == T.txn_id) is visible
   if not also deleted by T.

### Isolation Levels

**READ COMMITTED:**
- New snapshot taken for each statement.
- No phantom protection.
- Writes acquire row-level exclusive locks (released at commit).

**REPEATABLE READ (Snapshot Isolation):**
- Snapshot taken at transaction start, reused for all statements.
- Write-write conflict detection: if T1 and T2 both modify the same
  tuple, the second committer is aborted (first-committer-wins).
- No phantom protection.

**SERIALIZABLE (SSI — Serializable Snapshot Isolation):**
- Same as REPEATABLE READ plus rw-dependency tracking.
- Track "read-before" and "write-after" dependencies between
  concurrent transactions.
- Detect dangerous structures: if T1 →[rw] T2 →[rw] T3 and T1 and T3
  are concurrent, abort one of them.
- Implementation: per-tuple SIREAD locks (shared read markers) + conflict
  matrix checking at commit time.

### Lock Manager
```rust
enum LockMode {
    Shared,         // read lock
    Exclusive,      // write lock
    SIRead,         // SSI read tracking (never blocks)
}

struct LockManager {
    lock_table: HashMap<LockTarget, LockEntry>,
    waiter_graph: WaiterGraph,  // for deadlock detection
}

enum LockTarget {
    Table(TableId),
    Row(TableId, TID),
}
```

- Row-level locking for writes (exclusive).
- Table-level intention locks for DDL.
- No read locks under snapshot isolation (reads use snapshots).
- SIRead locks are advisory (never block, only track dependencies).

### Deadlock Detection
- Wait-for graph maintained by lock manager.
- Cycle detection via DFS every 100ms (or on every wait if few active txns).
- On deadlock: abort the transaction with the least work (fewest write records).

### Savepoints
```rust
struct Savepoint {
    name: String,
    write_set_position: usize,   // index into txn write_set
    lock_count: usize,           // number of locks held at savepoint
}
```
- `ROLLBACK TO SAVEPOINT name`: undo all write records after savepoint position,
  release locks acquired after savepoint, but keep transaction active.
- `RELEASE SAVEPOINT name`: discard savepoint marker (no undo).
- Nested savepoints supported (stack).

---

## 10) Write-Ahead Log (WAL)

### Log Record Format
```rust
enum WalRecord {
    // Transaction control
    Begin { txn_id: u64 },
    Commit { txn_id: u64 },
    Abort { txn_id: u64 },

    // Data modification (physiological logging)
    InsertTuple {
        txn_id: u64,
        table_id: TableId,
        page_id: u64,
        slot: u16,
        tuple_data: Vec<u8>,
    },
    DeleteTuple {
        txn_id: u64,
        table_id: TableId,
        page_id: u64,
        slot: u16,
        old_xmax: u64,       // for undo
    },
    UpdateTuple {
        txn_id: u64,
        table_id: TableId,
        old_page_id: u64,
        old_slot: u16,
        new_page_id: u64,
        new_slot: u16,
        old_tuple_data: Vec<u8>,  // for undo
        new_tuple_data: Vec<u8>,
    },

    // Index modification
    IndexInsert {
        txn_id: u64,
        index_id: IndexId,
        key: Vec<u8>,
        tid: TID,
    },
    IndexDelete {
        txn_id: u64,
        index_id: IndexId,
        key: Vec<u8>,
        tid: TID,
    },

    // Page-level
    PageAlloc { table_id: TableId, page_id: u64 },
    PageFree { table_id: TableId, page_id: u64 },

    // Checkpoint
    CheckpointBegin { active_txns: Vec<u64> },
    CheckpointEnd { active_txns: Vec<u64> },

    // Compensation (undo)
    CLR {
        txn_id: u64,
        undo_lsn: u64,       // LSN of the record being undone
        // ... redo data for the undo operation
    },
}

struct WalEntry {
    lsn: u64,              // log sequence number (byte offset in WAL)
    prev_lsn: u64,         // previous LSN for this transaction (undo chain)
    record: WalRecord,
    crc32: u32,            // integrity check
}
```

### WAL Protocol (Write-Ahead)
1. Before any dirty page is written to disk, all WAL records for
   modifications to that page must be flushed to WAL file.
2. Implementation: page header contains `page_lsn` (LSN of last
   modification). Buffer pool checks `page_lsn ≤ flushed_wal_lsn`
   before writing a dirty page.

### Group Commit
- WAL writes buffered in memory (64KB buffer).
- Flush triggered by: commit request, buffer full, or 10ms timeout.
- Multiple concurrent committing transactions share a single fsync.
- Reduces fsync overhead under high concurrency.

### Checkpointing
Periodic (every 5 minutes or configurable):
1. Write `CheckpointBegin` record (with list of active transactions).
2. Flush all dirty pages from buffer pool to disk.
3. Write `CheckpointEnd` record.
4. Truncate WAL before the `CheckpointBegin` LSN (all prior records no
   longer needed for recovery).

### ARIES-Style Recovery (3 phases)
On startup after crash:

**Phase 1 — Analysis:**
- Scan WAL forward from last checkpoint.
- Reconstruct dirty page table (pages that may need redo).
- Reconstruct active transaction table (transactions in-flight at crash).

**Phase 2 — Redo:**
- Scan WAL forward from earliest LSN in dirty page table.
- For each record: if page LSN < record LSN, re-apply the modification.
- This restores the database to the exact state at crash time.

**Phase 3 — Undo:**
- For each active (uncommitted) transaction: walk the undo chain
  (prev_lsn links) and undo each modification.
- Write CLR (Compensation Log Record) for each undo action (so undo
  is itself crash-safe — CLRs are never undone).
- Mark transaction as aborted.

After recovery: database is in a consistent state with only committed
transaction effects.

---

## 11) Catalog

System tables stored as regular heap tables (bootstrapped on first run):

```sql
-- sys_tables: one row per table
(table_id INTEGER PRIMARY KEY, table_name VARCHAR(128), schema_name VARCHAR(64),
 column_count INTEGER, row_count BIGINT, created_at TIMESTAMP)

-- sys_columns: one row per column
(table_id INTEGER, column_ordinal INTEGER, column_name VARCHAR(128),
 data_type VARCHAR(32), is_nullable BOOLEAN, default_expr TEXT,
 PRIMARY KEY (table_id, column_ordinal))

-- sys_indexes: one row per index
(index_id INTEGER PRIMARY KEY, table_id INTEGER, index_name VARCHAR(128),
 index_type VARCHAR(16), is_unique BOOLEAN, column_ordinals TEXT,
 created_at TIMESTAMP)

-- sys_constraints: one row per constraint
(constraint_id INTEGER PRIMARY KEY, table_id INTEGER, constraint_name VARCHAR(128),
 constraint_type VARCHAR(16), definition TEXT)

-- sys_views: one row per view
(view_id INTEGER PRIMARY KEY, view_name VARCHAR(128), definition TEXT)

-- sys_statistics: per-column statistics (updated by ANALYZE)
(table_id INTEGER, column_ordinal INTEGER, null_count BIGINT,
 distinct_count BIGINT, min_value TEXT, max_value TEXT, avg_width INTEGER,
 histogram_bounds TEXT, most_common_values TEXT, most_common_freqs TEXT,
 PRIMARY KEY (table_id, column_ordinal))
```

The catalog is loaded into an in-memory cache at startup. DDL statements
update both the heap tables and the cache transactionally.

---

## 12) PostgreSQL Wire Protocol

Implement PostgreSQL v3 frontend/backend protocol for client connectivity.
This allows any standard PostgreSQL client (psql, pgcli, JDBC, Python
psycopg2, Node pg, etc.) to connect.

### Message Flow
```
Client                          Server
  |--- StartupMessage --------->|
  |<-- AuthenticationOk --------|  (trust auth)
  |<-- ParameterStatus[] -------|  (server_version, encoding, etc.)
  |<-- ReadyForQuery -----------|
  |                             |
  |--- Query("SELECT ...") ---->|  (simple query protocol)
  |<-- RowDescription ----------|
  |<-- DataRow[] ---------------|
  |<-- CommandComplete ---------|
  |<-- ReadyForQuery -----------|
  |                             |
  |--- Parse (prepared) ------->|  (extended query protocol)
  |<-- ParseComplete -----------|
  |--- Bind (with params) ----->|
  |<-- BindComplete ------------|
  |--- Execute ---------------->|
  |<-- DataRow[] ---------------|
  |<-- CommandComplete ---------|
  |--- Sync ------------------->|
  |<-- ReadyForQuery -----------|
  |                             |
  |--- Terminate -------------->|
```

### Supported Messages
- **Simple Query Protocol**: `Query` → parse, bind, execute in one shot.
- **Extended Query Protocol**: `Parse` → `Bind` → `Describe` → `Execute` → `Sync`.
  Supports prepared statements with `$1`, `$2` parameter placeholders.
- **Error handling**: `ErrorResponse` with severity, code, message, detail,
  position (character offset in query).
- **COPY protocol**: `CopyInResponse` / `CopyData` / `CopyDone` for
  bulk import/export.
- **Transaction state indicator**: `ReadyForQuery` includes `I` (idle),
  `T` (in transaction), `E` (failed transaction).

### Connection Management
- TCP listener (default port 5433, configurable).
- One OS thread per connection (or async with tokio, configurable).
- Connection limit (default 100).
- Session state: current database, transaction, prepared statements,
  client encoding.

---

## 13) Configuration

All parameters in `sqld_config.toml`:

```toml
[server]
host = "127.0.0.1"
port = 5433
max_connections = 100
auth_mode = "trust"               # trust | password
password_file = "passwords.toml"  # if auth_mode = password

[storage]
data_directory = "./data"
page_size = 8192
default_tablespace = "default"

[buffer_pool]
size_mb = 256
replacement_policy = "lru-k"
lru_k = 2
prefetch_pages = 32

[wal]
directory = "./data/wal"
buffer_size_kb = 64
flush_interval_ms = 10
checkpoint_interval_sec = 300
max_wal_size_mb = 1024

[optimizer]
seq_page_cost = 1.0
random_page_cost = 4.0
cpu_tuple_cost = 0.01
cpu_index_cost = 0.005
cpu_hash_cost = 0.02
cpu_comparison_cost = 0.005
work_mem_kb = 4096
join_collapse_limit = 8          # max tables for DP join ordering
enable_hashjoin = true
enable_mergejoin = true
enable_indexscan = true
enable_seqscan = true
default_statistics_target = 100  # histogram bucket count

[transactions]
default_isolation = "read_committed"
deadlock_check_interval_ms = 100
max_locks_per_transaction = 1000

[vacuum]
auto_vacuum = true
auto_vacuum_threshold = 50       # min dead tuples before auto-vacuum
auto_vacuum_scale_factor = 0.2   # fraction of table size

[logging]
level = "info"                   # debug | info | warn | error
slow_query_threshold_ms = 1000
log_file = "./data/sqld.log"
```

---

## 14) Project Structure

```
sqld/
├── Cargo.toml
├── sqld_config.toml
├── README.md
├── src/
│   ├── main.rs                          # entry point, TCP listener, startup
│   ├── lib.rs                           # crate root, module declarations
│   ├── config.rs                        # TOML config loader + validation
│   ├── types/
│   │   ├── mod.rs
│   │   ├── datum.rs                     # Datum enum: typed runtime values
│   │   ├── data_type.rs                 # DataType enum + coercion rules
│   │   ├── schema.rs                    # Schema: ordered column definitions
│   │   └── tuple.rs                     # Tuple: row representation + serialization
│   ├── sql/
│   │   ├── mod.rs
│   │   ├── lexer.rs                     # SQL tokenizer
│   │   ├── token.rs                     # Token + TokenKind definitions
│   │   ├── parser.rs                    # Pratt parser + recursive descent
│   │   ├── ast.rs                       # AST node definitions
│   │   ├── binder.rs                    # Semantic analyzer (name resolution, typing)
│   │   ├── bound_ast.rs                 # Bound AST with resolved types
│   │   └── error.rs                     # Parser/binder error types with source location
│   ├── planner/
│   │   ├── mod.rs
│   │   ├── logical_plan.rs              # LogicalPlan enum + builder
│   │   ├── plan_builder.rs              # AST → LogicalPlan conversion
│   │   ├── optimizer.rs                 # Cost-based optimizer orchestrator
│   │   ├── rules/
│   │   │   ├── mod.rs
│   │   │   ├── predicate_pushdown.rs    # Push filters below joins
│   │   │   ├── projection_pushdown.rs   # Push column pruning to scans
│   │   │   ├── constant_folding.rs      # Fold constant expressions
│   │   │   ├── join_reorder.rs          # DP + greedy join ordering
│   │   │   ├── subquery_decorrelation.rs # Convert correlated subs to joins
│   │   │   ├── dead_column_elimination.rs
│   │   │   └── simplification.rs        # Boolean/arithmetic simplification
│   │   ├── cost_model.rs                # Cost estimation functions
│   │   ├── cardinality.rs               # Selectivity + row count estimation
│   │   ├── physical_plan.rs             # PhysicalPlan enum
│   │   ├── physical_planner.rs          # Logical → Physical plan selection
│   │   └── explain.rs                   # EXPLAIN formatter
│   ├── executor/
│   │   ├── mod.rs
│   │   ├── executor.rs                  # Executor trait + ExecContext
│   │   ├── seq_scan.rs                  # Sequential heap scan
│   │   ├── index_scan.rs                # B+ tree index scan
│   │   ├── filter.rs                    # Predicate evaluation
│   │   ├── project.rs                   # Expression projection
│   │   ├── hash_join.rs                 # Hash join (all join types)
│   │   ├── sort_merge_join.rs           # Sort-merge join
│   │   ├── nested_loop_join.rs          # Nested loop + index NL join
│   │   ├── sort.rs                      # In-memory + external merge sort
│   │   ├── hash_aggregate.rs            # Hash-based grouping + aggregation
│   │   ├── sort_aggregate.rs            # Sort-based grouping + aggregation
│   │   ├── limit.rs                     # Limit + Offset
│   │   ├── distinct.rs                  # Hash or sort-based dedup
│   │   ├── set_ops.rs                   # Union, Intersect, Except
│   │   ├── values.rs                    # Literal value rows
│   │   ├── modify.rs                    # Insert / Update / Delete execution
│   │   ├── expr_eval.rs                 # Compiled expression evaluator
│   │   └── scalar_functions.rs          # Built-in function implementations
│   ├── storage/
│   │   ├── mod.rs
│   │   ├── page.rs                      # Page layout: header, slots, tuples
│   │   ├── heap_file.rs                 # Heap file: insert, delete, scan, fetch
│   │   ├── btree/
│   │   │   ├── mod.rs
│   │   │   ├── btree.rs                 # B+ tree: insert, delete, search, scan
│   │   │   ├── node.rs                  # Internal + leaf node layout
│   │   │   ├── iterator.rs              # Range scan iterator
│   │   │   └── concurrent.rs            # Latch crabbing for concurrent access
│   │   ├── hash_index.rs                # Extendible hash index
│   │   ├── buffer_pool.rs               # Buffer pool manager + LRU-K
│   │   ├── disk_manager.rs              # File I/O, page read/write
│   │   ├── free_space_map.rs            # Per-table free space tracking
│   │   └── toast.rs                     # Oversized value storage
│   ├── transaction/
│   │   ├── mod.rs
│   │   ├── transaction.rs               # Transaction struct + lifecycle
│   │   ├── mvcc.rs                      # Visibility rules + snapshot management
│   │   ├── lock_manager.rs              # Row/table locking + deadlock detection
│   │   ├── ssi.rs                       # Serializable Snapshot Isolation
│   │   └── savepoint.rs                 # Savepoint management
│   ├── wal/
│   │   ├── mod.rs
│   │   ├── wal_manager.rs               # WAL writer, buffer, flush, group commit
│   │   ├── wal_record.rs                # Log record types + serialization
│   │   ├── recovery.rs                  # ARIES 3-phase crash recovery
│   │   └── checkpoint.rs                # Periodic checkpointing
│   ├── catalog/
│   │   ├── mod.rs
│   │   ├── catalog.rs                   # In-memory catalog cache
│   │   ├── system_tables.rs             # Bootstrap + system table schemas
│   │   └── statistics.rs                # Table/column statistics + ANALYZE
│   ├── protocol/
│   │   ├── mod.rs
│   │   ├── server.rs                    # TCP listener + connection dispatch
│   │   ├── connection.rs                # Per-connection state machine
│   │   ├── messages.rs                  # PG wire protocol message codec
│   │   ├── simple_query.rs              # Simple query flow
│   │   ├── extended_query.rs            # Parse/Bind/Execute flow
│   │   └── copy.rs                      # COPY protocol handler
│   └── utils/
│       ├── mod.rs
│       ├── error.rs                     # Error types hierarchy
│       ├── pool.rs                      # Object pool for reusable allocations
│       └── metrics.rs                   # Performance counters
├── tests/
│   ├── common/
│   │   ├── mod.rs                       # Shared test fixtures
│   │   ├── test_db.rs                   # In-memory test database builder
│   │   └── assertions.rs               # Custom assertion helpers
│   │
│   │  ## Layer 1: Types & Fundamentals
│   ├── test_datum.rs                    # Datum creation, comparison, hashing, coercion
│   ├── test_tuple.rs                    # Tuple serialization, deserialization, null handling
│   ├── test_schema.rs                   # Schema construction, column lookup, compatibility
│   │
│   │  ## Layer 2: SQL Frontend
│   ├── test_lexer.rs                    # Tokenization correctness
│   ├── test_parser_select.rs            # SELECT parsing (all clauses)
│   ├── test_parser_dml.rs              # INSERT, UPDATE, DELETE parsing
│   ├── test_parser_ddl.rs              # CREATE, DROP, ALTER parsing
│   ├── test_parser_expressions.rs       # Operator precedence, subqueries, CASE
│   ├── test_parser_errors.rs            # Error recovery, diagnostics quality
│   ├── test_binder.rs                   # Name resolution, type checking, scope
│   │
│   │  ## Layer 3: Storage Engine
│   ├── test_page.rs                     # Page layout, slot management, compaction
│   ├── test_heap_file.rs                # Insert, delete, scan, fetch, free space
│   ├── test_btree.rs                    # B+ tree insert, delete, search, split, merge
│   ├── test_btree_scan.rs              # Range scans, composite keys, direction
│   ├── test_btree_concurrent.rs         # Concurrent insert/delete with latch crabbing
│   ├── test_hash_index.rs              # Hash insert, lookup, split, directory growth
│   ├── test_buffer_pool.rs              # Pin/unpin, eviction, dirty writeback, LRU-K
│   ├── test_disk_manager.rs             # File I/O, page allocation, durability
│   ├── test_toast.rs                    # Large value storage and retrieval
│   ├── test_free_space_map.rs           # Free space tracking accuracy
│   │
│   │  ## Layer 4: Transactions
│   ├── test_mvcc_visibility.rs          # Tuple visibility under all isolation levels
│   ├── test_snapshot.rs                 # Snapshot correctness, active txn tracking
│   ├── test_transactions.rs             # Commit, abort, rollback semantics
│   ├── test_isolation_read_committed.rs # RC-specific behaviors
│   ├── test_isolation_snapshot.rs       # SI write-write conflict detection
│   ├── test_isolation_serializable.rs   # SSI rw-dependency cycle detection
│   ├── test_lock_manager.rs             # Lock acquisition, release, blocking, upgrade
│   ├── test_deadlock.rs                 # Deadlock detection + victim selection
│   ├── test_savepoints.rs              # Savepoint create, rollback, release, nesting
│   │
│   │  ## Layer 5: WAL & Recovery
│   ├── test_wal_write.rs               # Log record serialization, checksums
│   ├── test_wal_flush.rs               # Flush protocol, group commit correctness
│   ├── test_wal_protocol.rs            # WAL-before-page-write enforcement
│   ├── test_recovery_redo.rs            # Redo: committed txn effects restored
│   ├── test_recovery_undo.rs            # Undo: uncommitted txn effects removed
│   ├── test_recovery_checkpoint.rs      # Recovery from checkpoint + incremental WAL
│   ├── test_recovery_nested.rs          # Recovery with savepoints + CLRs
│   ├── test_crash_simulation.rs         # Inject crashes at various points, verify recovery
│   │
│   │  ## Layer 6: Query Planning & Optimization
│   ├── test_logical_plan.rs             # AST → logical plan conversion
│   ├── test_predicate_pushdown.rs       # Filter pushed below joins correctly
│   ├── test_projection_pushdown.rs      # Column pruning at scan level
│   ├── test_constant_folding.rs         # Constant expression evaluation
│   ├── test_join_reorder.rs             # DP and greedy join ordering
│   ├── test_subquery_decorrelation.rs   # Correlated subquery → join conversion
│   ├── test_cardinality_estimation.rs   # Selectivity estimates vs actual
│   ├── test_cost_model.rs              # Cost ranking: index scan vs seq scan, join strategies
│   ├── test_index_selection.rs          # Optimizer picks index when beneficial
│   ├── test_explain.rs                  # EXPLAIN output format + EXPLAIN ANALYZE
│   │
│   │  ## Layer 7: Executor
│   ├── test_seq_scan.rs                 # Full scan, filtered scan, MVCC-filtered
│   ├── test_index_scan.rs               # Point lookup, range, composite, index-only
│   ├── test_hash_join.rs                # Inner, left, right, full, semi, anti joins
│   ├── test_sort_merge_join.rs          # Merge join on sorted inputs
│   ├── test_nested_loop_join.rs         # NL join, index NL join
│   ├── test_sort.rs                     # In-memory sort, external sort, multi-key
│   ├── test_aggregate.rs               # Hash + sort aggregation, DISTINCT aggregates
│   ├── test_limit.rs                    # LIMIT, OFFSET, LIMIT+OFFSET
│   ├── test_set_ops.rs                  # UNION, INTERSECT, EXCEPT (ALL variants)
│   ├── test_modify.rs                   # INSERT/UPDATE/DELETE execution + constraint enforcement
│   ├── test_expr_eval.rs               # All operators, functions, NULL propagation, CASE
│   ├── test_scalar_functions.rs         # String, math, date/time functions
│   │
│   │  ## Layer 8: Catalog
│   ├── test_catalog.rs                  # Table/index/view creation, lookup, drop
│   ├── test_statistics.rs               # ANALYZE: histogram, MCV, distinct count accuracy
│   │
│   │  ## Layer 9: Wire Protocol
│   ├── test_protocol_simple.rs          # Simple query flow: query → results
│   ├── test_protocol_extended.rs        # Prepared statements: parse, bind, execute
│   ├── test_protocol_errors.rs          # Error response format, transaction state
│   ├── test_protocol_copy.rs            # COPY IN/OUT protocol
│   ├── test_protocol_types.rs           # Type serialization (text + binary format)
│   │
│   │  ## Layer 10: Integration Tests
│   ├── test_sql_e2e.rs                  # End-to-end SQL execution (comprehensive)
│   ├── test_concurrent_txns.rs          # Multi-client transactional correctness
│   ├── test_constraint_enforcement.rs   # PK, FK, UNIQUE, CHECK, NOT NULL
│   ├── test_views.rs                    # View creation, querying, drop cascade
│   ├── test_vacuum.rs                   # Dead tuple reclamation, space reuse
│   ├── test_bulk_load.rs               # COPY import/export, large datasets
│   ├── test_tpcc_subset.rs             # TPC-C-inspired transactional workload
│   └── test_edge_cases.rs              # Adversarial queries, huge expressions, deep nesting
│
├── benches/
│   ├── bench_lexer.rs                   # Tokens/sec on complex SQL
│   ├── bench_parser.rs                  # Parses/sec on various query types
│   ├── bench_btree.rs                   # Insert/lookup/scan throughput
│   ├── bench_buffer_pool.rs             # Hit ratio, eviction throughput
│   ├── bench_executor.rs               # TPC-H-inspired query benchmarks (Q1, Q3, Q5, Q6)
│   ├── bench_optimizer.rs              # Plan generation time for complex queries
│   ├── bench_wal.rs                     # WAL write throughput (MB/s)
│   ├── bench_transactions.rs            # Txn/sec under contention
│   └── bench_e2e.rs                     # Full query latency (parse → execute)
└── sql_tests/
    ├── basic_select.sql                 # Standard SELECT queries + expected results
    ├── joins.sql                        # All join types with expected output
    ├── aggregates.sql                   # GROUP BY, HAVING, all aggregate functions
    ├── subqueries.sql                   # Correlated, uncorrelated, EXISTS, IN
    ├── set_operations.sql               # UNION, INTERSECT, EXCEPT
    ├── dml.sql                          # INSERT, UPDATE, DELETE, RETURNING
    ├── ddl.sql                          # CREATE/ALTER/DROP TABLE, INDEX, VIEW
    ├── types_and_coercion.sql           # Type casting, implicit coercion
    ├── null_handling.sql                # Three-valued logic, NULL propagation
    ├── constraints.sql                  # PK, FK, UNIQUE, CHECK enforcement
    ├── transactions.sql                 # Isolation level behaviors
    └── edge_cases.sql                   # Empty tables, self-joins, deep nesting
```

---

## 15) Test Suite — Detailed Specifications

### Layer 1: Types & Fundamentals

**test_datum.rs**
- INTEGER comparison: `3 < 5`, `5 == 5`, `-1 < 0`
- FLOAT comparison: NaN handling (`NaN != NaN`, NaN sorts last)
- DECIMAL arithmetic: `0.1 + 0.2 == 0.3` (exact, no floating point error)
- VARCHAR comparison: lexicographic, case-sensitive
- TIMESTAMP arithmetic: add interval, subtract timestamps → interval
- NULL comparison: `NULL == NULL` → NULL (not true), `NULL < 5` → NULL
- Datum hashing: equal values produce equal hashes (HashMap compatibility)
- Type coercion: INTEGER → BIGINT → FLOAT, VARCHAR → TEXT, DATE → TIMESTAMP
- Invalid coercion: TEXT → INTEGER errors unless CAST
- Datum serialization: round-trip through binary encoding

**test_tuple.rs**
- Tuple with all data types: serialize → deserialize → values match
- Null bitmap: correctly marks null columns, non-null columns readable
- Variable-length fields: VARCHAR and TEXT stored and retrieved correctly
- Empty tuple (0 columns): valid
- Tuple comparison: for ORDER BY support, multi-column compare
- Tuple projection: extract subset of columns into new tuple
- Large tuple: with TOAST-eligible TEXT field, pointer stored correctly

**test_schema.rs**
- Column lookup by name: correct ordinal returned
- Column lookup by ordinal: correct name and type
- Unknown column: error
- Duplicate column names in CREATE: error
- Schema compatibility: two schemas equal if same columns, same types, same order
- Schema merge: for join output (left schema + right schema)

### Layer 2: SQL Frontend

**test_lexer.rs**
- Keywords case-insensitive: `SELECT` = `select` = `SeLeCt`
- Integer literal: `123` → IntegerLiteral(123)
- Float literal: `1.23`, `1.23e4`, `.5` → correct float values
- Hex literal: `0xFF` → IntegerLiteral(255)
- String literal: `'hello'`, `'it''s'` → correct unescaping
- E-string: `E'line1\nline2'` → string with actual newline
- Quoted identifier: `"MyTable"` → preserves case
- Unquoted identifier: `MyTable` → lowered to `mytable`
- Operators: `<>`, `<=`, `>=`, `::`, `||` — longest match
- Comments: `-- single line` and `/* block /* nested */ */` skipped
- Whitespace: not tokenized
- Error token: `@` → error with source location
- Placeholder: `$1`, `$42` → Placeholder with correct index
- Empty input: single EOF token
- Consecutive strings: `'a' 'b'` → two separate string tokens (not concatenated)

**test_parser_select.rs**
- Simple: `SELECT 1` → literal projection
- Columns: `SELECT a, b FROM t` → column refs with correct table
- Alias: `SELECT a AS x FROM t` → alias preserved in AST
- Star: `SELECT * FROM t` → wildcard
- Table alias: `FROM t AS alias` → alias usable in WHERE
- WHERE: `SELECT * FROM t WHERE a > 5` → Filter with BinaryOp
- ORDER BY: `ORDER BY a DESC NULLS FIRST` → all fields parsed
- LIMIT/OFFSET: `LIMIT 10 OFFSET 5` → correct values
- GROUP BY + HAVING: grouped aggregation parsed correctly
- DISTINCT: `SELECT DISTINCT a FROM t` → distinct flag set
- JOIN: `FROM t1 JOIN t2 ON t1.id = t2.id` → JoinClause in AST
- LEFT/RIGHT/FULL OUTER JOIN: join type preserved
- CROSS JOIN and NATURAL JOIN: parsed correctly
- Subquery in WHERE: `WHERE id IN (SELECT ...)` → InSubquery node
- Subquery in FROM: `FROM (SELECT ...) AS sub` → derived table
- Correlated subquery: `WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.a = t1.a)`
- Set operations: `SELECT ... UNION ALL SELECT ...`
- Nested parentheses: `SELECT ((a + b) * c)` → correct precedence
- Multiple set ops: `A UNION B INTERSECT C` → correct precedence (INTERSECT binds tighter)

**test_parser_dml.rs**
- INSERT with column list: parsed correctly
- INSERT with VALUES: single and multi-row
- INSERT ... SELECT: subquery as source
- UPDATE with SET and WHERE: assignments and filter
- UPDATE with RETURNING: returning clause parsed
- DELETE with WHERE: filter expression
- DELETE with RETURNING: returning clause parsed
- INSERT without column list: all columns assumed

**test_parser_ddl.rs**
- CREATE TABLE with all constraint types: PK, FK, UNIQUE, CHECK, NOT NULL, DEFAULT
- Composite PRIMARY KEY: multi-column
- FOREIGN KEY with ON DELETE/UPDATE actions: CASCADE, SET NULL, RESTRICT
- DROP TABLE IF EXISTS CASCADE: all options parsed
- ALTER TABLE ADD/DROP/RENAME COLUMN: each variant
- CREATE INDEX with ASC/DESC: column ordering preserved
- CREATE INDEX USING HASH: index type parsed
- CREATE UNIQUE INDEX: uniqueness flag
- CREATE VIEW AS SELECT: view definition stored
- DROP VIEW IF EXISTS: parsed correctly

**test_parser_expressions.rs**
- Operator precedence: `1 + 2 * 3` → `1 + (2 * 3)` = 7
- Precedence: `a OR b AND c` → `a OR (b AND c)`
- Precedence: `NOT a AND b` → `(NOT a) AND b`
- BETWEEN: `a BETWEEN 1 AND 10` → Between node
- LIKE: `name LIKE 'A%'` → Like node
- CASE: simple and searched CASE expressions
- CAST: `CAST(x AS INTEGER)` and shorthand `x::INTEGER`
- IS NULL / IS NOT NULL: parsed correctly
- IN (list): `a IN (1, 2, 3)` → InList
- Function call: `COUNT(DISTINCT a)` → AggregateCall with distinct
- Nested subquery: `(SELECT MAX(a) FROM t)` as scalar subquery
- Unary minus: `-5`, `-(a + b)` → UnaryOp
- String concatenation: `a || b` → Concat operator
- Complex: `CASE WHEN a > 0 THEN a * 2 ELSE -a END + 1` → correct tree

**test_parser_errors.rs**
- Missing FROM: `SELECT a WHERE ...` → error with helpful message
- Missing comma: `SELECT a b FROM t` → error, suggests comma
- Unterminated string: `'hello` → error with position
- Unknown token: `SELECT @` → error
- Extra tokens: `SELECT 1 2` → error after valid statement
- Empty SELECT list: `SELECT FROM t` → error
- Mismatched parens: `SELECT (a + b` → error
- Multiple errors: collect and report all (up to limit)
- Recovery: after error, parser can parse next statement after `;`

**test_binder.rs**
- Column resolution: `SELECT a FROM t` → resolves `a` to `t.a` with correct type
- Ambiguous column: `SELECT a FROM t1, t2` (both have `a`) → error
- Qualified column: `SELECT t1.a FROM t1, t2` → resolves correctly
- Unknown table: `SELECT * FROM nonexistent` → error
- Unknown column: `SELECT z FROM t` → error (where t has no column z)
- Type checking: `SELECT a + 'hello' FROM t` (a is INTEGER) → error
- Implicit coercion: `SELECT int_col + bigint_col` → BIGINT result
- Aggregate in WHERE: `SELECT * FROM t WHERE COUNT(*) > 5` → error
- Non-aggregated column with GROUP BY: `SELECT a, b FROM t GROUP BY a` → error (b not aggregated)
- Wildcard expansion: `SELECT * FROM t` → all columns listed explicitly
- View expansion: `SELECT * FROM my_view` → view SQL inlined + rebound
- Subquery scope: correlated subquery references outer column correctly
- Subquery scope: correlated subquery references nonexistent outer column → error
- NOT NULL check in INSERT: `INSERT INTO t (notnull_col) VALUES (NULL)` → error

### Layer 3: Storage Engine

**test_page.rs**
- New page: correct header, zero tuples, full free space
- Insert tuple: slot added, tuple data written at correct offset
- Insert multiple tuples: slots grow downward, data grows upward, no overlap
- Page full: insert fails when insufficient space
- Delete tuple: slot marked free (but space not immediately reclaimed)
- Compaction: after delete + compact, free space consolidated
- Tuple fetch by slot index: correct data returned
- Page serialization: write to bytes, read back, all tuples intact
- Checksum: correct on clean page, incorrect if byte flipped

**test_heap_file.rs**
- Insert single tuple: returns valid TID, fetch by TID returns same tuple
- Insert 1000 tuples: all retrievable by TID
- Sequential scan: returns all inserted tuples (order may vary)
- Delete by TID: subsequent fetch returns "deleted" indicator
- Scan after delete: deleted tuple not returned (MVCC: xmax set)
- Update (delete + insert): new TID returned, old TID marked deleted
- Free space reuse: after vacuum, inserts reuse freed space
- Multi-page: inserts spanning multiple pages all retrievable
- Empty table scan: returns zero tuples
- Large tuple: TOAST pointer created, actual data in overflow pages

**test_btree.rs**
- Insert ascending keys: tree balanced, height = ceil(log_B(N))
- Insert descending keys: same balance property (not degenerate)
- Insert random keys: all retrievable via exact-match search
- Insert 100k keys: tree height ≤ 3 (for reasonable page size)
- Delete key: no longer found in search
- Delete all keys: tree reduced to empty root
- Split: insert into full leaf → correct split, median promoted
- Split: internal node split when full → correct redistribution
- Duplicate keys (non-unique index): all TIDs retrievable
- Unique index: reject duplicate key insert
- Composite key: (a, b) — search on prefix (a) returns all matching
- Composite key: (a, b) — search on full key returns exact match

**test_btree_scan.rs**
- Range scan `[low, high]`: returns exactly matching keys
- Range scan `(low, high)`: exclusive bounds
- Range scan `[low, ∞)`: all keys ≥ low
- Range scan `(-∞, high]`: all keys ≤ high
- Full scan: all keys in sorted order
- Reverse scan: all keys in reverse sorted order
- Empty range: no results
- Scan after insert: new key visible
- Scan after delete: deleted key not visible
- Composite key range: `(a=5, b≥10)` → correct subset

**test_btree_concurrent.rs**
- Concurrent inserts from 8 threads: all values present after join
- Concurrent insert + search: searches never return corrupt data
- Concurrent insert + delete: no lost updates, no double-deletes
- Latch ordering: no deadlocks under random concurrent operations
- Stress: 100k operations across 16 threads, tree invariants maintained

**test_hash_index.rs**
- Insert and lookup: exact match works
- Insert 10k distinct keys: all retrievable
- Bucket split: correct redistribution, global depth increases
- Duplicate keys: all TIDs stored
- Delete key: no longer found
- No range scan support: range query returns error or empty
- Concurrent: 8-thread insert+lookup, all values correct

**test_buffer_pool.rs**
- Fetch page: pin count incremented
- Unpin page: pin count decremented
- Eviction: unpinned page evicted when pool full, pinned page not evicted
- Dirty page: marked dirty, written to disk on eviction
- LRU-K ordering: recently-used-twice page preferred over once-used page
- Sequential scan resistance: pages accessed once don't thrash cache
- Pin 0 pages: eviction candidate selection still works
- All pages pinned: new fetch blocks or errors (no eviction possible)
- WAL protocol: dirty page write only after WAL flushed to that LSN
- Page checksums: corrupt page detected on read

**test_disk_manager.rs**
- Allocate page: returns new page ID, page file grows
- Write page: data persisted to file at correct offset
- Read page: returns previously written data
- Deallocate page: page marked free, reused by next allocation
- File creation: data directory and files created on first use
- Concurrent read/write: file locking prevents corruption

**test_toast.rs**
- Store 5KB TEXT value: stored in overflow, heap tuple has pointer
- Retrieve: pointer resolved, full value returned
- Store exactly 2KB: stored inline (boundary case, no TOAST)
- Store 2001 bytes: TOAST triggered
- Multiple chunks: 10KB value stored across multiple overflow pages
- Delete TOASTed tuple: overflow pages freed

**test_free_space_map.rs**
- New table: all pages marked "empty"
- After insert: page free space updated
- After delete+vacuum: free space increased
- Find page with space: returns page with sufficient room
- No page with space: returns "allocate new page" indicator

### Layer 4: Transactions

**test_mvcc_visibility.rs**
- Committed insert visible to new snapshot
- Uncommitted insert invisible to other transactions
- Deleted tuple invisible after deleter commits
- Deleted tuple visible if deleter hasn't committed yet
- Own writes visible within same transaction
- Own deletes invisible within same transaction
- Tuple created and deleted in same txn: invisible to others after commit

**test_snapshot.rs**
- Snapshot captures all committed txns at creation time
- Active txns at snapshot time are invisible
- Txn that commits after snapshot still invisible
- READ COMMITTED: new snapshot per statement (sees newly committed rows)
- REPEATABLE READ: same snapshot for entire transaction

**test_transactions.rs**
- BEGIN/COMMIT: changes visible after commit
- BEGIN/ROLLBACK: changes reverted, not visible
- Auto-commit: statements without explicit BEGIN auto-commit
- Nested BEGIN: error (no nested transactions; use savepoints)
- Commit without begin: error
- Concurrent commits: both succeed if no conflict

**test_isolation_read_committed.rs**
- Non-repeatable read: T1 reads, T2 updates+commits, T1 re-reads sees new value
- Phantom: T1 counts rows, T2 inserts+commits, T1 re-counts sees new count
- Write-write: two txns update same row → second waits for lock → succeeds after first commits

**test_isolation_snapshot.rs**
- Repeatable read: T1 reads, T2 updates+commits, T1 re-reads sees OLD value
- No phantom: T1 counts, T2 inserts+commits, T1 re-counts sees SAME count
- Write-write conflict: T1 and T2 both update same row → second committer aborted (first-committer-wins)
- Write-write on different rows: both succeed
- Write skew allowed: T1 reads X writes Y, T2 reads Y writes X — both commit (SI anomaly)

**test_isolation_serializable.rs**
- Write skew prevented: same scenario as above → one transaction aborted
- Read-only anomaly: T1 (read-only), T2, T3 — dangerous structure detected if applicable
- No false positives: independent transactions on different data → both commit
- SSI with high concurrency: 8 threads, 1000 txns each, no anomalies in results

**test_lock_manager.rs**
- Shared locks: two txns both acquire shared lock on same row → no block
- Exclusive locks: second txn blocks on exclusive lock → proceeds after first releases
- Shared then exclusive: upgrade blocks if other shared holder exists
- Lock release: on commit/abort, all locks released
- No starvation: waiting txns eventually granted lock (FIFO queue)

**test_deadlock.rs**
- Two-txn cycle: T1 holds A wants B, T2 holds B wants A → deadlock detected
- Three-txn cycle: T1→T2→T3→T1 → detected
- No false deadlock: T1 holds A wants B (held by T2), T2 not waiting → no deadlock
- Victim selection: txn with fewer writes aborted
- After abort: other txn proceeds successfully

**test_savepoints.rs**
- Savepoint + rollback: changes after savepoint undone, changes before preserved
- Nested savepoints: rollback to outer savepoint also undoes inner savepoint changes
- Release savepoint: savepoint marker removed, changes preserved
- Rollback to nonexistent savepoint: error
- Savepoint in committed txn: error (can't rollback committed txn)
- Multiple savepoints: can rollback to any (discards all after it)

### Layer 5: WAL & Recovery

**test_wal_write.rs**
- Log record serialization: each record type serializes + deserializes correctly
- LSN assignment: monotonically increasing
- CRC32 integrity: correct checksum, tampered record detected
- Prev-LSN chain: follows correct per-transaction undo chain

**test_wal_flush.rs**
- After flush: all records up to flushed LSN on disk
- Group commit: 10 concurrent commits → single fsync covers all
- Flush ordering: records appear in LSN order on disk

**test_wal_protocol.rs**
- Dirty page not written before WAL flush: enforce page_lsn ≤ flushed_lsn
- Violating WAL protocol: test that buffer pool refuses to write page if WAL not flushed

**test_recovery_redo.rs**
- Insert + commit + crash: after recovery, row is present
- Update + commit + crash: after recovery, updated value present
- Delete + commit + crash: after recovery, row is gone
- Multiple committed txns: all effects present after recovery
- Idempotent redo: apply same WAL twice → same result (no double-insert)

**test_recovery_undo.rs**
- Insert without commit + crash: after recovery, row is NOT present
- Update without commit + crash: after recovery, original value restored
- Delete without commit + crash: after recovery, row is present
- Mixed: T1 committed, T2 uncommitted → T1 effects present, T2 effects undone

**test_recovery_checkpoint.rs**
- Checkpoint + crash: recovery only replays WAL after checkpoint
- Dirty pages flushed during checkpoint: redo skips pages with current LSN
- Active txn at checkpoint: correctly tracked and undone if uncommitted

**test_recovery_nested.rs**
- Savepoint + partial rollback + commit + crash: only final state survives
- CLR records: undo generates CLRs, recovery does not re-undo CLRs
- Crash during undo: CLRs allow resumption of undo after second crash

**test_crash_simulation.rs**
- Crash after WAL write but before page flush: redo restores page
- Crash during checkpoint: recovery handles partial checkpoint
- Crash mid-insert (between WAL record and page write): consistent after recovery
- Crash mid-B+ tree split: recovery rebuilds consistent tree
- 100 random crash points during workload: always recovers to consistent state

### Layer 6: Query Planning & Optimization

**test_logical_plan.rs**
- Simple SELECT → Scan + Project
- SELECT with WHERE → Scan + Filter + Project
- JOIN → Scan + Scan + Join
- Aggregate → Scan + Aggregate + Project
- Subquery → nested plan
- INSERT ... SELECT → Insert + child plan

**test_predicate_pushdown.rs**
- Filter above join → pushed to correct side (or both if applicable)
- Filter on left table pushed to left scan
- Filter on right table pushed to right scan
- Filter on join column: stays at join (or becomes join condition)
- Filter above aggregate: check if can push below (only if on GROUP BY column)
- Non-pushable filter (e.g., references both tables): stays above join

**test_projection_pushdown.rs**
- SELECT a FROM t (t has a, b, c) → scan only reads column a
- Join needs key column: pushed projection includes join keys
- Aggregate needs grouped column: projection includes GROUP BY columns
- Dead columns after projection: eliminated from child plans

**test_constant_folding.rs**
- `1 + 2` → `3`
- `'hello' || ' ' || 'world'` → `'hello world'`
- `true AND false` → `false`
- `x AND true` → `x` (partial folding)
- `x OR false` → `x`
- `CAST(5 AS FLOAT)` → `5.0`
- Non-constant: `a + 1` → unchanged

**test_join_reorder.rs**
- 2 tables: order by estimated cost (smaller table as build side for hash join)
- 3 tables: DP finds optimal order (verified against exhaustive search)
- 6 tables: DP produces valid plan with lower cost than left-deep
- 8 tables: greedy heuristic produces reasonable plan
- Cross join last: tables without join predicates pushed to outer position
- Star schema: fact table joined with all dimensions, optimizer produces star join

**test_subquery_decorrelation.rs**
- `WHERE id IN (SELECT fk FROM t2)` → semi-join
- `WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.a = t1.a)` → semi-join
- `WHERE NOT EXISTS (...)` → anti-join
- Correlated scalar subquery in SELECT list → left join + aggregate
- Deeply nested correlated subquery (2 levels) → correct flattening

**test_cardinality_estimation.rs**
- Equality selectivity: `1 / distinct_count` (within 20% of actual on test data)
- Range selectivity: histogram interpolation (within 30% of actual)
- MCV: frequent value selectivity matches stored frequency
- AND: independence assumption product
- OR: inclusion-exclusion
- Join: formula `|R| × |S| / max(distinct(R), distinct(S))` (within 50% of actual)
- Unknown predicate: 0.1 default
- After ANALYZE: estimates improve vs before ANALYZE

**test_cost_model.rs**
- SeqScan cheaper than IndexScan for full table (no filter)
- IndexScan cheaper than SeqScan for highly selective filter (< 5% rows)
- HashJoin preferred for equi-join with large tables
- NestedLoopJoin preferred when inner is very small (< 100 rows)
- SortMergeJoin preferred when both inputs already sorted
- Cost monotonicity: adding a filter doesn't increase scan cost estimate
- Index-only scan: cheaper than index scan + heap fetch

**test_index_selection.rs**
- Equality on indexed column: index scan chosen
- Range on indexed column: index scan chosen
- No matching index: seq scan chosen
- Composite index partial match: prefix columns match → index used
- Composite index suffix-only: no index prefix → seq scan
- Low selectivity (> 30% rows): seq scan preferred over index
- LIKE 'prefix%': B+ tree range scan
- LIKE '%suffix': seq scan (index not usable)

**test_explain.rs**
- Output includes operator name, estimated cost, estimated rows
- Nested indentation shows plan tree structure
- EXPLAIN ANALYZE: includes actual rows, actual time, buffers
- Join condition shown
- Filter condition shown
- Index name shown for index scans

### Layer 7: Executor

**test_seq_scan.rs**
- Empty table: returns 0 tuples
- 1000-row table: returns all 1000 tuples
- With pushed-down filter: returns only matching rows
- MVCC-filtered: uncommitted rows from other txns invisible
- Multiple sequential scans: each returns full dataset independently

**test_index_scan.rs**
- Point lookup: exact key → correct TID → correct tuple
- Range scan: all tuples in range returned
- Composite index: `(a=5, b=10)` → exact match
- Composite prefix: `(a=5)` → all matching tuples regardless of b
- Index-only scan: returns values from index without heap access
- Empty result: key not in index → no tuples
- Scan direction: forward and reverse

**test_hash_join.rs**
- Inner join: matching rows from both sides
- Left join: all left rows, nulls for non-matching right
- Right join: all right rows, nulls for non-matching left
- Full outer join: all rows from both, nulls where no match
- Semi join: left rows that have at least one match in right
- Anti join: left rows that have NO match in right
- Empty inner: left join returns all left rows with null right
- Multiple matches: correct Cartesian product within groups
- Null join key: no match (NULL ≠ NULL)

**test_sort_merge_join.rs**
- Equi-join: same result as hash join (cross-validation)
- Pre-sorted inputs: efficient (no re-sort)
- Range join: `a.val BETWEEN b.low AND b.high` (if supported by merge)

**test_nested_loop_join.rs**
- Small inner: correct results
- Cross join: all combinations
- Index nested loop: inner index scan parameterized by outer row

**test_sort.rs**
- In-memory sort: 100 rows by single column
- Multi-key sort: ORDER BY a ASC, b DESC NULLS LAST
- External sort: force `work_mem` to 32KB, sort 100k rows → disk spill, correct result
- Stability: equal keys maintain insertion order
- Null ordering: NULLS FIRST / NULLS LAST

**test_aggregate.rs**
- COUNT(*): correct count
- COUNT(col): excludes nulls
- COUNT(DISTINCT col): unique values only
- SUM, AVG: correct numeric result (including DECIMAL precision)
- MIN, MAX: correct extremes
- Empty input: COUNT → 0, SUM → NULL, AVG → NULL
- GROUP BY: groups formed correctly, one output per group
- HAVING: filters groups post-aggregation
- Multiple aggregates: `SELECT COUNT(*), SUM(a), AVG(b) FROM t`
- No GROUP BY with aggregate: entire table is one group

**test_limit.rs**
- LIMIT 10: exactly 10 rows (or fewer if table smaller)
- OFFSET 5: first 5 rows skipped
- LIMIT 10 OFFSET 5: rows 6–15
- LIMIT 0: no rows
- OFFSET > total rows: no rows
- LIMIT without ORDER BY: any valid subset (non-deterministic)

**test_set_ops.rs**
- UNION: deduplicates
- UNION ALL: preserves duplicates
- INTERSECT: only rows in both
- EXCEPT: rows in left but not right
- Schema compatibility: error if column counts/types differ
- Nested: `(A UNION B) EXCEPT C` → correct result

**test_modify.rs**
- INSERT: row present in subsequent scan
- INSERT with DEFAULT: default value applied
- INSERT with NOT NULL violation: error, no row inserted
- INSERT with UNIQUE violation: error, no row inserted
- INSERT with FK violation: error
- UPDATE: old value gone, new value present
- UPDATE with CHECK violation: error, old value preserved
- DELETE: row removed from scan
- DELETE with FK: CASCADE deletes dependent rows
- RETURNING: returns affected rows

**test_expr_eval.rs**
- All arithmetic operators on INTEGER, BIGINT, FLOAT, DECIMAL
- Division by zero: error (not NaN or infinity)
- Integer overflow: error (or wrap, specify behavior)
- String functions: LENGTH, UPPER, LOWER, SUBSTRING, etc.
- NULL propagation: any op with NULL → NULL (except IS NULL)
- Boolean logic: three-valued truth tables (TRUE/FALSE/NULL)
- CASE expression: correct branch selection, NULL handling
- BETWEEN: inclusive both ends
- LIKE: `%` matches any sequence, `_` matches one char
- IN list: with and without NULLs
- COALESCE: returns first non-null
- CAST: all valid conversions + error on invalid

**test_scalar_functions.rs**
- All string functions: test with edge cases (empty string, null, unicode)
- All math functions: test with edge cases (zero, negative, NaN for float)
- Date/time: EXTRACT, DATE_TRUNC, AGE, interval arithmetic
- NOW(): returns non-null timestamp
- RANDOM(): returns value in [0, 1)
- TO_CHAR: formatting patterns

### Layer 8: Catalog

**test_catalog.rs**
- CREATE TABLE: table appears in catalog, columns correct
- DROP TABLE: table removed, cannot be queried
- DROP TABLE CASCADE: dependent views/indexes also dropped
- CREATE INDEX: index appears in catalog, linked to table
- DROP INDEX: removed
- ALTER TABLE ADD COLUMN: new column visible in schema
- ALTER TABLE DROP COLUMN: column removed, data inaccessible
- CREATE VIEW: view definition stored, queryable
- DROP VIEW: removed
- Concurrent DDL: second CREATE TABLE with same name → error

**test_statistics.rs**
- ANALYZE on table: row_count matches actual
- Distinct count: correct for unique column, approximate for non-unique
- Null count: correct
- Histogram: bucket boundaries cover min-max range
- MCV: most frequent values correctly identified
- After INSERT: ANALYZE reflects new data
- Empty table: zero stats

### Layer 9: Wire Protocol

**test_protocol_simple.rs**
- Send `SELECT 1` → receive RowDescription + DataRow(1) + CommandComplete
- Send `SELECT * FROM t` (table with 5 rows) → 5 DataRows
- Send `CREATE TABLE ...` → CommandComplete("CREATE TABLE")
- Send invalid SQL → ErrorResponse with position

**test_protocol_extended.rs**
- Parse `SELECT * FROM t WHERE id = $1` → ParseComplete
- Bind with param value 5 → BindComplete
- Execute → correct rows for id=5
- Re-bind with param value 10 → correct rows for id=10 (prepared stmt reuse)
- Describe: returns parameter types and result columns

**test_protocol_errors.rs**
- Syntax error: ErrorResponse with SQLSTATE, message, position
- Constraint violation: ErrorResponse with detail
- Transaction error: ReadyForQuery shows 'E' (failed txn state)
- After error in txn: only ROLLBACK/COMMIT accepted

**test_protocol_copy.rs**
- COPY FROM: bulk import CSV → all rows present
- COPY TO: bulk export → correct CSV output
- Custom delimiter: pipe-delimited import/export
- HEADER option: skip/include header row
- Error in COPY data: transaction rolled back

**test_protocol_types.rs**
- INTEGER → text format "123", binary format 4 bytes big-endian
- FLOAT → text format "1.23", binary format 8 bytes IEEE 754
- VARCHAR → text format, length-prefixed binary
- BOOLEAN → text "t"/"f", binary 1 byte
- NULL → special -1 length indicator
- TIMESTAMP → text ISO format, binary 8-byte microseconds

### Layer 10: Integration Tests

**test_sql_e2e.rs**
SQL file-driven tests. Each `.sql` file in `sql_tests/` contains queries
with expected results annotated as comments:
```sql
-- TEST: basic select
SELECT 1 + 2 AS result;
-- EXPECT: [(3)]

-- TEST: join with aggregate
SELECT d.name, COUNT(e.id)
FROM departments d LEFT JOIN employees e ON d.id = e.dept_id
GROUP BY d.name ORDER BY d.name;
-- EXPECT: [("Engineering", 5), ("Marketing", 3), ("Sales", 0)]
```

Run all `.sql` test files. Parse expected results. Assert match.

**test_concurrent_txns.rs**
- 10 threads, each running 100 transfer transactions (debit A, credit B).
  Assert: total balance unchanged (conservation of money).
- 8 threads performing mixed read/write: no dirty reads under snapshot isolation.
- Write-write conflict under SI: exactly one of conflicting txns aborted.
- Serializable: no write skew anomaly under SSI.
- High concurrency: 32 threads, 1000 txns each, database consistent at end.

**test_constraint_enforcement.rs**
- PRIMARY KEY: reject duplicate
- PRIMARY KEY: reject null
- FOREIGN KEY: reject dangling reference
- FOREIGN KEY CASCADE: delete parent → children deleted
- FOREIGN KEY SET NULL: delete parent → children FK set to null
- UNIQUE: reject duplicate (but allow multiple NULLs)
- CHECK: reject violating value
- NOT NULL: reject null insert and update-to-null

**test_views.rs**
- Query through view: correct results
- View with JOIN: correct join semantics
- Nested views: view referencing view
- DROP TABLE CASCADE: dependent view dropped
- Insert through simple view (single-table, no aggregates): succeeds (if supported)

**test_vacuum.rs**
- After delete + vacuum: dead tuples reclaimed
- Space reuse: insert after vacuum uses reclaimed space
- VACUUM on table with no dead tuples: no-op, no error
- Auto-vacuum trigger: after threshold dead tuples, auto-vacuum fires

**test_bulk_load.rs**
- COPY 100k rows: all present and correct
- COPY export + re-import: round-trip produces identical data
- COPY with type conversions: text → INTEGER, etc.
- Large COPY (1M rows): completes within reasonable time (< 30s)

**test_tpcc_subset.rs**
TPC-C-inspired transactional workload:
- New Order: insert order + order lines + update stock
- Payment: update warehouse + district + customer balances
- Order Status: read customer's latest order
- Run mix for 60 seconds with 4 warehouses.
- Verify: all invariants hold (warehouse balance = sum of payments,
  order line counts consistent, stock quantities non-negative).

**test_edge_cases.rs**
- Self-join: `SELECT * FROM t t1 JOIN t t2 ON t1.a = t2.b`
- 30-way join: parser and planner handle it (even if slow)
- Deeply nested subquery: 10 levels deep → parses and executes
- Expression with 1000 terms: `1+1+1+...` → correct result
- Column name conflicts: all disambiguated or errored
- Empty string vs NULL: `'' IS NOT NULL` → true
- Unicode: table/column names, string values with emoji
- Very long VARCHAR: 10MB string via TOAST
- MAX(INTEGER) + 1: overflow handled
- Divide by zero in CASE: `CASE WHEN x<>0 THEN 1/x ELSE 0 END` → no error when x=0
  (short-circuit evaluation)

---

## 16) Benchmark Suite

### bench_lexer.rs
- Tokenize 1MB of complex SQL (mix of DDL + DML + expressions).
- Target: > 50 MB/s throughput.

### bench_parser.rs
- Parse 10-way join with subqueries: < 100μs.
- Parse simple `SELECT a FROM t WHERE b = 1`: < 5μs.
- Parse 1000 simple INSERTs: < 10ms.

### bench_btree.rs
- Insert 1M random keys: throughput (keys/sec). Target: > 500k/sec.
- Lookup 100k random keys: throughput. Target: > 1M/sec.
- Range scan (1% selectivity on 1M keys): throughput.

### bench_buffer_pool.rs
- Random page access pattern (Zipfian, 1M accesses, 10k pages, 1k frame pool):
  hit ratio and throughput. Target: > 90% hit ratio for skew=1.0.
- Sequential scan (10k pages, 1k frame pool): throughput.

### bench_executor.rs
TPC-H-inspired queries (scale factor 1, ~1M rows in lineitem):
- Q1 (aggregation): < 500ms
- Q3 (3-table join + aggregate): < 1s
- Q5 (6-table join): < 2s
- Q6 (single-table scan + filter): < 200ms

### bench_optimizer.rs
- 4-table join plan generation: < 1ms
- 8-table join plan generation: < 50ms
- 12-table join plan generation: < 500ms

### bench_wal.rs
- Sequential WAL write throughput: target > 100 MB/s.
- Group commit: 100 concurrent txns/commit, measure fsync overhead.

### bench_transactions.rs
- Short read-write txns (1 read + 1 write) under contention:
  - 1 thread: > 50k txn/sec
  - 4 threads: > 100k txn/sec
  - 16 threads: measure contention overhead (target: > 50k txn/sec)

### bench_e2e.rs
- Simple SELECT latency (parse → result): < 200μs p50, < 1ms p99.
- INSERT latency: < 500μs p50, < 2ms p99.
- Complex query (3-table join): < 5ms p50, < 20ms p99.

---

## 17) Deliverables

Source code for:
- SQL lexer (tokenization, error reporting, all token types)
- SQL parser (Pratt + recursive descent, full AST, error recovery)
- Semantic analyzer/binder (name resolution, type checking, coercion, scope)
- Logical planner (AST → logical plan tree)
- Cost-based optimizer (7 rewrite rules + physical plan selection + join ordering)
- Cardinality estimator (histograms, MCVs, selectivity formulas)
- Volcano-model executor (12+ physical operators)
- Expression evaluator (compiled bytecode, all SQL functions)
- Buffer pool manager (LRU-K replacement, pin/unpin, dirty tracking)
- Disk manager (page-level file I/O)
- Heap file (slotted page, insert/delete/scan/fetch)
- B+ tree index (insert, delete, search, range scan, concurrent access)
- Hash index (extendible hashing, exact-match lookup)
- TOAST (oversized value storage)
- Free space map
- MVCC (visibility rules, snapshot management)
- Lock manager (row-level locking, deadlock detection)
- SSI (serializable snapshot isolation, rw-dependency tracking)
- Savepoints
- Write-ahead log (ARIES-style, physiological logging, group commit)
- Crash recovery (analysis, redo, undo, CLR)
- Checkpointing
- Catalog (system tables, statistics, ANALYZE)
- PostgreSQL wire protocol (simple + extended query, COPY, type serialization)
- TOML configuration with documented defaults
- Full test suite (unit + integration + TPC-C-inspired + edge cases + benchmarks)
- SQL test files with expected results

README.md including:
- Architecture overview (layered design, data flow from SQL to disk)
- How to build and run (`cargo build --release`, connection instructions)
- Supported SQL reference (data types, statements, functions)
- How to connect (psql, pgcli, any PG-compatible client)
- Storage engine internals (page layout, B+ tree structure, TOAST)
- Transaction model (MVCC, isolation levels, SSI explanation)
- WAL and recovery mechanics
- Query optimizer internals (rules, cost model, join ordering)
- Crafting system (3-tier recipe tree, duration, city specialization)
- Test suite description + `cargo test` / `cargo bench` instructions
- Performance tuning (buffer pool size, work_mem, statistics target)
- Configuration reference (all TOML fields documented)
- Known limitations / future ideas (window functions, CTEs, parallel query,
  partitioning, replication)