# Architecture Overview

sqld is a PostgreSQL-compatible SQL database engine written from scratch in Rust. It was built autonomously by an [attocode](https://attocode.com) swarm of 15 Claude agents in a single shot, producing approximately 26,500 lines of Rust, 1,529 tests, and only 3 external dependencies (`crc32fast`, `serde`, `toml`).

## Layer Diagram

```
Client (psql / driver)
    |
    v
+---------------------+
|   Protocol Layer    |  PostgreSQL v3 wire protocol        [WIRED]
|  (server/connection)|  simple + extended query + COPY
+---------------------+
|   SQL Parser        |  Pratt parser, 14 precedence levels [WIRED]
+---------------------+
|   Plan Builder      |  AST -> LogicalPlan                 [WIRED]
+---------------------+
|   Optimizer         |  9 optimization rules                [WIRED]
+---------------------+
|   Physical Planner  |  LogicalPlan -> PhysicalPlan        [WIRED]
+---------------------+
|   Executor          |  Volcano pull-based model            [WIRED]
+---------------------+
|   Storage Engine    |  Slotted pages, B+ tree, buffer pool [ISOLATED]
+---------------------+
|   Transaction Mgr   |  MVCC, SSI, lock manager            [ISOLATED]
+---------------------+
|   WAL Manager       |  Write-ahead log, crash recovery    [ISOLATED]
+---------------------+
```

**WIRED** layers are connected end-to-end in the server pipeline: a `psql` client can connect, send SQL, and get results back. **ISOLATED** layers are fully implemented and tested but not yet integrated into the server. The server currently uses an in-memory `HashMap`-based catalog (`MemoryCatalogProvider`) instead of the real storage stack.

## Directory Structure

```
src/
  sql/              Lexer, parser, AST, tokens
    lexer.rs          Hand-written lexer (610 lines)
    parser.rs         Pratt parser with 14 precedence levels
    token.rs          TokenKind enum (89 SQL keywords + operators)
    ast.rs            All AST node types
    error.rs          Lexer and parser error types

  planner/          Plan builder, optimizer, physical planner
    plan_builder.rs   AST -> LogicalPlan transformation
    logical_plan.rs   16 logical plan node types
    optimizer.rs      Rule-based optimizer (9 rules, 3 phases)
    rules/            Individual optimization rules
    physical_plan.rs  21 physical plan node types
    physical_planner.rs  LogicalPlan -> PhysicalPlan with cost-based choices
    cost_model.rs     Cost estimation (PostgreSQL-style cost constants)
    cardinality.rs    Cardinality estimation with selectivity formulas

  executor/         Volcano pull-based execution engine
    executor.rs       Executor trait (init/next/close) + CatalogProvider trait
    seq_scan.rs       Sequential scan operator
    index_scan.rs     Index scan operator
    filter.rs         Predicate filter operator
    project.rs        Projection operator
    hash_join.rs      Hash join operator
    sort_merge_join.rs  Sort-merge join operator
    nested_loop_join.rs  Nested loop join operator
    hash_aggregate.rs  Hash-based aggregation
    sort_aggregate.rs  Sort-based aggregation
    sort.rs           External sort operator
    limit.rs          Limit/offset operator
    distinct.rs       Hash and sort distinct operators
    set_ops.rs        Union, Intersect, Except operators
    values.rs         Literal row set operator
    modify.rs         INSERT, UPDATE, DELETE operators
    expr_eval.rs      Stack-based expression evaluator (bytecode compiler)
    scalar_functions.rs  39 scalar functions (string, math, date/time, type)

  storage/          Disk-oriented storage engine
    buffer_pool.rs    Buffer pool with LRU-K eviction (default 32768 frames)
    disk_manager.rs   Page-level file I/O (8 KB pages)
    page.rs           Slotted page format (24-byte header, CRC-16 checksum)
    heap_file.rs      Heap file (insert, delete, update, scan, vacuum)
    btree/            B+ tree index
      btree.rs          Core B+ tree (search, insert, delete, range scan)
      node.rs           Internal/leaf node format
      iterator.rs       Forward/backward range iterator
      concurrent.rs     Concurrent access support
    hash_index.rs     Extendible hash index
    toast.rs          TOAST (Oversized Attribute Storage Technique)
    free_space_map.rs Free space map for heap page allocation

  transaction/      Transaction management
    transaction.rs    Transaction struct, isolation levels, read/write sets
    mvcc.rs           MVCC visibility rules (PostgreSQL-style snapshot)
    ssi.rs            Serializable Snapshot Isolation (rw-dependency tracking)
    lock_manager.rs   Lock manager with deadlock detection
    savepoint.rs      Savepoint support (partial rollback)

  wal/              Write-ahead logging
    wal_manager.rs    WAL manager (append, flush, commit, read)
    wal_record.rs     WAL record types (13 record kinds + CLR)
    recovery.rs       ARIES-style 3-phase crash recovery
    checkpoint.rs     Fuzzy checkpointing

  protocol/         PostgreSQL wire protocol
    server.rs         TCP server, connection acceptance, thread-per-connection
    connection.rs     Per-connection state machine (startup, message loop)
    messages.rs       PG v3 frontend/backend message serialization
    simple_query.rs   Simple query protocol handler
    extended_query.rs Extended query protocol (Parse/Bind/Execute)
    copy.rs           COPY protocol (CSV import/export)

  types/            Type system (DataType, Datum, Schema, Column, Tuple)
  config.rs         Configuration (TOML-based)
  utils/            Error types, shared utilities
```

## Statistics

| Metric | Value |
|--------|-------|
| Lines of Rust | ~26,500 |
| Unit + integration tests | 1,529 |
| Integration test files | 74 |
| SQL test suites | 12 |
| External dependencies | 3 (`crc32fast`, `serde`, `toml`) |

## Data Flow

A query goes through the following stages:

1. **Protocol Layer** -- The server accepts a TCP connection speaking the PostgreSQL v3 wire protocol. The connection handler reads messages and dispatches to the simple query or extended query handler.

2. **SQL Parser** -- The query string is tokenized by the hand-written lexer, then parsed by the Pratt parser into an AST (`Statement` enum).

3. **Plan Builder** -- The AST is transformed into a `LogicalPlan` tree. SELECT becomes a pipeline of Scan, Filter, Aggregate, Sort, Project, Limit, Distinct nodes. INSERT/UPDATE/DELETE create modification nodes.

4. **Optimizer** -- The logical plan is rewritten by 9 optimization rules applied in sequence: constant folding, simplification, subquery decorrelation, view merging, predicate pushdown, projection pushdown, dead column elimination, join elimination, and join reorder.

5. **Physical Planner** -- The optimized logical plan is converted to a `PhysicalPlan` by choosing physical algorithms. The planner uses cost estimation to decide between HashJoin vs SortMergeJoin, HashAggregate vs SortAggregate, SeqScan vs IndexScan, etc.

6. **Executor** -- The physical plan is built into a tree of `Executor` trait objects following the Volcano pull-based model. The root operator's `next()` method is called repeatedly to produce tuples. Results are serialized into PostgreSQL wire protocol messages and sent back to the client.

## What Is Wired vs Isolated

The current server pipeline is:

```
psql -> Protocol -> Parser -> PlanBuilder -> Optimizer -> PhysicalPlanner -> Executor -> MemoryCatalogProvider
```

The `MemoryCatalogProvider` stores all table data in `HashMap<String, Vec<Tuple>>` in memory. This means:

- Data does not survive server restarts
- `BEGIN`/`COMMIT`/`ROLLBACK` toggle a `TransactionState` enum but do not use real MVCC
- The binder/semantic analyzer (implemented in Task 12) is bypassed -- SQL goes directly from parser to planner

The following subsystems are fully implemented and independently tested but not yet connected to the server:

- **Storage engine** -- Buffer pool, disk manager, heap files, B+ tree, hash index, TOAST, free space map
- **Transaction manager** -- MVCC visibility, SSI, lock manager with deadlock detection, savepoints
- **WAL** -- Write-ahead logging, ARIES recovery, fuzzy checkpoints
- **Binder** -- Semantic analysis (name resolution, type checking)

Wiring these together -- replacing `MemoryCatalogProvider` with the real storage stack -- is the primary remaining work. See [Limitations & Roadmap](../limitations-and-roadmap.md) for details.
