# How It Was Built

sqld was built autonomously by an [attocode](https://attocode.com) swarm of 15 Claude agents in a single shot, starting from an empty directory. This page tells the story of that build.

## The Swarm

The attocode swarm consisted of 15 Claude agents working in parallel on separate tasks. Each agent was given a task description specifying what to implement and was expected to produce working, tested Rust code.

### Task Breakdown

The 15 tasks covered the complete database stack:

| Task | Component | Status |
|------|-----------|--------|
| 1 | Lexer and tokenizer | Completed |
| 2 | Parser (Pratt parsing, recursive descent) | Completed |
| 3 | AST node types | Completed |
| 4 | Type system (DataType, Datum, Schema, Tuple) | Completed |
| 5 | Buffer pool with LRU-K eviction | Completed |
| 6 | Disk manager, slotted pages, heap files | Completed |
| 7 | B+ tree index | Completed |
| 8 | Hash index, TOAST, free space map | Completed |
| 9 | WAL manager, records, ARIES recovery, checkpoints | Completed |
| 10 | MVCC, SSI, lock manager with deadlock detection | Completed |
| 11 | Plan builder, logical plan, optimizer (9 rules) | Completed |
| 12 | Semantic analyzer / binder | Completed |
| 13 | Physical planner, cost model, cardinality estimation | Completed |
| 14 | Executor (Volcano model, all operators, expression evaluator) | Completed |
| 15 | Protocol, server integration, integration tests | **Failed** |

### Results

- **14 out of 15 tasks completed successfully** in a single shot
- The completed tasks produced approximately **26,500 lines of Rust** from an empty directory
- **1,529 tests** were written, all passing after post-swarm fixes
- Only **3 external dependencies** were used: `crc32fast`, `serde`, `toml`

## The Failed Task

**Task 15** -- protocol/server integration, the final glue layer -- **failed all 5 attempts**:

- 3 attempts timed out at 600 seconds each
- 2 attempts ended in agent crashes

This task was the largest and most complex: it needed to wire all layers together (protocol server, query pipeline, catalog integration), write all integration tests, generate the README, and produce the configuration system.

The task description was too broad. It should have been split into 3-4 smaller tasks:

1. Protocol message serialization + server framework
2. Simple query handler (parse, plan, execute pipeline)
3. Extended query handler (prepared statements)
4. Integration tests + configuration

Despite Task 15's failure, partial protocol and server code was written by other tasks, and the post-swarm debugging session was able to complete the integration.

## Post-Swarm Debugging

A single Claude session fixed 3 bugs found during integration testing:

### Bug 1: UPDATE Deadlock

**Symptom**: UPDATE queries hung indefinitely.

**Cause**: `execute_query_plan()` in `simple_query.rs` held the catalog `Mutex` lock while calling `executor.init()`. The `SeqScanExecutor::init()` called `CatalogProvider::table_schema()` through `scan_table()`, which attempted to re-acquire the same non-reentrant `Mutex`. This caused a deadlock.

**Fix**: Drop the catalog lock after planning but before execution. The catalog lock is only needed during the planning phase (for schema lookup); the executor accesses data through `CatalogProvider` (which uses its own internal locking).

**File**: `src/protocol/simple_query.rs`

### Bug 2: Projection Pushdown Column Mismatch

**Symptom**: Queries returning wrong data after optimization.

**Cause**: The optimizer's `ProjectionPushdown` rule narrowed the `Scan` node's schema (e.g., from `[id, name, age]` to `[id, name]`), but the `SeqScanExecutor` still returned full tuples from storage with all columns. Column indices compiled against the narrowed schema pointed to wrong positions in the actual tuple.

**Fix**: Do not narrow scan node schemas in the projection pushdown rule. The scan always returns full tuples; projection is handled by the Project operator above it.

**File**: `src/planner/rules/projection_pushdown.rs`

### Bug 3: LEFT JOIN + ORDER BY Column Resolution

**Symptom**: `ORDER BY t.column` failed with "column not found" on queries with LEFT JOIN.

**Cause**: The plan builder applied `PROJECT` before `SORT`. When `ORDER BY` referenced a column not in the `SELECT` list, the column was no longer available after projection.

**Fix**: Reorder the plan builder to apply `SORT` before `PROJECT`. This ensures all source columns are available for ordering, and the final projection narrows the output afterward.

**File**: `src/planner/plan_builder.rs`

### Cleanup

28 compiler warnings were cleaned up across 12 source files: unused imports, unused variables, and dead code.

## Architecture Decisions

Several architectural decisions shaped the codebase:

**Hand-written lexer and parser** -- No parser generators (pest, nom, lalrpop) were used. The hand-written Pratt parser provides clear, debuggable code and complete control over error messages and precedence handling. The 14-level binding power table is defined as simple constants.

**Volcano pull-based execution** -- The classic iterator model was chosen for its simplicity and composability. Every operator implements the same 3-method trait (`init`/`next`/`close`), making it straightforward for each task's agent to implement operators independently.

**ARIES-style WAL** -- The WAL uses the industry-standard ARIES protocol with physiological logging, CLR records, and 3-phase recovery. This provides a solid foundation for crash recovery once the storage engine is wired into the server.

**PostgreSQL wire protocol** -- Rather than inventing a custom protocol, implementing the PG v3 wire protocol means any PostgreSQL client can connect immediately. This dramatically simplifies testing and makes the database usable from day one.

**Minimal dependencies** -- Only 3 external crates were used. The CRC-16 checksum for pages is hand-implemented. The random number generator is a xorshift64 PRNG. Date/time calculations use the Howard Hinnant date algorithm. This keeps the dependency tree tiny and compile times fast.

## What Made It Work

The attocode swarm succeeded because:

1. **Clear task boundaries** -- Each of the 14 successful tasks had a well-defined scope with clear inputs and outputs. The lexer produces tokens, the parser produces an AST, the plan builder produces a logical plan, and so on.

2. **Trait-based interfaces** -- Key interfaces like `Executor`, `CatalogProvider`, `OptimizationRule`, `PageStore`, and `DirtyPageFlusher` allowed agents to write code against abstract interfaces without needing the concrete implementations from other tasks.

3. **Independent testability** -- Each component is testable in isolation. The storage engine tests create their own buffer pools and disk managers. The optimizer tests create mock catalogs. The executor tests use in-memory catalog providers.

## What Could Be Improved

1. **Task 15 should have been split** -- The integration task was too broad. Smaller tasks with clearer deliverables would likely have succeeded.

2. **The binder should be wired in** -- Task 12 produced a semantic analyzer, but it is bypassed in the current pipeline. SQL goes directly from parser to planner, which means some semantic errors are caught late or not at all.

3. **Storage integration is the main gap** -- The most impactful next step is replacing `MemoryCatalogProvider` with a real storage-backed implementation, connecting the buffer pool, heap files, indexes, WAL, and transaction manager into the live query pipeline.
