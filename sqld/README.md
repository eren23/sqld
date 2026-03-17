# sqld

**A PostgreSQL-compatible SQL database written from scratch in Rust.**

> 26,500 lines of Rust | 1,529 tests passing | 3 external dependencies | Built by a swarm of AI agents

[Documentation](https://eren23.github.io/sqld/) | [Getting Started](https://eren23.github.io/sqld/getting-started.html) | [SQL Reference](https://eren23.github.io/sqld/sql-reference/statements.html) | [Architecture](https://eren23.github.io/sqld/architecture/overview.html)

---

![sqld demo](demo.gif)

---

## Highlights

| Metric | Value |
|--------|-------|
| Swarm Tasks Completed | **14 / 15** |
| Lines of Rust | **~26,500** |
| Tests Passing | **1,529** |
| External Dependencies | **3** (`crc32fast`, `serde`, `toml`) |

## Features

| Layer | Implementation |
|-------|---------------|
| **SQL Frontend** | Hand-written lexer + Pratt parser (14 precedence levels) |
| **Query Engine** | Logical planner, cost-based optimizer (9 rules), physical planner |
| **Executor** | Volcano pull-based model, 12+ operators (hash/sort-merge/nested-loop join, aggregation, external sort, set ops) |
| **Storage** | Buffer pool (LRU-K), heap files (slotted pages), B+ tree & hash indexes, TOAST `[tested in isolation]` |
| **Transactions** | MVCC, SSI, lock manager, deadlock detection, savepoints `[tested in isolation]` |
| **WAL** | Write-ahead logging, ARIES recovery, checkpoints `[tested in isolation]` |
| **Protocol** | PostgreSQL v3 wire protocol (simple + extended query, COPY) |

## Quick Start

```bash
cargo build --release
./target/release/sqld sqld_config.toml
psql -h 127.0.0.1 -p 5433 -U sqld
```

The server listens on port 5433 by default. Connect with any PostgreSQL client.

## Architecture

```
Client (psql / driver)
    |
    v
+---------------------+
|   Protocol Layer    |  PostgreSQL v3 wire protocol
|  (server/connection)|  simple + extended query
+---------------------+
|   SQL Parser        |  Pratt parser, 14 precedence levels
+---------------------+
|   Plan Builder      |  AST -> LogicalPlan
+---------------------+
|   Optimizer         |  9 optimization rules
+---------------------+
|   Physical Planner  |  LogicalPlan -> PhysicalPlan
+---------------------+
|   Executor          |  Volcano pull-based model
+---------------------+  - - - - - - - - - - - - -
|   Storage Engine    |  Slotted pages, B+ tree,
+---------------------+  buffer pool (not yet wired)
|   Transaction Mgr   |  MVCC, SSI, lock manager
+---------------------+  (not yet wired)
|   WAL Manager       |  Write-ahead log, crash
+---------------------+  recovery (not yet wired)
```

Layers above the dashed line are fully wired into the server's query pipeline. Layers below are implemented and tested in isolation but not yet integrated — the server currently uses an in-memory catalog provider.

## How It Was Built

sqld was built autonomously by an [attocode](https://attocode.com) swarm of 15 Claude agents in a single shot, starting from an empty directory. Each agent was responsible for a distinct layer of the database stack. 14 of 15 tasks completed successfully; the final integration task (wiring all layers + writing integration tests) was too broad and failed after multiple timeouts. A single post-swarm Claude session then debugged and fixed three runtime bugs to bring the system to a working state.

Read the full story in the [documentation](https://eren23.github.io/sqld/how-it-was-built.html).

## Post-Swarm Bug Fixes

Three bugs were found and fixed in a single debugging session: an UPDATE deadlock from re-entrant catalog locking, a projection pushdown column mismatch in the optimizer, and a LEFT JOIN + ORDER BY column resolution issue. Details in the [documentation](https://eren23.github.io/sqld/how-it-was-built.html).

## Known Limitations

- **In-memory only** — data does not survive restarts (storage engine is implemented but not wired in)
- **Unwired layers** — storage, transaction manager, and binder are tested in isolation but not integrated into the query pipeline
- **No auth** — no authentication or authorization
- **No-op statements** — `ANALYZE`, `VACUUM`, `ALTER TABLE` parse but do nothing
- **No CHECK / FOREIGN KEY constraints**

Full details on the [limitations page](https://eren23.github.io/sqld/known-limitations.html).

## Testing

```bash
# Run all 1,529 tests
cargo test

# Specific suites
cargo test --test test_sql_e2e         # End-to-end SQL tests
cargo test --test test_tpcc_subset     # TPC-C workload tests
cargo test --test test_protocol_simple # Protocol tests
cargo test --test test_concurrent_txns # Concurrency tests
cargo test --lib                       # Unit tests
```

## Documentation

Full documentation is available at [eren23.github.io/sqld](https://eren23.github.io/sqld/).

## License

MIT
