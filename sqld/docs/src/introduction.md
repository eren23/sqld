# sqld

**A PostgreSQL-compatible SQL database written from scratch in Rust.**

<div align="center">

![sqld demo](demo.gif)

</div>

sqld is a fully functional SQL database built entirely from the ground up by an [attocode](https://attocode.com) swarm of 15 Claude AI agents working in parallel. No fork, no embedded SQLite, no third-party query engine -- every layer from the wire protocol down to the page-level storage manager was written from scratch.

## At a Glance

| Metric | Value |
|--------|-------|
| Swarm tasks completed | 14 / 15 |
| Lines of code | ~26,500 |
| Test count | 1,529 |
| External dependencies | 3 (`crc32fast`, `serde`, `toml`) |

## What sqld Implements

sqld covers the full stack of a relational database:

- **SQL Frontend** -- A hand-written Pratt parser that handles DDL, DML, queries with joins, subqueries, aggregates, and more.
- **Cost-Based Query Optimizer** -- Estimates selectivity and cost to choose between sequential scans, index scans, and join orderings.
- **Volcano Executor** -- A pull-based iterator model that evaluates query plans one tuple at a time, supporting projections, filters, joins (nested-loop, hash, sort-merge), aggregation, sorting, and limits.
- **Buffer Pool Manager** -- An LRU-K page cache that mediates all access between the executor and disk, with pin/unpin semantics and dirty-page tracking.
- **B+ Tree and Hash Indexes** -- Persistent index structures for point lookups and range scans.
- **MVCC with Serializable Snapshot Isolation (SSI)** -- Multi-version concurrency control that allows concurrent readers and writers without blocking, with SSI-level conflict detection.
- **Write-Ahead Log (WAL)** -- ARIES-style logging with physiological redo/undo records, checkpointing, and crash recovery.
- **PostgreSQL Wire Protocol** -- Speaks the PostgreSQL v3 frontend/backend protocol, so any standard `psql` client can connect out of the box.

> **Note:** The storage engine, transaction manager, and WAL subsystems are individually implemented and tested in isolation, but they are not yet wired into the live server pipeline. The server currently uses an in-memory catalog and executor. Integrating the durable storage stack is the remaining work.

## Where to Go Next

- **[Getting Started](getting-started.md)** -- Build sqld, start the server, and run your first queries in under five minutes.
- **[SQL Reference](sql-reference/statements.md)** -- Complete reference for every SQL statement, expression, and data type that sqld supports.
- **[Architecture](architecture/overview.md)** -- Deep dive into the internals: parser, optimizer, executor, buffer pool, indexes, MVCC, and WAL.
- **[Configuration](configuration.md)** -- All configuration knobs explained, with defaults and examples.
