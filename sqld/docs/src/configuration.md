# Configuration

sqld is configured via a TOML file passed as a command-line argument:

```bash
./target/release/sqld sqld_config.toml
```

Every section is optional. When a section or key is omitted, sqld falls back to built-in defaults. The file is parsed at startup by the `Config::from_file` function in `src/config.rs`.

There are eight configuration sections:

1. [server](#server)
2. [storage](#storage)
3. [buffer_pool](#buffer_pool)
4. [wal](#wal)
5. [optimizer](#optimizer)
6. [transactions](#transactions)
7. [vacuum](#vacuum)
8. [logging](#logging)

---

## `[server]`

Network listener settings.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `host` | String | `"127.0.0.1"` | IP address the server binds to. Use `"0.0.0.0"` to listen on all interfaces. |
| `port` | u16 | `5433` | TCP port for the PostgreSQL wire protocol listener. |
| `max_connections` | usize | `128` | Maximum number of concurrent client connections. |

---

## `[storage]`

Disk storage settings for the page-oriented storage manager.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `data_dir` | String | `"data"` | Directory where database files (heap files, index files) are stored. Relative paths are resolved from the working directory. |
| `page_size` | usize | `8192` | Size of a single disk page in bytes. Changing this after data has been written requires a full rebuild. |

---

## `[buffer_pool]`

In-memory page cache that sits between the executor and the storage layer.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `pool_size` | usize | `1024` | Number of pages held in the buffer pool. Total memory usage is approximately `pool_size * page_size` bytes. |
| `eviction_policy` | String | `"lru-k"` | Page eviction algorithm. Currently only `"lru-k"` is supported. |
| `lru_k` | usize | `2` | The K parameter for LRU-K eviction. Higher values give more weight to historical access frequency over recency. |

---

## `[wal]`

Write-ahead log for crash recovery (ARIES-style).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `true` | Whether WAL logging is active. Disabling this removes durability guarantees but can speed up bulk loads. |
| `checkpoint_interval_secs` | u64 | `300` | Time in seconds between automatic checkpoints. A checkpoint flushes dirty pages and truncates the log. |
| `max_wal_size` | u64 | `67108864` (64 MiB) | Maximum WAL size in bytes before a forced checkpoint is triggered, regardless of the time interval. |

---

## `[optimizer]`

Query optimizer behavior.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enable_cost_based` | bool | `true` | Enable the cost-based optimizer. When disabled, the optimizer falls back to rule-based heuristics. |
| `statistics_sample_size` | usize | `1000` | Number of rows sampled from each table to estimate selectivity and cardinality for cost calculations. |

---

## `[transactions]`

Concurrency control and transaction settings.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `isolation_level` | String | `"snapshot"` | Default isolation level for transactions. Supported values: `"snapshot"` (snapshot isolation) and `"serializable"` (serializable snapshot isolation / SSI). |
| `lock_timeout_ms` | u64 | `5000` | Maximum time in milliseconds a transaction will wait to acquire a lock before aborting with a timeout error. |
| `deadlock_detection_interval_ms` | u64 | `1000` | How often (in milliseconds) the deadlock detector runs its cycle-detection algorithm on the wait-for graph. |

---

## `[vacuum]`

Dead tuple reclamation.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `auto_vacuum` | bool | `true` | Enable automatic vacuuming. When enabled, sqld periodically reclaims space from deleted or updated tuples. |
| `vacuum_threshold` | f64 | `0.2` | Fraction of dead tuples (0.0 -- 1.0) in a table before auto-vacuum is triggered. For example, `0.2` means vacuum runs when 20% or more of the tuples in a table are dead. |

---

## `[logging]`

Diagnostic logging.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `level` | String | `"info"` | Log verbosity level. One of: `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"`. |
| `file` | String | `""` (empty) | Path to a log file. When empty, logs are written to stderr. |

---

## Full Example

The following is the complete default configuration file shipped with sqld (`sqld_config.toml`):

```toml
# sqld configuration file
# All sections are optional; defaults are used when omitted.

[server]
host = "127.0.0.1"
port = 5433
max_connections = 100

[storage]
data_dir = "data"
page_size = 8192

[buffer_pool]
pool_size = 1024
eviction_policy = "lru-k"
lru_k = 2

[wal]
enabled = true
checkpoint_interval_secs = 300
max_wal_size = 67108864   # 64 MiB

[optimizer]
enable_cost_based = true
statistics_sample_size = 1000

[transactions]
isolation_level = "snapshot"
lock_timeout_ms = 5000
deadlock_detection_interval_ms = 1000

[vacuum]
auto_vacuum = true
vacuum_threshold = 0.2

[logging]
level = "info"
file = ""
```

> **Tip:** You can start with an empty file and only override the settings you need. All omitted keys revert to their defaults.
