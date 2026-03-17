# Protocol

The protocol layer implements the PostgreSQL v3 wire protocol, allowing standard PostgreSQL clients (`psql`, libpq-based drivers, JDBC, etc.) to connect and execute queries.

Source files:
- `src/protocol/server.rs` -- TCP server, connection acceptance
- `src/protocol/connection.rs` -- Per-connection state machine
- `src/protocol/messages.rs` -- PG v3 message serialization/deserialization
- `src/protocol/simple_query.rs` -- Simple query protocol handler
- `src/protocol/extended_query.rs` -- Extended query protocol (Parse/Bind/Execute)
- `src/protocol/copy.rs` -- COPY protocol support

## Server

The server (`src/protocol/server.rs`) listens on a TCP port (default 5433) and spawns a thread per connection.

### Architecture

```rust
pub struct Server {
    config: Config,
    catalog: Arc<Mutex<Catalog>>,
    catalog_provider: Arc<dyn CatalogProvider>,
    active_connections: Arc<AtomicUsize>,
    next_process_id: Arc<AtomicI32>,
    shutdown: Arc<AtomicBool>,
}
```

The server:

1. Binds to `host:port` from the configuration.
2. Accepts incoming TCP connections in a loop.
3. Checks the connection limit (`max_connections`, default 100). Rejects connections that exceed the limit.
4. Assigns a unique `process_id` to each connection.
5. Spawns a new thread that creates a `Connection` and runs its message loop.
6. Tracks active connection count via an `AtomicUsize`.

The `shutdown()` method sets an atomic flag to stop accepting new connections.

## Connection

Each connection (`src/protocol/connection.rs`) manages the full lifecycle from startup handshake through query execution.

### Session State

The `Session` struct holds per-connection state:

- `user` and `database` -- From the startup message
- `txn_state` -- `Idle` ('I'), `InBlock` ('T'), or `Failed` ('E')
- `catalog` and `catalog_provider` -- Shared references to the planner catalog and executor storage
- `prepared_statements` -- Named prepared statements (extended query protocol)
- `portals` -- Named portals (bound prepared statements ready for execution)
- `process_id` and `secret_key` -- For cancel request identification
- `params` -- Session parameters (server_version, encoding, DateStyle, TimeZone, etc.)

### Startup Handshake

1. Read the startup message (4-byte length + 4-byte version).
2. If version is `SSL_REQUEST_CODE` (80877103), respond with `'N'` (SSL not supported) and re-read.
3. If version is `CANCEL_REQUEST_CODE` (80877102), handle cancellation.
4. For normal startup (version 3.0 = 196608), extract the `user` and `database` parameters.
5. Send `AuthenticationOk`.
6. Send `ParameterStatus` messages for all session parameters.
7. Send `BackendKeyData` with process_id and secret_key.
8. Send `ReadyForQuery` with transaction state `'I'` (idle).

### Message Loop

After startup, the connection enters a message loop:

1. Read a frontend message (1-byte tag + 4-byte length + payload).
2. Dispatch based on the message tag:
   - `'Q'` -- Simple query: delegate to `simple_query::handle_simple_query()`
   - `'P'` -- Parse: delegate to `extended_query::handle_parse()`
   - `'B'` -- Bind: delegate to `extended_query::handle_bind()`
   - `'D'` -- Describe: delegate to `extended_query::handle_describe()`
   - `'E'` -- Execute: delegate to `extended_query::handle_execute()`
   - `'C'` -- Close: close a prepared statement or portal
   - `'S'` -- Sync: send `ReadyForQuery`
   - `'X'` -- Terminate: clean exit
3. Send response messages back to the client.
4. Loop until terminate or error.

## Simple Query Protocol

The simple query handler (`src/protocol/simple_query.rs`) processes a `'Q'` message containing one or more SQL statements.

For each statement:

1. **Parse** -- Tokenize and parse the SQL into an AST using the SQL frontend.
2. **Handle special statements** -- `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `SHOW TABLES`, `SHOW COLUMNS`, `EXPLAIN` are handled directly without going through the full planner/executor pipeline.
3. **Plan** -- Lock the catalog, create a `PlanBuilder`, and transform the AST into a `LogicalPlan`.
4. **Optimize** -- Apply the 9-rule optimizer.
5. **Physical plan** -- Convert to a `PhysicalPlan` via the physical planner.
6. **Execute** -- Build an executor tree, call `init()`, pull all tuples via `next()`, call `close()`.
7. **Serialize results** -- Convert tuples to PG wire protocol messages:
   - `RowDescription` -- Column names and types
   - `DataRow` -- One per result row, with values in text format
   - `CommandComplete` -- "SELECT N", "INSERT 0 N", "UPDATE N", "DELETE N"
8. **ReadyForQuery** -- Sent after all statements complete.

Error handling: if any step fails, an `ErrorResponse` message is sent. In a transaction block, the state transitions to `Failed`, and subsequent statements receive errors until `ROLLBACK`.

## Extended Query Protocol

The extended query handler (`src/protocol/extended_query.rs`) supports prepared statements with parameter placeholders (`$1`, `$2`, etc.).

### Prepared Statements

```rust
pub struct PreparedStatement {
    pub name: String,
    pub query: String,
    pub param_types: Vec<DataType>,
    pub statements: Vec<Statement>,
    pub result_schema: Option<Schema>,
}
```

### Portals

```rust
pub struct Portal {
    pub name: String,
    pub statement_name: String,
    pub param_values: Vec<Datum>,
    pub result_formats: Vec<i16>,
    pub statements: Vec<Statement>,
    pub result_schema: Option<Schema>,
}
```

### Message Flow

1. **Parse** (`'P'`) -- Parse the query text. Parameter type OIDs from the client are mapped to sqld's `DataType` enum. The parsed AST and parameter types are stored as a `PreparedStatement`.

2. **Bind** (`'B'`) -- Bind parameter values to a prepared statement, creating a `Portal`. Parameter values are deserialized from their wire format. The `$N` placeholders in the AST are substituted with the provided values.

3. **Describe** (`'D'`) -- Return metadata about a prepared statement or portal:
   - For statements: `ParameterDescription` (parameter types) + `RowDescription` (result columns)
   - For portals: `RowDescription` (result columns)

4. **Execute** (`'E'`) -- Execute a portal. The query goes through the same plan/optimize/execute pipeline as simple query. An optional row limit can be specified.

5. **Close** (`'C'`) -- Destroy a named prepared statement or portal.

6. **Sync** (`'S'`) -- Send `ReadyForQuery`, completing the extended query cycle.

## COPY Protocol

The COPY handler (`src/protocol/copy.rs`) supports bulk data import and export.

### COPY FROM (Import)

Reads CSV data from the client:

1. Send `CopyInResponse` message with format and column count.
2. Receive `CopyData` messages containing CSV rows.
3. Parse each CSV row, convert values to the target column types.
4. Insert tuples via the catalog provider.
5. Receive `CopyDone` to complete, or `CopyFail` to abort.

### COPY TO (Export)

Sends CSV data to the client:

1. Scan the source table.
2. Send `CopyOutResponse` message with format and column count.
3. For each tuple, format values as CSV and send as a `CopyData` message.
4. Send `CopyDone` to complete.

### Options

```rust
pub struct CopyOptions {
    pub delimiter: u8,       // Default: ','
    pub has_header: bool,    // Default: false
    pub null_string: String, // Default: ""
    pub format: CopyFormat,  // Csv or Text
}
```

## Message Types

The messages module (`src/protocol/messages.rs`) implements serialization and deserialization for all PG v3 message types.

### Frontend Messages (Client to Server)

| Tag | Message | Description |
|-----|---------|-------------|
| (none) | Startup | Initial connection with version and parameters |
| `'Q'` | Query | Simple query with SQL text |
| `'P'` | Parse | Prepare a named statement |
| `'B'` | Bind | Bind parameters to create a portal |
| `'D'` | Describe | Request metadata |
| `'E'` | Execute | Execute a portal |
| `'C'` | Close | Close statement/portal |
| `'S'` | Sync | Synchronization point |
| `'X'` | Terminate | Close connection |
| `'d'` | CopyData | COPY data row |
| `'c'` | CopyDone | COPY complete |
| `'f'` | CopyFail | COPY abort |

### Backend Messages (Server to Client)

| Tag | Message | Description |
|-----|---------|-------------|
| `'R'` | Authentication | Auth request/response (Ok, CleartextPassword, etc.) |
| `'K'` | BackendKeyData | Process ID and secret key |
| `'S'` | ParameterStatus | Session parameter key-value pair |
| `'Z'` | ReadyForQuery | Transaction state indicator |
| `'T'` | RowDescription | Column metadata for result set |
| `'D'` | DataRow | A single result row |
| `'C'` | CommandComplete | Statement completion tag |
| `'E'` | ErrorResponse | Error with severity, code, message |
| `'N'` | NoticeResponse | Non-fatal notice |
| `'I'` | EmptyQueryResponse | Empty query string |
| `'1'` | ParseComplete | Parse step succeeded |
| `'2'` | BindComplete | Bind step succeeded |
| `'3'` | CloseComplete | Close step succeeded |
| `'n'` | NoData | No result set |
| `'t'` | ParameterDescription | Parameter types for prepared statement |
| `'G'` | CopyInResponse | Ready to receive COPY data |
| `'H'` | CopyOutResponse | About to send COPY data |
| `'d'` | CopyData | COPY data chunk |
| `'c'` | CopyDone | COPY complete |

### Transaction State

The `ReadyForQuery` message includes a single byte indicating the transaction state:

- `'I'` -- Idle (not in a transaction)
- `'T'` -- In a transaction block
- `'E'` -- In a failed transaction (commands rejected until ROLLBACK)

### Error Fields

Error responses include structured fields following the PostgreSQL convention:

- Severity (ERROR, FATAL, PANIC, WARNING, NOTICE)
- SQLSTATE 5-character code
- Human-readable message
- Optional: detail, hint, position (byte offset in query)
