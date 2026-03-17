# SQL Frontend

The SQL frontend consists of a hand-written lexer and Pratt parser that convert SQL text into a typed AST. No parser generators or external grammar tools are used.

Source files:
- `src/sql/lexer.rs` -- Tokenizer
- `src/sql/token.rs` -- Token and TokenKind types
- `src/sql/parser.rs` -- Pratt parser
- `src/sql/ast.rs` -- AST node types
- `src/sql/error.rs` -- Error types

## Lexer

The lexer (`src/sql/lexer.rs`) is a hand-written, single-pass scanner that converts a SQL source string into a stream of tokens. It operates on raw bytes for performance, with UTF-8 handling only where necessary (string literals, error messages).

### Tokenization

The main `next_token()` method dispatches on the current byte:

1. **Whitespace and comments** are skipped before each token. The lexer supports:
   - Single-line comments (`-- ...`)
   - Nestable block comments (`/* ... /* ... */ ... */`)

2. **Identifiers and keywords** -- Sequences of `[a-zA-Z_][a-zA-Z0-9_]*` are scanned, then looked up in a keyword table (`lookup_keyword()`). If found, the token kind is the keyword variant; otherwise it is `Identifier`. Lookup is case-insensitive (the text is lowercased before matching).

3. **Numeric literals** -- Integer and float literals, including:
   - Decimal integers (`42`)
   - Hex literals (`0xFF`)
   - Floating point with decimal point (`3.14`)
   - Scientific notation (`1.5e10`, `2E-3`)

4. **String literals** -- Three forms:
   - Standard SQL strings (`'hello'`) with `''` escape for embedded quotes
   - E-strings (`E'tab\there'`) with C-style backslash escapes
   - Quoted identifiers (`"Column Name"`) with `""` escape

5. **Placeholders** -- `$N` parameter references for the extended query protocol (`$1`, `$2`, etc.)

6. **Operators and punctuation** -- Longest-match disambiguation for multi-character operators:
   - `<>` and `!=` both produce `NotEq`
   - `<=`, `>=` produce `LtEq`, `GtEq`
   - `||` produces `Concat`
   - `::` produces `ColonColon` (type cast)
   - Single-character operators: `+`, `-`, `*`, `/`, `%`, `^`, `=`, `<`, `>`, `(`, `)`, `,`, `;`, `.`

### Token Types

The `TokenKind` enum (`src/sql/token.rs`) has 228 variants organized into:

| Category | Count | Examples |
|----------|-------|---------|
| Literals | 5 | `IntegerLiteral`, `FloatLiteral`, `StringLiteral`, `BooleanLiteral`, `NullLiteral` |
| Identifiers | 2 | `Identifier`, `QuotedIdentifier` |
| SQL keywords | 89 | `KwSelect`, `KwFrom`, `KwWhere`, `KwInsert`, `KwJoin`, `KwBegin`, etc. |
| Operators | 13 | `Plus`, `Minus`, `Star`, `Eq`, `NotEq`, `Lt`, `Gt`, `Concat`, etc. |
| Punctuation | 6 | `LeftParen`, `RightParen`, `Comma`, `Semicolon`, `Dot`, `ColonColon` |
| Special | 3 | `Placeholder`, `Eof`, `Error` |

Each `Token` carries its `TokenKind`, a `Span` (byte offset range into the source), and the line/column position for error reporting.

### Error Handling

The lexer collects errors without stopping, up to a configurable `max_errors` limit (default 100). This allows the parser to see a complete token stream even when the input has minor lexical issues. Error tokens are emitted as `TokenKind::Error`.

## Parser

The parser (`src/sql/parser.rs`) uses a combination of recursive descent for statements and Pratt parsing for expressions.

### Pratt Parsing with 14 Precedence Levels

Expressions are parsed using Pratt (top-down operator precedence) parsing with 14 binding power levels:

| Level | BP | Operators |
|-------|-----|-----------|
| 1 | 10 | `OR` |
| 2 | 20 | `AND` |
| 3 | 30 | `NOT` (prefix) |
| 4 | 40 | `IS NULL`, `IS NOT NULL` |
| 5 | 50 | `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=` |
| 6 | 60 | `BETWEEN`, `IN`, `LIKE`, `ILIKE` |
| 7 | 70 | `||` (concatenation) |
| 8 | 80 | `+`, `-` (addition) |
| 9 | 90 | `*`, `/`, `%` (multiplication) |
| 10 | 100 | `^` (exponentiation) |
| 11 | 110 | Unary `+`, `-` (prefix) |
| 12 | 120 | `::` (type cast) |
| 13 | 130 | `.` (field access) |
| 14 | 140 | Function call `()` |

The parser implements `parse_expr(min_bp)` which:
1. Parses a **prefix** (nud) -- literals, identifiers, unary operators, parenthesized expressions, `CASE`, `CAST`, `EXISTS`, function calls, subqueries
2. Loops to parse **infix** (led) operators as long as the operator's binding power exceeds `min_bp`

### Statement Parsing

Top-level statements are parsed by recursive descent. The parser dispatches on the first token:

- `SELECT` -- Full SELECT with `DISTINCT`, column list, `FROM` with joins, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY` (with `NULLS FIRST/LAST`), `LIMIT`, `OFFSET`, set operations (`UNION`/`INTERSECT`/`EXCEPT`)
- `INSERT` -- `INSERT INTO table (cols) VALUES (...)` and `INSERT INTO ... SELECT ...`
- `UPDATE` -- `UPDATE table SET col = expr WHERE ...` with `RETURNING`
- `DELETE` -- `DELETE FROM table WHERE ...` with `RETURNING`
- `CREATE TABLE` -- Column definitions with data types, column constraints (`NOT NULL`, `DEFAULT`, `PRIMARY KEY`, `UNIQUE`, `CHECK`, `REFERENCES`), and table constraints
- `CREATE INDEX` -- With optional `USING btree/hash` method
- `CREATE VIEW` -- With optional column list
- `DROP TABLE/INDEX/VIEW` -- With `IF EXISTS` and `CASCADE`
- `ALTER TABLE` -- `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, `ADD CONSTRAINT`, `DROP CONSTRAINT`
- `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`
- `EXPLAIN [ANALYZE]`
- `SHOW TABLES`, `SHOW COLUMNS`
- `ANALYZE`, `VACUUM`
- `COPY table FROM/TO 'path'`

### Join Parsing

The parser recognizes all standard join types:

- `INNER JOIN ... ON ...`
- `LEFT [OUTER] JOIN ... ON ...`
- `RIGHT [OUTER] JOIN ... ON ...`
- `FULL [OUTER] JOIN ... ON ...`
- `CROSS JOIN ...`
- `NATURAL JOIN ...`
- `... JOIN ... USING (col1, col2)`
- Subqueries in the `FROM` clause: `(SELECT ...) AS alias`

## AST

The AST (`src/sql/ast.rs`) defines all node types as Rust enums and structs.

### Statement Enum

```rust
pub enum Statement {
    Select(Select),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    CreateTable(CreateTable),
    DropTable(DropTable),
    AlterTable(AlterTable),
    CreateIndex(CreateIndex),
    DropIndex(DropIndex),
    CreateView(CreateView),
    DropView(DropView),
    Begin,
    Commit,
    Rollback { savepoint: Option<String> },
    Savepoint { name: String },
    Explain { analyze: bool, statement: Box<Statement> },
    ShowTables,
    ShowColumns { table: String },
    Analyze { table: Option<String> },
    Vacuum { table: Option<String> },
    Copy(Copy),
}
```

### Expression Enum

The `Expr` enum covers all expression forms:

- **Literals**: `Integer(i64)`, `Float(f64)`, `String(String)`, `Boolean(bool)`, `Null`
- **References**: `Identifier(String)`, `QualifiedIdentifier { table, column }`, `Star`, `QualifiedStar(String)`
- **Operators**: `UnaryOp { op, expr }`, `BinaryOp { left, op, right }`
- **Comparison forms**: `IsNull`, `InList`, `InSubquery`, `Between`, `Like` (with `case_insensitive` for ILIKE), `Exists`
- **CASE**: `Case { operand, when_clauses, else_clause }`
- **CAST**: `Cast { expr, data_type }` (covers both `CAST(x AS type)` and `x::type`)
- **Functions**: `FunctionCall { name, args, distinct }`, `Coalesce(Vec<Expr>)`, `Nullif(Box<Expr>, Box<Expr>)`, `Greatest(Vec<Expr>)`, `Least(Vec<Expr>)`
- **Subquery**: `Subquery(Box<Select>)`
- **Placeholder**: `Placeholder(u32)` for `$1`, `$2`, etc.

### Supporting Types

- `BinaryOp` -- 15 variants: `Add`, `Sub`, `Mul`, `Div`, `Mod`, `Exp`, `Concat`, `Eq`, `NotEq`, `Lt`, `Gt`, `LtEq`, `GtEq`, `And`, `Or`
- `UnaryOp` -- 3 variants: `Plus`, `Minus`, `Not`
- `JoinType` -- 7 variants: `Inner`, `Left`, `Right`, `Full`, `Cross`, `LeftSemi`, `LeftAnti`
- `JoinCondition` -- `On(Expr)` or `Using(Vec<String>)`
- `SetOperator` -- `Union`, `Intersect`, `Except`
- `ColumnConstraint` -- `NotNull`, `Null`, `Default(Expr)`, `PrimaryKey`, `Unique`, `Check(Expr)`, `References { ... }`
- `TableConstraint` -- `PrimaryKey`, `Unique`, `Check`, `ForeignKey`
- `IndexMethod` -- `BTree`, `Hash`
- `CopyDirection` -- `From(String)`, `To(String)`

## Error Handling

Both the lexer and parser produce structured error values with line, column, and byte offset information. The parser's `ParseError` includes an `expected` field listing what tokens would have been valid, enabling helpful error messages. Multiple errors can be collected per parse, allowing the caller to report all issues at once rather than stopping at the first error.
