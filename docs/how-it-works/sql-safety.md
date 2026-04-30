# SQL Safety

Provero interpolates user-supplied strings (column names, table names,
enum values) into raw SQL in the [optimizer](optimizer.md). The SQL
safety layer is a small module whose only job is to make that
interpolation injection-proof.

- **File**: `provero-core/src/provero/core/sql.py`
- **Public API**: `quote_identifier(name)`, `quote_value(value)`, `is_expression(name)`

---

## Why This Module Exists

Parameterized queries are the standard defense against SQL injection, but
they only parameterize *values*. Identifiers (table names, column names)
cannot be bound through parameters in any major SQL dialect: they have
to be interpolated textually.

Provero needs identifier interpolation because users declare columns and
tables in YAML:

```yaml
source: {type: duckdb, table: orders}
checks:
  - not_null: order_id
```

The string `"order_id"` becomes part of a `SUM(CASE WHEN "order_id" IS
NULL ...)` expression. The SQL safety layer ensures that string can only
be a legitimate identifier, never a payload.

---

## `quote_identifier(name)`

The main guardrail. Two layers of defense:

### 1. Allowlist regex

```python
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")
```

An identifier must start with a letter or underscore, followed by letters,
digits, underscores, or dots. Anything else (including spaces, quotes,
semicolons, parentheses, backslashes, unicode) raises `ValueError`
immediately.

This is a hard reject, not an escape. A column named `"DROP TABLE; --"`
cannot be smuggled through by any quoting strategy: it fails validation
and the query never runs.

### 2. Double-quoting

After validation, each dotted part is double-quoted (ANSI SQL standard):

```python
parts = name.split(".")
return ".".join(f'"{part}"' for part in parts)
```

So `schema.table` becomes `"schema"."table"`. Double-quoting is the
standard identifier quote across Postgres, DuckDB, SQLite, BigQuery, and
Snowflake. MySQL in default mode needs backticks, which is why the
SQLAlchemy-based MySQL connector wraps queries through its own dialect
before execution.

### Empty identifiers

An empty string raises `ValueError` with a clear message. This keeps a
blank `column:` in YAML from producing a query fragment like `"" IS NULL`
which some dialects silently accept.

---

## `quote_value(value)`

For string literals that must appear inline (not bindable via
parameters), `quote_value` doubles single quotes:

```python
def quote_value(value: str) -> str:
    return value.replace("'", "''")
```

The optimizer uses this for `accepted_values`:

```sql
SUM(CASE WHEN "status" NOT IN ('pending', 'shipped', 'delivered') ... END)
```

Every value in the list is passed through `quote_value()` before being
wrapped in single quotes. This is defense in depth: the YAML parser
already normalizes values to strings, and column type checks catch most
abuse, but doubling single quotes makes the SQL shape safe even if
upstream layers fail.

---

## `is_expression(name)`: the DuckDB Table-Function Escape Hatch

Provero reads files through DuckDB by putting a `read_*` table-function
in the `table` field:

```yaml
source:
  type: duckdb
  table: read_parquet('data/orders/*.parquet')
```

This is not an identifier: it has parentheses, a string literal, and a
glob. Running it through `quote_identifier()` would fail validation.

`is_expression()` allowlists **exactly four** function names:

```python
_SAFE_EXPRESSIONS = re.compile(
    r"^(read_csv|read_parquet|read_json|read_json_auto)\s*\(",
    re.IGNORECASE,
)
```

When `quote_identifier()` sees a value starting with one of these, it
returns the string as-is. Any other function call, arbitrary expression,
or subquery fails the main regex and raises `ValueError`.

This keeps the "read from files" ergonomics DuckDB is famous for, without
opening a generic escape hatch that would let arbitrary SQL through the
`table:` field.

---

## What the Optimizer Adds

The optimizer combines the SQL safety layer with **numeric coercion** for
numeric parameters:

```python
min_val = check.params.get("min")
if min_val is not None:
    try:
        min_val = float(min_val)
    except (ValueError, TypeError):
        raise ValueError(f"range check: 'min' must be numeric, got {min_val!r}")
```

A `range` check with `min: "0; DROP TABLE orders"` fails the `float()`
coercion and never reaches the query. Combined with identifier quoting,
this closes the last injection vector for batchable checks.

---

## What This Layer Does Not Do

- **Dialect translation.** `quote_identifier()` always produces ANSI
  double-quoted identifiers. Dialects that need a different quote style
  (MySQL backticks, SQL Server square brackets) handle that in the
  connector layer.
- **General query sanitization.** `custom_sql` checks pass raw queries
  from YAML straight to the database. The SQL safety module does not
  validate those. Users who write `custom_sql` are responsible for their
  own SQL, just as they would be writing SQL anywhere else.
- **Permission checks.** Defense against an authorized but malicious
  user (e.g. someone with write access to the repo) is out of scope. The
  layer protects against injection from untrusted input that ends up in
  YAML (config from a user-editable source), not against committers
  deliberately putting malicious SQL in `custom_sql` blocks.
