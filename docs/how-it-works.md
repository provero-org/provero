# How Provero Works

This page is a guided tour through what actually happens when you type
`provero run`. It walks the pipeline end to end, from the YAML file on disk to
the final [CheckResult](api/results.md) list, stopping at every piece of code
that matters: the parser, the connector factory, the batch optimizer, the
execution engine, and the result aggregator.

It is meant to answer questions like "where is the parser?", "how does the
source string turn into a live connection?", "what does the optimizer actually
batch?", and "how are results aggregated back into a suite score?".

For the user-facing configuration reference, see
[Configuration](configuration.md). For the target architecture (including
features on the roadmap), see [Architecture](ARCHITECTURE.md).

---

## Pipeline at a Glance

```
provero.yaml                                    Return
    │                                             ▲
    │ 1. compile_file()                           │ 7. SuiteResult
    ▼                                             │
ProveroConfig                                     │
    │                                     ┌───────┴────────┐
    │ 2. create_connector()               │ compute_status │
    ▼                                     │ quality_score  │
Connector  ─────── connect() ──► Connection
    │                                     ▲
    │ 3. _expand_multi_column_checks()    │
    ▼                                     │
Expanded CheckConfig list                 │
    │                                     │
    │ 4. plan_batch()                     │
    ▼                                     │
BatchPlan (metrics + non_batchable)       │
    │                                     │
    │ 5. execute_batch()                  │ 6. _run_single_check()
    ▼                                     │    (sequential or parallel)
Batched CheckResults ─────────────────────┘
                                  ▲
                                  │
                      Non-batchable CheckResults
```

Every stage is implemented in `provero-core/src/provero/core/`:

| Stage | File | Entry point |
|-------|------|-------------|
| 1. Parse YAML | `core/compiler.py` | `compile_file()` |
| 2. Resolve source | `connectors/factory.py` | `create_connector()` |
| 3. Expand checks | `core/engine.py` | `_expand_multi_column_checks()` |
| 4. Plan batch | `core/optimizer.py` | `plan_batch()` |
| 5. Execute batch | `core/optimizer.py` | `execute_batch()` |
| 6. Run remainder | `core/engine.py` | `_run_single_check()` |
| 7. Aggregate | `core/results.py` | `SuiteResult.compute_status()` |

---

## Stage 1: Parsing the Configuration

The parser lives in [`core/compiler.py`](api/compiler.md). Its only public
entry point is `compile_file(path)`, which returns a `ProveroConfig` object
(a Pydantic v2 model). Nothing else in the codebase reads YAML directly.

### The four nested models

The parser produces four nested Pydantic models:

```python
ProveroConfig
  ├── sources: dict[str, SourceConfig]
  ├── suites:  list[SuiteConfig]
  │              ├── source: SourceConfig
  │              └── checks: list[CheckConfig]
  ├── contracts: list[ContractConfig]
  └── alerts:    list[AlertConfig]
```

- `SourceConfig` holds just a `type`, `connection`, `table`, and optional
  `conn_id` (Airflow). No connection is opened here, it is pure config.
- `CheckConfig` holds `check_type`, `column`, `columns`, a free-form `params`
  dict, and an optional `severity`. Every check, no matter how complex, is
  normalized into this shape.
- `SuiteConfig` wraps one source and a list of checks, plus optional `tags`
  and `schedule`.

### Two supported YAML shapes

The parser accepts two top-level shapes in `provero.yaml` and normalizes
both into the same `ProveroConfig`:

**Simple shape:** `source` and `checks` at the root. The parser wraps them
into a single suite named after the file stem (e.g. `provero.yaml` becomes
a suite called `provero`).

```yaml
source:
  type: duckdb
  table: orders
checks:
  - not_null: order_id
```

**Full shape:** a `sources:` map (named, reusable) plus a `suites:` list
that references them by name.

```yaml
sources:
  warehouse:
    type: postgres
    connection: ${POSTGRES_URI}

suites:
  - name: orders_daily
    source: warehouse
    table: orders
    checks:
      - not_null: order_id
```

When a suite references a source that was not declared, `compile_file` raises
a `ValueError` listing the available source names. The same check runs for
the simple shape as well.

When a suite sets its own `table`, the parser uses Pydantic's
`model_copy(update={"table": ...})` to produce a new `SourceConfig` without
mutating the shared one. This matters because the same named source can be
referenced by many suites with different tables.

### Check parsing: four shorthand forms

The `parse_check()` function normalizes four very different YAML shapes into
the single `CheckConfig` model:

| YAML you write | Interpretation |
|----------------|----------------|
| `"not_null: order_id"` (string) | `{check_type: "not_null", column: "order_id"}` |
| `{not_null: [a, b, c]}` | `{check_type: "not_null", columns: [a, b, c]}` |
| `{range: {column: amount, min: 0, max: 100}}` | `{check_type: "range", column: "amount", params: {min: 0, max: 100}}` |
| `{custom_sql: "SELECT ..."}` | `{check_type: "custom_sql", params: {"query": "..."}}` |

Anything the parser cannot interpret raises a `ValueError` with an example of
the expected syntax.

!!! note "Why Pydantic, not TypedDict"
    Pydantic validates types and coerces values at parse time. That means a
    malformed check fails during `compile_file()`, before any connection is
    opened. A broken YAML file never reaches the engine.

---

## Stage 2: Resolving the Source

A `SourceConfig` is config, not a connection. The bridge between the two is
the connector factory in [`connectors/factory.py`](api/connectors.md).

### `create_connector(source)` step by step

1. **Expand environment variables.** The `connection` string is passed through
   `_resolve_connection()`, which only expands explicit `${VAR}` placeholders.
   Bare `$VAR` is left alone. This is a deliberate design choice: many
   passwords and S3 paths contain literal `$`, and expanding bare `$VAR` would
   corrupt them.
2. **Reject DataFrame types from factory.** `dataframe`, `pandas`, and
   `polars` raise a `ValueError` here. DataFrames cannot be built from config
   alone (there is no "pandas URL"), so users must pass a
   `DataFrameConnector(df, table_name=...)` directly to the engine.
3. **Try plugins first.** The factory loads entry points from the
   `provero.connectors` group. A plugin wins over a built-in of the same
   name, but plugins cannot shadow a built-in type (the factory explicitly
   skips plugin names that collide with `_BUILTINS`). This prevents a
   malicious package from hijacking `postgres` or `duckdb`.
4. **Fall back to built-ins.** The built-in map covers `duckdb`, `postgres`,
   `postgresql` (alias), `mysql`, `sqlite`, `snowflake`, `bigquery`,
   `redshift`, and `databricks`. Only the first three have dedicated
   implementations. The others route through a generic `SQLAlchemyConnector`.
5. **Raise with hints on failure.** An unknown type produces a helpful error
   listing every registered type plus an `install` hint
   (`pip install provero-connector-<name>`).

### DuckDB special case

DuckDB is the only built-in where an empty connection string is valid: it
becomes `:memory:`. For every other connector, an empty connection string
raises `ValueError`.

### Plugin connectors via entry_points

Third-party connectors register themselves in their own package:

```toml
[project.entry-points."provero.connectors"]
mongodb = "provero_mongodb:MongoConnector"
```

At runtime, Provero calls `importlib.metadata.entry_points()` to discover
them. There is no centralized registry, no server, no config file to update.
Install the package, and the new source type is available.

### The `Connector` protocol

Every connector (built-in or plugin) satisfies the `Connector` protocol
defined in [`connectors/base.py`](api/connectors.md):

```python
class Connector(Protocol):
    def connect(self) -> Connection: ...
    def disconnect(self, connection: Connection) -> None: ...
    def get_schema(self, connection: Connection, table: str) -> list[dict]: ...
    def get_profile(self, connection, table, columns=None, sample_size=None) -> dict: ...
```

Note that `get_schema` and `get_profile` have default implementations on the
protocol itself (Python 3.11+ allows this via `Protocol` with concrete
defaults). A new connector only needs to implement `connect`, `disconnect`,
and a `Connection` that can `execute(query)` and `get_columns(table)`.

### The `Connection` protocol

A `Connection` is anything with two methods:

```python
def execute(query: str, params: dict | None = None) -> list[dict[str, Any]]
def get_columns(table: str) -> list[dict[str, Any]]
```

`execute` returns a list of row-dicts (column name to value), which is why
the optimizer can index into `data[alias]` directly. Connectors wrap native
drivers (the DuckDB Python API, SQLAlchemy, etc.) and normalize their output
into this shape.

---

## Stage 3: Preparing the Checks

Before execution, the engine runs a small normalization pass in
`_expand_multi_column_checks()` (in [`core/engine.py`](api/engine.md)).

Today it does exactly one thing: it expands multi-column `not_null` into
individual per-column checks. A YAML line like:

```yaml
- not_null: [order_id, customer_id, amount]
```

is normalized into three `CheckConfig` objects, one per column. This
guarantees every column gets its own `CheckResult`, regardless of whether
the column is executed inside the batch or as a standalone check.

Other multi-column checks (`unique_combination`, `accepted_values` over a
list) are passed through untouched because their semantics are tuple-based,
not per-column.

---

## Stage 4: Batch Planning

The optimizer in [`core/optimizer.py`](api/optimizer.md) is the main
performance lever. The idea is simple: instead of running N queries against
the same table, compile them into one.

### What can be batched

Six check types are batchable today:

- `not_null`
- `completeness`
- `unique`
- `range`
- `row_count`
- `accepted_values`

Every other check (`freshness`, `regex`, `custom_sql`, `referential_integrity`,
`anomaly`, etc.) is routed to `plan.non_batchable` and runs as its own
query in Stage 6.

### How metrics are composed

`plan_batch(table, checks)` walks the check list and, for each batchable
check, appends one or more `BatchedMetric` entries to the plan. Each metric
has three fields:

```python
@dataclass
class BatchedMetric:
    alias: str            # SQL column alias, e.g. "nn_order_id_null"
    expression: str       # SQL expression, e.g. "SUM(CASE WHEN ... END)"
    check_config: CheckConfig
```

Examples from the code:

| Check | Emitted expression(s) |
|-------|-----------------------|
| `not_null: order_id` | `SUM(CASE WHEN "order_id" IS NULL THEN 1 ELSE 0 END) AS nn_order_id_null` |
| `unique: order_id` | `COUNT("order_id") AS uniq_order_id_total`, `COUNT(DISTINCT "order_id") AS uniq_order_id_distinct` |
| `range: {column: amount, min: 0, max: 1000}` | `MIN("amount")`, `MAX("amount")`, `SUM(CASE WHEN "amount" < 0 OR "amount" > 1000 THEN 1 ELSE 0 END)` |
| `accepted_values: {column: status, values: [a,b]}` | `SUM(CASE WHEN "status" NOT IN ('a','b') AND "status" IS NOT NULL THEN 1 ELSE 0 END)` |
| `row_count: {min: 1}` | `COUNT(*) AS _row_count` |

A `COUNT(*) AS _total` metric is always added if no other total is present,
so every batch has a denominator for ratio-based checks like `completeness`.

### SQL injection defenses

The optimizer is the one place where user-supplied strings cross into raw
SQL. Two invariants keep it safe:

- **Identifiers are quoted via `quote_identifier()`** (in `core/sql.py`),
  which rejects anything that is not `^[A-Za-z_][A-Za-z0-9_.]*$` and
  double-quotes each part of `schema.table`.
- **Numeric parameters are coerced via `float()`** before interpolation. A
  `range` check with a non-numeric `min` raises `ValueError` during planning,
  never reaches the database.
- **Accepted values are escaped with `quote_value()`**, which doubles single
  quotes.

The only exception to identifier quoting is DuckDB table-functions like
`read_csv(...)` and `read_parquet(...)`. These are allowlisted by
`is_expression()` (a hard-coded regex) and passed through unquoted, because
they are valid SQL `FROM` targets but not identifiers.

### The generated query

`build_batch_query(plan)` assembles the metrics into one `SELECT`, with one
subtle optimization: identical expressions are deduplicated (a `COUNT(*)`
used by both `row_count` and completeness appears once). The final query
looks like:

```sql
SELECT
    SUM(CASE WHEN "order_id" IS NULL THEN 1 ELSE 0 END) as nn_order_id_null,
    COUNT("order_id") as uniq_order_id_total,
    COUNT(DISTINCT "order_id") as uniq_order_id_distinct,
    MIN("amount") as range_amount_min,
    MAX("amount") as range_amount_max,
    SUM(CASE WHEN "amount" < 0 OR "amount" > 1000 THEN 1 ELSE 0 END) as range_amount_oor,
    COUNT(*) as _total
FROM "orders"
```

---

## Stage 5: Executing the Batch

`execute_batch(connection, plan)` runs the single query and interprets the
result row back into one `CheckResult` per original check.

### The result row

`connection.execute(query)` returns a list with one row-dict, where keys are
the metric aliases. Every `None` is coerced to `0` to handle empty tables
(SUM/COUNT on empty tables return `NULL` in most dialects).

### Interpretation per check type

For each metric, the executor looks up its `check_config.check_type` and
builds the corresponding `CheckResult`. A few examples:

- **`not_null`**: PASS if the null count is zero, FAIL otherwise. The
  `failing_rows_query` is set to
  `SELECT * FROM "orders" WHERE "order_id" IS NULL`, ready for the user to
  inspect bad rows.
- **`unique`**: compares `COUNT(col)` (non-null total) against
  `COUNT(DISTINCT col)`. Nulls are excluded from the total because
  `COUNT(DISTINCT)` also excludes them, so including nulls would produce
  false positives on nullable unique columns.
- **`completeness`**: divides non-null count by the total. The `min` param
  is passed through `_normalize_min_completeness()` (from the completeness
  module), which accepts both `0.95` and `"95%"`.

Each check is processed once, deduplicated by a `processed_checks` set, so
a metric that appears twice in the plan (e.g. `COUNT(*)` used by both
`row_count` and `_total`) only produces one result.

### Failure modes

If the batch query itself fails (bad table name, permissions, etc.), the
engine catches the exception and emits a single `CheckResult` with
`check_type="batch"` and `status=ERROR`. The user sees a hint suggesting
`--no-optimize` to isolate the broken check.

---

## Stage 6: Running Non-batchable Checks

Everything in `plan.non_batchable` (plus all checks if `optimize=False`) is
executed through `_run_single_check()`.

### Runner resolution

Each check type has a runner function registered via the
`@register_check("name")` decorator in [`checks/registry.py`](api/checks.md).
The engine calls `get_check_runner(check_type)` to look it up. An unknown
type produces a `CheckResult` with `status=ERROR` listing all available
types. The suite continues rather than halting.

Like connectors, checks support plugins via entry_points
(`provero.checks` group). Plugins cannot override built-ins.

### Context injection for anomaly and row_count_change

Before running the check, the engine injects two synthetic params:
`_suite_name` and `_check_name`. The anomaly detector and the
`row_count_change` check use these to look up historical metrics from the
result store (see [Store](api/store.md) in the API reference).

### Error handling

Every check runs inside a `try/except`. A raised exception becomes a
`CheckResult` with `status=ERROR` and a context-aware hint:

- Messages containing "does not exist" or "not found" get
  "Verify that table 'X' exists."
- Permission errors get "Check your database permissions."
- Connection errors get "Verify that the database is running..."

The hint is derived from substring matching on the exception message, which
is pragmatic rather than precise, but good enough for the most common
failures.

### Sequential vs parallel

By default, non-batchable checks run sequentially on the suite's shared
connection. Pass `parallel=True` to `run_suite` to enable a
`ThreadPoolExecutor` with `max_workers` threads.

!!! warning "Thread safety"
    In parallel mode, each thread gets **its own connection** via
    `connector.connect()`. This is required because DuckDB (and many other
    drivers) are not thread-safe when sharing a single connection. The
    engine handles this inside `_run_single_check`: if a `connector` is
    passed, a new connection is created at function entry and closed in a
    `finally` block at function exit.

### Failing rows sample

If a check fails and sets `failing_rows_query` but not
`failing_rows_sample`, the engine runs the query with `LIMIT 5` and stores
the result in `failing_rows_sample`. Reports include this sample so users
can see *which* rows broke the check, not just how many.

---

## Stage 7: Aggregating Results

After every check has produced a `CheckResult`, the engine builds a
`SuiteResult` and calls `compute_status()`.

### Status counts

`SuiteResult.compute_status()` counts PASS, FAIL, WARN, and ERROR across
every check, then sets `status`:

- `PASS` if `failed == 0` and `errored == 0`
- `FAIL` otherwise (even if all failures are WARNs from low-severity checks)

### Severity-weighted quality score

The quality score is a weighted percentage where the weights come from
`SuiteResult._SEVERITY_WEIGHT`:

| Severity | Weight |
|----------|--------|
| `INFO` | 0.25 |
| `WARNING` | 0.5 |
| `CRITICAL` | 1.0 |
| `BLOCKER` | 1.0 |

```python
ok_weight    = sum of weights of PASS + WARN checks
total_weight = sum of weights of all checks
score        = round((ok_weight / total_weight) * 100, 1)
```

The key design choice: **PASS and WARN both count as "not failed"**. A WARN
is a check that detected an issue whose severity was too low to block the
suite (INFO or WARNING). These are still surfaced in the report but do not
reduce the score the same way a CRITICAL failure does.

### Severity downgrade

Inside `_run_single_check()`, after the runner returns, the engine calls
`result.apply_severity()`. This downgrades `FAIL` to `WARN` whenever the
check's severity is INFO or WARNING. In effect, severity is the dial users
turn to decide whether a failing check blocks the suite or just shows up
as a warning.

---

## The `Engine` Class

Everything above is wrapped by the `Engine` class in `core/engine.py`, which
is the documented public API:

```python
from provero import Engine

engine = Engine("provero.yaml")
results = engine.run(optimize=True, parallel=False)
```

`Engine.from_dict(cfg)` accepts an in-memory dict following the same YAML
shape, useful for programmatic use and tests. Both `run()` (flat list of
`CheckResult`) and `run_suites()` (list of `SuiteResult`) are available.

Internally, `Engine.run()` iterates over every suite in the config,
calls `create_connector(suite.source)` to get a live connector, and hands
both off to `run_suite()`. Connections are opened once per suite, inside a
`try/finally` that guarantees they are closed.

---

## Extending Provero

### Adding a new check type

1. Create a runner function that accepts
   `(connection, table, check_config)` and returns a `CheckResult`.
2. Decorate it with `@register_check("my_check")` in a module that gets
   imported by `checks/registry._load_builtins()`.
3. Optionally, if the check can be expressed as a single SQL aggregate, add
   a branch in `optimizer.plan_batch()` so it joins the batch.

### Adding a new connector

1. Implement the `Connector` and `Connection` protocols. Most connectors can
   inherit from `SQLAlchemyConnector` if the source speaks SQL.
2. Register it in a package entry point:

    ```toml
    [project.entry-points."provero.connectors"]
    mongodb = "provero_mongodb:MongoConnector"
    ```

3. Install the package alongside `provero`. The factory picks it up
   automatically on the next run.

### Adding a new export format

Exporters live in `provero/exporters/` and follow the same registry pattern.
The built-in `dbt` exporter is a good reference for a clean implementation.

---

## Where to Read Next

- [Check Types](checks.md): reference for every built-in check.
- [Connectors](connectors.md): setup instructions for each data source.
- [Configuration](configuration.md): the full `provero.yaml` schema.
- [API Reference](api/index.md): generated docs for every public symbol.
- [Architecture](ARCHITECTURE.md): the target design, including streaming,
  server mode, and other features on the roadmap.
