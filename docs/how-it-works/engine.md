# Engine

The engine orchestrates a full suite run: it takes a parsed
`ProveroConfig`, obtains a live connection from a connector, plans the
batch, executes it, runs non-batchable checks, and aggregates everything
into a `SuiteResult`.

- **File**: `provero-core/src/provero/core/engine.py`
- **Public API**: `Engine` class
- **Internal entry points**: `run_suite()`, `_run_single_check()`

---

## The `Engine` Class

The public API is a small class:

```python
from provero import Engine

engine = Engine("provero.yaml")
results = engine.run(optimize=True, parallel=False)
```

Two constructors are available:

| Constructor | Use case |
|-------------|----------|
| `Engine(path)` | Load from a YAML file on disk |
| `Engine.from_dict(cfg)` | In-memory config (tests, dynamic configs) |

Two run methods exist:

| Method | Returns |
|--------|---------|
| `run(...)` | Flat `list[CheckResult]` across every suite |
| `run_suites(...)` | `list[SuiteResult]` with per-suite metadata |

Both accept the same flags:

| Flag | Default | Effect |
|------|---------|--------|
| `optimize` | `True` | Batch checks into one query (see [Optimizer](optimizer.md)) |
| `parallel` | `False` | Run non-batchable checks on a thread pool |
| `max_workers` | `4` | Thread pool size when `parallel=True` |

Internally, `Engine.run()` iterates over every suite in the config, calls
`create_connector(suite.source)` (see [Connectors](connectors.md)) to get
a live connector, and hands both off to `run_suite()`. Connections are
opened once per suite, inside a `try/finally` that guarantees they are
closed.

---

## `run_suite()`: The Full Flow

```python
def run_suite(suite, connector, optimize=True, parallel=False, max_workers=4) -> SuiteResult:
    run_id = uuid.uuid4()
    connection = connector.connect()
    try:
        return _run_suite_inner(...)
    finally:
        connector.disconnect(connection)
```

Inside `_run_suite_inner`, the flow is:

1. **Expand multi-column checks** (`_expand_multi_column_checks`).
2. **Plan the batch** (`plan_batch` from the [optimizer](optimizer.md)).
3. **Execute the batched query** (`execute_batch`).
4. **Run remaining non-batchable checks** (sequential or parallel).
5. **Build and return the `SuiteResult`**.

Each phase is wrapped in targeted error handling so a single failure does
not take down the whole suite.

---

## Check Expansion

Before execution, the engine runs a small normalization pass:

```yaml
- not_null: [order_id, customer_id, amount]
```

is expanded into three separate `CheckConfig` objects, one per column.
This guarantees every column gets its own `CheckResult`, regardless of
whether the column is executed inside the batch or as a standalone check.

Today this only expands `not_null` with multiple columns. Other
multi-column checks (`unique_combination`, `accepted_values` over a list)
are passed through untouched because their semantics are tuple-based, not
per-column.

---

## The Non-batchable Runner

Everything in `plan.non_batchable` (plus all checks if `optimize=False`)
is executed through `_run_single_check()`.

### Runner resolution

Each check type has a runner function registered via `@register_check`
(see [Check Registry](registry.md)). The engine calls
`get_check_runner(check_type)` to look it up. An unknown type produces a
`CheckResult` with `status=ERROR` listing all available types. The suite
continues rather than halting.

### Context injection for anomaly and row_count_change

Before running certain checks, the engine injects two synthetic params:

```python
if check.check_type in ("anomaly", "row_count_change"):
    check = check.model_copy(update={
        "params": {
            **check.params,
            "_suite_name": suite.name,
            "_check_name": f"{check.check_type}:{check.column}",
        }
    })
```

The anomaly detector and the `row_count_change` check use these to look
up historical metrics from the result store.

### Context-aware error hints

Every check runs inside a `try/except`. A raised exception becomes a
`CheckResult` with `status=ERROR` and a hint derived from substring
matching on the exception message:

| Substring in error | Hint |
|--------------------|------|
| "does not exist", "not found" | "Verify that table 'X' exists." |
| "permission", "denied" | "Check your database permissions." |
| "connection", "refused" | "Verify that the database is running..." |

This is pragmatic rather than precise, but it covers the most common
failures without forcing every connector to classify errors.

### Failing rows sample

If a check fails and sets `failing_rows_query` but not
`failing_rows_sample`, the engine runs the query with `LIMIT 5` and
stores the result in `failing_rows_sample`. Reports include this sample
so users can see *which* rows broke the check, not just how many.

If that secondary query fails for any reason, the engine swallows the
error (the primary result is more important than the debug sample).

---

## Sequential vs Parallel

By default, non-batchable checks run sequentially on the suite's shared
connection:

```python
for runner, check_config in runnable:
    result = _run_single_check(runner, connection, ...)
    results.append(result)
```

Pass `parallel=True` to enable a `ThreadPoolExecutor` with `max_workers`
threads.

### Thread safety: connection per thread

In parallel mode, each thread gets **its own connection** via
`connector.connect()`, not the shared suite connection:

```python
own_connection = None
if connector is not None:
    own_connection = connector.connect()
    connection = own_connection
try:
    result = runner(connection=connection, ...)
finally:
    if own_connection is not None:
        connector.disconnect(own_connection)
```

This is required because DuckDB (and many other drivers) are not
thread-safe when sharing a single connection across threads. The engine
takes the cost of opening N extra connections to keep parallel mode
correct.

### When to enable parallel

Parallel mode mostly helps suites that have many non-batchable checks
(`custom_sql`, `freshness`, `referential_integrity`). For suites
dominated by batchable checks, the batched query already runs
everything in one round trip, and parallelism adds setup cost without
helping.

---

## Severity Downgrade

After the runner returns, the engine calls `result.apply_severity()`. This
downgrades `FAIL` to `WARN` whenever the check's severity is `INFO` or
`WARNING`. In effect, severity is the dial users turn to decide whether a
failing check blocks the suite or just shows up as a warning.

See [Results](results.md) for how severity interacts with the suite-level
quality score.

---

## `run_contract()`

A sibling function `run_contract(contract, connector, sources=None)`
validates a data contract. It obtains a connection the same way as
`run_suite`, then delegates to `validate_contract()` in the contracts
module. This is intentionally kept out of the main engine flow because
contract validation has different semantics (schema diffs, SLA checks)
than check execution.
