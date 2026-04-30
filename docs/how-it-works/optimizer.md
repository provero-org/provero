# Optimizer

The optimizer is the main performance lever. Instead of running N queries
against the same table (one per check), it compiles batchable checks into
a single `SELECT` with one aggregate per metric.

- **File**: `provero-core/src/provero/core/optimizer.py`
- **Entry points**: `plan_batch(table, checks)`, `execute_batch(connection, plan)`

---

## Why Batching Matters

A naive implementation of a 30-check suite runs 30 queries. Each query
pays per-round-trip latency (TCP, parser, planner, first row) that
dominates runtime on small-to-medium tables. With batching, all of those
aggregates fold into a single scan of the table.

A concrete example. Without batching:

```sql
SELECT COUNT(*) FROM orders WHERE order_id IS NULL;
SELECT COUNT(DISTINCT order_id) FROM orders;
SELECT MIN(amount), MAX(amount) FROM orders;
```

With batching:

```sql
SELECT
    SUM(CASE WHEN "order_id" IS NULL THEN 1 ELSE 0 END) AS nn_order_id_null,
    COUNT("order_id")                                  AS uniq_order_id_total,
    COUNT(DISTINCT "order_id")                         AS uniq_order_id_distinct,
    MIN("amount")                                      AS range_amount_min,
    MAX("amount")                                      AS range_amount_max,
    COUNT(*)                                           AS _total
FROM "orders"
```

One query. One scan.

---

## What Can Be Batched

Six check types are batchable today, declared in `_BATCHABLE_TYPES`:

- `not_null`
- `completeness`
- `unique`
- `range`
- `row_count`
- `accepted_values`

Every other check (`freshness`, `regex`, `custom_sql`,
`referential_integrity`, `anomaly`, `row_count_change`, etc.) is appended
to `plan.non_batchable` and runs as its own query in the [engine's
non-batchable path](engine.md#stage-6-non-batchable-checks).

## What Cannot Be Batched

A few edge cases are routed to non-batchable even within batchable types:

- **`accepted_values` with no values.** Without a list to compare against,
  there is no metric to emit.

Most non-batchable checks are non-batchable because they either need a
self-join (`referential_integrity`), a different scan strategy
(`freshness` on one column does not share a scan with another table), or
historical data (`anomaly`, `row_count_change` both read from the result
store).

---

## Building the Plan

`plan_batch(table, checks)` walks the check list and, for each batchable
check, appends one or more `BatchedMetric` entries to the plan:

```python
@dataclass
class BatchedMetric:
    alias: str            # SQL column alias, e.g. "nn_order_id_null"
    expression: str       # SQL expression, e.g. "SUM(CASE WHEN ... END)"
    check_config: CheckConfig
```

The plan itself holds both the metrics and the checks routed out:

```python
@dataclass
class BatchPlan:
    table: str
    metrics: list[BatchedMetric]
    non_batchable: list[CheckConfig]
```

### Per-check metric emission

| Check | Emitted expression(s) |
|-------|-----------------------|
| `not_null: order_id` | `SUM(CASE WHEN "order_id" IS NULL THEN 1 ELSE 0 END) AS nn_order_id_null` |
| `unique: order_id` | `COUNT("order_id") AS uniq_order_id_total`, `COUNT(DISTINCT "order_id") AS uniq_order_id_distinct` |
| `range: {column: amount, min: 0, max: 1000}` | `MIN("amount")`, `MAX("amount")`, `SUM(CASE WHEN "amount" < 0 OR "amount" > 1000 THEN 1 ELSE 0 END)` |
| `accepted_values: {column: status, values: [a, b]}` | `SUM(CASE WHEN "status" NOT IN ('a','b') AND "status" IS NOT NULL THEN 1 ELSE 0 END)` |
| `row_count: {min: 1}` | `COUNT(*) AS _row_count` |
| `completeness: {column: email}` | `COUNT("email") AS comp_email_nonnull` |

A `COUNT(*) AS _total` metric is always added if no other total is
present, so every batch has a denominator for ratio-based checks like
`completeness`.

### Alias sanitization

Column names are passed through `_safe_alias()`, which replaces dots with
`__dot__` and spaces with underscores. This keeps aliases unambiguous when
a column name contains a dot (e.g. `a.b` vs `a_b` would otherwise
collide).

---

## Building the Query

`build_batch_query(plan)` assembles the metrics into one `SELECT`, with
one subtle optimization: **identical expressions are deduplicated**. A
`COUNT(*)` used by both `row_count` and `completeness` appears once in
the final query under a single alias.

The `FROM` clause goes through `quote_identifier()` (see [SQL
safety](sql-safety.md)) so identifiers cannot inject SQL.

---

## Executing the Batch

`execute_batch(connection, plan)` runs the single query and interprets the
result row back into one `CheckResult` per original check.

### Handling NULL results

`SUM` and `COUNT` on empty tables return `NULL` in most dialects. The
executor coerces every `None` in the row-dict to `0` before interpretation,
which means a check on an empty table does not crash: it just reports
`observed=0`.

### Dedup via `processed_checks`

Some checks emit multiple metrics (e.g. `unique` emits both `total` and
`distinct`). The executor iterates over `plan.metrics` but uses a
`processed_checks` set keyed by `check_type:column` to ensure only one
`CheckResult` per original check is produced.

### Failing rows query

When a check fails, the executor also populates
`CheckResult.failing_rows_query` with SQL the user can copy-paste to
inspect the bad rows. For example:

- `not_null:order_id` sets
  `SELECT * FROM "orders" WHERE "order_id" IS NULL`
- `unique:order_id` sets
  `SELECT "order_id", COUNT(*) FROM "orders" GROUP BY "order_id" HAVING COUNT(*) > 1`

The engine (not the optimizer) later runs this with `LIMIT 5` to populate
`failing_rows_sample`, so reports can show actual rows, not just counts.

### Subtle correctness: `unique` uses `COUNT(col)`

The `unique` check compares `COUNT(col)` against `COUNT(DISTINCT col)`,
not `COUNT(*)`. This matters because `COUNT(DISTINCT)` excludes NULLs in
most dialects. If the total were `COUNT(*)` and the column allowed NULLs,
the check would report false positives (a column full of unique values
plus two NULLs would appear to have a "duplicate").

This correctness fix is in
[PR #128](https://github.com/provero-org/provero/pull/128).

---

## Failure Modes

If the batch query itself fails (bad table, permissions, etc.), the
[engine](engine.md) catches the exception and emits a single
`CheckResult` with `check_type="batch"` and `status=ERROR`. The user sees
a hint suggesting `--no-optimize` to isolate the broken check. This keeps
a single malformed column from taking down the entire suite silently.

If a single check in the batch has invalid params (e.g. `range` with a
non-numeric `min`), `plan_batch()` itself raises `ValueError`. The engine
catches it, emits an ERROR result, and continues with `plan=None` so that
non-batchable checks still run.

---

## Disabling the Optimizer

Pass `optimize=False` to `run_suite()` (or `--no-optimize` on the CLI) to
skip batching entirely. Every check runs as its own query through the
[engine's single-check path](engine.md#stage-6-non-batchable-checks).

This is primarily a debugging tool: if a batch fails, rerunning without
optimization makes the specific failing check obvious.
