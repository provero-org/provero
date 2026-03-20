# Check Types

Provero includes 14 built-in check types organized by category. Each check can be written in shorthand or expanded YAML form.

## Completeness

### not_null

Verifies that column(s) contain no null values.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| column(s) | string or list | Yes | Column name or list of column names |

Default severity: `critical`

=== "Shorthand"

    ```yaml
    checks:
      - not_null: order_id
      - not_null: [order_id, customer_id, amount]
    ```

=== "Expanded"

    ```yaml
    checks:
      - not_null:
          column: order_id
          severity: warning
    ```

### completeness

Checks that a column meets a minimum percentage of non-null values.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | string | Yes | | Column to check |
| `min` | number | No | `0.95` | Minimum completeness ratio (0 to 1) |

Default severity: `critical`

```yaml
checks:
  - completeness:
      column: email
      min: 0.90
```

## Uniqueness

### unique

Verifies that a column has no duplicate values.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| column | string | Yes | Column name |

Default severity: `critical`

```yaml
checks:
  - unique: order_id
```

### unique_combination

Checks that a combination of columns is unique (composite key).

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| columns | list | Yes | List of column names (minimum 2) |

Default severity: `critical`

```yaml
checks:
  - unique_combination: [date, store_id]
```

## Validity

### accepted_values

Ensures column values are within an allowed set.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `column` | string | Yes | Column to check |
| `values` | list | Yes | Allowed values (strings, numbers, or booleans) |

Default severity: `critical`

```yaml
checks:
  - accepted_values:
      column: status
      values: [pending, shipped, delivered, cancelled]
```

### range

Verifies that numeric values fall within min/max bounds.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `column` | string | Yes | Column to check |
| `min` | number | No | Minimum allowed value |
| `max` | number | No | Maximum allowed value |

At least one of `min` or `max` should be specified.

Default severity: `critical`

```yaml
checks:
  - range:
      column: amount
      min: 0
      max: 100000
```

### regex

Validates that column values match a regular expression pattern.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `column` | string | Yes | Column to check |
| `pattern` | string | Yes | Regular expression pattern |

Works across databases: uses `regexp_matches()` on DuckDB, `~` on PostgreSQL, and `REGEXP` on MySQL/SQLite.

Default severity: `warning`

```yaml
checks:
  - regex:
      column: email
      pattern: "^[^@]+@[^@]+\\.[^@]+$"
```

### type

Checks that a column's data type matches the expected type.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `column` | string | Yes | Column to check |
| `expected` | string | Yes | Expected type: `integer`, `float`, `string`, `boolean`, `date`, `timestamp` |

Type names are normalized across databases. For example, `int`, `int4`, `bigint`, and `smallint` all match `integer`.

Default severity: `critical`

```yaml
checks:
  - type:
      column: amount
      expected: float
```

## Freshness

### freshness

Checks that the most recent row is within a time threshold.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | string | Yes | | Timestamp column |
| `max_age` | string | Yes | `24h` | Maximum age. Format: `30m`, `24h`, `7d` |

Default severity: `critical`

```yaml
checks:
  - freshness:
      column: updated_at
      max_age: 24h
```

### latency

Measures the time difference between two timestamp columns. Useful for detecting pipeline delays.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `source_column` | string | Yes | | Start timestamp (e.g., event time) |
| `target_column` | string | Yes | | End timestamp (e.g., load time) |
| `max_latency` | string | No | `1h` | Maximum acceptable latency |

Default severity: `warning`

```yaml
checks:
  - latency:
      source_column: created_at
      target_column: processed_at
      max_latency: 1h
```

## Volume

### row_count

Validates that the table row count is within expected bounds.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `min` | integer | No | `0` | Minimum row count |
| `max` | integer | No | | Maximum row count |

Default severity: `critical`

```yaml
checks:
  - row_count:
      min: 1
      max: 1000000
```

### row_count_change

Compares the current row count against the previous run. Requires the result store to be enabled (default).

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `max_decrease` | string | No | `50%` | Maximum allowed decrease percentage |
| `max_increase` | string | No | `500%` | Maximum allowed increase percentage |

Default severity: `warning`

```yaml
checks:
  - row_count_change:
      max_decrease: "10%"
      max_increase: "200%"
```

## Anomaly Detection

### anomaly

Statistical anomaly detection on historical metrics. Compares the current value against stored history using one of three methods.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `metric` | string | Yes | | Metric to check: `row_count`, `null_count`, `null_rate`, `distinct_count`, `mean`, `min`, `max` |
| `column` | string | No | | Column for column-level metrics |
| `method` | string | No | `mad` | Detection method: `zscore`, `mad`, `iqr` |
| `sensitivity` | string | No | `medium` | Sensitivity level |
| `threshold` | float | No | | Direct threshold override |

Default severity: `warning`

```yaml
checks:
  - anomaly:
      column: daily_revenue
      metric: mean
      method: mad
      sensitivity: medium
```

**Detection methods:**

| Method | Description | Best for |
|--------|-------------|----------|
| `zscore` | Standard Z-Score | Normally distributed metrics |
| `mad` | Median Absolute Deviation | Robust to outliers |
| `iqr` | Interquartile Range | Skewed distributions |

All methods are implemented using Python stdlib only (no scipy dependency). Anomaly detection uses the result store to compare current values against historical data. Run `provero run` regularly to build up the baseline.

## Custom

### custom_sql

Runs a custom SQL query that must return a truthy value to pass.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | Yes | SQL query returning a single value |
| `name` | string | No | Custom name for the check |

Default severity: `critical`

=== "Shorthand"

    ```yaml
    checks:
      - custom_sql: "SELECT COUNT(*) > 0 FROM orders"
    ```

=== "Expanded"

    ```yaml
    checks:
      - custom_sql:
          name: positive_revenue
          query: "SELECT SUM(amount) > 0 FROM orders"
    ```
