# Provero

[![CI](https://github.com/provero-org/provero/actions/workflows/ci.yaml/badge.svg)](https://github.com/provero-org/provero/actions/workflows/ci.yaml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://python.org)
[![Contributing](https://img.shields.io/badge/contributing-guide-blue)](CONTRIBUTING.md)

> **provero** (Esperanto): to test, to put to proof.

A vendor-neutral, declarative data quality engine.

<!-- Demo GIF will be generated from docs/demo.tape using charmbracelet/vhs -->
<!-- To generate: vhs docs/demo.tape -o docs/assets/demo.gif -->
<p align="center">
  <img src="docs/assets/demo.gif" alt="Provero demo" width="700">
</p>

## Quick Start

```bash
pip install provero
provero init
```

Edit `provero.yaml`:

```yaml
source:
  type: duckdb
  table: orders

checks:
  - not_null: [order_id, customer_id, amount]
  - unique: order_id
  - accepted_values:
      column: status
      values: [pending, shipped, delivered, cancelled]
  - range:
      column: amount
      min: 0
      max: 100000
  - row_count:
      min: 1
```

Run:

```bash
provero run
```

```
┌─────────────────┬──────────────┬──────────┬──────────────────┬──────────────────┐
│ Check           │ Column       │ Status   │ Observed         │ Expected         │
├─────────────────┼──────────────┼──────────┼──────────────────┼──────────────────┤
│ not_null        │ order_id     │ ✓ PASS   │ 0 nulls          │ 0 nulls          │
│ not_null        │ customer_id  │ ✓ PASS   │ 0 nulls          │ 0 nulls          │
│ not_null        │ amount       │ ✓ PASS   │ 0 nulls          │ 0 nulls          │
│ unique          │ order_id     │ ✓ PASS   │ 0 duplicates     │ 0 duplicates     │
│ accepted_values │ status       │ ✓ PASS   │ 0 invalid values │ only [pending..] │
│ range           │ amount       │ ✓ PASS   │ min=45, max=999  │ min=0, max=100k  │
│ row_count       │ -            │ ✓ PASS   │ 5                │ >= 1             │
└─────────────────┴──────────────┴──────────┴──────────────────┴──────────────────┘

Score: 100/100 | 7 passed, 0 failed | 22ms
```

## Features

- **14 check types**: not_null, unique, unique_combination, completeness, accepted_values,
  range, regex, type, freshness, latency, row_count, row_count_change, anomaly, custom_sql
- **3 connectors**: DuckDB (files + in-memory), PostgreSQL, Pandas/Polars DataFrame
- **SQL batch optimizer**: compiles N checks into 1 query
- **Data contracts**: schema validation, SLA enforcement, contract diff
- **Anomaly detection**: Z-Score, MAD, IQR (stdlib only, no scipy needed)
- **HTML reports**: `provero run --report html`
- **Webhook alerts**: notify Slack, PagerDuty, or any HTTP endpoint on failure
- **Result store**: SQLite with time-series metrics and `provero history`
- **Data profiling**: `provero profile --suggest` auto-generates checks
- **Configurable severity**: info, warning, critical, blocker per check
- **JSON Schema validation** for provero.yaml
- **Airflow provider**: ProveroCheckOperator + @provero_check decorator

## Check Types

| Check | Description | Example |
|-------|-------------|---------|
| `not_null` | Column has no null values | `not_null: order_id` |
| `unique` | Column has no duplicate values | `unique: order_id` |
| `unique_combination` | Composite uniqueness across columns | `unique_combination: [date, store_id]` |
| `completeness` | Minimum percentage of non-null values | `completeness: { column: email, min: 95% }` |
| `accepted_values` | Column values are within allowed set | `accepted_values: { column: status, values: [a, b] }` |
| `range` | Numeric values within min/max bounds | `range: { column: amount, min: 0, max: 100000 }` |
| `regex` | Values match a regular expression | `regex: { column: email, pattern: ".+@.+" }` |
| `type` | Column data type matches expected | `type: { column: amount, expected: numeric }` |
| `freshness` | Most recent timestamp within threshold | `freshness: { column: updated_at, max_age: 24h }` |
| `latency` | Time between two timestamp columns | `latency: { start: created_at, end: processed_at, max: 1h }` |
| `row_count` | Table row count within bounds | `row_count: { min: 1, max: 1000000 }` |
| `row_count_change` | Row count change vs previous run | `row_count_change: { max_decrease: 10% }` |
| `anomaly` | Statistical anomaly detection | `anomaly: { column: amount, method: zscore }` |
| `custom_sql` | Custom SQL expression returns true | `custom_sql: { sql: "COUNT(*) > 0" }` |

## Configuration

A `provero.yaml` file defines your data source, checks, alerts, and contracts:

```yaml
# Source configuration
source:
  type: duckdb                    # duckdb, postgres, dataframe
  table: orders                   # table name or file expression
  # connection: postgres://...    # connection string for databases

# Quality checks
checks:
  - not_null: [order_id, customer_id]
  - unique: order_id
  - range:
      column: amount
      min: 0
      max: 100000
  - freshness:
      column: updated_at
      max_age: 24h
  - anomaly:
      column: amount
      method: zscore               # zscore, mad, iqr
      threshold: 3.0
      window: 30                   # lookback window in days

# Severity levels: info, warning, critical, blocker
# Blocker checks cause a non-zero exit code

# Alert notifications
alerts:
  - type: webhook
    url: https://hooks.slack.com/services/YOUR/WEBHOOK
    trigger: on_failure            # on_failure, on_success, always

# Data contracts (optional)
contracts:
  - name: orders_contract
    owner: data-team
    table: orders
    schema:
      columns:
        - name: order_id
          type: integer
          checks: [not_null, unique]
    sla:
      freshness: 24h
```

## Anomaly Detection

Provero includes built-in statistical anomaly detection that works without external dependencies (no scipy needed).

**Supported methods:**

| Method | Description | Best for |
|--------|-------------|----------|
| `zscore` | Standard Z-Score | Normally distributed metrics |
| `mad` | Median Absolute Deviation | Robust to outliers |
| `iqr` | Interquartile Range | Skewed distributions |

```yaml
checks:
  - anomaly:
      column: daily_revenue
      method: mad
      threshold: 3.5
      window: 30
```

Anomaly detection uses the result store to compare current values against historical data. Run `provero run` regularly to build up the baseline.

## CLI Commands

| Command | Description |
|---------|-------------|
| `provero init` | Create a new provero.yaml template |
| `provero run` | Execute quality checks |
| `provero validate` | Validate config syntax without running |
| `provero profile` | Profile a data source |
| `provero history` | Show historical check results |
| `provero contract validate` | Validate data contracts against live data |
| `provero contract diff` | Compare two contract versions |
| `provero version` | Show version |

## Alerts

Send webhook notifications when checks fail:

```yaml
source:
  type: duckdb
  table: orders

checks:
  - not_null: order_id
  - row_count:
      min: 1

alerts:
  - type: webhook
    url: https://hooks.slack.com/services/YOUR/WEBHOOK/URL
    trigger: on_failure
  - type: webhook
    url: ${PAGERDUTY_WEBHOOK}
    headers:
      Authorization: "Bearer ${PD_TOKEN}"
```

Triggers: `on_failure` (default), `on_success`, `always`.

## Data Contracts

Define and enforce schema contracts:

```yaml
contracts:
  - name: orders_contract
    owner: data-team
    table: orders
    on_violation: warn
    schema:
      columns:
        - name: order_id
          type: integer
          checks: [not_null, unique]
        - name: status
          type: varchar
    sla:
      freshness: 24h
      completeness: "95%"
```

```bash
provero contract validate
provero contract diff old.yaml new.yaml
```

## Connectors

| Connector   | Status   | Install                          |
|-------------|----------|----------------------------------|
| DuckDB      | Stable   | included                         |
| PostgreSQL  | Stable   | `pip install provero[postgres]`    |
| DataFrame   | Stable   | `pip install provero[dataframe]`   |

DuckDB supports file expressions: `read_csv('data.csv')`, `read_parquet('*.parquet')`.

## API

### Python API

```python
from provero.core.engine import Engine

engine = Engine("provero.yaml")
results = engine.run()

for result in results:
    print(f"{result.check_name}: {result.status}")
```

### Programmatic Configuration

```python
from provero.core.engine import Engine

engine = Engine.from_dict({
    "source": {"type": "duckdb", "table": "orders"},
    "checks": [
        {"not_null": "order_id"},
        {"row_count": {"min": 1}},
    ],
})
results = engine.run()
```

## Airflow Integration

```python
from provero.airflow.operators import ProveroCheckOperator

check_orders = ProveroCheckOperator(
    task_id="check_orders",
    config_path="dags/provero.yaml",
    suite="orders_daily",
)
```

## Documentation

**[Full documentation](https://provero-org.github.io/provero/)** is available on GitHub Pages.

- [Getting Started](https://provero-org.github.io/provero/getting-started/)
- [Configuration](https://provero-org.github.io/provero/configuration/)
- [Check Types](https://provero-org.github.io/provero/checks/)
- [Connectors](https://provero-org.github.io/provero/connectors/)
- [CLI Reference](https://provero-org.github.io/provero/cli/)
- [Architecture](docs/ARCHITECTURE.md)
- [Contributing](CONTRIBUTING.md)
- [Governance](GOVERNANCE.md)
- [Security Policy](SECURITY.md)
- [Support](SUPPORT.md)

## License

Apache License 2.0. See [LICENSE](LICENSE).
