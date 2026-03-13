# Provero

> **provero** /ˈæseɪ/ - the testing of a substance to determine its quality or purity.

A vendor-neutral, declarative data quality engine.

## Quickstart

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
| MySQL       | Beta     | `pip install provero` (SQLAlchemy) |
| SQLite      | Beta     | `pip install provero` (SQLAlchemy) |
| Snowflake   | Beta     | `pip install provero[snowflake]`   |
| BigQuery    | Beta     | `pip install provero[bigquery]`    |
| Redshift    | Beta     | `pip install provero[redshift]`    |

DuckDB supports file expressions: `read_csv('data.csv')`, `read_parquet('*.parquet')`.

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

- [AQL Specification](aql-spec/spec.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Contributing](CONTRIBUTING.md)
- [Development Plan](DEVELOPMENT_PLAN.md)

## License

Apache License 2.0. See [LICENSE](LICENSE).
