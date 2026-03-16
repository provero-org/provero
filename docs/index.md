# Provero

> **provero** (Esperanto): to test, to put to proof.

A vendor-neutral, declarative data quality engine.

<p align="center">
  <img src="assets/demo.gif" alt="Provero demo" width="700">
</p>

## Why Provero?

- **14 check types** covering completeness, uniqueness, validity, freshness, volume, anomaly detection, and custom SQL
- **3 connectors**: DuckDB (files + in-memory), PostgreSQL, Pandas/Polars DataFrame
- **SQL batch optimizer** compiles N checks into 1 query
- **Data contracts** with schema validation, SLA enforcement, and contract diff
- **Anomaly detection** using Z-Score, MAD, IQR (stdlib only, no scipy needed)
- **HTML reports** via `provero run --report html`
- **Webhook alerts** for Slack, PagerDuty, or any HTTP endpoint
- **Result store** with SQLite time-series metrics and `provero history`
- **Data profiling** with `provero profile --suggest` to auto-generate checks
- **Configurable severity**: info, warning, critical, blocker per check
- **JSON Schema validation** for `provero.yaml`
- **Airflow provider**: `ProveroCheckOperator` + `@provero_check` decorator

## Quick Install

```bash
pip install provero
```

## Minimal Example

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

## Next Steps

- [Getting Started](getting-started.md) for a full walkthrough
- [Configuration](configuration.md) for all config options
- [Check Types](checks.md) for the complete check reference
- [Connectors](connectors.md) for database setup
- [CLI Reference](cli.md) for all commands and flags
