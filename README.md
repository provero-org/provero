# Apache Assay

> **assay** /ˈæseɪ/ - the testing of a substance to determine its quality or purity.

A vendor-neutral, declarative data quality engine with built-in anomaly detection.

## Quickstart

```bash
pip install apache-assay
assay init
```

Edit `assay.yaml`:

```yaml
source:
  type: duckdb
  table: read_csv('data/orders.csv')

checks:
  - not_null: [order_id, customer_id, amount]
  - unique: order_id
  - range:
      column: amount
      min: 0
      max: 100000
  - row_count:
      min: 1
```

Run:

```bash
assay run
```

```
┌─────────────────┬──────────────┬──────────┬──────────────┬──────────────┐
│ Check           │ Column       │ Status   │ Observed     │ Expected     │
├─────────────────┼──────────────┼──────────┼──────────────┼──────────────┤
│ not_null        │ order_id     │ ✓ PASS   │ 0 nulls      │ 0 nulls      │
│ not_null        │ customer_id  │ ✓ PASS   │ 0 nulls      │ 0 nulls      │
│ unique          │ order_id     │ ✓ PASS   │ 0 duplicates │ 0 duplicates │
│ range           │ amount       │ ✓ PASS   │ min=45, max= │ min=0, max=  │
│                 │              │          │ 999.99       │ 100000       │
│ row_count       │ -            │ ✓ PASS   │ 5            │ >= 1         │
└─────────────────┴──────────────┴──────────┴──────────────┴──────────────┘

Score: 100/100 | 5 passed, 0 failed | 12ms
```

## Features

- **Declarative YAML** - Define checks in 3 lines, not 50
- **Built-in anomaly detection** - Z-Score, MAD, IQR, no SaaS required
- **Data contracts** - Producers declare, consumers verify
- **Airflow provider** - First-class integration
- **Streaming support** - Kafka, Kinesis (batch + real-time)
- **Vendor-neutral** - Works with Postgres, Snowflake, BigQuery, DuckDB, Parquet, CSV
- **AQL standard** - Portable rules that work across tools

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Development Plan](DEVELOPMENT_PLAN.md)
- [Contributing](CONTRIBUTING.md)

## License

Apache License 2.0. See [LICENSE](LICENSE).
