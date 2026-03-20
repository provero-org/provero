# Getting Started

## Installation

```bash
pip install provero
```

For additional connectors, install with extras:

| Extra | Install | Provides |
|-------|---------|----------|
| `postgres` | `pip install provero[postgres]` | PostgreSQL via SQLAlchemy |
| `dataframe` | `pip install provero[dataframe]` | Pandas/Polars DataFrame support |

## Initialize a Project

```bash
provero init
```

This creates a `provero.yaml` template in the current directory:

```yaml
# provero.yaml - Provero configuration
# Docs: https://provero-org.github.io/provero/

source:
  type: duckdb
  # type: postgres
  # connection: ${POSTGRES_URI}
  table: my_table

checks:
  - not_null: [id, name]
  - unique: id
  - row_count:
      min: 1
```

You can also generate checks automatically by profiling a live data source:

```bash
provero init --from-source duckdb:orders
```

## Write Your First Config

Edit `provero.yaml` to point at your data. Here is a complete example using a CSV file with DuckDB:

```yaml
source:
  type: duckdb
  table: read_csv('examples/quickstart/orders.csv')

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

## Run Checks

```bash
provero run
```

Provero compiles your checks into optimized SQL, executes them, and displays a results table with pass/fail status, observed vs expected values, and a quality score.

Common flags:

| Flag | Description |
|------|-------------|
| `--config`, `-c` | Config file path (default: `provero.yaml`) |
| `--format`, `-f` | Output format: `table` or `json` |
| `--suite`, `-s` | Run a specific suite |
| `--tag`, `-t` | Run suites with a specific tag |
| `--report html` | Generate an HTML report |
| `--no-store` | Don't persist results to SQLite |
| `--no-alerts` | Don't send webhook alerts |

## Validate Config

Check your configuration syntax without running any checks:

```bash
provero validate
```

This validates against the JSON Schema and compiles the config to verify semantic correctness.

## Next Steps

- [Configuration](configuration.md) for the full config reference (simple vs full format, alerts, contracts)
- [Check Types](checks.md) for all 14 check types with parameters and examples
- [Connectors](connectors.md) for PostgreSQL and DataFrame setup
- [CLI Reference](cli.md) for all commands and flags
