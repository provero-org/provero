# Connectors

Provero supports three built-in connectors. Additional connectors can be added through the plugin system via Python entry points.

| Connector | Status | Install |
|-----------|--------|---------|
| DuckDB | Stable | included |
| PostgreSQL | Stable | `pip install provero[postgres]` |
| DataFrame | Stable | `pip install provero[dataframe]` |

## DuckDB

DuckDB is the default connector and ships with Provero. It supports both in-memory databases and file-based databases, and can read CSV, Parquet, and JSON files directly.

### Setup

No additional installation is needed. DuckDB is bundled with the core `provero` package:

```bash
pip install provero
```

### Basic Usage

Point at a table in an in-memory DuckDB instance:

```yaml
source:
  type: duckdb
  table: orders
```

### File-based Database

To use a persistent DuckDB file instead of in-memory:

```yaml
source:
  type: duckdb
  connection: warehouse.duckdb
  table: orders
```

### File Expressions

DuckDB supports file expressions as the `table` value, letting you query files directly without loading them into a database. This is one of the most powerful features for quick data quality checks on local files.

#### CSV Files

```yaml
source:
  type: duckdb
  table: read_csv('data/orders.csv')
```

You can also use glob patterns to read multiple CSV files:

```yaml
source:
  type: duckdb
  table: read_csv('data/orders_*.csv')
```

#### Parquet Files

```yaml
source:
  type: duckdb
  table: read_parquet('data/orders.parquet')
```

Glob patterns work with Parquet as well:

```yaml
source:
  type: duckdb
  table: read_parquet('data/*.parquet')
```

#### JSON Files

```yaml
source:
  type: duckdb
  table: read_json('events.json')
```

!!! tip
    File expressions are detected automatically. Provero uses `DESCRIBE SELECT * FROM <expression>` to introspect schema for expressions, and `DESCRIBE <table>` for regular table names.

## PostgreSQL

Requires the `postgres` extra.

### Setup

```bash
pip install provero[postgres]
```

This installs SQLAlchemy and the `psycopg2` driver. Provero uses SQLAlchemy under the hood, so any PostgreSQL-compatible connection string works.

### Connection String

```yaml
source:
  type: postgres
  connection: postgresql://user:password@localhost:5432/mydb
  table: orders
```

Use environment variables to avoid hardcoding credentials:

```yaml
source:
  type: postgres
  connection: ${DATABASE_URL}
  table: orders
```

### Schema-qualified Tables

Use dot notation to specify a schema. Provero queries `information_schema.columns` with the correct schema filter:

```yaml
source:
  type: postgres
  connection: ${DATABASE_URL}
  table: analytics.orders
```

### Full Format with Named Sources

In the full config format, define a named source and reference it from suites:

```yaml
version: "1.0"

sources:
  warehouse:
    type: postgres
    connection: ${WAREHOUSE_URI}

suites:
  - name: orders_quality
    source: warehouse
    table: public.orders
    checks:
      - not_null: [order_id, amount]
      - unique: order_id
```

## DataFrame

Requires the `dataframe` extra.

### Setup

```bash
pip install provero[dataframe]
```

The DataFrame connector is designed for programmatic use from Python, not from YAML configs. It registers a Pandas or Polars DataFrame as a virtual table in an in-memory DuckDB instance, allowing full SQL execution against it.

### Pandas

```python
import pandas as pd
from provero.connectors.dataframe import DataFrameConnector
from provero.core.engine import Engine

df = pd.read_csv("orders.csv")

engine = Engine.from_dict(
    config={
        "source": {"type": "dataframe", "table": "orders"},
        "checks": [
            {"not_null": "order_id"},
            {"unique": "order_id"},
            {"row_count": {"min": 1}},
        ],
    },
    dataframe=df,
    table_name="orders",
)
results = engine.run()
```

### Polars

Polars DataFrames are converted to Arrow tables and registered in DuckDB transparently:

```python
import polars as pl
from provero.connectors.dataframe import DataFrameConnector
from provero.core.engine import Engine

df = pl.read_csv("orders.csv")

engine = Engine.from_dict(
    config={
        "source": {"type": "dataframe", "table": "orders"},
        "checks": [
            {"not_null": "order_id"},
            {"row_count": {"min": 1}},
        ],
    },
    dataframe=df,
    table_name="orders",
)
results = engine.run()
```

### Using DataFrameConnector Directly

You can also use the connector directly for lower-level access:

```python
import pandas as pd
from provero.connectors.dataframe import DataFrameConnector

df = pd.read_csv("orders.csv")
connector = DataFrameConnector(df, table_name="orders")
conn = connector.connect()
result = conn.execute("SELECT COUNT(*) as cnt FROM orders")
print(result)  # [{'cnt': 1000}]
connector.disconnect(conn)
```

## Environment Variable Substitution

All connectors support environment variable substitution using the `${VAR_NAME}` syntax. Provero resolves these at runtime before creating the connection.

Two resolution strategies are used:

1. **Full variable references** like `${DATABASE_URL}` are resolved by extracting the variable name and looking it up in `os.environ`. If the variable is not set, Provero raises an error with a clear message.

2. **Inline variables** like `postgresql://${DB_USER}:${DB_PASS}@${DB_HOST}/mydb` are expanded using `os.path.expandvars`, which replaces all `$VAR` and `${VAR}` patterns in the string.

```yaml
source:
  type: postgres
  connection: ${DATABASE_URL}
  table: orders
```

```yaml
source:
  type: postgres
  connection: postgresql://${DB_USER}:${DB_PASS}@${DB_HOST}:5432/${DB_NAME}
  table: orders
```

## Plugin System

Third-party connectors can register themselves using Python entry points. Add the following to your package's `pyproject.toml`:

```toml
[project.entry-points."provero.connectors"]
mysql = "provero_mysql:MySQLConnector"
```

Plugins take priority over built-in connectors, so you can override the default behavior if needed. Provero discovers plugins at runtime via `importlib.metadata.entry_points`.

Built-in SQLAlchemy-based types (`mysql`, `sqlite`, `snowflake`, `bigquery`, `redshift`, `databricks`) are also available and use the generic `SQLAlchemyConnector` class, but require the appropriate database driver to be installed.
