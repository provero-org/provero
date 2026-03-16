# Connectors

| Connector | Status | Install |
|-----------|--------|---------|
| DuckDB | Stable | included |
| PostgreSQL | Stable | `pip install provero[postgres]` |
| DataFrame | Stable | `pip install provero[dataframe]` |

## DuckDB

DuckDB is the default connector and ships with Provero. It supports both in-memory databases and file-based databases, and can read CSV, Parquet, and JSON files directly.

### Basic Usage

```yaml
source:
  type: duckdb
  table: orders
```

### File Expressions

DuckDB supports file expressions as the `table` value, letting you query files directly without loading them into a database:

```yaml
source:
  type: duckdb
  table: read_csv('data/orders.csv')
```

```yaml
source:
  type: duckdb
  table: read_parquet('data/*.parquet')
```

```yaml
source:
  type: duckdb
  table: read_json('events.json')
```

### File-based Database

To use a persistent DuckDB file instead of in-memory:

```yaml
source:
  type: duckdb
  connection: warehouse.duckdb
  table: orders
```

## PostgreSQL

Requires the `postgres` extra: `pip install provero[postgres]`.

Uses SQLAlchemy under the hood, so any PostgreSQL-compatible connection string works.

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

Use dot notation to specify a schema:

```yaml
source:
  type: postgres
  connection: ${DATABASE_URL}
  table: analytics.orders
```

## DataFrame

Requires the `dataframe` extra: `pip install provero[dataframe]`.

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
