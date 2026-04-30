# Plugin Development

Provero supports two types of plugins: **custom checks** and **custom connectors**. Both use Python entry points for discovery, so your plugin is a standard Python package that users install with `pip`.

## How plugins are discovered

When Provero starts, it scans two entry point groups:

- `provero.checks` for check plugins
- `provero.connectors` for connector plugins

Plugins are loaded lazily on first use. Built-in checks cannot be overridden by plugins (supply-chain protection), but connector plugins can override built-in connectors.

## Creating a custom check

A check is a function that receives a database connection, a table name, and a config object, then returns a `CheckResult`.

### Step 1: Write the check function

```python
# provero_percentile/checks.py

from provero.checks.registry import register_check
from provero.connectors.base import Connection
from provero.core.compiler import CheckConfig
from provero.core.results import CheckResult, Status


@register_check("percentile")
def check_percentile(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that a column's Nth percentile is within a range."""
    column = check_config.column
    p = check_config.params.get("p", 50)
    min_val = check_config.params.get("min")
    max_val = check_config.params.get("max")

    query = f"SELECT percentile_cont({p / 100}) WITHIN GROUP (ORDER BY {column}) AS pct FROM {table}"
    rows = connection.execute(query)
    observed = rows[0]["pct"]

    passed = True
    if min_val is not None and observed < min_val:
        passed = False
    if max_val is not None and observed > max_val:
        passed = False

    return CheckResult(
        check_name=f"percentile:{column}",
        check_type="percentile",
        status=Status.PASS if passed else Status.FAIL,
        severity=check_config.severity,
        column=column,
        observed_value=observed,
        expected_value=f"p{p} between {min_val} and {max_val}",
    )
```

### Step 2: Register via entry points

In your package's `pyproject.toml`:

```toml
[project]
name = "provero-percentile"
version = "0.1.0"
dependencies = ["provero>=0.2.0"]

[project.entry-points."provero.checks"]
percentile = "provero_percentile.checks:check_percentile"
```

The entry point name (`percentile`) is the check type users write in YAML. The value points to your decorated function.

### Step 3: Install and use

```bash
pip install provero-percentile
```

```yaml
source:
  type: duckdb
  table: orders

checks:
  - percentile:
      column: amount
      p: 95
      min: 0
      max: 5000
```

```bash
provero run
```

### Check function signature

Every check function must accept these three arguments:

| Parameter | Type | Description |
|-----------|------|-------------|
| `connection` | `Connection` | Database connection with `.execute(query)` method |
| `table` | `str` | Table name (may be a DuckDB expression like `read_parquet(...)`) |
| `check_config` | `CheckConfig` | Contains `check_type`, `column`, `columns`, `params`, `severity` |

And return a `CheckResult`:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `check_name` | `str` | yes | Display name (typically `type:column`) |
| `check_type` | `str` | yes | Must match your registered name |
| `status` | `Status` | yes | `PASS`, `FAIL`, `WARN`, `ERROR`, or `SKIP` |
| `severity` | `Severity` | no | Defaults to `CRITICAL`. Use `check_config.severity` to respect user config |
| `column` | `str \| None` | no | Column being checked |
| `observed_value` | `Any` | no | The actual measured value |
| `expected_value` | `Any` | no | What was expected |
| `row_count` | `int` | no | Total rows evaluated |
| `failing_rows` | `int` | no | Number of rows that failed |
| `failing_rows_query` | `str` | no | SQL query to fetch failing rows (Provero will add `LIMIT 5`) |

### Accessing check parameters

User-provided YAML parameters are available in `check_config.params` as a dict:

```yaml
checks:
  - percentile:
      column: amount
      p: 95
      min: 0
      max: 5000
```

```python
check_config.column    # "amount"
check_config.params    # {"p": 95, "min": 0, "max": 5000}
```

For multi-column checks (e.g. `not_null: [a, b, c]`), use `check_config.columns`.

## Creating a custom connector

A connector manages database connections. You implement two protocols: `Connection` and `Connector`.

### Step 1: Implement the protocols

```python
# provero_clickhouse/connector.py

from typing import Any

from provero.connectors.base import Connection, Connector


class ClickHouseConnection:
    """Wraps a ClickHouse client as a Provero Connection."""

    def __init__(self, client):
        self._client = client

    def execute(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        result = self._client.query(query)
        columns = result.column_names
        return [dict(zip(columns, row)) for row in result.result_rows]

    def get_columns(self, table: str) -> list[dict[str, Any]]:
        rows = self.execute(
            f"SELECT name, type, is_in_primary_key "
            f"FROM system.columns WHERE table = '{table}'"
        )
        return [
            {"name": r["name"], "type": r["type"], "nullable": True}
            for r in rows
        ]


class ClickHouseConnector:
    """Provero connector for ClickHouse."""

    def __init__(self, connection: str | None = None, **kwargs):
        self._connection_string = connection
        self._kwargs = kwargs

    def connect(self) -> ClickHouseConnection:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            dsn=self._connection_string,
            **self._kwargs,
        )
        return ClickHouseConnection(client)

    def disconnect(self, connection: ClickHouseConnection) -> None:
        pass

    def get_schema(
        self,
        connection: ClickHouseConnection,
        table: str,
    ) -> list[dict[str, Any]]:
        return connection.get_columns(table)

    def get_profile(
        self,
        connection: ClickHouseConnection,
        table: str,
        columns: list[str] | None = None,
        sample_size: int | None = None,
    ) -> dict[str, Any]:
        row_count = connection.execute(f"SELECT count() AS cnt FROM {table}")
        return {"row_count": row_count[0]["cnt"]}
```

### Step 2: Register via entry points

```toml
[project]
name = "provero-clickhouse"
version = "0.1.0"
dependencies = [
    "provero>=0.2.0",
    "clickhouse-connect>=0.7",
]

[project.entry-points."provero.connectors"]
clickhouse = "provero_clickhouse.connector:ClickHouseConnector"
```

The entry point name (`clickhouse`) is the `type` users write in YAML.

### Step 3: Install and use

```bash
pip install provero-clickhouse
```

```yaml
source:
  type: clickhouse
  connection: clickhouse://localhost:8123/default
  table: events

checks:
  - not_null: [event_id, timestamp]
  - row_count: { min: 1 }
```

### Connection protocol

Your connection class must implement:

| Method | Signature | Description |
|--------|-----------|-------------|
| `execute` | `(query: str, params: dict \| None) -> list[dict[str, Any]]` | Run SQL, return rows as dicts |
| `get_columns` | `(table: str) -> list[dict[str, Any]]` | Return `[{name, type, nullable}, ...]` |

### Connector protocol

Your connector class must implement:

| Method | Signature | Description |
|--------|-----------|-------------|
| `connect` | `() -> Connection` | Create and return a new connection |
| `disconnect` | `(connection) -> None` | Close a connection |
| `get_schema` | `(connection, table) -> list[dict]` | Return column metadata |
| `get_profile` | `(connection, table, columns?, sample_size?) -> dict` | Return profiling stats |

The constructor receives all fields from the YAML `source` block as keyword arguments: `type`, `connection`, `table`, and any extra fields the user adds.

## Testing your plugin

Test your plugin against Provero's check/connector interface without needing a real database:

```python
# tests/test_percentile.py

from provero.core.compiler import CheckConfig
from provero.core.results import Status

from provero_percentile.checks import check_percentile


class FakeConnection:
    def __init__(self, result):
        self._result = result

    def execute(self, query, params=None):
        return self._result

    def get_columns(self, table):
        return []


def test_percentile_pass():
    conn = FakeConnection([{"pct": 100}])
    config = CheckConfig(
        check_type="percentile",
        column="amount",
        params={"p": 95, "min": 0, "max": 500},
    )
    result = check_percentile(conn, "orders", config)
    assert result.status == Status.PASS


def test_percentile_fail_above_max():
    conn = FakeConnection([{"pct": 9999}])
    config = CheckConfig(
        check_type="percentile",
        column="amount",
        params={"p": 95, "min": 0, "max": 500},
    )
    result = check_percentile(conn, "orders", config)
    assert result.status == Status.FAIL
```

## Package structure

A typical plugin package looks like:

```
provero-percentile/
├── pyproject.toml
├── provero_percentile/
│   ├── __init__.py
│   └── checks.py
└── tests/
    └── test_percentile.py
```

## Publishing

Publish your plugin to PyPI like any Python package:

```bash
pip install build twine
python -m build
twine upload dist/*
```

Users install it alongside Provero and it works automatically:

```bash
pip install provero provero-percentile
provero run  # percentile check type is now available
```

## Listing available plugins

To see all registered checks and connectors (built-in and plugins):

```bash
provero validate  # shows available check types on error
```

Or programmatically:

```python
from provero.checks.registry import list_checks
from provero.connectors.factory import list_connectors

print(list_checks())       # ['not_null', 'unique', 'percentile', ...]
print(list_connectors())   # ['duckdb', 'postgres', 'clickhouse', ...]
```
