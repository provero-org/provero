# Connectors

A `SourceConfig` (from the [parser](parser.md)) is just configuration. The
bridge between config and a live database connection is the **connector
factory**, which discovers both built-in and third-party connectors and
hands back an instance that the engine can drive.

- **Factory**: `provero-core/src/provero/connectors/factory.py`
- **Protocol definitions**: `provero-core/src/provero/connectors/base.py`
- **Built-in implementations**: `connectors/duckdb.py`, `connectors/postgres.py`, `connectors/dataframe.py`

---

## The `Connector` Protocol

Every connector (built-in or plugin) satisfies a
[structural Protocol](https://peps.python.org/pep-0544/) defined in
`connectors/base.py`:

```python
class Connector(Protocol):
    def connect(self) -> Connection: ...
    def disconnect(self, connection: Connection) -> None: ...
    def get_schema(self, connection: Connection, table: str) -> list[dict]: ...
    def get_profile(self, connection, table, columns=None, sample_size=None) -> dict: ...
```

`get_schema` and `get_profile` have **default implementations on the protocol
itself** (Python 3.11+ allows concrete defaults in Protocols). A new
connector only needs to implement `connect`, `disconnect`, and a
`Connection` that can `execute(query)` and `get_columns(table)`.

## The `Connection` Protocol

A `Connection` is anything with two methods:

```python
def execute(query: str, params: dict | None = None) -> list[dict[str, Any]]
def get_columns(table: str) -> list[dict[str, Any]]
```

`execute` returns a list of row-dicts (column name to value), which is why
the [optimizer](optimizer.md) can index into `data[alias]` directly.
Connectors wrap native drivers (the DuckDB Python API, SQLAlchemy, etc.)
and normalize their output into this shape.

---

## `create_connector(source)` Step by Step

The factory is a single function with five resolution steps:

### 1. Expand environment variables

The `connection` string passes through `_resolve_connection()`, which only
expands explicit `${VAR}` placeholders. Bare `$VAR` is left alone. This is
a deliberate design choice: many passwords and S3 paths contain literal
`$`, and expanding bare `$VAR` would corrupt them.

If a referenced env var is not set, the factory raises `ValueError` with
the variable name, rather than silently passing an empty string to the
driver.

### 2. Reject DataFrame types from the factory

The types `dataframe`, `pandas`, and `polars` raise a `ValueError` here.
DataFrames cannot be built from config alone (there is no "pandas URL"),
so users must pass a `DataFrameConnector(df, table_name=...)` directly to
the engine.

### 3. Try plugins first

The factory loads entry points from the `provero.connectors` group. A
plugin wins over a built-in of the same name, **but plugins cannot shadow
a built-in type**: the factory explicitly skips plugin names that collide
with `_BUILTINS`. This prevents a malicious package from hijacking
`postgres` or `duckdb`.

### 4. Fall back to built-ins

The built-in map covers:

| Type | Implementation | Notes |
|------|----------------|-------|
| `duckdb` | `DuckDBConnector` | Embedded, file or in-memory |
| `postgres`, `postgresql` | `PostgresConnector` | Dedicated driver |
| `mysql`, `sqlite`, `snowflake`, `bigquery`, `redshift`, `databricks` | `SQLAlchemyConnector` | Generic SQLAlchemy dialect |

Only the first three have dedicated implementations. The others route
through a generic `SQLAlchemyConnector`.

### 5. Raise with hints on failure

An unknown type produces a helpful error listing every registered type
plus an install hint (`pip install provero-connector-<name>`). Missing
optional dependencies (e.g. Snowflake's driver) produce a
`pip install provero[snowflake]` hint via the `_INSTALL_EXTRAS` map.

---

## DuckDB Special Case

DuckDB is the only built-in where an empty connection string is valid: it
becomes `:memory:`. For every other connector, an empty connection string
raises `ValueError`. This keeps in-memory testing frictionless without
sacrificing explicitness elsewhere.

DuckDB is also how Provero reads files. A `source.type` of `duckdb` with a
`table` like `read_parquet('orders/*.parquet')` works because the [SQL
safety](sql-safety.md) layer allowlists a small set of DuckDB
table-functions.

---

## Plugin Connectors via `entry_points`

Third-party connectors register themselves in their own package:

```toml
[project.entry-points."provero.connectors"]
mongodb = "provero_mongodb:MongoConnector"
```

At runtime, Provero calls `importlib.metadata.entry_points()` to discover
them. There is no centralized registry, no server, no config file to
update. Install the package, and the new source type is available.

The plugin registry is cached in `_PLUGIN_REGISTRY`, populated lazily on
first `create_connector()` call and never reloaded during the same
process.

### Security: built-ins are protected

Inside `_load_plugins()`:

```python
for ep in entry_points(group="provero.connectors"):
    if ep.name in _BUILTINS:
        continue     # plugins cannot override built-ins
    _PLUGIN_REGISTRY[ep.name] = ep
```

A plugin named `postgres` is silently skipped. This is a
defense-in-depth measure against supply-chain attacks.

---

## Listing What Is Available

`list_connectors()` returns the union of built-in and plugin names,
deduped (the `postgresql` alias is collapsed). The CLI uses this to
produce the "Available:" line in error messages.

---

## How the Engine Uses Connectors

For each suite, the engine calls:

```python
connector = create_connector(suite.source)
connection = connector.connect()
try:
    # ... run every batch and non-batchable check on `connection` ...
finally:
    connector.disconnect(connection)
```

In **parallel mode** ([see Engine](engine.md#sequential-vs-parallel)), each
worker thread opens its own `connection` via `connector.connect()` because
most drivers (including DuckDB) are not thread-safe when sharing a
connection across threads.
