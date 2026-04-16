# Parser

The parser converts a `provero.yaml` file into a typed, validated
`ProveroConfig` object. Nothing else in the codebase reads YAML directly:
every other component starts from the parsed config.

- **File**: `provero-core/src/provero/core/compiler.py`
- **Entry point**: `compile_file(path) -> ProveroConfig`
- **Validation**: [Pydantic v2](https://docs.pydantic.dev/) models

---

## The Four Nested Models

The parser produces four nested Pydantic models:

```python
ProveroConfig
  ├── sources:    dict[str, SourceConfig]
  ├── suites:     list[SuiteConfig]
  │                 ├── source: SourceConfig
  │                 └── checks: list[CheckConfig]
  ├── contracts:  list[ContractConfig]
  └── alerts:     list[AlertConfig]
```

### `SourceConfig`

Holds configuration for one data source. It is pure data: no connection is
opened here.

```python
class SourceConfig(BaseModel):
    type: str                # "duckdb", "postgres", "mysql", ...
    connection: str = ""     # "${POSTGRES_URI}" or a literal string
    table: str = ""          # table name or DuckDB read_* expression
    conn_id: str = ""        # Airflow connection ID (when running via provider)
```

### `CheckConfig`

A single quality check. Every check, no matter how complex, is normalized
into this shape:

```python
class CheckConfig(BaseModel):
    check_type: str                   # "not_null", "unique", "range", ...
    column: str | None = None
    columns: list[str] = []
    params: dict[str, Any] = {}       # free-form per-check parameters
    severity: str | None = None       # "info" | "warning" | "critical" | "blocker"
```

### `SuiteConfig`

A collection of checks against one source, plus execution metadata:

```python
class SuiteConfig(BaseModel):
    name: str
    source: SourceConfig
    checks: list[CheckConfig] = []
    tags: list[str] = []
    schedule: str | None = None       # cron expression for standalone mode
```

### `ProveroConfig`

The root. It also holds `contracts` and `alerts` which are handed to their
respective subsystems after parsing.

---

## Two Supported YAML Shapes

The parser accepts two top-level shapes and normalizes both into the same
`ProveroConfig`.

### Simple shape

A single source and a flat check list at the root. The parser wraps them
into a single suite named after the file stem (e.g. `orders.yaml` becomes a
suite called `orders`).

```yaml
source:
  type: duckdb
  table: orders
checks:
  - not_null: order_id
  - unique: order_id
```

### Full shape

A `sources:` map (named, reusable) plus a `suites:` list that references
them by name.

```yaml
sources:
  warehouse:
    type: postgres
    connection: ${POSTGRES_URI}

suites:
  - name: orders_daily
    source: warehouse
    table: orders
    checks:
      - not_null: order_id
```

### Source reference validation

When a suite references a source that was not declared, `compile_file`
raises a `ValueError` that lists every available source name. The same
check runs for the simple shape as well.

When a suite sets its own `table`, the parser uses Pydantic's
`model_copy(update={"table": ...})` to produce a new `SourceConfig` without
mutating the shared one. This matters because the same named source can be
referenced by many suites with different tables.

---

## Check Parsing: Four Shorthand Forms

The `parse_check()` function normalizes four very different YAML shapes
into the single `CheckConfig` model:

| YAML you write | Parsed into |
|----------------|-------------|
| `"not_null: order_id"` (string) | `{check_type: "not_null", column: "order_id"}` |
| `{not_null: [a, b, c]}` | `{check_type: "not_null", columns: [a, b, c]}` |
| `{range: {column: amount, min: 0, max: 100}}` | `{check_type: "range", column: "amount", params: {min: 0, max: 100}}` |
| `{custom_sql: "SELECT ..."}` | `{check_type: "custom_sql", params: {"query": "..."}}` |

When a check has a dict value, the parser pulls `column`, `columns`, and
`severity` out as first-class fields and keeps everything else in `params`.

Anything the parser cannot interpret raises a `ValueError` with an example
of the expected syntax.

---

## Contract and Alert Parsing

Two helper functions, `_parse_contracts()` and `_parse_alerts()`, handle
the optional `contracts:` and `alerts:` blocks. They mirror the check
parser: a YAML block is translated into a typed Pydantic model
(`ContractConfig` or `AlertConfig`) with sensible defaults.

These are imported lazily inside the helpers so that projects using neither
feature do not pay the import cost at parse time.

---

## Programmatic Configs

`Engine.from_dict()` (in [`core/engine.py`](engine.md)) accepts the exact
same YAML shape as an in-memory dict. It reuses `parse_check()` directly,
so shorthand and long-form definitions are supported without
pre-processing. This is handy for tests and for building configs
dynamically at runtime.

---

## Why Pydantic, Not TypedDict

Pydantic validates types and coerces values at parse time. That means a
malformed check fails during `compile_file()`, before any connection is
opened. A broken YAML file never reaches the engine, and the error points
to the exact field that failed.

Pydantic also gives the entire codebase a single source of truth for the
shape of the config: every downstream component imports the same
`CheckConfig` or `SuiteConfig` class, so a new field added to the model is
immediately visible everywhere.
