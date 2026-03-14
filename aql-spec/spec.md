# Provero Quality Language (AQL) Specification

**Version:** 1.0-draft
**Status:** Draft
**License:** Apache License 2.0

## Overview

AQL (Provero Quality Language) is a declarative YAML-based language for defining
data quality rules. It is designed to be vendor-neutral, human-readable,
and machine-parseable.

AQL files (typically named `provero.yaml`) describe what quality checks to run
against which data sources. The language separates *what* to check from *how*
to execute, allowing the same rules to run across different engines and
connectors.

## File Structure

An AQL file supports two formats:

### Simple Format

For single-source configurations:

```yaml
version: "1.0"
source:
  type: duckdb
  table: orders
checks:
  - not_null: [id, name]
  - unique: id
  - row_count:
      min: 1
```

### Suite Format

For multi-suite, multi-source configurations:

```yaml
version: "1.0"
sources:
  warehouse:
    type: postgres
    connection: ${POSTGRES_URI}

suites:
  - name: orders_daily
    source: warehouse
    table: orders
    tags: [critical]
    checks:
      - not_null: [order_id, customer_id]
      - unique: order_id
      - row_count:
          min: 1000
```

## Source Definition

| Field        | Type   | Required | Description                          |
|-------------|--------|----------|--------------------------------------|
| `type`      | string | yes      | Connector type (duckdb, postgres, etc.) |
| `connection`| string | no       | Connection string or `${ENV_VAR}`    |
| `table`     | string | no       | Default table name                   |
| `conn_id`   | string | no       | Airflow connection ID                |

Supported types: `duckdb`, `postgres`, `postgresql`, `dataframe`, `pandas`, `polars`.

## Check Types

### Completeness

| Check          | Syntax                                  | Description                        |
|---------------|----------------------------------------|------------------------------------|
| `not_null`    | `not_null: column` or `not_null: [a, b]`| No null values in column(s)       |
| `completeness`| `completeness: {column: x, min: 0.95}` | Minimum non-null ratio             |

### Uniqueness

| Check                | Syntax                                       | Description                    |
|---------------------|----------------------------------------------|-------------------------------|
| `unique`            | `unique: column`                              | All values are unique         |
| `unique_combination`| `unique_combination: [col_a, col_b]`          | Combination is unique         |

### Validity

| Check             | Syntax                                            | Description                     |
|------------------|--------------------------------------------------|---------------------------------|
| `accepted_values` | `accepted_values: {column: x, values: [a, b]}`   | Values in allowed set          |
| `range`           | `range: {column: x, min: 0, max: 100}`           | Values within numeric range    |
| `regex`           | `regex: {column: x, pattern: "^[A-Z]+"}`         | Values match regex pattern     |
| `type`            | `type: {column: x, expected: integer}`            | Column has expected data type  |

### Freshness

| Check       | Syntax                                                       | Description                     |
|------------|-------------------------------------------------------------|---------------------------------|
| `freshness`| `freshness: {column: ts, max_age: 24h}`                      | Most recent row within max_age |
| `latency`  | `latency: {source_column: x, target_column: y, max_latency: 1h}` | Pipeline latency within bounds |

### Volume

| Check       | Syntax                              | Description                |
|------------|-------------------------------------|----------------------------|
| `row_count` | `row_count: {min: 1, max: 10000}`  | Row count within range     |

### Custom

| Check        | Syntax                                         | Description                  |
|-------------|------------------------------------------------|------------------------------|
| `custom_sql` | `custom_sql: "SELECT COUNT(*)=0 FROM ..."`     | Custom SQL returning boolean |

## Severity

Every check has a severity level that controls how failures are reported.

| Level      | Description                                  |
|-----------|----------------------------------------------|
| `info`    | Informational, does not affect suite status   |
| `warning` | Warning, logged but suite can still pass      |
| `critical`| Failure blocks the suite (default)            |
| `blocker` | Failure blocks downstream pipelines           |

Severity can be set per-check:

```yaml
checks:
  - range:
      column: amount
      min: 0
      severity: warning
```

## Duration Strings

Freshness and latency checks accept duration strings:

| Suffix | Meaning  | Example |
|--------|----------|---------|
| `s`    | seconds  | `30s`   |
| `m`    | minutes  | `15m`   |
| `h`    | hours    | `24h`   |
| `d`    | days     | `7d`    |

## Contracts

Contracts define data quality SLAs between producers and consumers:

```yaml
contracts:
  - name: orders_contract
    source:
      type: postgres
      connection: ${POSTGRES_URI}
    owner: data-team
    on_violation: block
```

Violation actions: `block`, `warn`, `quarantine`.

## JSON Schema

A formal JSON Schema for AQL validation is provided at `schema.json`
in this directory. It follows JSON Schema draft 2020-12.
