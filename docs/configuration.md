# Configuration

Provero uses a `provero.yaml` file to define data sources, quality checks, alerts, and contracts. There are two formats: **simple** (single source) and **full** (multiple suites).

## Simple Format

Best for single-table validation:

```yaml
source:
  type: duckdb
  table: orders

checks:
  - not_null: [order_id, customer_id]
  - unique: order_id
  - row_count:
      min: 1
```

## Full Format

For multi-table pipelines with named sources and suites:

```yaml
version: "1.0"

sources:
  warehouse:
    type: duckdb
    # type: postgres
    # connection: ${WAREHOUSE_URI}

suites:
  - name: orders_quality
    source: warehouse
    table: orders
    tags: [critical, daily]
    checks:
      - not_null: [order_id, customer_id, order_date, total_amount]
      - unique: order_id
      - row_count:
          min: 1
      - range:
          column: total_amount
          min: 0
          max: 100000
      - freshness:
          column: order_date
          max_age: 24h

  - name: customers_quality
    source: warehouse
    table: customers
    tags: [critical, daily]
    checks:
      - not_null: [customer_id, email, created_at]
      - unique: customer_id
      - unique: email
      - regex:
          column: email
          pattern: "^[^@]+@[^@]+\\.[^@]+$"
```

## Source Configuration

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | **Required.** Connector type: `duckdb`, `postgres`, `dataframe` |
| `table` | string | Table name or DuckDB expression (e.g., `read_csv('file.csv')`) |
| `connection` | string | Connection string for databases. Supports `${ENV_VAR}` |
| `conn_id` | string | Airflow connection ID (for Airflow provider) |

## Severity Levels

Each check has a severity level that determines behavior on failure:

| Level | Exit Code | Description |
|-------|-----------|-------------|
| `info` | 0 | Informational, does not affect exit code |
| `warning` | 0 | Warning, logged but does not affect exit code |
| `critical` | 1 | Failure causes non-zero exit code (default for most checks) |
| `blocker` | 1 | Same as critical, indicates a blocking issue |

Override severity per check:

```yaml
checks:
  - range:
      column: price
      min: 0
      severity: warning
  - completeness:
      column: description
      min: 0.80
      severity: warning
```

## Alerts

Send webhook notifications when checks fail:

```yaml
alerts:
  - type: webhook
    url: https://hooks.slack.com/services/YOUR/WEBHOOK/URL
    trigger: on_failure
  - type: webhook
    url: ${PAGERDUTY_WEBHOOK}
    headers:
      Authorization: "Bearer ${PD_TOKEN}"
    trigger: always
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | `webhook` | Alert type (currently only `webhook`) |
| `url` | string | | **Required.** Webhook URL. Supports `${ENV_VAR}` |
| `trigger` | string | `on_failure` | When to fire: `on_failure`, `on_success`, `always` |
| `headers` | object | | HTTP headers. Values support `${ENV_VAR}` |

## Data Contracts

Define and enforce schema contracts on your data:

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

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | **Required.** Contract identifier |
| `owner` | string | Team or person responsible |
| `table` | string | Table the contract applies to |
| `source` | string/object | Named source reference or inline source config |
| `on_violation` | string | Action on violation: `block`, `warn`, `quarantine` |
| `schema.columns` | array | Column definitions with `name`, `type`, and optional `checks` |
| `sla.freshness` | string | Maximum data age (e.g., `24h`) |
| `sla.completeness` | string | Minimum completeness percentage (e.g., `"95%"`) |

Validate contracts against live data:

```bash
provero contract validate
```

Compare two contract versions:

```bash
provero contract diff old.yaml new.yaml
```

## Environment Variables

Use `${VAR_NAME}` syntax anywhere in the config to reference environment variables:

```yaml
source:
  type: postgres
  connection: ${DATABASE_URL}

alerts:
  - type: webhook
    url: ${SLACK_WEBHOOK}
    headers:
      Authorization: "Bearer ${SLACK_TOKEN}"
```

## JSON Schema

Provero ships a JSON Schema for `provero.yaml`. Enable IDE autocomplete by adding a schema reference:

```yaml
# yaml-language-server: $schema=https://github.com/provero-org/provero/raw/main/aql-spec/schema.json
source:
  type: duckdb
  table: orders
checks:
  - not_null: order_id
```

Validate your config against the schema without running checks:

```bash
provero validate --schema-only
```
