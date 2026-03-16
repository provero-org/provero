# Data Contracts

Data contracts let you define and enforce expectations about your data's schema, quality, and freshness as a formal agreement between data producers and consumers. Provero validates contracts against live data sources and detects schema drift, SLA violations, and per-column quality issues.

## What Are Data Contracts?

A data contract is a YAML definition that specifies:

- **Schema**: which columns must exist and their expected types.
- **Column checks**: quality rules per column (not_null, unique, range, accepted_values, etc.).
- **SLAs**: service level agreements for freshness, completeness, and availability.
- **Violation policy**: what happens when the contract is broken (warn, block, or quarantine).

Contracts live inside your `provero.yaml` alongside regular check suites.

## Defining a Contract

```yaml
contracts:
  - name: orders_contract
    owner: data-team
    version: "1.0"
    table: orders
    on_violation: warn
    schema:
      columns:
        - name: order_id
          type: integer
          checks: [not_null, unique]
        - name: customer_id
          type: integer
          checks: [not_null]
        - name: amount
          type: float
          checks:
            - not_null
            - range:
                min: 0.01
        - name: status
          type: varchar
          checks:
            - accepted_values: [pending, shipped, delivered, cancelled]
        - name: created_at
          type: timestamp
    sla:
      freshness: 24h
      completeness: "95%"
      availability: "true"
```

### Contract Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | | Unique contract identifier |
| `owner` | string | No | | Team or person responsible for the data |
| `version` | string | No | `"1.0"` | Contract version for tracking changes |
| `table` | string | Yes | | Table the contract applies to |
| `source` | string | No | | Named source reference (from `sources:` block) |
| `on_violation` | string | No | `warn` | Action on violation: `block`, `warn`, `quarantine` |
| `schema.columns` | array | No | | Column definitions |
| `sla` | object | No | | Service level agreements |

## Schema Validation

Schema validation compares the columns defined in the contract against the actual table schema. Provero detects three types of drift:

| Drift Type | Description | Example |
|------------|-------------|---------|
| `removed` | A column in the contract is missing from the table | Contract expects `email`, table does not have it |
| `added` | The table has a column not in the contract | Table has `phone` but contract does not define it |
| `type_changed` | A column exists but its type differs from the contract | Contract says `integer`, table has `varchar` |

Type comparison is flexible. Provero normalizes types across databases, so `int`, `bigint`, `int4`, and `smallint` all match `integer`. Parameterized types like `decimal(10,2)` match `decimal`.

### Column Checks

Each column in the contract can have a list of checks. These use the same check types available for regular suites:

```yaml
schema:
  columns:
    - name: price
      type: float
      checks:
        - not_null
        - range:
            min: 0.01
            max: 99999.99
    - name: currency
      type: varchar
      checks:
        - accepted_values: [USD, EUR, GBP]
```

Checks can be written as simple strings (`not_null`, `unique`) or as dictionaries with parameters.

## SLA Enforcement

SLAs define operational expectations for the data. Provero validates three SLA types:

### Freshness

Checks that the most recent data is within a time threshold. Provero automatically finds the first timestamp/datetime/date column in the table and compares its maximum value against the current time.

```yaml
sla:
  freshness: 24h
```

Supported formats: `30m` (minutes), `24h` (hours), `7d` (days).

### Completeness

Checks that the overall non-null ratio across all contract columns meets a minimum threshold. Provero queries `COUNT(*)` and `COUNT(column)` for each contract column, then computes the aggregate ratio.

```yaml
sla:
  completeness: "95%"
```

### Availability

A simple check that the table exists and has at least one row.

```yaml
sla:
  availability: "true"
```

## Violation Actions

The `on_violation` field controls the severity assigned to contract violations:

| Action | Behavior |
|--------|----------|
| `warn` | Violations are reported as warnings. The overall status is `warn` unless a critical violation exists. |
| `block` | Violations are treated as critical. Any violation causes the contract to fail with `fail` status. |
| `quarantine` | Log warning and mark data for review. Violations get `warning` severity. Intended for pipelines that route failing data to a quarantine table. |

```yaml
contracts:
  - name: production_orders
    on_violation: block
    # ...
```

## Validating Contracts

Run contract validation against live data:

```bash
provero contract validate
```

```bash
provero contract validate -c production.yaml
```

This connects to the data source, retrieves the actual schema, runs SLA checks, and executes per-column checks.

## Contract Diffing

Compare two versions of a contract to understand what changed and whether the changes are breaking:

```bash
provero contract diff v1.yaml v2.yaml
```

The diff reports:

| Change Type | Breaking? | Example |
|-------------|-----------|---------|
| Column added | No | New column `phone` added to contract |
| Column removed | Yes | Column `email` removed from contract |
| Column type changed | Yes | `order_id` changed from `integer` to `varchar` |
| Check added to column | Yes | New `not_null` check on `status` |
| Check removed from column | No | Removed `unique` check from `email` |
| SLA changed | Yes (if stricter) | Freshness changed from `48h` to `24h` |
| Table changed | Yes | Table changed from `orders` to `orders_v2` |
| Owner changed | No | Owner changed from `data-team` to `platform-team` |
| Violation action changed | Conditional | Breaking if changed to `block` |

## Complete Example

```yaml
version: "1.0"

sources:
  warehouse:
    type: postgres
    connection: ${DATABASE_URL}

contracts:
  - name: orders_contract
    owner: data-team
    version: "2.0"
    table: public.orders
    on_violation: block
    schema:
      columns:
        - name: order_id
          type: integer
          checks: [not_null, unique]
        - name: customer_id
          type: integer
          checks: [not_null]
        - name: amount
          type: decimal
          checks:
            - not_null
            - range:
                min: 0.01
        - name: status
          type: varchar
          checks:
            - accepted_values: [pending, shipped, delivered, cancelled]
        - name: created_at
          type: timestamp
          description: Order creation timestamp
    sla:
      freshness: 12h
      completeness: "99%"
      availability: "true"

suites:
  - name: orders_quality
    source: warehouse
    table: public.orders
    checks:
      - not_null: [order_id, customer_id, amount]
      - unique: order_id
      - row_count:
          min: 1
      - freshness:
          column: created_at
          max_age: 24h
```
