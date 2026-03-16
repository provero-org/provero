# Anomaly Detection

Provero includes built-in statistical anomaly detection that compares current metric values against historical baselines. All detection algorithms use Python's standard library only, with no dependency on scipy or other scientific computing packages.

## Overview

Anomaly detection works by collecting metric values over time through the [result store](cli.md#provero-history) and flagging values that deviate significantly from the historical pattern. This means you need to run `provero run` regularly (daily, hourly, etc.) to build up a baseline before anomaly checks become effective.

A minimum of **5 historical data points** is required before any detection method will flag anomalies. Until that threshold is reached, anomaly checks return a `SKIP` status.

## Configuration

The `anomaly` check type accepts these parameters:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `metric` | string | Yes | | Metric to monitor: `row_count`, `null_count`, `null_rate`, `distinct_count`, `mean`, `min`, `max` |
| `column` | string | No | | Column name (required for column-level metrics) |
| `method` | string | No | `mad` | Detection method: `zscore`, `mad`, `iqr` |
| `sensitivity` | string | No | `medium` | Sensitivity level: `low`, `medium`, `high` |
| `threshold` | float | No | | Direct threshold override (bypasses sensitivity mapping) |

```yaml
checks:
  - anomaly:
      metric: row_count
      method: mad
      sensitivity: medium
```

## Available Metrics

Provero queries the current value directly from the data source using SQL, then compares it against stored history.

| Metric | Requires Column | SQL Used |
|--------|----------------|----------|
| `row_count` | No | `COUNT(*)` |
| `null_count` | Yes | `COUNT(*) FILTER (WHERE column IS NULL)` |
| `null_rate` | Yes | Ratio of null rows to total rows |
| `distinct_count` | Yes | `COUNT(DISTINCT column)` |
| `mean` | Yes | `AVG(column)` |
| `min` | Yes | `MIN(column)` |
| `max` | Yes | `MAX(column)` |

## Detection Methods

### Z-Score

The Z-Score method calculates how many standard deviations the current value is from the historical mean.

**Formula:** `z = |current - mean| / stdev`

**Best for:** Metrics that follow a roughly normal distribution with stable variance.

**Limitation:** Sensitive to outliers in the historical data, since both mean and standard deviation are affected by extreme values.

```yaml
checks:
  - anomaly:
      metric: row_count
      method: zscore
      sensitivity: medium
```

### MAD (Median Absolute Deviation)

MAD is the default method. It uses the median instead of the mean and computes `MAD = median(|xi - median(x)|) * 1.4826`. The scaling factor 1.4826 makes MAD comparable to standard deviation for normally distributed data.

**Formula:** `modified_z = |current - median| / MAD`

**Best for:** Most use cases. MAD is robust to outliers in the historical data, making it a safer default than Z-Score.

```yaml
checks:
  - anomaly:
      column: daily_revenue
      metric: mean
      method: mad
      sensitivity: high
```

### IQR (Interquartile Range)

The IQR method uses the first (Q1) and third (Q3) quartiles to define acceptable bounds: `[Q1 - k*IQR, Q3 + k*IQR]`, where `k` is the threshold.

**Formula:** `IQR = Q3 - Q1`, bounds = `Q1 - k*IQR` to `Q3 + k*IQR`

**Best for:** Metrics with skewed distributions where median-based methods might still be too sensitive on one side.

```yaml
checks:
  - anomaly:
      column: order_count
      metric: distinct_count
      method: iqr
      sensitivity: low
```

## Sensitivity Levels

Sensitivity controls the threshold used by the detection algorithm. Lower thresholds are more sensitive (flag more anomalies).

| Sensitivity | Threshold | Effect |
|-------------|-----------|--------|
| `high` | 2.0 | Flags smaller deviations, more false positives |
| `medium` | 3.0 | Balanced default |
| `low` | 4.0 | Only flags large deviations, fewer false positives |

You can also set a `threshold` value directly to bypass the sensitivity mapping:

```yaml
checks:
  - anomaly:
      metric: row_count
      method: zscore
      threshold: 2.5
```

## How the Result Store Feeds History

Anomaly detection depends on the result store to track metric values across runs. Each time you run `provero run`, the engine:

1. Queries the current metric value from the data source.
2. Loads historical values from the SQLite result store.
3. Runs the detection algorithm.
4. Saves the current value to the store for future comparisons.

The result store is enabled by default and persists to a local SQLite database. You can disable it with `--no-store`, but this also disables anomaly detection.

```bash
# Normal run: persists results and enables anomaly detection
provero run

# Disable persistence (anomaly checks will SKIP)
provero run --no-store
```

Use `provero history` to inspect stored results:

```bash
provero history --suite orders_quality
```

## Row Count Change Detection

The `row_count_change` check is a simpler alternative to full anomaly detection that compares the current row count against the previous run.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `max_decrease` | string | No | `50%` | Maximum allowed decrease percentage |
| `max_increase` | string | No | `500%` | Maximum allowed increase percentage |

```yaml
checks:
  - row_count_change:
      max_decrease: "10%"
      max_increase: "200%"
```

This check does not require a minimum number of data points. On the first run, it passes with a "first run" note. On subsequent runs, it compares against the most recent stored row count.

## Complete Example

```yaml
source:
  type: duckdb
  table: read_parquet('data/orders/*.parquet')

checks:
  # Basic volume check
  - row_count:
      min: 1

  # Row count stability between runs
  - row_count_change:
      max_decrease: "20%"
      max_increase: "300%"

  # Anomaly detection on row count trend
  - anomaly:
      metric: row_count
      method: mad
      sensitivity: medium

  # Anomaly detection on column metrics
  - anomaly:
      column: total_amount
      metric: mean
      method: mad
      sensitivity: high

  - anomaly:
      column: customer_id
      metric: distinct_count
      method: iqr
      sensitivity: low

  - anomaly:
      column: email
      metric: null_rate
      method: zscore
      sensitivity: medium
```
