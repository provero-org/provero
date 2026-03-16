# Airflow Integration

Provero ships an official Apache Airflow provider package that lets you run data quality checks as part of your DAGs.

## Installation

```bash
pip install provero-airflow
```

This installs the `provero-airflow` package, which depends on `provero` (core) and `apache-airflow>=2.9`.

## Provider Auto-discovery

The `provero-airflow` package includes a `provider.yaml` that Airflow uses for auto-discovery. Once installed, Provero appears in the Airflow providers list and the operator is available without any extra configuration.

The provider registers:

- **Package:** `provero-airflow`
- **Operators:** `provero.airflow.operators`

## ProveroCheckOperator

The `ProveroCheckOperator` reads a `provero.yaml` configuration file and executes the specified suite(s). If any critical check fails, the Airflow task fails.

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config_path` | string | `provero.yaml` | Path to the Provero config file |
| `suite` | string | `None` | Run only this suite (runs all if not specified) |
| `fail_on_error` | bool | `True` | Fail the Airflow task if any critical check fails |
| `optimize` | bool | `True` | Enable SQL batching optimization |

Both `config_path` and `suite` are Airflow template fields, so you can use Jinja templating.

### Basic Usage

```python
from provero.airflow.operators import ProveroCheckOperator

check_orders = ProveroCheckOperator(
    task_id="check_orders",
    config_path="dags/provero.yaml",
    suite="orders_daily",
)
```

### Return Value

The operator returns a dictionary with the structure `{"suites": [...]}`, where each suite entry contains the full result including check details, quality score, and pass/fail status. This makes it available via XCom for downstream tasks.

## @provero_check Decorator

The `@provero_check` decorator runs Provero quality checks after your task function completes. This is useful when you want to validate data produced by a Python task without creating a separate operator.

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config_path` | string | `provero.yaml` | Path to the Provero config file |
| `suite` | string | `None` | Run only this suite |
| `fail_on_error` | bool | `True` | Raise ValueError if any critical check fails |

### Usage

```python
from provero.airflow.decorators import provero_check

@provero_check(config_path="dags/provero.yaml", suite="orders_daily")
def process_orders(**context):
    # Your ETL logic here
    ...
```

The decorator wraps your function. It first executes the original task, then runs Provero checks. If checks fail and `fail_on_error` is True, a `ValueError` is raised with the list of failed checks.

## Example DAG

```python
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from provero.airflow.decorators import provero_check
from provero.airflow.operators import ProveroCheckOperator

with DAG(
    dag_id="orders_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    def load_orders(**context):
        """Load orders from source system."""
        ...

    def transform_orders(**context):
        """Apply business transformations."""
        ...

    load = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders,
    )

    transform = PythonOperator(
        task_id="transform_orders",
        python_callable=transform_orders,
    )

    # Option 1: Dedicated quality check task
    check_quality = ProveroCheckOperator(
        task_id="check_quality",
        config_path="dags/provero.yaml",
        suite="orders_daily",
        fail_on_error=True,
    )

    load >> transform >> check_quality
```

### Using the Decorator Pattern

```python
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from provero.airflow.decorators import provero_check

with DAG(
    dag_id="orders_pipeline_v2",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    @provero_check(config_path="dags/provero.yaml", suite="orders_daily")
    def load_and_validate(**context):
        """Load orders and validate quality in one step."""
        ...

    task = PythonOperator(
        task_id="load_and_validate",
        python_callable=load_and_validate,
    )
```

## Config File for Airflow

A typical `provero.yaml` used with Airflow might look like this:

```yaml
version: "1.0"

sources:
  warehouse:
    type: postgres
    connection: ${AIRFLOW_CONN_WAREHOUSE_URI}

suites:
  - name: orders_daily
    source: warehouse
    table: public.orders
    tags: [critical, daily]
    checks:
      - not_null: [order_id, customer_id, amount]
      - unique: order_id
      - row_count:
          min: 1
      - freshness:
          column: created_at
          max_age: 24h
      - row_count_change:
          max_decrease: "20%"
          max_increase: "300%"
```

!!! tip
    Use environment variables for connection strings. Airflow sets many environment variables that you can reference directly in your Provero config.
