# Flyte Integration

Provero provides a Flyte plugin with three integration patterns: a standalone task, a decorator, and `Annotated` type hints for automatic DataFrame validation at task boundaries.

## Installation

```bash
pip install provero-flyte
```

This installs the `provero-flyte` package, which depends on `provero` (core) and `flytekit`.

## Integration Patterns

### 1. Standalone Task

The `provero_check_task` function is a pre-built Flyte `@task` that reads a `provero.yaml` config and executes the specified suite(s). Results are persisted to the result store and published as a Flyte Deck.

```python
from flytekit import workflow
from provero.flyte.task import ProveroCheckConfig, ProveroCheckResult, provero_check_task

@workflow
def quality_pipeline() -> list[ProveroCheckResult]:
    config = ProveroCheckConfig(
        config_path="provero.yaml",
        suite="orders_daily",
        fail_on_error=True,
    )
    return provero_check_task(config=config)
```

#### ProveroCheckConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `config_path` | string | `provero.yaml` | Path to the Provero config file |
| `suite` | string | `None` | Run only this suite (runs all if not specified) |
| `fail_on_error` | bool | `True` | Raise ValueError on suite failure |
| `optimize` | bool | `True` | Enable SQL batching optimization |

#### ProveroCheckResult

The task returns a list of `ProveroCheckResult` dataclass instances with Flyte-compatible primitive types:

| Field | Type | Description |
|-------|------|-------------|
| `suite_name` | string | Name of the executed suite |
| `status` | string | Overall status (pass/fail) |
| `total` | int | Total number of checks |
| `passed` | int | Number of passed checks |
| `failed` | int | Number of failed checks |
| `warned` | int | Number of warned checks |
| `errored` | int | Number of errored checks |
| `quality_score` | float | Quality score (0-100) |
| `duration_ms` | int | Execution time in milliseconds |
| `failed_checks` | list[str] | Names of failed checks |

### 2. Decorator

The `@provero_check` decorator runs Provero checks after your task function completes. Quality results are published as Flyte Decks automatically.

```python
from flytekit import task
from provero.flyte.decorators import provero_check

@task
@provero_check(config_path="provero.yaml", suite="orders_daily")
def process_orders():
    """Process orders, then validate quality."""
    ...
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config_path` | string | `provero.yaml` | Path to the Provero config file |
| `suite` | string | `None` | Run only this suite |
| `fail_on_error` | bool | `True` | Raise ValueError on failure |

### 3. Annotated Type Hints

The most Flyte-native pattern uses `typing.Annotated` to attach Provero validation metadata to DataFrame type hints. Validation runs automatically when the DataFrame crosses a Flyte task boundary (during serialization).

This follows the same pattern as Flyte's Pandera plugin.

```python
from typing import Annotated
import pandas as pd
from flytekit import task, workflow
from provero.core.compiler import CheckConfig
from provero.flyte import ProveroSuite

ValidatedOrders = Annotated[
    pd.DataFrame,
    ProveroSuite(
        name="orders_validation",
        checks=[
            CheckConfig(check_type="not_null", column="order_id"),
            CheckConfig(check_type="not_null", column="amount"),
            CheckConfig(check_type="row_count", params={"min": 1}),
        ],
        on_error="raise",
    ),
]

@task
def load_orders() -> ValidatedOrders:
    """Load orders. Provero validates the output automatically."""
    return pd.DataFrame({
        "order_id": [1, 2, 3],
        "amount": [10.0, 25.5, 7.99],
    })

@task
def process_orders(df: ValidatedOrders) -> int:
    """Validated on input too."""
    return len(df)

@workflow
def pipeline() -> int:
    df = load_orders()
    return process_orders(df=df)
```

#### ProveroSuite Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | `provero_validation` | Suite name for reporting |
| `table_name` | string | `df` | Virtual table name for the DataFrame |
| `checks` | list | `[]` | List of `CheckConfig` objects for inline checks |
| `config_path` | string | `None` | Path to a YAML config file (alternative to inline checks) |
| `suite` | string | `None` | Suite name within the YAML config |
| `on_error` | string | `raise` | Error handling: `raise` or `warn` |

You can use either inline checks or a YAML config file, not both. When `config_path` is set, Provero loads suites from the file. When `checks` is provided, Provero creates an in-memory suite with a `DataFrameConnector`.

#### YAML-based Annotated Type

```python
from typing import Annotated
import pandas as pd
from provero.flyte import ProveroSuite

ValidatedOrders = Annotated[
    pd.DataFrame,
    ProveroSuite(
        config_path="provero.yaml",
        suite="orders_daily",
        on_error="warn",
    ),
]
```

## Flyte Decks

All three integration patterns automatically publish quality results as Flyte Decks. The deck renders an HTML report using Provero's built-in report generator, showing check results, quality score, and pass/fail details.

Decks are titled `Provero: <suite_name>` by default. They appear in the Flyte UI alongside your task's other decks.

If the code runs outside a Flyte execution context (e.g., during local testing), deck publishing is silently skipped.

## Polars Support

The type transformer supports both Pandas and Polars DataFrames. Polars support is registered conditionally when the `polars` package is installed:

```python
from typing import Annotated
import polars as pl
from provero.flyte import ProveroSuite
from provero.core.compiler import CheckConfig

ValidatedDF = Annotated[
    pl.DataFrame,
    ProveroSuite(
        checks=[
            CheckConfig(check_type="not_null", column="id"),
        ],
    ),
]
```

## Example Workflow

Here is a complete example combining the standalone task and Annotated type patterns:

```python
from __future__ import annotations

from typing import Annotated

import pandas as pd
from flytekit import task, workflow

from provero.core.compiler import CheckConfig
from provero.flyte import ProveroSuite
from provero.flyte.task import ProveroCheckConfig, ProveroCheckResult, provero_check_task

# Annotated type with inline checks
ValidatedOrders = Annotated[
    pd.DataFrame,
    ProveroSuite(
        name="orders_validation",
        checks=[
            CheckConfig(check_type="not_null", column="order_id"),
            CheckConfig(check_type="not_null", column="amount"),
        ],
        on_error="raise",
    ),
]


@task
def load_orders() -> ValidatedOrders:
    """Load orders data. Provero validates the output automatically."""
    return pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "amount": [10.0, 25.5, 7.99],
            "customer": ["alice", "bob", "charlie"],
        }
    )


@workflow
def quality_pipeline() -> list[ProveroCheckResult]:
    """Run YAML-based quality checks as a standalone task."""
    config = ProveroCheckConfig(
        config_path="provero.yaml",
        suite="orders_daily",
        fail_on_error=True,
    )
    return provero_check_task(config=config)
```

Run locally:

```bash
pyflyte run examples/flyte_workflow.py quality_pipeline
```
