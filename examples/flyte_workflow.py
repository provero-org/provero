# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example Flyte workflow with Provero quality checks.

Run with:
    pyflyte run examples/flyte_workflow.py quality_pipeline
"""

from __future__ import annotations

from typing import Annotated

import pandas as pd
from flytekit import task, workflow

from provero.core.compiler import CheckConfig
from provero.flyte import ProveroSuite
from provero.flyte.task import ProveroCheckConfig, ProveroCheckResult, provero_check_task

# DataFrame type with automatic Provero validation via Annotated
ValidatedOrders = Annotated[
    pd.DataFrame,
    ProveroSuite(
        name="orders_validation",
        checks=[
            CheckConfig(check_type="not_null", column="order_id"),
            CheckConfig(check_type="not_null", column="amount"),
            CheckConfig(check_type="range", column="amount", params={"min": 0}),
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
    """Run Provero quality checks as a Flyte workflow."""
    config = ProveroCheckConfig(
        config_path="provero.yaml",
        suite="orders_daily",
        fail_on_error=True,
    )
    return provero_check_task(config=config)


if __name__ == "__main__":
    results = quality_pipeline()
    for r in results:
        print(f"Suite: {r.suite_name} | Status: {r.status} | Score: {r.quality_score}")
