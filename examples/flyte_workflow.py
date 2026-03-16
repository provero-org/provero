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

from flytekit import workflow

from provero.flyte.task import ProveroCheckConfig, ProveroCheckResult, provero_check_task


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
