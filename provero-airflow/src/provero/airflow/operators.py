# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Provero Airflow operators."""

from __future__ import annotations

from typing import Any

try:
    from airflow.models import BaseOperator
except ImportError:
    # Allow import without Airflow installed (for testing/development)
    class BaseOperator:  # type: ignore[no-redef]
        """Stub for when Airflow is not installed."""

        def __init__(self, **kwargs: Any) -> None:
            self.task_id = kwargs.get("task_id", "")

        def execute(self, context: Any) -> Any:
            raise NotImplementedError


class ProveroCheckOperator(BaseOperator):
    """Run Provero quality checks as an Airflow task.

    Reads an provero.yaml configuration and executes the specified suite.
    Fails the task if any critical checks fail.

    Example usage in an Airflow DAG::

        check_orders = ProveroCheckOperator(
            task_id="check_orders",
            config_path="dags/provero.yaml",
            suite="orders_daily",
        )

    :param config_path: Path to provero.yaml configuration file
    :param suite: Name of the suite to run (optional, runs all if not specified)
    :param fail_on_error: Whether to fail the Airflow task on check failures
    :param optimize: Whether to use SQL batching optimization
    """

    template_fields = ("config_path", "suite")

    def __init__(
        self,
        config_path: str = "provero.yaml",
        suite: str | None = None,
        fail_on_error: bool = True,
        optimize: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.config_path = config_path
        self.suite = suite
        self.fail_on_error = fail_on_error
        self.optimize = optimize

    def execute(self, context: Any) -> dict[str, Any]:
        from provero.airflow.hooks import ProveroHook
        from provero.core.results import Status

        hook = ProveroHook(
            config_path=self.config_path,
            suite=self.suite,
            optimize=self.optimize,
        )
        results = hook.run_checks()
        all_results = []

        for result in results:
            all_results.append(result.model_dump())

            if self.fail_on_error and result.status == Status.FAIL:
                failed_checks = [c.check_name for c in result.checks if c.status == Status.FAIL]
                msg = (
                    f"Suite '{result.suite_name}' failed. "
                    f"Score: {result.quality_score}/100. "
                    f"Failed checks: {', '.join(failed_checks)}"
                )
                raise ValueError(msg)

        return {"suites": all_results}
