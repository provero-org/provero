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

"""Provero Airflow hook wrapping the Python API."""

from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    from airflow.hooks.base import BaseHook
except ImportError:

    class BaseHook:  # type: ignore[no-redef]
        """Stub for when Airflow is not installed."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass


class ProveroHook(BaseHook):
    """Hook for running Provero data quality checks from Airflow.

    Wraps the Provero Python API for use in operators, sensors, and
    PythonOperator callables. Handles config loading, suite execution,
    and result storage.

    Example usage in a PythonOperator::

        def run_checks(**context):
            hook = ProveroHook(config_path="dags/provero.yaml")
            results = hook.run_checks()
            for suite in results:
                if suite.status.value == "fail":
                    raise ValueError(f"Suite {suite.suite_name} failed")

        PythonOperator(
            task_id="custom_check",
            python_callable=run_checks,
        )

    :param config_path: Path to provero.yaml configuration file.
    :param suite: Run only this suite (optional, runs all if not set).
    :param optimize: Whether to use SQL batching optimization.
    :param store_results: Whether to persist results to the local store.
    """

    conn_name_attr = "provero_conn_id"
    default_conn_name = "provero_default"
    conn_type = "provero"
    hook_name = "Provero"

    def __init__(
        self,
        config_path: str = "provero.yaml",
        suite: str | None = None,
        optimize: bool = True,
        store_results: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.config_path = config_path
        self.suite = suite
        self.optimize = optimize
        self.store_results = store_results

    def run_checks(self) -> list:
        """Execute quality checks and return a list of SuiteResult objects.

        Returns:
            List of SuiteResult objects, one per executed suite.
        """
        from provero.connectors.factory import create_connector
        from provero.core.compiler import compile_file
        from provero.core.engine import run_suite
        from provero.store.sqlite import SQLiteStore

        config = compile_file(Path(self.config_path))
        results = []

        store = SQLiteStore() if self.store_results else None
        try:
            for suite_config in config.suites:
                if self.suite and suite_config.name != self.suite:
                    continue
                connector = create_connector(suite_config.source)
                result = run_suite(suite_config, connector, optimize=self.optimize)
                if store:
                    store.save_result(result)
                results.append(result)
        finally:
            if store:
                store.close()

        return results

    def check_passed(self, suite_name: str | None = None) -> bool:
        """Run checks and return True if all suites pass.

        Args:
            suite_name: Optional suite name to check. If not set,
                uses the hook's configured suite filter.

        Returns:
            True if all executed suites passed, False otherwise.
        """
        from provero.core.results import Status

        target = suite_name or self.suite
        original_suite = self.suite
        self.suite = target
        try:
            results = self.run_checks()
        finally:
            self.suite = original_suite

        return all(r.status in (Status.PASS, Status.WARN) for r in results)
