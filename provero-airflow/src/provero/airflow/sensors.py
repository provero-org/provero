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

"""Provero Airflow sensor that waits for data quality checks to pass."""

from __future__ import annotations

from typing import Any

try:
    from airflow.sensors.base import BaseSensorOperator
except ImportError:

    class BaseSensorOperator:  # type: ignore[no-redef]
        """Stub for when Airflow is not installed."""

        def __init__(self, **kwargs: Any) -> None:
            self.task_id = kwargs.get("task_id", "")
            self.poke_interval = kwargs.get("poke_interval", 60)
            self.timeout = kwargs.get("timeout", 3600)
            self.mode = kwargs.get("mode", "poke")

        def poke(self, context: Any) -> bool:
            raise NotImplementedError


class ProveroSensor(BaseSensorOperator):
    """Sensor that waits for Provero quality checks to pass.

    Re-runs checks on each poke until all pass or the sensor times out.
    Useful for gating downstream tasks on data quality.

    Example::

        wait_for_quality = ProveroSensor(
            task_id="wait_for_quality",
            config_path="dags/provero.yaml",
            suite="orders_daily",
            poke_interval=120,
            timeout=3600,
            mode="reschedule",
        )

        wait_for_quality >> transform >> load

    :param config_path: Path to provero.yaml configuration file.
    :param suite: Name of the suite to check (optional, checks all if not set).
    :param optimize: Whether to use SQL batching optimization.
    :param store_results: Whether to persist results to the local store.
    :param poke_interval: Seconds between each check attempt (default 60).
    :param timeout: Seconds before the sensor times out (default 3600).
    :param mode: ``"poke"`` (default) or ``"reschedule"`` to free the worker slot between pokes.
    """

    template_fields = ("config_path", "suite")

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

    def poke(self, context: Any) -> bool:
        """Run checks and return True if all pass.

        Called by Airflow on each poke interval. Returns True when all
        quality checks pass, allowing downstream tasks to proceed.
        """
        from provero.airflow.hooks import ProveroHook

        hook = ProveroHook(
            config_path=self.config_path,
            suite=self.suite,
            optimize=self.optimize,
            store_results=self.store_results,
        )
        return hook.check_passed()
