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

"""Store protocol defining the interface for result backends."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from provero.core.results import SuiteResult


@runtime_checkable
class Store(Protocol):
    """Protocol that all result store backends must implement."""

    def save_result(self, result: SuiteResult) -> str:
        """Save a suite result. Returns the run_id."""
        ...

    def get_history(self, suite_name: str | None = None, limit: int = 20) -> list[dict]:
        """Get recent run history."""
        ...

    def get_run_details(self, run_id: str) -> list[dict]:
        """Get check results for a specific run."""
        ...

    def get_metrics(
        self,
        suite_name: str,
        check_name: str,
        metric_name: str,
        limit: int = 30,
    ) -> list[dict]:
        """Get historical metric values for anomaly detection."""
        ...

    def close(self) -> None:
        """Close the store connection."""
        ...
