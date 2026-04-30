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

"""Smoke tests for the provero_check Airflow decorator."""

from __future__ import annotations

from provero.airflow.decorators import provero_check


def test_decorator_preserves_function_metadata() -> None:
    @provero_check(config_path="nonexistent.yaml", fail_on_error=False)
    def my_task() -> str:
        """Task docstring."""
        return "ok"

    assert my_task.__name__ == "my_task"
    assert my_task.__doc__ == "Task docstring."


def test_decorator_returns_callable() -> None:
    decorator = provero_check(config_path="foo.yaml", suite="bar")
    assert callable(decorator)
