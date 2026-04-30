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

"""Smoke tests for ProveroCheckOperator construction and metadata."""

from __future__ import annotations

from provero.airflow.operators import ProveroCheckOperator


def test_operator_constructs_with_defaults() -> None:
    op = ProveroCheckOperator(task_id="check")
    assert op.config_path == "provero.yaml"
    assert op.suite is None
    assert op.fail_on_error is True
    assert op.optimize is True


def test_operator_custom_params() -> None:
    op = ProveroCheckOperator(
        task_id="check_orders",
        config_path="dags/provero.yaml",
        suite="orders_daily",
        fail_on_error=False,
        optimize=False,
    )
    assert op.config_path == "dags/provero.yaml"
    assert op.suite == "orders_daily"
    assert op.fail_on_error is False
    assert op.optimize is False


def test_operator_template_fields() -> None:
    assert ProveroCheckOperator.template_fields == ("config_path", "suite")
