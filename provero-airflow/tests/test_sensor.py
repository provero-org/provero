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

"""Smoke tests for ProveroSensor construction."""

from __future__ import annotations

from provero.airflow.sensors import ProveroSensor


def test_sensor_constructs_with_defaults() -> None:
    sensor = ProveroSensor(task_id="wait_for_quality")
    assert sensor.config_path == "provero.yaml"
    assert sensor.suite is None
    assert sensor.optimize is True
    assert sensor.store_results is True


def test_sensor_template_fields() -> None:
    assert ProveroSensor.template_fields == ("config_path", "suite")
