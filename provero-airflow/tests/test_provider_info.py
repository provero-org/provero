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

"""Smoke tests for Airflow provider metadata."""

from __future__ import annotations

from provero.airflow import get_provider_info


def test_provider_info_loads() -> None:
    info = get_provider_info()
    assert info["package-name"] == "provero-airflow"
    assert info["name"] == "Provero"


def test_provider_info_declares_components() -> None:
    info = get_provider_info()
    assert "hooks" in info
    assert "operators" in info
    assert "sensors" in info

    hook_modules = {m for h in info["hooks"] for m in h["python-modules"]}
    operator_modules = {m for o in info["operators"] for m in o["python-modules"]}
    sensor_modules = {m for s in info["sensors"] for m in s["python-modules"]}

    assert "provero.airflow.hooks" in hook_modules
    assert "provero.airflow.operators" in operator_modules
    assert "provero.airflow.sensors" in sensor_modules
