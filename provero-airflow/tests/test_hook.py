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

"""Smoke tests for ProveroHook metadata and construction."""

from __future__ import annotations

from provero.airflow.hooks import ProveroHook


def test_hook_connection_metadata() -> None:
    assert ProveroHook.conn_type == "provero"
    assert ProveroHook.conn_name_attr == "provero_conn_id"
    assert ProveroHook.default_conn_name == "provero_default"
    assert ProveroHook.hook_name == "Provero"


def test_hook_constructs_with_defaults() -> None:
    hook = ProveroHook()
    assert hook.config_path == "provero.yaml"
    assert hook.suite is None
    assert hook.optimize is True
    assert hook.store_results is True


def test_hook_run_checks_is_callable() -> None:
    hook = ProveroHook(config_path="nonexistent.yaml")
    assert callable(hook.run_checks)
    assert callable(hook.check_passed)
