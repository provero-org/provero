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

"""Smoke tests for Flyte task dataclasses."""

from __future__ import annotations

from provero.flyte.task import ProveroCheckConfig, ProveroCheckResult


def test_check_config_defaults() -> None:
    cfg = ProveroCheckConfig()
    assert cfg.config_path == "provero.yaml"
    assert cfg.suite is None
    assert cfg.fail_on_error is True
    assert cfg.optimize is True


def test_check_config_overrides() -> None:
    cfg = ProveroCheckConfig(
        config_path="custom.yaml",
        suite="daily",
        fail_on_error=False,
        optimize=False,
    )
    assert cfg.config_path == "custom.yaml"
    assert cfg.suite == "daily"
    assert cfg.fail_on_error is False
    assert cfg.optimize is False


def test_check_result_defaults_primitive_types() -> None:
    result = ProveroCheckResult()
    assert result.suite_name == ""
    assert result.status == ""
    assert result.total == 0
    assert result.quality_score == 0.0
    assert result.failed_checks == []


def test_check_result_independent_failed_checks_lists() -> None:
    a = ProveroCheckResult()
    b = ProveroCheckResult()
    a.failed_checks.append("x")
    assert b.failed_checks == []
