# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Tests for the Flyte plugin (requires provero-flyte package)."""

from __future__ import annotations

import pytest

try:
    from provero.flyte.decorators import provero_check
    from provero.flyte.task import ProveroCheckConfig, ProveroCheckResult, provero_check_task

    HAS_FLYTE_PACKAGE = True
except ImportError:
    HAS_FLYTE_PACKAGE = False

pytestmark = pytest.mark.skipif(
    not HAS_FLYTE_PACKAGE,
    reason="provero-flyte package not installed",
)


class TestProveroCheckConfig:
    def test_defaults(self):
        config = ProveroCheckConfig()
        assert config.config_path == "provero.yaml"
        assert config.suite is None
        assert config.fail_on_error is True
        assert config.optimize is True

    def test_custom_values(self):
        config = ProveroCheckConfig(
            config_path="custom.yaml",
            suite="orders",
            fail_on_error=False,
            optimize=False,
        )
        assert config.config_path == "custom.yaml"
        assert config.suite == "orders"
        assert config.fail_on_error is False
        assert config.optimize is False


class TestProveroCheckResult:
    def test_defaults(self):
        result = ProveroCheckResult()
        assert result.suite_name == ""
        assert result.status == ""
        assert result.total == 0
        assert result.passed == 0
        assert result.failed == 0
        assert result.warned == 0
        assert result.errored == 0
        assert result.quality_score == 0.0
        assert result.duration_ms == 0
        assert result.failed_checks == []


class TestProveroCheckTask:
    def test_callable(self):
        assert callable(provero_check_task)


class TestProveroCheckDecorator:
    def test_import(self):
        assert callable(provero_check)

    def test_decorator_wraps_function(self):
        @provero_check(config_path="nonexistent.yaml")
        def my_task():
            return 42

        assert my_task.__name__ == "my_task"
