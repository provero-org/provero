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

"""Tests for the plugin discovery system (entry_points)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from provero.checks.registry import (
    _REGISTRY,
    get_check_runner,
    list_checks,
    register_check,
)
from provero.core.results import CheckResult, Severity, Status


class TestCheckRegistryPlugins:
    def test_builtin_checks_loaded(self):
        """Built-in checks are discovered via decorator imports."""
        runner = get_check_runner("not_null")
        assert runner is not None

    def test_all_builtins_present(self):
        """All expected built-in check types are registered."""
        checks = list_checks()
        expected = [
            "not_null",
            "unique",
            "unique_combination",
            "accepted_values",
            "range",
            "regex",
            "type",
            "freshness",
            "latency",
            "row_count",
            "completeness",
            "custom_sql",
            "anomaly",
            "row_count_change",
        ]
        for name in expected:
            assert name in checks, f"Built-in check '{name}' not found in registry"

    def test_plugin_check_registered_via_entry_points(self):
        """A plugin check discovered via entry_points is callable."""

        def fake_pii_check(**kwargs):
            return CheckResult(
                check_name="pii_detection",
                check_type="pii_detection",
                status=Status.PASS,
                severity=Severity.WARNING,
            )

        mock_ep = MagicMock()
        mock_ep.name = "pii_detection"
        mock_ep.load.return_value = fake_pii_check

        with patch("provero.checks.registry.entry_points", return_value=[mock_ep]):
            # Force re-discovery
            import provero.checks.registry as reg

            reg._PLUGINS_LOADED = False
            reg._load_plugins()

            runner = _REGISTRY.get("pii_detection")
            assert runner is not None
            result = runner()
            assert result.status == Status.PASS

        # Cleanup
        _REGISTRY.pop("pii_detection", None)

    def test_plugin_cannot_override_builtin(self):
        """Plugins cannot replace built-in checks (security)."""
        malicious_runner = MagicMock()
        mock_ep = MagicMock()
        mock_ep.name = "not_null"  # trying to override built-in
        mock_ep.load.return_value = malicious_runner

        # Ensure builtins are loaded first
        original_runner = get_check_runner("not_null")

        with patch("provero.checks.registry.entry_points", return_value=[mock_ep]):
            import provero.checks.registry as reg

            reg._PLUGINS_LOADED = False
            reg._load_plugins()

            # Built-in should still be in place
            current_runner = _REGISTRY.get("not_null")
            assert current_runner is original_runner
            mock_ep.load.assert_not_called()

    def test_register_check_decorator(self):
        """@register_check decorator works for inline registration."""

        @register_check("test_custom_check_xyz")
        def my_check(**kwargs):
            return CheckResult(
                check_name="test_custom_check_xyz",
                check_type="test_custom_check_xyz",
                status=Status.PASS,
                severity=Severity.INFO,
            )

        runner = _REGISTRY.get("test_custom_check_xyz")
        assert runner is my_check

        # Cleanup
        _REGISTRY.pop("test_custom_check_xyz", None)

    def test_list_checks_includes_plugins(self):
        """list_checks() returns both built-in and plugin checks."""
        _REGISTRY["plugin_test_check"] = MagicMock()
        try:
            checks = list_checks()
            assert "plugin_test_check" in checks
            assert "not_null" in checks
        finally:
            _REGISTRY.pop("plugin_test_check", None)
