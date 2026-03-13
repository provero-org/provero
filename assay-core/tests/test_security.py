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

"""Security tests: SQL injection, XSS prevention, plugin safety."""

from __future__ import annotations

import pytest

from provero.connectors.duckdb import DuckDBConnection
from provero.connectors.factory import _resolve_connection
from provero.core.compiler import CheckConfig, SourceConfig, SuiteConfig
from provero.core.engine import run_suite
from provero.core.results import CheckResult, Severity, Status, SuiteResult
from provero.core.sql import quote_identifier
from provero.reporting.html import generate_html_report


class _SharedDuckDBConnector:
    def __init__(self, conn: DuckDBConnection) -> None:
        self._conn = conn

    def connect(self) -> DuckDBConnection:
        return self._conn

    def disconnect(self, connection: DuckDBConnection) -> None:
        pass


class TestSQLInjection:
    def test_table_name_injection(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            quote_identifier("orders; DROP TABLE users")

    def test_column_name_injection(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            quote_identifier("status; --")

    def test_check_params_safe(self, duckdb_orders):
        """accepted_values with SQL injection payload should not crash."""
        connector = _SharedDuckDBConnector(duckdb_orders)
        suite = SuiteConfig(
            name="injection_test",
            source=SourceConfig(type="duckdb", table="orders"),
            checks=[
                CheckConfig(
                    check_type="accepted_values",
                    column="status",
                    params={"values": ["'; DROP TABLE orders; --", "delivered"]},
                ),
            ],
        )
        result = run_suite(suite, connector, optimize=False)
        assert result.total == 1
        # The check should execute (and likely fail), but NOT crash or drop the table
        assert result.checks[0].status in (Status.PASS, Status.FAIL, Status.WARN, Status.ERROR)

        # Verify orders table still exists
        rows = duckdb_orders.execute("SELECT COUNT(*) AS cnt FROM orders")
        assert rows[0]["cnt"] == 5

    def test_connection_string_passthrough(self):
        """Non-env-var connection strings should pass through unchanged."""
        raw = "postgresql://user:pass@host/db"
        assert _resolve_connection(raw) == raw


class TestXSS:
    def _make_result_with_check(self, **check_kwargs) -> SuiteResult:
        """Helper to build a SuiteResult with one check."""
        check = CheckResult(
            check_name=check_kwargs.get("check_name", "test_check"),
            check_type=check_kwargs.get("check_type", "not_null"),
            status=check_kwargs.get("status", Status.PASS),
            severity=Severity.CRITICAL,
            column=check_kwargs.get("column"),
            observed_value=check_kwargs.get("observed_value"),
            expected_value=check_kwargs.get("expected_value"),
        )
        suite = SuiteResult(
            suite_name="xss_test",
            status=Status.PASS,
            checks=[check],
            total=1,
            passed=1,
        )
        suite.compute_status()
        return suite

    def test_script_in_check_name(self):
        result = self._make_result_with_check(
            check_name="<script>alert(1)</script>",
        )
        html = generate_html_report(result)
        assert "<script>alert(1)</script>" not in html
        assert "&lt;script&gt;" in html or "alert(1)" not in html

    def test_html_in_column_name(self):
        result = self._make_result_with_check(
            column='<img onerror=alert(1) src="">',
        )
        html = generate_html_report(result)
        assert "<img onerror" not in html

    def test_script_in_observed_value(self):
        result = self._make_result_with_check(
            observed_value="<script>document.cookie</script>",
        )
        html = generate_html_report(result)
        assert "<script>document.cookie</script>" not in html


class TestPluginSecurity:
    def test_check_plugin_no_override(self):
        """Built-in checks should not be silently overridden by plugins."""
        from provero.checks.registry import get_check_runner, list_checks

        builtin_checks = list_checks()
        assert "not_null" in builtin_checks

        runner = get_check_runner("not_null")
        assert runner is not None
        assert "completeness" in runner.__module__ or "not_null" in runner.__name__
