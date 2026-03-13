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

"""Integration tests for anomaly detection checks."""

from __future__ import annotations

import pytest

from provero.checks.registry import get_check_runner
from provero.core.compiler import CheckConfig
from provero.core.results import CheckResult, Severity, Status, SuiteResult


class TestAnomalyCheckRegistered:
    def test_anomaly_check_exists(self):
        runner = get_check_runner("anomaly")
        assert runner is not None

    def test_row_count_change_check_exists(self):
        runner = get_check_runner("row_count_change")
        assert runner is not None


class TestAnomalyCheck:
    def test_missing_metric_param(self, duckdb_orders):
        runner = get_check_runner("anomaly")
        config = CheckConfig(check_type="anomaly", params={})
        result = runner(connection=duckdb_orders, table="orders", check_config=config)
        assert result.status == Status.ERROR

    def test_no_history_skips(self, duckdb_orders):
        """With no history and no suite context, should skip."""
        runner = get_check_runner("anomaly")
        config = CheckConfig(
            check_type="anomaly",
            params={"metric": "row_count", "_suite_name": "test", "_check_name": "anomaly:"},
        )
        result = runner(connection=duckdb_orders, table="orders", check_config=config)
        # No stored history, so it should SKIP
        assert result.status == Status.SKIP
        # But it should have queried the current value
        assert "5" in str(result.observed_value)  # 5 rows in orders

    def test_queries_current_row_count(self, duckdb_orders):
        """Anomaly check should query the actual data source."""
        runner = get_check_runner("anomaly")
        config = CheckConfig(
            check_type="anomaly",
            params={
                "metric": "row_count",
                "_history": [5.0, 5.0, 5.0, 5.0, 5.0, 5.0],
            },
        )
        result = runner(connection=duckdb_orders, table="orders", check_config=config)
        assert result.status == Status.PASS
        assert "5.0" in str(result.observed_value)

    def test_detects_anomaly_with_injected_history(self, duckdb_orders):
        """When current value deviates from history, should detect anomaly."""
        runner = get_check_runner("anomaly")
        # History shows ~1000 rows, but orders only has 5
        config = CheckConfig(
            check_type="anomaly",
            params={
                "metric": "row_count",
                "_history": [1000.0, 1010.0, 990.0, 1005.0, 995.0, 1002.0],
                "sensitivity": "high",
            },
        )
        result = runner(connection=duckdb_orders, table="orders", check_config=config)
        assert result.status == Status.FAIL

    def test_null_rate_metric(self, duckdb_orders):
        """Should support null_rate metric with column."""
        runner = get_check_runner("anomaly")
        config = CheckConfig(
            check_type="anomaly",
            column="order_id",
            params={
                "metric": "null_rate",
                "_history": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            },
        )
        result = runner(connection=duckdb_orders, table="orders", check_config=config)
        assert result.status == Status.PASS

    def test_unsupported_metric(self, duckdb_orders):
        """Unsupported metric should return ERROR."""
        runner = get_check_runner("anomaly")
        config = CheckConfig(
            check_type="anomaly",
            params={"metric": "nonexistent_metric"},
        )
        result = runner(connection=duckdb_orders, table="orders", check_config=config)
        assert result.status == Status.ERROR

    def test_threshold_override(self, duckdb_orders):
        """Direct threshold param should work."""
        runner = get_check_runner("anomaly")
        config = CheckConfig(
            check_type="anomaly",
            params={
                "metric": "row_count",
                "threshold": 1000.0,  # Extremely permissive (modified z ~134)
                "_history": [1000.0, 1010.0, 990.0, 1005.0, 995.0, 1002.0],
            },
        )
        result = runner(connection=duckdb_orders, table="orders", check_config=config)
        # 5 vs ~1000 has modified z-score ~134, but threshold=1000 allows it
        assert result.status == Status.PASS


class TestRowCountChange:
    def test_no_suite_context_skips(self, duckdb_orders):
        runner = get_check_runner("row_count_change")
        config = CheckConfig(check_type="row_count_change", params={})
        result = runner(connection=duckdb_orders, table="orders", check_config=config)
        assert result.status == Status.SKIP

    def test_queries_actual_count(self, duckdb_orders):
        """row_count_change should query COUNT(*) from data source."""
        runner = get_check_runner("row_count_change")
        config = CheckConfig(
            check_type="row_count_change",
            params={"_suite_name": "test"},
        )
        result = runner(connection=duckdb_orders, table="orders", check_config=config)
        # First run with suite context but no history = PASS
        assert result.status == Status.PASS
        assert result.row_count == 5
        assert "first run" in str(result.observed_value)

    def test_row_count_change_with_store(self, duckdb_orders, sqlite_store):
        """Test row_count_change stores its own metric."""
        # Simulate a previous run storing a row_count_change metric
        previous_result = SuiteResult(
            suite_name="test_suite",
            status=Status.PASS,
            checks=[
                CheckResult(
                    check_name="row_count_change",
                    check_type="row_count_change",
                    status=Status.PASS,
                    severity=Severity.WARNING,
                    observed_value="5 rows (first run)",
                    row_count=5,
                    run_id="prev-run",
                    suite="test_suite",
                )
            ],
            total=1,
            passed=1,
        )
        sqlite_store.save_result(previous_result)

        # Verify the row_count_change metric was stored under its own name
        metrics = sqlite_store.get_metrics("test_suite", "row_count_change", "row_count")
        assert len(metrics) == 1
        assert metrics[0]["value"] == 5.0
