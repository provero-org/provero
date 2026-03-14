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

"""Tests for the SQLite result store."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest

from provero.core.results import CheckResult, Severity, Status, SuiteResult
from provero.store.sqlite import SQLiteStore


@pytest.fixture
def store(tmp_path: Path):
    db_path = tmp_path / "test_results.db"
    s = SQLiteStore(db_path)
    yield s
    s.close()


def _make_suite_result(suite_name: str = "test_suite", passed: bool = True) -> SuiteResult:
    run_id = str(uuid.uuid4())
    checks = [
        CheckResult(
            check_name="not_null:id",
            check_type="not_null",
            status=Status.PASS,
            severity=Severity.CRITICAL,
            column="id",
            observed_value="0 nulls",
            expected_value="0 nulls",
            row_count=100,
            failing_rows=0,
            run_id=run_id,
            suite=suite_name,
            table="orders",
        ),
        CheckResult(
            check_name="row_count",
            check_type="row_count",
            status=Status.PASS if passed else Status.FAIL,
            severity=Severity.CRITICAL,
            observed_value="100" if passed else "0",
            expected_value=">= 1",
            row_count=100 if passed else 0,
            failing_rows=0,
            run_id=run_id,
            suite=suite_name,
            table="orders",
        ),
    ]
    result = SuiteResult(
        suite_name=suite_name,
        status=Status.PASS,
        checks=checks,
        started_at=datetime.now(tz=UTC),
        duration_ms=42,
    )
    result.compute_status()
    return result


class TestSQLiteStore:
    def test_save_and_retrieve_history(self, store):
        result = _make_suite_result()
        store.save_result(result)

        history = store.get_history()
        assert len(history) == 1
        assert history[0]["suite_name"] == "test_suite"
        assert history[0]["total"] == 2
        assert history[0]["passed"] == 2

    def test_get_run_details(self, store):
        result = _make_suite_result()
        run_id = store.save_result(result)

        details = store.get_run_details(run_id)
        assert len(details) == 2
        assert details[0]["check_name"] == "not_null:id"
        assert details[1]["check_name"] == "row_count"

    def test_filter_by_suite(self, store):
        store.save_result(_make_suite_result("suite_a"))
        store.save_result(_make_suite_result("suite_b"))

        all_history = store.get_history()
        assert len(all_history) == 2

        filtered = store.get_history(suite_name="suite_a")
        assert len(filtered) == 1
        assert filtered[0]["suite_name"] == "suite_a"

    def test_metrics_stored(self, store):
        result = _make_suite_result()
        store.save_result(result)

        metrics = store.get_metrics("test_suite", "row_count", "row_count")
        assert len(metrics) == 1
        assert metrics[0]["value"] == 100.0

    def test_multiple_runs_ordered(self, store):
        for _i in range(5):
            store.save_result(_make_suite_result())

        history = store.get_history(limit=3)
        assert len(history) == 3

    def test_failed_result(self, store):
        result = _make_suite_result(passed=False)
        store.save_result(result)

        history = store.get_history()
        assert history[0]["status"] == "fail"
        assert history[0]["failed"] == 1
