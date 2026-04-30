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

"""Tests for the OpenLineage event exporter."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from provero.core.results import CheckResult, Severity, Status, SuiteResult
from provero.exporters.openlineage import (
    export_events,
    suite_result_to_events,
)


def _make_check(
    check_type: str = "not_null",
    status: Status = Status.PASS,
    column: str | None = "order_id",
    severity: Severity = Severity.CRITICAL,
    row_count: int = 100,
    failing_rows: int = 0,
    observed_value=None,
) -> CheckResult:
    return CheckResult(
        check_name=f"{check_type}:{column or ''}",
        check_type=check_type,
        status=status,
        severity=severity,
        source="duckdb",
        table="orders",
        column=column,
        row_count=row_count,
        failing_rows=failing_rows,
        observed_value=observed_value,
        started_at=datetime(2026, 3, 31, 12, 0, 0, tzinfo=UTC),
        duration_ms=50,
        run_id="abc-123",
        suite="default",
    )


def _make_suite(checks: list[CheckResult] | None = None) -> SuiteResult:
    if checks is None:
        checks = [_make_check()]
    suite = SuiteResult(
        suite_name="default",
        status=Status.PASS,
        checks=checks,
        started_at=datetime(2026, 3, 31, 12, 0, 0, tzinfo=UTC),
        duration_ms=100,
    )
    suite.compute_status()
    return suite


class TestSuiteResultToEvents:
    def test_returns_two_events(self):
        events = suite_result_to_events(_make_suite())
        assert len(events) == 2

    def test_start_event_type(self):
        events = suite_result_to_events(_make_suite())
        assert events[0]["eventType"] == "START"

    def test_complete_event_on_pass(self):
        events = suite_result_to_events(_make_suite())
        assert events[1]["eventType"] == "COMPLETE"

    def test_fail_event_on_failure(self):
        suite = _make_suite([_make_check(status=Status.FAIL)])
        events = suite_result_to_events(suite)
        assert events[1]["eventType"] == "FAIL"

    def test_schema_url_present(self):
        events = suite_result_to_events(_make_suite())
        for event in events:
            assert "schemaURL" in event
            assert "OpenLineage" in event["schemaURL"]

    def test_producer_contains_provero(self):
        events = suite_result_to_events(_make_suite())
        assert "provero" in events[0]["producer"]

    def test_job_name_is_suite_name(self):
        events = suite_result_to_events(_make_suite())
        assert events[0]["job"]["name"] == "default"

    def test_run_id_from_check(self):
        events = suite_result_to_events(_make_suite())
        assert events[0]["run"]["runId"] == "abc-123"

    def test_custom_namespace(self):
        events = suite_result_to_events(_make_suite(), namespace="postgres://localhost/mydb")
        assert events[0]["job"]["namespace"] == "postgres://localhost/mydb"


class TestAssertionsFacet:
    def _get_assertions(self, checks: list[CheckResult]) -> list[dict]:
        suite = _make_suite(checks)
        events = suite_result_to_events(suite)
        end_event = events[1]
        facet = end_event["inputs"][0]["facets"]["dataQualityAssertions"]
        return facet["assertions"]

    def test_single_passing_check(self):
        assertions = self._get_assertions([_make_check()])
        assert len(assertions) == 1
        assert assertions[0]["assertion"] == "not_null"
        assert assertions[0]["success"] is True

    def test_failing_check(self):
        assertions = self._get_assertions([_make_check(status=Status.FAIL)])
        assert assertions[0]["success"] is False

    def test_warn_counts_as_success(self):
        assertions = self._get_assertions([_make_check(status=Status.WARN)])
        assert assertions[0]["success"] is True

    def test_skipped_checks_excluded(self):
        assertions = self._get_assertions(
            [
                _make_check(),
                _make_check(status=Status.SKIP, column="skipped_col"),
            ]
        )
        assert len(assertions) == 1

    def test_column_present_when_set(self):
        assertions = self._get_assertions([_make_check(column="amount")])
        assert assertions[0]["column"] == "amount"

    def test_column_absent_for_table_level(self):
        assertions = self._get_assertions(
            [
                _make_check(check_type="row_count", column=None, observed_value=100),
            ]
        )
        assert "column" not in assertions[0]

    def test_severity_mapping_critical(self):
        assertions = self._get_assertions(
            [
                _make_check(severity=Severity.CRITICAL),
            ]
        )
        assert assertions[0]["severity"] == "error"

    def test_severity_mapping_blocker(self):
        assertions = self._get_assertions(
            [
                _make_check(severity=Severity.BLOCKER),
            ]
        )
        assert assertions[0]["severity"] == "error"

    def test_severity_mapping_warning(self):
        assertions = self._get_assertions(
            [
                _make_check(severity=Severity.WARNING),
            ]
        )
        assert assertions[0]["severity"] == "warn"

    def test_severity_mapping_info(self):
        assertions = self._get_assertions(
            [
                _make_check(severity=Severity.INFO),
            ]
        )
        assert assertions[0]["severity"] == "warn"

    def test_multiple_checks(self):
        assertions = self._get_assertions(
            [
                _make_check(check_type="not_null", column="a"),
                _make_check(check_type="unique", column="b"),
                _make_check(check_type="range", column="c"),
            ]
        )
        assert len(assertions) == 3
        types = {a["assertion"] for a in assertions}
        assert types == {"not_null", "unique", "range"}


class TestMetricsFacet:
    def _get_metrics(self, checks: list[CheckResult]) -> dict:
        suite = _make_suite(checks)
        events = suite_result_to_events(suite)
        end_event = events[1]
        return end_event["inputs"][0]["facets"]["dataQualityMetrics"]

    def test_row_count_extracted(self):
        metrics = self._get_metrics(
            [
                _make_check(check_type="row_count", column=None, observed_value=500),
            ]
        )
        assert metrics["rowCount"] == 500

    def test_null_count_from_not_null(self):
        metrics = self._get_metrics(
            [
                _make_check(check_type="not_null", column="email", failing_rows=3),
            ]
        )
        assert metrics["columnMetrics"]["email"]["nullCount"] == 3

    def test_distinct_count_from_unique(self):
        metrics = self._get_metrics(
            [
                _make_check(check_type="unique", column="id", observed_value=99),
            ]
        )
        assert metrics["columnMetrics"]["id"]["distinctCount"] == 99

    def test_empty_column_metrics_when_no_columns(self):
        metrics = self._get_metrics(
            [
                _make_check(check_type="row_count", column=None, observed_value=10),
            ]
        )
        assert metrics["columnMetrics"] == {}

    def test_schema_url_present(self):
        metrics = self._get_metrics([_make_check()])
        assert "DataQualityMetrics" in metrics["_schemaURL"]


class TestRunFacets:
    def test_summary_facet_present(self):
        suite = _make_suite(
            [
                _make_check(status=Status.PASS),
                _make_check(status=Status.FAIL, column="bad"),
            ]
        )
        events = suite_result_to_events(suite)
        end_event = events[1]
        summary = end_event["run"]["facets"]["provero_summary"]
        assert summary["total"] == 2
        assert summary["passed"] == 1
        assert summary["failed"] == 1
        assert "qualityScore" in summary

    def test_duration_in_summary(self):
        suite = _make_suite()
        events = suite_result_to_events(suite)
        summary = events[1]["run"]["facets"]["provero_summary"]
        assert summary["durationMs"] == 100


class TestExportEvents:
    def test_valid_json_output(self):
        result = export_events([_make_suite()])
        parsed = json.loads(result)
        assert isinstance(parsed, list)
        assert len(parsed) == 2

    def test_multiple_suites(self):
        suite_a = _make_suite([_make_check(column="a")])
        suite_b = _make_suite([_make_check(column="b")])
        result = export_events([suite_a, suite_b])
        parsed = json.loads(result)
        assert len(parsed) == 4

    def test_namespace_propagated(self):
        result = export_events(
            [_make_suite()],
            namespace="bigquery://project.dataset",
        )
        parsed = json.loads(result)
        assert parsed[0]["job"]["namespace"] == "bigquery://project.dataset"
        assert parsed[1]["inputs"][0]["namespace"] == "bigquery://project.dataset"

    def test_input_dataset_name_is_table(self):
        result = export_events([_make_suite()])
        parsed = json.loads(result)
        end_event = parsed[1]
        assert end_event["inputs"][0]["name"] == "orders"
