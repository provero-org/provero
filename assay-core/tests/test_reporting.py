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

"""Tests for HTML report generation."""

from __future__ import annotations

from pathlib import Path

import pytest

from provero.contracts.models import (
    ContractResult,
    ContractViolation,
    SchemaDrift,
)
from provero.core.results import CheckResult, Severity, Status, SuiteResult
from provero.reporting.html import generate_html_report


@pytest.fixture
def sample_suite_result():
    """Create a sample suite result for testing."""
    return SuiteResult(
        suite_name="orders_quality",
        status=Status.FAIL,
        checks=[
            CheckResult(
                check_name="not_null:order_id",
                check_type="not_null",
                status=Status.PASS,
                severity=Severity.CRITICAL,
                column="order_id",
                observed_value="0 nulls",
                expected_value="0 nulls",
            ),
            CheckResult(
                check_name="unique:order_id",
                check_type="unique",
                status=Status.PASS,
                severity=Severity.CRITICAL,
                column="order_id",
                observed_value="0 duplicates",
                expected_value="0 duplicates",
            ),
            CheckResult(
                check_name="not_null:status",
                check_type="not_null",
                status=Status.FAIL,
                severity=Severity.CRITICAL,
                column="status",
                observed_value="3 nulls",
                expected_value="0 nulls",
                row_count=100,
                failing_rows=3,
                failing_rows_query="SELECT * FROM orders WHERE status IS NULL",
                failing_rows_sample=[
                    {"order_id": 10, "status": None},
                    {"order_id": 20, "status": None},
                ],
            ),
        ],
        total=3,
        passed=2,
        failed=1,
        quality_score=66.7,
    )


class TestHTMLReport:
    def test_returns_html_string(self, sample_suite_result):
        html = generate_html_report(sample_suite_result)
        assert isinstance(html, str)
        assert "<!DOCTYPE html>" in html

    def test_contains_suite_name(self, sample_suite_result):
        html = generate_html_report(sample_suite_result)
        assert "orders_quality" in html

    def test_contains_check_results(self, sample_suite_result):
        html = generate_html_report(sample_suite_result)
        assert "not_null" in html
        assert "unique" in html
        assert "PASS" in html
        assert "FAIL" in html

    def test_contains_quality_score(self, sample_suite_result):
        html = generate_html_report(sample_suite_result)
        assert "66.7" in html

    def test_contains_failing_rows(self, sample_suite_result):
        html = generate_html_report(sample_suite_result)
        assert "SELECT * FROM orders WHERE status IS NULL" in html
        assert "order_id" in html

    def test_self_contained(self, sample_suite_result):
        """Report should not reference external resources."""
        html = generate_html_report(sample_suite_result)
        assert "http://" not in html
        assert "https://" not in html
        assert "<style>" in html

    def test_writes_to_file(self, sample_suite_result, tmp_path):
        output = tmp_path / "report.html"
        html = generate_html_report(sample_suite_result, output_path=output)
        assert output.exists()
        assert output.read_text() == html

    def test_with_contract_results(self, sample_suite_result):
        contract_results = [
            ContractResult(
                contract_name="orders_contract",
                status="warn",
                violations=[
                    ContractViolation(
                        rule="schema.column_missing",
                        message="Column 'email' missing from table",
                        severity="warning",
                    ),
                ],
                schema_drift=[
                    SchemaDrift(
                        column="email",
                        change_type="removed",
                        expected="varchar",
                        actual="",
                    ),
                ],
            ),
        ]
        html = generate_html_report(sample_suite_result, contract_results=contract_results)
        assert "orders_contract" in html
        assert "email" in html
        assert "Schema Drift" in html

    def test_without_contracts(self, sample_suite_result):
        html = generate_html_report(sample_suite_result, contract_results=None)
        assert "Contract Validation" not in html
        assert "Schema Drift" not in html

    def test_creates_parent_dirs(self, sample_suite_result, tmp_path):
        output = tmp_path / "subdir" / "nested" / "report.html"
        generate_html_report(sample_suite_result, output_path=output)
        assert output.exists()
