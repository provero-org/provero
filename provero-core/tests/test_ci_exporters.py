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

"""Tests for the SARIF and JUnit CI exporters."""

from __future__ import annotations

import json
from xml.etree import ElementTree as ET

from provero.contracts.models import ContractResult, ContractViolation
from provero.core.results import CheckResult, Severity, Status, SuiteResult
from provero.exporters.junit import build_junit, to_junit
from provero.exporters.sarif import build_sarif, to_sarif


def _make_suite() -> SuiteResult:
    passing = CheckResult(
        check_name="orders_id_not_null",
        check_type="not_null",
        status=Status.PASS,
        severity=Severity.CRITICAL,
        source="duckdb",
        table="orders",
        column="order_id",
    )
    failing = CheckResult(
        check_name="orders_amount_range",
        check_type="range",
        status=Status.FAIL,
        severity=Severity.CRITICAL,
        source="duckdb",
        table="orders",
        column="amount",
        expected_value="[0, 1000]",
        observed_value=-5,
        failing_rows=3,
    )
    suite = SuiteResult(suite_name="orders_suite", status=Status.FAIL, checks=[passing, failing])
    suite.compute_status()
    return suite


class TestSarif:
    def test_top_level_shape(self) -> None:
        doc = build_sarif(_make_suite())
        assert doc["version"] == "2.1.0"
        assert "$schema" in doc
        assert doc["runs"][0]["tool"]["driver"]["name"] == "Provero"

    def test_failure_result_present_with_level(self) -> None:
        doc = build_sarif(_make_suite())
        results = doc["runs"][0]["results"]
        # Only the failing check produces a result; the passing one does not.
        assert len(results) == 1
        result = results[0]
        assert result["ruleId"] == "provero.check.range"
        assert result["level"] == "error"  # CRITICAL severity -> error
        assert "range" in result["message"]["text"]
        loc = result["locations"][0]["logicalLocations"][0]
        assert loc["name"] == "orders.amount"

    def test_warning_level_for_low_severity(self) -> None:
        warn_check = CheckResult(
            check_name="freshness",
            check_type="freshness",
            status=Status.WARN,
            severity=Severity.WARNING,
            table="orders",
        )
        suite = SuiteResult(suite_name="s", status=Status.PASS, checks=[warn_check])
        suite.compute_status()
        result = build_sarif(suite)["runs"][0]["results"][0]
        assert result["level"] == "warning"

    def test_contract_violations_become_results(self) -> None:
        suite = _make_suite()
        contract = ContractResult(
            contract_name="orders_contract",
            status="fail",
            violations=[
                ContractViolation(
                    rule="schema.column_removed",
                    message="column 'legacy' was removed",
                    severity="critical",
                )
            ],
        )
        doc = build_sarif(suite, [contract])
        rule_ids = [r["ruleId"] for r in doc["runs"][0]["results"]]
        assert "provero.contract.schema.column_removed" in rule_ids
        contract_result = next(
            r for r in doc["runs"][0]["results"] if r["ruleId"].startswith("provero.contract")
        )
        assert contract_result["level"] == "error"

    def test_to_sarif_is_valid_json(self) -> None:
        payload = to_sarif(_make_suite())
        parsed = json.loads(payload)
        assert parsed["version"] == "2.1.0"


class TestJUnit:
    def test_counts(self) -> None:
        suite_el = build_junit(_make_suite())
        assert suite_el.get("tests") == "2"
        assert suite_el.get("failures") == "1"
        assert suite_el.get("errors") == "0"

    def test_failing_testcase_has_failure_element(self) -> None:
        suite_el = build_junit(_make_suite())
        testcases = suite_el.findall("testcase")
        assert len(testcases) == 2
        failing = [tc for tc in testcases if tc.find("failure") is not None]
        assert len(failing) == 1
        failure = failing[0].find("failure")
        assert failure is not None
        assert "range" in (failure.get("message") or "")

    def test_to_junit_parses_and_rolls_up(self) -> None:
        xml = to_junit(_make_suite())
        assert xml.startswith("<?xml")
        root = ET.fromstring(xml)
        assert root.tag == "testsuites"
        assert root.get("tests") == "2"
        assert root.get("failures") == "1"
        testsuite = root.find("testsuite")
        assert testsuite is not None
        assert testsuite.get("tests") == "2"

    def test_warn_status_is_not_a_failure(self) -> None:
        # A WARN check is "not failed" per compute_status(); JUnit must agree,
        # otherwise CI goes red for a run Provero considers passing.
        warn_check = CheckResult(
            check_name="freshness",
            check_type="freshness",
            status=Status.WARN,
            severity=Severity.WARNING,
            table="orders",
        )
        suite = SuiteResult(suite_name="s", status=Status.PASS, checks=[warn_check])
        suite.compute_status()
        suite_el = build_junit(suite)
        assert suite_el.get("failures") == "0"
        testcase = suite_el.findall("testcase")[0]
        assert testcase.find("failure") is None
        assert testcase.find("error") is None

    def test_error_status_emits_error_element(self) -> None:
        errored = CheckResult(
            check_name="broken",
            check_type="custom_sql",
            status=Status.ERROR,
            table="orders",
        )
        suite = SuiteResult(suite_name="s", status=Status.FAIL, checks=[errored])
        suite.compute_status()
        suite_el = build_junit(suite)
        assert suite_el.get("errors") == "1"
        assert suite_el.findall("testcase")[0].find("error") is not None
