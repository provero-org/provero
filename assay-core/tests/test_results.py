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

"""Tests for result models and compute_status logic."""

from __future__ import annotations

from datetime import datetime, timezone

from provero.core.results import CheckResult, Severity, Status, SuiteResult


def _check(status: Status, severity: Severity = Severity.CRITICAL) -> CheckResult:
    return CheckResult(
        check_name="test",
        check_type="test",
        status=status,
        severity=severity,
    )


class TestComputeStatus:
    def test_all_pass(self):
        result = SuiteResult(
            suite_name="s",
            status=Status.PASS,
            checks=[_check(Status.PASS), _check(Status.PASS), _check(Status.PASS)],
            started_at=datetime.now(tz=timezone.utc),
        )
        result.compute_status()

        assert result.status == Status.PASS
        assert result.total == 3
        assert result.passed == 3
        assert result.failed == 0
        assert result.quality_score == 100.0

    def test_one_fail(self):
        result = SuiteResult(
            suite_name="s",
            status=Status.PASS,
            checks=[_check(Status.PASS), _check(Status.FAIL), _check(Status.PASS)],
            started_at=datetime.now(tz=timezone.utc),
        )
        result.compute_status()

        assert result.status == Status.FAIL
        assert result.total == 3
        assert result.passed == 2
        assert result.failed == 1
        assert result.quality_score == pytest.approx(66.7, abs=0.1)

    def test_all_fail(self):
        result = SuiteResult(
            suite_name="s",
            status=Status.PASS,
            checks=[_check(Status.FAIL), _check(Status.FAIL)],
            started_at=datetime.now(tz=timezone.utc),
        )
        result.compute_status()

        assert result.status == Status.FAIL
        assert result.quality_score == 0.0

    def test_error_causes_fail_status(self):
        result = SuiteResult(
            suite_name="s",
            status=Status.PASS,
            checks=[_check(Status.PASS), _check(Status.ERROR)],
            started_at=datetime.now(tz=timezone.utc),
        )
        result.compute_status()

        assert result.status == Status.FAIL
        assert result.errored == 1

    def test_warn_does_not_fail_suite(self):
        result = SuiteResult(
            suite_name="s",
            status=Status.PASS,
            checks=[_check(Status.PASS), _check(Status.WARN)],
            started_at=datetime.now(tz=timezone.utc),
        )
        result.compute_status()

        assert result.status == Status.PASS
        assert result.warned == 1

    def test_empty_checks(self):
        result = SuiteResult(
            suite_name="s",
            status=Status.PASS,
            checks=[],
            started_at=datetime.now(tz=timezone.utc),
        )
        result.compute_status()

        assert result.status == Status.PASS
        assert result.total == 0
        assert result.quality_score == 100.0

    def test_single_check(self):
        result = SuiteResult(
            suite_name="s",
            status=Status.PASS,
            checks=[_check(Status.FAIL)],
            started_at=datetime.now(tz=timezone.utc),
        )
        result.compute_status()

        assert result.status == Status.FAIL
        assert result.quality_score == 0.0

    def test_mixed_all_statuses(self):
        result = SuiteResult(
            suite_name="s",
            status=Status.PASS,
            checks=[
                _check(Status.PASS),
                _check(Status.FAIL),
                _check(Status.WARN),
                _check(Status.ERROR),
                _check(Status.SKIP),
            ],
            started_at=datetime.now(tz=timezone.utc),
        )
        result.compute_status()

        assert result.status == Status.FAIL
        assert result.total == 5
        assert result.passed == 1
        assert result.failed == 1
        assert result.warned == 1
        assert result.errored == 1
        # PASS(1) + WARN(1) = 2 ok out of 5 total, all CRITICAL weight
        assert result.quality_score == 40.0


# Need pytest for approx
import pytest
