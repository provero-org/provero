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

"""Result models for check execution."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from functools import partial
from typing import Any

from pydantic import BaseModel, Field


class Status(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    WARN = "warn"
    ERROR = "error"
    SKIP = "skip"


class Severity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    BLOCKER = "blocker"


class CheckResult(BaseModel):
    """Result of a single check execution."""

    check_name: str
    check_type: str
    status: Status
    severity: Severity = Severity.CRITICAL

    source: str = ""
    table: str = ""
    column: str | None = None

    observed_value: Any = None
    expected_value: Any = None

    row_count: int = 0
    failing_rows: int = 0
    failing_rows_sample: list[dict[str, Any]] = Field(default_factory=list)
    failing_rows_query: str = ""

    started_at: datetime = Field(default_factory=partial(datetime.now, tz=UTC))
    duration_ms: int = 0

    tags: list[str] = Field(default_factory=list)
    suite: str = ""
    run_id: str = ""

    def apply_severity(self) -> None:
        """Downgrade FAIL to WARN for checks with INFO or WARNING severity.

        Per the AQL spec, INFO/WARNING failures are logged but do not block
        the suite from passing.
        """
        if self.status == Status.FAIL and self.severity in (Severity.INFO, Severity.WARNING):
            self.status = Status.WARN


class SuiteResult(BaseModel):
    """Result of a full suite execution."""

    suite_name: str
    status: Status

    checks: list[CheckResult] = Field(default_factory=list)

    total: int = 0
    passed: int = 0
    failed: int = 0
    warned: int = 0
    errored: int = 0

    started_at: datetime = Field(default_factory=partial(datetime.now, tz=UTC))
    duration_ms: int = 0

    quality_score: float = 0.0

    _SEVERITY_WEIGHT: dict[Severity, float] = {
        Severity.INFO: 0.25,
        Severity.WARNING: 0.5,
        Severity.CRITICAL: 1.0,
        Severity.BLOCKER: 1.0,
    }

    def compute_status(self) -> None:
        """Compute suite status from individual check results.

        Quality score is weighted by severity: a failing INFO check has less
        impact than a failing CRITICAL check.
        """
        self.total = len(self.checks)
        self.passed = sum(1 for c in self.checks if c.status == Status.PASS)
        self.failed = sum(1 for c in self.checks if c.status == Status.FAIL)
        self.warned = sum(1 for c in self.checks if c.status == Status.WARN)
        self.errored = sum(1 for c in self.checks if c.status == Status.ERROR)

        if self.total == 0:
            self.quality_score = 100.0
            self.status = Status.PASS
            return

        total_weight = sum(self._SEVERITY_WEIGHT.get(c.severity, 1.0) for c in self.checks)
        # PASS and WARN both count as "not failed" in the quality score.
        # WARN means the check detected an issue but severity is too low to block.
        ok_weight = sum(
            self._SEVERITY_WEIGHT.get(c.severity, 1.0)
            for c in self.checks
            if c.status in (Status.PASS, Status.WARN)
        )
        self.quality_score = (
            round((ok_weight / total_weight) * 100, 1) if total_weight > 0 else 100.0
        )
        self.status = Status.PASS if self.failed == 0 and self.errored == 0 else Status.FAIL
