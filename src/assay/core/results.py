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

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class Status(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    WARN = "warn"
    ERROR = "error"
    SKIP = "skip"


class Severity(str, Enum):
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

    started_at: datetime = Field(default_factory=datetime.now)
    duration_ms: int = 0

    tags: list[str] = Field(default_factory=list)
    suite: str = ""
    run_id: str = ""


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

    started_at: datetime = Field(default_factory=datetime.now)
    duration_ms: int = 0

    quality_score: float = 0.0

    def compute_status(self) -> None:
        """Compute suite status from individual check results."""
        self.total = len(self.checks)
        self.passed = sum(1 for c in self.checks if c.status == Status.PASS)
        self.failed = sum(1 for c in self.checks if c.status == Status.FAIL)
        self.warned = sum(1 for c in self.checks if c.status == Status.WARN)
        self.errored = sum(1 for c in self.checks if c.status == Status.ERROR)

        if self.total == 0:
            self.quality_score = 100.0
            self.status = Status.PASS
            return

        self.quality_score = round((self.passed / self.total) * 100, 1)
        self.status = Status.PASS if self.failed == 0 and self.errored == 0 else Status.FAIL
