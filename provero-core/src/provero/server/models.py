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

"""Pydantic request/response models for the Provero server API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    """Liveness/readiness probe response."""

    status: str
    ready: bool = True


class SuiteSummary(BaseModel):
    """Lightweight description of a configured suite."""

    name: str
    source_type: str
    table: str
    check_count: int
    tags: list[str] = Field(default_factory=list)
    schedule: str | None = None


class SuiteListResponse(BaseModel):
    """Response listing all configured suites."""

    suites: list[SuiteSummary] = Field(default_factory=list)


class RunSummary(BaseModel):
    """One persisted run record from the result store."""

    id: str
    suite_name: str
    status: str
    trigger: str = "manual"
    total: int = 0
    passed: int = 0
    failed: int = 0
    warned: int = 0
    errored: int = 0
    quality_score: float | None = None
    duration_ms: int | None = None
    started_at: str
    completed_at: str | None = None


class RunListResponse(BaseModel):
    """Response listing recent runs."""

    runs: list[RunSummary] = Field(default_factory=list)


class CheckResultRecord(BaseModel):
    """One persisted check result belonging to a run."""

    id: int
    run_id: str
    check_name: str
    check_type: str
    status: str
    severity: str
    source_table: str | None = None
    source_column: str | None = None
    observed_value: str | None = None
    expected_value: str | None = None
    row_count: int | None = None
    failing_rows: int | None = None
    duration_ms: int | None = None


class RunDetailResponse(BaseModel):
    """Full detail for a single run, including its check results."""

    run: RunSummary
    checks: list[CheckResultRecord] = Field(default_factory=list)
