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

"""Check execution engine."""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone

from assay.checks.registry import get_check_runner
from assay.connectors.base import Connector
from assay.core.compiler import SuiteConfig
from assay.core.results import CheckResult, Severity, Status, SuiteResult


def run_suite(suite: SuiteConfig, connector: Connector) -> SuiteResult:
    """Execute all checks in a suite against a data source."""
    run_id = str(uuid.uuid4())
    suite_start = datetime.now(tz=timezone.utc)

    results: list[CheckResult] = []

    connection = connector.connect()

    for check_config in suite.checks:
        check_start = time.monotonic()

        runner = get_check_runner(check_config.check_type)
        if runner is None:
            results.append(
                CheckResult(
                    check_name=f"{check_config.check_type}:{check_config.column or ''}",
                    check_type=check_config.check_type,
                    status=Status.ERROR,
                    severity=Severity.CRITICAL,
                    source=suite.source.type,
                    table=suite.source.table,
                    column=check_config.column,
                    observed_value=f"Unknown check type: {check_config.check_type}",
                    run_id=run_id,
                    suite=suite.name,
                )
            )
            continue

        try:
            result = runner(
                connection=connection,
                table=suite.source.table,
                check_config=check_config,
            )
            result.run_id = run_id
            result.suite = suite.name
            result.source = suite.source.type
            result.table = suite.source.table
            result.duration_ms = int((time.monotonic() - check_start) * 1000)
            results.append(result)
        except Exception as e:
            results.append(
                CheckResult(
                    check_name=f"{check_config.check_type}:{check_config.column or ''}",
                    check_type=check_config.check_type,
                    status=Status.ERROR,
                    severity=Severity.CRITICAL,
                    source=suite.source.type,
                    table=suite.source.table,
                    column=check_config.column,
                    observed_value=str(e),
                    duration_ms=int((time.monotonic() - check_start) * 1000),
                    run_id=run_id,
                    suite=suite.name,
                )
            )

    connector.disconnect(connection)

    suite_result = SuiteResult(
        suite_name=suite.name,
        status=Status.PASS,
        checks=results,
        started_at=suite_start,
        duration_ms=int((time.monotonic() - suite_start.timestamp() + time.time() - time.monotonic()) * 1000),
    )
    suite_result.compute_status()

    return suite_result
