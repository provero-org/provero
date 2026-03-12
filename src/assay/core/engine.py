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
from assay.core.optimizer import execute_batch, plan_batch
from assay.core.results import CheckResult, Severity, Status, SuiteResult


def run_suite(
    suite: SuiteConfig,
    connector: Connector,
    optimize: bool = True,
) -> SuiteResult:
    """Execute all checks in a suite against a data source.

    When optimize=True (default), batchable checks are compiled into a
    single SQL query. Non-batchable checks run individually after.
    """
    run_id = str(uuid.uuid4())
    suite_start = time.monotonic()
    started_at = datetime.now(tz=timezone.utc)

    results: list[CheckResult] = []
    connection = connector.connect()

    if optimize:
        plan = plan_batch(suite.source.table, suite.checks)

        # Execute batched checks (single query)
        if plan.metrics:
            batch_start = time.monotonic()
            try:
                batch_results = execute_batch(connection, plan)
                batch_ms = int((time.monotonic() - batch_start) * 1000)
                for r in batch_results:
                    r.run_id = run_id
                    r.suite = suite.name
                    r.source = suite.source.type
                    r.table = suite.source.table
                    r.duration_ms = batch_ms
                results.extend(batch_results)
            except Exception as e:
                results.append(CheckResult(
                    check_name="batch_query",
                    check_type="batch",
                    status=Status.ERROR,
                    severity=Severity.CRITICAL,
                    source=suite.source.type,
                    table=suite.source.table,
                    observed_value=(
                        f"Batch query failed: {e}. "
                        f"Try running with --no-optimize to isolate the failing check, "
                        f"or verify that the table '{suite.source.table}' exists and is accessible."
                    ),
                    run_id=run_id,
                    suite=suite.name,
                ))

        # Execute non-batchable checks individually
        remaining = plan.non_batchable
    else:
        remaining = suite.checks

    for check_config in remaining:
        check_start = time.monotonic()

        runner = get_check_runner(check_config.check_type)
        if runner is None:
            from assay.checks.registry import list_checks
            available = ", ".join(sorted(list_checks()))
            results.append(CheckResult(
                check_name=f"{check_config.check_type}:{check_config.column or ''}",
                check_type=check_config.check_type,
                status=Status.ERROR,
                severity=Severity.CRITICAL,
                source=suite.source.type,
                table=suite.source.table,
                column=check_config.column,
                observed_value=(
                    f"Unknown check type '{check_config.check_type}'. "
                    f"Available types: {available}"
                ),
                run_id=run_id,
                suite=suite.name,
            ))
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
            error_msg = str(e)
            hint = ""
            if "does not exist" in error_msg.lower() or "not found" in error_msg.lower():
                hint = f" Verify that table '{suite.source.table}' exists."
            elif "permission" in error_msg.lower() or "denied" in error_msg.lower():
                hint = " Check your database permissions."
            elif "connection" in error_msg.lower() or "refused" in error_msg.lower():
                hint = " Verify that the database is running and the connection string is correct."
            results.append(CheckResult(
                check_name=f"{check_config.check_type}:{check_config.column or ''}",
                check_type=check_config.check_type,
                status=Status.ERROR,
                severity=Severity.CRITICAL,
                source=suite.source.type,
                table=suite.source.table,
                column=check_config.column,
                observed_value=f"{error_msg}{hint}",
                duration_ms=int((time.monotonic() - check_start) * 1000),
                run_id=run_id,
                suite=suite.name,
            ))

    connector.disconnect(connection)

    total_ms = int((time.monotonic() - suite_start) * 1000)
    suite_result = SuiteResult(
        suite_name=suite.name,
        status=Status.PASS,
        checks=results,
        started_at=started_at,
        duration_ms=total_ms,
    )
    suite_result.compute_status()

    return suite_result
