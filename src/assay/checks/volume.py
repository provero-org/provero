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

"""Volume checks: row_count."""

from __future__ import annotations

from assay.checks.registry import register_check
from assay.connectors.base import Connection
from assay.core.compiler import CheckConfig
from assay.core.results import CheckResult, Severity, Status


@register_check("row_count")
def check_row_count(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that table has expected number of rows."""
    min_count = check_config.params.get("min", 0)
    max_count = check_config.params.get("max")

    result = connection.execute(f"SELECT COUNT(*) as total FROM {table}")
    total = result[0]["total"]

    passed = total >= min_count
    if max_count is not None:
        passed = passed and total <= max_count

    expected_parts = []
    if min_count > 0:
        expected_parts.append(f">= {min_count:,}")
    if max_count is not None:
        expected_parts.append(f"<= {max_count:,}")

    severity = (
        Severity(check_config.severity)
        if check_config.severity
        else Severity.CRITICAL
    )

    return CheckResult(
        check_name="row_count",
        check_type="row_count",
        status=Status.PASS if passed else Status.FAIL,
        severity=severity,
        observed_value=f"{total:,}",
        expected_value=" and ".join(expected_parts) if expected_parts else "> 0",
        row_count=total,
    )
