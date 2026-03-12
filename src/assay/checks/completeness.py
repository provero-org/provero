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

"""Completeness checks: not_null, completeness."""

from __future__ import annotations

from assay.checks.registry import register_check
from assay.connectors.base import Connection
from assay.core.compiler import CheckConfig
from assay.core.results import CheckResult, Severity, Status


@register_check("not_null")
def check_not_null(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that column(s) have no null values."""
    columns = check_config.columns or ([check_config.column] if check_config.column else [])

    severity = (
        Severity(check_config.severity)
        if check_config.severity
        else Severity.CRITICAL
    )

    for col in columns:
        result = connection.execute(
            f"SELECT COUNT(*) as total, "
            f"COUNT(*) FILTER (WHERE {col} IS NULL) as null_count "
            f"FROM {table}"
        )
        row = result[0]
        null_count = row["null_count"]
        total = row["total"]

        if null_count > 0:
            return CheckResult(
                check_name=f"not_null:{col}",
                check_type="not_null",
                status=Status.FAIL,
                severity=severity,
                column=col,
                observed_value=f"{null_count} nulls",
                expected_value="0 nulls",
                row_count=total,
                failing_rows=null_count,
                failing_rows_query=f"SELECT * FROM {table} WHERE {col} IS NULL",
            )

    col_str = ", ".join(columns)
    return CheckResult(
        check_name=f"not_null:{col_str}",
        check_type="not_null",
        status=Status.PASS,
        severity=severity,
        column=columns[0] if len(columns) == 1 else None,
        observed_value="0 nulls",
        expected_value="0 nulls",
    )


@register_check("completeness")
def check_completeness(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that a column meets a minimum completeness threshold."""
    col = check_config.column
    min_completeness = check_config.params.get("min", 0.95)

    result = connection.execute(
        f"SELECT COUNT(*) as total, "
        f"COUNT({col}) as non_null_count "
        f"FROM {table}"
    )
    row = result[0]
    total = row["total"]
    non_null = row["non_null_count"]
    completeness = non_null / total if total > 0 else 0.0

    severity = (
        Severity(check_config.severity)
        if check_config.severity
        else Severity.CRITICAL
    )

    return CheckResult(
        check_name=f"completeness:{col}",
        check_type="completeness",
        status=Status.PASS if completeness >= min_completeness else Status.FAIL,
        severity=severity,
        column=col,
        observed_value=f"{completeness:.1%}",
        expected_value=f">= {min_completeness:.1%}",
        row_count=total,
        failing_rows=total - non_null,
    )
