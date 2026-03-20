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

from provero.checks.registry import register_check
from provero.connectors.base import Connection
from provero.core.compiler import CheckConfig
from provero.core.results import CheckResult, Severity, Status
from provero.core.sql import quote_identifier


@register_check("not_null")
def check_not_null(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that column(s) have no null values."""
    columns = check_config.columns or ([check_config.column] if check_config.column else [])

    severity = Severity(check_config.severity) if check_config.severity else Severity.CRITICAL

    qtable = quote_identifier(table)
    for col in columns:
        qcol = quote_identifier(col)
        result = connection.execute(
            f"SELECT COUNT(*) as total, "
            f"SUM(CASE WHEN {qcol} IS NULL THEN 1 ELSE 0 END) as null_count "
            f"FROM {qtable}"
        )
        row = result[0]
        null_count = row["null_count"] or 0
        total = row["total"] or 0

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
                failing_rows_query=f"SELECT * FROM {qtable} WHERE {qcol} IS NULL",
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


def _normalize_min_completeness(value) -> float:
    """Normalize a min completeness value to a 0-1 ratio.

    Handles:
    - Strings ending with "%": strip %, convert to float, divide by 100
    - Numbers > 1: treat as percentage, divide by 100
    - Numbers <= 1: use as-is (already a ratio)
    """
    if isinstance(value, str):
        value = value.strip()
        if value.endswith("%"):
            value = value[:-1].strip()
        return float(value) / 100 if float(value) > 1 else float(value)
    value = float(value)
    if value > 1:
        return value / 100
    return value


@register_check("completeness")
def check_completeness(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that a column meets a minimum completeness threshold."""
    col = check_config.column or ""
    min_completeness = _normalize_min_completeness(check_config.params.get("min", 0.95))
    qtable = quote_identifier(table)
    qcol = quote_identifier(col)

    result = connection.execute(
        f"SELECT COUNT(*) as total, COUNT({qcol}) as non_null_count FROM {qtable}"
    )
    row = result[0]
    total = row["total"]
    non_null = row["non_null_count"]
    completeness = non_null / total if total > 0 else 0.0

    severity = Severity(check_config.severity) if check_config.severity else Severity.CRITICAL

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
