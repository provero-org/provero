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

"""Uniqueness checks."""

from __future__ import annotations

from provero.checks.registry import register_check
from provero.connectors.base import Connection
from provero.core.compiler import CheckConfig
from provero.core.results import CheckResult, Severity, Status
from provero.core.sql import quote_identifier


@register_check("unique")
def check_unique(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that column values are unique."""
    col = check_config.column or (check_config.columns[0] if check_config.columns else "")
    qtable = quote_identifier(table)
    qcol = quote_identifier(col)

    result = connection.execute(
        f"SELECT COUNT(*) as total, COUNT(DISTINCT {qcol}) as distinct_count FROM {qtable}"
    )
    row = result[0]
    total = row["total"]
    distinct = row["distinct_count"]
    duplicates = total - distinct

    severity = Severity(check_config.severity) if check_config.severity else Severity.CRITICAL

    return CheckResult(
        check_name=f"unique:{col}",
        check_type="unique",
        status=Status.PASS if duplicates == 0 else Status.FAIL,
        severity=severity,
        column=col,
        observed_value=f"{duplicates} duplicates",
        expected_value="0 duplicates",
        row_count=total,
        failing_rows=duplicates,
        failing_rows_query=(
            f"SELECT {qcol}, COUNT(*) as cnt FROM {qtable} GROUP BY {qcol} HAVING COUNT(*) > 1"
        ),
    )


@register_check("unique_combination")
def check_unique_combination(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that a combination of columns is unique."""
    columns = check_config.columns
    col_str = ", ".join(columns)
    qtable = quote_identifier(table)
    qcols = ", ".join(quote_identifier(c) for c in columns)

    result = connection.execute(f"SELECT COUNT(*) as total FROM {qtable}")
    total = result[0]["total"]

    result = connection.execute(
        f"SELECT COUNT(*) as distinct_count FROM (SELECT DISTINCT {qcols} FROM {qtable})"
    )
    distinct = result[0]["distinct_count"]
    duplicates = total - distinct

    severity = Severity(check_config.severity) if check_config.severity else Severity.CRITICAL

    return CheckResult(
        check_name=f"unique_combination:{col_str}",
        check_type="unique_combination",
        status=Status.PASS if duplicates == 0 else Status.FAIL,
        severity=severity,
        observed_value=f"{duplicates} duplicates",
        expected_value="0 duplicates",
        row_count=total,
        failing_rows=duplicates,
        failing_rows_query=(
            f"SELECT {qcols}, COUNT(*) as cnt FROM {qtable} GROUP BY {qcols} HAVING COUNT(*) > 1"
        ),
    )
