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

from assay.checks.registry import register_check
from assay.connectors.base import Connection
from assay.core.compiler import CheckConfig
from assay.core.results import CheckResult, Severity, Status


@register_check("unique")
def check_unique(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that column values are unique."""
    col = check_config.column or (check_config.columns[0] if check_config.columns else "")

    result = connection.execute(
        f"SELECT COUNT(*) as total, COUNT(DISTINCT {col}) as distinct_count FROM {table}"
    )
    row = result[0]
    total = row["total"]
    distinct = row["distinct_count"]
    duplicates = total - distinct

    return CheckResult(
        check_name=f"unique:{col}",
        check_type="unique",
        status=Status.PASS if duplicates == 0 else Status.FAIL,
        severity=Severity.CRITICAL,
        column=col,
        observed_value=f"{duplicates} duplicates",
        expected_value="0 duplicates",
        row_count=total,
        failing_rows=duplicates,
        failing_rows_query=(
            f"SELECT {col}, COUNT(*) as cnt FROM {table} "
            f"GROUP BY {col} HAVING COUNT(*) > 1"
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

    result = connection.execute(
        f"SELECT COUNT(*) as total FROM {table}"
    )
    total = result[0]["total"]

    result = connection.execute(
        f"SELECT COUNT(*) as distinct_count FROM (SELECT DISTINCT {col_str} FROM {table})"
    )
    distinct = result[0]["distinct_count"]
    duplicates = total - distinct

    return CheckResult(
        check_name=f"unique_combination:{col_str}",
        check_type="unique_combination",
        status=Status.PASS if duplicates == 0 else Status.FAIL,
        severity=Severity.CRITICAL,
        observed_value=f"{duplicates} duplicates",
        expected_value="0 duplicates",
        row_count=total,
        failing_rows=duplicates,
        failing_rows_query=(
            f"SELECT {col_str}, COUNT(*) as cnt FROM {table} "
            f"GROUP BY {col_str} HAVING COUNT(*) > 1"
        ),
    )
