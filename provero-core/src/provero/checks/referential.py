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

"""Referential integrity check: validates foreign key relationships between tables."""

from __future__ import annotations

from provero.checks.registry import register_check
from provero.connectors.base import Connection
from provero.core.compiler import CheckConfig
from provero.core.results import CheckResult, Severity, Status
from provero.core.sql import quote_identifier


@register_check("referential_integrity")
def check_referential_integrity(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that all non-null values in a column exist in a reference table's column.

    Validates foreign key relationships by finding orphaned rows where the FK
    value does not exist in the referenced table. NULL values in the source
    column are excluded (NULLs are valid for optional foreign keys).
    """
    col = check_config.column
    reference_table = check_config.params.get("reference_table")
    reference_column = check_config.params.get("reference_column")

    severity = Severity(check_config.severity) if check_config.severity else Severity.CRITICAL

    if not col:
        return CheckResult(
            check_name="referential_integrity",
            check_type="referential_integrity",
            status=Status.ERROR,
            severity=severity,
            observed_value="missing required parameter: column",
            expected_value="column name",
        )

    if not reference_table:
        return CheckResult(
            check_name=f"referential_integrity:{col}",
            check_type="referential_integrity",
            status=Status.ERROR,
            severity=severity,
            column=col,
            observed_value="missing required parameter: reference_table",
            expected_value="reference table name",
        )

    if not reference_column:
        return CheckResult(
            check_name=f"referential_integrity:{col}",
            check_type="referential_integrity",
            status=Status.ERROR,
            severity=severity,
            column=col,
            observed_value="missing required parameter: reference_column",
            expected_value="reference column name",
        )

    qtable = quote_identifier(table)
    qcol = quote_identifier(col)
    qref_table = quote_identifier(reference_table)
    qref_col = quote_identifier(reference_column)

    # Count orphaned rows: source rows where the FK value doesn't exist
    # in the reference table. NULLs in the source column are excluded.
    try:
        result = connection.execute(
            f"SELECT COUNT(*) as orphaned_count "
            f"FROM {qtable} s "
            f"LEFT JOIN {qref_table} r ON s.{qcol} = r.{qref_col} "
            f"WHERE r.{qref_col} IS NULL AND s.{qcol} IS NOT NULL"
        )
    except Exception as e:
        error_msg = str(e)
        return CheckResult(
            check_name=f"referential_integrity:{col}",
            check_type="referential_integrity",
            status=Status.ERROR,
            severity=severity,
            column=col,
            observed_value=f"query error: {error_msg}",
            expected_value="0 orphaned rows",
        )

    orphaned = result[0]["orphaned_count"]

    # Get total non-null rows for context
    total_result = connection.execute(
        f"SELECT COUNT(*) as total FROM {qtable} WHERE {qcol} IS NOT NULL"
    )
    total = total_result[0]["total"]

    failing_rows_query = (
        f"SELECT s.* FROM {qtable} s "
        f"LEFT JOIN {qref_table} r ON s.{qcol} = r.{qref_col} "
        f"WHERE r.{qref_col} IS NULL AND s.{qcol} IS NOT NULL"
    )

    return CheckResult(
        check_name=f"referential_integrity:{col}",
        check_type="referential_integrity",
        status=Status.PASS if orphaned == 0 else Status.FAIL,
        severity=severity,
        column=col,
        observed_value=f"{orphaned} orphaned rows",
        expected_value="0 orphaned rows",
        row_count=total,
        failing_rows=orphaned,
        failing_rows_query=failing_rows_query,
    )
