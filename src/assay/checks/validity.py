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

"""Validity checks: accepted_values, range, regex."""

from __future__ import annotations

from assay.checks.registry import register_check
from assay.connectors.base import Connection
from assay.core.compiler import CheckConfig
from assay.core.results import CheckResult, Severity, Status


@register_check("accepted_values")
def check_accepted_values(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that column only contains accepted values."""
    col = check_config.column
    values = check_config.params.get("values", [])
    placeholders = ", ".join(f"'{v}'" for v in values)

    result = connection.execute(
        f"SELECT COUNT(*) as total, "
        f"COUNT(*) FILTER (WHERE {col} NOT IN ({placeholders})) as invalid_count "
        f"FROM {table} WHERE {col} IS NOT NULL"
    )
    row = result[0]
    total = row["total"]
    invalid = row["invalid_count"]

    return CheckResult(
        check_name=f"accepted_values:{col}",
        check_type="accepted_values",
        status=Status.PASS if invalid == 0 else Status.FAIL,
        severity=Severity.CRITICAL,
        column=col,
        observed_value=f"{invalid} invalid values",
        expected_value=f"only {values}",
        row_count=total,
        failing_rows=invalid,
        failing_rows_query=(
            f"SELECT DISTINCT {col} FROM {table} "
            f"WHERE {col} NOT IN ({placeholders}) AND {col} IS NOT NULL"
        ),
    )


@register_check("range")
def check_range(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that column values fall within a range."""
    col = check_config.column
    min_val = check_config.params.get("min")
    max_val = check_config.params.get("max")

    conditions = []
    if min_val is not None:
        conditions.append(f"{col} < {min_val}")
    if max_val is not None:
        conditions.append(f"{col} > {max_val}")

    where = " OR ".join(conditions) if conditions else "FALSE"

    result = connection.execute(
        f"SELECT COUNT(*) as total, "
        f"COUNT(*) FILTER (WHERE {where}) as out_of_range, "
        f"MIN({col}) as min_val, MAX({col}) as max_val "
        f"FROM {table} WHERE {col} IS NOT NULL"
    )
    row = result[0]
    total = row["total"]
    out_of_range = row["out_of_range"]

    expected = []
    if min_val is not None:
        expected.append(f"min={min_val}")
    if max_val is not None:
        expected.append(f"max={max_val}")

    return CheckResult(
        check_name=f"range:{col}",
        check_type="range",
        status=Status.PASS if out_of_range == 0 else Status.FAIL,
        severity=Severity.CRITICAL,
        column=col,
        observed_value=f"min={row['min_val']}, max={row['max_val']}",
        expected_value=", ".join(expected),
        row_count=total,
        failing_rows=out_of_range,
        failing_rows_query=f"SELECT * FROM {table} WHERE {where}",
    )


@register_check("regex")
def check_regex(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that column values match a regex pattern."""
    col = check_config.column
    pattern = check_config.params.get("pattern", "")

    result = connection.execute(
        f"SELECT COUNT(*) as total, "
        f"COUNT(*) FILTER (WHERE NOT regexp_matches({col}, '{pattern}')) as non_matching "
        f"FROM {table} WHERE {col} IS NOT NULL"
    )
    row = result[0]
    total = row["total"]
    non_matching = row["non_matching"]

    return CheckResult(
        check_name=f"regex:{col}",
        check_type="regex",
        status=Status.PASS if non_matching == 0 else Status.FAIL,
        severity=Severity.WARNING,
        column=col,
        observed_value=f"{non_matching} non-matching",
        expected_value=f"matches /{pattern}/",
        row_count=total,
        failing_rows=non_matching,
    )
