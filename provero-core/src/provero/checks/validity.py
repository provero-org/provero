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

"""Validity checks: accepted_values, range, regex, type."""

from __future__ import annotations

from provero.checks.registry import register_check
from provero.connectors.base import Connection
from provero.core.compiler import CheckConfig
from provero.core.results import CheckResult, Severity, Status
from provero.core.sql import quote_identifier, quote_value


@register_check("accepted_values")
def check_accepted_values(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that column only contains accepted values."""
    col = check_config.column or ""
    values = check_config.params.get("values", [])
    qtable = quote_identifier(table)
    qcol = quote_identifier(col)
    placeholders = ", ".join(f"'{quote_value(str(v))}'" for v in values)

    result = connection.execute(
        f"SELECT COUNT(*) as total, "
        f"COUNT(*) FILTER (WHERE {qcol} NOT IN ({placeholders})) as invalid_count "
        f"FROM {qtable} WHERE {qcol} IS NOT NULL"
    )
    row = result[0]
    total = row["total"]
    invalid = row["invalid_count"]

    severity = Severity(check_config.severity) if check_config.severity else Severity.CRITICAL

    return CheckResult(
        check_name=f"accepted_values:{col}",
        check_type="accepted_values",
        status=Status.PASS if invalid == 0 else Status.FAIL,
        severity=severity,
        column=col,
        observed_value=f"{invalid} invalid values",
        expected_value=f"only {values}",
        row_count=total,
        failing_rows=invalid,
        failing_rows_query=(
            f"SELECT DISTINCT {qcol} FROM {qtable} "
            f"WHERE {qcol} NOT IN ({placeholders}) AND {qcol} IS NOT NULL"
        ),
    )


@register_check("range")
def check_range(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that column values fall within a range."""
    col = check_config.column or ""
    min_val = check_config.params.get("min")
    max_val = check_config.params.get("max")
    qtable = quote_identifier(table)
    qcol = quote_identifier(col)

    conditions = []
    if min_val is not None:
        conditions.append(f"{qcol} < {min_val}")
    if max_val is not None:
        conditions.append(f"{qcol} > {max_val}")

    where = " OR ".join(conditions) if conditions else "FALSE"

    result = connection.execute(
        f"SELECT COUNT(*) as total, "
        f"COUNT(*) FILTER (WHERE {where}) as out_of_range, "
        f"MIN({qcol}) as min_val, MAX({qcol}) as max_val "
        f"FROM {qtable} WHERE {qcol} IS NOT NULL"
    )
    row = result[0]
    total = row["total"]
    out_of_range = row["out_of_range"]

    expected = []
    if min_val is not None:
        expected.append(f"min={min_val}")
    if max_val is not None:
        expected.append(f"max={max_val}")

    severity = Severity(check_config.severity) if check_config.severity else Severity.CRITICAL

    return CheckResult(
        check_name=f"range:{col}",
        check_type="range",
        status=Status.PASS if out_of_range == 0 else Status.FAIL,
        severity=severity,
        column=col,
        observed_value=f"min={row['min_val']}, max={row['max_val']}",
        expected_value=", ".join(expected),
        row_count=total,
        failing_rows=out_of_range,
        failing_rows_query=f"SELECT * FROM {qtable} WHERE {where}",
    )


@register_check("regex")
def check_regex(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that column values match a regex pattern.

    Uses regexp_matches() for DuckDB, falls back to col ~ 'pattern'
    (PostgreSQL) and REGEXP (MySQL/SQLite) for cross-database compatibility.
    """
    col = check_config.column or ""
    pattern = check_config.params.get("pattern", "")
    qtable = quote_identifier(table)
    qcol = quote_identifier(col)
    safe_pattern = quote_value(pattern)
    severity = Severity(check_config.severity) if check_config.severity else Severity.WARNING

    # Try DuckDB syntax first, then PostgreSQL ~, then MySQL/SQLite REGEXP
    queries = [
        (
            f"SELECT COUNT(*) as total, "
            f"COUNT(*) FILTER (WHERE NOT regexp_matches({qcol}, '{safe_pattern}')) as non_matching "
            f"FROM {qtable} WHERE {qcol} IS NOT NULL"
        ),
        (
            f"SELECT COUNT(*) as total, "
            f"SUM(CASE WHEN NOT ({qcol} ~ '{safe_pattern}') THEN 1 ELSE 0 END) as non_matching "
            f"FROM {qtable} WHERE {qcol} IS NOT NULL"
        ),
        (
            f"SELECT COUNT(*) as total, "
            f"SUM(CASE WHEN NOT ({qcol} REGEXP '{safe_pattern}') "
            f"THEN 1 ELSE 0 END) as non_matching "
            f"FROM {qtable} WHERE {qcol} IS NOT NULL"
        ),
    ]

    row = None
    for query in queries:
        try:
            result = connection.execute(query)
            row = result[0]
            break
        except Exception:
            continue

    if row is None:
        return CheckResult(
            check_name=f"regex:{col}",
            check_type="regex",
            status=Status.ERROR,
            severity=severity,
            column=col,
            observed_value="Regex not supported by this database. "
            "DuckDB (regexp_matches), PostgreSQL (~), and MySQL/SQLite (REGEXP) are supported.",
            expected_value=f"matches /{pattern}/",
        )

    total = row["total"]
    non_matching = int(row["non_matching"])

    return CheckResult(
        check_name=f"regex:{col}",
        check_type="regex",
        status=Status.PASS if non_matching == 0 else Status.FAIL,
        severity=severity,
        column=col,
        observed_value=f"{non_matching} non-matching",
        expected_value=f"matches /{pattern}/",
        row_count=total,
        failing_rows=non_matching,
    )


# Type mapping from common SQL types to normalized categories
_TYPE_MAP: dict[str, set[str]] = {
    "integer": {
        "integer",
        "int",
        "int4",
        "int8",
        "int2",
        "bigint",
        "smallint",
        "tinyint",
        "hugeint",
    },
    "float": {"float", "double", "real", "float4", "float8", "numeric", "decimal", "number"},
    "string": {"varchar", "text", "char", "string", "nvarchar", "nchar", "bpchar", "name"},
    "boolean": {"boolean", "bool"},
    "date": {"date"},
    "timestamp": {"timestamp", "timestamptz", "timestamp with time zone", "datetime"},
    "time": {"time", "timetz"},
}


def _normalize_type(db_type: str) -> str:
    """Normalize a database-specific type to a canonical category."""
    lower = db_type.lower().split("(")[0].strip()
    for category, variants in _TYPE_MAP.items():
        if lower in variants:
            return category
    return lower


@register_check("type")
def check_type(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that a column has the expected data type."""
    col = check_config.column or ""
    expected_type = check_config.params.get("expected", "")

    columns = connection.get_columns(table)
    actual_type = None
    for c in columns:
        if col and c["name"].lower() == col.lower():
            actual_type = c["type"]
            break

    severity = Severity(check_config.severity) if check_config.severity else Severity.CRITICAL

    if actual_type is None:
        return CheckResult(
            check_name=f"type:{col}",
            check_type="type",
            status=Status.ERROR,
            severity=severity,
            column=col,
            observed_value=f"column '{col}' not found",
            expected_value=expected_type,
        )

    normalized_actual = _normalize_type(actual_type)
    normalized_expected = _normalize_type(expected_type)
    passed = normalized_actual == normalized_expected

    return CheckResult(
        check_name=f"type:{col}",
        check_type="type",
        status=Status.PASS if passed else Status.FAIL,
        severity=severity,
        column=col,
        observed_value=actual_type,
        expected_value=expected_type,
    )
