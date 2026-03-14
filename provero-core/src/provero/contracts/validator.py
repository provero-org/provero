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

"""Data contract validation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from provero.contracts.models import (
    ContractConfig,
    ContractResult,
    ContractViolation,
    SchemaDrift,
    ViolationAction,
)

if TYPE_CHECKING:
    from provero.connectors.base import Connection
    from provero.core.compiler import SourceConfig


def validate_contract(
    contract: ContractConfig,
    connection: Connection,
    sources: dict[str, SourceConfig] | None = None,
) -> ContractResult:
    """Validate a data contract against a live data source.

    Performs three types of validation:
    1. Schema check: compares actual columns against contract definition.
    2. SLA check: validates freshness, completeness, availability.
    3. Per-column checks: expands column-level checks (not_null, unique).
    """
    violations: list[ContractViolation] = []
    drift: list[SchemaDrift] = []
    severity = "critical" if contract.on_violation == ViolationAction.BLOCK else "warning"

    table = contract.table
    if not table:
        return ContractResult(
            contract_name=contract.name,
            status="fail",
            violations=[
                ContractViolation(
                    rule="contract.table",
                    message="No table specified in contract",
                    severity=severity,
                )
            ],
        )

    # 1. Schema check
    if contract.schema_def.columns:
        try:
            actual_columns = connection.get_columns(table)
        except Exception as e:
            return ContractResult(
                contract_name=contract.name,
                status="fail",
                violations=[
                    ContractViolation(
                        rule="schema",
                        message=f"Could not retrieve schema: {e}",
                        severity=severity,
                    )
                ],
            )

        actual_map: dict[str, str] = {}
        for col in actual_columns:
            col_name = col.get("name", col.get("column_name", ""))
            col_type = col.get("type", col.get("data_type", ""))
            actual_map[col_name.lower()] = str(col_type).lower()

        expected_names = set()
        for col_contract in contract.schema_def.columns:
            expected_names.add(col_contract.name.lower())
            actual_type = actual_map.get(col_contract.name.lower())

            if actual_type is None:
                drift.append(
                    SchemaDrift(
                        column=col_contract.name,
                        change_type="removed",
                        expected=col_contract.type,
                        actual="",
                    )
                )
                violations.append(
                    ContractViolation(
                        rule="schema.column_missing",
                        message=f"Column '{col_contract.name}' missing from table",
                        severity=severity,
                    )
                )
            elif col_contract.type and not _types_compatible(col_contract.type, actual_type):
                drift.append(
                    SchemaDrift(
                        column=col_contract.name,
                        change_type="type_changed",
                        expected=col_contract.type,
                        actual=actual_type,
                    )
                )
                violations.append(
                    ContractViolation(
                        rule="schema.type_mismatch",
                        message=(
                            f"Column '{col_contract.name}' type:"
                            f" expected '{col_contract.type}', got '{actual_type}'"
                        ),
                        severity=severity,
                    )
                )

        for actual_name in actual_map:
            if actual_name not in expected_names:
                drift.append(
                    SchemaDrift(
                        column=actual_name,
                        change_type="added",
                        expected="",
                        actual=actual_map[actual_name],
                    )
                )

        # Per-column checks
        for col_contract in contract.schema_def.columns:
            if col_contract.name.lower() not in actual_map:
                continue
            for check_def in col_contract.checks:
                v = _run_column_check(connection, table, col_contract.name, check_def, severity)
                if v:
                    violations.append(v)

    # 2. SLA checks
    sla = contract.sla

    if sla.freshness:
        v = _check_freshness_sla(connection, table, sla.freshness, severity)
        if v:
            violations.append(v)

    if sla.completeness:
        v = _check_completeness_sla(connection, table, sla.completeness, contract, severity)
        if v:
            violations.append(v)

    if sla.availability:
        v = _check_availability_sla(connection, table, severity)
        if v:
            violations.append(v)

    # Determine status
    blocking = [v for v in violations if v.severity == "critical"]
    if blocking:
        status = "fail"
    elif violations:
        status = "warn" if contract.on_violation == ViolationAction.WARN else "fail"
    else:
        status = "pass"

    return ContractResult(
        contract_name=contract.name,
        status=status,
        violations=violations,
        schema_drift=drift,
    )


def _types_compatible(expected: str, actual: str) -> bool:
    """Check if expected and actual types are compatible."""
    e = expected.lower().strip()
    a = actual.lower().strip()
    if e == a:
        return True

    type_groups: list[set[str]] = [
        {"integer", "int", "bigint", "smallint", "int4", "int8", "int2", "int32", "int64"},
        {"varchar", "text", "string", "char", "character varying"},
        {"decimal", "numeric", "float", "double", "real", "float4", "float8", "double precision"},
        {"boolean", "bool"},
        {"timestamp", "timestamptz", "timestamp with time zone", "datetime"},
        {"date"},
    ]
    for group in type_groups:
        if e in group and a in group:
            return True

    # Handle parameterized types like decimal(10,2) matching decimal
    e_base = e.split("(")[0]
    a_base = a.split("(")[0]
    if e_base == a_base:
        return True

    # Check type groups with base types too
    return any(e_base in group and a_base in group for group in type_groups)


def _run_column_check(
    connection: Connection,
    table: str,
    column: str,
    check_def: str | dict,
    severity: str,
) -> ContractViolation | None:
    """Run a per-column check from a contract definition.

    Supports both simple names ("not_null") and parametrized dicts
    ({"range": {"min": 0.01}}, {"accepted_values": ["USD", "EUR"]}).
    """
    from provero.checks.registry import get_check_runner
    from provero.core.compiler import CheckConfig
    from provero.core.results import Status

    if isinstance(check_def, str):
        check_name = check_def
        params: dict = {}
    elif isinstance(check_def, dict):
        check_name = next(iter(check_def))
        raw_value = check_def[check_name]
        if isinstance(raw_value, dict):
            params = raw_value
        elif isinstance(raw_value, list):
            params = {"values": raw_value}
        else:
            params = {"value": raw_value}
    else:
        return None

    runner = get_check_runner(check_name)
    if runner is None:
        return None

    config = CheckConfig(check_type=check_name, column=column, params=params)
    try:
        result = runner(connection=connection, table=table, check_config=config)
        if result.status == Status.FAIL:
            return ContractViolation(
                rule=f"column.{check_name}",
                message=f"Column '{column}' failed {check_name}: {result.observed_value}",
                severity=severity,
            )
    except Exception as e:
        return ContractViolation(
            rule=f"column.{check_name}",
            message=f"Column '{column}' check '{check_name}' error: {e}",
            severity=severity,
        )

    return None


def _check_freshness_sla(
    connection: Connection,
    table: str,
    freshness_str: str,
    severity: str,
) -> ContractViolation | None:
    """Check freshness SLA by finding the most recent timestamp column."""
    from provero.checks.freshness import _parse_duration
    from provero.core.sql import quote_identifier

    max_age_seconds = _parse_duration(freshness_str)
    qtable = quote_identifier(table)

    columns = connection.get_columns(table)
    ts_columns = [
        c.get("name", c.get("column_name", ""))
        for c in columns
        if any(
            t in str(c.get("type", c.get("data_type", ""))).lower()
            for t in ("timestamp", "datetime", "date")
        )
    ]

    if not ts_columns:
        return ContractViolation(
            rule="sla.freshness",
            message=(f"No timestamp column found to check freshness (required: {freshness_str})"),
            severity=severity,
        )

    ts_col = ts_columns[0]
    qcol = quote_identifier(ts_col)

    try:
        result = connection.execute(
            f"SELECT epoch(CURRENT_TIMESTAMP) - epoch(MAX({qcol})) as age_seconds FROM {qtable}"
        )
    except Exception:
        try:
            result = connection.execute(
                f"SELECT EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX({qcol})))"
                f" as age_seconds FROM {qtable}"
            )
        except Exception as e:
            return ContractViolation(
                rule="sla.freshness",
                message=f"Could not check freshness: {e}",
                severity=severity,
            )

    age = result[0]["age_seconds"]
    if age is None:
        return ContractViolation(
            rule="sla.freshness",
            message="No data in table",
            severity=severity,
        )

    if float(age) > max_age_seconds:
        return ContractViolation(
            rule="sla.freshness",
            message=f"Data is {float(age):.0f}s old, SLA requires < {freshness_str}",
            severity=severity,
        )

    return None


def _check_completeness_sla(
    connection: Connection,
    table: str,
    completeness_str: str,
    contract: ContractConfig,
    severity: str,
) -> ContractViolation | None:
    """Check completeness SLA across contract columns."""
    from provero.core.sql import quote_identifier

    min_pct = float(completeness_str.rstrip("%")) / 100.0
    qtable = quote_identifier(table)

    columns_to_check = (
        [c.name for c in contract.schema_def.columns] if contract.schema_def.columns else []
    )
    if not columns_to_check:
        return None

    total_cells = 0
    non_null_cells = 0
    for col in columns_to_check:
        qcol = quote_identifier(col)
        try:
            result = connection.execute(
                f"SELECT COUNT(*) as total, COUNT({qcol}) as non_null FROM {qtable}"
            )
            total_cells += result[0]["total"]
            non_null_cells += result[0]["non_null"]
        except Exception:
            pass

    if total_cells == 0:
        return None

    actual_pct = non_null_cells / total_cells
    if actual_pct < min_pct:
        return ContractViolation(
            rule="sla.completeness",
            message=f"Completeness {actual_pct:.1%}, SLA requires >= {completeness_str}",
            severity=severity,
        )

    return None


def _check_availability_sla(
    connection: Connection,
    table: str,
    severity: str,
) -> ContractViolation | None:
    """Check that the table has data (row_count > 0)."""
    from provero.core.sql import quote_identifier

    qtable = quote_identifier(table)
    try:
        result = connection.execute(f"SELECT COUNT(*) as total FROM {qtable}")
        if result[0]["total"] == 0:
            return ContractViolation(
                rule="sla.availability",
                message="Table has 0 rows",
                severity=severity,
            )
    except Exception as e:
        return ContractViolation(
            rule="sla.availability",
            message=f"Table not accessible: {e}",
            severity=severity,
        )

    return None
