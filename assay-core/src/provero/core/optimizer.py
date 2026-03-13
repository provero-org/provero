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

"""SQL query optimizer: batches multiple checks into a single query.

Instead of running N separate queries:
    SELECT COUNT(*) FROM t WHERE col IS NULL;
    SELECT COUNT(DISTINCT col) FROM t;
    SELECT MIN(col), MAX(col) FROM t;

The optimizer compiles them into one:
    SELECT
        COUNT(*) FILTER (WHERE col IS NULL) as col_null_count,
        COUNT(DISTINCT col) as col_distinct_count,
        MIN(col) as col_min,
        MAX(col) as col_max,
        COUNT(*) as _total
    FROM t;
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from provero.connectors.base import Connection
from provero.core.compiler import CheckConfig
from provero.core.results import CheckResult, Severity, Status
from provero.core.sql import quote_identifier, quote_value


@dataclass
class BatchedMetric:
    """A single SQL expression to include in the batched query."""

    alias: str
    expression: str
    check_config: CheckConfig


@dataclass
class BatchPlan:
    """Plan for a batched query against a single table."""

    table: str
    metrics: list[BatchedMetric] = field(default_factory=list)
    non_batchable: list[CheckConfig] = field(default_factory=list)

    def add_metric(self, alias: str, expression: str, check_config: CheckConfig) -> None:
        self.metrics.append(BatchedMetric(alias=alias, expression=expression, check_config=check_config))


# Check types that can be batched into a single query
_BATCHABLE_TYPES = {"not_null", "completeness", "unique", "range", "row_count", "accepted_values"}


def plan_batch(table: str, checks: list[CheckConfig]) -> BatchPlan:
    """Create a batch plan from a list of checks."""
    plan = BatchPlan(table=table)

    for check in checks:
        if check.check_type not in _BATCHABLE_TYPES:
            plan.non_batchable.append(check)
            continue

        if check.check_type == "not_null":
            columns = check.columns or ([check.column] if check.column else [])
            for col in columns:
                qcol = quote_identifier(col)
                plan.add_metric(
                    alias=f"nn_{col}_null",
                    expression=f"COUNT(*) FILTER (WHERE {qcol} IS NULL)",
                    check_config=CheckConfig(check_type="not_null", column=col, severity=check.severity),
                )

        elif check.check_type == "completeness":
            col = check.column
            qcol = quote_identifier(col)
            plan.add_metric(
                alias=f"comp_{col}_nonnull",
                expression=f"COUNT({qcol})",
                check_config=check,
            )

        elif check.check_type == "unique":
            col = check.column or (check.columns[0] if check.columns else "")
            qcol = quote_identifier(col)
            plan.add_metric(
                alias=f"uniq_{col}_distinct",
                expression=f"COUNT(DISTINCT {qcol})",
                check_config=check,
            )

        elif check.check_type == "range":
            col = check.column
            qcol = quote_identifier(col)
            plan.add_metric(
                alias=f"range_{col}_min",
                expression=f"MIN({qcol})",
                check_config=check,
            )
            plan.add_metric(
                alias=f"range_{col}_max",
                expression=f"MAX({qcol})",
                check_config=check,
            )
            min_val = check.params.get("min")
            max_val = check.params.get("max")
            conditions = []
            if min_val is not None:
                conditions.append(f"{qcol} < {min_val}")
            if max_val is not None:
                conditions.append(f"{qcol} > {max_val}")
            if conditions:
                where = " OR ".join(conditions)
                plan.add_metric(
                    alias=f"range_{col}_oor",
                    expression=f"COUNT(*) FILTER (WHERE {where})",
                    check_config=check,
                )

        elif check.check_type == "row_count":
            plan.add_metric(
                alias="_row_count",
                expression="COUNT(*)",
                check_config=check,
            )

        elif check.check_type == "accepted_values":
            col = check.column
            qcol = quote_identifier(col)
            values = check.params.get("values", [])
            placeholders = ", ".join(f"'{quote_value(str(v))}'" for v in values)
            plan.add_metric(
                alias=f"av_{col}_invalid",
                expression=f"COUNT(*) FILTER (WHERE {qcol} NOT IN ({placeholders}) AND {qcol} IS NOT NULL)",
                check_config=check,
            )

    # Always include total row count
    has_total = any(m.alias == "_total" or m.alias == "_row_count" for m in plan.metrics)
    if not has_total and plan.metrics:
        plan.add_metric(
            alias="_total",
            expression="COUNT(*)",
            check_config=CheckConfig(check_type="_internal"),
        )

    return plan


def build_batch_query(plan: BatchPlan) -> str:
    """Build a single SQL query from a batch plan."""
    if not plan.metrics:
        return ""

    # Deduplicate: COUNT(*) appears multiple times
    seen_expressions: dict[str, str] = {}
    select_parts: list[str] = []

    for metric in plan.metrics:
        if metric.expression in seen_expressions:
            continue
        seen_expressions[metric.expression] = metric.alias
        select_parts.append(f"{metric.expression} as {metric.alias}")

    # Always add total if not present
    if "COUNT(*)" not in seen_expressions:
        select_parts.append("COUNT(*) as _total")

    select_clause = ",\n    ".join(select_parts)
    qtable = quote_identifier(plan.table)
    return f"SELECT\n    {select_clause}\nFROM {qtable}"


def execute_batch(
    connection: Connection,
    plan: BatchPlan,
) -> list[CheckResult]:
    """Execute a batched query and interpret results into CheckResults."""
    results: list[CheckResult] = []

    if plan.metrics:
        query = build_batch_query(plan)
        rows = connection.execute(query)
        data = rows[0] if rows else {}

        total = data.get("_total") or data.get("_row_count") or 0

        # Process each check from the batch results
        processed_checks: set[str] = set()

        for metric in plan.metrics:
            if metric.check_config.check_type == "_internal":
                continue

            check_key = f"{metric.check_config.check_type}:{metric.check_config.column}"
            if check_key in processed_checks:
                continue

            check = metric.check_config
            col = check.column

            if check.check_type == "not_null":
                null_count = data.get(f"nn_{col}_null", 0)
                severity = Severity(check.severity) if check.severity else Severity.CRITICAL
                qtable = quote_identifier(plan.table)
                qcol = quote_identifier(col)
                results.append(CheckResult(
                    check_name=f"not_null:{col}",
                    check_type="not_null",
                    status=Status.PASS if null_count == 0 else Status.FAIL,
                    severity=severity,
                    column=col,
                    observed_value=f"{null_count} nulls",
                    expected_value="0 nulls",
                    row_count=total,
                    failing_rows=null_count,
                    failing_rows_query=f"SELECT * FROM {qtable} WHERE {qcol} IS NULL" if null_count > 0 else "",
                ))
                processed_checks.add(check_key)

            elif check.check_type == "completeness":
                non_null = data.get(f"comp_{col}_nonnull", 0)
                min_comp = check.params.get("min", 0.95)
                completeness = non_null / total if total > 0 else 0.0
                severity = Severity(check.severity) if check.severity else Severity.CRITICAL
                results.append(CheckResult(
                    check_name=f"completeness:{col}",
                    check_type="completeness",
                    status=Status.PASS if completeness >= min_comp else Status.FAIL,
                    severity=severity,
                    column=col,
                    observed_value=f"{completeness:.1%}",
                    expected_value=f">= {min_comp:.1%}",
                    row_count=total,
                    failing_rows=total - non_null,
                ))
                processed_checks.add(check_key)

            elif check.check_type == "unique":
                distinct = data.get(f"uniq_{col}_distinct", 0)
                duplicates = total - distinct
                severity = Severity(check.severity) if check.severity else Severity.CRITICAL
                qtable = quote_identifier(plan.table)
                qcol = quote_identifier(col)
                results.append(CheckResult(
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
                        f"SELECT {qcol}, COUNT(*) as cnt FROM {qtable} "
                        f"GROUP BY {qcol} HAVING COUNT(*) > 1"
                    ) if duplicates > 0 else "",
                ))
                processed_checks.add(check_key)

            elif check.check_type == "range":
                min_val = data.get(f"range_{col}_min")
                max_val = data.get(f"range_{col}_max")
                out_of_range = data.get(f"range_{col}_oor", 0)
                expected_parts = []
                if check.params.get("min") is not None:
                    expected_parts.append(f"min={check.params['min']}")
                if check.params.get("max") is not None:
                    expected_parts.append(f"max={check.params['max']}")
                severity = Severity(check.severity) if check.severity else Severity.CRITICAL
                results.append(CheckResult(
                    check_name=f"range:{col}",
                    check_type="range",
                    status=Status.PASS if out_of_range == 0 else Status.FAIL,
                    severity=severity,
                    column=col,
                    observed_value=f"min={min_val}, max={max_val}",
                    expected_value=", ".join(expected_parts),
                    row_count=total,
                    failing_rows=out_of_range,
                ))
                processed_checks.add(check_key)

            elif check.check_type == "row_count":
                count = data.get("_row_count", total)
                min_count = check.params.get("min", 0)
                max_count = check.params.get("max")
                passed = count >= min_count
                if max_count is not None:
                    passed = passed and count <= max_count
                expected_parts = []
                if min_count > 0:
                    expected_parts.append(f">= {min_count:,}")
                if max_count is not None:
                    expected_parts.append(f"<= {max_count:,}")
                severity = Severity(check.severity) if check.severity else Severity.CRITICAL
                results.append(CheckResult(
                    check_name="row_count",
                    check_type="row_count",
                    status=Status.PASS if passed else Status.FAIL,
                    severity=severity,
                    observed_value=f"{count:,}",
                    expected_value=" and ".join(expected_parts) if expected_parts else "> 0",
                    row_count=count,
                ))
                processed_checks.add(check_key)

            elif check.check_type == "accepted_values":
                invalid = data.get(f"av_{col}_invalid", 0)
                values = check.params.get("values", [])
                severity = Severity(check.severity) if check.severity else Severity.CRITICAL
                results.append(CheckResult(
                    check_name=f"accepted_values:{col}",
                    check_type="accepted_values",
                    status=Status.PASS if invalid == 0 else Status.FAIL,
                    severity=severity,
                    column=col,
                    observed_value=f"{invalid} invalid values",
                    expected_value=f"only {values}",
                    row_count=total,
                    failing_rows=invalid,
                ))
                processed_checks.add(check_key)

    return results
