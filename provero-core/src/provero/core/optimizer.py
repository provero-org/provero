# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
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
        SUM(CASE WHEN col IS NULL THEN 1 ELSE 0 END) as col_null_count,
        COUNT(DISTINCT col) as col_distinct_count,
        MIN(col) as col_min,
        MAX(col) as col_max,
        COUNT(*) as _total
    FROM t;
"""

from __future__ import annotations

from dataclasses import dataclass, field

from provero.checks.completeness import _normalize_min_completeness
from provero.connectors.base import Connection
from provero.core.compiler import CheckConfig
from provero.core.results import CheckResult, Severity, Status
from provero.core.sql import quote_identifier, quote_value


def _safe_alias(col: str) -> str:
    """Sanitize a column name for use as a SQL alias, injectively.

    The encoding is collision-free and reversible: every character that is
    not an ASCII letter or digit is escaped as ``_<hex>_`` where ``<hex>`` is
    the character's code point in hexadecimal. The trailing ``_`` delimits the
    (variable-length) hex run, and a literal ``_`` is itself escaped (it is not
    an ASCII alphanumeric, so it falls into the same rule). Because ``_`` only
    ever appears as part of an escape sequence, the mapping is injective and
    distinct column names can never produce the same alias. For example ``a.b``
    -> ``a_2e_b`` while a real column literally named ``a_2e_b`` ->
    ``a_5f_2_5f_e_5f_b``; the two no longer collide. Spaces, dots and
    schema-qualified or nested/JSON column names (BigQuery, Snowflake) are all
    handled unambiguously.
    """
    out: list[str] = []
    for ch in col:
        if ch.isascii() and ch.isalnum():
            out.append(ch)
        else:
            out.append(f"_{ord(ch):x}_")
    return "".join(out)


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
        self.metrics.append(
            BatchedMetric(alias=alias, expression=expression, check_config=check_config)
        )


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
            columns: list[str] = check.columns or ([check.column] if check.column else [])
            for col in columns:
                qcol = quote_identifier(col)
                plan.add_metric(
                    alias=f"nn_{_safe_alias(col)}_null",
                    expression=f"SUM(CASE WHEN {qcol} IS NULL THEN 1 ELSE 0 END)",
                    check_config=CheckConfig(
                        check_type="not_null",
                        column=col,
                        severity=check.severity,
                        description=check.description,
                    ),
                )

        elif check.check_type == "completeness":
            col = check.column or ""
            qcol = quote_identifier(col)
            plan.add_metric(
                alias=f"comp_{_safe_alias(col)}_nonnull",
                expression=f"COUNT({qcol})",
                check_config=check,
            )

        elif check.check_type == "unique":
            col = check.column or (check.columns[0] if check.columns else "")
            qcol = quote_identifier(col)
            # Add COUNT(col) to exclude NULLs from total (matches uniqueness.py fix)
            plan.add_metric(
                alias=f"uniq_{_safe_alias(col)}_total",
                expression=f"COUNT({qcol})",
                check_config=check,
            )
            plan.add_metric(
                alias=f"uniq_{_safe_alias(col)}_distinct",
                expression=f"COUNT(DISTINCT {qcol})",
                check_config=check,
            )

        elif check.check_type == "range":
            col = check.column or ""
            qcol = quote_identifier(col)
            # COUNT(col) excludes NULLs so row_count matches the standalone
            # range check (validity.py uses WHERE col IS NOT NULL). Fixes M5.
            plan.add_metric(
                alias=f"range_{_safe_alias(col)}_total",
                expression=f"COUNT({qcol})",
                check_config=check,
            )
            plan.add_metric(
                alias=f"range_{_safe_alias(col)}_min",
                expression=f"MIN({qcol})",
                check_config=check,
            )
            plan.add_metric(
                alias=f"range_{_safe_alias(col)}_max",
                expression=f"MAX({qcol})",
                check_config=check,
            )
            min_val = check.params.get("min")
            max_val = check.params.get("max")
            # Validate that min/max are numeric to prevent SQL injection
            if min_val is not None:
                try:
                    min_val = float(min_val)
                except (ValueError, TypeError):
                    raise ValueError(
                        f"range check: 'min' must be numeric, got {min_val!r}"
                    ) from None
            if max_val is not None:
                try:
                    max_val = float(max_val)
                except (ValueError, TypeError):
                    raise ValueError(
                        f"range check: 'max' must be numeric, got {max_val!r}"
                    ) from None
            conditions = []
            if min_val is not None:
                conditions.append(f"{qcol} < {min_val}")
            if max_val is not None:
                conditions.append(f"{qcol} > {max_val}")
            if conditions:
                where = " OR ".join(conditions)
                plan.add_metric(
                    alias=f"range_{_safe_alias(col)}_oor",
                    expression=f"SUM(CASE WHEN {where} THEN 1 ELSE 0 END)",
                    check_config=check,
                )

        elif check.check_type == "row_count":
            plan.add_metric(
                alias="_row_count",
                expression="COUNT(*)",
                check_config=check,
            )

        elif check.check_type == "accepted_values":
            col = check.column or ""
            values = check.params.get("values", [])
            if not values:
                plan.non_batchable.append(check)
                continue
            qcol = quote_identifier(col)
            placeholders = ", ".join(f"'{quote_value(str(v))}'" for v in values)
            # COUNT(col) excludes NULLs so row_count matches the standalone
            # accepted_values check (validity.py uses WHERE col IS NOT NULL).
            # Fixes M4.
            plan.add_metric(
                alias=f"av_{_safe_alias(col)}_total",
                expression=f"COUNT({qcol})",
                check_config=check,
            )
            plan.add_metric(
                alias=f"av_{_safe_alias(col)}_invalid",
                expression=(
                    f"SUM(CASE WHEN {qcol} NOT IN ({placeholders}) "
                    f"AND {qcol} IS NOT NULL THEN 1 ELSE 0 END)"
                ),
                check_config=check,
            )

    # Always include total row count
    has_total = any(m.alias in ("_total", "_row_count") for m in plan.metrics)
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
        # SUM/COUNT on empty tables may return None; coerce all values.
        data = {k: (v if v is not None else 0) for k, v in data.items()}

        # Process each check from the batch results.
        # Dedup by the identity of the originating CheckConfig object: a single
        # logical check (e.g. range) emits several metrics that all share the
        # same check_config, so we must emit exactly one result for it. Two
        # *distinct* user checks that happen to be identical are separate
        # objects, so both are still emitted (no silent loss, fixes M2).
        processed_checks: set[int] = set()

        for metric in plan.metrics:
            if metric.check_config.check_type == "_internal":
                continue

            check_key = id(metric.check_config)
            if check_key in processed_checks:
                continue

            check = metric.check_config
            col = check.column or ""

            if check.check_type == "not_null":
                null_count = data.get(f"nn_{_safe_alias(col)}_null", 0)
                severity = Severity(check.severity) if check.severity else Severity.CRITICAL
                qtable = quote_identifier(plan.table)
                qcol = quote_identifier(col)
                results.append(
                    CheckResult(
                        check_name=f"not_null:{col}",
                        check_type="not_null",
                        status=Status.PASS if null_count == 0 else Status.FAIL,
                        severity=severity,
                        description=check.description,
                        column=col,
                        observed_value=f"{null_count} nulls",
                        expected_value="0 nulls",
                        row_count=total,
                        failing_rows=null_count,
                        failing_rows_query=(
                            f"SELECT * FROM {qtable} WHERE {qcol} IS NULL" if null_count > 0 else ""
                        ),
                    )
                )
                processed_checks.add(check_key)

            elif check.check_type == "completeness":
                non_null = data.get(f"comp_{_safe_alias(col)}_nonnull", 0)
                min_comp = _normalize_min_completeness(check.params.get("min", 0.95))
                completeness = non_null / total if total > 0 else 0.0
                severity = Severity(check.severity) if check.severity else Severity.CRITICAL
                results.append(
                    CheckResult(
                        check_name=f"completeness:{col}",
                        check_type="completeness",
                        status=Status.PASS if completeness >= min_comp else Status.FAIL,
                        severity=severity,
                        description=check.description,
                        column=col,
                        observed_value=f"{completeness:.1%}",
                        expected_value=f">= {min_comp:.1%}",
                        row_count=total,
                        failing_rows=total - non_null,
                    )
                )
                processed_checks.add(check_key)

            elif check.check_type == "unique":
                distinct = data.get(f"uniq_{_safe_alias(col)}_distinct", 0)
                # Use COUNT(col) instead of COUNT(*) to exclude NULLs
                col_total = data.get(f"uniq_{_safe_alias(col)}_total", total)
                duplicates = col_total - distinct
                severity = Severity(check.severity) if check.severity else Severity.CRITICAL
                qtable = quote_identifier(plan.table)
                qcol = quote_identifier(col)
                results.append(
                    CheckResult(
                        check_name=f"unique:{col}",
                        check_type="unique",
                        status=Status.PASS if duplicates == 0 else Status.FAIL,
                        severity=severity,
                        description=check.description,
                        column=col,
                        observed_value=f"{duplicates} duplicates",
                        expected_value="0 duplicates",
                        row_count=total,
                        failing_rows=duplicates,
                        failing_rows_query=(
                            f"SELECT {qcol}, COUNT(*) as cnt FROM {qtable} "
                            f"GROUP BY {qcol} HAVING COUNT(*) > 1"
                        )
                        if duplicates > 0
                        else "",
                    )
                )
                processed_checks.add(check_key)

            elif check.check_type == "range":
                min_val = data.get(f"range_{_safe_alias(col)}_min")
                max_val = data.get(f"range_{_safe_alias(col)}_max")
                out_of_range = data.get(f"range_{_safe_alias(col)}_oor", 0)
                # Non-null count, matching standalone semantics (M5).
                col_total = data.get(f"range_{_safe_alias(col)}_total", total)
                expected_parts = []
                oor_conditions: list[str] = []
                qtable = quote_identifier(plan.table)
                qcol = quote_identifier(col)
                raw_min = check.params.get("min")
                raw_max = check.params.get("max")
                if raw_min is not None:
                    expected_parts.append(f"min={raw_min}")
                    oor_conditions.append(f"{qcol} < {float(raw_min)}")
                if raw_max is not None:
                    expected_parts.append(f"max={raw_max}")
                    oor_conditions.append(f"{qcol} > {float(raw_max)}")
                severity = Severity(check.severity) if check.severity else Severity.CRITICAL
                # Drill-down query for failing rows, matching standalone (M3).
                drill_query = ""
                if out_of_range > 0 and oor_conditions:
                    drill_query = f"SELECT * FROM {qtable} WHERE {' OR '.join(oor_conditions)}"
                results.append(
                    CheckResult(
                        check_name=f"range:{col}",
                        check_type="range",
                        status=Status.PASS if out_of_range == 0 else Status.FAIL,
                        severity=severity,
                        description=check.description,
                        column=col,
                        observed_value=f"min={min_val}, max={max_val}",
                        expected_value=", ".join(expected_parts),
                        row_count=col_total,
                        failing_rows=out_of_range,
                        failing_rows_query=drill_query,
                    )
                )
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
                results.append(
                    CheckResult(
                        check_name="row_count",
                        check_type="row_count",
                        status=Status.PASS if passed else Status.FAIL,
                        severity=severity,
                        description=check.description,
                        observed_value=f"{count:,}",
                        expected_value=" and ".join(expected_parts) if expected_parts else "> 0",
                        row_count=count,
                    )
                )
                processed_checks.add(check_key)

            elif check.check_type == "accepted_values":
                invalid = data.get(f"av_{_safe_alias(col)}_invalid", 0)
                # Non-null count, matching standalone semantics (M4).
                col_total = data.get(f"av_{_safe_alias(col)}_total", total)
                values = check.params.get("values", [])
                severity = Severity(check.severity) if check.severity else Severity.CRITICAL
                qtable = quote_identifier(plan.table)
                qcol = quote_identifier(col)
                placeholders = ", ".join(f"'{quote_value(str(v))}'" for v in values)
                results.append(
                    CheckResult(
                        check_name=f"accepted_values:{col}",
                        check_type="accepted_values",
                        status=Status.PASS if invalid == 0 else Status.FAIL,
                        severity=severity,
                        description=check.description,
                        column=col,
                        observed_value=f"{invalid} invalid values",
                        expected_value=f"only {values}",
                        row_count=col_total,
                        failing_rows=invalid,
                        failing_rows_query=(
                            f"SELECT DISTINCT {qcol} FROM {qtable} "
                            f"WHERE {qcol} NOT IN ({placeholders}) AND {qcol} IS NOT NULL"
                        )
                        if invalid > 0
                        else "",
                    )
                )
                processed_checks.add(check_key)

    return results
