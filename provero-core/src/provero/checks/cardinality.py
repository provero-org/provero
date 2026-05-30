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

"""Cardinality check: distinct-count or distinct-ratio bounds.

Distinct count uses ``COUNT(DISTINCT col)`` which excludes NULLs. The distinct
ratio denominator is ``COUNT(col)`` (non-NULL row count), so both numerator and
denominator share the same NULL semantics and the ratio is well defined.
"""

from __future__ import annotations

from typing import Any

from provero.checks.registry import register_check
from provero.connectors.base import Connection
from provero.core.compiler import CheckConfig
from provero.core.results import CheckResult, Severity, Status
from provero.core.sql import quote_identifier


def compute_cardinality(
    connection: Connection,
    table: str,
    column: str,
) -> dict[str, Any]:
    """Return ``{"distinct": int, "non_null": int, "ratio": float|None}``.

    ``ratio`` is ``distinct / non_null`` and is ``None`` when ``non_null`` is 0.
    """
    qtable = quote_identifier(table)
    qcol = quote_identifier(column)
    rows = connection.execute(
        f"SELECT COUNT(DISTINCT {qcol}) AS distinct_count, COUNT({qcol}) AS non_null FROM {qtable}"
    )
    row = rows[0]
    distinct = int(row["distinct_count"])
    non_null = int(row["non_null"])
    ratio = (distinct / non_null) if non_null > 0 else None
    return {"distinct": distinct, "non_null": non_null, "ratio": ratio}


@register_check("cardinality")
def check_cardinality(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Assert distinct count and/or distinct ratio fall within bounds.

    Params (at least one constraint required):
        min / max: bounds on the distinct count (``COUNT(DISTINCT col)``).
        min_ratio / max_ratio: bounds on the distinct ratio
            (``COUNT(DISTINCT col) / COUNT(col)``), each in ``[0, 1]``.
    """
    col = check_config.column or check_config.params.get("column")
    severity = Severity(check_config.severity) if check_config.severity else Severity.CRITICAL

    if not col:
        return CheckResult(
            check_name="cardinality",
            check_type="cardinality",
            status=Status.ERROR,
            severity=severity,
            observed_value="missing required parameter: column",
            expected_value="column name",
        )

    p: dict[str, Any] = check_config.params
    min_count = p.get("min")
    max_count = p.get("max")
    min_ratio = p.get("min_ratio")
    max_ratio = p.get("max_ratio")

    if all(v is None for v in (min_count, max_count, min_ratio, max_ratio)):
        return CheckResult(
            check_name=f"cardinality:{col}",
            check_type="cardinality",
            status=Status.ERROR,
            severity=severity,
            column=col,
            observed_value="no constraint configured",
            expected_value="at least one of min/max/min_ratio/max_ratio",
        )

    stats = compute_cardinality(connection, table, col)
    distinct = int(stats["distinct"])
    non_null = int(stats["non_null"])
    ratio = stats["ratio"]

    passed = True
    if min_count is not None:
        passed = passed and distinct >= min_count
    if max_count is not None:
        passed = passed and distinct <= max_count

    ratio_constrained = min_ratio is not None or max_ratio is not None
    if ratio_constrained:
        if ratio is None:
            passed = False
        else:
            if min_ratio is not None:
                passed = passed and ratio >= min_ratio
            if max_ratio is not None:
                passed = passed and ratio <= max_ratio

    exp_parts: list[str] = []
    if min_count is not None:
        exp_parts.append(f"distinct >= {min_count}")
    if max_count is not None:
        exp_parts.append(f"distinct <= {max_count}")
    if min_ratio is not None:
        exp_parts.append(f"ratio >= {min_ratio}")
    if max_ratio is not None:
        exp_parts.append(f"ratio <= {max_ratio}")

    ratio_str = f"{ratio:.4f}" if ratio is not None else "n/a"
    return CheckResult(
        check_name=f"cardinality:{col}",
        check_type="cardinality",
        status=Status.PASS if passed else Status.FAIL,
        severity=severity,
        column=col,
        observed_value=f"distinct={distinct}, ratio={ratio_str}",
        expected_value=" and ".join(exp_parts),
        row_count=non_null,
    )
