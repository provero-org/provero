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

"""Distribution check: assert column mean and/or stddev within bounds.

The check computes the population mean (``AVG``) and population standard
deviation (``STDDEV_POP``) of a numeric column and asserts they fall within
explicit ``min``/``max`` bounds, or match an ``expected`` value within an
absolute ``tolerance``. Population statistics (denominator ``n``) are used so
that the numbers are reproducible and hand-verifiable.
"""

from __future__ import annotations

from typing import Any

from provero.checks.registry import register_check
from provero.connectors.base import Connection
from provero.core.compiler import CheckConfig
from provero.core.results import CheckResult, Severity, Status
from provero.core.sql import quote_identifier


def _within(
    value: float,
    *,
    minimum: float | None,
    maximum: float | None,
    expected: float | None,
    tolerance: float,
) -> bool:
    """Return True if *value* satisfies the configured bounds.

    ``expected``/``tolerance`` and ``min``/``max`` may be combined; all
    configured constraints must hold.
    """
    ok = True
    if expected is not None:
        ok = ok and abs(value - expected) <= tolerance
    if minimum is not None:
        ok = ok and value >= minimum
    if maximum is not None:
        ok = ok and value <= maximum
    return ok


def _bounds_label(
    *,
    minimum: float | None,
    maximum: float | None,
    expected: float | None,
    tolerance: float,
) -> str:
    parts: list[str] = []
    if expected is not None:
        parts.append(f"{expected} +/- {tolerance}")
    if minimum is not None:
        parts.append(f">= {minimum}")
    if maximum is not None:
        parts.append(f"<= {maximum}")
    return " and ".join(parts) if parts else "any"


def compute_distribution_stats(
    connection: Connection,
    table: str,
    column: str,
) -> dict[str, float | None]:
    """Return ``{"mean": float|None, "stddev": float|None, "count": int}``.

    ``mean`` and ``stddev`` are ``None`` when the column has no non-NULL rows.
    ``stddev`` is the *population* standard deviation (``STDDEV_POP``).
    """
    qtable = quote_identifier(table)
    qcol = quote_identifier(column)
    rows = connection.execute(
        f"SELECT AVG({qcol}) AS mean_val, "
        f"STDDEV_POP({qcol}) AS std_val, "
        f"COUNT({qcol}) AS n FROM {qtable}"
    )
    row = rows[0]
    mean_val = row["mean_val"]
    std_val = row["std_val"]
    return {
        "mean": float(mean_val) if mean_val is not None else None,
        "stddev": float(std_val) if std_val is not None else None,
        "count": int(row["n"]),
    }


@register_check("distribution")
def check_distribution(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Assert a numeric column's mean and/or stddev fall within bounds.

    Params (all optional, but at least one stat constraint is required):
        mean_min / mean_max: bounds on the population mean.
        mean / mean_tolerance: expected mean within absolute tolerance.
        stddev_min / stddev_max: bounds on the population stddev.
        stddev / stddev_tolerance: expected stddev within absolute tolerance.
    """
    col = check_config.column or check_config.params.get("column")
    severity = Severity(check_config.severity) if check_config.severity else Severity.CRITICAL

    if not col:
        return CheckResult(
            check_name="distribution",
            check_type="distribution",
            status=Status.ERROR,
            severity=severity,
            observed_value="missing required parameter: column",
            expected_value="column name",
        )

    p: dict[str, Any] = check_config.params
    mean_min = p.get("mean_min")
    mean_max = p.get("mean_max")
    mean_expected = p.get("mean")
    mean_tol = float(p.get("mean_tolerance", 0.0))
    std_min = p.get("stddev_min")
    std_max = p.get("stddev_max")
    std_expected = p.get("stddev")
    std_tol = float(p.get("stddev_tolerance", 0.0))

    constrains_mean = any(v is not None for v in (mean_min, mean_max, mean_expected))
    constrains_std = any(v is not None for v in (std_min, std_max, std_expected))
    if not constrains_mean and not constrains_std:
        return CheckResult(
            check_name=f"distribution:{col}",
            check_type="distribution",
            status=Status.ERROR,
            severity=severity,
            column=col,
            observed_value="no constraint configured",
            expected_value="at least one of mean*/stddev* bounds",
        )

    stats = compute_distribution_stats(connection, table, col)
    mean_val = stats["mean"]
    std_val = stats["stddev"]
    count = int(stats["count"] or 0)

    if mean_val is None or (constrains_std and std_val is None):
        return CheckResult(
            check_name=f"distribution:{col}",
            check_type="distribution",
            status=Status.ERROR,
            severity=severity,
            column=col,
            observed_value="insufficient non-null data",
            expected_value="at least one non-null value",
            row_count=count,
        )

    passed = True
    obs_parts: list[str] = []
    exp_parts: list[str] = []
    if constrains_mean:
        passed = passed and _within(
            mean_val, minimum=mean_min, maximum=mean_max, expected=mean_expected, tolerance=mean_tol
        )
        obs_parts.append(f"mean={mean_val:g}")
        exp_parts.append(
            "mean "
            + _bounds_label(
                minimum=mean_min, maximum=mean_max, expected=mean_expected, tolerance=mean_tol
            )
        )
    if constrains_std and std_val is not None:
        passed = passed and _within(
            std_val, minimum=std_min, maximum=std_max, expected=std_expected, tolerance=std_tol
        )
        obs_parts.append(f"stddev={std_val:g}")
        exp_parts.append(
            "stddev "
            + _bounds_label(
                minimum=std_min, maximum=std_max, expected=std_expected, tolerance=std_tol
            )
        )

    return CheckResult(
        check_name=f"distribution:{col}",
        check_type="distribution",
        status=Status.PASS if passed else Status.FAIL,
        severity=severity,
        column=col,
        observed_value=", ".join(obs_parts),
        expected_value="; ".join(exp_parts),
        row_count=count,
    )
