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

"""Drift check via Population Stability Index (PSI).

The baseline is a *discrete* distribution provided as ``{value: proportion}``,
where the proportions are expected (but not required) to sum to ~1.0. The check
counts current rows per baseline category, converts to proportions, and
computes::

    PSI = sum_over_bins( (cur_pct - base_pct) * ln(cur_pct / base_pct) )

A small epsilon floor is applied to every proportion (current and baseline)
before the log/ratio so empty bins never cause division-by-zero or log(0).
PSI conventions: < 0.1 stable, 0.1-0.25 moderate shift (warn),
> 0.25 significant shift (fail by default).
"""

from __future__ import annotations

import math
from typing import Any

from provero.checks.registry import register_check
from provero.connectors.base import Connection
from provero.core.compiler import CheckConfig
from provero.core.results import CheckResult, Severity, Status
from provero.core.sql import quote_identifier

_EPSILON = 1e-6


def compute_psi(
    current_counts: dict[str, int],
    baseline: dict[str, float],
    *,
    epsilon: float = _EPSILON,
) -> float:
    """Compute PSI of *current_counts* against a *baseline* distribution.

    The set of bins is the union of baseline keys and current keys. Current
    counts are normalised to proportions over the total current count. Each
    proportion (current and baseline) is floored at *epsilon* before the ratio
    and logarithm to avoid div-by-zero / log(0).
    """
    bins = set(baseline) | set(current_counts)
    total = sum(current_counts.values())

    psi = 0.0
    for b in bins:
        base_pct = baseline.get(b, 0.0)
        cur_pct = (current_counts.get(b, 0) / total) if total > 0 else 0.0
        base_safe = max(base_pct, epsilon)
        cur_safe = max(cur_pct, epsilon)
        psi += (cur_safe - base_safe) * math.log(cur_safe / base_safe)
    return psi


def _query_counts(
    connection: Connection,
    table: str,
    column: str,
) -> dict[str, int]:
    """Return ``{stringified_value: count}`` for non-NULL values of *column*."""
    qtable = quote_identifier(table)
    qcol = quote_identifier(column)
    rows = connection.execute(
        f"SELECT {qcol} AS bucket, COUNT(*) AS cnt "
        f"FROM {qtable} WHERE {qcol} IS NOT NULL GROUP BY {qcol}"
    )
    return {str(r["bucket"]): int(r["cnt"]) for r in rows}


@register_check("drift")
def check_drift(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Detect distribution drift of a column versus a baseline via PSI.

    Params:
        baseline (dict[str, float]): required. Discrete baseline distribution
            mapping category -> proportion.
        threshold (float): PSI above which the check FAILs. Default 0.25.
        warn_threshold (float): PSI above which the check WARNs. Default 0.1.
        epsilon (float): proportion floor for div0/log0 safety. Default 1e-6.
    """
    col = check_config.column or check_config.params.get("column")
    severity = Severity(check_config.severity) if check_config.severity else Severity.WARNING

    if not col:
        return CheckResult(
            check_name="drift",
            check_type="drift",
            status=Status.ERROR,
            severity=severity,
            observed_value="missing required parameter: column",
            expected_value="column name",
        )

    p: dict[str, Any] = check_config.params
    raw_baseline = p.get("baseline")
    if not isinstance(raw_baseline, dict) or not raw_baseline:
        return CheckResult(
            check_name=f"drift:{col}",
            check_type="drift",
            status=Status.ERROR,
            severity=severity,
            column=col,
            observed_value="missing or invalid required parameter: baseline",
            expected_value="non-empty {value: proportion} mapping",
        )

    baseline: dict[str, float] = {str(k): float(v) for k, v in raw_baseline.items()}
    threshold = float(p.get("threshold", 0.25))
    warn_threshold = float(p.get("warn_threshold", 0.1))
    epsilon = float(p.get("epsilon", _EPSILON))

    counts = _query_counts(connection, table, col)
    total = sum(counts.values())
    if total == 0:
        return CheckResult(
            check_name=f"drift:{col}",
            check_type="drift",
            status=Status.ERROR,
            severity=severity,
            column=col,
            observed_value="no non-null rows to compare",
            expected_value=f"PSI <= {threshold}",
        )

    psi = compute_psi(counts, baseline, epsilon=epsilon)

    if psi > threshold:
        status = Status.FAIL
    elif psi > warn_threshold:
        status = Status.WARN
    else:
        status = Status.PASS

    return CheckResult(
        check_name=f"drift:{col}",
        check_type="drift",
        status=status,
        severity=severity,
        column=col,
        observed_value=f"PSI={psi:.4f}",
        expected_value=f"PSI <= {threshold} (warn > {warn_threshold})",
        row_count=total,
    )
