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

"""Cross-table row-count parity / ratio check.

Compares the row count of the primary ``table`` against a second table on the
*same source/connection*. Supports two modes:

* parity (default): the two counts must be equal, optionally within an
  absolute ``tolerance`` of rows.
* ratio: ``count(table) / count(other_table)`` must fall within
  ``min_ratio``/``max_ratio`` bounds.
"""

from __future__ import annotations

from typing import Any

from provero.checks.registry import register_check
from provero.connectors.base import Connection
from provero.core.compiler import CheckConfig
from provero.core.results import CheckResult, Severity, Status
from provero.core.sql import quote_identifier


def _count(connection: Connection, table: str) -> int:
    rows = connection.execute(f"SELECT COUNT(*) AS total FROM {quote_identifier(table)}")
    return int(rows[0]["total"])


@register_check("cross_table_count")
def check_cross_table_count(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Compare row counts between *table* and a second table on the same source.

    Params:
        other_table (str): required. Second table to compare against.
            ``reference_table`` is accepted as an alias.
        mode (str): "parity" (default) or "ratio".
        tolerance (int): parity mode only; allowed absolute row-count
            difference. Default 0.
        min_ratio / max_ratio: ratio mode bounds on count(table)/count(other).
    """
    severity = Severity(check_config.severity) if check_config.severity else Severity.CRITICAL
    p: dict[str, Any] = check_config.params
    other = p.get("other_table") or p.get("reference_table")

    if not other:
        return CheckResult(
            check_name="cross_table_count",
            check_type="cross_table_count",
            status=Status.ERROR,
            severity=severity,
            observed_value="missing required parameter: other_table",
            expected_value="second table name",
        )

    mode = str(p.get("mode", "parity"))
    primary = _count(connection, table)
    secondary = _count(connection, other)

    if mode == "ratio":
        min_ratio = p.get("min_ratio")
        max_ratio = p.get("max_ratio")
        ratio = (primary / secondary) if secondary > 0 else None
        passed = True
        if ratio is None:
            passed = False
        else:
            if min_ratio is not None:
                passed = passed and ratio >= min_ratio
            if max_ratio is not None:
                passed = passed and ratio <= max_ratio
        exp_parts: list[str] = []
        if min_ratio is not None:
            exp_parts.append(f"ratio >= {min_ratio}")
        if max_ratio is not None:
            exp_parts.append(f"ratio <= {max_ratio}")
        ratio_str = f"{ratio:.4f}" if ratio is not None else "n/a"
        return CheckResult(
            check_name=f"cross_table_count:{table}~{other}",
            check_type="cross_table_count",
            status=Status.PASS if passed else Status.FAIL,
            severity=severity,
            observed_value=f"{primary} vs {secondary} (ratio={ratio_str})",
            expected_value=" and ".join(exp_parts) if exp_parts else "ratio bounds",
            row_count=primary,
        )

    # parity mode
    tolerance = int(p.get("tolerance", 0))
    diff = abs(primary - secondary)
    passed = diff <= tolerance
    return CheckResult(
        check_name=f"cross_table_count:{table}~{other}",
        check_type="cross_table_count",
        status=Status.PASS if passed else Status.FAIL,
        severity=severity,
        observed_value=f"{primary} vs {secondary} (diff={diff})",
        expected_value=(f"equal +/- {tolerance}" if tolerance else "equal counts"),
        row_count=primary,
    )
