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

"""Freshness checks: freshness, latency."""

from __future__ import annotations

import re

from provero.checks.registry import register_check
from provero.connectors.base import Connection
from provero.core.compiler import CheckConfig
from provero.core.results import CheckResult, Severity, Status
from provero.core.sql import quote_identifier


def _parse_duration(duration_str: str) -> int:
    """Parse duration string (e.g., '24h', '30m', '7d') to seconds."""
    match = re.match(r"^(\d+)([smhd])$", duration_str.strip())
    if not match:
        msg = f"Invalid duration format: {duration_str}. Use format like '24h', '30m', '7d'."
        raise ValueError(msg)

    value = int(match.group(1))
    unit = match.group(2)

    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return value * multipliers[unit]


def _format_duration(seconds: int) -> str:
    """Format seconds into human-readable duration."""
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"
    return f"{seconds // 86400}d {(seconds % 86400) // 3600}h"


@register_check("freshness")
def check_freshness(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that data is fresh (most recent row within max_age)."""
    col = check_config.column or ""
    max_age_str = check_config.params.get("max_age", "24h")
    max_age_seconds = _parse_duration(max_age_str)
    qtable = quote_identifier(table)
    qcol = quote_identifier(col)

    # Use epoch() which works on DuckDB, and EXTRACT(EPOCH FROM ...) as fallback
    # for PostgreSQL/other databases.
    try:
        result = connection.execute(
            f"SELECT MAX({qcol}) as latest, "
            f"epoch(CURRENT_TIMESTAMP) - epoch(MAX({qcol})) as age_seconds "
            f"FROM {qtable}"
        )
    except Exception:
        result = connection.execute(
            f"SELECT MAX({qcol}) as latest, "
            f"EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX({qcol}))) as age_seconds "
            f"FROM {qtable}"
        )

    row = result[0]
    raw_age = row["age_seconds"]
    age_seconds = int(float(raw_age)) if raw_age is not None else None

    if age_seconds is None:
        return CheckResult(
            check_name=f"freshness:{col}",
            check_type="freshness",
            status=Status.FAIL,
            severity=(
                Severity(check_config.severity) if check_config.severity else Severity.CRITICAL
            ),
            column=col,
            observed_value="no data",
            expected_value=f"< {max_age_str}",
        )

    return CheckResult(
        check_name=f"freshness:{col}",
        check_type="freshness",
        status=Status.PASS if age_seconds <= max_age_seconds else Status.FAIL,
        severity=Severity(check_config.severity) if check_config.severity else Severity.CRITICAL,
        column=col,
        observed_value=f"{_format_duration(age_seconds)} ago",
        expected_value=f"< {max_age_str}",
    )


@register_check("latency")
def check_latency(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that the latency between two timestamp columns is within bounds.

    Measures the time difference between a source timestamp (e.g., event_time)
    and a target timestamp (e.g., loaded_at). Useful for detecting pipeline delays.
    """
    source_col: str = check_config.params.get("source_column", check_config.column or "")
    target_col = check_config.params.get("target_column", "")
    max_latency_str = check_config.params.get("max_latency", "1h")
    max_latency_seconds = _parse_duration(max_latency_str)

    severity = Severity(check_config.severity) if check_config.severity else Severity.WARNING

    if not target_col:
        return CheckResult(
            check_name=f"latency:{source_col}",
            check_type="latency",
            status=Status.ERROR,
            severity=severity,
            column=source_col,
            observed_value="target_column parameter is required",
            expected_value=f"< {max_latency_str}",
        )

    qtable = quote_identifier(table)
    qsource = quote_identifier(source_col)
    qtarget = quote_identifier(target_col)

    # Compute average and max latency between the two columns
    try:
        result = connection.execute(
            f"SELECT "
            f"AVG(epoch({qtarget}) - epoch({qsource})) as avg_latency, "
            f"MAX(epoch({qtarget}) - epoch({qsource})) as max_lat, "
            f"COUNT(*) as total "
            f"FROM {qtable} WHERE {qsource} IS NOT NULL AND {qtarget} IS NOT NULL"
        )
    except Exception:
        result = connection.execute(
            f"SELECT "
            f"AVG(EXTRACT(EPOCH FROM ({qtarget} - {qsource}))) as avg_latency, "
            f"MAX(EXTRACT(EPOCH FROM ({qtarget} - {qsource}))) as max_lat, "
            f"COUNT(*) as total "
            f"FROM {qtable} WHERE {qsource} IS NOT NULL AND {qtarget} IS NOT NULL"
        )

    row = result[0]
    avg_latency = float(row["avg_latency"]) if row["avg_latency"] is not None else None
    max_lat = float(row["max_lat"]) if row["max_lat"] is not None else None
    total = row["total"]

    if avg_latency is None or total == 0:
        return CheckResult(
            check_name=f"latency:{source_col}",
            check_type="latency",
            status=Status.FAIL,
            severity=severity,
            column=source_col,
            observed_value="no data",
            expected_value=f"< {max_latency_str}",
        )

    passed = max_lat is not None and max_lat <= max_latency_seconds

    return CheckResult(
        check_name=f"latency:{source_col}",
        check_type="latency",
        status=Status.PASS if passed else Status.FAIL,
        severity=severity,
        column=source_col,
        observed_value=(
            f"avg={_format_duration(int(avg_latency or 0))}, "
            f"max={_format_duration(int(max_lat or 0))}"
        ),
        expected_value=f"< {max_latency_str}",
        row_count=total,
    )
