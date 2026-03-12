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

"""Freshness checks."""

from __future__ import annotations

import re

from assay.checks.registry import register_check
from assay.connectors.base import Connection
from assay.core.compiler import CheckConfig
from assay.core.results import CheckResult, Severity, Status


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
    col = check_config.column
    max_age_str = check_config.params.get("max_age", "24h")
    max_age_seconds = _parse_duration(max_age_str)

    # Use epoch() which works on DuckDB, and EXTRACT(EPOCH FROM ...) as fallback
    # for PostgreSQL/other databases.
    try:
        result = connection.execute(
            f"SELECT MAX({col}) as latest, "
            f"epoch(CURRENT_TIMESTAMP) - epoch(MAX({col})) as age_seconds "
            f"FROM {table}"
        )
    except Exception:
        result = connection.execute(
            f"SELECT MAX({col}) as latest, "
            f"EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX({col}))) as age_seconds "
            f"FROM {table}"
        )

    row = result[0]
    raw_age = row["age_seconds"]
    age_seconds = int(float(raw_age)) if raw_age is not None else None

    if age_seconds is None:
        return CheckResult(
            check_name=f"freshness:{col}",
            check_type="freshness",
            status=Status.FAIL,
            severity=check_config.severity or Severity.CRITICAL,
            column=col,
            observed_value="no data",
            expected_value=f"< {max_age_str}",
        )

    return CheckResult(
        check_name=f"freshness:{col}",
        check_type="freshness",
        status=Status.PASS if age_seconds <= max_age_seconds else Status.FAIL,
        severity=check_config.severity or Severity.CRITICAL,
        column=col,
        observed_value=f"{_format_duration(age_seconds)} ago",
        expected_value=f"< {max_age_str}",
    )
