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

"""Data profiler: generates statistical profiles and suggests checks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from provero.connectors.base import Connection
from provero.core.sql import quote_identifier


@dataclass
class ColumnProfile:
    """Statistical profile of a single column."""

    name: str
    dtype: str
    total_count: int = 0
    null_count: int = 0
    null_pct: float = 0.0
    distinct_count: int = 0
    distinct_pct: float = 0.0

    # Numeric stats
    min_value: Any = None
    max_value: Any = None
    mean_value: float | None = None
    median_value: float | None = None
    stddev_value: float | None = None

    # String stats
    min_length: int | None = None
    max_length: int | None = None
    avg_length: float | None = None

    # Top values
    top_values: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class TableProfile:
    """Statistical profile of a table."""

    table: str
    row_count: int
    column_count: int
    columns: list[ColumnProfile] = field(default_factory=list)


def profile_table(
    connection: Connection,
    table: str,
    sample_size: int | None = None,
) -> TableProfile:
    """Generate a statistical profile of a table."""
    qtable = quote_identifier(table)

    # Get row count
    result = connection.execute(f"SELECT COUNT(*) as cnt FROM {qtable}")
    row_count = result[0]["cnt"]

    # Get column info
    col_info = connection.get_columns(table)

    source_expr = qtable
    if sample_size and row_count > sample_size:
        # Use TABLESAMPLE for cross-database compatibility, with DuckDB USING SAMPLE fallback
        try:
            connection.execute(f"SELECT 1 FROM {qtable} TABLESAMPLE BERNOULLI(1) LIMIT 1")
            pct = min(100, max(1, int(sample_size / row_count * 100)))
            source_expr = f"(SELECT * FROM {qtable} TABLESAMPLE BERNOULLI({pct})) AS _sample"
        except Exception:
            source_expr = f"(SELECT * FROM {qtable} LIMIT {sample_size}) AS _sample"

    columns: list[ColumnProfile] = []

    for col in col_info:
        col_name = col["name"]
        col_type = col["type"].lower()
        qcol = quote_identifier(col_name)

        # Basic stats: nulls and distinct count
        basic = connection.execute(
            f"SELECT "
            f"COUNT(*) as total, "
            f"COUNT(*) - COUNT({qcol}) as null_count, "
            f"COUNT(DISTINCT {qcol}) as distinct_count "
            f"FROM {source_expr}"
        )[0]

        total = basic["total"]
        null_count = basic["null_count"]
        distinct_count = basic["distinct_count"]

        profile = ColumnProfile(
            name=col_name,
            dtype=col_type,
            total_count=total,
            null_count=null_count,
            null_pct=round(null_count / total * 100, 2) if total > 0 else 0,
            distinct_count=distinct_count,
            distinct_pct=round(distinct_count / total * 100, 2) if total > 0 else 0,
        )

        # Numeric stats
        is_numeric = any(
            t in col_type
            for t in [
                "int",
                "float",
                "double",
                "decimal",
                "numeric",
                "real",
                "bigint",
                "smallint",
                "number",
                "money",
            ]
        )
        if is_numeric:
            # Use ANSI SQL: AVG and STDDEV are widely supported.
            # PERCENTILE_CONT is ANSI but not all DBs support it. Try it with a fallback.
            num_stats = connection.execute(
                f"SELECT "
                f"MIN({qcol}) as min_val, "
                f"MAX({qcol}) as max_val, "
                f"AVG(CAST({qcol} AS DOUBLE PRECISION)) as mean_val, "
                f"STDDEV(CAST({qcol} AS DOUBLE PRECISION)) as stddev_val "
                f"FROM {source_expr} WHERE {qcol} IS NOT NULL"
            )[0]
            profile.min_value = num_stats["min_val"]
            profile.max_value = num_stats["max_val"]
            profile.mean_value = (
                round(float(num_stats["mean_val"]), 4) if num_stats["mean_val"] else None
            )
            profile.stddev_value = (
                round(float(num_stats["stddev_val"]), 4) if num_stats["stddev_val"] else None
            )

            # Try to get median (not universally supported)
            try:
                median_result = connection.execute(
                    f"SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {qcol}) as median_val "
                    f"FROM {source_expr} WHERE {qcol} IS NOT NULL"
                )[0]
                profile.median_value = (
                    round(float(median_result["median_val"]), 4)
                    if median_result["median_val"]
                    else None
                )
            except Exception:
                profile.median_value = None

        # String stats
        is_string = any(t in col_type for t in ["varchar", "text", "char", "string"])
        if is_string:
            str_stats = connection.execute(
                f"SELECT "
                f"MIN(LENGTH({qcol})) as min_len, "
                f"MAX(LENGTH({qcol})) as max_len, "
                f"AVG(LENGTH({qcol})) as avg_len "
                f"FROM {source_expr} WHERE {qcol} IS NOT NULL"
            )[0]
            profile.min_length = str_stats["min_len"]
            profile.max_length = str_stats["max_len"]
            profile.avg_length = (
                round(float(str_stats["avg_len"]), 1) if str_stats["avg_len"] else None
            )

        # Top values (for columns with reasonable cardinality)
        if distinct_count <= 50:
            top = connection.execute(
                f"SELECT {qcol} as value, COUNT(*) as count "
                f"FROM {source_expr} WHERE {qcol} IS NOT NULL "
                f"GROUP BY {qcol} ORDER BY count DESC LIMIT 10"
            )
            profile.top_values = [{"value": r["value"], "count": r["count"]} for r in top]

        columns.append(profile)

    return TableProfile(
        table=table,
        row_count=row_count,
        column_count=len(columns),
        columns=columns,
    )


def suggest_checks(profile: TableProfile) -> list[dict[str, Any]]:
    """Suggest quality checks based on a table profile."""
    checks: list[dict[str, Any]] = []

    # Always suggest row_count
    checks.append({"row_count": {"min": max(1, profile.row_count // 2)}})

    not_null_cols = []
    unique_cols = []

    for col in profile.columns:
        # Suggest not_null for columns with 0% nulls
        if col.null_pct == 0:
            not_null_cols.append(col.name)

        # Suggest unique for columns that are 100% distinct
        if col.distinct_pct == 100 and col.total_count > 1:
            unique_cols.append(col.name)

        # Suggest accepted_values for low-cardinality non-numeric columns.
        # For numeric columns, range checks are more appropriate.
        is_numeric_col = col.min_value is not None and col.max_value is not None
        if 0 < col.distinct_count <= 20 and col.top_values and not is_numeric_col:
            values = [str(v["value"]) for v in col.top_values]
            checks.append(
                {
                    "accepted_values": {
                        "column": col.name,
                        "values": values,
                    }
                }
            )

        # Suggest range for numeric columns
        if col.min_value is not None and col.max_value is not None:
            # Add 10% margin
            try:
                min_val = float(col.min_value)
                max_val = float(col.max_value)
            except (TypeError, ValueError):
                min_val = None
                max_val = None
            if min_val is not None and max_val is not None:
                margin = (max_val - min_val) * 0.1 if max_val != min_val else abs(min_val) * 0.1
                checks.append(
                    {
                        "range": {
                            "column": col.name,
                            "min": round(min_val - margin, 2),
                            "max": round(max_val + margin, 2),
                        }
                    }
                )

    if not_null_cols:
        checks.insert(0, {"not_null": not_null_cols})

    for ucol in unique_cols:
        checks.insert(1, {"unique": ucol})

    return checks


def checks_to_yaml(checks: list[dict[str, Any]], source_type: str, table: str) -> str:
    """Convert suggested checks to provero.yaml format."""
    import yaml

    config = {
        "source": {"type": source_type, "table": table},
        "checks": checks,
    }
    return yaml.dump(config, default_flow_style=False, sort_keys=False)
