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

"""Convert SodaCL YAML configurations to Provero format.

Supported SodaCL checks:

- ``missing_count(col) = 0``      -> ``not_null: col``
- ``duplicate_count(col) = 0``    -> ``unique: col``
- ``missing_percent(col) < N%``   -> ``completeness: {column, min}``
- ``invalid_count(col) = 0``      -> ``accepted_values: {column, values}``
- ``row_count > N``               -> ``row_count: {min}``
- ``freshness(col) < Xh``         -> ``freshness: {column, max_age}``
- ``schema`` with required cols   -> ``not_null`` per required column

Unsupported checks are emitted as YAML comments.
"""

from __future__ import annotations

import re
from typing import Any

import yaml


def convert_soda_to_provero(soda_yaml: str, source_type: str = "duckdb") -> str:
    """Parse a SodaCL YAML string and return equivalent Provero YAML.

    Parameters
    ----------
    soda_yaml:
        Raw YAML content in SodaCL format.
    source_type:
        The ``source.type`` value for the generated config (default ``duckdb``).

    Returns
    -------
    str
        A valid Provero YAML configuration string.
    """
    data = yaml.safe_load(soda_yaml)
    if data is None:
        return _build_output(source_type, "unknown", [])

    tables: list[tuple[str, list[dict[str, Any]], list[str]]] = []

    for key, checks_list in data.items():
        match = re.match(r"checks\s+for\s+(\S+)", key)
        if not match:
            continue
        table_name = match.group(1)
        provero_checks: list[dict[str, Any]] = []
        unsupported: list[str] = []
        if not isinstance(checks_list, list):
            continue
        for check in checks_list:
            _convert_check(check, provero_checks, unsupported)
        tables.append((table_name, provero_checks, unsupported))

    if not tables:
        return _build_output(source_type, "unknown", [])

    # For a single table, produce one document.
    # For multiple tables, produce multiple YAML documents separated by ---.
    parts: list[str] = []
    for table_name, checks, unsupported in tables:
        parts.append(_build_output(source_type, table_name, checks, unsupported))

    return "\n---\n".join(parts)


def _convert_check(
    check: Any,
    provero_checks: list[dict[str, Any]],
    unsupported: list[str],
) -> None:
    """Convert a single SodaCL check entry into Provero check(s)."""
    if isinstance(check, str):
        _convert_simple_check(check, provero_checks, unsupported)
    elif isinstance(check, dict):
        # Dict checks have one key (the check expression) and a value with config
        for expr, config in check.items():
            _convert_dict_check(expr, config, provero_checks, unsupported)


def _convert_simple_check(
    expr: str,
    provero_checks: list[dict[str, Any]],
    unsupported: list[str],
) -> None:
    """Convert a simple string-based SodaCL check expression."""
    expr = expr.strip()

    # missing_count(col) = 0
    m = re.match(r"missing_count\((\w+)\)\s*=\s*0", expr)
    if m:
        provero_checks.append({"not_null": m.group(1)})
        return

    # duplicate_count(col) = 0
    m = re.match(r"duplicate_count\((\w+)\)\s*=\s*0", expr)
    if m:
        provero_checks.append({"unique": m.group(1)})
        return

    # missing_percent(col) < N%
    m = re.match(r"missing_percent\((\w+)\)\s*<\s*([\d.]+)\s*%", expr)
    if m:
        col = m.group(1)
        pct = float(m.group(2))
        min_completeness = round(1 - pct / 100, 4)
        provero_checks.append({"completeness": {"column": col, "min": min_completeness}})
        return

    # row_count > N  /  row_count >= N  /  row_count = N
    m = re.match(r"row_count\s*(>|>=|=)\s*(\d+)", expr)
    if m:
        op = m.group(1)
        n = int(m.group(2))
        if op == ">":
            provero_checks.append({"row_count": {"min": n + 1}})
        elif op == ">=":
            provero_checks.append({"row_count": {"min": n}})
        else:
            provero_checks.append({"row_count": {"min": n, "max": n}})
        return

    # freshness(col) < Xh / Xd / Xm
    m = re.match(r"freshness\((\w+)\)\s*<\s*(\d+[hHdDmM])", expr)
    if m:
        col = m.group(1)
        max_age = m.group(2).lower()
        provero_checks.append({"freshness": {"column": col, "max_age": max_age}})
        return

    # Unsupported simple check
    unsupported.append(expr)


def _convert_dict_check(
    expr: str,
    config: Any,
    provero_checks: list[dict[str, Any]],
    unsupported: list[str],
) -> None:
    """Convert a dict-based SodaCL check (expression with nested config)."""
    expr = expr.strip()

    # invalid_count(col) = 0  with  valid values: [...]
    m = re.match(r"invalid_count\((\w+)\)\s*=\s*0", expr)
    if m and isinstance(config, dict):
        col = m.group(1)
        values = config.get("valid values") or config.get("valid_values")
        if values:
            provero_checks.append({"accepted_values": {"column": col, "values": list(values)}})
            return

    # schema check with required columns
    if expr == "schema" and isinstance(config, dict):
        fail_config = config.get("fail", {})
        if isinstance(fail_config, dict):
            required_cols = fail_config.get("when required column missing", [])
            if isinstance(required_cols, list):
                for col in required_cols:
                    provero_checks.append({"not_null": col})
                return

    # Unsupported dict check
    unsupported.append(expr)


def _build_output(
    source_type: str,
    table_name: str,
    checks: list[dict[str, Any]],
    unsupported: list[str] | None = None,
) -> str:
    """Build the final Provero YAML string."""
    doc: dict[str, Any] = {
        "source": {"type": source_type, "table": table_name},
    }
    if checks:
        doc["checks"] = checks

    output = yaml.dump(doc, default_flow_style=False, sort_keys=False, allow_unicode=True)

    if unsupported:
        output += "\n# Unsupported SodaCL checks (review manually):\n"
        for u in unsupported:
            output += f"#   - {u}\n"

    return output
