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

"""Export Provero checks to dbt schema.yml test definitions."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import yaml

from provero.core.compiler import CheckConfig, ProveroConfig, SuiteConfig

# Checks that map directly to a dbt column test
_SIMPLE_COLUMN_TESTS = {"not_null", "unique"}

# Checks that have no direct dbt equivalent
_UNMAPPABLE_CHECKS = {
    "row_count",
    "freshness",
    "completeness",
    "regex",
    "custom_sql",
    "latency",
}


def _build_column_entry(
    column_name: str,
    tests: list[str | dict[str, Any]],
) -> dict[str, Any]:
    """Build a single column entry for dbt schema.yml."""
    entry: dict[str, Any] = {"name": column_name}
    if tests:
        entry["tests"] = tests
    return entry


def _map_check_to_dbt(
    check: CheckConfig,
) -> tuple[str | None, str | dict[str, Any] | None, str | None]:
    """Map a single Provero check to a dbt test.

    Returns a tuple of (column_name, dbt_test, comment).
    - column_name: the column this test applies to (None for table-level).
    - dbt_test: the dbt test definition (None if unmappable).
    - comment: a comment string if the check cannot be mapped.
    """
    check_type = check.check_type

    if check_type in _SIMPLE_COLUMN_TESTS:
        # Handle multi-column shorthand (not_null: [col1, col2])
        if check.columns:
            return None, None, None  # handled by caller
        return check.column, check_type, None

    if check_type == "accepted_values":
        values = check.params.get("values", [])
        return check.column, {"accepted_values": {"values": values}}, None

    if check_type == "range":
        column = check.column
        min_val = check.params.get("min")
        max_val = check.params.get("max")
        parts = []
        if min_val is not None:
            parts.append(f">= {min_val}")
        if max_val is not None:
            parts.append(f"<= {max_val}")
        expression = " and ".join(parts)
        if expression:
            return (
                column,
                {"dbt_utils.expression_is_true": {"expression": expression}},
                None,
            )
        return column, None, f"# range check on '{column}': no min/max specified"

    if check_type in _UNMAPPABLE_CHECKS:
        detail = check_type
        if check.column:
            detail = f"{check_type} on '{check.column}'"
        return None, None, f"# {detail}: no direct dbt equivalent"

    # Unknown check type
    detail = check_type
    if check.column:
        detail = f"{check_type} on '{check.column}'"
    return None, None, f"# {detail}: no direct dbt equivalent"


def export_suite(suite: SuiteConfig) -> tuple[dict[str, Any], list[str]]:
    """Convert a single SuiteConfig into a dbt model entry dict and comments."""
    column_tests: dict[str, list[str | dict[str, Any]]] = defaultdict(list)
    comments: list[str] = []

    for check in suite.checks:
        # Handle multi-column shorthand (not_null: [col1, col2])
        if check.columns and check.check_type in _SIMPLE_COLUMN_TESTS:
            for col in check.columns:
                column_tests[col].append(check.check_type)
            continue

        mapped_col, dbt_test, comment = _map_check_to_dbt(check)
        if comment:
            comments.append(comment)
        if mapped_col and dbt_test:
            column_tests[mapped_col].append(dbt_test)

    model: dict[str, Any] = {"name": suite.name}

    if column_tests:
        columns = []
        for col_name in column_tests:
            columns.append(_build_column_entry(col_name, column_tests[col_name]))
        model["columns"] = columns

    return model, comments


def export_config(config: ProveroConfig) -> str:
    """Export a full ProveroConfig to dbt schema.yml content.

    Returns a YAML string representing the dbt schema.yml file.
    """
    models = []
    all_comments: list[str] = []

    for suite in config.suites:
        model, comments = export_suite(suite)
        models.append(model)
        if comments:
            all_comments.extend([f"# Model '{suite.name}':"] + [f"  {c}" for c in comments])

    schema: dict[str, Any] = {
        "version": 2,
        "models": models,
    }

    output = yaml.dump(schema, default_flow_style=False, sort_keys=False, allow_unicode=True)

    if all_comments:
        comment_block = "\n".join(all_comments)
        output = output + "\n" + comment_block + "\n"

    return output
