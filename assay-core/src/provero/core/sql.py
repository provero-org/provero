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

"""SQL utilities for safe identifier handling."""

from __future__ import annotations

import re

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


def is_expression(name: str) -> bool:
    """Check if *name* is a SQL expression rather than a plain identifier.

    DuckDB supports table-function expressions like ``read_csv('file.csv')``
    and ``read_parquet('*.parquet')`` in the FROM clause.  These are not
    identifiers and must not be quoted or validated as such.
    """
    return "(" in name


def quote_identifier(name: str) -> str:
    """Quote a SQL identifier (table or column name) to prevent injection.

    Uses double-quoting (ANSI SQL standard). Rejects identifiers that contain
    characters outside the safe set as an extra layer of defense.

    Table-function expressions (containing parentheses) are returned as-is
    because they are valid SQL but not identifiers.
    """
    if not name:
        msg = "SQL identifier cannot be empty"
        raise ValueError(msg)
    if is_expression(name):
        return name
    if not _IDENTIFIER_RE.match(name):
        msg = (
            f"Invalid SQL identifier: {name!r}. "
            "Only alphanumeric characters, underscores, and dots are allowed."
        )
        raise ValueError(msg)
    # Double-quote each part (schema.table -> "schema"."table")
    parts = name.split(".")
    return ".".join(f'"{part}"' for part in parts)


def quote_value(value: str) -> str:
    """Escape a string value for safe use in SQL literals.

    Doubles single quotes to prevent SQL injection.
    """
    return value.replace("'", "''")
