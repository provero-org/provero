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

"""Shared CLI utilities: console, echo helper, quiet flag, and templates."""

from __future__ import annotations

from rich.console import Console

console = Console()

# Module-level quiet flag toggled by the top-level callback.
_quiet: bool = False

TEMPLATE = """\
# provero.yaml - Provero configuration
# Docs: https://provero-org.github.io/provero/

source:
  type: duckdb
  # type: postgres
  # connection: ${POSTGRES_URI}
  table: my_table

checks:
  - not_null: [id, name]
  - unique: id
  - row_count:
      min: 1
"""


def _echo(msg: str, **kwargs) -> None:
    """Print *msg* via the shared console, unless quiet mode is active."""
    if not _quiet:
        console.print(msg, **kwargs)


def set_quiet(value: bool) -> None:
    """Set the module-level quiet flag."""
    global _quiet
    _quiet = value


def is_quiet() -> bool:
    """Return the current quiet-mode state."""
    return _quiet
