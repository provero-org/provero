# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Result store backends for Provero."""

from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING

from provero.store.sqlite import SQLiteStore

if TYPE_CHECKING:
    from provero.store.base import Store

__all__ = ["SQLiteStore", "create_store"]


def _expand_env_vars(value: str) -> str:
    """Expand ${ENV_VAR} patterns in a string."""
    return re.sub(r"\$\{(\w+)\}", lambda m: os.environ.get(m.group(1), m.group(0)), value)


def create_store(config: dict | None = None) -> Store:
    """Create a store from config. Defaults to SQLite.

    Config keys:
        type: "sqlite" (default) or "postgres"
        path: database path for SQLite (default: .provero/results.db)
        connection_url: connection URL for PostgreSQL
    """
    if config is None:
        return SQLiteStore()

    store_type = config.get("type", "sqlite")

    if store_type == "sqlite":
        path = config.get("path")
        if path:
            return SQLiteStore(db_path=path)
        return SQLiteStore()

    if store_type == "postgres":
        connection_url = config.get("connection_url", "")
        if not connection_url:
            raise ValueError("PostgreSQL store requires a 'connection_url' in config")
        connection_url = _expand_env_vars(connection_url)

        from provero.store.postgres import PostgresStore

        return PostgresStore(connection_url=connection_url)

    raise ValueError(f"Unknown store type: {store_type!r}. Supported: 'sqlite', 'postgres'")
