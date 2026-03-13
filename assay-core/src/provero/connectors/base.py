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

"""Base connector protocol."""

from __future__ import annotations

from typing import Any, Protocol


class Connection(Protocol):
    """A database connection that can execute SQL."""

    def execute(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        ...

    def get_columns(self, table: str) -> list[dict[str, Any]]:
        """Return column metadata: [{name, type, nullable}, ...]."""
        ...


class Connector(Protocol):
    """Interface for data source connectors.

    Every connector implements at minimum connect/disconnect/execute.
    The get_profile and get_schema methods have default implementations
    that work via SQL, but connectors may override them with
    database-specific optimizations.
    """

    def connect(self) -> Connection:
        """Establish connection to the data source."""
        ...

    def disconnect(self, connection: Connection) -> None:
        """Close the connection."""
        ...

    def get_schema(self, connection: Connection, table: str) -> list[dict[str, Any]]:
        """Return schema info for a table.

        Default implementation uses get_columns(). Connectors may override
        with native INFORMATION_SCHEMA queries for richer metadata.
        """
        return connection.get_columns(table)

    def get_profile(
        self,
        connection: Connection,
        table: str,
        columns: list[str] | None = None,
        sample_size: int | None = None,
    ) -> dict[str, Any]:
        """Return statistical profile of a table.

        Default implementation delegates to the profiler module.
        Connectors may override with database-specific profiling
        (e.g., Snowflake DESCRIBE TABLE EXTENDED).
        """
        from dataclasses import asdict

        from provero.core.profiler import profile_table
        result = profile_table(connection, table, sample_size=sample_size)
        return asdict(result)
