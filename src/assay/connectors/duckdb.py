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

"""DuckDB connector for local files (Parquet, CSV, JSON)."""

from __future__ import annotations

from typing import Any

import duckdb


class DuckDBConnection:
    """DuckDB connection wrapper."""

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def execute(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        result = self._conn.execute(query)
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def get_columns(self, table: str) -> list[dict[str, Any]]:
        result = self._conn.execute(f"DESCRIBE {table}")
        return [
            {"name": row[0], "type": row[1], "nullable": row[2] == "YES"}
            for row in result.fetchall()
        ]


class DuckDBConnector:
    """Connector for DuckDB (local files and in-memory)."""

    def __init__(self, database: str = ":memory:") -> None:
        self.database = database

    def connect(self) -> DuckDBConnection:
        conn = duckdb.connect(self.database)
        return DuckDBConnection(conn)

    def disconnect(self, connection: DuckDBConnection) -> None:
        connection._conn.close()
