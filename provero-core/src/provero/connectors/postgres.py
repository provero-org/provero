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

"""PostgreSQL connector via SQLAlchemy."""

from __future__ import annotations

from typing import Any, cast

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class SQLAlchemyConnection:
    """SQLAlchemy-based connection wrapper."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._conn = engine.connect()

    def execute(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        result = self._conn.execute(text(query), params or {})
        columns = list(result.keys())
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]

    def get_columns(self, table: str) -> list[dict[str, Any]]:
        if "." in table:
            schema_name, table_name = table.rsplit(".", 1)
            result = self._conn.execute(
                text(
                    "SELECT column_name, data_type, is_nullable "
                    "FROM information_schema.columns "
                    "WHERE table_schema = :schema AND table_name = :table "
                    "ORDER BY ordinal_position"
                ),
                {"schema": schema_name, "table": table_name},
            )
        else:
            result = self._conn.execute(
                text(
                    "SELECT column_name, data_type, is_nullable "
                    "FROM information_schema.columns "
                    "WHERE table_name = :table "
                    "AND table_schema NOT IN ('information_schema', 'pg_catalog') "
                    "ORDER BY ordinal_position"
                ),
                {"table": table},
            )
        return [
            {"name": row[0], "type": row[1], "nullable": row[2] == "YES"}
            for row in result.fetchall()
        ]

    def close(self) -> None:
        self._conn.close()
        self._engine.dispose()


class PostgresConnector:
    """Connector for PostgreSQL databases."""

    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string
        self._engine: Engine | None = None

    def connect(self) -> SQLAlchemyConnection:
        # Lazily create engine on first connect, then reuse for pooling
        if self._engine is None:
            self._engine = create_engine(self.connection_string)
        return SQLAlchemyConnection(self._engine)

    def disconnect(self, connection: SQLAlchemyConnection) -> None:
        connection.close()

    def get_schema(self, connection: SQLAlchemyConnection, table: str) -> list[dict[str, Any]]:
        return connection.get_columns(table)

    def get_profile(
        self,
        connection: SQLAlchemyConnection,
        table: str,
        columns: list[str] | None = None,
        sample_size: int | None = None,
    ) -> dict[str, Any]:
        from provero.core.profiler import profile_table

        result = profile_table(connection, table, sample_size=sample_size)
        data = {
            "table": result.table,
            "row_count": result.row_count,
            "column_count": result.column_count,
            "columns": [
                {
                    "name": c.name,
                    "dtype": c.dtype,
                    "null_count": c.null_count,
                    "null_pct": c.null_pct,
                    "distinct_count": c.distinct_count,
                    "distinct_pct": c.distinct_pct,
                }
                for c in result.columns
            ],
        }
        if columns:
            cols_list = cast(list[dict[str, Any]], data["columns"])
            data["columns"] = [c for c in cols_list if c["name"] in columns]
        return data


class SQLAlchemyConnector:
    """Generic connector for any SQLAlchemy-supported database."""

    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string

    def connect(self) -> SQLAlchemyConnection:
        engine = create_engine(self.connection_string)
        return SQLAlchemyConnection(engine)

    def disconnect(self, connection: SQLAlchemyConnection) -> None:
        connection.close()

    def get_schema(self, connection: SQLAlchemyConnection, table: str) -> list[dict[str, Any]]:
        return connection.get_columns(table)

    def get_profile(
        self,
        connection: SQLAlchemyConnection,
        table: str,
        columns: list[str] | None = None,
        sample_size: int | None = None,
    ) -> dict[str, Any]:
        from provero.core.profiler import profile_table

        result = profile_table(connection, table, sample_size=sample_size)
        data = {
            "table": result.table,
            "row_count": result.row_count,
            "column_count": result.column_count,
            "columns": [
                {
                    "name": c.name,
                    "dtype": c.dtype,
                    "null_count": c.null_count,
                    "null_pct": c.null_pct,
                    "distinct_count": c.distinct_count,
                    "distinct_pct": c.distinct_pct,
                }
                for c in result.columns
            ],
        }
        if columns:
            cols_list = cast(list[dict[str, Any]], data["columns"])
            data["columns"] = [c for c in cols_list if c["name"] in columns]
        return data
