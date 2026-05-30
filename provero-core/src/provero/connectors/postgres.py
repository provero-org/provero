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

"""PostgreSQL connector via SQLAlchemy.

This module backs the postgres/mysql/snowflake/bigquery/redshift/databricks
connectors. It is hardened for scale with configurable connection pooling,
bounded retry-with-backoff on transient errors, and best-effort per-driver
timeouts. All new constructor arguments are optional and default to the
historical behavior, so existing callers are unaffected.
"""

from __future__ import annotations

from types import TracebackType
from typing import Any, cast

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from provero.connectors.pool import PoolConfig, build_engine_kwargs
from provero.connectors.retry import RetryConfig, retry_call


class SQLAlchemyConnection:
    """SQLAlchemy-based connection wrapper.

    Supports the context-manager protocol so the underlying SQLAlchemy
    connection is always closed, even when used directly::

        with connector.connect() as conn:
            conn.execute("SELECT 1")

    Queries are retried on transient connection errors when a non-trivial
    ``RetryConfig`` is supplied; programming errors (missing table, bad SQL)
    are never retried and surface immediately.
    """

    def __init__(self, engine: Engine, retry: RetryConfig | None = None) -> None:
        self._engine = engine
        self._retry = retry if retry is not None else RetryConfig()
        # Establishing the connection itself can hit transient failures, so
        # the initial connect is also retried under the same policy.
        self._conn = retry_call(engine.connect, self._retry)

    def __enter__(self) -> SQLAlchemyConnection:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def execute(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        def _run() -> list[dict[str, Any]]:
            result = self._conn.execute(text(query), params or {})
            columns = list(result.keys())
            return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]

        return retry_call(_run, self._retry)

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


class PostgresConnector:
    """Connector for PostgreSQL databases.

    The engine is created lazily on first ``connect`` and reused, so the
    SQLAlchemy connection pool is shared across connections. Pooling and
    retry behavior are configurable via the optional ``pool`` and ``retry``
    arguments; both default to backward-compatible no-ops.
    """

    def __init__(
        self,
        connection_string: str,
        pool: PoolConfig | None = None,
        retry: RetryConfig | None = None,
    ) -> None:
        self.connection_string = connection_string
        self._pool = pool if pool is not None else PoolConfig()
        self._retry = retry if retry is not None else RetryConfig()
        self._engine: Engine | None = None

    def connect(self) -> SQLAlchemyConnection:
        # Lazily create engine on first connect, then reuse for pooling
        if self._engine is None:
            kwargs = build_engine_kwargs(self.connection_string, self._pool)
            self._engine = create_engine(self.connection_string, **kwargs)
        return SQLAlchemyConnection(self._engine, retry=self._retry)

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
    """Generic connector for any SQLAlchemy-supported database.

    A fresh engine is created per ``connect`` and disposed on ``disconnect``
    to avoid leaking pooled engines in long-running or parallel runs. Pooling
    and retry behavior are configurable via the optional ``pool`` and
    ``retry`` arguments; both default to backward-compatible no-ops.
    """

    def __init__(
        self,
        connection_string: str,
        pool: PoolConfig | None = None,
        retry: RetryConfig | None = None,
    ) -> None:
        self.connection_string = connection_string
        self._pool = pool if pool is not None else PoolConfig()
        self._retry = retry if retry is not None else RetryConfig()

    def connect(self) -> SQLAlchemyConnection:
        kwargs = build_engine_kwargs(self.connection_string, self._pool)
        engine = create_engine(self.connection_string, **kwargs)
        return SQLAlchemyConnection(engine, retry=self._retry)

    def disconnect(self, connection: SQLAlchemyConnection) -> None:
        # This connector creates a fresh engine per connect (no caching), so
        # the engine must be disposed alongside the connection to avoid
        # leaking pooled engines in long-running or parallel runs.
        connection.close()
        connection._engine.dispose()

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
