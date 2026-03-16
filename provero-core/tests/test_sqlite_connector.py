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

"""Integration tests for the SQLite connector (SQLAlchemy-based)."""

from __future__ import annotations

import sqlite3
import textwrap
from pathlib import Path

import pytest
from sqlalchemy import text

from provero.checks.registry import get_check_runner
from provero.connectors.factory import create_connector
from provero.connectors.postgres import SQLAlchemyConnection, SQLAlchemyConnector
from provero.core.compiler import CheckConfig, SourceConfig, SuiteConfig, compile_file
from provero.core.engine import run_suite
from provero.core.results import Status

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_db(tmp_path: Path) -> Path:
    """Create a temporary SQLite database with test tables."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id TEXT,
            amount REAL,
            status TEXT
        )
    """)
    cur.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?)",
        [
            (1, "C001", 150.00, "delivered"),
            (2, "C002", 89.99, "shipped"),
            (3, "C003", 220.50, "pending"),
            (4, "C001", 45.00, "delivered"),
            (5, "C004", 999.99, "cancelled"),
        ],
    )
    cur.execute("""
        CREATE TABLE events (
            event_id INTEGER,
            user_id TEXT,
            event_type TEXT,
            email TEXT,
            created_at TEXT
        )
    """)
    cur.executemany(
        "INSERT INTO events VALUES (?, ?, ?, ?, datetime('now', ?))",
        [
            (1, "U001", "login", "alice@example.com", "-1 hours"),
            (2, "U002", "purchase", "bob@test.com", "-2 hours"),
            (3, "U001", "logout", "alice@example.com", "-30 minutes"),
            (4, "U003", "login", "charlie@example.com", "-10 minutes"),
            (5, "U004", "purchase", "bad-email", "-5 minutes"),
        ],
    )
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def sqlite_connector(sqlite_db: Path) -> SQLAlchemyConnector:
    """SQLAlchemyConnector pointing at the temporary SQLite database."""
    return SQLAlchemyConnector(connection_string=f"sqlite:///{sqlite_db}")


@pytest.fixture
def sqlite_connection(sqlite_connector: SQLAlchemyConnector) -> SQLAlchemyConnection:
    """An open SQLAlchemy connection to the temporary SQLite database."""
    conn = sqlite_connector.connect()
    yield conn
    sqlite_connector.disconnect(conn)


class _SharedSQLiteConnector:
    """Connector wrapper that reuses a single connection (for test isolation)."""

    def __init__(self, conn: SQLAlchemyConnection) -> None:
        self._conn = conn

    def connect(self) -> SQLAlchemyConnection:
        return self._conn

    def disconnect(self, connection: SQLAlchemyConnection) -> None:
        pass  # lifecycle managed by fixture


# ---------------------------------------------------------------------------
# Connection tests
# ---------------------------------------------------------------------------


class TestSQLiteConnection:
    def test_connect_and_execute(self, sqlite_connection: SQLAlchemyConnection):
        rows = sqlite_connection.execute("SELECT COUNT(*) AS cnt FROM orders")
        assert rows == [{"cnt": 5}]

    def test_factory_creates_sqlite_connector(self, sqlite_db: Path):
        source = SourceConfig(
            type="sqlite",
            connection=f"sqlite:///{sqlite_db}",
            table="orders",
        )
        connector = create_connector(source)
        assert isinstance(connector, SQLAlchemyConnector)
        conn = connector.connect()
        rows = conn.execute("SELECT COUNT(*) AS cnt FROM orders")
        assert rows[0]["cnt"] == 5
        connector.disconnect(conn)

    def test_query_missing_table_raises(self, sqlite_db: Path):
        connector = SQLAlchemyConnector(connection_string=f"sqlite:///{sqlite_db}")
        conn = connector.connect()
        with pytest.raises(Exception, match="no_such_table"):
            conn.execute("SELECT * FROM no_such_table")
        connector.disconnect(conn)


# ---------------------------------------------------------------------------
# Individual check tests against SQLite
# ---------------------------------------------------------------------------


class TestNotNullSQLite:
    def test_pass(self, sqlite_connection):
        runner = get_check_runner("not_null")
        result = runner(
            connection=sqlite_connection,
            table="orders",
            check_config=CheckConfig(check_type="not_null", column="order_id"),
        )
        assert result.status == Status.PASS

    def test_fail(self, sqlite_connection):
        sqlite_connection._conn.execute(
            text(
                "INSERT INTO orders (order_id, customer_id, amount, status) "
                "VALUES (6, NULL, 10.00, 'pending')"
            )
        )
        runner = get_check_runner("not_null")
        result = runner(
            connection=sqlite_connection,
            table="orders",
            check_config=CheckConfig(check_type="not_null", column="customer_id"),
        )
        assert result.status == Status.FAIL
        assert result.failing_rows >= 1


class TestUniqueSQLite:
    def test_pass(self, sqlite_connection):
        runner = get_check_runner("unique")
        result = runner(
            connection=sqlite_connection,
            table="orders",
            check_config=CheckConfig(check_type="unique", column="order_id"),
        )
        assert result.status == Status.PASS

    def test_fail(self, sqlite_connection):
        runner = get_check_runner("unique")
        result = runner(
            connection=sqlite_connection,
            table="orders",
            check_config=CheckConfig(check_type="unique", column="customer_id"),
        )
        assert result.status == Status.FAIL
        assert result.failing_rows > 0


class TestRangeSQLite:
    def test_pass(self, sqlite_connection):
        runner = get_check_runner("range")
        result = runner(
            connection=sqlite_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="range",
                column="amount",
                params={"min": 0, "max": 10000},
            ),
        )
        assert result.status == Status.PASS

    def test_fail(self, sqlite_connection):
        runner = get_check_runner("range")
        result = runner(
            connection=sqlite_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="range",
                column="amount",
                params={"min": 100, "max": 500},
            ),
        )
        assert result.status == Status.FAIL


class TestCompletenessSQLite:
    def test_pass(self, sqlite_connection):
        runner = get_check_runner("completeness")
        result = runner(
            connection=sqlite_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="completeness",
                column="order_id",
                params={"min": 0.95},
            ),
        )
        assert result.status == Status.PASS
        assert result.observed_value == "100.0%"

    def test_fail(self, sqlite_connection):
        sqlite_connection._conn.execute(
            text(
                "INSERT INTO orders (order_id, customer_id, amount, status) "
                "VALUES (6, NULL, 10.00, 'pending')"
            )
        )
        runner = get_check_runner("completeness")
        result = runner(
            connection=sqlite_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="completeness",
                column="customer_id",
                params={"min": 1.0},
            ),
        )
        assert result.status == Status.FAIL
        assert result.failing_rows == 1


class TestAcceptedValuesSQLite:
    def test_pass(self, sqlite_connection):
        runner = get_check_runner("accepted_values")
        result = runner(
            connection=sqlite_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="accepted_values",
                column="status",
                params={"values": ["pending", "shipped", "delivered", "cancelled"]},
            ),
        )
        assert result.status == Status.PASS

    def test_fail(self, sqlite_connection):
        runner = get_check_runner("accepted_values")
        result = runner(
            connection=sqlite_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="accepted_values",
                column="status",
                params={"values": ["pending", "shipped"]},
            ),
        )
        assert result.status == Status.FAIL


class TestRowCountSQLite:
    def test_pass(self, sqlite_connection):
        runner = get_check_runner("row_count")
        result = runner(
            connection=sqlite_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="row_count",
                params={"min": 1, "max": 100},
            ),
        )
        assert result.status == Status.PASS

    def test_fail_below_min(self, sqlite_connection):
        runner = get_check_runner("row_count")
        result = runner(
            connection=sqlite_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="row_count",
                params={"min": 1000},
            ),
        )
        assert result.status == Status.FAIL


class TestCustomSqlSQLite:
    def test_pass(self, sqlite_connection):
        runner = get_check_runner("custom_sql")
        result = runner(
            connection=sqlite_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="custom_sql",
                params={
                    "name": "no_negative_amounts",
                    "query": "SELECT COUNT(*) = 0 FROM orders WHERE amount < 0",
                },
            ),
        )
        assert result.status == Status.PASS

    def test_fail(self, sqlite_connection):
        sqlite_connection._conn.execute(
            text("INSERT INTO orders VALUES (6, 'C005', -10.00, 'pending')")
        )
        runner = get_check_runner("custom_sql")
        result = runner(
            connection=sqlite_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="custom_sql",
                params={
                    "name": "no_negative_amounts",
                    "query": "SELECT COUNT(*) = 0 FROM orders WHERE amount < 0",
                },
            ),
        )
        assert result.status == Status.FAIL


# ---------------------------------------------------------------------------
# Engine integration (run_suite via SQLite)
# ---------------------------------------------------------------------------


class TestEngineSQLite:
    def test_run_suite_all_pass(self, sqlite_connection):
        connector = _SharedSQLiteConnector(sqlite_connection)
        suite = SuiteConfig(
            name="sqlite_suite",
            source=SourceConfig(type="sqlite", table="orders"),
            checks=[
                CheckConfig(check_type="not_null", column="order_id"),
                CheckConfig(check_type="unique", column="order_id"),
                CheckConfig(check_type="row_count", params={"min": 1, "max": 100}),
            ],
        )
        result = run_suite(suite, connector, optimize=True)
        assert result.status == Status.PASS
        assert result.total >= 3
        assert result.failed == 0

    def test_run_suite_mixed_results(self, sqlite_connection):
        connector = _SharedSQLiteConnector(sqlite_connection)
        suite = SuiteConfig(
            name="sqlite_suite",
            source=SourceConfig(type="sqlite", table="orders"),
            checks=[
                CheckConfig(check_type="not_null", column="order_id"),
                CheckConfig(check_type="unique", column="customer_id"),  # C001 duplicated
            ],
        )
        result = run_suite(suite, connector, optimize=True)
        assert result.status == Status.FAIL
        assert result.passed >= 1
        assert result.failed >= 1

    def test_run_suite_no_optimize(self, sqlite_connection):
        connector = _SharedSQLiteConnector(sqlite_connection)
        suite = SuiteConfig(
            name="sqlite_suite",
            source=SourceConfig(type="sqlite", table="orders"),
            checks=[
                CheckConfig(check_type="not_null", column="order_id"),
                CheckConfig(check_type="row_count", params={"min": 1}),
            ],
        )
        result = run_suite(suite, connector, optimize=False)
        assert result.status == Status.PASS
        assert result.total == 2

    def test_missing_table_error(self, sqlite_connection):
        connector = _SharedSQLiteConnector(sqlite_connection)
        suite = SuiteConfig(
            name="sqlite_suite",
            source=SourceConfig(type="sqlite", table="nonexistent_table"),
            checks=[
                CheckConfig(check_type="row_count", params={"min": 1}),
            ],
        )
        result = run_suite(suite, connector, optimize=True)
        error_checks = [c for c in result.checks if c.status == Status.ERROR]
        assert len(error_checks) >= 1


# ---------------------------------------------------------------------------
# Compiler integration (YAML -> SQLite suite)
# ---------------------------------------------------------------------------


class TestCompilerSQLite:
    def test_compile_and_run_sqlite_config(self, sqlite_db: Path, tmp_path: Path):
        config_path = tmp_path / "provero.yaml"
        config_path.write_text(
            textwrap.dedent(f"""\
            source:
              type: sqlite
              connection: "sqlite:///{sqlite_db}"
              table: orders

            checks:
              - not_null: order_id
              - unique: order_id
              - row_count:
                  min: 1
              - accepted_values:
                  column: status
                  values:
                    - pending
                    - shipped
                    - delivered
                    - cancelled
              - range:
                  column: amount
                  min: 0
                  max: 10000
              - completeness:
                  column: customer_id
                  min: 0.95
        """)
        )

        config = compile_file(config_path)
        assert len(config.suites) == 1

        suite = config.suites[0]
        assert suite.source.type == "sqlite"

        connector = create_connector(suite.source)
        result = run_suite(suite, connector, optimize=True)

        assert result.status == Status.PASS
        assert result.total >= 6
        assert result.failed == 0


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandlingSQLite:
    def test_factory_requires_connection_string(self):
        source = SourceConfig(type="sqlite", table="orders")
        with pytest.raises(ValueError, match="requires a connection string"):
            create_connector(source)

    def test_bad_sql_returns_error(self, sqlite_connection):
        connector = _SharedSQLiteConnector(sqlite_connection)
        suite = SuiteConfig(
            name="sqlite_suite",
            source=SourceConfig(type="sqlite", table="orders"),
            checks=[
                CheckConfig(
                    check_type="custom_sql",
                    params={"name": "bad", "query": "THIS IS NOT SQL"},
                ),
            ],
        )
        result = run_suite(suite, connector, optimize=True)
        assert result.checks[0].status == Status.ERROR
