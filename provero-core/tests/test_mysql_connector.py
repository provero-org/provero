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

"""Integration tests for the MySQL connector.

These tests require a running MySQL server and the pymysql driver.
They are skipped automatically when either is unavailable.

To run locally, set the MYSQL_TEST_URI environment variable:

    export MYSQL_TEST_URI="mysql+pymysql://user:pass@localhost:3306/testdb"
    uv run pytest provero-core/tests/test_mysql_connector.py -v
"""

from __future__ import annotations

import os

import pytest

# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------

try:
    import pymysql  # noqa: F401

    _HAS_PYMYSQL = True
except ImportError:
    _HAS_PYMYSQL = False

_MYSQL_URI = os.environ.get("MYSQL_TEST_URI", "")

_skip_no_driver = pytest.mark.skipif(
    not _HAS_PYMYSQL,
    reason="pymysql not installed (pip install pymysql)",
)

_skip_no_server = pytest.mark.skipif(
    not _MYSQL_URI,
    reason="MYSQL_TEST_URI environment variable not set",
)

mysql_required = pytest.mark.usefixtures()  # placeholder for stacking
pytestmark = [_skip_no_driver, _skip_no_server]

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_TEST_TABLE = "provero_test_orders"


@pytest.fixture(scope="module")
def mysql_connector():
    """Create a SQLAlchemyConnector pointing at the test MySQL database."""
    from provero.connectors.postgres import SQLAlchemyConnector

    return SQLAlchemyConnector(connection_string=_MYSQL_URI)


@pytest.fixture(scope="module")
def mysql_connection(mysql_connector):
    """Establish a connection and seed a test table, tear down after module."""
    conn = mysql_connector.connect()

    conn.execute(f"DROP TABLE IF EXISTS {_TEST_TABLE}")
    conn.execute(
        f"""
        CREATE TABLE {_TEST_TABLE} (
            order_id INT NOT NULL,
            customer_id VARCHAR(50),
            amount DECIMAL(10,2),
            status VARCHAR(20)
        )
        """
    )
    conn.execute(
        f"""
        INSERT INTO {_TEST_TABLE} (order_id, customer_id, amount, status) VALUES
        (1, 'C001', 150.00, 'delivered'),
        (2, 'C002', 89.99, 'shipped'),
        (3, 'C003', 220.50, 'pending'),
        (4, 'C001', 45.00, 'delivered'),
        (5, 'C004', 999.99, 'cancelled')
        """
    )

    yield conn

    conn.execute(f"DROP TABLE IF EXISTS {_TEST_TABLE}")
    mysql_connector.disconnect(conn)


# ---------------------------------------------------------------------------
# Connection tests
# ---------------------------------------------------------------------------


class TestMySQLConnection:
    def test_connect_and_execute(self, mysql_connection):
        """Basic connectivity: run a trivial query."""
        rows = mysql_connection.execute("SELECT 1 AS n")
        assert len(rows) == 1
        assert rows[0]["n"] == 1

    def test_query_test_table(self, mysql_connection):
        """Verify the seeded test table is readable."""
        rows = mysql_connection.execute(f"SELECT COUNT(*) AS cnt FROM {_TEST_TABLE}")
        assert rows[0]["cnt"] == 5

    def test_get_columns(self, mysql_connection):
        """get_columns returns column metadata from INFORMATION_SCHEMA."""
        columns = mysql_connection.get_columns(_TEST_TABLE)
        names = [c["name"] for c in columns]
        assert "order_id" in names
        assert "customer_id" in names
        assert "amount" in names
        assert "status" in names

    def test_column_nullable_flag(self, mysql_connection):
        """order_id is NOT NULL; other columns are nullable."""
        columns = mysql_connection.get_columns(_TEST_TABLE)
        col_map = {c["name"]: c for c in columns}
        assert col_map["order_id"]["nullable"] is False
        assert col_map["customer_id"]["nullable"] is True


# ---------------------------------------------------------------------------
# Quality check tests
# ---------------------------------------------------------------------------


class TestMySQLQualityChecks:
    def test_not_null_check_passes(self, mysql_connector, mysql_connection):
        """not_null on order_id should pass (all non-null)."""
        from provero.core.compiler import CheckConfig, SourceConfig, SuiteConfig
        from provero.core.engine import run_suite
        from provero.core.results import Status

        suite = SuiteConfig(
            name="mysql_not_null",
            source=SourceConfig(type="mysql", table=_TEST_TABLE),
            checks=[CheckConfig(check_type="not_null", column="order_id")],
        )

        class _Connector:
            def connect(self):
                return mysql_connection

            def disconnect(self, _conn):
                pass

        result = run_suite(suite, _Connector())
        assert result.total == 1
        assert result.checks[0].status == Status.PASS

    def test_unique_check_fails_on_duplicate(self, mysql_connector, mysql_connection):
        """unique on customer_id should fail (C001 appears twice)."""
        from provero.core.compiler import CheckConfig, SourceConfig, SuiteConfig
        from provero.core.engine import run_suite
        from provero.core.results import Status

        suite = SuiteConfig(
            name="mysql_unique",
            source=SourceConfig(type="mysql", table=_TEST_TABLE),
            checks=[CheckConfig(check_type="unique", column="customer_id")],
        )

        class _Connector:
            def connect(self):
                return mysql_connection

            def disconnect(self, _conn):
                pass

        result = run_suite(suite, _Connector())
        assert result.total == 1
        assert result.checks[0].status == Status.FAIL

    def test_row_count_check(self, mysql_connector, mysql_connection):
        """row_count with min/max boundaries."""
        from provero.core.compiler import CheckConfig, SourceConfig, SuiteConfig
        from provero.core.engine import run_suite
        from provero.core.results import Status

        suite = SuiteConfig(
            name="mysql_row_count",
            source=SourceConfig(type="mysql", table=_TEST_TABLE),
            checks=[CheckConfig(check_type="row_count", params={"min": 1, "max": 100})],
        )

        class _Connector:
            def connect(self):
                return mysql_connection

            def disconnect(self, _conn):
                pass

        result = run_suite(suite, _Connector())
        assert result.total == 1
        assert result.checks[0].status == Status.PASS

    def test_multiple_checks_mixed(self, mysql_connector, mysql_connection):
        """Run several checks at once, expect mixed pass/fail."""
        from provero.core.compiler import CheckConfig, SourceConfig, SuiteConfig
        from provero.core.engine import run_suite
        from provero.core.results import Status

        suite = SuiteConfig(
            name="mysql_mixed",
            source=SourceConfig(type="mysql", table=_TEST_TABLE),
            checks=[
                CheckConfig(check_type="not_null", column="order_id"),
                CheckConfig(check_type="unique", column="order_id"),
                CheckConfig(check_type="unique", column="customer_id"),
                CheckConfig(check_type="row_count", params={"min": 1}),
            ],
        )

        class _Connector:
            def connect(self):
                return mysql_connection

            def disconnect(self, _conn):
                pass

        result = run_suite(suite, _Connector())
        assert result.total >= 4
        statuses = {c.status for c in result.checks}
        assert Status.PASS in statuses
        assert Status.FAIL in statuses


# ---------------------------------------------------------------------------
# Engine integration via factory
# ---------------------------------------------------------------------------


class TestMySQLFactoryIntegration:
    def test_factory_creates_connector(self):
        """create_connector resolves 'mysql' to SQLAlchemyConnector."""
        from provero.connectors.factory import create_connector
        from provero.connectors.postgres import SQLAlchemyConnector
        from provero.core.compiler import SourceConfig

        source = SourceConfig(type="mysql", connection=_MYSQL_URI, table=_TEST_TABLE)
        connector = create_connector(source)
        assert isinstance(connector, SQLAlchemyConnector)

    def test_factory_roundtrip(self, mysql_connection):
        """Factory-created connector can run a full suite."""
        from provero.connectors.factory import create_connector
        from provero.core.compiler import CheckConfig, SourceConfig, SuiteConfig
        from provero.core.engine import run_suite
        from provero.core.results import Status

        source = SourceConfig(type="mysql", connection=_MYSQL_URI, table=_TEST_TABLE)
        connector = create_connector(source)
        suite = SuiteConfig(
            name="mysql_factory",
            source=source,
            checks=[
                CheckConfig(check_type="not_null", column="order_id"),
                CheckConfig(check_type="row_count", params={"min": 1}),
            ],
        )

        result = run_suite(suite, connector)
        assert result.total == 2
        assert result.passed == 2
        assert result.status == Status.PASS


# ---------------------------------------------------------------------------
# Schema / profile integration
# ---------------------------------------------------------------------------


class TestMySQLSchemaAndProfile:
    def test_get_schema(self, mysql_connector, mysql_connection):
        """get_schema returns column metadata."""
        schema = mysql_connector.get_schema(mysql_connection, _TEST_TABLE)
        assert len(schema) == 4
        names = [c["name"] for c in schema]
        assert "order_id" in names

    def test_get_profile(self, mysql_connector, mysql_connection):
        """get_profile returns row_count, column_count and per-column stats."""
        profile = mysql_connector.get_profile(mysql_connection, _TEST_TABLE)
        assert profile["table"] == _TEST_TABLE
        assert profile["row_count"] == 5
        assert profile["column_count"] == 4
        for col in profile["columns"]:
            assert "null_count" in col
            assert "distinct_count" in col


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestMySQLErrorHandling:
    def test_bad_table_name(self, mysql_connector, mysql_connection):
        """Querying a non-existent table produces an error check result."""
        from provero.core.compiler import CheckConfig, SourceConfig, SuiteConfig
        from provero.core.engine import run_suite
        from provero.core.results import Status

        suite = SuiteConfig(
            name="mysql_bad_table",
            source=SourceConfig(type="mysql", table="table_does_not_exist"),
            checks=[
                CheckConfig(check_type="row_count", params={"min": 1}),
            ],
        )

        class _Connector:
            def connect(self):
                return mysql_connection

            def disconnect(self, _conn):
                pass

        result = run_suite(suite, _Connector())
        error_checks = [c for c in result.checks if c.status == Status.ERROR]
        assert len(error_checks) >= 1

    def test_invalid_connection_string(self):
        """An unreachable URI raises on connect()."""
        from sqlalchemy.exc import OperationalError, ProgrammingError

        from provero.connectors.postgres import SQLAlchemyConnector

        connector = SQLAlchemyConnector(
            connection_string="mysql+pymysql://bad:bad@localhost:19999/nope"
        )
        with pytest.raises((OperationalError, ProgrammingError, OSError)):
            conn = connector.connect()
            # Force the connection to actually reach out
            conn.execute("SELECT 1")
