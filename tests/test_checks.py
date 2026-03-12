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

"""Tests for built-in checks using DuckDB."""

from __future__ import annotations

import pytest

from assay.checks.registry import get_check_runner
from assay.connectors.duckdb import DuckDBConnector
from assay.core.compiler import CheckConfig
from assay.core.results import Severity, Status


@pytest.fixture
def duckdb_connection():
    """Create a DuckDB connection with test data."""
    connector = DuckDBConnector()
    conn = connector.connect()
    conn._conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id VARCHAR,
            amount DECIMAL(10,2),
            status VARCHAR
        )
    """)
    conn._conn.execute("""
        INSERT INTO orders VALUES
        (1, 'C001', 150.00, 'delivered'),
        (2, 'C002', 89.99, 'shipped'),
        (3, 'C003', 220.50, 'pending'),
        (4, 'C001', 45.00, 'delivered'),
        (5, 'C004', 999.99, 'cancelled')
    """)
    conn._conn.execute("""
        CREATE TABLE events (
            event_id INTEGER,
            user_id VARCHAR,
            event_type VARCHAR,
            email VARCHAR,
            created_at TIMESTAMP
        )
    """)
    conn._conn.execute("""
        INSERT INTO events VALUES
        (1, 'U001', 'login', 'alice@example.com', CURRENT_TIMESTAMP - INTERVAL '1 hour'),
        (2, 'U002', 'purchase', 'bob@test.com', CURRENT_TIMESTAMP - INTERVAL '2 hours'),
        (3, 'U001', 'logout', 'alice@example.com', CURRENT_TIMESTAMP - INTERVAL '30 minutes'),
        (4, 'U003', 'login', 'charlie@example.com', CURRENT_TIMESTAMP - INTERVAL '10 minutes'),
        (5, 'U004', 'purchase', 'bad-email', CURRENT_TIMESTAMP - INTERVAL '5 minutes')
    """)
    yield conn
    connector.disconnect(conn)


class TestNotNull:
    def test_pass(self, duckdb_connection):
        runner = get_check_runner("not_null")
        result = runner(
            connection=duckdb_connection,
            table="orders",
            check_config=CheckConfig(check_type="not_null", column="order_id"),
        )
        assert result.status == Status.PASS

    def test_fail(self, duckdb_connection):
        duckdb_connection._conn.execute(
            "INSERT INTO orders VALUES (6, NULL, 10.00, 'pending')"
        )
        runner = get_check_runner("not_null")
        result = runner(
            connection=duckdb_connection,
            table="orders",
            check_config=CheckConfig(check_type="not_null", column="customer_id"),
        )
        assert result.status == Status.FAIL
        assert result.failing_rows == 1


class TestUnique:
    def test_pass(self, duckdb_connection):
        runner = get_check_runner("unique")
        result = runner(
            connection=duckdb_connection,
            table="orders",
            check_config=CheckConfig(check_type="unique", column="order_id"),
        )
        assert result.status == Status.PASS

    def test_fail(self, duckdb_connection):
        runner = get_check_runner("unique")
        result = runner(
            connection=duckdb_connection,
            table="orders",
            check_config=CheckConfig(check_type="unique", column="customer_id"),
        )
        assert result.status == Status.FAIL
        assert result.failing_rows > 0


class TestAcceptedValues:
    def test_pass(self, duckdb_connection):
        runner = get_check_runner("accepted_values")
        result = runner(
            connection=duckdb_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="accepted_values",
                column="status",
                params={"values": ["pending", "shipped", "delivered", "cancelled"]},
            ),
        )
        assert result.status == Status.PASS

    def test_fail(self, duckdb_connection):
        runner = get_check_runner("accepted_values")
        result = runner(
            connection=duckdb_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="accepted_values",
                column="status",
                params={"values": ["pending", "shipped"]},
            ),
        )
        assert result.status == Status.FAIL


class TestRange:
    def test_pass(self, duckdb_connection):
        runner = get_check_runner("range")
        result = runner(
            connection=duckdb_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="range",
                column="amount",
                params={"min": 0, "max": 10000},
            ),
        )
        assert result.status == Status.PASS

    def test_fail(self, duckdb_connection):
        runner = get_check_runner("range")
        result = runner(
            connection=duckdb_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="range",
                column="amount",
                params={"min": 100, "max": 500},
            ),
        )
        assert result.status == Status.FAIL


class TestRowCount:
    def test_pass(self, duckdb_connection):
        runner = get_check_runner("row_count")
        result = runner(
            connection=duckdb_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="row_count",
                params={"min": 1, "max": 100},
            ),
        )
        assert result.status == Status.PASS

    def test_fail_below_min(self, duckdb_connection):
        runner = get_check_runner("row_count")
        result = runner(
            connection=duckdb_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="row_count",
                params={"min": 1000},
            ),
        )
        assert result.status == Status.FAIL


class TestCustomSql:
    def test_pass(self, duckdb_connection):
        runner = get_check_runner("custom_sql")
        result = runner(
            connection=duckdb_connection,
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

    def test_fail(self, duckdb_connection):
        duckdb_connection._conn.execute(
            "INSERT INTO orders VALUES (6, 'C005', -10.00, 'pending')"
        )
        runner = get_check_runner("custom_sql")
        result = runner(
            connection=duckdb_connection,
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


class TestFreshness:
    def test_pass_recent_data(self, duckdb_connection):
        runner = get_check_runner("freshness")
        result = runner(
            connection=duckdb_connection,
            table="events",
            check_config=CheckConfig(
                check_type="freshness",
                column="created_at",
                params={"max_age": "24h"},
            ),
        )
        assert result.status == Status.PASS

    def test_fail_stale_data(self, duckdb_connection):
        # Create a table with old data
        duckdb_connection._conn.execute("""
            CREATE TABLE old_events (ts TIMESTAMP)
        """)
        duckdb_connection._conn.execute("""
            INSERT INTO old_events VALUES (CURRENT_TIMESTAMP - INTERVAL '48 hours')
        """)
        runner = get_check_runner("freshness")
        result = runner(
            connection=duckdb_connection,
            table="old_events",
            check_config=CheckConfig(
                check_type="freshness",
                column="ts",
                params={"max_age": "24h"},
            ),
        )
        assert result.status == Status.FAIL

    def test_fail_no_data(self, duckdb_connection):
        duckdb_connection._conn.execute("CREATE TABLE empty_ts (ts TIMESTAMP)")
        runner = get_check_runner("freshness")
        result = runner(
            connection=duckdb_connection,
            table="empty_ts",
            check_config=CheckConfig(
                check_type="freshness",
                column="ts",
                params={"max_age": "1h"},
            ),
        )
        assert result.status == Status.FAIL
        assert result.observed_value == "no data"


class TestRegex:
    def test_pass(self, duckdb_connection):
        runner = get_check_runner("regex")
        result = runner(
            connection=duckdb_connection,
            table="events",
            check_config=CheckConfig(
                check_type="regex",
                column="event_type",
                params={"pattern": "^(login|logout|purchase)$"},
            ),
        )
        assert result.status == Status.PASS

    def test_fail(self, duckdb_connection):
        runner = get_check_runner("regex")
        result = runner(
            connection=duckdb_connection,
            table="events",
            check_config=CheckConfig(
                check_type="regex",
                column="email",
                params={"pattern": "^[^@]+@[^@]+\\.[^@]+$"},
            ),
        )
        assert result.status == Status.FAIL
        assert result.failing_rows == 1  # "bad-email" doesn't match

    def test_default_severity_is_warning(self, duckdb_connection):
        runner = get_check_runner("regex")
        result = runner(
            connection=duckdb_connection,
            table="events",
            check_config=CheckConfig(
                check_type="regex",
                column="event_type",
                params={"pattern": ".*"},
            ),
        )
        assert result.severity == Severity.WARNING


class TestUniqueCombination:
    def test_pass(self, duckdb_connection):
        runner = get_check_runner("unique_combination")
        result = runner(
            connection=duckdb_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="unique_combination",
                columns=["order_id", "customer_id"],
            ),
        )
        assert result.status == Status.PASS

    def test_fail(self, duckdb_connection):
        # customer_id is not unique per order, and status repeats: (C001,delivered) appears twice
        runner = get_check_runner("unique_combination")
        result = runner(
            connection=duckdb_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="unique_combination",
                columns=["customer_id", "status"],
            ),
        )
        assert result.status == Status.FAIL

    def test_fail_duplicate_combos(self, duckdb_connection):
        duckdb_connection._conn.execute("""
            CREATE TABLE dupes (a INTEGER, b VARCHAR)
        """)
        duckdb_connection._conn.execute("""
            INSERT INTO dupes VALUES (1, 'x'), (1, 'x'), (2, 'y')
        """)
        runner = get_check_runner("unique_combination")
        result = runner(
            connection=duckdb_connection,
            table="dupes",
            check_config=CheckConfig(
                check_type="unique_combination",
                columns=["a", "b"],
            ),
        )
        assert result.status == Status.FAIL
        assert result.failing_rows == 1  # 3 total - 2 distinct = 1


class TestConfigurableSeverity:
    def test_default_severity_is_critical(self, duckdb_connection):
        runner = get_check_runner("not_null")
        result = runner(
            connection=duckdb_connection,
            table="orders",
            check_config=CheckConfig(check_type="not_null", column="order_id"),
        )
        assert result.severity == Severity.CRITICAL

    def test_override_severity_to_warning(self, duckdb_connection):
        runner = get_check_runner("not_null")
        result = runner(
            connection=duckdb_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="not_null",
                column="order_id",
                severity="warning",
            ),
        )
        assert result.severity == Severity.WARNING

    def test_override_severity_on_range(self, duckdb_connection):
        runner = get_check_runner("range")
        result = runner(
            connection=duckdb_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="range",
                column="amount",
                params={"min": 0, "max": 10000},
                severity="info",
            ),
        )
        assert result.severity == Severity.INFO

    def test_override_regex_severity_to_blocker(self, duckdb_connection):
        runner = get_check_runner("regex")
        result = runner(
            connection=duckdb_connection,
            table="events",
            check_config=CheckConfig(
                check_type="regex",
                column="event_type",
                params={"pattern": ".*"},
                severity="blocker",
            ),
        )
        assert result.severity == Severity.BLOCKER
