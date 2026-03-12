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
from assay.core.results import Status


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
