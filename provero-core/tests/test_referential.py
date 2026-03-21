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

"""Tests for the referential_integrity check."""

from __future__ import annotations

import pytest

from provero.checks.registry import get_check_runner
from provero.connectors.duckdb import DuckDBConnector
from provero.core.compiler import CheckConfig
from provero.core.results import Status


@pytest.fixture
def duckdb_with_fk():
    """DuckDB connection with orders and customers tables for FK testing."""
    connector = DuckDBConnector()
    conn = connector.connect()
    conn._conn.execute("""
        CREATE TABLE customers (
            id INTEGER,
            name VARCHAR
        )
    """)
    conn._conn.execute("""
        INSERT INTO customers VALUES
        (1, 'Alice'),
        (2, 'Bob'),
        (3, 'Charlie')
    """)
    conn._conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id INTEGER,
            amount DECIMAL(10,2)
        )
    """)
    conn._conn.execute("""
        INSERT INTO orders VALUES
        (100, 1, 50.00),
        (101, 2, 75.00),
        (102, 3, 120.00)
    """)
    yield conn
    connector.disconnect(conn)


class TestReferentialIntegrity:
    def test_all_fks_valid(self, duckdb_with_fk):
        """All FK values exist in the reference table, should PASS."""
        runner = get_check_runner("referential_integrity")
        result = runner(
            connection=duckdb_with_fk,
            table="orders",
            check_config=CheckConfig(
                check_type="referential_integrity",
                column="customer_id",
                params={"reference_table": "customers", "reference_column": "id"},
            ),
        )
        assert result.status == Status.PASS
        assert result.failing_rows == 0
        assert "0 orphaned" in result.observed_value

    def test_orphaned_fk(self, duckdb_with_fk):
        """FK value references non-existent row, should FAIL with correct count."""
        duckdb_with_fk._conn.execute("""
            INSERT INTO orders VALUES
            (103, 999, 200.00),
            (104, 888, 300.00)
        """)
        runner = get_check_runner("referential_integrity")
        result = runner(
            connection=duckdb_with_fk,
            table="orders",
            check_config=CheckConfig(
                check_type="referential_integrity",
                column="customer_id",
                params={"reference_table": "customers", "reference_column": "id"},
            ),
        )
        assert result.status == Status.FAIL
        assert result.failing_rows == 2
        assert "2 orphaned" in result.observed_value

    def test_null_fk_excluded(self, duckdb_with_fk):
        """NULL FK values should be excluded (NULLs are OK for optional FKs)."""
        duckdb_with_fk._conn.execute("""
            INSERT INTO orders VALUES
            (103, NULL, 200.00),
            (104, NULL, 300.00)
        """)
        runner = get_check_runner("referential_integrity")
        result = runner(
            connection=duckdb_with_fk,
            table="orders",
            check_config=CheckConfig(
                check_type="referential_integrity",
                column="customer_id",
                params={"reference_table": "customers", "reference_column": "id"},
            ),
        )
        assert result.status == Status.PASS
        assert result.failing_rows == 0

    def test_missing_reference_table_param(self, duckdb_with_fk):
        """Missing reference_table parameter should return ERROR."""
        runner = get_check_runner("referential_integrity")
        result = runner(
            connection=duckdb_with_fk,
            table="orders",
            check_config=CheckConfig(
                check_type="referential_integrity",
                column="customer_id",
                params={"reference_column": "id"},
            ),
        )
        assert result.status == Status.ERROR
        assert "reference_table" in result.observed_value

    def test_missing_reference_column_param(self, duckdb_with_fk):
        """Missing reference_column parameter should return ERROR."""
        runner = get_check_runner("referential_integrity")
        result = runner(
            connection=duckdb_with_fk,
            table="orders",
            check_config=CheckConfig(
                check_type="referential_integrity",
                column="customer_id",
                params={"reference_table": "customers"},
            ),
        )
        assert result.status == Status.ERROR
        assert "reference_column" in result.observed_value

    def test_nonexistent_reference_table(self, duckdb_with_fk):
        """Reference table that doesn't exist should return ERROR, not crash."""
        runner = get_check_runner("referential_integrity")
        result = runner(
            connection=duckdb_with_fk,
            table="orders",
            check_config=CheckConfig(
                check_type="referential_integrity",
                column="customer_id",
                params={
                    "reference_table": "nonexistent_table",
                    "reference_column": "id",
                },
            ),
        )
        assert result.status == Status.ERROR
        assert "query error" in result.observed_value
