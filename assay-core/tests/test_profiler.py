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

"""Tests for the data profiler."""

from __future__ import annotations

import pytest

from provero.connectors.duckdb import DuckDBConnector
from provero.core.profiler import profile_table, suggest_checks


@pytest.fixture
def orders_connection():
    connector = DuckDBConnector()
    conn = connector.connect()
    conn._conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id VARCHAR,
            amount DECIMAL(10,2),
            status VARCHAR,
            email VARCHAR
        )
    """)
    conn._conn.execute("""
        INSERT INTO orders VALUES
        (1, 'C001', 150.00, 'delivered', 'a@test.com'),
        (2, 'C002', 89.99, 'shipped', 'b@test.com'),
        (3, 'C003', 220.50, 'pending', NULL),
        (4, 'C004', 45.00, 'delivered', 'd@test.com'),
        (5, 'C005', 999.99, 'cancelled', 'e@test.com')
    """)
    yield conn
    connector.disconnect(conn)


class TestProfileTable:
    def test_row_count(self, orders_connection):
        profile = profile_table(orders_connection, "orders")
        assert profile.row_count == 5
        assert profile.column_count == 5

    def test_null_detection(self, orders_connection):
        profile = profile_table(orders_connection, "orders")
        email_col = next(c for c in profile.columns if c.name == "email")
        assert email_col.null_count == 1
        assert email_col.null_pct == 20.0

    def test_unique_detection(self, orders_connection):
        profile = profile_table(orders_connection, "orders")
        order_id_col = next(c for c in profile.columns if c.name == "order_id")
        assert order_id_col.distinct_count == 5
        assert order_id_col.distinct_pct == 100.0

    def test_numeric_stats(self, orders_connection):
        profile = profile_table(orders_connection, "orders")
        amount_col = next(c for c in profile.columns if c.name == "amount")
        assert amount_col.min_value is not None
        assert amount_col.max_value is not None
        assert amount_col.mean_value is not None

    def test_top_values(self, orders_connection):
        profile = profile_table(orders_connection, "orders")
        status_col = next(c for c in profile.columns if c.name == "status")
        assert len(status_col.top_values) == 4  # 4 distinct statuses
        assert status_col.top_values[0]["value"] == "delivered"  # most common


class TestSuggestChecks:
    def test_suggests_not_null(self, orders_connection):
        profile = profile_table(orders_connection, "orders")
        checks = suggest_checks(profile)

        not_null_check = next((c for c in checks if "not_null" in c), None)
        assert not_null_check is not None
        # email has nulls, so it should NOT be in the not_null list
        assert "email" not in not_null_check["not_null"]

    def test_suggests_unique(self, orders_connection):
        profile = profile_table(orders_connection, "orders")
        checks = suggest_checks(profile)

        unique_checks = [c for c in checks if "unique" in c]
        unique_cols = [c["unique"] for c in unique_checks]
        assert "order_id" in unique_cols

    def test_suggests_accepted_values(self, orders_connection):
        profile = profile_table(orders_connection, "orders")
        checks = suggest_checks(profile)

        av_checks = [c for c in checks if "accepted_values" in c]
        status_check = next(
            (c for c in av_checks if c["accepted_values"]["column"] == "status"),
            None,
        )
        assert status_check is not None
        assert "delivered" in status_check["accepted_values"]["values"]

    def test_suggests_range(self, orders_connection):
        profile = profile_table(orders_connection, "orders")
        checks = suggest_checks(profile)

        range_checks = [c for c in checks if "range" in c]
        amount_check = next(
            (c for c in range_checks if c["range"]["column"] == "amount"),
            None,
        )
        assert amount_check is not None
        assert amount_check["range"]["min"] < 45.0
        assert amount_check["range"]["max"] > 999.99

    def test_suggests_row_count(self, orders_connection):
        profile = profile_table(orders_connection, "orders")
        checks = suggest_checks(profile)

        row_count_check = next((c for c in checks if "row_count" in c), None)
        assert row_count_check is not None
        assert row_count_check["row_count"]["min"] >= 1
