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

"""Tests for the SQL query optimizer."""

from __future__ import annotations

import pytest

from provero.connectors.duckdb import DuckDBConnector
from provero.core.compiler import CheckConfig
from provero.core.optimizer import build_batch_query, execute_batch, plan_batch
from provero.core.results import Status


@pytest.fixture
def orders_connection():
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
        (5, 'C004', -10.00, 'cancelled')
    """)
    yield conn
    connector.disconnect(conn)


class TestPlanBatch:
    def test_batchable_checks_grouped(self):
        checks = [
            CheckConfig(check_type="not_null", columns=["order_id", "amount"]),
            CheckConfig(check_type="unique", column="order_id"),
            CheckConfig(check_type="row_count", params={"min": 1}),
        ]
        plan = plan_batch("orders", checks)
        assert len(plan.metrics) > 0
        assert len(plan.non_batchable) == 0

    def test_non_batchable_separated(self):
        checks = [
            CheckConfig(check_type="not_null", column="order_id"),
            CheckConfig(check_type="custom_sql", params={"query": "SELECT 1"}),
            CheckConfig(check_type="freshness", column="created_at", params={"max_age": "24h"}),
        ]
        plan = plan_batch("orders", checks)
        assert len(plan.non_batchable) == 2  # custom_sql + freshness

    def test_empty_checks(self):
        plan = plan_batch("orders", [])
        assert len(plan.metrics) == 0
        assert len(plan.non_batchable) == 0


class TestBuildBatchQuery:
    def test_single_query_multiple_checks(self):
        checks = [
            CheckConfig(check_type="not_null", columns=["order_id", "amount"]),
            CheckConfig(check_type="unique", column="order_id"),
            CheckConfig(check_type="row_count", params={"min": 1}),
        ]
        plan = plan_batch("orders", checks)
        query = build_batch_query(plan)

        assert "SELECT" in query
        assert 'FROM "orders"' in query
        # Should be one query, not multiple
        assert query.count("FROM") == 1

    def test_empty_plan_returns_empty(self):
        plan = plan_batch("orders", [])
        query = build_batch_query(plan)
        assert query == ""


class TestExecuteBatch:
    def test_all_pass(self, orders_connection):
        checks = [
            CheckConfig(check_type="not_null", columns=["order_id", "customer_id"]),
            CheckConfig(check_type="unique", column="order_id"),
            CheckConfig(check_type="row_count", params={"min": 1, "max": 100}),
        ]
        plan = plan_batch("orders", checks)
        results = execute_batch(orders_connection, plan)

        assert len(results) >= 3
        statuses = {r.check_name: r.status for r in results}
        assert statuses["not_null:order_id"] == Status.PASS
        assert statuses["not_null:customer_id"] == Status.PASS
        assert statuses["unique:order_id"] == Status.PASS
        assert statuses["row_count"] == Status.PASS

    def test_detects_failures(self, orders_connection):
        checks = [
            CheckConfig(check_type="unique", column="customer_id"),  # C001 is duplicated
            CheckConfig(
                check_type="range",
                column="amount",
                params={"min": 0, "max": 1000},
            ),  # -10 is out
        ]
        plan = plan_batch("orders", checks)
        results = execute_batch(orders_connection, plan)

        statuses = {r.check_name: r.status for r in results}
        assert statuses["unique:customer_id"] == Status.FAIL
        assert statuses["range:amount"] == Status.FAIL

    def test_accepted_values(self, orders_connection):
        checks = [
            CheckConfig(
                check_type="accepted_values",
                column="status",
                params={"values": ["pending", "shipped", "delivered", "cancelled"]},
            ),
        ]
        plan = plan_batch("orders", checks)
        results = execute_batch(orders_connection, plan)
        assert results[0].status == Status.PASS

    def test_single_query_efficiency(self, orders_connection):
        """Verify that 5 checks result in 1 query, not 5."""
        checks = [
            CheckConfig(check_type="not_null", columns=["order_id", "amount"]),
            CheckConfig(check_type="unique", column="order_id"),
            CheckConfig(check_type="range", column="amount", params={"min": 0, "max": 10000}),
            CheckConfig(check_type="row_count", params={"min": 1}),
            CheckConfig(
                check_type="accepted_values",
                column="status",
                params={"values": ["pending", "shipped", "delivered", "cancelled"]},
            ),
        ]
        plan = plan_batch("orders", checks)
        query = build_batch_query(plan)
        # One SELECT, one FROM
        assert query.count("FROM") == 1
        # All results should be present
        results = execute_batch(orders_connection, plan)
        assert len(results) >= 5
