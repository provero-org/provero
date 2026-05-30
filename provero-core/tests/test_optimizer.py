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

"""Tests for the SQL query optimizer."""

from __future__ import annotations

import pytest

from provero.connectors.duckdb import DuckDBConnector
from provero.core.compiler import CheckConfig
from provero.core.optimizer import _safe_alias, build_batch_query, execute_batch, plan_batch
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


class TestSafeAliasInjective:
    """A11: schema-qualified / nested column names must not collide."""

    def test_dotted_vs_literal_do_not_collide(self):
        # ``a.b`` and a literal column named with the old sentinel must differ.
        assert _safe_alias("a.b") != _safe_alias("a__dot__b")

    def test_distinct_columns_distinct_aliases(self):
        names = ["a.b", "a_b", "a b", "a__dot__b", "schema.tbl.col", "json.a.b"]
        aliases = [_safe_alias(n) for n in names]
        assert len(set(aliases)) == len(names)

    def test_alias_is_valid_sql_identifier(self):
        # Only letters, digits and underscores may appear in an alias.
        alias = _safe_alias("payload.user.id")
        assert all(c.isalnum() or c == "_" for c in alias)

    def test_unicode_char_vs_dot_sequence_no_collision(self):
        # U+02EB is alnum but non-ascii; ".b" must not collapse onto it.
        assert _safe_alias("˫") != _safe_alias(".b")


class TestSchemaQualifiedNoCollision:
    """A11: nested/schema-qualified columns get distinct aliases in one query."""

    def test_dotted_columns_distinct_aliases_in_query(self):
        # ``a.b`` (nested/JSON path, e.g. BigQuery) and a real column literally
        # named ``a__dot__b`` previously aliased to the same column, so the
        # result row dict silently overwrote one with the other.
        checks = [
            CheckConfig(check_type="not_null", column="a.b"),
            CheckConfig(check_type="not_null", column="a__dot__b"),
        ]
        plan = plan_batch("t", checks)
        aliases = [m.alias for m in plan.metrics if m.check_config.check_type == "not_null"]
        assert len(aliases) == 2
        assert len(set(aliases)) == 2  # no collision
        query = build_batch_query(plan)
        # Both metrics survive into the SQL with their own alias.
        for alias in aliases:
            assert f"as {alias}" in query


class TestDuplicateCheckNotLost:
    """M2: two distinct identical checks must both yield a result."""

    def test_duplicate_unique_checks_both_emitted(self, orders_connection):
        checks = [
            CheckConfig(check_type="unique", column="customer_id", severity="critical"),
            CheckConfig(check_type="unique", column="customer_id", severity="warning"),
        ]
        plan = plan_batch("orders", checks)
        results = execute_batch(orders_connection, plan)
        unique_results = [r for r in results if r.check_name == "unique:customer_id"]
        assert len(unique_results) == 2
        severities = {str(r.severity) for r in unique_results}
        assert len(severities) == 2


class TestRowCountSemanticsParity:
    """M4/M5: row_count must exclude NULLs to match standalone checks."""

    @pytest.fixture
    def nullable_connection(self):
        connector = DuckDBConnector()
        conn = connector.connect()
        conn._conn.execute("CREATE TABLE n (val INTEGER, label VARCHAR)")
        # 5 rows; val has 1 NULL, label has 1 NULL.
        conn._conn.execute(
            "INSERT INTO n VALUES (10, 'a'), (20, 'b'), (30, 'a'), (40, NULL), (NULL, 'b')"
        )
        yield conn
        connector.disconnect(conn)

    def test_range_row_count_excludes_nulls(self, nullable_connection):
        checks = [CheckConfig(check_type="range", column="val", params={"min": 0, "max": 100})]
        plan = plan_batch("n", checks)
        results = execute_batch(nullable_connection, plan)
        r = next(x for x in results if x.check_type == "range")
        # 4 non-null vals, matching standalone WHERE val IS NOT NULL.
        assert r.row_count == 4

    def test_accepted_values_row_count_excludes_nulls(self, nullable_connection):
        checks = [
            CheckConfig(
                check_type="accepted_values",
                column="label",
                params={"values": ["a", "b"]},
            )
        ]
        plan = plan_batch("n", checks)
        results = execute_batch(nullable_connection, plan)
        r = next(x for x in results if x.check_type == "accepted_values")
        # 4 non-null labels.
        assert r.row_count == 4


class TestBatchDrillDownQueries:
    """M3: range and accepted_values populate failing_rows_query in batch."""

    def test_range_failing_rows_query_populated(self, orders_connection):
        checks = [CheckConfig(check_type="range", column="amount", params={"min": 0, "max": 1000})]
        plan = plan_batch("orders", checks)
        results = execute_batch(orders_connection, plan)
        r = next(x for x in results if x.check_type == "range")
        assert r.status == Status.FAIL
        assert r.failing_rows_query
        assert "WHERE" in r.failing_rows_query

    def test_accepted_values_failing_rows_query_populated(self, orders_connection):
        checks = [
            CheckConfig(
                check_type="accepted_values",
                column="status",
                params={"values": ["delivered"]},  # most rows are invalid
            )
        ]
        plan = plan_batch("orders", checks)
        results = execute_batch(orders_connection, plan)
        r = next(x for x in results if x.check_type == "accepted_values")
        assert r.status == Status.FAIL
        assert r.failing_rows_query
        assert "NOT IN" in r.failing_rows_query
