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

"""Tests for the check execution engine."""

from __future__ import annotations

import pytest

from provero.connectors.duckdb import DuckDBConnection, DuckDBConnector
from provero.core.compiler import CheckConfig, SourceConfig, SuiteConfig
from provero.core.engine import run_suite
from provero.core.results import Severity, Status


class _SharedDuckDBConnector:
    """A connector that always returns the same connection (for testing)."""

    def __init__(self, conn: DuckDBConnection) -> None:
        self._conn = conn

    def connect(self) -> DuckDBConnection:
        return self._conn

    def disconnect(self, connection: DuckDBConnection) -> None:
        pass  # Don't close, fixture manages lifecycle


@pytest.fixture
def orders_connector():
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
    yield _SharedDuckDBConnector(conn)
    connector.disconnect(conn)


def _make_suite(checks: list[CheckConfig], table: str = "orders") -> SuiteConfig:
    return SuiteConfig(
        name="test_suite",
        source=SourceConfig(type="duckdb", table=table),
        checks=checks,
    )


class TestRunSuiteOptimized:
    def test_all_pass(self, orders_connector):
        suite = _make_suite(
            [
                CheckConfig(check_type="not_null", columns=["order_id", "customer_id"]),
                CheckConfig(check_type="unique", column="order_id"),
                CheckConfig(check_type="row_count", params={"min": 1, "max": 100}),
            ]
        )
        result = run_suite(suite, orders_connector, optimize=True)

        assert result.status == Status.PASS
        assert result.total >= 3
        assert result.passed == result.total
        assert result.failed == 0
        assert result.quality_score == 100.0

    def test_mixed_pass_fail(self, orders_connector):
        suite = _make_suite(
            [
                CheckConfig(check_type="not_null", column="order_id"),
                CheckConfig(check_type="unique", column="customer_id"),  # C001 duplicated
            ]
        )
        result = run_suite(suite, orders_connector, optimize=True)

        assert result.status == Status.FAIL
        assert result.passed >= 1
        assert result.failed >= 1
        assert 0 < result.quality_score < 100

    def test_run_id_consistent_across_checks(self, orders_connector):
        suite = _make_suite(
            [
                CheckConfig(check_type="not_null", column="order_id"),
                CheckConfig(check_type="row_count", params={"min": 1}),
            ]
        )
        result = run_suite(suite, orders_connector)
        run_ids = {c.run_id for c in result.checks}
        assert len(run_ids) == 1
        assert "" not in run_ids

    def test_duration_tracked(self, orders_connector):
        suite = _make_suite(
            [
                CheckConfig(check_type="row_count", params={"min": 1}),
            ]
        )
        result = run_suite(suite, orders_connector)
        assert result.duration_ms >= 0

    def test_severity_propagated_through_optimizer(self, orders_connector):
        suite = _make_suite(
            [
                CheckConfig(check_type="not_null", column="order_id", severity="warning"),
            ]
        )
        result = run_suite(suite, orders_connector, optimize=True)
        assert result.checks[0].severity == Severity.WARNING


class TestRunSuiteUnoptimized:
    def test_all_pass_no_optimize(self, orders_connector):
        suite = _make_suite(
            [
                CheckConfig(check_type="not_null", column="order_id"),
                CheckConfig(check_type="unique", column="order_id"),
                CheckConfig(check_type="row_count", params={"min": 1}),
            ]
        )
        result = run_suite(suite, orders_connector, optimize=False)

        assert result.status == Status.PASS
        assert result.total == 3
        assert result.passed == 3

    def test_non_batchable_checks(self, orders_connector):
        suite = _make_suite(
            [
                CheckConfig(
                    check_type="custom_sql",
                    params={
                        "name": "positive_amounts",
                        "query": "SELECT COUNT(*) = 0 FROM orders WHERE amount < 0",
                    },
                ),
            ]
        )
        result = run_suite(suite, orders_connector, optimize=True)
        assert result.total == 1
        assert result.checks[0].check_type == "custom_sql"
        assert result.checks[0].status == Status.PASS


class TestRunSuiteErrorHandling:
    def test_unknown_check_type_lists_available(self, orders_connector):
        suite = _make_suite(
            [
                CheckConfig(check_type="nonexistent_check", column="order_id"),
            ]
        )
        result = run_suite(suite, orders_connector, optimize=False)

        assert result.total == 1
        assert result.checks[0].status == Status.ERROR
        assert "nonexistent_check" in result.checks[0].observed_value
        assert "Available types:" in result.checks[0].observed_value

    def test_missing_table_error(self, orders_connector):
        suite = _make_suite(
            [CheckConfig(check_type="row_count", params={"min": 1})],
            table="table_that_does_not_exist",
        )
        result = run_suite(suite, orders_connector, optimize=True)

        assert result.status == Status.FAIL or result.status == Status.PASS
        # Should have an error in checks
        error_checks = [c for c in result.checks if c.status == Status.ERROR]
        assert len(error_checks) >= 1

    def test_error_does_not_crash_other_checks(self, orders_connector):
        suite = _make_suite(
            [
                CheckConfig(check_type="not_null", column="order_id"),
                CheckConfig(
                    check_type="custom_sql",
                    params={"name": "bad_query", "query": "THIS IS NOT SQL"},
                ),
                CheckConfig(check_type="row_count", params={"min": 1}),
            ]
        )
        result = run_suite(suite, orders_connector, optimize=True)

        # Should have at least the batchable checks + the error
        assert result.total >= 2
        statuses = [c.status for c in result.checks]
        assert Status.PASS in statuses
        assert Status.ERROR in statuses


class TestRunSuiteMixedBatchAndIndividual:
    def test_batch_plus_individual(self, orders_connector):
        """Batchable and non-batchable checks both execute correctly."""
        suite = _make_suite(
            [
                CheckConfig(check_type="not_null", column="order_id"),
                CheckConfig(check_type="unique", column="order_id"),
                CheckConfig(check_type="row_count", params={"min": 1}),
                CheckConfig(
                    check_type="custom_sql",
                    params={
                        "name": "no_neg",
                        "query": "SELECT COUNT(*) = 0 FROM orders WHERE amount < 0",
                    },
                ),
            ]
        )
        result = run_suite(suite, orders_connector, optimize=True)

        check_types = {c.check_type for c in result.checks}
        assert "not_null" in check_types
        assert "unique" in check_types
        assert "row_count" in check_types
        assert "custom_sql" in check_types
        assert result.status == Status.PASS


class _DisconnectRaisesConnector:
    """Wraps a connector but raises in disconnect (parallel cleanup failure).

    Reproduces M1: in parallel mode the per-thread connection cleanup runs in
    the ``finally`` of ``_run_single_check``. If ``disconnect`` raises, the
    exception used to propagate out of ``future.result()`` and abort the whole
    executor loop, discarding every other result.
    """

    def __init__(self, inner) -> None:
        self._inner = inner
        self._main_connection = None

    def connect(self):
        conn = self._inner.connect()
        if self._main_connection is None:
            # The first connect() is the suite-level connection used by the
            # outer run_suite try/finally; leave its disconnect alone.
            self._main_connection = conn
        return conn

    def disconnect(self, connection) -> None:
        if connection is self._main_connection:
            self._inner.disconnect(connection)
            return
        # Per-thread connection cleanup raises (reproduces M1).
        raise RuntimeError("simulated cleanup failure on disconnect")


class TestParallelDisconnectFailure:
    def test_disconnect_raise_does_not_abort_parallel_run(self, duckdb_file):
        # Use a file-based connector so each connect() yields a *distinct*
        # connection (parallel mode opens one per thread), letting us spare the
        # main connection's disconnect while failing the per-thread ones.
        inner = DuckDBConnector(database=str(duckdb_file))
        # Two non-batchable checks so the parallel branch (len(runnable) > 1)
        # is taken with optimize disabled.
        suite = _make_suite(
            [
                CheckConfig(
                    check_type="custom_sql",
                    params={
                        "name": "c1",
                        "query": "SELECT COUNT(*) >= 0 FROM orders",
                    },
                ),
                CheckConfig(
                    check_type="custom_sql",
                    params={
                        "name": "c2",
                        "query": "SELECT COUNT(*) >= 0 FROM orders",
                    },
                ),
            ]
        )
        connector = _DisconnectRaisesConnector(inner)

        # Must not raise, and must return a result for every check.
        result = run_suite(suite, connector, optimize=False, parallel=True)

        assert result.total == 2
        # Each check becomes an ERROR result carrying the cleanup failure.
        assert result.errored == 2
        for c in result.checks:
            assert c.status == Status.ERROR
            assert "simulated cleanup failure" in str(c.observed_value)


class TestDescriptionPropagation:
    """description: declared on a check flows through to CheckResult."""

    def test_description_on_individual_check(self, orders_connector):
        suite = _make_suite(
            [
                CheckConfig(
                    check_type="range",
                    column="amount",
                    params={"min": 0, "max": 10000},
                    description="Order amount must be in [0, 10000]",
                ),
            ]
        )
        # optimize=False forces the per-check path through _run_single_check.
        result = run_suite(suite, orders_connector, optimize=False)
        assert len(result.checks) == 1
        assert result.checks[0].description == "Order amount must be in [0, 10000]"

    def test_description_on_batched_not_null(self, orders_connector):
        suite = _make_suite(
            [
                CheckConfig(
                    check_type="not_null",
                    columns=["order_id", "customer_id"],
                    description="Required identity fields",
                ),
            ]
        )
        # optimize=True routes through plan_batch + execute_batch, exercising
        # both the per-column expansion (preserving description) and the
        # batched-result decoration loop in _run_suite_inner.
        result = run_suite(suite, orders_connector, optimize=True)
        assert len(result.checks) == 2  # one per column
        for c in result.checks:
            assert c.description == "Required identity fields"

    def test_description_on_custom_sql(self, orders_connector):
        suite = _make_suite(
            [
                CheckConfig(
                    check_type="custom_sql",
                    params={
                        "name": "all_amounts_positive",
                        "query": "SELECT MIN(amount) > 0 FROM orders",
                    },
                    description="All order amounts must be positive",
                ),
            ]
        )
        result = run_suite(suite, orders_connector, optimize=False)
        assert result.checks[0].description == "All order amounts must be positive"
