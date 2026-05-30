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

"""Known-answer tests for enterprise statistical checks.

Each expected statistic (PSI, mean, stddev, cardinality, parity) is computed by
hand below and hard-coded, so the test pins the implementation to an exact
numeric contract rather than re-deriving it from the same code under test.
"""

from __future__ import annotations

import math

import pytest

from provero.checks.cardinality import check_cardinality, compute_cardinality
from provero.checks.cross_table import check_cross_table_count
from provero.checks.distribution import check_distribution, compute_distribution_stats
from provero.checks.drift import check_drift, compute_psi
from provero.connectors.duckdb import DuckDBConnector
from provero.core.compiler import CheckConfig
from provero.core.results import Status

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conn():
    """DuckDB connection with hand-crafted, NULL-free statistical fixtures."""
    connector = DuckDBConnector()
    connection = connector.connect()

    # nums: 8 rows. values = [2, 4, 4, 4, 5, 5, 7, 9]
    #   sum = 40 -> population mean = 40 / 8 = 5.0
    #   squared deviations from 5: 9,1,1,1,0,0,4,16 -> sum = 32
    #   population variance = 32 / 8 = 4.0 -> population stddev = 2.0
    #   category column: 4 distinct values over 8 non-null rows -> ratio = 0.5
    connection._conn.execute("CREATE TABLE nums (val INTEGER, cat VARCHAR)")
    connection._conn.execute(
        "INSERT INTO nums VALUES (2,'a'),(4,'a'),(4,'b'),(4,'b'),(5,'c'),(5,'c'),(7,'d'),(9,'d')"
    )

    # drift_data: 100 rows over categories A/B/C with counts 60/30/10.
    connection._conn.execute("CREATE TABLE drift_data (grp VARCHAR)")
    connection._conn.execute(
        "INSERT INTO drift_data SELECT 'A' FROM range(60) "
        "UNION ALL SELECT 'B' FROM range(30) "
        "UNION ALL SELECT 'C' FROM range(10)"
    )

    # parity tables: left has 5 rows, right has 5 rows (equal).
    connection._conn.execute("CREATE TABLE left_t (id INTEGER)")
    connection._conn.execute("INSERT INTO left_t SELECT * FROM range(5)")
    connection._conn.execute("CREATE TABLE right_t (id INTEGER)")
    connection._conn.execute("INSERT INTO right_t SELECT * FROM range(5)")
    # mismatch table: 3 rows.
    connection._conn.execute("CREATE TABLE small_t (id INTEGER)")
    connection._conn.execute("INSERT INTO small_t SELECT * FROM range(3)")

    yield connection
    connector.disconnect(connection)


# ---------------------------------------------------------------------------
# PSI / drift
# ---------------------------------------------------------------------------

# Hand computation:
#   baseline = {A: 0.5, B: 0.3, C: 0.1}
#   current counts 60/30/10 over total 100 -> cur = {A: 0.6, B: 0.3, C: 0.1}
#   Only bin A differs; B and C match exactly so their terms are 0.
#   PSI = (0.6-0.5)*ln(0.6/0.5) + (0.3-0.3)*ln(1) + (0.1-0.1)*ln(1)
#       = 0.1 * ln(1.2) + 0 + 0
#       = 0.1 * 0.18232155679395463 = 0.018232155679395464
EXPECTED_PSI = 0.1 * math.log(1.2)


def test_compute_psi_known_answer() -> None:
    current = {"A": 60, "B": 30, "C": 10}
    baseline = {"A": 0.5, "B": 0.3, "C": 0.1}
    psi = compute_psi(current, baseline)
    assert psi == pytest.approx(0.018232155679395464, abs=1e-12)
    assert psi == pytest.approx(EXPECTED_PSI, abs=1e-12)


def test_compute_psi_identical_is_zero() -> None:
    # Current proportions exactly equal baseline -> PSI must be 0.
    current = {"A": 50, "B": 30, "C": 20}
    baseline = {"A": 0.5, "B": 0.3, "C": 0.2}
    assert compute_psi(current, baseline) == pytest.approx(0.0, abs=1e-12)


def test_compute_psi_zero_bin_uses_epsilon() -> None:
    # Baseline has a category 'D' the current data lacks; epsilon floor keeps
    # the term finite (no log(0)/div0). This exercises the epsilon path
    # separately from the load-bearing exact value above.
    current = {"A": 60, "B": 30, "C": 10}
    baseline = {"A": 0.5, "B": 0.3, "C": 0.1, "D": 0.1}
    psi = compute_psi(current, baseline, epsilon=1e-6)
    assert math.isfinite(psi)
    assert psi > 0.0


def test_check_drift_pass(conn) -> None:
    cfg = CheckConfig(
        check_type="drift",
        column="grp",
        params={"baseline": {"A": 0.5, "B": 0.3, "C": 0.1}, "threshold": 0.25},
    )
    result = check_drift(conn, "drift_data", cfg)
    assert result.status == Status.PASS
    assert result.observed_value == f"PSI={EXPECTED_PSI:.4f}"
    assert result.row_count == 100


def test_check_drift_fail_on_large_shift(conn) -> None:
    # Baseline far from observed -> PSI well above 0.25 -> FAIL.
    cfg = CheckConfig(
        check_type="drift",
        column="grp",
        params={"baseline": {"A": 0.1, "B": 0.1, "C": 0.8}, "threshold": 0.25},
    )
    result = check_drift(conn, "drift_data", cfg)
    assert result.status == Status.FAIL


def test_check_drift_missing_baseline(conn) -> None:
    cfg = CheckConfig(check_type="drift", column="grp", params={})
    result = check_drift(conn, "drift_data", cfg)
    assert result.status == Status.ERROR


# ---------------------------------------------------------------------------
# distribution (mean / stddev)
# ---------------------------------------------------------------------------


def test_compute_distribution_stats_known_answer(conn) -> None:
    stats = compute_distribution_stats(conn, "nums", "val")
    assert stats["mean"] == pytest.approx(5.0, abs=1e-9)
    # population stddev (denominator n=8) of [2,4,4,4,5,5,7,9] = 2.0
    assert stats["stddev"] == pytest.approx(2.0, abs=1e-9)
    assert stats["count"] == 8


def test_check_distribution_mean_pass(conn) -> None:
    cfg = CheckConfig(
        check_type="distribution",
        column="val",
        params={"mean": 5.0, "mean_tolerance": 0.01},
    )
    result = check_distribution(conn, "nums", cfg)
    assert result.status == Status.PASS


def test_check_distribution_stddev_bounds_pass(conn) -> None:
    cfg = CheckConfig(
        check_type="distribution",
        column="val",
        params={"stddev_min": 1.5, "stddev_max": 2.5},
    )
    result = check_distribution(conn, "nums", cfg)
    assert result.status == Status.PASS


def test_check_distribution_mean_fail(conn) -> None:
    cfg = CheckConfig(
        check_type="distribution",
        column="val",
        params={"mean": 10.0, "mean_tolerance": 0.5},
    )
    result = check_distribution(conn, "nums", cfg)
    assert result.status == Status.FAIL


def test_check_distribution_no_constraint_errors(conn) -> None:
    cfg = CheckConfig(check_type="distribution", column="val", params={})
    result = check_distribution(conn, "nums", cfg)
    assert result.status == Status.ERROR


# ---------------------------------------------------------------------------
# cardinality
# ---------------------------------------------------------------------------


def test_compute_cardinality_known_answer(conn) -> None:
    stats = compute_cardinality(conn, "nums", "cat")
    # cat has 4 distinct values (a,b,c,d) over 8 non-null rows.
    assert stats["distinct"] == 4
    assert stats["non_null"] == 8
    assert stats["ratio"] == pytest.approx(0.5, abs=1e-9)


def test_check_cardinality_count_pass(conn) -> None:
    cfg = CheckConfig(
        check_type="cardinality",
        column="cat",
        params={"min": 3, "max": 5},
    )
    result = check_cardinality(conn, "nums", cfg)
    assert result.status == Status.PASS


def test_check_cardinality_ratio_pass(conn) -> None:
    cfg = CheckConfig(
        check_type="cardinality",
        column="cat",
        params={"min_ratio": 0.4, "max_ratio": 0.6},
    )
    result = check_cardinality(conn, "nums", cfg)
    assert result.status == Status.PASS


def test_check_cardinality_count_fail(conn) -> None:
    cfg = CheckConfig(
        check_type="cardinality",
        column="cat",
        params={"min": 5},
    )
    result = check_cardinality(conn, "nums", cfg)
    assert result.status == Status.FAIL


def test_check_cardinality_no_constraint_errors(conn) -> None:
    cfg = CheckConfig(check_type="cardinality", column="cat", params={})
    result = check_cardinality(conn, "nums", cfg)
    assert result.status == Status.ERROR


# ---------------------------------------------------------------------------
# cross_table_count
# ---------------------------------------------------------------------------


def test_cross_table_parity_pass(conn) -> None:
    cfg = CheckConfig(
        check_type="cross_table_count",
        params={"other_table": "right_t"},
    )
    result = check_cross_table_count(conn, "left_t", cfg)
    assert result.status == Status.PASS
    assert result.observed_value == "5 vs 5 (diff=0)"


def test_cross_table_parity_fail(conn) -> None:
    cfg = CheckConfig(
        check_type="cross_table_count",
        params={"other_table": "small_t"},
    )
    result = check_cross_table_count(conn, "left_t", cfg)
    assert result.status == Status.FAIL
    assert result.observed_value == "5 vs 3 (diff=2)"


def test_cross_table_parity_within_tolerance(conn) -> None:
    cfg = CheckConfig(
        check_type="cross_table_count",
        params={"other_table": "small_t", "tolerance": 2},
    )
    result = check_cross_table_count(conn, "left_t", cfg)
    assert result.status == Status.PASS


def test_cross_table_ratio_pass(conn) -> None:
    # 5 / 3 = 1.6667 within [1.0, 2.0]
    cfg = CheckConfig(
        check_type="cross_table_count",
        params={"other_table": "small_t", "mode": "ratio", "min_ratio": 1.0, "max_ratio": 2.0},
    )
    result = check_cross_table_count(conn, "left_t", cfg)
    assert result.status == Status.PASS


def test_cross_table_missing_other(conn) -> None:
    cfg = CheckConfig(check_type="cross_table_count", params={})
    result = check_cross_table_count(conn, "left_t", cfg)
    assert result.status == Status.ERROR
