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

"""Tests for check logic bug fixes."""

from __future__ import annotations

import pytest

from provero.checks.completeness import _normalize_min_completeness
from provero.checks.registry import get_check_runner
from provero.connectors.duckdb import DuckDBConnector
from provero.core.compiler import CheckConfig
from provero.core.results import Status


@pytest.fixture
def duckdb_connection():
    connector = DuckDBConnector()
    conn = connector.connect()
    conn._conn.execute("CREATE TABLE test_unique (id INTEGER, name VARCHAR)")
    conn._conn.execute(
        "INSERT INTO test_unique VALUES (1, 'alex'), (2, 'bob'), (3, NULL), (4, NULL)"
    )
    conn._conn.execute("CREATE TABLE test_completeness (id INTEGER, value VARCHAR)")
    conn._conn.execute(
        "INSERT INTO test_completeness VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, NULL)"
    )
    conn._conn.execute("CREATE TABLE test_range (id INTEGER, amount DECIMAL(10,2))")
    conn._conn.execute("INSERT INTO test_range VALUES (1, 10.00), (2, 50.00), (3, 100.00)")
    yield conn
    connector.disconnect(conn)


class TestUniqueNullHandling:
    def test_unique_with_nulls_passes(self, duckdb_connection):
        runner = get_check_runner("unique")
        result = runner(
            connection=duckdb_connection,
            table="test_unique",
            check_config=CheckConfig(check_type="unique", column="name"),
        )
        assert result.status == Status.PASS

    def test_unique_with_real_duplicates_fails(self, duckdb_connection):
        duckdb_connection._conn.execute("INSERT INTO test_unique VALUES (5, 'alex')")
        runner = get_check_runner("unique")
        result = runner(
            connection=duckdb_connection,
            table="test_unique",
            check_config=CheckConfig(check_type="unique", column="name"),
        )
        assert result.status == Status.FAIL


class TestCompletenessPercentageParsing:
    def test_normalize_string_percent(self):
        assert _normalize_min_completeness("95%") == pytest.approx(0.95)

    def test_normalize_integer_as_percentage(self):
        assert _normalize_min_completeness(95) == pytest.approx(0.95)

    def test_normalize_float_ratio(self):
        assert _normalize_min_completeness(0.95) == pytest.approx(0.95)

    def test_completeness_with_percent_string(self, duckdb_connection):
        runner = get_check_runner("completeness")
        result = runner(
            connection=duckdb_connection,
            table="test_completeness",
            check_config=CheckConfig(
                check_type="completeness",
                column="value",
                params={"min": "80%"},
            ),
        )
        assert result.status == Status.PASS

    def test_completeness_with_integer(self, duckdb_connection):
        runner = get_check_runner("completeness")
        result = runner(
            connection=duckdb_connection,
            table="test_completeness",
            check_config=CheckConfig(
                check_type="completeness",
                column="value",
                params={"min": 80},
            ),
        )
        assert result.status == Status.PASS

    def test_completeness_with_ratio(self, duckdb_connection):
        runner = get_check_runner("completeness")
        result = runner(
            connection=duckdb_connection,
            table="test_completeness",
            check_config=CheckConfig(
                check_type="completeness",
                column="value",
                params={"min": 0.80},
            ),
        )
        assert result.status == Status.PASS


class TestRangeNonNumericRejection:
    def test_rejects_string_min(self, duckdb_connection):
        runner = get_check_runner("range")
        with pytest.raises(ValueError, match="must be numeric"):
            runner(
                connection=duckdb_connection,
                table="test_range",
                check_config=CheckConfig(
                    check_type="range",
                    column="amount",
                    params={"min": "DROP TABLE test_range"},
                ),
            )

    def test_rejects_string_max(self, duckdb_connection):
        runner = get_check_runner("range")
        with pytest.raises(ValueError, match="must be numeric"):
            runner(
                connection=duckdb_connection,
                table="test_range",
                check_config=CheckConfig(
                    check_type="range",
                    column="amount",
                    params={"max": "1; DROP TABLE test_range"},
                ),
            )

    def test_accepts_numeric_string(self, duckdb_connection):
        runner = get_check_runner("range")
        result = runner(
            connection=duckdb_connection,
            table="test_range",
            check_config=CheckConfig(
                check_type="range",
                column="amount",
                params={"min": "0", "max": "1000"},
            ),
        )
        assert result.status == Status.PASS
