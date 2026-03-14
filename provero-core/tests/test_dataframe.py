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

"""Tests for the DataFrame connector."""

from __future__ import annotations

import pytest

from provero.checks.registry import get_check_runner
from provero.connectors.dataframe import DataFrameConnector
from provero.core.compiler import CheckConfig
from provero.core.results import Status

pandas = pytest.importorskip("pandas")


@pytest.fixture
def df_connection():
    """Create a DataFrame connector with test data."""
    df = pandas.DataFrame(
        {
            "order_id": [1, 2, 3, 4, 5],
            "customer_id": ["C001", "C002", "C003", "C001", "C004"],
            "amount": [150.00, 89.99, 220.50, 45.00, 999.99],
            "status": ["delivered", "shipped", "pending", "delivered", "cancelled"],
        }
    )
    connector = DataFrameConnector(df, table_name="orders")
    conn = connector.connect()
    yield conn
    connector.disconnect(conn)


class TestDataFrameConnector:
    def test_execute_query(self, df_connection):
        result = df_connection.execute("SELECT COUNT(*) as cnt FROM orders")
        assert result[0]["cnt"] == 5

    def test_get_columns(self, df_connection):
        cols = df_connection.get_columns("orders")
        col_names = [c["name"] for c in cols]
        assert "order_id" in col_names
        assert "customer_id" in col_names
        assert "amount" in col_names

    def test_not_null_check(self, df_connection):
        runner = get_check_runner("not_null")
        result = runner(
            connection=df_connection,
            table="orders",
            check_config=CheckConfig(check_type="not_null", column="order_id"),
        )
        assert result.status == Status.PASS

    def test_unique_check(self, df_connection):
        runner = get_check_runner("unique")
        result = runner(
            connection=df_connection,
            table="orders",
            check_config=CheckConfig(check_type="unique", column="order_id"),
        )
        assert result.status == Status.PASS

    def test_unique_check_fail(self, df_connection):
        runner = get_check_runner("unique")
        result = runner(
            connection=df_connection,
            table="orders",
            check_config=CheckConfig(check_type="unique", column="customer_id"),
        )
        assert result.status == Status.FAIL

    def test_row_count_check(self, df_connection):
        runner = get_check_runner("row_count")
        result = runner(
            connection=df_connection,
            table="orders",
            check_config=CheckConfig(check_type="row_count", params={"min": 1, "max": 100}),
        )
        assert result.status == Status.PASS

    def test_range_check(self, df_connection):
        runner = get_check_runner("range")
        result = runner(
            connection=df_connection,
            table="orders",
            check_config=CheckConfig(
                check_type="range", column="amount", params={"min": 0, "max": 10000}
            ),
        )
        assert result.status == Status.PASS


class TestDataFrameWithNulls:
    def test_not_null_detects_nulls(self):
        df = pandas.DataFrame(
            {
                "id": [1, 2, None, 4],
                "name": ["a", "b", "c", "d"],
            }
        )
        connector = DataFrameConnector(df, table_name="t")
        conn = connector.connect()
        runner = get_check_runner("not_null")
        result = runner(
            connection=conn,
            table="t",
            check_config=CheckConfig(check_type="not_null", column="id"),
        )
        assert result.status == Status.FAIL
        assert result.failing_rows == 1
        connector.disconnect(conn)
