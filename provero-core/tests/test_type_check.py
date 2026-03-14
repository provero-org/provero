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

"""Tests for the type check."""

from __future__ import annotations

from provero.checks.registry import get_check_runner
from provero.core.compiler import CheckConfig
from provero.core.results import Status


class TestTypeCheck:
    def test_integer_pass(self, duckdb_orders):
        runner = get_check_runner("type")
        result = runner(
            connection=duckdb_orders,
            table="orders",
            check_config=CheckConfig(
                check_type="type",
                column="order_id",
                params={"expected": "integer"},
            ),
        )
        assert result.status == Status.PASS

    def test_string_pass(self, duckdb_orders):
        runner = get_check_runner("type")
        result = runner(
            connection=duckdb_orders,
            table="orders",
            check_config=CheckConfig(
                check_type="type",
                column="customer_id",
                params={"expected": "string"},
            ),
        )
        assert result.status == Status.PASS

    def test_type_mismatch_fail(self, duckdb_orders):
        runner = get_check_runner("type")
        result = runner(
            connection=duckdb_orders,
            table="orders",
            check_config=CheckConfig(
                check_type="type",
                column="order_id",
                params={"expected": "string"},
            ),
        )
        assert result.status == Status.FAIL

    def test_column_not_found(self, duckdb_orders):
        runner = get_check_runner("type")
        result = runner(
            connection=duckdb_orders,
            table="orders",
            check_config=CheckConfig(
                check_type="type",
                column="nonexistent",
                params={"expected": "integer"},
            ),
        )
        assert result.status == Status.ERROR

    def test_decimal_as_float(self, duckdb_orders):
        runner = get_check_runner("type")
        result = runner(
            connection=duckdb_orders,
            table="orders",
            check_config=CheckConfig(
                check_type="type",
                column="amount",
                params={"expected": "float"},
            ),
        )
        assert result.status == Status.PASS

    def test_timestamp_type(self, duckdb_orders):
        runner = get_check_runner("type")
        result = runner(
            connection=duckdb_orders,
            table="events",
            check_config=CheckConfig(
                check_type="type",
                column="created_at",
                params={"expected": "timestamp"},
            ),
        )
        assert result.status == Status.PASS
