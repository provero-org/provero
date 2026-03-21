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

"""Tests for the dbt schema.yml exporter."""

from __future__ import annotations

import yaml

from provero.core.compiler import CheckConfig, ProveroConfig, SourceConfig, SuiteConfig
from provero.exporters.dbt import export_config, export_suite


def _make_suite(name: str, checks: list[CheckConfig]) -> SuiteConfig:
    return SuiteConfig(
        name=name,
        source=SourceConfig(type="duckdb", table=name),
        checks=checks,
    )


def _make_config(suites: list[SuiteConfig]) -> ProveroConfig:
    return ProveroConfig(suites=suites)


class TestNotNullMapping:
    def test_single_column(self):
        suite = _make_suite("orders", [CheckConfig(check_type="not_null", column="order_id")])
        model, _comments = export_suite(suite)
        assert model["name"] == "orders"
        assert len(model["columns"]) == 1
        assert model["columns"][0]["name"] == "order_id"
        assert "not_null" in model["columns"][0]["tests"]

    def test_multi_column(self):
        suite = _make_suite(
            "orders",
            [CheckConfig(check_type="not_null", columns=["order_id", "customer_id", "amount"])],
        )
        model, _comments = export_suite(suite)
        col_names = [c["name"] for c in model["columns"]]
        assert "order_id" in col_names
        assert "customer_id" in col_names
        assert "amount" in col_names
        for col in model["columns"]:
            assert "not_null" in col["tests"]


class TestUniqueMapping:
    def test_single_column(self):
        suite = _make_suite("orders", [CheckConfig(check_type="unique", column="order_id")])
        model, _comments = export_suite(suite)
        assert model["columns"][0]["name"] == "order_id"
        assert "unique" in model["columns"][0]["tests"]

    def test_multi_column(self):
        suite = _make_suite("orders", [CheckConfig(check_type="unique", columns=["id", "email"])])
        model, _comments = export_suite(suite)
        col_names = [c["name"] for c in model["columns"]]
        assert "id" in col_names
        assert "email" in col_names


class TestAcceptedValuesMapping:
    def test_accepted_values(self):
        suite = _make_suite(
            "orders",
            [
                CheckConfig(
                    check_type="accepted_values",
                    column="status",
                    params={"values": ["pending", "shipped", "delivered"]},
                )
            ],
        )
        model, _comments = export_suite(suite)
        assert model["columns"][0]["name"] == "status"
        test = model["columns"][0]["tests"][0]
        assert isinstance(test, dict)
        assert "accepted_values" in test
        assert test["accepted_values"]["values"] == ["pending", "shipped", "delivered"]


class TestRangeMapping:
    def test_range_min_max(self):
        suite = _make_suite(
            "orders",
            [
                CheckConfig(
                    check_type="range",
                    column="amount",
                    params={"min": 0, "max": 100000},
                )
            ],
        )
        model, _comments = export_suite(suite)
        assert model["columns"][0]["name"] == "amount"
        test = model["columns"][0]["tests"][0]
        assert "dbt_utils.expression_is_true" in test
        expr = test["dbt_utils.expression_is_true"]["expression"]
        assert ">= 0" in expr
        assert "<= 100000" in expr

    def test_range_min_only(self):
        suite = _make_suite(
            "orders",
            [CheckConfig(check_type="range", column="amount", params={"min": 0})],
        )
        model, _comments = export_suite(suite)
        test = model["columns"][0]["tests"][0]
        assert test["dbt_utils.expression_is_true"]["expression"] == ">= 0"

    def test_range_max_only(self):
        suite = _make_suite(
            "orders",
            [CheckConfig(check_type="range", column="price", params={"max": 999})],
        )
        model, _comments = export_suite(suite)
        test = model["columns"][0]["tests"][0]
        assert test["dbt_utils.expression_is_true"]["expression"] == "<= 999"


class TestUnmappableChecks:
    def test_row_count_produces_comment(self):
        suite = _make_suite(
            "orders",
            [CheckConfig(check_type="row_count", params={"min": 1})],
        )
        model, comments = export_suite(suite)
        assert "columns" not in model
        assert any("row_count" in c and "no direct dbt equivalent" in c for c in comments)

    def test_freshness_produces_comment(self):
        suite = _make_suite(
            "orders",
            [
                CheckConfig(
                    check_type="freshness",
                    column="order_date",
                    params={"max_age": "24h"},
                )
            ],
        )
        _model, comments = export_suite(suite)
        assert any("freshness" in c for c in comments)

    def test_regex_produces_comment(self):
        suite = _make_suite(
            "users",
            [
                CheckConfig(
                    check_type="regex",
                    column="email",
                    params={"pattern": "^[^@]+@[^@]+$"},
                )
            ],
        )
        _model, comments = export_suite(suite)
        assert any("regex" in c for c in comments)

    def test_unknown_check_produces_comment(self):
        suite = _make_suite(
            "orders",
            [CheckConfig(check_type="some_future_check", column="col")],
        )
        _model, comments = export_suite(suite)
        assert any("some_future_check" in c for c in comments)


class TestFullRoundtrip:
    def test_quickstart_roundtrip(self):
        """Test that a typical provero.yaml converts to valid dbt schema.yml."""
        config = _make_config(
            [
                _make_suite(
                    "orders",
                    [
                        CheckConfig(
                            check_type="not_null",
                            columns=["order_id", "customer_id", "amount"],
                        ),
                        CheckConfig(check_type="unique", column="order_id"),
                        CheckConfig(
                            check_type="accepted_values",
                            column="status",
                            params={"values": ["pending", "shipped", "delivered", "cancelled"]},
                        ),
                        CheckConfig(
                            check_type="range",
                            column="amount",
                            params={"min": 0, "max": 100000},
                        ),
                        CheckConfig(check_type="row_count", params={"min": 1}),
                    ],
                )
            ]
        )

        result = export_config(config)

        # Must be valid YAML
        parsed = yaml.safe_load(result)
        assert parsed["version"] == 2
        assert len(parsed["models"]) == 1

        model = parsed["models"][0]
        assert model["name"] == "orders"

        col_names = [c["name"] for c in model["columns"]]
        assert "order_id" in col_names
        assert "customer_id" in col_names
        assert "amount" in col_names
        assert "status" in col_names

        # order_id should have both not_null and unique
        order_id_col = next(c for c in model["columns"] if c["name"] == "order_id")
        assert "not_null" in order_id_col["tests"]
        assert "unique" in order_id_col["tests"]

        # row_count should appear as comment in the raw output
        assert "row_count" in result
        assert "no direct dbt equivalent" in result


class TestMultiSuite:
    def test_multi_suite_config(self):
        config = _make_config(
            [
                _make_suite(
                    "orders",
                    [
                        CheckConfig(check_type="not_null", column="order_id"),
                        CheckConfig(check_type="unique", column="order_id"),
                    ],
                ),
                _make_suite(
                    "customers",
                    [
                        CheckConfig(check_type="not_null", column="customer_id"),
                        CheckConfig(check_type="unique", column="email"),
                    ],
                ),
            ]
        )

        result = export_config(config)
        parsed = yaml.safe_load(result)

        assert parsed["version"] == 2
        assert len(parsed["models"]) == 2

        model_names = [m["name"] for m in parsed["models"]]
        assert "orders" in model_names
        assert "customers" in model_names

    def test_multi_suite_with_comments(self):
        config = _make_config(
            [
                _make_suite(
                    "orders",
                    [
                        CheckConfig(check_type="not_null", column="id"),
                        CheckConfig(check_type="row_count", params={"min": 1}),
                    ],
                ),
                _make_suite(
                    "products",
                    [
                        CheckConfig(
                            check_type="freshness",
                            column="updated_at",
                            params={"max_age": "1h"},
                        ),
                    ],
                ),
            ]
        )

        result = export_config(config)
        assert "Model 'orders'" in result
        assert "row_count" in result
        assert "Model 'products'" in result
        assert "freshness" in result
