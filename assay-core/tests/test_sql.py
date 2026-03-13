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

"""Tests for SQL identifier sanitization."""

from __future__ import annotations

import pytest

from provero.core.sql import is_expression, quote_identifier, quote_value


class TestExpression:
    def test_read_csv_is_expression(self):
        assert is_expression("read_csv('data.csv')") is True

    def test_read_parquet_is_expression(self):
        assert is_expression("read_parquet('*.parquet')") is True

    def test_plain_table_is_not_expression(self):
        assert is_expression("orders") is False

    def test_schema_qualified_is_not_expression(self):
        assert is_expression("public.orders") is False


class TestQuoteIdentifier:
    def test_simple_name(self):
        assert quote_identifier("orders") == '"orders"'

    def test_schema_qualified(self):
        assert quote_identifier("public.orders") == '"public"."orders"'

    def test_underscore_name(self):
        assert quote_identifier("order_items") == '"order_items"'

    def test_rejects_injection(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            quote_identifier("orders; DROP TABLE users")

    def test_rejects_quotes(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            quote_identifier('orders"')

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            quote_identifier("")

    def test_rejects_special_chars(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            quote_identifier("orders--comment")

    def test_expression_passed_through(self):
        expr = "read_csv('data/orders.csv')"
        assert quote_identifier(expr) == expr

    def test_parquet_expression_passed_through(self):
        expr = "read_parquet('s3://bucket/*.parquet')"
        assert quote_identifier(expr) == expr


class TestQuoteValue:
    def test_simple_string(self):
        assert quote_value("hello") == "hello"

    def test_single_quote_escape(self):
        assert quote_value("it's") == "it''s"

    def test_double_quotes_preserved(self):
        assert quote_value('say "hi"') == 'say "hi"'
