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

"""Tests for the AQL compiler."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from assay.core.compiler import compile_file, parse_check


class TestParseCheck:
    def test_string_shorthand(self):
        check = parse_check("not_null: order_id")
        assert check.check_type == "not_null"
        assert check.column == "order_id"

    def test_list_columns(self):
        check = parse_check({"not_null": ["id", "name"]})
        assert check.check_type == "not_null"
        assert check.columns == ["id", "name"]

    def test_single_column(self):
        check = parse_check({"unique": "order_id"})
        assert check.check_type == "unique"
        assert check.column == "order_id"

    def test_dict_params(self):
        check = parse_check({"range": {"column": "amount", "min": 0, "max": 1000}})
        assert check.check_type == "range"
        assert check.column == "amount"
        assert check.params == {"min": 0, "max": 1000}

    def test_accepted_values(self):
        check = parse_check({
            "accepted_values": {
                "column": "status",
                "values": ["a", "b", "c"],
            }
        })
        assert check.check_type == "accepted_values"
        assert check.column == "status"
        assert check.params["values"] == ["a", "b", "c"]

    def test_severity_parsed_from_dict(self):
        check = parse_check({
            "range": {"column": "amount", "min": 0, "max": 1000, "severity": "warning"}
        })
        assert check.check_type == "range"
        assert check.severity == "warning"
        assert "severity" not in check.params

    def test_severity_none_by_default(self):
        check = parse_check({"unique": "order_id"})
        assert check.severity is None

    def test_invalid_check_error_message(self):
        import pytest
        with pytest.raises(ValueError, match="Invalid check definition"):
            parse_check({})


class TestCompileFile:
    def test_simple_format(self, tmp_path: Path):
        config_file = tmp_path / "assay.yaml"
        config_file.write_text(dedent("""\
            source:
              type: duckdb
              table: orders
            checks:
              - not_null: [id, name]
              - unique: id
              - row_count:
                  min: 100
        """))

        config = compile_file(config_file)
        assert len(config.suites) == 1
        assert config.suites[0].name == "assay"
        assert config.suites[0].source.type == "duckdb"
        assert len(config.suites[0].checks) == 3

    def test_full_format_with_suites(self, tmp_path: Path):
        config_file = tmp_path / "assay.yaml"
        config_file.write_text(dedent("""\
            version: "1.0"
            sources:
              warehouse:
                type: postgres
                connection: postgresql://localhost/db
            suites:
              - name: orders_daily
                source: warehouse
                table: orders
                tags: [critical, daily]
                checks:
                  - not_null: order_id
                  - unique: order_id
              - name: users_daily
                source: warehouse
                table: users
                checks:
                  - not_null: user_id
        """))

        config = compile_file(config_file)
        assert len(config.suites) == 2
        assert config.suites[0].name == "orders_daily"
        assert config.suites[0].source.table == "orders"
        assert config.suites[0].tags == ["critical", "daily"]
        assert len(config.suites[0].checks) == 2
        assert config.suites[1].name == "users_daily"
