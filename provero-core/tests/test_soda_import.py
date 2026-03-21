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

"""Tests for the SodaCL importer."""

from __future__ import annotations

import textwrap

import yaml

from provero.importers.soda import convert_soda_to_provero


def _parse_output(output: str) -> dict:
    """Parse the Provero YAML output back into a dict for assertions."""
    return yaml.safe_load(output)


class TestMissingCount:
    def test_missing_count_zero(self):
        soda = textwrap.dedent("""\
            checks for orders:
              - missing_count(order_id) = 0
        """)
        result = _parse_output(convert_soda_to_provero(soda))
        assert result["checks"] == [{"not_null": "order_id"}]
        assert result["source"]["table"] == "orders"

    def test_missing_count_multiple_columns(self):
        soda = textwrap.dedent("""\
            checks for users:
              - missing_count(email) = 0
              - missing_count(name) = 0
        """)
        result = _parse_output(convert_soda_to_provero(soda))
        assert {"not_null": "email"} in result["checks"]
        assert {"not_null": "name"} in result["checks"]


class TestDuplicateCount:
    def test_duplicate_count_zero(self):
        soda = textwrap.dedent("""\
            checks for orders:
              - duplicate_count(order_id) = 0
        """)
        result = _parse_output(convert_soda_to_provero(soda))
        assert result["checks"] == [{"unique": "order_id"}]


class TestMissingPercent:
    def test_missing_percent(self):
        soda = textwrap.dedent("""\
            checks for orders:
              - missing_percent(email) < 5%
        """)
        result = _parse_output(convert_soda_to_provero(soda))
        check = result["checks"][0]
        assert check == {"completeness": {"column": "email", "min": 0.95}}

    def test_missing_percent_small(self):
        soda = textwrap.dedent("""\
            checks for orders:
              - missing_percent(phone) < 1%
        """)
        result = _parse_output(convert_soda_to_provero(soda))
        check = result["checks"][0]
        assert check == {"completeness": {"column": "phone", "min": 0.99}}

    def test_missing_percent_decimal(self):
        soda = textwrap.dedent("""\
            checks for orders:
              - missing_percent(zip) < 0.5%
        """)
        result = _parse_output(convert_soda_to_provero(soda))
        check = result["checks"][0]
        assert check == {"completeness": {"column": "zip", "min": 0.995}}


class TestInvalidCount:
    def test_invalid_count_with_valid_values(self):
        soda = textwrap.dedent("""\
            checks for orders:
              - invalid_count(status) = 0:
                  valid values: [pending, shipped, delivered]
        """)
        result = _parse_output(convert_soda_to_provero(soda))
        check = result["checks"][0]
        assert check == {
            "accepted_values": {
                "column": "status",
                "values": ["pending", "shipped", "delivered"],
            }
        }


class TestRowCount:
    def test_row_count_greater_than(self):
        soda = textwrap.dedent("""\
            checks for orders:
              - row_count > 0
        """)
        result = _parse_output(convert_soda_to_provero(soda))
        assert result["checks"] == [{"row_count": {"min": 1}}]

    def test_row_count_greater_equal(self):
        soda = textwrap.dedent("""\
            checks for orders:
              - row_count >= 100
        """)
        result = _parse_output(convert_soda_to_provero(soda))
        assert result["checks"] == [{"row_count": {"min": 100}}]

    def test_row_count_equal(self):
        soda = textwrap.dedent("""\
            checks for orders:
              - row_count = 50
        """)
        result = _parse_output(convert_soda_to_provero(soda))
        assert result["checks"] == [{"row_count": {"min": 50, "max": 50}}]


class TestFreshness:
    def test_freshness_hours(self):
        soda = textwrap.dedent("""\
            checks for orders:
              - freshness(updated_at) < 24h
        """)
        result = _parse_output(convert_soda_to_provero(soda))
        assert result["checks"] == [{"freshness": {"column": "updated_at", "max_age": "24h"}}]

    def test_freshness_days(self):
        soda = textwrap.dedent("""\
            checks for orders:
              - freshness(created_at) < 7d
        """)
        result = _parse_output(convert_soda_to_provero(soda))
        assert result["checks"] == [{"freshness": {"column": "created_at", "max_age": "7d"}}]


class TestSchema:
    def test_schema_required_columns(self):
        soda = textwrap.dedent("""\
            checks for orders:
              - schema:
                  fail:
                    when required column missing: [order_id, amount, status]
        """)
        result = _parse_output(convert_soda_to_provero(soda))
        assert {"not_null": "order_id"} in result["checks"]
        assert {"not_null": "amount"} in result["checks"]
        assert {"not_null": "status"} in result["checks"]


class TestFullConversion:
    def test_complete_soda_file(self):
        soda = textwrap.dedent("""\
            checks for orders:
              - missing_count(order_id) = 0
              - duplicate_count(order_id) = 0
              - missing_percent(email) < 5%
              - invalid_count(status) = 0:
                  valid values: [pending, shipped, delivered]
              - row_count > 0
              - schema:
                  fail:
                    when required column missing: [order_id, amount, status]
              - freshness(updated_at) < 24h
        """)
        result = _parse_output(convert_soda_to_provero(soda))
        assert result["source"]["type"] == "duckdb"
        assert result["source"]["table"] == "orders"
        assert len(result["checks"]) == 9  # 7 checks + 3 schema cols - 1 (order_id already there)

    def test_custom_source_type(self):
        soda = textwrap.dedent("""\
            checks for users:
              - row_count > 0
        """)
        result = _parse_output(convert_soda_to_provero(soda, source_type="postgres"))
        assert result["source"]["type"] == "postgres"


class TestUnsupportedChecks:
    def test_unsupported_check_in_comments(self):
        soda = textwrap.dedent("""\
            checks for orders:
              - row_count > 0
              - avg(amount) between 10 and 500
        """)
        output = convert_soda_to_provero(soda)
        result = _parse_output(output)
        # Only the supported check should appear
        assert len(result["checks"]) == 1
        # Unsupported check should be in a comment
        assert "avg(amount) between 10 and 500" in output
        assert "# Unsupported" in output


class TestMalformedInput:
    def test_empty_input(self):
        output = convert_soda_to_provero("")
        result = _parse_output(output)
        assert result["source"]["table"] == "unknown"

    def test_no_checks_key(self):
        soda = textwrap.dedent("""\
            some_random_key:
              foo: bar
        """)
        output = convert_soda_to_provero(soda)
        result = _parse_output(output)
        assert result["source"]["table"] == "unknown"

    def test_invalid_yaml_structure(self):
        # Valid YAML but checks value is a dict instead of a list
        soda = textwrap.dedent("""\
            checks for orders:
              key: value
        """)
        # Should not crash; non-list checks are skipped
        output = convert_soda_to_provero(soda)
        result = _parse_output(output)
        assert result["source"] is not None

    def test_checks_list_with_non_matching_expressions(self):
        soda = textwrap.dedent("""\
            checks for orders:
              - some_weird_check > 42
        """)
        output = convert_soda_to_provero(soda)
        assert "some_weird_check > 42" in output


class TestCLI:
    """Test the CLI integration for the import soda command."""

    def test_import_soda_stdout(self, tmp_path):
        from typer.testing import CliRunner

        from provero.cli.main import app

        soda_file = tmp_path / "soda.yaml"
        soda_file.write_text(
            textwrap.dedent("""\
            checks for orders:
              - missing_count(id) = 0
              - row_count > 0
        """)
        )

        runner = CliRunner()
        result = runner.invoke(app, ["import", "soda", str(soda_file)])
        assert result.exit_code == 0
        parsed = yaml.safe_load(result.output)
        assert parsed["source"]["table"] == "orders"
        assert {"not_null": "id"} in parsed["checks"]

    def test_import_soda_output_file(self, tmp_path):
        from typer.testing import CliRunner

        from provero.cli.main import app

        soda_file = tmp_path / "soda.yaml"
        soda_file.write_text(
            textwrap.dedent("""\
            checks for orders:
              - row_count > 0
        """)
        )
        out_file = tmp_path / "provero.yaml"

        runner = CliRunner()
        result = runner.invoke(app, ["import", "soda", str(soda_file), "-o", str(out_file)])
        assert result.exit_code == 0
        assert out_file.exists()
        parsed = yaml.safe_load(out_file.read_text())
        assert parsed["source"]["table"] == "orders"

    def test_import_soda_file_not_found(self, tmp_path):
        from typer.testing import CliRunner

        from provero.cli.main import app

        runner = CliRunner()
        result = runner.invoke(app, ["import", "soda", str(tmp_path / "nope.yaml")])
        assert result.exit_code == 1
