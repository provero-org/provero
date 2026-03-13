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

"""Tests for the CLI commands."""

from __future__ import annotations

import json
import textwrap

from provero import __version__
from provero.cli.main import app


class TestVersion:
    def test_shows_version(self, cli_runner):
        result = cli_runner.invoke(app, ["version"])
        assert result.exit_code == 0
        assert __version__ in result.output


class TestInit:
    def test_creates_template(self, cli_runner, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = cli_runner.invoke(app, ["init"])
        assert result.exit_code == 0
        content = (tmp_path / "provero.yaml").read_text()
        assert "source:" in content

    def test_existing_file_aborts(self, cli_runner, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "provero.yaml").write_text("existing")
        result = cli_runner.invoke(app, ["init"])
        assert result.exit_code == 1
        assert "already exists" in result.output

    def test_from_source_bad_format(self, cli_runner, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = cli_runner.invoke(app, ["init", "--from-source", "bad_format"])
        assert result.exit_code == 1
        assert "Format" in result.output


class TestRun:
    def test_missing_config(self, cli_runner, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = cli_runner.invoke(app, ["run", "--config", "nonexistent.yaml"])
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_passing_checks(self, cli_runner, duckdb_config_file, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        config_path = str(duckdb_config_file["config_path"])
        result = cli_runner.invoke(app, ["run", "--config", config_path, "--no-store"])
        assert result.exit_code == 0

    def test_failing_checks(self, cli_runner, duckdb_file, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        config = tmp_path / "fail.yaml"
        config.write_text(textwrap.dedent(f"""\
            source:
              type: duckdb
              connection: "{duckdb_file}"
              table: orders

            checks:
              - unique: customer_id
        """))
        result = cli_runner.invoke(app, ["run", "--config", str(config), "--no-store"])
        assert result.exit_code == 1

    def test_json_format(self, cli_runner, duckdb_config_file, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        config_path = str(duckdb_config_file["config_path"])
        result = cli_runner.invoke(
            app, ["run", "--config", config_path, "--format", "json", "--no-store"]
        )
        assert result.exit_code == 0
        # Output should contain valid JSON (may have Rich markup too, parse the JSON part)
        # The JSON is printed via console.print, find the JSON object in output
        output = result.output
        start = output.index("{")
        end = output.rindex("}") + 1
        parsed = json.loads(output[start:end])
        assert "suite_name" in parsed

    def test_html_report(self, cli_runner, duckdb_config_file, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        config_path = str(duckdb_config_file["config_path"])
        result = cli_runner.invoke(
            app, ["run", "--config", config_path, "--report", "html", "--no-store"]
        )
        assert result.exit_code == 0
        # Check that HTML report was generated somewhere under .provero/reports/
        reports = list(tmp_path.glob(".provero/reports/*.html"))
        assert len(reports) >= 1

    def test_no_store(self, cli_runner, duckdb_config_file, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        config_path = str(duckdb_config_file["config_path"])
        result = cli_runner.invoke(app, ["run", "--config", config_path, "--no-store"])
        assert result.exit_code == 0
        assert not (tmp_path / ".provero" / "results.db").exists()

    def test_no_optimize(self, cli_runner, duckdb_config_file, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        config_path = str(duckdb_config_file["config_path"])
        result = cli_runner.invoke(
            app, ["run", "--config", config_path, "--no-optimize", "--no-store"]
        )
        assert result.exit_code == 0


class TestValidate:
    def test_valid_config(self, cli_runner, sample_config_file):
        result = cli_runner.invoke(app, ["validate", "--config", str(sample_config_file)])
        assert result.exit_code == 0
        assert "Valid" in result.output

    def test_invalid_config(self, cli_runner, tmp_path):
        bad_config = tmp_path / "bad.yaml"
        bad_config.write_text("not_a_valid_key: true\n")
        result = cli_runner.invoke(app, ["validate", "--config", str(bad_config)])
        assert result.exit_code == 1


class TestContract:
    def test_contract_validate(self, cli_runner, duckdb_file, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        config = tmp_path / "contract.yaml"
        config.write_text(textwrap.dedent(f"""\
            version: "1.0"
            sources:
              warehouse:
                type: duckdb
                connection: "{duckdb_file}"

            suites:
              - name: orders_checks
                source: warehouse
                table: orders
                checks:
                  - not_null: order_id

            contracts:
              - name: orders_contract
                source: warehouse
                table: orders
                schema:
                  columns:
                    - name: order_id
                      type: integer
                    - name: customer_id
                      type: varchar
                    - name: amount
                      type: decimal
                    - name: status
                      type: varchar
        """))
        result = cli_runner.invoke(
            app, ["contract", "validate", "--config", str(config)]
        )
        assert result.exit_code == 0

    def test_contract_diff(self, cli_runner, tmp_path):
        old_config = tmp_path / "old.yaml"
        old_config.write_text(textwrap.dedent("""\
            source:
              type: duckdb
              table: orders

            checks:
              - not_null: order_id

            contracts:
              - name: orders_contract
                table: orders
                schema:
                  columns:
                    - name: order_id
                      type: integer
        """))
        new_config = tmp_path / "new.yaml"
        new_config.write_text(textwrap.dedent("""\
            source:
              type: duckdb
              table: orders

            checks:
              - not_null: order_id

            contracts:
              - name: orders_contract
                table: orders
                schema:
                  columns:
                    - name: order_id
                      type: integer
                    - name: name
                      type: varchar
        """))
        result = cli_runner.invoke(
            app, ["contract", "diff", str(old_config), str(new_config)]
        )
        assert result.exit_code == 0
        assert "orders_contract" in result.output
