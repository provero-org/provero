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

"""Tests for the watch command and interval parsing."""

from __future__ import annotations

import json
import textwrap
from unittest.mock import patch

import pytest

from provero.cli.main import _parse_interval, app


class TestParseInterval:
    """Tests for the _parse_interval helper."""

    def test_seconds(self):
        assert _parse_interval("30s") == 30

    def test_minutes(self):
        assert _parse_interval("5m") == 300

    def test_hours(self):
        assert _parse_interval("1h") == 3600

    def test_combined_hours_minutes(self):
        assert _parse_interval("1h30m") == 5400

    def test_combined_minutes_seconds(self):
        assert _parse_interval("2m15s") == 135

    def test_combined_all(self):
        assert _parse_interval("1h30m15s") == 5415

    def test_whitespace_stripped(self):
        assert _parse_interval("  5m  ") == 300

    def test_case_insensitive(self):
        assert _parse_interval("5M") == 300

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="empty"):
            _parse_interval("")

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="Invalid interval"):
            _parse_interval("abc")

    def test_bare_number_raises(self):
        with pytest.raises(ValueError, match="Invalid interval"):
            _parse_interval("30")

    def test_zero_raises(self):
        with pytest.raises(ValueError, match="greater than zero"):
            _parse_interval("0s")


class TestWatchCommand:
    """Tests for the watch CLI command."""

    def test_missing_config(self, cli_runner, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = cli_runner.invoke(
            app, ["watch", "--config", "nonexistent.yaml"]
        )
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_invalid_interval(self, cli_runner, duckdb_config_file, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        config_path = str(duckdb_config_file["config_path"])
        result = cli_runner.invoke(
            app, ["watch", "--config", config_path, "--interval", "bad"]
        )
        assert result.exit_code == 1
        assert "Invalid interval" in result.output

    def test_count_one_runs_once(self, cli_runner, duckdb_config_file, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        config_path = str(duckdb_config_file["config_path"])
        result = cli_runner.invoke(
            app,
            ["watch", "--config", config_path, "--no-store", "--count", "1"],
        )
        assert result.exit_code == 0
        assert "Run #1" in result.output

    def test_count_two_runs_twice(self, cli_runner, duckdb_config_file, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        config_path = str(duckdb_config_file["config_path"])
        result = cli_runner.invoke(
            app,
            [
                "watch",
                "--config", config_path,
                "--no-store",
                "--count", "2",
                "--interval", "1s",
            ],
        )
        assert result.exit_code == 0
        assert "Run #1" in result.output
        assert "Run #2" in result.output

    def test_json_format_outputs_jsonl(self, cli_runner, duckdb_config_file, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        config_path = str(duckdb_config_file["config_path"])
        result = cli_runner.invoke(
            app,
            [
                "watch",
                "--config", config_path,
                "--no-store",
                "--count", "2",
                "--interval", "1s",
                "--format", "json",
            ],
        )
        assert result.exit_code == 0
        lines = [line for line in result.output.strip().splitlines() if line.strip()]
        assert len(lines) == 2
        for line in lines:
            parsed = json.loads(line)
            assert "suite_name" in parsed

    def test_keyboard_interrupt_handled(
        self, cli_runner, duckdb_config_file, monkeypatch, tmp_path
    ):
        monkeypatch.chdir(tmp_path)
        config_path = str(duckdb_config_file["config_path"])

        with patch("provero.cli.main.time.sleep", side_effect=KeyboardInterrupt):
            result = cli_runner.invoke(
                app,
                [
                    "watch",
                    "--config", config_path,
                    "--no-store",
                    "--interval", "5m",
                ],
            )
        # Should exit gracefully, not crash
        assert result.exit_code == 0
        assert "Watch stopped" in result.output

    def test_exit_code_1_on_failure(
        self, cli_runner, duckdb_file, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        config = tmp_path / "fail.yaml"
        config.write_text(
            textwrap.dedent(f"""\
            source:
              type: duckdb
              connection: "{duckdb_file}"
              table: orders

            checks:
              - unique: customer_id
        """)
        )
        result = cli_runner.invoke(
            app,
            ["watch", "--config", str(config), "--no-store", "--count", "1"],
        )
        assert result.exit_code == 1

    def test_suite_filter(self, cli_runner, duckdb_file, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        config = tmp_path / "multi.yaml"
        config.write_text(
            textwrap.dedent(f"""\
            version: "1.0"
            sources:
              warehouse:
                type: duckdb
                connection: "{duckdb_file}"

            suites:
              - name: orders_suite
                source: warehouse
                table: orders
                checks:
                  - not_null: order_id

              - name: events_suite
                source: warehouse
                table: events
                checks:
                  - not_null: event_id
        """)
        )
        result = cli_runner.invoke(
            app,
            [
                "watch",
                "--config", str(config),
                "--no-store",
                "--count", "1",
                "--suite", "orders_suite",
                "--format", "json",
            ],
        )
        assert result.exit_code == 0
        parsed = json.loads(result.output.strip())
        assert parsed["suite_name"] == "orders_suite"
