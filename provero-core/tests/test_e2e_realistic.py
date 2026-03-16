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

"""Realistic end-to-end tests with dirty data.

These tests exercise the full CLI pipeline against a DuckDB file
containing intentionally dirty data (NULLs, outliers, invalid values)
to verify that Provero correctly detects and reports quality issues.
"""

from __future__ import annotations

import csv
import importlib.metadata
import io
import json
import textwrap
from pathlib import Path

import duckdb
import pytest

from provero.cli import main as cli_main
from provero.cli.main import app

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def dirty_db(tmp_path: Path) -> Path:
    """File-based DuckDB with dirty ``orders`` table (100 rows) and clean
    ``products`` table (20 rows).

    Orders anomalies:
    - 2 rows with NULL customer_id
    - 1 row with negative amount (-15.0)
    - 1 row with very high amount (999999.99)
    - 1 row with invalid status ("INVALID")
    - 1 row with invalid currency ("XYZ")
    - 1 row with NULL email
    """
    db_path = tmp_path / "dirty.db"
    conn = duckdb.connect(str(db_path))

    # -- orders table --
    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id VARCHAR,
            amount DECIMAL(10,2),
            status VARCHAR,
            currency VARCHAR,
            email VARCHAR
        )
    """)

    # Build 100 rows: first 93 are clean, then 7 dirty rows.
    clean_rows = []
    for i in range(1, 94):
        clean_rows.append(
            f"({i}, 'C{i:03d}', {50 + i * 1.1:.2f}, 'delivered', 'USD', 'user{i}@example.com')"
        )
    conn.execute(f"INSERT INTO orders VALUES {', '.join(clean_rows)}")

    # Dirty rows (ids 94-100)
    conn.execute("""
        INSERT INTO orders VALUES
        (94, NULL,   100.00, 'delivered', 'USD', 'user94@example.com'),
        (95, NULL,   200.00, 'shipped',   'USD', 'user95@example.com'),
        (96, 'C096', -15.00, 'delivered', 'USD', 'user96@example.com'),
        (97, 'C097', 999999.99, 'pending', 'USD', 'user97@example.com'),
        (98, 'C098', 75.00,  'INVALID',  'USD', 'user98@example.com'),
        (99, 'C099', 80.00,  'delivered', 'XYZ', 'user99@example.com'),
        (100, 'C100', 90.00, 'delivered', 'USD', NULL)
    """)

    # -- products table (clean) --
    conn.execute("""
        CREATE TABLE products (
            product_id INTEGER,
            name VARCHAR,
            price DECIMAL(10,2),
            category VARCHAR
        )
    """)
    product_rows = []
    for i in range(1, 21):
        product_rows.append(f"({i}, 'Product {i}', {10 + i * 5:.2f}, 'cat_{(i % 3) + 1}')")
    conn.execute(f"INSERT INTO products VALUES {', '.join(product_rows)}")

    conn.close()
    return db_path


@pytest.fixture
def cli_runner():
    """Typer CLI test runner."""
    from typer.testing import CliRunner

    return CliRunner()


@pytest.fixture(autouse=True)
def _reset_quiet_flag():
    """Reset the module-level quiet flag after each test."""
    yield
    cli_main._quiet = False


# ---------------------------------------------------------------------------
# YAML config helpers
# ---------------------------------------------------------------------------


def _write_failing_checks_config(tmp_path: Path, db_path: Path) -> Path:
    """Config with checks that will fail against dirty data."""
    config_path = tmp_path / "failing.yaml"
    config_path.write_text(
        textwrap.dedent(f"""\
        source:
          type: duckdb
          connection: "{db_path}"
          table: orders

        checks:
          - not_null: customer_id
          - range:
              column: amount
              min: 0
              max: 10000
          - accepted_values:
              column: status
              values:
                - delivered
                - shipped
                - pending
                - cancelled
          - accepted_values:
              column: currency
              values:
                - USD
                - EUR
                - GBP
    """)
    )
    return config_path


def _write_contract_config(tmp_path: Path, db_path: Path) -> Path:
    """Contract config for DuckDB file-based source."""
    config_path = tmp_path / "contract.yaml"
    config_path.write_text(
        textwrap.dedent(f"""\
        version: "1.0"
        sources:
          warehouse:
            type: duckdb
            connection: "{db_path}"

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
                - name: currency
                  type: varchar
                - name: email
                  type: varchar
    """)
    )
    return config_path


def _write_multi_suite_config(tmp_path: Path, db_path: Path) -> Path:
    """Multi-suite config: orders suite fails, products suite passes."""
    config_path = tmp_path / "multi.yaml"
    config_path.write_text(
        textwrap.dedent(f"""\
        version: "1.0"
        sources:
          warehouse:
            type: duckdb
            connection: "{db_path}"

        suites:
          - name: orders_suite
            source: warehouse
            table: orders
            checks:
              - not_null: customer_id
              - accepted_values:
                  column: status
                  values:
                    - delivered
                    - shipped
                    - pending
                    - cancelled

          - name: products_suite
            source: warehouse
            table: products
            checks:
              - not_null: product_id
              - not_null: name
              - row_count:
                  min: 1
                  max: 100
    """)
    )
    return config_path


def _write_profile_config(tmp_path: Path, db_path: Path) -> Path:
    """Minimal config pointing at the dirty DuckDB for profiling."""
    config_path = tmp_path / "profile.yaml"
    config_path.write_text(
        textwrap.dedent(f"""\
        source:
          type: duckdb
          connection: "{db_path}"
          table: orders

        checks:
          - not_null: order_id
    """)
    )
    return config_path


# ---------------------------------------------------------------------------
# Tests: CLI with failures
# ---------------------------------------------------------------------------


class TestCLIWithFailures:
    """Tests that verify Provero correctly reports failing checks."""

    def test_run_with_failing_checks(self, cli_runner, dirty_db, tmp_path, monkeypatch):
        """Checks that fail (range, accepted_values) produce exit_code 1."""
        monkeypatch.chdir(tmp_path)
        config_path = _write_failing_checks_config(tmp_path, dirty_db)

        result = cli_runner.invoke(app, ["run", "--config", str(config_path), "--no-store"])

        assert result.exit_code == 1, f"Expected exit_code 1, got {result.exit_code}"
        assert "FAIL" in result.output

    def test_json_output_with_failures(self, cli_runner, dirty_db, tmp_path, monkeypatch):
        """JSON output with failing checks should be valid JSON."""
        monkeypatch.chdir(tmp_path)
        config_path = _write_failing_checks_config(tmp_path, dirty_db)

        result = cli_runner.invoke(
            app,
            ["run", "--config", str(config_path), "--format", "json", "--no-store"],
        )

        output = result.output.strip()
        start = output.index("{")
        end = output.rindex("}") + 1
        parsed = json.loads(output[start:end])
        assert "suite_name" in parsed

    def test_csv_output_with_failures(self, cli_runner, dirty_db, tmp_path, monkeypatch):
        """CSV output with failing checks should be parseable."""
        monkeypatch.chdir(tmp_path)
        config_path = _write_failing_checks_config(tmp_path, dirty_db)

        result = cli_runner.invoke(
            app,
            ["run", "--config", str(config_path), "--format", "csv", "--no-store"],
        )

        rows = list(csv.DictReader(io.StringIO(result.output)))
        assert len(rows) > 0
        statuses = {row["status"] for row in rows}
        assert "Status.FAIL" in statuses or "fail" in {s.lower() for s in statuses}

    def test_quiet_mode_with_failures(self, cli_runner, dirty_db, tmp_path, monkeypatch):
        """Quiet mode with failures: empty output and exit_code 1."""
        monkeypatch.chdir(tmp_path)
        config_path = _write_failing_checks_config(tmp_path, dirty_db)

        result = cli_runner.invoke(app, ["-q", "run", "--config", str(config_path), "--no-store"])

        assert result.exit_code == 1
        assert result.output.strip() == ""


# ---------------------------------------------------------------------------
# Tests: Contract via CLI
# ---------------------------------------------------------------------------


class TestContractViaCLI:
    """Contract validation through the CLI."""

    def test_contract_validate_duckdb_file(self, cli_runner, dirty_db, tmp_path, monkeypatch):
        """Contract YAML with DuckDB file source runs successfully."""
        monkeypatch.chdir(tmp_path)
        config_path = _write_contract_config(tmp_path, dirty_db)

        result = cli_runner.invoke(app, ["contract", "validate", "--config", str(config_path)])

        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# Tests: Multi-suite
# ---------------------------------------------------------------------------


class TestMultiSuite:
    """Tests for configs with multiple suites."""

    def test_multi_suite_mixed_results(self, cli_runner, dirty_db, tmp_path, monkeypatch):
        """Two suites (orders fails, products passes), both appear in output."""
        monkeypatch.chdir(tmp_path)
        config_path = _write_multi_suite_config(tmp_path, dirty_db)

        result = cli_runner.invoke(app, ["run", "--config", str(config_path), "--no-store"])

        assert "orders_suite" in result.output
        assert "products_suite" in result.output
        # Overall should fail because orders_suite has failures
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# Tests: Profile
# ---------------------------------------------------------------------------


class TestProfile:
    """Profiling dirty data."""

    def test_profile_dirty_data(self, cli_runner, dirty_db, tmp_path, monkeypatch):
        """Profile command shows table output with column stats."""
        monkeypatch.chdir(tmp_path)
        config_path = _write_profile_config(tmp_path, dirty_db)

        result = cli_runner.invoke(app, ["profile", "--config", str(config_path)])

        assert result.exit_code == 0
        # Profile output should mention the table and column names.
        # Rich may truncate column names depending on terminal width,
        # so we check for prefixes that survive truncation.
        assert "orders" in result.output
        assert "order_id" in result.output
        assert "custome" in result.output  # customer_id may be truncated
        assert "amount" in result.output


# ---------------------------------------------------------------------------
# Tests: Version
# ---------------------------------------------------------------------------


class TestVersionMetadata:
    """Version command should match installed package metadata."""

    def test_version_matches_metadata(self, cli_runner):
        """The version command output matches importlib.metadata.version."""
        try:
            expected = importlib.metadata.version("provero")
        except importlib.metadata.PackageNotFoundError:
            pytest.skip("provero package metadata not installed in this environment")

        result = cli_runner.invoke(app, ["version"])

        assert result.exit_code == 0
        assert expected in result.output

    def test_version_not_hardcoded(self, cli_runner):
        """The version command should not show the old hardcoded 0.0.1."""
        from provero import __version__

        result = cli_runner.invoke(app, ["version"])

        assert result.exit_code == 0
        assert __version__ in result.output
        assert __version__ != "0.0.1"
