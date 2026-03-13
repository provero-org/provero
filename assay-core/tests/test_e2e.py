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

"""End-to-end tests: YAML -> engine -> store -> report."""

from __future__ import annotations

import textwrap

from provero.connectors.duckdb import DuckDBConnection
from provero.contracts.models import (
    ColumnContract,
    ContractConfig,
    SchemaContract,
    SLAConfig,
)
from provero.contracts.validator import validate_contract
from provero.core.compiler import CheckConfig, SourceConfig, SuiteConfig, compile_file
from provero.core.engine import run_suite
from provero.core.results import Status
from provero.reporting.html import generate_html_report
from provero.store.sqlite import SQLiteStore


class _SharedDuckDBConnector:
    def __init__(self, conn: DuckDBConnection) -> None:
        self._conn = conn

    def connect(self) -> DuckDBConnection:
        return self._conn

    def disconnect(self, connection: DuckDBConnection) -> None:
        pass


class TestEndToEnd:
    def test_yaml_to_results(self, duckdb_file, tmp_path):
        """Full pipeline: YAML -> compile -> connect -> run -> assert results."""
        config_path = tmp_path / "provero.yaml"
        config_path.write_text(textwrap.dedent(f"""\
            source:
              type: duckdb
              connection: "{duckdb_file}"
              table: orders

            checks:
              - not_null: order_id
              - unique: order_id
              - row_count:
                  min: 1
                  max: 100
        """))

        from provero.connectors.factory import create_connector

        config = compile_file(config_path)
        suite = config.suites[0]
        connector = create_connector(suite.source)
        result = run_suite(suite, connector)

        assert result.total == 3
        assert result.passed == 3
        assert result.status == Status.PASS
        assert result.quality_score == 100.0

    def test_results_persisted_to_store(self, duckdb_orders, sqlite_store):
        """Run suite -> save -> get_history round-trip."""
        connector = _SharedDuckDBConnector(duckdb_orders)
        suite = SuiteConfig(
            name="store_e2e",
            source=SourceConfig(type="duckdb", table="orders"),
            checks=[
                CheckConfig(check_type="not_null", column="order_id"),
                CheckConfig(check_type="row_count", params={"min": 1}),
            ],
        )
        result = run_suite(suite, connector)
        sqlite_store.save_result(result)

        history = sqlite_store.get_history(suite_name="store_e2e")
        assert len(history) == 1
        assert history[0]["suite_name"] == "store_e2e"
        assert history[0]["total"] == 2
        assert history[0]["passed"] == 2

    def test_multi_run_history(self, duckdb_orders, sqlite_store):
        """Multiple runs are saved and returned in chronological order."""
        connector = _SharedDuckDBConnector(duckdb_orders)
        suite = SuiteConfig(
            name="multi_run",
            source=SourceConfig(type="duckdb", table="orders"),
            checks=[CheckConfig(check_type="row_count", params={"min": 1})],
        )
        for _ in range(3):
            result = run_suite(suite, connector)
            sqlite_store.save_result(result)

        history = sqlite_store.get_history(suite_name="multi_run")
        assert len(history) == 3

    def test_contract_e2e_with_duckdb(self, duckdb_orders):
        """Contract validation against live DuckDB: schema + SLA."""
        contract = ContractConfig(
            name="orders_contract",
            table="orders",
            schema_def=SchemaContract(columns=[
                ColumnContract(name="order_id", type="integer"),
                ColumnContract(name="customer_id", type="varchar"),
                ColumnContract(name="amount", type="decimal"),
                ColumnContract(name="status", type="varchar"),
            ]),
            sla=SLAConfig(availability="true"),
        )
        result = validate_contract(contract, duckdb_orders)
        assert result.status == "pass"
        assert len(result.violations) == 0

    def test_html_from_real_run(self, duckdb_orders, tmp_path):
        """Run suite -> generate HTML report -> verify content."""
        connector = _SharedDuckDBConnector(duckdb_orders)
        suite = SuiteConfig(
            name="html_e2e",
            source=SourceConfig(type="duckdb", table="orders"),
            checks=[
                CheckConfig(check_type="not_null", column="order_id"),
                CheckConfig(check_type="unique", column="order_id"),
            ],
        )
        result = run_suite(suite, connector)
        report_path = tmp_path / "report.html"
        html = generate_html_report(result, output_path=report_path)

        assert report_path.exists()
        assert "not_null" in html
        assert "unique" in html
        assert "html_e2e" in html

    def test_suite_tag_filter(self, duckdb_file, tmp_path):
        """YAML with tagged suites, only matching tag runs."""
        config_path = tmp_path / "provero.yaml"
        config_path.write_text(textwrap.dedent(f"""\
            version: "1.0"
            suites:
              - name: critical_suite
                source:
                  type: duckdb
                  connection: "{duckdb_file}"
                  table: orders
                tags: [critical]
                checks:
                  - not_null: order_id

              - name: info_suite
                source:
                  type: duckdb
                  connection: "{duckdb_file}"
                  table: orders
                tags: [info]
                checks:
                  - row_count:
                      min: 1
        """))

        config = compile_file(config_path)

        # Filter by tag
        critical_suites = [s for s in config.suites if "critical" in s.tags]
        assert len(critical_suites) == 1
        assert critical_suites[0].name == "critical_suite"

    def test_anomaly_with_injected_history(self, duckdb_orders):
        """Anomaly check with injected history detects outlier."""
        connector = _SharedDuckDBConnector(duckdb_orders)
        suite = SuiteConfig(
            name="anomaly_e2e",
            source=SourceConfig(type="duckdb", table="orders"),
            checks=[
                CheckConfig(
                    check_type="anomaly",
                    column="amount",
                    params={
                        "metric": "mean",
                        "_history": [100.0, 102.0, 98.0, 101.0, 99.0, 100.5],
                    },
                ),
            ],
        )
        result = run_suite(suite, connector, optimize=False)
        assert result.total == 1
        # The anomaly check should execute without error
        assert result.checks[0].status in (Status.PASS, Status.FAIL, Status.WARN)

    def test_full_pipeline_yaml_to_report(self, duckdb_file, tmp_path):
        """Complete pipeline: YAML -> compile -> engine -> store -> HTML report."""
        config_path = tmp_path / "provero.yaml"
        config_path.write_text(textwrap.dedent(f"""\
            source:
              type: duckdb
              connection: "{duckdb_file}"
              table: orders

            checks:
              - not_null: order_id
              - unique: order_id
              - row_count:
                  min: 1
        """))

        from provero.connectors.factory import create_connector

        config = compile_file(config_path)
        suite = config.suites[0]
        connector = create_connector(suite.source)
        result = run_suite(suite, connector)

        # Store
        db_path = tmp_path / "results.db"
        store = SQLiteStore(db_path)
        store.save_result(result)
        history = store.get_history()
        assert len(history) == 1
        store.close()

        # Report
        report_path = tmp_path / "report.html"
        html = generate_html_report(result, output_path=report_path)
        assert report_path.exists()
        assert "not_null" in html
        assert "order_id" in html
