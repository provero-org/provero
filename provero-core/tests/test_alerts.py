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

"""Tests for webhook alert system."""

from __future__ import annotations

import json
import textwrap
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

from provero.alerts.models import AlertConfig
from provero.alerts.sender import _build_payload, _should_fire, send_alert, send_alerts
from provero.core.compiler import compile_file
from provero.core.results import CheckResult, Status, SuiteResult


def _make_result(status: Status = Status.FAIL, suite_name: str = "test") -> SuiteResult:
    check = CheckResult(
        check_name="not_null:id",
        check_type="not_null",
        status=status,
        column="id",
        observed_value="2 nulls",
        expected_value="0 nulls",
    )
    result = SuiteResult(
        suite_name=suite_name,
        status=Status.PASS,
        checks=[check],
    )
    result.compute_status()
    return result


class TestShouldFire:
    def test_on_failure_fires_when_failed(self):
        alert = AlertConfig(url="http://x", trigger="on_failure")
        assert _should_fire(alert, _make_result(Status.FAIL)) is True

    def test_on_failure_skips_when_passed(self):
        alert = AlertConfig(url="http://x", trigger="on_failure")
        assert _should_fire(alert, _make_result(Status.PASS)) is False

    def test_always_fires_on_pass(self):
        alert = AlertConfig(url="http://x", trigger="always")
        assert _should_fire(alert, _make_result(Status.PASS)) is True

    def test_on_success_fires_on_pass(self):
        alert = AlertConfig(url="http://x", trigger="on_success")
        assert _should_fire(alert, _make_result(Status.PASS)) is True

    def test_on_success_skips_on_fail(self):
        alert = AlertConfig(url="http://x", trigger="on_success")
        assert _should_fire(alert, _make_result(Status.FAIL)) is False


class TestBuildPayload:
    def test_contains_suite_info(self):
        result = _make_result(Status.FAIL, suite_name="orders_daily")
        payload = _build_payload(result)
        assert payload["suite"] == "orders_daily"
        assert payload["status"] == "fail"
        assert payload["failed"] >= 1
        assert "timestamp" in payload

    def test_includes_failed_checks(self):
        result = _make_result(Status.FAIL)
        payload = _build_payload(result)
        assert len(payload["failed_checks"]) >= 1
        assert payload["failed_checks"][0]["check"] == "not_null:id"

    def test_passing_result_has_empty_failed_checks(self):
        result = _make_result(Status.PASS)
        payload = _build_payload(result)
        assert payload["failed_checks"] == []


class TestSendAlert:
    def test_sends_to_local_server(self):
        received = {}

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers["Content-Length"])
                received["body"] = json.loads(self.rfile.read(length))
                received["headers"] = dict(self.headers)
                self.send_response(200)
                self.end_headers()

            def log_message(self, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        port = server.server_address[1]
        thread = Thread(target=server.handle_request, daemon=True)
        thread.start()

        alert = AlertConfig(
            url=f"http://127.0.0.1:{port}/hook",
            trigger="on_failure",
            headers={"X-Custom": "test-value"},
        )
        result = _make_result(Status.FAIL)
        ok = send_alert(alert, result)
        thread.join(timeout=5)
        server.server_close()

        assert ok is True
        assert received["body"]["suite"] == "test"
        assert received["body"]["status"] == "fail"
        assert received["headers"]["X-Custom"] == "test-value"

    def test_returns_false_on_connection_error(self):
        alert = AlertConfig(url="http://127.0.0.1:1/nope", trigger="on_failure")
        result = _make_result(Status.FAIL)
        ok = send_alert(alert, result)
        assert ok is False

    def test_skips_when_trigger_not_met(self):
        alert = AlertConfig(url="http://127.0.0.1:1/nope", trigger="on_failure")
        result = _make_result(Status.PASS)
        ok = send_alert(alert, result)
        assert ok is False


class TestSendAlerts:
    def test_multiple_alerts(self):
        alerts = [
            AlertConfig(url="http://127.0.0.1:1/a", trigger="on_failure"),
            AlertConfig(url="http://127.0.0.1:1/b", trigger="on_success"),
        ]
        result = _make_result(Status.FAIL)
        outcomes = send_alerts(alerts, result)
        assert len(outcomes) == 2
        # First should try to fire (and fail due to connection), second should skip
        assert outcomes[0] is False  # connection error
        assert outcomes[1] is False  # trigger not met


class TestAlertConfigParsing:
    def test_alerts_parsed_from_yaml(self, tmp_path):
        config_path = tmp_path / "provero.yaml"
        config_path.write_text(
            textwrap.dedent("""\
            source:
              type: duckdb
              table: orders

            checks:
              - not_null: order_id

            alerts:
              - type: webhook
                url: https://hooks.example.com/test
                trigger: on_failure
                headers:
                  Authorization: "Bearer token123"
              - type: webhook
                url: https://pd.example.com/alert
                trigger: always
        """)
        )

        config = compile_file(config_path)
        assert len(config.alerts) == 2
        assert config.alerts[0].url == "https://hooks.example.com/test"
        assert config.alerts[0].trigger == "on_failure"
        assert config.alerts[0].headers["Authorization"] == "Bearer token123"
        assert config.alerts[1].trigger == "always"

    def test_no_alerts_is_empty_list(self, tmp_path):
        config_path = tmp_path / "provero.yaml"
        config_path.write_text(
            textwrap.dedent("""\
            source:
              type: duckdb
              table: orders

            checks:
              - not_null: order_id
        """)
        )

        config = compile_file(config_path)
        assert config.alerts == []
