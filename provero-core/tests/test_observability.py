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

"""Tests for the observability + governance foundation."""

from __future__ import annotations

import io
import json
from collections.abc import Iterator
from typing import Any

import pytest

from provero.connectors.duckdb import DuckDBConnection, DuckDBConnector
from provero.core.compiler import CheckConfig, SourceConfig, SuiteConfig
from provero.core.engine import run_suite
from provero.core.results import CheckResult, Status, SuiteResult
from provero.observability import (
    AuditLogObserver,
    clear_observers,
    iter_observers,
    redact,
    redact_string,
    register_observer,
)


class _SharedDuckDBConnector:
    """Connector that always returns the same connection (test only)."""

    def __init__(self, conn: DuckDBConnection) -> None:
        self._conn = conn

    def connect(self) -> DuckDBConnection:
        return self._conn

    def disconnect(self, connection: DuckDBConnection) -> None:
        pass


@pytest.fixture(autouse=True)
def _clean_registry() -> Iterator[None]:
    """Ensure the global observer registry is empty around every test.

    Without this, an observer registered by one test would leak into
    test_engine.py (same process) and silently change its behavior.
    """
    clear_observers()
    try:
        yield
    finally:
        clear_observers()


@pytest.fixture
def orders_connector() -> Iterator[_SharedDuckDBConnector]:
    connector = DuckDBConnector()
    conn = connector.connect()
    conn._conn.execute(
        """
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id VARCHAR,
            amount DECIMAL(10,2),
            status VARCHAR
        )
        """
    )
    conn._conn.execute(
        """
        INSERT INTO orders VALUES
        (1, 'C001', 150.00, 'delivered'),
        (2, 'C002', 89.99, 'shipped'),
        (3, 'C003', 220.50, 'pending'),
        (4, 'C001', 45.00, 'delivered'),
        (5, 'C004', 999.99, 'cancelled')
        """
    )
    yield _SharedDuckDBConnector(conn)
    connector.disconnect(conn)


def _two_check_suite() -> SuiteConfig:
    return SuiteConfig(
        name="obs_suite",
        source=SourceConfig(type="duckdb", table="orders"),
        checks=[
            CheckConfig(check_type="not_null", column="order_id"),
            CheckConfig(check_type="row_count", params={"min": 1, "max": 100}),
        ],
    )


class _FakeObserver:
    """Records the exact sequence of lifecycle calls and their payloads."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def on_suite_start(self, suite: SuiteConfig, run_id: str) -> None:
        self.calls.append(("on_suite_start", (suite.name, run_id)))

    def on_check_complete(self, result: CheckResult) -> None:
        self.calls.append(("on_check_complete", (result.check_type, str(result.status))))

    def on_suite_complete(self, result: SuiteResult) -> None:
        self.calls.append(("on_suite_complete", (result.suite_name, str(result.status))))

    def on_error(self, suite: SuiteConfig, run_id: str, error: BaseException) -> None:
        self.calls.append(("on_error", (suite.name, type(error).__name__)))


# --------------------------------------------------------------------------- #
# Registry + hook sequencing
# --------------------------------------------------------------------------- #


def test_registry_register_iter_clear() -> None:
    obs = _FakeObserver()
    assert iter_observers() == ()
    register_observer(obs)
    assert iter_observers() == (obs,)
    register_observer(obs)  # idempotent
    assert iter_observers() == (obs,)
    clear_observers()
    assert iter_observers() == ()


def test_hook_call_sequence_and_payloads(orders_connector: _SharedDuckDBConnector) -> None:
    obs = _FakeObserver()
    register_observer(obs)

    result = run_suite(_two_check_suite(), orders_connector, optimize=False)

    events = [name for name, _ in obs.calls]
    # suite_start first, suite_complete last, two check completions in between.
    assert events[0] == "on_suite_start"
    assert events[-1] == "on_suite_complete"
    assert events.count("on_check_complete") == 2

    # Payload assertions.
    start_payload = obs.calls[0][1]
    assert start_payload[0] == "obs_suite"
    assert start_payload[1]  # non-empty run_id

    check_payloads = [p for n, p in obs.calls if n == "on_check_complete"]
    assert {ct for ct, _ in check_payloads} == {"not_null", "row_count"}
    assert all(status == "pass" for _, status in check_payloads)

    complete_payload = obs.calls[-1][1]
    assert complete_payload == ("obs_suite", "pass")
    assert result.status == Status.PASS


def test_no_observers_zero_overhead(orders_connector: _SharedDuckDBConnector) -> None:
    # No observers registered: run must succeed identically and registry stays empty.
    result = run_suite(_two_check_suite(), orders_connector, optimize=False)
    assert result.status == Status.PASS
    assert iter_observers() == ()


def test_observer_exception_does_not_break_run(
    orders_connector: _SharedDuckDBConnector,
) -> None:
    class _BoomObserver:
        def on_check_complete(self, result: CheckResult) -> None:
            raise RuntimeError("boom")

    register_observer(_BoomObserver())
    # Must complete normally despite the misbehaving observer.
    result = run_suite(_two_check_suite(), orders_connector, optimize=False)
    assert result.status == Status.PASS


def test_on_error_emitted_and_reraised() -> None:
    obs = _FakeObserver()
    register_observer(obs)

    class _FailingConnector:
        def connect(self) -> Any:
            raise ConnectionError("cannot reach database")

        def disconnect(self, connection: Any) -> None:
            pass

    with pytest.raises(ConnectionError):
        run_suite(_two_check_suite(), _FailingConnector(), optimize=False)

    events = [name for name, _ in obs.calls]
    assert "on_error" in events
    error_payload = next(p for n, p in obs.calls if n == "on_error")
    assert error_payload == ("obs_suite", "ConnectionError")


# --------------------------------------------------------------------------- #
# Redaction
# --------------------------------------------------------------------------- #


def test_redact_connection_string_password() -> None:
    raw = "postgresql://admin:s3cr3tP@ss@db.internal:5432/prod"
    out = redact_string(raw)
    assert "s3cr3tP" not in out
    assert "***REDACTED***" in out
    assert "admin" in out
    assert "db.internal" in out


def test_redact_bearer_token() -> None:
    out = redact_string("X-Auth: Bearer eyJabc123DEFtoken456")
    assert "eyJabc123DEFtoken456" not in out
    assert "***REDACTED***" in out


def test_redact_dict_sensitive_keys() -> None:
    payload = {
        "host": "localhost",
        "password": "hunter2",
        "api_key": "AK-LIVE-9999",
        "nested": {"token": "abc", "ok": "visible"},
    }
    out = redact(payload)
    assert out["host"] == "localhost"
    assert out["password"] == "***REDACTED***"
    assert out["api_key"] == "***REDACTED***"
    assert out["nested"]["token"] == "***REDACTED***"
    assert out["nested"]["ok"] == "visible"


# --------------------------------------------------------------------------- #
# Prometheus metrics
# --------------------------------------------------------------------------- #


def test_prometheus_metrics_after_two_check_run(
    orders_connector: _SharedDuckDBConnector,
) -> None:
    pytest.importorskip("prometheus_client")
    from provero.observability import PrometheusObserver, render_metrics

    obs = PrometheusObserver()
    register_observer(obs)

    run_suite(_two_check_suite(), orders_connector, optimize=False)

    text = render_metrics(obs)
    assert "provero_checks_total" in text
    assert "provero_check_duration_seconds" in text
    assert "provero_suite_score" in text

    # Two checks, both passing -> checks_total{status="pass"} == 2.0
    pass_line = next(
        line
        for line in text.splitlines()
        if line.startswith("provero_checks_total{") and 'status="pass"' in line
    )
    assert pass_line.endswith(" 2.0")

    # suite_score gauge reflects the 100.0 quality score for the passing suite.
    score_line = next(
        line
        for line in text.splitlines()
        if line.startswith("provero_suite_score{") and 'suite="obs_suite"' in line
    )
    assert score_line.endswith(" 100.0")


def test_prometheus_private_registries_no_duplicate_error() -> None:
    pytest.importorskip("prometheus_client")
    from provero.observability import PrometheusObserver

    # Instantiating multiple observers must not raise "Duplicated timeseries".
    a = PrometheusObserver()
    b = PrometheusObserver()
    assert a.registry is not b.registry


# --------------------------------------------------------------------------- #
# Audit log
# --------------------------------------------------------------------------- #


def test_audit_log_has_required_fields(orders_connector: _SharedDuckDBConnector) -> None:
    stream = io.StringIO()
    register_observer(AuditLogObserver(stream=stream))

    run_suite(_two_check_suite(), orders_connector, optimize=False)

    lines = [line for line in stream.getvalue().splitlines() if line.strip()]
    records = [json.loads(line) for line in lines]
    events = [r["event"] for r in records]
    assert "suite_start" in events
    assert "suite_complete" in events

    start = next(r for r in records if r["event"] == "suite_start")
    for field in ("event", "run_id", "suite", "config_hash", "status", "counts", "timestamp"):
        assert field in start
    assert start["suite"] == "obs_suite"
    assert start["config_hash"]

    complete = next(r for r in records if r["event"] == "suite_complete")
    assert complete["run_id"] == start["run_id"]
    assert complete["config_hash"] == start["config_hash"]
    assert complete["status"] == "pass"
    assert complete["counts"]["total"] == 2
    assert complete["counts"]["passed"] == 2


def test_audit_log_to_file(orders_connector: _SharedDuckDBConnector, tmp_path: Any) -> None:
    log_path = tmp_path / "audit.jsonl"
    register_observer(AuditLogObserver(path=log_path))

    run_suite(_two_check_suite(), orders_connector, optimize=False)

    content = log_path.read_text(encoding="utf-8")
    records = [json.loads(line) for line in content.splitlines() if line.strip()]
    assert any(r["event"] == "suite_complete" for r in records)


def test_audit_log_redacts_secrets() -> None:
    stream = io.StringIO()
    obs = AuditLogObserver(stream=stream)
    suite = SuiteConfig(
        name="secret_suite",
        source=SourceConfig(
            type="postgres",
            connection="postgresql://u:supersecret@h:5432/db",
            table="t",
        ),
    )
    obs.on_error(suite, "rid-1", ConnectionError("auth failed for postgresql://u:supersecret@h/db"))

    out = stream.getvalue()
    assert "supersecret" not in out
    assert "***REDACTED***" in out
