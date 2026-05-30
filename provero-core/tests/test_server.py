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

"""Tests for the Provero FastAPI server, auth, and scheduler."""

from __future__ import annotations

import time
from pathlib import Path

import duckdb
import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from provero.observability import hooks
from provero.server.app import create_app
from provero.server.scheduler import SuiteScheduler


@pytest.fixture(autouse=True)
def _clear_observers():
    """Avoid leaking the per-app Prometheus observer across tests."""
    hooks.clear_observers()
    yield
    hooks.clear_observers()


@pytest.fixture
def duckdb_db(tmp_path: Path) -> Path:
    """File-based DuckDB with an orders table (5 rows)."""
    db_path = tmp_path / "data.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE orders (order_id INTEGER, customer_id VARCHAR, amount DOUBLE)")
    conn.execute("INSERT INTO orders VALUES (1,'C1',10.0),(2,'C2',20.0),(3,'C3',30.0)")
    conn.close()
    return db_path


@pytest.fixture
def config(duckdb_db: Path) -> dict:
    """In-memory Provero config pointing at the file-based DuckDB."""
    return {
        "source": {
            "type": "duckdb",
            "connection": str(duckdb_db),
            "table": "orders",
        },
        "checks": [
            {"not_null": "order_id"},
            {"unique": "order_id"},
            {"row_count": {"min": 1}},
        ],
    }


@pytest.fixture
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "results.db"


def _client(config: dict, store_path: Path, api_keys=None) -> TestClient:
    app = create_app(config=config, db_path=store_path, api_keys=api_keys)
    return TestClient(app)


def test_health_returns_200(config: dict, store_path: Path) -> None:
    with _client(config, store_path) as client:
        resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_ready_returns_200(config: dict, store_path: Path) -> None:
    with _client(config, store_path) as client:
        resp = client.get("/ready")
    assert resp.status_code == 200
    assert resp.json()["ready"] is True


def test_list_suites(config: dict, store_path: Path) -> None:
    with _client(config, store_path) as client:
        resp = client.get("/suites")
    assert resp.status_code == 200
    suites = resp.json()["suites"]
    assert len(suites) == 1
    assert suites[0]["name"] == "default"
    assert suites[0]["check_count"] == 3


def test_run_suite_returns_results(config: dict, store_path: Path) -> None:
    with _client(config, store_path) as client:
        resp = client.post("/suites/default/run")
    assert resp.status_code == 200
    body = resp.json()
    assert body["suite_name"] == "default"
    assert body["status"] == "pass"
    assert body["total"] == 3
    assert len(body["checks"]) == 3


def test_run_unknown_suite_404(config: dict, store_path: Path) -> None:
    with _client(config, store_path) as client:
        resp = client.post("/suites/nope/run")
    assert resp.status_code == 404


def test_run_persists_to_store_and_runs_endpoints(config: dict, store_path: Path) -> None:
    with _client(config, store_path) as client:
        run = client.post("/suites/default/run").json()
        run_id = run["checks"][0]["run_id"]

        runs = client.get("/runs").json()["runs"]
        assert any(r["id"] == run_id for r in runs)

        detail = client.get(f"/runs/{run_id}")
        assert detail.status_code == 200
        body = detail.json()
        assert body["run"]["id"] == run_id
        assert len(body["checks"]) == 3

        missing = client.get("/runs/does-not-exist")
        assert missing.status_code == 404


def test_metrics_returns_prometheus_text(config: dict, store_path: Path) -> None:
    with _client(config, store_path) as client:
        client.post("/suites/default/run")
        resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]
    assert "provero_checks_total" in resp.text


def test_auth_blocks_wrong_and_blank_key(config: dict, store_path: Path) -> None:
    with _client(config, store_path, api_keys=["secret-key"]) as client:
        # No header -> 401.
        assert client.get("/suites").status_code == 401
        # Wrong key -> 401.
        assert client.get("/suites", headers={"X-API-Key": "wrong"}).status_code == 401
        # Blank key -> 401.
        assert client.get("/suites", headers={"X-API-Key": ""}).status_code == 401
        # Correct key -> 200.
        ok = client.get("/suites", headers={"X-API-Key": "secret-key"})
        assert ok.status_code == 200


def test_auth_never_echoes_key(config: dict, store_path: Path) -> None:
    with _client(config, store_path, api_keys=["secret-key"]) as client:
        resp = client.get("/suites", headers={"X-API-Key": "leak-me-please"})
    assert resp.status_code == 401
    assert "leak-me-please" not in resp.text


def test_no_keys_configured_allows(config: dict, store_path: Path) -> None:
    with _client(config, store_path, api_keys=[]) as client:
        assert client.get("/suites").status_code == 200


def test_runs_limit_is_bounded(config: dict, store_path: Path) -> None:
    """An out-of-range ``?limit`` is rejected with 422, never a full scan."""
    with _client(config, store_path) as client:
        # SQLite treats LIMIT -1 as unbounded; the API must refuse it.
        assert client.get("/runs?limit=-1").status_code == 422
        assert client.get("/runs?limit=0").status_code == 422
        # Above the cap is also refused.
        assert client.get("/runs?limit=100000").status_code == 422
        # An in-range value is accepted.
        assert client.get("/runs?limit=5").status_code == 200


def test_ready_failure_redacts_connection_string(
    config: dict, store_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A connection string inside a readiness error is scrubbed, not echoed."""
    import provero.store.sqlite as sqlite_module

    def _boom(self: object, *args: object, **kwargs: object) -> None:
        raise RuntimeError("could not connect to postgresql://user:s3cr3t@db/prod")

    # Force the per-request store open to fail with an exception carrying a secret.
    monkeypatch.setattr(sqlite_module.SQLiteStore, "__init__", _boom, raising=True)

    with _client(config, store_path) as client:
        resp = client.get("/ready")

    assert resp.status_code == 503
    body = resp.text
    assert "s3cr3t" not in body
    assert "***REDACTED***" in body


def test_scheduler_executes_suite_at_least_once(config: dict, store_path: Path) -> None:
    from provero.core.engine import Engine

    suite = Engine.from_dict(config).config.suites[0]
    scheduler = SuiteScheduler(suite, store_path, interval_seconds=0.05)
    scheduler.start()
    try:
        deadline = time.monotonic() + 5.0
        while scheduler.run_count < 1 and time.monotonic() < deadline:
            time.sleep(0.02)
    finally:
        scheduler.stop()

    assert scheduler.last_error is None
    assert scheduler.run_count >= 1
    assert scheduler.last_run_id is not None
    assert not scheduler.is_running

    # The run was persisted and is readable from the store (own-thread open).
    from provero.store.sqlite import SQLiteStore

    store = SQLiteStore(store_path)
    try:
        history = store.get_history()
    finally:
        store.close()
    assert len(history) >= 1
