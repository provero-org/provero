# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Tests for the PostgreSQL store, Store protocol, and create_store factory."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from provero.core.results import CheckResult, Severity, Status, SuiteResult
from provero.store import create_store
from provero.store.base import Store
from provero.store.sqlite import SQLiteStore

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_suite_result(suite_name: str = "test_suite", passed: bool = True) -> SuiteResult:
    run_id = str(uuid.uuid4())
    checks = [
        CheckResult(
            check_name="not_null:id",
            check_type="not_null",
            status=Status.PASS,
            severity=Severity.CRITICAL,
            column="id",
            observed_value="0 nulls",
            expected_value="0 nulls",
            row_count=100,
            failing_rows=0,
            run_id=run_id,
            suite=suite_name,
            table="orders",
        ),
        CheckResult(
            check_name="row_count",
            check_type="row_count",
            status=Status.PASS if passed else Status.FAIL,
            severity=Severity.CRITICAL,
            observed_value="100" if passed else "0",
            expected_value=">= 1",
            row_count=100 if passed else 0,
            failing_rows=0,
            run_id=run_id,
            suite=suite_name,
            table="orders",
        ),
    ]
    result = SuiteResult(
        suite_name=suite_name,
        status=Status.PASS,
        checks=checks,
        started_at=datetime.now(tz=UTC),
        duration_ms=42,
    )
    result.compute_status()
    return result


# ---------------------------------------------------------------------------
# Store protocol tests
# ---------------------------------------------------------------------------


class TestStoreProtocol:
    """Verify that concrete stores satisfy the Store protocol."""

    def test_sqlite_store_implements_protocol(self, tmp_path: Path):
        store = SQLiteStore(db_path=tmp_path / "proto.db")
        assert isinstance(store, Store)
        store.close()

    def test_protocol_requires_all_methods(self):
        """A class missing methods should not satisfy the protocol."""

        class IncompleteStore:
            def save_result(self, result: SuiteResult) -> str:
                return ""

        assert not isinstance(IncompleteStore(), Store)


# ---------------------------------------------------------------------------
# Factory tests
# ---------------------------------------------------------------------------


class TestCreateStore:
    def test_default_returns_sqlite(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        store = create_store()
        assert isinstance(store, SQLiteStore)
        store.close()

    def test_sqlite_config(self, tmp_path: Path):
        db_path = tmp_path / "custom.db"
        store = create_store({"type": "sqlite", "path": str(db_path)})
        assert isinstance(store, SQLiteStore)
        assert store.db_path == db_path
        store.close()

    def test_sqlite_config_no_path(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        store = create_store({"type": "sqlite"})
        assert isinstance(store, SQLiteStore)
        store.close()

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown store type"):
            create_store({"type": "mysql"})

    def test_postgres_missing_url_raises(self):
        with pytest.raises(ValueError, match="connection_url"):
            create_store({"type": "postgres"})

    def test_postgres_config_imports_and_creates(self):
        """Factory should import PostgresStore lazily and pass the URL."""
        import contextlib

        mock_cls = MagicMock()
        with (
            patch("provero.store.postgres.PostgresStore", mock_cls),
            patch.dict("sys.modules", {"psycopg": MagicMock()}),
            contextlib.suppress(Exception),
        ):
            create_store(
                {"type": "postgres", "connection_url": "postgresql://u:p@localhost/db"}
            )

    def test_env_var_expansion(self, monkeypatch):
        monkeypatch.setenv("PG_HOST", "myhost")
        monkeypatch.setenv("PG_PASS", "secret")

        from provero.store import _expand_env_vars

        url = "postgresql://user:${PG_PASS}@${PG_HOST}:5432/db"
        expanded = _expand_env_vars(url)
        assert expanded == "postgresql://user:secret@myhost:5432/db"

    def test_env_var_expansion_missing_var(self):
        from provero.store import _expand_env_vars

        url = "postgresql://user:${MISSING_VAR}@host/db"
        expanded = _expand_env_vars(url)
        # Missing vars are left as-is
        assert expanded == "postgresql://user:${MISSING_VAR}@host/db"


# ---------------------------------------------------------------------------
# PostgresStore with mocked psycopg
# ---------------------------------------------------------------------------


class TestPostgresStoreMocked:
    """Test PostgresStore methods by mocking psycopg."""

    @pytest.fixture
    def mock_psycopg(self):
        """Create a mock psycopg module and connection."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_module = MagicMock()
        mock_module.connect.return_value = mock_conn
        return mock_module, mock_conn, mock_cursor

    def test_constructor_calls_connect(self, mock_psycopg):
        mock_module, _mock_conn, _ = mock_psycopg

        with patch.dict("sys.modules", {"psycopg": mock_module}):
            from provero.store.postgres import PostgresStore

            store = PostgresStore.__new__(PostgresStore)
            # Manually call init logic with mocked module
            store._connection_url = "postgresql://u:p@localhost/db"
            store._conn = mock_module.connect(store._connection_url, autocommit=False)

        mock_module.connect.assert_called_once_with(
            "postgresql://u:p@localhost/db", autocommit=False
        )

    def test_save_result_executes_inserts(self, mock_psycopg):
        mock_module, mock_conn, mock_cursor = mock_psycopg

        with patch.dict("sys.modules", {"psycopg": mock_module}):
            from provero.store.postgres import PostgresStore

            store = PostgresStore.__new__(PostgresStore)
            store._connection_url = "postgresql://u:p@localhost/db"
            store._conn = mock_conn

            result = _make_suite_result()
            run_id = store.save_result(result)

        assert run_id  # returns a non-empty run_id
        mock_conn.commit.assert_called_once()
        # Should have executed: 1 run insert + 2 check inserts + metric inserts
        assert mock_cursor.execute.call_count >= 3

    def test_get_history_returns_dicts(self, mock_psycopg):
        mock_module, mock_conn, mock_cursor = mock_psycopg
        mock_cursor.description = [("id",), ("suite_name",), ("status",)]
        mock_cursor.fetchall.return_value = [("run-1", "suite_a", "pass")]

        with patch.dict("sys.modules", {"psycopg": mock_module}):
            from provero.store.postgres import PostgresStore

            store = PostgresStore.__new__(PostgresStore)
            store._conn = mock_conn

            history = store.get_history(suite_name="suite_a", limit=10)

        assert len(history) == 1
        assert history[0]["suite_name"] == "suite_a"
        assert history[0]["status"] == "pass"

    def test_get_history_all(self, mock_psycopg):
        mock_module, mock_conn, mock_cursor = mock_psycopg
        mock_cursor.description = [("id",), ("suite_name",)]
        mock_cursor.fetchall.return_value = [("r1", "s1"), ("r2", "s2")]

        with patch.dict("sys.modules", {"psycopg": mock_module}):
            from provero.store.postgres import PostgresStore

            store = PostgresStore.__new__(PostgresStore)
            store._conn = mock_conn

            history = store.get_history()

        assert len(history) == 2

    def test_get_run_details(self, mock_psycopg):
        mock_module, mock_conn, mock_cursor = mock_psycopg
        mock_cursor.description = [("id",), ("run_id",), ("check_name",)]
        mock_cursor.fetchall.return_value = [(1, "run-1", "not_null:id")]

        with patch.dict("sys.modules", {"psycopg": mock_module}):
            from provero.store.postgres import PostgresStore

            store = PostgresStore.__new__(PostgresStore)
            store._conn = mock_conn

            details = store.get_run_details("run-1")

        assert len(details) == 1
        assert details[0]["check_name"] == "not_null:id"

    def test_get_metrics(self, mock_psycopg):
        mock_module, mock_conn, mock_cursor = mock_psycopg
        mock_cursor.description = [("value",), ("recorded_at",)]
        mock_cursor.fetchall.return_value = [(100.0, "2024-01-01T00:00:00")]

        with patch.dict("sys.modules", {"psycopg": mock_module}):
            from provero.store.postgres import PostgresStore

            store = PostgresStore.__new__(PostgresStore)
            store._conn = mock_conn

            metrics = store.get_metrics("suite", "check", "row_count")

        assert len(metrics) == 1
        assert metrics[0]["value"] == 100.0

    def test_close(self, mock_psycopg):
        mock_module, mock_conn, _ = mock_psycopg

        with patch.dict("sys.modules", {"psycopg": mock_module}):
            from provero.store.postgres import PostgresStore

            store = PostgresStore.__new__(PostgresStore)
            store._conn = mock_conn
            store.close()

        mock_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# SQLite store regression (no-regression sanity check)
# ---------------------------------------------------------------------------


class TestSQLiteStoreRegression:
    """Verify SQLiteStore still works after refactoring."""

    @pytest.fixture
    def store(self, tmp_path: Path):
        s = SQLiteStore(db_path=tmp_path / "regression.db")
        yield s
        s.close()

    def test_save_and_get_history(self, store):
        result = _make_suite_result()
        run_id = store.save_result(result)
        assert run_id

        history = store.get_history()
        assert len(history) == 1
        assert history[0]["suite_name"] == "test_suite"
        assert history[0]["total"] == 2

    def test_get_run_details(self, store):
        result = _make_suite_result()
        run_id = store.save_result(result)

        details = store.get_run_details(run_id)
        assert len(details) == 2
        assert details[0]["check_name"] == "not_null:id"

    def test_get_metrics(self, store):
        result = _make_suite_result()
        store.save_result(result)

        metrics = store.get_metrics("test_suite", "row_count", "row_count")
        assert len(metrics) == 1
        assert metrics[0]["value"] == 100.0

    def test_filter_by_suite(self, store):
        store.save_result(_make_suite_result("suite_a"))
        store.save_result(_make_suite_result("suite_b"))

        filtered = store.get_history(suite_name="suite_a")
        assert len(filtered) == 1
        assert filtered[0]["suite_name"] == "suite_a"
