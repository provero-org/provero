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

"""Scale-hardening tests: pooling kwargs, retry/backoff, and leak safety."""

from __future__ import annotations

import random
from pathlib import Path
from typing import Any

import pytest

import provero.connectors.postgres as postgres_mod
from provero.connectors.pool import PoolConfig, build_connect_args, build_engine_kwargs
from provero.connectors.postgres import PostgresConnector, SQLAlchemyConnector
from provero.connectors.retry import RetryConfig, is_transient_error, retry_call

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_url(tmp_path: Path) -> str:
    """A file-based SQLite URL with a single populated table."""
    import sqlite3

    db_path = tmp_path / "scale.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t (id INTEGER)")
    conn.executemany("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])
    conn.commit()
    conn.close()
    return f"sqlite:///{db_path}"


# ---------------------------------------------------------------------------
# Pool kwargs assembly
# ---------------------------------------------------------------------------


class TestPoolConfig:
    def test_empty_config_yields_no_kwargs(self) -> None:
        """Default config must not change create_engine behavior at all."""
        assert build_engine_kwargs("postgresql://x/y", PoolConfig()) == {}

    def test_pool_args_forwarded(self) -> None:
        cfg = PoolConfig(
            pool_size=10,
            max_overflow=5,
            pool_pre_ping=True,
            pool_recycle=1800,
            pool_timeout=30,
        )
        kwargs = build_engine_kwargs("postgresql://x/y", cfg)
        assert kwargs == {
            "pool_size": 10,
            "max_overflow": 5,
            "pool_pre_ping": True,
            "pool_recycle": 1800,
            "pool_timeout": 30,
        }

    def test_unset_fields_are_omitted(self) -> None:
        kwargs = build_engine_kwargs("postgresql://x/y", PoolConfig(pool_pre_ping=True))
        assert kwargs == {"pool_pre_ping": True}

    def test_connect_timeout_maps_per_driver(self) -> None:
        cfg = PoolConfig(connect_timeout=7)
        assert build_connect_args("postgresql://x/y", cfg) == {"connect_timeout": 7}
        assert build_connect_args("mysql://x/y", cfg) == {"connect_timeout": 7}
        assert build_connect_args("snowflake://x/y", cfg) == {"login_timeout": 7}

    def test_unknown_driver_gets_no_timeout(self) -> None:
        """SQLite has no known timeout knob, so connect_args stays empty."""
        cfg = PoolConfig(connect_timeout=7, query_timeout=3)
        assert build_connect_args("sqlite:///x.db", cfg) == {}
        assert build_engine_kwargs("sqlite:///x.db", cfg) == {}

    def test_query_timeout_maps_per_driver(self) -> None:
        cfg = PoolConfig(query_timeout=12)
        assert build_connect_args("snowflake://x/y", cfg) == {"network_timeout": 12}
        assert build_connect_args("mysql://x/y", cfg) == {"read_timeout": 12}


class TestPoolForwardingToCreateEngine:
    """Pool kwargs reach create_engine for both connector classes."""

    def test_sqlalchemy_connector_forwards_kwargs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, Any] = {}

        class _StubEngine:
            def connect(self) -> str:
                return "conn-sentinel"

        def fake_create_engine(url: str, **kwargs: Any) -> _StubEngine:
            captured["url"] = url
            captured["kwargs"] = kwargs
            return _StubEngine()

        monkeypatch.setattr(postgres_mod, "create_engine", fake_create_engine)

        connector = SQLAlchemyConnector(
            "postgresql://x/y",
            pool=PoolConfig(pool_size=4, max_overflow=2, pool_pre_ping=True),
        )
        connector.connect()
        assert captured["url"] == "postgresql://x/y"
        assert captured["kwargs"] == {
            "pool_size": 4,
            "max_overflow": 2,
            "pool_pre_ping": True,
        }

    def test_postgres_connector_forwards_kwargs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, Any] = {}

        class _StubEngine:
            def connect(self) -> str:
                return "conn-sentinel"

        def fake_create_engine(url: str, **kwargs: Any) -> _StubEngine:
            captured["kwargs"] = kwargs
            return _StubEngine()

        monkeypatch.setattr(postgres_mod, "create_engine", fake_create_engine)

        connector = PostgresConnector("postgresql://x/y", pool=PoolConfig(pool_recycle=900))
        connector.connect()
        assert captured["kwargs"] == {"pool_recycle": 900}


# ---------------------------------------------------------------------------
# Retry / backoff
# ---------------------------------------------------------------------------


class _FlakyError(Exception):
    """Stand-in for a transient connection error (treated as retryable)."""


def _always_retryable(_exc: BaseException) -> bool:
    return True


class TestRetryCall:
    def test_succeeds_after_n_transient_failures(self) -> None:
        calls = {"n": 0}
        slept: list[float] = []

        def flaky() -> str:
            calls["n"] += 1
            if calls["n"] < 3:
                raise _FlakyError("boom")
            return "ok"

        result = retry_call(
            flaky,
            RetryConfig(attempts=5, base_delay=0.01, jitter=False),
            is_retryable=_always_retryable,
            sleep=slept.append,
        )
        assert result == "ok"
        assert calls["n"] == 3  # failed twice, succeeded on third
        assert len(slept) == 2  # one sleep between each retry

    def test_exhausts_attempts_then_raises(self) -> None:
        calls = {"n": 0}
        slept: list[float] = []

        def always_fails() -> str:
            calls["n"] += 1
            raise _FlakyError("boom")

        with pytest.raises(_FlakyError):
            retry_call(
                always_fails,
                RetryConfig(attempts=4, base_delay=0.01, jitter=False),
                is_retryable=_always_retryable,
                sleep=slept.append,
            )
        assert calls["n"] == 4  # exactly the configured number of attempts
        assert len(slept) == 3  # slept between attempts, not after the last

    def test_non_transient_error_not_retried(self) -> None:
        calls = {"n": 0}

        def fails_permanently() -> str:
            calls["n"] += 1
            raise ValueError("programming error")

        with pytest.raises(ValueError, match="programming error"):
            retry_call(
                fails_permanently,
                RetryConfig(attempts=5, base_delay=0.01, jitter=False),
                is_retryable=lambda _e: False,
                sleep=lambda _s: None,
            )
        assert calls["n"] == 1  # not retried

    def test_attempts_one_disables_retry(self) -> None:
        calls = {"n": 0}

        def flaky() -> str:
            calls["n"] += 1
            raise _FlakyError("boom")

        with pytest.raises(_FlakyError):
            retry_call(
                flaky,
                RetryConfig(attempts=1),
                is_retryable=_always_retryable,
                sleep=lambda _s: None,
            )
        assert calls["n"] == 1

    def test_backoff_is_bounded_and_jittered(self) -> None:
        slept: list[float] = []

        def always_fails() -> str:
            raise _FlakyError("boom")

        with pytest.raises(_FlakyError):
            retry_call(
                always_fails,
                RetryConfig(attempts=4, base_delay=1.0, max_delay=2.0, jitter=True),
                is_retryable=_always_retryable,
                sleep=slept.append,
                rng=random.Random(0),
            )
        # Every delay must respect max_delay; jitter keeps them in [0, cap].
        assert all(0.0 <= d <= 2.0 for d in slept)


class TestTransientClassifier:
    def test_missing_table_is_not_transient(self, sqlite_url: str) -> None:
        """SQLite 'no such table' is OperationalError but must NOT retry."""
        from sqlalchemy.exc import OperationalError

        connector = SQLAlchemyConnector(sqlite_url)
        conn = connector.connect()
        try:
            err: OperationalError | None = None
            try:
                conn.execute("SELECT * FROM does_not_exist")
            except OperationalError as exc:
                err = exc
            assert err is not None
            assert is_transient_error(err) is False
        finally:
            connector.disconnect(conn)

    def test_disconnect_style_message_is_transient(self) -> None:
        from sqlalchemy.exc import OperationalError

        exc = OperationalError("stmt", {}, Exception("server closed the connection"))
        assert is_transient_error(exc) is True


class TestConnectorRetriesQueries:
    def test_query_retries_then_succeeds(
        self, sqlite_url: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A flaky underlying execute is retried by the connection wrapper.

        The injected failure is a SQLAlchemy disconnect-style ``OperationalError``
        so the real (un-patched) transient classifier retries it, exercising the
        end-to-end retry path inside ``SQLAlchemyConnection.execute``.
        """
        from sqlalchemy.exc import OperationalError

        connector = SQLAlchemyConnector(
            sqlite_url,
            retry=RetryConfig(attempts=4, base_delay=0.0, jitter=False),
        )
        conn = connector.connect()
        try:
            real_execute = conn._conn.execute
            state = {"n": 0}

            def flaky_execute(*args: Any, **kwargs: Any) -> Any:
                state["n"] += 1
                if state["n"] < 3:
                    raise OperationalError("stmt", {}, Exception("server closed the connection"))
                return real_execute(*args, **kwargs)

            monkeypatch.setattr(conn._conn, "execute", flaky_execute)
            rows = conn.execute("SELECT COUNT(*) AS c FROM t")
            assert rows[0]["c"] == 3
            assert state["n"] == 3
        finally:
            connector.disconnect(conn)


# ---------------------------------------------------------------------------
# Leak safety
# ---------------------------------------------------------------------------


class TestNoLeakOnError:
    def test_no_checked_out_connection_after_query_error(self, sqlite_url: str) -> None:
        """Using the context manager must release the connection even on error."""
        from sqlalchemy.exc import OperationalError

        connector = SQLAlchemyConnector(sqlite_url)
        conn = connector.connect()
        engine = conn._engine
        try:
            with pytest.raises(OperationalError), conn:
                conn.execute("SELECT * FROM no_such_table")
        finally:
            # The wrapped connection must be closed (returned to the pool).
            assert conn._conn.closed is True
            assert engine.pool.checkedout() == 0
            engine.dispose()

    def test_disconnect_releases_and_disposes(self, sqlite_url: str) -> None:
        connector = SQLAlchemyConnector(sqlite_url)
        conn = connector.connect()
        engine = conn._engine
        connector.disconnect(conn)
        assert conn._conn.closed is True
        assert engine.pool.checkedout() == 0
