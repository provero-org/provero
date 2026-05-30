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

"""FastAPI application factory for Provero server mode.

``fastapi`` and ``uvicorn`` are optional and imported lazily inside
:func:`create_app` / :func:`serve`, so importing :mod:`provero` without the
``server`` extra never fails. Both functions raise a clear ``RuntimeError`` if
the extra is missing.

Endpoints:
    - ``GET  /health``            liveness probe (always 200)
    - ``GET  /ready``             readiness probe (config + store usable)
    - ``GET  /suites``            list configured suites
    - ``POST /suites/{name}/run`` run a suite via the engine, persist, return it
    - ``GET  /runs``              list recent runs from the store
    - ``GET  /runs/{run_id}``     run detail with its check results
    - ``GET  /metrics``           Prometheus text exposition

The store (:class:`~provero.store.sqlite.SQLiteStore`) is opened per request,
inside the handling thread, because SQLite connections are thread-bound and
FastAPI dispatches sync handlers across a worker threadpool.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from provero.core.engine import Engine
from provero.server.auth import make_api_key_dependency
from provero.server.models import (
    CheckResultRecord,
    HealthResponse,
    RunDetailResponse,
    RunListResponse,
    RunSummary,
    SuiteListResponse,
    SuiteSummary,
)
from provero.store.sqlite import DEFAULT_DB_PATH

if TYPE_CHECKING:
    from collections.abc import Iterable

    from fastapi import FastAPI  # type: ignore[import-not-found]

    from provero.observability.metrics import PrometheusObserver

_MISSING_MSG = (
    "Provero server mode requires the 'server' extra. "
    "Install it with: pip install 'provero[server]' "
    "(or pip install fastapi uvicorn)."
)


def _require_fastapi() -> Any:
    try:
        import fastapi
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised via guard test
        raise RuntimeError(_MISSING_MSG) from exc
    return fastapi


def _build_engine(config_path: str | Path | None, config: dict[str, Any] | None) -> Engine:
    if (config_path is None) == (config is None):
        msg = "Provide exactly one of 'config_path' or 'config'."
        raise ValueError(msg)
    if config is not None:
        return Engine.from_dict(config)
    assert config_path is not None  # narrow for mypy
    return Engine(config_path)


def create_app(
    *,
    config_path: str | Path | None = None,
    config: dict[str, Any] | None = None,
    db_path: str | Path = DEFAULT_DB_PATH,
    api_keys: Iterable[str] | None = None,
) -> FastAPI:
    """Create the Provero FastAPI application.

    Args:
        config_path: Path to a ``provero.yaml`` config (mutually exclusive with
            ``config``).
        config: In-memory config dict (mutually exclusive with ``config_path``).
        db_path: SQLite result store path. Opened per request, in the handling
            thread, to respect SQLite's thread-bound connections.
        api_keys: Allowed API keys. When omitted, falls back to the
            ``PROVERO_API_KEYS`` env var; when neither is set, auth is disabled
            and all requests are allowed.

    Returns:
        A configured FastAPI application.

    Raises:
        RuntimeError: if the ``server`` extra (fastapi) is not installed.
    """
    _require_fastapi()  # friendly error if the 'server' extra is missing
    from fastapi import (  # type: ignore[import-not-found]
        Depends,
        FastAPI,
        HTTPException,
        Query,
        status,
    )
    from fastapi.responses import PlainTextResponse  # type: ignore[import-not-found]

    engine = _build_engine(config_path, config)
    store_path = Path(db_path)
    require_key = make_api_key_dependency(api_keys)

    # A single Prometheus observer is registered so the engine emits metrics
    # for runs triggered through this app. /metrics renders *this* observer's
    # exposition, keeping each app's metrics isolated from any other app.
    observer = _make_observer()

    app = FastAPI(
        title="Provero",
        description="Vendor-neutral declarative data quality engine - server mode.",
        version=_provero_version(),
    )
    app.state.engine = engine
    app.state.db_path = store_path
    app.state.observer = observer

    def _open_store() -> Any:
        from provero.store.sqlite import SQLiteStore

        return SQLiteStore(store_path)

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(status="ok", ready=True)

    @app.get("/ready", response_model=HealthResponse)
    def ready() -> HealthResponse:
        try:
            # Config is parsed at construction; touching it confirms usability.
            _ = engine.config.suites
            store = _open_store()
            store.close()
        except Exception as exc:
            # An exception's text can embed a connection string or credential
            # (e.g. a SQLAlchemy URL). Scrub it before it reaches the response.
            from provero.observability.redaction import redact_string

            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Not ready: {redact_string(str(exc))}",
            ) from exc
        return HealthResponse(status="ok", ready=True)

    @app.get("/suites", response_model=SuiteListResponse, dependencies=[Depends(require_key)])
    def list_suites() -> SuiteListResponse:
        summaries = [
            SuiteSummary(
                name=suite.name,
                source_type=suite.source.type,
                table=suite.source.table,
                check_count=len(suite.checks),
                tags=list(suite.tags),
                schedule=suite.schedule,
            )
            for suite in engine.config.suites
        ]
        return SuiteListResponse(suites=summaries)

    @app.post("/suites/{name}/run", dependencies=[Depends(require_key)])
    def run_suite_endpoint(name: str) -> Any:
        from provero.connectors.factory import create_connector
        from provero.core.engine import run_suite

        suite = next((s for s in engine.config.suites if s.name == name), None)
        if suite is None:
            available = ", ".join(s.name for s in engine.config.suites) or "none"
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Suite '{name}' not found. Available suites: {available}.",
            )
        connector = create_connector(suite.source)
        result = run_suite(suite, connector)
        store = _open_store()
        try:
            store.save_result(result)
        finally:
            store.close()
        return result

    @app.get("/runs", response_model=RunListResponse, dependencies=[Depends(require_key)])
    def list_runs(
        suite: str | None = None,
        limit: int = Query(default=20, ge=1, le=1000),
    ) -> RunListResponse:
        # ``limit`` is bounded at the API boundary: SQLite treats ``LIMIT -1`` as
        # unbounded, and an unbounded/huge limit on untrusted input is a resource
        # exhaustion vector. Out-of-range values yield a 422, not a full scan.
        store = _open_store()
        try:
            rows = store.get_history(suite_name=suite, limit=limit)
        finally:
            store.close()
        return RunListResponse(runs=[RunSummary(**row) for row in rows])

    @app.get(
        "/runs/{run_id}",
        response_model=RunDetailResponse,
        dependencies=[Depends(require_key)],
    )
    def get_run(run_id: str) -> RunDetailResponse:
        store = _open_store()
        try:
            history = store.get_history(limit=1000)
            run_row = next((r for r in history if r["id"] == run_id), None)
            if run_row is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Run '{run_id}' not found.",
                )
            checks = store.get_run_details(run_id)
        finally:
            store.close()
        return RunDetailResponse(
            run=RunSummary(**run_row),
            checks=[CheckResultRecord(**c) for c in checks],
        )

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics() -> str:
        if observer is None:
            return "# prometheus extra not installed; no metrics available\n"
        return observer.exposition()

    return app


def _make_observer() -> PrometheusObserver | None:
    """Build and register a Prometheus observer, or None if extra is missing."""
    try:
        from provero.observability import hooks
        from provero.observability.metrics import PrometheusObserver
    except ModuleNotFoundError:  # pragma: no cover
        return None
    try:
        observer = PrometheusObserver()
    except RuntimeError:
        # prometheus_client not installed: metrics endpoint degrades gracefully.
        return None
    hooks.register_observer(observer)
    return observer


def _provero_version() -> str:
    from provero import __version__

    return __version__


def serve(
    *,
    config_path: str | Path | None = None,
    config: dict[str, Any] | None = None,
    db_path: str | Path = DEFAULT_DB_PATH,
    api_keys: Iterable[str] | None = None,
    host: str = "127.0.0.1",
    port: int = 8000,
) -> None:
    """Build the app and run it with uvicorn (blocking).

    Raises:
        RuntimeError: if the ``server`` extra (uvicorn) is not installed.
    """
    try:
        import uvicorn  # type: ignore[import-not-found]
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised via guard test
        raise RuntimeError(_MISSING_MSG) from exc

    app = create_app(
        config_path=config_path,
        config=config,
        db_path=db_path,
        api_keys=api_keys,
    )
    uvicorn.run(app, host=host, port=port)
