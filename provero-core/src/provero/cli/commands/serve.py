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

"""``provero serve`` command."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from provero.cli.main import app


@app.command()
def serve(
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to provero.yaml."),
    ] = None,
    host: Annotated[str, typer.Option(help="Bind host.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Bind port.")] = 8000,
    api_key: Annotated[
        list[str] | None,
        typer.Option(
            "--api-key",
            help=(
                "Allowed API key (repeatable). If omitted, PROVERO_API_KEYS "
                "env is used; if neither is set, auth is disabled."
            ),
        ),
    ] = None,
    db_path: Annotated[
        Path | None,
        typer.Option("--db-path", help="SQLite result store path."),
    ] = None,
) -> None:
    """Run Provero in server mode (REST API + scheduler-capable).

    Starts a FastAPI application exposing health, suite, run, and metrics
    endpoints. Requires the 'server' extra (fastapi + uvicorn).

    Examples:

        provero serve

        provero serve -c production.yaml --host 0.0.0.0 --port 9000

        provero serve --api-key secret1 --api-key secret2
    """
    from provero.server.app import serve as _serve
    from provero.store.sqlite import DEFAULT_DB_PATH

    cfg = config or Path("provero.yaml")
    try:
        _serve(
            config_path=cfg,
            db_path=db_path or DEFAULT_DB_PATH,
            api_keys=api_key,  # None -> env fallback inside auth.resolve_api_keys
            host=host,
            port=port,
        )
    except RuntimeError as exc:  # 'server' extra missing
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
