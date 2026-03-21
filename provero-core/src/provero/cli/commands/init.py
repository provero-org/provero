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

"""``provero init`` command."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from provero.cli.utils import TEMPLATE, _echo, console


def init(
    path: Annotated[
        Path,
        typer.Argument(
            help=("Destination path for the generated config file. Defaults to provero.yaml."),
        ),
    ] = Path("provero.yaml"),
    from_source: Annotated[
        str | None,
        typer.Option(
            "--from-source",
            help=(
                "Auto-generate checks by profiling a live data source. "
                "Format: TYPE:TABLE (e.g. duckdb:orders, postgres:users)."
            ),
        ),
    ] = None,
) -> None:
    """Create a new provero.yaml configuration file.

    Generates a starter template you can customise. Use --from-source to
    profile a live table and pre-populate checks automatically.

    Examples:

        provero init

        provero init my_checks.yaml

        provero init --from-source duckdb:orders
    """
    if path.exists():
        console.print(f"[yellow]File already exists: {path}[/yellow]")
        raise typer.Exit(1)

    if from_source:
        parts = from_source.split(":", 1)
        if len(parts) != 2 or not parts[1]:
            console.print("[red]Format: --from-source TYPE:TABLE (e.g. duckdb:orders)[/red]")
            raise typer.Exit(1)

        source_type, table_name = parts

        from provero.connectors.factory import create_connector
        from provero.core.compiler import SourceConfig
        from provero.core.profiler import (
            checks_to_yaml,
            profile_table,
            suggest_checks,
        )

        source = SourceConfig(type=source_type, table=table_name)
        try:
            connector = create_connector(source)
            connection = connector.connect()
        except Exception as e:
            console.print(f"[red]Cannot connect to {source_type}: {e}[/red]")
            raise typer.Exit(1) from None
        try:
            profile = profile_table(connection, table_name)
        except Exception as e:
            connector.disconnect(connection)
            console.print(f"[red]Cannot profile table '{table_name}': {e}[/red]")
            raise typer.Exit(1) from None
        connector.disconnect(connection)

        checks = suggest_checks(profile)
        yaml_content = checks_to_yaml(checks, source_type, table_name)
        path.write_text(yaml_content)
        _echo(
            f"[green]Created {path} with {len(checks)} suggested checks"
            f" from profiling {table_name}[/green]"
        )
        _echo("Review the file and run: provero run")
    else:
        path.write_text(TEMPLATE)
        _echo(f"[green]Created {path}[/green]")
        _echo("Edit the file and run: provero run")
