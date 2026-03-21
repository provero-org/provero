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

"""``provero validate`` command."""

from __future__ import annotations

import json as json_mod
from pathlib import Path
from typing import Annotated

import typer

from provero.cli.utils import _echo, console


def validate(
    config_path: Annotated[
        Path | None,
        typer.Argument(help="Path to config file (alternative to -c)."),
    ] = None,
    config: Annotated[
        Path,
        typer.Option(
            "--config",
            "-c",
            help="Path to the Provero YAML configuration file to validate.",
        ),
    ] = Path("provero.yaml"),
    schema_only: Annotated[
        bool,
        typer.Option(
            "--schema-only",
            help=("Only run JSON Schema validation; skip semantic compilation."),
        ),
    ] = False,
) -> None:
    """Validate provero.yaml syntax without executing any checks.

    Performs two levels of validation:

      1. JSON Schema validation (structure and types).

      2. Semantic compilation (connector resolution, check definitions).

    Use --schema-only to limit validation to the first step.

    Examples:

        provero validate

        provero validate -c staging.yaml --schema-only
    """
    config = config_path or config
    if not config.exists():
        console.print(f"[red]Config file not found: {config}[/red]")
        raise typer.Exit(1)

    import importlib.resources

    import yaml
    from jsonschema import ValidationError
    from jsonschema import validate as json_validate

    # Step 1: Validate against JSON Schema
    schema = None
    try:
        schema_ref = importlib.resources.files("provero").joinpath("schema.json")
        schema = json_mod.loads(schema_ref.read_text(encoding="utf-8"))
    except (FileNotFoundError, TypeError):
        pass

    if schema is None:
        console.print("[red]Schema file not found. Cannot validate.[/red]")
        raise typer.Exit(1)

    with config.open() as f:
        raw = yaml.safe_load(f)
    try:
        json_validate(instance=raw, schema=schema)
        _echo("[green]Schema validation passed.[/green]")
    except ValidationError as e:
        path = " -> ".join(str(p) for p in e.absolute_path) if e.absolute_path else "root"
        console.print(f"[red]Schema validation failed at '{path}':[/red] {e.message}")
        raise typer.Exit(1) from None

    if schema_only:
        return

    # Step 2: Compile to verify semantic correctness
    from provero.core.compiler import compile_file

    try:
        provero_config = compile_file(config)
        total_checks = sum(len(s.checks) for s in provero_config.suites)
        _echo(
            f"[green]Valid.[/green] {len(provero_config.suites)} suite(s), {total_checks} check(s)"
        )
    except Exception as e:
        console.print(f"[red]Invalid:[/red] {e}")
        raise typer.Exit(1) from None
