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

"""``provero contract`` subcommands."""

from __future__ import annotations

import json as json_mod
from pathlib import Path
from typing import Annotated

import typer

from provero.cli.utils import _echo, console, is_quiet

contract_app = typer.Typer(
    name="contract",
    help=(
        "Data contract commands.\n\n"
        "Validate schemas and freshness rules against live data sources, "
        "or compare two versions of a contract to detect breaking changes."
    ),
    no_args_is_help=True,
)


def _resolve_contract_source(contract, provero_config):
    """Resolve a contract's source reference against config sources."""
    from provero.core.compiler import SourceConfig

    source_ref = contract.source
    if source_ref and source_ref in provero_config.sources:
        source = provero_config.sources[source_ref]
        return source.model_copy(update={"table": contract.table}) if contract.table else source

    # Fall back to the first suite's full source config (preserves connection
    # strings for file-based backends like DuckDB).
    if provero_config.suites:
        base = provero_config.suites[0].source
        updates: dict = {}
        if contract.table:
            updates["table"] = contract.table
        if source_ref:
            updates["type"] = source_ref
        return base.model_copy(update=updates) if updates else base

    source_type = source_ref or "duckdb"
    return SourceConfig(type=source_type, table=contract.table)


@contract_app.command("validate")
def contract_validate(
    config: Annotated[
        Path,
        typer.Option(
            "--config",
            "-c",
            help=("Path to the Provero YAML configuration file containing contracts."),
        ),
    ] = Path("provero.yaml"),
) -> None:
    """Validate data contracts against live data sources.

    Connects to each source referenced by a contract and verifies that
    the actual schema matches the declared one. Reports drift and
    violations.

    Examples:

        provero contract validate

        provero contract validate -c production.yaml
    """
    from rich.table import Table

    if not config.exists():
        console.print(f"[red]Config file not found: {config}[/red]")
        raise typer.Exit(1)

    from provero.connectors.factory import create_connector
    from provero.core.compiler import compile_file
    from provero.core.engine import run_contract

    provero_config = compile_file(config)

    if not provero_config.contracts:
        _echo("[yellow]No contracts defined in config.[/yellow]")
        return

    exit_code = 0
    for contract in provero_config.contracts:
        source = _resolve_contract_source(contract, provero_config)
        connector = create_connector(source)
        result = run_contract(contract, connector, provero_config.sources)

        if result.status == "fail":
            exit_code = 1

        if is_quiet():
            # Minimal structured output in quiet mode.
            typer.echo(
                json_mod.dumps(
                    {
                        "contract": result.contract_name,
                        "status": result.status,
                        "violations": len(result.violations),
                        "drift": len(result.schema_drift),
                    }
                )
            )
        else:
            status_style = {
                "pass": "green",
                "fail": "red",
                "warn": "yellow",
            }.get(result.status, "dim")
            console.print(
                f"\n[{status_style}]{result.contract_name}: "
                f"{result.status.upper()}[/{status_style}]"
            )

            if result.schema_drift:
                drift_table = Table(title="Schema Drift")
                drift_table.add_column("Column")
                drift_table.add_column("Change")
                drift_table.add_column("Expected")
                drift_table.add_column("Actual")
                for d in result.schema_drift:
                    drift_table.add_row(d.column, d.change_type, d.expected, d.actual)
                console.print(drift_table)

            for v in result.violations:
                console.print(f"  [{v.severity}] {v.rule}: {v.message}")

    if exit_code:
        raise typer.Exit(exit_code)


@contract_app.command("diff")
def contract_diff(
    old_config: Annotated[
        Path,
        typer.Argument(
            help="Path to the older version of the config file.",
        ),
    ],
    new_config: Annotated[
        Path,
        typer.Argument(
            help="Path to the newer version of the config file.",
        ),
    ],
) -> None:
    """Show differences between two contract versions.

    Compares the contracts defined in OLD_CONFIG against NEW_CONFIG and
    highlights added, removed, or changed fields. Breaking changes are
    flagged explicitly.

    Examples:

        provero contract diff v1.yaml v2.yaml
    """
    from rich.table import Table

    from provero.contracts.diff import diff_contracts
    from provero.core.compiler import compile_file

    old_provero = compile_file(old_config)
    new_provero = compile_file(new_config)

    if not old_provero.contracts or not new_provero.contracts:
        console.print("[yellow]Both files must contain contracts.[/yellow]")
        raise typer.Exit(1)

    old_map = {c.name: c for c in old_provero.contracts}
    new_map = {c.name: c for c in new_provero.contracts}

    all_names = set(old_map.keys()) | set(new_map.keys())

    for name in sorted(all_names):
        if name not in old_map:
            _echo(f"\n[green]+ Contract '{name}' added[/green]")
            continue
        if name not in new_map:
            _echo(f"\n[red]- Contract '{name}' removed[/red]")
            continue

        changes = diff_contracts(old_map[name], new_map[name])
        if not changes:
            _echo(f"\n[dim]Contract '{name}': no changes[/dim]")
            continue

        _echo(f"\n[bold]Contract '{name}':[/bold]")
        table = Table()
        table.add_column("Field")
        table.add_column("Change")
        table.add_column("Old")
        table.add_column("New")
        table.add_column("Breaking")
        for c in changes:
            breaking = "[red]YES[/red]" if c.is_breaking else "[green]no[/green]"
            table.add_row(
                c.field,
                c.change_type,
                c.old_value,
                c.new_value,
                breaking,
            )
        if not is_quiet():
            console.print(table)
