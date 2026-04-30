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

"""Provero CLI.

Entry point for all ``provero`` commands.  The module exposes a Typer
application (``app``) that is registered as a console script via
``pyproject.toml``.
"""

from __future__ import annotations

import csv
import io
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from provero import __version__


def _version_callback(value: bool) -> None:
    """Print version and exit when ``--version`` is passed."""
    if value:
        typer.echo(f"provero {__version__}")
        raise typer.Exit()


app = typer.Typer(
    name="provero",
    help=(
        "Provero - Data quality checks made simple.\n\n"
        "Declarative, vendor-neutral data quality engine. Define checks in a "
        "YAML file and run them against any SQL data source.\n\n"
        "Quick start:\n\n"
        "  provero init            Create a starter provero.yaml\n\n"
        "  provero run             Execute quality checks\n\n"
        "  provero validate        Validate config without running checks\n\n"
        "  provero profile         Profile a data source\n\n"
        "  provero history         View past check results\n\n"
        "  provero contract        Manage data contracts"
    ),
    no_args_is_help=True,
    rich_markup_mode="rich",
)
console = Console()

# Module-level quiet flag toggled by the top-level callback.
_quiet: bool = False

TEMPLATE = """\
# provero.yaml - Provero configuration
# Docs: https://provero-org.github.io/provero/

source:
  type: duckdb
  # type: postgres
  # connection: ${POSTGRES_URI}
  table: my_table

checks:
  - not_null: [id, name]
  - unique: id
  - row_count:
      min: 1
"""


def is_quiet() -> bool:
    """Return whether quiet mode is active."""
    return _quiet


def _echo(msg: str, **kwargs) -> None:
    """Print *msg* via the shared console, unless quiet mode is active."""
    if not _quiet:
        console.print(msg, **kwargs)


@app.callback()
def main(
    quiet: Annotated[
        bool,
        typer.Option(
            "--quiet",
            "-q",
            help=("Suppress non-essential output. Only final results and exit codes are emitted."),
        ),
    ] = False,
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-V",
            help="Show the installed version and exit.",
            callback=_version_callback,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """Provero - Data quality checks made simple."""
    global _quiet
    _quiet = quiet


@app.command()
def version() -> None:
    """Print the installed Provero version.

    Example:

        provero version
    """
    # Always print, even in quiet mode, since this is the whole point.
    console.print(f"provero {__version__}")


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


def _print_table(result) -> None:
    """Print suite results as a rich table."""
    from provero.core.results import Status

    table = Table(title=f"Suite: {result.suite_name}")
    table.add_column("Check", style="cyan")
    table.add_column("Column", style="dim")
    table.add_column("Status")
    table.add_column("Observed")
    table.add_column("Expected")

    status_styles = {
        Status.PASS: "[green]\u2713 PASS[/green]",
        Status.FAIL: "[red]\u2717 FAIL[/red]",
        Status.WARN: "[yellow]\u26a0 WARN[/yellow]",
        Status.ERROR: "[red]! ERROR[/red]",
        Status.SKIP: "[dim]- SKIP[/dim]",
    }

    for check in result.checks:
        table.add_row(
            check.check_type,
            check.column or "-",
            status_styles.get(check.status, str(check.status)),
            str(check.observed_value),
            str(check.expected_value),
        )

    console.print(table)
    console.print(
        f"\nScore: {result.quality_score}/100 | "
        f"{result.passed} passed, {result.failed} failed, "
        f"{result.warned} warned | {result.duration_ms}ms"
    )

    for check in result.checks:
        if check.status == Status.FAIL and check.failing_rows_query:
            console.print(f"\n[red]FAILED:[/red] {check.check_name}")
            console.print(f"  Query: {check.failing_rows_query}")


def _print_csv(result, include_header: bool = True) -> None:
    """Print suite results as CSV."""
    output = io.StringIO()
    writer = csv.writer(output)

    if include_header:
        writer.writerow(
            [
                "suite_name",
                "check_type",
                "column",
                "status",
                "severity",
                "observed_value",
                "expected_value",
            ]
        )

    for check in result.checks:
        writer.writerow(
            [
                result.suite_name,
                check.check_type,
                check.column or "",
                str(check.status),
                str(check.severity),
                str(check.observed_value) if check.observed_value is not None else "",
                str(check.expected_value) if check.expected_value is not None else "",
            ]
        )

    typer.echo(output.getvalue(), nl=False)


def _register_commands() -> None:
    """Import command modules so their decorators attach to ``app``.

    Command modules depend on ``app``, ``console``, and helpers defined above,
    which is why registration runs after those are in scope rather than via
    top-level imports.
    """
    import importlib

    for module_name in (
        "provero.cli.commands.history",
        "provero.cli.commands.init",
        "provero.cli.commands.profile",
        "provero.cli.commands.run",
        "provero.cli.commands.validate",
        "provero.cli.commands.watch",
    ):
        importlib.import_module(module_name)

    from provero.cli.commands.contract import contract_app
    from provero.cli.commands.export import export_app
    from provero.cli.commands.import_cmd import import_app
    from provero.cli.commands.watch import _parse_interval

    app.add_typer(contract_app, name="contract")
    app.add_typer(import_app, name="import")
    app.add_typer(export_app, name="export")

    globals()["_parse_interval"] = _parse_interval


_register_commands()


if __name__ == "__main__":
    app()
