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

"""Assay CLI."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from assay import __version__

app = typer.Typer(
    name="assay",
    help="Apache Assay - Data quality checks made simple.",
    no_args_is_help=True,
)
console = Console()

TEMPLATE = """\
# assay.yaml - Apache Assay configuration
# Docs: https://assay.apache.org/docs

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


@app.callback()
def main() -> None:
    """Apache Assay - Data quality checks made simple."""


@app.command()
def version() -> None:
    """Show version."""
    console.print(f"assay {__version__}")


@app.command()
def init(
    path: Path = typer.Argument(Path("assay.yaml"), help="Path for the config file"),
) -> None:
    """Create a new assay.yaml template."""
    if path.exists():
        console.print(f"[yellow]File already exists: {path}[/yellow]")
        raise typer.Exit(1)

    path.write_text(TEMPLATE)
    console.print(f"[green]Created {path}[/green]")
    console.print("Edit the file and run: assay run")


@app.command()
def run(
    config: Path = typer.Option(Path("assay.yaml"), "--config", "-c", help="Config file path"),
    suite: str | None = typer.Option(None, "--suite", "-s", help="Run specific suite"),
    output_format: str = typer.Option("table", "--format", "-f", help="Output format: table, json"),
) -> None:
    """Run quality checks."""
    if not config.exists():
        console.print(f"[red]Config file not found: {config}[/red]")
        console.print("Run 'assay init' to create one.")
        raise typer.Exit(1)

    from assay.core.compiler import compile_file
    from assay.core.engine import run_suite
    from assay.connectors.duckdb import DuckDBConnector

    assay_config = compile_file(config)

    for suite_config in assay_config.suites:
        if suite and suite_config.name != suite:
            continue

        # TODO: connector factory based on source type
        connector = DuckDBConnector()
        result = run_suite(suite_config, connector)

        if output_format == "json":
            console.print(result.model_dump_json(indent=2))
        else:
            _print_table(result)


def _print_table(result: "SuiteResult") -> None:
    """Print suite results as a rich table."""
    from assay.core.results import Status, SuiteResult

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


if __name__ == "__main__":
    app()
