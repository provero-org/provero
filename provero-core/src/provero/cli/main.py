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

"""Provero CLI.

Entry point for all ``provero`` commands.  The module exposes a Typer
application (``app``) that is registered as a console script via
``pyproject.toml``.
"""

from __future__ import annotations

import csv
import io
import json as json_mod
import re
import time
from datetime import datetime
from pathlib import Path
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


@app.command()
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


@app.command()
def run(
    config_path: Annotated[
        Path | None,
        typer.Argument(help="Path to config file (alternative to -c)."),
    ] = None,
    config: Annotated[
        Path,
        typer.Option(
            "--config",
            "-c",
            help=("Path to the Provero YAML configuration file. Defaults to provero.yaml."),
        ),
    ] = Path("provero.yaml"),
    suite: Annotated[
        str | None,
        typer.Option(
            "--suite",
            "-s",
            help="Run only the suite with this name (skip all others).",
        ),
    ] = None,
    tag: Annotated[
        str | None,
        typer.Option(
            "--tag",
            "-t",
            help="Run only suites tagged with this value.",
        ),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option(
            "--format",
            "-f",
            help="Output format for check results. One of: table, json, csv.",
        ),
    ] = "table",
    no_store: Annotated[
        bool,
        typer.Option(
            "--no-store",
            help="Skip persisting results to the local store.",
        ),
    ] = False,
    no_optimize: Annotated[
        bool,
        typer.Option(
            "--no-optimize",
            help="Disable SQL query batching (run each check individually).",
        ),
    ] = False,
    no_alerts: Annotated[
        bool,
        typer.Option(
            "--no-alerts",
            help="Do not send alert notifications, even if configured.",
        ),
    ] = False,
    report: Annotated[
        str | None,
        typer.Option(
            "--report",
            help="Generate a report after the run. Supported values: html.",
        ),
    ] = None,
) -> None:
    """Execute data quality checks defined in a Provero config file.

    Reads the YAML configuration, connects to each data source, runs every
    check, and prints the results. Returns exit code 1 when any check fails
    or errors.

    Examples:

        provero run

        provero run -c my_checks.yaml --format json --no-store

        provero run --suite orders_suite --tag nightly

        provero run --report html

        provero run --quiet --format json
    """
    config = config_path or config
    if not config.exists():
        console.print(f"[red]Config file not found: {config}[/red]")
        console.print("Run 'provero init' to create one.")
        raise typer.Exit(1)

    from provero.connectors.factory import create_connector
    from provero.core.compiler import compile_file
    from provero.core.engine import run_suite

    provero_config = compile_file(config)

    store = None
    if not no_store:
        from provero.store.sqlite import SQLiteStore

        store = SQLiteStore()

    exit_code = 0
    all_results = []
    contract_results = []
    csv_header_written = False

    for suite_config in provero_config.suites:
        if suite and suite_config.name != suite:
            continue
        if tag and tag not in suite_config.tags:
            continue

        connector = create_connector(suite_config.source)
        result = run_suite(suite_config, connector, optimize=not no_optimize)

        if store:
            store.save_result(result)

        all_results.append(result)

        if _quiet:
            # In quiet mode only emit structured formats (json/csv).
            if output_format == "json":
                typer.echo(result.model_dump_json(indent=2))
            elif output_format == "csv":
                _print_csv(result, include_header=not csv_header_written)
                csv_header_written = True
            # table format is suppressed entirely in quiet mode.
        else:
            if output_format == "json":
                typer.echo(result.model_dump_json(indent=2))
            elif output_format == "csv":
                _print_csv(result, include_header=not csv_header_written)
                csv_header_written = True
            else:
                _print_table(result)

        if result.failed > 0 or result.errored > 0:
            exit_code = 1

    # Run contracts if present
    if provero_config.contracts:
        from provero.core.engine import run_contract

        for contract in provero_config.contracts:
            source = _resolve_contract_source(contract, provero_config)
            connector = create_connector(source)
            cr = run_contract(contract, connector, provero_config.sources)
            contract_results.append(cr)

            if cr.status == "fail":
                exit_code = 1
                _echo(f"\n[red]Contract '{cr.contract_name}' FAILED[/red]")
            elif cr.status == "warn":
                _echo(f"\n[yellow]Contract '{cr.contract_name}' has warnings[/yellow]")
            else:
                _echo(f"\n[green]Contract '{cr.contract_name}' PASSED[/green]")

            for v in cr.violations:
                _echo(f"  [{v.severity}] {v.rule}: {v.message}")

    # Send alerts if configured
    if not no_alerts and provero_config.alerts:
        from provero.alerts.sender import send_alerts

        for result in all_results:
            outcomes = send_alerts(provero_config.alerts, result)
            for alert_cfg, ok in zip(provero_config.alerts, outcomes, strict=True):
                if ok:
                    _echo(f"[green]Alert sent to {alert_cfg.url}[/green]")
                elif ok is False and result.failed > 0:
                    from provero.alerts.sender import _should_fire

                    if _should_fire(alert_cfg, result):
                        _echo(f"[yellow]Alert delivery failed: {alert_cfg.url}[/yellow]")

    # Generate HTML report if requested
    if report == "html" and all_results:
        from provero.reporting.html import generate_html_report

        for result in all_results:
            report_path = Path(f".provero/reports/{result.suite_name}.html")
            generate_html_report(
                result,
                contract_results=contract_results or None,
                output_path=report_path,
            )
            _echo(f"\n[green]HTML report: {report_path}[/green]")

    if store:
        store.close()

    if exit_code:
        raise typer.Exit(exit_code)


def _parse_interval(s: str) -> int:
    """Parse a human-friendly interval string into seconds.

    Supported formats: "30s", "5m", "1h", "1h30m", "2m15s".
    Raises ValueError for invalid input.
    """
    s = s.strip().lower()
    if not s:
        raise ValueError("Interval cannot be empty.")

    pattern = re.compile(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$")
    match = pattern.fullmatch(s)
    if not match or not any(match.groups()):
        raise ValueError(f"Invalid interval '{s}'. Use formats like 30s, 5m, 1h, or 1h30m.")

    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    total = hours * 3600 + minutes * 60 + seconds
    if total <= 0:
        raise ValueError("Interval must be greater than zero.")
    return total


def _format_interval(seconds: int) -> str:
    """Format seconds into a human-readable string like '5m' or '1h30m'."""
    if seconds >= 3600:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h{m}m" if m else f"{h}h"
    if seconds >= 60:
        m = seconds // 60
        s = seconds % 60
        return f"{m}m{s}s" if s else f"{m}m"
    return f"{seconds}s"


@app.command()
def watch(
    config_path: Annotated[
        Path | None,
        typer.Argument(help="Path to config file (alternative to -c)."),
    ] = None,
    config: Annotated[
        Path,
        typer.Option(
            "--config",
            "-c",
            help="Path to the Provero YAML configuration file. Defaults to provero.yaml.",
        ),
    ] = Path("provero.yaml"),
    suite: Annotated[
        str | None,
        typer.Option(
            "--suite",
            "-s",
            help="Run only the suite with this name (skip all others).",
        ),
    ] = None,
    tag: Annotated[
        str | None,
        typer.Option(
            "--tag",
            "-t",
            help="Run only suites tagged with this value.",
        ),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option(
            "--format",
            "-f",
            help="Output format for check results. One of: table, json, csv.",
        ),
    ] = "table",
    no_store: Annotated[
        bool,
        typer.Option(
            "--no-store",
            help="Skip persisting results to the local store.",
        ),
    ] = False,
    no_optimize: Annotated[
        bool,
        typer.Option(
            "--no-optimize",
            help="Disable SQL query batching (run each check individually).",
        ),
    ] = False,
    interval: Annotated[
        str,
        typer.Option(
            "--interval",
            "-i",
            help="Polling interval. Supports 30s, 5m, 1h, 1h30m. Default: 5m.",
        ),
    ] = "5m",
    count: Annotated[
        int | None,
        typer.Option(
            "--count",
            "-n",
            help="Run exactly N times then exit. Useful for CI.",
        ),
    ] = None,
) -> None:
    """Continuously run data quality checks on a polling interval.

    Executes the same checks as ``provero run`` but repeats them on a
    configurable schedule. Useful for monitoring dashboards and CI
    pipelines.

    Examples:

        provero watch

        provero watch --interval 30s

        provero watch -c staging.yaml -i 1m --count 3

        provero watch --format json --interval 5m
    """
    config = config_path or config
    if not config.exists():
        console.print(f"[red]Config file not found: {config}[/red]")
        console.print("Run 'provero init' to create one.")
        raise typer.Exit(1)

    try:
        interval_seconds = _parse_interval(interval)
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1) from None

    from provero.connectors.factory import create_connector
    from provero.core.compiler import compile_file
    from provero.core.engine import run_suite

    provero_config = compile_file(config)

    store = None
    if not no_store:
        from provero.store.sqlite import SQLiteStore

        store = SQLiteStore()

    any_failure = False
    iteration = 0

    try:
        while True:
            iteration += 1

            if count is not None and iteration > count:
                break

            now = datetime.now().strftime("%H:%M:%S")
            next_label = _format_interval(interval_seconds)

            is_last = count is not None and iteration == count

            if output_format == "table":
                if iteration > 1:
                    console.clear()
                if is_last:
                    _echo(f"[bold]Run #{iteration} at {now}[/bold]")
                else:
                    _echo(
                        f"[bold]Run #{iteration} at {now}[/bold] [dim](next in {next_label})[/dim]"
                    )

            exit_code = 0
            csv_header_written = iteration > 1

            for suite_config in provero_config.suites:
                if suite and suite_config.name != suite:
                    continue
                if tag and tag not in suite_config.tags:
                    continue

                connector = create_connector(suite_config.source)
                result = run_suite(suite_config, connector, optimize=not no_optimize)

                if store:
                    store.save_result(result)

                if output_format == "json":
                    typer.echo(result.model_dump_json())
                elif output_format == "csv":
                    _print_csv(result, include_header=not csv_header_written)
                    csv_header_written = True
                else:
                    _print_table(result)

                if result.failed > 0 or result.errored > 0:
                    exit_code = 1

            if exit_code:
                any_failure = True

            if is_last:
                break

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        _echo("\n[yellow]Watch stopped.[/yellow]")
    finally:
        if store:
            store.close()

    if any_failure:
        raise typer.Exit(1)


# Contract subcommand
contract_app = typer.Typer(
    name="contract",
    help=(
        "Data contract commands.\n\n"
        "Validate schemas and freshness rules against live data sources, "
        "or compare two versions of a contract to detect breaking changes."
    ),
    no_args_is_help=True,
)
app.add_typer(contract_app, name="contract")


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

        if _quiet:
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
        if not _quiet:
            console.print(table)


@app.command()
def history(
    suite_name: Annotated[
        str | None,
        typer.Option(
            "--suite",
            "-s",
            help="Only show runs for this suite name.",
        ),
    ] = None,
    limit: Annotated[
        int,
        typer.Option(
            "--limit",
            "-n",
            help="Maximum number of runs to display. Defaults to 20.",
        ),
    ] = 20,
    run_id: Annotated[
        str | None,
        typer.Option(
            "--run",
            "-r",
            help="Show detailed check results for a specific run ID.",
        ),
    ] = None,
) -> None:
    """Browse historical check results stored locally.

    Without options, lists the most recent runs. Use --run to drill into
    the individual check results of a specific run.

    Examples:

        provero history

        provero history --suite orders_suite --limit 5

        provero history --run abc123
    """
    from provero.store.sqlite import SQLiteStore

    store = SQLiteStore()

    if run_id:
        details = store.get_run_details(run_id)
        if not details:
            console.print(f"[red]Run not found: {run_id}[/red]")
            raise typer.Exit(1)

        table = Table(title=f"Run: {run_id[:8]}...")
        table.add_column("Check", style="cyan")
        table.add_column("Column", style="dim")
        table.add_column("Status")
        table.add_column("Observed")
        table.add_column("Expected")

        for row in details:
            status_str = {
                "pass": "[green]PASS[/green]",
                "fail": "[red]FAIL[/red]",
                "warn": "[yellow]WARN[/yellow]",
                "error": "[red]ERROR[/red]",
            }.get(row["status"], row["status"])

            table.add_row(
                row["check_type"],
                row["source_column"] or "-",
                status_str,
                row["observed_value"] or "",
                row["expected_value"] or "",
            )

        if not _quiet:
            console.print(table)
    else:
        runs = store.get_history(suite_name=suite_name, limit=limit)
        if not runs:
            _echo("[dim]No history yet. Run 'provero run' first.[/dim]")
            store.close()
            return

        table = Table(title="Run History")
        table.add_column("Run ID", style="dim")
        table.add_column("Suite", style="cyan")
        table.add_column("Status")
        table.add_column("Score", justify="right")
        table.add_column("Checks", justify="right")
        table.add_column("Failed", justify="right")
        table.add_column("Duration", justify="right")
        table.add_column("Time")

        for row in runs:
            status_str = "[green]PASS[/green]" if row["status"] == "pass" else "[red]FAIL[/red]"
            table.add_row(
                row["id"][:8] + "...",
                row["suite_name"],
                status_str,
                f"{row['quality_score']:.0f}/100",
                str(row["total"]),
                str(row["failed"]),
                f"{row['duration_ms']}ms",
                row["started_at"][:19],
            )

        if not _quiet:
            console.print(table)

    store.close()


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


@app.command()
def profile(
    config: Annotated[
        Path,
        typer.Option(
            "--config",
            "-c",
            help=("Path to the Provero YAML configuration file. Defaults to provero.yaml."),
        ),
    ] = Path("provero.yaml"),
    table_name: Annotated[
        str | None,
        typer.Option(
            "--table",
            "-t",
            help=("Name of the table to profile. Overrides the table in the config."),
        ),
    ] = None,
    suggest: Annotated[
        bool,
        typer.Option(
            "--suggest",
            help=("After profiling, suggest quality checks based on the data."),
        ),
    ] = False,
    sample: Annotated[
        int | None,
        typer.Option(
            "--sample",
            help=("Number of rows to sample for profiling (useful for large tables)."),
        ),
    ] = None,
) -> None:
    """Profile a data source and optionally suggest quality checks.

    Connects to the data source, collects column-level statistics (nulls,
    distinct values, min/max, mean), and prints a summary table.  Use
    --suggest to get a ready-to-use YAML snippet with recommended checks.

    Examples:

        provero profile

        provero profile --table orders --suggest

        provero profile --sample 10000
    """
    from provero.connectors.factory import create_connector
    from provero.core.compiler import SourceConfig, compile_file
    from provero.core.profiler import (
        checks_to_yaml,
        profile_table,
        suggest_checks,
    )

    if config.exists():
        provero_config = compile_file(config)
        source = (
            provero_config.suites[0].source
            if provero_config.suites
            else SourceConfig(type="duckdb")
        )
        tbl = table_name or source.table
    else:
        source = SourceConfig(type="duckdb")
        tbl = table_name or ""

    if not tbl:
        console.print("[red]No table specified. Use --table or define one in provero.yaml[/red]")
        raise typer.Exit(1)

    connector = create_connector(source)
    connection = connector.connect()

    try:
        result = profile_table(connection, tbl, sample_size=sample)
    finally:
        connector.disconnect(connection)

    # Print profile
    tbl_display = Table(title=f"Profile: {result.table} ({result.row_count:,} rows)")
    tbl_display.add_column("Column", style="cyan")
    tbl_display.add_column("Type", style="dim")
    tbl_display.add_column("Nulls", justify="right")
    tbl_display.add_column("Distinct", justify="right")
    tbl_display.add_column("Min")
    tbl_display.add_column("Max")
    tbl_display.add_column("Mean", justify="right")

    for col in result.columns:
        null_str = f"{col.null_count:,} ({col.null_pct}%)"
        distinct_str = f"{col.distinct_count:,} ({col.distinct_pct}%)"
        if col.min_value is not None:
            min_str = str(col.min_value)
        elif col.min_length is not None:
            min_str = str(col.min_length) + " chars"
        else:
            min_str = "-"
        if col.max_value is not None:
            max_str = str(col.max_value)
        elif col.max_length is not None:
            max_str = str(col.max_length) + " chars"
        else:
            max_str = "-"
        if col.mean_value is not None:
            mean_str = str(col.mean_value)
        elif col.avg_length is not None:
            mean_str = str(col.avg_length) + " chars"
        else:
            mean_str = "-"

        tbl_display.add_row(
            col.name,
            col.dtype,
            null_str,
            distinct_str,
            min_str,
            max_str,
            mean_str,
        )

    if not _quiet:
        console.print(tbl_display)

    if suggest:
        checks = suggest_checks(result)
        _echo("\n[bold green]Suggested checks:[/bold green]\n")
        yaml_output = checks_to_yaml(checks, source.type, tbl)
        if not _quiet:
            console.print(yaml_output)


@app.command()
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


# Import subcommand
import_app = typer.Typer(
    name="import",
    help=(
        "Import data quality configs from other tools.\n\n"
        "Convert third-party formats (e.g. SodaCL) into Provero YAML."
    ),
    no_args_is_help=True,
)
app.add_typer(import_app, name="import")


@import_app.command("soda")
def import_soda(
    file: Annotated[
        Path,
        typer.Argument(
            help="Path to a SodaCL YAML configuration file.",
        ),
    ],
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Write the converted config to a file instead of stdout.",
        ),
    ] = None,
    source_type: Annotated[
        str,
        typer.Option(
            "--source-type",
            "-s",
            help="Source type for the generated config (default: duckdb).",
        ),
    ] = "duckdb",
) -> None:
    """Convert a SodaCL config file to Provero format.

    Reads a SodaCL YAML file, maps supported checks to their Provero
    equivalents, and prints the result. Unsupported checks are included
    as comments for manual review.

    Examples:

        provero import soda checks.yaml

        provero import soda checks.yaml -o provero.yaml

        provero import soda checks.yaml --source-type postgres
    """
    if not file.exists():
        console.print(f"[red]File not found: {file}[/red]")
        raise typer.Exit(1)

    from provero.importers.soda import convert_soda_to_provero

    soda_content = file.read_text(encoding="utf-8")
    try:
        provero_yaml = convert_soda_to_provero(soda_content, source_type=source_type)
    except Exception as e:
        console.print(f"[red]Failed to convert SodaCL config: {e}[/red]")
        raise typer.Exit(1) from None

    if output:
        output.write_text(provero_yaml, encoding="utf-8")
        _echo(f"[green]Converted config written to {output}[/green]")
    else:
        typer.echo(provero_yaml)


# Export subcommand
export_app = typer.Typer(
    name="export",
    help=(
        "Export Provero checks to other formats.\n\n"
        "Convert your provero.yaml definitions into configuration files "
        "compatible with other data quality tools."
    ),
    no_args_is_help=True,
)
app.add_typer(export_app, name="export")


@export_app.command("dbt")
def export_dbt(
    config_path: Annotated[
        Path | None,
        typer.Argument(help="Path to config file (alternative to -c)."),
    ] = None,
    config: Annotated[
        Path,
        typer.Option(
            "--config",
            "-c",
            help="Path to the Provero YAML configuration file. Defaults to provero.yaml.",
        ),
    ] = Path("provero.yaml"),
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Write output to a file instead of stdout.",
        ),
    ] = None,
) -> None:
    """Export Provero checks as dbt schema.yml test definitions.

    Reads a provero.yaml file and generates the equivalent dbt schema.yml
    with column-level tests. Checks without a direct dbt equivalent are
    included as YAML comments.

    Examples:

        provero export dbt

        provero export dbt -c my_checks.yaml -o schema.yml
    """
    config = config_path or config
    if not config.exists():
        console.print(f"[red]Config file not found: {config}[/red]")
        console.print("Run 'provero init' to create one.")
        raise typer.Exit(1)

    from provero.core.compiler import compile_file
    from provero.exporters.dbt import export_config

    provero_config = compile_file(config)
    result = export_config(provero_config)

    if output:
        output.write_text(result)
        _echo(f"[green]dbt schema written to {output}[/green]")
    else:
        typer.echo(result, nl=False)


if __name__ == "__main__":
    app()
