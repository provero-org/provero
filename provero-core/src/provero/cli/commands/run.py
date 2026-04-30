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

"""``provero run`` command."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from provero.cli.main import (
    _echo,
    _print_csv,
    _print_table,
    _resolve_contract_source,
    app,
    console,
    is_quiet,
)


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

        if is_quiet():
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

    try:
        if exit_code:
            raise typer.Exit(exit_code)
    finally:
        if store:
            store.close()
