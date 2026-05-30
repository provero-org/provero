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

import json as json_mod
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

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

if TYPE_CHECKING:
    from provero.core.results import SuiteResult
    from provero.observability import PrometheusObserver


def _render_ci(results: list[SuiteResult], fmt: str) -> str:
    """Render all suite results as a single SARIF or JUnit document.

    SARIF and JUnit are whole-run documents, so every suite's output is merged
    into one document rather than emitted per-suite.
    """
    from xml.etree import ElementTree as ET

    from provero.exporters.junit import build_junit
    from provero.exporters.sarif import build_sarif

    if fmt == "sarif":
        runs: list[object] = []
        for r in results:
            runs.extend(build_sarif(r)["runs"])
        doc = {
            "version": "2.1.0",
            "$schema": (
                "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/"
                "master/Schemas/sarif-schema-2.1.0.json"
            ),
            "runs": runs,
        }
        return json_mod.dumps(doc, indent=2)

    # junit: wrap every <testsuite> element in one <testsuites> root.
    root = ET.Element("testsuites")
    for r in results:
        root.append(build_junit(r))
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="unicode")


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
            help="Output format for check results. One of: table, json, csv, sarif, junit.",
        ),
    ] = "table",
    output_file: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Write the formatted output to this file instead of stdout.",
        ),
    ] = None,
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
    audit_log: Annotated[
        Path | None,
        typer.Option(
            "--audit-log",
            help="Append a structured JSON audit log of the run to this file.",
        ),
    ] = None,
    otel: Annotated[
        bool,
        typer.Option(
            "--otel",
            help="Emit OpenTelemetry spans (requires the 'otel' extra).",
        ),
    ] = False,
    metrics_file: Annotated[
        Path | None,
        typer.Option(
            "--prometheus",
            help=(
                "Write Prometheus text exposition for the run to this file "
                "(requires the 'prometheus' extra)."
            ),
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

    # Wire optional observability observers for this run. They are cleared in
    # the finally block so repeated CLI invocations never accumulate observers.
    prometheus_observer: PrometheusObserver | None = None
    if audit_log is not None or otel or metrics_file is not None:
        from provero.observability import register_observer

        if audit_log is not None:
            from provero.observability import AuditLogObserver

            register_observer(AuditLogObserver(path=audit_log))
        if otel:
            from provero.observability import OTelObserver

            try:
                register_observer(OTelObserver())
            except RuntimeError as exc:
                console.print(f"[red]{exc}[/red]")
                raise typer.Exit(1) from exc
        if metrics_file is not None:
            from provero.observability import PrometheusObserver

            try:
                prometheus_observer = PrometheusObserver()
            except RuntimeError as exc:
                console.print(f"[red]{exc}[/red]")
                raise typer.Exit(1) from exc
            register_observer(prometheus_observer)

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

        # sarif/junit are whole-run documents emitted once after the loop, so
        # they must not be printed per-suite here.
        if output_format in ("sarif", "junit"):
            pass
        elif is_quiet():
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

    if (suite or tag) and not all_results:
        # A filter was given but matched no suite. Running with zero checks and
        # exiting 0 would silently mask a misconfigured --suite/--tag in CI.
        _filter_desc = f"suite '{suite}'" if suite else f"tag '{tag}'"
        _echo(f"[yellow]Warning: no suite matched {_filter_desc}; nothing ran.[/yellow]")

    # CI output formats (sarif/junit) are whole-run documents: render once over
    # all collected suite results, then write to --output or stdout.
    if output_format in ("sarif", "junit") and all_results:
        rendered = _render_ci(all_results, output_format)
        if output_file is not None:
            output_file.write_text(rendered, encoding="utf-8")
        else:
            typer.echo(rendered)

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
        from provero.alerts.sender import _should_fire, send_alerts

        for result in all_results:
            outcomes = send_alerts(provero_config.alerts, result)
            for alert_cfg, ok in zip(provero_config.alerts, outcomes, strict=True):
                if ok:
                    _echo(f"[green]Alert sent to {alert_cfg.url}[/green]")
                elif _should_fire(alert_cfg, result):
                    # The alert should have fired but delivery returned False.
                    # Warn regardless of pass/fail status so that failed
                    # ``always``/``on_success`` deliveries are not silenced.
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

    # Write Prometheus exposition for this run, if requested.
    if prometheus_observer is not None and metrics_file is not None:
        from provero.observability import render_metrics

        metrics_file.parent.mkdir(parents=True, exist_ok=True)
        metrics_file.write_text(render_metrics(prometheus_observer))
        _echo(f"\n[green]Metrics written: {metrics_file}[/green]")

    try:
        if exit_code:
            raise typer.Exit(exit_code)
    finally:
        if store:
            store.close()
        if audit_log is not None or otel or metrics_file is not None:
            from provero.observability import clear_observers

            clear_observers()
