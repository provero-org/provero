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

"""Provero CLI."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from provero import __version__

app = typer.Typer(
    name="provero",
    help="Provero - Data quality checks made simple.",
    no_args_is_help=True,
)
console = Console()

TEMPLATE = """\
# provero.yaml - Provero configuration
# Docs: https://provero.apache.org/docs

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
    """Provero - Data quality checks made simple."""


@app.command()
def version() -> None:
    """Show version."""
    console.print(f"provero {__version__}")


@app.command()
def init(
    path: Path = typer.Argument(Path("provero.yaml"), help="Path for the config file"),
    from_source: str | None = typer.Option(None, "--from-source", help="Generate checks by profiling a table (e.g. duckdb:my_table)"),
) -> None:
    """Create a new provero.yaml template.

    Use --from-source to auto-generate checks by profiling a live data source.
    Format: --from-source TYPE:TABLE (e.g. duckdb:orders, postgres:users)
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
        from provero.core.profiler import checks_to_yaml, profile_table, suggest_checks

        source = SourceConfig(type=source_type, table=table_name)
        connector = create_connector(source)
        connection = connector.connect()
        try:
            profile = profile_table(connection, table_name)
        finally:
            connector.disconnect(connection)

        checks = suggest_checks(profile)
        yaml_content = checks_to_yaml(checks, source_type, table_name)
        path.write_text(yaml_content)
        console.print(f"[green]Created {path} with {len(checks)} suggested checks from profiling {table_name}[/green]")
        console.print("Review the file and run: provero run")
    else:
        path.write_text(TEMPLATE)
        console.print(f"[green]Created {path}[/green]")
        console.print("Edit the file and run: provero run")


@app.command()
def run(
    config: Path = typer.Option(Path("provero.yaml"), "--config", "-c", help="Config file path"),
    suite: str | None = typer.Option(None, "--suite", "-s", help="Run specific suite"),
    tag: str | None = typer.Option(None, "--tag", "-t", help="Run suites with this tag"),
    output_format: str = typer.Option("table", "--format", "-f", help="Output format: table, json"),
    no_store: bool = typer.Option(False, "--no-store", help="Don't persist results"),
    no_optimize: bool = typer.Option(False, "--no-optimize", help="Disable SQL batching"),
    no_alerts: bool = typer.Option(False, "--no-alerts", help="Don't send alerts"),
    report: str | None = typer.Option(None, "--report", help="Generate report: html"),
) -> None:
    """Run quality checks."""
    if not config.exists():
        console.print(f"[red]Config file not found: {config}[/red]")
        console.print("Run 'provero init' to create one.")
        raise typer.Exit(1)

    from provero.connectors.factory import create_connector
    from provero.core.compiler import compile_file
    from provero.core.engine import run_suite

    assay_config = compile_file(config)

    store = None
    if not no_store:
        from provero.store.sqlite import SQLiteStore
        store = SQLiteStore()

    exit_code = 0
    all_results = []
    contract_results = []

    for suite_config in assay_config.suites:
        if suite and suite_config.name != suite:
            continue
        if tag and tag not in suite_config.tags:
            continue

        connector = create_connector(suite_config.source)
        result = run_suite(suite_config, connector, optimize=not no_optimize)

        if store:
            store.save_result(result)

        all_results.append(result)

        if output_format == "json":
            console.print(result.model_dump_json(indent=2))
        else:
            _print_table(result)

        if result.failed > 0 or result.errored > 0:
            exit_code = 1

    # Run contracts if present
    if assay_config.contracts:
        from provero.core.engine import run_contract

        for contract in assay_config.contracts:
            source = _resolve_contract_source(contract, assay_config)
            connector = create_connector(source)
            cr = run_contract(contract, connector, assay_config.sources)
            contract_results.append(cr)

            if cr.status == "fail":
                exit_code = 1
                console.print(f"\n[red]Contract '{cr.contract_name}' FAILED[/red]")
            elif cr.status == "warn":
                console.print(f"\n[yellow]Contract '{cr.contract_name}' has warnings[/yellow]")
            else:
                console.print(f"\n[green]Contract '{cr.contract_name}' PASSED[/green]")

            for v in cr.violations:
                console.print(f"  [{v.severity}] {v.rule}: {v.message}")

    # Send alerts if configured
    if not no_alerts and assay_config.alerts:
        from provero.alerts.sender import send_alerts

        for result in all_results:
            outcomes = send_alerts(assay_config.alerts, result)
            for alert_cfg, ok in zip(assay_config.alerts, outcomes, strict=True):
                if ok:
                    console.print(f"[green]Alert sent to {alert_cfg.url}[/green]")
                elif ok is False and result.failed > 0:
                    # Only warn about delivery failure if the alert should have fired
                    from provero.alerts.sender import _should_fire

                    if _should_fire(alert_cfg, result):
                        console.print(
                            f"[yellow]Alert delivery failed: {alert_cfg.url}[/yellow]"
                        )

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
            console.print(f"\n[green]HTML report: {report_path}[/green]")

    if store:
        store.close()

    if exit_code:
        raise typer.Exit(exit_code)


# Contract subcommand
contract_app = typer.Typer(name="contract", help="Data contract commands.", no_args_is_help=True)
app.add_typer(contract_app, name="contract")


@contract_app.command("validate")
def contract_validate(
    config: Path = typer.Option(Path("provero.yaml"), "--config", "-c", help="Config file path"),
) -> None:
    """Validate data contracts against live sources."""
    if not config.exists():
        console.print(f"[red]Config file not found: {config}[/red]")
        raise typer.Exit(1)

    from provero.connectors.factory import create_connector
    from provero.core.compiler import SourceConfig, compile_file
    from provero.core.engine import run_contract

    assay_config = compile_file(config)

    if not assay_config.contracts:
        console.print("[yellow]No contracts defined in config.[/yellow]")
        return

    exit_code = 0
    for contract in assay_config.contracts:
        source = _resolve_contract_source(contract, assay_config)
        connector = create_connector(source)
        result = run_contract(contract, connector, assay_config.sources)

        if result.status == "fail":
            exit_code = 1

        status_style = {"pass": "green", "fail": "red", "warn": "yellow"}.get(result.status, "dim")
        console.print(f"\n[{status_style}]{result.contract_name}: {result.status.upper()}[/{status_style}]")

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
    old_config: Path = typer.Argument(..., help="Old config file"),
    new_config: Path = typer.Argument(..., help="New config file"),
) -> None:
    """Show differences between two contract versions."""
    from provero.contracts.diff import diff_contracts
    from provero.core.compiler import compile_file

    old_assay = compile_file(old_config)
    new_assay = compile_file(new_config)

    if not old_assay.contracts or not new_assay.contracts:
        console.print("[yellow]Both files must contain contracts.[/yellow]")
        raise typer.Exit(1)

    old_map = {c.name: c for c in old_assay.contracts}
    new_map = {c.name: c for c in new_assay.contracts}

    all_names = set(old_map.keys()) | set(new_map.keys())

    for name in sorted(all_names):
        if name not in old_map:
            console.print(f"\n[green]+ Contract '{name}' added[/green]")
            continue
        if name not in new_map:
            console.print(f"\n[red]- Contract '{name}' removed[/red]")
            continue

        changes = diff_contracts(old_map[name], new_map[name])
        if not changes:
            console.print(f"\n[dim]Contract '{name}': no changes[/dim]")
            continue

        console.print(f"\n[bold]Contract '{name}':[/bold]")
        table = Table()
        table.add_column("Field")
        table.add_column("Change")
        table.add_column("Old")
        table.add_column("New")
        table.add_column("Breaking")
        for c in changes:
            breaking = "[red]YES[/red]" if c.is_breaking else "[green]no[/green]"
            table.add_row(c.field, c.change_type, c.old_value, c.new_value, breaking)
        console.print(table)


@app.command()
def history(
    suite_name: str | None = typer.Option(None, "--suite", "-s", help="Filter by suite"),
    limit: int = typer.Option(20, "--limit", "-n", help="Number of runs to show"),
    run_id: str | None = typer.Option(None, "--run", "-r", help="Show details for a specific run"),
) -> None:
    """Show historical check results."""
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

        console.print(table)
    else:
        runs = store.get_history(suite_name=suite_name, limit=limit)
        if not runs:
            console.print("[dim]No history yet. Run 'provero run' first.[/dim]")
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

        console.print(table)

    store.close()


def _resolve_contract_source(contract, assay_config) -> "SourceConfig":
    """Resolve a contract's source reference against config sources."""
    from provero.core.compiler import SourceConfig

    source_ref = contract.source
    if source_ref and source_ref in assay_config.sources:
        source = assay_config.sources[source_ref]
        return source.model_copy(update={"table": contract.table}) if contract.table else source

    source_type = source_ref or (assay_config.suites[0].source.type if assay_config.suites else "duckdb")
    return SourceConfig(type=source_type, table=contract.table)


def _print_table(result: "SuiteResult") -> None:
    """Print suite results as a rich table."""
    from provero.core.results import Status, SuiteResult

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


@app.command()
def profile(
    config: Path = typer.Option(Path("provero.yaml"), "--config", "-c", help="Config file path"),
    table_name: str | None = typer.Option(None, "--table", "-t", help="Table to profile"),
    suggest: bool = typer.Option(False, "--suggest", help="Suggest checks based on profile"),
    sample: int | None = typer.Option(None, "--sample", help="Sample size for large tables"),
) -> None:
    """Profile a data source and optionally suggest checks."""
    from provero.connectors.factory import create_connector
    from provero.core.compiler import SourceConfig, compile_file
    from provero.core.profiler import checks_to_yaml, profile_table, suggest_checks

    if config.exists():
        assay_config = compile_file(config)
        source = assay_config.suites[0].source if assay_config.suites else SourceConfig(type="duckdb")
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
        min_str = str(col.min_value) if col.min_value is not None else (str(col.min_length) + " chars" if col.min_length is not None else "-")
        max_str = str(col.max_value) if col.max_value is not None else (str(col.max_length) + " chars" if col.max_length is not None else "-")
        mean_str = str(col.mean_value) if col.mean_value is not None else (str(col.avg_length) + " chars" if col.avg_length is not None else "-")

        tbl_display.add_row(col.name, col.dtype, null_str, distinct_str, min_str, max_str, mean_str)

    console.print(tbl_display)

    if suggest:
        checks = suggest_checks(result)
        console.print("\n[bold green]Suggested checks:[/bold green]\n")
        yaml_output = checks_to_yaml(checks, source.type, tbl)
        console.print(yaml_output)


@app.command()
def validate(
    config: Path = typer.Option(Path("provero.yaml"), "--config", "-c", help="Config file path"),
    schema_only: bool = typer.Option(False, "--schema-only", help="Only validate against JSON Schema"),
) -> None:
    """Validate provero.yaml syntax without running checks."""
    if not config.exists():
        console.print(f"[red]Config file not found: {config}[/red]")
        raise typer.Exit(1)

    import importlib.resources
    import json

    import yaml
    from jsonschema import ValidationError, validate as json_validate

    # Step 1: Validate against JSON Schema
    # Load schema.json bundled inside the provero package
    schema = None
    try:
        schema_ref = importlib.resources.files("provero").joinpath("schema.json")
        schema = json.loads(schema_ref.read_text(encoding="utf-8"))
    except (FileNotFoundError, TypeError):
        pass

    if schema is None:
        console.print("[red]Schema file not found. Cannot validate.[/red]")
        raise typer.Exit(1)

    with config.open() as f:
        raw = yaml.safe_load(f)
    try:
        json_validate(instance=raw, schema=schema)
        console.print("[green]Schema validation passed.[/green]")
    except ValidationError as e:
        path = " -> ".join(str(p) for p in e.absolute_path) if e.absolute_path else "root"
        console.print(f"[red]Schema validation failed at '{path}':[/red] {e.message}")
        raise typer.Exit(1)

    if schema_only:
        return

    # Step 2: Compile to verify semantic correctness
    from provero.core.compiler import compile_file

    try:
        assay_config = compile_file(config)
        total_checks = sum(len(s.checks) for s in assay_config.suites)
        console.print(f"[green]Valid.[/green] {len(assay_config.suites)} suite(s), {total_checks} check(s)")
    except Exception as e:
        console.print(f"[red]Invalid:[/red] {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
