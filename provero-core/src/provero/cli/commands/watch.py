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

"""``provero watch`` command and interval helpers."""

from __future__ import annotations

import re
import time
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from provero.cli.main import _echo, _print_csv, _print_table, app, console


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
