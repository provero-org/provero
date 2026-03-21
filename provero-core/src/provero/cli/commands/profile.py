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

"""``provero profile`` command."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from provero.cli.utils import _echo, console, is_quiet


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
    from rich.table import Table

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

    if not is_quiet():
        console.print(tbl_display)

    if suggest:
        checks = suggest_checks(result)
        _echo("\n[bold green]Suggested checks:[/bold green]\n")
        yaml_output = checks_to_yaml(checks, source.type, tbl)
        if not is_quiet():
            console.print(yaml_output)
