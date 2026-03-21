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

"""``provero history`` command."""

from __future__ import annotations

from typing import Annotated

import typer

from provero.cli.utils import _echo, console, is_quiet


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
    from rich.table import Table

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

        if not is_quiet():
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

        if not is_quiet():
            console.print(table)

    store.close()
