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

"""``provero import`` subcommand group (soda)."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from provero.cli.main import _echo, console

import_app = typer.Typer(
    name="import",
    help=(
        "Import data quality configs from other tools.\n\n"
        "Convert third-party formats (e.g. SodaCL) into Provero YAML."
    ),
    no_args_is_help=True,
)


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
