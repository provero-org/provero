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

"""``provero export`` subcommand group (dbt, openlineage)."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from provero.cli.main import _echo, console

export_app = typer.Typer(
    name="export",
    help=(
        "Export Provero checks to other formats.\n\n"
        "Convert your provero.yaml definitions into configuration files "
        "compatible with other data quality tools."
    ),
    no_args_is_help=True,
)


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


@export_app.command("openlineage")
def export_openlineage(
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
    namespace: Annotated[
        str,
        typer.Option(
            "--namespace",
            "-n",
            help="OpenLineage namespace for datasets. Defaults to 'provero'.",
        ),
    ] = "provero",
) -> None:
    """Run checks and export results as OpenLineage events.

    Executes all suites in the config file, then emits OpenLineage
    RunEvents with DataQualityAssertions and DataQualityMetrics facets.
    Output is a JSON array of RunEvent objects.

    Examples:

        provero export openlineage

        provero export openlineage -c checks.yaml -o events.json

        provero export openlineage -n postgres://localhost:5432/mydb
    """
    config = config_path or config
    if not config.exists():
        console.print(f"[red]Config file not found: {config}[/red]")
        console.print("Run 'provero init' to create one.")
        raise typer.Exit(1)

    from provero.core.engine import Engine
    from provero.exporters.openlineage import export_events  # type: ignore[import-untyped]

    engine = Engine(config)
    suites = engine.run_suites()
    result = export_events(suites, namespace=namespace)

    if output:
        output.write_text(result)
        _echo(f"[green]OpenLineage events written to {output}[/green]")
    else:
        typer.echo(result, nl=False)
