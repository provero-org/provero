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

Individual commands live in :mod:`provero.cli.commands`.
"""

from __future__ import annotations

import sys as _sys
from typing import Annotated

import typer

import provero.cli.utils as _utils
from provero import __version__
from provero.cli.commands.contract import contract_app
from provero.cli.commands.history import history
from provero.cli.commands.init import init
from provero.cli.commands.profile import profile
from provero.cli.commands.run import _print_csv, run  # noqa: F401
from provero.cli.commands.validate import validate
from provero.cli.commands.version import version
from provero.cli.utils import console, set_quiet  # noqa: F401


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
    set_quiet(quiet)


# Register commands
app.command()(run)
app.command()(init)
app.command()(validate)
app.command()(profile)
app.command()(history)
app.command(name="version")(version)
app.add_typer(contract_app, name="contract")


# ---------------------------------------------------------------------------
# Backward-compatible module wrapper so existing code that reads/writes
# ``cli_main._quiet`` keeps working transparently.
# ---------------------------------------------------------------------------


class _MainModule:
    """Module wrapper that proxies ``_quiet`` to the shared utils flag."""

    def __init__(self, mod):
        self.__dict__["_mod"] = mod

    def __getattr__(self, name):
        if name == "_quiet":
            return _utils._quiet
        return getattr(self.__dict__["_mod"], name)

    def __setattr__(self, name, value):
        if name == "_quiet":
            _utils._quiet = value
        else:
            setattr(self.__dict__["_mod"], name, value)


_sys.modules[__name__] = _MainModule(_sys.modules[__name__])  # type: ignore[assignment]


if __name__ == "__main__":
    app()
