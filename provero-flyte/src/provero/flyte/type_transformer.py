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

"""Flyte TypeTransformer for automatic DataFrame validation with Provero.

Validates DataFrames at task boundaries using ``typing.Annotated`` metadata,
following the same pattern as Flyte's Pandera plugin.

Usage::

    from typing import Annotated
    import pandas as pd
    from provero.flyte.type_transformer import ProveroSuite

    # Inline checks
    ValidatedDF = Annotated[pd.DataFrame, ProveroSuite(
        checks=[CheckConfig(check_type="not_null", column="id")],
        on_error="raise",
    )]

    # YAML-based checks
    ValidatedDF = Annotated[pd.DataFrame, ProveroSuite(
        config_path="provero.yaml",
        suite="orders_daily",
    )]
"""

from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass, field
from typing import Any, Literal, get_args, get_origin

logger = logging.getLogger(__name__)


@dataclass
class ProveroSuite:
    """Metadata class for ``Annotated`` DataFrame type hints.

    Attach to a DataFrame type annotation to trigger automatic validation
    when the DataFrame crosses a Flyte task boundary (serialization).

    Either provide ``checks`` for inline validation or ``config_path``/``suite``
    to load checks from a YAML file.
    """

    name: str = "provero_validation"
    table_name: str = "df"
    checks: list[Any] = field(default_factory=list)  # list[CheckConfig]
    config_path: str | None = None
    suite: str | None = None
    on_error: Literal["raise", "warn"] = "raise"


def _extract_provero_suite(python_type: type) -> ProveroSuite | None:
    """Extract ProveroSuite metadata from an Annotated type, if present."""
    if get_origin(python_type) is not None:
        for arg in get_args(python_type):
            if isinstance(arg, ProveroSuite):
                return arg
    return None


def _validate_dataframe(df: Any, suite_meta: ProveroSuite) -> None:
    """Run Provero validation on a DataFrame using the given suite metadata."""
    from provero.core.engine import run_suite
    from provero.core.results import Status
    from provero.flyte.deck import publish_provero_deck

    if suite_meta.config_path:
        from pathlib import Path

        from provero.connectors.factory import create_connector
        from provero.core.compiler import compile_file

        config = compile_file(Path(suite_meta.config_path))
        for suite_config in config.suites:
            if suite_meta.suite and suite_config.name != suite_meta.suite:
                continue
            connector = create_connector(suite_config.source)
            result = run_suite(suite_config, connector)
            publish_provero_deck(result)
            if result.status == Status.FAIL:
                _handle_failure(result, suite_meta)
    else:
        from provero.connectors.dataframe import DataFrameConnector
        from provero.core.compiler import SourceConfig, SuiteConfig

        source = SourceConfig(type="dataframe", table=suite_meta.table_name)
        suite_config = SuiteConfig(
            name=suite_meta.name,
            source=source,
            checks=list(suite_meta.checks),
        )
        connector = DataFrameConnector(df, table_name=suite_meta.table_name)
        result = run_suite(suite_config, connector)
        publish_provero_deck(result)
        if result.status == Status.FAIL:
            _handle_failure(result, suite_meta)


def _handle_failure(result: Any, suite_meta: ProveroSuite) -> None:
    """Handle a failing suite result based on the on_error policy."""
    from provero.core.results import Status

    failed = [c.check_name for c in result.checks if c.status == Status.FAIL]
    msg = (
        f"Provero validation failed for '{result.suite_name}'. "
        f"Score: {result.quality_score}/100. "
        f"Failed checks: {', '.join(failed)}"
    )
    if suite_meta.on_error == "raise":
        raise ValueError(msg)
    warnings.warn(msg, UserWarning, stacklevel=3)


def _register_transformer() -> None:
    """Register the Provero TypeTransformer with Flyte's TypeEngine.

    Captures the original pandas (and optionally polars) transformer
    before registering to avoid conflicts, delegating serialization
    to the original transformer.
    """
    try:
        import pandas as pd
        from flytekit import FlyteContext
        from flytekit.core.type_engine import TypeEngine, TypeTransformer
        from flytekit.models.literals import Literal as FlyteLiteral
        from flytekit.models.types import LiteralType
    except ImportError:
        return

    _original_pd_transformer = TypeEngine.get_transformer(pd.DataFrame)

    class ProveroDataFrameTransformer(TypeTransformer[pd.DataFrame]):
        def __init__(self) -> None:
            super().__init__("Provero DataFrame Transformer", pd.DataFrame)

        def get_literal_type(self, t: type) -> LiteralType:
            base = _get_base_type(t)
            return _original_pd_transformer.get_literal_type(base)

        def to_literal(
            self,
            ctx: FlyteContext,
            python_val: pd.DataFrame,
            python_type: type,
            expected: LiteralType,
        ) -> FlyteLiteral:
            suite_meta = _extract_provero_suite(python_type)
            if suite_meta is not None:
                _validate_dataframe(python_val, suite_meta)

            base = _get_base_type(python_type)
            return _original_pd_transformer.to_literal(ctx, python_val, base, expected)

        def to_python_value(
            self,
            ctx: FlyteContext,
            lv: Literal,
            expected_python_type: type,
        ) -> pd.DataFrame:
            base = _get_base_type(expected_python_type)
            return _original_pd_transformer.to_python_value(ctx, lv, base)

    _safe_register(TypeEngine, ProveroDataFrameTransformer(), pd.DataFrame)

    # Polars support: register conditionally
    try:
        import polars as pl

        _original_pl_transformer = TypeEngine.get_transformer(pl.DataFrame)

        class ProveroPolarsTransformer(TypeTransformer[pl.DataFrame]):
            def __init__(self) -> None:
                super().__init__("Provero Polars Transformer", pl.DataFrame)

            def get_literal_type(self, t: type) -> LiteralType:
                base = _get_base_type(t)
                return _original_pl_transformer.get_literal_type(base)

            def to_literal(
                self,
                ctx: FlyteContext,
                python_val: pl.DataFrame,
                python_type: type,
                expected: LiteralType,
            ) -> FlyteLiteral:
                suite_meta = _extract_provero_suite(python_type)
                if suite_meta is not None:
                    _validate_dataframe(python_val, suite_meta)

                base = _get_base_type(python_type)
                return _original_pl_transformer.to_literal(ctx, python_val, base, expected)

            def to_python_value(
                self,
                ctx: FlyteContext,
                lv: Literal,
                expected_python_type: type,
            ) -> pl.DataFrame:
                base = _get_base_type(expected_python_type)
                return _original_pl_transformer.to_python_value(ctx, lv, base)

        _safe_register(TypeEngine, ProveroPolarsTransformer(), pl.DataFrame)
    except (ImportError, ValueError):
        pass


def _safe_register(type_engine: type, transformer: Any, python_type: type) -> None:
    """Register a transformer, handling flytekit versions with/without override support."""
    try:
        type_engine.register(transformer, override=True)
    except TypeError:
        # flytekit version does not support override parameter
        try:
            type_engine.register(transformer)
        except ValueError:
            # Already registered for this type, override directly
            logger.warning(
                "Using internal flytekit _REGISTRY API for transformer registration, "
                "this may break in future versions"
            )
            type_engine._REGISTRY[python_type] = transformer


def _get_base_type(t: type) -> type:
    """Strip Annotated wrapper to get the base type."""
    origin = get_origin(t)
    if origin is not None:
        args = get_args(t)
        if args:
            return args[0]
    return t


_register_transformer()
