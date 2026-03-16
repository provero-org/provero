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

"""Provero Flyte decorators for inline quality checks."""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any


def provero_check(
    config_path: str = "provero.yaml",
    suite: str | None = None,
    fail_on_error: bool = True,
) -> Callable:
    """Decorator to run Provero checks after a Flyte task function.

    Example::

        @provero_check(config_path="provero.yaml", suite="orders_daily")
        def process_orders():
            ...
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)

            from pathlib import Path

            from provero.connectors.factory import create_connector
            from provero.core.compiler import compile_file
            from provero.core.engine import run_suite
            from provero.core.results import Status
            from provero.flyte.deck import publish_provero_deck

            config = compile_file(Path(config_path))
            for suite_config in config.suites:
                if suite and suite_config.name != suite:
                    continue
                connector = create_connector(suite_config.source)
                suite_result = run_suite(suite_config, connector)
                publish_provero_deck(suite_result)

                if fail_on_error and suite_result.status == Status.FAIL:
                    failed = [c.check_name for c in suite_result.checks if c.status == Status.FAIL]
                    raise ValueError(f"Quality checks failed: {', '.join(failed)}")

            return result

        return wrapper

    return decorator
