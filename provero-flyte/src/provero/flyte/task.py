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

"""Provero Flyte task for running quality checks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

try:
    from flytekit import task
except ImportError:

    def task(fn=None, **kwargs):  # type: ignore[no-redef]
        """Stub for when flytekit is not installed."""
        if fn is not None:
            return fn
        return lambda f: f


@dataclass
class ProveroCheckConfig:
    """Configuration for a Provero check task."""

    config_path: str = "provero.yaml"
    suite: str | None = None
    fail_on_error: bool = True
    optimize: bool = True


@dataclass
class ProveroCheckResult:
    """Result of a Provero suite execution.

    Uses only primitive types for Flyte type system compatibility.
    """

    suite_name: str = ""
    status: str = ""
    total: int = 0
    passed: int = 0
    failed: int = 0
    warned: int = 0
    errored: int = 0
    quality_score: float = 0.0
    duration_ms: int = 0
    failed_checks: list[str] = field(default_factory=list)


@task
def provero_check_task(config: ProveroCheckConfig) -> list[ProveroCheckResult]:
    """Run Provero quality checks as a Flyte task.

    Reads a provero.yaml configuration and executes the specified suite(s).
    Raises ValueError if fail_on_error is True and any suite fails.
    """
    from provero.connectors.factory import create_connector
    from provero.core.compiler import compile_file
    from provero.core.engine import run_suite
    from provero.core.results import Status
    from provero.store.sqlite import SQLiteStore

    compiled = compile_file(Path(config.config_path))
    store = SQLiteStore()
    results: list[ProveroCheckResult] = []

    try:
        for suite_config in compiled.suites:
            if config.suite and suite_config.name != config.suite:
                continue

            connector = create_connector(suite_config.source)
            suite_result = run_suite(suite_config, connector, optimize=config.optimize)
            store.save_result(suite_result)

            failed_checks = [c.check_name for c in suite_result.checks if c.status == Status.FAIL]

            results.append(
                ProveroCheckResult(
                    suite_name=suite_result.suite_name,
                    status=str(suite_result.status),
                    total=suite_result.total,
                    passed=suite_result.passed,
                    failed=suite_result.failed,
                    warned=suite_result.warned,
                    errored=suite_result.errored,
                    quality_score=suite_result.quality_score,
                    duration_ms=suite_result.duration_ms,
                    failed_checks=failed_checks,
                )
            )

            if config.fail_on_error and suite_result.status == Status.FAIL:
                msg = (
                    f"Suite '{suite_config.name}' failed. "
                    f"Score: {suite_result.quality_score}/100. "
                    f"Failed checks: {', '.join(failed_checks)}"
                )
                raise ValueError(msg)
    finally:
        store.close()

    return results
