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

"""Check execution engine with optional parallelization."""

from __future__ import annotations

import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from provero.checks.registry import get_check_runner
from provero.connectors.base import Connector
from provero.core.compiler import (
    CheckConfig,
    ProveroConfig,
    SourceConfig,
    SuiteConfig,
    compile_file,
)
from provero.core.optimizer import execute_batch, plan_batch
from provero.core.results import CheckResult, Severity, Status, SuiteResult

if TYPE_CHECKING:
    from provero.contracts.models import ContractConfig, ContractResult


def _run_single_check(
    runner,
    connection,
    table: str,
    check_config: CheckConfig,
    suite_name: str,
    source_type: str,
    run_id: str,
    connector: Connector | None = None,
) -> CheckResult:
    """Execute a single check and return the result.

    When ``connector`` is provided (parallel mode), a dedicated connection is
    created and closed within this function so that each thread owns its own
    connection. DuckDB (and many other databases) are not thread-safe when
    sharing a single connection across threads.
    """
    own_connection = None
    if connector is not None:
        own_connection = connector.connect()
        connection = own_connection

    check_start = time.monotonic()

    # Inject suite context for anomaly/row_count_change checks
    if check_config.check_type in ("anomaly", "row_count_change"):
        check_config = check_config.model_copy(
            update={
                "params": {
                    **check_config.params,
                    "_suite_name": suite_name,
                    "_check_name": (
                        f"{check_config.check_type}:{check_config.column}"
                        if check_config.column
                        else check_config.check_type
                    ),
                }
            }
        )

    try:
        result = runner(
            connection=connection,
            table=table,
            check_config=check_config,
        )
        result.run_id = run_id
        result.suite = suite_name
        result.source = source_type
        result.table = table
        result.duration_ms = int((time.monotonic() - check_start) * 1000)

        # Downgrade FAIL to WARN for INFO/WARNING severity
        result.apply_severity()

        # Populate failing_rows_sample when check fails and a query is available
        if (
            result.status in (Status.FAIL, Status.WARN)
            and result.failing_rows_query
            and not result.failing_rows_sample
        ):
            try:
                sample_query = f"{result.failing_rows_query} LIMIT 5"
                result.failing_rows_sample = connection.execute(sample_query)
            except Exception:
                pass

        return result
    except Exception as e:
        error_msg = str(e)
        hint = ""
        if "does not exist" in error_msg.lower() or "not found" in error_msg.lower():
            hint = f" Verify that table '{table}' exists."
        elif "permission" in error_msg.lower() or "denied" in error_msg.lower():
            hint = " Check your database permissions."
        elif "connection" in error_msg.lower() or "refused" in error_msg.lower():
            hint = " Verify that the database is running and the connection string is correct."
        return CheckResult(
            check_name=f"{check_config.check_type}:{check_config.column or ''}",
            check_type=check_config.check_type,
            status=Status.ERROR,
            severity=Severity.CRITICAL,
            source=source_type,
            table=table,
            column=check_config.column,
            observed_value=f"{error_msg}{hint}",
            duration_ms=int((time.monotonic() - check_start) * 1000),
            run_id=run_id,
            suite=suite_name,
        )
    finally:
        if own_connection is not None and connector is not None:
            connector.disconnect(own_connection)


def _expand_multi_column_checks(checks: list[CheckConfig]) -> list[CheckConfig]:
    """Expand multi-column checks into individual per-column checks.

    For example, ``not_null: [a, b, c]`` becomes three separate
    ``not_null: a``, ``not_null: b``, ``not_null: c`` checks so that
    each column gets its own CheckResult regardless of execution mode.
    """
    expanded: list[CheckConfig] = []
    for check in checks:
        if check.check_type == "not_null" and check.columns and len(check.columns) > 1:
            for col in check.columns:
                expanded.append(
                    CheckConfig(
                        check_type="not_null",
                        column=col,
                        severity=check.severity,
                    )
                )
        else:
            expanded.append(check)
    return expanded


def run_suite(
    suite: SuiteConfig,
    connector: Connector,
    optimize: bool = True,
    parallel: bool = False,
    max_workers: int = 4,
) -> SuiteResult:
    """Execute all checks in a suite against a data source.

    When optimize=True (default), batchable checks are compiled into a
    single SQL query. Non-batchable checks run individually after.
    """
    run_id = str(uuid.uuid4())
    suite_start = time.monotonic()
    started_at = datetime.now(tz=UTC)

    results: list[CheckResult] = []
    connection = connector.connect()

    try:
        return _run_suite_inner(
            suite,
            connector,
            connection,
            optimize,
            parallel,
            max_workers,
            run_id,
            suite_start,
            started_at,
            results,
        )
    finally:
        connector.disconnect(connection)


def _run_suite_inner(
    suite: SuiteConfig,
    connector: Connector,
    connection,
    optimize: bool,
    parallel: bool,
    max_workers: int,
    run_id: str,
    suite_start: float,
    started_at,
    results: list[CheckResult],
) -> SuiteResult:
    """Inner implementation of run_suite, always called within try/finally."""
    expanded_checks = _expand_multi_column_checks(suite.checks)

    if optimize:
        try:
            plan = plan_batch(suite.source.table, expanded_checks)
        except (ValueError, TypeError) as e:
            results.append(
                CheckResult(
                    check_name="plan_batch",
                    check_type="batch",
                    status=Status.ERROR,
                    severity=Severity.CRITICAL,
                    source=suite.source.type,
                    table=suite.source.table,
                    observed_value=str(e),
                    run_id=run_id,
                    suite=suite.name,
                )
            )
            plan = None

        # Execute batched checks (single query)
        if plan and plan.metrics:
            batch_start = time.monotonic()
            try:
                batch_results = execute_batch(connection, plan)
                batch_ms = int((time.monotonic() - batch_start) * 1000)
                for r in batch_results:
                    r.run_id = run_id
                    r.suite = suite.name
                    r.source = suite.source.type
                    r.table = suite.source.table
                    r.duration_ms = batch_ms
                    r.apply_severity()
                results.extend(batch_results)
            except Exception as e:
                results.append(
                    CheckResult(
                        check_name="batch_query",
                        check_type="batch",
                        status=Status.ERROR,
                        severity=Severity.CRITICAL,
                        source=suite.source.type,
                        table=suite.source.table,
                        observed_value=(
                            f"Batch query failed: {e}. "
                            f"Try running with --no-optimize to isolate"
                            f" the failing check, or verify that the"
                            f" table '{suite.source.table}' exists"
                            f" and is accessible."
                        ),
                        run_id=run_id,
                        suite=suite.name,
                    )
                )

        # Execute non-batchable checks individually
        remaining = plan.non_batchable if plan else expanded_checks
    else:
        remaining = expanded_checks

    # Resolve runners and filter unknowns
    runnable = []
    for check_config in remaining:
        runner = get_check_runner(check_config.check_type)
        if runner is None:
            from provero.checks.registry import list_checks

            available = ", ".join(sorted(list_checks()))
            results.append(
                CheckResult(
                    check_name=f"{check_config.check_type}:{check_config.column or ''}",
                    check_type=check_config.check_type,
                    status=Status.ERROR,
                    severity=Severity.CRITICAL,
                    source=suite.source.type,
                    table=suite.source.table,
                    column=check_config.column,
                    observed_value=(
                        f"Unknown check type '{check_config.check_type}'. "
                        f"Available types: {available}"
                    ),
                    run_id=run_id,
                    suite=suite.name,
                )
            )
        else:
            runnable.append((runner, check_config))

    if parallel and len(runnable) > 1:
        # Parallel execution using ThreadPoolExecutor.
        # Each thread gets its own connection via the connector to avoid
        # sharing a single connection across threads (not thread-safe).
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _run_single_check,
                    runner,
                    connection,
                    suite.source.table,
                    check_config,
                    suite.name,
                    suite.source.type,
                    run_id,
                    connector=connector,
                ): check_config
                for runner, check_config in runnable
            }
            for future in as_completed(futures):
                results.append(future.result())
    else:
        # Sequential execution (default)
        for runner, check_config in runnable:
            result = _run_single_check(
                runner,
                connection,
                suite.source.table,
                check_config,
                suite.name,
                suite.source.type,
                run_id,
            )
            results.append(result)

    total_ms = int((time.monotonic() - suite_start) * 1000)
    suite_result = SuiteResult(
        suite_name=suite.name,
        status=Status.PASS,
        checks=results,
        started_at=started_at,
        duration_ms=total_ms,
    )
    suite_result.compute_status()

    return suite_result


def run_contract(
    contract: ContractConfig,
    connector: Connector,
    sources: dict | None = None,
) -> ContractResult:
    """Execute contract validation against a data source.

    Args:
        contract: The contract configuration to validate.
        connector: The data source connector.
        sources: Optional source configurations for resolution.

    Returns:
        ContractResult with validation details.
    """
    from provero.contracts.validator import validate_contract

    connection = connector.connect()
    try:
        result = validate_contract(contract, connection, sources)
    finally:
        connector.disconnect(connection)
    return result


class Engine:
    """High-level API for running Provero data quality checks.

    Loads configuration from a YAML file or a dictionary and executes
    all suites, returning a flat list of check results.

    Examples::

        engine = Engine("provero.yaml")
        results = engine.run()

        engine = Engine.from_dict({
            "source": {"type": "duckdb", "table": "orders"},
            "checks": [{"not_null": "order_id"}],
        })
        results = engine.run()
    """

    def __init__(self, config_path: str | Path) -> None:
        self._config = compile_file(config_path)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> Engine:
        """Create an Engine from an in-memory configuration dictionary."""
        from provero.core.compiler import parse_check

        sources: dict[str, SourceConfig] = {}
        if "sources" in raw:
            for name, src in raw["sources"].items():
                sources[name] = SourceConfig(**src)

        if "source" in raw and "checks" in raw:
            if isinstance(raw["source"], dict):
                source = SourceConfig(**raw["source"])
            else:
                ref = raw["source"]
                if ref not in sources:
                    msg = f"Source '{ref}' not found in declared sources: {sorted(sources)}"
                    raise ValueError(msg)
                source = sources[ref]
            checks = [parse_check(c) for c in raw["checks"]]
            suite = SuiteConfig(
                name="default",
                source=source,
                checks=checks,
                tags=raw.get("tags", []),
                schedule=raw.get("schedule"),
            )
            config = ProveroConfig(
                version=raw.get("version", "1.0"),
                sources=sources,
                suites=[suite],
            )
        else:
            suites = []
            for raw_suite in raw.get("suites", []):
                source_ref = raw_suite.get("source", {})
                if isinstance(source_ref, str):
                    if source_ref not in sources:
                        available = sorted(sources)
                        msg = f"Source '{source_ref}' not found: {available}"
                        raise ValueError(msg)
                    source = sources[source_ref]
                else:
                    source = SourceConfig(**source_ref)
                if "table" in raw_suite:
                    source = source.model_copy(update={"table": raw_suite["table"]})
                checks = [parse_check(c) for c in raw_suite.get("checks", [])]
                suites.append(
                    SuiteConfig(
                        name=raw_suite["name"],
                        source=source,
                        checks=checks,
                        tags=raw_suite.get("tags", []),
                        schedule=raw_suite.get("schedule"),
                    )
                )
            config = ProveroConfig(
                version=raw.get("version", "1.0"),
                sources=sources,
                suites=suites,
            )

        instance = cls.__new__(cls)
        instance._config = config
        return instance

    @property
    def config(self) -> ProveroConfig:
        """Return the parsed configuration."""
        return self._config

    def run(
        self,
        *,
        optimize: bool = True,
        parallel: bool = False,
        max_workers: int = 4,
    ) -> list[CheckResult]:
        """Execute all suites and return a flat list of check results."""
        from provero.connectors.factory import create_connector

        all_results: list[CheckResult] = []
        for suite in self._config.suites:
            connector = create_connector(suite.source)
            suite_result = run_suite(
                suite,
                connector,
                optimize=optimize,
                parallel=parallel,
                max_workers=max_workers,
            )
            all_results.extend(suite_result.checks)
        return all_results

    def run_suites(
        self,
        *,
        optimize: bool = True,
        parallel: bool = False,
        max_workers: int = 4,
    ) -> list[SuiteResult]:
        """Execute all suites and return a list of SuiteResult objects."""
        from provero.connectors.factory import create_connector

        results: list[SuiteResult] = []
        for suite in self._config.suites:
            connector = create_connector(suite.source)
            suite_result = run_suite(
                suite,
                connector,
                optimize=optimize,
                parallel=parallel,
                max_workers=max_workers,
            )
            results.append(suite_result)
        return results
