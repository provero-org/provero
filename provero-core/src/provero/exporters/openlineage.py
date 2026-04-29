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

"""Export Provero check results as OpenLineage RunEvents.

Generates OpenLineage-compatible JSON dicts from SuiteResult objects.
Uses the DataQualityAssertionsDatasetFacet (v1-0-2) to represent
individual check results and DataQualityMetricsInputDatasetFacet
(v1-0-3) for dataset-level metrics.

No external dependencies required: the exporter builds plain dicts
that conform to the OpenLineage JSON schema.
"""

from __future__ import annotations

import contextlib
import json
from datetime import timedelta
from typing import Any

from provero import __version__
from provero.core.results import CheckResult, Severity, Status, SuiteResult

_SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json"
_ASSERTIONS_SCHEMA_URL = (
    "https://openlineage.io/spec/facets/1-0-2/DataQualityAssertionsDatasetFacet.json"
)
_METRICS_SCHEMA_URL = (
    "https://openlineage.io/spec/facets/1-0-3/DataQualityMetricsInputDatasetFacet.json"
)

_PRODUCER = f"https://github.com/provero-org/provero/{__version__}"


def _map_severity(severity: Severity) -> str:
    """Map Provero severity to OpenLineage assertion severity."""
    if severity in (Severity.CRITICAL, Severity.BLOCKER):
        return "error"
    return "warn"


def _check_to_assertion(check: CheckResult) -> dict[str, Any]:
    """Convert a single CheckResult to an OpenLineage assertion dict."""
    assertion: dict[str, Any] = {
        "assertion": check.check_type,
        "success": check.status in (Status.PASS, Status.WARN),
        "severity": _map_severity(check.severity),
    }
    if check.column:
        assertion["column"] = check.column
    return assertion


def _build_assertions_facet(checks: list[CheckResult]) -> dict[str, Any]:
    """Build a DataQualityAssertionsDatasetFacet from check results."""
    return {
        "_producer": _PRODUCER,
        "_schemaURL": _ASSERTIONS_SCHEMA_URL,
        "assertions": [_check_to_assertion(c) for c in checks if c.status != Status.SKIP],
    }


def _build_metrics_facet(suite: SuiteResult) -> dict[str, Any]:
    """Build a DataQualityMetricsInputDatasetFacet from suite results.

    Extracts row_count and per-column null/distinct counts from check
    results when available.
    """
    column_metrics: dict[str, dict[str, Any]] = {}

    row_count: int | None = None

    for check in suite.checks:
        if check.status == Status.SKIP:
            continue

        if check.check_type == "row_count" and check.observed_value is not None:
            with contextlib.suppress(ValueError, TypeError):
                row_count = int(check.observed_value)

        if not check.column:
            continue

        col = check.column
        if col not in column_metrics:
            column_metrics[col] = {}

        if check.check_type == "not_null" and check.failing_rows is not None:
            column_metrics[col]["nullCount"] = check.failing_rows

        if check.check_type == "unique" and check.observed_value is not None:
            with contextlib.suppress(ValueError, TypeError):
                column_metrics[col]["distinctCount"] = int(check.observed_value)

        if check.check_type == "completeness" and check.observed_value is not None:
            with contextlib.suppress(ValueError, TypeError):
                column_metrics[col]["nullCount"] = check.row_count - int(
                    check.row_count * float(check.observed_value) / 100
                )

    facet: dict[str, Any] = {
        "_producer": _PRODUCER,
        "_schemaURL": _METRICS_SCHEMA_URL,
        "columnMetrics": column_metrics,
    }
    if row_count is not None:
        facet["rowCount"] = row_count

    return facet


def _build_input_dataset(
    suite: SuiteResult,
    namespace: str,
) -> dict[str, Any]:
    """Build an OpenLineage InputDataset from a SuiteResult."""
    table = suite.checks[0].table if suite.checks else "unknown"

    return {
        "namespace": namespace,
        "name": table,
        "facets": {
            "dataQualityAssertions": _build_assertions_facet(suite.checks),
            "dataQualityMetrics": _build_metrics_facet(suite),
        },
    }


def _format_time(dt) -> str:
    """Format a datetime as ISO 8601 with timezone."""
    return dt.isoformat()


def suite_result_to_events(
    suite: SuiteResult,
    *,
    namespace: str = "provero",
) -> list[dict[str, Any]]:
    """Convert a SuiteResult into OpenLineage RunEvent dicts.

    Returns two events: a START event and a COMPLETE or FAIL event.

    Args:
        suite: The executed suite result.
        namespace: The OpenLineage namespace for the dataset. Defaults to
            ``"provero"``. Set this to your database URI for meaningful
            lineage (e.g. ``"postgres://host:5432/mydb"``).

    Returns:
        A list of two RunEvent dicts (START + COMPLETE/FAIL).
    """
    run_id = suite.checks[0].run_id if suite.checks else "unknown"
    end_time = suite.started_at + timedelta(milliseconds=suite.duration_ms)

    event_type = "COMPLETE" if suite.status in (Status.PASS, Status.WARN) else "FAIL"

    base = {
        "producer": _PRODUCER,
        "schemaURL": _SCHEMA_URL,
        "run": {"runId": run_id},
        "job": {
            "namespace": namespace,
            "name": suite.suite_name,
        },
    }

    start_event = {
        **base,
        "eventType": "START",
        "eventTime": _format_time(suite.started_at),
    }

    end_event = {
        **base,
        "eventType": event_type,
        "eventTime": _format_time(end_time),
        "inputs": [_build_input_dataset(suite, namespace)],
        "run": {
            "runId": run_id,
            "facets": {
                "provero_summary": {
                    "_producer": _PRODUCER,
                    "_schemaURL": _SCHEMA_URL,
                    "total": suite.total,
                    "passed": suite.passed,
                    "failed": suite.failed,
                    "warned": suite.warned,
                    "errored": suite.errored,
                    "qualityScore": suite.quality_score,
                    "durationMs": suite.duration_ms,
                },
            },
        },
    }

    return [start_event, end_event]


def export_events(
    suites: list[SuiteResult],
    *,
    namespace: str = "provero",
) -> str:
    """Export multiple SuiteResults as a JSON array of OpenLineage events.

    Args:
        suites: List of executed suite results.
        namespace: The OpenLineage namespace for datasets.

    Returns:
        A JSON string containing an array of RunEvent objects.
    """
    events: list[dict[str, Any]] = []
    for suite in suites:
        events.extend(suite_result_to_events(suite, namespace=namespace))
    return json.dumps(events, indent=2, default=str)
