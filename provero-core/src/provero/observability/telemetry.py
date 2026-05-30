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

"""OpenTelemetry tracing observer.

Creates one span per suite and one child span per check. ``opentelemetry`` is
imported lazily so importing :mod:`provero` without the ``otel`` extra never
fails.

The :class:`ExecutionObserver` protocol has no ``on_check_start`` hook, so
per-check spans cannot wrap execution live. Instead, the suite span is opened
at ``on_suite_start`` (kept in a dict keyed by ``run_id`` so parallel suites do
not collide) and each check span is synthesized retroactively at
``on_check_complete`` using the result's ``duration_ms`` to set explicit start
and end times, parented to the stored suite span.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from opentelemetry.trace import Span, Tracer  # type: ignore[import-not-found]

    from provero.core.compiler import SuiteConfig
    from provero.core.results import CheckResult, SuiteResult

_MISSING_MSG = (
    "OTelObserver requires the 'otel' extra. "
    "Install it with: pip install 'provero[otel]' "
    "(or pip install opentelemetry-api opentelemetry-sdk)."
)


def _require_otel_trace() -> Any:
    try:
        from opentelemetry import trace  # type: ignore[import-not-found]
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised via guard test
        raise RuntimeError(_MISSING_MSG) from exc
    return trace


class OTelObserver:
    """Emit OpenTelemetry spans for suites and checks.

    Args:
        tracer: An optional pre-built tracer. When omitted, a tracer is
            obtained from the globally configured provider via
            ``opentelemetry.trace.get_tracer``.
    """

    def __init__(self, tracer: Tracer | None = None) -> None:
        trace = _require_otel_trace()
        self._trace = trace
        self._tracer: Tracer = tracer if tracer is not None else trace.get_tracer("provero")
        self._suite_spans: dict[str, Span] = {}

    def on_suite_start(self, suite: SuiteConfig, run_id: str) -> None:
        span = self._tracer.start_span(
            "provero.suite",
            attributes={
                "provero.suite.name": suite.name,
                "provero.run_id": run_id,
                "provero.source.type": suite.source.type,
                "provero.source.table": suite.source.table,
            },
        )
        self._suite_spans[run_id] = span

    def on_check_complete(self, result: CheckResult) -> None:
        suite_span = self._suite_spans.get(result.run_id)
        end_ns = time.time_ns()
        start_ns = end_ns - int(result.duration_ms * 1_000_000)
        context = self._trace.set_span_in_context(suite_span) if suite_span is not None else None
        span = self._tracer.start_span(
            "provero.check",
            context=context,
            start_time=start_ns,
            attributes={
                "provero.check.name": result.check_name,
                "provero.check.type": result.check_type,
                "provero.check.status": str(result.status),
                "provero.check.column": result.column or "",
                "provero.check.duration_ms": result.duration_ms,
            },
        )
        if str(result.status) in ("fail", "error"):
            span.set_status(self._trace.Status(self._trace.StatusCode.ERROR))
        span.end(end_time=end_ns)

    def on_suite_complete(self, result: SuiteResult) -> None:
        run_id = result.checks[0].run_id if result.checks else ""
        span = self._suite_spans.pop(run_id, None)
        if span is None:
            return
        span.set_attribute("provero.suite.status", str(result.status))
        span.set_attribute("provero.suite.quality_score", result.quality_score)
        span.set_attribute("provero.suite.total", result.total)
        span.set_attribute("provero.suite.failed", result.failed)
        span.end()

    def on_error(self, suite: SuiteConfig, run_id: str, error: BaseException) -> None:
        span = self._suite_spans.pop(run_id, None)
        if span is None:
            return
        span.record_exception(error)
        span.set_status(self._trace.Status(self._trace.StatusCode.ERROR))
        span.end()
