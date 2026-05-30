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

"""Prometheus metrics observer.

Exposes execution metrics for scraping. ``prometheus_client`` is imported
lazily so importing :mod:`provero` without the ``prometheus`` extra never
fails. Each observer owns a private ``CollectorRegistry`` instead of the global
default registry, so instantiating several observers (notably in tests) never
raises "Duplicated timeseries" and each observer's exposition reflects only its
own counts.

Metrics:
    - ``provero_checks_total`` (counter, labelled by ``status``)
    - ``provero_check_duration_seconds`` (histogram)
    - ``provero_suite_score`` (gauge, labelled by ``suite``)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry  # type: ignore[import-not-found]

    from provero.core.compiler import SuiteConfig
    from provero.core.results import CheckResult, SuiteResult

_MISSING_MSG = (
    "PrometheusObserver requires the 'prometheus' extra. "
    "Install it with: pip install 'provero[prometheus]' "
    "(or pip install prometheus-client)."
)


def _require_prometheus() -> Any:
    try:
        import prometheus_client
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised via guard test
        raise RuntimeError(_MISSING_MSG) from exc
    return prometheus_client


class PrometheusObserver:
    """Record execution metrics into a private Prometheus registry."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        pc = _require_prometheus()
        self._registry = registry if registry is not None else pc.CollectorRegistry()
        self._checks_total = pc.Counter(
            "provero_checks_total",
            "Total number of checks executed, labelled by result status.",
            labelnames=("status",),
            registry=self._registry,
        )
        self._check_duration = pc.Histogram(
            "provero_check_duration_seconds",
            "Duration of individual check execution in seconds.",
            registry=self._registry,
        )
        self._suite_score = pc.Gauge(
            "provero_suite_score",
            "Quality score of the most recent run of a suite (0-100).",
            labelnames=("suite",),
            registry=self._registry,
        )

    @property
    def registry(self) -> CollectorRegistry:
        """The private collector registry backing this observer."""
        return self._registry

    def on_suite_start(self, suite: SuiteConfig, run_id: str) -> None:
        return

    def on_check_complete(self, result: CheckResult) -> None:
        self._checks_total.labels(status=str(result.status)).inc()
        self._check_duration.observe(result.duration_ms / 1000.0)

    def on_suite_complete(self, result: SuiteResult) -> None:
        self._suite_score.labels(suite=result.suite_name).set(result.quality_score)

    def on_error(self, suite: SuiteConfig, run_id: str, error: BaseException) -> None:
        self._checks_total.labels(status="error").inc()

    def exposition(self) -> str:
        """Return the Prometheus text exposition for this observer's metrics."""
        pc = _require_prometheus()
        text: str = pc.generate_latest(self._registry).decode("utf-8")
        return text


def render_metrics(observer: PrometheusObserver) -> str:
    """Return the Prometheus text exposition for ``observer``."""
    return observer.exposition()
