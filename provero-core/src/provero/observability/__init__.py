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

"""Observability and governance foundation for Provero.

Public surface:

- :class:`ExecutionObserver` protocol plus the process-global registry
  (:func:`register_observer`, :func:`clear_observers`, :func:`iter_observers`).
- :class:`AuditLogObserver` (structured JSON audit log, pure stdlib).
- :class:`PrometheusObserver` (metrics, requires the ``prometheus`` extra).
- :class:`OTelObserver` (tracing, requires the ``otel`` extra).
- :func:`redact` / :func:`redact_string` for secret redaction.

The observer classes that depend on optional extras are imported lazily inside
:func:`__getattr__` so that ``import provero.observability`` succeeds even when
``prometheus_client`` or ``opentelemetry`` are not installed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from provero.observability.audit import AuditLogObserver
from provero.observability.hooks import (
    ExecutionObserver,
    clear_observers,
    has_observers,
    iter_observers,
    register_observer,
)
from provero.observability.redaction import REDACTED, redact, redact_string

if TYPE_CHECKING:
    from provero.observability.metrics import PrometheusObserver, render_metrics
    from provero.observability.telemetry import OTelObserver

__all__ = [
    "REDACTED",
    "AuditLogObserver",
    "ExecutionObserver",
    "OTelObserver",
    "PrometheusObserver",
    "clear_observers",
    "has_observers",
    "iter_observers",
    "redact",
    "redact_string",
    "register_observer",
    "render_metrics",
]


def __getattr__(name: str) -> Any:
    """Lazily resolve optional-extra observers without importing their deps eagerly."""
    if name in ("PrometheusObserver", "render_metrics"):
        from provero.observability import metrics

        return getattr(metrics, name)
    if name == "OTelObserver":
        from provero.observability.telemetry import OTelObserver

        return OTelObserver
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
