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

"""Observation hooks for the execution engine.

This module defines a dependency-free :class:`ExecutionObserver` protocol and a
process-global observer registry. The engine emits lifecycle events to every
registered observer. When no observers are registered, emission is a cheap
no-op (a single ``if not _OBSERVERS`` check), so the default execution path
behaves exactly as it did before observability was added.

Observers are intentionally fire-and-forget: an exception raised by an observer
must never abort a check run. The :func:`emit` helper swallows observer
exceptions for the lifecycle events. The ``on_error`` event is special: the
engine emits it and then re-raises the original exception, so observers see the
failure but the engine's error-propagation behavior is unchanged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from provero.core.compiler import SuiteConfig
    from provero.core.results import CheckResult, SuiteResult


@runtime_checkable
class ExecutionObserver(Protocol):
    """Receives lifecycle events during suite execution.

    All methods have default no-op semantics from the caller's perspective:
    observers may implement only the events they care about. Implementations
    must not raise; any exception is caught and discarded by :func:`emit`
    (except for the ``on_error`` path, where the engine re-raises the original
    exception after notifying observers).
    """

    def on_suite_start(self, suite: SuiteConfig, run_id: str) -> None:
        """Called once before any check in ``suite`` runs."""
        ...

    def on_check_complete(self, result: CheckResult) -> None:
        """Called once per finalized check result.

        This fires for every check that produces a :class:`CheckResult`,
        including results with ``ERROR`` status (the engine converts most
        runner exceptions into ERROR-status results before this point).
        """
        ...

    def on_suite_complete(self, result: SuiteResult) -> None:
        """Called once after the suite result has been computed."""
        ...

    def on_error(self, suite: SuiteConfig, run_id: str, error: BaseException) -> None:
        """Called when an unexpected exception escapes suite execution.

        This is distinct from ``on_check_complete`` with an ERROR result: it
        fires only for exceptions that are not converted into a CheckResult
        (for example a connector failing to connect). The engine re-raises the
        exception after this hook returns.
        """
        ...


# Process-global registry. Kept as a module-level list so the common
# "no observers" path is a single truthiness check with zero allocation.
_OBSERVERS: list[ExecutionObserver] = []


def register_observer(observer: ExecutionObserver) -> None:
    """Register an observer to receive execution events.

    Idempotent: registering the same observer instance twice has no effect.
    """
    if observer not in _OBSERVERS:
        _OBSERVERS.append(observer)


def clear_observers() -> None:
    """Remove all registered observers.

    Primarily useful for tests, which must clear the global registry between
    cases to avoid leaking observers into unrelated runs.
    """
    _OBSERVERS.clear()


def iter_observers() -> tuple[ExecutionObserver, ...]:
    """Return a snapshot of the currently registered observers."""
    return tuple(_OBSERVERS)


def has_observers() -> bool:
    """Return True if at least one observer is registered."""
    return bool(_OBSERVERS)


def emit(event: str, /, *args: object, **kwargs: object) -> None:
    """Dispatch ``event`` to every registered observer, swallowing errors.

    This is the cheap path used by the engine. When no observers are
    registered it returns immediately. Observer exceptions are caught and
    discarded so a misbehaving observer cannot abort a check run.

    Note: this helper is for the non-error lifecycle events. The ``on_error``
    event is dispatched directly by the engine because it must re-raise the
    original exception afterwards; see :mod:`provero.core.engine`.
    """
    if not _OBSERVERS:
        return
    for observer in tuple(_OBSERVERS):
        method = getattr(observer, event, None)
        if method is None:
            continue
        try:
            method(*args, **kwargs)
        except Exception:
            continue


def emit_error(suite: SuiteConfig, run_id: str, error: BaseException) -> None:
    """Dispatch the ``on_error`` event without suppressing observer errors' impact.

    Observer exceptions raised while handling ``on_error`` are still swallowed
    (so observers cannot mask the original failure), but the caller is
    responsible for re-raising the original ``error`` afterwards.
    """
    if not _OBSERVERS:
        return
    for observer in tuple(_OBSERVERS):
        method = getattr(observer, "on_error", None)
        if method is None:
            continue
        try:
            method(suite, run_id, error)
        except Exception:
            continue
