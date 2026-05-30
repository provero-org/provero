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

"""Retry helpers for transient database connection and query errors.

Hardens SQLAlchemy-backed connectors against flaky networks and brief
backend outages. Only *transient* failures are retried: a missing table,
bad SQL, or other programming error fails immediately so callers see the
real problem instead of waiting through backoff.

The backoff is exponential with full jitter. Both the sleep function and
the random source are injectable so tests run instantly and deterministically.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from collections.abc import Iterable

T = TypeVar("T")

# Substrings found in transient connection-level error messages across drivers.
# Used as a last-resort signal when SQLAlchemy cannot classify the error via
# ``connection_invalidated`` (e.g. raw DBAPI errors raised before a statement).
_TRANSIENT_MESSAGE_HINTS: tuple[str, ...] = (
    "server closed the connection",
    "connection reset",
    "connection refused",
    "connection timed out",
    "could not connect",
    "timeout expired",
    "broken pipe",
    "too many connections",
    "terminating connection",
    "recovery mode",
    "the database system is starting up",
    "deadlock detected",
)


@dataclass(frozen=True)
class RetryConfig:
    """Configuration for retrying transient operations.

    Attributes:
        attempts: Total number of attempts (1 disables retrying). Bounded
            to at least 1.
        base_delay: Base delay in seconds for the first backoff step.
        max_delay: Upper bound for any single backoff sleep, in seconds.
        jitter: When True, apply full jitter to each delay.
    """

    attempts: int = 1
    base_delay: float = 0.1
    max_delay: float = 5.0
    jitter: bool = True

    def __post_init__(self) -> None:
        if self.attempts < 1:
            object.__setattr__(self, "attempts", 1)
        if self.base_delay < 0:
            object.__setattr__(self, "base_delay", 0.0)
        if self.max_delay < 0:
            object.__setattr__(self, "max_delay", 0.0)


def is_transient_error(exc: BaseException) -> bool:
    """Return True if ``exc`` looks like a retryable, transient failure.

    A transient error is a connection-level problem that may resolve on a
    retry (dropped connection, backend restarting, deadlock). Programming
    errors (missing table, bad SQL, constraint violations) are *not*
    transient and must propagate immediately.
    """
    # Lazy import: SQLAlchemy is an optional/heavy dependency for some
    # installs, and this module must import cleanly without it.
    try:
        from sqlalchemy.exc import DBAPIError, OperationalError, ProgrammingError
    except ImportError:
        return False

    # Programming errors (e.g. missing table, syntax error) are never transient.
    if isinstance(exc, ProgrammingError):
        return False

    # SQLAlchemy's authoritative signal: the connection was invalidated.
    if isinstance(exc, DBAPIError) and exc.connection_invalidated:
        return True

    # OperationalError covers both transient (network) and permanent (SQLite
    # "no such table") cases. Fall back to message inspection to discriminate.
    if isinstance(exc, OperationalError | DBAPIError):
        message = str(exc).lower()
        return any(hint in message for hint in _TRANSIENT_MESSAGE_HINTS)

    return False


def _backoff_delay(attempt: int, config: RetryConfig, rng: random.Random) -> float:
    """Compute the delay before ``attempt`` (1-indexed) using exponential backoff."""
    exp: float = config.base_delay * float(2 ** (attempt - 1))
    capped: float = min(exp, config.max_delay)
    if config.jitter:
        return rng.uniform(0.0, capped)
    return capped


def retry_call(
    func: Callable[[], T],
    config: RetryConfig,
    *,
    is_retryable: Callable[[BaseException], bool] = is_transient_error,
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
) -> T:
    """Call ``func`` with bounded exponential backoff on transient errors.

    Args:
        func: Zero-argument callable to execute.
        config: Retry policy.
        is_retryable: Predicate deciding whether an exception is transient.
        sleep: Sleep function (injectable so tests do not block).
        rng: Random source for jitter (injectable for determinism).

    Returns:
        The result of ``func``.

    Raises:
        The last exception if all attempts fail or the error is not transient.
    """
    rng = rng if rng is not None else random.Random()
    last_exc: BaseException | None = None
    for attempt in range(1, config.attempts + 1):
        try:
            return func()
        except Exception as exc:
            last_exc = exc
            if attempt >= config.attempts or not is_retryable(exc):
                raise
            sleep(_backoff_delay(attempt, config, rng))
    # Unreachable: the loop either returns or raises. Present for type-checkers.
    assert last_exc is not None
    raise last_exc


def transient_hints() -> Iterable[str]:
    """Expose the transient-message hints (used in tests and diagnostics)."""
    return _TRANSIENT_MESSAGE_HINTS
