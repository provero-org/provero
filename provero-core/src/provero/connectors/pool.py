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

"""Connection-pool and timeout configuration for SQLAlchemy connectors.

Builds the keyword arguments passed to ``create_engine``. The governing
rule: **only forward arguments the caller explicitly set**. When every
pooling/timeout option is left unset, the produced kwargs dict is empty
and ``create_engine`` behaves exactly as before. This keeps the connectors
backward compatible and avoids passing pool sizing to dialects (e.g. SQLite)
whose default pool class would reject it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PoolConfig:
    """SQLAlchemy connection-pool and timeout settings.

    Every field defaults to ``None`` meaning "use the driver/SQLAlchemy
    default". Only non-None fields are forwarded to ``create_engine``.

    Attributes:
        pool_size: Number of persistent connections kept in the pool.
        max_overflow: Extra connections allowed beyond ``pool_size``.
        pool_pre_ping: Test connections for liveness before using them.
        pool_recycle: Recycle connections older than this many seconds.
        pool_timeout: Seconds to wait for a connection from the pool.
        connect_timeout: Best-effort per-connect timeout (driver dependent),
            forwarded via ``connect_args``.
        query_timeout: Best-effort per-statement timeout in seconds (driver
            dependent), forwarded via ``connect_args`` where supported.
    """

    pool_size: int | None = None
    max_overflow: int | None = None
    pool_pre_ping: bool | None = None
    pool_recycle: int | None = None
    pool_timeout: float | None = None
    connect_timeout: float | None = None
    query_timeout: float | None = None


# Driver dialect name -> the connect_args key that carries a connect timeout.
# Only drivers with a well-known knob are listed; others silently skip the
# timeout (it is documented as best-effort).
_CONNECT_TIMEOUT_KEY: dict[str, str] = {
    "postgresql": "connect_timeout",  # psycopg2 / psycopg
    "mysql": "connect_timeout",  # mysqlclient / PyMySQL
    "snowflake": "login_timeout",
}

# Drivers that accept a per-statement / network timeout in connect_args.
_QUERY_TIMEOUT_KEY: dict[str, str] = {
    "snowflake": "network_timeout",
    "mysql": "read_timeout",
}


def _dialect_name(connection_string: str) -> str:
    """Extract the SQLAlchemy dialect name from a connection URL.

    Returns the part before ``://`` and before any ``+driver`` suffix, e.g.
    ``postgresql+psycopg2://...`` -> ``postgresql``. Falls back to an empty
    string when the URL is unparseable, so callers degrade gracefully.
    """
    scheme = connection_string.split("://", 1)[0]
    return scheme.split("+", 1)[0].lower()


def build_connect_args(connection_string: str, config: PoolConfig) -> dict[str, Any]:
    """Build driver-specific ``connect_args`` for timeouts.

    Best-effort: only drivers with a known timeout knob receive one. Unknown
    drivers (e.g. SQLite) get an empty dict and are left untouched.
    """
    dialect = _dialect_name(connection_string)
    connect_args: dict[str, Any] = {}

    if config.connect_timeout is not None:
        key = _CONNECT_TIMEOUT_KEY.get(dialect)
        if key is not None:
            connect_args[key] = config.connect_timeout

    if config.query_timeout is not None:
        key = _QUERY_TIMEOUT_KEY.get(dialect)
        if key is not None:
            connect_args[key] = config.query_timeout

    return connect_args


def build_engine_kwargs(connection_string: str, config: PoolConfig) -> dict[str, Any]:
    """Assemble ``create_engine`` kwargs, omitting every unset option.

    When ``config`` has all fields ``None``, returns an empty dict so the
    engine is created identically to the pre-hardening behavior.
    """
    kwargs: dict[str, Any] = {}

    if config.pool_size is not None:
        kwargs["pool_size"] = config.pool_size
    if config.max_overflow is not None:
        kwargs["max_overflow"] = config.max_overflow
    if config.pool_pre_ping is not None:
        kwargs["pool_pre_ping"] = config.pool_pre_ping
    if config.pool_recycle is not None:
        kwargs["pool_recycle"] = config.pool_recycle
    if config.pool_timeout is not None:
        kwargs["pool_timeout"] = config.pool_timeout

    connect_args = build_connect_args(connection_string, config)
    if connect_args:
        kwargs["connect_args"] = connect_args

    return kwargs
