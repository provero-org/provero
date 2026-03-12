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

"""Check runner registry."""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from assay.connectors.base import Connection
    from assay.core.compiler import CheckConfig
    from assay.core.results import CheckResult

CheckRunner = Callable[..., "CheckResult"]

_REGISTRY: dict[str, CheckRunner] = {}


def register_check(name: str) -> Callable[[CheckRunner], CheckRunner]:
    """Decorator to register a check runner."""
    def decorator(fn: CheckRunner) -> CheckRunner:
        _REGISTRY[name] = fn
        return fn
    return decorator


def get_check_runner(name: str) -> CheckRunner | None:
    """Get a check runner by name."""
    # Lazy import to trigger registration
    if not _REGISTRY:
        import assay.checks.completeness  # noqa: F401
        import assay.checks.uniqueness  # noqa: F401
        import assay.checks.validity  # noqa: F401
        import assay.checks.freshness  # noqa: F401
        import assay.checks.volume  # noqa: F401
        import assay.checks.custom  # noqa: F401
    return _REGISTRY.get(name)


def list_checks() -> list[str]:
    """List all registered check types."""
    if not _REGISTRY:
        get_check_runner("_trigger_import")
    return sorted(_REGISTRY.keys())
