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

"""Check runner registry with plugin discovery via entry_points.

Built-in checks register via the ``@register_check`` decorator.
Third-party checks register in their pyproject.toml::

    [project.entry-points."provero.checks"]
    pii_detection = "provero_pii:check_pii"

The registry discovers them automatically at runtime.
"""

from __future__ import annotations

from collections.abc import Callable
from importlib.metadata import entry_points
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provero.core.results import CheckResult

CheckRunner = Callable[..., "CheckResult"]

_REGISTRY: dict[str, CheckRunner] = {}
_BUILTINS_LOADED = False
_PLUGINS_LOADED = False


def register_check(name: str) -> Callable[[CheckRunner], CheckRunner]:
    """Decorator to register a check runner.

    Used by built-in checks and can be used by plugins that
    are imported directly (not via entry_points).
    """

    def decorator(fn: CheckRunner) -> CheckRunner:
        _REGISTRY[name] = fn
        return fn

    return decorator


def _load_builtins() -> None:
    """Import built-in check modules to trigger @register_check decorators."""
    global _BUILTINS_LOADED
    if _BUILTINS_LOADED:
        return
    _BUILTINS_LOADED = True
    import provero.anomaly.checks
    import provero.checks.completeness
    import provero.checks.custom
    import provero.checks.freshness
    import provero.checks.referential
    import provero.checks.uniqueness
    import provero.checks.validity
    import provero.checks.volume  # noqa: F401


def _load_plugins() -> None:
    """Discover check plugins via entry_points."""
    global _PLUGINS_LOADED
    if _PLUGINS_LOADED:
        return
    _PLUGINS_LOADED = True
    for ep in entry_points(group="provero.checks"):
        if ep.name not in _REGISTRY:
            _REGISTRY[ep.name] = ep.load()


def _ensure_loaded() -> None:
    """Ensure both built-in and plugin checks are loaded."""
    _load_builtins()
    _load_plugins()


def get_check_runner(name: str) -> CheckRunner | None:
    """Get a check runner by name.

    Resolution order:
    1. Built-in checks (via @register_check decorator)
    2. Plugin checks (via entry_points, ``provero.checks`` group)

    Built-ins load first. Plugins can add new checks but
    cannot override built-ins (to prevent supply-chain attacks).
    """
    _ensure_loaded()
    return _REGISTRY.get(name)


def list_checks() -> list[str]:
    """List all registered check types (built-in + plugins)."""
    _ensure_loaded()
    return sorted(_REGISTRY.keys())
