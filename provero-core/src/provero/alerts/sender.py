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

"""Webhook alert sender."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provero.alerts.models import AlertConfig
    from provero.core.results import SuiteResult


def _resolve_env_vars(value: str) -> str:
    """Expand ``${ENV_VAR}`` references in a string.

    Only explicit ``${VAR}`` placeholders are expanded.  Bare ``$VAR``
    syntax is left as-is to avoid corrupting URLs or tokens containing
    literal ``$`` characters.  Raises ``ValueError`` when a referenced
    variable is not set.
    """
    import re

    def _replace(match: re.Match) -> str:
        var = match.group(1)
        resolved = os.environ.get(var)
        if resolved is None:
            msg = f"Environment variable {var} is not set"
            raise ValueError(msg)
        return resolved

    return re.sub(r"\$\{([^}]+)\}", _replace, value)


def _should_fire(alert: AlertConfig, result: SuiteResult) -> bool:
    """Determine if an alert should fire based on the trigger condition."""
    from provero.core.results import Status

    trigger = alert.trigger.lower()
    if trigger == "on_failure":
        return result.status == Status.FAIL
    if trigger == "always":
        return True
    if trigger == "on_success":
        return result.status == Status.PASS
    return result.status == Status.FAIL


def _build_payload(result: SuiteResult) -> dict:
    """Build the JSON payload for a webhook alert."""
    failed_checks = [
        {
            "check": c.check_name,
            "type": c.check_type,
            "column": c.column,
            "observed": str(c.observed_value),
            "expected": str(c.expected_value),
        }
        for c in result.checks
        if c.status.value in ("fail", "error")
    ]

    return {
        "suite": result.suite_name,
        "status": result.status.value,
        "quality_score": result.quality_score,
        "total": result.total,
        "passed": result.passed,
        "failed": result.failed,
        "errored": result.errored,
        "duration_ms": result.duration_ms,
        "timestamp": result.started_at.isoformat(),
        "failed_checks": failed_checks,
    }


def send_alert(alert: AlertConfig, result: SuiteResult) -> bool:
    """Send a single webhook alert. Returns True on success."""
    if not _should_fire(alert, result):
        return False

    url = _resolve_env_vars(alert.url)
    headers = {k: _resolve_env_vars(v) for k, v in alert.headers.items()}
    headers.setdefault("Content-Type", "application/json")

    payload = json.dumps(_build_payload(result)).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return 200 <= resp.status < 300
    except (urllib.error.URLError, urllib.error.HTTPError, OSError):
        return False


def send_alerts(
    alerts: list[AlertConfig],
    result: SuiteResult,
) -> list[bool]:
    """Send all configured alerts for a suite result.

    Returns a list of booleans indicating success/failure for each alert.
    """
    return [send_alert(alert, result) for alert in alerts]
