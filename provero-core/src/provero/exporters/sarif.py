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

"""Export Provero results to SARIF 2.1.0 JSON for CI code-scanning tools.

SARIF (Static Analysis Results Interchange Format) is consumed natively by
GitHub code scanning, Azure DevOps, and many other CI systems. Each failing
check (and each contract violation) becomes a SARIF ``result`` with a stable
``ruleId``, a ``level`` derived from severity, a message, and a logical
location identifying the table/column under test.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from provero.core.results import Severity, Status

if TYPE_CHECKING:
    from provero.contracts.models import ContractResult
    from provero.core.results import CheckResult, SuiteResult

SARIF_VERSION = "2.1.0"
SARIF_SCHEMA = (
    "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/master/Schemas/sarif-schema-2.1.0.json"
)
TOOL_NAME = "Provero"
TOOL_URI = "https://github.com/provero-org/provero"

# A check in one of these statuses produces a SARIF result.
_REPORTED_STATUSES = frozenset({Status.FAIL, Status.ERROR, Status.WARN})

# Severities that escalate a reported result to error level.
_ERROR_SEVERITIES = frozenset({Severity.CRITICAL, Severity.BLOCKER})


def _check_level(check: CheckResult) -> str:
    """Map a check result to a SARIF level (``error`` or ``warning``)."""
    if check.status == Status.ERROR or check.severity in _ERROR_SEVERITIES:
        return "error"
    return "warning"


def _check_rule_id(check: CheckResult) -> str:
    """Build a stable SARIF ruleId for a check."""
    return f"provero.check.{check.check_type}"


def _logical_location(table: str, column: str | None) -> dict[str, Any]:
    """Build a SARIF logicalLocation for a table/column pair."""
    if column:
        name = f"{table}.{column}" if table else column
        kind = "member"
    else:
        name = table or "table"
        kind = "table"
    return {"name": name, "kind": kind}


def _check_message(check: CheckResult) -> str:
    """Build a human-readable message for a failing check."""
    target = check.table or "table"
    if check.column:
        target = f"{target}.{check.column}"
    parts = [f"Check '{check.check_name}' ({check.check_type}) {check.status.value} on {target}"]
    if check.expected_value is not None:
        parts.append(f"expected {check.expected_value!r}")
    if check.observed_value is not None:
        parts.append(f"observed {check.observed_value!r}")
    if check.failing_rows:
        parts.append(f"{check.failing_rows} failing row(s)")
    return "; ".join(parts) + "."


def _check_result(check: CheckResult) -> dict[str, Any]:
    """Build a single SARIF result object for a failing check."""
    location: dict[str, Any] = {
        "logicalLocations": [_logical_location(check.table, check.column)],
    }
    if check.source or check.table:
        artifact = check.source or check.table
        location["physicalLocation"] = {
            "artifactLocation": {"uri": artifact},
        }
    return {
        "ruleId": _check_rule_id(check),
        "level": _check_level(check),
        "message": {"text": _check_message(check)},
        "locations": [location],
    }


def _contract_results(contract: ContractResult) -> list[dict[str, Any]]:
    """Build SARIF results for a contract's violations."""
    results: list[dict[str, Any]] = []
    for violation in contract.violations:
        # ContractViolation.severity vocabulary is warning|critical|quarantine.
        level = "error" if violation.severity == "critical" else "warning"
        results.append(
            {
                "ruleId": f"provero.contract.{violation.rule}",
                "level": level,
                "message": {
                    "text": (
                        f"Contract '{contract.contract_name}' violation "
                        f"[{violation.rule}]: {violation.message}"
                    )
                },
                "locations": [
                    {"logicalLocations": [{"name": contract.contract_name, "kind": "module"}]}
                ],
            }
        )
    return results


def _collect_rules(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build the tool driver ``rules`` list from the emitted results."""
    seen: dict[str, dict[str, Any]] = {}
    for result in results:
        rule_id = result["ruleId"]
        if rule_id not in seen:
            seen[rule_id] = {"id": rule_id, "name": rule_id}
    return list(seen.values())


def build_sarif(
    suite: SuiteResult,
    contracts: list[ContractResult] | None = None,
) -> dict[str, Any]:
    """Build a SARIF 2.1.0 document (as a dict) for a suite + optional contracts."""
    results: list[dict[str, Any]] = []
    for check in suite.checks:
        if check.status in _REPORTED_STATUSES:
            results.append(_check_result(check))
    for contract in contracts or []:
        results.extend(_contract_results(contract))

    return {
        "version": SARIF_VERSION,
        "$schema": SARIF_SCHEMA,
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": TOOL_NAME,
                        "informationUri": TOOL_URI,
                        "rules": _collect_rules(results),
                    }
                },
                "results": results,
            }
        ],
    }


def to_sarif(
    suite: SuiteResult,
    contracts: list[ContractResult] | None = None,
    *,
    indent: int = 2,
) -> str:
    """Render a suite (+ optional contract results) as a SARIF 2.1.0 JSON string."""
    return json.dumps(build_sarif(suite, contracts), indent=indent)
