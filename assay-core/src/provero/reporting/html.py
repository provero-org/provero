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

"""HTML report generator."""

from __future__ import annotations

import importlib.resources
from pathlib import Path
from typing import TYPE_CHECKING

from jinja2 import Environment

from provero import __version__
from provero.core.results import Status

if TYPE_CHECKING:
    from provero.contracts.models import ContractResult
    from provero.core.results import SuiteResult


def generate_html_report(
    suite_result: SuiteResult,
    contract_results: list[ContractResult] | None = None,
    output_path: Path | None = None,
) -> str:
    """Generate a self-contained HTML report from suite results.

    Args:
        suite_result: The suite execution result.
        contract_results: Optional contract validation results.
        output_path: If provided, writes HTML to this file path.

    Returns:
        The rendered HTML string.
    """
    template_text = _load_template()
    env = Environment(autoescape=True)
    template = env.from_string(template_text)

    failing_checks = [
        c for c in suite_result.checks
        if c.status in (Status.FAIL, Status.WARN) and (c.failing_rows_query or c.failing_rows_sample)
    ]

    checks_data = []
    for c in suite_result.checks:
        checks_data.append({
            "check_type": c.check_type,
            "check_name": c.check_name,
            "column": c.column,
            "status": c.status.value,
            "observed_value": str(c.observed_value) if c.observed_value is not None else "",
            "expected_value": str(c.expected_value) if c.expected_value is not None else "",
            "severity": c.severity.value,
            "failing_rows_query": c.failing_rows_query,
            "failing_rows_sample": c.failing_rows_sample,
        })

    failing_data = []
    for c in failing_checks:
        failing_data.append({
            "check_name": c.check_name,
            "failing_rows_query": c.failing_rows_query,
            "failing_rows_sample": c.failing_rows_sample,
        })

    contract_data = None
    if contract_results:
        contract_data = []
        for cr in contract_results:
            contract_data.append({
                "contract_name": cr.contract_name,
                "status": cr.status,
                "violations": [{"rule": v.rule, "message": v.message, "severity": v.severity} for v in cr.violations],
                "schema_drift": [
                    {"column": d.column, "change_type": d.change_type, "expected": d.expected, "actual": d.actual}
                    for d in cr.schema_drift
                ],
            })

    html = template.render(
        suite_name=suite_result.suite_name,
        status=suite_result.status.value,
        timestamp=suite_result.started_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
        total=suite_result.total,
        passed=suite_result.passed,
        failed=suite_result.failed,
        warned=suite_result.warned,
        errored=suite_result.errored,
        quality_score=suite_result.quality_score,
        checks=checks_data,
        failing_checks=failing_data,
        contract_results=contract_data,
        version=__version__,
    )

    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(html, encoding="utf-8")

    return html


def _load_template() -> str:
    """Load the HTML template from package resources."""
    try:
        ref = importlib.resources.files("provero.reporting").joinpath("templates/report.html")
        return ref.read_text(encoding="utf-8")
    except (FileNotFoundError, TypeError):
        template_path = Path(__file__).parent / "templates" / "report.html"
        return template_path.read_text(encoding="utf-8")
