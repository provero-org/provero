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

"""Export Provero suite results to JUnit XML for CI test reporting.

The JUnit XML format is understood by virtually every CI system (Jenkins,
GitLab CI, CircleCI, GitHub Actions test reporters). Each check becomes a
``<testcase>``; failing checks carry a ``<failure>`` element and errored
checks carry an ``<error>`` element so the CI surfaces them distinctly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from xml.etree import ElementTree as ET

from provero.core.results import Status

if TYPE_CHECKING:
    from provero.core.results import CheckResult, SuiteResult

# Statuses that count as a test failure (vs. error) in JUnit terms.
#
# Only FAIL counts as a JUnit failure. WARN is deliberately excluded: the
# engine's SuiteResult.compute_status() treats WARN as "not failed" (a WARN-only
# suite has status PASS and the CLI exits 0), so reporting it as a JUnit failure
# would turn CI red for a run Provero itself considers passing.
_FAILURE_STATUSES = frozenset({Status.FAIL})


def _testcase_name(check: CheckResult) -> str:
    """Build a JUnit testcase name from a check result."""
    if check.column:
        return f"{check.check_name}[{check.table}.{check.column}]"
    if check.table:
        return f"{check.check_name}[{check.table}]"
    return check.check_name


def _failure_message(check: CheckResult) -> str:
    """Build a failure/error message for a check."""
    target = check.table or "table"
    if check.column:
        target = f"{target}.{check.column}"
    parts = [f"{check.check_type} {check.status.value} on {target}"]
    if check.expected_value is not None:
        parts.append(f"expected {check.expected_value!r}")
    if check.observed_value is not None:
        parts.append(f"observed {check.observed_value!r}")
    if check.failing_rows:
        parts.append(f"{check.failing_rows} failing row(s)")
    return "; ".join(parts)


def build_junit(suite: SuiteResult) -> ET.Element:
    """Build a JUnit ``<testsuite>`` element for a suite result."""
    tests = len(suite.checks)
    failures = sum(1 for c in suite.checks if c.status in _FAILURE_STATUSES)
    errors = sum(1 for c in suite.checks if c.status == Status.ERROR)
    skipped = sum(1 for c in suite.checks if c.status == Status.SKIP)

    testsuite = ET.Element(
        "testsuite",
        {
            "name": suite.suite_name,
            "tests": str(tests),
            "failures": str(failures),
            "errors": str(errors),
            "skipped": str(skipped),
            "time": f"{suite.duration_ms / 1000:.3f}",
        },
    )

    for check in suite.checks:
        testcase = ET.SubElement(
            testsuite,
            "testcase",
            {
                "name": _testcase_name(check),
                "classname": f"{suite.suite_name}.{check.check_type}",
                "time": f"{check.duration_ms / 1000:.3f}",
            },
        )
        message = _failure_message(check)
        if check.status == Status.ERROR:
            error = ET.SubElement(testcase, "error", {"message": message, "type": check.check_type})
            error.text = check.failing_rows_query or message
        elif check.status in _FAILURE_STATUSES:
            failure = ET.SubElement(
                testcase, "failure", {"message": message, "type": check.check_type}
            )
            failure.text = check.failing_rows_query or message
        elif check.status == Status.SKIP:
            ET.SubElement(testcase, "skipped")

    return testsuite


def to_junit(suite: SuiteResult, *, xml_declaration: bool = True) -> str:
    """Render a suite result as a JUnit XML string.

    The element is wrapped in a ``<testsuites>`` root so the document is valid
    even when a single suite is emitted, matching what CI parsers expect.
    """
    testsuites = ET.Element("testsuites")
    suite_el = build_junit(suite)
    testsuites.append(suite_el)
    # Roll the aggregate counts up onto the root for parsers that read them there.
    testsuites.set("name", suite.suite_name)
    testsuites.set("tests", suite_el.get("tests", "0"))
    testsuites.set("failures", suite_el.get("failures", "0"))
    testsuites.set("errors", suite_el.get("errors", "0"))

    body = ET.tostring(testsuites, encoding="unicode")
    if xml_declaration:
        return '<?xml version="1.0" encoding="UTF-8"?>\n' + body
    return body
