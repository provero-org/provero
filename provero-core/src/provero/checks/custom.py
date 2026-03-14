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

"""Custom SQL checks."""

from __future__ import annotations

from provero.checks.registry import register_check
from provero.connectors.base import Connection
from provero.core.compiler import CheckConfig
from provero.core.results import CheckResult, Severity, Status


@register_check("custom_sql")
def check_custom_sql(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Execute a custom SQL check. The query must return a single boolean value."""
    query = check_config.params.get("query", "")
    name = check_config.params.get("name", "custom_sql")

    severity = Severity(check_config.severity) if check_config.severity else Severity.CRITICAL

    if not query.strip():
        return CheckResult(
            check_name=name,
            check_type="custom_sql",
            status=Status.ERROR,
            severity=severity,
            observed_value=(
                "No query provided. Use: custom_sql: "
                '"SELECT ..." or custom_sql: {query: "SELECT ..."}'
            ),
            expected_value="True",
        )

    result = connection.execute(query)

    if not result:
        return CheckResult(
            check_name=name,
            check_type="custom_sql",
            status=Status.ERROR,
            severity=severity,
            observed_value="Query returned no results",
            expected_value="True",
        )

    first_row = result[0]
    first_value = next(iter(first_row.values()))

    passed = bool(first_value)

    return CheckResult(
        check_name=name,
        check_type="custom_sql",
        status=Status.PASS if passed else Status.FAIL,
        severity=severity,
        observed_value=str(first_value),
        expected_value="True",
    )
