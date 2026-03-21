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

"""Tests for the email_validation check."""

from __future__ import annotations

import pytest

from provero.checks.registry import get_check_runner
from provero.connectors.duckdb import DuckDBConnector
from provero.core.compiler import CheckConfig
from provero.core.results import Severity, Status


@pytest.fixture
def duckdb_emails():
    """DuckDB connection with an emails table for testing."""
    connector = DuckDBConnector()
    conn = connector.connect()
    conn._conn.execute("""
        CREATE TABLE emails (
            id INTEGER,
            email VARCHAR
        )
    """)
    yield conn
    connector.disconnect(conn)


def _insert(conn, rows: list[tuple]):
    for row in rows:
        conn._conn.execute(
            "INSERT INTO emails VALUES (?, ?)", row
        )


def _run_check(conn, column="email", severity=None):
    runner = get_check_runner("email_validation")
    config = CheckConfig(
        check_type="email_validation",
        column=column,
        severity=severity,
    )
    return runner(connection=conn, table="emails", check_config=config)


class TestEmailValidation:
    def test_valid_emails_pass(self, duckdb_emails):
        _insert(duckdb_emails, [
            (1, "alice@example.com"),
            (2, "bob.smith@test.co.uk"),
            (3, "user+tag@domain.org"),
            (4, "name%special@sub.domain.com"),
        ])
        result = _run_check(duckdb_emails)
        assert result.status == Status.PASS
        assert result.failing_rows == 0

    def test_invalid_no_at(self, duckdb_emails):
        _insert(duckdb_emails, [
            (1, "alice@example.com"),
            (2, "invalid-email"),
        ])
        result = _run_check(duckdb_emails)
        assert result.status == Status.FAIL
        assert result.failing_rows == 1

    def test_invalid_no_domain(self, duckdb_emails):
        _insert(duckdb_emails, [
            (1, "user@"),
        ])
        result = _run_check(duckdb_emails)
        assert result.status == Status.FAIL
        assert result.failing_rows == 1

    def test_invalid_no_tld(self, duckdb_emails):
        _insert(duckdb_emails, [
            (1, "user@domain"),
        ])
        result = _run_check(duckdb_emails)
        assert result.status == Status.FAIL
        assert result.failing_rows == 1

    def test_invalid_spaces(self, duckdb_emails):
        _insert(duckdb_emails, [
            (1, "user @example.com"),
            (2, "user@ example.com"),
            (3, " user@example.com"),
        ])
        result = _run_check(duckdb_emails)
        assert result.status == Status.FAIL
        assert result.failing_rows == 3

    def test_nulls_excluded(self, duckdb_emails):
        """NULLs should not count as invalid emails."""
        _insert(duckdb_emails, [
            (1, "valid@example.com"),
        ])
        duckdb_emails._conn.execute("INSERT INTO emails VALUES (2, NULL)")
        result = _run_check(duckdb_emails)
        assert result.status == Status.PASS
        assert result.row_count == 1

    def test_empty_table(self, duckdb_emails):
        """Empty table should pass (no invalid emails found)."""
        result = _run_check(duckdb_emails)
        assert result.status == Status.PASS
        assert result.row_count == 0
        assert result.failing_rows == 0

    def test_default_severity_is_warning(self, duckdb_emails):
        _insert(duckdb_emails, [(1, "valid@example.com")])
        result = _run_check(duckdb_emails)
        assert result.severity == Severity.WARNING

    def test_custom_severity(self, duckdb_emails):
        _insert(duckdb_emails, [(1, "valid@example.com")])
        result = _run_check(duckdb_emails, severity="critical")
        assert result.severity == Severity.CRITICAL

    def test_check_metadata(self, duckdb_emails):
        _insert(duckdb_emails, [(1, "valid@example.com")])
        result = _run_check(duckdb_emails)
        assert result.check_type == "email_validation"
        assert result.check_name == "email_validation:email"
        assert result.column == "email"
        assert result.expected_value == "valid email addresses"

    def test_mixed_valid_and_invalid(self, duckdb_emails):
        _insert(duckdb_emails, [
            (1, "good@example.com"),
            (2, "bad-email"),
            (3, "also.good@test.org"),
            (4, "@nodomain.com"),
            (5, "noat"),
        ])
        result = _run_check(duckdb_emails)
        assert result.status == Status.FAIL
        assert result.failing_rows == 3
        assert result.row_count == 5

    def test_uses_events_table(self, duckdb_orders):
        """Test using the shared fixture that has valid and invalid emails."""
        runner = get_check_runner("email_validation")
        config = CheckConfig(check_type="email_validation", column="email")
        result = runner(connection=duckdb_orders, table="events", check_config=config)
        assert result.status == Status.FAIL
        assert result.failing_rows == 1  # 'bad-email' is the only invalid one
