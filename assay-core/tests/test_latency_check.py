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

"""Tests for the latency check."""

from __future__ import annotations

import pytest

from provero.checks.registry import get_check_runner
from provero.connectors.duckdb import DuckDBConnector
from provero.core.compiler import CheckConfig
from provero.core.results import Severity, Status


@pytest.fixture
def latency_connection():
    """DuckDB connection with pipeline_events table for latency testing."""
    connector = DuckDBConnector()
    conn = connector.connect()
    conn._conn.execute("""
        CREATE TABLE pipeline_events (
            event_id INTEGER,
            event_time TIMESTAMP,
            loaded_at TIMESTAMP
        )
    """)
    conn._conn.execute("""
        INSERT INTO pipeline_events VALUES
        (1, CURRENT_TIMESTAMP - INTERVAL '2 hours', CURRENT_TIMESTAMP - INTERVAL '1 hour'),
        (2, CURRENT_TIMESTAMP - INTERVAL '3 hours', CURRENT_TIMESTAMP - INTERVAL '2 hours'),
        (3, CURRENT_TIMESTAMP - INTERVAL '1 hour', CURRENT_TIMESTAMP - INTERVAL '30 minutes')
    """)
    yield conn
    connector.disconnect(conn)


class TestLatency:
    def test_pass_within_bounds(self, latency_connection):
        runner = get_check_runner("latency")
        result = runner(
            connection=latency_connection,
            table="pipeline_events",
            check_config=CheckConfig(
                check_type="latency",
                column="event_time",
                params={
                    "source_column": "event_time",
                    "target_column": "loaded_at",
                    "max_latency": "2h",
                },
            ),
        )
        assert result.status == Status.PASS

    def test_fail_exceeds_bounds(self, latency_connection):
        runner = get_check_runner("latency")
        result = runner(
            connection=latency_connection,
            table="pipeline_events",
            check_config=CheckConfig(
                check_type="latency",
                column="event_time",
                params={
                    "source_column": "event_time",
                    "target_column": "loaded_at",
                    "max_latency": "10m",
                },
            ),
        )
        assert result.status == Status.FAIL

    def test_missing_target_column(self, latency_connection):
        runner = get_check_runner("latency")
        result = runner(
            connection=latency_connection,
            table="pipeline_events",
            check_config=CheckConfig(
                check_type="latency",
                column="event_time",
                params={"source_column": "event_time"},
            ),
        )
        assert result.status == Status.ERROR

    def test_default_severity_is_warning(self, latency_connection):
        runner = get_check_runner("latency")
        result = runner(
            connection=latency_connection,
            table="pipeline_events",
            check_config=CheckConfig(
                check_type="latency",
                column="event_time",
                params={
                    "source_column": "event_time",
                    "target_column": "loaded_at",
                    "max_latency": "2h",
                },
            ),
        )
        assert result.severity == Severity.WARNING

    def test_empty_table(self):
        connector = DuckDBConnector()
        conn = connector.connect()
        conn._conn.execute("""
            CREATE TABLE empty_pipeline (
                event_time TIMESTAMP,
                loaded_at TIMESTAMP
            )
        """)
        runner = get_check_runner("latency")
        result = runner(
            connection=conn,
            table="empty_pipeline",
            check_config=CheckConfig(
                check_type="latency",
                column="event_time",
                params={
                    "source_column": "event_time",
                    "target_column": "loaded_at",
                    "max_latency": "1h",
                },
            ),
        )
        assert result.status == Status.FAIL
        connector.disconnect(conn)
