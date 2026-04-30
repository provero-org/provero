# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""PostgreSQL result store for persisting check results."""

from __future__ import annotations

import json
import os
import re
import uuid
from datetime import UTC, datetime

from provero.core.results import CheckResult, SuiteResult


def _expand_env_vars(url: str) -> str:
    """Expand ${ENV_VAR} patterns in a connection URL."""
    return re.sub(r"\$\{(\w+)\}", lambda m: os.environ.get(m.group(1), m.group(0)), url)


class PostgresStore:
    """Stores check results in a PostgreSQL database."""

    def __init__(self, connection_url: str) -> None:
        try:
            import psycopg  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                "psycopg is required for the PostgreSQL store. "
                "Install it with: pip install provero[postgres]"
            ) from exc

        self._connection_url = _expand_env_vars(connection_url)
        self._conn = psycopg.connect(self._connection_url, autocommit=False)
        self._create_tables()

    def _create_tables(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS provero_run (
                    id TEXT PRIMARY KEY,
                    suite_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    trigger TEXT NOT NULL DEFAULT 'manual',
                    total INTEGER NOT NULL DEFAULT 0,
                    passed INTEGER NOT NULL DEFAULT 0,
                    failed INTEGER NOT NULL DEFAULT 0,
                    warned INTEGER NOT NULL DEFAULT 0,
                    errored INTEGER NOT NULL DEFAULT 0,
                    quality_score DOUBLE PRECISION,
                    duration_ms INTEGER,
                    started_at TEXT NOT NULL,
                    completed_at TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS provero_check_result (
                    id SERIAL PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES provero_run(id),
                    check_name TEXT NOT NULL,
                    check_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    source_table TEXT,
                    source_column TEXT,
                    observed_value TEXT,
                    expected_value TEXT,
                    row_count INTEGER,
                    failing_rows INTEGER,
                    failing_sample TEXT,
                    failing_query TEXT,
                    duration_ms INTEGER
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS provero_metric (
                    id SERIAL PRIMARY KEY,
                    suite_name TEXT NOT NULL,
                    check_name TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    value DOUBLE PRECISION NOT NULL,
                    recorded_at TEXT NOT NULL
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_run_suite
                    ON provero_run(suite_name)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_run_started
                    ON provero_run(started_at)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_check_run
                    ON provero_check_result(run_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_check_type
                    ON provero_check_result(check_type)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_check_status
                    ON provero_check_result(status)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_metric_lookup
                    ON provero_metric(suite_name, check_name, metric_name, recorded_at)
            """)
        self._conn.commit()

    def save_result(self, result: SuiteResult) -> str:
        """Save a suite result. Returns the run_id."""
        run_id = result.checks[0].run_id if result.checks else ""
        if not run_id:
            run_id = str(uuid.uuid4())

        completed_at = datetime.now(tz=UTC).isoformat()

        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO provero_run
                   (id, suite_name, status, trigger, total, passed, failed, warned, errored,
                    quality_score, duration_ms, started_at, completed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    run_id,
                    result.suite_name,
                    result.status.value,
                    "manual",
                    result.total,
                    result.passed,
                    result.failed,
                    result.warned,
                    result.errored,
                    result.quality_score,
                    result.duration_ms,
                    result.started_at.isoformat(),
                    completed_at,
                ),
            )

            for check in result.checks:
                cur.execute(
                    """INSERT INTO provero_check_result
                       (run_id, check_name, check_type, status, severity,
                        source_table, source_column, observed_value, expected_value,
                        row_count, failing_rows, failing_sample, failing_query, duration_ms)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        run_id,
                        check.check_name,
                        check.check_type,
                        check.status.value,
                        check.severity.value,
                        check.table,
                        check.column,
                        str(check.observed_value),
                        str(check.expected_value),
                        check.row_count,
                        check.failing_rows,
                        json.dumps(check.failing_rows_sample)
                        if check.failing_rows_sample
                        else None,
                        check.failing_rows_query,
                        check.duration_ms,
                    ),
                )

                self._store_metrics(cur, result.suite_name, check)

        self._conn.commit()
        return run_id

    def _store_metrics(self, cur, suite_name: str, check: CheckResult) -> None:
        """Extract and store numeric metrics from a check result."""
        now = datetime.now(tz=UTC).isoformat()
        insert_sql = (
            "INSERT INTO provero_metric "
            "(suite_name, check_name, metric_name, value, recorded_at) "
            "VALUES (%s, %s, %s, %s, %s)"
        )

        if check.check_type == "row_count":
            try:
                value = float(str(check.observed_value).replace(",", ""))
                cur.execute(insert_sql, (suite_name, check.check_name, "row_count", value, now))
            except ValueError:
                pass

        if check.check_type == "not_null" and check.failing_rows is not None:
            cur.execute(
                insert_sql,
                (suite_name, check.check_name, "null_count", float(check.failing_rows), now),
            )

        if check.check_type == "completeness" and check.observed_value:
            try:
                raw = str(check.observed_value).strip()
                pct = float(raw[:-1]) / 100.0 if raw.endswith("%") else float(raw)
                cur.execute(
                    insert_sql,
                    (suite_name, check.check_name, "completeness_pct", pct, now),
                )
            except (ValueError, TypeError):
                pass

        if check.check_type == "row_count_change" and check.row_count is not None:
            cur.execute(
                insert_sql,
                (suite_name, check.check_name, "row_count", float(check.row_count), now),
            )

        if check.failing_rows is not None and check.row_count and check.row_count > 0:
            fail_rate = check.failing_rows / check.row_count
            cur.execute(
                insert_sql,
                (suite_name, check.check_name, "fail_rate", fail_rate, now),
            )

        if check.check_type not in ("row_count", "not_null", "completeness", "row_count_change"):
            try:
                observed_str = str(check.observed_value).split()[0].replace(",", "")
                numeric_val = float(observed_str)
                cur.execute(
                    insert_sql,
                    (suite_name, check.check_name, "observed_value", numeric_val, now),
                )
            except (ValueError, TypeError, IndexError):
                pass

    def get_history(
        self,
        suite_name: str | None = None,
        limit: int = 20,
    ) -> list[dict]:
        """Get recent run history."""
        with self._conn.cursor() as cur:
            if suite_name:
                cur.execute(
                    "SELECT * FROM provero_run WHERE suite_name = %s "
                    "ORDER BY started_at DESC LIMIT %s",
                    (suite_name, limit),
                )
            else:
                cur.execute(
                    "SELECT * FROM provero_run ORDER BY started_at DESC LIMIT %s",
                    (limit,),
                )
            columns = [desc[0] for desc in cur.description or []]
            return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]

    def get_run_details(self, run_id: str) -> list[dict]:
        """Get check results for a specific run."""
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM provero_check_result WHERE run_id = %s ORDER BY id",
                (run_id,),
            )
            columns = [desc[0] for desc in cur.description or []]
            return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]

    def get_metrics(
        self,
        suite_name: str,
        check_name: str,
        metric_name: str,
        limit: int = 30,
    ) -> list[dict]:
        """Get historical metric values for anomaly detection."""
        with self._conn.cursor() as cur:
            cur.execute(
                """SELECT value, recorded_at FROM provero_metric
                   WHERE suite_name = %s AND check_name = %s AND metric_name = %s
                   ORDER BY recorded_at DESC LIMIT %s""",
                (suite_name, check_name, metric_name, limit),
            )
            columns = [desc[0] for desc in cur.description or []]
            return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]

    def close(self) -> None:
        self._conn.close()
