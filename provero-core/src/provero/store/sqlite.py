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

"""SQLite result store for persisting check results locally."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from provero.core.results import CheckResult, SuiteResult

DEFAULT_DB_PATH = Path(".provero/results.db")


class SQLiteStore:
    """Stores check results in a local SQLite database."""

    def __init__(self, db_path: str | Path = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self) -> None:
        self._conn.executescript("""
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
                quality_score REAL,
                duration_ms INTEGER,
                started_at TEXT NOT NULL,
                completed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS provero_check_result (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            );

            CREATE TABLE IF NOT EXISTS provero_metric (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                suite_name TEXT NOT NULL,
                check_name TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                value REAL NOT NULL,
                recorded_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_run_suite ON provero_run(suite_name);
            CREATE INDEX IF NOT EXISTS idx_run_started ON provero_run(started_at);
            CREATE INDEX IF NOT EXISTS idx_check_run ON provero_check_result(run_id);
            CREATE INDEX IF NOT EXISTS idx_check_type ON provero_check_result(check_type);
            CREATE INDEX IF NOT EXISTS idx_check_status ON provero_check_result(status);
            CREATE INDEX IF NOT EXISTS idx_metric_lookup
                ON provero_metric(suite_name, check_name, metric_name, recorded_at);
        """)

    def save_result(self, result: SuiteResult) -> str:
        """Save a suite result. Returns the run_id."""
        run_id = result.checks[0].run_id if result.checks else ""
        if not run_id:
            import uuid

            run_id = str(uuid.uuid4())

        completed_at = datetime.now(tz=UTC).isoformat()
        self._conn.execute(
            """INSERT INTO provero_run
               (id, suite_name, status, trigger, total, passed, failed, warned, errored,
                quality_score, duration_ms, started_at, completed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
            self._conn.execute(
                """INSERT INTO provero_check_result
                   (run_id, check_name, check_type, status, severity,
                    source_table, source_column, observed_value, expected_value,
                    row_count, failing_rows, failing_sample, failing_query, duration_ms)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                    json.dumps(check.failing_rows_sample) if check.failing_rows_sample else None,
                    check.failing_rows_query,
                    check.duration_ms,
                ),
            )

            # Store numeric metrics for anomaly detection
            self._store_metrics(result.suite_name, check)

        self._conn.commit()
        return run_id

    def _store_metrics(self, suite_name: str, check: CheckResult) -> None:
        """Extract and store numeric metrics from a check result."""
        now = datetime.now(tz=UTC).isoformat()
        insert_sql = (
            "INSERT INTO provero_metric "
            "(suite_name, check_name, metric_name, value, recorded_at) "
            "VALUES (?, ?, ?, ?, ?)"
        )

        if check.check_type == "row_count":
            try:
                value = float(str(check.observed_value).replace(",", ""))
                self._conn.execute(
                    insert_sql,
                    (suite_name, check.check_name, "row_count", value, now),
                )
            except ValueError:
                pass

        if check.check_type == "not_null" and check.failing_rows is not None:
            self._conn.execute(
                insert_sql,
                (suite_name, check.check_name, "null_count", float(check.failing_rows), now),
            )

        if check.check_type == "completeness" and check.observed_value:
            try:
                # observed_value is like "95.0%" from completeness check
                raw = str(check.observed_value).strip()
                pct = float(raw[:-1]) / 100.0 if raw.endswith("%") else float(raw)
                self._conn.execute(
                    insert_sql,
                    (suite_name, check.check_name, "completeness_pct", pct, now),
                )
            except (ValueError, TypeError):
                pass

        if check.check_type == "row_count_change" and check.row_count is not None:
            self._conn.execute(
                insert_sql, (suite_name, check.check_name, "row_count", float(check.row_count), now)
            )

        if check.failing_rows is not None and check.row_count and check.row_count > 0:
            fail_rate = check.failing_rows / check.row_count
            self._conn.execute(
                insert_sql,
                (suite_name, check.check_name, "fail_rate", fail_rate, now),
            )

        # Store any numeric observed_value as a generic metric
        if check.check_type not in ("row_count", "not_null", "completeness", "row_count_change"):
            try:
                observed_str = str(check.observed_value).split()[0].replace(",", "")
                numeric_val = float(observed_str)
                self._conn.execute(
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
        if suite_name:
            rows = self._conn.execute(
                "SELECT * FROM provero_run WHERE suite_name = ? ORDER BY started_at DESC LIMIT ?",
                (suite_name, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM provero_run ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_run_details(self, run_id: str) -> list[dict]:
        """Get check results for a specific run."""
        rows = self._conn.execute(
            "SELECT * FROM provero_check_result WHERE run_id = ? ORDER BY id",
            (run_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def get_metrics(
        self,
        suite_name: str,
        check_name: str,
        metric_name: str,
        limit: int = 30,
    ) -> list[dict]:
        """Get historical metric values for anomaly detection."""
        rows = self._conn.execute(
            """SELECT value, recorded_at FROM provero_metric
               WHERE suite_name = ? AND check_name = ? AND metric_name = ?
               ORDER BY recorded_at DESC LIMIT ?""",
            (suite_name, check_name, metric_name, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def close(self) -> None:
        self._conn.close()
