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

"""Tests for the expanded SQLite store schema."""

from __future__ import annotations

import sqlite3

import pytest

from provero.store.sqlite import SQLiteStore


@pytest.fixture
def store(tmp_path):
    db_path = tmp_path / "test.db"
    s = SQLiteStore(db_path)
    yield s, db_path
    s.close()


class TestStoreSchema:
    def test_all_tables_created(self, store):
        _s, db_path = store
        conn = sqlite3.connect(str(db_path))
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "provero_run" in table_names
        assert "provero_check_result" in table_names
        assert "provero_metric" in table_names
        conn.close()

    def test_run_has_trigger_and_completed_at(self, store):
        _s, db_path = store
        conn = sqlite3.connect(str(db_path))
        info = conn.execute("PRAGMA table_info(provero_run)").fetchall()
        col_names = [row[1] for row in info]
        assert "trigger" in col_names
        assert "completed_at" in col_names
        conn.close()

    def test_check_result_has_failing_sample(self, store):
        _s, db_path = store
        conn = sqlite3.connect(str(db_path))
        info = conn.execute("PRAGMA table_info(provero_check_result)").fetchall()
        col_names = [row[1] for row in info]
        assert "failing_sample" in col_names
        conn.close()

    def test_indexes_created(self, store):
        _s, db_path = store
        conn = sqlite3.connect(str(db_path))
        indexes = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name"
        ).fetchall()
        index_names = [i[0] for i in indexes]
        assert "idx_check_type" in index_names
        assert "idx_check_status" in index_names
        assert "idx_metric_lookup" in index_names
        conn.close()
