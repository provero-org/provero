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

"""Tests for the connector protocol: get_schema and get_profile."""

from __future__ import annotations

import pytest

from provero.connectors.duckdb import DuckDBConnector


@pytest.fixture
def duckdb_with_data():
    connector = DuckDBConnector()
    conn = connector.connect()
    conn._conn.execute("""
        CREATE TABLE test_table (
            id INTEGER,
            name VARCHAR,
            score DECIMAL(10,2)
        )
    """)
    conn._conn.execute("""
        INSERT INTO test_table VALUES
        (1, 'alice', 95.5),
        (2, 'bob', 87.3),
        (3, 'charlie', 92.1)
    """)
    yield connector, conn
    connector.disconnect(conn)


class TestGetSchema:
    def test_returns_columns(self, duckdb_with_data):
        connector, conn = duckdb_with_data
        schema = connector.get_schema(conn, "test_table")
        assert len(schema) == 3
        names = [c["name"] for c in schema]
        assert "id" in names
        assert "name" in names
        assert "score" in names

    def test_column_types(self, duckdb_with_data):
        connector, conn = duckdb_with_data
        schema = connector.get_schema(conn, "test_table")
        type_map = {c["name"]: c["type"] for c in schema}
        assert "INTEGER" in type_map["id"]
        assert "VARCHAR" in type_map["name"]


class TestGetProfile:
    def test_returns_profile_data(self, duckdb_with_data):
        connector, conn = duckdb_with_data
        profile = connector.get_profile(conn, "test_table")
        assert profile["table"] == "test_table"
        assert profile["row_count"] == 3
        assert profile["column_count"] == 3

    def test_profile_column_filter(self, duckdb_with_data):
        connector, conn = duckdb_with_data
        profile = connector.get_profile(conn, "test_table", columns=["id"])
        col_names = [c["name"] for c in profile["columns"]]
        assert col_names == ["id"]

    def test_profile_stats(self, duckdb_with_data):
        connector, conn = duckdb_with_data
        profile = connector.get_profile(conn, "test_table")
        for col in profile["columns"]:
            assert "null_count" in col
            assert "distinct_count" in col
