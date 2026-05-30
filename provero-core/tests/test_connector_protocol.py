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


class TestContextManager:
    """Regression tests for A9: connections must support ``with`` (no leak)."""

    def test_with_block_closes_connection(self):
        connector = DuckDBConnector()
        with connector.connect() as conn:
            assert conn.execute("SELECT 1 AS one")[0]["one"] == 1
        # After the with block the underlying connection must be closed.
        with pytest.raises(Exception, match=r"[Cc]losed"):
            conn.execute("SELECT 1")

    def test_enter_returns_connection(self):
        connector = DuckDBConnector()
        conn = connector.connect()
        with conn as entered:
            assert entered is conn

    def test_with_block_closes_on_exception(self):
        connector = DuckDBConnector()
        conn = connector.connect()
        with pytest.raises(RuntimeError), conn:
            raise RuntimeError("boom")
        # Connection still closed despite the exception propagating.
        with pytest.raises(Exception, match=r"[Cc]losed"):
            conn.execute("SELECT 1")

    def test_close_method(self):
        connector = DuckDBConnector()
        conn = connector.connect()
        conn.close()
        with pytest.raises(Exception, match=r"[Cc]losed"):
            conn.execute("SELECT 1")


class TestExecuteParams:
    """Regression for M11: execute() must honour its params argument."""

    def test_named_params_are_bound(self):
        connector = DuckDBConnector()
        conn = connector.connect()
        conn._conn.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")
        conn._conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        # Old behaviour dropped params, so the named placeholder raised
        # "Values were not provided". With params bound it filters correctly.
        rows = conn.execute("SELECT name FROM t WHERE id = $id", {"id": 2})
        assert rows == [{"name": "b"}]
        connector.disconnect(conn)

    def test_no_params_still_works(self):
        connector = DuckDBConnector()
        conn = connector.connect()
        assert conn.execute("SELECT 1 AS one") == [{"one": 1}]
        connector.disconnect(conn)


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
