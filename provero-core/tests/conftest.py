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

"""Shared test fixtures."""

from __future__ import annotations

import textwrap
from pathlib import Path

import duckdb
import pytest

from provero.connectors.duckdb import DuckDBConnector
from provero.store.sqlite import SQLiteStore


@pytest.fixture
def duckdb_orders():
    """DuckDB connection with orders + events tables for check testing."""
    connector = DuckDBConnector()
    conn = connector.connect()
    conn._conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id VARCHAR,
            amount DECIMAL(10,2),
            status VARCHAR
        )
    """)
    conn._conn.execute("""
        INSERT INTO orders VALUES
        (1, 'C001', 150.00, 'delivered'),
        (2, 'C002', 89.99, 'shipped'),
        (3, 'C003', 220.50, 'pending'),
        (4, 'C001', 45.00, 'delivered'),
        (5, 'C004', 999.99, 'cancelled')
    """)
    conn._conn.execute("""
        CREATE TABLE events (
            event_id INTEGER,
            user_id VARCHAR,
            event_type VARCHAR,
            email VARCHAR,
            created_at TIMESTAMP
        )
    """)
    conn._conn.execute("""
        INSERT INTO events VALUES
        (1, 'U001', 'login', 'alice@example.com', CURRENT_TIMESTAMP - INTERVAL '1 hour'),
        (2, 'U002', 'purchase', 'bob@test.com', CURRENT_TIMESTAMP - INTERVAL '2 hours'),
        (3, 'U001', 'logout', 'alice@example.com', CURRENT_TIMESTAMP - INTERVAL '30 minutes'),
        (4, 'U003', 'login', 'charlie@example.com', CURRENT_TIMESTAMP - INTERVAL '10 minutes'),
        (5, 'U004', 'purchase', 'bad-email', CURRENT_TIMESTAMP - INTERVAL '5 minutes')
    """)
    yield conn
    connector.disconnect(conn)


@pytest.fixture
def sqlite_store(tmp_path: Path):
    """SQLite store with temp database."""
    db_path = tmp_path / "test_results.db"
    s = SQLiteStore(db_path)
    yield s
    s.close()


@pytest.fixture
def cli_runner():
    """Typer CLI test runner."""
    from typer.testing import CliRunner

    return CliRunner()


@pytest.fixture
def duckdb_file(tmp_path: Path) -> Path:
    """File-based DuckDB with orders table (5 rows).

    CLI tests need file-based DuckDB because create_connector() opens a new
    connection, so :memory: databases would be empty.
    """
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id VARCHAR,
            amount DECIMAL(10,2),
            status VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO orders VALUES
        (1, 'C001', 150.00, 'delivered'),
        (2, 'C002', 89.99, 'shipped'),
        (3, 'C003', 220.50, 'pending'),
        (4, 'C001', 45.00, 'delivered'),
        (5, 'C004', 999.99, 'cancelled')
    """)
    conn.execute("""
        CREATE TABLE events (
            event_id INTEGER,
            user_id VARCHAR,
            event_type VARCHAR,
            email VARCHAR,
            created_at TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO events VALUES
        (1, 'U001', 'login', 'alice@example.com', CURRENT_TIMESTAMP - INTERVAL '1 hour'),
        (2, 'U002', 'purchase', 'bob@test.com', CURRENT_TIMESTAMP - INTERVAL '2 hours'),
        (3, 'U001', 'logout', 'alice@example.com', CURRENT_TIMESTAMP - INTERVAL '30 minutes'),
        (4, 'U003', 'login', 'charlie@example.com', CURRENT_TIMESTAMP - INTERVAL '10 minutes'),
        (5, 'U004', 'purchase', 'bad-email', CURRENT_TIMESTAMP - INTERVAL '5 minutes')
    """)
    conn.close()
    return db_path


@pytest.fixture
def sample_config_file(tmp_path: Path) -> Path:
    """Minimal valid provero.yaml for testing."""
    config_path = tmp_path / "provero.yaml"
    config_path.write_text(
        textwrap.dedent("""\
        source:
          type: duckdb
          table: orders

        checks:
          - not_null: order_id
          - unique: order_id
          - row_count:
              min: 1
    """)
    )
    return config_path


@pytest.fixture
def duckdb_config_file(tmp_path: Path, duckdb_file: Path) -> dict:
    """provero.yaml pointing to a file-based DuckDB.

    Returns dict with 'config_path' and 'db_path'.
    """
    config_path = tmp_path / "provero.yaml"
    config_path.write_text(
        textwrap.dedent(f"""\
        source:
          type: duckdb
          connection: "{duckdb_file}"
          table: orders

        checks:
          - not_null: order_id
          - unique: order_id
          - row_count:
              min: 1
    """)
    )
    return {"config_path": config_path, "db_path": duckdb_file}
