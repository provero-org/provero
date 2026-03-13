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

"""Tests for the connector factory and plugin discovery."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from provero.connectors.duckdb import DuckDBConnector
from provero.connectors.factory import (
    _PLUGIN_REGISTRY,
    create_connector,
    list_connectors,
)
from provero.core.compiler import SourceConfig


class TestCreateConnector:
    def test_duckdb(self):
        source = SourceConfig(type="duckdb")
        connector = create_connector(source)
        assert isinstance(connector, DuckDBConnector)

    def test_duckdb_with_database(self):
        source = SourceConfig(type="duckdb", connection=":memory:")
        connector = create_connector(source)
        assert isinstance(connector, DuckDBConnector)

    def test_unknown_type_raises(self):
        source = SourceConfig(type="unknown_db")
        with pytest.raises(ValueError, match="Unknown source type"):
            create_connector(source)

    def test_postgres_without_connection_raises(self):
        source = SourceConfig(type="postgres", connection="")
        with pytest.raises(ValueError, match="requires a connection string"):
            create_connector(source)

    def test_env_var_resolution(self):
        os.environ["TEST_ASSAY_DB"] = "postgresql://localhost/test"
        try:
            source = SourceConfig(type="postgres", connection="${TEST_ASSAY_DB}")
            from provero.connectors.postgres import PostgresConnector
            connector = create_connector(source)
            assert isinstance(connector, PostgresConnector)
        finally:
            del os.environ["TEST_ASSAY_DB"]

    def test_missing_env_var_raises(self):
        source = SourceConfig(type="postgres", connection="${NONEXISTENT_VAR_ASSAY}")
        with pytest.raises(ValueError, match="not set"):
            create_connector(source)

    def test_error_message_suggests_pip_install(self):
        source = SourceConfig(type="clickhouse")
        with pytest.raises(ValueError, match="pip install provero-connector-clickhouse"):
            create_connector(source)


class TestPluginDiscovery:
    def test_plugin_connector_takes_priority(self):
        """A plugin connector overrides built-in for the same type."""
        mock_connector = MagicMock()
        mock_ep = MagicMock()
        mock_ep.load.return_value = mock_connector

        saved = _PLUGIN_REGISTRY.copy()
        try:
            _PLUGIN_REGISTRY["custom_db"] = mock_ep
            source = SourceConfig(type="custom_db", connection="custom://localhost")
            connector = create_connector(source)
            mock_ep.load.assert_called_once()
            mock_connector.assert_called_once_with(connection_string="custom://localhost")
        finally:
            _PLUGIN_REGISTRY.clear()
            _PLUGIN_REGISTRY.update(saved)

    def test_plugin_connector_without_connection(self):
        """A plugin connector with no connection string."""
        mock_connector = MagicMock()
        mock_ep = MagicMock()
        mock_ep.load.return_value = mock_connector

        saved = _PLUGIN_REGISTRY.copy()
        try:
            _PLUGIN_REGISTRY["embedded_db"] = mock_ep
            source = SourceConfig(type="embedded_db")
            connector = create_connector(source)
            mock_connector.assert_called_once_with()
        finally:
            _PLUGIN_REGISTRY.clear()
            _PLUGIN_REGISTRY.update(saved)


class TestListConnectors:
    def test_includes_builtins(self):
        connectors = list_connectors()
        assert "duckdb" in connectors
        assert "postgres" in connectors
        assert "snowflake" in connectors

    def test_no_postgresql_alias(self):
        connectors = list_connectors()
        assert "postgresql" not in connectors

    def test_includes_plugins(self):
        saved = _PLUGIN_REGISTRY.copy()
        try:
            _PLUGIN_REGISTRY["custom_db"] = MagicMock()
            connectors = list_connectors()
            assert "custom_db" in connectors
        finally:
            _PLUGIN_REGISTRY.clear()
            _PLUGIN_REGISTRY.update(saved)
