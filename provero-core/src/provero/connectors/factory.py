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

"""Connector factory with plugin discovery via entry_points.

Third-party connectors register themselves in their pyproject.toml::

    [project.entry-points."provero.connectors"]
    mysql = "provero_mysql:MySQLConnector"

The factory discovers them automatically at runtime.
"""

from __future__ import annotations

import os
from importlib.metadata import entry_points
from typing import Any

from provero.core.compiler import SourceConfig

# Built-in connector mappings (type_name -> import_path, class_name)
_BUILTINS: dict[str, tuple[str, str]] = {
    "duckdb": ("provero.connectors.duckdb", "DuckDBConnector"),
    "postgres": ("provero.connectors.postgres", "PostgresConnector"),
    "postgresql": ("provero.connectors.postgres", "PostgresConnector"),
    "mysql": ("provero.connectors.postgres", "SQLAlchemyConnector"),
    "sqlite": ("provero.connectors.postgres", "SQLAlchemyConnector"),
    "snowflake": ("provero.connectors.postgres", "SQLAlchemyConnector"),
    "bigquery": ("provero.connectors.postgres", "SQLAlchemyConnector"),
    "redshift": ("provero.connectors.postgres", "SQLAlchemyConnector"),
    "databricks": ("provero.connectors.postgres", "SQLAlchemyConnector"),
}

_PLUGIN_REGISTRY: dict[str, Any] = {}
_PLUGINS_LOADED = False


def _load_plugins() -> None:
    """Discover connector plugins via entry_points."""
    global _PLUGINS_LOADED
    if _PLUGINS_LOADED:
        return
    for ep in entry_points(group="provero.connectors"):
        _PLUGIN_REGISTRY[ep.name] = ep
    _PLUGINS_LOADED = True


def _load_builtin(source_type: str) -> Any:
    """Lazy-import a built-in connector class."""
    if source_type not in _BUILTINS:
        return None
    module_path, class_name = _BUILTINS[source_type]
    import importlib

    mod = importlib.import_module(module_path)
    return getattr(mod, class_name)


def create_connector(source: SourceConfig):
    """Create a connector based on source type.

    Resolution order:
    1. entry_points plugins (``provero.connectors`` group)
    2. Built-in connectors (DuckDB, Postgres, SQLAlchemy-based)

    Plugins take priority so users can override built-ins.
    """
    source_type = source.type.lower()
    connection = _resolve_connection(source.connection)

    # DataFrame is special: can't be created from config alone
    if source_type in ("dataframe", "pandas", "polars"):
        msg = (
            "DataFrame connector requires passing the connector directly. "
            "Use DataFrameConnector(df, table_name='...') instead of factory."
        )
        raise ValueError(msg)

    # 1. Check plugins first (they can override built-ins)
    _load_plugins()
    if source_type in _PLUGIN_REGISTRY:
        connector_class = _PLUGIN_REGISTRY[source_type].load()
        return connector_class(connection_string=connection) if connection else connector_class()

    # 2. Fall back to built-ins
    connector_class = _load_builtin(source_type)
    if connector_class is not None:
        if source_type == "duckdb":
            database = connection if connection else ":memory:"
            return connector_class(database=database)
        if not connection:
            msg = f"{source_type} connector requires a connection string"
            raise ValueError(msg)
        return connector_class(connection_string=connection)

    # 3. Nothing found
    available = sorted(set(list(_BUILTINS.keys()) + list(_PLUGIN_REGISTRY.keys())))
    available = [t for t in available if t not in ("postgresql",)]  # dedupe alias
    msg = (
        f"Unknown source type: '{source_type}'. "
        f"Available: {', '.join(available)}. "
        f"Install a plugin (pip install provero-connector-{source_type}) or check the type name."
    )
    raise ValueError(msg)


def list_connectors() -> list[str]:
    """List all available connector types (built-in + plugins)."""
    _load_plugins()
    all_types = set(_BUILTINS.keys()) | set(_PLUGIN_REGISTRY.keys())
    all_types.discard("postgresql")  # alias
    return sorted(all_types)


def _resolve_connection(connection: str) -> str:
    """Resolve environment variables in connection strings."""
    if not connection:
        return connection
    if connection.startswith("${") and connection.endswith("}"):
        env_var = connection[2:-1]
        value = os.environ.get(env_var)
        if value is None:
            msg = f"Environment variable {env_var} is not set"
            raise ValueError(msg)
        return value
    return os.path.expandvars(connection)
