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

"""DataFrame connector for Pandas and Polars DataFrames via DuckDB."""

from __future__ import annotations

from typing import Any

import duckdb

from provero.connectors.duckdb import DuckDBConnection


class DataFrameConnection(DuckDBConnection):
    """Connection that wraps a DataFrame registered in DuckDB."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        super().__init__(conn)
        self._table_name = table_name


class DataFrameConnector:
    """Connector for Pandas and Polars DataFrames.

    Registers the DataFrame as a virtual table in an in-memory DuckDB
    instance, allowing full SQL execution against it. Supports both
    Pandas and Polars DataFrames transparently.

    Usage::

        import pandas as pd
        df = pd.read_csv("orders.csv")
        connector = DataFrameConnector(df, table_name="orders")
        conn = connector.connect()
        result = conn.execute("SELECT COUNT(*) as cnt FROM orders")
    """

    def __init__(
        self,
        dataframe: Any,
        table_name: str = "df",
    ) -> None:
        self._dataframe = dataframe
        self._table_name = table_name

    def connect(self) -> DataFrameConnection:
        conn = duckdb.connect(":memory:")

        # Polars DataFrames: convert to Arrow for DuckDB registration
        if hasattr(self._dataframe, "to_arrow"):
            arrow_table = self._dataframe.to_arrow()
            conn.register(self._table_name, arrow_table)
        else:
            # Pandas DataFrames are natively supported by DuckDB
            conn.register(self._table_name, self._dataframe)

        return DataFrameConnection(conn, self._table_name)

    def disconnect(self, connection: DataFrameConnection) -> None:
        connection._conn.close()
