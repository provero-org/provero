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

"""Tests for graceful degradation when optional dependencies are missing."""

from __future__ import annotations

import importlib
import inspect
import sys
from unittest.mock import patch


class TestGracefulDegradation:
    def test_core_import_without_pandas(self):
        """provero.core should import even without pandas installed."""
        with patch.dict(sys.modules, {"pandas": None}):
            # Force re-import
            mod = importlib.import_module("provero.core.compiler")
            assert hasattr(mod, "compile_file")

    def test_core_import_without_scipy(self):
        """provero.anomaly should work without scipy (uses stdlib statistics)."""
        with patch.dict(sys.modules, {"scipy": None, "scipy.stats": None}):
            mod = importlib.import_module("provero.anomaly.detectors")
            assert hasattr(mod, "detect_zscore")

    def test_anomaly_detectors_use_stdlib_only(self):
        """Anomaly detectors should not import scipy or numpy."""
        from provero.anomaly import detectors

        source = inspect.getsource(detectors)
        # Detectors should use stdlib statistics, not scipy/numpy
        assert "import scipy" not in source
        assert "import numpy" not in source
        assert "from scipy" not in source
        assert "from numpy" not in source

    def test_dataframe_connector_needs_duckdb(self):
        """DataFrameConnector uses duckdb internally and should work."""
        # duckdb is a required dep, not optional

        from provero.connectors.dataframe import DataFrameConnector

        assert DataFrameConnector is not None
