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

"""Unit tests for anomaly detection algorithms."""

from __future__ import annotations

import pytest

from provero.anomaly.detectors import detect_anomaly, detect_iqr, detect_mad, detect_zscore


class TestZScore:
    def test_normal_value(self):
        values = [100.0, 102.0, 98.0, 101.0, 99.0, 100.5, 101.5, 99.5]
        result = detect_zscore(values, 100.0, threshold=3.0)
        assert not result.is_anomaly
        assert result.method == "zscore"
        assert result.data_points == 8

    def test_anomalous_value(self):
        values = [100.0, 102.0, 98.0, 101.0, 99.0, 100.5, 101.5, 99.5]
        result = detect_zscore(values, 200.0, threshold=3.0)
        assert result.is_anomaly
        assert result.anomaly_score > 0

    def test_insufficient_data(self):
        values = [1.0, 2.0, 3.0]
        result = detect_zscore(values, 100.0)
        assert not result.is_anomaly
        assert "Insufficient" in result.explanation

    def test_constant_values_same(self):
        values = [5.0] * 10
        result = detect_zscore(values, 5.0)
        assert not result.is_anomaly

    def test_constant_values_different(self):
        values = [5.0] * 10
        result = detect_zscore(values, 6.0)
        assert result.is_anomaly
        assert result.anomaly_score == 1.0


class TestMAD:
    def test_normal_value(self):
        values = [100.0, 102.0, 98.0, 101.0, 99.0, 100.5, 101.5, 99.5]
        result = detect_mad(values, 100.0, threshold=3.0)
        assert not result.is_anomaly
        assert result.method == "mad"

    def test_anomalous_value(self):
        values = [100.0, 102.0, 98.0, 101.0, 99.0, 100.5, 101.5, 99.5]
        result = detect_mad(values, 200.0, threshold=3.0)
        assert result.is_anomaly

    def test_robust_to_outliers(self):
        """MAD should handle datasets with existing outliers better than Z-Score."""
        values = [10.0, 11.0, 10.5, 10.2, 9.8, 100.0, 10.3, 10.1]  # 100 is an outlier
        result = detect_mad(values, 10.0)
        assert not result.is_anomaly

    def test_insufficient_data(self):
        result = detect_mad([1.0, 2.0], 5.0)
        assert not result.is_anomaly

    def test_constant_values(self):
        values = [42.0] * 10
        result = detect_mad(values, 43.0)
        assert result.is_anomaly


class TestIQR:
    def test_normal_value(self):
        values = list(range(20))
        result = detect_iqr([float(v) for v in values], 10.0)
        assert not result.is_anomaly
        assert result.method == "iqr"

    def test_anomalous_value(self):
        values = [float(v) for v in range(20)]
        result = detect_iqr(values, 100.0, threshold=1.5)
        assert result.is_anomaly

    def test_insufficient_data(self):
        result = detect_iqr([1.0, 2.0], 50.0)
        assert not result.is_anomaly

    def test_concentrated_values(self):
        values = [5.0] * 10
        result = detect_iqr(values, 5.0)
        assert not result.is_anomaly


class TestDispatcher:
    def test_default_method_is_mad(self):
        values = [100.0, 102.0, 98.0, 101.0, 99.0, 100.5, 101.5, 99.5]
        result = detect_anomaly(values, 100.0)
        assert result.method == "mad"

    def test_sensitivity_low(self):
        values = [100.0, 102.0, 98.0, 101.0, 99.0, 100.5, 101.5, 99.5]
        result = detect_anomaly(values, 108.0, sensitivity="low")
        assert result.sensitivity == "low"
        # Low sensitivity = higher threshold = less likely to flag

    def test_sensitivity_high(self):
        values = [100.0, 102.0, 98.0, 101.0, 99.0, 100.5, 101.5, 99.5]
        result = detect_anomaly(values, 108.0, sensitivity="high")
        assert result.sensitivity == "high"

    def test_unknown_method(self):
        result = detect_anomaly([1.0, 2.0, 3.0, 4.0, 5.0], 3.0, method="invalid")
        assert not result.is_anomaly
        assert "Unknown" in result.explanation

    def test_zscore_via_dispatcher(self):
        values = [100.0, 102.0, 98.0, 101.0, 99.0, 100.5, 101.5, 99.5]
        result = detect_anomaly(values, 100.0, method="zscore")
        assert result.method == "zscore"

    def test_iqr_via_dispatcher(self):
        values = [float(v) for v in range(20)]
        result = detect_anomaly(values, 10.0, method="iqr")
        assert result.method == "iqr"
