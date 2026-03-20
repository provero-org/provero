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

"""Anomaly detection algorithms using stdlib only."""

from __future__ import annotations

import statistics

from provero.anomaly.models import SENSITIVITY_THRESHOLDS, AnomalyResult

MIN_DATA_POINTS = 5


def detect_zscore(
    values: list[float],
    current: float,
    threshold: float = 3.0,
) -> AnomalyResult:
    """Detect anomalies using Z-Score method.

    Uses mean and standard deviation. Simple but sensitive to outliers
    in the historical data.
    """
    if len(values) < MIN_DATA_POINTS:
        return AnomalyResult(
            is_anomaly=False,
            observed_value=current,
            method="zscore",
            explanation="Insufficient history",
            data_points=len(values),
        )

    mean = statistics.mean(values)
    stdev = statistics.stdev(values)

    if stdev == 0:
        is_anomaly = current != mean
        return AnomalyResult(
            is_anomaly=is_anomaly,
            anomaly_score=1.0 if is_anomaly else 0.0,
            observed_value=current,
            expected_range=(mean, mean),
            method="zscore",
            explanation="All historical values identical; any deviation is anomalous"
            if is_anomaly
            else "Value matches constant history",
            data_points=len(values),
        )

    z = abs(current - mean) / stdev
    score = min(z / threshold, 1.0)
    lower = mean - threshold * stdev
    upper = mean + threshold * stdev

    return AnomalyResult(
        is_anomaly=z > threshold,
        anomaly_score=round(score, 4),
        observed_value=current,
        expected_range=(round(lower, 4), round(upper, 4)),
        method="zscore",
        explanation=f"Z-score={z:.2f}, threshold={threshold}",
        data_points=len(values),
    )


def detect_mad(
    values: list[float],
    current: float,
    threshold: float = 3.0,
) -> AnomalyResult:
    """Detect anomalies using Median Absolute Deviation (MAD).

    More robust to outliers than Z-Score. Default method.
    MAD = median(|xi - median(x)|) * 1.4826
    """
    if len(values) < MIN_DATA_POINTS:
        return AnomalyResult(
            is_anomaly=False,
            observed_value=current,
            method="mad",
            explanation="Insufficient history",
            data_points=len(values),
        )

    med = statistics.median(values)
    deviations = [abs(v - med) for v in values]
    mad = statistics.median(deviations) * 1.4826

    if mad == 0:
        is_anomaly = current != med
        return AnomalyResult(
            is_anomaly=is_anomaly,
            anomaly_score=1.0 if is_anomaly else 0.0,
            observed_value=current,
            expected_range=(med, med),
            method="mad",
            explanation="All historical values identical; any deviation is anomalous"
            if is_anomaly
            else "Value matches constant history",
            data_points=len(values),
        )

    modified_z = abs(current - med) / mad
    score = min(modified_z / threshold, 1.0)
    lower = med - threshold * mad
    upper = med + threshold * mad

    return AnomalyResult(
        is_anomaly=modified_z > threshold,
        anomaly_score=round(score, 4),
        observed_value=current,
        expected_range=(round(lower, 4), round(upper, 4)),
        method="mad",
        explanation=f"Modified Z-score={modified_z:.2f}, threshold={threshold}",
        data_points=len(values),
    )


def detect_iqr(
    values: list[float],
    current: float,
    threshold: float = 1.5,
) -> AnomalyResult:
    """Detect anomalies using Interquartile Range (IQR).

    Uses Q1/Q3 with fence k * IQR. Good for skewed distributions.
    """
    if len(values) < MIN_DATA_POINTS:
        return AnomalyResult(
            is_anomaly=False,
            observed_value=current,
            method="iqr",
            explanation="Insufficient history",
            data_points=len(values),
        )

    quartiles = statistics.quantiles(values, n=4)
    q1 = quartiles[0]
    q3 = quartiles[2]
    iqr = q3 - q1

    if iqr == 0:
        is_anomaly = current < q1 or current > q3
        return AnomalyResult(
            is_anomaly=is_anomaly,
            anomaly_score=1.0 if is_anomaly else 0.0,
            observed_value=current,
            expected_range=(q1, q3),
            method="iqr",
            explanation="IQR is zero; values are concentrated"
            if is_anomaly
            else "Value within concentrated range",
            data_points=len(values),
        )

    lower = q1 - threshold * iqr
    upper = q3 + threshold * iqr
    is_anomaly = current < lower or current > upper

    if is_anomaly:
        distance = max(lower - current, current - upper, 0)
        score = min(distance / (threshold * iqr), 1.0)
    else:
        score = 0.0

    return AnomalyResult(
        is_anomaly=is_anomaly,
        anomaly_score=round(score, 4),
        observed_value=current,
        expected_range=(round(lower, 4), round(upper, 4)),
        method="iqr",
        explanation=f"IQR={iqr:.2f}, fence={threshold}",
        data_points=len(values),
    )


_DETECTORS = {
    "zscore": detect_zscore,
    "mad": detect_mad,
    "iqr": detect_iqr,
}


def detect_anomaly(
    values: list[float],
    current: float,
    method: str = "mad",
    sensitivity: str = "medium",
) -> AnomalyResult:
    """Dispatch anomaly detection to the appropriate method.

    Args:
        values: Historical metric values (oldest to newest).
        current: The current value to check.
        method: Detection method ("zscore", "mad", "iqr").
        sensitivity: Sensitivity level ("low", "medium", "high").

    Returns:
        AnomalyResult with detection details.
    """
    detector = _DETECTORS.get(method)
    if detector is None:
        available = ", ".join(sorted(_DETECTORS.keys()))
        return AnomalyResult(
            is_anomaly=False,
            observed_value=current,
            method=method,
            explanation=f"Unknown method '{method}'. Available: {available}",
            data_points=len(values),
        )

    threshold = SENSITIVITY_THRESHOLDS.get(sensitivity, 3.0)
    result = detector(values, current, threshold)
    result.sensitivity = sensitivity
    return result
