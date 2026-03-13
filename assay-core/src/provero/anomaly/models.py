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

"""Anomaly detection models."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class Sensitivity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


SENSITIVITY_THRESHOLDS: dict[str, float] = {
    "low": 4.0,
    "medium": 3.0,
    "high": 2.0,
}


class AnomalyResult(BaseModel):
    """Result of an anomaly detection check."""

    is_anomaly: bool
    anomaly_score: float = 0.0
    observed_value: float = 0.0
    expected_range: tuple[float, float] = (0.0, 0.0)
    method: str = ""
    sensitivity: str = "medium"
    explanation: str = ""
    data_points: int = 0
