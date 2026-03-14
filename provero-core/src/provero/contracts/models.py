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

"""Data contract models."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class ColumnContract(BaseModel):
    """Contract for a single column.

    Checks can be simple strings ("not_null", "unique") or dicts with
    params ({"range": {"min": 0.01}}, {"accepted_values": ["USD", "EUR"]}).
    """

    name: str
    type: str = ""
    checks: list[str | dict] = Field(default_factory=list)
    description: str = ""


class SchemaContract(BaseModel):
    """Schema contract defining expected columns."""

    columns: list[ColumnContract] = Field(default_factory=list)


class SLAConfig(BaseModel):
    """Service Level Agreement configuration."""

    freshness: str = ""
    completeness: str = ""
    availability: str = ""


class ViolationAction(StrEnum):
    BLOCK = "block"
    WARN = "warn"
    QUARANTINE = "quarantine"


class ContractConfig(BaseModel):
    """Full data contract configuration."""

    name: str
    owner: str = ""
    version: str = "1.0"
    source: str = ""
    table: str = ""
    sla: SLAConfig = Field(default_factory=SLAConfig)
    schema_def: SchemaContract = Field(default_factory=SchemaContract)
    on_violation: ViolationAction = ViolationAction.WARN


class ContractViolation(BaseModel):
    """A single contract violation."""

    rule: str
    message: str
    severity: str = "warning"


class SchemaDrift(BaseModel):
    """Schema drift detected between contract and actual table."""

    column: str
    change_type: str  # added, removed, type_changed
    expected: str = ""
    actual: str = ""


class ContractResult(BaseModel):
    """Result of contract validation."""

    contract_name: str
    status: str = "pass"  # pass, fail, warn
    violations: list[ContractViolation] = Field(default_factory=list)
    schema_drift: list[SchemaDrift] = Field(default_factory=list)


class ContractChange(BaseModel):
    """A single change between two contract versions."""

    field: str
    change_type: str  # added, removed, changed
    old_value: str = ""
    new_value: str = ""
    is_breaking: bool = False
