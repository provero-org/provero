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

"""Data contract models."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class ChangeLevel(StrEnum):
    """Aggregate classification of a set of contract changes.

    NONE means the two versions are identical. NON_BREAKING means only
    backward-compatible changes were found (e.g. adding an optional column).
    BREAKING means at least one change would break existing consumers (e.g.
    removing a column or tightening a type).
    """

    NONE = "none"
    NON_BREAKING = "non_breaking"
    BREAKING = "breaking"


class SeverityLevel(StrEnum):
    """Severity assigned to a change by a severity policy.

    BLOCKER is the strongest level and is intended to halt a pipeline.
    """

    INFO = "info"
    WARNING = "warning"
    BLOCKER = "blocker"


class SeverityPolicy(BaseModel):
    """Policy mapping classified changes to a severity level.

    ``on_breaking`` is the severity assigned when any breaking change is
    present; ``on_non_breaking`` applies when only non-breaking changes exist.
    ``per_rule`` lets a contract override the severity for a specific change
    field (matched by prefix), e.g. ``{"sla.freshness": "warning"}``.
    """

    on_breaking: SeverityLevel = SeverityLevel.BLOCKER
    on_non_breaking: SeverityLevel = SeverityLevel.INFO
    per_rule: dict[str, SeverityLevel] = Field(default_factory=dict)


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
    severity_policy: SeverityPolicy = Field(default_factory=SeverityPolicy)


class ContractViolation(BaseModel):
    """A single contract violation."""

    rule: str
    message: str
    severity: str = "warning"  # warning, critical, quarantine


class SchemaDrift(BaseModel):
    """Schema drift detected between contract and actual table."""

    column: str
    change_type: str  # added, removed, type_changed
    expected: str = ""
    actual: str = ""


class ContractResult(BaseModel):
    """Result of contract validation."""

    contract_name: str
    status: str = "pass"  # pass, fail, warn, quarantine
    violations: list[ContractViolation] = Field(default_factory=list)
    schema_drift: list[SchemaDrift] = Field(default_factory=list)


class ContractChange(BaseModel):
    """A single change between two contract versions."""

    field: str
    change_type: str  # added, removed, changed
    old_value: str = ""
    new_value: str = ""
    is_breaking: bool = False


class VersionedDiff(BaseModel):
    """Structured, version-aware diff between two contract revisions.

    Wraps the raw :class:`ContractChange` list with the old/new contract
    version strings, an aggregate :class:`ChangeLevel`, and the severity
    verdict produced by applying a :class:`SeverityPolicy`.

    ``version_bumped`` reports whether the major version was increased; when a
    breaking change is present without a major bump, ``version_warning`` flags
    it so the diff can recommend a version bump.
    """

    old_version: str = "1.0"
    new_version: str = "1.0"
    changes: list[ContractChange] = Field(default_factory=list)
    change_level: ChangeLevel = ChangeLevel.NONE
    severity: SeverityLevel = SeverityLevel.INFO
    version_bumped: bool = False
    version_warning: bool = False

    @property
    def is_breaking(self) -> bool:
        """Whether this diff contains at least one breaking change."""
        return self.change_level == ChangeLevel.BREAKING

    @property
    def is_blocker(self) -> bool:
        """Whether the applied severity policy escalates this diff to a blocker."""
        return self.severity == SeverityLevel.BLOCKER
