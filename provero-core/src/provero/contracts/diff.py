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

"""Contract diff: compare two versions of a data contract."""

from __future__ import annotations

from provero.contracts.models import (
    ChangeLevel,
    ContractChange,
    ContractConfig,
    SeverityLevel,
    SeverityPolicy,
    VersionedDiff,
)


def _major_version(version: str) -> int:
    """Extract the leading integer (major) component of a version string.

    Returns ``0`` when the version has no parseable leading integer so that
    callers never crash on free-form version strings.
    """
    head = version.strip().split(".", 1)[0]
    try:
        return int(head)
    except ValueError:
        return 0


def classify_changes(changes: list[ContractChange]) -> ChangeLevel:
    """Classify a list of contract changes into an aggregate change level.

    Returns :attr:`ChangeLevel.BREAKING` if any change is breaking,
    :attr:`ChangeLevel.NON_BREAKING` if there are only non-breaking changes,
    and :attr:`ChangeLevel.NONE` when the list is empty.
    """
    if not changes:
        return ChangeLevel.NONE
    if any(c.is_breaking for c in changes):
        return ChangeLevel.BREAKING
    return ChangeLevel.NON_BREAKING


def apply_severity_policy(
    changes: list[ContractChange],
    policy: SeverityPolicy | None = None,
) -> SeverityLevel:
    """Apply a severity policy to a set of changes and return the verdict.

    The per-rule overrides take precedence: if any changed field matches a
    ``per_rule`` key (by prefix) the highest matching override wins. Otherwise
    the policy's ``on_breaking`` severity applies when a breaking change is
    present, ``on_non_breaking`` applies for non-breaking changes, and an empty
    change set yields :attr:`SeverityLevel.INFO`.
    """
    policy = policy or SeverityPolicy()
    level = classify_changes(changes)

    rank = {SeverityLevel.INFO: 0, SeverityLevel.WARNING: 1, SeverityLevel.BLOCKER: 2}
    override: SeverityLevel | None = None
    for change in changes:
        for prefix, sev in policy.per_rule.items():
            matches = change.field == prefix or change.field.startswith(f"{prefix}.")
            if matches and (override is None or rank[sev] > rank[override]):
                override = sev

    base: SeverityLevel
    if level == ChangeLevel.BREAKING:
        base = policy.on_breaking
    elif level == ChangeLevel.NON_BREAKING:
        base = policy.on_non_breaking
    else:
        base = SeverityLevel.INFO

    if override is not None and rank[override] > rank[base]:
        return override
    return base


def versioned_diff(
    old: ContractConfig,
    new: ContractConfig,
    policy: SeverityPolicy | None = None,
) -> VersionedDiff:
    """Produce a structured, version-aware diff between two contracts.

    Computes the raw change list via :func:`diff_contracts`, classifies it,
    applies the severity policy (defaulting to ``new.severity_policy``), and
    flags whether a breaking change lacks an accompanying major version bump.
    """
    changes = diff_contracts(old, new)
    level = classify_changes(changes)
    effective_policy = policy if policy is not None else new.severity_policy
    severity = apply_severity_policy(changes, effective_policy)

    version_bumped = _major_version(new.version) > _major_version(old.version)
    version_warning = level == ChangeLevel.BREAKING and not version_bumped

    return VersionedDiff(
        old_version=old.version,
        new_version=new.version,
        changes=changes,
        change_level=level,
        severity=severity,
        version_bumped=version_bumped,
        version_warning=version_warning,
    )


def diff_contracts(old: ContractConfig, new: ContractConfig) -> list[ContractChange]:
    """Compare two contract versions and return the list of changes.

    Identifies added/removed/changed columns, type changes, SLA changes,
    and marks breaking changes.
    """
    changes: list[ContractChange] = []

    # Compare top-level fields
    if old.owner != new.owner:
        changes.append(
            ContractChange(
                field="owner",
                change_type="changed",
                old_value=old.owner,
                new_value=new.owner,
                is_breaking=False,
            )
        )

    if old.table != new.table:
        changes.append(
            ContractChange(
                field="table",
                change_type="changed",
                old_value=old.table,
                new_value=new.table,
                is_breaking=True,
            )
        )

    if old.on_violation != new.on_violation:
        from provero.contracts.models import ViolationAction

        changes.append(
            ContractChange(
                field="on_violation",
                change_type="changed",
                old_value=old.on_violation.value,
                new_value=new.on_violation.value,
                is_breaking=new.on_violation == ViolationAction.BLOCK,
            )
        )

    # Compare schema columns
    old_cols = {c.name: c for c in old.schema_def.columns}
    new_cols = {c.name: c for c in new.schema_def.columns}

    for name in old_cols:
        if name not in new_cols:
            changes.append(
                ContractChange(
                    field=f"schema.columns.{name}",
                    change_type="removed",
                    old_value=old_cols[name].type,
                    is_breaking=True,
                )
            )

    for name in new_cols:
        if name not in old_cols:
            changes.append(
                ContractChange(
                    field=f"schema.columns.{name}",
                    change_type="added",
                    new_value=new_cols[name].type,
                    is_breaking=False,
                )
            )

    for name in old_cols:
        if name in new_cols:
            old_col = old_cols[name]
            new_col = new_cols[name]

            if old_col.type != new_col.type:
                changes.append(
                    ContractChange(
                        field=f"schema.columns.{name}.type",
                        change_type="changed",
                        old_value=old_col.type,
                        new_value=new_col.type,
                        is_breaking=True,
                    )
                )

            old_checks = old_col.checks
            new_checks = new_col.checks

            old_strs = [str(c) for c in old_checks]
            new_strs = [str(c) for c in new_checks]

            for i, check in enumerate(old_checks):
                if old_strs[i] not in new_strs:
                    changes.append(
                        ContractChange(
                            field=f"schema.columns.{name}.checks",
                            change_type="removed",
                            old_value=str(check),
                            is_breaking=False,
                        )
                    )

            for i, check in enumerate(new_checks):
                if new_strs[i] not in old_strs:
                    changes.append(
                        ContractChange(
                            field=f"schema.columns.{name}.checks",
                            change_type="added",
                            new_value=str(check),
                            is_breaking=True,
                        )
                    )

    # Compare SLA
    if old.sla.freshness != new.sla.freshness:
        changes.append(
            ContractChange(
                field="sla.freshness",
                change_type=(
                    "changed"
                    if old.sla.freshness and new.sla.freshness
                    else ("added" if new.sla.freshness else "removed")
                ),
                old_value=old.sla.freshness,
                new_value=new.sla.freshness,
                is_breaking=bool(new.sla.freshness),
            )
        )

    if old.sla.completeness != new.sla.completeness:
        changes.append(
            ContractChange(
                field="sla.completeness",
                change_type=(
                    "changed"
                    if old.sla.completeness and new.sla.completeness
                    else ("added" if new.sla.completeness else "removed")
                ),
                old_value=old.sla.completeness,
                new_value=new.sla.completeness,
                is_breaking=bool(new.sla.completeness),
            )
        )

    if old.sla.availability != new.sla.availability:
        changes.append(
            ContractChange(
                field="sla.availability",
                change_type=(
                    "changed"
                    if old.sla.availability and new.sla.availability
                    else ("added" if new.sla.availability else "removed")
                ),
                old_value=old.sla.availability,
                new_value=new.sla.availability,
                is_breaking=bool(new.sla.availability),
            )
        )

    return changes
