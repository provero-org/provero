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

"""Contract diff: compare two versions of a data contract."""

from __future__ import annotations

from provero.contracts.models import ContractChange, ContractConfig


def diff_contracts(old: ContractConfig, new: ContractConfig) -> list[ContractChange]:
    """Compare two contract versions and return the list of changes.

    Identifies added/removed/changed columns, type changes, SLA changes,
    and marks breaking changes.
    """
    changes: list[ContractChange] = []

    # Compare top-level fields
    if old.owner != new.owner:
        changes.append(ContractChange(
            field="owner",
            change_type="changed",
            old_value=old.owner,
            new_value=new.owner,
            is_breaking=False,
        ))

    if old.table != new.table:
        changes.append(ContractChange(
            field="table",
            change_type="changed",
            old_value=old.table,
            new_value=new.table,
            is_breaking=True,
        ))

    if old.on_violation != new.on_violation:
        from provero.contracts.models import ViolationAction

        changes.append(ContractChange(
            field="on_violation",
            change_type="changed",
            old_value=old.on_violation.value,
            new_value=new.on_violation.value,
            is_breaking=new.on_violation == ViolationAction.BLOCK,
        ))

    # Compare schema columns
    old_cols = {c.name: c for c in old.schema_def.columns}
    new_cols = {c.name: c for c in new.schema_def.columns}

    for name in old_cols:
        if name not in new_cols:
            changes.append(ContractChange(
                field=f"schema.columns.{name}",
                change_type="removed",
                old_value=old_cols[name].type,
                is_breaking=True,
            ))

    for name in new_cols:
        if name not in old_cols:
            changes.append(ContractChange(
                field=f"schema.columns.{name}",
                change_type="added",
                new_value=new_cols[name].type,
                is_breaking=False,
            ))

    for name in old_cols:
        if name in new_cols:
            old_col = old_cols[name]
            new_col = new_cols[name]

            if old_col.type != new_col.type:
                changes.append(ContractChange(
                    field=f"schema.columns.{name}.type",
                    change_type="changed",
                    old_value=old_col.type,
                    new_value=new_col.type,
                    is_breaking=True,
                ))

            old_checks = old_col.checks
            new_checks = new_col.checks

            old_strs = [str(c) for c in old_checks]
            new_strs = [str(c) for c in new_checks]

            for i, check in enumerate(old_checks):
                if old_strs[i] not in new_strs:
                    changes.append(ContractChange(
                        field=f"schema.columns.{name}.checks",
                        change_type="removed",
                        old_value=str(check),
                        is_breaking=False,
                    ))

            for i, check in enumerate(new_checks):
                if new_strs[i] not in old_strs:
                    changes.append(ContractChange(
                        field=f"schema.columns.{name}.checks",
                        change_type="added",
                        new_value=str(check),
                        is_breaking=True,
                    ))

    # Compare SLA
    if old.sla.freshness != new.sla.freshness:
        changes.append(ContractChange(
            field="sla.freshness",
            change_type="changed" if old.sla.freshness and new.sla.freshness else ("added" if new.sla.freshness else "removed"),
            old_value=old.sla.freshness,
            new_value=new.sla.freshness,
            is_breaking=bool(new.sla.freshness),
        ))

    if old.sla.completeness != new.sla.completeness:
        changes.append(ContractChange(
            field="sla.completeness",
            change_type="changed" if old.sla.completeness and new.sla.completeness else ("added" if new.sla.completeness else "removed"),
            old_value=old.sla.completeness,
            new_value=new.sla.completeness,
            is_breaking=bool(new.sla.completeness),
        ))

    if old.sla.availability != new.sla.availability:
        changes.append(ContractChange(
            field="sla.availability",
            change_type="changed" if old.sla.availability and new.sla.availability else ("added" if new.sla.availability else "removed"),
            old_value=old.sla.availability,
            new_value=new.sla.availability,
            is_breaking=bool(new.sla.availability),
        ))

    return changes
