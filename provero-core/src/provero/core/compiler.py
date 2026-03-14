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

"""AQL compiler: parses provero.yaml into execution plans."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from provero.alerts.models import AlertConfig
from provero.contracts.models import ContractConfig


class SourceConfig(BaseModel):
    """Data source configuration."""

    type: str
    connection: str = ""
    table: str = ""
    conn_id: str = ""  # Airflow connection ID


class CheckConfig(BaseModel):
    """Single check configuration."""

    check_type: str
    column: str | None = None
    columns: list[str] = Field(default_factory=list)
    params: dict[str, Any] = Field(default_factory=dict)
    severity: str | None = None


class SuiteConfig(BaseModel):
    """Suite configuration parsed from AQL."""

    name: str
    source: SourceConfig
    checks: list[CheckConfig] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    schedule: str | None = None


class ProveroConfig(BaseModel):
    """Top-level Provero configuration."""

    version: str = "1.0"
    sources: dict[str, SourceConfig] = Field(default_factory=dict)
    suites: list[SuiteConfig] = Field(default_factory=list)
    contracts: list[ContractConfig] = Field(default_factory=list)
    alerts: list[AlertConfig] = Field(default_factory=list)


def parse_check(raw: dict[str, Any] | str) -> CheckConfig:
    """Parse a single check from YAML into CheckConfig."""
    if isinstance(raw, str):
        # Shorthand: "not_null: column_name"
        parts = raw.split(":", 1)
        return CheckConfig(
            check_type=parts[0].strip(),
            column=parts[1].strip() if len(parts) > 1 else None,
        )

    for check_type, value in raw.items():
        if isinstance(value, list):
            return CheckConfig(check_type=check_type, columns=value)
        if isinstance(value, str):
            if check_type == "custom_sql":
                return CheckConfig(check_type=check_type, params={"query": value})
            return CheckConfig(check_type=check_type, column=value)
        if isinstance(value, dict):
            params = {k: v for k, v in value.items() if k not in ("column", "columns", "severity")}
            return CheckConfig(
                check_type=check_type,
                column=value.get("column"),
                columns=value.get("columns", []),
                params=params,
                severity=value.get("severity"),
            )
        return CheckConfig(check_type=check_type, params={"value": value})

    msg = (
        f"Invalid check definition: {raw}. "
        f"Expected one of: "
        f"'not_null: column', "
        f"{{not_null: [col1, col2]}}, "
        f"{{range: {{column: col, min: 0, max: 100}}}}, "
        f"or similar. See https://github.com/provero-org/provero for syntax reference."
    )
    raise ValueError(msg)


def compile_file(path: str | Path) -> ProveroConfig:
    """Compile an provero.yaml file into an ProveroConfig."""
    path = Path(path)
    with path.open() as f:
        raw = yaml.safe_load(f)

    if raw is None:
        msg = f"Empty configuration file: {path}"
        raise ValueError(msg)

    sources: dict[str, SourceConfig] = {}
    if "sources" in raw:
        for name, src in raw["sources"].items():
            sources[name] = SourceConfig(**src)

    # Simple format: source + checks at top level
    if "source" in raw and "checks" in raw:
        source = (
            SourceConfig(**raw["source"])
            if isinstance(raw["source"], dict)
            else sources.get(raw["source"], SourceConfig(type="unknown"))
        )
        checks = [parse_check(c) for c in raw["checks"]]
        suite = SuiteConfig(
            name=path.stem,
            source=source,
            checks=checks,
            tags=raw.get("tags", []),
            schedule=raw.get("schedule"),
        )
        contracts = _parse_contracts(raw.get("contracts", []))
        alerts = _parse_alerts(raw.get("alerts", []))
        return ProveroConfig(
            version=raw.get("version", "1.0"),
            sources=sources,
            suites=[suite],
            contracts=contracts,
            alerts=alerts,
        )

    # Full format: suites list
    suites = []
    for raw_suite in raw.get("suites", []):
        source_ref = raw_suite.get("source", {})
        if isinstance(source_ref, str):
            source = sources.get(source_ref, SourceConfig(type="unknown"))
        else:
            source = SourceConfig(**source_ref)

        if "table" in raw_suite:
            source = source.model_copy(update={"table": raw_suite["table"]})

        checks = [parse_check(c) for c in raw_suite.get("checks", [])]
        suites.append(
            SuiteConfig(
                name=raw_suite["name"],
                source=source,
                checks=checks,
                tags=raw_suite.get("tags", []),
                schedule=raw_suite.get("schedule"),
            )
        )

    contracts = _parse_contracts(raw.get("contracts", []))
    alerts = _parse_alerts(raw.get("alerts", []))

    return ProveroConfig(
        version=raw.get("version", "1.0"),
        sources=sources,
        suites=suites,
        contracts=contracts,
        alerts=alerts,
    )


def _parse_contracts(raw_contracts: list[dict[str, Any]]) -> list[ContractConfig]:
    """Parse contract definitions from YAML into ContractConfig objects."""
    if not raw_contracts:
        return []

    from provero.contracts.models import (
        ColumnContract,
        ContractConfig,
        SchemaContract,
        SLAConfig,
        ViolationAction,
    )

    contracts = []
    for raw in raw_contracts:
        schema_def = SchemaContract()
        if "schema" in raw:
            columns = []
            for col_raw in raw["schema"].get("columns", []):
                columns.append(
                    ColumnContract(
                        name=col_raw["name"],
                        type=col_raw.get("type", ""),
                        checks=col_raw.get("checks", []),
                        description=col_raw.get("description", ""),
                    )
                )
            schema_def = SchemaContract(columns=columns)

        sla = SLAConfig()
        if "sla" in raw:
            sla = SLAConfig(
                freshness=raw["sla"].get("freshness", ""),
                completeness=raw["sla"].get("completeness", ""),
                availability=raw["sla"].get("availability", ""),
            )

        on_violation = ViolationAction.WARN
        if "on_violation" in raw:
            on_violation = ViolationAction(raw["on_violation"])

        contracts.append(
            ContractConfig(
                name=raw["name"],
                owner=raw.get("owner", ""),
                version=raw.get("version", "1.0"),
                source=raw.get("source", ""),
                table=raw.get("table", ""),
                sla=sla,
                schema_def=schema_def,
                on_violation=on_violation,
            )
        )

    return contracts


def _parse_alerts(raw_alerts: list[dict[str, Any]]) -> list[AlertConfig]:
    """Parse alert definitions from YAML into AlertConfig objects."""
    if not raw_alerts:
        return []
    return [
        AlertConfig(
            type=raw.get("type", "webhook"),
            url=raw.get("url", ""),
            trigger=raw.get("trigger", "on_failure"),
            headers=raw.get("headers", {}),
        )
        for raw in raw_alerts
    ]
