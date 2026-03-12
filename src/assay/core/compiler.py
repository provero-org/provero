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

"""AQL compiler: parses assay.yaml into execution plans."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


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


class AssayConfig(BaseModel):
    """Top-level Assay configuration."""

    version: str = "1.0"
    sources: dict[str, SourceConfig] = Field(default_factory=dict)
    suites: list[SuiteConfig] = Field(default_factory=list)


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
            return CheckConfig(check_type=check_type, column=value)
        if isinstance(value, dict):
            column = value.pop("column", None)
            columns = value.pop("columns", [])
            severity = value.pop("severity", None)
            return CheckConfig(
                check_type=check_type,
                column=column,
                columns=columns,
                params=value,
                severity=severity,
            )
        return CheckConfig(check_type=check_type, params={"value": value})

    msg = (
        f"Invalid check definition: {raw}. "
        f"Expected one of: "
        f"'not_null: column', "
        f"{{not_null: [col1, col2]}}, "
        f"{{range: {{column: col, min: 0, max: 100}}}}, "
        f"or similar. See https://github.com/andreahlert/assay for syntax reference."
    )
    raise ValueError(msg)


def compile_file(path: str | Path) -> AssayConfig:
    """Compile an assay.yaml file into an AssayConfig."""
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
        source = SourceConfig(**raw["source"]) if isinstance(raw["source"], dict) else sources.get(
            raw["source"], SourceConfig(type="unknown")
        )
        checks = [parse_check(c) for c in raw["checks"]]
        suite = SuiteConfig(
            name=path.stem,
            source=source,
            checks=checks,
            tags=raw.get("tags", []),
            schedule=raw.get("schedule"),
        )
        return AssayConfig(version=raw.get("version", "1.0"), sources=sources, suites=[suite])

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

    return AssayConfig(
        version=raw.get("version", "1.0"),
        sources=sources,
        suites=suites,
    )
