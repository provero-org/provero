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

"""Tests for JSON Schema validation of provero.yaml."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from jsonschema import ValidationError, validate


@pytest.fixture
def schema():
    # Try workspace root first, then assay-core local
    schema_path = Path(__file__).parent.parent.parent / "aql-spec" / "schema.json"
    if not schema_path.exists():
        schema_path = Path(__file__).parent.parent / "aql-spec" / "schema.json"
    with schema_path.open() as f:
        return json.load(f)


class TestSchemaSimpleFormat:
    def test_minimal_valid(self, schema):
        config = {
            "source": {"type": "duckdb", "table": "orders"},
            "checks": [{"not_null": "id"}],
        }
        validate(instance=config, schema=schema)

    def test_full_simple_format(self, schema):
        config = {
            "version": "1.0",
            "source": {"type": "postgres", "connection": "postgresql://localhost/db", "table": "orders"},
            "checks": [
                {"not_null": ["id", "name"]},
                {"unique": "id"},
                {"row_count": {"min": 1, "max": 1000}},
                {"range": {"column": "amount", "min": 0, "max": 100000}},
                {"accepted_values": {"column": "status", "values": ["a", "b"]}},
                {"regex": {"column": "email", "pattern": "^.+@.+$"}},
                {"freshness": {"column": "created_at", "max_age": "24h"}},
                {"completeness": {"column": "email", "min": 0.95}},
                {"custom_sql": "SELECT COUNT(*) = 0 FROM orders WHERE amount < 0"},
                {"type": {"column": "id", "expected": "integer"}},
                {"latency": {"source_column": "event_time", "target_column": "loaded_at", "max_latency": "1h"}},
            ],
        }
        validate(instance=config, schema=schema)

    def test_missing_source_fails(self, schema):
        config = {"checks": [{"not_null": "id"}]}
        with pytest.raises(ValidationError):
            validate(instance=config, schema=schema)

    def test_missing_checks_fails(self, schema):
        config = {"source": {"type": "duckdb"}}
        with pytest.raises(ValidationError):
            validate(instance=config, schema=schema)

    def test_invalid_source_type_fails(self, schema):
        config = {
            "source": {"type": "oracle"},
            "checks": [{"not_null": "id"}],
        }
        with pytest.raises(ValidationError):
            validate(instance=config, schema=schema)


class TestSchemaSuiteFormat:
    def test_valid_suite_format(self, schema):
        config = {
            "suites": [
                {
                    "name": "orders_daily",
                    "source": {"type": "duckdb", "table": "orders"},
                    "checks": [{"not_null": "id"}],
                }
            ]
        }
        validate(instance=config, schema=schema)

    def test_suite_with_named_sources(self, schema):
        config = {
            "sources": {
                "warehouse": {"type": "postgres", "connection": "postgresql://localhost/db"},
            },
            "suites": [
                {
                    "name": "orders_daily",
                    "source": "warehouse",
                    "table": "orders",
                    "tags": ["critical"],
                    "checks": [{"unique": "id"}],
                }
            ],
        }
        validate(instance=config, schema=schema)

    def test_suite_missing_name_fails(self, schema):
        config = {
            "suites": [
                {
                    "source": {"type": "duckdb"},
                    "checks": [{"not_null": "id"}],
                }
            ]
        }
        with pytest.raises(ValidationError):
            validate(instance=config, schema=schema)


class TestSchemaSeverity:
    def test_severity_on_dict_check(self, schema):
        config = {
            "source": {"type": "duckdb", "table": "orders"},
            "checks": [
                {"range": {"column": "amount", "min": 0, "severity": "warning"}},
            ],
        }
        validate(instance=config, schema=schema)

    def test_invalid_severity_fails(self, schema):
        config = {
            "source": {"type": "duckdb", "table": "orders"},
            "checks": [
                {"range": {"column": "amount", "min": 0, "severity": "mega_critical"}},
            ],
        }
        with pytest.raises(ValidationError):
            validate(instance=config, schema=schema)


class TestSchemaContracts:
    def test_valid_contract(self, schema):
        config = {
            "suites": [
                {
                    "name": "s",
                    "source": {"type": "duckdb"},
                    "checks": [{"not_null": "id"}],
                }
            ],
            "contracts": [
                {
                    "name": "my_contract",
                    "source": {"type": "postgres", "connection": "postgresql://localhost/db"},
                    "owner": "data-team",
                    "on_violation": "block",
                }
            ],
        }
        validate(instance=config, schema=schema)

    def test_invalid_on_violation_fails(self, schema):
        config = {
            "suites": [
                {
                    "name": "s",
                    "source": {"type": "duckdb"},
                    "checks": [{"not_null": "id"}],
                }
            ],
            "contracts": [
                {
                    "name": "c",
                    "source": {"type": "duckdb"},
                    "on_violation": "explode",
                }
            ],
        }
        with pytest.raises(ValidationError):
            validate(instance=config, schema=schema)


class TestSchemaExampleFile:
    def test_quickstart_example_validates(self, schema):
        example_path = Path(__file__).parent.parent.parent / "examples" / "quickstart" / "provero.yaml"
        if not example_path.exists():
            example_path = Path(__file__).parent.parent / "examples" / "quickstart" / "provero.yaml"
        if not example_path.exists():
            pytest.skip("quickstart example not found")
        with example_path.open() as f:
            config = yaml.safe_load(f)
        validate(instance=config, schema=schema)
