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

"""Tests for data contracts: validation, diff, and compiler parsing."""

from __future__ import annotations

import textwrap

from provero.contracts.diff import (
    apply_severity_policy,
    classify_changes,
    diff_contracts,
    versioned_diff,
)
from provero.contracts.models import (
    ChangeLevel,
    ColumnContract,
    ContractConfig,
    SchemaContract,
    SeverityLevel,
    SeverityPolicy,
    SLAConfig,
    ViolationAction,
)
from provero.contracts.validator import validate_contract


class TestSchemaValidation:
    def test_schema_pass(self, duckdb_orders):
        contract = ContractConfig(
            name="orders_contract",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="order_id", type="integer"),
                    ColumnContract(name="customer_id", type="varchar"),
                    ColumnContract(name="amount", type="decimal"),
                    ColumnContract(name="status", type="varchar"),
                ]
            ),
        )
        result = validate_contract(contract, duckdb_orders)
        assert result.status == "pass"
        assert len(result.violations) == 0

    def test_missing_column(self, duckdb_orders):
        contract = ContractConfig(
            name="orders_contract",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="order_id", type="integer"),
                    ColumnContract(name="nonexistent_col", type="varchar"),
                ]
            ),
        )
        result = validate_contract(contract, duckdb_orders)
        assert any(d.change_type == "removed" for d in result.schema_drift)
        assert any("missing" in v.message.lower() for v in result.violations)

    def test_extra_column(self, duckdb_orders):
        contract = ContractConfig(
            name="orders_contract",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="order_id", type="integer"),
                ]
            ),
        )
        result = validate_contract(contract, duckdb_orders)
        # Extra columns in table should show as "added" drift
        added = [d for d in result.schema_drift if d.change_type == "added"]
        assert len(added) > 0

    def test_type_mismatch(self, duckdb_orders):
        contract = ContractConfig(
            name="orders_contract",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="order_id", type="varchar"),  # actual is integer
                ]
            ),
        )
        result = validate_contract(contract, duckdb_orders)
        assert any(d.change_type == "type_changed" for d in result.schema_drift)

    def test_column_checks(self, duckdb_orders):
        contract = ContractConfig(
            name="orders_contract",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="order_id", type="integer", checks=["not_null", "unique"]),
                ]
            ),
        )
        result = validate_contract(contract, duckdb_orders)
        # order_id has no nulls and is unique, so should pass
        assert result.status == "pass"

    def test_parametrized_column_checks(self, duckdb_orders):
        """Column checks with params like range and accepted_values."""
        contract = ContractConfig(
            name="orders_contract",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(
                        name="status",
                        type="varchar",
                        checks=[
                            {
                                "accepted_values": {
                                    "values": ["delivered", "shipped", "pending", "cancelled"],
                                }
                            },
                        ],
                    ),
                    ColumnContract(
                        name="amount",
                        type="decimal",
                        checks=[
                            {"range": {"min": 0, "max": 1000000}},
                        ],
                    ),
                ]
            ),
        )
        result = validate_contract(contract, duckdb_orders)
        assert result.status == "pass"

    def test_parametrized_check_fails(self, duckdb_orders):
        """Parametrized check that should fail."""
        contract = ContractConfig(
            name="orders_contract",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(
                        name="status",
                        type="varchar",
                        checks=[
                            {
                                "accepted_values": {
                                    "values": ["delivered", "shipped"],
                                }
                            },  # missing pending, cancelled
                        ],
                    ),
                ]
            ),
        )
        result = validate_contract(contract, duckdb_orders)
        assert any("accepted_values" in v.rule for v in result.violations)


class TestSLAValidation:
    def test_availability_pass(self, duckdb_orders):
        contract = ContractConfig(
            name="orders_contract",
            table="orders",
            sla=SLAConfig(availability="true"),
        )
        result = validate_contract(contract, duckdb_orders)
        assert not any(v.rule == "sla.availability" for v in result.violations)

    def test_freshness_with_timestamp(self, duckdb_orders):
        contract = ContractConfig(
            name="events_contract",
            table="events",
            sla=SLAConfig(freshness="24h"),
        )
        result = validate_contract(contract, duckdb_orders)
        # Events have recent timestamps, should pass
        assert not any(v.rule == "sla.freshness" for v in result.violations)

    def test_completeness_pass(self, duckdb_orders):
        contract = ContractConfig(
            name="orders_contract",
            table="orders",
            sla=SLAConfig(completeness="90%"),
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="order_id"),
                    ColumnContract(name="status"),
                ]
            ),
        )
        result = validate_contract(contract, duckdb_orders)
        assert not any(v.rule == "sla.completeness" for v in result.violations)


class TestOnViolation:
    def test_block_makes_critical(self, duckdb_orders):
        contract = ContractConfig(
            name="orders_contract",
            table="orders",
            on_violation=ViolationAction.BLOCK,
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="nonexistent", type="varchar"),
                ]
            ),
        )
        result = validate_contract(contract, duckdb_orders)
        assert result.status == "fail"
        assert any(v.severity == "critical" for v in result.violations)

    def test_warn_makes_warning(self, duckdb_orders):
        contract = ContractConfig(
            name="orders_contract",
            table="orders",
            on_violation=ViolationAction.WARN,
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="nonexistent", type="varchar"),
                ]
            ),
        )
        result = validate_contract(contract, duckdb_orders)
        assert result.status == "warn"
        assert any(v.severity == "warning" for v in result.violations)

    def test_no_table(self):
        """Contract with no table should fail immediately."""
        contract = ContractConfig(name="bad_contract", table="")
        result = validate_contract(contract, None)  # type: ignore[arg-type]
        assert result.status == "fail"


class TestSLAParsingErrors:
    """A4/A5: malformed SLA strings must yield a violation, not crash."""

    def test_invalid_freshness_sla_returns_violation(self, duckdb_orders):
        """A4: bad freshness format yields a violation instead of ValueError."""
        contract = ContractConfig(
            name="bad_freshness",
            table="events",
            sla=SLAConfig(freshness="h24"),  # invalid: no leading digits
        )
        # Must not raise; must surface a freshness violation.
        result = validate_contract(contract, duckdb_orders)
        fresh_violations = [v for v in result.violations if v.rule == "sla.freshness"]
        assert len(fresh_violations) == 1
        assert "h24" in fresh_violations[0].message

    def test_invalid_freshness_sla_numeric_only(self, duckdb_orders):
        """A4: a bare number ('24', no unit) must not crash."""
        contract = ContractConfig(
            name="bad_freshness2",
            table="events",
            sla=SLAConfig(freshness="24"),
        )
        result = validate_contract(contract, duckdb_orders)
        assert any(v.rule == "sla.freshness" for v in result.violations)

    def test_invalid_completeness_sla_returns_violation(self, duckdb_orders):
        """A5: non-numeric completeness yields a violation instead of ValueError."""
        contract = ContractConfig(
            name="bad_completeness",
            table="orders",
            sla=SLAConfig(completeness="invalid"),
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="order_id"),
                    ColumnContract(name="status"),
                ]
            ),
        )
        result = validate_contract(contract, duckdb_orders)
        comp_violations = [v for v in result.violations if v.rule == "sla.completeness"]
        assert len(comp_violations) == 1
        assert "invalid" in comp_violations[0].message

    def test_invalid_completeness_sla_empty_percent(self, duckdb_orders):
        """A5: a lone '%' must not crash."""
        contract = ContractConfig(
            name="bad_completeness2",
            table="orders",
            sla=SLAConfig(completeness="%"),
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="order_id"),
                    ColumnContract(name="status"),
                ]
            ),
        )
        result = validate_contract(contract, duckdb_orders)
        assert any(v.rule == "sla.completeness" for v in result.violations)


class TestQuarantineSemantics:
    """A12: QUARANTINE is non-blocking and distinct from BLOCK and WARN."""

    def _violating_contract(self, action):
        return ContractConfig(
            name="q_contract",
            table="orders",
            on_violation=action,
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="nonexistent", type="varchar"),
                ]
            ),
        )

    def test_quarantine_status_is_distinct_and_non_blocking(self, duckdb_orders):
        """QUARANTINE status must be 'quarantine', not 'fail' (BLOCK) nor 'warn'."""
        contract = self._violating_contract(ViolationAction.QUARANTINE)
        result = validate_contract(contract, duckdb_orders)
        assert result.status == "quarantine"
        assert result.status != "fail"
        assert result.status != "warn"
        assert len(result.violations) > 0

    def test_quarantine_severity_is_distinct(self, duckdb_orders):
        """QUARANTINE violations carry severity 'quarantine', not 'critical'/'warning'."""
        contract = self._violating_contract(ViolationAction.QUARANTINE)
        result = validate_contract(contract, duckdb_orders)
        severities = {v.severity for v in result.violations}
        assert severities == {"quarantine"}
        # No critical => not treated as a blocking failure.
        assert not any(v.severity == "critical" for v in result.violations)

    def test_quarantine_differs_from_block(self, duckdb_orders):
        """Same violation under BLOCK must fail; under QUARANTINE must not fail."""
        block_result = validate_contract(
            self._violating_contract(ViolationAction.BLOCK), duckdb_orders
        )
        quarantine_result = validate_contract(
            self._violating_contract(ViolationAction.QUARANTINE), duckdb_orders
        )
        assert block_result.status == "fail"
        assert quarantine_result.status == "quarantine"

    def test_quarantine_passes_when_no_violations(self, duckdb_orders):
        """QUARANTINE with a satisfied contract still reports 'pass'."""
        contract = ContractConfig(
            name="q_ok",
            table="orders",
            on_violation=ViolationAction.QUARANTINE,
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="order_id", type="integer"),
                ]
            ),
        )
        result = validate_contract(contract, duckdb_orders)
        assert result.status == "pass"


class TestContractDiff:
    def test_no_changes(self):
        contract = ContractConfig(
            name="test",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="id", type="integer"),
                ]
            ),
        )
        changes = diff_contracts(contract, contract)
        assert len(changes) == 0

    def test_column_added(self):
        old = ContractConfig(
            name="test",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="id", type="integer"),
                ]
            ),
        )
        new = ContractConfig(
            name="test",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="id", type="integer"),
                    ColumnContract(name="name", type="varchar"),
                ]
            ),
        )
        changes = diff_contracts(old, new)
        added = [c for c in changes if c.change_type == "added" and "name" in c.field]
        assert len(added) == 1
        assert not added[0].is_breaking

    def test_column_removed_is_breaking(self):
        old = ContractConfig(
            name="test",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="id", type="integer"),
                    ColumnContract(name="name", type="varchar"),
                ]
            ),
        )
        new = ContractConfig(
            name="test",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="id", type="integer"),
                ]
            ),
        )
        changes = diff_contracts(old, new)
        removed = [c for c in changes if c.change_type == "removed"]
        assert len(removed) == 1
        assert removed[0].is_breaking

    def test_type_change_is_breaking(self):
        old = ContractConfig(
            name="test",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="id", type="integer"),
                ]
            ),
        )
        new = ContractConfig(
            name="test",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="id", type="varchar"),
                ]
            ),
        )
        changes = diff_contracts(old, new)
        type_changes = [c for c in changes if "type" in c.field]
        assert len(type_changes) == 1
        assert type_changes[0].is_breaking

    def test_sla_change(self):
        old = ContractConfig(name="test", table="orders", sla=SLAConfig(freshness="24h"))
        new = ContractConfig(name="test", table="orders", sla=SLAConfig(freshness="1h"))
        changes = diff_contracts(old, new)
        sla_changes = [c for c in changes if "freshness" in c.field]
        assert len(sla_changes) == 1

    def test_on_violation_change_to_block_is_breaking(self):
        old = ContractConfig(name="test", table="orders", on_violation=ViolationAction.WARN)
        new = ContractConfig(name="test", table="orders", on_violation=ViolationAction.BLOCK)
        changes = diff_contracts(old, new)
        violation_changes = [c for c in changes if c.field == "on_violation"]
        assert len(violation_changes) == 1
        assert violation_changes[0].is_breaking is True

    def test_on_violation_change_to_warn_not_breaking(self):
        old = ContractConfig(name="test", table="orders", on_violation=ViolationAction.BLOCK)
        new = ContractConfig(name="test", table="orders", on_violation=ViolationAction.WARN)
        changes = diff_contracts(old, new)
        violation_changes = [c for c in changes if c.field == "on_violation"]
        assert len(violation_changes) == 1
        assert violation_changes[0].is_breaking is False


def _add_col(name: str, type_: str = "varchar") -> ColumnContract:
    return ColumnContract(name=name, type=type_)


class TestVersioningClassification:
    """Breaking vs non-breaking classification on concrete contract pairs."""

    def _base(self, cols: list[ColumnContract], version: str = "1.0") -> ContractConfig:
        return ContractConfig(
            name="c",
            table="orders",
            version=version,
            schema_def=SchemaContract(columns=cols),
        )

    def test_identical_contracts_classify_as_none(self):
        contract = self._base([_add_col("id", "integer")])
        assert classify_changes(diff_contracts(contract, contract)) == ChangeLevel.NONE

    def test_adding_optional_column_is_non_breaking(self):
        old = self._base([_add_col("id", "integer")])
        new = self._base([_add_col("id", "integer"), _add_col("name", "varchar")])
        assert classify_changes(diff_contracts(old, new)) == ChangeLevel.NON_BREAKING

    def test_removing_column_is_breaking(self):
        old = self._base([_add_col("id", "integer"), _add_col("name", "varchar")])
        new = self._base([_add_col("id", "integer")])
        assert classify_changes(diff_contracts(old, new)) == ChangeLevel.BREAKING

    def test_tightening_type_is_breaking(self):
        old = self._base([_add_col("id", "varchar")])
        new = self._base([_add_col("id", "integer")])
        assert classify_changes(diff_contracts(old, new)) == ChangeLevel.BREAKING


class TestSeverityPolicy:
    """Severity policy must escalate a breaking change to blocker."""

    def _breaking_pair(self) -> tuple[ContractConfig, ContractConfig]:
        old = ContractConfig(
            name="c",
            table="orders",
            schema_def=SchemaContract(
                columns=[_add_col("id", "integer"), _add_col("name", "varchar")]
            ),
        )
        new = ContractConfig(
            name="c",
            table="orders",
            schema_def=SchemaContract(columns=[_add_col("id", "integer")]),
        )
        return old, new

    def test_breaking_change_escalates_to_blocker(self):
        old, new = self._breaking_pair()
        changes = diff_contracts(old, new)
        assert apply_severity_policy(changes, SeverityPolicy()) == SeverityLevel.BLOCKER

    def test_non_breaking_default_is_not_blocker(self):
        old = ContractConfig(
            name="c",
            table="orders",
            schema_def=SchemaContract(columns=[_add_col("id", "integer")]),
        )
        new = ContractConfig(
            name="c",
            table="orders",
            schema_def=SchemaContract(
                columns=[_add_col("id", "integer"), _add_col("extra", "varchar")]
            ),
        )
        changes = diff_contracts(old, new)
        assert apply_severity_policy(changes, SeverityPolicy()) == SeverityLevel.INFO

    def test_custom_policy_downgrades_breaking(self):
        old, new = self._breaking_pair()
        changes = diff_contracts(old, new)
        policy = SeverityPolicy(on_breaking=SeverityLevel.WARNING)
        assert apply_severity_policy(changes, policy) == SeverityLevel.WARNING

    def test_per_rule_override_escalates_specific_field(self):
        old = ContractConfig(name="c", table="orders", sla=SLAConfig(freshness="24h"))
        new = ContractConfig(name="c", table="orders", sla=SLAConfig(freshness="1h"))
        changes = diff_contracts(old, new)
        # Base on_breaking is WARNING; the per-rule override must beat it.
        policy = SeverityPolicy(
            on_breaking=SeverityLevel.WARNING,
            per_rule={"sla.freshness": SeverityLevel.BLOCKER},
        )
        assert apply_severity_policy(changes, policy) == SeverityLevel.BLOCKER

    def test_contract_severity_policy_field_defaults(self):
        contract = ContractConfig(name="c", table="orders")
        assert contract.severity_policy.on_breaking == SeverityLevel.BLOCKER


class TestVersionedDiff:
    """Versioned diff output bundles version strings, level, and verdict."""

    def test_versioned_diff_reports_versions_and_blocker(self):
        old = ContractConfig(
            name="c",
            table="orders",
            version="1.0",
            schema_def=SchemaContract(
                columns=[_add_col("id", "integer"), _add_col("name", "varchar")]
            ),
        )
        new = ContractConfig(
            name="c",
            table="orders",
            version="1.1",
            schema_def=SchemaContract(columns=[_add_col("id", "integer")]),
        )
        vd = versioned_diff(old, new)
        assert vd.old_version == "1.0"
        assert vd.new_version == "1.1"
        assert vd.change_level == ChangeLevel.BREAKING
        assert vd.is_breaking is True
        assert vd.severity == SeverityLevel.BLOCKER
        assert vd.is_blocker is True
        assert len(vd.changes) >= 1

    def test_versioned_diff_warns_breaking_without_major_bump(self):
        old = ContractConfig(
            name="c",
            table="orders",
            version="1.0",
            schema_def=SchemaContract(
                columns=[_add_col("id", "integer"), _add_col("name", "varchar")]
            ),
        )
        new = ContractConfig(
            name="c",
            table="orders",
            version="1.1",
            schema_def=SchemaContract(columns=[_add_col("id", "integer")]),
        )
        vd = versioned_diff(old, new)
        assert vd.version_bumped is False
        assert vd.version_warning is True

    def test_versioned_diff_major_bump_clears_warning(self):
        old = ContractConfig(
            name="c",
            table="orders",
            version="1.0",
            schema_def=SchemaContract(
                columns=[_add_col("id", "integer"), _add_col("name", "varchar")]
            ),
        )
        new = ContractConfig(
            name="c",
            table="orders",
            version="2.0",
            schema_def=SchemaContract(columns=[_add_col("id", "integer")]),
        )
        vd = versioned_diff(old, new)
        assert vd.version_bumped is True
        assert vd.version_warning is False

    def test_versioned_diff_uses_contract_policy(self):
        old = ContractConfig(
            name="c",
            table="orders",
            schema_def=SchemaContract(
                columns=[_add_col("id", "integer"), _add_col("name", "varchar")]
            ),
        )
        new = ContractConfig(
            name="c",
            table="orders",
            severity_policy=SeverityPolicy(on_breaking=SeverityLevel.WARNING),
            schema_def=SchemaContract(columns=[_add_col("id", "integer")]),
        )
        vd = versioned_diff(old, new)
        assert vd.change_level == ChangeLevel.BREAKING
        assert vd.severity == SeverityLevel.WARNING
        assert vd.is_blocker is False

    def test_versioned_diff_no_changes(self):
        contract = ContractConfig(
            name="c",
            table="orders",
            schema_def=SchemaContract(columns=[_add_col("id", "integer")]),
        )
        vd = versioned_diff(contract, contract)
        assert vd.change_level == ChangeLevel.NONE
        assert vd.is_breaking is False
        assert vd.severity == SeverityLevel.INFO
        assert vd.version_warning is False


class TestCompilerParsing:
    def test_simple_format_with_contracts(self, tmp_path):
        """Contracts should be parsed even in simple format (source + checks at top)."""
        config_path = tmp_path / "provero.yaml"
        config_path.write_text(
            textwrap.dedent("""\
            source:
              type: duckdb
              table: orders

            checks:
              - not_null: order_id

            contracts:
              - name: orders_contract
                table: orders
                schema:
                  columns:
                    - name: order_id
                      type: integer
        """)
        )

        from provero.core.compiler import compile_file

        config = compile_file(config_path)
        assert len(config.contracts) == 1
        assert config.contracts[0].name == "orders_contract"

    def test_parse_contracts_from_yaml(self, tmp_path):
        config_path = tmp_path / "provero.yaml"
        config_path.write_text(
            textwrap.dedent("""\
            version: "1.0"
            suites:
              - name: orders_checks
                source:
                  type: duckdb
                  table: orders
                checks:
                  - not_null: order_id

            contracts:
              - name: orders_contract
                owner: data-team
                table: orders
                on_violation: warn
                schema:
                  columns:
                    - name: order_id
                      type: integer
                      checks: [not_null, unique]
                    - name: status
                      type: varchar
                sla:
                  freshness: 24h
                  completeness: "95%"
        """)
        )

        from provero.core.compiler import compile_file

        config = compile_file(config_path)
        assert len(config.contracts) == 1
        contract = config.contracts[0]
        assert contract.name == "orders_contract"
        assert contract.owner == "data-team"
        assert contract.on_violation == ViolationAction.WARN
        assert len(contract.schema_def.columns) == 2
        assert contract.sla.freshness == "24h"
        assert contract.sla.completeness == "95%"


class TestContractLiveDB:
    def test_full_validation_with_duckdb(self, duckdb_orders):
        """Full contract validation against live DuckDB."""
        contract = ContractConfig(
            name="orders_full",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="order_id", type="integer"),
                    ColumnContract(name="customer_id", type="varchar"),
                    ColumnContract(name="amount", type="decimal"),
                    ColumnContract(name="status", type="varchar"),
                ]
            ),
        )
        result = validate_contract(contract, duckdb_orders)
        assert result.status == "pass"

    def test_schema_drift_extra_column(self, duckdb_orders):
        """ALTER TABLE adds a column, contract should detect drift."""
        duckdb_orders._conn.execute("ALTER TABLE orders ADD COLUMN region VARCHAR")
        contract = ContractConfig(
            name="orders_drift",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(name="order_id", type="integer"),
                    ColumnContract(name="customer_id", type="varchar"),
                    ColumnContract(name="amount", type="decimal"),
                    ColumnContract(name="status", type="varchar"),
                ]
            ),
        )
        result = validate_contract(contract, duckdb_orders)
        added = [d for d in result.schema_drift if d.change_type == "added"]
        assert any("region" in d.column for d in added)

    def test_sla_freshness_live(self, duckdb_orders):
        """Events with recent timestamps should pass freshness SLA."""
        contract = ContractConfig(
            name="events_fresh",
            table="events",
            sla=SLAConfig(freshness="24h"),
        )
        result = validate_contract(contract, duckdb_orders)
        assert not any(v.rule == "sla.freshness" for v in result.violations)

    def test_diff_with_dict_checks(self):
        """Contract diff handles parametrized (dict) checks in columns."""
        old = ContractConfig(
            name="test",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(
                        name="status",
                        type="varchar",
                        checks=[{"accepted_values": {"values": ["a", "b"]}}],
                    ),
                ]
            ),
        )
        new = ContractConfig(
            name="test",
            table="orders",
            schema_def=SchemaContract(
                columns=[
                    ColumnContract(
                        name="status",
                        type="varchar",
                        checks=[{"accepted_values": {"values": ["a", "b", "c"]}}],
                    ),
                ]
            ),
        )
        changes = diff_contracts(old, new)
        # Should detect the check change without crashing
        assert isinstance(changes, list)

    def test_multiple_contracts_parsed(self, tmp_path):
        """YAML with 2 contracts, compile_file parses both."""
        config_path = tmp_path / "provero.yaml"
        config_path.write_text(
            textwrap.dedent("""\
            source:
              type: duckdb
              table: orders

            checks:
              - not_null: order_id

            contracts:
              - name: contract_one
                table: orders
                schema:
                  columns:
                    - name: order_id
                      type: integer
              - name: contract_two
                table: events
                schema:
                  columns:
                    - name: event_id
                      type: integer
        """)
        )

        from provero.core.compiler import compile_file

        config = compile_file(config_path)
        assert len(config.contracts) == 2
        names = {c.name for c in config.contracts}
        assert names == {"contract_one", "contract_two"}
