# Provero - Development Plan

## Overview

This plan covers the development roadmap from initial prototype to Apache
incubation proposal. Each phase has clear deliverables and success criteria.

---

## Phase 0: Foundation (Weeks 1-2)

**Goal:** Working skeleton that compiles AQL and runs checks against DuckDB.

### Tasks

- [x] Project scaffolding (pyproject.toml, CI, linting, structure)
- [x] AQL compiler: parse provero.yaml into execution plan
- [x] Result models (CheckResult, SuiteResult)
- [x] Check registry (pluggable check system)
- [x] Core checks: not_null, unique, accepted_values, range, row_count, custom_sql
- [x] Completeness check
- [x] DuckDB connector (embedded, for files)
- [x] Check engine: execute suite against connector
- [x] CLI: `provero init`, `provero run`, `provero version`
- [x] Rich table output
- [x] JSON output
- [x] Quickstart example (orders.csv)
- [x] Unit tests for compiler and all checks
- [x] GitHub Actions CI (lint + test + typecheck)

### Success Criteria

- `provero init` creates a template
- `provero run` executes checks against a CSV file and prints results
- All tests pass on Python 3.11, 3.12, 3.13

---

## Phase 1: Core Engine Completion (Weeks 3-6)

**Goal:** Production-quality check engine with SQL connectors and profiling.

### Tasks

- [x] PostgreSQL connector (via SQLAlchemy)
- [x] Generic SQLAlchemy connector (MySQL, SQLite, any SA-supported DB)
- [x] SQL query optimizer (batch multiple checks into single query)
- [x] `provero profile` command (statistical profiling of data sources)
- [x] `provero profile --suggest` (auto-generate checks from data profile)
- [x] Connector factory (auto-creates connector from source config)
- [x] Environment variable resolution in connection strings
- [x] DuckDB: handle read_csv/read_parquet expressions
- [x] Freshness check: fix for DuckDB (EXTRACT EPOCH syntax)
- [x] unique_combination check: end-to-end tests
- [x] regex check: cross-database compatibility
- [x] Result store: SQLite backend (persist results locally)
- [x] Historical results query via CLI: `provero history`
- [x] Sampling support for large tables (in profiler)
- [x] Configurable severity per check in AQL
- [x] `provero validate` command (validate provero.yaml syntax without running)
- [x] JSON Schema for provero.yaml (published as aql-spec/schema.json)
- [x] Error messages: clear, actionable, with suggestions

### Success Criteria

- Run checks against PostgreSQL and DuckDB
- Profile a table and get auto-suggested checks
- Results persisted locally in SQLite
- JSON Schema validates all example provero.yaml files

---

## Phase 2: Anomaly Detection (Weeks 7-10)

**Goal:** Built-in statistical anomaly detection over historical check results.

### Tasks

- [x] Anomaly detector orchestrator
- [x] Z-Score method
- [x] MAD (Median Absolute Deviation) method
- [x] IQR (Interquartile Range) method
- [x] Sensitivity presets (low, medium, high)
- [x] `anomaly` check type in AQL
- [x] `row_count_change` check (compare vs previous run)
- [x] Anomaly results in CLI output (warning indicators)
- [x] Historical metrics storage (time-series in SQLite)
- [ ] Trend visualization in terminal (sparklines via Rich)
- [ ] `provero watch` command (continuous monitoring mode)
- [x] Tests with synthetic time-series data
- [ ] Documentation: anomaly detection guide

### Success Criteria

- ~~Detect row count anomalies based on 30-day history~~ Done
- ~~Z-Score and MAD methods produce correct results on known distributions~~ Done
- `provero watch` runs checks on interval and alerts on anomalies

---

## Phase 3: Data Contracts (Weeks 11-14)

**Goal:** Contract-first workflow where producers declare and consumers verify.

### Tasks

- [x] Contract model (schema + SLAs + checks + ownership + version)
- [x] Contract section in AQL (contracts: key in provero.yaml)
- [x] Contract validation against data
- [ ] Contract versioning (semver)
- [x] `provero contract validate` command
- [x] `provero contract diff v1 v2` command
- [ ] `provero contract breaking-changes` command
- [ ] Contract registry (local file-based, git-friendly)
- [x] On-violation actions: block, warn, quarantine
- [x] Contract compliance report (HTML)
- [ ] Example: producer/consumer workflow
- [ ] Documentation: data contracts guide

### Success Criteria

- ~~Define a contract, run validation, see pass/fail~~ Done
- ~~Detect breaking changes between contract versions~~ Done
- ~~Generate compliance report~~ Done

---

## Phase 4: Airflow Provider (Weeks 15-18)

**Goal:** First-class Airflow integration as a provider package.

### Tasks

- [x] provero-airflow package setup
- [x] ProveroCheckOperator (run checks as Airflow task)
- [ ] ProveroSensor (wait for data quality to pass)
- [ ] ProveroHook (API client for Provero server)
- [x] @provero_check decorator for @task functions
- [ ] DAG auto-generation from provero.yaml files
- [ ] Airflow connection type for Provero
- [ ] provider.yaml metadata
- [ ] Integration tests with Airflow 2.9+ and 3.0
- [ ] Example DAGs
- [ ] Documentation: Airflow integration guide
- [ ] Publish to PyPI as provero-airflow

### Success Criteria

- ~~Install provider, use operator in DAG, see results in Airflow logs~~ Done
- DAG auto-generation works from provero.yaml directory
- Tests pass with Airflow 2.9 and 3.0

---

## Phase 5: Cloud Connectors & Reporting (Weeks 19-22)

**Goal:** Support major cloud data warehouses and rich reporting.

### Tasks

- [x] Snowflake connector (via SQLAlchemy generic)
- [x] BigQuery connector (via SQLAlchemy generic)
- [x] Redshift connector (via SQLAlchemy generic)
- [x] Databricks connector (via SQLAlchemy)
- [ ] Spark DataFrame connector
- [x] Polars DataFrame connector
- [x] HTML report generator (Jinja2 templates)
- [x] Report: suite summary, check details, trends, failing rows
- [x] `provero run --report html` command
- [ ] Report publishing to S3/GCS
- [ ] OpenLineage event export
- [ ] Export to Great Expectations format
- [ ] Export to SodaCL format
- [ ] Documentation: connector guide for each platform

### Success Criteria

- ~~Run checks against Snowflake, BigQuery, Redshift~~ Done (via SQLAlchemy)
- ~~Generate HTML report viewable in browser~~ Done
- Export rules to GX and Soda formats

---

## Phase 6: Alerts & Actions (Weeks 23-26)

**Goal:** Automated responses to quality failures.

### Tasks

- [x] Alert system: dispatch alerts based on check results
- [x] Slack integration (via generic webhook)
- [x] PagerDuty integration (via generic webhook)
- [ ] Email integration (SMTP)
- [x] Generic webhook integration
- [ ] Block downstream action (fail pipeline)
- [ ] Quarantine action (move bad rows to quarantine table)
- [ ] Trigger action (trigger external pipeline on failure/drift)
- [ ] `on_failure` section in AQL
- [ ] Alert templating (Jinja2, with check result context)
- [ ] Alert deduplication (don't spam same alert)
- [ ] Documentation: alerts and actions guide

### Success Criteria

- Check fails, Slack notification sent with details
- Quarantine action moves failing rows to separate table
- Alert deduplication prevents repeated notifications

---

## Phase 7: Server Mode & API (Weeks 27-30)

**Goal:** REST API for centralized quality management.

### Tasks

- [ ] FastAPI server (`provero server`)
- [ ] API routes: /checks, /contracts, /reports, /health
- [ ] API authentication (API keys)
- [ ] PostgreSQL result store (server mode)
- [ ] Scheduled execution (built-in scheduler)
- [ ] WebSocket for real-time check progress
- [ ] API documentation (auto-generated OpenAPI spec)
- [ ] Docker image
- [ ] docker-compose for quick start (Provero + PostgreSQL)
- [ ] Helm chart (Kubernetes deployment)
- [ ] Documentation: server mode guide

### Success Criteria

- `provero server` starts, API responds, checks can be triggered via API
- Docker image works out of the box
- OpenAPI spec is complete and accurate

---

## Phase 8: Streaming (Weeks 31-34)

**Goal:** Real-time data quality checks on streaming sources.

### Tasks

- [ ] Streaming engine (windowed check execution)
- [ ] Kafka connector
- [ ] Kinesis connector (optional, based on demand)
- [ ] Window types: tumbling, sliding
- [ ] Streaming check types: schema, throughput, latency, null_rate
- [ ] Streaming anomaly detection (on windowed metrics)
- [ ] `provero watch --stream` command
- [ ] Streaming section in AQL
- [ ] Backpressure handling
- [ ] Documentation: streaming guide
- [ ] Integration tests with Kafka (testcontainers)

### Success Criteria

- Consume Kafka topic, validate messages, detect throughput anomalies
- Windowed metrics stored and queryable

---

## Phase 9: Flyte Plugin & Cross-Orchestrator (Weeks 35-36)

**Goal:** Demonstrate orchestrator-agnostic positioning.

### Tasks

- [ ] provero-flyte package
- [ ] @provero_check decorator for Flyte tasks
- [ ] Flyte type transformer for ProveroResult
- [ ] Example Flyte workflow with quality gates
- [ ] Documentation: Flyte integration guide
- [ ] Blog post: "Using Provero with Flyte"

### Success Criteria

- Provero works as Flyte plugin
- Same provero.yaml used with both Airflow and Flyte

---

## Phase 10: AQL Spec v1.0 & Community (Weeks 37-40)

**Goal:** Formal AQL specification and community building for incubation.

### Tasks

- [ ] AQL Specification v1.0 document
- [ ] JSON Schema v1.0 for provero.yaml
- [ ] AQL compatibility test suite (for other tools to validate their AQL support)
- [ ] Migration guides: from Great Expectations, from Soda, from dbt tests
- [ ] Website: provero.dev (landing page, docs, blog)
- [ ] Conference talk proposal (Airflow Summit, Data Council, PyCon)
- [ ] Blog: "Why we built Provero" (story, positioning, differentiation)
- [ ] Blog: "From Griffin to Provero: lessons learned"
- [ ] Outreach to potential committers from other organizations
- [ ] Identify 3+ organizations willing to be listed as adopters
- [ ] Identify ASF Champion (potiuk) and Mentors (jscheffl + others)

### Success Criteria

- AQL spec published and reviewable
- At least 3 organizations using Provero or committed to using it
- Champion and 3 mentors confirmed

---

## Phase 11: Apache Incubation Proposal (Weeks 41-44)

**Goal:** Submit proposal to Apache Incubator.

### Tasks

- [ ] Write incubation proposal (following ASF template)
  - Abstract
  - Background and rationale
  - Initial goals
  - Current status (meritocracy, community, core developers, alignment)
  - Known risks and mitigation
  - External dependencies and licenses
  - Required resources (mailing lists, git, CI)
  - Initial committers (from 3+ organizations)
  - Champion and mentors
- [ ] Submit to general@incubator.apache.org for discussion
- [ ] Address feedback from IPMC
- [ ] Call for vote
- [ ] If accepted: set up ASF infrastructure
  - Mailing lists (dev@, commits@, issues@)
  - JIRA project
  - Confluence wiki
  - Transfer repo to apache/ org
  - Set up ASF CI (GitHub Actions with ASF secrets)
  - KEYS file for release signing

### Success Criteria

- Proposal accepted by IPMC vote (3+ binding +1, no veto)
- Infrastructure provisioned
- First release as Apache podling

---

## Timeline Summary

| Phase | Description | Weeks | Target |
|-------|-------------|-------|--------|
| 0 | Foundation | 1-2 | Done |
| 1 | Core engine | 3-6 | Done |
| 2 | Anomaly detection | 7-10 | Done (partial) |
| 3 | Data contracts | 11-14 | Done (partial) |
| 4 | Airflow provider | 15-18 | Done (partial) |
| 5 | Cloud connectors & reporting | 19-22 | Done (partial) |
| 6 | Alerts & actions | 23-26 | Done (partial) |
| 7 | Server mode & API | 27-30 | Oct 2026 |
| 8 | Streaming | 31-34 | Nov 2026 |
| 9 | Flyte plugin | 35-36 | Dec 2026 |
| 10 | AQL spec & community | 37-40 | Jan 2027 |
| 11 | Incubation proposal | 41-44 | Feb 2027 |

---

## Milestones

### M1: "It works" (end of Phase 1)
- Run quality checks from YAML against real databases
- Usable in personal projects

### M2: "It's smart" (end of Phase 2)
- Anomaly detection without external dependencies
- Differentiated from competitors

### M3: "It integrates" (end of Phase 4)
- Airflow provider published on PyPI
- Usable in production Airflow deployments

### M4: "It scales" (end of Phase 7)
- Server mode, cloud connectors, alerts
- Usable by teams and organizations

### M5: "It's Apache" (end of Phase 11)
- Accepted into Apache Incubator
- Community of contributors from multiple organizations

---

## Anti-Griffin Checkpoints

At each phase boundary, verify:

- [ ] Are there contributors from more than one organization?
- [ ] Was a release published in the last 30 days?
- [ ] Is the mailing list / GitHub Discussions active?
- [ ] Were any new contributors onboarded?
- [ ] Is documentation up to date?

If any answer is "no" for two consecutive phases, stop feature development
and focus on community health.

---

## Key Dependencies

| Dependency | License | Purpose |
|------------|---------|---------|
| pydantic | MIT | Config and result models |
| pyyaml | MIT | YAML parsing |
| sqlalchemy | MIT | Database connectivity |
| duckdb | MIT | Embedded SQL for files |
| rich | MIT | Terminal output |
| typer | MIT | CLI framework |
| scipy | BSD | Statistical tests (anomaly) |
| numpy | BSD | Numerical operations |
| fastapi | MIT | REST API (server mode) |
| prophet | MIT | Time-series anomaly (optional) |

All dependencies are Apache 2.0 compatible.
