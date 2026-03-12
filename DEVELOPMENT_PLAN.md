# Apache Assay - Development Plan

## Overview

This plan covers the development roadmap from initial prototype to Apache
incubation proposal. Each phase has clear deliverables and success criteria.

---

## Phase 0: Foundation (Weeks 1-2)

**Goal:** Working skeleton that compiles AQL and runs checks against DuckDB.

### Tasks

- [x] Project scaffolding (pyproject.toml, CI, linting, structure)
- [x] AQL compiler: parse assay.yaml into execution plan
- [x] Result models (CheckResult, SuiteResult)
- [x] Check registry (pluggable check system)
- [x] Core checks: not_null, unique, accepted_values, range, row_count, custom_sql
- [x] Completeness check
- [x] DuckDB connector (embedded, for files)
- [x] Check engine: execute suite against connector
- [x] CLI: `assay init`, `assay run`, `assay version`
- [x] Rich table output
- [x] JSON output
- [x] Quickstart example (orders.csv)
- [x] Unit tests for compiler and all checks
- [x] GitHub Actions CI (lint + test + typecheck)

### Success Criteria

- `assay init` creates a template
- `assay run` executes checks against a CSV file and prints results
- All tests pass on Python 3.11, 3.12, 3.13

---

## Phase 1: Core Engine Completion (Weeks 3-6)

**Goal:** Production-quality check engine with SQL connectors and profiling.

### Tasks

- [ ] PostgreSQL connector (via SQLAlchemy)
- [ ] MySQL connector
- [ ] SQL query optimizer (batch multiple checks into single query)
- [ ] `assay profile` command (statistical profiling of data sources)
- [ ] `assay profile --suggest` (auto-generate checks from data profile)
- [ ] Freshness check: fix for DuckDB (EXTRACT EPOCH syntax)
- [ ] unique_combination check: end-to-end tests
- [ ] regex check: cross-database compatibility
- [ ] Result store: SQLite backend (persist results locally)
- [ ] Historical results query via CLI: `assay history`
- [ ] Sampling support for large tables
- [ ] Configurable severity per check in AQL
- [ ] `assay validate` command (validate assay.yaml syntax without running)
- [ ] JSON Schema for assay.yaml (published as aql-spec/schema.json)
- [ ] Error messages: clear, actionable, with suggestions

### Success Criteria

- Run checks against PostgreSQL and DuckDB
- Profile a table and get auto-suggested checks
- Results persisted locally in SQLite
- JSON Schema validates all example assay.yaml files

---

## Phase 2: Anomaly Detection (Weeks 7-10)

**Goal:** Built-in statistical anomaly detection over historical check results.

### Tasks

- [ ] Anomaly detector orchestrator
- [ ] Z-Score method
- [ ] MAD (Median Absolute Deviation) method
- [ ] IQR (Interquartile Range) method
- [ ] Sensitivity presets (low, medium, high)
- [ ] `anomaly` check type in AQL
- [ ] `row_count_change` check (compare vs previous run)
- [ ] Anomaly results in CLI output (warning indicators)
- [ ] Historical metrics storage (time-series in SQLite)
- [ ] Trend visualization in terminal (sparklines via Rich)
- [ ] `assay watch` command (continuous monitoring mode)
- [ ] Tests with synthetic time-series data
- [ ] Documentation: anomaly detection guide

### Success Criteria

- Detect row count anomalies based on 30-day history
- Z-Score and MAD methods produce correct results on known distributions
- `assay watch` runs checks on interval and alerts on anomalies

---

## Phase 3: Data Contracts (Weeks 11-14)

**Goal:** Contract-first workflow where producers declare and consumers verify.

### Tasks

- [ ] Contract model (schema + SLAs + checks + ownership + version)
- [ ] Contract section in AQL (contracts: key in assay.yaml)
- [ ] Contract validation against data
- [ ] Contract versioning (semver)
- [ ] `assay contract validate` command
- [ ] `assay contract diff v1 v2` command
- [ ] `assay contract breaking-changes` command
- [ ] Contract registry (local file-based, git-friendly)
- [ ] On-violation actions: block, warn, quarantine
- [ ] Contract compliance report (HTML)
- [ ] Example: producer/consumer workflow
- [ ] Documentation: data contracts guide

### Success Criteria

- Define a contract, run validation, see pass/fail
- Detect breaking changes between contract versions
- Generate compliance report

---

## Phase 4: Airflow Provider (Weeks 15-18)

**Goal:** First-class Airflow integration as a provider package.

### Tasks

- [ ] assay-airflow package setup
- [ ] AssayCheckOperator (run checks as Airflow task)
- [ ] AssaySensor (wait for data quality to pass)
- [ ] AssayHook (API client for Assay server)
- [ ] @assay_check decorator for @task functions
- [ ] DAG auto-generation from assay.yaml files
- [ ] Airflow connection type for Assay
- [ ] provider.yaml metadata
- [ ] Integration tests with Airflow 2.9+ and 3.0
- [ ] Example DAGs
- [ ] Documentation: Airflow integration guide
- [ ] Publish to PyPI as apache-airflow-providers-assay

### Success Criteria

- Install provider, use operator in DAG, see results in Airflow logs
- DAG auto-generation works from assay.yaml directory
- Tests pass with Airflow 2.9 and 3.0

---

## Phase 5: Cloud Connectors & Reporting (Weeks 19-22)

**Goal:** Support major cloud data warehouses and rich reporting.

### Tasks

- [ ] Snowflake connector
- [ ] BigQuery connector
- [ ] Redshift connector
- [ ] Databricks connector (via SQLAlchemy)
- [ ] Spark DataFrame connector
- [ ] Polars DataFrame connector
- [ ] HTML report generator (Jinja2 templates)
- [ ] Report: suite summary, check details, trends, failing rows
- [ ] `assay run --report html` command
- [ ] Report publishing to S3/GCS
- [ ] OpenLineage event export
- [ ] Export to Great Expectations format
- [ ] Export to SodaCL format
- [ ] Documentation: connector guide for each platform

### Success Criteria

- Run checks against Snowflake, BigQuery, Redshift
- Generate HTML report viewable in browser
- Export rules to GX and Soda formats

---

## Phase 6: Alerts & Actions (Weeks 23-26)

**Goal:** Automated responses to quality failures.

### Tasks

- [ ] Alert system: dispatch alerts based on check results
- [ ] Slack integration (webhook)
- [ ] PagerDuty integration
- [ ] Email integration (SMTP)
- [ ] Generic webhook integration
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

- [ ] FastAPI server (`assay server`)
- [ ] API routes: /checks, /contracts, /reports, /health
- [ ] API authentication (API keys)
- [ ] PostgreSQL result store (server mode)
- [ ] Scheduled execution (built-in scheduler)
- [ ] WebSocket for real-time check progress
- [ ] API documentation (auto-generated OpenAPI spec)
- [ ] Docker image
- [ ] docker-compose for quick start (Assay + PostgreSQL)
- [ ] Helm chart (Kubernetes deployment)
- [ ] Documentation: server mode guide

### Success Criteria

- `assay server` starts, API responds, checks can be triggered via API
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
- [ ] `assay watch --stream` command
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

- [ ] assay-flyte package
- [ ] @assay_check decorator for Flyte tasks
- [ ] Flyte type transformer for AssayResult
- [ ] Example Flyte workflow with quality gates
- [ ] Documentation: Flyte integration guide
- [ ] Blog post: "Using Assay with Flyte"

### Success Criteria

- Assay works as Flyte plugin
- Same assay.yaml used with both Airflow and Flyte

---

## Phase 10: AQL Spec v1.0 & Community (Weeks 37-40)

**Goal:** Formal AQL specification and community building for incubation.

### Tasks

- [ ] AQL Specification v1.0 document
- [ ] JSON Schema v1.0 for assay.yaml
- [ ] AQL compatibility test suite (for other tools to validate their AQL support)
- [ ] Migration guides: from Great Expectations, from Soda, from dbt tests
- [ ] Website: assay.dev (landing page, docs, blog)
- [ ] Conference talk proposal (Airflow Summit, Data Council, PyCon)
- [ ] Blog: "Why we built Assay" (story, positioning, differentiation)
- [ ] Blog: "From Griffin to Assay: lessons learned"
- [ ] Outreach to potential committers from other organizations
- [ ] Identify 3+ organizations willing to be listed as adopters
- [ ] Identify ASF Champion (potiuk) and Mentors (jscheffl + others)

### Success Criteria

- AQL spec published and reviewable
- At least 3 organizations using Assay or committed to using it
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
| 1 | Core engine | 3-6 | Apr 2026 |
| 2 | Anomaly detection | 7-10 | May 2026 |
| 3 | Data contracts | 11-14 | Jun 2026 |
| 4 | Airflow provider | 15-18 | Jul 2026 |
| 5 | Cloud connectors & reporting | 19-22 | Aug 2026 |
| 6 | Alerts & actions | 23-26 | Sep 2026 |
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
