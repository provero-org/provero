# Apache Assay - Technical Architecture

> **assay** /ˈæseɪ/ — the testing of a substance to determine its quality or purity.

A vendor-neutral, declarative data quality engine with built-in anomaly detection.
Works standalone, as an Airflow provider, or with any orchestrator.
Successor espiritual do Apache Griffin, aprendendo com seus erros.

---

## Design Principles

1. **Simple by default, powerful when needed.** Um check de qualidade em 3 linhas de YAML.
   GX precisa de 50+ linhas para o mesmo resultado.
2. **Portable rules.** Regras definidas uma vez, executadas em qualquer lugar.
   Introduce o Assay Quality Language (AQL), um padrão aberto.
3. **Anomaly detection built-in.** Não precisa de SaaS de $100k/ano.
   Detecção estatística roda localmente, sem dependência externa.
4. **Orchestrator-agnostic.** Funciona como CLI, como lib Python, como Airflow
   provider, como sidecar em qualquer pipeline.
5. **Streaming + Batch.** Primeiro framework open source que trata streaming
   como cidadão de primeira classe.
6. **Data contracts first.** Produtores declaram, consumidores verificam,
   Assay enforce.

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           USER LAYER                                    │
│                                                                         │
│  assay.yaml          Python SDK          CLI            REST API        │
│  (declarative)       (programmatic)      (assay-ctl)    (server mode)   │
│                                                                         │
│  ┌──────────────┐   ┌──────────────┐   ┌────────────┐  ┌────────────┐  │
│  │ source: pg   │   │ @check       │   │ assay run  │  │ POST /scan │  │
│  │ checks:      │   │ @contract    │   │ assay scan │  │ GET /report│  │
│  │  - not_null  │   │ @monitor     │   │ assay watch│  │ GET /health│  │
│  │  - unique    │   │              │   │ assay diff  │  │            │  │
│  └──────────────┘   └──────────────┘   └────────────┘  └────────────┘  │
└──────────┬──────────────────┬──────────────────┬──────────────┬─────────┘
           │                  │                  │              │
           ▼                  ▼                  ▼              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         ASSAY CORE ENGINE                               │
│                                                                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌────────────────────────┐  │
│  │  Rule Compiler   │  │  Check Engine    │  │  Anomaly Detector     │  │
│  │                  │  │                  │  │                       │  │
│  │  AQL → exec plan │  │  Executes checks │  │  Statistical models   │  │
│  │  Validates rules │  │  against data    │  │  over historical      │  │
│  │  Optimizes scans │  │  Returns verdicts│  │  check results        │  │
│  └────────┬─────────┘  └────────┬─────────┘  └───────────┬───────────┘  │
│           │                     │                         │             │
│  ┌────────┴─────────────────────┴─────────────────────────┴──────────┐  │
│  │                     Result Store                                   │  │
│  │  Check results, metrics, anomalies, trends (time-series)          │  │
│  │  Backends: SQLite (local) | PostgreSQL (server) | S3 (archive)    │  │
│  └───────────────────────────┬───────────────────────────────────────┘  │
│                               │                                         │
│  ┌────────────────────────────┴──────────────────────────────────────┐  │
│  │                     Contract Registry                              │  │
│  │  Data contracts: schema + quality SLAs + ownership                 │  │
│  │  Versioned, git-friendly, publishable                             │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
┌──────────────────┐ ┌─────────────────┐ ┌──────────────────────┐
│  DATA CONNECTORS │ │  ORCHESTRATION  │ │  OUTPUT / ACTIONS    │
│                  │ │  INTEGRATIONS   │ │                      │
│ ┌──────────────┐ │ │ ┌─────────────┐ │ │ ┌──────────────────┐ │
│ │ SQL          │ │ │ │ Airflow     │ │ │ │ Reports (HTML,   │ │
│ │ (Postgres,   │ │ │ │ Provider    │ │ │ │ JSON, Markdown)  │ │
│ │  MySQL,      │ │ │ ├─────────────┤ │ │ ├──────────────────┤ │
│ │  Snowflake,  │ │ │ │ Standalone  │ │ │ │ Alerts (Slack,   │ │
│ │  BigQuery,   │ │ │ │ (cron/CLI)  │ │ │ │ PagerDuty, email,│ │
│ │  Redshift,   │ │ │ ├─────────────┤ │ │ │ webhook)         │ │
│ │  DuckDB)     │ │ │ │ Flyte       │ │ │ ├──────────────────┤ │
│ ├──────────────┤ │ │ │ (plugin)    │ │ │ │ OpenLineage      │ │
│ │ DataFrame    │ │ │ ├─────────────┤ │ │ │ (lineage events) │ │
│ │ (Pandas,     │ │ │ │ Dagster,    │ │ │ ├──────────────────┤ │
│ │  Polars,     │ │ │ │ Prefect     │ │ │ │ Block / Quarantine│ │
│ │  Spark)      │ │ │ │ (future)    │ │ │ │ (halt pipeline   │ │
│ ├──────────────┤ │ │ └─────────────┘ │ │ │  on failure)     │ │
│ │ Files        │ │ │                 │ │ ├──────────────────┤ │
│ │ (Parquet,    │ │ │                 │ │ │ OpenTelemetry    │ │
│ │  CSV, JSON,  │ │ │                 │ │ │ (metrics export) │ │
│ │  Avro, Delta,│ │ │                 │ │ └──────────────────┘ │
│ │  Iceberg)    │ │ │                 │ │                      │
│ ├──────────────┤ │ │                 │ │                      │
│ │ Streaming    │ │ │                 │ │                      │
│ │ (Kafka,      │ │ │                 │ │                      │
│ │  Kinesis,    │ │ │                 │ │                      │
│ │  Pulsar)     │ │ │                 │ │                      │
│ ├──────────────┤ │ │                 │ │                      │
│ │ APIs         │ │ │                 │ │                      │
│ │ (REST, gRPC, │ │ │                 │ │                      │
│ │  GraphQL)    │ │ │                 │ │                      │
│ └──────────────┘ │ │                 │ │                      │
└──────────────────┘ └─────────────────┘ └──────────────────────┘
```

---

## Assay Quality Language (AQL)

O diferencial principal. Um padrão aberto para definir regras de qualidade
que funciona em qualquer ferramenta, como SQL funciona em qualquer banco.

### Sintaxe básica

```yaml
# assay.yaml - O mais simples possível
source:
  type: postgres
  connection: ${POSTGRES_URI}    # variavel de ambiente ou Airflow connection
  table: orders

checks:
  - not_null: [order_id, customer_id, amount]
  - unique: order_id
  - accepted_values:
      column: status
      values: [pending, shipped, delivered, cancelled]
  - range:
      column: amount
      min: 0
      max: 1000000
  - freshness:
      column: created_at
      max_age: 24h
  - row_count:
      min: 1000
  - custom_sql: |
      SELECT COUNT(*) = 0
      FROM orders
      WHERE amount < 0 AND status = 'delivered'
```

3 linhas para o caso mais comum. Sem boilerplate, sem classes, sem configs.

### Exemplo completo com todas as features

```yaml
# assay.yaml - Exemplo avancado
version: "1.0"

# Fontes de dados reutilizaveis
sources:
  warehouse:
    type: snowflake
    connection: ${SNOWFLAKE_URI}
  lake:
    type: s3
    bucket: data-lake-prod
    format: parquet
  stream:
    type: kafka
    bootstrap_servers: ${KAFKA_BROKERS}
    topic: events.transactions

# ──────────────────────────────────────────
# Data Contracts (produtor declara, consumidor verifica)
# ──────────────────────────────────────────
contracts:
  - name: transactions_contract
    owner: payments-team
    source: warehouse
    table: transactions
    version: "2.1"
    sla:
      freshness: 1h
      completeness: 99.5%    # max 0.5% nulls em campos obrigatorios
      availability: 99.9%    # uptime do data source
    schema:
      columns:
        - name: tx_id
          type: string
          checks: [not_null, unique]
        - name: amount
          type: decimal(10,2)
          checks:
            - not_null
            - range: {min: 0.01}
        - name: currency
          type: string
          checks:
            - accepted_values: [USD, EUR, GBP, BRL, JPY]
        - name: customer_id
          type: string
          checks: [not_null]
        - name: created_at
          type: timestamp
          checks:
            - not_null
            - freshness: {max_age: 1h}
        - name: status
          type: string
          checks:
            - accepted_values: [pending, completed, failed, refunded]
    on_violation: block       # block | warn | quarantine

# ──────────────────────────────────────────
# Check Suites (agrupamento logico)
# ──────────────────────────────────────────
suites:
  - name: transactions_daily
    description: "Daily quality checks on transactions table"
    source: warehouse
    table: transactions
    schedule: "0 6 * * *"    # 6am UTC (standalone mode)
    tags: [payments, daily, critical]

    checks:
      # ── Completeness ──
      - not_null: [tx_id, amount, customer_id, created_at]
      - completeness:
          column: email
          min: 0.95            # at least 95% non-null

      # ── Uniqueness ──
      - unique: tx_id
      - unique_combination: [customer_id, created_at, amount]

      # ── Validity ──
      - accepted_values:
          column: status
          values: [pending, completed, failed, refunded]
      - regex:
          column: email
          pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
      - range:
          column: amount
          min: 0.01
          max: 999999.99

      # ── Freshness ──
      - freshness:
          column: created_at
          max_age: 2h

      # ── Volume ──
      - row_count:
          min: 10000
          max: 10000000
      - row_count_change:
          max_decrease: 20%    # alert if count drops >20% vs yesterday
          max_increase: 300%   # alert if count spikes >300%

      # ── Consistency ──
      - referential_integrity:
          column: customer_id
          reference:
            source: warehouse
            table: customers
            column: id
      - cross_source:
          description: "Row count matches between warehouse and lake"
          left:
            source: warehouse
            query: "SELECT COUNT(*) FROM transactions WHERE date = CURRENT_DATE"
          right:
            source: lake
            path: "transactions/date={{ today }}/"
            query: "SELECT COUNT(*) FROM read_parquet('*.parquet')"
          check: "abs(left - right) / left < 0.01"   # <1% difference

      # ── Statistical / Anomaly ──
      - distribution:
          column: amount
          method: ks_test          # Kolmogorov-Smirnov
          reference: last_30d      # compare against last 30 days
          significance: 0.05
      - anomaly:
          metric: row_count
          method: prophet           # prophet | zscore | mad | iqr
          sensitivity: medium       # low | medium | high
      - anomaly:
          metric: null_rate
          column: email
          method: zscore
          threshold: 3.0

      # ── Custom SQL ──
      - custom_sql:
          name: "no_negative_completed"
          query: |
            SELECT COUNT(*) = 0
            FROM transactions
            WHERE amount < 0 AND status = 'completed'
      - custom_sql:
          name: "daily_revenue_sanity"
          query: |
            SELECT SUM(amount) BETWEEN 100000 AND 50000000
            FROM transactions
            WHERE date = CURRENT_DATE AND status = 'completed'

    # Acoes quando checks falham
    on_failure:
      - alert:
          channels: [slack, pagerduty]
          severity: critical
          message: "Transaction quality checks failed: {{ failed_checks }}"
      - block_downstream: true      # impede DAGs downstream de rodar
      - quarantine:                  # move dados ruins para quarentena
          target: warehouse.quarantine.transactions
          filter: "{{ failing_rows_query }}"

  # ── Streaming checks ──
  - name: transactions_stream
    source: stream
    type: streaming                  # key difference: runs continuously
    window: 5m                       # evaluate every 5 min window

    checks:
      - schema:
          type: json_schema
          ref: schemas/transaction_event.json
      - throughput:
          min: 100                   # min 100 events per window
          max: 50000
      - latency:
          field: event_time
          max_delay: 30s             # event_time vs processing_time
      - anomaly:
          metric: throughput
          method: mad                # Median Absolute Deviation
          sensitivity: high

    on_failure:
      - alert:
          channels: [pagerduty]
          severity: critical

# ──────────────────────────────────────────
# Monitoring (continuous, post-deploy)
# ──────────────────────────────────────────
monitors:
  - name: transactions_drift
    source: warehouse
    table: transactions
    schedule: "0 */6 * * *"         # every 6 hours
    columns: [amount, currency, status]
    methods:
      - psi                          # Population Stability Index
      - wasserstein                  # Earth Mover's Distance
    reference: last_7d
    threshold: 0.2
    on_drift:
      - alert: {channels: [slack]}
      - trigger_pipeline: retrain_fraud_model   # trigger Airflow DAG

# ──────────────────────────────────────────
# Report config
# ──────────────────────────────────────────
reporting:
  formats: [html, json]
  retention: 90d
  publish_to: s3://data-quality-reports/
```

### AQL como padrão aberto

```
AQL (Assay Quality Language) Specification:

Goal: become the "SQL of data quality"
     Define once, run anywhere.

Core check types (universal):
┌─────────────────────┬────────────────────────────────────────┐
│ Category            │ Checks                                 │
├─────────────────────┼────────────────────────────────────────┤
│ Completeness        │ not_null, completeness                 │
│ Uniqueness          │ unique, unique_combination             │
│ Validity            │ accepted_values, range, regex, type    │
│ Freshness           │ freshness, latency                     │
│ Volume              │ row_count, row_count_change, throughput│
│ Consistency         │ referential_integrity, cross_source    │
│ Distribution        │ distribution, anomaly                  │
│ Custom              │ custom_sql, custom_python              │
└─────────────────────┴────────────────────────────────────────┘

The spec defines:
- YAML schema (JSON Schema published)
- Check semantics (what each check means, precisely)
- Result format (standardized output)
- Severity levels (info, warning, critical, blocker)

Other tools can implement AQL:
- Great Expectations could add an AQL importer
- Soda could add AQL compatibility
- dbt could compile AQL to dbt tests
- Cloud DQ tools could adopt AQL as input format
```

---

## Core Components

### 1. Rule Compiler

Transforma AQL (YAML) em planos de execucao otimizados.

```
assay.yaml
    │
    ▼
┌──────────────────────────────┐
│         Rule Compiler         │
│                              │
│  1. Parse YAML               │
│  2. Validate against schema  │
│  3. Resolve references       │
│     (sources, variables)     │
│  4. Build dependency graph   │
│  5. Optimize execution:      │
│     - Batch SQL checks into  │
│       single query           │
│     - Parallelize independent│
│       checks                 │
│     - Sample large tables    │
│       when configured        │
│  6. Output: ExecutionPlan    │
└──────────────────────────────┘

Key optimization: SQL check batching
Instead of:
  SELECT COUNT(*) FROM t WHERE col IS NULL;     -- check 1
  SELECT COUNT(DISTINCT col) FROM t;            -- check 2
  SELECT MIN(col), MAX(col) FROM t;             -- check 3

Compiled into:
  SELECT
    COUNT(*) FILTER (WHERE col IS NULL) as null_count,
    COUNT(DISTINCT col) as distinct_count,
    MIN(col) as min_val,
    MAX(col) as max_val,
    COUNT(*) as total_count
  FROM t;

One query instead of three. Massive performance difference at scale.
```

### 2. Check Engine

Executa os checks contra os dados e retorna resultados padronizados.

```python
# Resultado padronizado de cada check
@dataclass
class CheckResult:
    check_name: str
    check_type: str              # "not_null", "unique", "anomaly", etc.
    status: Status               # PASS | FAIL | WARN | ERROR | SKIP
    severity: Severity           # INFO | WARNING | CRITICAL | BLOCKER

    # O que foi verificado
    source: str
    table: str
    column: str | None

    # Resultado
    observed_value: Any          # o que foi encontrado
    expected_value: Any          # o que era esperado

    # Contexto
    row_count: int               # linhas verificadas
    failing_rows: int            # linhas que falharam
    failing_rows_sample: list    # amostra de linhas ruins (configurable)
    failing_rows_query: str      # SQL para reproduzir

    # Tempo
    started_at: datetime
    duration_ms: int

    # Metadata
    tags: list[str]
    suite: str
    run_id: str

# Resultado de uma suite completa
@dataclass
class SuiteResult:
    suite_name: str
    status: Status               # PASS se todos passaram, senao FAIL
    checks: list[CheckResult]

    total: int
    passed: int
    failed: int
    warned: int
    errored: int

    started_at: datetime
    duration_ms: int

    # Score de qualidade (0-100)
    quality_score: float
```

### 3. Anomaly Detector

Detecçao de anomalias embutida, sem dependencia de SaaS externo.

```
┌─────────────────────────────────────────────────────────────┐
│                    Anomaly Detector                          │
│                                                             │
│  Funciona sobre o historico de resultados de checks.        │
│  Nao precisa dos dados brutos, apenas das metricas.        │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ Methods (built-in, no external deps)                  │  │
│  │                                                       │  │
│  │  Z-Score          Simples, rapido.                    │  │
│  │                   Bom para metricas com distribuicao  │  │
│  │                   normal (row_count, latency)         │  │
│  │                                                       │  │
│  │  MAD              Median Absolute Deviation.          │  │
│  │  (default)        Robusto a outliers. Funciona bem    │  │
│  │                   na maioria dos casos.               │  │
│  │                                                       │  │
│  │  IQR              Interquartile Range.                │  │
│  │                   Bom para metricas com skew.         │  │
│  │                                                       │  │
│  │  Prophet           Facebook Prophet.                  │  │
│  │  (optional dep)    Para metricas com sazonalidade     │  │
│  │                    (weekday/weekend, holidays).       │  │
│  │                                                       │  │
│  │  DBSCAN            Density-based clustering.          │  │
│  │                    Para detectar anomalias             │  │
│  │                    multivariadas.                     │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                             │
│  Input:  historico de metricas (result store)                │
│  Output: anomaly score + is_anomaly boolean + explanation   │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ Sensitivity presets (user-friendly)                   │  │
│  │                                                       │  │
│  │  low:    flags only extreme deviations (>4 sigma)     │  │
│  │  medium: flags significant deviations (>3 sigma)      │  │
│  │  high:   flags moderate deviations (>2 sigma)         │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                             │
│  Nao requer GPU, nao requer cloud, roda em qualquer lugar. │
└─────────────────────────────────────────────────────────────┘
```

### 4. Contract Registry

Data contracts versionados, publicaveis, auditaveis.

```
Conceito:

  PRODUTOR (payments team)          CONSUMIDOR (analytics team)
       │                                    │
       │  publica contrato                  │  importa contrato
       ▼                                    ▼
  ┌──────────────┐                 ┌──────────────┐
  │ transactions │                 │ analytics    │
  │ _contract    │                 │ _pipeline    │
  │ v2.1         │                 │              │
  │              │    registry     │ expects:     │
  │ schema: ...  │◄──────────────►│  transactions│
  │ sla: ...     │                 │  _contract   │
  │ checks: ...  │                 │  v2.x       │
  └──────────────┘                 └──────────────┘

O contrato define:
  - Schema (colunas, tipos, nullable)
  - Quality SLAs (freshness, completeness, availability)
  - Owner (quem e responsavel)
  - Version (semver, breaking changes = major bump)
  - Checks (quality rules que o produtor garante)

Armazenamento:
  - Git (YAML files no repo, versionados com o codigo)
  - Registry Server (API para publicar/consultar, modo server)
  - Ambos (git como source of truth, server como cache)

Workflow:
  1. Produtor define contrato em assay.yaml
  2. CI verifica que dados satisfazem o contrato
  3. Consumidor declara dependencia no contrato
  4. Se produtor quebra o contrato, consumidor e alertado
  5. Breaking changes (major version) requerem opt-in do consumidor
```

### 5. Data Connectors

Arquitetura plugavel. Cada conector implementa uma interface simples.

```python
class Connector(Protocol):
    """Interface que todo conector implementa."""

    def connect(self, config: dict) -> Connection:
        """Estabelece conexao com a fonte de dados."""
        ...

    def execute_checks(
        self,
        connection: Connection,
        checks: list[CompiledCheck]
    ) -> list[CheckResult]:
        """Executa checks compilados contra a fonte."""
        ...

    def get_profile(
        self,
        connection: Connection,
        table: str,
        columns: list[str] | None = None,
        sample_size: int | None = None
    ) -> DataProfile:
        """Gera perfil estatistico dos dados."""
        ...

    def get_schema(
        self,
        connection: Connection,
        table: str
    ) -> SchemaInfo:
        """Retorna schema da tabela."""
        ...


# Conectores built-in no core:
# SQL (SQLAlchemy-based): Postgres, MySQL, SQLite, DuckDB
# Cloud SQL: Snowflake, BigQuery, Redshift, Databricks
# Files: Parquet, CSV, JSON, Avro (via DuckDB ou Polars)
# DataFrame: Pandas, Polars, Spark
# Streaming: Kafka, Kinesis, Pulsar

# Conectores via plugins (pacotes separados):
# assay-connector-mongodb
# assay-connector-elasticsearch
# assay-connector-dynamodb
# etc.
```

---

## Airflow Integration

### Airflow Provider Package: apache-airflow-providers-assay

```python
# ── Operator: executa checks inline na DAG ──

from airflow.decorators import dag, task
from assay.airflow import AssayCheckOperator, AssaySensor

@dag(schedule="@daily")
def etl_pipeline():

    extract = ...
    transform = ...

    # Quality gate entre transform e load
    quality_check = AssayCheckOperator(
        task_id="quality_check",
        # Opcao 1: referencia a arquivo assay.yaml
        assay_file="checks/transactions.yaml",
        # Opcao 2: inline checks
        # source={"type": "postgres", "conn_id": "warehouse"},
        # table="staging.transactions",
        # checks=[
        #     {"not_null": ["id", "amount"]},
        #     {"row_count": {"min": 1000}},
        # ],
        fail_on=["critical", "blocker"],  # quais severidades bloqueiam
    )

    load = ...

    extract >> transform >> quality_check >> load


# ── Sensor: espera dados atingirem qualidade minima ──

    wait_for_quality = AssaySensor(
        task_id="wait_for_quality",
        source={"type": "postgres", "conn_id": "warehouse"},
        table="raw.events",
        checks=[
            {"freshness": {"column": "event_time", "max_age": "1h"}},
            {"row_count": {"min": 10000}},
        ],
        poke_interval=300,      # check every 5 min
        timeout=3600,           # give up after 1h
    )


# ── Decorator: mais Pythonico ──

from assay.airflow import assay_check

@dag(schedule="@daily")
def modern_pipeline():

    @task
    def transform():
        return process_data()

    @assay_check(
        source="warehouse",
        table="staging.output",
        checks=["not_null:id", "unique:id", "freshness:updated_at<1h"],
        fail_on="critical",
    )
    @task
    def load(data):
        write_to_warehouse(data)

    transform() >> load()


# ── DAG auto-generated from assay.yaml ──
# Se assay.yaml tem schedule definido, gera DAG automaticamente

# Arquivo: dags/assay_auto.py (unica linha)
from assay.airflow import generate_dags_from_directory
generate_dags_from_directory("checks/")
# Isso gera um DAG para cada suite com schedule em assay.yaml
```

### Flyte Integration (plugin separado)

```python
# assay-flyte package
from flytekit import task, workflow
from assay.flyte import assay_check

@task
def train_model(data):
    ...

@assay_check(
    source="s3://training-data/",
    checks=["row_count:min=10000", "not_null:label", "completeness:features>0.99"],
)
@task
def validate_training_data(path: str):
    ...

@workflow
def ml_pipeline():
    validate_training_data(path="s3://data/") >> train_model(data=...)
```

---

## Streaming Architecture

```
┌─────────────────────────────────────────────────────────┐
│               Streaming Check Engine                     │
│                                                         │
│  Kafka/Kinesis/Pulsar                                   │
│       │                                                 │
│       ▼                                                 │
│  ┌──────────┐     ┌───────────┐     ┌───────────────┐  │
│  │ Ingester │────▶│ Windowed  │────▶│ Check Engine  │  │
│  │          │     │ Aggregator│     │ (same as batch)│  │
│  └──────────┘     └───────────┘     └───────┬───────┘  │
│                                             │          │
│  Windows:                                   ▼          │
│  - Tumbling (every 5m)          ┌───────────────────┐  │
│  - Sliding (5m window, 1m step) │ Result Store      │  │
│  - Session (gap-based)          │ + Anomaly Detector│  │
│                                 └───────────────────┘  │
│                                                         │
│  Checks suportados em streaming:                       │
│  - schema (JSON Schema validation per message)         │
│  - throughput (messages per window)                     │
│  - latency (event_time vs processing_time)             │
│  - null_rate (per window)                              │
│  - anomaly (on windowed metrics)                       │
│  - custom_python (UDF on each message or window)       │
│                                                         │
│  Nao suportados em streaming (batch-only):             │
│  - unique (requires full scan)                         │
│  - referential_integrity (requires join)               │
│  - distribution tests (requires full sample)           │
└─────────────────────────────────────────────────────────┘
```

---

## Data Model

```sql
-- Minimal schema. SQLite for local, PostgreSQL for server mode.

-- Sources registradas
CREATE TABLE assay_source (
    id          TEXT PRIMARY KEY,    -- "warehouse", "lake", etc.
    type        TEXT NOT NULL,       -- "postgres", "snowflake", etc.
    config      TEXT NOT NULL,       -- JSON (connection params, no secrets)
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

-- Suites de checks
CREATE TABLE assay_suite (
    id          TEXT PRIMARY KEY,
    name        TEXT UNIQUE NOT NULL,
    source_id   TEXT REFERENCES assay_source(id),
    definition  TEXT NOT NULL,       -- JSON (compiled AQL)
    tags        TEXT,                -- JSON array
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

-- Execucoes de suites
CREATE TABLE assay_run (
    id          TEXT PRIMARY KEY,
    suite_id    TEXT REFERENCES assay_suite(id),
    status      TEXT NOT NULL,       -- running, passed, failed, error
    trigger     TEXT NOT NULL,       -- schedule, manual, api, airflow

    total       INTEGER NOT NULL DEFAULT 0,
    passed      INTEGER NOT NULL DEFAULT 0,
    failed      INTEGER NOT NULL DEFAULT 0,
    warned      INTEGER NOT NULL DEFAULT 0,
    errored     INTEGER NOT NULL DEFAULT 0,

    quality_score REAL,              -- 0-100

    started_at  TEXT NOT NULL,
    completed_at TEXT,
    duration_ms INTEGER
);

-- Resultados individuais de cada check
CREATE TABLE assay_check_result (
    id              TEXT PRIMARY KEY,
    run_id          TEXT REFERENCES assay_run(id),
    check_name      TEXT NOT NULL,
    check_type      TEXT NOT NULL,

    status          TEXT NOT NULL,    -- pass, fail, warn, error, skip
    severity        TEXT NOT NULL,    -- info, warning, critical, blocker

    source_table    TEXT,
    source_column   TEXT,

    observed_value  TEXT,             -- JSON
    expected_value  TEXT,             -- JSON

    row_count       INTEGER,
    failing_rows    INTEGER,
    failing_sample  TEXT,             -- JSON (sample of bad rows)
    failing_query   TEXT,             -- SQL to reproduce

    duration_ms     INTEGER,

    INDEX idx_run (run_id),
    INDEX idx_check_type (check_type),
    INDEX idx_status (status)
);

-- Metricas historicas (para anomaly detection)
CREATE TABLE assay_metric (
    id          TEXT PRIMARY KEY,
    suite_id    TEXT REFERENCES assay_suite(id),
    check_name  TEXT NOT NULL,
    metric_name TEXT NOT NULL,        -- "row_count", "null_rate", "mean", etc.
    value       REAL NOT NULL,
    recorded_at TEXT NOT NULL,

    INDEX idx_metric_lookup (suite_id, check_name, metric_name, recorded_at)
);

-- Data contracts
CREATE TABLE assay_contract (
    id          TEXT PRIMARY KEY,
    name        TEXT UNIQUE NOT NULL,
    version     TEXT NOT NULL,        -- semver
    owner       TEXT,
    source_id   TEXT REFERENCES assay_source(id),
    definition  TEXT NOT NULL,        -- JSON (schema + SLAs + checks)
    status      TEXT DEFAULT 'active', -- active, deprecated, violated
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

-- Drift events
CREATE TABLE assay_drift_event (
    id          TEXT PRIMARY KEY,
    suite_id    TEXT REFERENCES assay_suite(id),
    column_name TEXT,
    drift_type  TEXT NOT NULL,        -- data, schema, volume
    method      TEXT NOT NULL,        -- psi, wasserstein, ks_test
    score       REAL NOT NULL,
    threshold   REAL NOT NULL,
    is_anomaly  BOOLEAN NOT NULL,
    detected_at TEXT NOT NULL
);
```

---

## CLI Design

```bash
# ── Init ──
assay init                          # cria assay.yaml template
assay init --from-source postgres://...  # gera checks automaticamente
                                         # baseado no profile dos dados

# ── Run checks ──
assay run                           # executa todos os checks em assay.yaml
assay run --suite transactions      # executa suite especifica
assay run --tag critical            # executa checks com tag
assay run --source warehouse        # executa checks de uma source

# ── Output ──
assay run --format json             # resultado em JSON
assay run --format table            # resultado em tabela (default)
assay run --report html             # gera report HTML

# Exemplo de output (table format):
# ┌─────────────────────┬──────────┬──────────┬───────────┬──────────┐
# │ Check               │ Column   │ Status   │ Observed  │ Expected │
# ├─────────────────────┼──────────┼──────────┼───────────┼──────────┤
# │ not_null            │ order_id │ ✓ PASS   │ 0 nulls   │ 0 nulls  │
# │ not_null            │ amount   │ ✓ PASS   │ 0 nulls   │ 0 nulls  │
# │ unique              │ order_id │ ✓ PASS   │ 0 dupes   │ 0 dupes  │
# │ range               │ amount   │ ✗ FAIL   │ min=-5.00 │ min=0.01 │
# │ freshness           │ created  │ ✓ PASS   │ 23m ago   │ < 2h     │
# │ row_count           │ -        │ ✓ PASS   │ 48,291    │ > 10,000 │
# │ anomaly(row_count)  │ -        │ ⚠ WARN   │ -18% vs 7d│ < ±25%   │
# └─────────────────────┴──────────┴──────────┴───────────┴──────────┘
#
# Suite: transactions_daily
# Score: 85/100 | 5 passed, 1 failed, 1 warning | 1.2s
#
# BLOCKER: range check failed on 'amount' (3 rows with negative values)
# Query to inspect: SELECT * FROM orders WHERE amount < 0.01

# ── Profile ──
assay profile postgres://...        # profile estatistico de uma source
assay profile --table orders        # profile de uma tabela
assay profile --suggest             # sugere checks baseado no profile

# ── Contracts ──
assay contract validate             # valida dados contra contrato
assay contract publish              # publica contrato no registry
assay contract diff v2.0 v2.1       # diff entre versoes do contrato
assay contract breaking-changes     # lista breaking changes

# ── Monitor (continuous) ──
assay watch                         # roda checks continuamente
assay watch --interval 5m           # a cada 5 minutos
assay watch --stream                # modo streaming (Kafka, etc.)

# ── Server mode ──
assay server                        # inicia API server (FastAPI)
assay server --port 8080

# ── Utilities ──
assay diff source_a source_b        # compara duas fontes de dados
assay lineage                       # mostra lineage (OpenLineage)
assay export openlineage            # exporta resultados como OpenLineage events
assay export great-expectations     # exporta regras como GX expectations
assay export soda                   # exporta regras como SodaCL
```

---

## Technology Stack

```
Language:        Python 3.11+ (core engine, connectors, SDK)
                 Rust (CLI binary, via PyO3 - optional, MVP in Python)

SQL Engine:      DuckDB (embedded, for file-based checks and profiling)
                 SQLAlchemy 2.0 (for database connectors)

API:             FastAPI (server mode)
Validation:      Pydantic v2 (config parsing, result models)
Stats:           scipy (statistical tests)
                 numpy (numerical operations)
                 prophet (optional, for time-series anomaly detection)

Build:           uv (package manager)
                 Hatch (build backend)

Testing:         pytest + hypothesis
Docs:            Sphinx + MyST (Markdown)
CI:              GitHub Actions

Packaging:
  assay-core              # engine + CLI + basic connectors (Postgres, DuckDB, files)
  assay-airflow           # Airflow provider
  assay-flyte             # Flyte plugin
  assay-snowflake         # Snowflake connector
  assay-bigquery          # BigQuery connector
  assay-redshift          # Redshift connector
  assay-kafka             # Kafka streaming connector
  assay-server            # REST API server mode
```

---

## Package Structure

```
apache-assay/
├── assay-core/
│   └── src/assay/
│       ├── __init__.py
│       ├── core/
│       │   ├── compiler.py          # AQL YAML → execution plan
│       │   ├── engine.py            # Check execution engine
│       │   ├── results.py           # CheckResult, SuiteResult models
│       │   ├── profiler.py          # Data profiling
│       │   └── optimizer.py         # SQL batching, parallelization
│       ├── checks/
│       │   ├── completeness.py      # not_null, completeness
│       │   ├── uniqueness.py        # unique, unique_combination
│       │   ├── validity.py          # accepted_values, range, regex
│       │   ├── freshness.py         # freshness, latency
│       │   ├── volume.py            # row_count, row_count_change, throughput
│       │   ├── consistency.py       # referential_integrity, cross_source
│       │   ├── distribution.py      # ks_test, chi_squared, psi
│       │   ├── custom.py            # custom_sql, custom_python
│       │   └── registry.py          # check type registry (plugable)
│       ├── anomaly/
│       │   ├── detector.py          # Anomaly detection orchestrator
│       │   ├── methods/
│       │   │   ├── zscore.py
│       │   │   ├── mad.py           # Median Absolute Deviation
│       │   │   ├── iqr.py           # Interquartile Range
│       │   │   ├── prophet.py       # Facebook Prophet (optional)
│       │   │   └── dbscan.py        # Density-based (optional)
│       │   └── sensitivity.py       # low/medium/high presets
│       ├── connectors/
│       │   ├── base.py              # Connector protocol
│       │   ├── sql.py               # Generic SQL (SQLAlchemy)
│       │   ├── postgres.py
│       │   ├── duckdb.py            # Embedded (files, Parquet, CSV)
│       │   ├── dataframe.py         # Pandas, Polars
│       │   └── registry.py          # Connector registry
│       ├── contracts/
│       │   ├── models.py            # Contract data models
│       │   ├── registry.py          # Contract storage/retrieval
│       │   ├── validator.py         # Validate data against contract
│       │   └── diff.py              # Contract version diffing
│       ├── streaming/
│       │   ├── engine.py            # Streaming check engine
│       │   ├── window.py            # Windowing logic
│       │   └── ingester.py          # Message ingestion
│       ├── store/
│       │   ├── sqlite.py            # Local result store
│       │   ├── postgres.py          # Server result store
│       │   └── models.py            # SQLAlchemy models
│       ├── reporting/
│       │   ├── html.py              # HTML report generator
│       │   ├── json.py              # JSON output
│       │   ├── table.py             # CLI table output
│       │   └── templates/           # Jinja2 HTML templates
│       ├── actions/
│       │   ├── alert.py             # Slack, PagerDuty, webhook, email
│       │   ├── block.py             # Block downstream pipelines
│       │   ├── quarantine.py        # Move bad data to quarantine
│       │   └── trigger.py           # Trigger external pipelines
│       ├── export/
│       │   ├── openlineage.py       # Export as OpenLineage events
│       │   ├── great_expectations.py # Export as GX expectations
│       │   └── soda.py              # Export as SodaCL
│       ├── api/                     # REST API (server mode)
│       │   ├── app.py
│       │   ├── routes/
│       │   │   ├── checks.py
│       │   │   ├── contracts.py
│       │   │   ├── reports.py
│       │   │   └── health.py
│       │   └── auth.py
│       └── cli/
│           ├── main.py              # Click/Typer CLI
│           ├── commands/
│           │   ├── run.py
│           │   ├── profile.py
│           │   ├── contract.py
│           │   ├── watch.py
│           │   ├── server.py
│           │   └── export.py
│           └── output.py            # Rich terminal output
│
├── assay-airflow/
│   └── src/assay/airflow/
│       ├── operators.py             # AssayCheckOperator
│       ├── sensors.py               # AssaySensor, AssayDriftSensor
│       ├── hooks.py                 # AssayHook (API client)
│       ├── decorators.py            # @assay_check
│       ├── dag_generator.py         # Auto-gen DAGs from assay.yaml
│       └── provider.yaml
│
├── assay-flyte/
│   └── src/assay/flyte/
│       ├── plugin.py
│       └── decorators.py
│
├── assay-snowflake/
│   └── src/assay/connectors/
│       └── snowflake.py
│
├── assay-bigquery/
│   └── src/assay/connectors/
│       └── bigquery.py
│
├── assay-kafka/
│   └── src/assay/connectors/
│       └── kafka.py
│
├── assay-server/
│   └── (installs assay-core + API dependencies)
│
├── docs/
│   ├── getting-started.md
│   ├── aql-spec.md                  # AQL language specification
│   ├── connectors/
│   ├── airflow-integration.md
│   └── migration-from-gx.md        # Guia para migrar do Great Expectations
│
├── examples/
│   ├── quickstart/                  # 3-line assay.yaml
│   ├── ecommerce/                   # Full e-commerce pipeline
│   ├── iot-sensors/                 # IoT/industrial (caso Bosch)
│   ├── streaming/                   # Kafka streaming checks
│   └── data-contracts/              # Contract-first workflow
│
├── aql-spec/                        # AQL spec (separado, para adocao por outros)
│   ├── spec.md
│   ├── schema.json                  # JSON Schema for assay.yaml
│   └── examples/
│
├── pyproject.toml                   # uv workspace root
└── CONTRIBUTING.md
```

---

## What Makes Assay Different

```
┌────────────────────┬──────────┬───────────┬─────────┬──────────┐
│                    │  Assay   │  Great Ex │  Soda   │  Pandera │
├────────────────────┼──────────┼───────────┼─────────┼──────────┤
│ Lines for 1 check  │ 3        │ 50+       │ 5       │ 10       │
│ Anomaly detection  │ Built-in │ No        │ Paid    │ No       │
│ Data contracts     │ Built-in │ No        │ Yes     │ No       │
│ Streaming          │ Yes      │ No        │ No      │ No       │
│ Airflow provider   │ Yes      │ Community │ Partial │ No       │
│ Flyte plugin       │ Yes      │ No        │ No      │ Yes*     │
│ CLI                │ Yes      │ Partial   │ Yes     │ No       │
│ SQL batching       │ Yes      │ No        │ No      │ N/A      │
│ Rule portability   │ AQL std  │ No        │ No      │ No       │
│ Governance         │ Apache   │ VC-backed │ VC-back │ Union.ai │
│ Self-hosted cost   │ Free     │ Free*     │ Free*   │ Free     │
│ Anomaly (self-host)│ Free     │ No        │ Paid    │ No       │
└────────────────────┴──────────┴───────────┴─────────┴──────────┘

* Free with significant engineering effort
```

---

## MVP Scope (Phase 1 - Incubation Proposal)

What ships for the proposal demo:

```
Must have (MVP):
  ✓ assay.yaml parsing (AQL core subset)
  ✓ Check engine with 8 core check types:
    - not_null, unique, accepted_values, range
    - freshness, row_count, custom_sql, completeness
  ✓ 3 connectors: PostgreSQL, DuckDB (files), Pandas DataFrame
  ✓ CLI: assay init, assay run, assay profile
  ✓ Result store: SQLite
  ✓ Table + JSON output
  ✓ Airflow provider: AssayCheckOperator (basic)
  ✓ One complete example: e-commerce pipeline

Phase 2 (post-acceptance):
  - Anomaly detection (Z-Score, MAD)
  - HTML reports
  - Data contracts
  - Snowflake, BigQuery connectors
  - assay profile --suggest
  - SQL batching optimizer

Phase 3:
  - Streaming engine (Kafka)
  - Server mode (REST API)
  - Prophet anomaly detection
  - Flyte plugin
  - Export to GX/Soda formats
  - AQL spec v1.0 formal publication

Phase 4:
  - Advanced anomaly (DBSCAN, multivariate)
  - Quarantine actions
  - OpenLineage integration
  - Rust CLI
  - UI dashboard (optional, community-driven)
```

---

## Anti-Griffin: Community Health Plan

Lessons from Griffin's failure, built into the project DNA:

```
1. Multi-company from day 0
   - Seek committers from at least 3 organizations before proposing
   - No single company holds >40% of committers

2. Release early, release often
   - Monthly releases during incubation
   - No multi-year rewrites. Incremental improvements only.

3. Onboarding pipeline
   - "good first issue" labels always available
   - Contributor guide with video walkthrough
   - Monthly contributor office hours

4. Communication cadence
   - Weekly dev sync (async, on mailing list)
   - Monthly community call (video)
   - Board reports always on time

5. User community
   - Discord/Slack for real-time help
   - dev@assay.apache.org for decisions (Apache Way)
   - Blog post for every release

6. Meritocratic path
   - Clear criteria for contributor → committer → PMC
   - Actively invite new committers (target: 2+ new per quarter)
```
