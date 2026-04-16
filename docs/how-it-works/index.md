# How Provero Works

This section is a guided tour through what actually happens when you type
`provero run`. It walks the pipeline end to end, from the YAML file on disk
to the final [CheckResult](../api/results.md) list, stopping at every
component that matters.

If you want to know "where is the parser?", "how does the source string turn
into a live connection?", or "what does the optimizer actually batch?", you
are in the right place.

For the user-facing configuration reference, see
[Configuration](../configuration.md). For the target architecture (including
features on the roadmap), see [Architecture](../ARCHITECTURE.md).

---

## Pipeline at a Glance

```
provero.yaml                                    Return
    │                                             ▲
    │ 1. compile_file()                           │ 7. SuiteResult
    ▼                                             │
ProveroConfig                                     │
    │                                     ┌───────┴────────┐
    │ 2. create_connector()               │ compute_status │
    ▼                                     │ quality_score  │
Connector  ─────── connect() ──► Connection
    │                                     ▲
    │ 3. _expand_multi_column_checks()    │
    ▼                                     │
Expanded CheckConfig list                 │
    │                                     │
    │ 4. plan_batch()                     │
    ▼                                     │
BatchPlan (metrics + non_batchable)       │
    │                                     │
    │ 5. execute_batch()                  │ 6. _run_single_check()
    ▼                                     │    (sequential or parallel)
Batched CheckResults ─────────────────────┘
                                  ▲
                                  │
                      Non-batchable CheckResults
```

Every stage maps to one file in `provero-core/src/provero/`:

| Stage | Component | Entry point |
|-------|-----------|-------------|
| 1. Parse YAML | [Parser](parser.md) | `core/compiler.py::compile_file()` |
| 2. Resolve source | [Connectors](connectors.md) | `connectors/factory.py::create_connector()` |
| 3, 4, 5. Plan and batch SQL | [Optimizer](optimizer.md) | `core/optimizer.py::plan_batch()` |
| 6. Run remainder, orchestrate | [Engine](engine.md) | `core/engine.py::run_suite()` |
| Register checks | [Check Registry](registry.md) | `checks/registry.py::register_check()` |
| 7. Aggregate | [Results](results.md) | `core/results.py::SuiteResult.compute_status()` |
| Defense in depth | [SQL Safety](sql-safety.md) | `core/sql.py::quote_identifier()` |

---

## Read in Order

If this is your first time reading the internals, follow the pages in this
order:

1. **[Parser](parser.md)**: how `provero.yaml` becomes a typed `ProveroConfig`.
2. **[Connectors](connectors.md)**: how a `SourceConfig` becomes a live
   database connection, with plugin support via entry_points.
3. **[Optimizer](optimizer.md)**: how multiple checks are compiled into a
   single SQL query to minimize round trips.
4. **[Engine](engine.md)**: how a full suite is executed, including the
   non-batchable runner path and parallel mode.
5. **[Check Registry](registry.md)**: how check runners are registered and
   discovered (built-ins + third-party plugins).
6. **[Results](results.md)**: how individual `CheckResult` objects are
   aggregated into a `SuiteResult` with a severity-weighted quality score.
7. **[SQL Safety](sql-safety.md)**: the defenses that keep user-supplied
   identifiers and values from becoming a SQL injection vector.

Each page is self-contained: it explains what the component does, where it
lives, its public API, and the design decisions behind it.

---

## The `Engine` Class Shortcut

If you just want to use Provero programmatically, the `Engine` class wraps
every stage:

```python
from provero import Engine

engine = Engine("provero.yaml")
results = engine.run(optimize=True, parallel=False)
```

See the [Engine](engine.md) page for the full API, including
`Engine.from_dict()` for in-memory configs and `run_suites()` for richer
output.
