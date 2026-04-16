# Results

Every check, batched or not, produces a `CheckResult`. A full suite run
aggregates them into a `SuiteResult` with an overall status and a
severity-weighted quality score.

- **File**: `provero-core/src/provero/core/results.py`
- **Models**: `CheckResult`, `SuiteResult`
- **Enums**: `Status`, `Severity`

---

## `Status` and `Severity`

Both are `StrEnum` values (Python 3.11+).

### `Status`

```python
class Status(StrEnum):
    PASS  = "pass"       # check succeeded
    FAIL  = "fail"       # check failed at CRITICAL/BLOCKER severity
    WARN  = "warn"       # check failed at INFO/WARNING severity
    ERROR = "error"      # runner raised an exception
    SKIP  = "skip"       # check was skipped (not currently emitted by core)
```

### `Severity`

```python
class Severity(StrEnum):
    INFO     = "info"
    WARNING  = "warning"
    CRITICAL = "critical"   # default
    BLOCKER  = "blocker"
```

Severity is set per check via the `severity:` key in YAML. It defaults to
`CRITICAL`. Severity controls two things:

1. **Whether a failing check downgrades to WARN** (INFO/WARNING do).
2. **How much weight the check carries in the quality score**.

See [Engine](engine.md#severity-downgrade) for the downgrade logic.

---

## `CheckResult`

The record produced by every check execution:

```python
class CheckResult(BaseModel):
    check_name: str                       # "not_null:order_id"
    check_type: str                       # "not_null"
    status: Status
    severity: Severity = Severity.CRITICAL

    source: str = ""                      # "duckdb", "postgres", ...
    table: str = ""
    column: str | None = None

    observed_value: Any = None            # what was found
    expected_value: Any = None            # what was expected

    row_count: int = 0                    # rows scanned
    failing_rows: int = 0                 # rows that failed
    failing_rows_sample: list[dict] = []  # LIMIT 5 of failing rows
    failing_rows_query: str = ""          # SQL to reproduce

    started_at: datetime = <now>
    duration_ms: int = 0

    tags: list[str] = []
    suite: str = ""
    run_id: str = ""
```

### Observed vs expected

Every `CheckResult` carries both what was found (`observed_value`) and
what was expected (`expected_value`) as arbitrary values. Reports render
these two side by side:

| Check | Observed | Expected |
|-------|----------|----------|
| `not_null:order_id` | "0 nulls" | "0 nulls" |
| `range:amount` | "min=45, max=999" | "min=0, max=100000" |
| `row_count` | "5" | ">= 1" |

Checks are free to use any type here. Human-readable strings are the
convention for terminal output; JSON reports pass the values through
unmodified.

### The debug trio

Three fields make failures actionable:

- **`failing_rows`**: how many rows failed.
- **`failing_rows_sample`**: up to 5 real rows that failed, populated by
  the engine after running `failing_rows_query` with `LIMIT 5`.
- **`failing_rows_query`**: exact SQL the user can copy-paste to see
  every failing row.

Checks that can produce a SQL expression for failures always set
`failing_rows_query`. The [optimizer](optimizer.md) does this for every
batchable check; the per-check runners do it for everything else where
feasible.

### `apply_severity()`

```python
def apply_severity(self) -> None:
    if self.status == Status.FAIL and self.severity in (Severity.INFO, Severity.WARNING):
        self.status = Status.WARN
```

Called by the engine after each check. A failing INFO or WARNING check
becomes a WARN, which does not fail the suite but is still surfaced in
the report.

---

## `SuiteResult`

Aggregates every `CheckResult` in a suite run:

```python
class SuiteResult(BaseModel):
    suite_name: str
    status: Status
    checks: list[CheckResult] = []

    total:   int = 0
    passed:  int = 0
    failed:  int = 0
    warned:  int = 0
    errored: int = 0

    started_at: datetime = <now>
    duration_ms: int = 0

    quality_score: float = 0.0
```

### `compute_status()`

Called once at the end of a suite run. It does two things:

1. **Counts** PASS / FAIL / WARN / ERROR across all checks.
2. **Computes** the suite status and quality score.

The suite status is simple:

```python
self.status = Status.PASS if self.failed == 0 and self.errored == 0 else Status.FAIL
```

A WARN does not fail the suite. An ERROR does (because an ERROR means the
check could not even run, which is almost always a real problem).

---

## Severity-Weighted Quality Score

The quality score is a weighted percentage:

```python
ok_weight    = sum of weights of PASS + WARN checks
total_weight = sum of weights of all checks
score        = round((ok_weight / total_weight) * 100, 1)
```

Weights come from `_SEVERITY_WEIGHT`:

| Severity | Weight |
|----------|--------|
| `INFO` | 0.25 |
| `WARNING` | 0.5 |
| `CRITICAL` | 1.0 |
| `BLOCKER` | 1.0 |

### Key design choice: PASS and WARN count the same

A WARN means the check detected an issue whose severity was too low to
block the suite (INFO or WARNING). These are still surfaced in the
report but do not reduce the score the way a FAIL or ERROR does.

This matches the mental model users have when tagging a check as
`severity: warning`: "I want to know about this, but it is not worth
blocking the pipeline."

### Example

Three checks in a suite:

| Check | Severity | Status | Weight | Ok? |
|-------|----------|--------|--------|-----|
| `not_null:order_id` | CRITICAL | PASS | 1.0 | yes |
| `completeness:email` | WARNING | FAIL -> WARN | 0.5 | yes |
| `unique:order_id` | CRITICAL | FAIL | 1.0 | no |

```
ok_weight    = 1.0 + 0.5 = 1.5
total_weight = 1.0 + 0.5 + 1.0 = 2.5
score        = 1.5 / 2.5 * 100 = 60.0
```

The suite fails (one CRITICAL failure) with a score of 60.

---

## Rendering

`CheckResult` and `SuiteResult` are Pydantic models, which means:

- **JSON export** is free: `suite_result.model_dump_json()`.
- **Dictionary conversion** is free: `suite_result.model_dump()`.
- **Deserialization** is free: `SuiteResult.model_validate(data)`.

The reporting module (`provero/reporting/`) and the result store
(`provero/store/`) rely on this to round-trip results through files,
HTML reports, and the SQLite database without custom serializers.
