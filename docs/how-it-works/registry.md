# Check Registry

The check registry is how Provero knows which runner function to call for
each check type. It supports both built-in checks (registered via
decorator) and third-party checks (registered via entry points).

- **File**: `provero-core/src/provero/checks/registry.py`
- **Public API**: `register_check`, `get_check_runner`, `list_checks`

---

## A Runner is Just a Function

Every check type is backed by a function with this signature:

```python
from provero.core.compiler import CheckConfig
from provero.core.results import CheckResult
from provero.connectors.base import Connection

def my_check(connection: Connection, table: str, check_config: CheckConfig) -> CheckResult:
    ...
```

No classes, no inheritance, no base class to subclass. A runner reads its
parameters from `check_config.column` / `check_config.params`, runs one
or more queries through `connection.execute(...)`, and returns a
`CheckResult`.

This keeps the contribution surface small. Adding a new check type
usually takes under 50 lines.

---

## Registering a Built-in Check

Inside a check module (e.g. `provero/checks/freshness.py`):

```python
from provero.checks.registry import register_check

@register_check("freshness")
def run_freshness(connection, table, check_config):
    col = check_config.column
    max_age = check_config.params.get("max_age", "24h")
    ...
    return CheckResult(...)
```

The `@register_check(name)` decorator stores the function in the module-
level `_REGISTRY` dict keyed by `name`. Multiple calls with the same name
simply overwrite, which is handy for tests but should never happen in
production code.

## Eager Registration on First Use

The registry is lazy: built-in check modules are not imported at startup.
On the first call to `get_check_runner()` (or `list_checks()`), the
registry calls `_load_builtins()`, which imports every built-in check
module. The import triggers every `@register_check` decorator at
module load time, and the registry is populated.

The list of built-in modules is hard-coded in `_load_builtins()`:

```python
import provero.anomaly.checks
import provero.checks.completeness
import provero.checks.custom
import provero.checks.freshness
import provero.checks.referential
import provero.checks.uniqueness
import provero.checks.validity
import provero.checks.volume
```

Adding a new built-in module means adding one line here.

---

## Third-Party Checks via `entry_points`

Plugins register via `pyproject.toml`:

```toml
[project.entry-points."provero.checks"]
pii_detection = "provero_pii:check_pii"
```

On first `get_check_runner()` call, the registry also runs
`_load_plugins()`, which discovers entry points in the `provero.checks`
group and registers them.

### Plugins cannot override built-ins

```python
for ep in entry_points(group="provero.checks"):
    if ep.name not in _REGISTRY:
        _REGISTRY[ep.name] = ep.load()
```

Because built-ins load first, a plugin named `not_null` is silently
skipped. This is a supply-chain defense: a rogue package cannot hijack
the semantics of a standard check.

---

## `get_check_runner(name)`

The main lookup function used by the [engine](engine.md):

```python
def get_check_runner(name: str) -> CheckRunner | None:
    _ensure_loaded()
    return _REGISTRY.get(name)
```

Returns `None` for unknown names. The engine turns that `None` into an
ERROR `CheckResult` that lists every available check type via
`list_checks()`, so the user sees a helpful error immediately.

## `list_checks()`

Returns every registered check name (built-in + plugin), sorted. The CLI
uses this for the error message and for autocompletion helpers.

---

## Lazy Loading for Optional Dependencies

Some checks depend on optional packages:

- `anomaly` imports `numpy` (or scipy for some methods).
- `custom_sql` has no extra deps.
- `referential_integrity` needs a reference table connection.

The registry's lazy-loading pattern means a user who never uses
`anomaly` never pays the import cost of numpy. If an optional dep is
missing, the check module catches the `ImportError` at import time and
registers a stub runner that returns an ERROR with an
`pip install provero[anomaly]` hint.

---

## Extension Recipe: Adding a New Check

Four steps:

1. **Write the runner.** A function matching the signature above.
2. **Decorate it** with `@register_check("my_check")`.
3. **If you want it in the standard distribution**, add the module path
   to `_load_builtins()` in `checks/registry.py`.
4. **If it can be expressed as a single SQL aggregate**, add a branch in
   `optimizer.plan_batch()` so it joins the batch.

See [`provero/checks/volume.py`](https://github.com/provero-org/provero/blob/main/provero-core/src/provero/checks/volume.py)
for a complete reference implementation covering `row_count` and
`row_count_change`.
