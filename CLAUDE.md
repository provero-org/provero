# Provero

Vendor-neutral, declarative data quality engine.

## Project structure

uv workspace monorepo with three packages:

- `provero-core/` - pacote principal `provero` (engine, checks, connectors, CLI)
- `provero-airflow/` - provider Airflow (`ProveroCheckOperator`, decorators)
- `provero-flyte/` - plugin Flyte (`provero_check_task`, decorators)

## Commands

Always use `uv` from the project root:

```bash
uv sync                          # install all deps
uv run pytest --tb=short -q      # run tests
uv run ruff check .              # lint
uv run ruff format .             # format
uv run mypy provero-core/src/    # type check
```

## Testing

- Tests live in `provero-core/tests/`
- Fixtures in `conftest.py` use DuckDB in-memory and SQLite temp for isolation
- CLI tests need file-based DuckDB (`:memory:` is empty on new connections)
- Some tests skip when optional deps are missing (airflow, pandas, scipy)

## Key conventions

- Python 3.11+
- pydantic v2 models
- SQLAlchemy 2.0 style
- Apache License 2.0 headers on all files
- Commits in English, no Co-Authored-By
