# Changelog

All notable changes to Provero will be documented in this file.

This project uses [towncrier](https://towncrier.readthedocs.io/) to manage its changelog.
Each PR should include a news fragment in the `newsfragments/` directory.

<!-- towncrier release notes start -->

0.2.1 (2026-03-22)

# Bug Fixes

- Fix SQL injection via table expressions, connection leak in run_suite, regex crash on empty tables, SQLite WAL mode for thread safety, Airflow operator store leak, artifact version mismatch, optimizer alias collisions, completeness percentage parsing, and env var resolution safety. (code-review)
- Fix empty table false failures in accepted_values/range, crash with empty values list, PostgreSQL subquery aliases, optimizer alias collisions, alert env var expansion, and SQLiteStore resource leaks in CLI. (review-round2)
- Fix email_validation failing_rows_query to use correct regex dialect per database, and fix unique_combination false positives when composite key columns contain NULLs. (review-round3)


0.2.0 (2026-03-21)

# Features

- Add `referential_integrity` check for foreign key validation between tables. (#32)
- Added `email_validation` check type that validates email format in a column using cross-database regex. (#34)
- Add `provero import soda` command to convert SodaCL YAML configs to Provero format. (#44)
- Added `provero export dbt` command that generates dbt `schema.yml` test definitions from Provero checks. Supports not_null, unique, accepted_values, and range mappings. (#45)
- Add `provero watch` command for continuous monitoring with configurable polling interval, iteration count, and graceful Ctrl+C handling. (#55)

# Bug Fixes

- Fix unique check false positives with NULLs, completeness percentage parsing, SQL injection in range, non-portable FILTER clause, broken URLs, incorrect docs, CLI positional args, connector security, PostgreSQL pooling, package exports, and IQR calculation. (all-fixes)
- Fix unique check false positives with NULLs, completeness percentage parsing, thread-safe parallel mode, SQL injection prevention, and cross-database SQL compatibility. (check-logic)
- Fix broken URLs, incorrect parameter names in docs, and README accuracy. (docs-urls)
- Fix schema validation for completeness percentage values, custom_sql example, init error handling, and documentation gaps. (pre-release)
- Add positional config path to CLI, fix empty table crash, graceful range validation errors, and cross-database SQL in anomaly checks. (remaining-fixes)
- Add Engine class and expose public API from top-level package import. (#93)
- Fix provero.airflow module not importable from published package. (#94)
- Register --version/-V flag on the CLI root command. (#95)


0.1.1 (2026-03-16)

# Features

- Add realistic end-to-end tests with dirty data. (#88)

# Bug Fixes

- Fix JSON output containing invalid control characters when checks fail. (#85)
- Fix contract validation with DuckDB file-based connections. (#86)
- Fix version command to read from package metadata instead of hardcoded value. (#87)


0.1.0 (2026-03-16)

# Features

- Add integration tests for MySQL connector. (#20)
- Add integration tests for SQLite connector. (#21)
- Add `provero-flyte` plugin for running quality checks as Flyte tasks. (#23)
- Add --quiet flag to CLI for scripting and CI usage. (#24)
- Improve error message when connector package is not installed. (#25)
- Add a CSV output format for provero run via --format CSV with columns: suite_name, check_type, column, status, severity, observed_value, expected_value. (#26)
- Improve CLI help text for all commands. (#27)
- Add provider.yaml for Airflow provider discovery. (#72)

# Documentation

- Add API reference documentation. (api-docs)
- Add connectors, anomaly detection, data contracts, Airflow and Flyte integration guides. (docs-guides)
- Add VHS tape script for terminal demo GIF in README. (#9)
