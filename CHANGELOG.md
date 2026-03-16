# Changelog

All notable changes to Provero will be documented in this file.

This project uses [towncrier](https://towncrier.readthedocs.io/) to manage its changelog.
Each PR should include a news fragment in the `newsfragments/` directory.

<!-- towncrier release notes start -->

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
