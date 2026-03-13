# Contributing to Provero

## Getting Started

### Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) package manager

### Development setup

```bash
git clone https://github.com/provero-org/provero.git
cd provero
uv sync --all-extras
uv run pytest
```

This should take less than 5 minutes. If it takes longer, please open an issue.

## How to Contribute

### Reporting Issues

Use [GitHub Issues](https://github.com/provero-org/provero/issues) to report bugs or request features. Please check existing issues first to avoid duplicates.

### Submitting Changes

1. Fork the repository
2. Create a feature branch from `main` (`git checkout -b feature/my-change`)
3. Make your changes
4. Add tests for new functionality
5. Run the checks:
   ```bash
   uv run ruff check provero-core/src/ provero-core/tests/
   uv run ruff format --check provero-core/src/ provero-core/tests/
   uv run pytest provero-core/tests/ -v
   uv run mypy provero-core/src/provero/
   ```
6. Submit a pull request

### Code Style

- Follow PEP 8
- Use type hints for all public APIs
- Run `ruff check` and `ruff format` before committing

### Apache License Headers

All new `.py` files must include the Apache 2.0 license header at the top:

```python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
```

CI will fail if the header is missing.

### Commit Messages

Use clear, descriptive commit messages:

- `add freshness check for timestamp columns`
- `fix SQL generation for BigQuery connector`
- `update profiler to suggest range checks`

### Pull Request Process

1. Fill out the PR template
2. Ensure CI passes (lint, tests, type check, license check)
3. Wait for review (target: < 1 week)
4. Address feedback
5. A maintainer will merge once approved

### Good First Issues

Look for issues labeled [`good first issue`](https://github.com/provero-org/provero/issues?q=label%3A%22good+first+issue%22) for a guided entry point. These include pointers to the relevant files and expected behavior.

## Contributor License Agreement

By contributing to Provero, you agree that your contributions will be licensed under the Apache License 2.0. When the project enters Apache Incubation, contributors will be asked to sign the Apache Individual Contributor License Agreement (ICLA).

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Communication

- [GitHub Discussions](https://github.com/provero-org/provero/discussions) for questions and ideas
- [GitHub Issues](https://github.com/provero-org/provero/issues) for bug reports and feature requests
- [Slack](https://provero.slack.com) for real-time chat
