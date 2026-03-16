# Release Process

## Versioning

Provero follows [Semantic Versioning](https://semver.org/): `MAJOR.MINOR.PATCH`.

Pre-1.0 releases may include breaking changes in minor versions.

## Branch Strategy

| Branch | Purpose |
|--------|---------|
| `main` | Active development |
| `release/v0.x` | Stable release branch, receives backports |

## How to Release

1. **Create a release branch** from `main`:
   ```bash
   git checkout -b release/v0.1 main
   ```

2. **Generate the changelog**:
   ```bash
   uv run towncrier build --version 0.1.0
   ```
   This consumes all news fragments and updates `CHANGELOG.md`.

3. **Bump the version** in `provero-core/pyproject.toml` and `provero-airflow/pyproject.toml`.

4. **Commit and tag**:
   ```bash
   git commit -am "Release v0.1.0"
   git tag v0.1.0
   git push origin release/v0.1 --tags
   ```

5. **Create a GitHub Release** from the tag. This triggers the `Publish to PyPI` workflow automatically.

6. **Merge changelog back to main**:
   ```bash
   git checkout main
   git merge release/v0.1 --no-ff
   git push origin main
   ```

## Backports

To backport a fix to a release branch:

1. Merge the fix to `main` first
2. Add the label `backport-to-release/v0.x` to the PR
3. The backport workflow will create a cherry-pick PR automatically
4. If the cherry-pick conflicts, resolve manually

## Constraint Files

Each release branch has constraint files in `constraints/` that pin exact dependency versions per Python version. These are generated automatically by CI and ensure reproducible installs.

## News Fragments

Every PR must include a news fragment in `newsfragments/`. See [CONTRIBUTING.md](https://github.com/provero-org/provero/blob/main/CONTRIBUTING.md) for details.

## PyPI Publishing

Publishing uses [trusted publishers](https://docs.pypi.org/trusted-publishers/) (OIDC). No manual tokens needed. The `Publish to PyPI` workflow runs automatically when a GitHub Release is created.
