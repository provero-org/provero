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

"""CI hygiene tests: license headers, packaging, imports, code quality."""

from __future__ import annotations

import importlib
import json
import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent  # assay-core/
SRC_DIR = PROJECT_ROOT / "src" / "provero"
REPO_ROOT = PROJECT_ROOT.parent  # assay/ (the mono-repo root)


class TestLicenseHeaders:
    def test_all_py_files_have_apache_header(self):
        py_files = list(SRC_DIR.rglob("*.py"))
        assert len(py_files) > 0, "No Python files found in src/provero"

        missing = []
        for f in py_files:
            content = f.read_text(encoding="utf-8")
            if "Licensed to the Apache Software Foundation" not in content:
                missing.append(str(f.relative_to(PROJECT_ROOT)))

        assert missing == [], f"Files missing Apache license header: {missing}"


class TestProjectStructure:
    def test_all_packages_have_init(self):
        for d in SRC_DIR.rglob("*"):
            if not d.is_dir():
                continue
            py_files = list(d.glob("*.py"))
            # Skip directories that only have __pycache__ or no .py files
            non_init = [f for f in py_files if f.name != "__init__.py"]
            if non_init:
                init = d / "__init__.py"
                assert init.exists(), f"Package {d.relative_to(PROJECT_ROOT)} missing __init__.py"

    def test_license_and_notice_exist(self):
        # LICENSE and NOTICE are at the mono-repo root
        assert (REPO_ROOT / "LICENSE").exists(), "LICENSE file not found at repo root"
        assert (REPO_ROOT / "NOTICE").exists(), "NOTICE file not found at repo root"


class TestImports:
    def test_no_circular_imports(self):
        """Import all modules to detect circular import errors."""
        py_files = list(SRC_DIR.rglob("*.py"))
        for f in py_files:
            if f.name == "__init__.py":
                parts = f.parent.relative_to(SRC_DIR.parent).parts
            else:
                parts = f.relative_to(SRC_DIR.parent).with_suffix("").parts
            module_name = ".".join(parts)

            # Skip modules that require optional deps
            if any(skip in module_name for skip in ("postgres", "dataframe")):
                continue

            try:
                importlib.import_module(module_name)
            except ImportError as e:
                if "circular" in str(e).lower():
                    pytest.fail(f"Circular import in {module_name}: {e}")
                # Other ImportErrors (missing optional deps) are OK


class TestSchema:
    def test_schema_json_is_valid(self):
        schema_path = SRC_DIR / "schema.json"
        assert schema_path.exists(), "schema.json not found"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        assert "$schema" in schema or "type" in schema

    def test_schema_validates_quickstart_example(self):
        schema_path = SRC_DIR / "schema.json"
        example_path = REPO_ROOT / "examples" / "quickstart" / "provero.yaml"
        if not example_path.exists():
            pytest.skip("quickstart example not found")

        import yaml
        from jsonschema import validate

        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        with example_path.open() as f:
            config = yaml.safe_load(f)
        validate(instance=config, schema=schema)

    def test_schema_validates_ecommerce_example(self):
        schema_path = SRC_DIR / "schema.json"
        example_path = REPO_ROOT / "examples" / "ecommerce" / "provero.yaml"
        if not example_path.exists():
            pytest.skip("ecommerce example not found")

        import yaml
        from jsonschema import validate

        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        with example_path.open() as f:
            config = yaml.safe_load(f)
        validate(instance=config, schema=schema)


class TestPackaging:
    def test_report_template_accessible(self):
        template_path = SRC_DIR / "reporting" / "templates" / "report.html"
        assert template_path.exists(), "report.html template not found"
        content = template_path.read_text(encoding="utf-8")
        assert len(content) > 100, "Template seems too short"

    def test_schema_json_accessible(self):
        ref = importlib.resources.files("provero").joinpath("schema.json")
        content = ref.read_text(encoding="utf-8")
        assert '"type"' in content or '"$schema"' in content

    def test_entry_point_app_is_typer(self):
        import typer

        from provero.cli.main import app

        assert isinstance(app, typer.Typer)


class TestCodeQuality:
    def test_no_raw_print_in_src(self):
        """Source code should use rich console.print, not bare print()."""
        violations = []
        for f in SRC_DIR.rglob("*.py"):
            # CLI uses console.print which is fine
            if "cli" in str(f):
                continue
            content = f.read_text(encoding="utf-8")
            for i, line in enumerate(content.splitlines(), 1):
                stripped = line.lstrip()
                if stripped.startswith("print(") and not stripped.startswith("#"):
                    violations.append(f"{f.relative_to(PROJECT_ROOT)}:{i}")

        assert violations == [], f"Raw print() found in source: {violations}"

    def test_pyproject_version_format(self):
        pyproject = PROJECT_ROOT / "pyproject.toml"
        assert pyproject.exists()
        content = pyproject.read_text(encoding="utf-8")
        match = re.search(r'version\s*=\s*"([^"]+)"', content)
        assert match, "No version found in pyproject.toml"
        version = match.group(1)
        # Should be valid PEP 440 (simplified check)
        assert re.match(r"^\d+\.\d+\.\d+", version), f"Invalid version format: {version}"
