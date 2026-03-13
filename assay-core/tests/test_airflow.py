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

"""Tests for the Airflow provider (requires provero-airflow package)."""

from __future__ import annotations

import pytest

try:
    from provero.airflow.operators import AssayCheckOperator
    from provero.airflow.decorators import assay_check
    HAS_AIRFLOW_PACKAGE = True
except ImportError:
    HAS_AIRFLOW_PACKAGE = False

pytestmark = pytest.mark.skipif(
    not HAS_AIRFLOW_PACKAGE,
    reason="provero-airflow package not installed",
)


class TestAssayCheckOperator:
    def test_instantiation(self):
        op = AssayCheckOperator(
            task_id="test_check",
            config_path="provero.yaml",
            suite="orders",
        )
        assert op.config_path == "provero.yaml"
        assert op.suite == "orders"
        assert op.fail_on_error is True
        assert op.optimize is True

    def test_template_fields(self):
        assert "config_path" in AssayCheckOperator.template_fields
        assert "suite" in AssayCheckOperator.template_fields


class TestAssayCheckDecorator:
    def test_import(self):
        assert callable(assay_check)

    def test_decorator_wraps_function(self):
        @assay_check(config_path="nonexistent.yaml")
        def my_task():
            return 42

        assert my_task.__name__ == "my_task"
