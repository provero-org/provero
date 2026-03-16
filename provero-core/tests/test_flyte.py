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

"""Tests for the Flyte plugin (requires provero-flyte package)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

try:
    from provero.flyte.decorators import provero_check
    from provero.flyte.task import ProveroCheckConfig, ProveroCheckResult, provero_check_task

    HAS_FLYTE_PACKAGE = True
except ImportError:
    HAS_FLYTE_PACKAGE = False

pytestmark = pytest.mark.skipif(
    not HAS_FLYTE_PACKAGE,
    reason="provero-flyte package not installed",
)


class TestProveroCheckConfig:
    def test_defaults(self):
        config = ProveroCheckConfig()
        assert config.config_path == "provero.yaml"
        assert config.suite is None
        assert config.fail_on_error is True
        assert config.optimize is True

    def test_custom_values(self):
        config = ProveroCheckConfig(
            config_path="custom.yaml",
            suite="orders",
            fail_on_error=False,
            optimize=False,
        )
        assert config.config_path == "custom.yaml"
        assert config.suite == "orders"
        assert config.fail_on_error is False
        assert config.optimize is False


class TestProveroCheckResult:
    def test_defaults(self):
        result = ProveroCheckResult()
        assert result.suite_name == ""
        assert result.status == ""
        assert result.total == 0
        assert result.passed == 0
        assert result.failed == 0
        assert result.warned == 0
        assert result.errored == 0
        assert result.quality_score == 0.0
        assert result.duration_ms == 0
        assert result.failed_checks == []


class TestProveroCheckTask:
    def test_callable(self):
        assert callable(provero_check_task)


class TestProveroCheckDecorator:
    def test_import(self):
        assert callable(provero_check)

    def test_decorator_wraps_function(self):
        @provero_check(config_path="nonexistent.yaml")
        def my_task():
            return 42

        assert my_task.__name__ == "my_task"


class TestProveroRenderer:
    def test_to_html_returns_string_with_suite_name(self):
        from provero.flyte.deck import ProveroRenderer

        from provero.core.results import Status, SuiteResult

        suite_result = SuiteResult(
            suite_name="test_suite",
            status=Status.PASS,
            total=1,
            passed=1,
            quality_score=100.0,
        )
        renderer = ProveroRenderer()
        html = renderer.to_html(suite_result)
        assert isinstance(html, str)
        assert "test_suite" in html


class TestPublishProveroDeck:
    def test_noop_without_flytekit(self):
        from provero.flyte.deck import publish_provero_deck

        from provero.core.results import Status, SuiteResult

        suite_result = SuiteResult(
            suite_name="test_suite",
            status=Status.PASS,
            total=1,
            passed=1,
            quality_score=100.0,
        )
        # Should not raise even if Deck() fails (no task context)
        publish_provero_deck(suite_result)

    def test_creates_deck_with_correct_args(self):
        from provero.flyte.deck import publish_provero_deck

        from provero.core.results import Status, SuiteResult

        suite_result = SuiteResult(
            suite_name="orders_daily",
            status=Status.PASS,
            total=2,
            passed=2,
            quality_score=100.0,
        )

        mock_deck = MagicMock()
        with patch("flytekit.Deck", mock_deck):
            publish_provero_deck(suite_result)

        mock_deck.assert_called_once()
        call_args = mock_deck.call_args
        assert call_args[0][0] == "Provero: orders_daily"
        assert "orders_daily" in call_args[0][1]

    def test_custom_title(self):
        from provero.flyte.deck import publish_provero_deck

        from provero.core.results import Status, SuiteResult

        suite_result = SuiteResult(
            suite_name="orders_daily",
            status=Status.PASS,
        )

        mock_deck = MagicMock()
        with patch("flytekit.Deck", mock_deck):
            publish_provero_deck(suite_result, title="Custom Title")

        assert mock_deck.call_args[0][0] == "Custom Title"

    def test_noop_when_deck_raises(self):
        from provero.flyte.deck import publish_provero_deck

        from provero.core.results import Status, SuiteResult

        suite_result = SuiteResult(
            suite_name="test",
            status=Status.PASS,
        )

        mock_deck = MagicMock(side_effect=RuntimeError("no task context"))
        with patch("flytekit.Deck", mock_deck):
            # Should not raise even though Deck() throws
            publish_provero_deck(suite_result)


class TestProveroSuite:
    def test_defaults(self):
        from provero.flyte.type_transformer import ProveroSuite

        suite = ProveroSuite()
        assert suite.name == "provero_validation"
        assert suite.table_name == "df"
        assert suite.checks == []
        assert suite.config_path is None
        assert suite.suite is None
        assert suite.on_error == "raise"

    def test_inline_checks(self):
        from provero.flyte.type_transformer import ProveroSuite

        from provero.core.compiler import CheckConfig

        checks = [CheckConfig(check_type="not_null", column="id")]
        suite = ProveroSuite(checks=checks, on_error="warn")
        assert len(suite.checks) == 1
        assert suite.checks[0].check_type == "not_null"
        assert suite.on_error == "warn"

    def test_config_path(self):
        from provero.flyte.type_transformer import ProveroSuite

        suite = ProveroSuite(config_path="provero.yaml", suite="orders_daily")
        assert suite.config_path == "provero.yaml"
        assert suite.suite == "orders_daily"


class TestTypeTransformer:
    @pytest.fixture()
    def sample_df(self):
        pd = pytest.importorskip("pandas")
        return pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    def test_validation_passes(self, sample_df):
        from provero.flyte.type_transformer import ProveroSuite, _validate_dataframe

        from provero.core.compiler import CheckConfig

        suite_meta = ProveroSuite(
            checks=[CheckConfig(check_type="not_null", column="id")],
            on_error="raise",
        )
        # Should not raise
        with patch("provero.flyte.deck.publish_provero_deck"):
            _validate_dataframe(sample_df, suite_meta)

    def test_validation_fails_raises(self, sample_df):
        pd = pytest.importorskip("pandas")
        df_with_nulls = pd.DataFrame({"id": [1, None, 3], "name": ["a", "b", "c"]})

        from provero.flyte.type_transformer import ProveroSuite, _validate_dataframe

        from provero.core.compiler import CheckConfig

        suite_meta = ProveroSuite(
            checks=[CheckConfig(check_type="not_null", column="id")],
            on_error="raise",
        )
        with (
            patch("provero.flyte.deck.publish_provero_deck"),
            pytest.raises(ValueError, match="Provero validation failed"),
        ):
            _validate_dataframe(df_with_nulls, suite_meta)

    def test_validation_fails_warns(self, sample_df):
        pd = pytest.importorskip("pandas")
        df_with_nulls = pd.DataFrame({"id": [1, None, 3], "name": ["a", "b", "c"]})

        from provero.flyte.type_transformer import ProveroSuite, _validate_dataframe

        from provero.core.compiler import CheckConfig

        suite_meta = ProveroSuite(
            checks=[CheckConfig(check_type="not_null", column="id")],
            on_error="warn",
        )
        with (
            patch("provero.flyte.deck.publish_provero_deck"),
            pytest.warns(UserWarning, match="Provero validation failed"),
        ):
            _validate_dataframe(df_with_nulls, suite_meta)
