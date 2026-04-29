# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Smoke tests for Flyte Deck integration."""

from __future__ import annotations

from provero.core.results import Severity, Status, SuiteResult
from provero.flyte.deck import ProveroRenderer, publish_provero_deck


def _make_suite_result() -> SuiteResult:
    return SuiteResult(
        suite_name="smoke",
        status=Status.PASS,
        severity=Severity.CRITICAL,
        total=0,
        passed=0,
        failed=0,
        warned=0,
        errored=0,
        quality_score=100.0,
        duration_ms=1,
        checks=[],
    )


def test_renderer_produces_html() -> None:
    html = ProveroRenderer().to_html(_make_suite_result())
    assert "<html" in html.lower() or "<!doctype" in html.lower()
    assert "smoke" in html


def test_publish_deck_is_noop_without_flyte_context() -> None:
    publish_provero_deck(_make_suite_result())


def test_publish_deck_custom_title_is_noop_without_flyte_context() -> None:
    publish_provero_deck(_make_suite_result(), title="Custom")
