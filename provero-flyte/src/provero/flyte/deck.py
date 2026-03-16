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

"""Flyte Deck integration for Provero quality reports."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provero.core.results import SuiteResult

logger = logging.getLogger(__name__)


class ProveroRenderer:
    """Renders a Provero SuiteResult as HTML for Flyte Decks."""

    def to_html(self, suite_result: SuiteResult) -> str:
        """Convert a SuiteResult into an HTML report string.

        Delegates to provero's built-in HTML report generator.
        """
        from provero.reporting.html import generate_html_report

        return generate_html_report(suite_result)


def publish_provero_deck(
    suite_result: SuiteResult,
    title: str | None = None,
) -> None:
    """Publish a Provero quality report as a Flyte Deck.

    No-op if flytekit is not installed or if called outside a Flyte
    execution context (e.g. Deck() construction fails).

    Args:
        suite_result: The suite execution result to render.
        title: Optional deck title. Defaults to the suite name.
    """
    try:
        from flytekit import Deck
    except ImportError:
        return

    renderer = ProveroRenderer()
    html = renderer.to_html(suite_result)
    deck_title = title or f"Provero: {suite_result.suite_name}"

    try:
        Deck(deck_title, html)
    except Exception:
        logger.debug("Could not publish Provero deck (not in Flyte task context)")
