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

"""Smoke tests for the Flyte plugin public API and lazy loading."""

from __future__ import annotations

import pytest

import provero.flyte as flyte_pkg


def test_public_api_exposes_expected_symbols() -> None:
    assert set(flyte_pkg.__all__) == {
        "ProveroRenderer",
        "ProveroSuite",
        "publish_provero_deck",
    }


def test_lazy_import_renderer() -> None:
    from provero.flyte.deck import ProveroRenderer as Direct

    assert flyte_pkg.ProveroRenderer is Direct


def test_lazy_import_publish_deck() -> None:
    from provero.flyte.deck import publish_provero_deck as direct

    assert flyte_pkg.publish_provero_deck is direct


def test_lazy_import_unknown_attr_raises() -> None:
    with pytest.raises(AttributeError, match="no attribute 'bogus'"):
        flyte_pkg.bogus  # noqa: B018
