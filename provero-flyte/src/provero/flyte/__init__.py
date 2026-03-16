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

"""Provero Flyte plugin package."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provero.flyte.deck import ProveroRenderer, publish_provero_deck
    from provero.flyte.type_transformer import ProveroSuite


def __getattr__(name: str):
    if name == "ProveroRenderer":
        from provero.flyte.deck import ProveroRenderer

        return ProveroRenderer
    if name == "publish_provero_deck":
        from provero.flyte.deck import publish_provero_deck

        return publish_provero_deck
    if name == "ProveroSuite":
        from provero.flyte.type_transformer import ProveroSuite

        return ProveroSuite
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "ProveroRenderer",
    "ProveroSuite",
    "publish_provero_deck",
]
