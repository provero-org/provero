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

"""API-key authentication for the Provero server.

Authentication is opt-in. The set of allowed keys is resolved, in order, from:

1. The ``api_keys`` argument passed to :func:`provero.server.app.create_app`.
2. The ``PROVERO_API_KEYS`` environment variable (comma-separated).

**Default (no keys configured): all requests are allowed.** This keeps the
server usable out of the box for trusted/local deployments. When at least one
key is configured, every request must present a matching key in the
``X-API-Key`` header; a missing, blank, or wrong key is rejected with HTTP 401.

Comparisons use :func:`hmac.compare_digest` for constant-time matching, and the
caller-supplied key is never echoed back in error responses.
"""

from __future__ import annotations

import hmac
import os
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import Request  # type: ignore[import-not-found]

API_KEY_HEADER = "X-API-Key"
API_KEYS_ENV = "PROVERO_API_KEYS"


def resolve_api_keys(api_keys: Iterable[str] | None) -> tuple[str, ...]:
    """Return the configured, non-blank API keys.

    Keys come from the ``api_keys`` argument when provided, otherwise from the
    ``PROVERO_API_KEYS`` environment variable (comma-separated). Blank entries
    are discarded so an empty string can never authenticate a request.
    """
    if api_keys is None:
        raw = os.environ.get(API_KEYS_ENV, "")
        candidates: Iterable[str] = raw.split(",")
    else:
        candidates = api_keys
    return tuple(k.strip() for k in candidates if k and k.strip())


def _key_is_valid(provided: str | None, allowed: tuple[str, ...]) -> bool:
    """Constant-time check that ``provided`` matches one of ``allowed``."""
    if not provided:
        return False
    # Compare against every allowed key so timing does not reveal how many
    # keys are configured or which prefix matched.
    matched = False
    for key in allowed:
        if hmac.compare_digest(provided, key):
            matched = True
    return matched


def make_api_key_dependency(api_keys: Iterable[str] | None) -> Callable[..., Any]:
    """Build a FastAPI dependency enforcing API-key auth.

    When no keys are configured the returned dependency is a no-op that allows
    every request. When keys are configured it raises HTTP 401 for any request
    whose ``X-API-Key`` header is missing, blank, or does not match.
    """
    from fastapi import HTTPException, Request, status

    allowed = resolve_api_keys(api_keys)

    def _dependency(request: Request) -> None:
        if not allowed:
            return
        provided = request.headers.get(API_KEY_HEADER)
        if not _key_is_valid(provided, allowed):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing API key.",
                headers={"WWW-Authenticate": API_KEY_HEADER},
            )

    # ``from __future__ import annotations`` turns the ``Request`` annotation
    # into the string "Request", which FastAPI's get_type_hints cannot resolve
    # (the symbol is only imported inside this function). Bind the concrete
    # class so FastAPI recognises ``request`` as the ASGI Request, not a query
    # parameter (which would otherwise yield a spurious 422).
    _dependency.__annotations__["request"] = Request
    return _dependency
