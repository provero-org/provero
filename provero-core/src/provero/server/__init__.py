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

"""Provero server subsystem.

Provides an optional FastAPI application (``create_app``), an API-key
authentication dependency, a stdlib threading-based suite scheduler, and the
pydantic request/response models that tie them together.

FastAPI and uvicorn are optional: they are imported lazily inside
:func:`provero.server.app.create_app` and :func:`provero.server.app.serve` so
that importing :mod:`provero` without the ``server`` extra never fails.
Install the extra with ``pip install 'provero[server]'``.
"""

from __future__ import annotations

from provero.server.scheduler import SuiteScheduler

__all__ = ["SuiteScheduler", "create_app", "serve"]


def __getattr__(name: str) -> object:
    # Lazily expose the FastAPI-dependent entry points so that ``import
    # provero.server`` does not require the optional ``server`` extra.
    if name in ("create_app", "serve"):
        from provero.server import app as _app

        return getattr(_app, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
