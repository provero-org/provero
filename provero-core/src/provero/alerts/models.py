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

"""Alert configuration models."""

from __future__ import annotations

from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, Field, field_validator

AlertTrigger = Literal["on_failure", "on_success", "always"]


class AlertConfig(BaseModel):
    """Configuration for an alert destination.

    The ``url`` is validated for shape (must be an ``http``/``https`` URL with
    a host) and the ``trigger`` is constrained to the supported values so that
    misconfiguration is caught at config-load time instead of crashing the
    sender at delivery time.  ``${ENV_VAR}`` placeholders are still permitted
    in the URL and are resolved later by the sender.
    """

    type: str = "webhook"
    url: str = ""
    trigger: AlertTrigger = "on_failure"
    headers: dict[str, str] = Field(default_factory=dict)

    @field_validator("url")
    @classmethod
    def _validate_url(cls, value: str) -> str:
        """Validate the webhook URL shape, allowing ``${ENV_VAR}`` templates.

        An empty string is accepted (an unconfigured destination is a no-op
        rather than a parse error).  Any non-empty value must parse as an
        ``http``/``https`` URL with a host component.  Substrings of the form
        ``${VAR}`` are masked before validation so that env-var templates such
        as ``${WEBHOOK_HOST}`` do not get rejected for not looking like a URL.
        """
        if not value:
            return value

        import re

        masked = re.sub(r"\$\{[^}]+\}", "envvar", value)
        parsed = urlsplit(masked)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            msg = (
                f"Invalid alert URL {value!r}: must be an http(s) URL with a host "
                "(env-var templates like ${VAR} are allowed)"
            )
            raise ValueError(msg)
        return value
