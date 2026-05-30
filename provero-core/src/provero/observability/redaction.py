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

"""Secret redaction for anything that might be logged.

Pure-stdlib regex helpers that strip credentials from strings and dictionaries
before they reach an audit log, a span attribute, or a metric label. The goal
is defense-in-depth: even if a connection string or a header value carrying a
token is handed to an observer, the secret never lands in durable output.

Redaction is best-effort and conservative. It targets the common shapes
(URL userinfo passwords, ``key=value`` secret assignments, bearer tokens, and
sensitively named dict keys) without attempting to parse arbitrary formats.
"""

from __future__ import annotations

import re
from itertools import pairwise
from typing import Any

REDACTED = "***REDACTED***"

# Exact sensitive key names (lower-cased). A dict key is sensitive when any of
# its tokens (split on separators / camelCase boundaries) equals one of these.
# Using exact-token matching avoids false positives such as ``passed`` matching
# ``pass`` or ``access_count`` matching ``access_key``.
_SENSITIVE_TOKENS = frozenset(
    {
        "pass",
        "passwd",
        "password",
        "secret",
        "token",
        "apikey",
        "accesskey",
        "privatekey",
        "clientsecret",
        "auth",
        "authorization",
        "credential",
        "credentials",
        "sastoken",
        "connectionstring",
    }
)

# Split a key into tokens on separators and camelCase boundaries.
_TOKEN_SPLIT_RE = re.compile(r"[^a-zA-Z0-9]+|(?<=[a-z0-9])(?=[A-Z])")

# Password inside a URL userinfo component, e.g. ``user:pa55@host``. We keep the
# scheme, username, and host but blank the password.
_URL_USERINFO_RE = re.compile(
    r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.\-]*://)"
    r"(?P<user>[^:/?#@\s]+):"
    r"(?P<pwd>[^@/?#\s]+)"
    r"(?P<at>@)"
)

# ``password=...`` / ``token: ...`` style assignments in arbitrary strings.
# Captures the key + separator and replaces the value up to a delimiter.
_ASSIGNMENT_RE = re.compile(
    r"(?i)(?P<key>pass(?:word|wd)?|secret|token|api[_-]?key|access[_-]?key|"
    r"private[_-]?key|client[_-]?secret|authorization|credential|sas[_-]?token)"
    r"(?P<sep>\s*[=:]\s*)"
    r"(?P<val>\"[^\"]*\"|'[^']*'|[^\s;,&]+)"
)

# Bearer / token authorization header values, e.g. ``Bearer eyJ...``.
_BEARER_RE = re.compile(r"(?i)\b(?P<kind>bearer|token)\s+(?P<val>[A-Za-z0-9._\-+/=]{8,})")


def _redact_assignment(match: re.Match[str]) -> str:
    return f"{match.group('key')}{match.group('sep')}{REDACTED}"


def redact_string(value: str) -> str:
    """Return ``value`` with recognizable secrets replaced by a marker.

    Handles URL userinfo passwords, ``key=value`` secret assignments, and
    bearer/token authorization values. Non-secret text is returned unchanged.
    """
    redacted = _URL_USERINFO_RE.sub(
        lambda m: f"{m.group('scheme')}{m.group('user')}:{REDACTED}{m.group('at')}",
        value,
    )
    redacted = _ASSIGNMENT_RE.sub(_redact_assignment, redacted)
    redacted = _BEARER_RE.sub(lambda m: f"{m.group('kind')} {REDACTED}", redacted)
    return redacted


def is_sensitive_key(key: str) -> bool:
    """Return True if a dict key name indicates its value is a secret.

    The key is split into tokens on separators and camelCase boundaries. The
    key is sensitive if any single token, or any run of adjacent tokens joined
    together, matches a known sensitive name. This recognizes ``password``,
    ``api_key``, ``apiKey``, ``access-key`` and ``client_secret`` while not
    flagging unrelated names such as ``passed`` or ``access_count``.
    """
    raw_tokens = [t.lower() for t in _TOKEN_SPLIT_RE.split(key) if t]
    if not raw_tokens:
        return False
    # Single tokens (handles "password", "secret", "token", "apikey").
    if any(tok in _SENSITIVE_TOKENS for tok in raw_tokens):
        return True
    # Adjacent token pairs joined (handles "api_key", "client_secret", ...).
    return any(first + second in _SENSITIVE_TOKENS for first, second in pairwise(raw_tokens))


def redact(value: Any) -> Any:
    """Recursively redact secrets from a string, dict, or list.

    - Strings are passed through :func:`redact_string`.
    - Dict values under sensitive keys are fully replaced; other values are
      redacted recursively.
    - Lists/tuples are redacted element-wise.
    - Other types are returned unchanged.
    """
    if isinstance(value, str):
        return redact_string(value)
    if isinstance(value, dict):
        result: dict[Any, Any] = {}
        for key, val in value.items():
            if isinstance(key, str) and is_sensitive_key(key):
                result[key] = REDACTED
            else:
                result[key] = redact(val)
        return result
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact(item) for item in value)
    return value
