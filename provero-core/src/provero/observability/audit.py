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

"""Structured JSON audit-log observer.

Emits one JSON object per audit event (suite start, suite complete, error) to a
configurable text stream or file. Pure stdlib. Every record is passed through
:func:`provero.observability.redaction.redact` so credentials never reach the
audit sink, satisfying the governance requirement that audit output be safe to
ship to a SIEM.
"""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, TextIO

from provero.observability.redaction import redact

if TYPE_CHECKING:
    from provero.core.compiler import SuiteConfig
    from provero.core.results import CheckResult, SuiteResult


def _config_hash(suite: SuiteConfig) -> str:
    """Return a stable short hash of the suite configuration.

    Uses the pydantic JSON dump with sorted keys so the same configuration
    always hashes to the same value across runs, which lets auditors detect
    when the checks behind a run_id changed.
    """
    payload = suite.model_dump(mode="json")
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


class AuditLogObserver:
    """Write structured JSON audit records for each suite run.

    Args:
        stream: A writable text stream to append JSON lines to. Defaults to
            ``sys.stderr``. Ignored when ``path`` is given.
        path: Optional file path; when set, records are appended to this file
            (one JSON object per line) instead of ``stream``.

    Each record carries: ``event``, ``run_id``, ``suite``, ``config_hash``,
    ``status``, ``counts`` (per-status totals), and ``timestamp``. Records are
    redacted before being written.
    """

    def __init__(self, stream: TextIO | None = None, path: str | Path | None = None) -> None:
        self._path = Path(path) if path is not None else None
        self._stream: TextIO = stream if stream is not None else sys.stderr
        self._run_config_hash: dict[str, str] = {}

    def _write(self, record: dict[str, object]) -> None:
        line = json.dumps(redact(record), sort_keys=True)
        if self._path is not None:
            with self._path.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        else:
            self._stream.write(line + "\n")
            self._stream.flush()

    def on_suite_start(self, suite: SuiteConfig, run_id: str) -> None:
        config_hash = _config_hash(suite)
        self._run_config_hash[run_id] = config_hash
        self._write(
            {
                "event": "suite_start",
                "run_id": run_id,
                "suite": suite.name,
                "config_hash": config_hash,
                "status": None,
                "counts": {},
                "timestamp": _now_iso(),
            }
        )

    def on_check_complete(self, result: CheckResult) -> None:
        # Per-check completion is intentionally not written to the audit log to
        # keep audit volume governance-grade (one record per lifecycle event).
        # Counts are summarized at suite completion.
        return

    def on_suite_complete(self, result: SuiteResult) -> None:
        run_id = result.checks[0].run_id if result.checks else ""
        self._write(
            {
                "event": "suite_complete",
                "run_id": run_id,
                "suite": result.suite_name,
                "config_hash": self._run_config_hash.get(run_id, ""),
                "status": str(result.status),
                "counts": {
                    "total": result.total,
                    "passed": result.passed,
                    "failed": result.failed,
                    "warned": result.warned,
                    "errored": result.errored,
                },
                "quality_score": result.quality_score,
                "timestamp": _now_iso(),
            }
        )

    def on_error(self, suite: SuiteConfig, run_id: str, error: BaseException) -> None:
        self._write(
            {
                "event": "error",
                "run_id": run_id,
                "suite": suite.name,
                "config_hash": self._run_config_hash.get(run_id, _config_hash(suite)),
                "status": "error",
                "counts": {},
                "error": f"{type(error).__name__}: {error}",
                "timestamp": _now_iso(),
            }
        )
