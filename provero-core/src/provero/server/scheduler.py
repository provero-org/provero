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

"""Stdlib threading-based interval scheduler for suites.

Runs a single :class:`~provero.core.compiler.SuiteConfig` periodically and
records each result to a :class:`~provero.store.sqlite.SQLiteStore`. No new
dependency is introduced: the scheduler uses :mod:`threading` only.

SQLite connections are bound to the thread that created them, so the scheduler
opens (and closes) a fresh ``SQLiteStore`` *inside the worker thread* on every
tick rather than sharing one created elsewhere.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provero.core.compiler import SuiteConfig


class SuiteScheduler:
    """Run a suite on a fixed interval, persisting every result to the store.

    Args:
        suite: The suite to execute on each tick.
        db_path: Path to the SQLite result store. A fresh store is opened in
            the worker thread per tick (SQLite connections are thread-bound).
        interval_seconds: Seconds to wait between the end of one run and the
            start of the next.
        run_immediately: When True (default) the first run happens as soon as
            :meth:`start` is called, before the first interval wait.
    """

    def __init__(
        self,
        suite: SuiteConfig,
        db_path: str | Path,
        interval_seconds: float,
        *,
        run_immediately: bool = True,
    ) -> None:
        self._suite = suite
        self._db_path = Path(db_path)
        self._interval = interval_seconds
        self._run_immediately = run_immediately

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._run_count = 0
        self._last_run_id: str | None = None
        self._last_error: str | None = None

    @property
    def run_count(self) -> int:
        """Number of completed (recorded) runs so far."""
        with self._lock:
            return self._run_count

    @property
    def last_run_id(self) -> str | None:
        """The run_id of the most recently recorded run, if any."""
        with self._lock:
            return self._last_run_id

    @property
    def last_error(self) -> str | None:
        """The string of the last exception raised during a tick, if any."""
        with self._lock:
            return self._last_error

    @property
    def is_running(self) -> bool:
        """True while the worker thread is alive."""
        thread = self._thread
        return thread is not None and thread.is_alive()

    def _tick(self) -> None:
        """Execute the suite once and persist the result."""
        from provero.connectors.factory import create_connector
        from provero.core.engine import run_suite
        from provero.store.sqlite import SQLiteStore

        store = SQLiteStore(self._db_path)
        try:
            connector = create_connector(self._suite.source)
            result = run_suite(self._suite, connector)
            run_id = store.save_result(result)
            with self._lock:
                self._run_count += 1
                self._last_run_id = run_id
        finally:
            store.close()

    def _loop(self) -> None:
        if not self._run_immediately and self._stop_event.wait(self._interval):
            return
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception as exc:
                with self._lock:
                    self._last_error = str(exc)
            # wait() returns True if stopped, ending the loop promptly.
            if self._stop_event.wait(self._interval):
                break

    def start(self) -> None:
        """Start the background scheduler thread. Idempotent while running."""
        if self.is_running:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name=f"provero-scheduler-{self._suite.name}",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float | None = 5.0) -> None:
        """Signal the scheduler to stop and join the worker thread."""
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)
        self._thread = None
