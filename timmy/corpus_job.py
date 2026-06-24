"""A single, in-process background job for building/updating the corpus.

Build and update can run for minutes. Rather than reach for a task queue, the web app
runs one at a time in a daemon thread and exposes its progress as an in-memory snapshot
the progress page polls. This is intentionally the "thread + poll" sweet spot, not a
durable job system: if the Flask process dies mid-job the snapshot is lost (a dead
*build* just leaves no corpus, since build writes a ``.building`` temp file and only
renames it into place on success).

Only one job may run at a time (there is one corpus, one writer). :data:`corpus_job` is
the process-wide singleton; routes call :meth:`CorpusJob.start` with a ``runner`` that
does the actual work (and its own ``dataset_lock`` acquisition) and reports progress
through the callback passed to it.
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Callable

# runner(on_progress) -> corpus_meta dict. It owns acquiring dataset_lock around the
# TDA reads; the job only supplies the progress callback and captures the result/errors.
Runner = Callable[[Callable[[str, int, "int | None"], None]], dict]


class CorpusJob:
    """Tracks at most one running build/update and its live progress."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self.kind: str | None = None  # "build" | "update"
        self.phase: str | None = None
        self.done: int = 0
        self.total: int | None = None
        self.started_at: datetime | None = None
        self.finished_at: datetime | None = None
        self.error: str | None = None
        self.result: dict[str, Any] | None = None

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self, kind: str, runner: Runner) -> None:
        """Spawn the job. Raises ``RuntimeError`` if one is already running."""
        with self._lock:
            if self.is_running():
                raise RuntimeError("A corpus job is already running.")
            self.kind = kind
            self.phase = "starting"
            self.done = 0
            self.total = None
            self.started_at = datetime.now(timezone.utc)
            self.finished_at = None
            self.error = None
            self.result = None
            self._thread = threading.Thread(
                target=self._run, args=(runner,), daemon=True
            )
            self._thread.start()

    def _run(self, runner: Runner) -> None:
        try:
            self.result = runner(self._on_progress)
        except Exception as exc:  # noqa: BLE001 -- surfaced to the progress page
            self.error = str(exc)
        finally:
            self.finished_at = datetime.now(timezone.utc)

    def _on_progress(self, phase: str, done: int, total: int | None) -> None:
        self.phase = phase
        self.done = done
        self.total = total

    def snapshot(self) -> dict[str, Any]:
        """A JSON-able view of the current/last job for the progress endpoint."""
        elapsed = None
        if self.started_at:
            end = self.finished_at or datetime.now(timezone.utc)
            elapsed = round((end - self.started_at).total_seconds(), 1)
        return {
            "running": self.is_running(),
            "kind": self.kind,
            "phase": self.phase,
            "done": self.done,
            "total": self.total,
            "error": self.error,
            "finished": self.finished_at is not None,
            "elapsed": elapsed,
            "result": self.result,
        }


# Process-wide singleton (one corpus, one writer).
corpus_job = CorpusJob()
