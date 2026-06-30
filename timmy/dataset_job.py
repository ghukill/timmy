"""A single, in-process background job for dataset maintenance actions.

The dataset management space runs operations that mutate the underlying
``TIMDEXDataset`` itself (rebuilding the static metadata database being the first).
These can run for minutes, so -- exactly like :mod:`timmy.corpus_job` -- the web app
runs one at a time in a daemon thread and exposes its progress as an in-memory
snapshot the progress page polls. This is the "thread + poll" sweet spot, not a
durable job system: if the Flask process dies mid-job the snapshot is lost (the
underlying action is responsible for leaving the dataset in a sane state -- e.g.
``rebuild_dataset_metadata`` builds a temp DB and only copies it into place at the end).

Where :class:`~timmy.corpus_job.CorpusJob` is specific to the one corpus writer, this
job is generic over the *action*: ``kind`` is the action's machine name, ``title`` its
human label, and ``result`` whatever dict the runner returns. Only one may run at a
time (the actions take ``dataset_lock``); :data:`dataset_job` is the process-wide
singleton.
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Callable

# runner(on_progress) -> result dict. It owns acquiring dataset_lock around the work;
# the job only supplies the progress callback and captures the result/errors.
Runner = Callable[[Callable[[str, int, "int | None"], None]], dict]


class DatasetJob:
    """Tracks at most one running dataset maintenance action and its live progress."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self.kind: str | None = None  # action machine name, e.g. "metadata_rebuild"
        self.title: str | None = None  # human label, e.g. "Rebuild dataset metadata"
        self.phase: str | None = None
        self.done: int = 0
        self.total: int | None = None
        self.started_at: datetime | None = None
        self.finished_at: datetime | None = None
        self.error: str | None = None
        self.result: dict[str, Any] | None = None

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self, kind: str, title: str, runner: Runner) -> None:
        """Spawn the job. Raises ``RuntimeError`` if one is already running."""
        with self._lock:
            if self.is_running():
                raise RuntimeError("A dataset job is already running.")
            self.kind = kind
            self.title = title
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
            "title": self.title,
            "phase": self.phase,
            "done": self.done,
            "total": self.total,
            "error": self.error,
            "finished": self.finished_at is not None,
            "elapsed": elapsed,
            "result": self.result,
        }


# Process-wide singleton (one dataset writer at a time).
dataset_job = DatasetJob()
