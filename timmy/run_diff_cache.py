"""In-process cache + background runner for run-diffs, keyed by ``run_id``.

A run-diff (:func:`timmy.analysis.run_diff.diff_run`) reads and flattens every
payload a run touched plus each record's prior version -- a second or two for a
small daily run, much longer for a large full ingest. So the web app computes each
run's diff once in a daemon thread, streams its phase to a progress page, and
caches the finished report for the life of the process. A second visit to the same
run is instant.

This mirrors :mod:`timmy.corpus_job`'s "thread + poll" approach, but keyed by
``run_id``: run-diffs are read-only and independently cacheable, so many can be
held at once (unlike the single global corpus writer). The CLI does not use this --
it computes synchronously and exits, so caching buys it nothing.
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Callable

# compute(on_progress) -> report dict. The closure owns acquiring dataset_lock
# around the reads; the cache only supplies the phase callback and stores the result.
Compute = Callable[[Callable[[str], None]], dict]


class _Job:
    """One run's diff: its live progress while computing, then its cached report."""

    def __init__(self) -> None:
        self.phase: str = "starting"
        self.started_at: datetime = datetime.now(timezone.utc)
        self.finished_at: datetime | None = None
        self.error: str | None = None
        self.report: dict[str, Any] | None = None

    def running(self) -> bool:
        return self.finished_at is None

    def snapshot(self) -> dict[str, Any]:
        end = self.finished_at or datetime.now(timezone.utc)
        return {
            "running": self.running(),
            "phase": self.phase,
            "error": self.error,
            "finished": self.finished_at is not None,
            "ready": self.report is not None,
            "elapsed": round((end - self.started_at).total_seconds(), 1),
        }


class RunDiffCache:
    """Process-wide store of run-diffs, each computed once per ``run_id``."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, _Job] = {}

    def get(self, run_id: str) -> dict[str, Any] | None:
        """The finished report for ``run_id``, or ``None`` if not ready."""
        with self._lock:
            job = self._jobs.get(run_id)
            return job.report if job and job.report is not None else None

    def status(self, run_id: str) -> dict[str, Any] | None:
        """A snapshot of ``run_id``'s job (running/phase/error/ready), or ``None``."""
        with self._lock:
            job = self._jobs.get(run_id)
            return job.snapshot() if job else None

    def ensure(self, run_id: str, compute: Compute) -> dict[str, Any]:
        """Start computing ``run_id`` in the background if it isn't already.

        Idempotent: concurrent visits to a still-computing run share one job, and a
        run that already has a report is left untouched. A run whose previous attempt
        errored out is retried. Returns the (current) job snapshot.
        """
        with self._lock:
            job = self._jobs.get(run_id)
            if job and (job.running() or job.report is not None):
                return job.snapshot()
            job = _Job()
            self._jobs[run_id] = job
        threading.Thread(
            target=self._run, args=(job, compute), daemon=True
        ).start()
        return job.snapshot()

    def compute_now(self, run_id: str, compute: Compute) -> dict[str, Any]:
        """Return ``run_id``'s report, computing it synchronously if not cached.

        For the JSON/scripting path, which wants the data in-hand rather than a
        progress page. Populates the cache so a later page view is instant.
        """
        cached = self.get(run_id)
        if cached is not None:
            return cached
        report = compute(lambda _phase: None)
        job = _Job()
        job.report = report
        job.finished_at = datetime.now(timezone.utc)
        with self._lock:
            self._jobs[run_id] = job
        return report

    def _run(self, job: _Job, compute: Compute) -> None:
        def on_progress(phase: str) -> None:
            job.phase = phase

        try:
            job.report = compute(on_progress)
        except Exception as exc:  # noqa: BLE001 -- surfaced to the progress page
            job.error = str(exc)
        finally:
            job.finished_at = datetime.now(timezone.utc)


# Process-wide singleton.
run_diff_cache = RunDiffCache()
