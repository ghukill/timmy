"""Tests for the decoupled parallel ingest in ``build_corpus``.

These target ``timmy.analysis.corpus._ingest_records_parallel`` -- the reader-thread /
process-pool / main-thread-inserter pipeline -- through the public ``build_corpus``.
The happy-path reconcile cases live in ``test_corpus_diff.py``; here the focus is the
parallel path's three risk areas:

- **backpressure + ordering** at a scale that actually fills the bounded queue, and
- **failure propagation without deadlock** for both an error raised inside a flatten
  worker and an error raised by the reader (the TDA/rustfs read).

The failure tests run ``build_corpus`` on a side thread and join with a timeout, so a
regression that reintroduces a hang (e.g. the reader parking on a full queue after the
inserter has bailed) fails loudly instead of stalling the suite.

No pytest in the repo yet, so this doubles as a runnable script::

    .venv/bin/python tests/test_corpus_build.py
"""

from __future__ import annotations

import json
import tempfile
import threading
from datetime import datetime, timezone

from timmy.analysis import corpus

# Force the parallel path deterministically regardless of the host's cpu_count
# (build_corpus's default would pick min(8, cpu_count), which is 1 on a single-core CI).
_WORKERS = 2
# Small batches so even a few hundred records cross many batch boundaries and the
# bounded in-flight queue (maxsize = workers * 2) genuinely exerts backpressure.
_BATCH = 25


# --------------------------------------------------------------------------- #
# Minimal fakes (build_corpus only touches these three surfaces)
# --------------------------------------------------------------------------- #
class _Records:
    """``records.read_dicts_iter`` source. ``raise_after`` simulates a mid-stream
    read failure (the rustfs/TDA hiccup the reader thread must surface cleanly)."""

    def __init__(self, records, *, raise_after=None):
        self._records = records
        self._raise_after = raise_after

    def read_dicts_iter(self, table=None, columns=None, run_id=None, **_kw):
        for i, rec in enumerate(self._records):
            if self._raise_after is not None and i >= self._raise_after:
                raise RuntimeError("boom: simulated read failure from rustfs")
            yield dict(rec)


class _Meta:
    def __init__(self, records):
        self._records = records

    @property
    def current_records_count(self):
        return len(self._records)


class BuildDataset:
    """Just enough of ``TIMDEXDataset`` for ``build_corpus``."""

    def __init__(self, records, *, raise_after=None, location="mem://test"):
        self.records = _Records(records, raise_after=raise_after)
        self.metadata = _Meta(records)
        self.location = location


def rec(tid, run_id, offset, *, source="alma", action="index", title="t",
        run_date="2026-06-01", run_type="daily", payload="__valid__"):
    """One current-record dict. ``payload`` defaults to a valid JSON object; pass a
    raw string (e.g. malformed JSON) to drive the worker-error path, or ``None`` for a
    delete (no payload -> counted as skipped)."""
    if payload == "__valid__":
        payload = json.dumps({"title": title, "timdex_record_id": tid})
    return {
        "timdex_record_id": tid,
        "source": source,
        "run_id": run_id,
        "run_record_offset": offset,
        "run_timestamp": datetime(2026, 6, 1, tzinfo=timezone.utc),
        "run_date": run_date,
        "run_type": run_type,
        "action": action,
        "transformed_record": payload,
    }


def _run_with_timeout(fn, timeout=60.0):
    """Run ``fn`` on a daemon thread; return ``(finished, raised)``.

    ``finished`` is False if it was still running at ``timeout`` (i.e. a deadlock),
    in which case the test should fail rather than hang the whole suite.
    """
    box = {}

    def target():
        try:
            fn()
        except BaseException as exc:  # noqa: BLE001 -- capture to assert on it
            box["exc"] = exc

    t = threading.Thread(target=target)
    t.start()
    t.join(timeout)
    return (not t.is_alive()), box.get("exc")


def _composites(analyses_dir):
    con = corpus.open_corpus(analyses_dir)
    try:
        return {r[0] for r in con.execute("select timdex_composite_id from docs").fetchall()}
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def test_parallel_build_backpressure_and_counts():
    """A multi-batch build fills the bounded queue, yet every record lands exactly
    once with the right eav fan-out, and progress is reported monotonically."""
    n = 600
    records = [rec(f"R{i}", "run1", i, title=f"title-{i}") for i in range(n)]

    seen = []
    def on_progress(_phase, done, _total):
        seen.append(done)

    with tempfile.TemporaryDirectory() as d:
        meta = corpus.build_corpus(
            BuildDataset(records), d,
            workers=_WORKERS, batch_size=_BATCH, on_progress=on_progress,
        )
        assert meta["doc_count"] == n, meta
        # Two leaves per record (title + timdex_record_id), so eav == 2 * docs.
        assert meta["eav_count"] == 2 * n, meta
        assert meta["skipped_count"] == 0, meta
        got = _composites(d)
        assert got == {f"R{i}|run1|{i}" for i in range(n)}, "every record present once"

    # Drained in submit order, so the reported running total never goes backwards.
    assert seen == sorted(seen), f"progress not monotonic: {seen}"
    assert seen and seen[-1] == n
    print("  ok: parallel build counts correct under queue backpressure, progress monotonic")


def test_parallel_build_skips_deletes():
    """Records with no payload (deletes) are counted as skipped, not flattened."""
    records = [
        rec("A", "run1", 0, title="A"),
        rec("B", "run1", 1, payload=None),   # delete -> skipped
        rec("C", "run1", 2, title="C"),
    ]
    with tempfile.TemporaryDirectory() as d:
        meta = corpus.build_corpus(
            BuildDataset(records), d, workers=_WORKERS, batch_size=_BATCH
        )
        assert meta["doc_count"] == 2, meta
        assert meta["skipped_count"] == 1, meta
        assert _composites(d) == {"A|run1|0", "C|run1|2"}
    print("  ok: deletes skipped on the parallel path")


def test_parallel_build_worker_error_propagates_no_hang():
    """A flatten worker raising (malformed JSON payload) surfaces as an exception out
    of build_corpus -- promptly, with the half-written corpus cleaned up."""
    records = [rec(f"R{i}", "run1", i) for i in range(60)]
    records[30]["transformed_record"] = "{not valid json"  # json.loads boom in a worker

    with tempfile.TemporaryDirectory() as d:
        finished, exc = _run_with_timeout(
            lambda: corpus.build_corpus(
                BuildDataset(records), d, workers=_WORKERS, batch_size=_BATCH
            )
        )
        assert finished, "build_corpus hung on a worker error (deadlock)"
        assert exc is not None, "expected the worker error to propagate"
        assert isinstance(exc, (ValueError, json.JSONDecodeError)), repr(exc)
        # On failure the build unlinks its .building temp and never publishes a corpus.
        assert not corpus.corpus_exists(d), "no corpus should be left after a failed build"
    print("  ok: worker error propagates, no hang, no leftover corpus")


def test_parallel_build_reader_error_propagates_no_hang():
    """An error raised by the reader (the TDA/rustfs read) surfaces out of
    build_corpus rather than deadlocking the inserter."""
    records = [rec(f"R{i}", "run1", i) for i in range(200)]

    with tempfile.TemporaryDirectory() as d:
        finished, exc = _run_with_timeout(
            lambda: corpus.build_corpus(
                # fail the read partway through the stream
                BuildDataset(records, raise_after=70), d,
                workers=_WORKERS, batch_size=_BATCH,
            )
        )
        assert finished, "build_corpus hung on a reader error (deadlock)"
        assert isinstance(exc, RuntimeError), repr(exc)
        assert "simulated read failure" in str(exc), repr(exc)
        assert not corpus.corpus_exists(d), "no corpus should be left after a failed build"
    print("  ok: reader error propagates, no hang, no leftover corpus")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            print(f"- {name}")
            fn()
    print("\nALL PASSED")
