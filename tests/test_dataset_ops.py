"""Tests for the metadata-rebuild log->progress adapter in ``timmy.dataset_ops``.

``td.metadata.rebuild_dataset_metadata()`` reports nothing through a callback -- it only
logs per-table lines as it goes. ``dataset_ops.rebuild_metadata`` attaches a logging
handler to translate those lines into ``(phase, done, total)`` progress and a structured
per-table result. These tests drive a fake ``td`` whose rebuild emits the exact log
wording the library uses, and assert the adapter:

- counts each data type once (determinate bar reaches done == total),
- captures row counts for built tables and marks missing-parquet tables "skipped", and
- still attaches/detaches its handler (no leak) even when the rebuild raises.

No pytest in the repo yet, so this doubles as a runnable script::

    .venv/bin/python tests/test_dataset_ops.py
"""

from __future__ import annotations

import logging

from timmy import dataset_ops

_TDA_LOGGER = logging.getLogger("timdex_dataset_api")


class _DataType:
    def __init__(self, name: str) -> None:
        self.NAME = name


class _Metadata:
    """A fake ``td.metadata`` whose rebuild logs the library's real per-table lines."""

    metadata_database_path = "s3://bucket/dataset/metadata/metadata.duckdb"

    def __init__(self, *, built: dict[str, int], skipped: set[str], raise_at=None):
        # Order matters: records, embeddings, fulltexts -- the real data type order.
        self.data_type_classes = [
            _DataType("records"),
            _DataType("embeddings"),
            _DataType("fulltexts"),
        ]
        self._built = built
        self._skipped = skipped
        self._raise_at = raise_at

    def rebuild_dataset_metadata(self) -> None:
        for i, cls in enumerate(self.data_type_classes):
            name = cls.NAME
            if self._raise_at is not None and i >= self._raise_at:
                raise RuntimeError("boom: simulated rebuild failure")
            _TDA_LOGGER.debug(f"creating table static_db.main.{name}")
            if name in self._skipped:
                _TDA_LOGGER.warning(
                    f"Could not create metadata table for '{name}' "
                    f"(no parquet data at '/x/{name}'). Skipping."
                )
            else:
                rows = self._built[name]
                _TDA_LOGGER.info(f"'{name}' table created - rows: {rows}, elapsed: 0.5")


class _Conn:
    """Stands in for ``td.conn`` so ``_existing_metadata_counts`` can snapshot rows.

    ``prior`` maps data type -> current row count; a name that's absent simulates a
    missing ``metadata.<name>`` view (the query raises -> counted as ``None``).
    """

    def __init__(self, prior: dict[str, int]) -> None:
        self._prior = prior
        self._pending: int | None = None

    def execute(self, sql: str) -> "_Conn":
        name = sql.rsplit(".", 1)[-1].strip()
        if name not in self._prior:
            raise RuntimeError("Catalog Error: no such view")
        self._pending = self._prior[name]
        return self

    def fetchone(self):
        return (self._pending,)


class _TD:
    def __init__(
        self, metadata: _Metadata, prior: dict[str, int] | None = None
    ) -> None:
        self.metadata = metadata
        self.conn = _Conn(prior or {})


def _record_progress():
    """Return (on_progress, calls) capturing every (phase, done, total) emitted."""
    calls: list[tuple[str, int, int | None]] = []

    def on_progress(phase: str, done: int, total: int | None) -> None:
        calls.append((phase, done, total))

    return on_progress, calls


def test_all_built() -> None:
    td = _TD(
        _Metadata(
            built={"records": 100, "embeddings": 50, "fulltexts": 7}, skipped=set()
        )
    )
    on_progress, calls = _record_progress()

    # Library logs DEBUG too; make sure they reach the handler during the test.
    _TDA_LOGGER.setLevel(logging.DEBUG)
    result = dataset_ops.rebuild_metadata(td, on_progress)

    assert result["table_count"] == 3, result
    by_name = {t["name"]: t for t in result["tables"]}
    assert by_name["records"] == {"name": "records", "status": "built", "rows": 100}
    assert by_name["embeddings"]["rows"] == 50
    assert by_name["fulltexts"]["rows"] == 7
    assert all(t["status"] == "built" for t in result["tables"])

    # Determinate progress: ends at done == total, and never exceeds total.
    assert calls[-1] == ("Refreshed dataset", 3, 3), calls[-1]
    assert all(done <= total for _, done, total in calls)
    assert max(done for _, done, _ in calls) == 3
    print("test_all_built: OK")


def test_skipped_table_counts_and_marked() -> None:
    td = _TD(_Metadata(built={"records": 9, "embeddings": 4}, skipped={"fulltexts"}))
    on_progress, calls = _record_progress()

    _TDA_LOGGER.setLevel(logging.DEBUG)
    result = dataset_ops.rebuild_metadata(td, on_progress)

    by_name = {t["name"]: t for t in result["tables"]}
    assert by_name["fulltexts"] == {
        "name": "fulltexts",
        "status": "skipped",
        "rows": None,
    }, by_name["fulltexts"]
    # A skipped table still advances the bar to total.
    assert calls[-1][1] == 3
    # The skip emitted its own phase line.
    assert any("Skipped fulltexts" in phase for phase, _, _ in calls), calls
    print("test_skipped_table_counts_and_marked: OK")


def test_regression_raises_when_populated_table_goes_empty() -> None:
    # records held 10M rows, but this rebuild "skips" it (a masked read failure).
    # embeddings legitimately had 0 rows and is skipped -- that must NOT trip the guard.
    td = _TD(
        _Metadata(built={"fulltexts": 7}, skipped={"records", "embeddings"}),
        prior={"records": 10_236_655, "embeddings": 0},  # fulltexts view absent -> None
    )
    on_progress, _ = _record_progress()

    _TDA_LOGGER.setLevel(logging.DEBUG)
    raised = None
    try:
        dataset_ops.rebuild_metadata(td, on_progress)
    except dataset_ops.DatasetRebuildError as exc:
        raised = exc

    assert raised is not None, "a populated table going empty must fail the rebuild"
    msg = str(raised)
    # The message must name the regressed table, the count it had, and the clobbered DB.
    assert "records" in msg and "10,236,655" in msg, msg
    assert "metadata.duckdb" in msg, msg
    print("test_regression_raises_when_populated_table_goes_empty: OK")


def test_no_regression_for_legitimately_empty_dataset() -> None:
    # Nothing in the prior metadata (fresh/empty dataset): skipping all tables is fine.
    td = _TD(_Metadata(built={}, skipped={"records", "embeddings", "fulltexts"}))
    on_progress, _ = _record_progress()

    _TDA_LOGGER.setLevel(logging.DEBUG)
    result = dataset_ops.rebuild_metadata(td, on_progress)  # must not raise
    assert all(t["status"] == "skipped" for t in result["tables"]), result
    print("test_no_regression_for_legitimately_empty_dataset: OK")


def test_handler_detached_on_failure() -> None:
    before = list(_TDA_LOGGER.handlers)
    td = _TD(
        _Metadata(
            built={"records": 1, "embeddings": 1, "fulltexts": 1},
            skipped=set(),
            raise_at=1,
        )
    )
    on_progress, calls = _record_progress()

    raised = False
    try:
        dataset_ops.rebuild_metadata(td, on_progress)
    except RuntimeError:
        raised = True

    assert raised, "rebuild should propagate the underlying error"
    # The handler must be removed even when the rebuild blows up (no handler leak).
    assert list(_TDA_LOGGER.handlers) == before, _TDA_LOGGER.handlers
    # Total was wired up before any table work, so the bar had a denominator.
    assert calls and calls[0][2] == 3
    print("test_handler_detached_on_failure: OK")


if __name__ == "__main__":
    test_all_built()
    test_skipped_table_counts_and_marked()
    test_regression_raises_when_populated_table_goes_empty()
    test_no_regression_for_legitimately_empty_dataset()
    test_handler_detached_on_failure()
    print("all tests passed")
