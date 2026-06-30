"""Tests for the run-diff engine (``timmy.analysis.run_diff``).

The engine reads two surfaces of a ``TIMDEXDataset``: ``conn`` (DuckDB SQL over
``metadata.records`` -- *all* versions) and ``records.read_dicts_iter`` (payload
reads, narrowable by ``run_id`` + a raw ``where``). Rather than hand-fake the
window logic, the fake here is backed by a **real in-memory DuckDB** holding a
``metadata.records`` table, so the per-record "previous version" selection (the
fiddly part) is exercised for real -- including picking the latest prior when a
record has several, and assembling priors from *different* earlier runs.

No pytest in the repo, so this doubles as a runnable script::

    .venv/bin/python tests/test_run_diff.py
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import duckdb

from timmy.analysis import run_diff

# Three runs in time order; X is the run under analysis.
TS = {
    "old1": datetime(2026, 1, 1, tzinfo=timezone.utc),
    "old2": datetime(2026, 3, 1, tzinfo=timezone.utc),
    "X": datetime(2026, 6, 1, tzinfo=timezone.utc),
}


class _Records:
    """``read_dicts_iter`` over the in-memory ``metadata.records`` table.

    Supports exactly what the engine uses: a ``run_id`` equality filter, a chosen
    ``columns`` list, and a raw ``where`` (the ``run_record_offset in (...)`` the
    prior-version read builds).
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def read_dicts_iter(self, table="records", run_id=None, columns=None,
                        where=None, **filters):
        cols = columns or [
            "timdex_record_id", "run_id", "run_record_offset",
            "action", "transformed_record",
        ]
        clauses, params = [], []
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        for key, val in filters.items():
            clauses.append(f"{key} = ?")
            params.append(val)
        if where:
            clauses.append(f"({where})")
        sql = f"select {', '.join(cols)} from metadata.records"  # noqa: S608 -- fixed cols
        if clauses:
            sql += " where " + " and ".join(clauses)
        for row in self.conn.execute(sql, params).fetchall():
            yield dict(zip(cols, row, strict=True))


class FakeDataset:
    """Just enough of ``TIMDEXDataset`` for the run-diff: ``conn`` + ``records``."""

    def __init__(self, versions: list[dict]):
        self.conn = duckdb.connect(":memory:")
        self.conn.execute("create schema metadata")
        self.conn.execute(
            "create table metadata.records("
            "timdex_record_id text, source text, run_type text, run_date date, "
            "run_timestamp timestamp, action text, run_id text, "
            "run_record_offset bigint, transformed_record text)"
        )
        self.conn.executemany(
            "insert into metadata.records values (?,?,?,?,?,?,?,?,?)",
            [
                (
                    v["tid"], "alma", "daily", TS[v["run"]].date(),
                    TS[v["run"]], v.get("action", "index"), v["run"],
                    v["off"],
                    None if v.get("payload") is None else json.dumps(v["payload"]),
                )
                for v in versions
            ],
        )
        self.records = _Records(self.conn)


def ver(tid, run, off, payload="__none__", action="index"):
    """One record version row. ``payload`` defaults to no-payload (a delete)."""
    row = {"tid": tid, "run": run, "off": off, "action": action}
    row["payload"] = None if payload == "__none__" else payload
    return row


def _scenario() -> FakeDataset:
    """Six records exercising every class + per-record prior assembly.

    - R1: modified (title changed), prior from old1
    - R2: modified (field 'extra' added), prior from old2  (a *different* prior run)
    - R3: added (no prior version at all)
    - R4: deleted by X (had a prior in old1)
    - R5: unchanged -- has TWO priors (old1, old2); the latest (old2) matches X,
          so a correct "newest prior" pick yields unchanged (old1 would be modified)
    - R6: modified (field 'note' removed), prior from old1
    """
    return FakeDataset([
        # old1
        ver("R1", "old1", 0, {"title": "A", "subjects": ["x"]}),
        ver("R4", "old1", 1, {"title": "D"}),
        ver("R5", "old1", 2, {"title": "E"}),
        ver("R6", "old1", 3, {"title": "F", "note": "keep"}),
        # old2
        ver("R2", "old2", 0, {"title": "B"}),
        ver("R5", "old2", 1, {"title": "E2"}),
        # X (the run under analysis)
        ver("R1", "X", 0, {"title": "A2", "subjects": ["x"]}),
        ver("R2", "X", 1, {"title": "B", "extra": "new"}),
        ver("R3", "X", 2, {"title": "C"}),
        ver("R4", "X", 3, action="delete"),
        ver("R5", "X", 4, {"title": "E2"}),
        ver("R6", "X", 5, {"title": "F"}),
    ])


def test_record_level_classification():
    """Each record lands in the right class, and the new-vs-modified split (the
    thing the corpus can't do) is exact."""
    report = run_diff.diff_run(_scenario(), "X")
    recs = report["records"]
    assert recs == {
        "touched": 6, "added": 1, "modified": 3, "unchanged": 1, "deleted": 1
    }, recs
    assert report["baseline"] == "previous"
    # R3 is the only record with no prior version.
    assert report["records_read"]["without_prior"] == 1, report["records_read"]
    print("  ok: record-level classification (added/modified/unchanged/deleted)")


def test_prior_is_newest_before_run():
    """R5 has two priors; the engine must pick old2 (newest before X), making it
    unchanged. Picking old1 would wrongly mark it modified."""
    report = run_diff.diff_run(_scenario(), "X")
    assert "R5" not in report["examples"]["modified"], report["examples"]
    assert report["records"]["unchanged"] == 1
    print("  ok: previous version is the newest one strictly before the run")


def test_field_level_aggregation():
    """Field-level rollup names the right paths and the right kind of change."""
    report = run_diff.diff_run(_scenario(), "X")
    by_path = {f["path"]: f for f in report["fields"]}
    assert by_path["title"]["changed_in"] == 1, by_path["title"]   # R1
    assert by_path["extra"]["added_in"] == 1, by_path["extra"]     # R2
    assert by_path["note"]["removed_in"] == 1, by_path["note"]     # R6
    # A new record's fields (R3) must NOT show up as field additions.
    assert "subjects[]" not in by_path or by_path["subjects[]"]["added_in"] == 0
    print("  ok: field-level added/changed/removed attributed to the right paths")


def test_examples_surface_real_ids():
    """Example ids per class point at the actual records."""
    report = run_diff.diff_run(_scenario(), "X")
    ex = report["examples"]
    assert ex["added"] == ["R3"], ex
    assert ex["deleted"] == ["R4"], ex
    assert set(ex["modified"]) == {"R1", "R2", "R6"}, ex
    print("  ok: example ids surface the real records per change class")


def test_records_detail_rows():
    """include_records=True yields one row per changed record, with prior-version
    key/date and the paths it changed -- and excludes unchanged records."""
    report = run_diff.diff_run(_scenario(), "X", include_records=True)
    detail = {r["record_id"]: r for r in report["records_detail"]}
    # Unchanged R5 is absent; the five changed records are present.
    assert set(detail) == {"R1", "R2", "R3", "R4", "R6"}, set(detail)

    # R1 modified, prior from old2? No -- R1's only prior is old1.
    assert detail["R1"]["status"] == "modified"
    assert detail["R1"]["prev_run_id"] == "old1"
    assert detail["R1"]["prev_run_date"] == "2026-01-01"
    assert detail["R1"]["changed_paths"] == ["title"], detail["R1"]

    # R2's prior is from a *different* run (old2) than R1's (old1).
    assert detail["R2"]["prev_run_id"] == "old2"
    assert detail["R2"]["changed_paths"] == ["extra"]

    # R3 added: no prior key/date; R4 deleted but has a prior (old1).
    assert detail["R3"]["status"] == "added"
    assert detail["R3"]["prev_run_id"] is None
    assert detail["R4"]["status"] == "deleted"
    assert detail["R4"]["prev_run_id"] == "old1"

    # The default report omits the (potentially huge) detail list.
    assert "records_detail" not in run_diff.diff_run(_scenario(), "X")
    print("  ok: per-record detail rows (status, prior key/date, changed paths)")


def test_run_meta_counts_and_unknown():
    """run_meta mirrors the run's action breakdown; unknown run_id -> None / raises."""
    ds = _scenario()
    meta = run_diff.run_meta(ds, "X")
    assert meta["record_count"] == 6 and meta["index_count"] == 5
    assert meta["delete_count"] == 1, meta
    assert run_diff.run_meta(ds, "nope") is None
    try:
        run_diff.diff_run(ds, "nope")
    except ValueError as exc:
        assert "nope" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected ValueError for unknown run")
    print("  ok: run_meta counts correct; unknown run handled")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            print(f"- {name}")
            fn()
    print("\nALL PASSED")
