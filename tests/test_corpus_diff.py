"""Diff/reconcile tests for the single analysis corpus.

No pytest in the repo yet, so this doubles as a runnable script::

    .venv/bin/python tests/test_corpus_diff.py

The functions are named ``test_*`` so the file works unchanged under pytest later. A
tiny in-memory fake stands in for ``TIMDEXDataset`` -- enough surface for build_corpus
(``records.read_dicts_iter`` + ``metadata.current_records_count``) and update_corpus
(``conn.execute(...).fetchall()`` for the keyset + a run_id-filtered payload read).
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone

from timmy.analysis import corpus, store
from timmy.analysis.scope import make_scope, scoped


# --------------------------------------------------------------------------- #
# In-memory fake dataset
# --------------------------------------------------------------------------- #
class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeRecords:
    def __init__(self, records):
        self._records = records

    def read_dicts_iter(self, table=None, columns=None, run_id=None, **_kw):
        run_ids = set(run_id) if run_id is not None else None
        for rec in self._records:
            if run_ids is not None and rec["run_id"] not in run_ids:
                continue
            yield dict(rec)


class _FakeConn:
    def __init__(self, records):
        self._records = records

    def execute(self, _sql, _params=None):
        # Only the current_records keyset query is issued against this.
        rows = [
            (r["timdex_record_id"], r["run_id"], r["run_record_offset"], r["action"])
            for r in self._records
        ]
        return _Result(rows)


class _FakeMeta:
    def __init__(self, records):
        self._records = records

    @property
    def current_records_count(self):
        return len(self._records)


class FakeDataset:
    """Holds the live ``current_records`` set; swap it to simulate the next ETL state."""

    def __init__(self, records, location="mem://test"):
        self.records = _FakeRecords(records)
        self.conn = _FakeConn(records)
        self.metadata = _FakeMeta(records)
        self.location = location


def rec(tid, run_id, offset, *, source="alma", action="index", title=None, when=None,
        run_date="2026-06-01", run_type="daily"):
    """One current-record dict. ``title=None`` => no payload (e.g. a delete)."""
    payload = json.dumps({"title": title, "timdex_record_id": tid}) if title else None
    return {
        "timdex_record_id": tid,
        "source": source,
        "run_id": run_id,
        "run_record_offset": offset,
        "run_timestamp": when or datetime(2026, 6, 1, tzinfo=timezone.utc),
        "run_date": run_date,
        "run_type": run_type,
        "action": action,
        "transformed_record": payload,
    }


def _composites(analyses_dir):
    con = corpus.open_corpus(analyses_dir)
    try:
        return {r[0] for r in con.execute("select timdex_composite_id from docs").fetchall()}
    finally:
        con.close()


def _title_for(analyses_dir, tid):
    con = corpus.open_corpus(analyses_dir)
    try:
        row = con.execute(
            "select value from eav e join docs d using (timdex_composite_id) "
            "where d.timdex_record_id = ? and e.path = 'title'",
            [tid],
        ).fetchone()
        return row[0] if row else None
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def test_build_then_update_reconciles_all_cases():
    with tempfile.TemporaryDirectory() as d:
        # Initial corpus: A, B, C (each indexed, with a payload).
        initial = [
            rec("A", "run1", 0, title="A v1"),
            rec("B", "run1", 1, title="B v1"),
            rec("C", "run1", 2, title="C v1"),
        ]
        meta = corpus.build_corpus(FakeDataset(initial), d)
        assert meta["doc_count"] == 3, meta
        assert _composites(d) == {"A|run1|0", "B|run1|1", "C|run1|2"}

        # Next ETL state:
        #   A unchanged; B changed (new run2 version); C gone; D new; E delete-action.
        live = [
            rec("A", "run1", 0, title="A v1"),          # unchanged
            rec("B", "run2", 0, title="B v2"),          # changed -> new composite
            rec("D", "run2", 1, title="D v1"),          # brand new
            rec("E", "run2", 2, action="delete"),       # current=delete, no payload
        ]
        corpus.update_corpus(FakeDataset(live), d)

        # B's old version dropped, new one in; C deleted; D added; E never inserted.
        assert _composites(d) == {"A|run1|0", "B|run2|0", "D|run2|1"}, _composites(d)
        assert _title_for(d, "B") == "B v2"      # changed half reflects the new payload
        assert _title_for(d, "C") is None        # deleted
        assert _title_for(d, "E") is None        # delete-action stays absent

        meta = corpus.read_corpus_meta(d)
        assert meta["doc_count"] == 3, meta
        assert meta["report_stale"] is False
        print("  ok: build + update reconcile (unchanged/changed/deleted/new/delete-action)")


def test_update_noop_when_nothing_changed():
    with tempfile.TemporaryDirectory() as d:
        records = [rec("A", "run1", 0, title="A"), rec("B", "run1", 1, title="B")]
        corpus.build_corpus(FakeDataset(records), d)
        before = _composites(d)
        corpus.update_corpus(FakeDataset(list(records)), d)  # identical live set
        assert _composites(d) == before
        assert corpus.read_corpus_meta(d)["doc_count"] == 2
        print("  ok: update is a no-op when the live set is identical")


def test_run_timestamp_persisted_naive_utc():
    with tempfile.TemporaryDirectory() as d:
        when = datetime(2026, 6, 11, 19, 1, 47, tzinfo=timezone.utc)
        corpus.build_corpus(FakeDataset([rec("A", "run1", 0, title="A", when=when)]), d)
        con = corpus.open_corpus(d)
        try:
            ts = con.execute(
                "select run_timestamp from docs where timdex_record_id = 'A'"
            ).fetchone()[0]
        finally:
            con.close()
        assert ts == datetime(2026, 6, 11, 19, 1, 47), ts  # tz dropped, UTC kept
        print("  ok: run_timestamp stored as naive-UTC timestamp")


def test_build_counts_skipped_payloadless():
    with tempfile.TemporaryDirectory() as d:
        records = [
            rec("A", "run1", 0, title="A"),
            rec("B", "run1", 1, action="delete"),  # no payload -> skipped
        ]
        meta = corpus.build_corpus(FakeDataset(records), d)
        assert meta["doc_count"] == 1, meta
        assert meta["skipped_count"] == 1, meta
        print("  ok: build skips payloadless (delete) current records")


def _doc_count_for_field(report, field):
    for row in report:
        if row["field"] == field:
            return row["doc_count"]
    return None


def test_scoped_report_and_query():
    with tempfile.TemporaryDirectory() as d:
        records = [
            rec("A", "run1", 0, source="dspace", title="A"),
            rec("B", "run1", 1, source="alma", title="B"),
            rec("C", "run1", 2, source="dspace", title="C"),
            rec("D", "run1", 3, source="alma", title="D"),
        ]
        corpus.build_corpus(FakeDataset(records), d)

        # Whole-corpus report: title covers all 4 docs.
        full = corpus.field_usage_report(d)
        assert _doc_count_for_field(full, "title") == 4, full

        # Scoped to dspace: title covers only its 2 docs -- a live subset, no new file.
        dspace = corpus.field_usage_report(d, make_scope({"source": ["dspace"]}))
        assert _doc_count_for_field(dspace, "title") == 2, dspace

        # A direct scoped query through the shadow views: store.path_values sees only the
        # subset though its SQL is written against bare `eav`/`docs`.
        con = corpus.open_corpus(d)
        try:
            with scoped(con, make_scope({"source": ["dspace"]})):
                _t, _f, rows = store.path_values(con, "title")
            titles = {r["value"] for r in rows}
        finally:
            con.close()
        assert titles == {"A", "C"}, titles
        print("  ok: scoped report + scoped query restrict to the subset")


def test_scope_report_cached_and_invalidated_on_update():
    with tempfile.TemporaryDirectory() as d:
        records = [
            rec("A", "run1", 0, source="dspace", title="A"),
            rec("B", "run1", 1, source="alma", title="B"),
        ]
        corpus.build_corpus(FakeDataset(records), d)
        scope = make_scope({"source": ["dspace"]})

        corpus.field_usage_report(d, scope)  # computes + caches
        con = corpus.open_corpus(d)
        try:
            n_cached = con.execute(
                "select count(*) from scope_report_cache where scope_key = ?",
                [scope.key()],
            ).fetchone()[0]
        finally:
            con.close()
        assert n_cached == 1, "scoped report should be cached after first view"

        # An update bumps corpus_version -> the stale scoped entry is cleared.
        corpus.update_corpus(FakeDataset(list(records)), d)
        con = corpus.open_corpus(d)
        try:
            keys = [r[0] for r in con.execute(
                "select scope_key from scope_report_cache"
            ).fetchall()]
        finally:
            con.close()
        assert keys == [""], f"only the whole-corpus report should remain, got {keys}"
        print("  ok: scope report cached, and invalidated on update")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            print(f"- {name}")
            fn()
    print("\nALL PASSED")
