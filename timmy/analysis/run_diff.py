"""Run Analysis: diff a single ETL run against the prior state of its records.

A *run-diff* is a different artifact from the corpus. The corpus is
cross-sectional and **current-only**; a run-diff is longitudinal and
event-oriented, computed **live from the dataset on demand**. It never touches
the corpus, so it is immune to corpus staleness and works even when no corpus
exists.

Given one ``run_id`` (call it run X), it assembles two states of the *same
records* and diffs them on ``timdex_record_id``:

- **Rv** -- the versions run X actually wrote (its ``index``/``delete`` actions).
- **Rp** -- each touched record's *previous* version: the newest version whose
  ``run_timestamp`` is earlier than X's. Assembled **per record**, so different
  records' prior versions come from different earlier runs (a record last touched
  a week ago and one last touched a year ago each contribute their own neighbour).
  A record that is brand new at X has no prior version at all.

From those two states it answers the two questions a run raises:

- **record-level** -- how many records did X *add* / *modify* / *delete* (and how
  many did it re-index *unchanged*)? The add-vs-modify split is only knowable
  *because* Rp exists; the run's own action counts conflate new and updated under
  ``index``.
- **field-level** -- which paths did X *add* / *change* / *remove*, and across how
  many records? Computed over records present in *both* states (the genuine
  field-edit population); whole-record adds and deletes are their own category, so
  a new record's fields don't masquerade as field additions.

The only input is a ``run_id``. The comparison baseline is always "the previous
version of each record" -- so selecting the most recent run still shows real work
(what that run did relative to the state before it), not an empty "these are
current" diff.

This module is Flask-free and takes a ``TIMDEXDataset``-shaped object exposing
``conn`` (a DuckDB connection over ``metadata.records`` -- all versions) and
``records.read_dicts_iter`` (payload reads with a raw ``where``). The CLI passes
its own dataset; the web app passes its shared, locked one.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Any

from timmy.analysis.flatten import flatten

if TYPE_CHECKING:
    from timdex_dataset_api import TIMDEXDataset

# A leaf, keyed by its exact (index-preserving) location, carrying enough to tell
# whether it changed: ``path_indexed -> (collapsed_path, value, value_type)``. The
# collapsed ``path`` (array indices -> ``[]``) is what field-level results report;
# ``path_indexed`` is the comparison key so a value move within an array is seen.
RecordFields = dict[str, "tuple[str, str | None, str]"]

# Default number of example record ids surfaced per change class (added/modified/
# deleted), for drill-down without dumping thousands of ids.
DEFAULT_EXAMPLES = 10

# Cap on how many run_record_offsets go into one ``where ... in (...)`` read of a
# prior run, so a popular prior run doesn't build one enormous predicate.
_OFFSET_CHUNK = 1000


def run_meta(dataset: TIMDEXDataset, run_id: str) -> dict[str, Any] | None:
    """Summarize run ``run_id`` from ``metadata.records``, or ``None`` if unknown.

    Metadata-only (no payload reads). ``record_count``/``index_count``/
    ``delete_count`` mirror what the sources runs listing shows, so the run-diff
    header lines up with the run the user picked.
    """
    row = dataset.conn.execute(
        "select "
        "  any_value(source) as source, "
        "  any_value(run_type) as run_type, "
        "  cast(any_value(run_date) as date)::varchar as run_date, "
        "  cast(max(run_timestamp) as timestamp)::varchar as run_timestamp, "
        "  count(*) as record_count, "
        "  count(*) filter (where action = 'index') as index_count, "
        "  count(*) filter (where action = 'delete') as delete_count "
        "from metadata.records where run_id = ?",
        [run_id],
    ).fetchone()
    if row is None or row[4] == 0:
        return None
    keys = (
        "source",
        "run_type",
        "run_date",
        "run_timestamp",
        "record_count",
        "index_count",
        "delete_count",
    )
    return dict(zip(keys, row, strict=True))


def _touched(dataset: TIMDEXDataset, run_id: str) -> dict[str, tuple[str, int]]:
    """``timdex_record_id -> (action, run_record_offset)`` for run X's records.

    The authoritative set of records the diff is about (including deletes, which
    carry no payload). The offset pins X's *own* version -- the right-hand side of
    a per-record diff. If a record appears more than once in the run, the last-seen
    row wins -- a rare edge that doesn't affect the headline split.
    """
    rows = dataset.conn.execute(
        "select timdex_record_id, action, run_record_offset "
        "from metadata.records where run_id = ?",
        [run_id],
    ).fetchall()
    return {tid: (action, off) for tid, action, off in rows}


def _prior_meta(dataset: TIMDEXDataset, run_id: str) -> dict[str, dict[str, Any]]:
    """``timdex_record_id -> {run_id, offset, run_date}`` for the prior version.

    For each record X touched, the single newest version strictly older than X
    (by ``run_timestamp``, tie-broken by run_id/offset for determinism). Records
    with no earlier version -- brand new at X -- are simply absent. Metadata-only;
    the heavy payload read happens later, against exactly these keys. ``run_date``
    feeds the table's "previous run" column; the key pins the diff's left side.
    """
    rows = dataset.conn.execute(
        """
        with
        x as (select max(run_timestamp) as ts from metadata.records where run_id = ?),
        touched as (select distinct timdex_record_id from metadata.records where run_id = ?),
        prior as (
            select
                r.timdex_record_id,
                r.run_id,
                r.run_record_offset,
                cast(r.run_date as date)::varchar as run_date,
                row_number() over (
                    partition by r.timdex_record_id
                    order by r.run_timestamp desc nulls last,
                             r.run_id desc nulls last,
                             r.run_record_offset desc nulls last
                ) as rn
            from metadata.records r
            join touched t using (timdex_record_id), x
            where r.run_timestamp < x.ts
        )
        select timdex_record_id, run_id, run_record_offset, run_date
        from prior where rn = 1
        """,
        [run_id, run_id],
    ).fetchall()
    return {
        tid: {"run_id": prid, "offset": off, "run_date": rd}
        for tid, prid, off, rd in rows
    }


def _fields(payload: Mapping[str, Any]) -> RecordFields:
    """Flatten a parsed transformed record into the comparison map."""
    return {
        row.path_indexed: (row.path, row.value, row.value_type)
        for row in flatten(payload)
    }


def _chunks(items: list[int], n: int) -> Iterator[list[int]]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _read_run_fields(dataset: TIMDEXDataset, run_id: str) -> dict[str, RecordFields]:
    """Read + flatten run X's own versions: ``timdex_record_id -> RecordFields``.

    One filtered payload read of the whole run. Deletes (no ``transformed_record``)
    contribute no entry -- absence of content *is* the deletion, so the field-level
    diff sees every prior leaf as removed.
    """
    out: dict[str, RecordFields] = {}
    for rec in dataset.records.read_dicts_iter(
        table="records",
        run_id=run_id,
        columns=[
            "timdex_record_id",
            "run_record_offset",
            "action",
            "transformed_record",
        ],
    ):
        payload = rec.get("transformed_record")
        if payload:
            out[rec["timdex_record_id"]] = _fields(json.loads(payload))
    return out


def _read_prior_fields(
    dataset: TIMDEXDataset, prior_meta: dict[str, dict[str, Any]]
) -> dict[str, RecordFields]:
    """Read + flatten the assembled prior versions: ``timdex_record_id -> RecordFields``.

    The prior versions are scattered across many earlier runs, so reads are grouped
    by ``run_id`` and narrowed to just the needed ``run_record_offset``s -- exactly
    the prior payloads, nothing more (a prior full-ingest run isn't dragged in
    wholesale). A prior version that was itself a delete yields no content, which
    correctly reads as "no prior state" for that record.
    """
    by_run: dict[str, list[int]] = defaultdict(list)
    for meta in prior_meta.values():
        by_run[meta["run_id"]].append(meta["offset"])

    out: dict[str, RecordFields] = {}
    for prev_run_id, offsets in by_run.items():
        for chunk in _chunks(offsets, _OFFSET_CHUNK):
            in_list = ", ".join(str(o) for o in chunk)
            for rec in dataset.records.read_dicts_iter(
                table="records",
                run_id=prev_run_id,
                columns=["timdex_record_id", "run_record_offset", "transformed_record"],
                where=f"run_record_offset in ({in_list})",
            ):
                payload = rec.get("transformed_record")
                if payload:
                    out[rec["timdex_record_id"]] = _fields(json.loads(payload))
    return out


def _classify_and_aggregate(
    touched: dict[str, tuple[str, int]],
    prior_meta: dict[str, dict[str, Any]],
    rv_fields: dict[str, RecordFields],
    prev_fields: dict[str, RecordFields],
    *,
    examples: int,
) -> dict[str, Any]:
    """The pure diff: classify each touched record and roll up per-path changes.

    Record classes:
    - **deleted**  -- X deleted it, or its content went away (had prior, none now).
    - **added**    -- content now, none before (new record, or re-add after delete).
    - **unchanged**-- present both sides, byte-identical flattened leaves.
    - **modified** -- present both sides, leaves differ.

    Field-level counts come only from *modified* records (present and differing on
    both sides), so a path is counted once per record in each category it changed.

    Also returns ``detail``: one row per *changed* record (added/modified/deleted,
    not unchanged), carrying the prior-version key/date and the set of collapsed
    paths it changed -- enough to drive a records table, a per-record diff
    deep-link, and filtering that table by a field path.
    """
    records = {"touched": 0, "added": 0, "modified": 0, "unchanged": 0, "deleted": 0}
    added_paths: Counter[str] = Counter()
    changed_paths: Counter[str] = Counter()
    removed_paths: Counter[str] = Counter()
    detail: list[dict[str, Any]] = []

    def row(tid: str, status: str, run_offset: int, changed: list[str]) -> dict[str, Any]:
        prior = prior_meta.get(tid)
        return {
            "record_id": tid,
            "status": status,
            "run_offset": run_offset,
            "prev_run_id": prior["run_id"] if prior else None,
            "prev_run_date": prior["run_date"] if prior else None,
            "prev_offset": prior["offset"] if prior else None,
            "changed_paths": changed,
        }

    for tid, (action, run_offset) in touched.items():
        records["touched"] += 1
        rv = rv_fields.get(tid, {})
        prev = prev_fields.get(tid, {})

        if action == "delete" or (prev and not rv):
            records["deleted"] += 1
            detail.append(row(tid, "deleted", run_offset, []))
            continue
        if rv and not prev:
            records["added"] += 1
            detail.append(row(tid, "added", run_offset, []))
            continue
        if rv == prev:
            records["unchanged"] += 1
            continue

        records["modified"] += 1
        rv_keys, prev_keys = set(rv), set(prev)
        rec_added = {rv[pi][0] for pi in rv_keys - prev_keys}
        rec_removed = {prev[pi][0] for pi in prev_keys - rv_keys}
        rec_changed = {
            rv[pi][0]
            for pi in rv_keys & prev_keys
            if (rv[pi][1], rv[pi][2]) != (prev[pi][1], prev[pi][2])
        }
        for path in rec_added:
            added_paths[path] += 1
        for path in rec_removed:
            removed_paths[path] += 1
        for path in rec_changed:
            changed_paths[path] += 1
        detail.append(
            row(tid, "modified", run_offset, sorted(rec_added | rec_changed | rec_removed))
        )

    all_paths = set(added_paths) | set(changed_paths) | set(removed_paths)
    fields = sorted(
        (
            {
                "path": path,
                "added_in": added_paths[path],
                "changed_in": changed_paths[path],
                "removed_in": removed_paths[path],
                "records_affected": added_paths[path]
                + changed_paths[path]
                + removed_paths[path],
            }
            for path in all_paths
        ),
        key=lambda r: (-r["records_affected"], r["path"]),
    )
    example_ids = {
        cls: [r["record_id"] for r in detail if r["status"] == cls][:examples]
        for cls in ("added", "modified", "deleted")
    }
    return {
        "records": records,
        "fields": fields,
        "examples": example_ids,
        "detail": detail,
    }


def diff_run(
    dataset: TIMDEXDataset,
    run_id: str,
    *,
    examples: int = DEFAULT_EXAMPLES,
    include_records: bool = False,
    on_progress: Any | None = None,
) -> dict[str, Any]:
    """Diff run ``run_id`` against the prior state of the records it touched.

    Returns a structured report: run metadata, a record-level summary
    (added/modified/unchanged/deleted), a field-level breakdown (per path, in how
    many records it was added/changed/removed), and a few example record ids per
    change class. With ``include_records=True`` the report also carries ``records``
    detail rows (one per *changed* record, with prior-version keys and the paths it
    changed) -- the per-record table the web view renders. Left out by default so
    the CLI/JSON summary stays compact. Raises :class:`ValueError` if the run id is
    unknown.
    """
    meta = run_meta(dataset, run_id)
    if meta is None:
        raise ValueError(f"No run found with run_id {run_id!r}.")

    def progress(phase: str) -> None:
        if on_progress:
            on_progress(phase)

    progress("reading run records")
    touched = _touched(dataset, run_id)
    rv_fields = _read_run_fields(dataset, run_id)

    progress("resolving previous versions")
    prior_meta = _prior_meta(dataset, run_id)
    prev_fields = _read_prior_fields(dataset, prior_meta)

    progress("computing diff")
    result = _classify_and_aggregate(
        touched, prior_meta, rv_fields, prev_fields, examples=examples
    )
    detail = result.pop("detail")

    report = {
        "run_id": run_id,
        "baseline": "previous",
        **meta,
        "records_read": {
            "run_version": len(rv_fields),
            "previous_version": len(prev_fields),
            "without_prior": len(touched) - len(prior_meta),
        },
        **result,
    }
    if include_records:
        report["records_detail"] = detail
    return report
