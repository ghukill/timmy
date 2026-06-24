"""The single always-current analysis corpus.

Where :mod:`timmy.analysis.store` built many independent, filtered analyses (each its
own ``<analysis_id>.duckdb``), the corpus is **one** DuckDB file -- a fixed
``corpus.duckdb`` -- holding the flattened EAV view of *all current records*. There is
no filter, no id, no registry: the corpus either exists or it doesn't.

Two operations maintain it:

- :func:`build_corpus` -- (re)create it from scratch by streaming every current record.
- :func:`update_corpus` -- reconcile it against the live dataset cheaply, touching only
  what changed.

The reconcile is a ``timdex_composite_id`` set difference. The composite id
(``timdex_record_id|run_id|run_record_offset``) *is* the version identity, so a changed
record simply has a new composite: its old composite leaves the corpus (delete set) and
its new one enters (insert set), with unchanged records never read. Payloads -- the
expensive part -- are read only for the insert set, scoped by its distinct ``run_id``s,
so an update's cost scales with what changed, not with the corpus (validated at ~5M
records: a one-new-run update diffs in ~1s and reads only that run's payloads).

The ``docs``/``eav`` tables match the store's schema (so every query helper in
``store`` works unchanged) plus a ``run_timestamp`` on ``docs``; the per-analysis
``manifest`` is replaced by a single-row ``corpus_meta``.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable, Iterable
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import pyarrow as pa

from timmy.analysis.flatten import flatten, make_timdex_composite_id
from timmy.analysis.scope import EMPTY_SCOPE, Scope, scoped
from timmy.analysis.store import (
    BUILD_FLUSH_EVERY,
    EAV_ARROW_SCHEMA,
    EXCLUDED_FIELDS,
    _bulk_insert,
    top_level_fields,
)

if TYPE_CHECKING:
    from timdex_dataset_api import TIMDEXDataset

logger = logging.getLogger(__name__)

# The corpus is one fixed file; its existence is the whole "does a corpus exist?" state.
CORPUS_FILENAME = "corpus.duckdb"

# Columns pulled per record. The identity + run metadata land in `docs`; the metadata
# columns (source/run_date/run_type/action/run_timestamp) are what corpus *scopes* filter
# on (see timmy.analysis.scope), so they mirror the /records filter vocabulary. The diff
# itself keys on the composite id, not on any of these.
CORPUS_READ_COLUMNS = [
    "timdex_record_id",
    "source",
    "run_id",
    "run_record_offset",
    "run_timestamp",
    "run_date",
    "run_type",
    "action",
    "transformed_record",
]

# docs carries the scope-able metadata columns; eav is identical to the store's (reuses
# EAV_ARROW_SCHEMA). Both are pivoted to Arrow and bulk-loaded per flush. run_date is text
# (ISO ``YYYY-MM-DD``) so ``run_date > '…'`` is a plain string comparison.
DOCS_ARROW_SCHEMA = pa.schema(
    [
        ("timdex_composite_id", pa.string()),
        ("source", pa.string()),
        ("timdex_record_id", pa.string()),
        ("run_id", pa.string()),
        ("run_record_offset", pa.int64()),
        ("run_timestamp", pa.timestamp("us")),
        ("run_date", pa.date32()),
        ("run_type", pa.string()),
        ("action", pa.string()),
    ]
)

CORPUS_SCHEMA_SQL = """
create table docs (
    timdex_composite_id text primary key,
    source text,
    timdex_record_id text,
    run_id text,
    run_record_offset bigint,
    run_timestamp timestamp,
    run_date date,
    run_type text,
    action text
);

create table eav (
    timdex_composite_id text,
    path text,
    path_indexed text,
    value text,
    value_type text
);

create table corpus_meta (
    created_at timestamp,
    last_updated_at timestamp,
    dataset_location text,
    doc_count bigint,
    eav_count bigint,
    skipped_count bigint,
    source_record_count bigint,
    report_stale boolean
);

-- One cached schema-overview report per scope. scope_key '' is the whole corpus.
-- corpus_version is the corpus's last_updated_at at compute time, so an update
-- invalidates every entry (the reader compares versions).
create table scope_report_cache (
    scope_key text,
    corpus_version timestamp,
    computed_at timestamp,
    report_json text
);
"""

# A progress hook: phase label, items done so far, and the total if known.
OnProgress = Callable[[str, int, "int | None"], None]

# Above this scoped doc count, an uncached schema-overview report is slow enough
# (a multi-minute full scan) to warn about before computing it.
REPORT_WARN_THRESHOLD = 100_000


def corpus_path(analyses_dir: str | os.PathLike[str]) -> Path:
    """On-disk path of the corpus DB (fixed filename within the analyses dir)."""
    return Path(analyses_dir) / CORPUS_FILENAME


def corpus_exists(analyses_dir: str | os.PathLike[str]) -> bool:
    """True if a built corpus is present."""
    return corpus_path(analyses_dir).exists()


def open_corpus(
    analyses_dir: str | os.PathLike[str], *, read_only: bool = True
) -> duckdb.DuckDBPyConnection:
    """Open a connection to the corpus DB (read-only by default).

    Read-only is right for serving queries -- the engine then rejects any DDL/DML,
    which is what makes the user-facing SQL console safe without sanitizing input.
    """
    path = corpus_path(analyses_dir)
    if not path.exists():
        raise FileNotFoundError(f"No corpus DB at {path}")
    return duckdb.connect(str(path), read_only=read_only)


def _naive_utc(value: Any) -> datetime | None:
    """Coerce a run_timestamp into a naive-UTC datetime for the docs.run_timestamp col.

    TDA hands back tz-aware datetimes; the Arrow schema is a plain ``timestamp``, so we
    normalize to UTC and drop the tzinfo. Non-datetime/None passes through as None.
    """
    if not isinstance(value, datetime):
        return None
    return value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value


def _as_date(value: Any) -> date | None:
    """Coerce a run_date into a ``datetime.date`` for the docs.run_date col.

    TDA may hand back a ``datetime``/``date``; tests pass an ISO string. The DATE column
    makes ``run_date > '2026-01-01'`` a natural date comparison.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        return date.fromisoformat(value.strip()[:10])
    return None


def _ingest_records(
    con: duckdb.DuckDBPyConnection,
    records: Iterable[dict[str, Any]],
    *,
    only_composites: set[str] | None,
    on_progress: OnProgress | None,
    total: int | None,
    phase: str,
) -> tuple[int, int, int]:
    """Flatten ``records`` into ``docs``/``eav``; return (docs, eav, skipped) counts.

    With ``only_composites`` set, records whose composite isn't in it are ignored
    entirely (the targeted-read path may return whole runs, of which only some rows are
    the insert set). Records with no transformed payload are counted as ``skipped``.
    Rows are buffered and bulk-inserted every ``BUILD_FLUSH_EVERY`` docs to bound memory.
    """
    doc_rows: list[tuple] = []
    eav_rows: list[tuple] = []
    doc_count = eav_count = skipped = 0

    def flush() -> None:
        _bulk_insert(con, "docs", DOCS_ARROW_SCHEMA, doc_rows)
        doc_rows.clear()
        _bulk_insert(con, "eav", EAV_ARROW_SCHEMA, eav_rows)
        eav_rows.clear()

    for rec in records:
        composite = make_timdex_composite_id(
            rec["timdex_record_id"], rec["run_id"], rec["run_record_offset"]
        )
        if only_composites is not None and composite not in only_composites:
            continue

        payload = rec.get("transformed_record")
        if not payload:
            skipped += 1
            continue

        parsed = json.loads(payload)
        for field in EXCLUDED_FIELDS:
            parsed.pop(field, None)

        doc_rows.append(
            (
                composite,
                rec["source"],
                rec["timdex_record_id"],
                rec["run_id"],
                rec["run_record_offset"],
                _naive_utc(rec.get("run_timestamp")),
                _as_date(rec.get("run_date")),
                rec.get("run_type"),
                rec.get("action"),
            )
        )
        for leaf in flatten(parsed):
            eav_rows.append(
                (composite, leaf.path, leaf.path_indexed, leaf.value, leaf.value_type)
            )
            eav_count += 1

        doc_count += 1
        if doc_count % BUILD_FLUSH_EVERY == 0:
            flush()
            if on_progress:
                on_progress(phase, doc_count, total)

    flush()
    if on_progress:
        on_progress(phase, doc_count, total)
    return doc_count, eav_count, skipped


def _create_indexes(con: duckdb.DuckDBPyConnection) -> None:
    """Indexes the corpus relies on for full-corpus and scoped queries.

    - ``eav(path)``: the full-corpus field-usage GROUP BY key.
    - ``eav(timdex_composite_id)``: lets a *scoped* query (which restricts to a subset of
      composites) probe eav instead of scanning all ~50-100M rows -- the price of subsets.
    - ``docs(source)`` / ``docs(run_timestamp)``: selective scope predicates over docs.
    """
    con.execute("create index eav_path_idx on eav (path)")
    con.execute("create index eav_composite_idx on eav (timdex_composite_id)")
    con.execute("create index docs_source_idx on docs (source)")
    con.execute("create index docs_run_ts_idx on docs (run_timestamp)")


def build_corpus(
    dataset: TIMDEXDataset,
    analyses_dir: str | os.PathLike[str],
    *,
    on_progress: OnProgress | None = None,
) -> dict[str, Any]:
    """(Re)build the corpus from every current record; return its meta row.

    Streams all of ``metadata.current_records`` through the flattener. Records whose
    current version is a delete (no transformed payload) contribute nothing and are
    counted under ``skipped_count``. Writes to a ``.building`` temp file and atomically
    renames it into place on success, so a failed build never leaves a half-written
    corpus (and never disturbs an existing one until it's done).
    """
    analyses_dir = Path(analyses_dir)
    analyses_dir.mkdir(parents=True, exist_ok=True)

    final_path = corpus_path(analyses_dir)
    building_path = final_path.with_suffix(".duckdb.building")
    building_path.unlink(missing_ok=True)

    created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    total = dataset.metadata.current_records_count

    con = duckdb.connect(str(building_path))
    try:
        con.execute(CORPUS_SCHEMA_SQL)
        if on_progress:
            on_progress("reading records", 0, total)
        records = dataset.records.read_dicts_iter(
            table="current_records", columns=CORPUS_READ_COLUMNS
        )
        doc_count, eav_count, skipped = _ingest_records(
            con, records, only_composites=None,
            on_progress=on_progress, total=total, phase="flattening records",
        )

        if on_progress:
            on_progress("indexing", doc_count, total)
        _create_indexes(con)
        con.execute(
            "insert into corpus_meta values (?, ?, ?, ?, ?, ?, ?, ?)",
            [created_at, created_at, str(dataset.location), doc_count, eav_count,
             skipped, total, False],
        )
        if on_progress:
            on_progress("computing field-usage report", doc_count, total)
        # The whole-corpus report is the empty-scope cache entry; subset reports are
        # computed and cached lazily on first view (see field_usage_report).
        _write_scope_report(con, EMPTY_SCOPE.key(), created_at, top_level_fields(con))
    except Exception:
        con.close()
        building_path.unlink(missing_ok=True)
        raise
    con.close()

    os.replace(building_path, final_path)
    logger.info(
        "Built corpus: %d docs, %d eav rows, %d skipped", doc_count, eav_count, skipped
    )
    return read_corpus_meta(analyses_dir)


def _source_keyset(
    dataset: TIMDEXDataset,
) -> list[tuple[str, str, str]]:
    """(composite, run_id, action) for every current record -- the cheap metadata side.

    This is the source-of-truth set the corpus is reconciled against. Metadata-only
    (no payloads), so it stays cheap even at multi-million-record scale.
    """
    rows = dataset.conn.execute(
        "select timdex_record_id, run_id, run_record_offset, action "
        "from metadata.current_records"
    ).fetchall()
    return [
        (make_timdex_composite_id(tid, run_id, off), run_id, action)
        for (tid, run_id, off, action) in rows
    ]


def update_corpus(
    dataset: TIMDEXDataset,
    analyses_dir: str | os.PathLike[str],
    *,
    on_progress: OnProgress | None = None,
) -> dict[str, Any]:
    """Reconcile the corpus against the live dataset; return its updated meta row.

    The diff is a composite-id set difference (see module docstring):

    - delete set = corpus composites absent from the live current_records;
    - insert set = live composites (non-delete) absent from the corpus.

    Deletes run first, then payloads are read only for the insert set -- scoped by its
    distinct ``run_id``s -- flattened, and inserted. The whole reconcile runs in one
    transaction so readers (on their own read-only connection) see the pre-update
    snapshot until commit. The field-usage report is recomputed afterward.
    """
    analyses_dir = Path(analyses_dir)
    if not corpus_exists(analyses_dir):
        raise FileNotFoundError(f"No corpus to update at {corpus_path(analyses_dir)}")

    if on_progress:
        on_progress("scanning source keyset", 0, None)
    keyset = _source_keyset(dataset)
    src_arrow = pa.table(
        {
            "composite": pa.array([k[0] for k in keyset], pa.string()),
            "run_id": pa.array([k[1] for k in keyset], pa.string()),
            "action": pa.array([k[2] for k in keyset], pa.string()),
        }
    )

    con = open_corpus(analyses_dir, read_only=False)
    try:
        con.register("src_keys", src_arrow)
        if on_progress:
            on_progress("computing diff", 0, None)

        # Insert set: live, non-delete composites not already in the corpus.
        insert_pairs = con.execute(
            "select composite, run_id from src_keys "
            "where action <> 'delete' "
            "and composite not in (select timdex_composite_id from docs)"
        ).fetchall()
        insert_composites = {c for c, _ in insert_pairs}
        insert_run_ids = sorted({run_id for _, run_id in insert_pairs})

        con.execute("begin transaction")
        # Delete set: corpus composites no longer current (vanished + stale halves of
        # changed records). eav first (FK-free, but keep docs as the anchor).
        con.execute(
            "delete from eav where timdex_composite_id in "
            "(select timdex_composite_id from docs "
            " where timdex_composite_id not in (select composite from src_keys))"
        )
        deleted = con.execute(
            "delete from docs where timdex_composite_id not in "
            "(select composite from src_keys)"
        ).fetchone()
        delete_count = deleted[0] if deleted else 0

        ins_docs = ins_eav = skipped = 0
        if insert_run_ids:
            if on_progress:
                on_progress("reading changed payloads", 0, len(insert_composites))
            records = dataset.records.read_dicts_iter(
                table="current_records",
                columns=CORPUS_READ_COLUMNS,
                run_id=insert_run_ids,
            )
            ins_docs, ins_eav, skipped = _ingest_records(
                con, records, only_composites=insert_composites,
                on_progress=on_progress, total=len(insert_composites),
                phase="flattening changed records",
            )
        con.execute("commit")

        if on_progress:
            on_progress("computing field-usage report", ins_docs, None)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        _refresh_meta_after_update(
            con, dataset, source_record_count=len(keyset), now=now
        )
        # The corpus changed, so every cached scope report is stale. Drop them all and
        # recompute just the whole-corpus one; subset reports recompute lazily on view.
        con.execute("delete from scope_report_cache")
        _write_scope_report(con, EMPTY_SCOPE.key(), now, top_level_fields(con))
    finally:
        con.unregister("src_keys")
        con.close()

    logger.info(
        "Updated corpus: +%d docs, -%d docs (%d eav added, %d skipped)",
        ins_docs, delete_count, ins_eav, skipped,
    )
    return read_corpus_meta(analyses_dir)


def _refresh_meta_after_update(
    con: duckdb.DuckDBPyConnection,
    dataset: TIMDEXDataset,
    *,
    source_record_count: int,
    now: datetime,
) -> None:
    """Recompute corpus_meta counts from the live tables after an update.

    ``now`` is shared with the scope-report cache write so ``last_updated_at`` and the
    cached reports' ``corpus_version`` agree (the reader keys cache hits on that match).
    """
    doc_count = con.execute("select count(*) from docs").fetchone()[0]
    eav_count = con.execute("select count(*) from eav").fetchone()[0]
    con.execute(
        "update corpus_meta set last_updated_at = ?, dataset_location = ?, "
        "doc_count = ?, eav_count = ?, source_record_count = ?, report_stale = false",
        [now, str(dataset.location), doc_count, eav_count, source_record_count],
    )


def read_corpus_meta(analyses_dir: str | os.PathLike[str]) -> dict[str, Any]:
    """Return the single corpus_meta row as a dict (empty dict if no corpus)."""
    if not corpus_exists(analyses_dir):
        return {}
    con = open_corpus(analyses_dir)
    try:
        cols = [d[0] for d in con.execute("select * from corpus_meta").description]
        row = con.execute("select * from corpus_meta").fetchone()
    finally:
        con.close()
    return dict(zip(cols, row, strict=True)) if row else {}


def delete_corpus(analyses_dir: str | os.PathLike[str]) -> bool:
    """Delete the corpus DB file. Returns True if it existed."""
    path = corpus_path(analyses_dir)
    existed = path.exists()
    path.unlink(missing_ok=True)
    return existed


# --------------------------------------------------------------------------- #
# Scoped schema-overview report (cached per scope)
# --------------------------------------------------------------------------- #
def _write_scope_report(
    con: duckdb.DuckDBPyConnection,
    scope_key: str,
    corpus_version: datetime,
    report: list[dict[str, Any]],
) -> None:
    """Cache ``report`` for ``scope_key`` at ``corpus_version`` (replacing any prior)."""
    con.execute("delete from scope_report_cache where scope_key = ?", [scope_key])
    con.execute(
        "insert into scope_report_cache values (?, ?, ?, ?)",
        [scope_key, corpus_version, datetime.now(timezone.utc).replace(tzinfo=None),
         json.dumps(report, default=str)],
    )


def _read_scope_report(
    con: duckdb.DuckDBPyConnection, scope_key: str, corpus_version: datetime
) -> list[dict[str, Any]] | None:
    """Return the cached report for a scope iff it matches the current corpus version."""
    row = con.execute(
        "select report_json from scope_report_cache "
        "where scope_key = ? and corpus_version = ?",
        [scope_key, corpus_version],
    ).fetchone()
    return json.loads(row[0]) if row and row[0] else None


def field_usage_report(
    analyses_dir: str | os.PathLike[str], scope: Scope = EMPTY_SCOPE
) -> list[dict[str, Any]]:
    """The schema-overview report for a scope, served from cache.

    The whole-corpus report (empty scope) is materialized at build/update time, so it's
    always a hit. A subset report is computed under the scope on first view and cached
    keyed by ``(scope_key, corpus_version)``; later views are instant until the next
    update bumps the version. The compute requires a read-write connection; if that
    can't be acquired (a concurrent reader holds the file), the report is still computed
    and returned, just not cached this time.
    """
    version = read_corpus_meta(analyses_dir).get("last_updated_at")
    skey = scope.key()

    conn = open_corpus(analyses_dir, read_only=True)
    try:
        cached = _read_scope_report(conn, skey, version)
    finally:
        conn.close()
    if cached is not None:
        return cached

    conn = open_corpus(analyses_dir, read_only=False)
    try:
        with scoped(conn, scope):
            # Warn before a slow compute: an uncached report over a large subset is a
            # multi-minute full scan. Both surfaces go through here, so the CLI sees the
            # heads-up on stderr and the web app logs it (its spinner is the user-facing
            # cue). The doc count is a cheap indexed query.
            n_docs = conn.execute("select count(*) from docs").fetchone()[0]
            if n_docs >= REPORT_WARN_THRESHOLD:
                logger.warning(
                    "Computing the field-usage report for this subset (%s docs); it "
                    "isn't cached yet, so this may take a few minutes. Later views of "
                    "this subset are instant.",
                    f"{n_docs:,}",
                )
            report = top_level_fields(conn)
        try:
            _write_scope_report(conn, skey, version, report)
        except Exception:  # noqa: BLE001 -- caching is best-effort
            logger.warning("Could not cache scope report for %r", skey, exc_info=True)
    finally:
        conn.close()
    return report
