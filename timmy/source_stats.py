"""Flask-free, metadata-only source/run aggregates.

The cheap counterpart to the EAV analysis subsystem: everything here is a plain
aggregate over the dataset's ``metadata`` tables (``current_records`` /
``records``), with no payload reads and no materialized artifact. Answers like
"how many records does source X have?" or "what were its recent ETL runs?" come
straight from here in milliseconds -- no ``analysis build`` required.

Both surfaces share this module: the web ``/sources`` view (``sources_views``)
and the CLI ``sources`` commands. Callers pass a DuckDB connection (the web app
holds ``dataset_lock`` around its shared one; a one-shot CLI process owns its
own), so this stays free of Flask and of the dataset-locking policy.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

# Per-source overview row (the lightweight "what sources exist, how big" table).
# record_count is the authoritative *current* count (one row per current record
# in metadata.current_records); versions counts every historical row.
SOURCE_OVERVIEW_COLUMNS = [
    "source",
    "record_count",
    "versions",
    "runs",
    "first_run",
    "last_run",
]

# Per-run aggregate columns, in display order. Doubles as the web runs table's
# order-by whitelist, so the column name is never user-controlled SQL.
RUN_COLUMNS = [
    "run_id",
    "source",
    "run_type",
    "run_date",
    "run_timestamp",
    "record_count",
    "index_count",
    "delete_count",
    "skip_count",
    "error_count",
]

# Columns the runs table's global search box applies to (the descriptive identity
# columns; searching the numeric counts would be noise).
SEARCHABLE_RUN_COLUMNS = ["run_id", "source", "run_type"]

# One row per run. run_id determines source/run_type/run_date/run_timestamp, so
# any_value() picks the (single) value per group cheaply; the action breakdown is
# a set of conditional counts over the run's records. Used as a CTE the
# count/page/filter queries build on.
RUNS_CTE = """
    runs as (
        select
            run_id,
            any_value(source) as source,
            any_value(run_type) as run_type,
            cast(any_value(run_date) as date)::varchar as run_date,
            cast(max(run_timestamp) as timestamp)::varchar as run_timestamp,
            count(*) as record_count,
            count(*) filter (where action = 'index') as index_count,
            count(*) filter (where action = 'delete') as delete_count,
            count(*) filter (where action = 'skip') as skip_count,
            count(*) filter (where action = 'error') as error_count
        from metadata.records
        group by run_id
    )
"""


def source_overview(conn: DuckDBPyConnection) -> list[dict]:
    """Per-source metadata summary, one row per source (sorted by source).

    ``record_count`` is the current count (``metadata.current_records``);
    ``versions``/``runs``/``first_run``/``last_run`` come from the full history
    (``metadata.records``). A source with only superseded/deleted records still
    appears (with a ``record_count`` of 0).
    """
    current = {
        source: count
        for source, count in conn.execute(
            "select source, count(*) from metadata.current_records group by source"
        ).fetchall()
    }
    history = conn.execute(
        "select source, "
        "count(*) as versions, "
        "count(distinct run_id) as runs, "
        "cast(min(run_date) as date)::varchar as first_run, "
        "cast(max(run_date) as date)::varchar as last_run "
        "from metadata.records group by source"
    ).fetchall()

    by_source: dict[str, dict] = {}
    for source, versions, runs, first_run, last_run in history:
        by_source[source] = {
            "source": source,
            "record_count": current.get(source, 0),
            "versions": versions,
            "runs": runs,
            "first_run": first_run,
            "last_run": last_run,
        }
    # Defensive: a source present only in current_records (shouldn't happen, but
    # keeps the count honest if it does).
    for source, count in current.items():
        if source not in by_source:
            by_source[source] = {
                "source": source,
                "record_count": count,
                "versions": None,
                "runs": None,
                "first_run": None,
                "last_run": None,
            }
    return [by_source[s] for s in sorted(by_source)]


def run_summaries(
    conn: DuckDBPyConnection,
    *,
    sources: list[str] | None = None,
    limit: int | None = None,
) -> list[dict]:
    """Per-run aggregates, newest first, optionally filtered to ``sources``.

    The non-paged counterpart to the web runs DataTables endpoint -- used by the
    CLI (`sources show` / `sources runs`). Metadata-only, no payload reads.
    """
    params: list = []
    where = ""
    if sources:
        placeholders = ", ".join(["?"] * len(sources))
        where = f" where source in ({placeholders})"
        params.extend(sources)
    sql = (
        f"with {RUNS_CTE} "  # noqa: S608 -- fixed column list + parameterized filter
        f"select {', '.join(RUN_COLUMNS)} from runs{where} "
        "order by run_timestamp desc"
    )
    if limit is not None:
        sql += " limit ?"
        params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [dict(zip(RUN_COLUMNS, row, strict=True)) for row in rows]
