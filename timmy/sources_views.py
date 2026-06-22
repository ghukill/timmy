"""Web layer for the sources wing (the ``/sources`` blueprint).

The operational/provenance view that complements the metadata-profiling wings.
It has two parts, both served straight from the dataset's ``metadata`` tables
(no payload reads, no materialized artifact):

- a **static per-source overview** (current record count, history, run count,
  first/last run) rendered once on page load -- the "what sources exist, how
  big" answer; and
- the **dynamic runs table** (DataTables, server-side): one row per ETL
  ``run_id`` with its record count and action breakdown.

The aggregate SQL lives in :mod:`timmy.source_stats` (Flask-free) so the CLI
``sources`` commands compute exactly the same numbers.

Routes:

- ``GET /sources``          overview table + the runs table shell
- ``GET /sources/runs/data``  DataTables server-side endpoint, one row per run

Each run links into ``/records`` pre-filtered to its ``run_id``; multi-select
ports several runs over as a single ``run_id IN (...)`` corpus.
"""

from __future__ import annotations

from flask import Blueprint, jsonify, render_template, request

from timmy.dataset import dataset_lock, get_app_dataset
from timmy.filters import split_csv
from timmy.source_stats import (
    RUN_COLUMNS,
    RUNS_CTE,
    SEARCHABLE_RUN_COLUMNS,
    SOURCE_OVERVIEW_COLUMNS,
    source_overview,
)

sources_bp = Blueprint("sources", __name__, url_prefix="/sources")

# Cap on rows returned per request, regardless of what the client asks for.
MAX_PAGE_LENGTH = 200


@sources_bp.get("/")
def index() -> str:
    """Render the sources page: static overview table + the runs table shell."""
    with dataset_lock:
        overview = source_overview(get_app_dataset().conn)
    return render_template(
        "sources.html",
        overview=overview,
        overview_columns=SOURCE_OVERVIEW_COLUMNS,
        columns=RUN_COLUMNS,
    )


def _build_where(args) -> tuple[str, list]:
    """Build the WHERE clause (and params) applied to the aggregated runs.

    All predicates are over the ``runs`` CTE columns and combined with AND. The
    global search box ORs across the identity columns; the typed filters are
    parameterized IN-lists / an exact run_date match. There is no freeform raw
    ``where`` here -- the runs view is a fixed-shape operational table.
    """
    clauses: list[str] = []
    params: list = []

    search_value = args.get("search[value]", default="", type=str).strip()
    if search_value:
        ors = [f"cast({col} as varchar) ilike ?" for col in SEARCHABLE_RUN_COLUMNS]
        clauses.append("(" + " or ".join(ors) + ")")
        params.extend([f"%{search_value}%"] * len(SEARCHABLE_RUN_COLUMNS))

    for column in ("source", "run_type"):
        values = split_csv(args.get(f"f_{column}", default="", type=str))
        if values:
            placeholders = ", ".join(["?"] * len(values))
            clauses.append(f"{column} in ({placeholders})")
            params.extend(values)

    run_date = args.get("f_run_date", default="", type=str).strip()
    if run_date:
        clauses.append("cast(run_date as date) = cast(? as date)")
        params.append(run_date)

    where_sql = (" where " + " and ".join(clauses)) if clauses else ""
    return where_sql, params


@sources_bp.get("/runs/data")
def runs_data():
    """DataTables server-side endpoint: one aggregated row per run.

    Mirrors ``main.records_data`` but over the ``runs`` aggregate. Metadata-only
    (no payload reads), so it stays fast even though it scans every version in
    ``metadata.records``.
    """
    conn = get_app_dataset().conn

    draw = request.args.get("draw", default=1, type=int)
    start = max(request.args.get("start", default=0, type=int), 0)
    length = request.args.get("length", default=25, type=int)
    length = min(max(length, 1), MAX_PAGE_LENGTH)

    order_col_idx = request.args.get("order[0][column]", default=0, type=int)
    if 0 <= order_col_idx < len(RUN_COLUMNS):
        order_col = RUN_COLUMNS[order_col_idx]
    else:
        order_col = RUN_COLUMNS[0]
    order_dir = "desc" if request.args.get("order[0][dir]") == "desc" else "asc"

    where_sql, params = _build_where(request.args)
    columns_sql = ", ".join(RUN_COLUMNS)

    try:
        with dataset_lock:
            # Unfiltered total is just the run count -- no need to build the
            # per-run action aggregation the CTE does.
            runs_total = conn.execute(
                "select count(distinct run_id) from metadata.records"
            ).fetchone()[0]
            runs_filtered = conn.execute(
                f"with {RUNS_CTE} select count(*) from runs{where_sql}",  # noqa: S608
                params,
            ).fetchone()[0]
            rows = conn.execute(
                f"with {RUNS_CTE} "  # noqa: S608
                f"select {columns_sql} from runs{where_sql} "
                f"order by {order_col} {order_dir} "
                f"limit ? offset ?",
                [*params, length, start],
            ).fetchall()
    except Exception as exc:  # noqa: BLE001 -- surfaced inline by DataTables
        return jsonify(draw=draw, error=str(exc))

    data = [dict(zip(RUN_COLUMNS, row, strict=True)) for row in rows]
    return jsonify(
        draw=draw,
        recordsTotal=runs_total,
        recordsFiltered=runs_filtered,
        data=data,
    )
