"""Web layer for the runs wing (the ``/runs`` blueprint).

The operational/provenance view that complements the metadata-profiling wings.
A *run* is one ETL run (identified by ``run_id``); ``run_id`` uniquely determines
its source/run_type/run_date/run_timestamp, so a run is just an aggregate over
``metadata.records`` -- counts and an action breakdown, no payload reads.

This is deliberately TDA-only: there is no materialized artifact like the
``/analysis`` corpus. Everything is served from the shared dataset connection,
the same metadata-first path as ``/records``.

Routes:

- ``GET /runs``        the runs table (DataTables server-side)
- ``GET /runs/data``   DataTables server-side endpoint, one row per run

Each run links into ``/records`` pre-filtered to its ``run_id``; multi-select
ports several runs over as a single ``run_id IN (...)`` corpus.
"""

from __future__ import annotations

from flask import Blueprint, jsonify, render_template, request

from timmy.dataset import dataset_lock, get_app_dataset
from timmy.main import _split_csv

runs_bp = Blueprint("runs", __name__, url_prefix="/runs")

# Columns surfaced in the runs table, in display order. Doubles as the order-by
# whitelist (the DataTables order index maps into this list), so the column name
# is never user-controlled SQL.
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

# Columns the global search box applies to (cast to varchar so ILIKE is
# type-agnostic). Only the descriptive run identity columns -- searching the
# numeric count columns would be noise.
SEARCHABLE_COLUMNS = ["run_id", "source", "run_type"]

# One row per run. run_id determines source/run_type/run_date/run_timestamp, so
# any_value() picks the (single) value per group cheaply. The action breakdown is
# a set of conditional counts over the run's records. Selected once as a CTE that
# the count/page queries then filter and page over.
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

# Cap on rows returned per request, regardless of what the client asks for.
MAX_PAGE_LENGTH = 200


@runs_bp.get("/")
def index() -> str:
    """Render the runs table page (DataTables in server-side mode)."""
    return render_template("runs.html", columns=RUN_COLUMNS)


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
        ors = [f"cast({col} as varchar) ilike ?" for col in SEARCHABLE_COLUMNS]
        clauses.append("(" + " or ".join(ors) + ")")
        params.extend([f"%{search_value}%"] * len(SEARCHABLE_COLUMNS))

    for column in ("source", "run_type"):
        values = _split_csv(args.get(f"f_{column}", default="", type=str))
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


@runs_bp.get("/data")
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
