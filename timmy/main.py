from flask import Blueprint, abort, jsonify, render_template, request

from timmy.dataset import dataset_lock, get_app_dataset
from timmy.sources import get_source_record_format, prettify

main = Blueprint("main", __name__)

# Metadata columns that accept a comma-separated list of values, applied as an
# `IN (...)` predicate. All are parameterized, so the values are safe.
IN_FILTER_COLUMNS = [
    "source",
    "run_type",
    "action",
    "run_id",
]

# Full set of metadata fields shown at the top of the record detail page, in
# display order. These are TDA's TIMDEXRecords.METADATA_COLUMNS.
RECORD_METADATA_COLUMNS = [
    "timdex_record_id",
    "source",
    "run_date",
    "run_type",
    "action",
    "run_id",
    "run_record_offset",
    "run_timestamp",
    "filename",
]

# Metadata-only columns surfaced in the records table. These all live in
# metadata.current_records and can be served straight from DuckDB without the
# parquet read/join step needed for source_record / transformed_record.
RECORD_COLUMNS = [
    "timdex_record_id",
    "source",
    "run_date",
    "run_type",
    "action",
    "run_id",
    "run_record_offset",
]

# Columns the DataTables global search box applies to. We cast to varchar so the
# ILIKE works regardless of the underlying column type (dates, uuids, etc.).
SEARCHABLE_COLUMNS = [
    "timdex_record_id",
    "source",
    "run_type",
    "action",
    "run_id",
]

# SQL select expressions for columns that need shaping for display. Columns not
# listed here are selected as-is. Dates/timestamps are cast to plain ISO strings
# so JSON serialization doesn't render them as RFC-1123 timestamps.
SELECT_EXPRESSIONS = {
    "run_date": "cast(run_date as date)::varchar",
    "run_timestamp": "cast(run_timestamp as timestamp)::varchar",
}

# Columns fetched for the per-record versions page. source/timdex_record_id are
# constant across the page (shown in the header), so source is fetched only to
# populate that header and is not a displayed row column.
VERSION_COLUMNS = [
    "source",
    "run_timestamp",
    "run_date",
    "run_type",
    "action",
    "run_id",
    "run_record_offset",
]

# Per-row columns actually rendered in the versions table (run_timestamp first
# as the finer-grained sort key); same shape as the list view minus the
# constants. run_id/run_record_offset are the diff/detail composite key.
VERSION_DISPLAY_COLUMNS = [c for c in VERSION_COLUMNS if c != "source"]

# The metadata view we browse. current_records is one current row per
# (source, timdex_record_id); swap for metadata.records to see all versions.
RECORDS_TABLE = "metadata.current_records"

# Cap on rows returned per request, regardless of what the client asks for.
MAX_PAGE_LENGTH = 200


@main.get("/")
def index() -> str:
    td = get_app_dataset()
    return render_template(
        "index.html",
        td=td,
    )


@main.get("/records")
def records() -> str:
    """Render the records table page (DataTables in server-side mode)."""
    return render_template("records.html", columns=RECORD_COLUMNS)


def _split_csv(value: str) -> list[str]:
    """Split a comma-separated filter value into a clean list of terms."""
    return [item.strip() for item in value.split(",") if item.strip()]


def _build_where(args) -> tuple[str, list]:
    """Build the WHERE clause (and its params) from search + filter inputs.

    All predicates are combined with AND. Typed filters are parameterized; the
    freeform `f_where` fragment is injected raw -- it is an internal power tool,
    not safe for untrusted input (see scratch/tda.md).
    """
    clauses: list[str] = []
    params: list = []

    # Global DataTables search box: OR across searchable columns.
    search_value = args.get("search[value]", default="", type=str).strip()
    if search_value:
        ors = [f"cast({col} as varchar) ilike ?" for col in SEARCHABLE_COLUMNS]
        clauses.append("(" + " or ".join(ors) + ")")
        params.extend([f"%{search_value}%"] * len(SEARCHABLE_COLUMNS))

    # Comma-separated IN-list filters.
    for column in IN_FILTER_COLUMNS:
        values = _split_csv(args.get(f"f_{column}", default="", type=str))
        if values:
            placeholders = ", ".join(["?"] * len(values))
            clauses.append(f"{column} in ({placeholders})")
            params.extend(values)

    # Exact run_date match (YYYY-MM-DD), cast both sides to be type-agnostic.
    run_date = args.get("f_run_date", default="", type=str).strip()
    if run_date:
        clauses.append("cast(run_date as date) = cast(? as date)")
        params.append(run_date)

    # Freeform raw SQL predicate -- parenthesized so it composes with the rest.
    where_raw = args.get("f_where", default="", type=str).strip()
    if where_raw:
        clauses.append(f"({where_raw})")

    where_sql = (" where " + " and ".join(clauses)) if clauses else ""
    return where_sql, params


@main.get("/records/data")
def records_data():
    """DataTables server-side processing endpoint.

    Implements the DataTables request/response contract by translating draw /
    start / length / search / order / filter params into metadata-only DuckDB
    SQL. No record payload columns are read here, keeping list views fast.
    """
    conn = get_app_dataset().conn

    draw = request.args.get("draw", default=1, type=int)
    start = max(request.args.get("start", default=0, type=int), 0)
    length = request.args.get("length", default=25, type=int)
    length = min(max(length, 1), MAX_PAGE_LENGTH)

    # Resolve ordering from a whitelist so the column name is never user-controlled.
    order_col_idx = request.args.get("order[0][column]", default=0, type=int)
    if 0 <= order_col_idx < len(RECORD_COLUMNS):
        order_col = RECORD_COLUMNS[order_col_idx]
    else:
        order_col = RECORD_COLUMNS[0]
    order_dir = "desc" if request.args.get("order[0][dir]") == "desc" else "asc"

    where_sql, params = _build_where(request.args)
    columns_sql = ", ".join(
        f"{SELECT_EXPRESSIONS[col]} as {col}" if col in SELECT_EXPRESSIONS else col
        for col in RECORD_COLUMNS
    )

    try:
        with dataset_lock:
            records_total = conn.execute(
                f"select count(*) from {RECORDS_TABLE}"  # noqa: S608
            ).fetchone()[0]
            records_filtered = conn.execute(
                f"select count(*) from {RECORDS_TABLE}{where_sql}",  # noqa: S608
                params,
            ).fetchone()[0]
            page_query = (
                f"select {columns_sql} from {RECORDS_TABLE}{where_sql} "  # noqa: S608
                f"order by {order_col} {order_dir} "
                f"limit ? offset ?"
            )
            rows = conn.execute(page_query, [*params, length, start]).fetchall()
    except Exception as exc:  # noqa: BLE001
        # Surfaced inline by DataTables (e.g. an invalid freeform `where`).
        return jsonify(draw=draw, error=str(exc))

    data = [dict(zip(RECORD_COLUMNS, row, strict=True)) for row in rows]
    return jsonify(
        draw=draw,
        recordsTotal=records_total,
        recordsFiltered=records_filtered,
        data=data,
    )


def _fetch_record_version(
    timdex_record_id: str, run_id: str, run_record_offset: int
) -> dict | None:
    """Read a single record version (incl. payloads) by its composite key.

    Reads from table='records' so any historical version is reachable, not just
    the current one. The typed equality filters are parameterized by TDA, so the
    URL values are not interpolated into raw SQL. Returns None if not found.
    """
    with dataset_lock:
        matches = list(
            get_app_dataset().records.read_dicts_iter(
                table="records",
                timdex_record_id=timdex_record_id,
                run_id=run_id,
                run_record_offset=run_record_offset,
                limit=1,
            )
        )
    return matches[0] if matches else None


@main.get("/record/<timdex_record_id>")
def record_versions(timdex_record_id: str) -> str:
    """Overview of every version of a record across all runs.

    Metadata-first: reads only metadata columns from metadata.records (no
    payload reads here), ordered newest-first by run_timestamp. Two versions can
    be selected to diff their payloads, which are fetched on demand.
    """
    conn = get_app_dataset().conn
    columns_sql = ", ".join(
        f"{SELECT_EXPRESSIONS[col]} as {col}" if col in SELECT_EXPRESSIONS else col
        for col in VERSION_COLUMNS
    )
    with dataset_lock:
        rows = conn.execute(
            f"select {columns_sql} from metadata.records "  # noqa: S608
            "where timdex_record_id = ? "
            "order by run_timestamp desc nulls last",
            [timdex_record_id],
        ).fetchall()
    if not rows:
        abort(404, description="No versions found for that record id.")

    versions = [dict(zip(VERSION_COLUMNS, row, strict=True)) for row in rows]
    return render_template(
        "versions.html",
        timdex_record_id=timdex_record_id,
        source=versions[0]["source"],
        columns=VERSION_DISPLAY_COLUMNS,
        versions=versions,
    )


@main.get("/record/<timdex_record_id>/<run_id>/<int:run_record_offset>")
def record(timdex_record_id: str, run_id: str, run_record_offset: int) -> str:
    """Detail view for a single record version.

    Identified by the (timdex_record_id, run_id, run_record_offset) composite
    key. Unlike the list view, this reads the actual payload columns
    (source_record, transformed_record) via TDA's parquet-backed read path.
    """
    rec = _fetch_record_version(timdex_record_id, run_id, run_record_offset)
    if rec is None:
        abort(404, description="No record found for that key.")

    source_format = get_source_record_format(rec["source"])
    return render_template(
        "record.html",
        rec=rec,
        metadata_columns=RECORD_METADATA_COLUMNS,
        source_format=source_format,
        source_pretty=prettify(rec.get("source_record"), source_format),
        transformed_pretty=prettify(rec.get("transformed_record"), "json"),
    )


@main.get("/record/<timdex_record_id>/<run_id>/<int:run_record_offset>/payloads")
def record_payloads(timdex_record_id: str, run_id: str, run_record_offset: int):
    """Prettified source + transformed payloads for one version, as JSON.

    Used by the versions page to fetch the two sides of a diff on demand.
    """
    rec = _fetch_record_version(timdex_record_id, run_id, run_record_offset)
    if rec is None:
        abort(404, description="No record found for that key.")

    source_format = get_source_record_format(rec["source"])
    return jsonify(
        source_format=source_format,
        source=prettify(rec.get("source_record"), source_format),
        transformed=prettify(rec.get("transformed_record"), "json"),
    )
