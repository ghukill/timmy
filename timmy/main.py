from flask import Blueprint, abort, current_app, jsonify, render_template, request

from timmy.dataset import dataset_lock, get_app_dataset
from timmy.filters import IN_FILTER_COLUMNS, SEARCHABLE_COLUMNS, split_csv
from timmy.records import (
    RECORD_METADATA_COLUMNS,
    VERSION_COLUMNS,
    read_record_version,
    resolve_current_key,
)
from timmy.sources import (
    extract_timdex_fields,
    flatten_transformed,
    get_source_record_format,
    prettify,
)

main = Blueprint("main", __name__)

# Filter columns and the free-text search set are shared, Flask-free, with the
# analysis build (see timmy.filters): IN_FILTER_COLUMNS, SEARCHABLE_COLUMNS.
# RECORD_METADATA_COLUMNS / VERSION_COLUMNS + the single-version read path are
# shared, Flask-free, with the CLI `record` commands (see timmy.records).

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

# SQL select expressions for columns that need shaping for display. Columns not
# listed here are selected as-is. Dates/timestamps are cast to plain ISO strings
# so JSON serialization doesn't render them as RFC-1123 timestamps.
SELECT_EXPRESSIONS = {
    "run_date": "cast(run_date as date)::varchar",
    "run_timestamp": "cast(run_timestamp as timestamp)::varchar",
}

# Per-row columns actually rendered in the versions table (run_timestamp first
# as the finer-grained sort key); same shape as the list view minus the
# constants. run_id/run_record_offset are the diff/detail composite key.
VERSION_DISPLAY_COLUMNS = [c for c in VERSION_COLUMNS if c != "source"]

# The metadata view we browse. current_records is one current row per
# (source, timdex_record_id); metadata.records holds every version. The
# `f_all_versions` filter swaps between them (see _records_table) -- the runs
# wing turns it on so a run's browse corpus matches its (history-based) count.
RECORDS_TABLE = "metadata.current_records"
ALL_VERSIONS_TABLE = "metadata.records"

# Cap on rows returned per request, regardless of what the client asks for.
MAX_PAGE_LENGTH = 200


@main.get("/")
def index() -> str:
    return render_template("index.html")


@main.get("/records")
def records() -> str:
    """Render the records table page (DataTables in server-side mode)."""
    return render_template("records.html", columns=RECORD_COLUMNS)


def _all_versions(args) -> bool:
    """Whether to browse every version (metadata.records) vs. current only.

    Shared by the browse view and the analysis build so a single flag bounds both
    the on-screen corpus and what TDA reads for an analysis -- the runs wing sets
    it so a run's records match its history-based count.
    """
    return bool(args.get("f_all_versions", default="", type=str).strip())


def _records_table(args) -> str:
    """Resolve which metadata table to browse from the request args."""
    return ALL_VERSIONS_TABLE if _all_versions(args) else RECORDS_TABLE


def _record_limit(args) -> int | None:
    """Parse the optional ``f_limit`` cap. Returns a positive int, else ``None``.

    Shared by the browse view and the analysis build so a single filter value
    bounds both the on-screen corpus and what TDA reads for an analysis.
    """
    limit = args.get("f_limit", default=0, type=int)
    return limit if limit and limit > 0 else None


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
        values = split_csv(args.get(f"f_{column}", default="", type=str))
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
    # current_records (default) or metadata.records (all versions); the latter is
    # how a run's browse corpus matches its history-based count.
    records_table = _records_table(request.args)
    # Optional cap on the matched corpus (maps to TDA's read `limit`). It bounds
    # the filtered count and the page slice so the browse mirrors exactly what an
    # analysis built from this filter would read.
    record_limit = _record_limit(request.args)
    columns_sql = ", ".join(
        f"{SELECT_EXPRESSIONS[col]} as {col}" if col in SELECT_EXPRESSIONS else col
        for col in RECORD_COLUMNS
    )

    try:
        with dataset_lock:
            records_total = conn.execute(
                f"select count(*) from {records_table}"  # noqa: S608
            ).fetchone()[0]
            records_filtered = conn.execute(
                f"select count(*) from {records_table}{where_sql}",  # noqa: S608
                params,
            ).fetchone()[0]
            if record_limit is not None:
                records_filtered = min(records_filtered, record_limit)
            # Never serve a page that reaches past the cap.
            page_length = length
            if record_limit is not None:
                page_length = min(length, max(record_limit - start, 0))
            if page_length <= 0:
                rows = []
            else:
                page_query = (
                    f"select {columns_sql} from {records_table}{where_sql} "  # noqa: S608
                    f"order by {order_col} {order_dir} "
                    f"limit ? offset ?"
                )
                rows = conn.execute(
                    page_query, [*params, page_length, start]
                ).fetchall()
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

    Thin web wrapper over ``timmy.records.read_record_version`` that holds the
    app's ``dataset_lock`` around the shared, threaded connection.
    """
    with dataset_lock:
        return read_record_version(
            get_app_dataset(), timdex_record_id, run_id, run_record_offset
        )


@main.get("/record/<timdex_record_id>/versions")
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


def _resolve_analysis_context(analysis_id: str | None) -> dict | None:
    """Validate an ``?analysis=<id>`` param and return its display context.

    The EAV table's path/value links only make sense relative to a corpus, so a
    record drilled into *from* an analysis carries that id forward. Returns
    ``{"id", "label"}`` for a real analysis, or ``None`` when the param is
    absent, malformed, or names an analysis that no longer exists -- in which
    case the table renders its leaves as plain (unlinked) text.
    """
    if not analysis_id:
        return None
    from timmy import analysis

    if not analysis.is_valid_analysis_id(analysis_id):
        return None
    try:
        manifest = analysis.read_manifest(
            current_app.config["TIMDEX_ANALYSIS_DIR"], analysis_id
        )
    except FileNotFoundError:
        return None
    label = manifest.get("name") or manifest.get("label") or analysis_id
    return {"id": analysis_id, "label": label}


def _render_record_detail(
    timdex_record_id: str, run_id: str, run_record_offset: int
) -> str:
    """Fetch one record version (incl. payloads) and render the detail page.

    Shared by the current-record route and the verbose version route. Unlike the
    list view, this reads the actual payload columns (source_record,
    transformed_record) via TDA's parquet-backed read path.
    """
    rec = _fetch_record_version(timdex_record_id, run_id, run_record_offset)
    if rec is None:
        abort(404, description="No record found for that key.")

    source_format = get_source_record_format(rec["source"])
    timdex_fields = extract_timdex_fields(rec.get("transformed_record"))
    return render_template(
        "record.html",
        rec=rec,
        metadata_columns=RECORD_METADATA_COLUMNS,
        timdex_title=timdex_fields["title"],
        timdex_source_link=timdex_fields["source_link"],
        source_format=source_format,
        source_pretty=prettify(rec.get("source_record"), source_format),
        transformed_pretty=prettify(rec.get("transformed_record"), "json"),
        eav_rows=flatten_transformed(rec.get("transformed_record")),
        analysis_ctx=_resolve_analysis_context(request.args.get("analysis")),
    )


@main.get("/record/<timdex_record_id>")
def record_current(timdex_record_id: str) -> str:
    """Detail view for the current version of a record.

    The friendly entry point: resolves the current (run_id, run_record_offset)
    from metadata.current_records, then renders the same detail page as the
    verbose version route.
    """
    with dataset_lock:
        key = resolve_current_key(get_app_dataset(), timdex_record_id)
    if key is None:
        abort(404, description="No current version found for that record id.")

    run_id, run_record_offset = key
    return _render_record_detail(timdex_record_id, run_id, run_record_offset)


@main.get("/record/<timdex_record_id>/<run_id>/<int:run_record_offset>")
def record(timdex_record_id: str, run_id: str, run_record_offset: int) -> str:
    """Detail view for a specific record version (verbose composite-key form).

    Identified by the (timdex_record_id, run_id, run_record_offset) composite
    key, so any historical version is reachable -- not just the current one.
    """
    return _render_record_detail(timdex_record_id, run_id, run_record_offset)


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
