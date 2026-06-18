from flask import Blueprint, jsonify, render_template, request

from timmy.dataset import get_app_dataset

main = Blueprint("main", __name__)

# Metadata columns that accept a comma-separated list of values, applied as an
# `IN (...)` predicate. All are parameterized, so the values are safe.
IN_FILTER_COLUMNS = [
    "source",
    "run_type",
    "action",
    "run_id",
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
# listed here are selected as-is. run_date is cast to a plain ISO date string so
# JSON serialization doesn't render it as an RFC-1123 timestamp.
SELECT_EXPRESSIONS = {
    "run_date": "cast(run_date as date)::varchar",
}

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
