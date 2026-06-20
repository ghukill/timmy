"""Web layer for the analysis subsystem (the ``/analysis`` blueprint).

This module is the Flask surface over :mod:`timmy.analysis`, kept separate so the
analysis package itself stays free of Flask and reusable from a CLI/agent later.

Routes:

- ``GET  /analysis/``               registry of built analyses
- ``POST /analysis/build``          build one from the current /records filter,
  then redirect to its detail page (synchronous; holds ``dataset_lock``)
- ``GET  /analysis/<id>``           manifest + SQL console + (stubbed) report
- ``POST /analysis/<id>/query``     run read-only SQL, return JSON results
"""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from flask import (
    Blueprint,
    abort,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from timmy import analysis
from timmy.dataset import dataset_lock, get_app_dataset
from timmy.main import IN_FILTER_COLUMNS, SEARCHABLE_COLUMNS, _split_csv

analysis_bp = Blueprint("analysis", __name__, url_prefix="/analysis")

# analysis_id is also a filename stem, so constrain it before touching the
# filesystem -- this is what makes path traversal via the URL impossible.
ANALYSIS_ID_RE = re.compile(r"^\d{8}T\d{6}-[0-9a-f]{8}$")

# Cap rows returned by the SQL console (one extra is fetched to detect overflow).
MAX_QUERY_ROWS = 1000

# Max rows per page for the server-side drill-down tables.
MAX_PAGE = 200


def _analyses_dir() -> str:
    return current_app.config["TIMDEX_ANALYSIS_DIR"]


def _check_analysis_id(analysis_id: str) -> None:
    """404 unless the id matches our generated format (also blocks traversal)."""
    if not ANALYSIS_ID_RE.match(analysis_id):
        abort(404)


def _dt_params(args, columns: list[str]) -> tuple[int, int, int, str, str, str]:
    """Parse the DataTables server-side request into a tidy tuple.

    Returns (draw, start, length, search, order_col, order_dir). The order
    column is resolved from a whitelist so it is never user-controlled SQL.
    """
    draw = args.get("draw", default=1, type=int)
    start = max(args.get("start", default=0, type=int), 0)
    length = min(max(args.get("length", default=25, type=int), 1), MAX_PAGE)
    search = args.get("search[value]", default="", type=str).strip()
    idx = args.get("order[0][column]", default=0, type=int)
    order_col = columns[idx] if 0 <= idx < len(columns) else columns[0]
    order_dir = "desc" if args.get("order[0][dir]") == "desc" else "asc"
    return draw, start, length, search, order_col, order_dir


def _sql_literal(value: str) -> str:
    """Quote a string as a SQL literal (single quotes doubled)."""
    return "'" + value.replace("'", "''") + "'"


def _tda_filter_from_request(values) -> tuple[str | None, dict[str, Any]]:
    """Translate the /records filter inputs into TDA's (where, **filters) model.

    The typed comma-separated filters map onto TDA's typed ``**filters`` (which
    TDA binds safely); the global search box and the freeform ``f_where`` become
    a raw ``where`` string -- the same trusted power-tool semantics as the
    browse view, and harmless here because the analysis read is the user's own.
    """
    filters: dict[str, Any] = {}
    for column in IN_FILTER_COLUMNS:
        items = _split_csv(values.get(f"f_{column}", "") or "")
        if items:
            filters[column] = items
    run_date = (values.get("f_run_date", "") or "").strip()
    if run_date:
        filters["run_date"] = run_date

    where_parts: list[str] = []
    search_value = (values.get("search[value]", "") or "").strip()
    if search_value:
        term = _sql_literal(f"%{search_value}%")
        ors = " or ".join(
            f"cast({col} as varchar) ilike {term}" for col in SEARCHABLE_COLUMNS
        )
        where_parts.append(f"({ors})")
    where_raw = (values.get("f_where", "") or "").strip()
    if where_raw:
        where_parts.append(f"({where_raw})")

    where = " and ".join(where_parts) if where_parts else None
    return where, filters


def _label_from(where: str | None, filters: dict[str, Any]) -> str:
    """A short human label for the analysis, derived from its filter."""
    parts = [
        f"{key}={','.join(val) if isinstance(val, list) else val}"
        for key, val in filters.items()
    ]
    if where:
        parts.append("custom where")
    return "; ".join(parts) if parts else "all current records"


def _jsonable(value: Any) -> Any:
    """Coerce a DuckDB cell into something JSON-serializable."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


@analysis_bp.get("/")
def index() -> str:
    """Registry of built analyses, newest first."""
    return render_template("analysis_index.html", analyses=analysis.list_analyses(_analyses_dir()))


@analysis_bp.post("/build")
def build():
    """Build an analysis from the current /records filter, then redirect to it.

    Synchronous by design (v1): the read path uses the shared dataset
    connection, so the build runs under ``dataset_lock``.
    """
    where, filters = _tda_filter_from_request(request.values)
    label = _label_from(where, filters)
    name = request.values.get("name", default="", type=str).strip() or None
    notes = request.values.get("notes", default="", type=str).strip() or None
    try:
        with dataset_lock:
            manifest = analysis.build_analysis(
                get_app_dataset(),
                _analyses_dir(),
                where=where,
                label=label,
                name=name,
                notes=notes,
                **filters,
            )
    except Exception as exc:  # noqa: BLE001 -- surface build failures to the user
        abort(400, description=f"Analysis build failed: {exc}")
    return redirect(url_for("analysis.detail", analysis_id=manifest["analysis_id"]))


@analysis_bp.post("/<analysis_id>/update")
def update(analysis_id: str):
    """Persist an edited name/notes onto the analysis, then return to it."""
    _check_analysis_id(analysis_id)
    name = request.form.get("name", default="", type=str).strip() or None
    notes = request.form.get("notes", default="", type=str).strip() or None
    try:
        analysis.update_manifest(_analyses_dir(), analysis_id, name=name, notes=notes)
    except FileNotFoundError:
        abort(404)
    return redirect(url_for("analysis.detail", analysis_id=analysis_id))


@analysis_bp.post("/<analysis_id>/delete")
def delete(analysis_id: str):
    """Delete an analysis (its single DuckDB file), then return to the registry."""
    _check_analysis_id(analysis_id)
    analysis.delete_analysis(_analyses_dir(), analysis_id)
    return redirect(url_for("analysis.index"))


@analysis_bp.get("/<analysis_id>")
def detail(analysis_id: str) -> str:
    """Analysis home: manifest, SQL console, and the (stubbed) field report."""
    _check_analysis_id(analysis_id)
    try:
        manifest = analysis.read_manifest(_analyses_dir(), analysis_id)
        conn = analysis.open_analysis(_analyses_dir(), analysis_id, read_only=True)
    except FileNotFoundError:
        abort(404)
    try:
        report = analysis.field_usage(conn)
    finally:
        conn.close()
    return render_template(
        "analysis_detail.html",
        analysis_id=analysis_id,
        manifest=manifest,
        report=report,
        max_rows=MAX_QUERY_ROWS,
    )


@analysis_bp.post("/<analysis_id>/query")
def query(analysis_id: str):
    """Run read-only SQL against the analysis DB; return JSON {columns, rows}.

    The connection is opened read-only, so the engine itself rejects any write;
    no SQL sanitizing is needed. This path never touches ``dataset_lock`` -- each
    request gets its own connection to the standalone analysis file -- so the
    console stays fully concurrent with browsing.
    """
    _check_analysis_id(analysis_id)
    payload = request.get_json(silent=True) or {}
    sql = (payload.get("sql") or "").strip()
    if not sql:
        return jsonify(error="No SQL provided."), 400

    try:
        conn = analysis.open_analysis(_analyses_dir(), analysis_id, read_only=True)
    except FileNotFoundError:
        abort(404)
    try:
        cursor = conn.execute(sql)
        columns = [d[0] for d in cursor.description] if cursor.description else []
        rows = cursor.fetchmany(MAX_QUERY_ROWS + 1)
        truncated = len(rows) > MAX_QUERY_ROWS
        data = [[_jsonable(v) for v in row] for row in rows[:MAX_QUERY_ROWS]]
        return jsonify(
            columns=columns, rows=data, truncated=truncated, row_count=len(data)
        )
    except Exception as exc:  # noqa: BLE001 -- report SQL errors inline
        return jsonify(error=str(exc)), 400
    finally:
        conn.close()


def _table_data(analysis_id: str, columns: list[str], runner):
    """Shared server-side DataTables responder for the drill-down tables.

    ``runner(conn, search, order_col, order_dir, limit, offset)`` returns
    ``(total, filtered, rows)``; this handles param parsing, the read-only
    connection, and the DataTables JSON envelope.
    """
    _check_analysis_id(analysis_id)
    draw, start, length, search, order_col, order_dir = _dt_params(request.args, columns)
    try:
        conn = analysis.open_analysis(_analyses_dir(), analysis_id, read_only=True)
    except FileNotFoundError:
        abort(404)
    try:
        total, filtered, rows = runner(
            conn, search, order_col, order_dir, length, start
        )
    except Exception as exc:  # noqa: BLE001 -- surfaced inline by DataTables
        return jsonify(draw=draw, error=str(exc))
    finally:
        conn.close()
    return jsonify(
        draw=draw, recordsTotal=total, recordsFiltered=filtered, data=rows
    )


@analysis_bp.get("/<analysis_id>/values")
def values(analysis_id: str) -> str:
    """Path-scoped value table: distinct values for every path under a prefix."""
    _check_analysis_id(analysis_id)
    return render_template(
        "analysis_values.html",
        analysis_id=analysis_id,
        prefix=request.args.get("prefix", default="", type=str),
        columns=analysis.PATH_VALUE_COLUMNS,
    )


@analysis_bp.get("/<analysis_id>/values/data")
def values_data(analysis_id: str):
    """DataTables server-side endpoint for the path-scoped value table."""
    prefix = request.args.get("prefix", default="", type=str)
    return _table_data(
        analysis_id,
        analysis.PATH_VALUE_COLUMNS,
        lambda conn, search, oc, od, limit, offset: analysis.path_values(
            conn, prefix, search=search, order_col=oc, order_dir=od,
            limit=limit, offset=offset,
        ),
    )


@analysis_bp.get("/<analysis_id>/records")
def value_records_page(analysis_id: str) -> str:
    """Records that carry a given value at a given path (value -> records drill)."""
    _check_analysis_id(analysis_id)
    return render_template(
        "analysis_records.html",
        analysis_id=analysis_id,
        path=request.args.get("path", default="", type=str),
        value=request.args.get("value", default="", type=str),
        columns=analysis.VALUE_RECORD_COLUMNS,
    )


@analysis_bp.get("/<analysis_id>/records/data")
def value_records_data(analysis_id: str):
    """DataTables server-side endpoint for the value -> records drill."""
    path = request.args.get("path", default="", type=str)
    value = request.args.get("value", default="", type=str)
    return _table_data(
        analysis_id,
        analysis.VALUE_RECORD_COLUMNS,
        lambda conn, search, oc, od, limit, offset: analysis.value_records(
            conn, path, value, search=search, order_col=oc, order_dir=od,
            limit=limit, offset=offset,
        ),
    )
