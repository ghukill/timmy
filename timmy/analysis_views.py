"""Web layer for the single analysis corpus (the ``/analysis`` blueprint).

This is the Flask surface over :mod:`timmy.analysis`, kept separate so the analysis
package itself stays Flask-free and reusable from the CLI. There is **one** corpus over
all current records -- no per-analysis ids or registry. Read views query the whole
corpus by default, or a live *subset* via scope params (``f_source``, ``f_run_type``,
``f_action``, ``f_run_id``, ``f_where``) carried through every drill link.

Routes:

- ``GET  /analysis/``             corpus dashboard (or the "not built yet" empty state)
- ``POST /analysis/build``        (re)build the whole corpus -- runs in a background job
- ``POST /analysis/update``       reconcile against the live dataset -- background job
- ``POST /analysis/delete``       delete the corpus file
- ``GET  /analysis/job``          progress page for a running build/update
- ``GET  /analysis/job.json``     progress snapshot (polled by the progress page)
- ``POST /analysis/query``        run read-only SQL (scoped to the subset if given)
- ``GET  /analysis/sql``          the SQL console: schema reference + saved-query library
- ``POST /analysis/sql/queries``  save (upsert) a named query
- ``POST /analysis/sql/queries/delete``  delete a saved query
- ``GET  /analysis/values``       path-scoped value table  (+ ``/values/data``)
- ``GET  /analysis/object``       parent-object drill       (+ ``/object/data``)
- ``GET  /analysis/records``      value -> records drill    (+ ``/records/data``)
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

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
from timmy.analysis import queries
from timmy.analysis.corpus import REPORT_WARN_THRESHOLD
from timmy.corpus_job import corpus_job
from timmy.dataset import dataset_lock, get_app_dataset
from timmy.filters import split_csv
from timmy.run_diff_cache import run_diff_cache

analysis_bp = Blueprint("analysis", __name__, url_prefix="/analysis")

# Cap rows returned by the SQL console (one extra is fetched to detect overflow).
MAX_QUERY_ROWS = 1000

# Max rows per page for the server-side drill-down tables.
MAX_PAGE = 200

# Request-arg name -> docs scope column, for the corpus-subset filter shared by the
# dashboard, the drills, and the SQL console.
SCOPE_ARGS = {
    "f_source": "source",
    "f_run_type": "run_type",
    "f_action": "action",
    "f_run_id": "run_id",
}


def _analyses_dir() -> str:
    return current_app.config["TIMDEX_ANALYSIS_DIR"]


def _scope_from_request(values) -> analysis.Scope:
    """Build a :class:`Scope` from the scope params on a request (args or JSON body)."""
    filters: dict[str, list[str]] = {}
    for arg, col in SCOPE_ARGS.items():
        vals = split_csv(values.get(arg, "") or "")
        if vals:
            filters[col] = vals
    where = (values.get("f_where", "") or "").strip() or None
    return analysis.make_scope(filters, where)


def _scope_args(values) -> dict[str, str]:
    """The subset of request args that encode the scope, for carrying through links."""
    out: dict[str, str] = {}
    for arg in (*SCOPE_ARGS, "f_where"):
        val = (values.get(arg, "") or "").strip()
        if val:
            out[arg] = val
    return out


def _open():
    """Open the corpus read-only, 404 if it hasn't been built."""
    analyses_dir = _analyses_dir()
    if not analysis.corpus_exists(analyses_dir):
        abort(404, description="No corpus built yet.")
    return analysis.open_corpus(analyses_dir)


def _scoped_doc_total(scope: analysis.Scope) -> int:
    """Doc count under a scope (the coverage denominator). Cheap: indexed count."""
    analyses_dir = _analyses_dir()
    if scope.is_empty():
        return analysis.read_corpus_meta(analyses_dir).get("doc_count") or 0
    conn = analysis.open_corpus(analyses_dir)
    try:
        with analysis.scoped(conn, scope):
            return conn.execute("select count(*) from docs").fetchone()[0]
    finally:
        conn.close()


def _dt_params(args, columns: list[str]) -> tuple[int, int, int, str, str, str]:
    """Parse the DataTables server-side request into a tidy tuple.

    Returns (draw, start, length, search, order_col, order_dir). The order column is
    resolved from a whitelist so it is never user-controlled SQL.
    """
    draw = args.get("draw", default=1, type=int)
    start = max(args.get("start", default=0, type=int), 0)
    length = min(max(args.get("length", default=25, type=int), 1), MAX_PAGE)
    search = args.get("search[value]", default="", type=str).strip()
    idx = args.get("order[0][column]", default=0, type=int)
    order_col = columns[idx] if 0 <= idx < len(columns) else columns[0]
    order_dir = "desc" if args.get("order[0][dir]") == "desc" else "asc"
    return draw, start, length, search, order_col, order_dir


def _jsonable(value: Any) -> Any:
    """Coerce a DuckDB cell into something JSON-serializable."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


# --------------------------------------------------------------------------- #
# Dashboard + lifecycle (build / update / delete / progress)
# --------------------------------------------------------------------------- #
@analysis_bp.get("/")
def index() -> str:
    """Corpus dashboard: meta, the (scoped) schema overview, and the SQL console.

    Redirects to the progress page while a build/update runs, and shows an empty state
    with a Build button when no corpus exists yet.
    """
    if corpus_job.is_running():
        return redirect(url_for("analysis.job_page"))

    analyses_dir = _analyses_dir()
    if not analysis.corpus_exists(analyses_dir):
        return render_template("analysis_empty.html", last_job=corpus_job.snapshot())

    scope = _scope_from_request(request.args)
    scope_args = _scope_args(request.args)
    meta = analysis.read_corpus_meta(analyses_dir)
    # The schema overview (the one expensive part) is NOT computed here -- the page
    # loads instantly and fetches it asynchronously from `report_fragment`, so a large
    # subset's first (uncached) report computes behind a spinner instead of hanging the
    # whole page. Everything else on the dashboard is cheap.
    doc_total = _scoped_doc_total(scope)

    return render_template(
        "analysis_detail.html",
        meta=meta,
        doc_total=doc_total,
        report_may_be_slow=doc_total >= REPORT_WARN_THRESHOLD,
        scope_args=scope_args,
        scope_qs=urlencode(scope_args),
        scoped=bool(scope_args),
        max_rows=MAX_QUERY_ROWS,
    )


@analysis_bp.get("/report")
def report_fragment() -> str:
    """The schema-overview table as an HTML fragment, for the current scope.

    The dashboard fetches this asynchronously. Computing the report is the only slow
    part of the page (an uncached report over a large subset is a multi-minute scan),
    so isolating it here keeps the dashboard itself instant. Reuses the same Jinja table
    partial the dashboard would have rendered inline, so drill links/coverage bars stay
    identical and scope-aware.
    """
    analyses_dir = _analyses_dir()
    if not analysis.corpus_exists(analyses_dir):
        abort(404, description="No corpus built yet.")
    scope = _scope_from_request(request.args)
    scope_args = _scope_args(request.args)
    report = analysis.field_usage_report(analyses_dir, scope)
    return render_template(
        "_report_table.html",
        report=report,
        doc_total=_scoped_doc_total(scope),
        scoped=bool(scope_args),
        scope_args=scope_args,
    )


def _diff_compute(run_id: str):
    """A closure that computes ``run_id``'s diff, holding ``dataset_lock`` around
    the (shared-connection) reads. Handed to the run-diff cache to run in the
    background or synchronously.

    The dataset is resolved *now* (in the request thread, where the app context
    exists) and captured -- the background thread that later runs ``compute`` has no
    app context, so it can't call ``get_app_dataset()`` itself.
    """
    dataset = get_app_dataset()

    def compute(on_progress):
        with dataset_lock:
            return analysis.diff_run(
                dataset,
                run_id,
                include_records=True,
                on_progress=on_progress,
            )

    return compute


@analysis_bp.get("/run/<path:run_id>")
def run_page(run_id: str):
    """Run analysis: what one ETL run changed vs. the prior state of its records.

    Computed live from the dataset (not the corpus) and cached per ``run_id`` for the
    life of the process -- a large run can take a while, so the first view computes
    in the background behind a progress page and a revisit is instant.
    ``?format=json`` blocks and returns the raw report for agents/scripting. A run id
    that doesn't exist 404s.
    """
    want_json = request.args.get("format") == "json"
    report = run_diff_cache.get(run_id)

    # Nothing cached: validate the run id up front (cheap, metadata-only) so an
    # unknown run 404s immediately instead of after a background round-trip. The
    # meta also heads the progress page.
    meta = None
    if report is None:
        with dataset_lock:
            meta = analysis.run_meta(get_app_dataset(), run_id)
        if meta is None:
            abort(404, description=f"No run found with run_id {run_id!r}.")

    if want_json:
        if report is None:
            report = run_diff_cache.compute_now(run_id, _diff_compute(run_id))
        return jsonify(report)

    if report is not None:
        return render_template("analysis_run.html", report=report)

    # Kick off (or join) the background diff and show a progress page that polls and
    # reloads this URL -- which then hits the cache and renders the result -- when done.
    run_diff_cache.ensure(run_id, _diff_compute(run_id))
    return render_template("analysis_run_job.html", run_id=run_id, meta=meta)


@analysis_bp.get("/run/<path:run_id>/status.json")
def run_status(run_id: str):
    """Progress snapshot for a run-diff being computed, polled by the progress page."""
    status = run_diff_cache.status(run_id)
    if status is None:
        # No job started (e.g. a stray poll) -- report an idle, not-ready state.
        status = {
            "running": False,
            "phase": None,
            "error": None,
            "finished": False,
            "ready": False,
            "elapsed": 0,
        }
    return jsonify(status)


def _run_with_lock(fn, dataset, analyses_dir, **kwargs):
    """Wrap a corpus build/update so it holds ``dataset_lock`` around the TDA reads.

    Extra ``kwargs`` pass through to ``fn`` (e.g. the build's worker/batch config).
    """

    def runner(on_progress):
        with dataset_lock:
            return fn(dataset, analyses_dir, on_progress=on_progress, **kwargs)

    return runner


@analysis_bp.post("/build")
def build():
    """Kick off a (re)build of the whole corpus in the background, then show progress."""
    dataset = get_app_dataset()
    runner = _run_with_lock(
        analysis.build_corpus, dataset, _analyses_dir(),
        workers=current_app.config["BUILD_WORKERS"],
        batch_size=current_app.config["BUILD_BATCH_SIZE"],
    )
    try:
        corpus_job.start("build", runner)
    except RuntimeError:
        pass  # one already running -- just fall through to its progress page
    return redirect(url_for("analysis.job_page"))


@analysis_bp.post("/update")
def update():
    """Kick off an incremental reconcile in the background, then show progress."""
    if not analysis.corpus_exists(_analyses_dir()):
        abort(404, description="No corpus to update.")
    dataset = get_app_dataset()
    runner = _run_with_lock(analysis.update_corpus, dataset, _analyses_dir())
    try:
        corpus_job.start("update", runner)
    except RuntimeError:
        pass
    return redirect(url_for("analysis.job_page"))


@analysis_bp.post("/delete")
def delete():
    """Delete the corpus file, then return to the (now empty-state) dashboard."""
    analysis.delete_corpus(_analyses_dir())
    return redirect(url_for("analysis.index"))


@analysis_bp.get("/job")
def job_page() -> str:
    """Progress page for the running (or just-finished) build/update."""
    return render_template("analysis_job.html")


@analysis_bp.get("/job.json")
def job_json():
    """Progress snapshot, polled by the progress page."""
    return jsonify(corpus_job.snapshot())


@analysis_bp.post("/query")
def query():
    """Run read-only SQL against the corpus; return JSON {columns, rows}.

    The connection is opened read-only, so the engine itself rejects any write -- no SQL
    sanitizing needed. Scope params in the body narrow the ``docs``/``eav`` the SQL sees
    to that subset (via temp views shadowing the base tables).
    """
    payload = request.get_json(silent=True) or {}
    sql = (payload.get("sql") or "").strip()
    if not sql:
        return jsonify(error="No SQL provided."), 400

    scope = _scope_from_request(payload)
    conn = _open()
    try:
        with analysis.scoped(conn, scope):
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


# --------------------------------------------------------------------------- #
# SQL console (dedicated page): schema reference + saved-query library
# --------------------------------------------------------------------------- #
# Tables worth surfacing in the console's schema reference (corpus internals like
# scope_report_cache are omitted -- they aren't useful to query directly).
SCHEMA_TABLES = ["docs", "eav", "corpus_meta"]


def _corpus_schema(conn) -> list[dict]:
    """Introspect the queryable tables into ``[{table, columns:[{name,type}]}]``.

    Read live from the corpus via ``DESCRIBE`` so it always matches the actual DB.
    """
    schema = []
    for table in SCHEMA_TABLES:
        rows = conn.execute(f"describe {table}").fetchall()
        # DESCRIBE yields (column_name, column_type, null, key, default, extra).
        columns = [{"name": r[0], "type": r[1]} for r in rows]
        schema.append({"table": table, "columns": columns})
    return schema


@analysis_bp.get("/sql")
def sql_console() -> str:
    """The dedicated SQL console: schema reference, saved-query library, and editor.

    Degrades to the empty state before a corpus exists. Scope params carry through so
    queries run here can still be restricted to a subset (the editor posts them to
    ``/analysis/query`` alongside the SQL).
    """
    analyses_dir = _analyses_dir()
    if not analysis.corpus_exists(analyses_dir):
        return render_template("analysis_empty.html", last_job=corpus_job.snapshot())

    conn = _open()
    try:
        schema = _corpus_schema(conn)
    finally:
        conn.close()
    scope_args = _scope_args(request.args)
    return render_template(
        "analysis_sql.html",
        schema=schema,
        saved_queries=queries.list_queries(analyses_dir),
        scope_args=scope_args,
        scope_qs=urlencode(scope_args),
        scoped=bool(scope_args),
        max_rows=MAX_QUERY_ROWS,
    )


@analysis_bp.post("/sql/queries")
def save_query():
    """Upsert a saved query; return the refreshed library so the dropdown updates."""
    payload = request.get_json(silent=True) or {}
    try:
        queries.save_query(
            _analyses_dir(),
            payload.get("name", ""),
            payload.get("description", "") or "",
            payload.get("sql", "") or "",
            create=bool(payload.get("create")),
        )
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    return jsonify(queries=queries.list_queries(_analyses_dir()))


@analysis_bp.post("/sql/queries/delete")
def delete_saved_query():
    """Delete a saved query; return the refreshed library."""
    payload = request.get_json(silent=True) or {}
    queries.delete_query(_analyses_dir(), payload.get("name", ""))
    return jsonify(queries=queries.list_queries(_analyses_dir()))


# --------------------------------------------------------------------------- #
# Drill-down tables (all scope-aware)
# --------------------------------------------------------------------------- #
def _table_data(columns: list[str], runner):
    """Shared server-side DataTables responder for the drill-down tables.

    ``runner(conn, search, order_col, order_dir, limit, offset)`` returns
    ``(total, filtered, rows)``; this handles param parsing, the read-only connection,
    the active scope (temp views shadowing docs/eav), and the DataTables JSON envelope.
    """
    draw, start, length, search, order_col, order_dir = _dt_params(request.args, columns)
    scope = _scope_from_request(request.args)
    conn = _open()
    try:
        with analysis.scoped(conn, scope):
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


@analysis_bp.get("/values")
def values() -> str:
    """Path-scoped value table: distinct values for every path under a prefix."""
    prefix = request.args.get("prefix", default="", type=str)
    scope = _scope_from_request(request.args)
    conn = _open()
    try:
        with analysis.scoped(conn, scope):
            # Paths whose parent is a complex (object) field get an "object" drill.
            object_path_set = analysis.object_field_paths(conn)
            # A scalar array field (path ending in ``[]`` that is itself a stored leaf)
            # gets a per-record element-count table.
            is_scalar_array = bool(
                prefix.endswith("[]")
                and conn.execute(
                    "select 1 from eav where path = ? limit 1", [prefix]
                ).fetchone()
            )
    finally:
        conn.close()
    object_prefix = (
        _collapsed_object_prefix(prefix) if prefix in object_path_set else None
    )
    scope_args = _scope_args(request.args)
    return render_template(
        "analysis_values.html",
        prefix=prefix,
        columns=analysis.PATH_VALUE_COLUMNS,
        object_paths=sorted(object_path_set),
        object_prefix=object_prefix,
        record_count_path=prefix if is_scalar_array else None,
        scope_args=scope_args,
        scope_qs=urlencode(scope_args),
        whole_corpus_url=url_for("analysis.values", prefix=prefix),
    )


@analysis_bp.get("/values/data")
def values_data():
    """DataTables server-side endpoint for the path-scoped value table."""
    prefix = request.args.get("prefix", default="", type=str)
    return _table_data(
        analysis.PATH_VALUE_COLUMNS,
        lambda conn, search, oc, od, limit, offset: analysis.path_values(
            conn, prefix, search=search, order_col=oc, order_dir=od,
            limit=limit, offset=offset,
        ),
    )


@analysis_bp.get("/values/records/data")
def values_records_data():
    """DataTables endpoint for the per-record element-count table of a scalar array."""
    path = request.args.get("path", default="", type=str)
    display_cols = ["timdex_record_id", "element_count"]
    return _table_data(
        display_cols,
        lambda conn, search, oc, od, limit, offset: analysis.path_record_counts(
            conn, path, search=search, order_col=oc, order_dir=od,
            limit=limit, offset=offset,
        ),
    )


def _collapsed_object_prefix(path: str) -> str:
    """Collapsed array-element prefix of a path: keep up to the last ``]``.

    ``subjects[].kind`` -> ``subjects[]``; a path with no ``]`` has no object.
    """
    idx = path.rfind("]")
    return path[: idx + 1] if idx != -1 else ""


def _member_label(member_path: str, object_prefix: str) -> str:
    """Object-relative label for a member field, e.g. ``subjects[].value[]`` ->
    ``value[]`` under prefix ``subjects[]``. Falls back to the full path."""
    if object_prefix and member_path.startswith(object_prefix):
        return member_path[len(object_prefix):].lstrip(".") or member_path
    return member_path


def _object_request() -> tuple[str, str, str | None]:
    """Parse the object-drill request into ``(object_prefix, path, value)``.

    Value-filtered (from the values view): ``path`` is a member leaf and ``value`` is
    set; the object prefix is the leaf's collapsed parent. Unfiltered (from the schema
    table): ``value`` is absent and ``path`` is the object prefix itself.
    """
    path = request.args.get("path", default="", type=str)
    value = request.args.get("value", default=None, type=str)
    object_prefix = path if value is None else _collapsed_object_prefix(path)
    return object_prefix, path, value


def _object_columns(conn, object_prefix: str, path: str, value: str | None):
    """(member_paths, display columns) for the object table under ``object_prefix``."""
    member_paths = analysis.object_columns(
        conn, object_prefix, value_path=path, value=value
    )
    columns = [
        {"key": f"c{i}", "label": _member_label(mp, object_prefix), "path": mp}
        for i, mp in enumerate(member_paths)
    ]
    return member_paths, columns


@analysis_bp.get("/object")
def object_page() -> str:
    """Parent-object drill: object instances under a complex field, pivoted."""
    object_prefix, path, value = _object_request()
    scope = _scope_from_request(request.args)
    conn = _open()
    try:
        with analysis.scoped(conn, scope):
            _, columns = _object_columns(conn, object_prefix, path, value)
            member_stats = analysis.object_member_stats(conn, object_prefix)
    finally:
        conn.close()
    scope_args = _scope_args(request.args)
    whole_corpus_url = (
        url_for("analysis.object_page", path=path)
        if value is None
        else url_for("analysis.object_page", path=path, value=value)
    )
    return render_template(
        "analysis_object.html",
        path=path,
        value=value,
        object_prefix=object_prefix,
        columns=columns,
        member_stats=member_stats,
        show_record_shape=value is None,
        scope_args=scope_args,
        scope_qs=urlencode(scope_args),
        whole_corpus_url=whole_corpus_url,
    )


@analysis_bp.get("/object/data")
def object_data():
    """DataTables server-side endpoint for the parent-object drill."""
    object_prefix, path, value = _object_request()
    scope = _scope_from_request(request.args)
    # Recompute member columns (deterministic order) so the order-by whitelist and the
    # SQL pivot stay in lockstep -- under the same scope as the data rows.
    conn = _open()
    try:
        with analysis.scoped(conn, scope):
            member_paths = analysis.object_columns(
                conn, object_prefix, value_path=path, value=value
            )
    finally:
        conn.close()
    display_cols = ["timdex_record_id"] + [f"c{i}" for i in range(len(member_paths))]
    return _table_data(
        display_cols,
        lambda conn, search, oc, od, limit, offset: analysis.object_rows(
            conn, object_prefix, member_paths, value_path=path, value=value,
            search=search, order_col=oc, order_dir=od, limit=limit, offset=offset,
        ),
    )


@analysis_bp.get("/object/records/data")
def object_records_data():
    """DataTables endpoint for the per-record shape table of an object field."""
    object_prefix, _path, _value = _object_request()
    display_cols = ["timdex_record_id", "objects", "total_values", "max_per_object"]
    return _table_data(
        display_cols,
        lambda conn, search, oc, od, limit, offset: analysis.object_record_shape(
            conn, object_prefix, search=search, order_col=oc, order_dir=od,
            limit=limit, offset=offset,
        ),
    )


@analysis_bp.get("/records")
def value_records_page() -> str:
    """Records that carry a given value at a given path (value -> records drill)."""
    scope_args = _scope_args(request.args)
    path = request.args.get("path", default="", type=str)
    value = request.args.get("value", default="", type=str)
    return render_template(
        "analysis_records.html",
        path=path,
        value=value,
        columns=analysis.VALUE_RECORD_COLUMNS,
        scope_args=scope_args,
        scope_qs=urlencode(scope_args),
        whole_corpus_url=url_for("analysis.value_records_page", path=path, value=value),
    )


@analysis_bp.get("/records/data")
def value_records_data():
    """DataTables server-side endpoint for the value -> records drill."""
    path = request.args.get("path", default="", type=str)
    value = request.args.get("value", default="", type=str)
    return _table_data(
        analysis.VALUE_RECORD_COLUMNS,
        lambda conn, search, oc, od, limit, offset: analysis.value_records(
            conn, path, value, search=search, order_col=oc, order_dir=od,
            limit=limit, offset=offset,
        ),
    )
