"""Materialize a TIMDEX metadata analysis as a standalone DuckDB file.

Each analysis is one self-contained ``<analysis_id>.duckdb`` file holding three
tables:

- ``docs``     -- one row per analyzed record version (the dimension).
- ``eav``      -- the flattened transformed payload, one row per leaf
  (see :mod:`timmy.analysis.flatten`).
- ``manifest`` -- a single row describing how the analysis was built (the
  filter predicate, source dataset, counts, timestamps) so the artifact is
  self-describing and reproducible.

Keeping each analysis in its own file makes it immutable, portable, and trivial
to drop; comparisons across analyses are opt-in via DuckDB ``ATTACH``. The build
reads transformed payloads through TDA's metadata-driven read path, so it must
run while the caller holds the app's ``dataset_lock`` (the Flask route does
this); the write side uses an independent DuckDB connection to the analysis file
and needs no such coordination.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import duckdb

from timmy.analysis.flatten import flatten, make_timdex_composite_id

if TYPE_CHECKING:
    from timdex_dataset_api import TIMDEXDataset

logger = logging.getLogger(__name__)

# Columns pulled per record: the identity/metadata for `docs` plus the payload
# we flatten. transformed_record arrives as the raw JSON text TDA stored.
READ_COLUMNS = [
    "timdex_record_id",
    "source",
    "run_id",
    "run_record_offset",
    "transformed_record",
]

# Records are profiled in their current form by default.
DEFAULT_TABLE = "current_records"

# Top-level transformed fields dropped before flattening: provenance/bookkeeping
# that isn't descriptive content worth profiling. Removing the key means these
# never produce EAV rows, so they're absent from every downstream view.
EXCLUDED_FIELDS = frozenset({"timdex_provenance"})

# Rows are flushed to the analysis DB every this many docs to bound memory.
BUILD_FLUSH_EVERY = 2000

SCHEMA_SQL = """
create table docs (
    timdex_composite_id text primary key,
    source text,
    timdex_record_id text,
    run_id text,
    run_record_offset bigint
);

create table eav (
    timdex_composite_id text,
    path text,
    path_indexed text,
    value text,
    value_type text
);

create table manifest (
    analysis_id text,
    created_at timestamp,
    dataset_location text,
    table_name text,
    where_predicate text,
    filters_json text,
    label text,
    doc_count bigint,
    eav_count bigint,
    skipped_count bigint,
    name text,
    notes text
);
"""


def new_analysis_id() -> str:
    """A human-sortable, collision-resistant id (also the DB filename stem)."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{stamp}-{uuid4().hex[:8]}"


def analysis_path(analyses_dir: str | os.PathLike[str], analysis_id: str) -> Path:
    """Resolve the on-disk path of an analysis DB by id."""
    return Path(analyses_dir) / f"{analysis_id}.duckdb"


def build_analysis(
    dataset: TIMDEXDataset,
    analyses_dir: str | os.PathLike[str],
    *,
    where: str | None = None,
    table: str = DEFAULT_TABLE,
    limit: int | None = None,
    label: str | None = None,
    name: str | None = None,
    notes: str | None = None,
    **filters: Any,
) -> dict[str, Any]:
    """Build one analysis DB from records matching a filter; return its manifest.

    ``where`` is a raw SQL predicate and ``**filters`` are TDA typed filters
    (e.g. ``source="libguides"``); both are forwarded to TDA's read path and
    recorded in the manifest. ``limit`` caps how many records TDA reads (its own
    ``read_dicts_iter`` limit). Records with no ``transformed_record`` (e.g. a
    current version whose latest action is a delete) contribute nothing and are
    counted under ``skipped_count``.

    The build writes to a ``.building`` temp file and atomically renames it into
    place on success, so a failed build never leaves a half-written artifact.
    """
    analyses_dir = Path(analyses_dir)
    analyses_dir.mkdir(parents=True, exist_ok=True)

    analysis_id = new_analysis_id()
    final_path = analysis_path(analyses_dir, analysis_id)
    building_path = final_path.with_suffix(".duckdb.building")
    building_path.unlink(missing_ok=True)

    created_at = datetime.now(timezone.utc)
    doc_count = eav_count = skipped_count = 0

    con = duckdb.connect(str(building_path))
    try:
        con.execute(SCHEMA_SQL)

        doc_rows: list[tuple] = []
        eav_rows: list[tuple] = []

        def flush() -> None:
            if doc_rows:
                con.executemany(
                    "insert into docs values (?, ?, ?, ?, ?)", doc_rows
                )
                doc_rows.clear()
            if eav_rows:
                con.executemany(
                    "insert into eav values (?, ?, ?, ?, ?)", eav_rows
                )
                eav_rows.clear()

        for rec in dataset.records.read_dicts_iter(
            table=table,
            columns=READ_COLUMNS,
            where=where,
            limit=limit,
            **filters,
        ):
            payload = rec.get("transformed_record")
            if not payload:
                skipped_count += 1
                continue

            parsed = json.loads(payload)
            for field in EXCLUDED_FIELDS:
                parsed.pop(field, None)

            composite_id = make_timdex_composite_id(
                rec["timdex_record_id"], rec["run_id"], rec["run_record_offset"]
            )
            doc_rows.append(
                (
                    composite_id,
                    rec["source"],
                    rec["timdex_record_id"],
                    rec["run_id"],
                    rec["run_record_offset"],
                )
            )
            for leaf in flatten(parsed):
                eav_rows.append(
                    (composite_id, leaf.path, leaf.path_indexed, leaf.value, leaf.value_type)
                )
                eav_count += 1

            doc_count += 1
            if doc_count % BUILD_FLUSH_EVERY == 0:
                flush()

        flush()

        # Index the GROUP BY key now that all rows are in.
        con.execute("create index eav_path_idx on eav (path)")

        con.execute(
            "insert into manifest values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                analysis_id,
                created_at,
                str(dataset.location),
                table,
                where,
                json.dumps(filters, default=str),
                label,
                doc_count,
                eav_count,
                skipped_count,
                name,
                notes,
            ],
        )
    except Exception:
        con.close()
        building_path.unlink(missing_ok=True)
        raise
    con.close()

    os.replace(building_path, final_path)
    logger.info(
        "Built analysis %s: %d docs, %d eav rows, %d skipped",
        analysis_id,
        doc_count,
        eav_count,
        skipped_count,
    )
    return read_manifest(analyses_dir, analysis_id)


# Per-path coverage stats for the field-usage report. mode() picks the dominant
# value_type for a path; distinct value count ignores nulls; sample is a
# deterministic non-null value.
FIELD_USAGE_SQL = """
select
    path,
    mode() within group (order by value_type) as value_type,
    count(distinct timdex_composite_id) as doc_count,
    count(distinct value) as distinct_values,
    count(value) as value_count,
    min(value) filter (where value is not null) as sample_value
from eav
group by path
order by doc_count desc, path
"""


def field_usage(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Per-path coverage report rows for an open analysis connection.

    Coverage is the share of the corpus's docs that have at least one value at
    that path (``doc_count / total docs``), so 100% means every doc populates
    the field.
    """
    total = conn.execute("select count(*) from docs").fetchone()[0]
    report = []
    for (
        path,
        value_type,
        doc_count,
        distinct_values,
        value_count,
        sample_value,
    ) in conn.execute(FIELD_USAGE_SQL).fetchall():
        report.append(
            {
                "path": path,
                "value_type": value_type,
                "doc_count": doc_count,
                "distinct_values": distinct_values,
                "value_count": value_count,
                "sample_value": sample_value,
                "coverage_pct": round(100 * doc_count / total, 1) if total else 0.0,
                # Share of values that are distinct: ~100% = free text /
                # uncontrolled, low = repeated / controlled vocabulary.
                "pct_unique": (
                    round(100 * distinct_values / value_count, 1)
                    if value_count
                    else None
                ),
            }
        )
    return report


# Columns for the path-scoped value-frequency table (server-side paginated).
PATH_VALUE_COLUMNS = ["path", "value", "documents", "occurrences", "pct_of_path"]

# Columns for the value -> records drill (server-side paginated).
VALUE_RECORD_COLUMNS = ["timdex_record_id", "source", "run_id", "run_record_offset"]

# Identity columns carried alongside the dynamic member-field columns in the
# object table (the first is rendered as a link to the record).
OBJECT_IDENTITY_COLUMNS = ["timdex_record_id", "run_id", "run_record_offset"]

# Strip everything after the last ``]`` to get a leaf's array-element prefix:
# ``subjects[3].kind`` -> ``subjects[3]`` (the object instance it belongs to). The
# same expression on a collapsed ``path`` yields the collapsed prefix
# (``subjects[].kind`` -> ``subjects[]``). Leaves with no ``]`` have no parent
# object.
_ELEM_EXPR = r"regexp_replace({col}, '\][^\]]*$', ']')"

# A leaf is "in" an object instance when its path_indexed equals the element
# prefix or is boundary-nested under it (next char is ``.`` or ``[``). This prefix
# match -- not equality of stripped forms -- is what lets a deeper member field
# (subjects[3].value[0]) join back to the matched object (subjects[3]); see
# scratch/ideas.md sec.8.
def _under_elem(leaf_col: str, elem_col: str) -> str:
    return (
        f"({leaf_col} = {elem_col}"
        f" or starts_with({leaf_col}, {elem_col} || '.')"
        f" or starts_with({leaf_col}, {elem_col} || '['))"
    )


def _collapsed_prefix(path: str) -> str:
    """Collapsed array-element prefix of a path: keep up to the last ``]``.

    ``dates[].kind`` -> ``dates[]``; a path with no ``]`` has no parent object.
    """
    i = path.rfind("]")
    return path[: i + 1] if i != -1 else ""


def object_field_paths(conn: duckdb.DuckDBPyConnection) -> set[str]:
    """Collapsed paths that are members of a complex (object) parent field.

    These are the paths worth offering an "object" drill on -- a path qualifies
    when it shares an array element with some other distinct path, i.e. its parent
    is a multi-key object (``subjects[].kind`` qualifies because
    ``subjects[].value[]`` also lives under ``subjects[]``). Computed over the
    *distinct* paths only (not every row), so it stays cheap regardless of corpus
    size.
    """
    rows = conn.execute(
        f"""
        with paths as (select distinct path from eav where path like '%]%'),
             elems as (select path, {_ELEM_EXPR.format(col="path")} as elem
                       from paths)
        select distinct a.path
        from elems a
        join paths q
          on q.path <> a.path and {_under_elem("q.path", "a.elem")}
        """  # noqa: S608 -- only constant SQL text is interpolated
    ).fetchall()
    return {r[0] for r in rows}


def _instance_expr(col: str, depth: int) -> str:
    r"""SQL truncating a ``path_indexed`` to its first ``depth`` array indices.

    ``depth`` is the bracket count of the object prefix; the result is the
    object-instance key at that level -- ``dates[3].value`` at depth 1 ->
    ``dates[3]``, and a nested member ``subjects[3].value[0]`` at depth 1 ->
    ``subjects[3]`` too, so members join to their instance by equality. A path with
    fewer than ``depth`` brackets fails the match and is returned unchanged, so it
    can never collide with a real instance key.
    """
    return rf"regexp_replace({col}, '^((?:[^\]]*\]){{{depth}}}).*$', '\1')"


def _object_hits(
    object_prefix: str, value_path: str | None, value: str | None
) -> tuple[str, list]:
    """(SQL, params) for the ``hits`` CTE: the object instances to profile.

    Filtered (``value_path`` + ``value`` given): instances containing that leaf
    value. Unfiltered: every instance under ``object_prefix``.
    """
    inst = _instance_expr("path_indexed", object_prefix.count("]"))
    if value_path is not None and value is not None:
        return (
            f"select distinct timdex_composite_id, {inst} as elem "  # noqa: S608
            "from eav where path = ? and value = ?",
            [value_path, value],
        )
    scope_sql, scope_params = _scope_clause(object_prefix)
    return (
        f"select distinct timdex_composite_id, {inst} as elem "  # noqa: S608
        f"from eav where {scope_sql}",
        list(scope_params),
    )


def object_columns(
    conn: duckdb.DuckDBPyConnection,
    object_prefix: str,
    *,
    value_path: str | None = None,
    value: str | None = None,
) -> list[str]:
    """Ordered distinct member-field paths of the objects under ``object_prefix``.

    With ``value_path`` + ``value``, scoped to instances carrying that value;
    otherwise every instance under the prefix (the unfiltered object view). Returns
    all collapsed leaf paths living in those objects -- the dynamic columns of the
    object table; ordered by path so the page and data endpoints agree.
    """
    leaf_inst = _instance_expr("e.path_indexed", object_prefix.count("]"))
    hits_sql, hit_params = _object_hits(object_prefix, value_path, value)
    rows = conn.execute(
        f"""
        with hits as ({hits_sql})
        select distinct e.path
        from eav e
        join hits h
          on e.timdex_composite_id = h.timdex_composite_id
         and {leaf_inst} = h.elem
        order by e.path
        """,  # noqa: S608 -- only constant SQL text is interpolated
        hit_params,
    ).fetchall()
    return [r[0] for r in rows]


def object_rows(
    conn: duckdb.DuckDBPyConnection,
    object_prefix: str,
    member_paths: list[str],
    *,
    value_path: str | None = None,
    value: str | None = None,
    search: str = "",
    order_col: str = "timdex_record_id",
    order_dir: str = "asc",
    limit: int = 25,
    offset: int = 0,
) -> tuple[int, int, list[dict[str, Any]]]:
    """Pivoted object table: one row per object instance under ``object_prefix``.

    With ``value_path`` + ``value`` the instances are scoped to those carrying that
    value; otherwise every instance under the prefix. Each ``member_paths`` entry
    becomes a ``c{i}`` column holding that field's value(s) within the object
    (multiple leaves -- e.g. ``value[]`` -- joined with `` | ``). Identity columns
    key each row back to its record version. Returns ``(total, filtered, rows)``.
    """
    value_cols = [f"c{i}" for i in range(len(member_paths))]
    all_cols = OBJECT_IDENTITY_COLUMNS + value_cols

    leaf_inst = _instance_expr("e.path_indexed", object_prefix.count("]"))
    # One string_agg per member field; the path is bound as a parameter.
    pivots = ",\n".join(
        f"string_agg(case when e.path = ? then e.value end, ' | ') as {col}"
        for col in value_cols
    )
    pivot_params = list(member_paths)
    hits_sql, hit_params = _object_hits(object_prefix, value_path, value)

    base = f"""
        with hits as ({hits_sql}),
        elems as (
            select e.timdex_composite_id, h.elem,
                   d.timdex_record_id, d.run_id, d.run_record_offset,
                   {pivots}
            from eav e
            join hits h
              on e.timdex_composite_id = h.timdex_composite_id
             and {leaf_inst} = h.elem
            join docs d on d.timdex_composite_id = e.timdex_composite_id
            group by e.timdex_composite_id, h.elem,
                     d.timdex_record_id, d.run_id, d.run_record_offset
        )
        select {", ".join(all_cols)} from elems
    """  # noqa: S608 -- columns are constant; the prefix/value are parameters
    # Param order follows the SQL text: hits params first, then each pivot path.
    base_params = [*hit_params, *pivot_params]

    total = conn.execute(
        f"select count(*) from ({base})", base_params  # noqa: S608
    ).fetchone()[0]

    params = list(base_params)
    search_sql = ""
    if search:
        cols_for_search = ["timdex_record_id", *value_cols]
        ors = " or ".join(f"{c} ilike ?" for c in cols_for_search)
        search_sql = f" where {ors}"
        params += [f"%{search}%"] * len(cols_for_search)
    filtered = conn.execute(
        f"select count(*) from ({base}){search_sql}", params  # noqa: S608
    ).fetchone()[0]

    if order_col not in all_cols:
        order_col = "timdex_record_id"
    direction = "desc" if order_dir == "desc" else "asc"
    rows = conn.execute(
        f"select * from ({base}){search_sql} "  # noqa: S608
        # secondary sort by element keeps a record's elements grouped together
        f"order by {order_col} {direction}, timdex_record_id, run_record_offset "
        f"limit ? offset ?",
        [*params, limit, offset],
    ).fetchall()
    return (
        total,
        filtered,
        [dict(zip(all_cols, r, strict=True)) for r in rows],
    )


def object_field_summaries(
    conn: duckdb.DuckDBPyConnection,
) -> list[dict[str, Any]]:
    """One summary per complex (object) field, for the field-usage report.

    Each entry is a navigational parent -- the collapsed object prefix (e.g.
    ``dates[]``), how many docs carry it, how many object instances exist, and its
    member-field paths -- so the report can surface the whole object as a single
    clickable row above its member leaves.
    """
    prefixes = sorted({_collapsed_prefix(p) for p in object_field_paths(conn)})
    summaries = []
    for prefix in prefixes:
        scope_sql, scope_params = _scope_clause(prefix)
        inst = _instance_expr("path_indexed", prefix.count("]"))
        doc_count = conn.execute(
            f"select count(distinct timdex_composite_id) from eav "  # noqa: S608
            f"where {scope_sql}",
            scope_params,
        ).fetchone()[0]
        instance_count = conn.execute(
            f"select count(*) from (select distinct timdex_composite_id, "  # noqa: S608
            f"{inst} as elem from eav where {scope_sql})",
            scope_params,
        ).fetchone()[0]
        summaries.append(
            {
                "object_prefix": prefix,
                "doc_count": doc_count,
                "instance_count": instance_count,
                "members": object_columns(conn, prefix),
            }
        )
    return summaries


def _scope_clause(prefix: str) -> tuple[str, list]:
    """SQL predicate + params matching a path and its descendants.

    Boundary-aware via ``starts_with`` against the next path separator (``[`` or
    ``.``), so ``subjects`` never matches a hypothetical ``subjectsx`` and we
    avoid LIKE-wildcard escaping entirely. An empty prefix matches every path.
    """
    if not prefix:
        return "true", []
    return (
        "(path = ? or starts_with(path, ? || '[') or starts_with(path, ? || '.'))",
        [prefix, prefix, prefix],
    )


def path_values(
    conn: duckdb.DuckDBPyConnection,
    prefix: str,
    *,
    search: str = "",
    order_col: str = "documents",
    order_dir: str = "desc",
    limit: int = 25,
    offset: int = 0,
) -> tuple[int, int, list[dict[str, Any]]]:
    """Distinct values (with counts) for every path under ``prefix``.

    Returns ``(total, filtered, rows)`` for server-side pagination. ``pct_of_path``
    is a value's share of its own path's non-null values -- the controlled-vocab
    signal -- computed before the search filter so it stays absolute.
    """
    scope_sql, scope_params = _scope_clause(prefix)
    base = f"""
        select path, value,
               count(distinct timdex_composite_id) as documents,
               count(*) as occurrences
        from eav
        where {scope_sql} and value is not null
        group by path, value
    """  # noqa: S608 -- scope_sql is constant text; values are parameterized
    total = conn.execute(f"select count(*) from ({base})", scope_params).fetchone()[0]  # noqa: S608

    with_pct = f"""
        with grp as ({base})
        select path, value, documents, occurrences,
               round(100.0 * occurrences / sum(occurrences) over (partition by path), 1)
                   as pct_of_path
        from grp
    """  # noqa: S608
    params = list(scope_params)
    search_sql = ""
    if search:
        search_sql = " where cast(value as varchar) ilike ? or path ilike ?"
        params += [f"%{search}%", f"%{search}%"]

    filtered = conn.execute(
        f"select count(*) from ({with_pct}){search_sql}", params  # noqa: S608
    ).fetchone()[0]

    if order_col not in PATH_VALUE_COLUMNS:
        order_col = "documents"
    direction = "desc" if order_dir == "desc" else "asc"
    rows = conn.execute(
        f"select * from ({with_pct}){search_sql} "  # noqa: S608
        f"order by {order_col} {direction} limit ? offset ?",
        [*params, limit, offset],
    ).fetchall()
    return (
        total,
        filtered,
        [dict(zip(PATH_VALUE_COLUMNS, r, strict=True)) for r in rows],
    )


def value_records(
    conn: duckdb.DuckDBPyConnection,
    path: str,
    value: str,
    *,
    search: str = "",
    order_col: str = "timdex_record_id",
    order_dir: str = "asc",
    limit: int = 25,
    offset: int = 0,
) -> tuple[int, int, list[dict[str, Any]]]:
    """Records that carry a given ``value`` at a given ``path``.

    Returns ``(total, filtered, rows)`` for server-side pagination.
    """
    base = """
        select d.timdex_record_id, d.source, d.run_id, d.run_record_offset
        from eav e join docs d using (timdex_composite_id)
        where e.path = ? and e.value = ?
    """
    base_params = [path, value]
    total = conn.execute(
        f"select count(*) from ({base})", base_params  # noqa: S608
    ).fetchone()[0]

    params = list(base_params)
    search_sql = ""
    if search:
        search_sql = " where timdex_record_id ilike ? or source ilike ?"
        params += [f"%{search}%", f"%{search}%"]
    filtered = conn.execute(
        f"select count(*) from ({base}){search_sql}", params  # noqa: S608
    ).fetchone()[0]

    if order_col not in VALUE_RECORD_COLUMNS:
        order_col = "timdex_record_id"
    direction = "desc" if order_dir == "desc" else "asc"
    rows = conn.execute(
        f"select * from ({base}){search_sql} "  # noqa: S608
        f"order by {order_col} {direction} limit ? offset ?",
        [*params, limit, offset],
    ).fetchall()
    return (
        total,
        filtered,
        [dict(zip(VALUE_RECORD_COLUMNS, r, strict=True)) for r in rows],
    )


def open_analysis(
    analyses_dir: str | os.PathLike[str],
    analysis_id: str,
    *,
    read_only: bool = True,
) -> duckdb.DuckDBPyConnection:
    """Open a connection to an analysis DB (read-only by default).

    Read-only is the right default for serving queries: the artifact is
    immutable, and the engine itself then rejects any DDL/DML, which is what
    makes the user-facing SQL console safe to expose without sanitizing input.
    """
    path = analysis_path(analyses_dir, analysis_id)
    if not path.exists():
        raise FileNotFoundError(f"No analysis DB at {path}")
    return duckdb.connect(str(path), read_only=read_only)


def read_manifest(
    analyses_dir: str | os.PathLike[str], analysis_id: str
) -> dict[str, Any]:
    """Return the manifest row of an analysis as a dict."""
    con = open_analysis(analyses_dir, analysis_id)
    try:
        cols = [d[0] for d in con.execute("select * from manifest").description]
        row = con.execute("select * from manifest").fetchone()
    finally:
        con.close()
    return dict(zip(cols, row, strict=True)) if row else {}


def update_manifest(
    analyses_dir: str | os.PathLike[str],
    analysis_id: str,
    *,
    name: str | None = None,
    notes: str | None = None,
) -> None:
    """Set the user-facing name/notes on an analysis (opens read-write).

    Tolerates older analysis DBs built before these columns existed by adding
    them if missing.
    """
    con = open_analysis(analyses_dir, analysis_id, read_only=False)
    try:
        con.execute("alter table manifest add column if not exists name text")
        con.execute("alter table manifest add column if not exists notes text")
        con.execute("update manifest set name = ?, notes = ?", [name, notes])
    finally:
        con.close()


def delete_analysis(
    analyses_dir: str | os.PathLike[str], analysis_id: str
) -> bool:
    """Delete an analysis DB file. Returns True if it existed.

    Each analysis is a single self-contained file, so removing it removes the
    analysis everywhere -- there is no other state that references it.
    """
    path = analysis_path(analyses_dir, analysis_id)
    existed = path.exists()
    path.unlink(missing_ok=True)
    return existed


def list_analyses(analyses_dir: str | os.PathLike[str]) -> list[dict[str, Any]]:
    """List built analyses (newest first) by reading each DB's manifest."""
    analyses_dir = Path(analyses_dir)
    if not analyses_dir.exists():
        return []
    manifests = []
    for path in analyses_dir.glob("*.duckdb"):
        try:
            manifests.append(read_manifest(analyses_dir, path.stem))
        except Exception:  # noqa: BLE001, S112 -- skip unreadable/partial DBs
            logger.warning("Could not read manifest from %s", path, exc_info=True)
            continue
    manifests.sort(key=lambda m: m.get("created_at") or datetime.min, reverse=True)
    return manifests
