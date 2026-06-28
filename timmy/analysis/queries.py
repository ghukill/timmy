"""Saved SQL queries for the analysis SQL console.

A tiny JSON-backed library of named, described SQL snippets. It lives **next to**
the corpus (``saved_queries.json`` in the analyses dir) but deliberately *outside*
``corpus.duckdb``: a corpus rebuild/delete must not take a user's saved queries with
it. The store is Flask-free (like the rest of :mod:`timmy.analysis`) so it stays
usable from the CLI, and a module-level lock plus atomic write make it safe for the
threaded web app -- the same reason :data:`timmy.dataset.dataset_lock` exists.

A query is just ``{name, description, sql, created_at, updated_at}``; ``name`` is the
identity key (unique). Scope is not captured -- callers note any intended scope in
the description prose. On first access the file is seeded with :data:`DEFAULT_QUERIES`
(the examples that used to be hardcoded in the dashboard template) so the console
ships with inspiration that is itself editable/deletable.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

# Sits beside corpus.duckdb in the analyses dir, but survives a corpus rebuild/delete.
SAVED_QUERIES_FILENAME = "saved_queries.json"

# Serializes the read-modify-write cycle across threaded web requests.
_lock = threading.Lock()

# Seeded on first access. These are the four examples that used to live as hardcoded
# buttons in analysis_detail.html; here they are ordinary (editable) saved queries.
DEFAULT_QUERIES: list[dict[str, str]] = [
    {
        "name": "Field usage ranking",
        "description": "Every path ranked by how many docs use it (and total "
        "occurrences) -- the schema overview as raw SQL.",
        "sql": (
            "select path,\n"
            "       count(*) as occurrences,\n"
            "       count(distinct timdex_composite_id) as docs\n"
            "from eav\n"
            "group by path\n"
            "order by docs desc\n"
            "limit 50;"
        ),
    },
    {
        "name": "Coverage of a field",
        "description": "How many docs populate one path, against the total doc count "
        "(edit the path).",
        "sql": (
            "select count(distinct timdex_composite_id) as uses,\n"
            "       (select count(*) from docs) as total_docs\n"
            "from eav\n"
            "where path = 'contributors[].value';"
        ),
    },
    {
        "name": "Value distribution",
        "description": "Distinct values at one path, ranked by doc count -- a quick "
        "look at a controlled field (edit the path).",
        "sql": (
            "select value, count(distinct timdex_composite_id) as docs\n"
            "from eav\n"
            "where path = 'contributors[].kind'\n"
            "group by value\n"
            "order by docs desc\n"
            "limit 50;"
        ),
    },
    {
        "name": "Records using a field",
        "description": "Records carrying a value at one path, with their run keys "
        "(timdex_record_id links to the record; edit the path).",
        "sql": (
            "select d.timdex_record_id, d.run_id, d.run_record_offset,\n"
            "       d.source, e.value\n"
            "from eav e join docs d using (timdex_composite_id)\n"
            "where e.path = 'contributors[].value'\n"
            "limit 100;"
        ),
    },
]


def _path(analyses_dir: str | os.PathLike[str]) -> Path:
    return Path(analyses_dir) / SAVED_QUERIES_FILENAME


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read(analyses_dir: str | os.PathLike[str]) -> list[dict]:
    """Load the stored queries, seeding the file with the defaults on first access."""
    path = _path(analyses_dir)
    if not path.exists():
        seeded = [{**q, "created_at": _now(), "updated_at": _now()} for q in DEFAULT_QUERIES]
        _write(analyses_dir, seeded)
        return seeded
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("queries", [])


def _write(analyses_dir: str | os.PathLike[str], queries: list[dict]) -> None:
    """Atomically replace the store file (write a temp sibling, then rename)."""
    path = _path(analyses_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps({"queries": queries}, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def list_queries(analyses_dir: str | os.PathLike[str]) -> list[dict]:
    """All saved queries, name-sorted (case-insensitive)."""
    with _lock:
        queries = _read(analyses_dir)
    return sorted(queries, key=lambda q: q["name"].lower())


def get_query(analyses_dir: str | os.PathLike[str], name: str) -> dict | None:
    """The saved query with this name, or ``None``."""
    with _lock:
        for query in _read(analyses_dir):
            if query["name"] == name:
                return query
    return None


def save_query(
    analyses_dir: str | os.PathLike[str],
    name: str,
    description: str,
    sql: str,
    *,
    create: bool,
) -> dict:
    """Upsert a saved query.

    ``create=True`` ("Save as") adds a new query and rejects a name that already
    exists. ``create=False`` ("Save") updates the description/sql of an existing
    query and rejects a name that does not exist. Raises :class:`ValueError` on an
    empty name or a name/mode mismatch.
    """
    name = (name or "").strip()
    if not name:
        raise ValueError("Query name is required.")
    with _lock:
        queries = _read(analyses_dir)
        existing = next((q for q in queries if q["name"] == name), None)
        if create and existing is not None:
            raise ValueError(f"A query named {name!r} already exists.")
        if not create and existing is None:
            raise ValueError(f"No saved query named {name!r} to update.")
        if existing is not None:
            existing["description"] = description
            existing["sql"] = sql
            existing["updated_at"] = _now()
            saved = existing
        else:
            saved = {
                "name": name,
                "description": description,
                "sql": sql,
                "created_at": _now(),
                "updated_at": _now(),
            }
            queries.append(saved)
        _write(analyses_dir, queries)
    return saved


def delete_query(analyses_dir: str | os.PathLike[str], name: str) -> bool:
    """Remove a saved query by name; ``True`` if one was removed."""
    with _lock:
        queries = _read(analyses_dir)
        remaining = [q for q in queries if q["name"] != name]
        if len(remaining) == len(queries):
            return False
        _write(analyses_dir, remaining)
    return True
