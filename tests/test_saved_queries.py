"""Tests for the JSON-backed saved-query store (``timmy.analysis.queries``).

Covers the CRUD contract the SQL console relies on: first-access seeding of the
defaults, Save-as create (with duplicate rejection), Save update (with missing-name
rejection), delete, and that the file round-trips so saved queries survive a process
restart (and a corpus rebuild -- they live outside corpus.duckdb).

No pytest in the repo, so this doubles as a runnable script::

    .venv/bin/python tests/test_saved_queries.py
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from timmy.analysis import queries


def test_seeds_defaults_on_first_access() -> None:
    with tempfile.TemporaryDirectory() as d:
        got = queries.list_queries(d)
        assert len(got) == len(queries.DEFAULT_QUERIES), got
        names = {q["name"] for q in got}
        assert names == {q["name"] for q in queries.DEFAULT_QUERIES}
        # The file now exists and is valid JSON in the expected shape.
        path = Path(d) / queries.SAVED_QUERIES_FILENAME
        assert path.exists()
        data = json.loads(path.read_text())
        assert "queries" in data and len(data["queries"]) == len(got)
        # Seeded entries are stamped.
        assert all(q.get("created_at") and q.get("updated_at") for q in got)


def test_save_as_creates_and_rejects_duplicates() -> None:
    with tempfile.TemporaryDirectory() as d:
        queries.list_queries(d)  # seed
        saved = queries.save_query(
            d, "Blank titles", "QA: records with no title", "select 1;", create=True
        )
        assert saved["name"] == "Blank titles"
        assert any(q["name"] == "Blank titles" for q in queries.list_queries(d))

        # Duplicate name on create is rejected.
        try:
            queries.save_query(d, "Blank titles", "x", "select 2;", create=True)
        except ValueError:
            pass
        else:
            raise AssertionError("expected ValueError on duplicate create")

        # Empty name is rejected.
        try:
            queries.save_query(d, "   ", "x", "select 3;", create=True)
        except ValueError:
            pass
        else:
            raise AssertionError("expected ValueError on empty name")


def test_save_updates_existing_and_rejects_missing() -> None:
    with tempfile.TemporaryDirectory() as d:
        queries.list_queries(d)  # seed
        updated = queries.save_query(
            d,
            "Field usage ranking",
            "edited description",
            "select 'edited';",
            create=False,
        )
        assert updated["description"] == "edited description"
        assert updated["sql"] == "select 'edited';"
        fetched = queries.get_query(d, "Field usage ranking")
        assert fetched["sql"] == "select 'edited';"

        # Updating a name that doesn't exist is rejected.
        try:
            queries.save_query(d, "Nope", "x", "select 4;", create=False)
        except ValueError:
            pass
        else:
            raise AssertionError("expected ValueError updating a missing query")


def test_delete_and_roundtrip() -> None:
    with tempfile.TemporaryDirectory() as d:
        queries.list_queries(d)  # seed
        assert queries.delete_query(d, "Value distribution") is True
        assert queries.get_query(d, "Value distribution") is None
        assert queries.delete_query(d, "Value distribution") is False  # already gone

        # A fresh read of the same dir reflects the deletion (file round-trips).
        remaining = {q["name"] for q in queries.list_queries(d)}
        assert "Value distribution" not in remaining
        assert "Field usage ranking" in remaining


def main() -> None:
    test_seeds_defaults_on_first_access()
    test_save_as_creates_and_rejects_duplicates()
    test_save_updates_existing_and_rejects_missing()
    test_delete_and_roundtrip()
    print("ok: all saved-query store tests passed")


if __name__ == "__main__":
    main()
