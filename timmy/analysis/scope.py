"""Scoping the single corpus to a subset, without standalone analyses.

A *subset* of the corpus (``source=dspace``, ``run_date > '2026-01-01'``, …) is not a
separate file or a pre-built analysis -- it's a **scope**: a predicate over the corpus's
``docs`` table, applied at query time. The full corpus is just the empty scope.

The mechanism is deliberately invisible to the query layer. :func:`scoped` opens a
``with`` block that creates two connection-local temp views which *shadow* the base
``docs``/``eav`` tables for the duration of the block:

- ``docs`` -> the rows of the real ``docs`` matching the scope predicate;
- ``eav``  -> the real ``eav`` rows whose composite is in that scoped ``docs``.

Because the views take the unqualified names ``docs``/``eav`` (their bodies reference the
qualified base tables via ``current_database()``), every existing query helper in
:mod:`timmy.analysis.store` -- which is written against bare ``docs``/``eav`` -- becomes
scoped with no code change. Leaving the block drops the views and restores the base
tables. An empty scope creates nothing, so the full-corpus path is exactly as before.

Scope vocabulary deliberately mirrors the ``/records`` filter (:mod:`timmy.filters`): a
set of IN-list filters over scope-able ``docs`` columns plus an optional raw ``where``
(a trusted power-tool predicate, e.g. a ``run_date`` range).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from timmy.filters import sql_literal

if TYPE_CHECKING:
    import duckdb

# docs columns that accept an IN-list scope. Mirrors timmy.filters.IN_FILTER_COLUMNS,
# and every name here must be a real, indexed-or-cheap column on `docs`.
SCOPE_COLUMNS = ["source", "run_type", "action", "run_id"]


@dataclass(frozen=True)
class Scope:
    """A subset predicate over the corpus ``docs`` table.

    ``filters`` maps a scope-able column to the values it may take (an IN-list);
    ``where`` is an optional raw predicate over ``docs`` for anything the typed filters
    don't cover (notably ``run_date`` / ``run_timestamp`` ranges). Frozen so a scope can
    be a cache key.
    """

    filters: dict[str, tuple[str, ...]] = field(default_factory=dict)
    where: str | None = None

    def is_empty(self) -> bool:
        """True when this scope selects the whole corpus (no predicate at all)."""
        return not any(self.filters.values()) and not (self.where or "").strip()

    def key(self) -> str:
        """A stable, normalized string identity for caching (order-independent)."""
        if self.is_empty():
            return ""
        norm = {
            col: sorted(vals)
            for col, vals in sorted(self.filters.items())
            if vals
        }
        return json.dumps(
            {"filters": norm, "where": (self.where or "").strip() or None},
            sort_keys=True,
            separators=(",", ":"),
        )

    def compile(self) -> str:
        """Compile to a SQL WHERE body over ``docs`` (``""`` when empty).

        IN-list values are inlined as quoted SQL literals (they're user filter values,
        e.g. source names); the raw ``where`` is passed through as the trusted power-tool
        predicate it is. Unknown filter columns are ignored, so the column set can never
        be attacker-controlled SQL.
        """
        parts: list[str] = []
        for col in SCOPE_COLUMNS:
            vals = self.filters.get(col)
            if vals:
                literals = ", ".join(sql_literal(v) for v in vals)
                parts.append(f"{col} in ({literals})")
        if self.where and self.where.strip():
            parts.append(f"({self.where.strip()})")
        return " and ".join(parts)


# The whole corpus: the canonical empty scope (and its cache key is "").
EMPTY_SCOPE = Scope()


def make_scope(
    filters: dict[str, list[str]] | None = None, where: str | None = None
) -> Scope:
    """Build a :class:`Scope` from loose dict/list input (e.g. parsed request args)."""
    typed = {
        col: tuple(vals)
        for col, vals in (filters or {}).items()
        if col in SCOPE_COLUMNS and vals
    }
    return Scope(filters=typed, where=(where or None))


@contextmanager
def scoped(conn: duckdb.DuckDBPyConnection, scope: Scope) -> Iterator[None]:
    """Within the block, shadow ``docs``/``eav`` with views restricted to ``scope``.

    A no-op for the empty scope (the full corpus). Otherwise the existing query helpers,
    which read bare ``docs``/``eav``, transparently see only the subset. The shadow views
    are connection-local temp views (allowed even on a read-only connection) and are
    dropped on exit, restoring the base tables.
    """
    if scope.is_empty():
        yield
        return

    predicate = scope.compile()
    base = conn.execute("select current_database()").fetchone()[0]
    try:
        conn.execute(
            f"create temp view docs as select * from {base}.docs where {predicate}"
        )
        conn.execute(
            f"create temp view eav as select e.* from {base}.eav e "
            f"semi join docs d using (timdex_composite_id)"
        )
        yield
    finally:
        conn.execute("drop view if exists eav")
        conn.execute("drop view if exists docs")
