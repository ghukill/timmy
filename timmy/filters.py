"""Flask-free construction of TDA record filters and their human labels.

The web build (``analysis_views``) and the CLI build (``timmy analysis build``)
both turn typed user input into TDA's ``(where, **filters)`` model plus a short
label. Keeping that logic here -- with no Flask import -- is what lets the two
surfaces produce identical analyses from the same filter, instead of drifting.
"""

from __future__ import annotations

from typing import Any

# Metadata columns that accept a list of values (TDA typed filters / IN-lists).
# Shared by the browse query and the analysis build.
IN_FILTER_COLUMNS = [
    "source",
    "run_type",
    "action",
    "run_id",
]

# Columns the free-text search expands across (OR of ILIKEs). Cast to varchar so
# the match works regardless of the underlying column type.
SEARCHABLE_COLUMNS = [
    "timdex_record_id",
    "source",
    "run_type",
    "action",
    "run_id",
]


def split_csv(value: str) -> list[str]:
    """Split a comma-separated filter value into a clean list of terms."""
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def sql_literal(value: str) -> str:
    """Quote a string as a SQL literal (single quotes doubled)."""
    return "'" + value.replace("'", "''") + "'"


def build_tda_filter(
    filters: dict[str, Any],
    *,
    search: str | None = None,
    where: str | None = None,
) -> tuple[str | None, dict[str, Any]]:
    """Combine typed ``filters`` + a free-text ``search`` + a raw ``where`` into
    TDA's ``(where, **filters)`` model.

    ``filters`` are TDA typed filters (bound safely by TDA). ``search`` expands to
    a raw OR-of-ILIKEs across :data:`SEARCHABLE_COLUMNS`, and ``where`` is a raw
    predicate; both are trusted power-tool inputs -- the same semantics as the
    browse view, and harmless here because the analysis read is the user's own.
    """
    where_parts: list[str] = []
    if search and search.strip():
        term = sql_literal(f"%{search.strip()}%")
        ors = " or ".join(
            f"cast({col} as varchar) ilike {term}" for col in SEARCHABLE_COLUMNS
        )
        where_parts.append(f"({ors})")
    if where and where.strip():
        where_parts.append(f"({where.strip()})")
    combined = " and ".join(where_parts) if where_parts else None
    return combined, dict(filters)


def filter_label(
    where: str | None, filters: dict[str, Any], limit: int | None = None
) -> str:
    """A short human label for an analysis, derived from its filter."""
    parts = [
        f"{key}={','.join(val) if isinstance(val, list) else val}"
        for key, val in filters.items()
    ]
    if where:
        parts.append("custom where")
    if limit is not None:
        parts.append(f"limit={limit}")
    return "; ".join(parts) if parts else "all current records"
