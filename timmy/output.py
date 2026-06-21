"""Rendering helpers for CLI read commands: one place that turns analysis
results into either a human table or ``--json``.

Kept Flask-free and dependency-light so every ``timmy analysis`` command shares
the same output contract: stdout is data (a table for humans, stable JSON for
agents), and the JSON shapes mirror the dicts the analysis functions return.
"""

from __future__ import annotations

import csv
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import click

# Cap a table cell so one long value can't blow up the layout. JSON/CSV output is
# never truncated -- only the human table is.
_MAX_CELL = 60


def _json_default(value: Any) -> Any:
    """Coerce non-JSON-native cells (datetimes, Decimals) for json.dumps."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def _cell(value: Any) -> str:
    return "" if value is None else str(value)


def emit_json(payload: Any) -> None:
    """Dump any JSON-able payload to stdout (the agent contract)."""
    click.echo(json.dumps(payload, indent=2, default=_json_default))


def emit_rows(
    rows: list[dict[str, Any]],
    *,
    as_json: bool,
    columns: list[str] | None = None,
) -> None:
    """Render a list of dict rows as a table (human) or JSON (full rows).

    ``columns`` selects/orders the columns shown in the *table* only; ``--json``
    always emits the complete row dicts so agents get every field.
    """
    if as_json:
        emit_json(rows)
        return
    if not rows:
        click.echo("(no rows)", err=True)
        return
    cols = columns or list(rows[0].keys())
    _print_table(cols, rows)


def emit_record(record: dict[str, Any], *, as_json: bool) -> None:
    """Render a single record as a vertical key/value list or a JSON object."""
    if as_json:
        emit_json(record)
        return
    if not record:
        click.echo("(empty)", err=True)
        return
    width = max(len(k) for k in record)
    for key, value in record.items():
        click.echo(f"{key:<{width}}  {_cell(value)}")


def emit_csv(columns: list[str], rows: list[list[Any]]) -> None:
    """Write column-oriented results as CSV to stdout (for ``query --csv``)."""
    writer = csv.writer(sys.stdout)
    writer.writerow(columns)
    for row in rows:
        writer.writerow(["" if v is None else v for v in row])


def _print_table(columns: list[str], rows: list[dict[str, Any]]) -> None:
    def fmt(value: Any) -> str:
        text = _cell(value)
        return text if len(text) <= _MAX_CELL else text[: _MAX_CELL - 1] + "…"

    widths = {c: len(c) for c in columns}
    rendered: list[list[str]] = []
    for row in rows:
        cells = [fmt(row.get(c)) for c in columns]
        rendered.append(cells)
        for c, cell in zip(columns, cells, strict=True):
            widths[c] = max(widths[c], len(cell))

    header = "  ".join(c.ljust(widths[c]) for c in columns)
    click.echo(header)
    click.echo("  ".join("-" * widths[c] for c in columns))
    for cells in rendered:
        click.echo("  ".join(cell.ljust(widths[c]) for c, cell in zip(columns, cells, strict=True)))
