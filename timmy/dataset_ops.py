"""Flask-free orchestration for dataset maintenance actions.

Currently one action: rebuild the dataset's static metadata database. The underlying
``TIMDEXDataset.metadata.rebuild_dataset_metadata()`` takes no progress callback and
returns ``None`` -- it only *logs* per-table progress as it goes. We adapt that log
stream into the ``(phase, done, total)`` contract the dataset job + progress page
expect, and into a structured per-table result for the completion summary.

Kept Flask-free (like :mod:`timmy.analysis` and :mod:`timmy.source_stats`) so the CLI
could trigger the same work, and so the log->progress adapter is unit-testable without
spinning up the app or touching S3.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Callable

# The library logger ``rebuild_dataset_metadata`` emits its progress on.
_TDA_LOGGER = "timdex_dataset_api"

# Per-table log lines we translate into progress (see timdex_dataset_api/metadata.py):
#   debug   "creating table static_db.main.<name>"
#   info    "'<name>' table created - rows: <n>, elapsed: <secs>"
#   warning "Could not create metadata table for '<name>' (no parquet ...). Skipping."
_CREATING_RE = re.compile(r"creating table .*\.(?P<name>\w+)")
_CREATED_RE = re.compile(r"'(?P<name>[^']+)' table created - rows: (?P<rows>\d+)")
_SKIP_RE = re.compile(r"Could not create metadata table for '(?P<name>[^']+)'")

OnProgress = Callable[[str, int, "int | None"], None]


class DatasetRebuildError(RuntimeError):
    """The rebuild finished but produced an empty/degenerate metadata database.

    ``rebuild_dataset_metadata`` reads each data type's parquet and catches *any*
    ``DuckDBIOException`` as "no parquet data" -- so a failed S3 read (auth, network,
    a flaky/empty listing) is silently treated as an empty table, and the library then
    overwrites the canonical ``metadata.duckdb`` with that empty result regardless. We
    can't stop that overwrite from here (it happens inside the library), but we can
    refuse to report it as success when a previously-populated table came back empty.
    """


class _MetadataProgressHandler(logging.Handler):
    """Translate ``rebuild_dataset_metadata``'s log lines into job progress + result.

    Completed/skipped tables drive a determinate bar (total = number of data types),
    and each table's row count is captured for the completion summary. ``tables`` is
    the same list the caller returns as the result, mutated in place.
    """

    def __init__(self, on_progress: OnProgress, total: int, tables: list[dict]) -> None:
        super().__init__()
        self.on_progress = on_progress
        self.total = total
        self.tables = tables
        self.done = 0
        self._by_name = {t["name"]: t for t in tables}

    def _advance(self, table: dict | None) -> None:
        # A table only counts once, even if it somehow logs twice.
        if table is not None and table["status"] == "pending":
            self.done += 1

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = record.getMessage()
        except Exception:  # noqa: BLE001 -- never let logging adaptation break the job
            return

        m = _CREATED_RE.search(msg)
        if m:
            table = self._by_name.get(m.group("name"))
            self._advance(table)
            if table is not None and table["status"] == "pending":
                table["status"] = "built"
                table["rows"] = int(m.group("rows"))
            self.on_progress(f"Built {m.group('name')} table", self.done, self.total)
            return

        m = _SKIP_RE.search(msg)
        if m:
            table = self._by_name.get(m.group("name"))
            self._advance(table)
            if table is not None and table["status"] == "pending":
                table["status"] = "skipped"
            self.on_progress(
                f"Skipped {m.group('name')} (no data)", self.done, self.total
            )
            return

        m = _CREATING_RE.search(msg)
        if m:
            self.on_progress(
                f"Building {m.group('name')} table…", self.done, self.total
            )


def _existing_metadata_counts(td: Any, names: list[str]) -> dict[str, int | None]:
    """Best-effort current row count per data type in the *live* metadata.

    Captured before the rebuild so we can tell a real "this table is empty" from a
    "the rebuild's read failed and dropped a populated table to empty". ``None`` for a
    data type whose metadata view doesn't exist yet (nothing to regress from).
    """
    counts: dict[str, int | None] = {}
    for name in names:
        try:
            row = td.conn.execute(f"select count(*) from metadata.{name}").fetchone()
            counts[name] = int(row[0]) if row else None
        except Exception:  # noqa: BLE001 -- missing view / not-yet-built metadata
            counts[name] = None
    return counts


def _regression_message(
    tables: list[dict], prior: dict[str, int | None], td: Any
) -> str:
    parts = []
    for table in tables:
        had = prior.get(table["name"])
        had_s = f"{had:,}" if isinstance(had, int) else "?"
        if table["status"] == "built":
            now = f"{table['rows']:,} rows" if table["rows"] is not None else "built"
        else:
            now = table["status"]
        parts.append(f"{table['name']}: now {now} (had {had_s})")
    return (
        "Metadata rebuild produced an empty/degenerate database and has already "
        "OVERWRITTEN the canonical metadata DB at "
        f"{td.metadata.metadata_database_path}. A previously-populated table came back "
        "empty, which means the rebuild's reads of the dataset parquet failed -- the "
        "library reports failed S3 reads as 'no parquet data' and overwrites the "
        "metadata regardless. [" + " · ".join(parts) + "]. Recover by re-running the "
        "rebuild from a known-good context (e.g. `flask shell` -> "
        "td.metadata.rebuild_dataset_metadata()) once the read issue is resolved."
    )


def rebuild_metadata(td: Any, on_progress: OnProgress) -> dict[str, Any]:
    """Rebuild the dataset's static metadata DB, reporting progress per data type.

    ``td`` is a ``TIMDEXDataset``. The caller is responsible for holding
    ``dataset_lock`` around this call: the rebuild ends with ``td.refresh()``, which
    mutates the process-wide shared DuckDB connection.

    Raises :class:`DatasetRebuildError` if a previously-populated data type came back
    empty/skipped (a masked read failure -- see that class). Otherwise returns
    ``{"tables": [{"name", "status", "rows"}...], "table_count": int}``.
    """
    names = [cls.NAME for cls in td.metadata.data_type_classes]
    total = len(names)
    tables = [{"name": name, "status": "pending", "rows": None} for name in names]
    # Snapshot live counts up front: the rebuild overwrites + refreshes the metadata,
    # so this is our only chance to know what each table held *before*.
    prior = _existing_metadata_counts(td, names)
    on_progress("Clearing append deltas…", 0, total)

    handler = _MetadataProgressHandler(on_progress, total, tables)
    logger = logging.getLogger(_TDA_LOGGER)
    logger.addHandler(handler)
    try:
        td.metadata.rebuild_dataset_metadata()
    finally:
        logger.removeHandler(handler)

    # Any table that never logged a completion or skip (e.g. the library changed its
    # log wording): don't claim a status we didn't observe.
    for table in tables:
        if table["status"] == "pending":
            table["status"] = "unknown"
    on_progress("Refreshed dataset", total, total)

    # A table that held rows before but isn't "built" now is a masked read failure,
    # not a real change in the data -- surface it as a failure rather than a green Done.
    regressed = [
        t for t in tables if t["status"] != "built" and (prior.get(t["name"]) or 0) > 0
    ]
    if regressed:
        raise DatasetRebuildError(_regression_message(tables, prior, td))
    return {"tables": tables, "table_count": total}
