"""Flask-free readers for individual record versions.

The web record-detail routes (``timmy.main``) and the CLI ``record`` commands
both need to pull one record version -- including its ``source_record`` and
``transformed_record`` payloads -- by identity. That logic lives here, free of
Flask, taking a ``TIMDEXDataset`` directly so either surface can call it:

- the web app passes its shared, locked dataset (callers hold ``dataset_lock``);
- a one-shot CLI process owns its own connection and needs no lock.

Reads go through ``table="records"`` so any historical version is reachable, not
just the current one; ``resolve_current_key`` first maps a bare record id to the
current version's composite key via ``metadata.current_records``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from timdex_dataset_api import TIMDEXDataset

# Metadata columns describing a record version, in display order. Mirrors TDA's
# TIMDEXRecords.METADATA_COLUMNS (also RECORD_METADATA_COLUMNS in timmy.main).
RECORD_METADATA_COLUMNS = [
    "timdex_record_id",
    "source",
    "run_date",
    "run_type",
    "action",
    "run_id",
    "run_record_offset",
    "run_timestamp",
    "filename",
]

# Columns fetched for the per-record versions listing, newest-first by
# run_timestamp. source is constant across versions (header), the rest vary.
VERSION_COLUMNS = [
    "source",
    "run_timestamp",
    "run_date",
    "run_type",
    "action",
    "run_id",
    "run_record_offset",
]


def resolve_current_key(
    dataset: TIMDEXDataset, timdex_record_id: str
) -> tuple[str, int] | None:
    """Map a record id to its current ``(run_id, run_record_offset)`` key.

    Reads from ``metadata.current_records`` (one current row per record). Returns
    ``None`` if the id has no current version (e.g. its latest action is delete).
    """
    key = dataset.conn.execute(
        "select run_id, run_record_offset from metadata.current_records "
        "where timdex_record_id = ? limit 1",
        [timdex_record_id],
    ).fetchone()
    if key is None:
        return None
    return key[0], key[1]


def read_record_version(
    dataset: TIMDEXDataset,
    timdex_record_id: str,
    run_id: str,
    run_record_offset: int,
) -> dict | None:
    """Read one record version (incl. payloads) by its composite key.

    The typed equality filters are parameterized by TDA, so caller-supplied
    values are never interpolated into raw SQL. Returns ``None`` if not found.
    """
    matches = list(
        dataset.records.read_dicts_iter(
            table="records",
            timdex_record_id=timdex_record_id,
            run_id=run_id,
            run_record_offset=run_record_offset,
            limit=1,
        )
    )
    return matches[0] if matches else None


def list_record_versions(
    dataset: TIMDEXDataset, timdex_record_id: str
) -> list[dict]:
    """List every version of a record across all runs, newest-first.

    Metadata-only (no payload reads): selects ``VERSION_COLUMNS`` from
    ``metadata.records`` ordered by ``run_timestamp`` descending.
    """
    columns_sql = ", ".join(VERSION_COLUMNS)
    rows = dataset.conn.execute(
        f"select {columns_sql} from metadata.records "  # noqa: S608 -- fixed column list
        "where timdex_record_id = ? "
        "order by run_timestamp desc nulls last",
        [timdex_record_id],
    ).fetchall()
    return [dict(zip(VERSION_COLUMNS, row, strict=True)) for row in rows]
