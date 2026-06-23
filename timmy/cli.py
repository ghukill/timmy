"""The ``timmy`` command-line interface.

Two roles share one foundation (see ``scratch/cli.md``): make Timmy a standalone
tool (``uv tool install`` -> ``init`` -> ``webapp run``) and, later, a scriptable
agent surface over ``timmy.analysis``. This module is Phase 1: config + init +
webapp run. Analysis/agent commands land in later phases.

Conventions for everything here: stdout is data, stderr is progress/errors, and
read commands offer ``--json`` with stable shapes for agents.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import click

from timmy.config import (
    USER_CONFIG_DIR,
    USER_CONFIG_PATH,
    load_config,
    resolve_config,
)
from timmy.filters import build_tda_filter, filter_label, split_csv
from timmy.logging_setup import apply_log_level, normalize_level
from timmy.output import emit_csv, emit_json, emit_record, emit_rows

# Cap rows pulled by `analysis query`; one extra is fetched to detect overflow.
MAX_QUERY_ROWS = 1000


def _overrides(**kwargs: Any) -> dict[str, Any]:
    """Drop unset (None) flag values so they don't shadow lower config layers."""
    return {k: v for k, v in kwargs.items() if v is not None}


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--dataset-location",
    default=None,
    help="Override the TIMDEX dataset location for this invocation.",
)
@click.option(
    "--analysis-dir",
    default=None,
    help="Override the analysis output directory for this invocation.",
)
@click.pass_context
def cli(ctx: click.Context, dataset_location: str | None, analysis_dir: str | None) -> None:
    """Timmy: inspect and analyze TIMDEX metadata."""
    ctx.ensure_object(dict)
    ctx.obj["overrides"] = _overrides(
        dataset_location=dataset_location, analysis_dir=analysis_dir
    )


# --------------------------------------------------------------------------- #
# config
# --------------------------------------------------------------------------- #
@cli.group()
def config() -> None:
    """Inspect resolved configuration."""


@config.command("show")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON for agents.")
@click.pass_context
def config_show(ctx: click.Context, as_json: bool) -> None:
    """Print the resolved config and which layer each value came from."""
    resolved = resolve_config(ctx.obj["overrides"])
    if as_json:
        payload = {k: {"value": v["value"], "source": v["source"]} for k, v in resolved.items()}
        click.echo(json.dumps(payload, indent=2, default=str))
        return
    width = max(len(k) for k in resolved)
    for name, entry in resolved.items():
        value = entry["value"]
        shown = "(unset)" if value is None else value
        click.echo(f"{name:<{width}}  {shown}  [{entry['source']}]")


@config.command("path")
def config_path() -> None:
    """Print the path to the user config file (~/.timmy/config.toml)."""
    click.echo(str(USER_CONFIG_PATH))


# --------------------------------------------------------------------------- #
# init
# --------------------------------------------------------------------------- #
def _validate_dataset(location: str) -> str | None:
    """Try to open the dataset (without the expensive preload). Returns an error string or None."""
    try:
        from timmy.dataset import load_dataset

        load_dataset(location, preload_current_records=False)
    except Exception as exc:  # noqa: BLE001 - surface any open failure to the user
        return str(exc)
    return None


@cli.command("init")
@click.option("--dataset-location", default=None, help="Dataset location (skip the prompt).")
@click.option("--analysis-dir", default=None, help="Analysis output directory (skip the prompt).")
@click.option("--log-level", default=None, help="Log level, e.g. INFO or DEBUG (skip the prompt).")
@click.option("--no-input", is_flag=True, help="Non-interactive: use flags/current config, no prompts.")
@click.option("--force", is_flag=True, help="Overwrite an existing config without confirming.")
def init(
    dataset_location: str | None,
    analysis_dir: str | None,
    log_level: str | None,
    no_input: bool,
    force: bool,
) -> None:
    """Interactively set up ~/.timmy/config.toml."""
    import tomli_w

    if USER_CONFIG_PATH.exists() and not force:
        if no_input:
            raise click.ClickException(
                f"{USER_CONFIG_PATH} already exists; pass --force to overwrite."
            )
        if not click.confirm(f"{USER_CONFIG_PATH} exists. Overwrite?", default=False):
            click.echo("Aborted.", err=True)
            raise SystemExit(1)

    # Seed prompts from whatever the resolver currently sees (env, existing file).
    current = load_config()
    ds = dataset_location or current.get("dataset_location")
    adir = analysis_dir or current.get("analysis_dir")
    level = log_level or current.get("log_level")

    if not no_input:
        ds = click.prompt("TIMDEX dataset location", default=ds or "", show_default=bool(ds))
        adir = click.prompt("Analysis output directory", default=adir)
        level = click.prompt("Log level (INFO, DEBUG, …)", default=level)

    ds = (ds or "").strip() or None
    if not ds:
        raise click.ClickException("dataset_location is required.")

    try:
        level = normalize_level(level)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc

    err = _validate_dataset(ds)
    if err:
        msg = f"Could not open dataset at {ds!r}: {err}"
        if no_input:
            click.echo(f"Warning: {msg}", err=True)
        elif not click.confirm(f"{msg}\nSave anyway?", default=False):
            click.echo("Aborted.", err=True)
            raise SystemExit(1)

    payload: dict[str, Any] = {"dataset_location": ds}
    if adir:
        payload["analysis_dir"] = adir
    payload["log_level"] = level

    USER_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with USER_CONFIG_PATH.open("wb") as fh:
        tomli_w.dump(payload, fh)
    click.echo(f"Wrote {USER_CONFIG_PATH}", err=True)


# --------------------------------------------------------------------------- #
# webapp
# --------------------------------------------------------------------------- #
@cli.group()
def webapp() -> None:
    """Run the Timmy web app."""


@webapp.command("run")
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=5000, show_default=True, type=int)
@click.option("--debug", is_flag=True, help="Enable the Flask debugger/reloader.")
@click.pass_context
def webapp_run(ctx: click.Context, host: str, port: int, debug: bool) -> None:
    """Boot the Flask app and serve it (development server, not for production)."""
    from timmy.app import create_app

    try:
        app = create_app(config_overrides=ctx.obj["overrides"])
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc
    app.run(host=host, port=port, debug=debug)


# --------------------------------------------------------------------------- #
# analysis (agent read surface)
# --------------------------------------------------------------------------- #
json_option = click.option("--json", "as_json", is_flag=True, help="Emit JSON for agents.")


def _analyses_dir(ctx: click.Context) -> str:
    return load_config(ctx.obj["overrides"])["analysis_dir"]


def _open(ctx: click.Context, analysis_id: str):
    """Open an analysis DB read-only, mapping bad-id/missing into clean CLI errors."""
    from timmy.analysis import is_valid_analysis_id, open_analysis

    if not is_valid_analysis_id(analysis_id):
        raise click.ClickException(f"Not a valid analysis id: {analysis_id!r}")
    try:
        return open_analysis(_analyses_dir(ctx), analysis_id)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc


def _manifest_matches_source(manifest: dict[str, Any], source: str) -> bool:
    """True if an analysis was built for ``source`` (per filters_json, then label)."""
    raw = manifest.get("filters_json")
    if raw:
        try:
            src = json.loads(raw).get("source")
        except (json.JSONDecodeError, AttributeError):
            src = None
        if isinstance(src, list) and source in src:
            return True
        if isinstance(src, str) and source == src:
            return True
    return source.lower() in (manifest.get("label") or "").lower()


@cli.group()
def analysis() -> None:
    """Inspect built analyses (read-only; agent-facing)."""


@analysis.command("list")
@click.option("--source", default=None, help="Only analyses built for this source.")
@json_option
@click.pass_context
def analysis_list(ctx: click.Context, source: str | None, as_json: bool) -> None:
    """List built analyses, newest first."""
    from timmy.analysis import list_analyses

    manifests = list_analyses(_analyses_dir(ctx))
    if source:
        manifests = [m for m in manifests if _manifest_matches_source(m, source)]
    emit_rows(
        manifests,
        as_json=as_json,
        columns=["analysis_id", "created_at", "label", "doc_count", "eav_count", "name"],
    )


@analysis.command("show")
@click.argument("analysis_id")
@json_option
@click.pass_context
def analysis_show(ctx: click.Context, analysis_id: str, as_json: bool) -> None:
    """Show one analysis manifest."""
    from timmy.analysis import is_valid_analysis_id, read_manifest

    if not is_valid_analysis_id(analysis_id):
        raise click.ClickException(f"Not a valid analysis id: {analysis_id!r}")
    try:
        manifest = read_manifest(_analyses_dir(ctx), analysis_id)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc
    emit_record(manifest, as_json=as_json)


@analysis.command("fields")
@click.argument("analysis_id")
@click.option("--field", default=None, help="Restrict to a single top-level field.")
@json_option
@click.pass_context
def analysis_fields(
    ctx: click.Context, analysis_id: str, field: str | None, as_json: bool
) -> None:
    """Per-field coverage: type, coverage_pct, cardinality, distinct values."""
    from timmy.analysis import get_field_usage_report, is_valid_analysis_id

    if not is_valid_analysis_id(analysis_id):
        raise click.ClickException(f"Not a valid analysis id: {analysis_id!r}")
    try:
        # Served from the build-time cache (computed + backfilled on first read
        # for legacy DBs) so this stays instant at the multi-million-record scale.
        rows = get_field_usage_report(_analyses_dir(ctx), analysis_id)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc
    if field:
        rows = [r for r in rows if r["field"] == field]
        if not rows:
            raise click.ClickException(f"No field {field!r} in analysis {analysis_id}.")
    emit_rows(
        rows,
        as_json=as_json,
        columns=[
            "field", "type", "coverage_pct", "doc_count", "distinct_values",
            "count_min", "count_avg", "count_max", "sample_value",
        ],
    )


@analysis.command("values")
@click.argument("analysis_id")
@click.option("--path", required=True, help="Field/path prefix to read values under.")
@click.option("--search", default="", help="Filter values containing this text.")
@click.option("--limit", default=100, show_default=True, type=int)
@click.option("--offset", default=0, show_default=True, type=int)
@json_option
@click.pass_context
def analysis_values(
    ctx: click.Context,
    analysis_id: str,
    path: str,
    search: str,
    limit: int,
    offset: int,
    as_json: bool,
) -> None:
    """Distinct values (with counts) under a path."""
    from timmy.analysis import path_values

    conn = _open(ctx, analysis_id)
    try:
        total, filtered, rows = path_values(
            conn, path, search=search, limit=limit, offset=offset
        )
    finally:
        conn.close()
    click.echo(f"{len(rows)} of {filtered} value(s) (path total {total})", err=True)
    emit_rows(
        rows,
        as_json=as_json,
        columns=["path", "value", "documents", "occurrences", "pct_of_path"],
    )


@analysis.command("records")
@click.argument("analysis_id")
@click.option("--path", required=True, help="Path the value lives at.")
@click.option("--value", required=True, help="Value to find records for.")
@click.option("--search", default="", help="Filter records by id/source text.")
@click.option("--limit", default=100, show_default=True, type=int)
@click.option("--offset", default=0, show_default=True, type=int)
@json_option
@click.pass_context
def analysis_records(
    ctx: click.Context,
    analysis_id: str,
    path: str,
    value: str,
    search: str,
    limit: int,
    offset: int,
    as_json: bool,
) -> None:
    """Records carrying a given value at a given path."""
    from timmy.analysis import value_records

    conn = _open(ctx, analysis_id)
    try:
        total, filtered, rows = value_records(
            conn, path, value, search=search, limit=limit, offset=offset
        )
    finally:
        conn.close()
    click.echo(f"{len(rows)} of {filtered} record(s) (total {total})", err=True)
    emit_rows(
        rows,
        as_json=as_json,
        columns=["timdex_record_id", "source", "run_id", "run_record_offset"],
    )


@analysis.command("query")
@click.argument("analysis_id")
@click.argument("sql")
@click.option("--csv", "as_csv", is_flag=True, help="Emit CSV instead of a table.")
@json_option
@click.pass_context
def analysis_query(
    ctx: click.Context, analysis_id: str, sql: str, as_csv: bool, as_json: bool
) -> None:
    """Run read-only SQL against an analysis DB (docs/eav/manifest schema).

    The connection is read-only, so the engine itself rejects any write -- the
    universal escape hatch for anything the typed commands don't cover.
    """
    if as_csv and as_json:
        raise click.ClickException("Choose either --csv or --json, not both.")

    conn = _open(ctx, analysis_id)
    try:
        cursor = conn.execute(sql)
        columns = [d[0] for d in cursor.description] if cursor.description else []
        fetched = cursor.fetchmany(MAX_QUERY_ROWS + 1)
    except Exception as exc:  # noqa: BLE001 -- report SQL errors as a clean CLI failure
        raise click.ClickException(str(exc)) from exc
    finally:
        conn.close()

    truncated = len(fetched) > MAX_QUERY_ROWS
    rows = fetched[:MAX_QUERY_ROWS]
    if truncated:
        click.echo(f"(truncated to {MAX_QUERY_ROWS} rows)", err=True)

    if as_csv:
        emit_csv(columns, [list(r) for r in rows])
        return
    dict_rows = [dict(zip(columns, r, strict=True)) for r in rows]
    if as_json:
        emit_json(dict_rows)
        return
    emit_rows(dict_rows, as_json=False, columns=columns)


# --------------------------------------------------------------------------- #
# analysis build / delete / prune (write & manage surface)
# --------------------------------------------------------------------------- #
def _load_dataset(ctx: click.Context):
    """Load the TIMDEXDataset from resolved config (the expensive, once-per-run cost).

    No ``dataset_lock``: a one-shot CLI process owns its own DuckDB connection,
    unlike the web app's shared, threaded one.
    """
    from timmy.dataset import load_dataset

    location = load_config(ctx.obj["overrides"]).get("dataset_location")
    if not location:
        raise click.ClickException(
            "dataset_location is not configured. Run `timmy init` or pass "
            "--dataset-location."
        )
    return load_dataset(location)


def _flatten_csv(values: tuple[str, ...]) -> list[str]:
    """Flatten repeatable flags, each of which may also be comma-separated."""
    out: list[str] = []
    for value in values:
        out.extend(split_csv(value))
    return out


@analysis.command("build")
@click.option("--source", "sources", multiple=True, help="Filter by source (repeatable/CSV).")
@click.option("--run-type", "run_types", multiple=True, help="Filter by run_type.")
@click.option("--action", "actions", multiple=True, help="Filter by action.")
@click.option("--run-id", "run_ids", multiple=True, help="Filter by run_id.")
@click.option("--run-date", default=None, help="Exact run date (YYYY-MM-DD).")
@click.option("--where", default=None, help="Raw SQL predicate (trusted power tool).")
@click.option("--limit", default=None, type=int, help="Cap how many records are read.")
@click.option(
    "--all-versions",
    is_flag=True,
    help="Read every record version (metadata.records), not just current ones.",
)
@click.option("--name", default=None, help="Human name for the analysis.")
@click.option("--notes", default=None, help="Free-text notes stored in the manifest.")
@json_option
@click.pass_context
def analysis_build(
    ctx: click.Context,
    sources: tuple[str, ...],
    run_types: tuple[str, ...],
    actions: tuple[str, ...],
    run_ids: tuple[str, ...],
    run_date: str | None,
    where: str | None,
    limit: int | None,
    all_versions: bool,
    name: str | None,
    notes: str | None,
    as_json: bool,
) -> None:
    """Build one analysis DB from records matching a filter (foreground).

    Progress and the build summary go to stderr; the manifest is the data, on
    stdout. Filter semantics match the web build exactly (shared timmy.filters).
    """
    from timmy.analysis import build_analysis

    filters: dict[str, Any] = {}
    for column, values in (
        ("source", sources),
        ("run_type", run_types),
        ("action", actions),
        ("run_id", run_ids),
    ):
        collected = _flatten_csv(values)
        if collected:
            filters[column] = collected
    if run_date:
        filters["run_date"] = run_date.strip()

    where_combined, filters = build_tda_filter(filters, where=where)
    label = filter_label(where_combined, filters, limit)
    table = "records" if all_versions else "current_records"

    _configure_cli_logging(ctx)
    click.echo(f"Building analysis ({label})…", err=True)
    td = _load_dataset(ctx)
    try:
        manifest = build_analysis(
            td,
            _analyses_dir(ctx),
            where=where_combined,
            table=table,
            limit=limit,
            label=label,
            name=name,
            notes=notes,
            **filters,
        )
    except Exception as exc:  # noqa: BLE001 -- surface build failures as a clean exit
        raise click.ClickException(f"Analysis build failed: {exc}") from exc

    click.echo(
        f"Built {manifest['analysis_id']}: {manifest['doc_count']} docs, "
        f"{manifest['eav_count']} eav rows, {manifest['skipped_count']} skipped",
        err=True,
    )
    if as_json:
        emit_json(manifest)
    else:
        emit_record(manifest, as_json=False)


def _configure_cli_logging(ctx: click.Context) -> None:
    """Send logs (e.g. build_analysis's summary) to stderr for a CLI run.

    The level comes from the resolved ``log_level`` config (default INFO), so a
    ``log_level = "DEBUG"`` in ~/.timmy/config.toml turns on DEBUG here and in TDA.
    """
    log_level = load_config(ctx.obj["overrides"]).get("log_level")
    logging.basicConfig(
        stream=sys.stderr,
        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    )
    apply_log_level(log_level)


@analysis.command("delete")
@click.argument("analysis_id")
@click.option("--yes", is_flag=True, help="Delete without confirming.")
@click.pass_context
def analysis_delete(ctx: click.Context, analysis_id: str, yes: bool) -> None:
    """Delete one analysis DB file."""
    from timmy.analysis import delete_analysis, is_valid_analysis_id

    if not is_valid_analysis_id(analysis_id):
        raise click.ClickException(f"Not a valid analysis id: {analysis_id!r}")
    if not yes and not click.confirm(f"Delete analysis {analysis_id}?", default=False):
        click.echo("Aborted.", err=True)
        raise SystemExit(1)
    if not delete_analysis(_analyses_dir(ctx), analysis_id):
        raise click.ClickException(f"No analysis {analysis_id} to delete.")
    click.echo(f"Deleted {analysis_id}", err=True)


_DURATION_RE = re.compile(r"^\s*(\d+)\s*([wdhms])\s*$")
_DURATION_UNITS = {
    "w": "weeks",
    "d": "days",
    "h": "hours",
    "m": "minutes",
    "s": "seconds",
}


def _parse_duration(text: str) -> timedelta:
    """Parse a duration like ``30d``, ``2w``, ``12h`` into a timedelta."""
    match = _DURATION_RE.match(text)
    if not match:
        raise click.ClickException(
            f"Bad --older-than {text!r}; use e.g. 30d, 2w, 12h, 45m."
        )
    amount, unit = int(match.group(1)), match.group(2)
    return timedelta(**{_DURATION_UNITS[unit]: amount})


def _created_at_naive(manifest: dict[str, Any]) -> datetime | None:
    """Manifest created_at as a naive (UTC) datetime, for cutoff comparison."""
    created = manifest.get("created_at")
    if not isinstance(created, datetime):
        return None
    return created.replace(tzinfo=None) if created.tzinfo else created


@analysis.command("prune")
@click.option("--older-than", default=None, help="Prune analyses older than e.g. 30d, 2w, 12h.")
@click.option("--keep", default=None, type=int, help="Always keep the N newest in scope.")
@click.option("--source", default=None, help="Limit pruning to analyses built for this source.")
@click.option("--dry-run", is_flag=True, help="Show what would be pruned; delete nothing.")
@click.option("--yes", is_flag=True, help="Prune without confirming.")
@json_option
@click.pass_context
def analysis_prune(
    ctx: click.Context,
    older_than: str | None,
    keep: int | None,
    source: str | None,
    dry_run: bool,
    yes: bool,
    as_json: bool,
) -> None:
    """Bulk-delete old analyses, selected by age / count / source.

    ``--source`` scopes the candidate set; ``--older-than`` and ``--keep`` then
    select within it (``--keep`` always protects the N newest). At least one of
    the three is required -- prune never targets everything by default.
    """
    from timmy.analysis import delete_analysis, list_analyses

    if older_than is None and keep is None and source is None:
        raise click.ClickException(
            "Refusing to prune everything; pass --older-than, --keep, or --source."
        )

    analyses_dir = _analyses_dir(ctx)
    scope = list_analyses(analyses_dir)  # newest first
    if source:
        scope = [m for m in scope if _manifest_matches_source(m, source)]

    protected = {m["analysis_id"] for m in scope[:keep]} if keep is not None else set()
    if older_than:
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - _parse_duration(older_than)
        aged = [m for m in scope if (c := _created_at_naive(m)) and c < cutoff]
    else:
        # --keep or --source alone: every in-scope analysis is a candidate, and
        # the newest `keep` are protected below.
        aged = scope
    to_prune = [m for m in aged if m["analysis_id"] not in protected]

    columns = ["analysis_id", "created_at", "label", "doc_count"]
    if not to_prune:
        click.echo("Nothing to prune.", err=True)
        if as_json:
            emit_json([])
        return

    if dry_run:
        click.echo(f"Would prune {len(to_prune)} analysis file(s) (dry run):", err=True)
        emit_rows(to_prune, as_json=as_json, columns=columns)
        return

    if not yes and not click.confirm(
        f"Delete {len(to_prune)} analysis file(s)?", default=False
    ):
        click.echo("Aborted.", err=True)
        raise SystemExit(1)

    for manifest in to_prune:
        delete_analysis(analyses_dir, manifest["analysis_id"])
    click.echo(f"Pruned {len(to_prune)} analysis file(s).", err=True)
    emit_rows(to_prune, as_json=as_json, columns=columns)


# --------------------------------------------------------------------------- #
# sources (cheap, metadata-only source/run aggregates)
# --------------------------------------------------------------------------- #
@cli.group()
def sources() -> None:
    """Per-source metadata: counts and ETL runs (no analysis needed).

    Everything here is a plain aggregate over the dataset's metadata tables --
    milliseconds, no payload reads, no `analysis build`. Start here to learn what
    sources exist and how big they are before deciding whether to build an EAV
    analysis for field-content questions.
    """


@sources.command("list")
@json_option
@click.pass_context
def sources_list(ctx: click.Context, as_json: bool) -> None:
    """Per-source overview: current record count, history, runs, dates."""
    from timmy.source_stats import SOURCE_OVERVIEW_COLUMNS, source_overview

    td = _load_dataset(ctx)
    emit_rows(source_overview(td.conn), as_json=as_json, columns=SOURCE_OVERVIEW_COLUMNS)


@sources.command("show")
@click.argument("source")
@click.option("--limit", default=None, type=int, help="Cap how many runs are listed.")
@json_option
@click.pass_context
def sources_show(
    ctx: click.Context, source: str, limit: int | None, as_json: bool
) -> None:
    """Show one source's summary plus its ETL runs (newest first)."""
    from timmy.source_stats import (
        RUN_COLUMNS,
        SOURCE_OVERVIEW_COLUMNS,
        run_summaries,
        source_overview,
    )

    td = _load_dataset(ctx)
    summary = next((r for r in source_overview(td.conn) if r["source"] == source), None)
    if summary is None:
        raise click.ClickException(f"No source {source!r} in the dataset.")
    runs = run_summaries(td.conn, sources=[source], limit=limit)

    if as_json:
        emit_json({"summary": summary, "runs": runs})
        return
    emit_record({c: summary[c] for c in SOURCE_OVERVIEW_COLUMNS}, as_json=False)
    click.echo(f"\n--- ETL runs ({len(runs)}) ---", err=True)
    emit_rows(runs, as_json=False, columns=RUN_COLUMNS)


@sources.command("runs")
@click.option("--source", "src", multiple=True, help="Filter to source(s) (repeatable/CSV).")
@click.option("--limit", default=None, type=int, help="Cap how many runs are listed.")
@json_option
@click.pass_context
def sources_runs(
    ctx: click.Context, src: tuple[str, ...], limit: int | None, as_json: bool
) -> None:
    """List ETL runs (newest first), optionally filtered by source."""
    from timmy.source_stats import RUN_COLUMNS, run_summaries

    td = _load_dataset(ctx)
    filter_sources = _flatten_csv(src) or None
    runs = run_summaries(td.conn, sources=filter_sources, limit=limit)
    emit_rows(runs, as_json=as_json, columns=RUN_COLUMNS)


# --------------------------------------------------------------------------- #
# transmog (manage the local Transmogrifier checkout)
# --------------------------------------------------------------------------- #
@cli.group()
def transmog() -> None:
    """Clone/manage a local Transmogrifier checkout (the transform engine).

    Transmogrifier turns a source record into the normalized TIMDEX
    `transformed_record` -- how records enter the dataset. Timmy reads the
    finished records but never runs the transform; cloning the real repo gives an
    agent the actual transform code to interrogate (`timmy docs show
    transmogrifier`). `clone` once, `update` to refresh, `path` to find the code.
    """


def _transmog_config(ctx: click.Context) -> tuple[str, str]:
    """Resolve the (transmog_dir, transmog_repo_url) pair from layered config."""
    cfg = load_config(ctx.obj["overrides"])
    return cfg["transmog_dir"], cfg["transmog_repo_url"]


@transmog.command("clone")
@click.option("--force", is_flag=True, help="Re-clone over an existing checkout.")
@json_option
@click.pass_context
def transmog_clone(ctx: click.Context, force: bool, as_json: bool) -> None:
    """Clone Transmogrifier into the configured transmog dir."""
    from timmy.transmog import TransmogError, clone_repo

    transmog_dir, repo_url = _transmog_config(ctx)
    click.echo(f"Cloning {repo_url} -> {transmog_dir}…", err=True)
    try:
        status = clone_repo(transmog_dir, repo_url, force=force)
    except TransmogError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        f"Cloned at commit {status['commit']} ({status['branch']}).", err=True
    )
    emit_record(status, as_json=as_json)


@transmog.command("update")
@json_option
@click.pass_context
def transmog_update(ctx: click.Context, as_json: bool) -> None:
    """Fast-forward the existing checkout to its upstream."""
    from timmy.transmog import TransmogError, update_repo

    transmog_dir, repo_url = _transmog_config(ctx)
    try:
        status = update_repo(transmog_dir, repo_url)
    except TransmogError as exc:
        raise click.ClickException(str(exc)) from exc
    if status["updated"]:
        click.echo(
            f"Updated {status['previous_commit']} -> {status['commit']}.", err=True
        )
    else:
        click.echo(f"Already up to date at {status['commit']}.", err=True)
    emit_record(status, as_json=as_json)


@transmog.command("status")
@json_option
@click.pass_context
def transmog_status(ctx: click.Context, as_json: bool) -> None:
    """Show whether Transmogrifier is cloned, and at which commit."""
    from timmy.transmog import repo_status

    transmog_dir, repo_url = _transmog_config(ctx)
    status = repo_status(transmog_dir, repo_url)
    if not status["cloned"]:
        click.echo("Not cloned; run `timmy transmog clone`.", err=True)
    emit_record(status, as_json=as_json)


@transmog.command("path")
@click.pass_context
def transmog_path(ctx: click.Context) -> None:
    """Print the path to the local Transmogrifier checkout.

    Always prints the configured path (data, on stdout) so it's scriptable; if it
    isn't cloned yet, a hint goes to stderr.
    """
    from timmy.transmog import repo_status

    transmog_dir, repo_url = _transmog_config(ctx)
    status = repo_status(transmog_dir, repo_url)
    if not status["cloned"]:
        click.echo("(not cloned yet; run `timmy transmog clone`)", err=True)
    click.echo(status["path"])


# --------------------------------------------------------------------------- #
# docs (human/agent documentation surface)
# --------------------------------------------------------------------------- #
@cli.group()
def docs() -> None:
    """Read Timmy's docs, or install them as an agent skill.

    Narrative topics ship as markdown; `commands` and `schema` are generated
    from the live code, so they never drift. `install-skill` snapshots all of it
    into an agent skill directory.
    """


@docs.command("list")
@json_option
def docs_list(as_json: bool) -> None:
    """List available doc topics."""
    from timmy.docsgen import list_topics

    rows = [{"topic": name, "description": desc} for name, desc in list_topics()]
    emit_rows(rows, as_json=as_json, columns=["topic", "description"])


@docs.command("show")
@click.argument("topic")
@click.pass_context
def docs_show(ctx: click.Context, topic: str) -> None:
    """Print one doc topic as markdown (see `docs list` for names)."""
    from timmy.docsgen import get_topic

    try:
        click.echo(get_topic(topic))
    except KeyError:
        raise click.ClickException(
            f"No doc topic {topic!r}. Run `timmy docs list`."
        ) from None


@docs.command("catalog")
@json_option
def docs_catalog(as_json: bool) -> None:
    """The command reference, generated from the live CLI."""
    from timmy.docsgen import command_catalog, render_catalog_markdown

    catalog = command_catalog()
    if as_json:
        emit_json(catalog)
        return
    click.echo(render_catalog_markdown(catalog))


@docs.command("schema")
@json_option
def docs_schema(as_json: bool) -> None:
    """The analysis-DB schema (docs/eav/manifest), generated from the code."""
    from timmy.analysis.store import EXCLUDED_FIELDS, SCHEMA_SQL
    from timmy.docsgen import render_schema_markdown

    if as_json:
        emit_json({"schema_sql": SCHEMA_SQL.strip(), "excluded_fields": sorted(EXCLUDED_FIELDS)})
        return
    click.echo(render_schema_markdown())


@docs.command("install-skill")
@click.option(
    "--path",
    "target_dir",
    default=None,
    help="Skills directory to install into (default: ~/.agents/skills).",
)
def docs_install_skill(target_dir: str | None) -> None:
    """Install the docs as a self-contained agent skill.

    Writes a `timmy/` skill (SKILL.md + reference/) into the skills directory,
    snapshotting the generated topics and stamping the source version/commit.
    """
    from pathlib import Path

    from timmy.docsgen import build_skill

    target = Path(target_dir).expanduser() if target_dir else Path.home() / ".agents" / "skills"
    skill_dir = build_skill(target)
    click.echo(f"Installed timmy skill to {skill_dir}", err=True)
    click.echo(str(skill_dir))


# --------------------------------------------------------------------------- #
# record (single-record inspection)
# --------------------------------------------------------------------------- #
@cli.group()
def record() -> None:
    """Inspect individual record versions, including raw payloads."""


def _decode_payload(payload: Any) -> str | None:
    """Decode a TDA payload (bytes/str/None) to text, tolerating None."""
    if payload is None:
        return None
    if isinstance(payload, bytes):
        return payload.decode("utf-8", errors="replace")
    return payload


@record.command("show")
@click.argument("timdex_record_id")
@click.option("--run-id", default=None, help="Pin a specific version's run_id (with --run-record-offset).")
@click.option("--run-record-offset", default=None, type=int, help="Pin a specific version's offset (with --run-id).")
@click.option("--source-only", is_flag=True, help="Emit only the source_record payload.")
@click.option("--transformed-only", is_flag=True, help="Emit only the transformed_record payload.")
@json_option
@click.pass_context
def record_show(
    ctx: click.Context,
    timdex_record_id: str,
    run_id: str | None,
    run_record_offset: int | None,
    source_only: bool,
    transformed_only: bool,
    as_json: bool,
) -> None:
    """Show one record version: metadata + transformed + source payloads.

    Defaults to the current version; pass --run-id and --run-record-offset
    together to pin a specific historical version. The source payload is what an
    agent reads to reason about *why* a transformed field looks the way it does.
    """
    from timmy.records import (
        RECORD_METADATA_COLUMNS,
        read_record_version,
        resolve_current_key,
    )
    from timmy.sources import get_source_record_format, prettify

    if source_only and transformed_only:
        raise click.ClickException("Choose either --source-only or --transformed-only, not both.")
    if (run_id is None) != (run_record_offset is None):
        raise click.ClickException("Pass --run-id and --run-record-offset together, or neither.")

    td = _load_dataset(ctx)
    if run_id is None:
        key = resolve_current_key(td, timdex_record_id)
        if key is None:
            raise click.ClickException(f"No current version for record {timdex_record_id!r}.")
        run_id, run_record_offset = key
    rec = read_record_version(td, timdex_record_id, run_id, run_record_offset)
    if rec is None:
        raise click.ClickException(f"No record version found for {timdex_record_id!r}.")

    source_format = get_source_record_format(rec["source"])
    source_text = _decode_payload(rec.get("source_record"))
    transformed_text = _decode_payload(rec.get("transformed_record"))

    if as_json:
        payload: dict[str, Any] = {}
        if not source_only:
            # Parse transformed (always JSON) so agents get structured data, not a blob.
            payload["transformed"] = json.loads(transformed_text) if transformed_text else None
        if not transformed_only:
            payload["source_record"] = source_text
            payload["source_format"] = source_format
        if not (source_only or transformed_only):
            payload["metadata"] = {c: rec.get(c) for c in RECORD_METADATA_COLUMNS}
        emit_json(payload)
        return

    if not (source_only or transformed_only):
        emit_record({c: rec.get(c) for c in RECORD_METADATA_COLUMNS}, as_json=False)
    if not source_only:
        click.echo("\n--- transformed_record ---", err=True)
        click.echo(prettify(transformed_text, "json"))
    if not transformed_only:
        click.echo(f"\n--- source_record ({source_format}) ---", err=True)
        click.echo(prettify(source_text, source_format))


@record.command("versions")
@click.argument("timdex_record_id")
@json_option
@click.pass_context
def record_versions(ctx: click.Context, timdex_record_id: str, as_json: bool) -> None:
    """List every version of a record across all runs, newest first."""
    from timmy.records import VERSION_COLUMNS, list_record_versions

    td = _load_dataset(ctx)
    versions = list_record_versions(td, timdex_record_id)
    if not versions:
        raise click.ClickException(f"No versions found for record {timdex_record_id!r}.")
    emit_rows(versions, as_json=as_json, columns=VERSION_COLUMNS)


if __name__ == "__main__":
    cli()
