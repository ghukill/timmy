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
from typing import Any

import click

from timmy.config import (
    USER_CONFIG_DIR,
    USER_CONFIG_PATH,
    load_config,
    resolve_config,
)
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
@click.option("--no-input", is_flag=True, help="Non-interactive: use flags/current config, no prompts.")
@click.option("--force", is_flag=True, help="Overwrite an existing config without confirming.")
def init(
    dataset_location: str | None,
    analysis_dir: str | None,
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

    if not no_input:
        ds = click.prompt("TIMDEX dataset location", default=ds or "", show_default=bool(ds))
        adir = click.prompt("Analysis output directory", default=adir)

    ds = (ds or "").strip() or None
    if not ds:
        raise click.ClickException("dataset_location is required.")

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
    from timmy.analysis import top_level_fields

    conn = _open(ctx, analysis_id)
    try:
        rows = top_level_fields(conn)
    finally:
        conn.close()
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


if __name__ == "__main__":
    cli()
