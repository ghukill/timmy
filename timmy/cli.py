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


if __name__ == "__main__":
    cli()
