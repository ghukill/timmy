"""Generate and assemble Timmy's agent-facing documentation.

The docs are deliberately split by *churn rate* so they don't rot:

- **Generated** (this module): the command reference is introspected from the
  live Click tree, and the analysis-DB schema is derived from
  :mod:`timmy.analysis.store` constants. Neither can drift from the code because
  both are read from it.
- **Narrative** (``timmy/docs/*.md``): the mental model, playbooks, and
  per-source notes -- the judgement layer, hand-maintained.

Both surfaces (``timmy docs`` and the installed skill) render from here, so
there is exactly one source of truth. A skill is static text the agent reads, so
``build_skill`` *snapshots* the generated topics into the skill dir and stamps
the result with the timmy version + git commit; the live ``timmy docs`` command
always regenerates.
"""

from __future__ import annotations

import importlib.metadata
import importlib.resources
import shutil
import subprocess
from pathlib import Path
from typing import Any

import click

# Narrative topics ship as packaged markdown; generated topics are computed
# here. Order is the reading order presented to humans and agents alike.
NARRATIVE_TOPICS: dict[str, str] = {
    "overview": "Mental model: TIMDEX records, the EAV analysis model, find-or-build.",
    "playbooks": "Question -> command recipes for common agent tasks.",
    "sources": "Per-source notes: payload formats and known quirks.",
    "transmogrifier": "How source -> transformed works; reading the cloned transform code.",
}
GENERATED_TOPICS: dict[str, str] = {
    "commands": "Full command reference (generated from the live CLI).",
    "schema": "Analysis DuckDB schema: docs / eav / manifest (generated).",
}


def list_topics() -> list[tuple[str, str]]:
    """All doc topics as ``(name, description)``, in reading order."""
    return list(NARRATIVE_TOPICS.items()) + list(GENERATED_TOPICS.items())


def get_topic(name: str) -> str:
    """Render one topic to markdown (reading a file or generating it)."""
    if name in NARRATIVE_TOPICS:
        return _read_packaged(f"{name}.md")
    if name == "commands":
        return render_catalog_markdown(command_catalog())
    if name == "schema":
        return render_schema_markdown()
    raise KeyError(name)


def _read_packaged(filename: str) -> str:
    """Read a packaged narrative markdown file from ``timmy.docs``."""
    return importlib.resources.files("timmy.docs").joinpath(filename).read_text(
        encoding="utf-8"
    )


# --------------------------------------------------------------------------- #
# command catalog (introspected from the live Click tree)
# --------------------------------------------------------------------------- #
def command_catalog() -> list[dict[str, Any]]:
    """Walk the live ``timmy`` Click tree into a flat list of leaf commands.

    Each entry is ``{"command", "summary", "params": [...]}``; params capture
    flags/arguments with their metavar, requiredness, and help. Imported lazily
    to avoid a circular import (``timmy.cli`` defines the ``docs`` group).
    """
    from timmy.cli import cli

    catalog: list[dict[str, Any]] = []
    _walk(cli, ["timmy"], catalog)
    return catalog


def _walk(command: click.Command, path: list[str], out: list[dict[str, Any]]) -> None:
    if isinstance(command, click.Group):
        for name in sorted(command.commands):
            _walk(command.commands[name], [*path, name], out)
        return
    out.append(
        {
            "command": " ".join(path),
            "summary": command.get_short_help_str(limit=200),
            "params": [_param_info(p) for p in command.params],
        }
    )


def _param_info(param: click.Parameter) -> dict[str, Any]:
    """Serialize one option/argument to a documentation dict."""
    info: dict[str, Any] = {
        "kind": param.param_type_name,  # "option" | "argument"
        "name": param.name,
        "flags": list(param.opts) + list(param.secondary_opts),
        "required": bool(param.required),
    }
    if isinstance(param, click.Option):
        info["help"] = param.help
        info["is_flag"] = param.is_flag
        info["multiple"] = param.multiple
        # Only surface concrete scalar defaults (host/port/limit/offset); skip
        # flags, None, and Click's UNSET sentinel for `multiple` options.
        default = param.default
        info["default"] = (
            default
            if isinstance(default, (str, int, float)) and not isinstance(default, bool)
            else None
        )
    return info


def render_catalog_markdown(catalog: list[dict[str, Any]]) -> str:
    """Render the command catalog as readable markdown."""
    lines = [
        "# Command reference",
        "",
        "Generated from the live `timmy` CLI. Every read command accepts `--json`",
        "for stable, machine-readable output; stdout is data, stderr is progress.",
        "Run `timmy <command> -h` for the authoritative, always-current help.",
        "",
    ]
    for entry in catalog:
        lines.append(f"## `{entry['command']}`")
        lines.append("")
        if entry["summary"]:
            lines.append(entry["summary"])
            lines.append("")
        args = [p for p in entry["params"] if p["kind"] == "argument"]
        opts = [p for p in entry["params"] if p["kind"] == "option"]
        if args:
            lines.append("Arguments: " + ", ".join(f"`{a['name'].upper()}`" for a in args))
            lines.append("")
        if opts:
            lines.append("| Option | Req | Description |")
            lines.append("|---|---|---|")
            for opt in opts:
                flag = ", ".join(f"`{f}`" for f in opt["flags"])
                req = "yes" if opt["required"] else ""
                desc = opt.get("help") or ""
                if opt.get("default") is not None:
                    desc = f"{desc} (default: {opt['default']})".strip()
                lines.append(f"| {flag} | {req} | {desc} |")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


# --------------------------------------------------------------------------- #
# schema reference (derived from store/flatten constants)
# --------------------------------------------------------------------------- #
def render_schema_markdown() -> str:
    """Render the analysis-DB schema doc from the live DDL + flatten semantics."""
    from timmy.analysis.store import EXCLUDED_FIELDS, SCHEMA_SQL

    excluded = ", ".join(f"`{f}`" for f in sorted(EXCLUDED_FIELDS)) or "(none)"
    return f"""# Analysis DuckDB schema

Each analysis is one self-contained, read-only `<analysis_id>.duckdb` file with
three tables. `timmy analysis query <id> "<sql>"` runs read-only SQL against
them -- the universal escape hatch for anything the typed commands don't cover.

## Tables (authoritative DDL)

```sql
{SCHEMA_SQL.strip()}
```

## How to read them

- **`docs`** -- one row per analyzed record *version* (the dimension table).
  `timdex_composite_id` (`timdex_record_id|run_id|run_record_offset`) joins to
  `eav`. A record version is uniquely `(timdex_record_id, run_id,
  run_record_offset)`.
- **`eav`** -- the flattened transformed payload, one row per JSON *leaf*:
  - `path` collapses array indices to `[]` (e.g. `contributors[].kind`) -- the
    GROUP BY key for corpus-wide field usage.
  - `path_indexed` preserves indices (e.g. `contributors[0].kind`) for when a
    specific element matters.
  - `value` is always text; `value_type` preserves the original JSON type, one
    of: `string`, `number`, `boolean`, `null`, `object-empty`, `array-empty`.
    An empty object/array gets its own row, so "present but empty" stays
    distinguishable from absent (absent = no row for that path).
- **`manifest`** -- a single self-describing row: how the analysis was built
  (`where_predicate`, `filters_json`, `table_name`), `dataset_location`, counts
  (`doc_count`, `eav_count`, `skipped_count`), and `created_at`.

Provenance/bookkeeping fields are dropped before flattening and never appear in
`eav`: {excluded}.

## Worked patterns

```sql
-- Coverage of a field: how many docs carry at least one `subjects` leaf.
select count(distinct e.timdex_composite_id) * 100.0
       / (select count(*) from docs) as coverage_pct
from eav e where e.path like 'subjects%';

-- Per-record metadata size, to find outliers (much more / much less metadata).
select d.timdex_record_id, count(*) as leaf_count
from docs d join eav e using (timdex_composite_id)
group by 1 order by leaf_count desc;
```
"""


# --------------------------------------------------------------------------- #
# skill assembly
# --------------------------------------------------------------------------- #
def _provenance() -> str:
    """A version + git-commit stamp for an installed skill (best effort)."""
    try:
        version = importlib.metadata.version("timmy")
    except importlib.metadata.PackageNotFoundError:
        version = "unknown"
    commit = "unknown"
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=Path(__file__).resolve().parent,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip() or "unknown"
    except (subprocess.SubprocessError, OSError):
        pass
    return f"timmy {version} (commit {commit})"


def _skill_md(provenance: str) -> str:
    """The SKILL.md entry point: short orientation + pointers to reference files."""
    refs = "\n".join(
        f"- `reference/{name}.md` -- {desc}" for name, desc in list_topics()
    )
    return f"""---
name: timmy
description: >-
  Inspect and analyze TIMDEX metadata via the `timmy` CLI. Use when asked about
  field coverage/usage, outlier records, cross-source comparison, distinct
  values/vocabulary, or why a specific record's field looks the way it does.
---

# Timmy: TIMDEX metadata analysis

`timmy` is a CLI for profiling TIMDEX records. Records are flattened into an
entity-attribute-value (EAV) model and materialized as immutable, read-only
DuckDB analysis files you can query. You drive it entirely from the shell.

Start with `reference/overview.md` for the mental model, then
`reference/playbooks.md` for question -> command recipes. The CLI is the source
of truth: `timmy docs catalog` and `timmy <command> -h` always reflect the
installed version (this snapshot was generated from **{provenance}** -- if it
looks stale, prefer the live commands).

## Reference files

{refs}

## The contract

- stdout is data, stderr is progress/errors; every read command takes `--json`.
- Exit code 0 on success, non-zero on failure; nothing blocks on a prompt when
  you pass non-interactive flags (`--no-input`, `--yes`).
- The escape hatch for anything bespoke: `timmy analysis query <id> "<sql>"`
  against the documented `docs`/`eav`/`manifest` schema.
"""


def build_skill(target_dir: str | Path) -> Path:
    """Write the timmy skill (SKILL.md + reference/) into ``target_dir/timmy``.

    Snapshots the generated topics so the static skill is self-contained, and
    stamps SKILL.md with the source version/commit. Returns the skill directory.
    Replaces any existing ``timmy`` skill at the target.
    """
    skill_dir = Path(target_dir).expanduser() / "timmy"
    if skill_dir.exists():
        shutil.rmtree(skill_dir)
    reference = skill_dir / "reference"
    reference.mkdir(parents=True)

    (skill_dir / "SKILL.md").write_text(_skill_md(_provenance()), encoding="utf-8")
    for name, _desc in list_topics():
        (reference / f"{name}.md").write_text(get_topic(name), encoding="utf-8")
    return skill_dir
