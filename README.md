# Timmy

Timmy inspects and analyzes **TIMDEX metadata** — the library records aggregated
from many sources (`alma`, `dspace`, `aspace`, `libguides`, …) into the TIMDEX
parquet dataset. It’s two surfaces over the same data:

- a **web app** for browsing records, sources, and metadata analyses; and
- a **CLI** that doubles as a first-class **agent surface** — stable, scriptable
  commands an AI agent can drive to answer questions about the corpus.

Everything reads a TIMDEX dataset (a local path or an `s3://…` location). No
data is modified.

---

## Installation

Timmy installs as a standalone [uv](https://docs.astral.sh/uv/) tool. Clone the
repo and install it (editable, so updates are a `git pull` away):

```sh
git clone git@github.com:ghukill/timmy.git
uv tool install --editable ./timmy
```

This puts a `timmy` command on your PATH. If it isn’t found, run
`uv tool update-shell` and restart your shell.

```sh
timmy --help        # confirm it’s installed
```

## Quickstart

1. **Point Timmy at a dataset.** `init` is interactive and writes
   `~/.timmy/config.toml`:

   ```sh
   timmy init
   ```

   It prompts for the **dataset location** (a local path or `s3://…`), the
   directory where analyses are stored (defaults to `~/.timmy/analyses`), and the
   **log level** (`INFO` by default; set `DEBUG` to trace Timmy and the
   underlying timdex_dataset_api). If the dataset is on S3, make sure your **AWS
   credentials** are set in the environment first.

2. **Check what it resolved** (config layers: flags > env > `./timmy.toml` >
   `~/.timmy/config.toml` > defaults):

   ```sh
   timmy config show
   ```

3. **Use it** — launch the web app, or drive the CLI:

   ```sh
   timmy webapp run                 # browse in a browser
   timmy sources list               # every source + record counts (instant)
   ```

## CLI

The CLI is grouped by area. The headline commands:

| Command group | What it does |
|---|---|
| `timmy init` / `timmy config …` | Set up and inspect configuration. |
| `timmy webapp run` | Run the web app (see below). |
| `timmy sources …` | Per-source metadata: record counts, ETL run history. Cheap, metadata-only — no analysis needed. |
| `timmy analysis …` | Build and query **metadata analyses** (records flattened into a queryable DuckDB file) for field coverage, vocabulary, outliers, etc. |
| `timmy record …` | Inspect a single record version: metadata plus its raw source and transformed payloads. |
| `timmy docs …` | Read Timmy’s own documentation, or install it as an agent skill. |

Every read command takes `--json` for machine-readable output (stdout is data,
stderr is progress). For the **full, always-current** reference:

```sh
timmy --help                 # the command tree
timmy <command> -h           # help for any command
timmy docs catalog           # the complete command reference, generated live
timmy docs list              # all documentation topics
```

## Web app

```sh
timmy webapp run                          # http://127.0.0.1:5000
timmy webapp run --host 0.0.0.0 --port 8000 --debug
```

It’s a development server (not for production). Once it’s up, poke around:

- **Records** — browse and filter the corpus; open any record to see its source
  vs. transformed payloads (and diff versions).
- **Sources** — every source with its record count and full ETL run history.
- **Analyses** — build a metadata analysis over a filtered slice of records,
  then drill into field usage, distinct values, and per-record shape.

## AI / agent use

The CLI is designed to be driven by an AI agent, and Timmy ships the
documentation an agent needs to use it well. Install it as a **skill** for the
agent of your choice:

```sh
timmy docs install-skill                          # default: ~/.agents/skills
timmy docs install-skill --path /some/other/skills  # or any skills directory
```

This writes a self-contained `timmy/` skill (a `SKILL.md` entry point plus
reference files: the mental model, question→command **playbooks**, the analysis
schema, and a generated command reference). An agent with the skill can then
answer questions like:

- *“For `dspace`, how is the `subjects` field utilized?”*
- *“For `aspace`, find records with much more or less metadata than the rest.”*
- *“How do `libguides` records compare against `mitlibwebsite`?”*
- *“For record XYZ, why is the `contributors` field blank?”*

The skill is a **snapshot** — after upgrading Timmy or changing its docs, re-run
`install-skill` to refresh it. The live `timmy docs` commands are always current.
Don’t have an agent handy? The same content is readable directly:

```sh
timmy docs show overview     # start here
timmy docs show playbooks    # question -> command recipes
```
