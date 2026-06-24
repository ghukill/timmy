# Timmy overview (start here)

Timmy inspects and analyzes **TIMDEX metadata** from the shell. This page is the
mental model; `playbooks.md` turns it into concrete command recipes, and
`commands.md` / `schema.md` are the generated reference.

## What the data is

TIMDEX aggregates library metadata from many **sources** (e.g. `alma`, `dspace`,
`aspace`, `libguides`, `mitlibwebsite`). Each record exists in two forms:

- **`source_record`** -- the original payload as harvested (XML or JSON,
  depending on source; see `sources.md`).
- **`transformed_record`** -- the normalized TIMDEX JSON produced from the
  source by the Transmogrifier pipeline. This is the shape downstream systems
  index, and the shape Timmy profiles.

A record is versioned: every ETL run can produce a new version, so a record
*version* is uniquely `(timdex_record_id, run_id, run_record_offset)`. The
"current" version is the latest non-deleted one.

## The analysis model (EAV)

Profiling a free-form JSON corpus is awkward field-by-field, so Timmy
**flattens** each `transformed_record` into entity-attribute-value rows: one row
per JSON leaf, keyed back to its record. Array indices collapse to `[]` in
`path` (`contributors[].kind`) so you can aggregate across the whole corpus, and
are preserved in `path_indexed` (`contributors[0].kind`) when a specific element
matters. See `schema.md` for the exact tables and `value_type` values.

Key consequence: **absent vs. empty are different.** A field that isn't present
produces *no* eav row; a field that's present but empty produces a row with
`value_type` `object-empty`/`array-empty`. "Coverage" means "how many records
have at least one row under this path."

## One corpus, kept current

There is a single **corpus** -- one read-only `corpus.duckdb` flattening *every
current record* into `docs` + `eav`. It isn't built per question; it's built once
and then kept up to date:

- `timmy analysis status` -- does the corpus exist, how big is it, when was it
  last updated?
- `timmy analysis build` -- (re)build it from all current records. Reads the
  whole dataset once, so it can take minutes.
- `timmy analysis update` -- reconcile it with the live dataset incrementally
  (adds new/changed records, drops vanished ones). Cost scales with what changed,
  not the corpus size -- cheap, run it freely.

If `status` shows no corpus, build it before asking field-content questions.

## Subsets are scopes, not separate analyses

To analyze a slice -- `source=dspace`, `run_date > '2026-01-01'` -- you don't
build a new artifact; you **scope** a read command to that subset of the one
corpus. Every read command takes scope flags:

```sh
timmy analysis fields --source dspace
timmy analysis values --path subjects --source dspace
timmy analysis query --where "run_date > '2026-01-01'" "select count(*) from docs"
```

No flags = the whole corpus. A scope is a live filter over the current corpus, so
it's never stale and there's nothing to clean up.

## Two surfaces: cheap metadata vs. the corpus

Not every question needs the corpus. There are two layers:

- **`timmy sources`** -- instant, metadata-only aggregates (record counts per
  source, version/run history, dates). No payload reads. This answers "how many
  records", "what sources exist", "what runs happened".
- **`timmy analysis ...`** -- the EAV corpus above, for questions about *field
  content* (coverage, vocabulary, outliers, comparisons).

Reach for the cheap surface first; use the corpus when the question is genuinely
about what's *inside* the records.

## You are expected to editorialize

Many questions are open-ended ("how is `subjects` used?", "find outlier
records", "how do these sources compare?"). Timmy gives you the data; the
*interpretation* is your job. Ground every claim in a number or a record you
pulled, name the scope you used (e.g. `--source dspace`), and say when something
is judgement vs. measurement. If the corpus doesn't exist yet, say so and offer
to build it rather than guessing.

## Single records

For "why does record XYZ look like this?", drop to the record itself:
`timmy record show <id>` returns its metadata, the transformed JSON, and the raw
source payload. Comparing the source against the transformed output is how you
reason about *why* a transformed field is blank or surprising.

To go from inference to a definitive answer, read the transform code itself: the
`transformed_record` was produced from the source by **Transmogrifier**, which
Timmy can clone locally (`timmy transmog clone`). See `transmogrifier.md` for how
records become TIMDEX records and how to find the exact `get_<field>` mapping for
a source.
