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

## An analysis is an immutable file

An **analysis** is a self-contained, read-only `<analysis_id>.duckdb` file built
from a filtered slice of records (e.g. "all current `dspace` records"). It holds
`docs`, `eav`, and a self-describing `manifest`. Because it's just a file, it's
portable, cheap to drop, and you can `ATTACH` two of them to compare.

Building one reads the dataset and can take seconds to minutes depending on
corpus size, so analyses are reused, not rebuilt per question.

## Two surfaces: cheap metadata vs. analyses

Not every question needs an analysis. There are two layers:

- **`timmy sources`** -- instant, metadata-only aggregates (record counts per
  source, version/run history, dates). No payload reads, no artifact. This
  answers "how many records", "what sources exist", "what runs happened".
- **`timmy analysis ...`** -- the EAV model above, for questions about *field
  content* (coverage, vocabulary, outliers, comparisons).

Reach for the cheap surface first; only build an analysis when the question is
genuinely about what's *inside* the records.

## The find-or-build pattern

Most questions are answered against an analysis. The flow is explicit (no hidden
rebuilds):

1. `timmy analysis list [--source X] --json` -- is there a recent analysis to
   reuse?
2. if not, `timmy analysis build --source X --json` -- build one, get its id.
3. answer the question with `analysis fields` / `values` / `records` / `query`.

Prefer reusing a recent analysis; only build when none fits, or when the user
wants fresh data. Tell the user when you build one (it has a cost) and that it
persists until pruned.

## You are expected to editorialize

Many questions are open-ended ("how is `subjects` used?", "find outlier
records", "how do these sources compare?"). Timmy gives you the data; the
*interpretation* is your job. Ground every claim in a number or a record you
pulled, name the analysis id you used, and say when something is judgement vs.
measurement. If a question needs data that doesn't exist yet (e.g. no analysis
for that source), say so and offer to build it rather than guessing.

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
