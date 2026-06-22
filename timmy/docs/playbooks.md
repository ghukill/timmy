# Playbooks: questions -> commands

Recipes for the question archetypes Timmy is built to answer. All assume you've
read `overview.md`. Add `--json` to any read command for machine-parseable
output. Replace `<id>` with an analysis id from `timmy analysis list`.

First, pick the right surface for the question:

- **Counts, sizes, "what sources exist", run history** -> `timmy sources`
  (metadata-only, instant, **no analysis**). See the next section.
- **Field *content*** (coverage, vocabulary, outliers, cross-source field
  comparison) -> an analysis, via find-or-build:
  ```sh
  timmy analysis list --source <source> --json   # reuse a recent one?
  timmy analysis build --source <source> --json  # else build, note the id
  ```

Don't build an analysis to answer a counting question -- that reads and flattens
every payload to compute something `timmy sources` already has.

---

## "How many records / what sources exist?"

*e.g. "How many records does aspace have?", "Which sources are biggest?"* No
analysis -- this is pure metadata:

```sh
timmy sources list --json            # every source: record_count, versions, runs, first/last run
timmy sources show aspace --json     # one source's summary + its ETL runs
timmy sources runs --source aspace   # just the ETL run history
```

`record_count` is the authoritative *current* count. (An analysis `doc_count`
can be slightly lower -- it drops records with no transformed payload, like a
current version whose latest action is a delete. For "how many records", trust
`timmy sources`.)

## "How is field X used / what's its coverage?"

*e.g. "For dspace, how is the `subjects` field utilized?"*

1. Coverage + cardinality at a glance:
   ```sh
   timmy analysis fields <id> --field subjects --json
   ```
   Reports `coverage_pct`, `distinct_values`, and per-record count
   min/avg/max -- i.e. how many records have it and how heavily.
2. The actual vocabulary:
   ```sh
   timmy analysis values <id> --path subjects --json
   ```
   Distinct values with document/occurrence counts. Skim the head for the
   dominant terms and the long tail for inconsistency.
3. Editorialize: is it controlled or free-text? Concentrated or sprawling?
   Often empty? Cite the numbers and a few example values.

The same recipe answers "how is `identifiers` used for alma?" -- swap the field.
For nested fields, use the parent path (`subjects` covers `subjects[].value`,
`subjects[].kind`, etc.); inspect sub-paths with `values --path subjects[].kind`.

## "Find outlier records (much more / much less metadata than others)"

*e.g. "For aspace, look for outlier records."* "Amount of metadata" = number of
eav leaves per record. Use the query escape hatch:

```sh
timmy analysis query <id> "
  select d.timdex_record_id, count(*) as leaf_count
  from docs d join eav e using (timdex_composite_id)
  group by 1 order by leaf_count desc limit 25" --json
```

For the thin end, `order by leaf_count asc`. To frame "outlier" statistically,
compare against the distribution:

```sh
timmy analysis query <id> "
  with per_doc as (
    select timdex_composite_id, count(*) n
    from eav group by 1)
  select min(n), avg(n), median(n), max(n),
         quantile_cont(n, 0.95) as p95, quantile_cont(n, 0.05) as p05
  from per_doc"
```

Then pull the records past p95/below p05 and `timmy record show <record-id>` a
couple to characterize *why* they're fat/thin (a repeated field? a whole missing
section?).

## "How do records from source A compare against source B?"

*e.g. "How does libguides compare against mitlibwebsite?"* Two ways:

- **One multi-source analysis** (simplest): build with both sources, then split
  by `docs.source` in SQL.
  ```sh
  timmy analysis build --source libguides --source mitlibwebsite --json
  timmy analysis query <id> "
    select d.source, e.path, count(distinct d.timdex_composite_id) as docs
    from docs d join eav e using (timdex_composite_id)
    group by 1, 2 order by e.path, d.source"
  ```
  This gives per-source coverage of each path side by side -- the core of a
  comparison (which fields each source populates, and how often).
- **Two analyses + ATTACH**: build one per source and `attach` the second inside
  a query against the first. Useful when you already have separate analyses.

Lead with the structural diff (fields present in one but not the other, big
coverage gaps), then drill into a couple of telling fields with `values`.

## "Why is field Z blank / wrong for record XYZ?"

*e.g. "For record XYZ, why is `contributors` blank?"* This is a single-record
question -- no analysis needed:

```sh
timmy record show XYZ --json          # current version: transformed + source
timmy record versions XYZ             # if you need a specific/older version
```

Confirm it's actually blank/absent in `transformed`, then read `source_record`
to see whether the data was present in the original at all:

- **Absent in source too** -> nothing to map; the source simply lacks it.
- **Present in source but not transformed** -> a mapping gap: the field exists in
  the source under some element/path that the transform didn't pick up. Quote
  the relevant source snippet and the transformed field, and explain the
  mismatch as best the payloads support.

To turn that inference into a definitive answer, read the transform code. The
`transformed_record` is produced by Transmogrifier, which Timmy clones locally:

```sh
timmy transmog status   # cloned? else: timmy transmog clone
timmy transmog path     # where the transform code lives
```

Then follow `transmogrifier.md`: `transmogrifier/config.py`'s `SOURCES` maps the
record's `source` to its transform class, whose `get_<field>` method is the exact
rule that did (or didn't) populate field Z. Cite the method and the source
element it reads.

## Bespoke questions

Anything the typed commands don't cover goes through SQL against the
`docs`/`eav`/`manifest` schema (see `schema.md`):

```sh
timmy analysis query <id> "<sql>" --json   # or --csv
```

The connection is read-only, so the engine itself rejects writes. Build queries
from the schema doc's worked patterns.

## Housekeeping

```sh
timmy analysis list --json                       # what exists
timmy analysis prune --older-than 30d --dry-run  # preview cleanup
timmy analysis delete <id> --yes                 # drop one
```
