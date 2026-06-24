# Playbooks: questions -> commands

Recipes for the question archetypes Timmy is built to answer. All assume you've
read `overview.md`. Add `--json` to any read command for machine-parseable
output. There is one corpus and no ids: read commands query the whole corpus, or
a subset via scope flags (`--source`, `--run-type`, `--action`, `--run-id`,
`--where`).

First, pick the right surface for the question:

- **Counts, sizes, "what sources exist", run history** -> `timmy sources`
  (metadata-only, instant, **no corpus needed**). See the next section.
- **Field *content*** (coverage, vocabulary, outliers, cross-source field
  comparison) -> the corpus. Make sure it exists, then scope as needed:
  ```sh
  timmy analysis status --json                 # built? how fresh?
  timmy analysis build                         # if missing (reads everything once)
  timmy analysis update                        # bring an existing corpus current (cheap)
  ```

Don't use the corpus to answer a counting question -- `timmy sources` already
has it without flattening payloads.

---

## "How many records / what sources exist?"

*e.g. "How many records does aspace have?", "Which sources are biggest?"* No
corpus -- this is pure metadata:

```sh
timmy sources list --json            # every source: record_count, versions, runs, first/last run
timmy sources show aspace --json     # one source's summary + its ETL runs
timmy sources runs --source aspace   # just the ETL run history
```

`record_count` is the authoritative *current* count. (The corpus `doc_count` can
be slightly lower -- it drops records with no transformed payload, like a current
version whose latest action is a delete. For "how many records", trust
`timmy sources`.)

## "How is field X used / what's its coverage?"

*e.g. "For dspace, how is the `subjects` field utilized?"* Scope to the source:

1. Coverage + cardinality at a glance:
   ```sh
   timmy analysis fields --source dspace --field subjects --json
   ```
   Reports `coverage_pct`, `distinct_values`, and per-record count
   min/avg/max -- i.e. how many records have it and how heavily.
2. The actual vocabulary:
   ```sh
   timmy analysis values --source dspace --path subjects --json
   ```
   Distinct values with document/occurrence counts. Skim the head for the
   dominant terms and the long tail for inconsistency.
3. Editorialize: is it controlled or free-text? Concentrated or sprawling?
   Often empty? Cite the numbers and a few example values.

Drop `--source dspace` to ask the same across the whole corpus. For nested
fields, use the parent path (`subjects` covers `subjects[].value`,
`subjects[].kind`, etc.); inspect sub-paths with `values --path subjects[].kind`.

## "Find outlier records (much more / much less metadata than others)"

*e.g. "For aspace, look for outlier records."* "Amount of metadata" = number of
eav leaves per record. Use the query escape hatch, scoped to the source:

```sh
timmy analysis query --source aspace "
  select d.timdex_record_id, count(*) as leaf_count
  from docs d join eav e using (timdex_composite_id)
  group by 1 order by leaf_count desc limit 25" --json
```

For the thin end, `order by leaf_count asc`. To frame "outlier" statistically,
compare against the distribution:

```sh
timmy analysis query --source aspace "
  with per_doc as (
    select timdex_composite_id, count(*) n
    from eav group by 1)
  select min(n), avg(n), median(n), max(n),
         quantile_cont(n, 0.95) as p95, quantile_cont(n, 0.05) as p05
  from per_doc"
```

With a scope active, `docs`/`eav` already cover only that subset, so the SQL
stays simple. Then pull the records past p95/below p05 and
`timmy record show <record-id>` a couple to characterize *why* they're fat/thin.

## "How do records from source A compare against source B?"

*e.g. "How does libguides compare against mitlibwebsite?"* It's all one corpus,
so just group by `docs.source` -- no separate builds, no ATTACH:

```sh
timmy analysis query --source libguides --source mitlibwebsite "
  select d.source, e.path, count(distinct d.timdex_composite_id) as docs
  from docs d join eav e using (timdex_composite_id)
  group by 1, 2 order by e.path, d.source"
```

This gives per-source coverage of each path side by side -- the core of a
comparison (which fields each source populates, and how often). Lead with the
structural diff (fields present in one but not the other, big coverage gaps),
then drill into a couple of telling fields with `values --source <one>`.

## "Why is field Z blank / wrong for record XYZ?"

*e.g. "For record XYZ, why is `contributors` blank?"* This is a single-record
question -- no corpus needed:

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
`docs`/`eav`/`corpus_meta` schema (see `schema.md`). Scope flags narrow what the
SQL sees:

```sh
timmy analysis query "<sql>" --json                 # whole corpus; or --csv
timmy analysis query --source dspace "<sql>" --json # scoped to a subset
```

The connection is read-only, so the engine itself rejects writes. Build queries
from the schema doc's worked patterns.

## Keeping the corpus current

```sh
timmy analysis status              # exists? doc_count, last_updated_at
timmy analysis update              # incremental reconcile (cheap; run anytime)
timmy analysis build --yes         # full rebuild from scratch (reads everything)
```
