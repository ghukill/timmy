# Sources notes

TIMDEX aggregates many sources. The `source` field on every record names it, and
it's the primary filter for `analysis build` and the primary split key in
comparison queries (`docs.source`).

## Source-record formats

`source_record` payloads are harvested in the source's native serialization;
`transformed_record` is always TIMDEX JSON. `timmy record show <id>` reports the
detected source format and pretty-prints accordingly. Known formats (authority:
`timmy/sources.py`; unknown sources default to XML):

| Source | source_record format |
|---|---|
| `alma` | XML |
| `dspace` | XML |
| `aspace` | XML |
| `researchdatabases` | XML |
| `mitlibwebsite` | JSON |
| `libguides` | JSON |
| `gismit` | JSON |
| `gisogm` | JSON |

When reasoning about "why is field Z blank" (see `playbooks.md`), the format
tells you how to read the source: XML sources hide data in elements/attributes,
JSON sources in nested keys.

## Discovering sources in a dataset

Don't assume the list above is exhaustive for a given dataset. To see what's
actually present and how big each source is, use the **metadata-only** sources
surface -- it is instant (no payload reads) and needs **no analysis**:

```sh
timmy sources list --json            # every source: current record_count, versions, runs, dates
timmy sources show aspace --json     # one source's summary + its ETL runs
```

`record_count` here is the authoritative *current* count from
`metadata.current_records`. (Note it can differ from an analysis `doc_count`,
which excludes records with no transformed payload, e.g. a current version whose
latest action is a delete.) Never build an EAV analysis just to count records or
enumerate sources -- reach for `timmy sources` instead, and only `analysis
build` when the question is about *field content*.

## Per-source character

Sources differ a lot in how richly and consistently they populate fields -- that
variance is often the point of a question. Rather than hardcode claims that
drift, *measure* it per source with the coverage and outlier playbooks, and
report what the data shows for the dataset in front of you.
