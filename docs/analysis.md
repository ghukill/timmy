# The analysis corpus: architecture & internals

How Timmy's metadata analysis works under the hood: one always-current corpus, live
subsets as query-time scopes, and the DuckDB temp-view trick that makes both the typed
drills and the raw SQL console respect a subset for free.

This is the **developer/architecture** doc. For *using* the analysis commands, see the
agent-facing docs (`timmy docs show overview` / `playbooks`, sourced from
`timmy/docs/*.md`). For the schema reference, `timmy docs show schema`.

Code map:

| Concern | Where |
|---|---|
| Flatten a record into EAV rows | `timmy/analysis/flatten.py` |
| Build / update the corpus, report cache | `timmy/analysis/corpus.py` |
| Subsets (scopes) + the shadow-view trick | `timmy/analysis/scope.py` |
| Shared EAV query layer (reports, drills) | `timmy/analysis/store.py` |
| Background build/update job + progress | `timmy/corpus_job.py` |
| Web routes (`/analysis/...`) | `timmy/analysis_views.py` |
| CLI (`timmy analysis ...`) | `timmy/cli.py` |

---

## 1. One corpus, not many analyses

There is a single **corpus**: one read-only `corpus.duckdb` (in the configured
`analysis_dir`) that flattens *every current record* into queryable rows. Its existence
is the whole "is there an analysis?" state — no registry, no per-analysis ids.

Two tables hold the data (`CORPUS_SCHEMA_SQL` in `corpus.py`):

- **`docs`** — one row per current record *version*. Primary key
  `timdex_composite_id = "{timdex_record_id}|{run_id}|{run_record_offset}"`, plus the
  scope-able metadata columns `source`, `run_date`, `run_type`, `action`,
  `run_timestamp`.
- **`eav`** — the flattened transformed payload, one row per JSON leaf, keyed back to
  `docs` by `timdex_composite_id`. `path` collapses array indices to `[]`
  (`contributors[].kind`, the corpus-wide GROUP BY key); `path_indexed` preserves them
  (`contributors[0].kind`); `value` is text and `value_type` keeps the original JSON
  type. (See `flatten.py` for the leaf rules — notably that *present-but-empty* gets its
  own `object-empty`/`array-empty` row, so it stays distinct from *absent*.)

Plus two bookkeeping tables: **`corpus_meta`** (one self-describing row: timestamps,
counts, dataset location) and **`scope_report_cache`** (see §5).

The **composite id is the version identity** — that single fact drives the whole update
algorithm below.

---

## 2. Build

`build_corpus` (`corpus.py`) streams every current record from TDA
(`read_dicts_iter(table="current_records")`), flattens each transformed payload, and
bulk-loads rows into `docs`/`eav` via Arrow (`_bulk_insert` + `EAV_ARROW_SCHEMA`,
~200× faster than row-at-a-time inserts), flushing every `BUILD_FLUSH_EVERY` records to
bound memory. Records whose current version is a *delete* (no transformed payload) are
counted under `skipped_count` and contribute nothing.

It writes to a `corpus.duckdb.building` temp file and **atomically renames** it into
place on success, so a failed or interrupted build never leaves a half-written corpus
(and never disturbs an existing one until the new one is ready). After loading it builds
the indexes (`_create_indexes`) and materializes the whole-corpus schema-overview report
into `scope_report_cache`.

Indexes that matter:

- `eav(path)` — the field-usage GROUP BY key.
- `eav(timdex_composite_id)` — lets a *scoped* query (§4) probe `eav` for a subset of
  composites instead of scanning all of it.
- `docs(source)`, `docs(run_timestamp)` — selective scope predicates.

---

## 3. Update: a composite-id set difference

`update_corpus` reconciles the corpus against the live dataset cheaply. Because the
composite id *is* the version identity, a changed record simply gets a new composite, so
the entire reconcile is a set difference between two sets of composites:

- `S` = composites of the live `current_records` (cheap, metadata-only — `_source_keyset`)
- `C` = composites already in the corpus `docs`

Then:

- **delete set** = `C − S` → vanished records *and* the stale half of changed records;
- **insert set** = `S − C`, restricted to non-delete rows → new records *and* the new
  half of changed records;
- **unchanged** = `S ∩ C` → never touched, never re-read.

A "changed" record falls out for free: old composite leaves via `C − S`, new composite
enters via `S − C`. The diff itself runs as DuckDB anti-joins over the keyset (validated
at ~5M records in ~1s).

The one expensive part — reading transformed payloads — is done **only for the insert
set**, scoped to its *distinct `run_id`s*: new/changed records are produced by ETL runs,
so the insert set spans just a few runs, and `read_dicts_iter(..., run_id=[...])` reads
roughly those runs' records rather than the whole corpus. The reconcile runs in one
transaction so concurrent readers see the pre-update snapshot until commit.

`run_timestamp` is stored for display/inspection, but the diff does **not** depend on it
— composite equality is exact.

---

## 4. Subsets are scopes, not separate analyses

To analyze a slice — `source=dspace`, `run_date > '2026-01-01'` — you don't build a new
artifact. You **scope** a read to that subset of the one corpus. A scope is a predicate
over `docs`, applied at query time, so it's always live (never stale) and there's nothing
to clean up.

A `Scope` (`scope.py`) is a set of IN-list filters over scope-able `docs` columns plus an
optional raw `where`. It compiles to a SQL `WHERE` body (`Scope.compile()`, inlining
IN-list values as quoted literals) and has a stable normalized cache key (`Scope.key()`;
`""` = the whole corpus). The web carries it as `f_source`/`f_run_type`/`f_action`/
`f_run_id`/`f_where` URL params; the CLI as `--source`/`--run-type`/…/`--where`.

### The finesse: temp views that *shadow* the base tables

This is the part worth understanding. The ~15 query functions in `store.py` — and any
SQL a user types into the console — are all written against bare `docs`/`eav`. We make
them see a subset **without rewriting a single query**, via `scoped()` (`scope.py`):

```python
with scoped(conn, scope):
    rows = top_level_fields(conn)   # or path_values(conn, ...), or a user's raw SQL
```

For a non-empty scope, `scoped()` creates two **connection-local temp views** that
shadow the base tables for the duration of the block:

```sql
create temp view docs as
  select * from corpus.docs where <scope predicate>;
create temp view eav as
  select e.* from corpus.eav e semi join docs d using (timdex_composite_id);
```

The mechanism is DuckDB **name resolution**: the `temp` schema takes precedence over the
base catalog, so an unqualified `from eav` / `from docs` resolves to these views —
`docs` filtered to the subset, `eav` restricted (by the semi-join) to only the
composites in that filtered `docs`. The view *bodies* reference the **qualified** base
tables (`corpus.<table>`, where `corpus` is `current_database()`) so they don't reference
themselves. On exit the views are dropped.

So the SQL console respects the subset because the console route does exactly the same
thing (`analysis_views.py`, `/query`): it parses the scope params out of the request
body, wraps `conn.execute(sql)` in `with scoped(conn, scope)`, and your query — never
inspected by us — simply sees a smaller `eav`/`docs`. One mechanism powers every typed
drill *and* the raw console.

Two consequences worth knowing:

- **It works on a read-only connection.** The corpus is opened read-only (which is what
  makes the SQL console safe to expose without sanitizing input), yet temp objects live
  in DuckDB's always-writable `temp` catalog, so the shadow views can still be created.
  This was spiked before committing to the approach.
- **It's self-cleaning.** The `with` block drops the views, and each request opens its
  own connection anyway, so nothing leaks between queries.

The seam: because it shadows *by name*, explicitly qualifying a base table in your SQL
(`select * from corpus.eav`) bypasses the subset and hits the full table. In practice
nobody writes that. And `corpus_meta` is deliberately *not* shadowed — metadata should
read true regardless of scope.

### Why this beats per-source files

A selective subset (e.g. `dspace` ≈ 3% of the corpus) probes `eav` via the composite
index and stays fast. A huge subset (`alma` ≈ 90%) costs about as much as the whole
corpus — but that's *inherent* to profiling that many records, not a property of the
storage. The old per-analysis model didn't make it cheaper; it paid the same cost at
build time. Scopes pay it on first view and then cache (§5), with none of the file /
routing / staleness machinery.

---

## 5. The schema-overview report cache

The schema-overview (`top_level_fields` — one row per top-level field with type,
coverage, cardinality) is a multi-minute full scan at the multi-million-record scale, so
it's cached. `field_usage_report(dir, scope)` (`corpus.py`):

1. computes `scope.key()` and reads `corpus_meta.last_updated_at` as the corpus version;
2. looks in `scope_report_cache` for `(scope_key, corpus_version)` — a hit returns
   instantly;
3. on a miss, computes `top_level_fields` *under the scope* and writes it to the cache.

The whole-corpus report (empty scope) is materialized at build/update time, so it's
always a hit. Each distinct subset computes once on first view, then is instant until the
corpus changes.

**Invalidation on update:** `update_corpus` bumps `last_updated_at`, deletes every
`scope_report_cache` row, and rewrites just the whole-corpus report at the new version.
So after an update, subset reports are gone and recompute lazily on next view. (Belt and
suspenders: even without the explicit delete, the reader keys on
`(scope_key, corpus_version)`, so a bumped version would miss the stale rows anyway.)

Only the *overview report* is cached. The drill tables (values / object / records) are
server-side paginated and recompute per request under the scope — cheap because they're
bounded and path-filtered.

**Serving it without hanging.** Computing the report is the one slow step, so it's never
done inline. The web dashboard renders instantly and fetches the report *fragment*
asynchronously (`GET /analysis/report` → the `_report_table.html` partial, injected
behind a spinner), so a large subset's first (uncached) report computes without blocking
the page; the spinner always shows the scoped doc count, and adds a "may take a few
minutes" note above `REPORT_WARN_THRESHOLD` (100k). The CLI can't show a spinner, so on
the same slow path `field_usage_report` emits a `logger.warning` (surfaced on stderr —
the read commands enable logging via `_open_corpus` / the `fields` command) telling you
it's computing and will cache. Both cues live at the one shared chokepoint
(`field_usage_report`), gated on a cache miss whose scoped doc count exceeds the
threshold.

> Note (current behavior): an update bumps the version and clears the cache
> *unconditionally*, even a no-op update that changed nothing. A natural optimization is
> to only bump/clear when the diff actually inserted or deleted rows.

---

## 6. Long-running builds without a task queue

Build and update can run for minutes, so the web app runs one at a time in a background
daemon thread (`corpus_job.py`), not a task queue. The running job updates an in-memory
progress snapshot (phase, done/total, elapsed, error/result); the progress page polls
`/analysis/job.json` and renders a real progress bar (the denominator is the
`current_records` count for a build, or the insert-set size for an update). You can
navigate away and back — progress lives server-side.

The job holds the app's `dataset_lock` for its duration (TDA reads go through the shared
connection), so record browsing blocks while a build runs — acceptable for a single-user
tool. It's deliberately the "thread + poll" sweet spot, not a durable job system: if the
process dies mid-build, the snapshot is lost, but the `.building` + atomic-rename means a
dead build just leaves no corpus (clean retry).
