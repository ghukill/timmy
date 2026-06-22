# Transmogrifier: how records become TIMDEX records

Every `transformed_record` Timmy profiles was produced from a `source_record` by
**Transmogrifier** (https://github.com/MITLibraries/transmogrifier) -- the ETL
transform engine. Source records are harvested, Transmogrifier maps them into the
normalized TIMDEX JSON shape, and *that* is what lands in the dataset. In other
words: **Transmogrifier is how records enter TIMDEX.** Timmy reads the finished
records; it does not run the transform.

For the "why does field Z look like this for record XYZ?" class of question
(`playbooks.md`), reasoning from source-vs-transformed payloads gets you an
*inference*. The **definitive** answer is in the transform code: the exact rule
that did (or didn't) populate the field. Timmy clones the real repo so that code
is on disk for you to read.

## Get the code on disk

```sh
timmy transmog status        # cloned? at which commit?
timmy transmog clone         # clone it (once) into ~/.timmy/transmogrifier
timmy transmog update        # fast-forward an existing checkout to upstream
timmy transmog path          # print the checkout path (scriptable)
```

`transmog path` is the breadcrumb: it tells you where the transform code lives so
you can open and read it. Everything below is navigation *within that tree* --
read the actual files, since this doc describes the layout, not the line-by-line
mappings (which evolve with the repo).

## Navigating the transform code

The repo's own `transmogrifier/` package holds the transform code (you can skip
`tests/` and `docs/` for this purpose -- but **not** the top-level `config/`,
which holds lookup tables the transformers load; see step 4):

1. **`transmogrifier/config.py` -> `SOURCES`** is the registry. Each Timmy
   `source` name maps to a `transform-class` (a dotted path), e.g.
   `dspace -> transmogrifier.sources.xml.dspace_mets.DspaceMets`,
   `libguides -> transmogrifier.sources.json.libguides.LibGuides`. Start here to
   go from a source name to the class that transforms it. (A few sources share a
   class, e.g. `gismit`/`gisogm`.)
2. **`transmogrifier/sources/`** holds the transformers, split by source-record
   serialization: `sources/xml/*.py` and `sources/json/*.py`. The base
   `sources/transformer.py` (the `Transformer` ABC) defines the contract; the
   intermediate `xmltransformer.py` / `jsontransformer.py` add per-format
   machinery; the concrete class for a source subclasses one of those.
3. **Fields are methods.** A TIMDEX field maps to a `get_<field>` method on the
   transformer -- `get_subjects`, `get_dates`, `get_contributors`, etc. To learn
   why `subjects` is shaped the way it is for a source, read that source's
   `get_subjects` (and the source-record snippet it reads from). Required core
   fields (e.g. `get_main_titles`) are abstract on the base and implemented per
   source; optional fields are wired through `get_optional_field_methods`.
4. **External lookup tables live in `config/*.json`.** A `get_<field>` method
   often doesn't hardcode its mapping -- it reads a crosswalk loaded near the top
   of the module via `load_external_config("config/<name>.json", "json")` (e.g.
   `aspace_type_crosswalk` in `sources/xml/ead.py`). When a method's value
   resolves through one of these, the actual term/normalization is in that JSON,
   so read it too -- it's the difference between "the kind comes from a crosswalk"
   and naming the exact value (and what the fallback would have been).

## The "why is field Z like this?" workflow, made definitive

Building on the `playbooks.md` recipe -- once the checkout exists you can close
the inference gap:

1. `timmy record show <id>` -- confirm the field's transformed value and read the
   `source_record` (note the `source` and its format).
2. `transmogrifier/config.py` -> `SOURCES[<source>]["transform-class"]` -- find
   the transformer class for that source.
3. Open that class under `timmy transmog path`'s tree and read its `get_<field>`
   method (walking up to the parent class if it's inherited, not overridden).
4. Now state definitively *why*: the method reads element/key `…`, applies `…`,
   and that's why the transformed field is empty / populated / surprising.

Caveat: a transformer spans an inheritance chain (concrete -> format base ->
`Transformer`), a single field can pull from helpers, and the final value may
resolve through a `config/*.json` crosswalk (see "Navigating the transform code"
above). Read up the chain and
into any crosswalk it loads, rather than assuming the concrete `get_<field>`
method is the whole story.

## What this is not (yet)

This surface is **read-only interrogation** of the transform code -- it does not
run Transmogrifier. Running a transform over real records into a temporary,
browsable dataset (to compare transform logic against the live output) is a
planned, separate capability; see `scratch/ideas.md` -> "Transforms and Diffs".
For now, the cloned code plus the source-vs-transformed payloads is how you
reason about transforms.
