"""Analysis subsystem: the single, always-current metadata corpus.

The flattener (``flatten.py``) turns a parsed transformed record into ``(path,
path_indexed, value, value_type)`` rows. ``corpus.py`` materializes every current
record's rows into one ``corpus.duckdb`` and keeps it current; ``scope.py`` narrows
queries to a live subset; ``store.py`` holds the shared flatten/query layer both the
corpus and the ``/analysis`` blueprint read through.
"""

from timmy.analysis.corpus import (
    CORPUS_FILENAME,
    build_corpus,
    corpus_exists,
    corpus_path,
    delete_corpus,
    field_usage_report,
    open_corpus,
    read_corpus_meta,
    update_corpus,
)
from timmy.analysis.flatten import (
    EAVRow,
    SAMPLE_RECORD,
    flatten,
    flatten_record,
    make_timdex_composite_id,
)
from timmy.analysis.run_diff import (
    diff_run,
    run_meta,
)
from timmy.analysis.scope import (
    EMPTY_SCOPE,
    SCOPE_COLUMNS,
    Scope,
    make_scope,
    scoped,
)
from timmy.analysis.store import (
    OBJECT_IDENTITY_COLUMNS,
    OBJECT_RECORD_COLUMNS,
    PATH_RECORD_COLUMNS,
    PATH_VALUE_COLUMNS,
    VALUE_RECORD_COLUMNS,
    object_columns,
    object_field_paths,
    object_member_stats,
    object_record_shape,
    object_rows,
    path_record_counts,
    path_values,
    top_level_fields,
    value_records,
)

__all__ = [
    "CORPUS_FILENAME",
    "EMPTY_SCOPE",
    "SCOPE_COLUMNS",
    "Scope",
    "build_corpus",
    "corpus_exists",
    "corpus_path",
    "delete_corpus",
    "diff_run",
    "field_usage_report",
    "make_scope",
    "run_meta",
    "open_corpus",
    "read_corpus_meta",
    "scoped",
    "update_corpus",
    "OBJECT_IDENTITY_COLUMNS",
    "OBJECT_RECORD_COLUMNS",
    "PATH_RECORD_COLUMNS",
    "PATH_VALUE_COLUMNS",
    "VALUE_RECORD_COLUMNS",
    "EAVRow",
    "SAMPLE_RECORD",
    "flatten",
    "flatten_record",
    "make_timdex_composite_id",
    "object_columns",
    "object_field_paths",
    "object_member_stats",
    "object_record_shape",
    "object_rows",
    "path_record_counts",
    "path_values",
    "top_level_fields",
    "value_records",
]
