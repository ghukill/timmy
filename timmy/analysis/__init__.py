"""Analysis subsystem: flatten transformed TIMDEX records into EAV rows.

The first building block here is the recursive flattener (``flatten.py``), which
turns a parsed transformed record into ``(path, path_indexed, value,
value_type)`` rows. Later stages (build job, materialized DuckDB artifact,
``/analysis`` blueprint) consume these rows.
"""

from timmy.analysis.flatten import (
    EAVRow,
    SAMPLE_RECORD,
    flatten,
    flatten_record,
    make_timdex_composite_id,
)
from timmy.analysis.store import (
    OBJECT_IDENTITY_COLUMNS,
    PATH_VALUE_COLUMNS,
    VALUE_RECORD_COLUMNS,
    build_analysis,
    delete_analysis,
    field_usage,
    list_analyses,
    new_analysis_id,
    object_columns,
    object_field_paths,
    object_field_summaries,
    object_rows,
    open_analysis,
    path_values,
    read_manifest,
    update_manifest,
    value_records,
)

__all__ = [
    "OBJECT_IDENTITY_COLUMNS",
    "PATH_VALUE_COLUMNS",
    "VALUE_RECORD_COLUMNS",
    "EAVRow",
    "SAMPLE_RECORD",
    "build_analysis",
    "delete_analysis",
    "field_usage",
    "flatten",
    "flatten_record",
    "list_analyses",
    "make_timdex_composite_id",
    "new_analysis_id",
    "object_columns",
    "object_field_paths",
    "object_field_summaries",
    "object_rows",
    "open_analysis",
    "path_values",
    "read_manifest",
    "update_manifest",
    "value_records",
]
