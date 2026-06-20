"""Recursively flatten a transformed TIMDEX record into EAV rows.

Each leaf of the JSON document becomes one :class:`EAVRow`. Two path forms are
emitted per row:

- ``path``        -- array indices collapsed to ``[]`` (the GROUP BY key for
  corpus-wide field-usage aggregates, e.g. ``contributors[].kind``).
- ``path_indexed``-- array indices preserved (e.g. ``contributors[0].kind``),
  for when a specific element matters.

A "leaf" is a scalar (string / number / boolean / null) OR an *empty* container.
An empty object/array is recorded as its own row (``object-empty`` /
``array-empty``) so "present but empty" stays distinguishable from absent --
which, in a flattened model, is simply the lack of any row for that path.

The flattener is pure and has no Flask/TDA dependency, so it is trivial to
exercise in a REPL against :data:`SAMPLE_RECORD` or a real transformed dict
pulled via ``sample_transformed(...)`` in the Flask shell.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any, NamedTuple


class EAVRow(NamedTuple):
    """One flattened leaf of a transformed record.

    ``value`` is always text (or ``None`` for nulls and empty containers); the
    original JSON type is preserved in ``value_type`` so numeric/boolean leaves
    can still be told apart after the cast to text.
    """

    path: str
    path_indexed: str
    value: str | None
    value_type: str


def _scalar(value: Any) -> tuple[str | None, str]:
    """Return ``(text_value, value_type)`` for a JSON scalar leaf.

    ``bool`` is checked before ``int`` because ``bool`` is a subclass of ``int``
    in Python; booleans render as lowercase ``true``/``false`` to match JSON.
    """
    if value is None:
        return None, "null"
    if isinstance(value, bool):
        return ("true" if value else "false"), "boolean"
    if isinstance(value, (int, float)):
        return str(value), "number"
    if isinstance(value, str):
        return value, "string"
    # Anything else (unexpected for parsed JSON) is coerced to its text form.
    return str(value), "string"


def flatten(
    record: Any,
    *,
    path: str = "",
    path_indexed: str = "",
) -> Iterator[EAVRow]:
    """Yield :class:`EAVRow` for every leaf in ``record``, depth-first.

    ``path`` / ``path_indexed`` carry the location of ``record`` within the
    document and are empty only for the top-level call.
    """
    if isinstance(record, Mapping):
        if not record:
            yield EAVRow(path, path_indexed, None, "object-empty")
            return
        for key, value in record.items():
            key = str(key)
            child_path = f"{path}.{key}" if path else key
            child_indexed = f"{path_indexed}.{key}" if path_indexed else key
            yield from flatten(value, path=child_path, path_indexed=child_indexed)
        return

    if isinstance(record, (list, tuple)):
        if not record:
            yield EAVRow(path, path_indexed, None, "array-empty")
            return
        for index, item in enumerate(record):
            child_path = f"{path}[]"
            child_indexed = f"{path_indexed}[{index}]"
            yield from flatten(item, path=child_path, path_indexed=child_indexed)
        return

    value_text, value_type = _scalar(record)
    yield EAVRow(path, path_indexed, value_text, value_type)


def flatten_record(record: Mapping[str, Any]) -> list[EAVRow]:
    """Eagerly flatten a whole transformed record into a list of rows."""
    return list(flatten(record))


def make_timdex_composite_id(
    timdex_record_id: str,
    run_id: str,
    run_record_offset: int,
) -> str:
    """Compose the stable per-version identity used to key EAV rows back to docs.

    A record *version* is uniquely identified by
    ``(timdex_record_id, run_id, run_record_offset)``. We join them with ``|``
    rather than ``:`` because ``timdex_record_id`` itself often contains colons
    (e.g. ``alma:990012345``).
    """
    return f"{timdex_record_id}|{run_id}|{run_record_offset}"


# A small, representative transformed-ish record for offline REPL testing. It
# exercises every branch of the flattener: nested object, list of objects, list
# of scalars, an empty list, an empty object, a null, a boolean, and a number.
SAMPLE_RECORD: dict[str, Any] = {
    "timdex_record_id": "alma:990012345",
    "title": "An Example Record",
    "languages": ["en", "fr"],
    "contributors": [
        {"value": "Ada Lovelace", "kind": "author"},
        {"value": "Charles Babbage", "kind": "author", "affiliation": None},
    ],
    "dates": [{"value": "1843", "kind": "publication"}],
    "citation": {"year": 1843, "peer_reviewed": True},
    "subjects": [],
    "notes": {},
}
