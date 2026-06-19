"""Per-source configuration and payload formatting for record detail views.

`SOURCE_RECORD_FORMATS` maps a record's ``source`` to the serialization format
of its ``source_record`` payload, which drives pretty-printing and syntax
highlighting. ``transformed_record`` is always JSON, so it is not configured
here. Unknown sources fall back to ``DEFAULT_SOURCE_RECORD_FORMAT``.
"""

from __future__ import annotations

import json
from xml.dom import minidom
from xml.parsers.expat import ExpatError

# Format of each source's source_record payload.
SOURCE_RECORD_FORMATS: dict[str, str] = {
    "alma": "xml",
    "dspace": "xml",
    "aspace": "xml",
    "researchdatabases": "xml",
    "mitlibwebsite": "json",
    "libguides": "json",
    "gismit": "json",
    "gisogm": "json",
}

# Used when a source is not listed above; XML is the most common case.
DEFAULT_SOURCE_RECORD_FORMAT = "xml"


def get_source_record_format(source: str) -> str:
    """Return the source_record format ("xml" / "json") for a given source."""
    return SOURCE_RECORD_FORMATS.get(source, DEFAULT_SOURCE_RECORD_FORMAT)


def _to_text(payload: bytes | str | None) -> str:
    """Decode a payload (TDA returns bytes) to text, tolerating None."""
    if payload is None:
        return ""
    if isinstance(payload, bytes):
        return payload.decode("utf-8", errors="replace")
    return payload


def prettify(payload: bytes | str | None, fmt: str) -> str:
    """Pretty-print an XML or JSON payload for display.

    Falls back to the raw decoded text if the payload can't be parsed, so a
    malformed record still renders something useful.
    """
    text = _to_text(payload).strip()
    if not text:
        return ""
    try:
        if fmt == "json":
            return json.dumps(json.loads(text), indent=2, ensure_ascii=False)
        if fmt == "xml":
            pretty = minidom.parseString(text).toprettyxml(indent="  ")
            # minidom emits blank lines between nodes; drop them for readability.
            return "\n".join(line for line in pretty.splitlines() if line.strip())
    except (ValueError, TypeError, ExpatError):
        return text
    return text
