"""Format detection by extension and magic bytes."""

from __future__ import annotations

import os

_EXT_MAP = {
    ".parquet": "parquet",
    ".pq": "parquet",
    ".avro": "avro",
    ".orc": "orc",
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
    ".csv": "csv",
    ".tsv": "tsv",
}


def detect_format(path: str) -> str | None:
    """Detect file format from extension. Returns format name or None for plain text."""
    # Strip query params for URLs
    clean = path.split("?")[0].split("#")[0]
    _, ext = os.path.splitext(clean.lower())
    return _EXT_MAP.get(ext)
