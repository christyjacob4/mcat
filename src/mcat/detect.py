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

# Magic byte signatures for binary formats
_MAGIC_BYTES = {
    b"PAR1": "parquet",
    b"ORC": "orc",
    b"Obj\x01": "avro",
}


def _detect_magic(path: str) -> str | None:
    """Try to detect format from file magic bytes (local files only)."""
    if "://" in path:
        return None
    try:
        with open(path, "rb") as f:
            header = f.read(4)
        if not header:
            return None
        for magic, fmt in _MAGIC_BYTES.items():
            if header[: len(magic)] == magic:
                return fmt
    except (OSError, IOError):
        pass
    return None


def detect_format(path: str) -> str | None:
    """Detect file format from extension, falling back to magic bytes.

    Returns format name or None for plain text.
    """
    # Strip query params for URLs
    clean = path.split("?")[0].split("#")[0]
    _, ext = os.path.splitext(clean.lower())
    fmt = _EXT_MAP.get(ext)
    if fmt:
        return fmt
    # Fall back to magic byte detection
    return _detect_magic(path)


def detect_format_verbose(path: str) -> tuple[str | None, str]:
    """Detect format and return (format, method) where method is 'extension' or 'magic-bytes'."""
    clean = path.split("?")[0].split("#")[0]
    _, ext = os.path.splitext(clean.lower())
    fmt = _EXT_MAP.get(ext)
    if fmt:
        return fmt, "extension"
    magic = _detect_magic(path)
    if magic:
        return magic, "magic-bytes"
    return None, "unknown"
