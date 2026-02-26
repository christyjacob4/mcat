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
    ".xlsx": "excel",
    ".xls": "excel",
    ".feather": "feather",
    ".arrow": "arrow",
    ".json": "json",
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

    Handles compressed files by stripping the compression extension first.
    Returns format name or None for plain text.
    """
    from mcat.compression import detect_compression, strip_compression_ext

    # Strip query params for URLs
    clean = path.split("?")[0].split("#")[0]

    # Check for compression — if compressed, detect format on inner name
    comp = detect_compression(path)
    if comp and comp != "tar":
        inner = strip_compression_ext(clean)
        _, ext = os.path.splitext(inner.lower())
        fmt = _EXT_MAP.get(ext)
        if fmt:
            return fmt
        # Can't determine inner format from extension alone
        return None

    _, ext = os.path.splitext(clean.lower())
    fmt = _EXT_MAP.get(ext)
    if fmt:
        return fmt
    # Fall back to magic byte detection
    return _detect_magic(path)


def detect_format_verbose(path: str) -> tuple[str | None, str]:
    """Detect format and return (format, method) where method is 'extension' or 'magic-bytes'.

    Also reports compression if present.
    """
    from mcat.compression import detect_compression, strip_compression_ext

    clean = path.split("?")[0].split("#")[0]
    comp = detect_compression(path)

    if comp and comp != "tar":
        inner = strip_compression_ext(clean)
        _, ext = os.path.splitext(inner.lower())
        fmt = _EXT_MAP.get(ext)
        if fmt:
            return fmt, "extension"
        return None, "unknown"

    _, ext = os.path.splitext(clean.lower())
    fmt = _EXT_MAP.get(ext)
    if fmt:
        return fmt, "extension"
    magic = _detect_magic(path)
    if magic:
        return magic, "magic-bytes"
    return None, "unknown"
