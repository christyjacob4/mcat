"""Format detection by extension and magic bytes."""

from __future__ import annotations

import os

_EXT_MAP = {
    ".parquet": "parquet",
    ".pq": "parquet",
    ".avro": "avro",
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
    ".csv": "csv",
    ".tsv": "tsv",
    ".xlsx": "excel",
    ".xls": "excel",
    ".json": "json",
}

_MAGIC_BYTES = {
    b"PAR1": "parquet",
    b"Obj\x01": "avro",
}


def _detect_magic(path: str) -> str | None:
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
    from mcat.compression import detect_compression, strip_compression_ext
    clean = path.split("?")[0].split("#")[0]
    comp = detect_compression(path)
    if comp and comp != "tar":
        inner = strip_compression_ext(clean)
        _, ext = os.path.splitext(inner.lower())
        fmt = _EXT_MAP.get(ext)
        if fmt:
            return fmt
        return None
    _, ext = os.path.splitext(clean.lower())
    fmt = _EXT_MAP.get(ext)
    if fmt:
        return fmt
    return _detect_magic(path)


def detect_format_verbose(path: str) -> tuple[str | None, str]:
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
