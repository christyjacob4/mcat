"""Transparent compression detection and decompression."""

from __future__ import annotations

import os
from typing import BinaryIO

# Extension → compression type
_COMP_EXT_MAP = {
    ".gz": "gzip",
    ".gzip": "gzip",
    ".zst": "zstd",
    ".zstd": "zstd",
    ".bz2": "bz2",
    ".bzip2": "bz2",
    ".lz4": "lz4",
    ".xz": "xz",
    ".br": "brotli",
}

# Magic bytes → compression type
_COMP_MAGIC = [
    (b"\x1f\x8b", "gzip"),
    (b"\x28\xb5\x2f\xfd", "zstd"),
    (b"BZh", "bz2"),
    (b"\x04\x22\x4d\x18", "lz4"),
    (b"\xfd7zXZ\x00", "xz"),
]

# Tar-related extensions
_TAR_EXTENSIONS = {".tar.gz", ".tgz", ".tar.bz2", ".tar.xz", ".tar.zst"}


def detect_compression(path: str) -> str | None:
    """Detect compression from file extension."""
    clean = path.split("?")[0].split("#")[0].lower()

    # Check for tar archives first
    for tar_ext in _TAR_EXTENSIONS:
        if clean.endswith(tar_ext):
            return "tar"

    _, ext = os.path.splitext(clean)
    return _COMP_EXT_MAP.get(ext)


def detect_compression_magic(file_obj: BinaryIO) -> str | None:
    """Detect compression from magic bytes. Resets file position."""
    pos = file_obj.tell()
    header = file_obj.read(6)
    file_obj.seek(pos)
    if not header:
        return None
    for magic, comp in _COMP_MAGIC:
        if header[:len(magic)] == magic:
            return comp
    return None


def strip_compression_ext(path: str) -> str:
    """Strip compression extension to get the inner filename."""
    clean = path.split("?")[0].split("#")[0]
    lower = clean.lower()
    for ext in _COMP_EXT_MAP:
        if lower.endswith(ext):
            return clean[: len(clean) - len(ext)]
    return clean


def decompress_open(file_obj: BinaryIO, compression: str) -> BinaryIO:
    """Wrap a file object with transparent decompression.

    Returns a file-like object that decompresses on read.
    """
    if compression == "gzip":
        import gzip
        return gzip.open(file_obj, "rb")
    elif compression == "bz2":
        import bz2
        return bz2.open(file_obj, "rb")
    elif compression == "xz":
        import lzma
        return lzma.open(file_obj, "rb")
    elif compression == "zstd":
        import zstandard
        reader = zstandard.ZstdDecompressor().stream_reader(file_obj)
        return reader
    elif compression == "lz4":
        import lz4.frame
        return lz4.frame.open(file_obj, "rb")
    else:
        raise ValueError(f"Unsupported compression: {compression}")
