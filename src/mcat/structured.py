"""Structured format handlers: Parquet, ORC, Avro, JSONL, CSV/TSV."""

from __future__ import annotations

import json
import sys
from typing import Any

from rich.console import Console
from rich.table import Table

console = Console()


def _storage_options(path: str, s3_endpoint: str | None = None) -> dict:
    """Build fsspec storage_options for the given path."""
    opts: dict = {}
    if s3_endpoint and path.startswith("s3://"):
        opts["client_kwargs"] = {"endpoint_url": s3_endpoint}
    return opts


def _open_file(path: str, mode: str = "rb", s3_endpoint: str | None = None):
    """Open local or remote file via fsspec."""
    if "://" in path:
        import fsspec
        return fsspec.open(path, mode, **_storage_options(path, s3_endpoint)).open()
    return open(path, mode)


def _cols_filter(columns: list[str] | None, all_columns: list[str]) -> list[str]:
    """Return filtered column list or all."""
    if not columns:
        return all_columns
    return [c for c in columns if c in all_columns]


def _print_table(rows: list[dict], columns: list[str] | None = None):
    """Print rows as a Rich table."""
    if not rows:
        return
    cols = columns or list(rows[0].keys())
    table = Table(show_header=True, header_style="bold cyan")
    for c in cols:
        table.add_column(c)
    for row in rows:
        table.add_row(*[str(row.get(c, "")) for c in cols])
    console.print(table)


def _print_jsonl(rows: list[dict], columns: list[str] | None = None):
    """Print rows as JSON Lines."""
    for row in rows:
        if columns:
            row = {k: v for k, v in row.items() if k in columns}
        print(json.dumps(row, default=str))


def _print_csv(rows: list[dict], columns: list[str] | None = None):
    """Print rows as CSV."""
    import csv
    import io
    if not rows:
        return
    cols = columns or list(rows[0].keys())
    writer = csv.DictWriter(io.TextIOWrapper(sys.stdout.buffer, write_through=True), fieldnames=cols, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)


def _output_rows(rows: list[dict], opts: dict):
    """Output rows in the requested format."""
    fmt = opts.get("format") or "table"
    columns = opts.get("columns")

    if fmt == "jsonl":
        _print_jsonl(rows, columns)
    elif fmt == "csv":
        _print_csv(rows, columns)
    elif fmt == "raw":
        for row in rows:
            print(row)
    else:
        _print_table(rows, columns)


def _apply_head_tail(rows: list[dict], opts: dict) -> list[dict]:
    """Apply --head and --tail limits."""
    if opts.get("head"):
        rows = rows[: opts["head"]]
    if opts.get("tail"):
        rows = rows[-opts["tail"]:]
    return rows


# --- Parquet ---

def _handle_parquet(path: str, opts: dict):
    import pyarrow.parquet as pq

    if "://" in path:
        import fsspec
        so = _storage_options(path, opts.get("s3_endpoint"))
        fs, fpath = fsspec.core.url_to_fs(path, **so)
        pf = pq.ParquetFile(fs.open(fpath, "rb"))
    else:
        pf = pq.ParquetFile(path)

    if opts.get("schema"):
        console.print(pf.schema_arrow)
        return

    col_filter = opts.get("columns")
    rows: list[dict] = []
    limit = opts.get("head")

    for i in range(pf.metadata.num_row_groups):
        rg = pf.read_row_group(i, columns=col_filter)
        batch_rows = rg.to_pydict()
        # Convert columnar to row-oriented
        if batch_rows:
            keys = list(batch_rows.keys())
            n = len(batch_rows[keys[0]])
            for j in range(n):
                rows.append({k: batch_rows[k][j] for k in keys})
                if limit and len(rows) >= limit:
                    break
        if limit and len(rows) >= limit:
            break

    rows = _apply_head_tail(rows, opts)
    _output_rows(rows, opts)


# --- ORC ---

def _handle_orc(path: str, opts: dict):
    import pyarrow.orc as orc

    f = _open_file(path, s3_endpoint=opts.get("s3_endpoint"))
    reader = orc.ORCFile(f)

    if opts.get("schema"):
        console.print(reader.schema)
        f.close()
        return

    col_filter = opts.get("columns")
    table = reader.read(columns=col_filter)
    rows_dict = table.to_pydict()
    f.close()

    if not rows_dict:
        return

    keys = list(rows_dict.keys())
    n = len(rows_dict[keys[0]])
    rows = [{k: rows_dict[k][i] for k in keys} for i in range(n)]

    rows = _apply_head_tail(rows, opts)
    _output_rows(rows, opts)


# --- Avro ---

def _handle_avro(path: str, opts: dict):
    try:
        import fastavro
    except ImportError:
        print("mcat: Avro support requires fastavro. Install with: pip install mcat[avro]", file=sys.stderr)
        raise SystemExit(1)

    f = _open_file(path, s3_endpoint=opts.get("s3_endpoint"))
    reader = fastavro.reader(f)

    if opts.get("schema"):
        console.print_json(json.dumps(reader.writer_schema, default=str))
        f.close()
        return

    col_filter = opts.get("columns")
    limit = opts.get("head")
    rows: list[dict] = []

    for record in reader:
        if col_filter:
            record = {k: v for k, v in record.items() if k in col_filter}
        rows.append(record)
        if limit and len(rows) >= limit:
            break

    f.close()
    rows = _apply_head_tail(rows, opts)
    _output_rows(rows, opts)


# --- JSONL ---

def _handle_jsonl(path: str, opts: dict):
    f = _open_file(path, "r", s3_endpoint=opts.get("s3_endpoint"))
    col_filter = opts.get("columns")
    limit = opts.get("head")
    rows: list[dict] = []

    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            print(line)
            continue
        if col_filter:
            obj = {k: v for k, v in obj.items() if k in col_filter}
        rows.append(obj)
        if limit and len(rows) >= limit:
            break

    f.close()
    rows = _apply_head_tail(rows, opts)
    _output_rows(rows, opts)


# --- CSV/TSV ---

def _handle_csv(path: str, opts: dict, delimiter: str = ","):
    import csv as csv_mod

    f = _open_file(path, "r", s3_endpoint=opts.get("s3_endpoint"))
    reader = csv_mod.DictReader(f, delimiter=delimiter)

    col_filter = opts.get("columns")

    if opts.get("schema"):
        # Print fieldnames
        console.print(reader.fieldnames)
        f.close()
        return

    limit = opts.get("head")
    rows: list[dict] = []

    for record in reader:
        if col_filter:
            record = {k: v for k, v in record.items() if k in col_filter}
        rows.append(record)
        if limit and len(rows) >= limit:
            break

    f.close()
    rows = _apply_head_tail(rows, opts)
    _output_rows(rows, opts)


# --- Dispatcher ---

_HANDLERS = {
    "parquet": _handle_parquet,
    "orc": _handle_orc,
    "avro": _handle_avro,
    "jsonl": _handle_jsonl,
    "csv": lambda p, o: _handle_csv(p, o, ","),
    "tsv": lambda p, o: _handle_csv(p, o, "\t"),
}


def handle_structured(path: str, fmt: str, opts: dict):
    """Dispatch to the appropriate structured handler."""
    handler = _HANDLERS.get(fmt)
    if not handler:
        raise ValueError(f"Unknown format: {fmt}")
    handler(path, opts)
