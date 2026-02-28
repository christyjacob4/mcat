"""Side-by-side diff for structured files."""
from __future__ import annotations

import csv as csv_mod
import io
import json

from rich.console import Console
from rich.table import Table

console = Console()


def diff_files(path1: str, path2: str, opts: dict) -> None:
    """Compare two structured files and show differences."""
    rows1 = _load_rows(path1, opts)
    rows2 = _load_rows(path2, opts)

    # Gather all columns from both files, preserving order
    all_cols: list[str] = []
    if rows1:
        all_cols = list(rows1[0].keys())
    if rows2:
        for k in rows2[0].keys():
            if k not in all_cols:
                all_cols.append(k)

    col_filter = opts.get("columns")
    if col_filter:
        all_cols = [c for c in all_cols if c in col_filter]

    # Compare row by row
    max_rows = max(len(rows1), len(rows2))

    added = 0
    removed = 0
    modified = 0
    unchanged = 0

    table = Table(
        title=f"Diff: {path1} vs {path2}",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Row", justify="right", style="dim")
    table.add_column("Status", justify="center")
    for col in all_cols:
        table.add_column(col)

    for i in range(max_rows):
        r1 = rows1[i] if i < len(rows1) else None
        r2 = rows2[i] if i < len(rows2) else None

        if r1 is None:
            # Row exists only in file2 (added)
            added += 1
            table.add_row(
                str(i),
                "[green]+[/green]",
                *[f"[green]{r2.get(c, '')}[/green]" for c in all_cols],
            )
        elif r2 is None:
            # Row exists only in file1 (removed)
            removed += 1
            table.add_row(
                str(i),
                "[red]-[/red]",
                *[f"[red]{r1.get(c, '')}[/red]" for c in all_cols],
            )
        elif _rows_equal(r1, r2, all_cols):
            unchanged += 1
            # Only show unchanged rows when the total is small
            if max_rows <= 50:
                table.add_row(
                    str(i),
                    " ",
                    *[str(r1.get(c, "")) for c in all_cols],
                )
        else:
            modified += 1
            vals = []
            for c in all_cols:
                v1 = str(r1.get(c, ""))
                v2 = str(r2.get(c, ""))
                if v1 != v2:
                    vals.append(f"[red]{v1}[/red] → [green]{v2}[/green]")
                else:
                    vals.append(v1)
            table.add_row(str(i), "[yellow]~[/yellow]", *vals)

    console.print(table)
    console.print(
        f"\n[dim]{unchanged} unchanged · "
        f"[yellow]{modified} modified[/yellow] · "
        f"[green]{added} added[/green] · "
        f"[red]{removed} removed[/red][/dim]"
    )


def _rows_equal(r1: dict, r2: dict, cols: list[str]) -> bool:
    """Compare two row dicts across the given columns."""
    for c in cols:
        if str(r1.get(c, "")) != str(r2.get(c, "")):
            return False
    return True


def _load_rows(path: str, opts: dict) -> list[dict]:
    """Load a structured file into a list of dicts."""
    from mcat.detect import detect_format
    from mcat.structured import _open_file

    fmt = detect_format(path)
    if not fmt:
        raise ValueError(f"Cannot detect format of {path}")

    s3_endpoint = opts.get("s3_endpoint")

    if fmt == "parquet":
        return _load_parquet(path, s3_endpoint)
    elif fmt == "orc":
        return _load_orc(path, s3_endpoint)
    elif fmt == "avro":
        return _load_avro(path, s3_endpoint)
    elif fmt == "jsonl":
        return _load_jsonl(path, opts)
    elif fmt == "csv":
        return _load_csv(path, opts, delimiter=",")
    elif fmt == "tsv":
        return _load_csv(path, opts, delimiter="\t")
    elif fmt == "excel":
        return _load_excel(path, s3_endpoint)
    elif fmt in ("feather", "arrow"):
        return _load_feather(path, fmt, s3_endpoint)
    elif fmt == "json":
        return _load_json(path, opts)
    else:
        raise ValueError(f"Unsupported format for diff: {fmt}")


def _load_parquet(path: str, s3_endpoint: str | None) -> list[dict]:
    import pyarrow.parquet as pq
    from mcat.structured import _storage_options

    if "://" in path:
        import fsspec
        so = _storage_options(path, s3_endpoint)
        fs, fpath = fsspec.core.url_to_fs(path, **so)
        pf = pq.ParquetFile(fs.open(fpath, "rb"))
    else:
        pf = pq.ParquetFile(path)

    table = pf.read()
    rows_dict = table.to_pydict()
    if not rows_dict:
        return []
    keys = list(rows_dict.keys())
    n = len(rows_dict[keys[0]])
    return [{k: rows_dict[k][i] for k in keys} for i in range(n)]


def _load_orc(path: str, s3_endpoint: str | None) -> list[dict]:
    import pyarrow.orc as orc
    from mcat.structured import _open_file

    f = _open_file(path, s3_endpoint=s3_endpoint)
    reader = orc.ORCFile(f)
    table = reader.read()
    f.close()

    rows_dict = table.to_pydict()
    if not rows_dict:
        return []
    keys = list(rows_dict.keys())
    n = len(rows_dict[keys[0]])
    return [{k: rows_dict[k][i] for k in keys} for i in range(n)]


def _load_avro(path: str, s3_endpoint: str | None) -> list[dict]:
    import fastavro
    from mcat.structured import _open_file

    f = _open_file(path, s3_endpoint=s3_endpoint)
    reader = fastavro.reader(f)
    rows = list(reader)
    f.close()
    return rows


def _load_jsonl(path: str, opts: dict) -> list[dict]:
    from mcat.compression import detect_compression, decompress_open
    from mcat.structured import _open_file

    s3_endpoint = opts.get("s3_endpoint")
    comp = detect_compression(path)
    if comp and comp != "tar":
        raw_f = _open_file(path, "rb", s3_endpoint=s3_endpoint)
        decompressed = decompress_open(raw_f, comp)
        text_f = io.TextIOWrapper(decompressed, encoding="utf-8")
        rows: list[dict] = []
        for line in text_f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        text_f.close()
        try:
            decompressed.close()
        except Exception:
            pass
        try:
            raw_f.close()
        except Exception:
            pass
        return rows
    else:
        f = _open_file(path, "r", s3_endpoint=s3_endpoint)
        rows = []
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        f.close()
        return rows


def _load_csv(path: str, opts: dict, delimiter: str = ",") -> list[dict]:
    from mcat.compression import detect_compression, decompress_open
    from mcat.structured import _open_file

    s3_endpoint = opts.get("s3_endpoint")
    comp = detect_compression(path)
    if comp and comp != "tar":
        raw_f = _open_file(path, "rb", s3_endpoint=s3_endpoint)
        decompressed = decompress_open(raw_f, comp)
        text_f = io.TextIOWrapper(decompressed, encoding="utf-8")
        reader = csv_mod.DictReader(text_f, delimiter=delimiter)
        rows = list(reader)
        text_f.close()
        try:
            decompressed.close()
        except Exception:
            pass
        try:
            raw_f.close()
        except Exception:
            pass
        return rows
    else:
        f = _open_file(path, "r", s3_endpoint=s3_endpoint)
        reader = csv_mod.DictReader(f, delimiter=delimiter)
        rows = list(reader)
        f.close()
        return rows


def _load_excel(path: str, s3_endpoint: str | None) -> list[dict]:
    from mcat.structured import _open_file

    is_xls = path.split("?")[0].split("#")[0].lower().endswith(".xls")

    if is_xls:
        import xlrd
        f = _open_file(path, "rb", s3_endpoint=s3_endpoint)
        data = f.read()
        f.close()
        wb = xlrd.open_workbook(file_contents=data)
        sheet = wb.sheet_by_index(0)
        headers = [str(sheet.cell_value(0, c)) for c in range(sheet.ncols)]
        rows: list[dict] = []
        for r in range(1, sheet.nrows):
            rows.append({headers[c]: sheet.cell_value(r, c) for c in range(sheet.ncols)})
        return rows
    else:
        import openpyxl
        f = _open_file(path, "rb", s3_endpoint=s3_endpoint)
        wb = openpyxl.load_workbook(f, read_only=True, data_only=True)
        ws = wb.active
        row_iter = ws.iter_rows(values_only=True)
        headers = [str(c) if c is not None else "" for c in next(row_iter)]
        rows = []
        for values in row_iter:
            rows.append({headers[i]: values[i] for i in range(len(headers))})
        wb.close()
        f.close()
        return rows


def _load_feather(path: str, fmt: str, s3_endpoint: str | None) -> list[dict]:
    import pyarrow
    import pyarrow.feather
    import pyarrow.ipc
    from mcat.structured import _open_file

    f = _open_file(path, "rb", s3_endpoint=s3_endpoint)
    clean = path.split("?")[0].split("#")[0].lower()
    if clean.endswith(".arrow") or fmt == "arrow":
        table = pyarrow.ipc.open_stream(f).read_all()
    else:
        table = pyarrow.feather.read_table(f)
    f.close()

    rows_dict = table.to_pydict()
    if not rows_dict:
        return []
    keys = list(rows_dict.keys())
    n = len(rows_dict[keys[0]])
    return [{k: rows_dict[k][i] for k in keys} for i in range(n)]


def _load_json(path: str, opts: dict) -> list[dict]:
    from mcat.compression import detect_compression, decompress_open
    from mcat.structured import _open_file

    s3_endpoint = opts.get("s3_endpoint")
    comp = detect_compression(path)
    if comp and comp != "tar":
        raw_f = _open_file(path, "rb", s3_endpoint=s3_endpoint)
        decompressed = decompress_open(raw_f, comp)
        text_f = io.TextIOWrapper(decompressed, encoding="utf-8")
        data = json.load(text_f)
        text_f.close()
        try:
            decompressed.close()
        except Exception:
            pass
        try:
            raw_f.close()
        except Exception:
            pass
    else:
        f = _open_file(path, "r", s3_endpoint=s3_endpoint)
        data = json.load(f)
        f.close()

    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        raise ValueError(f"JSON file must contain an array or object: {path}")
    return data
