"""Structured format handlers: Parquet, ORC, Avro, JSONL, CSV/TSV."""

from __future__ import annotations

import json
import sys
from typing import Any

from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn
from rich.table import Table

console = Console()
err_console = Console(stderr=True)


def _should_show_progress() -> bool:
    """Show progress only when both stdout and stderr are TTYs."""
    return sys.stderr.isatty() and sys.stdout.isatty()


def _make_bar_progress() -> Progress:
    """Create a progress bar with spinner, text, bar, and percentage (for known totals)."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=Console(stderr=True),
    )


def _make_spinner_progress() -> Progress:
    """Create a spinner-only progress indicator (for unknown totals)."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=Console(stderr=True),
    )


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


def _sample_rows(rows: list[dict], n: int) -> list[dict]:
    """Return a random sample of *n* rows."""
    import random
    if n >= len(rows):
        return rows
    return random.sample(rows, n)


def _output_rows(rows: list[dict], opts: dict):
    """Output rows in the requested format."""
    sample_n = opts.get("sample")
    if sample_n is not None:
        rows = _sample_rows(rows, sample_n)
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

    # --count: use metadata for instant row count
    if opts.get("count"):
        print(pf.metadata.num_rows)
        return

    col_filter = opts.get("columns")

    num_row_groups = pf.metadata.num_row_groups
    show_progress = num_row_groups > 1 and _should_show_progress()

    # Smart --tail for Parquet: read only the last row group(s) needed
    if opts.get("tail") and not opts.get("head"):
        tail_n = opts["tail"]
        rows: list[dict] = []
        # Read row groups from the end
        if show_progress:
            with _make_bar_progress() as progress:
                task = progress.add_task("Reading...", total=num_row_groups)
                for i in range(num_row_groups - 1, -1, -1):
                    rg = pf.read_row_group(i, columns=col_filter)
                    batch_rows = rg.to_pydict()
                    if batch_rows:
                        keys = list(batch_rows.keys())
                        n = len(batch_rows[keys[0]])
                        chunk = [{k: batch_rows[k][j] for k in keys} for j in range(n)]
                        rows = chunk + rows
                    progress.update(task, advance=1)
                    if len(rows) >= tail_n:
                        rows = rows[-tail_n:]
                        break
        else:
            for i in range(num_row_groups - 1, -1, -1):
                rg = pf.read_row_group(i, columns=col_filter)
                batch_rows = rg.to_pydict()
                if batch_rows:
                    keys = list(batch_rows.keys())
                    n = len(batch_rows[keys[0]])
                    chunk = [{k: batch_rows[k][j] for k in keys} for j in range(n)]
                    rows = chunk + rows
                    if len(rows) >= tail_n:
                        rows = rows[-tail_n:]
                        break
        _output_rows(rows, opts)
        return

    limit = opts.get("head")
    rows = []

    if show_progress:
        with _make_bar_progress() as progress:
            task = progress.add_task("Reading...", total=num_row_groups)
            for i in range(num_row_groups):
                rg = pf.read_row_group(i, columns=col_filter)
                batch_rows = rg.to_pydict()
                if batch_rows:
                    keys = list(batch_rows.keys())
                    n = len(batch_rows[keys[0]])
                    for j in range(n):
                        rows.append({k: batch_rows[k][j] for k in keys})
                        if limit and len(rows) >= limit:
                            break
                progress.update(task, advance=1)
                if limit and len(rows) >= limit:
                    break
    else:
        for i in range(num_row_groups):
            rg = pf.read_row_group(i, columns=col_filter)
            batch_rows = rg.to_pydict()
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

    if opts.get("count"):
        print(reader.nrows)
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
    import fastavro

    f = _open_file(path, s3_endpoint=opts.get("s3_endpoint"))
    reader = fastavro.reader(f)

    if opts.get("schema"):
        console.print_json(json.dumps(reader.writer_schema, default=str))
        f.close()
        return

    if opts.get("count"):
        count = sum(1 for _ in reader)
        print(count)
        f.close()
        return

    col_filter = opts.get("columns")
    limit = opts.get("head")
    rows: list[dict] = []

    if _should_show_progress():
        with _make_spinner_progress() as progress:
            task = progress.add_task("Reading rows...", total=None)
            for record in reader:
                if col_filter:
                    record = {k: v for k, v in record.items() if k in col_filter}
                rows.append(record)
                progress.update(task, description=f"Reading... {len(rows)} rows")
                if limit and len(rows) >= limit:
                    break
    else:
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

    if opts.get("count"):
        count = sum(1 for line in f if line.strip())
        print(count)
        f.close()
        return

    rows: list[dict] = []

    if _should_show_progress():
        with _make_spinner_progress() as progress:
            task = progress.add_task("Reading rows...", total=None)
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
                progress.update(task, description=f"Reading... {len(rows)} rows")
                if limit and len(rows) >= limit:
                    break
    else:
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
        console.print(reader.fieldnames)
        f.close()
        return

    if opts.get("count"):
        count = sum(1 for _ in reader)
        print(count)
        f.close()
        return

    limit = opts.get("head")
    rows: list[dict] = []

    if _should_show_progress():
        with _make_spinner_progress() as progress:
            task = progress.add_task("Reading rows...", total=None)
            for record in reader:
                if col_filter:
                    record = {k: v for k, v in record.items() if k in col_filter}
                rows.append(record)
                progress.update(task, description=f"Reading... {len(rows)} rows")
                if limit and len(rows) >= limit:
                    break
    else:
        for record in reader:
            if col_filter:
                record = {k: v for k, v in record.items() if k in col_filter}
            rows.append(record)
            if limit and len(rows) >= limit:
                break

    f.close()
    rows = _apply_head_tail(rows, opts)
    _output_rows(rows, opts)


# --- Excel ---

def _handle_excel(path: str, opts: dict):
    is_xls = path.split("?")[0].split("#")[0].lower().endswith(".xls")

    if is_xls:
        import xlrd
        f = _open_file(path, "rb", s3_endpoint=opts.get("s3_endpoint"))
        data = f.read()
        f.close()
        wb = xlrd.open_workbook(file_contents=data)
        sheet = wb.sheet_by_index(0)
        headers = [str(sheet.cell_value(0, c)) for c in range(sheet.ncols)]

        if opts.get("schema"):
            console.print(headers)
            return
        if opts.get("count"):
            print(sheet.nrows - 1)
            return

        col_filter = opts.get("columns")
        cols = _cols_filter(col_filter, headers)
        rows: list[dict] = []
        for r in range(1, sheet.nrows):
            row = {headers[c]: sheet.cell_value(r, c) for c in range(sheet.ncols)}
            if col_filter:
                row = {k: v for k, v in row.items() if k in cols}
            rows.append(row)
    else:
        import openpyxl
        f = _open_file(path, "rb", s3_endpoint=opts.get("s3_endpoint"))
        wb = openpyxl.load_workbook(f, read_only=True, data_only=True)
        ws = wb.active
        row_iter = ws.iter_rows(values_only=True)
        headers = [str(c) if c is not None else "" for c in next(row_iter)]

        if opts.get("schema"):
            console.print(headers)
            wb.close()
            f.close()
            return
        if opts.get("count"):
            count = sum(1 for _ in row_iter)
            print(count)
            wb.close()
            f.close()
            return

        col_filter = opts.get("columns")
        cols = _cols_filter(col_filter, headers)
        rows = []
        for values in row_iter:
            row = {headers[i]: values[i] for i in range(len(headers))}
            if col_filter:
                row = {k: v for k, v in row.items() if k in cols}
            rows.append(row)
        wb.close()
        f.close()

    rows = _apply_head_tail(rows, opts)
    _output_rows(rows, opts)


# --- Feather / Arrow IPC ---

def _handle_feather(path: str, opts: dict):
    import pyarrow
    import pyarrow.feather
    import pyarrow.ipc

    f = _open_file(path, "rb", s3_endpoint=opts.get("s3_endpoint"))
    clean = path.split("?")[0].split("#")[0].lower()
    if clean.endswith(".arrow"):
        table = pyarrow.ipc.open_stream(f).read_all()
    else:
        table = pyarrow.feather.read_table(f)
    f.close()

    if opts.get("schema"):
        console.print(table.schema)
        return
    if opts.get("count"):
        print(table.num_rows)
        return

    col_filter = opts.get("columns")
    if col_filter:
        table = table.select([c for c in col_filter if c in table.column_names])

    rows_dict = table.to_pydict()
    if not rows_dict:
        return
    keys = list(rows_dict.keys())
    n = len(rows_dict[keys[0]])
    rows = [{k: rows_dict[k][i] for k in keys} for i in range(n)]

    rows = _apply_head_tail(rows, opts)
    _output_rows(rows, opts)


# --- JSON ---

def _handle_json(path: str, opts: dict):
    f = _open_file(path, "r", s3_endpoint=opts.get("s3_endpoint"))
    data = json.load(f)
    f.close()

    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        err_console.print("[bold red]Error:[/bold red] JSON file must contain an array or object")
        raise SystemExit(1)

    if opts.get("schema"):
        if data:
            console.print(list(data[0].keys()))
        return
    if opts.get("count"):
        print(len(data))
        return

    col_filter = opts.get("columns")
    if col_filter:
        data = [{k: v for k, v in row.items() if k in col_filter} for row in data]

    data = _apply_head_tail(data, opts)
    _output_rows(data, opts)


# --- Dispatcher ---

_HANDLERS = {
    "parquet": _handle_parquet,
    "orc": _handle_orc,
    "avro": _handle_avro,
    "jsonl": _handle_jsonl,
    "csv": lambda p, o: _handle_csv(p, o, ","),
    "tsv": lambda p, o: _handle_csv(p, o, "\t"),
    "excel": _handle_excel,
    "feather": _handle_feather,
    "arrow": _handle_feather,
    "json": _handle_json,
}


def handle_structured(path: str, fmt: str, opts: dict, file_obj=None):
    """Dispatch to the appropriate structured handler."""
    handler = _HANDLERS.get(fmt)
    if not handler:
        raise ValueError(f"Unknown format: {fmt}")
    if file_obj is not None:
        # For streaming formats with pre-opened (decompressed) file objects
        _handle_with_file_obj(path, fmt, opts, file_obj)
    else:
        handler(path, opts)


def _handle_with_file_obj(path: str, fmt: str, opts: dict, file_obj):
    """Handle structured format using a pre-opened file object (e.g., decompressed stream)."""
    import io

    if fmt in ("csv", "tsv"):
        import csv as csv_mod
        delimiter = "\t" if fmt == "tsv" else ","
        text_f = io.TextIOWrapper(file_obj, encoding="utf-8")
        reader = csv_mod.DictReader(text_f, delimiter=delimiter)
        col_filter = opts.get("columns")

        if opts.get("schema"):
            console.print(reader.fieldnames)
            return
        if opts.get("count"):
            count = sum(1 for _ in reader)
            print(count)
            return

        limit = opts.get("head")
        rows: list[dict] = []
        if _should_show_progress():
            with _make_spinner_progress() as progress:
                task = progress.add_task("Reading rows...", total=None)
                for record in reader:
                    if col_filter:
                        record = {k: v for k, v in record.items() if k in col_filter}
                    rows.append(record)
                    progress.update(task, description=f"Reading... {len(rows)} rows")
                    if limit and len(rows) >= limit:
                        break
        else:
            for record in reader:
                if col_filter:
                    record = {k: v for k, v in record.items() if k in col_filter}
                rows.append(record)
                if limit and len(rows) >= limit:
                    break
        rows = _apply_head_tail(rows, opts)
        _output_rows(rows, opts)

    elif fmt == "jsonl":
        text_f = io.TextIOWrapper(file_obj, encoding="utf-8")
        col_filter = opts.get("columns")

        if opts.get("count"):
            count = sum(1 for line in text_f if line.strip())
            print(count)
            return

        limit = opts.get("head")
        rows = []
        if _should_show_progress():
            with _make_spinner_progress() as progress:
                task = progress.add_task("Reading rows...", total=None)
                for line in text_f:
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
                    progress.update(task, description=f"Reading... {len(rows)} rows")
                    if limit and len(rows) >= limit:
                        break
        else:
            for line in text_f:
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
        rows = _apply_head_tail(rows, opts)
        _output_rows(rows, opts)

    elif fmt == "avro":
        import fastavro
        reader = fastavro.reader(file_obj)

        if opts.get("schema"):
            console.print_json(json.dumps(reader.writer_schema, default=str))
            return
        if opts.get("count"):
            print(sum(1 for _ in reader))
            return

        col_filter = opts.get("columns")
        limit = opts.get("head")
        rows = []
        if _should_show_progress():
            with _make_spinner_progress() as progress:
                task = progress.add_task("Reading rows...", total=None)
                for record in reader:
                    if col_filter:
                        record = {k: v for k, v in record.items() if k in col_filter}
                    rows.append(record)
                    progress.update(task, description=f"Reading... {len(rows)} rows")
                    if limit and len(rows) >= limit:
                        break
        else:
            for record in reader:
                if col_filter:
                    record = {k: v for k, v in record.items() if k in col_filter}
                rows.append(record)
                if limit and len(rows) >= limit:
                    break
        rows = _apply_head_tail(rows, opts)
        _output_rows(rows, opts)

    elif fmt == "orc":
        import pyarrow.orc as orc
        reader = orc.ORCFile(file_obj)

        if opts.get("schema"):
            console.print(reader.schema)
            return
        if opts.get("count"):
            print(reader.nrows)
            return

        col_filter = opts.get("columns")
        table = reader.read(columns=col_filter)
        rows_dict = table.to_pydict()
        if not rows_dict:
            return
        keys = list(rows_dict.keys())
        n = len(rows_dict[keys[0]])
        rows = [{k: rows_dict[k][i] for k in keys} for i in range(n)]
        rows = _apply_head_tail(rows, opts)
        _output_rows(rows, opts)
    else:
        raise ValueError(f"Unsupported format for file_obj streaming: {fmt}")
