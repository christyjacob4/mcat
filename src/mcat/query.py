"""SQL query filtering via DuckDB."""

from __future__ import annotations

import duckdb

from mcat.structured import _output_rows, _apply_head_tail


# Formats that DuckDB can read natively
_DUCKDB_READERS = {
    "parquet": "read_parquet('{path}')",
    "csv": "read_csv_auto('{path}')",
    "tsv": "read_csv_auto('{path}', delim='\\t')",
    "jsonl": "read_json_auto('{path}', format='newline_delimited')",
    "json": "read_json_auto('{path}')",
}


def _result_to_dicts(result: duckdb.DuckDBPyRelation) -> list[dict]:
    """Convert a DuckDB result to a list of dicts without requiring pandas."""
    col_names = result.columns
    rows_raw = result.fetchall()
    return [dict(zip(col_names, row)) for row in rows_raw]


def handle_query(path: str, fmt: str, query: str, opts: dict) -> None:
    """Filter rows using a SQL WHERE clause via DuckDB.

    For formats DuckDB supports natively (Parquet, CSV, TSV, JSONL, JSON),
    the query is pushed down directly. For other formats, we fall back to
    loading via existing handlers and filtering in Python.
    """
    reader_template = _DUCKDB_READERS.get(fmt)

    if reader_template is not None:
        _query_native(path, fmt, query, opts, reader_template)
    else:
        _query_fallback(path, fmt, query, opts)


def _query_native(
    path: str, fmt: str, query: str, opts: dict, reader_template: str
) -> None:
    """Run the query directly in DuckDB using a native reader."""
    # Build the SELECT clause with optional column filtering
    columns = opts.get("columns")
    select_cols = ", ".join(columns) if columns else "*"

    source = reader_template.format(path=path.replace("'", "''"))
    sql = f"SELECT {select_cols} FROM {source} WHERE {query}"

    try:
        s3_endpoint = opts.get("s3_endpoint")
        if s3_endpoint and path.startswith("s3://"):
            # Configure DuckDB for custom S3 endpoint
            con = duckdb.connect()
            # Strip protocol from endpoint
            endpoint = s3_endpoint.replace("https://", "").replace("http://", "")
            use_ssl = "true" if s3_endpoint.startswith("https://") else "false"
            con.execute(f"SET s3_endpoint='{endpoint}'")
            con.execute(f"SET s3_use_ssl={use_ssl}")
            con.execute(f"SET s3_url_style='path'")
            result = con.execute(sql).fetchall()
            columns_out = [desc[0] for desc in con.description]
            rows = [dict(zip(columns_out, row)) for row in result]
            con.close()
        else:
            result = duckdb.sql(sql)
            rows = _result_to_dicts(result)
    except duckdb.Error as exc:
        from rich.console import Console
        Console(stderr=True).print(
            f"[bold red]Query error:[/bold red] {exc}"
        )
        raise SystemExit(1)

    # Apply --head / --tail after the WHERE filter
    rows = _apply_head_tail(rows, opts)
    _output_rows(rows, opts)


def _query_fallback(path: str, fmt: str, query: str, opts: dict) -> None:
    """Fallback: load data with existing handlers, then filter via DuckDB in-memory."""
    from mcat.structured import _HANDLERS

    handler = _HANDLERS.get(fmt)
    if not handler:
        raise ValueError(f"Unknown format: {fmt}")

    # Capture rows by temporarily replacing _output_rows
    import mcat.structured as structured_mod

    captured_rows: list[dict] = []

    original_output = structured_mod._output_rows
    original_apply = structured_mod._apply_head_tail

    def _capture_output(rows: list[dict], opts: dict) -> None:
        captured_rows.extend(rows)

    def _noop_head_tail(rows: list[dict], opts: dict) -> list[dict]:
        return rows

    # Temporarily monkey-patch to capture all rows without head/tail limits
    structured_mod._output_rows = _capture_output
    structured_mod._apply_head_tail = _noop_head_tail

    # Build opts without head/tail/columns/count/schema/query so we get all raw data
    raw_opts = dict(opts)
    raw_opts.pop("head", None)
    raw_opts.pop("tail", None)
    raw_opts.pop("columns", None)
    raw_opts.pop("count", None)
    raw_opts.pop("schema", None)
    raw_opts.pop("query", None)

    try:
        handler(path, raw_opts)
    finally:
        structured_mod._output_rows = original_output
        structured_mod._apply_head_tail = original_apply

    if not captured_rows:
        return

    # Load captured rows into DuckDB via PyArrow and filter
    try:
        import pyarrow as pa

        # Convert list-of-dicts to a PyArrow table for DuckDB ingestion
        keys = list(captured_rows[0].keys())
        arrays = {k: [row.get(k) for row in captured_rows] for k in keys}
        arrow_table = pa.table(arrays)

        con = duckdb.connect()
        con.register("data", arrow_table)

        columns = opts.get("columns")
        select_cols = ", ".join(columns) if columns else "*"
        sql = f"SELECT {select_cols} FROM data WHERE {query}"

        result = con.sql(sql)
        rows = _result_to_dicts(result)
        con.close()
    except duckdb.Error as exc:
        from rich.console import Console
        Console(stderr=True).print(
            f"[bold red]Query error:[/bold red] {exc}"
        )
        raise SystemExit(1)

    rows = _apply_head_tail(rows, opts)
    _output_rows(rows, opts)
