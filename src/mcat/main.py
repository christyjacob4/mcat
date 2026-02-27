"""mcat CLI entry point."""

from __future__ import annotations

import sys
from contextlib import nullcontext
from typing import Optional

import typer

from mcat.cat_core import cat_files
from mcat.detect import detect_format, detect_format_verbose
from mcat.pager import pager_context
from mcat.structured import handle_structured

app_typer = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


@app_typer.command(
    context_settings={"allow_extra_args": False, "allow_interspersed_args": True},
    no_args_is_help=False,
)
def main(
    files: list[str] = typer.Argument(None, help="Files to concatenate"),
    # cat-compatible flags
    number: bool = typer.Option(False, "-n", "--number", help="Number all output lines"),
    number_nonblank: bool = typer.Option(False, "-b", "--number-nonblank", help="Number non-blank lines"),
    squeeze_blank: bool = typer.Option(False, "-s", "--squeeze-blank", help="Squeeze multiple blank lines"),
    show_all: bool = typer.Option(False, "-A", "--show-all", help="Equivalent to -vET"),
    show_ends: bool = typer.Option(False, "-E", "--show-ends", help="Display $ at end of each line"),
    show_tabs: bool = typer.Option(False, "-T", "--show-tabs", help="Display TAB as ^I"),
    show_nonprinting: bool = typer.Option(False, "-v", "--show-nonprinting", help="Use ^ and M- notation"),
    e_flag: bool = typer.Option(False, "-e", help="Equivalent to -vE"),
    t_flag: bool = typer.Option(False, "-t", help="Equivalent to -vT"),
    # structured format flags
    format: Optional[str] = typer.Option(None, "--format", help="Output format: table|jsonl|csv|raw"),
    head: Optional[int] = typer.Option(None, "--head", help="Show first N rows"),
    tail: Optional[int] = typer.Option(None, "--tail", help="Show last N rows"),
    schema: bool = typer.Option(False, "--schema", help="Print schema only"),
    columns: Optional[str] = typer.Option(None, "--columns", help="Comma-separated column names"),
    sample: Optional[int] = typer.Option(None, "--sample", help="Random sample of N rows"),
    count: bool = typer.Option(False, "-c", "--count", help="Print row count only"),
    stats: bool = typer.Option(False, "--stats", help="Print column statistics summary"),
    detect: bool = typer.Option(False, "--detect", help="Print detected format and exit"),
    output: Optional[str] = typer.Option(None, "-o", "--output", help="Write output to file instead of stdout"),
    s3_endpoint: Optional[str] = typer.Option(None, "--s3-endpoint", help="Custom S3 endpoint URL (MinIO, R2, B2, Spaces)", envvar="AWS_ENDPOINT_URL"),
    pager: bool = typer.Option(False, "--pager", help="Pipe output through pager (less/more)"),
    version: bool = typer.Option(False, "--version", "-V", help="Show version"),
) -> None:
    """cat on steroids — read files with support for Parquet, Avro, ORC, CSV, JSONL, Excel, Feather, Arrow IPC, JSON, and remote sources."""
    if version:
        from mcat import __version__
        typer.echo(f"mcat {__version__}")
        raise SystemExit(0)

    # --detect: just print what format mcat thinks each file is
    if detect:
        if not files:
            typer.echo("mcat: --detect requires at least one file", err=True)
            raise SystemExit(1)
        from mcat.compression import detect_compression
        for f in files:
            fmt, method = detect_format_verbose(f)
            fmt_str = fmt or "text"
            comp = detect_compression(f)
            if comp and comp != "tar":
                typer.echo(f"file:        {f}")
                typer.echo(f"compression: {comp}")
                typer.echo(f"format:      {fmt_str}")
                typer.echo(f"method:      {method}")
            else:
                typer.echo(f"{f}: {fmt_str} (via {method})")
        raise SystemExit(0)

    # --stats: print column statistics
    if stats:
        if not files:
            typer.echo("mcat: --stats requires at least one file", err=True)
            raise SystemExit(1)
        from mcat.stats import handle_stats
        from mcat.compression import detect_compression, decompress_open
        cols = columns.split(",") if columns else None
        exit_code = 0
        for f in files:
            fmt_detected = detect_format(f)
            if not fmt_detected or fmt_detected == "text":
                typer.echo(f"mcat: {f}: --stats requires a structured format", err=True)
                exit_code = 1
                continue
            try:
                comp = detect_compression(f)
                if comp and comp != "tar":
                    from mcat.structured import _open_file
                    raw_f = _open_file(f, "rb", s3_endpoint=s3_endpoint)
                    decompressed = decompress_open(raw_f, comp)
                    handle_stats(f, fmt_detected, columns=cols, s3_endpoint=s3_endpoint, file_obj=decompressed)
                    try:
                        decompressed.close()
                    except Exception:
                        pass
                    try:
                        raw_f.close()
                    except Exception:
                        pass
                else:
                    handle_stats(f, fmt_detected, columns=cols, s3_endpoint=s3_endpoint)
            except Exception as exc:
                _print_error(f, exc)
                exit_code = 1
        raise SystemExit(exit_code)

    # Resolve combined flags
    if show_all:
        show_nonprinting = show_ends = show_tabs = True
    if e_flag:
        show_nonprinting = show_ends = True
    if t_flag:
        show_nonprinting = show_tabs = True

    cat_opts = {
        "number": number,
        "number_nonblank": number_nonblank,
        "squeeze_blank": squeeze_blank,
        "show_ends": show_ends,
        "show_tabs": show_tabs,
        "show_nonprinting": show_nonprinting,
    }

    struct_opts = {
        "format": format,
        "head": head,
        "tail": tail,
        "schema": schema,
        "columns": columns.split(",") if columns else None,
        "sample": sample,
        "s3_endpoint": s3_endpoint,
        "count": count,
        "output": output,
    }

    # Handle --output: redirect stdout to file
    original_stdout = None
    output_file = None
    if output:
        try:
            output_file = open(output, "w")
            original_stdout = sys.stdout
            sys.stdout = output_file
        except OSError as exc:
            typer.echo(f"mcat: {output}: {exc}", err=True)
            raise SystemExit(1)

    # Use pager when --pager is set and not writing to file
    use_pager = pager and not output
    pager_ctx = pager_context() if use_pager else nullcontext()

    try:
        with pager_ctx:
            if not files:
                # stdin passthrough
                cat_files(["-"], cat_opts, s3_endpoint=s3_endpoint)
                return

            from mcat.compression import detect_compression, strip_compression_ext, decompress_open

            exit_code = 0
            for f in files:
                comp = detect_compression(f)

                # Handle tar archives
                if comp == "tar":
                    typer.echo(f"mcat: {f}: Compressed archive detected. Use tar -tzf to list contents.", err=True)
                    exit_code = 1
                    continue

                fmt = detect_format(f)

                if comp and comp != "tar" and fmt and fmt != "text":
                    # Compressed structured file — decompress and handle
                    try:
                        from mcat.structured import _open_file
                        raw_f = _open_file(f, "rb", s3_endpoint=s3_endpoint)
                        decompressed = decompress_open(raw_f, comp)

                        if fmt == "parquet":
                            # Parquet needs random access — read fully into memory
                            import io
                            import pyarrow.parquet as pq
                            data = decompressed.read()
                            buf = io.BytesIO(data)
                            pf = pq.ParquetFile(buf)

                            if struct_opts.get("schema"):
                                from rich.console import Console
                                Console().print(pf.schema_arrow)
                            elif struct_opts.get("count"):
                                print(pf.metadata.num_rows)
                            elif stats:
                                from mcat.stats import handle_stats
                                cols = columns.split(",") if columns else None
                                handle_stats(f, fmt, columns=cols, s3_endpoint=s3_endpoint)
                            else:
                                # Use normal parquet handler on the buffer
                                from mcat.structured import _handle_parquet
                                # Save and restore — hack: write to temp
                                import tempfile, os
                                tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
                                tmp.write(data)
                                tmp.close()
                                try:
                                    _handle_parquet(tmp.name, struct_opts)
                                finally:
                                    os.unlink(tmp.name)
                        else:
                            # For streaming formats, pass decompressed file object
                            handle_structured(f, fmt, struct_opts, file_obj=decompressed)

                        try:
                            decompressed.close()
                        except Exception:
                            pass
                        try:
                            raw_f.close()
                        except Exception:
                            pass
                    except Exception as exc:
                        _print_error(f, exc)
                        exit_code = 1
                elif comp and (not fmt or fmt == "text"):
                    # Compressed text file — decompress and cat
                    try:
                        from mcat.structured import _open_file
                        raw_f = _open_file(f, "rb", s3_endpoint=s3_endpoint)
                        decompressed = decompress_open(raw_f, comp)
                        # Write decompressed content to stdout
                        stdout_buf = sys.stdout.buffer
                        while True:
                            chunk = decompressed.read(65536)
                            if not chunk:
                                break
                            stdout_buf.write(chunk)
                        stdout_buf.flush()
                        try:
                            decompressed.close()
                        except Exception:
                            pass
                        try:
                            raw_f.close()
                        except Exception:
                            pass
                    except Exception as exc:
                        _print_error(f, exc)
                        exit_code = 1
                elif fmt and (fmt != "text"):
                    try:
                        handle_structured(f, fmt, struct_opts)
                    except Exception as exc:
                        _print_error(f, exc)
                        exit_code = 1
                else:
                    rc = cat_files([f], cat_opts, s3_endpoint=s3_endpoint)
                    if rc != 0:
                        exit_code = 1

            raise SystemExit(exit_code)
    finally:
        if output_file:
            sys.stdout = original_stdout
            output_file.close()


def _print_error(path: str, exc: Exception) -> None:
    """Print a structured error message using Rich panels."""
    try:
        from rich.console import Console
        from rich.panel import Panel
        err_console = Console(stderr=True)
        err_console.print(Panel(
            f"[bold red]{type(exc).__name__}[/bold red]: {exc}",
            title=f"[bold]mcat: {path}[/bold]",
            border_style="red",
        ))
    except Exception:
        print(f"mcat: {path}: {exc}", file=sys.stderr)


def app():
    """Entry point for pyproject.toml."""
    app_typer()
