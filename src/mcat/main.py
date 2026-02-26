"""mcat CLI entry point."""

from __future__ import annotations

import sys
from typing import Optional

import typer

from mcat.cat_core import cat_files
from mcat.detect import detect_format
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
    version: bool = typer.Option(False, "--version", "-V", help="Show version"),
) -> None:
    """cat on steroids — read files with support for Parquet, Avro, ORC, CSV, JSONL, and remote sources."""
    if version:
        from mcat import __version__
        typer.echo(f"mcat {__version__}")
        raise SystemExit(0)

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
    }

    if not files:
        # stdin passthrough
        cat_files(["-"], cat_opts)
        return

    exit_code = 0
    for f in files:
        fmt = detect_format(f)
        if fmt and (fmt != "text"):
            try:
                handle_structured(f, fmt, struct_opts)
            except Exception as exc:
                print(f"mcat: {f}: {exc}", file=sys.stderr)
                exit_code = 1
        else:
            rc = cat_files([f], cat_opts)
            if rc != 0:
                exit_code = 1

    raise SystemExit(exit_code)


def app():
    """Entry point for pyproject.toml."""
    app_typer()
