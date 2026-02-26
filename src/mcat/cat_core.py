"""GNU cat-compatible core implementation."""

from __future__ import annotations

import sys


def _show_nonprinting_char(b: int) -> str:
    """Convert a byte to cat -v notation."""
    if b == 9 or b == 10:  # TAB and LF pass through
        return chr(b)
    if b < 32:
        return "^" + chr(b + 64)
    if b == 127:
        return "^?"
    if 128 <= b < 128 + 32:
        return "M-^" + chr(b - 128 + 64)
    if b == 128 + 127:
        return "M-^?"
    if b > 127:
        return "M-" + chr(b - 128)
    return chr(b)


def _process_line(raw: bytes, opts: dict, line_num: int, prev_blank: bool) -> tuple[str | None, int, bool]:
    """Process a single line of bytes. Returns (output_string_or_None, new_line_num, is_blank)."""
    # Determine if line is blank (only newline or empty)
    stripped = raw.rstrip(b"\n")
    is_blank = len(stripped) == 0
    newline = raw.endswith(b"\n")

    # Squeeze blank lines
    if opts["squeeze_blank"] and is_blank and prev_blank:
        return None, line_num, True

    parts: list[str] = []

    # Line numbering
    if opts["number_nonblank"]:
        if not is_blank:
            parts.append(f"{line_num:6d}\t")
            line_num += 1
        else:
            parts.append("      \t")
    elif opts["number"]:
        parts.append(f"{line_num:6d}\t")
        line_num += 1

    # Process content bytes (without trailing newline)
    if opts["show_nonprinting"] or opts["show_tabs"]:
        for byte in stripped:
            if byte == 9:  # TAB
                if opts["show_tabs"]:
                    parts.append("^I")
                else:
                    parts.append("\t")
            else:
                if opts["show_nonprinting"]:
                    parts.append(_show_nonprinting_char(byte))
                else:
                    parts.append(chr(byte) if byte < 128 else _show_nonprinting_char(byte))
    else:
        # Fast path — no char-level processing needed
        try:
            parts.append(stripped.decode("utf-8", errors="surrogateescape"))
        except Exception:
            parts.append(stripped.decode("latin-1"))

    if opts["show_ends"] and newline:
        parts.append("$")

    if newline:
        parts.append("\n")

    return "".join(parts), line_num, is_blank


def cat_files(files: list[str], opts: dict, s3_endpoint: str | None = None) -> int:
    """Concatenate files to stdout, cat-style. Returns exit code."""
    exit_code = 0
    line_num = 1
    prev_blank = False
    stdout_buf = sys.stdout.buffer

    need_processing = any(opts.values())

    for path in files:
        try:
            if path == "-":
                f = sys.stdin.buffer
            else:
                # Use fsspec for remote, regular open for local
                if "://" in path:
                    import fsspec
                    so = {}
                    if s3_endpoint and path.startswith("s3://"):
                        so["client_kwargs"] = {"endpoint_url": s3_endpoint}
                    f = fsspec.open(path, "rb", **so).open()
                else:
                    f = open(path, "rb")

            try:
                if not need_processing:
                    # Fast path: raw copy
                    while True:
                        chunk = f.read(65536)
                        if not chunk:
                            break
                        stdout_buf.write(chunk)
                    stdout_buf.flush()
                else:
                    # Line-by-line processing
                    for raw_line in f:
                        result, line_num, prev_blank = _process_line(
                            raw_line, opts, line_num, prev_blank
                        )
                        if result is not None:
                            stdout_buf.write(result.encode("utf-8", errors="surrogateescape"))
                    stdout_buf.flush()
            finally:
                if path != "-":
                    f.close()

        except FileNotFoundError:
            print(f"mcat: {path}: No such file or directory", file=sys.stderr)
            exit_code = 1
        except IsADirectoryError:
            print(f"mcat: {path}: Is a directory", file=sys.stderr)
            exit_code = 1
        except PermissionError:
            print(f"mcat: {path}: Permission denied", file=sys.stderr)
            exit_code = 1
        except BrokenPipeError:
            # Match cat behavior: silently exit
            sys.stderr.close()
            raise SystemExit(141)
        except Exception as exc:
            print(f"mcat: {path}: {exc}", file=sys.stderr)
            exit_code = 1

    return exit_code
