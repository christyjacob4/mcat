"""Pager support for large output."""
from __future__ import annotations

import os
import subprocess
import sys
from contextlib import contextmanager


def _get_pager() -> str:
    """Get the pager command. Respects $PAGER env var."""
    return os.environ.get("PAGER", "less -R")


@contextmanager
def pager_context():
    """Context manager that pipes stdout through a pager.

    Usage:
        with pager_context():
            print("lots of output...")
    """
    if not sys.stdout.isatty():
        # Not a terminal, don't page
        yield
        return

    pager_cmd = _get_pager()

    try:
        proc = subprocess.Popen(
            pager_cmd,
            shell=True,
            stdin=subprocess.PIPE,
            encoding="utf-8",
            errors="replace",
        )
        old_stdout = sys.stdout
        sys.stdout = proc.stdin

        try:
            yield
        finally:
            sys.stdout = old_stdout
            if proc.stdin and not proc.stdin.closed:
                try:
                    proc.stdin.close()
                except BrokenPipeError:
                    pass
            proc.wait()
    except (BrokenPipeError, KeyboardInterrupt):
        pass
