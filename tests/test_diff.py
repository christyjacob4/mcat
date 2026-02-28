import json
import subprocess
import sys
import os
import csv
import tempfile

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

def _run_mcat(*args):
    return subprocess.run([sys.executable, "-m", "mcat", *args], capture_output=True, text=True)

def test_diff_identical_files():
    path = os.path.join(FIXTURE_DIR, "sample.csv")
    result = _run_mcat("--diff", path, path)
    assert result.returncode == 0
    assert "unchanged" in result.stdout.lower() or "0 modified" in result.stdout

def test_diff_different_files():
    """Create a modified CSV and diff against original."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "age", "city", "score"])
        writer.writeheader()
        writer.writerow({"name": "Alice", "age": "31", "city": "NYC", "score": "95.5"})  # age changed
        writer.writerow({"name": "Bob", "age": "25", "city": "LA", "score": "87.3"})
        writer.writerow({"name": "Carol", "age": "35", "city": "Chicago", "score": "91.8"})
        writer.writerow({"name": "Dave", "age": "28", "city": "NYC", "score": "78.2"})
        writer.writerow({"name": "Eve", "age": "32", "city": "LA", "score": "88.9"})
        tmp_path = f.name
    try:
        result = _run_mcat("--diff", os.path.join(FIXTURE_DIR, "sample.csv"), tmp_path)
        assert result.returncode == 0
        assert "modified" in result.stdout.lower() or "~" in result.stdout
    finally:
        os.unlink(tmp_path)

def test_diff_requires_two_files():
    result = _run_mcat("--diff", os.path.join(FIXTURE_DIR, "sample.csv"))
    assert result.returncode != 0

def test_diff_output_flag():
    """--diff with --output should write to file."""
    path = os.path.join(FIXTURE_DIR, "sample.csv")
    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
        out_path = f.name
    try:
        result = _run_mcat("--diff", path, path, "-o", out_path)
        assert result.returncode == 0
        with open(out_path) as f:
            content = f.read()
        assert len(content) > 0
    finally:
        os.unlink(out_path)
