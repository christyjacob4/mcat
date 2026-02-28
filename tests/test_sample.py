import json
import subprocess
import sys
import os

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

def _run_mcat(*args):
    return subprocess.run([sys.executable, "-m", "mcat", *args], capture_output=True, text=True)

def test_sample_returns_n_rows():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.csv"), "--sample", "2", "--format", "jsonl")
    assert result.returncode == 0
    lines = [l for l in result.stdout.strip().split("\n") if l]
    assert len(lines) == 2

def test_sample_larger_than_rows():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.csv"), "--sample", "100", "--format", "jsonl")
    lines = [l for l in result.stdout.strip().split("\n") if l]
    assert len(lines) == 5  # all rows returned

def test_sample_zero():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.csv"), "--sample", "0", "--format", "jsonl")
    assert result.returncode == 0
    output = result.stdout.strip()
    assert output == ""  # no rows

def test_sample_with_format_csv():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.parquet"), "--sample", "3", "--format", "csv")
    assert result.returncode == 0
    lines = [l for l in result.stdout.strip().split("\n") if l]
    assert len(lines) == 4  # header + 3 rows
