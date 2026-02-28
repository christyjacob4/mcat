import json
import subprocess
import sys
import os

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

def _run_mcat(*args):
    return subprocess.run([sys.executable, "-m", "mcat", *args], capture_output=True, text=True)

def test_sort_ascending():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.csv"), "--sort", "name", "--format", "jsonl")
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.strip().split("\n") if l]
    names = [r["name"] for r in lines]
    assert names == sorted(names)

def test_sort_descending():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.csv"), "--sort", "-name", "--format", "jsonl")
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.strip().split("\n") if l]
    names = [r["name"] for r in lines]
    assert names == sorted(names, reverse=True)

def test_sort_with_head():
    """--sort age --head 2 should return the 2 rows with lowest age from ALL rows."""
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.csv"), "--sort", "age", "--head", "2", "--format", "jsonl")
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.strip().split("\n") if l]
    assert len(lines) == 2
    # Bob (25) and Dave (28) should be the two youngest
    names = {r["name"] for r in lines}
    assert "Bob" in names  # age 25, youngest

def test_sort_numeric():
    """Sort by a numeric-like column."""
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.jsonl"), "--sort", "age", "--format", "jsonl")
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.strip().split("\n") if l]
    ages = [r["age"] for r in lines]
    assert ages == sorted(ages)
