import json
import subprocess
import sys
import os

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

def _run_mcat(*args):
    return subprocess.run([sys.executable, "-m", "mcat", *args], capture_output=True, text=True)

def test_grep_simple():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.csv"), "--grep", "NYC", "--format", "jsonl")
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.strip().split("\n") if l]
    assert len(lines) == 2  # Alice and Dave are in NYC
    assert all("NYC" in json.dumps(r) for r in lines)

def test_grep_case_insensitive():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.csv"), "--grep", "nyc", "--format", "jsonl")
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.strip().split("\n") if l]
    assert len(lines) == 2  # case insensitive

def test_grep_regex():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.csv"), "--grep", "^A", "--format", "jsonl")
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.strip().split("\n") if l]
    assert len(lines) >= 1  # At least Alice

def test_grep_no_matches():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.csv"), "--grep", "ZZZZZ", "--format", "jsonl")
    assert result.returncode == 0
    assert result.stdout.strip() == ""

def test_grep_with_head():
    """--grep NYC --head 1 should find all NYC matches, then take first."""
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.csv"), "--grep", "NYC", "--head", "1", "--format", "jsonl")
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.strip().split("\n") if l]
    assert len(lines) == 1

def test_grep_parquet():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.parquet"), "--grep", "LA", "--format", "jsonl")
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.strip().split("\n") if l]
    assert len(lines) == 2  # Bob and Eve
