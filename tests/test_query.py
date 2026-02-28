import json
import subprocess
import sys
import os

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

def _run_mcat(*args):
    return subprocess.run([sys.executable, "-m", "mcat", *args], capture_output=True, text=True)

def test_query_csv():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.csv"), "--query", "age > 30", "--format", "jsonl")
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.strip().split("\n") if l]
    assert all(int(r["age"]) > 30 for r in lines)
    assert len(lines) == 2  # Carol (35), Eve (32)

def test_query_parquet():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.parquet"), "--query", "city = 'NYC'", "--format", "jsonl")
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.strip().split("\n") if l]
    assert all(r["city"] == "NYC" for r in lines)
    assert len(lines) == 2  # Alice, Dave

def test_query_jsonl():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.jsonl"), "--query", "score > 90", "--format", "jsonl")
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.strip().split("\n") if l]
    assert all(r["score"] > 90 for r in lines)

def test_query_with_head():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.csv"), "--query", "age > 25", "--head", "2", "--format", "jsonl")
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.strip().split("\n") if l]
    assert len(lines) == 2

def test_query_no_matches():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.csv"), "--query", "age > 100", "--format", "jsonl")
    assert result.returncode == 0
    assert result.stdout.strip() == ""

def test_query_invalid_sql():
    result = _run_mcat(os.path.join(FIXTURE_DIR, "sample.csv"), "--query", "INVALID SYNTAX !!!")
    assert result.returncode != 0
