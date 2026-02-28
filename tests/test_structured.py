import subprocess
import sys
import json


def test_csv_table(sample_csv):
    result = subprocess.run([sys.executable, "-m", "mcat", sample_csv], capture_output=True, text=True)
    assert result.returncode == 0
    assert "Alice" in result.stdout


def test_csv_jsonl(sample_csv):
    result = subprocess.run([sys.executable, "-m", "mcat", sample_csv, "--format", "jsonl"], capture_output=True, text=True)
    assert result.returncode == 0
    lines = [l for l in result.stdout.strip().split("\n") if l]
    assert len(lines) == 5
    first = json.loads(lines[0])
    assert first["name"] == "Alice"


def test_csv_count(sample_csv):
    result = subprocess.run([sys.executable, "-m", "mcat", "-c", sample_csv], capture_output=True, text=True)
    assert result.stdout.strip() == "5"


def test_csv_head(sample_csv):
    result = subprocess.run([sys.executable, "-m", "mcat", sample_csv, "--head", "1", "--format", "jsonl"], capture_output=True, text=True)
    lines = [l for l in result.stdout.strip().split("\n") if l]
    assert len(lines) == 1


def test_csv_columns(sample_csv):
    result = subprocess.run([sys.executable, "-m", "mcat", sample_csv, "--columns", "name", "--format", "jsonl"], capture_output=True, text=True)
    first = json.loads(result.stdout.strip().split("\n")[0])
    assert "name" in first
    assert "age" not in first


def test_csv_schema(sample_csv):
    result = subprocess.run([sys.executable, "-m", "mcat", sample_csv, "--schema"], capture_output=True, text=True)
    assert result.returncode == 0


def test_jsonl_read(sample_jsonl):
    result = subprocess.run([sys.executable, "-m", "mcat", sample_jsonl], capture_output=True, text=True)
    assert result.returncode == 0
    assert "Alice" in result.stdout


def test_jsonl_count(sample_jsonl):
    result = subprocess.run([sys.executable, "-m", "mcat", "-c", sample_jsonl], capture_output=True, text=True)
    assert result.stdout.strip() == "5"


def test_json_read(sample_json):
    result = subprocess.run([sys.executable, "-m", "mcat", sample_json], capture_output=True, text=True)
    assert result.returncode == 0
    assert "Alice" in result.stdout


def test_parquet_read(sample_parquet):
    result = subprocess.run([sys.executable, "-m", "mcat", sample_parquet], capture_output=True, text=True)
    assert result.returncode == 0
    assert "Alice" in result.stdout


def test_parquet_count(sample_parquet):
    result = subprocess.run([sys.executable, "-m", "mcat", "-c", sample_parquet], capture_output=True, text=True)
    assert result.stdout.strip() == "5"


def test_parquet_schema(sample_parquet):
    result = subprocess.run([sys.executable, "-m", "mcat", "--schema", sample_parquet], capture_output=True, text=True)
    assert result.returncode == 0


def test_parquet_jsonl(sample_parquet):
    result = subprocess.run([sys.executable, "-m", "mcat", sample_parquet, "--format", "jsonl"], capture_output=True, text=True)
    lines = [l for l in result.stdout.strip().split("\n") if l]
    assert len(lines) == 5


def test_tsv_read(sample_tsv):
    result = subprocess.run([sys.executable, "-m", "mcat", sample_tsv], capture_output=True, text=True)
    assert result.returncode == 0
    assert "Alice" in result.stdout


def test_detect_flag(sample_csv):
    result = subprocess.run([sys.executable, "-m", "mcat", "--detect", sample_csv], capture_output=True, text=True)
    assert "csv" in result.stdout


def test_output_flag(sample_csv, tmp_dir):
    import os
    out = os.path.join(tmp_dir, "out.jsonl")
    result = subprocess.run([sys.executable, "-m", "mcat", sample_csv, "--format", "jsonl", "-o", out], capture_output=True, text=True)
    assert result.returncode == 0
    assert os.path.exists(out)
    with open(out) as f:
        lines = f.readlines()
    assert len(lines) == 5
