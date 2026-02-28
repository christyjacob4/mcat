"""Test all supported formats with the same 5-row dataset."""
import json
import subprocess
import sys
import pytest

EXPECTED_NAMES = ["Alice", "Bob", "Carol", "Dave", "Eve"]
EXPECTED_COUNT = "5"

# Parametrize across all formats
FORMAT_FILES = [
    "sample.csv",
    "sample.tsv",
    "sample.json",
    "sample.jsonl",
    "sample.parquet",
    "sample.avro",
    "sample.xlsx",
]

def _run_mcat(*args):
    result = subprocess.run(
        [sys.executable, "-m", "mcat", *args],
        capture_output=True, text=True
    )
    return result

class TestReadAll:
    """Test basic reading of each format."""

    @pytest.mark.parametrize("filename", FORMAT_FILES)
    def test_read_table(self, fixture_dir, filename):
        path = f"{fixture_dir}/{filename}"
        result = _run_mcat(path)
        assert result.returncode == 0
        for name in EXPECTED_NAMES:
            assert name in result.stdout

    @pytest.mark.parametrize("filename", FORMAT_FILES)
    def test_count(self, fixture_dir, filename):
        path = f"{fixture_dir}/{filename}"
        result = _run_mcat("-c", path)
        assert result.returncode == 0
        assert result.stdout.strip() == EXPECTED_COUNT

    @pytest.mark.parametrize("filename", FORMAT_FILES)
    def test_head(self, fixture_dir, filename):
        path = f"{fixture_dir}/{filename}"
        result = _run_mcat(path, "--head", "2", "--format", "jsonl")
        assert result.returncode == 0
        lines = [l for l in result.stdout.strip().split("\n") if l]
        assert len(lines) == 2

    @pytest.mark.parametrize("filename", FORMAT_FILES)
    def test_tail(self, fixture_dir, filename):
        path = f"{fixture_dir}/{filename}"
        result = _run_mcat(path, "--tail", "2", "--format", "jsonl")
        assert result.returncode == 0
        lines = [l for l in result.stdout.strip().split("\n") if l]
        assert len(lines) == 2

    @pytest.mark.parametrize("filename", FORMAT_FILES)
    def test_columns(self, fixture_dir, filename):
        path = f"{fixture_dir}/{filename}"
        result = _run_mcat(path, "--columns", "name,age", "--format", "jsonl")
        assert result.returncode == 0
        lines = [l for l in result.stdout.strip().split("\n") if l]
        first = json.loads(lines[0])
        assert "name" in first
        # city and score should NOT be in output
        assert "city" not in first
        assert "score" not in first

    @pytest.mark.parametrize("filename", FORMAT_FILES)
    def test_schema(self, fixture_dir, filename):
        path = f"{fixture_dir}/{filename}"
        result = _run_mcat(path, "--schema")
        assert result.returncode == 0
        assert len(result.stdout.strip()) > 0

    @pytest.mark.parametrize("filename", FORMAT_FILES)
    def test_format_jsonl(self, fixture_dir, filename):
        path = f"{fixture_dir}/{filename}"
        result = _run_mcat(path, "--format", "jsonl")
        assert result.returncode == 0
        lines = [l for l in result.stdout.strip().split("\n") if l]
        assert len(lines) == 5
        # Each line should be valid JSON
        for line in lines:
            obj = json.loads(line)
            assert "name" in obj

    @pytest.mark.parametrize("filename", FORMAT_FILES)
    def test_format_csv(self, fixture_dir, filename):
        path = f"{fixture_dir}/{filename}"
        result = _run_mcat(path, "--format", "csv")
        assert result.returncode == 0
        lines = [l for l in result.stdout.strip().split("\n") if l]
        assert len(lines) == 6  # header + 5 rows

    @pytest.mark.parametrize("filename", FORMAT_FILES)
    def test_detect(self, fixture_dir, filename):
        path = f"{fixture_dir}/{filename}"
        result = _run_mcat("--detect", path)
        assert result.returncode == 0
        # Should detect the format correctly
        expected_fmts = {
            "sample.csv": "csv",
            "sample.tsv": "tsv",
            "sample.json": "json",
            "sample.jsonl": "jsonl",
            "sample.parquet": "parquet",
            "sample.avro": "avro",
            "sample.xlsx": "excel",
        }
        assert expected_fmts[filename] in result.stdout

class TestStats:
    """Test --stats on each format."""

    @pytest.mark.parametrize("filename", FORMAT_FILES)
    def test_stats(self, fixture_dir, filename):
        path = f"{fixture_dir}/{filename}"
        result = _run_mcat("--stats", path)
        assert result.returncode == 0
        # Should mention column names
        assert "name" in result.stdout

class TestOutput:
    """Test --output flag."""

    def test_output_to_file(self, sample_csv, tmp_dir):
        import os
        outpath = os.path.join(tmp_dir, "out.jsonl")
        result = _run_mcat(sample_csv, "--format", "jsonl", "-o", outpath)
        assert result.returncode == 0
        with open(outpath) as f:
            lines = f.readlines()
        assert len(lines) == 5

class TestHeadTailCombined:
    """Test head and tail edge cases."""

    def test_head_larger_than_rows(self, sample_csv):
        result = _run_mcat(sample_csv, "--head", "100", "--format", "jsonl")
        lines = [l for l in result.stdout.strip().split("\n") if l]
        assert len(lines) == 5

    def test_head_one(self, sample_parquet):
        result = _run_mcat(sample_parquet, "--head", "1", "--format", "jsonl")
        lines = [l for l in result.stdout.strip().split("\n") if l]
        assert len(lines) == 1
        obj = json.loads(lines[0])
        assert obj["name"] == "Alice"
