import subprocess
import sys
import os

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

def test_no_progress_when_piped():
    """Progress should not appear in stdout when output is piped."""
    path = os.path.join(FIXTURE_DIR, "sample.csv")
    result = subprocess.run(
        [sys.executable, "-m", "mcat", path],
        capture_output=True, text=True
    )
    assert result.returncode == 0
    # stdout should have table data, NOT progress indicators
    assert "Reading" not in result.stdout
    # Progress goes to stderr, but since we capture (not a TTY), it shouldn't appear there either
    assert "Reading" not in result.stderr

def test_no_progress_in_jsonl_output():
    """Progress should not corrupt JSONL output."""
    path = os.path.join(FIXTURE_DIR, "sample.csv")
    result = subprocess.run(
        [sys.executable, "-m", "mcat", path, "--format", "jsonl"],
        capture_output=True, text=True
    )
    assert result.returncode == 0
    import json
    lines = [l for l in result.stdout.strip().split("\n") if l]
    # Every line should be valid JSON (no progress bar corruption)
    for line in lines:
        json.loads(line)  # should not raise

def test_no_progress_in_count():
    """--count should not show progress."""
    path = os.path.join(FIXTURE_DIR, "sample.parquet")
    result = subprocess.run(
        [sys.executable, "-m", "mcat", "-c", path],
        capture_output=True, text=True
    )
    assert result.returncode == 0
    assert result.stdout.strip() == "5"
    assert "Reading" not in result.stderr
