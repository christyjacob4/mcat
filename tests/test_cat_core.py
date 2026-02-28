import subprocess
import sys


def test_cat_stdin(sample_text):
    """Test basic file reading."""
    result = subprocess.run([sys.executable, "-m", "mcat", sample_text], capture_output=True, text=True)
    assert "hello" in result.stdout
    assert "world" in result.stdout


def test_cat_number(sample_text):
    result = subprocess.run([sys.executable, "-m", "mcat", "-n", sample_text], capture_output=True, text=True)
    assert "1" in result.stdout


def test_cat_squeeze(sample_text):
    result = subprocess.run([sys.executable, "-m", "mcat", "-s", sample_text], capture_output=True, text=True)
    # Should squeeze multiple blank lines
    assert "\n\n\n" not in result.stdout
