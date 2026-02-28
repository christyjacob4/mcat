import subprocess
import sys


def test_version():
    result = subprocess.run([sys.executable, "-m", "mcat", "--version"], capture_output=True, text=True)
    assert "mcat" in result.stdout
    assert result.returncode == 0


def test_help():
    result = subprocess.run([sys.executable, "-m", "mcat", "--help"], capture_output=True, text=True)
    assert result.returncode == 0


def test_stdin():
    result = subprocess.run([sys.executable, "-m", "mcat"], input="hello world\n", capture_output=True, text=True)
    assert "hello world" in result.stdout


def test_missing_file():
    result = subprocess.run([sys.executable, "-m", "mcat", "nonexistent.txt"], capture_output=True, text=True)
    assert result.returncode != 0
