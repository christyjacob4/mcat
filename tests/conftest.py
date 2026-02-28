import os
import pytest

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

@pytest.fixture
def fixture_dir():
    return FIXTURE_DIR

@pytest.fixture
def sample_csv():
    return os.path.join(FIXTURE_DIR, "sample.csv")

@pytest.fixture
def sample_tsv():
    return os.path.join(FIXTURE_DIR, "sample.tsv")

@pytest.fixture
def sample_json():
    return os.path.join(FIXTURE_DIR, "sample.json")

@pytest.fixture
def sample_jsonl():
    return os.path.join(FIXTURE_DIR, "sample.jsonl")

@pytest.fixture
def sample_parquet():
    return os.path.join(FIXTURE_DIR, "sample.parquet")

@pytest.fixture
def sample_avro():
    return os.path.join(FIXTURE_DIR, "sample.avro")

@pytest.fixture
def sample_xlsx():
    return os.path.join(FIXTURE_DIR, "sample.xlsx")

# Keep any existing fixtures like sample_text and tmp_dir
@pytest.fixture
def sample_text():
    return os.path.join(FIXTURE_DIR, "sample.txt")

@pytest.fixture
def tmp_dir():
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        yield d
