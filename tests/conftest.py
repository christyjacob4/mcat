import pytest
import os
import csv
import json
import tempfile


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def sample_csv(tmp_dir):
    path = os.path.join(tmp_dir, "test.csv")
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "age", "city"])
        writer.writeheader()
        writer.writerow({"name": "Alice", "age": "30", "city": "NYC"})
        writer.writerow({"name": "Bob", "age": "25", "city": "LA"})
        writer.writerow({"name": "Carol", "age": "35", "city": "NYC"})
    return path


@pytest.fixture
def sample_jsonl(tmp_dir):
    path = os.path.join(tmp_dir, "test.jsonl")
    with open(path, "w") as f:
        for row in [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}, {"name": "Carol", "age": 35}]:
            f.write(json.dumps(row) + "\n")
    return path


@pytest.fixture
def sample_json(tmp_dir):
    path = os.path.join(tmp_dir, "test.json")
    with open(path, "w") as f:
        json.dump([{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}], f)
    return path


@pytest.fixture
def sample_tsv(tmp_dir):
    path = os.path.join(tmp_dir, "test.tsv")
    with open(path, "w") as f:
        f.write("name\tage\tcity\n")
        f.write("Alice\t30\tNYC\n")
        f.write("Bob\t25\tLA\n")
    return path


@pytest.fixture
def sample_parquet(tmp_dir):
    """Create a simple Parquet file using pyarrow."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    path = os.path.join(tmp_dir, "test.parquet")
    table = pa.table({"name": ["Alice", "Bob", "Carol"], "age": [30, 25, 35], "city": ["NYC", "LA", "NYC"]})
    pq.write_table(table, path)
    return path


@pytest.fixture
def sample_text(tmp_dir):
    path = os.path.join(tmp_dir, "test.txt")
    with open(path, "w") as f:
        f.write("hello\nworld\n\n\nfoo\n")
    return path
