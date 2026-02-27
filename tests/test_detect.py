from mcat.detect import detect_format, detect_format_verbose


def test_detect_csv():
    assert detect_format("data.csv") == "csv"


def test_detect_parquet():
    assert detect_format("data.parquet") == "parquet"
    assert detect_format("data.pq") == "parquet"


def test_detect_jsonl():
    assert detect_format("data.jsonl") == "jsonl"
    assert detect_format("data.ndjson") == "jsonl"


def test_detect_tsv():
    assert detect_format("data.tsv") == "tsv"


def test_detect_json():
    assert detect_format("data.json") == "json"


def test_detect_avro():
    assert detect_format("data.avro") == "avro"


def test_detect_excel():
    assert detect_format("data.xlsx") == "excel"
    assert detect_format("data.xls") == "excel"


def test_detect_text():
    assert detect_format("file.txt") is None


def test_detect_unknown():
    assert detect_format("file.xyz") is None


def test_detect_compressed():
    assert detect_format("data.csv.gz") == "csv"
    assert detect_format("data.parquet.zst") == "parquet"


def test_detect_url():
    assert detect_format("s3://bucket/data.parquet") == "parquet"
    assert detect_format("https://example.com/data.csv?token=abc") == "csv"


def test_detect_verbose():
    fmt, method = detect_format_verbose("data.csv")
    assert fmt == "csv"
    assert method == "extension"
