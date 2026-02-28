from mcat.compression import detect_compression, strip_compression_ext


def test_detect_gz():
    assert detect_compression("file.csv.gz") == "gzip"


def test_detect_zst():
    assert detect_compression("file.parquet.zst") == "zstd"


def test_detect_bz2():
    assert detect_compression("file.csv.bz2") == "bz2"


def test_detect_xz():
    assert detect_compression("file.csv.xz") == "xz"


def test_detect_lz4():
    assert detect_compression("file.csv.lz4") == "lz4"


def test_detect_none():
    assert detect_compression("file.csv") is None


def test_detect_tar():
    assert detect_compression("file.tar.gz") == "tar"


def test_strip_ext():
    assert strip_compression_ext("data.csv.gz") == "data.csv"
    assert strip_compression_ext("data.parquet.zst") == "data.parquet"
    assert strip_compression_ext("data.csv") == "data.csv"


def test_detect_url_compression():
    assert detect_compression("s3://bucket/file.csv.gz?v=1") == "gzip"
