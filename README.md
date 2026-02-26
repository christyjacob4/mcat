# 🐱 mcat

> **cat on steroids** — a drop-in `cat` replacement that understands Parquet, Avro, ORC, CSV, JSONL, and remote sources.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-green.svg)](https://python.org)

---

## Install

```bash
# With uv (recommended)
uv tool install mcat

# Or with pip
pip install mcat

# With extras
pip install "mcat[all]"     # S3 + GCS + Avro
pip install "mcat[s3]"      # Just S3 support
```

## Usage

mcat works exactly like `cat` — all the same flags work:

```bash
mcat file.txt                    # Same as cat
mcat -n file.txt                 # Number lines
mcat -b -s file.txt              # Number non-blank, squeeze blanks
mcat -A file.txt                 # Show all (tabs, ends, non-printing)
echo "hello" | mcat              # Stdin passthrough
```

But it also understands structured data:

```bash
mcat data.parquet                # Pretty table output
mcat data.parquet --format jsonl # As JSON Lines
mcat data.csv                    # CSV as table
mcat data.jsonl --head 10        # First 10 records
mcat data.parquet --schema       # Print schema only
mcat data.parquet --columns name,age  # Select columns
```

And remote sources (streaming, no full download):

```bash
mcat s3://bucket/data.parquet
mcat gs://bucket/data.parquet
mcat https://example.com/data.csv
```

## Format Support

| Format  | Extensions         | Features                          |
|---------|--------------------|-----------------------------------|
| Parquet | `.parquet`, `.pq`  | Stream row groups, schema inspect |
| ORC     | `.orc`             | Stream stripes                    |
| Avro    | `.avro`            | Stream blocks (requires `mcat[avro]`) |
| JSONL   | `.jsonl`, `.ndjson`| Pretty-print each record          |
| CSV     | `.csv`             | Table with headers                |
| TSV     | `.tsv`             | Table with headers                |

## Output Formats

Use `--format` to control output:

- `table` (default) — Rich formatted table
- `jsonl` — One JSON object per line
- `csv` — CSV with headers
- `raw` — Python repr

## Optional Extras

| Extra       | Adds                    |
|-------------|-------------------------|
| `mcat[s3]`  | `boto3`, `s3fs`         |
| `mcat[gcs]` | `gcsfs`                 |
| `mcat[avro]`| `fastavro`              |
| `mcat[all]` | Everything above        |

## License

MIT
