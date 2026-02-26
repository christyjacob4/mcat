# 🐱 mcat

> **cat on steroids** — a drop-in `cat` replacement that understands Parquet, Avro, ORC, CSV, JSONL, and remote sources.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-green.svg)](https://python.org)
[![GitHub Stars](https://img.shields.io/github/stars/christyjacob4/mcat?style=flat)](https://github.com/christyjacob4/mcat)

---

## Demo

```
$ mcat sales_data.parquet
┌─────────────┬──────────┬────────┬─────────┐
│ name        │ region   │  sales │ quarter │
├─────────────┼──────────┼────────┼─────────┤
│ Alice Chen  │ APAC     │ 94,230 │ Q1 2024 │
│ Bob Müller  │ EMEA     │ 71,450 │ Q1 2024 │
│ Carol Smith │ Americas │ 88,920 │ Q1 2024 │
└─────────────┴──────────┴────────┴─────────┘
3 rows · 4 columns · parquet
```

---

## Why mcat?

`cat` is everywhere, but it can't read Parquet or Avro. The existing tools (`parquet-cli`, `avro-tools`) are heavy Java dependencies that take ages to install. `mcat` is a single `pip install` (or `uv tool install`) that just works — all GNU cat flags, plus structured format support and remote sources out of the box.

---

## Install

```bash
# With uv (recommended)
uv tool install mcat

# Or with pip
pip install mcat

# With extras
pip install "mcat[all]"     # S3 + GCS + Avro
pip install "mcat[s3]"      # S3 + S3-compatible (MinIO, R2, B2, Spaces)
pip install "mcat[cloud]"   # S3 + GCS combined

# With Homebrew
brew tap christyjacob4/tap
brew install mcat
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
mcat data.parquet --count        # Row count (instant for Parquet)
mcat data.parquet --detect       # Print detected format
```

Column statistics (instant for Parquet — reads metadata only):

```bash
mcat --stats data.parquet
mcat --stats --columns age,salary data.parquet   # specific columns only
```

Transparent compression (gzip, zstd, bz2, lz4, xz — all work):

```bash
mcat data.parquet.gz
mcat s3://bucket/logs.jsonl.zst --head 100
mcat data.csv.bz2 --stats
```

And remote sources (streaming, no full download):

```bash
mcat s3://bucket/data.parquet
mcat gs://bucket/data.parquet
mcat https://example.com/data.csv

# S3-compatible storage (MinIO, Cloudflare R2, Backblaze B2, DigitalOcean Spaces)
mcat --s3-endpoint https://play.min.io s3://mybucket/data.parquet
```

Format conversion with `--output`:

```bash
mcat data.parquet --format jsonl --output data.jsonl
mcat data.csv --format jsonl --output data.jsonl
```

## Flag Reference

| Flag | Short | Description |
|------|-------|-------------|
| `--number` | `-n` | Number all output lines |
| `--number-nonblank` | `-b` | Number non-blank lines only |
| `--squeeze-blank` | `-s` | Squeeze multiple blank lines |
| `--show-all` | `-A` | Equivalent to `-vET` |
| `--show-ends` | `-E` | Display `$` at end of each line |
| `--show-tabs` | `-T` | Display TAB as `^I` |
| `--show-nonprinting` | `-v` | Use `^` and `M-` notation |
| | `-e` | Equivalent to `-vE` |
| | `-t` | Equivalent to `-vT` |
| `--format` | | Output format: `table` \| `jsonl` \| `csv` \| `raw` |
| `--head` | | Show first N rows |
| `--tail` | | Show last N rows |
| `--schema` | | Print schema only |
| `--columns` | | Comma-separated column names |
| `--count` | `-c` | Print row count only |
| `--stats` | | Print column statistics summary |
| `--detect` | | Print detected format and exit |
| `--output` | `-o` | Write output to file instead of stdout |
| `--s3-endpoint` | | Custom S3 endpoint URL (MinIO, R2, B2, Spaces) |
| `--version` | `-V` | Show version |

## Format Support

| Format  | Extensions         | Features                          |
|---------|--------------------|-----------------------------------|
| Parquet | `.parquet`, `.pq`  | Stream row groups, schema inspect |
| ORC     | `.orc`             | Stream stripes                    |
| Avro    | `.avro`            | Stream blocks (requires `mcat[avro]`) |
| JSONL   | `.jsonl`, `.ndjson`| Pretty-print each record          |
| CSV     | `.csv`             | Table with headers                |
| TSV     | `.tsv`             | Table with headers                |

Formats are detected by extension first, then by magic bytes (`PAR1`, `ORC`, `Obj\x01`) as a fallback.

## Output Formats

Use `--format` to control output:

- `table` (default) — Rich formatted table
- `jsonl` — One JSON object per line
- `csv` — CSV with headers
- `raw` — Python repr

## Optional Extras

| Extra         | Adds                    | Notes                                    |
|---------------|-------------------------|------------------------------------------|
| `mcat[s3]`    | `boto3`, `s3fs`         | AWS S3, MinIO, R2, B2, DO Spaces         |
| `mcat[gcs]`   | `gcsfs`                 | Native GCS (`gs://`) — richest features  |
| `mcat[cloud]` | `boto3`, `s3fs`, `gcsfs`| S3 + GCS combined                        |
| `mcat[avro]`  | `fastavro`              | Avro format support                      |
| `mcat[compress]` | `zstandard`, `lz4`   | zstd and lz4 decompression               |
| `mcat[all]`   | Everything above        | All formats + all remotes + compression  |

## Authentication

### AWS S3

```bash
# Option 1: AWS CLI (recommended)
aws configure   # stores in ~/.aws/credentials
mcat s3://my-bucket/data.parquet

# Option 2: Environment variables
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=us-east-1
mcat s3://my-bucket/data.parquet

# Option 3: Named profile
AWS_PROFILE=myprofile mcat s3://my-bucket/data.parquet

# Option 4: Temporary session token (MFA, assume-role)
export AWS_SESSION_TOKEN=AQoXnyc4lcK4w4OIAHPOjgWDSB...
mcat s3://my-bucket/data.parquet
```

### S3-Compatible Storage (MinIO, Cloudflare R2, Backblaze B2, DigitalOcean Spaces)

```bash
# Via --s3-endpoint flag
mcat --s3-endpoint https://play.min.io s3://mybucket/data.parquet

# Via environment variable
export FSSPEC_S3_ENDPOINT_URL=https://play.min.io
mcat s3://mybucket/data.parquet

# Cloudflare R2 example
mcat --s3-endpoint https://<account-id>.r2.cloudflarestorage.com s3://bucket/file.parquet

# MinIO with credentials
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  mcat --s3-endpoint http://localhost:9000 s3://mybucket/data.parquet
```

### Google Cloud Storage (native gs://)

```bash
# Option 1: gcloud CLI (recommended)
gcloud auth application-default login
mcat gs://my-bucket/data.parquet

# Option 2: Service account key
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
mcat gs://my-bucket/data.parquet
```

## License

MIT
