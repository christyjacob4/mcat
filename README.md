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
```

And remote sources (streaming, no full download):

```bash
mcat s3://bucket/data.parquet
mcat gs://bucket/data.parquet
mcat https://example.com/data.csv

# S3-compatible storage (MinIO, Cloudflare R2, Backblaze B2, DigitalOcean Spaces)
mcat --s3-endpoint https://play.min.io s3://mybucket/data.parquet
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

| Extra         | Adds                    | Notes                                    |
|---------------|-------------------------|------------------------------------------|
| `mcat[s3]`    | `boto3`, `s3fs`         | AWS S3, MinIO, R2, B2, DO Spaces         |
| `mcat[gcs]`   | `gcsfs`                 | Native GCS (`gs://`) — richest features  |
| `mcat[cloud]` | `boto3`, `s3fs`, `gcsfs`| S3 + GCS combined                        |
| `mcat[avro]`  | `fastavro`              | Avro format support                      |
| `mcat[all]`   | Everything above        | All formats + all remotes                |

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
