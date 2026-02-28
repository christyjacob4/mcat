# 🐱 mcat

> **cat on steroids** — a drop-in `cat` replacement that understands Parquet, Avro, CSV, JSONL, and remote sources.

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

`cat` is everywhere, but it can't read Parquet or Avro. The existing tools (`parquet-cli`, `avro-tools`) are heavy Java dependencies that take ages to install. `mcat` is a single `pip install` (or `uv tool install`) that just works -- all GNU cat flags, plus structured format support and remote sources out of the box.

---

## Install

```bash
# With uv (recommended)
uv tool install mcat

# Or with pip
pip install mcat

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
mcat data.parquet --grep "Smith"  # Rows where any column matches "Smith"
mcat data.csv --grep "^A" --columns name  # Names starting with A
mcat data.parquet --grep "2024" --format jsonl  # Rows mentioning 2024
mcat data.parquet --count        # Row count (instant for Parquet)
mcat data.parquet --sample 10    # Random 10 rows
mcat data.csv --sample 5 --format jsonl  # 5 random rows as JSONL
mcat data.parquet --detect       # Print detected format
mcat data.parquet --sort age           # Sort ascending by age
mcat data.parquet --sort -age          # Sort descending by age
mcat data.csv --sort "region,-sales"   # Multi-column sort
mcat data.csv --sort name --head 10    # Sort + head
```

Comparing two structured files:

```bash
mcat --diff old.csv new.csv
mcat --diff prod.parquet staging.parquet --columns name,age
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

Pager support for large output (respects `$PAGER`, defaults to `less -R`):

```bash
mcat large_data.parquet --pager          # view in pager
mcat data.csv --pager                     # page through CSV table
PAGER="more" mcat data.parquet --pager   # use 'more' instead of 'less'
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
| `--grep` | | Filter rows where any column matches pattern (regex) |
| `--sample` | | Random sample of N rows |
| `--count` | `-c` | Print row count only |
| `--sort` | | Sort by column(s), prefix with `-` for descending |
| `--stats` | | Print column statistics summary |
| `--diff` | | Compare two structured files side by side |
| `--detect` | | Print detected format and exit |
| `--output` | `-o` | Write output to file instead of stdout |
| `--pager` | | Pipe output through pager (`less`/`more`) |
| `--s3-endpoint` | | Custom S3 endpoint URL (MinIO, R2, B2, Spaces) |
| `--version` | `-V` | Show version |

## Format Support

| Format    | Extensions           | Features                          |
|-----------|----------------------|-----------------------------------|
| Parquet   | `.parquet`, `.pq`    | Stream row groups, schema inspect |
| Avro      | `.avro`              | Stream blocks                         |
| JSONL     | `.jsonl`, `.ndjson`  | Pretty-print each record          |
| CSV       | `.csv`               | Table with headers                |
| TSV       | `.tsv`               | Table with headers                |
| Excel     | `.xlsx`, `.xls`      | First sheet                           |
| JSON      | `.json`              | Array of objects or single object |

Formats are detected by extension first, then by magic bytes (`PAR1`, `Obj\x01`) as a fallback.

## Output Formats

Use `--format` to control output:

- `table` (default) — Rich formatted table
- `jsonl` — One JSON object per line
- `csv` — CSV with headers
- `raw` — Python repr

## Authentication

mcat uses **zero-config auth** — it piggybacks on credentials you've already set up for your cloud provider. No mcat-specific credential flags needed.

### AWS S3

```bash
aws configure   # one-time setup → works everywhere
mcat s3://my-bucket/data.parquet
```

All standard AWS auth methods work automatically: `~/.aws/credentials`, env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), named profiles (`AWS_PROFILE`), IAM roles, SSO, etc.

### Google Cloud Storage

```bash
gcloud auth application-default login   # one-time setup
mcat gs://my-bucket/data.parquet
```

Also supports `GOOGLE_APPLICATION_CREDENTIALS` for service account keys.

### Azure Blob Storage

```bash
# Set env vars once
export AZURE_STORAGE_ACCOUNT_NAME=myaccount
export AZURE_STORAGE_ACCOUNT_KEY=...
mcat az://mycontainer/data.parquet
```

Also works with `az login` and `DefaultAzureCredential`.

### S3-Compatible Storage (MinIO, Cloudflare R2, Backblaze B2, DigitalOcean Spaces, Wasabi, Vultr)

```bash
# Option 1: AWS_ENDPOINT_URL env var (recommended — boto3/botocore 1.29+ official)
export AWS_ENDPOINT_URL=https://play.min.io
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
mcat s3://mybucket/data.parquet

# Option 2: Named profile in ~/.aws/config
# [profile minio]
# endpoint_url = https://play.min.io
# aws_access_key_id = minioadmin
# aws_secret_access_key = minioadmin
AWS_PROFILE=minio mcat s3://mybucket/data.parquet

# Option 3: Per-command --s3-endpoint override
mcat --s3-endpoint https://play.min.io s3://mybucket/data.parquet
```

## License

MIT
