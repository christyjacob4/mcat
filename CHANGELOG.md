# Changelog

## [Unreleased]

## [0.1.0] — 2026-02-26
### Added
- Full GNU cat compatibility (all flags)
- Parquet, ORC, Avro, JSONL, CSV/TSV streaming
- Remote sources: S3, GCS, HTTP via fsspec
- S3-compatible endpoint support (--s3-endpoint)
- Rich table output with --format, --head, --tail, --schema, --columns
- --count flag for row counting without full reads
- --output / -o flag for format conversion
- --detect flag for format identification
- Magic byte format detection (PAR1, ORC, Obj\x01)
- Homebrew formula (Formula/mcat.rb)
