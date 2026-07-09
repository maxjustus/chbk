# CLAUDE.md

Guidance for agents working in this repository.

## Project Overview

`chbk` is a Rust CLI for backing up active ClickHouse MergeTree parts to S3 or
S3-compatible storage. It uses `system.parts.hash_of_all_files` as the
content-addressed key for each immutable part, stores each part as a streamed
stored ZIP object, and writes per-snapshot JSON.zst manifests under
`snapshots/`.

Backups are lock-free. Garbage collection serializes through `gc/.lock`.

## Common Commands

```bash
cargo build --release
cargo run -- --help
cargo test
cargo fmt --all
cargo clippy --all-targets -- -D warnings
make build-x86-linux
```

The full integration tests require Docker, the AWS CLI, and ClickHouse or
`clickhouse-local`.

## Important Details

- `BACKUP_DIR` is a local staging/work directory, not the backup destination.
  It must be on the same filesystem as `CH_DATA_PATH` because staging uses
  hardlinks.
- S3 configuration is required: `S3_BUCKET`, `S3_REGION`,
  `S3_ACCESS_KEY_ID`, and `S3_SECRET_ACCESS_KEY`.
- `S3_ENDPOINT` is used for MinIO and other S3-compatible storage.
- `CH_SHARD` and `CH_REPLICA` can be passed explicitly; otherwise HTTP mode
  tries to read them from `system.macros`.
- `--only` and `--ignore` are ClickHouse regex patterns applied to
  `database.table`.

Keep public docs and examples free of real credentials, production bucket
names, and local absolute paths.
