# Agent Guide

`chbk` is a Rust CLI for backing up active ClickHouse MergeTree parts to S3 or
S3-compatible storage. Keep this file short and operational: it should tell an
agent what matters before editing code.

## Project facts

- Crate/binary name: `chbk` (`target/release/chbk`). Rust edition: 2024.
- Backups are lock-free. GC serializes through the S3 object `gc/.lock`.
- Each immutable part is stored as one content-addressed stored-ZIP object keyed
  by `system.parts.hash_of_all_files`.
- Snapshot manifests are zstd-compressed JSON at `snapshots/{name}.json.zst`.
- This is not ClickHouse native `.backup` XML; see
  `docs/clickhouse-backup-format.md` for the on-S3 layout.
- `BACKUP_DIR` is local staging, not the backup destination. It must be on the
  same filesystem as `CH_DATA_PATH` because staging uses hardlinks.

## Code map

- `src/main.rs` — CLI args, env handling, subcommand dispatch.
- `src/clickhouse.rs`, `src/parts.rs` — ClickHouse queries and part discovery.
- `src/part_zip.rs` — non-seeking stored-ZIP writer/reader for part blobs.
- `src/storage.rs`, `src/upload.rs` — S3 client, multipart upload, retry, delete.
- `src/manifest.rs`, `src/snapshots.rs` — snapshot format and snapshot actions.
- `src/gc.rs`, `src/gc/lock.rs` — blob/snapshot GC and lock handling.
- `src/tui.rs` — terminal progress UI.
- `src/util.rs`, `src/blob_hash.rs`, `src/retry.rs` — small shared helpers.
- `tests/integration.rs` — smoke test; `tests/ch_local_harness.rs` and
  `tests/live_snapshot_e2e.rs` are Docker-backed flow tests.

## Commands

```bash
cargo run -- --help
cargo test --bins
cargo test --test integration
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo build --release
make build-x86-linux
```

Docker-backed tests require Docker, AWS CLI, and ClickHouse or
`clickhouse-local` where noted:

```bash
cargo test --test ch_local_harness -- --test-threads=1
cargo test --test live_snapshot_e2e -- --test-threads=1
```

## Runtime configuration

- Required S3 settings: `S3_BUCKET`, `S3_REGION`, `S3_ACCESS_KEY_ID`,
  `S3_SECRET_ACCESS_KEY`. `S3_PREFIX` is optional.
- `S3_ENDPOINT` is for MinIO/S3-compatible storage. For local HTTP endpoints set
  `AWS_ALLOW_HTTP=true`.
- ClickHouse defaults: `CH_URL=http://localhost:8123`, `CH_USER=default`, empty
  `CH_PASSWORD`, `CH_DATA_PATH=/var/lib/clickhouse`.
- `CH_SHARD` and `CH_REPLICA` can be explicit; HTTP mode can auto-read them from
  `system.macros`.
- `CH_USE_LOCAL=1` uses `clickhouse local`; `CH_CONFIG_PATH` supplies XML config.
- `--only` / `CHBK_ONLY` and `--ignore` / `CHBK_IGNORE` are ClickHouse regexes
  matched against `database.table`. Default ignore is `^system\.`; use `none` to
  disable it.
- Generate the env template with `chbk generate-env`.

## Development rules

- Preserve the core data model: content-addressed immutable part blobs plus
  write-once manifests. Do not add a central metadata database or backup lock.
- Preserve streaming behavior. Do not build whole part archives on disk or in
  memory before upload.
- Do not change blob keys, manifest schema, or snapshot naming without updating
  `docs/clickhouse-backup-format.md` and adding compatibility coverage.
- Keep public docs/examples free of real credentials, production bucket names,
  and local absolute paths. MinIO sample credentials are fine.
- Prefer `anyhow::Result` with context for fallible flows. User-facing error
  handling belongs near command dispatch in `main.rs`.
- CLI or environment-variable changes should update `README.md` and
  `chbk.example.env`.
- The lint profile denies `unwrap`, `expect`, `panic`, and indexing/slicing in
  normal code. Tests have local allowances.
- Add unit tests beside modules; extend Docker tests only for behavior that needs
  ClickHouse/S3.
- Treat `README.md`, `docs/`, `src/`, and `tests/` as source of truth. Root
  proposal/scratch files may be stale.
