# chbk

`chbk` streams active ClickHouse MergeTree parts to S3 or S3-compatible
storage. Parts are stored as content-addressed objects and snapshots as
compressed JSON manifests, so unchanged parts are reused across snapshots.
Backup writers are lock-free; only garbage collection takes an S3 lock.

See [the storage format](docs/clickhouse-backup-format.md) for the on-S3 layout
and consistency model.

## Scope

- Backs up active MergeTree parts from `system.parts`.
- Includes projections, secondary indexes, metadata DDL, `user_defined/`, and
  `user_scripts/`.
- Restores a ClickHouse data directory, with optional `ATTACH PART`.
- Uses `system.parts.hash_of_all_files` for part-level deduplication.
- Requires S3 or an S3-compatible service such as MinIO.
- Does not emit ClickHouse native `.backup` XML.

## Installation

```bash
cargo build --release
cp target/release/chbk /usr/local/bin/chbk
```

## Quick start

Set the S3 destination and ClickHouse identity:

```bash
export S3_BUCKET=my-clickhouse-backups
export S3_REGION=us-east-1
export S3_PREFIX=prod/cluster-a
export S3_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID"
export S3_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"

export CH_URL=http://localhost:8123
export CH_DATA_PATH=/var/lib/clickhouse
export CH_SHARD=01
export CH_REPLICA=replica-1

chbk
```

`CH_SHARD` and `CH_REPLICA` may be omitted in HTTP mode when ClickHouse's
`system.macros` provides them. Running `chbk` without a subcommand creates an
auto-named live snapshot.

Common commands:

```bash
chbk create-snapshot before-upgrade
chbk list-snapshots
chbk restore before-upgrade --to /restore/clickhouse
chbk rm-snapshot before-upgrade
chbk gc-all
chbk gc-live --retain-all 24h --retain-daily 30d
```

Destructive commands accept `--dry-run`. `gc-all` preserves blobs younger than
six hours by default; override this with `--grace-period-hours`.

For every flag and default, run:

```bash
chbk --help
chbk <command> --help
```

## Configuration

`chbk` loads `./.env` automatically. Generate the environment template with:

```bash
chbk generate-env > .env
```

Precedence, from highest to lowest, is CLI flag, environment variable, `.env`,
then built-in default.

`BACKUP_DIR` is the local staging directory and defaults to `./backup`. It must
be on the same filesystem as `CH_DATA_PATH` because staging uses hardlinks.

`S3_ENDPOINT` selects an S3-compatible service. For a local HTTP endpoint, also
set `AWS_ALLOW_HTTP=true`.

### Table filtering

`--only` and `--ignore` are ClickHouse regular expressions matched against
`database.table`. The default ignore pattern is `^system\.`.

```bash
chbk --only '^prod\.'
chbk --ignore '\.staging$'
chbk --only '^prod\.' --ignore '\.staging$'
chbk --ignore none
```

`gc-live` durations accept `m`, `h`, `d`, `w`, and 30-day `M` suffixes.

## Restore

```bash
chbk restore snap1 --to /var/lib/clickhouse
chbk restore snap1 --to /var/lib/clickhouse --attach
```

Restore downloads part blobs, recreates the data directory and metadata, and
can attach parts to a running ClickHouse server. See the
[restore layout](docs/clickhouse-backup-format.md#restore-layout) and command
help for table filtering, overwrite, identity guards, and concurrency options.

## Design

### Part-level content addressing

MergeTree parts are immutable. Merges, mutations, projections, and indexes
create new parts, so `system.parts.hash_of_all_files` identifies a part and
lets snapshots reuse unchanged objects.

### Streaming uploads

Each part is hardlinked into a staging tree. Part files are then streamed as
an uncompressed ZIP byte stream straight into an S3 multipart upload—no full
part archive is built in memory or on disk. Compression is skipped because
ClickHouse part files are already compressed. Each staged part is removed after
its upload completes and its ZIP size is verified.

### Consistency and garbage collection

Backups upload immutable blobs and conditionally create manifests without a
backup lock. GC serializes through `gc/.lock`; `gc-all` uses its grace period to
avoid deleting blobs from an in-flight backup. The
[consistency model](docs/clickhouse-backup-format.md#consistency-model) describes
failure handling in detail.

## Guides

- [Local ClickHouse and MinIO trial](docs/local-minio.md)
- [systemd timers](docs/systemd.md)
- [Storage and restore format](docs/clickhouse-backup-format.md)

## Testing

```bash
cargo test --bins
cargo test --test integration

# Requires Docker, AWS CLI, and ClickHouse or clickhouse-local:
cargo test --test ch_local_harness -- --test-threads=1
cargo test --test live_snapshot_e2e -- --test-threads=1
```

## License

MIT
