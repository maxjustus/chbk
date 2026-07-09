# chbk

`chbk` backs up ClickHouse MergeTree parts to S3 or S3-compatible storage.

It stores each immutable ClickHouse part as one content-addressed object, keyed
by `system.parts.hash_of_all_files`, and writes each snapshot as a compressed
JSON manifest at `snapshots/{name}.json.zst`. There is no central metadata
database. Backup writers do not take a lock; garbage collection is the only
operation that serializes through an S3 lock.

The main goal is a fast, low-memory backup path for large ClickHouse nodes.
Instead of building full archives on local disk, `chbk` hardlinks each part into
a local staging tree, writes a stored ZIP for that part, and streams the ZIP
directly into an S3 multipart upload. The ZIP is stored rather than compressed
because ClickHouse part files are already column-compressed and because the hot
path should spend its time reading disk and pushing bytes to S3.

In the environment this was built for, using `snmalloc` plus direct ZIP to S3
streaming was enough to saturate a 50 Gbit/s link without making CPU the
bottleneck. Treat that as a design data point, not a portable benchmark claim.

## Current Scope

- Backs up active MergeTree parts from `system.parts`.
- Deduplicates at the ClickHouse part level by `hash_of_all_files`.
- Includes projection directories, secondary indexes, metadata DDL,
  `user_defined/`, and `user_scripts/`.
- Restores to a ClickHouse data directory shape, with optional `ATTACH PART`.
- Requires S3 or an S3-compatible service such as MinIO.

It does not currently emit ClickHouse native `.backup` XML. See
[`docs/clickhouse-backup-format.md`](docs/clickhouse-backup-format.md) for the
actual `chbk` storage layout.

## Installation

```bash
cargo build --release
cp target/release/chbk /usr/local/bin/chbk
```

## Usage

Set the S3 destination and ClickHouse identity. If `system.macros` contains
`shard` and `replica`, `CH_SHARD` and `CH_REPLICA` can be omitted when using the
HTTP mode.

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

# Create an auto-named live snapshot:
chbk

# Create a named snapshot:
chbk create-snapshot before-upgrade

# List snapshots:
chbk list-snapshots

# Restore a snapshot to a filesystem path:
chbk restore before-upgrade --to /restore/clickhouse

# Delete one snapshot and blobs referenced only by that snapshot:
chbk rm-snapshot before-upgrade

# Garbage collect unreferenced blobs. The default grace period is 6 hours:
chbk gc-all

# Use a custom grace period, mostly useful for tests:
chbk gc-all --grace-period-hours 0

# Prune auto-named live snapshots:
chbk gc-live --retain-all 24h --retain-daily 30d

# Destructive commands support --dry-run:
chbk gc-all --dry-run
```

### Local MinIO Example

The included `docker-compose.yml` starts ClickHouse and MinIO for a quick local
trial:

```bash
docker compose up -d

clickhouse client --port 9008 --password default --multiquery --query "
  CREATE TABLE IF NOT EXISTS db1.events
  (
    id UInt64,
    event_time DateTime,
    body String
  )
  ENGINE = MergeTree
  ORDER BY id;

  INSERT INTO db1.events
  SELECT number, now(), concat('event-', toString(number))
  FROM numbers(100000);
"

CH_URL=http://localhost:8128 \
CH_PASSWORD=default \
CH_DATA_PATH="$PWD/ch_data" \
CH_SHARD=local \
CH_REPLICA=local \
S3_ENDPOINT=http://localhost:9000 \
S3_BUCKET=test-bucket \
S3_REGION=us-east-1 \
S3_ACCESS_KEY_ID=minioadmin \
S3_SECRET_ACCESS_KEY=minioadmin \
AWS_ALLOW_HTTP=true \
chbk create-snapshot local-test

CH_SHARD=local \
CH_REPLICA=local \
S3_ENDPOINT=http://localhost:9000 \
S3_BUCKET=test-bucket \
S3_REGION=us-east-1 \
S3_ACCESS_KEY_ID=minioadmin \
S3_SECRET_ACCESS_KEY=minioadmin \
AWS_ALLOW_HTTP=true \
chbk restore local-test --to /tmp/chbk-restore
```

### Table Filtering

Filter which tables to back up using `--only` and `--ignore` regex patterns:

```bash
# Only backup tables matching pattern
chbk --only '^prod\.'

# Ignore specific tables (default: ^system\.)
chbk --ignore '\.staging$'

# Combine: prod database except staging tables
chbk --only '^prod\.' --ignore '\.staging$'

# Disable default system exclusion
chbk --ignore none
```

Patterns match against `database.table` format and are evaluated by ClickHouse's `match()` function. By default, `^system\.` is ignored.

### Duration Format

Commands like `gc-live` accept durations with these units:

- `m` - minutes (e.g., `30m`)
- `h` - hours (e.g., `24h`)
- `d` - days (e.g., `7d`)
- `w` - weeks (e.g., `2w`)
- `M` - months, 30 days (e.g., `3M`)

## Configuration File

chbk loads `./.env` automatically via dotenvy. Generate a documented template:

```bash
chbk generate-env > .env
```

Precedence (highest wins): CLI flag > environment variable > `.env` file >
built-in default.

## Environment Variables

- `BACKUP_DIR` - Local staging/work directory (default: `./backup`). It must be on the same filesystem as `CH_DATA_PATH` because staging uses hardlinks.
- `CH_URL` - ClickHouse HTTP URL (default: `http://localhost:8123`)
- `CH_USER` / `CH_PASSWORD` - Credentials (default: `default` / empty)
- `CH_DATA_PATH` - ClickHouse data directory (default: `/var/lib/clickhouse`)
- `CH_USE_LOCAL` - Use `clickhouse local` instead of HTTP
- `CH_CONFIG_PATH` - Config XML for `clickhouse local` (defines Disks for restore)
- `CH_SHARD` / `CH_REPLICA` - Shard/replica identity (auto-detected from `system.macros` if not set)
- `PART_CONCURRENCY` - Max concurrent parts processed in parallel (default: 8)
- `MULTIPART_PART_CONCURRENCY` - Max concurrent part uploads within one multipart upload (default: 16)
- `UPLOAD_MIN_CHUNK_SIZE_MB` / `UPLOAD_MAX_CHUNK_SIZE_MB` - Multipart chunk size bounds
- `UPLOAD_TARGET_PARTS` - Target multipart part count (default: 128)
- `DELETE_CONCURRENCY` - Max concurrent deletions during GC (default: 32)

## S3 Configuration

```bash
chbk \
  --s3-bucket my-bucket \
  --s3-region us-east-1 \
  --s3-access-key-id $AWS_ACCESS_KEY_ID \
  --s3-secret-access-key $AWS_SECRET_ACCESS_KEY \
  --s3-prefix backups/prod
```

Or via environment: `S3_BUCKET`, `S3_REGION`, `S3_ACCESS_KEY_ID`,
`S3_SECRET_ACCESS_KEY`, `S3_PREFIX`, and `S3_ENDPOINT`.

`S3_ENDPOINT` is optional for AWS S3 and required for many S3-compatible
providers. For local HTTP endpoints, set `AWS_ALLOW_HTTP=true`.

## Restore

Restore a snapshot directly to a local directory:

```bash
# Restore snapshot to ClickHouse data directory
chbk restore snap1 --to /var/lib/clickhouse

# With custom download concurrency
chbk restore snap1 --to /var/lib/clickhouse --download-concurrency 64

# Restore and ATTACH tables to a running ClickHouse instance
chbk restore snap1 --to /var/lib/clickhouse --attach --ch-url http://localhost:8123
```

Downloads blobs from S3, recreates the part directory structure, and writes DDL files. The output directory can then be used as `CH_DATA_PATH` for a ClickHouse instance, or use `--attach` to load tables into a running server.

## Design Notes

### Part-Level CAS

ClickHouse MergeTree parts are immutable. Mutations, merges, projection
materialization, and index materialization create new parts rather than editing
existing part directories in place. That makes `system.parts.hash_of_all_files`
a useful content key.

Each part directory is archived recursively, so projection subdirectories and
secondary index files are included. `hash_of_all_files` covers those files too:
secondary index files appear as checksum entries and projections are represented
in ClickHouse's aggregate part hash. If a part changes, it gets a different hash
and is uploaded as a new object. If it does not change, snapshots reuse the same
object.

### Streaming ZIP Uploads

Each part is written as a stored ZIP and streamed through a bounded pipe into a
multipart S3 upload. The tool does not need a full part archive in memory or on
disk. Stored ZIPs also keep restore simple: a restored object expands back into
the original part directory.

The ZIP writer/reader is custom because normal ZIP libraries usually require
seeking or omit enough local-header size information that a non-seeking reader
cannot reliably stream stored entries. `chbk` knows file sizes from `stat`, so it
can write stream-friendly local headers and still emit a conventional central
directory for compatibility.

### Locking and GC

Backups are lock-free:

- blob keys are content-addressed and safe to upload idempotently
- auto snapshot names include shard, replica, timestamp, and a UUID suffix
- named snapshots are written with conditional create semantics

GC operations serialize through `gc/.lock`. `gc-all` also keeps blobs younger
than the grace period, defaulting to 6 hours, so it does not delete blobs from an
in-flight backup before that backup has written its manifest.

## Operations

### Concurrency Safety

Backup is lock-free: blobs are content-addressed (idempotent PUT), and manifest names include shard, replica, timestamp, and a UUID suffix to avoid collisions. Multiple nodes can back up to the same S3 prefix concurrently.

GC operations serialize on a distributed lock at `gc/.lock`. The grace period (default 6h) protects blobs uploaded by in-flight backups without requiring coordination markers.

Safe patterns:
- Multiple nodes backing up to the same S3 prefix concurrently
- Running backup and gc-live concurrently (GC holds exclusive lock)
- Running gc-all while backups run (grace period protects in-flight uploads)

### Concurrency Tuning

| Option | Default | Description |
|--------|---------|-------------|
| `--part-concurrency` | 8 | Parallel parts processed |
| `--multipart-part-concurrency` | 16 | Parallel chunk uploads per multipart upload |
| `--upload-min-chunk-size-mb` | 16 | Multipart chunk size lower bound |
| `--upload-max-chunk-size-mb` | 512 | Multipart chunk size upper bound |
| `--upload-target-parts` | 128 | Target multipart part count (lower = larger chunks) |
| `--download-concurrency` | 32 | Concurrent blob downloads (restore only) |
| `--delete-concurrency` | 32 | Concurrent deletions during GC |

### systemd Timers

Create `/etc/chbk.env`:
```
BACKUP_DIR=/var/lib/chbk
CH_DATA_PATH=/var/lib/clickhouse
CH_URL=http://localhost:8123
CH_SHARD=01
CH_REPLICA=replica-1
S3_BUCKET=my-bucket
S3_REGION=us-east-1
S3_ACCESS_KEY_ID=...
S3_SECRET_ACCESS_KEY=...
```

Hourly backups, `/etc/systemd/system/chbk.service`:

```ini
[Unit]
Description=ClickHouse backup

[Service]
Type=oneshot
EnvironmentFile=/etc/chbk.env
ExecStart=/usr/local/bin/chbk
```

`/etc/systemd/system/chbk.timer`:

```ini
[Unit]
Description=Run chbk hourly

[Timer]
OnCalendar=hourly
Persistent=true

[Install]
WantedBy=timers.target
```

Daily GC, `/etc/systemd/system/chbk-gc.service`:

```ini
[Unit]
Description=ClickHouse backup GC

[Service]
Type=oneshot
EnvironmentFile=/etc/chbk.env
ExecStart=/usr/local/bin/chbk gc-live --retain-all 24h --retain-daily 30d
```

`/etc/systemd/system/chbk-gc.timer`:

```ini
[Unit]
Description=Run chbk gc daily

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

Enable:
```bash
systemctl daemon-reload
systemctl enable --now chbk.timer chbk-gc.timer
```

## Testing

```bash
cargo test --bins
cargo test --test integration

# Requires Docker, AWS CLI, and ClickHouse/clickhouse-local:
cargo test --test ch_local_harness -- --test-threads=1
cargo test --test live_snapshot_e2e -- --test-threads=1
```

The full integration tests need Docker, the AWS CLI, and either `clickhouse`
with the `local` subcommand or `clickhouse-local`.

## License

MIT
