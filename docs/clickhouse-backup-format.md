# chbk Storage Format

This document describes the format written by `chbk` today. It is not the
ClickHouse native `.backup` XML format.

## Object Layout

All paths are relative to the configured S3 prefix.

```text
base/
  data/
    blobs/
      aa/
        aa11...ff.zip-like-object
snapshots/
  live_01_replica-1_y2026m05d15_h13m22s10_ab12cd34.json.zst
  before-upgrade.json.zst
gc/
  .lock
```

Blob keys use the first two hex characters of the part hash as a shard prefix:

```text
base/data/blobs/{hash[0..2]}/{hash}
```

Snapshot keys are:

```text
snapshots/{snapshot-name}.json.zst
```

## Blob Objects

Each blob object is one stored ZIP archive containing one ClickHouse part
directory. The blob name is the part's `system.parts.hash_of_all_files` value.

The ZIP payload is deliberately stored, not compressed:

- ClickHouse part files are already compressed.
- Compression would add CPU cost in the backup hot path.
- Stored ZIP extraction reconstructs the original part directory directly.
- Streaming upload only needs bounded buffers, not a completed local archive.

The ZIP writer emits normal central-directory records for compatibility, but
restore reads local headers sequentially so it can stream without seeking.

## Manifest Objects

Each snapshot manifest is JSON compressed with zstd level 3. The manifest is a
self-contained record with identity, part references, and embedded metadata
files.

Example after decompression:

```json
{
  "name": "before-upgrade",
  "timestamp": 1778876530,
  "shard": "01",
  "replica": "replica-1",
  "created_by": "clickhouse-a.example",
  "parts": [
    {
      "database": "analytics",
      "table_name": "events",
      "part_name": "202605_123_123_0",
      "blob_hash": "aa11bb22cc33dd44ee55ff6677889900",
      "blob_size": 73400320
    }
  ],
  "files": [
    {
      "path": "metadata/analytics.sql",
      "content_b64": "Q1JFQVRFIERBVEFCQVNFIGFuYWx5dGljcw=="
    },
    {
      "path": "metadata/analytics/events.sql",
      "content_b64": "Q1JFQVRFIFRBQkxFIGFuYWx5dGljcy5ldmVudHMgLi4u"
    }
  ]
}
```

The S3 key is authoritative for the snapshot name. If the name stored inside the
JSON differs from the key, `chbk` uses the key-derived name when reading.

Metadata files are embedded as base64 so SQL text and binary user scripts can be
handled uniformly.

## Snapshot Names

Auto-created snapshots use this format:

```text
live_{shard}_{replica}_yYYYYmMMdDD_hHHmMMsSS_{uuid8}
```

The timestamp portion is lexicographically sortable, and the UUID suffix avoids
collisions between concurrent runs on the same replica.

Named snapshots use the name supplied to `create-snapshot`. Manifests are
write-once, so trying to create the same named snapshot twice fails rather than
replacing the existing snapshot.

## Restore Layout

Restore writes a ClickHouse data directory shape:

```text
metadata/
  analytics.sql
  analytics/
    events.sql
data/
  analytics/
    events/
      202605_123_123_0/
        checksums.txt
        columns.txt
        data.bin
        ...
```

With `--attach`, parts are extracted under each table's `detached/` directory
and then attached with `ALTER TABLE db.table ATTACH PART 'part_name'`.

## Consistency Model

Backup writes are lock-free:

1. Query active parts from ClickHouse.
2. Stage needed parts through hardlinks.
3. Upload missing blobs by content hash.
4. Write the manifest with conditional create.

If a process crashes before the manifest write, uploaded blobs are harmless
orphans. `gc-all` later removes orphaned blobs after the configured grace
period.

GC operations take `gc/.lock` because they delete shared objects. The lock is
held with a heartbeat and destructive operations stop if the lock is lost.
