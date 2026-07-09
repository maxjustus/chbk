# Local ClickHouse and MinIO trial

The repository's `docker-compose.yml` starts ClickHouse and creates a MinIO
bucket for a local backup and restore trial.

Start the services and create sample data:

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
```

Configure `chbk` for the local services:

```bash
export CH_URL=http://localhost:8128
export CH_PASSWORD=default
export CH_DATA_PATH="$PWD/ch_data"
export CH_SHARD=local
export CH_REPLICA=local

export S3_ENDPOINT=http://localhost:9000
export S3_BUCKET=test-bucket
export S3_REGION=us-east-1
export S3_ACCESS_KEY_ID=minioadmin
export S3_SECRET_ACCESS_KEY=minioadmin
export AWS_ALLOW_HTTP=true
```

Create and restore a snapshot:

```bash
chbk create-snapshot local-test
chbk restore local-test --to /tmp/chbk-restore
```
