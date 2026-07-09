#!/usr/bin/env bash
set -euo pipefail

# Bandwidth bench for part-level CAS uploads:
# - Generates MergeTree parts with clickhouse-local
# - Runs `chbk create-snapshot` streaming stored ZIPs to a local MinIO (S3)
# - Prints parsed per-second ZIP/write and upload rates from Progress logs
#
# Requirements:
# - clickhouse (with `local`) or clickhouse-local
# - docker
# - aws CLI (recommended; curl fallback attempted)
#
# Usage:
#   ./bench_bw_local_minio.sh
#
# Tuning (env vars):
#   INSERTS=20 ROWS_PER_INSERT=400000 BYTES_PER_ROW=256 PART_CONCURRENCY=4 ./bench_bw_local_minio.sh

INSERTS="${INSERTS:-20}"
ROWS_PER_INSERT="${ROWS_PER_INSERT:-400000}"
BYTES_PER_ROW="${BYTES_PER_ROW:-256}"
PART_CONCURRENCY="${PART_CONCURRENCY:-4}"

if command -v clickhouse >/dev/null 2>&1; then
  CLICKHOUSE_LOCAL=(clickhouse local)
elif command -v clickhouse-local >/dev/null 2>&1; then
  CLICKHOUSE_LOCAL=(clickhouse-local)
else
  echo "error: need clickhouse (with local) or clickhouse-local in PATH" >&2
  exit 2
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "error: need docker in PATH" >&2
  exit 2
fi

if [ ! -x "./target/release/chbk" ]; then
  echo "Building release binary..."
  cargo build --release
fi

root="$(mktemp -d -t chbk_bwbench.XXXXXX)"
mkdir -p "$root/ch_data" "$root/backups"

cat >"$root/config.xml" <<EOF
<clickhouse>
  <listen_host>127.0.0.1</listen_host>
  <path>$root/ch_data</path>
  <tmp_path>$root/ch_data/tmp/</tmp_path>
  <user_files_path>$root/ch_data/user_files/</user_files_path>
</clickhouse>
EOF

sql_file="$root/setup.sql"
{
  echo "CREATE DATABASE IF NOT EXISTS bench;"
  echo "DROP TABLE IF EXISTS bench.t;"
  if [ "$BYTES_PER_ROW" -gt 256 ]; then
    echo "SET allow_suspicious_fixed_string_types = 1;"
  fi
  echo "CREATE TABLE bench.t (id UInt64, v FixedString(${BYTES_PER_ROW})) ENGINE=MergeTree ORDER BY id;"
  for i in $(seq 0 $((INSERTS - 1))); do
    off=$((i * ROWS_PER_INSERT))
    echo "INSERT INTO bench.t SELECT number + ${off}, randomFixedString(${BYTES_PER_ROW}) FROM numbers(${ROWS_PER_INSERT});"
  done
} >"$sql_file"

echo "bench root: $root"
echo "Generating parts (${INSERTS} inserts x ${ROWS_PER_INSERT} rows x ${BYTES_PER_ROW}B)..."

"${CLICKHOUSE_LOCAL[@]}" --path "$root/ch_data" --config-file "$root/config.xml" --query "$(tr "\n" " " <"$sql_file")" >/dev/null

name="chbk-bwbench-minio-$RANDOM"
docker run -d --rm --name "$name" -p 127.0.0.1::9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data >/dev/null

for _ in $(seq 1 50); do
  if docker port "$name" 9000/tcp >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

port="$(docker port "$name" 9000/tcp | head -n1 | sed -E 's/.*:([0-9]+)$/\1/')"
endpoint="http://127.0.0.1:${port}"

echo "MinIO endpoint: $endpoint"

for _ in $(seq 1 50); do
  if curl -sSf "$endpoint/minio/health/ready" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

bucket="bench-bw"
if command -v aws >/dev/null 2>&1; then
  AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_DEFAULT_REGION=us-east-1 \
    aws --endpoint-url "$endpoint" s3 mb "s3://${bucket}" >/dev/null 2>&1 || true
else
  echo "warning: aws CLI not found; attempting curl bucket create" >&2
  curl -sS -X PUT "${endpoint}/${bucket}" >/dev/null 2>&1 || true
fi

out="$root/chbk_output.txt"
echo "Running create-snapshot (watch Progress lines)..."

BACKUP_DIR="$root/backups" \
CH_USE_LOCAL=1 \
CH_DATA_PATH="$root/ch_data" \
CH_CONFIG_PATH="$root/config.xml" \
S3_ENDPOINT="$endpoint" \
S3_BUCKET="$bucket" \
S3_ACCESS_KEY_ID=minioadmin \
S3_SECRET_ACCESS_KEY=minioadmin \
S3_REGION=us-east-1 \
AWS_ALLOW_HTTP=true \
PART_CONCURRENCY="$PART_CONCURRENCY" \
./target/release/chbk --only '^bench\.' create-snapshot bwbench 2>&1 | tee "$out"

(docker stop "$name" >/dev/null 2>&1 || true) &


echo "Output log: $out"
echo "Bench data dir (delete when done): $root"
