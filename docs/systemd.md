# systemd timers

This example runs hourly backups and daily live-snapshot pruning.

Create `/etc/chbk.env`:

```ini
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

`BACKUP_DIR` and `CH_DATA_PATH` must be on the same filesystem because backup
staging uses hardlinks.

Create `/etc/systemd/system/chbk.service`:

```ini
[Unit]
Description=ClickHouse backup

[Service]
Type=oneshot
EnvironmentFile=/etc/chbk.env
ExecStart=/usr/local/bin/chbk
```

Create `/etc/systemd/system/chbk.timer`:

```ini
[Unit]
Description=Run chbk hourly

[Timer]
OnCalendar=hourly
Persistent=true

[Install]
WantedBy=timers.target
```

Create `/etc/systemd/system/chbk-gc.service`:

```ini
[Unit]
Description=ClickHouse backup GC

[Service]
Type=oneshot
EnvironmentFile=/etc/chbk.env
ExecStart=/usr/local/bin/chbk gc-live --retain-all 24h --retain-daily 30d
```

Create `/etc/systemd/system/chbk-gc.timer`:

```ini
[Unit]
Description=Run chbk GC daily

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

Enable both timers:

```bash
systemctl daemon-reload
systemctl enable --now chbk.timer chbk-gc.timer
```
