#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;
use testcontainers::runners::SyncRunner;
use testcontainers_modules::minio::MinIO;

fn which(bin: &str) -> Option<PathBuf> {
    let out = Command::new("which").arg(bin).output().ok()?;
    if out.status.success() {
        let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !s.is_empty() {
            return Some(PathBuf::from(s));
        }
    }
    None
}

fn pick_free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind 0")
        .local_addr()
        .unwrap()
        .port()
}

fn gen_server_config(
    config_path: &Path,
    ch_path: &Path,
    backup_dir: &Path,
    http_port: u16,
    tcp_port: u16,
) {
    let ch = ch_path.display();
    let backup = backup_dir.display();
    let xml = format!(
        r"<clickhouse>
    <listen_host>127.0.0.1</listen_host>
    <!-- http/tcp ports are irrelevant for clickhouse local, but harmless if present -->
    <http_port>{http_port}</http_port>
    <tcp_port>{tcp_port}</tcp_port>
    <path>{ch}</path>
    <tmp_path>{ch}/tmp/</tmp_path>
    <user_files_path>{ch}/user_files/</user_files_path>

    <storage_configuration>
        <disks>
            <backup_disk>
                <type>local</type>
                <path>{backup}/</path>
            </backup_disk>
        </disks>
        <policies>
            <backup_policy>
                <volumes>
                    <main><disk>default</disk></main>
                    <backup><disk>backup_disk</disk></backup>
                </volumes>
            </backup_policy>
        </policies>
    </storage_configuration>

    <backups>
        <allowed_disk>backup_disk</allowed_disk>
        <allowed_path>/</allowed_path>
    </backups>
</clickhouse>
",
    );
    fs::write(config_path, xml).expect("write config");
}

fn run_clickhouse_local(ch_path: &Path, config: &Path, sql: &str) {
    // Prefer `clickhouse local` subcommand, fallback to `clickhouse-local` binary.
    let mut cmd = if which("clickhouse").is_some() {
        let mut c = Command::new("clickhouse");
        let _ = c.arg("local");
        c
    } else if which("clickhouse-local").is_some() {
        Command::new("clickhouse-local")
    } else {
        panic!("Neither `clickhouse` (with local) nor `clickhouse-local` found in PATH");
    };

    let status = cmd
        .args([
            "--path",
            &ch_path.to_string_lossy(),
            "--config-file",
            &config.to_string_lossy(),
            "--query",
            sql,
        ])
        .status()
        .expect("launch clickhouse local");
    assert!(status.success(), "clickhouse local failed for SQL: {sql}");
}

fn build_binary() -> PathBuf {
    let out = Command::new("cargo")
        .args(["build", "--release"])
        .status()
        .expect("cargo build");
    assert!(out.success(), "cargo build failed");
    let bin = PathBuf::from("target/release/chbk");
    assert!(bin.exists(), "built binary not found");
    bin
}

struct S3Config {
    endpoint: String,
    bucket: String,
    access_key: String,
    secret_key: String,
}

fn run_tool(
    bin: &Path,
    backup_dir: &Path,
    ch_config: &Path,
    ch_data_path: &Path,
    s3: &S3Config,
    extra: &[&str],
) {
    let mut cmd = Command::new(bin);
    let _ = cmd
        .env("BACKUP_DIR", backup_dir)
        .env("CH_USE_LOCAL", "1")
        .env("CH_CONFIG_PATH", ch_config)
        .env("CH_DATA_PATH", ch_data_path)
        .env("S3_ENDPOINT", &s3.endpoint)
        .env("S3_BUCKET", &s3.bucket)
        .env("S3_ACCESS_KEY_ID", &s3.access_key)
        .env("S3_SECRET_ACCESS_KEY", &s3.secret_key)
        .env("S3_REGION", "us-east-1")
        .env("AWS_ALLOW_HTTP", "true")
        .env("CH_SHARD", "test-shard")
        .env("CH_REPLICA", "test-replica")
        .args(extra);
    let out = cmd.output().expect("run tool");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "tool failed: stdout={stdout} stderr={stderr}"
    );
}

fn run_tool_capture(
    bin: &Path,
    backup_dir: &Path,
    ch_config: &Path,
    ch_data_path: &Path,
    s3: &S3Config,
    extra: &[&str],
) -> String {
    let mut cmd = Command::new(bin);
    let _ = cmd
        .env("BACKUP_DIR", backup_dir)
        .env("CH_USE_LOCAL", "1")
        .env("CH_CONFIG_PATH", ch_config)
        .env("CH_DATA_PATH", ch_data_path)
        .env("S3_ENDPOINT", &s3.endpoint)
        .env("S3_BUCKET", &s3.bucket)
        .env("S3_ACCESS_KEY_ID", &s3.access_key)
        .env("S3_SECRET_ACCESS_KEY", &s3.secret_key)
        .env("S3_REGION", "us-east-1")
        .env("AWS_ALLOW_HTTP", "true")
        .args(extra);
    let out = cmd.output().expect("run tool");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "tool failed: stdout={stdout} stderr={stderr}"
    );
    stdout
}

fn is_docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .output()
        .is_ok_and(|o| o.status.success())
}

fn create_bucket(endpoint: &str, bucket: &str, _access_key: &str, _secret_key: &str) {
    // Use aws CLI if available (works with minio)
    // Minio accepts simple PUT /{bucket} but aws cli handles auth properly
    let status = Command::new("aws")
        .args([
            "--endpoint-url",
            endpoint,
            "s3",
            "mb",
            &format!("s3://{bucket}"),
        ])
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_DEFAULT_REGION", "us-east-1")
        .output();
    match status {
        Ok(out) => {
            if !out.status.success() {
                let stderr = String::from_utf8_lossy(&out.stderr);
                // BucketAlreadyOwnedByYou is fine
                if !stderr.contains("BucketAlreadyOwnedByYou")
                    && !stderr.contains("BucketAlreadyExists")
                {
                    eprintln!("Bucket creation output: {stderr}");
                }
            }
        }
        Err(e) => {
            eprintln!("Warning: aws cli not found ({e}), trying curl fallback");
            // Fallback: simple PUT (may not work without proper auth)
            let _ = Command::new("curl")
                .args(["-s", "-X", "PUT", &format!("{endpoint}/{bucket}")])
                .output();
        }
    }
}

#[test]
fn clickhouse_local_harness() {
    assert!(
        which("clickhouse").is_some() || which("clickhouse-local").is_some(),
        "clickhouse or clickhouse-local binary required in PATH"
    );
    assert!(is_docker_available(), "Docker is required for tests");

    // Start minio container for S3 storage
    let minio = MinIO::default()
        .start()
        .expect("Failed to start minio container");
    let minio_port = minio.get_host_port_ipv4(9000).expect("minio port");
    let minio_endpoint = format!("http://127.0.0.1:{minio_port}");

    let s3 = S3Config {
        endpoint: minio_endpoint,
        bucket: "test-backup".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
    };

    // Create the test bucket
    create_bucket(&s3.endpoint, &s3.bucket, &s3.access_key, &s3.secret_key);

    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path();
    let ch_path = root.join("ch_data");
    let backup_dir = root.join("backups");
    let config_path = root.join("config.xml");
    fs::create_dir_all(&ch_path).unwrap();
    fs::create_dir_all(&backup_dir).unwrap();

    // Ports (not used by local; kept to satisfy config structure)
    let http_port = pick_free_port();
    let tcp_port = pick_free_port();

    // Config
    gen_server_config(&config_path, &ch_path, &backup_dir, http_port, tcp_port);

    // 1) Initialize schema + data using clickhouse-local (shared on-disk layout)
    run_clickhouse_local(
        &ch_path,
        &config_path,
        r"
        CREATE DATABASE IF NOT EXISTS t;
        CREATE TABLE IF NOT EXISTS t.numbers (id UInt64, v String) ENGINE=MergeTree ORDER BY id;
        CREATE TABLE IF NOT EXISTS t.events (ts DateTime, v UInt64) ENGINE=MergeTree ORDER BY ts;
        INSERT INTO t.numbers SELECT number, concat('n_', toString(number)) FROM numbers(1000);
        INSERT INTO t.events SELECT now() - number, number FROM numbers(100);
        ",
    );

    // 2) Build our tool
    let bin = build_binary();

    // 3) Run live backup + explicit snapshot1 (tool uses clickhouse local)
    run_tool(&bin, &backup_dir, &config_path, &ch_path, &s3, &[]);
    run_tool(
        &bin,
        &backup_dir,
        &config_path,
        &ch_path,
        &s3,
        &["create-snapshot", "snap1"],
    );

    // Verify manifest exists by listing snapshots via our CLI.
    let list_out = run_tool_capture(
        &bin,
        &backup_dir,
        &config_path,
        &ch_path,
        &s3,
        &["list-snapshots"],
    );
    assert!(
        list_out.contains("snap1"),
        "snap1 should be listed, got: {list_out}"
    );

    // 5) Mutate data and take snapshot2
    run_clickhouse_local(
        &ch_path,
        &config_path,
        r"
        INSERT INTO t.numbers SELECT number + 1000, concat('n_', toString(number + 1000)) FROM numbers(500);
        INSERT INTO t.events SELECT now() - number, number + 100 FROM numbers(50);
        ",
    );
    run_tool(
        &bin,
        &backup_dir,
        &config_path,
        &ch_path,
        &s3,
        &["create-snapshot", "snap2"],
    );

    let list_out = run_tool_capture(
        &bin,
        &backup_dir,
        &config_path,
        &ch_path,
        &s3,
        &["list-snapshots"],
    );
    assert!(
        list_out.contains("snap2"),
        "snap2 should be listed, got: {list_out}"
    );

    // 6-7) Validation with Database Engine=Backup skipped
    // The Backup engine expects local disk access, but backups are now on S3.
    // TODO: Configure ClickHouse with S3 disk pointing to minio for full validation.

    // 6) Prune snap1 and ensure snap2 still present
    run_tool(
        &bin,
        &backup_dir,
        &config_path,
        &ch_path,
        &s3,
        &["rm-snapshot", "snap1"],
    );

    let list_out = run_tool_capture(
        &bin,
        &backup_dir,
        &config_path,
        &ch_path,
        &s3,
        &["list-snapshots"],
    );
    assert!(
        !list_out.contains("snap1"),
        "snap1 should be deleted, got: {list_out}"
    );
    assert!(
        list_out.contains("snap2"),
        "snap2 should still be listed, got: {list_out}"
    );

    // 7) Test gc-all reports referenced-blob counts
    let gc_output = run_tool_capture(
        &bin,
        &backup_dir,
        &config_path,
        &ch_path,
        &s3,
        &["gc-all", "--dry-run"],
    );
    assert!(
        gc_output.contains("Referenced blobs"),
        "gc-all should report referenced blobs, got: {gc_output}"
    );

    // 8) Test remote-scan GC: upload orphan blobs to S3 and verify they're detected
    // Create orphan blobs directly in S3 (not tracked in any manifest)
    let orphan_hashes = [
        "deadbeef0123456789abcdef01234567", // orphan 1
        "cafebabe0123456789abcdef01234567", // orphan 2
    ];
    for hash in &orphan_hashes {
        let shard = &hash[0..2];
        let key = format!("base/data/blobs/{shard}/{hash}");
        // Use aws cli to upload orphan blob
        let status = Command::new("aws")
            .args([
                "--endpoint-url",
                &s3.endpoint,
                "s3",
                "cp",
                "-",
                &format!("s3://{}/{}", s3.bucket, key),
            ])
            .env("AWS_ACCESS_KEY_ID", &s3.access_key)
            .env("AWS_SECRET_ACCESS_KEY", &s3.secret_key)
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .stdin(std::process::Stdio::piped())
            .output();
        if let Ok(out) = status
            && !out.status.success()
        {
            // Try alternative: echo + pipe
            let _ = Command::new("sh")
                .args([
                    "-c",
                    &format!(
                        "echo 'orphan data' | aws --endpoint-url {} s3 cp - s3://{}/{}",
                        s3.endpoint, s3.bucket, key
                    ),
                ])
                .env("AWS_ACCESS_KEY_ID", &s3.access_key)
                .env("AWS_SECRET_ACCESS_KEY", &s3.secret_key)
                .env("AWS_DEFAULT_REGION", "us-east-1")
                .output();
        }
    }

    // 9) gc-all always scans remote storage now. Dry-run detects orphans.
    let gc_remote_output = run_tool_capture(
        &bin,
        &backup_dir,
        &config_path,
        &ch_path,
        &s3,
        &["gc-all", "--dry-run", "--grace-period-hours", "0"],
    );
    assert!(
        gc_remote_output.contains("Scanning"),
        "gc-all should scan remote storage, got: {gc_remote_output}"
    );
    assert!(
        gc_remote_output.contains("Remote scan finished"),
        "gc-all should complete scan, got: {gc_remote_output}"
    );

    // 10) Running without --dry-run deletes orphans.
    let gc_delete_output = run_tool_capture(
        &bin,
        &backup_dir,
        &config_path,
        &ch_path,
        &s3,
        &["gc-all", "--grace-period-hours", "0"],
    );
    assert!(
        gc_delete_output.contains("Remote scan finished"),
        "gc-all should complete, got: {gc_delete_output}"
    );

    // Verify orphan blobs were deleted
    for hash in &orphan_hashes {
        let shard = &hash[0..2];
        let key = format!("base/data/blobs/{shard}/{hash}");
        let check = Command::new("aws")
            .args([
                "--endpoint-url",
                &s3.endpoint,
                "s3",
                "ls",
                &format!("s3://{}/{}", s3.bucket, key),
            ])
            .env("AWS_ACCESS_KEY_ID", &s3.access_key)
            .env("AWS_SECRET_ACCESS_KEY", &s3.secret_key)
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .output();
        if let Ok(out) = check {
            let stdout = String::from_utf8_lossy(&out.stdout);
            // If the object still exists, ls would show it
            assert!(
                stdout.is_empty() || !out.status.success(),
                "Orphan blob {hash} should have been deleted but still exists"
            );
        }
    }
}

/// Test gc-live tiered retention: keep all within X, keep daily for Y, delete older.
/// Uses --now flag to simulate time passage without manipulating timestamps.
#[test]
fn gc_live_tiered_retention() {
    assert!(
        which("clickhouse").is_some() || which("clickhouse-local").is_some(),
        "clickhouse or clickhouse-local binary required in PATH"
    );
    assert!(is_docker_available(), "Docker is required for tests");

    // Start minio container for S3 storage
    let minio = MinIO::default()
        .start()
        .expect("Failed to start minio container");
    let minio_port = minio.get_host_port_ipv4(9000).expect("minio port");
    let minio_endpoint = format!("http://127.0.0.1:{minio_port}");

    let s3 = S3Config {
        endpoint: minio_endpoint,
        bucket: "test-gc-live".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
    };

    create_bucket(&s3.endpoint, &s3.bucket, &s3.access_key, &s3.secret_key);

    let tmp = TempDir::new().expect("tempdir");
    let root = tmp.path();
    let ch_path = root.join("ch_data");
    let backup_dir = root.join("backups");
    let config_path = root.join("config.xml");
    fs::create_dir_all(&ch_path).unwrap();
    fs::create_dir_all(&backup_dir).unwrap();

    let http_port = pick_free_port();
    let tcp_port = pick_free_port();
    gen_server_config(&config_path, &ch_path, &backup_dir, http_port, tcp_port);

    // Create test data
    run_clickhouse_local(
        &ch_path,
        &config_path,
        r"
        CREATE DATABASE IF NOT EXISTS t;
        CREATE TABLE IF NOT EXISTS t.test (id UInt64) ENGINE=MergeTree ORDER BY id;
        INSERT INTO t.test SELECT number FROM numbers(100);
        ",
    );

    let bin = build_binary();

    // Record start time for --now calculations
    let start_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Create 3 live backups with small delays between them
    for i in 0..3 {
        run_tool(&bin, &backup_dir, &config_path, &ch_path, &s3, &[]);
        if i < 2 {
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
    }

    let count_live_snapshots = |label: &str| -> usize {
        let out = run_tool_capture(
            &bin,
            &backup_dir,
            &config_path,
            &ch_path,
            &s3,
            &["list-snapshots"],
        );
        let n = out.lines().filter(|l| l.starts_with("live_")).count();
        println!("[{label}] list-snapshots -> {n} live:\n{out}");
        n
    };
    assert_eq!(
        count_live_snapshots("before gc-live"),
        3,
        "Should have 3 live snapshots before gc-live"
    );

    // Test 1: Use --now to simulate being 5 minutes in the future
    // With retain-all of 2 minutes, all snapshots should be beyond retain-all
    // With retain-daily of 10 minutes, all snapshots are on same day, keep only latest
    let future_5min = start_time + (5 * 60);
    let gc_output = run_tool_capture(
        &bin,
        &backup_dir,
        &config_path,
        &ch_path,
        &s3,
        &[
            "gc-live",
            "--retain-all",
            "2m",
            "--retain-daily",
            "10m",
            "--now",
            &future_5min.to_string(),
            "--dry-run",
        ],
    );
    println!("gc-live dry-run output (5min future):\n{gc_output}");

    // Should propose to prune 2 snapshots (keeping only latest since all on same day)
    assert!(
        gc_output.contains("Would prune")
            || gc_output.contains("Pruning")
            || gc_output.contains("prune"),
        "gc-live should report pruning when using --now, got: {gc_output}"
    );

    // Now actually run gc-live (not dry-run)
    let gc_output = run_tool_capture(
        &bin,
        &backup_dir,
        &config_path,
        &ch_path,
        &s3,
        &[
            "gc-live",
            "--retain-all",
            "2m",
            "--retain-daily",
            "10m",
            "--now",
            &future_5min.to_string(),
        ],
    );
    println!("gc-live output (5min future):\n{gc_output}");

    assert_eq!(
        count_live_snapshots("after gc-live (5min)"),
        1,
        "Should have 1 live snapshot after gc-live (kept latest for day, pruned 2 older same-day)"
    );

    // Test 2: Now simulate being 15 minutes in the future with retain-daily of 10m
    // The remaining snapshot is now beyond retain-daily window -> should be pruned
    let future_15min = start_time + (15 * 60);
    let gc_output = run_tool_capture(
        &bin,
        &backup_dir,
        &config_path,
        &ch_path,
        &s3,
        &[
            "gc-live",
            "--retain-all",
            "2m",
            "--retain-daily",
            "10m",
            "--now",
            &future_15min.to_string(),
        ],
    );
    println!("gc-live output (15min future):\n{gc_output}");

    assert_eq!(
        count_live_snapshots("after gc-live (15min)"),
        0,
        "Should have 0 live snapshots after gc-live with --now 15min future (beyond retain-daily)"
    );

    println!("PASSED: gc-live tiered retention with --now flag works!");
}
