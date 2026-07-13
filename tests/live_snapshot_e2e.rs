#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
//! End-to-end test for snapshot restore.
//!
//! This is THE critical test - verifies the core use case works:
//! 1. Create backup (writes a manifest to snapshots/{name}.json.zst)
//! 2. Restore snapshot to a data directory using `restore` command
//! 3. Start ClickHouse pointing at the restored directory
//! 4. Data is queryable and correct
//!
//! Uses testcontainers for MinIO and ClickHouse - fully self-contained.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use tempfile::TempDir;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{GenericImage, ImageExt};
use testcontainers_modules::minio::MinIO;

const MINIO_ACCESS_KEY: &str = "minioadmin";
const MINIO_SECRET_KEY: &str = "minioadmin";
const BUCKET: &str = "backups";
const CH_USER: &str = "default";
const CH_PASSWORD: &str = "test";

fn is_docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .output()
        .is_ok_and(|o| o.status.success())
}

fn create_bucket(endpoint: &str, bucket: &str) {
    let _ = Command::new("aws")
        .args([
            "--endpoint-url",
            endpoint,
            "s3",
            "mb",
            &format!("s3://{bucket}"),
        ])
        .env("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
        .env("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
        .env("AWS_DEFAULT_REGION", "us-east-1")
        .output();
}

fn ch_query(ch_url: &str, sql: &str) -> Result<String, String> {
    let url_with_auth = format!("{ch_url}/?user={CH_USER}&password={CH_PASSWORD}");
    let output = Command::new("curl")
        .args(["-s", "-X", "POST", &url_with_auth, "-d", sql])
        .output()
        .map_err(|e| format!("curl failed: {e}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();

    if stdout.contains("Code:") || stdout.contains("DB::Exception") {
        return Err(format!("ClickHouse error: {stdout}"));
    }

    if !output.status.success() {
        return Err(format!("Query failed: {stdout} {stderr}"));
    }
    Ok(stdout)
}

fn ch_query_expect(ch_url: &str, sql: &str) -> String {
    ch_query(ch_url, sql).unwrap_or_else(|e| panic!("Query failed: {e} -- SQL: {sql}"))
}

fn build_binary() -> std::path::PathBuf {
    let out = Command::new("cargo")
        .args(["build", "--release"])
        .status()
        .expect("cargo build");
    assert!(out.success(), "cargo build failed");
    let bin = std::path::PathBuf::from("target/release/chbk");
    assert!(bin.exists(), "built binary not found");
    bin
}

fn assert_staging_clean(backup_dir: &Path) {
    let leftovers: Vec<_> = fs::read_dir(backup_dir)
        .expect("read backup dir")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name())
        .filter(|name| {
            name.to_str().is_some_and(|name| {
                name == "staging_parts" || name.starts_with("staging_parts-old-")
            })
        })
        .collect();
    assert!(leftovers.is_empty(), "staging leftovers: {leftovers:?}");
}

fn wait_for_clickhouse(url: &str, timeout_secs: u64) -> bool {
    let start = std::time::Instant::now();
    let url_with_auth = format!("{url}/?user={CH_USER}&password={CH_PASSWORD}");
    while start.elapsed().as_secs() < timeout_secs {
        if let Ok(output) = Command::new("curl")
            .args(["-s", "-f", "-X", "POST", &url_with_auth, "-d", "SELECT 1"])
            .output()
            && output.status.success()
        {
            return true;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    false
}

fn generate_ch_config(config_dir: &Path, minio_endpoint: &str) {
    generate_ch_config_inner(config_dir, minio_endpoint, false);
}

fn generate_ch_config_with_keeper(config_dir: &Path, minio_endpoint: &str) {
    generate_ch_config_inner(config_dir, minio_endpoint, true);
}

fn generate_ch_config_inner(config_dir: &Path, minio_endpoint: &str, enable_keeper: bool) {
    let listen_xml = r"<clickhouse>
    <listen_host>0.0.0.0</listen_host>
</clickhouse>
";
    let listen_path = config_dir.join("listen.xml");
    let mut f = fs::File::create(&listen_path).expect("create listen.xml");
    f.write_all(listen_xml.as_bytes())
        .expect("write listen.xml");

    let storage_xml = format!(
        r"<clickhouse>
    <storage_configuration>
        <disks>
            <s3_backup>
                <type>s3_plain</type>
                <endpoint>{minio_endpoint}/{BUCKET}/</endpoint>
                <access_key_id>{MINIO_ACCESS_KEY}</access_key_id>
                <secret_access_key>{MINIO_SECRET_KEY}</secret_access_key>
            </s3_backup>
        </disks>
    </storage_configuration>
    <backups>
        <allowed_disk>s3_backup</allowed_disk>
        <allowed_path>/</allowed_path>
    </backups>
</clickhouse>
",
    );

    let storage_path = config_dir.join("storage.xml");
    let mut f = fs::File::create(&storage_path).expect("create storage.xml");
    f.write_all(storage_xml.as_bytes())
        .expect("write storage.xml");

    if enable_keeper {
        let keeper_xml = r"<clickhouse>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>30000</session_timeout_ms>
        </coordination_settings>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>localhost</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
    <zookeeper>
        <node>
            <host>localhost</host>
            <port>9181</port>
        </node>
    </zookeeper>
    <macros>
        <shard>01</shard>
        <replica>01</replica>
    </macros>
</clickhouse>
";
        let keeper_path = config_dir.join("keeper.xml");
        let mut f = fs::File::create(&keeper_path).expect("create keeper.xml");
        f.write_all(keeper_xml.as_bytes())
            .expect("write keeper.xml");
    }
}

/// Tests the backup and restore workflow with CONCRETE DATA VALIDATION.
///
/// This test:
/// 1. Creates test data in ClickHouse
/// 2. Backs up using chbk (writes a manifest per snapshot)
/// 3. Creates a named snapshot
/// 4. Uses `restore --to` to export to a data directory
/// 5. Attaches restored parts to ClickHouse
/// 6. QUERIES THE RESTORED DATA TO VERIFY IT MATCHES THE ORIGINAL
#[test]
fn snapshot_restore_and_verify_data() {
    assert!(is_docker_available(), "Docker is required for tests");
    assert!(
        Command::new("aws").arg("--version").output().is_ok(),
        "aws CLI is required for tests"
    );

    // Create a Docker network for container communication
    let network_name = format!("chbk_test_{}", std::process::id());
    let _ = Command::new("docker")
        .args(["network", "create", &network_name])
        .output();

    // Cleanup on exit
    struct NetworkCleanup(String);
    impl Drop for NetworkCleanup {
        fn drop(&mut self) {
            let _ = Command::new("docker")
                .args(["network", "rm", &self.0])
                .output();
        }
    }
    let _network_cleanup = NetworkCleanup(network_name.clone());

    println!("Step 1: Starting MinIO container...");

    let minio_container_name = "minio";
    let minio = MinIO::default()
        .with_network(&network_name)
        .with_container_name(minio_container_name)
        .start()
        .expect("Failed to start MinIO container");

    std::thread::sleep(Duration::from_secs(2));

    let minio_port = minio.get_host_port_ipv4(9000).expect("minio port");
    let minio_host_endpoint = format!("http://127.0.0.1:{minio_port}");
    let minio_internal_endpoint = format!("http://{minio_container_name}:9000");

    create_bucket(&minio_host_endpoint, BUCKET);
    println!("  MinIO ready at {minio_host_endpoint}");

    // Create temp dirs - restore_tmp is separate so ClickHouse can mount it
    let config_tmp = TempDir::new().expect("tempdir for config");
    let ch_data_tmp = TempDir::new().expect("tempdir for clickhouse data");
    let restore_tmp = TempDir::new().expect("tempdir for restore");

    generate_ch_config(config_tmp.path(), &minio_internal_endpoint);

    println!("Step 2: Starting ClickHouse container...");

    // Mount the restore directory so ClickHouse can access restored parts
    let ch_image = GenericImage::new("clickhouse/clickhouse-server", "latest")
        .with_wait_for(WaitFor::seconds(3))
        .with_exposed_port(8123.tcp())
        .with_network(&network_name)
        .with_env_var("CLICKHOUSE_USER", "default")
        .with_env_var("CLICKHOUSE_PASSWORD", "test")
        .with_mount(Mount::bind_mount(
            config_tmp.path().to_string_lossy().to_string(),
            "/etc/clickhouse-server/config.d",
        ))
        .with_mount(Mount::bind_mount(
            ch_data_tmp.path().to_string_lossy().to_string(),
            "/var/lib/clickhouse",
        ))
        .with_mount(Mount::bind_mount(
            restore_tmp.path().to_string_lossy().to_string(),
            "/restore",
        ));

    let clickhouse = ch_image
        .start()
        .expect("Failed to start ClickHouse container");
    let ch_port = clickhouse
        .get_host_port_ipv4(8123)
        .expect("clickhouse port");
    let ch_url = format!("http://127.0.0.1:{ch_port}");

    assert!(
        wait_for_clickhouse(&ch_url, 60),
        "ClickHouse did not become ready in 60 seconds"
    );

    let version = ch_query_expect(&ch_url, "SELECT version()");
    println!("  ClickHouse {version} ready at {ch_url}");

    println!("Step 3: Creating test data...");
    let _ = ch_query_expect(&ch_url, "CREATE DATABASE IF NOT EXISTS testdb");
    let _ = ch_query_expect(
        &ch_url,
        "CREATE TABLE testdb.users (id UInt64, name String) ENGINE = MergeTree ORDER BY id",
    );

    let _ = ch_query_expect(
        &ch_url,
        "INSERT INTO testdb.users SELECT number, concat('user_', toString(number)) FROM numbers(100)",
    );

    std::thread::sleep(Duration::from_secs(1));

    // Capture expected values BEFORE backup
    let expected_count = ch_query_expect(&ch_url, "SELECT count() FROM testdb.users");
    let expected_sum = ch_query_expect(&ch_url, "SELECT sum(id) FROM testdb.users");
    let expected_first_name =
        ch_query_expect(&ch_url, "SELECT name FROM testdb.users WHERE id = 0");
    let expected_last_name =
        ch_query_expect(&ch_url, "SELECT name FROM testdb.users WHERE id = 99");

    assert_eq!(expected_count, "100");
    println!(
        "  Created 100 users (sum={expected_sum}, first='{expected_first_name}', last='{expected_last_name}')"
    );

    println!("Step 4: Running backup to create named snapshot...");
    let binary = build_binary();
    let backup_tmp = TempDir::new().expect("tempdir for backup");

    // Create a named snapshot
    let output = Command::new(&binary)
        .args(["--only", "^testdb\\.", "create-snapshot", "test_snap"])
        .env("CH_URL", &ch_url)
        .env("CH_USER", CH_USER)
        .env("CH_PASSWORD", CH_PASSWORD)
        .env("CH_DATA_PATH", ch_data_tmp.path())
        .env("BACKUP_DIR", backup_tmp.path())
        .env("S3_ENDPOINT", &minio_host_endpoint)
        .env("S3_BUCKET", BUCKET)
        .env("S3_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
        .env("S3_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
        .env("S3_REGION", "us-east-1")
        .env("CH_SHARD", "01")
        .env("CH_REPLICA", "01")
        .output()
        .expect("backup failed");

    assert!(
        output.status.success(),
        "Backup failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_staging_clean(backup_tmp.path());
    println!("  Backup completed");

    println!("Step 5: Restoring snapshot to mounted directory...");
    let restore_dir = restore_tmp.path().join("restored_data");

    let output = Command::new(&binary)
        .args([
            "restore",
            "test_snap",
            "--to",
            restore_dir.to_str().unwrap(),
        ])
        .env("BACKUP_DIR", backup_tmp.path())
        .env("S3_ENDPOINT", &minio_host_endpoint)
        .env("S3_BUCKET", BUCKET)
        .env("S3_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
        .env("S3_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
        .env("S3_REGION", "us-east-1")
        .output()
        .expect("restore failed");

    assert!(
        output.status.success(),
        "Restore failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("  Restore completed to {restore_dir:?}");

    println!("Step 6: Creating restore target database and table...");

    // Create a new database for restored data
    let _ = ch_query_expect(&ch_url, "CREATE DATABASE IF NOT EXISTS restored_db");

    // Create table with same schema
    let _ = ch_query_expect(
        &ch_url,
        "CREATE TABLE restored_db.users (id UInt64, name String) ENGINE = MergeTree ORDER BY id",
    );

    println!("Step 7: Copying restored parts to ClickHouse detached folder...");

    // List parts from restored data
    let restored_parts_dir = restore_dir.join("data").join("testdb").join("users");
    let parts: Vec<_> = fs::read_dir(&restored_parts_dir)
        .expect("read restored parts dir")
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .collect();

    println!("  Found {} part(s) to attach", parts.len());
    assert!(!parts.is_empty(), "Should have at least one part to attach");

    // The restored parts are now at /restore/restored_data/data/testdb/users/<part>/
    // We need to copy them to ClickHouse's detached folder
    // ClickHouse data is at /var/lib/clickhouse which maps to ch_data_tmp

    // Create detached directory for the restored table
    let detached_dir = ch_data_tmp
        .path()
        .join("data")
        .join("restored_db")
        .join("users")
        .join("detached");
    fs::create_dir_all(&detached_dir).expect("create detached dir");

    // Copy each part to detached folder
    for part in &parts {
        let part_name = part.file_name();
        let src = part.path();
        let dst = detached_dir.join(&part_name);

        // Copy directory recursively
        copy_dir_recursive(&src, &dst).expect("copy part to detached");
        println!("  Copied part {} to detached/", part_name.to_string_lossy());
    }

    println!("Step 8: Attaching parts to ClickHouse...");

    // Attach all parts from detached folder
    let attach_result = ch_query(
        &ch_url,
        "ALTER TABLE restored_db.users ATTACH PARTITION tuple()",
    );

    // If tuple() doesn't work, try attaching each part individually
    if attach_result.is_err() {
        println!("  Partition attach failed, trying individual parts...");
        for part in &parts {
            let part_name = part.file_name().to_string_lossy().to_string();
            let attach_sql = format!("ALTER TABLE restored_db.users ATTACH PART '{part_name}'");
            match ch_query(&ch_url, &attach_sql) {
                Ok(_) => println!("  Attached part: {part_name}"),
                Err(e) => println!("  Failed to attach {part_name}: {e}"),
            }
        }
    } else {
        println!("  Attached all parts via partition");
    }

    // Wait for attach to complete
    std::thread::sleep(Duration::from_secs(1));

    println!("Step 9: VERIFYING RESTORED DATA MATCHES ORIGINAL...");

    // Query the restored table
    let restored_count = ch_query_expect(&ch_url, "SELECT count() FROM restored_db.users");
    let restored_sum = ch_query_expect(&ch_url, "SELECT sum(id) FROM restored_db.users");
    let restored_first_name =
        ch_query_expect(&ch_url, "SELECT name FROM restored_db.users WHERE id = 0");
    let restored_last_name =
        ch_query_expect(&ch_url, "SELECT name FROM restored_db.users WHERE id = 99");

    println!("  Original: count={expected_count}, sum={expected_sum}");
    println!("  Restored: count={restored_count}, sum={restored_sum}");

    // CRITICAL ASSERTIONS - This is the real test
    assert_eq!(
        restored_count, expected_count,
        "ROW COUNT MISMATCH: expected {expected_count}, got {restored_count}"
    );

    assert_eq!(
        restored_sum, expected_sum,
        "SUM(id) MISMATCH: expected {expected_sum}, got {restored_sum}"
    );

    assert_eq!(
        restored_first_name, expected_first_name,
        "FIRST ROW NAME MISMATCH: expected '{expected_first_name}', got '{restored_first_name}'"
    );

    assert_eq!(
        restored_last_name, expected_last_name,
        "LAST ROW NAME MISMATCH: expected '{expected_last_name}', got '{restored_last_name}'"
    );

    println!("  ALL DATA VERIFIED - count, sum, and sample rows match!");

    // Cleanup
    let _ = ch_query(&ch_url, "DROP DATABASE IF EXISTS restored_db");
    let _ = ch_query(&ch_url, "DROP DATABASE IF EXISTS testdb");

    println!("PASSED: Snapshot restore with concrete data validation works!");
}

/// Recursively copy a directory.
fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            let _ = fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

/// Tests incremental backup with CONCRETE DATA VALIDATION.
///
/// Verifies that after incremental backups, the restored data matches the original.
#[test]
fn incremental_backup_and_verify_data() {
    assert!(is_docker_available(), "Docker is required for tests");
    assert!(
        Command::new("aws").arg("--version").output().is_ok(),
        "aws CLI is required for tests"
    );

    let network_name = format!("chbk_incr_{}", std::process::id());
    let _ = Command::new("docker")
        .args(["network", "create", &network_name])
        .output();

    struct NetworkCleanup(String);
    impl Drop for NetworkCleanup {
        fn drop(&mut self) {
            let _ = Command::new("docker")
                .args(["network", "rm", &self.0])
                .output();
        }
    }
    let _network_cleanup = NetworkCleanup(network_name.clone());

    println!("Step 1: Starting MinIO...");
    let minio_container_name = "minio-incr";
    let minio = MinIO::default()
        .with_network(&network_name)
        .with_container_name(minio_container_name)
        .start()
        .expect("Failed to start MinIO");

    std::thread::sleep(Duration::from_secs(2));

    let minio_port = minio.get_host_port_ipv4(9000).expect("minio port");
    let minio_host_endpoint = format!("http://127.0.0.1:{minio_port}");
    let minio_internal_endpoint = format!("http://{minio_container_name}:9000");

    create_bucket(&minio_host_endpoint, BUCKET);
    println!("  MinIO ready at {minio_host_endpoint}");

    let config_tmp = TempDir::new().expect("tempdir for config");
    let ch_data_tmp = TempDir::new().expect("tempdir for clickhouse data");
    let restore_tmp = TempDir::new().expect("tempdir for restore");

    generate_ch_config(config_tmp.path(), &minio_internal_endpoint);

    println!("Step 2: Starting ClickHouse...");
    let ch_image = GenericImage::new("clickhouse/clickhouse-server", "latest")
        .with_wait_for(WaitFor::seconds(3))
        .with_exposed_port(8123.tcp())
        .with_network(&network_name)
        .with_env_var("CLICKHOUSE_USER", "default")
        .with_env_var("CLICKHOUSE_PASSWORD", "test")
        .with_mount(Mount::bind_mount(
            config_tmp.path().to_string_lossy().to_string(),
            "/etc/clickhouse-server/config.d",
        ))
        .with_mount(Mount::bind_mount(
            ch_data_tmp.path().to_string_lossy().to_string(),
            "/var/lib/clickhouse",
        ))
        .with_mount(Mount::bind_mount(
            restore_tmp.path().to_string_lossy().to_string(),
            "/restore",
        ));

    let clickhouse = ch_image.start().expect("Failed to start ClickHouse");
    let ch_port = clickhouse
        .get_host_port_ipv4(8123)
        .expect("clickhouse port");
    let ch_url = format!("http://127.0.0.1:{ch_port}");

    assert!(
        wait_for_clickhouse(&ch_url, 60),
        "ClickHouse did not become ready"
    );
    println!("  ClickHouse ready at {ch_url}");

    println!("Step 3: Creating initial test data (1000 rows)...");
    let _ = ch_query_expect(&ch_url, "CREATE DATABASE IF NOT EXISTS incrdb");
    let _ = ch_query_expect(
        &ch_url,
        "CREATE TABLE incrdb.numbers (n UInt64) ENGINE = MergeTree ORDER BY n",
    );
    let _ = ch_query_expect(
        &ch_url,
        "INSERT INTO incrdb.numbers SELECT number FROM numbers(1000)",
    );

    std::thread::sleep(Duration::from_secs(1));

    println!("Step 4: First backup...");
    let binary = build_binary();
    let backup_tmp = TempDir::new().expect("tempdir for backup");

    let run_backup = |extra_args: &[&str]| {
        let mut cmd = Command::new(&binary);
        let _ = cmd
            .args(["--only", "^incrdb\\."])
            .env("CH_URL", &ch_url)
            .env("CH_USER", CH_USER)
            .env("CH_PASSWORD", CH_PASSWORD)
            .env("CH_DATA_PATH", ch_data_tmp.path())
            .env("BACKUP_DIR", backup_tmp.path())
            .env("S3_ENDPOINT", &minio_host_endpoint)
            .env("S3_BUCKET", BUCKET)
            .env("S3_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
            .env("S3_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
            .env("S3_REGION", "us-east-1")
            .env("CH_SHARD", "01")
            .env("CH_REPLICA", "01");
        let _ = cmd.args(extra_args);
        cmd.output().expect("backup command failed")
    };

    let output = run_backup(&[]);
    assert!(
        output.status.success(),
        "First backup failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    println!("  First backup completed");

    println!("Step 5: Inserting more data (500 more rows)...");
    let _ = ch_query_expect(
        &ch_url,
        "INSERT INTO incrdb.numbers SELECT number + 1000 FROM numbers(500)",
    );
    std::thread::sleep(Duration::from_secs(1));

    // Capture expected values AFTER all inserts
    let expected_count = ch_query_expect(&ch_url, "SELECT count() FROM incrdb.numbers");
    let expected_sum = ch_query_expect(&ch_url, "SELECT sum(n) FROM incrdb.numbers");
    let expected_min = ch_query_expect(&ch_url, "SELECT min(n) FROM incrdb.numbers");
    let expected_max = ch_query_expect(&ch_url, "SELECT max(n) FROM incrdb.numbers");

    assert_eq!(expected_count, "1500");
    println!(
        "  Total: {expected_count} rows, sum={expected_sum}, min={expected_min}, max={expected_max}"
    );

    println!("Step 6: Incremental backup and named snapshot...");
    let output = run_backup(&["create-snapshot", "snap1"]);
    assert!(
        output.status.success(),
        "Create snapshot failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    println!("  Snapshot 'snap1' created");

    println!("Step 7: Restoring snapshot...");
    let restore_dir = restore_tmp.path().join("restored_incr");

    let output = Command::new(&binary)
        .args(["restore", "snap1", "--to", restore_dir.to_str().unwrap()])
        .env("BACKUP_DIR", backup_tmp.path())
        .env("S3_ENDPOINT", &minio_host_endpoint)
        .env("S3_BUCKET", BUCKET)
        .env("S3_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
        .env("S3_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
        .env("S3_REGION", "us-east-1")
        .output()
        .expect("restore failed");

    assert!(
        output.status.success(),
        "Restore failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("  Restore completed");

    println!("Step 8: Attaching restored data to ClickHouse...");

    // Create restore target
    let _ = ch_query_expect(&ch_url, "CREATE DATABASE IF NOT EXISTS restored_db");
    let _ = ch_query_expect(
        &ch_url,
        "CREATE TABLE restored_db.numbers (n UInt64) ENGINE = MergeTree ORDER BY n",
    );

    // Copy parts to detached folder
    let restored_parts_dir = restore_dir.join("data").join("incrdb").join("numbers");
    let parts: Vec<_> = fs::read_dir(&restored_parts_dir)
        .expect("read restored parts")
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .collect();

    println!("  Found {} part(s) to attach", parts.len());

    let detached_dir = ch_data_tmp
        .path()
        .join("data")
        .join("restored_db")
        .join("numbers")
        .join("detached");
    fs::create_dir_all(&detached_dir).expect("create detached dir");

    for part in &parts {
        let src = part.path();
        let dst = detached_dir.join(part.file_name());
        copy_dir_recursive(&src, &dst).expect("copy part");
        println!("  Copied part {}", part.file_name().to_string_lossy());
    }

    // Attach parts
    let _ = ch_query(
        &ch_url,
        "ALTER TABLE restored_db.numbers ATTACH PARTITION tuple()",
    );
    std::thread::sleep(Duration::from_secs(1));

    println!("Step 9: VERIFYING RESTORED DATA...");

    let restored_count = ch_query_expect(&ch_url, "SELECT count() FROM restored_db.numbers");
    let restored_sum = ch_query_expect(&ch_url, "SELECT sum(n) FROM restored_db.numbers");
    let restored_min = ch_query_expect(&ch_url, "SELECT min(n) FROM restored_db.numbers");
    let restored_max = ch_query_expect(&ch_url, "SELECT max(n) FROM restored_db.numbers");

    println!(
        "  Original: count={expected_count}, sum={expected_sum}, min={expected_min}, max={expected_max}"
    );
    println!(
        "  Restored: count={restored_count}, sum={restored_sum}, min={restored_min}, max={restored_max}"
    );

    assert_eq!(
        restored_count, expected_count,
        "COUNT MISMATCH: expected {expected_count}, got {restored_count}"
    );
    assert_eq!(
        restored_sum, expected_sum,
        "SUM MISMATCH: expected {expected_sum}, got {restored_sum}"
    );
    assert_eq!(
        restored_min, expected_min,
        "MIN MISMATCH: expected {expected_min}, got {restored_min}"
    );
    assert_eq!(
        restored_max, expected_max,
        "MAX MISMATCH: expected {expected_max}, got {restored_max}"
    );

    println!("  ALL DATA VERIFIED!");

    // Cleanup
    let _ = ch_query(&ch_url, "DROP DATABASE IF EXISTS restored_db");
    let _ = ch_query(&ch_url, "DROP DATABASE IF EXISTS incrdb");

    println!("PASSED: Incremental backup with concrete data validation works!");
}

/// Test multiple parts backup with CONCRETE DATA VALIDATION.
///
/// Verifies that data spread across multiple parts restores correctly.
#[test]
fn multiple_parts_backup_and_verify() {
    assert!(is_docker_available(), "Docker is required for tests");
    assert!(
        Command::new("aws").arg("--version").output().is_ok(),
        "aws CLI is required for tests"
    );

    let network_name = format!("chbk_parts_{}", std::process::id());
    let _ = Command::new("docker")
        .args(["network", "create", &network_name])
        .output();

    struct NetworkCleanup(String);
    impl Drop for NetworkCleanup {
        fn drop(&mut self) {
            let _ = Command::new("docker")
                .args(["network", "rm", &self.0])
                .output();
        }
    }
    let _network_cleanup = NetworkCleanup(network_name.clone());

    println!("Step 1: Starting MinIO...");
    let minio_container_name = "minio-parts";
    let minio = MinIO::default()
        .with_network(&network_name)
        .with_container_name(minio_container_name)
        .start()
        .expect("Failed to start MinIO");

    std::thread::sleep(Duration::from_secs(2));

    let minio_port = minio.get_host_port_ipv4(9000).expect("minio port");
    let minio_host_endpoint = format!("http://127.0.0.1:{minio_port}");
    let minio_internal_endpoint = format!("http://{minio_container_name}:9000");

    create_bucket(&minio_host_endpoint, BUCKET);
    println!("  MinIO ready at {minio_host_endpoint}");

    let config_tmp = TempDir::new().expect("tempdir for config");
    let ch_data_tmp = TempDir::new().expect("tempdir for clickhouse data");
    let restore_tmp = TempDir::new().expect("tempdir for restore");

    generate_ch_config(config_tmp.path(), &minio_internal_endpoint);

    println!("Step 2: Starting ClickHouse...");
    let ch_image = GenericImage::new("clickhouse/clickhouse-server", "latest")
        .with_wait_for(WaitFor::seconds(3))
        .with_exposed_port(8123.tcp())
        .with_network(&network_name)
        .with_env_var("CLICKHOUSE_USER", "default")
        .with_env_var("CLICKHOUSE_PASSWORD", "test")
        .with_mount(Mount::bind_mount(
            config_tmp.path().to_string_lossy().to_string(),
            "/etc/clickhouse-server/config.d",
        ))
        .with_mount(Mount::bind_mount(
            ch_data_tmp.path().to_string_lossy().to_string(),
            "/var/lib/clickhouse",
        ))
        .with_mount(Mount::bind_mount(
            restore_tmp.path().to_string_lossy().to_string(),
            "/restore",
        ));

    let clickhouse = ch_image.start().expect("Failed to start ClickHouse");
    let ch_port = clickhouse
        .get_host_port_ipv4(8123)
        .expect("clickhouse port");
    let ch_url = format!("http://127.0.0.1:{ch_port}");

    assert!(
        wait_for_clickhouse(&ch_url, 60),
        "ClickHouse did not become ready"
    );
    println!("  ClickHouse ready at {ch_url}");

    println!("Step 3: Creating table with multiple parts...");
    let _ = ch_query_expect(&ch_url, "CREATE DATABASE IF NOT EXISTS partsdb");
    let _ = ch_query_expect(
        &ch_url,
        "CREATE TABLE partsdb.test (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192",
    );
    // Disable merges to ensure we keep multiple parts for testing
    let _ = ch_query_expect(&ch_url, "SYSTEM STOP MERGES partsdb.test");

    // Insert 5 separate small batches to create 5 parts
    for i in 0..5 {
        let _ = ch_query_expect(
            &ch_url,
            &format!(
                "INSERT INTO partsdb.test SELECT number + {} FROM numbers(100)",
                i * 100
            ),
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    std::thread::sleep(Duration::from_secs(1));

    // Capture expected values
    let expected_count = ch_query_expect(&ch_url, "SELECT count() FROM partsdb.test");
    let expected_sum = ch_query_expect(&ch_url, "SELECT sum(id) FROM partsdb.test");
    let orig_parts = ch_query_expect(
        &ch_url,
        "SELECT count() FROM system.parts WHERE database = 'partsdb' AND table = 'test' AND active",
    );

    assert_eq!(expected_count, "500");
    println!("  Created {orig_parts} parts with {expected_count} rows, sum={expected_sum}");

    println!("Step 4: Running backup...");
    let binary = build_binary();
    let backup_tmp = TempDir::new().expect("tempdir for backup");

    let output = Command::new(&binary)
        .args(["--only", "^partsdb\\.", "create-snapshot", "parts_snap"])
        .env("CH_URL", &ch_url)
        .env("CH_USER", CH_USER)
        .env("CH_PASSWORD", CH_PASSWORD)
        .env("CH_DATA_PATH", ch_data_tmp.path())
        .env("BACKUP_DIR", backup_tmp.path())
        .env("S3_ENDPOINT", &minio_host_endpoint)
        .env("S3_BUCKET", BUCKET)
        .env("S3_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
        .env("S3_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
        .env("S3_REGION", "us-east-1")
        .env("CH_SHARD", "01")
        .env("CH_REPLICA", "01")
        .output()
        .expect("backup failed");

    assert!(
        output.status.success(),
        "Backup failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("  Backup completed");

    println!("Step 5: Restoring snapshot...");
    let restore_dir = restore_tmp.path().join("restored_parts");

    let output = Command::new(&binary)
        .args([
            "restore",
            "parts_snap",
            "--to",
            restore_dir.to_str().unwrap(),
        ])
        .env("BACKUP_DIR", backup_tmp.path())
        .env("S3_ENDPOINT", &minio_host_endpoint)
        .env("S3_BUCKET", BUCKET)
        .env("S3_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
        .env("S3_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
        .env("S3_REGION", "us-east-1")
        .output()
        .expect("restore failed");

    assert!(
        output.status.success(),
        "Restore failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("  Restore completed");

    println!("Step 6: Attaching all parts to ClickHouse...");

    let _ = ch_query_expect(&ch_url, "CREATE DATABASE IF NOT EXISTS restored_db");
    let _ = ch_query_expect(
        &ch_url,
        "CREATE TABLE restored_db.test (id UInt64) ENGINE = MergeTree ORDER BY id",
    );

    let restored_parts_dir = restore_dir.join("data").join("partsdb").join("test");
    let parts: Vec<_> = fs::read_dir(&restored_parts_dir)
        .expect("read restored parts")
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .collect();

    println!("  Found {} part(s) to attach", parts.len());
    assert!(
        parts.len() >= 2,
        "Expected at least 2 parts, got {}",
        parts.len()
    );

    let detached_dir = ch_data_tmp
        .path()
        .join("data")
        .join("restored_db")
        .join("test")
        .join("detached");
    fs::create_dir_all(&detached_dir).expect("create detached dir");

    for part in &parts {
        let src = part.path();
        let dst = detached_dir.join(part.file_name());
        copy_dir_recursive(&src, &dst).expect("copy part");
    }
    println!("  Copied all parts to detached/");

    // Attach all parts
    let _ = ch_query(
        &ch_url,
        "ALTER TABLE restored_db.test ATTACH PARTITION tuple()",
    );
    std::thread::sleep(Duration::from_secs(1));

    println!("Step 7: VERIFYING RESTORED DATA...");

    let restored_count = ch_query_expect(&ch_url, "SELECT count() FROM restored_db.test");
    let restored_sum = ch_query_expect(&ch_url, "SELECT sum(id) FROM restored_db.test");

    println!("  Original: count={expected_count}, sum={expected_sum}");
    println!("  Restored: count={restored_count}, sum={restored_sum}");

    assert_eq!(
        restored_count, expected_count,
        "COUNT MISMATCH: expected {expected_count}, got {restored_count}"
    );
    assert_eq!(
        restored_sum, expected_sum,
        "SUM MISMATCH: expected {expected_sum}, got {restored_sum}"
    );

    println!("  ALL DATA FROM ALL PARTS VERIFIED!");

    // Cleanup
    let _ = ch_query(&ch_url, "DROP DATABASE IF EXISTS restored_db");
    let _ = ch_query(&ch_url, "DROP DATABASE IF EXISTS partsdb");

    println!("PASSED: Multiple parts backup with concrete data validation works!");
}

/// Tests backup and restore of ReplicatedMergeTree tables with DDL verification.
///
/// Enables embedded ClickHouse Keeper on a single node, creates a
/// ReplicatedMergeTree table, backs up, restores, and asserts:
/// 1. The restored metadata SQL files contain `ReplicatedMergeTree` engine verbatim
/// 2. The restored data matches the original
#[test]
fn replicated_table_ddl_preserved() {
    assert!(is_docker_available(), "Docker is required for tests");
    assert!(
        Command::new("aws").arg("--version").output().is_ok(),
        "aws CLI is required for tests"
    );

    let network_name = format!("chbk_repl_{}", std::process::id());
    let _ = Command::new("docker")
        .args(["network", "create", &network_name])
        .output();

    struct NetworkCleanup(String);
    impl Drop for NetworkCleanup {
        fn drop(&mut self) {
            let _ = Command::new("docker")
                .args(["network", "rm", &self.0])
                .output();
        }
    }
    let _network_cleanup = NetworkCleanup(network_name.clone());

    println!("Step 1: Starting MinIO...");
    let minio_container_name = "minio-repl";
    let minio = MinIO::default()
        .with_network(&network_name)
        .with_container_name(minio_container_name)
        .start()
        .expect("Failed to start MinIO");

    std::thread::sleep(Duration::from_secs(2));

    let minio_port = minio.get_host_port_ipv4(9000).expect("minio port");
    let minio_host_endpoint = format!("http://127.0.0.1:{minio_port}");
    let minio_internal_endpoint = format!("http://{minio_container_name}:9000");

    create_bucket(&minio_host_endpoint, BUCKET);
    println!("  MinIO ready at {minio_host_endpoint}");

    let config_tmp = TempDir::new().expect("tempdir for config");
    let ch_data_tmp = TempDir::new().expect("tempdir for clickhouse data");
    let restore_tmp = TempDir::new().expect("tempdir for restore");

    generate_ch_config_with_keeper(config_tmp.path(), &minio_internal_endpoint);

    println!("Step 2: Starting ClickHouse with embedded Keeper...");
    let ch_image = GenericImage::new("clickhouse/clickhouse-server", "latest")
        .with_wait_for(WaitFor::seconds(5))
        .with_exposed_port(8123.tcp())
        .with_network(&network_name)
        .with_env_var("CLICKHOUSE_USER", "default")
        .with_env_var("CLICKHOUSE_PASSWORD", "test")
        .with_mount(Mount::bind_mount(
            config_tmp.path().to_string_lossy().to_string(),
            "/etc/clickhouse-server/config.d",
        ))
        .with_mount(Mount::bind_mount(
            ch_data_tmp.path().to_string_lossy().to_string(),
            "/var/lib/clickhouse",
        ))
        .with_mount(Mount::bind_mount(
            restore_tmp.path().to_string_lossy().to_string(),
            "/restore",
        ));

    let clickhouse = ch_image.start().expect("Failed to start ClickHouse");
    let ch_port = clickhouse
        .get_host_port_ipv4(8123)
        .expect("clickhouse port");
    let ch_url = format!("http://127.0.0.1:{ch_port}");

    assert!(
        wait_for_clickhouse(&ch_url, 60),
        "ClickHouse did not become ready"
    );
    let version = ch_query_expect(&ch_url, "SELECT version()");
    println!("  ClickHouse {version} ready at {ch_url}");

    // Verify Keeper is running
    let keeper_ok = ch_query(&ch_url, "SELECT * FROM system.zookeeper WHERE path = '/'");
    assert!(
        keeper_ok.is_ok(),
        "Embedded Keeper not available: {:?}",
        keeper_ok.err()
    );
    println!("  Embedded Keeper confirmed running");

    println!("Step 3: Creating ReplicatedMergeTree table...");
    let _ = ch_query_expect(&ch_url, "CREATE DATABASE IF NOT EXISTS repldb");
    let create_table_ddl = concat!(
        "CREATE TABLE repldb.events ",
        "(id UInt64, ts DateTime DEFAULT now(), payload String) ",
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/repldb/events', '{replica}') ",
        "ORDER BY id"
    );
    let _ = ch_query_expect(&ch_url, create_table_ddl);

    let _ = ch_query_expect(
        &ch_url,
        "INSERT INTO repldb.events (id, payload) SELECT number, concat('evt_', toString(number)) FROM numbers(200)",
    );
    std::thread::sleep(Duration::from_secs(1));

    let expected_count = ch_query_expect(&ch_url, "SELECT count() FROM repldb.events");
    let expected_sum = ch_query_expect(&ch_url, "SELECT sum(id) FROM repldb.events");
    assert_eq!(expected_count, "200");
    println!("  Created ReplicatedMergeTree table with {expected_count} rows, sum={expected_sum}");

    // Read the on-disk DDL so we can compare after restore
    let original_table_ddl_path = ch_data_tmp
        .path()
        .join("metadata")
        .join("repldb")
        .join("events.sql");
    let original_table_ddl = fs::read_to_string(&original_table_ddl_path)
        .expect("read original table DDL from CH data dir");
    assert!(
        original_table_ddl.contains("ReplicatedMergeTree"),
        "Original on-disk DDL should contain ReplicatedMergeTree, got: {original_table_ddl}"
    );
    println!(
        "  Original DDL on disk:\n    {}",
        original_table_ddl.trim().replace('\n', "\n    ")
    );

    println!("Step 4: Running backup...");
    let binary = build_binary();
    let backup_tmp = TempDir::new().expect("tempdir for backup");

    let output = Command::new(&binary)
        .args(["--only", "^repldb\\.", "create-snapshot", "repl_snap"])
        .env("CH_URL", &ch_url)
        .env("CH_USER", CH_USER)
        .env("CH_PASSWORD", CH_PASSWORD)
        .env("CH_DATA_PATH", ch_data_tmp.path())
        .env("BACKUP_DIR", backup_tmp.path())
        .env("S3_ENDPOINT", &minio_host_endpoint)
        .env("S3_BUCKET", BUCKET)
        .env("S3_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
        .env("S3_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
        .env("S3_REGION", "us-east-1")
        .env("CH_SHARD", "01")
        .env("CH_REPLICA", "01")
        .output()
        .expect("backup failed");

    assert!(
        output.status.success(),
        "Backup failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("  Backup completed");

    println!("Step 5: Restoring snapshot...");
    let restore_dir = restore_tmp.path().join("restored_repl");

    let output = Command::new(&binary)
        .args([
            "restore",
            "repl_snap",
            "--to",
            restore_dir.to_str().unwrap(),
        ])
        .env("BACKUP_DIR", backup_tmp.path())
        .env("S3_ENDPOINT", &minio_host_endpoint)
        .env("S3_BUCKET", BUCKET)
        .env("S3_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
        .env("S3_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
        .env("S3_REGION", "us-east-1")
        .output()
        .expect("restore failed");

    assert!(
        output.status.success(),
        "Restore failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("  Restore completed");

    println!("Step 6: Verifying restored DDL files...");

    let restored_db_ddl_path = restore_dir.join("metadata").join("repldb.sql");
    assert!(
        restored_db_ddl_path.exists(),
        "Restored database DDL not found at {restored_db_ddl_path:?}"
    );
    let restored_db_ddl = fs::read_to_string(&restored_db_ddl_path).expect("read restored db DDL");
    println!("  Restored DB DDL: {}", restored_db_ddl.trim());

    let restored_table_ddl_path = restore_dir
        .join("metadata")
        .join("repldb")
        .join("events.sql");
    assert!(
        restored_table_ddl_path.exists(),
        "Restored table DDL not found at {restored_table_ddl_path:?}"
    );
    let restored_table_ddl =
        fs::read_to_string(&restored_table_ddl_path).expect("read restored table DDL");
    println!(
        "  Restored table DDL:\n    {}",
        restored_table_ddl.trim().replace('\n', "\n    ")
    );

    assert_eq!(
        restored_table_ddl, original_table_ddl,
        "Restored table DDL does not match original byte-for-byte"
    );
    println!("  DDL preserved verbatim (byte-for-byte match)");

    println!("Step 7: Attaching restored data and verifying...");

    let _ = ch_query_expect(&ch_url, "CREATE DATABASE IF NOT EXISTS restored_db");
    let _ = ch_query_expect(
        &ch_url,
        concat!(
            "CREATE TABLE restored_db.events ",
            "(id UInt64, ts DateTime DEFAULT now(), payload String) ",
            "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/restored_db/events', '{replica}') ",
            "ORDER BY id"
        ),
    );

    let restored_parts_dir = restore_dir.join("data").join("repldb").join("events");
    let parts: Vec<_> = fs::read_dir(&restored_parts_dir)
        .expect("read restored parts")
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .collect();

    println!("  Found {} part(s) to attach", parts.len());
    assert!(!parts.is_empty(), "Should have at least one part");

    let detached_dir = ch_data_tmp
        .path()
        .join("data")
        .join("restored_db")
        .join("events")
        .join("detached");
    fs::create_dir_all(&detached_dir).expect("create detached dir");

    for part in &parts {
        let src = part.path();
        let dst = detached_dir.join(part.file_name());
        copy_dir_recursive(&src, &dst).expect("copy part to detached");
    }

    let _ = ch_query(
        &ch_url,
        "ALTER TABLE restored_db.events ATTACH PARTITION tuple()",
    );
    std::thread::sleep(Duration::from_secs(1));

    let restored_count = ch_query_expect(&ch_url, "SELECT count() FROM restored_db.events");
    let restored_sum = ch_query_expect(&ch_url, "SELECT sum(id) FROM restored_db.events");

    println!("  Original: count={expected_count}, sum={expected_sum}");
    println!("  Restored: count={restored_count}, sum={restored_sum}");

    assert_eq!(
        restored_count, expected_count,
        "COUNT MISMATCH: expected {expected_count}, got {restored_count}"
    );
    assert_eq!(
        restored_sum, expected_sum,
        "SUM MISMATCH: expected {expected_sum}, got {restored_sum}"
    );

    // Cleanup
    let _ = ch_query(&ch_url, "DROP DATABASE IF EXISTS restored_db");
    let _ = ch_query(&ch_url, "DROP DATABASE IF EXISTS repldb");

    println!("PASSED: ReplicatedMergeTree DDL preserved verbatim through backup/restore!");
}
