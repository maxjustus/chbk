#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
    )
)]

use snmalloc_rs::SnMalloc;

#[global_allocator]
static GLOBAL: SnMalloc = SnMalloc;

mod blob_hash;
mod clickhouse;
mod gc;
mod manifest;
mod part_zip;
mod parts;
mod retry;
mod snapshots;
mod storage;
mod tui;
mod upload;
mod util;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "chbk")]
#[command(about = "S3-native ClickHouse MergeTree part backup tool")]
struct Args {
    #[command(subcommand)]
    command: Option<Commands>,

    /// ClickHouse HTTP URL
    #[arg(
        long,
        env = "CH_URL",
        default_value = "http://localhost:8123",
        value_name = "URL"
    )]
    ch_url: String,

    /// ClickHouse username
    #[arg(long, env = "CH_USER", default_value = "default", value_name = "USER")]
    ch_user: String,

    /// ClickHouse password
    #[arg(
        long,
        env = "CH_PASSWORD",
        default_value = "",
        value_name = "PASSWORD",
        hide_env_values = true
    )]
    ch_password: String,

    /// Use `clickhouse local` instead of HTTP
    #[arg(long, env = "CH_USE_LOCAL", value_parser = clap::builder::FalseyValueParser::new())]
    use_clickhouse_local: bool,

    /// ClickHouse config XML for `clickhouse local` (defines Disks, etc.)
    #[arg(long, env = "CH_CONFIG_PATH", value_name = "PATH")]
    ch_config_path: Option<PathBuf>,

    /// Path to ClickHouse data directory
    #[arg(
        long,
        env = "CH_DATA_PATH",
        default_value = "/var/lib/clickhouse",
        value_name = "PATH"
    )]
    ch_data_path: String,

    /// Local staging/work directory; must share a filesystem with CH_DATA_PATH
    #[arg(
        long,
        env = "BACKUP_DIR",
        default_value = "./backup",
        value_name = "DIR"
    )]
    backup_dir: PathBuf,

    /// Regex to ignore tables (database.table). Use "none" to disable.
    #[arg(
        long,
        env = "CHBK_IGNORE",
        default_value = r"^system\.",
        value_name = "PATTERN"
    )]
    ignore: String,

    /// Regex to include tables (database.table). Combined with --ignore.
    #[arg(long, env = "CHBK_ONLY", value_name = "PATTERN")]
    only: Option<String>,

    /// S3 bucket name
    #[arg(long, env = "S3_BUCKET", value_name = "BUCKET")]
    s3_bucket: Option<String>,

    /// S3 region
    #[arg(long, env = "S3_REGION", value_name = "REGION")]
    s3_region: Option<String>,

    /// S3 endpoint (for S3-compatible services)
    #[arg(long, env = "S3_ENDPOINT", value_name = "ENDPOINT")]
    s3_endpoint: Option<String>,

    /// S3 access key ID
    #[arg(
        long,
        env = "S3_ACCESS_KEY_ID",
        value_name = "KEY",
        hide_env_values = true
    )]
    s3_access_key_id: Option<String>,

    /// S3 secret access key
    #[arg(
        long,
        env = "S3_SECRET_ACCESS_KEY",
        value_name = "SECRET",
        hide_env_values = true
    )]
    s3_secret_access_key: Option<String>,

    /// S3 prefix/path within bucket
    #[arg(long, env = "S3_PREFIX", value_name = "PREFIX")]
    s3_prefix: Option<String>,

    /// Max concurrent part uploads per multipart upload
    #[arg(long, env = "MULTIPART_PART_CONCURRENCY", default_value_t = storage::DEFAULT_MULTIPART_PART_CONCURRENCY, value_name = "N")]
    multipart_part_concurrency: usize,

    /// Max concurrent parts processed in parallel
    #[arg(long, env = "PART_CONCURRENCY", default_value_t = 8, value_name = "N")]
    part_concurrency: usize,

    /// Max concurrent deletions from storage
    #[arg(
        long,
        env = "DELETE_CONCURRENCY",
        default_value_t = 32,
        value_name = "N"
    )]
    delete_concurrency: usize,

    /// Minimum multipart chunk size in MB (min: 5 per AWS)
    #[arg(long, env = "UPLOAD_MIN_CHUNK_SIZE_MB", value_name = "MB")]
    upload_min_chunk_size: Option<u64>,

    /// Maximum multipart chunk size in MB (max: 5120 per AWS)
    #[arg(long, env = "UPLOAD_MAX_CHUNK_SIZE_MB", value_name = "MB")]
    upload_max_chunk_size: Option<u64>,

    /// Target number of parts for multipart uploads (lower = bigger chunks)
    #[arg(long, env = "UPLOAD_TARGET_PARTS", default_value_t = storage::DEFAULT_TARGET_PARTS, value_name = "N")]
    upload_target_parts: u64,

    /// Shard identity (auto-detected from system.macros if not set)
    #[arg(long, env = "CH_SHARD", value_name = "SHARD")]
    shard: Option<String>,

    /// Replica identity (auto-detected from system.macros if not set)
    #[arg(long, env = "CH_REPLICA", value_name = "REPLICA")]
    replica: Option<String>,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Create a named snapshot
    CreateSnapshot {
        /// Name for the snapshot
        name: String,
    },
    /// Restore a snapshot to a local ClickHouse data directory
    Restore {
        /// Snapshot name to restore
        name: String,
        /// Output data directory path
        #[arg(long)]
        to: PathBuf,
        /// Restore only this table (format: db.table)
        #[arg(long, value_name = "DB.TABLE")]
        table: Option<String>,
        /// Overwrite output directory if it already exists
        #[arg(long)]
        force: bool,
        /// Download concurrency for blob files
        #[arg(long, default_value = "32")]
        download_concurrency: usize,
        /// Require snapshot to match this shard (safety guard)
        #[arg(long)]
        shard: Option<String>,
        /// Require snapshot to match this replica (safety guard)
        #[arg(long)]
        replica: Option<String>,
        /// Skip tables that already exist in the target ClickHouse (queries CH_URL)
        #[arg(long)]
        skip_existing: bool,
        /// Attach parts to running ClickHouse (writes to detached/, runs ATTACH PART)
        #[arg(long)]
        attach: bool,
    },
    /// Delete snapshot and its exclusive blobs
    RmSnapshot {
        /// Snapshot name to delete
        name: String,
        /// Show what would be deleted without actually deleting
        #[arg(long)]
        dry_run: bool,
    },
    /// Scan all blobs and delete those unused by any snapshot manifest
    GcAll {
        /// Show what would be deleted without actually deleting
        #[arg(long)]
        dry_run: bool,
        /// Number of shard prefixes (00-ff) to scan in parallel
        #[arg(long, default_value = "16")]
        shard_concurrency: usize,
        /// Grace period in hours — blobs younger than this are preserved to
        /// protect in-flight backups (replaces the legacy .active/ marker).
        #[arg(long, default_value = "6", value_name = "HOURS")]
        grace_period_hours: u64,
    },
    /// List all snapshots
    ListSnapshots {
        /// Filter by shard
        #[arg(long)]
        shard: Option<String>,
        /// Filter by replica
        #[arg(long)]
        replica: Option<String>,
    },
    /// Print an example .env file to stdout
    GenerateEnv,
    /// Prune old live_* snapshots with tiered retention
    GcLive {
        /// Keep all snapshots within this duration (e.g., 1440m, 24h, 1d)
        #[arg(long)]
        retain_all: String,
        /// Keep one snapshot per day for this duration (e.g., 90d, 12w, 3M)
        #[arg(long)]
        retain_daily: Option<String>,
        /// Show what would be deleted without actually deleting
        #[arg(long)]
        dry_run: bool,
        /// Override current timestamp (unix seconds) for testing/what-if scenarios
        #[arg(long)]
        now: Option<i64>,
    },
}

/// Parse duration string with unit suffix (e.g., "1440m", "24h", "90d", "3M")
#[allow(clippy::indexing_slicing)] // string slicing guarded by suffix/length checks
fn parse_duration_minutes(s: &str) -> Result<u64> {
    use anyhow::{Context, bail};

    let s = s.trim();
    if s.is_empty() {
        bail!("Empty duration string");
    }

    // Handle capital M for months specially (to not conflict with 'm' for minutes)
    let (num_str, unit) = if let Some(stripped) = s.strip_suffix('M') {
        (stripped, 'M')
    } else if let Some(unit) = s.chars().last().filter(|c| c.is_alphabetic()) {
        (&s[..s.len() - unit.len_utf8()], unit)
    } else {
        bail!("Duration must have unit suffix (m/h/d/w/M): {s}");
    };

    let num: u64 = num_str
        .parse()
        .with_context(|| format!("Invalid number in duration: {s}"))?;

    let minutes = match unit {
        'm' => num,
        'h' => num * 60,
        'd' => num * 60 * 24,
        'w' => num * 60 * 24 * 7,
        'M' => num * 60 * 24 * 30, // Approximate month (30 days)
        _ => bail!(
            "Invalid unit '{unit}'. Use m (minutes), h (hours), d (days), w (weeks), M (months)"
        ),
    };

    Ok(minutes)
}

#[derive(Debug)]
pub struct Config {
    pub ch_url: String,
    pub ch_user: String,
    pub ch_password: String,
    pub ch_data_path: String,
    pub ch_config_path: Option<PathBuf>,
    pub use_clickhouse_local: bool,
    pub output_dir: PathBuf,
    pub only_tables_pattern: Option<String>,
    pub ignore_tables_pattern: Option<String>,
    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
    pub s3_endpoint: Option<String>,
    pub s3_access_key_id: Option<String>,
    pub s3_secret_access_key: Option<String>,
    pub s3_prefix: Option<String>,
    pub multipart_part_concurrency: usize,
    pub part_concurrency: usize,
    pub delete_concurrency: usize,
    pub upload_min_chunk_size: u64,
    pub upload_max_chunk_size: u64,
    pub upload_target_parts: u64,
    pub shard: Option<String>,
    pub replica: Option<String>,
}

fn normalize_pattern(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("none") {
        None
    } else {
        Some(trimmed.to_string())
    }
}

impl Config {
    fn from_args(args: Args) -> Self {
        Self {
            ignore_tables_pattern: normalize_pattern(&args.ignore),
            only_tables_pattern: args.only.as_deref().and_then(normalize_pattern),
            upload_min_chunk_size: args
                .upload_min_chunk_size
                .map_or(storage::DEFAULT_MIN_PART_SIZE, |mb| mb * 1024 * 1024),
            upload_max_chunk_size: args
                .upload_max_chunk_size
                .map_or(storage::DEFAULT_MAX_PART_SIZE, |mb| mb * 1024 * 1024),
            ch_url: args.ch_url,
            ch_user: args.ch_user,
            ch_password: args.ch_password,
            ch_data_path: args.ch_data_path,
            ch_config_path: args.ch_config_path,
            use_clickhouse_local: args.use_clickhouse_local,
            output_dir: args.backup_dir,
            s3_bucket: args.s3_bucket,
            s3_region: args.s3_region,
            s3_endpoint: args.s3_endpoint,
            s3_access_key_id: args.s3_access_key_id,
            s3_secret_access_key: args.s3_secret_access_key,
            s3_prefix: args.s3_prefix,
            multipart_part_concurrency: args.multipart_part_concurrency,
            part_concurrency: args.part_concurrency,
            delete_concurrency: args.delete_concurrency,
            upload_target_parts: args.upload_target_parts,
            shard: args.shard,
            replica: args.replica,
        }
    }
}

// With lock-free backup and a grace period protecting in-flight uploads,
// an abrupt exit is safe.

fn main() -> Result<()> {
    std::panic::set_hook(Box::new(|info| {
        let bt = std::backtrace::Backtrace::force_capture();
        let msg = format!("=== PANIC ===\n{info}\n\nBacktrace:\n{bt}\n=============\n");
        eprintln!("\n{msg}");
        if matches!(std::fs::write("panic.log", &msg), Ok(())) {
            eprintln!("Full panic details written to ./panic.log");
        }
    }));

    let _ = dotenvy::dotenv();

    async_main()
}

// Dispatch tree covers every CLI command, which naturally pushes the aggregate
// async state past the nursery threshold. No action we take here improves the
// binary; the lint is pragmatic.
#[allow(clippy::large_stack_frames)]
#[tokio::main]
async fn async_main() -> Result<()> {
    #[cfg(feature = "tokio-console")]
    console_subscriber::init();

    let mut args = Args::parse();
    let command = args.command.take();
    let cfg = Config::from_args(args);

    match command {
        Some(Commands::GenerateEnv) => {
            print!("{}", include_str!("../chbk.example.env"));
            return Ok(());
        }
        Some(Commands::CreateSnapshot { name }) => {
            let snapshot_name = snapshots::create_snapshot(&cfg, Some(&name)).await?;
            println!("Snapshot created: {snapshot_name}");
            return Ok(());
        }
        Some(Commands::Restore {
            name,
            to,
            table,
            force,
            download_concurrency,
            shard: restore_shard,
            replica: restore_replica,
            skip_existing,
            attach,
        }) => {
            let table_filter: Option<(String, String)> = match &table {
                None => None,
                Some(t) => {
                    let (db, tbl) = t
                        .split_once('.')
                        .filter(|(db, tbl)| !db.is_empty() && !tbl.is_empty())
                        .ok_or_else(|| {
                            anyhow::anyhow!("--table must be in format db.table, got: {t}")
                        })?;
                    Some((db.to_string(), tbl.to_string()))
                }
            };

            if to.exists() && to.read_dir()?.next().is_some() && !force {
                anyhow::bail!("'{}' is not empty. Use --force to overwrite.", to.display());
            }

            let exclude_tables = if skip_existing {
                let existing = clickhouse::query_existing_tables(&cfg).await?;
                println!(
                    "Found {} existing tables in target ClickHouse",
                    existing.len()
                );
                Some(existing)
            } else {
                None
            };

            let (storage, _) = upload::init_storage(&cfg)?;
            let filter_ref = table_filter
                .as_ref()
                .map(|(db, tbl)| (db.as_str(), tbl.as_str()));
            let attach_cfg = if attach { Some(&cfg) } else { None };
            snapshots::restore_snapshot(
                storage.as_ref(),
                &name,
                &to,
                download_concurrency,
                filter_ref,
                restore_shard.as_deref(),
                restore_replica.as_deref(),
                exclude_tables.as_ref(),
                attach_cfg,
            )
            .await?;

            println!("Restored snapshot '{}' to {}", name, to.display());
            if table_filter.is_some() && !attach {
                println!("Tip: query with: clickhouse-local --path {}", to.display());
            }
            return Ok(());
        }
        Some(Commands::RmSnapshot { name, dry_run }) => {
            let (storage, _) = upload::init_storage(&cfg)?;
            gc::gc_snapshot(&cfg, &name, dry_run, storage.as_ref()).await?;
            return Ok(());
        }
        Some(Commands::GcAll {
            dry_run,
            shard_concurrency,
            grace_period_hours,
        }) => {
            let (storage, _) = upload::init_storage(&cfg)?;
            gc::gc_all_blobs(
                &cfg,
                dry_run,
                shard_concurrency,
                grace_period_hours,
                storage.as_ref(),
            )
            .await?;
            return Ok(());
        }
        Some(Commands::GcLive {
            retain_all,
            retain_daily,
            dry_run,
            now,
        }) => {
            let (storage, _) = upload::init_storage(&cfg)?;
            let retain_all_minutes = parse_duration_minutes(&retain_all)?;
            let retain_daily_minutes = retain_daily
                .as_ref()
                .map(|s| parse_duration_minutes(s))
                .transpose()?;

            let now = now.unwrap_or_else(|| chrono::Utc::now().timestamp());
            gc::prune_old_live_snapshots(
                &cfg,
                retain_all_minutes,
                retain_daily_minutes,
                now,
                dry_run,
                storage.as_ref(),
            )
            .await?;
            return Ok(());
        }
        Some(Commands::ListSnapshots {
            shard: filter_shard,
            replica: filter_replica,
        }) => {
            let (storage, _) = upload::init_storage(&cfg)?;
            let mut manifests = manifest::read_all_manifests(storage.as_ref(), 32).await?;

            if let Some(s) = &filter_shard {
                manifests.retain(|m| m.shard == *s);
            }
            if let Some(r) = &filter_replica {
                manifests.retain(|m| m.replica == *r);
            }
            manifests.sort_by_key(|m| m.timestamp);

            for m in &manifests {
                let ts = chrono::DateTime::from_timestamp(m.timestamp, 0).map_or_else(
                    || m.timestamp.to_string(),
                    |dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                );
                let table_count = m
                    .parts
                    .iter()
                    .map(|p| (p.database.as_str(), p.table_name.as_str()))
                    .collect::<std::collections::BTreeSet<_>>()
                    .len();
                println!(
                    "{}  {}  shard={}  replica={}  tables={}",
                    m.name,
                    ts,
                    if m.shard.is_empty() { "-" } else { &m.shard },
                    if m.replica.is_empty() {
                        "-"
                    } else {
                        &m.replica
                    },
                    table_count,
                );
            }
            return Ok(());
        }
        None => {}
    }

    // Default command: create an auto-named snapshot.
    let snapshot_name = snapshots::create_snapshot(&cfg, None).await?;
    println!("Snapshot created: {snapshot_name}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Args, Config, normalize_pattern};

    #[test]
    fn normalize_pattern_handles_trim_and_none() {
        assert_eq!(normalize_pattern("  foo  "), Some("foo".into()));
        assert_eq!(normalize_pattern("none"), None);
        assert_eq!(normalize_pattern(""), None);
    }

    fn test_args() -> Args {
        Args {
            command: None,
            ch_url: "http://localhost:8123".into(),
            ch_user: "default".into(),
            ch_password: String::new(),
            use_clickhouse_local: false,
            ch_config_path: None,
            ch_data_path: "/var/lib/clickhouse".into(),
            backup_dir: "./backup".into(),
            ignore: r"^system\.".into(),
            only: None,
            s3_bucket: None,
            s3_region: None,
            s3_endpoint: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_prefix: None,
            multipart_part_concurrency: 16,
            part_concurrency: 8,
            delete_concurrency: 32,
            upload_min_chunk_size: None,
            upload_max_chunk_size: None,
            upload_target_parts: 128,
            shard: None,
            replica: None,
        }
    }

    #[test]
    fn only_and_ignore_can_be_combined() {
        let cfg = Config::from_args(Args {
            ignore: "bar".into(),
            only: Some("foo".into()),
            ..test_args()
        });
        assert_eq!(cfg.only_tables_pattern.as_deref(), Some("foo"));
        assert_eq!(cfg.ignore_tables_pattern.as_deref(), Some("bar"));

        let cfg2 = Config::from_args(Args {
            only: Some("foo".into()),
            ..test_args()
        });
        assert_eq!(cfg2.only_tables_pattern.as_deref(), Some("foo"));
        assert_eq!(cfg2.ignore_tables_pattern.as_deref(), Some(r"^system\."));
    }

    #[test]
    fn ignore_none_disables_default() {
        let cfg = Config::from_args(Args {
            ignore: "none".into(),
            ..test_args()
        });
        assert_eq!(cfg.ignore_tables_pattern, None);
    }

    // Note: upload tests removed - they required local storage mode.
    // S3-only upload tests should use integration test with real S3/MinIO.

    #[test]
    fn parse_duration_minutes_basic_units() {
        use super::parse_duration_minutes;

        // Minutes
        assert_eq!(parse_duration_minutes("1m").unwrap(), 1);
        assert_eq!(parse_duration_minutes("1440m").unwrap(), 1440);

        // Hours
        assert_eq!(parse_duration_minutes("1h").unwrap(), 60);
        assert_eq!(parse_duration_minutes("24h").unwrap(), 1440);

        // Days
        assert_eq!(parse_duration_minutes("1d").unwrap(), 1440);
        assert_eq!(parse_duration_minutes("90d").unwrap(), 90 * 1440);

        // Weeks
        assert_eq!(parse_duration_minutes("1w").unwrap(), 7 * 1440);
        assert_eq!(parse_duration_minutes("12w").unwrap(), 12 * 7 * 1440);

        // Months (30 days)
        assert_eq!(parse_duration_minutes("1M").unwrap(), 30 * 1440);
        assert_eq!(parse_duration_minutes("3M").unwrap(), 3 * 30 * 1440);
    }

    #[test]
    fn parse_duration_minutes_whitespace() {
        use super::parse_duration_minutes;

        assert_eq!(parse_duration_minutes("  24h  ").unwrap(), 1440);
        assert_eq!(parse_duration_minutes("\t1d\n").unwrap(), 1440);
    }

    #[test]
    fn parse_duration_minutes_errors() {
        use super::parse_duration_minutes;

        // No unit
        assert!(parse_duration_minutes("1440").is_err());

        // Empty
        assert!(parse_duration_minutes("").is_err());
        assert!(parse_duration_minutes("   ").is_err());

        // Invalid unit
        assert!(parse_duration_minutes("1x").is_err());
        assert!(parse_duration_minutes("1s").is_err()); // seconds not supported

        // Invalid number
        assert!(parse_duration_minutes("abch").is_err());
        assert!(parse_duration_minutes("-1h").is_err());
    }
}
