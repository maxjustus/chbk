use crate::Config;
use crate::parts::PartInfo;
use anyhow::{Context, Result, bail};
use std::collections::{HashMap, HashSet};
use std::process::Command;

fn find_clickhouse_local_bin() -> Result<(String, Vec<String>)> {
    // Prefer `clickhouse local` if `clickhouse` exists, else fallback to `clickhouse-local`
    let which = |bin: &str| Command::new("which").arg(bin).output();
    if let Ok(out) = which("clickhouse")
        && out.status.success()
    {
        return Ok(("clickhouse".to_string(), vec!["local".to_string()]));
    }
    if let Ok(out) = which("clickhouse-local")
        && out.status.success()
    {
        return Ok(("clickhouse-local".to_string(), vec![]));
    }
    bail!("Could not find clickhouse or clickhouse-local in PATH")
}

fn run_sql_via_clickhouse_local(cfg: &Config, sql: &str) -> Result<String> {
    let (bin, mut args) = find_clickhouse_local_bin()?;
    args.push("--path".into());
    args.push(cfg.ch_data_path.clone());
    if let Some(conf) = &cfg.ch_config_path {
        args.push("--config-file".into());
        args.push(conf.display().to_string());
    }
    args.push("--query".into());
    args.push(sql.to_string());

    let out = Command::new(bin).args(&args).output()?;
    if !out.status.success() {
        bail!(
            "clickhouse local failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

/// Execute a query using either clickhouse-local or HTTP API.
pub async fn run_query(cfg: &Config, sql: &str) -> Result<String> {
    if cfg.use_clickhouse_local {
        run_sql_via_clickhouse_local(cfg, sql)
    } else {
        let client = reqwest::Client::new();
        let resp = client
            .post(&cfg.ch_url)
            .basic_auth(&cfg.ch_user, Some(&cfg.ch_password))
            .body(sql.to_string())
            .send()
            .await?;
        Ok(resp.text().await?)
    }
}

/// Parse a u64 from a JSON value that may be a number or a quoted string.
fn parse_u64(v: &serde_json::Value) -> u64 {
    v.as_u64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(0)
}

/// Parse hash_of_all_files from JSON value.
/// Returns None if the value is null (can happen for parts being written or certain table engines).
fn parse_hash_of_all_files_hex(v: &serde_json::Value) -> Result<Option<String>> {
    let raw = match v {
        serde_json::Value::Null => return Ok(None),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => bail!(
            "hash_of_all_files has unexpected type: {}",
            serde_json::to_string(v).unwrap_or_else(|_| "<invalid>".to_string())
        ),
    };

    let s = raw.trim();
    if s.len() == 32 && s.chars().all(|c| c.is_ascii_hexdigit()) {
        return Ok(Some(s.to_ascii_lowercase()));
    }

    let n: u128 = s
        .parse()
        .with_context(|| format!("Failed to parse hash_of_all_files UInt128: {s}"))?;
    Ok(Some(format!("{n:032x}")))
}

// serde_json Value indexing returns Null for missing keys, never panics
#[allow(clippy::indexing_slicing)]
pub async fn query_active_parts(cfg: &Config) -> Result<Vec<PartInfo>> {
    // Build query text (respect include/ignore patterns)
    // --only and --ignore can now be used together: --only filters first, then --ignore filters the result
    let mut conditions = vec!["active = 1".to_string()];

    if let Some(pattern) = &cfg.only_tables_pattern {
        let mut esc = pattern.replace('\'', "''");
        if esc.is_empty() {
            esc = String::from(".*");
        }
        conditions.push(format!("match(database || '.' || table, '{esc}')"));
    }

    if let Some(pattern) = &cfg.ignore_tables_pattern {
        let mut esc = pattern.replace('\'', "''");
        if esc.is_empty() {
            esc = String::from(".*");
        }
        conditions.push(format!("NOT match(database || '.' || table, '{esc}')"));
    }

    let query = format!(
        r"
        SELECT database, table, name, path, hash_of_all_files,
               bytes_on_disk, rows
        FROM system.parts
        WHERE {}
        ORDER BY level ASC -- newest parts first so we're less likely to hit a part that's already been merged into a larger part by the time we try to stage (hard link) it
        FORMAT JSONEachRow
        ",
        conditions.join("\n            AND ")
    );

    let text = run_query(cfg, &query).await?;
    let mut parts = Vec::new();

    for line in text.lines() {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
            // Skip parts with null hash_of_all_files (can happen during writes or for certain engines)
            let Some(hash_of_all_files) = parse_hash_of_all_files_hex(&json["hash_of_all_files"])?
            else {
                let db = json["database"].as_str().unwrap_or("?");
                let table = json["table"].as_str().unwrap_or("?");
                let name = json["name"].as_str().unwrap_or("?");
                eprintln!("Warning: skipping part {db}.{table}/{name} with null hash_of_all_files");
                continue;
            };
            parts.push(PartInfo {
                database: json["database"].as_str().unwrap_or("").to_string(),
                table: json["table"].as_str().unwrap_or("").to_string(),
                name: json["name"].as_str().unwrap_or("").to_string(),
                path: json["path"].as_str().unwrap_or("").to_string(),
                hash_of_all_files,
                bytes_on_disk: parse_u64(&json["bytes_on_disk"]),
                rows_count: parse_u64(&json["rows"]),
            });
        }
    }

    Ok(parts)
}

/// Query ClickHouse system.macros and return as key-value map.
#[allow(clippy::indexing_slicing)] // serde_json Value indexing returns Null for missing keys
pub async fn query_macros(cfg: &Config) -> Result<HashMap<String, String>> {
    let query = "SELECT macro, substitution FROM system.macros FORMAT JSONEachRow";
    let text = run_query(cfg, query).await?;
    let mut macros = HashMap::new();

    for line in text.lines() {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(line)
            && let (Some(macro_name), Some(substitution)) =
                (json["macro"].as_str(), json["substitution"].as_str())
        {
            let _ = macros.insert(macro_name.to_string(), substitution.to_string());
        }
    }

    Ok(macros)
}

#[allow(clippy::indexing_slicing)]
pub async fn query_existing_tables(cfg: &Config) -> Result<HashSet<(String, String)>> {
    let query = r"
        SELECT database, name
        FROM system.tables
        WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
        FORMAT JSONEachRow
    ";
    let text = run_query(cfg, query).await?;
    let mut tables = HashSet::new();

    for line in text.lines() {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(line)
            && let (Some(db), Some(name)) = (json["database"].as_str(), json["name"].as_str())
        {
            let _ = tables.insert((db.to_string(), name.to_string()));
        }
    }

    Ok(tables)
}

pub async fn attach_part(cfg: &Config, database: &str, table: &str, part_name: &str) -> Result<()> {
    let query = format!(
        "ALTER TABLE `{}`.`{}` ATTACH PART '{}'",
        database.replace('`', "``"),
        table.replace('`', "``"),
        part_name.replace('\'', "''")
    );
    let result = run_query(cfg, &query).await?;
    if !result.trim().is_empty() {
        bail!("ATTACH PART failed: {}", result.trim());
    }
    Ok(())
}
