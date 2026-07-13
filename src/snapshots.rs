//! Snapshot creation and management (part-level CAS).
//!
//! This implementation treats each ClickHouse part as the unit of storage:
//! - `system.parts.hash_of_all_files` is used as the CAS key.
//! - Each part is serialized as a stored ZIP stream directly to S3; no local ZIP is created.
//! - Snapshot metadata lives in per-snapshot JSON.zst manifests on S3.

use crate::Config;
use crate::clickhouse::{query_active_parts, query_macros};
use crate::manifest::{self, Manifest, ManifestFile, ManifestPart};
use crate::parts::{PartInfo, table_map_from_parts};
use crate::storage::Storage;
use crate::upload::init_storage;
use crate::util::{blob_remote_key, format_bytes};
use anyhow::{Context, Result, bail};
use futures::stream::{self, StreamExt};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio_util::io::SyncIoBridge;
use uuid::Uuid;

const STAGING_DIR_NAME: &str = "staging_parts";

/// Print to stdout only when TUI is not active.
/// In TUI mode, ratatui owns the terminal and println would corrupt the display.
macro_rules! log_println {
    ($tui:expr, $($arg:tt)*) => {
        if !$tui {
            println!($($arg)*);
        }
    };
}

fn remove_dir_all_if_exists(dir: &Path) -> Result<()> {
    match fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("Failed to remove {}", dir.display())),
    }
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("Failed to remove {}", path.display())),
    }
}

fn cleanup_staged_part_sync(dir: &Path) -> Result<()> {
    remove_dir_all_if_exists(dir)?;
    remove_file_if_exists(&dir.with_extension("staged"))
}

fn cleanup_staging_dirs_sync(output_dir: &Path) -> Result<()> {
    let entries = match fs::read_dir(output_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| format!("Failed to read {}", output_dir.display()));
        }
    };
    let legacy_prefix = format!("{STAGING_DIR_NAME}-old-");
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name == STAGING_DIR_NAME || name.starts_with(&legacy_prefix) {
            remove_dir_all_if_exists(&entry.path())?;
        }
    }
    Ok(())
}

async fn cleanup_staged_part(dir: PathBuf) -> Result<()> {
    tokio::task::spawn_blocking(move || cleanup_staged_part_sync(&dir))
        .await
        .context("staged part cleanup task panicked")?
}

async fn cleanup_staged_parts(dirs: Vec<PathBuf>) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        use rayon::prelude::*;
        dirs.par_iter()
            .try_for_each(|dir| cleanup_staged_part_sync(dir))
    })
    .await
    .context("staged parts cleanup task panicked")?
}

#[derive(Debug)]
struct StagingCleanup {
    staging_root: PathBuf,
    cleaned: bool,
}

impl StagingCleanup {
    const fn new(staging_root: PathBuf) -> Self {
        Self {
            staging_root,
            cleaned: false,
        }
    }

    fn cleanup_now(&mut self) -> Result<()> {
        if self.cleaned {
            return Ok(());
        }

        remove_dir_all_if_exists(&self.staging_root)?;
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for StagingCleanup {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup_now() {
            eprintln!(
                "Warning: failed to remove staging directory {}: {error:#}",
                self.staging_root.display()
            );
        }
    }
}

/// Collect all files from a directory tree, storing them under the given prefix.
fn collect_directory_files(dir: &Path, prefix: &str) -> Result<Vec<ManifestFile>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }

    for entry in walkdir::WalkDir::new(dir)
        .min_depth(1)
        .into_iter()
        .filter_map(std::result::Result::ok)
    {
        if !entry.file_type().is_file() {
            continue;
        }
        let rel_path = entry
            .path()
            .strip_prefix(dir)
            .with_context(|| format!("Failed to strip prefix from {}", entry.path().display()))?;
        let content = fs::read(entry.path())?;
        out.push(ManifestFile::from_bytes(
            format!("{}/{}", prefix, rel_path.display()),
            &content,
        ));
    }
    Ok(out)
}

fn collect_metadata_files(
    tables: &BTreeMap<String, BTreeSet<String>>,
    ch_data_path: &Path,
) -> Result<Vec<ManifestFile>> {
    let mut out = Vec::new();
    let ch_metadata_dir = ch_data_path.join("metadata");

    for (db, table_set) in tables {
        let db_metadata_file = ch_metadata_dir.join(format!("{db}.sql"));
        let db_ddl = fs::read(&db_metadata_file)
            .with_context(|| format!("Failed to read {}", db_metadata_file.display()))?;
        out.push(ManifestFile::from_bytes(
            format!("metadata/{db}.sql"),
            &db_ddl,
        ));

        let db_metadata_dir = ch_metadata_dir.join(db);
        for table in table_set {
            // Internal backing tables for Materialized Views don't have standalone
            // metadata files — the MV's DDL handles recreation.
            if table.starts_with(".inner_id.") {
                continue;
            }
            let table_metadata_file = db_metadata_dir.join(format!("{table}.sql"));
            let table_ddl = fs::read(&table_metadata_file)
                .with_context(|| format!("Failed to read {}", table_metadata_file.display()))?;
            out.push(ManifestFile::from_bytes(
                format!("metadata/{db}/{table}.sql"),
                &table_ddl,
            ));
        }
    }

    // Collect user-defined functions (SQL definitions)
    out.extend(collect_directory_files(
        &ch_data_path.join("user_defined"),
        "user_defined",
    )?);

    // Collect user scripts (executable UDFs)
    out.extend(collect_directory_files(
        &ch_data_path.join("user_scripts"),
        "user_scripts",
    )?);

    Ok(out)
}

fn is_cross_device_link(err: &io::Error) -> bool {
    match err.raw_os_error() {
        #[cfg(unix)]
        Some(18) => true, // EXDEV
        _ => false,
    }
}

/// Stage a part directory and return the number of files staged.
fn stage_part_dir_sync(src: &Path, dst: &Path) -> Result<u32> {
    if !src.exists() {
        bail!("Part directory missing: {}", src.display());
    }

    // No marker inside the part directory (ClickHouse parts must remain pristine).
    // Use a sibling marker file in the shard directory instead.
    let marker = dst.with_extension("staged");
    if marker.exists() && dst.exists() {
        // Already staged — count files from the staged directory.
        let count = walkdir::WalkDir::new(dst)
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter(|e| e.file_type().is_file())
            .count() as u32;
        return Ok(count);
    }

    if dst.exists() {
        fs::remove_dir_all(dst).with_context(|| format!("Failed to remove {}", dst.display()))?;
    }
    if marker.exists() {
        let _ = fs::remove_file(&marker);
    }

    fs::create_dir_all(dst).with_context(|| format!("Failed to create {}", dst.display()))?;

    let mut file_count: u32 = 0;
    for entry in walkdir::WalkDir::new(src).follow_links(false) {
        let entry = entry?;
        let ft = entry.file_type();
        if ft.is_symlink() {
            bail!(
                "Symlinks are not supported in staged parts: {}",
                entry.path().display()
            );
        }

        let rel = entry
            .path()
            .strip_prefix(src)
            .with_context(|| format!("Failed to strip prefix {}", src.display()))?;
        if rel.as_os_str().is_empty() {
            continue;
        }
        let out_path = dst.join(rel);

        if ft.is_dir() {
            fs::create_dir_all(&out_path)
                .with_context(|| format!("Failed to create {}", out_path.display()))?;
            continue;
        }
        if !ft.is_file() {
            continue;
        }
        file_count += 1;

        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)?;
        }

        match fs::hard_link(entry.path(), &out_path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                let src_len = fs::metadata(entry.path())?.len();
                let dst_meta = fs::metadata(&out_path)?;
                if dst_meta.len() != src_len {
                    bail!(
                        "Staged file exists with different size: {} ({} vs {})",
                        out_path.display(),
                        dst_meta.len(),
                        src_len
                    );
                }
            }
            Err(e) if is_cross_device_link(&e) => {
                bail!(
                    "Cannot hardlink across filesystems: {} -> {}. \
                     BACKUP_DIR must be on the same filesystem as CH_DATA_PATH.",
                    entry.path().display(),
                    out_path.display()
                );
            }
            Err(e) => {
                return Err(e).with_context(|| format!("Failed to link {}", out_path.display()));
            }
        }
    }

    fs::write(&marker, b"ok").with_context(|| format!("Failed to write {}", marker.display()))?;
    Ok(file_count)
}

/// Resolve part path synchronously.
///
/// ClickHouse servers may report absolute paths in containers (e.g. /var/lib/clickhouse/...).
/// This maps them to the local `CH_DATA_PATH` by locating the "store" or "data" component.
fn resolve_part_path_sync(part: &PartInfo, ch_data_path: &Path) -> PathBuf {
    if part.path.starts_with('/') {
        let p = PathBuf::from(&part.path);
        if fs::metadata(&p).is_ok() {
            return p;
        }
        let components: Vec<_> = p
            .components()
            .map(|c| c.as_os_str().to_string_lossy())
            .collect();
        if let Some(idx) = components.iter().position(|c| c == "store" || c == "data") {
            // idx from position() is always a valid index
            #[allow(clippy::indexing_slicing)]
            let relative_part: PathBuf = components[idx..].iter().map(AsRef::as_ref).collect();
            return ch_data_path.join(relative_part);
        }
        p
    } else {
        let direct = ch_data_path.join(&part.path);
        if fs::metadata(&direct).is_ok() {
            return direct;
        }
        if !part.path.starts_with("data/") {
            let with_data = ch_data_path.join("data").join(&part.path);
            if fs::metadata(&with_data).is_ok() {
                return with_data;
            }
        }
        direct
    }
}

#[derive(Debug)]
struct StageOutcome {
    staged: HashMap<String, PathBuf>,
    missing_hashes: Vec<String>,
}

fn stage_parts_cas_sync(
    parts_by_hash: &HashMap<String, PartInfo>,
    ch_data_path: &Path,
    staging_root: &Path,
    progress: &Arc<crate::storage::UploadProgress>,
    event_tx: Option<&tokio::sync::mpsc::UnboundedSender<crate::tui::BackupEvent>>,
) -> StageOutcome {
    progress.set_staging_total(parts_by_hash.len() as u64);

    let parts: Vec<(String, PartInfo)> = parts_by_hash
        .iter()
        .map(|(h, p)| (h.clone(), p.clone()))
        .collect();

    let ch_data_path = ch_data_path.to_path_buf();
    let staging_root = staging_root.to_path_buf();

    let res: Vec<(String, Result<(PathBuf, u32)>)> = {
        use rayon::prelude::*;
        parts
            .par_iter()
            .map(|(hash, part)| {
                if let Some(tx) = event_tx {
                    let _ = tx.send(crate::tui::BackupEvent::PartStaging { hash: hash.clone() });
                }
                let src = resolve_part_path_sync(part, &ch_data_path);
                if !src.exists() {
                    progress.record_part_staged();
                    return (hash.clone(), Err(anyhow::anyhow!("missing")));
                }
                let shard = hash.get(0..2).unwrap_or("__");
                let dst = staging_root.join(shard).join(hash);
                let r = stage_part_dir_sync(&src, &dst).map(|fc| (dst, fc));
                progress.record_part_staged();
                if let Ok((_, fc)) = &r
                    && let Some(tx) = event_tx
                {
                    let _ = tx.send(crate::tui::BackupEvent::PartStaged {
                        hash: hash.clone(),
                        file_count: *fc,
                    });
                }
                (hash.clone(), r)
            })
            .collect()
    };

    let mut staged = HashMap::new();
    let mut missing_hashes = Vec::new();
    for (hash, r) in res {
        match r {
            Ok((dir, _file_count)) => {
                let _ = staged.insert(hash, dir);
            }
            Err(_) => {
                missing_hashes.push(hash);
            }
        }
    }

    StageOutcome {
        staged,
        missing_hashes,
    }
}

/// Threshold above which we HEAD-check S3 before uploading.
/// Catches cases where a previous run uploaded the blob but crashed before
/// writing its manifest, or where another replica already uploaded it.
const HEAD_CHECK_THRESHOLD: u64 = 512 * 1024; // 512 KB

/// Run parallel HEAD checks against S3 for hashes above the size threshold.
/// Returns a map of hash → S3 content-length for parts that already exist.
async fn head_check_existing(
    storage: &Storage,
    hashes_with_sizes: Vec<(String, u64)>,
    event_tx: &tokio::sync::mpsc::UnboundedSender<crate::tui::BackupEvent>,
) -> HashMap<String, u64> {
    let to_check: Vec<String> = hashes_with_sizes
        .into_iter()
        .filter(|(_, bytes)| *bytes >= HEAD_CHECK_THRESHOLD)
        .map(|(h, _)| h)
        .collect();

    if to_check.is_empty() {
        return HashMap::new();
    }

    let results: Vec<(String, Option<u64>)> = stream::iter(to_check)
        .map(|hash| {
            let storage = storage.clone();
            let etx = event_tx.clone();
            async move {
                let remote_key = blob_remote_key(&hash);
                let _ = etx.send(crate::tui::BackupEvent::PartHeadCheck { hash: hash.clone() });
                match storage.object_exists(&remote_key).await {
                    Ok(Some(size)) => {
                        let _ = etx.send(crate::tui::BackupEvent::PartHeadSkipped {
                            hash: hash.clone(),
                            size,
                        });
                        (hash, Some(size))
                    }
                    Ok(None) => (hash, None),
                    Err(e) => {
                        eprintln!("Warning: HEAD check failed for {hash}, will upload: {e}");
                        (hash, None)
                    }
                }
            }
        })
        .buffer_unordered(64)
        .collect()
        .await;

    results
        .into_iter()
        .filter_map(|(h, s)| s.map(|size| (h, size)))
        .collect()
}

async fn upload_part_archives(
    storage: Arc<Storage>,
    staged_dirs: HashMap<String, PathBuf>,
    part_concurrency: usize,
    progress: Arc<crate::storage::UploadProgress>,
    event_tx: tokio::sync::mpsc::UnboundedSender<crate::tui::BackupEvent>,
) -> Result<HashMap<String, u64>> {
    let part_concurrency = part_concurrency.max(1);

    progress.set_upload_total(staged_dirs.len() as u64);

    let results: Vec<Result<(String, u64)>> = stream::iter(staged_dirs)
        .map(|(hash, dir)| {
            let storage = Arc::clone(&storage);
            let progress = Arc::clone(&progress);
            let etx = event_tx.clone();
            async move {
                let remote_key = blob_remote_key(&hash);

                // Collect file list + estimate in blocking task.
                let (files, expected_size) = tokio::task::spawn_blocking({
                    let dir = dir.clone();
                    move || -> Result<(Vec<crate::part_zip::ZipFileEntry>, u64)> {
                        let files = crate::part_zip::collect_files(&dir)?;
                        let expected = crate::part_zip::estimate_zip_size(&files);
                        Ok((files, expected))
                    }
                })
                .await
                .context("collect_files spawn_blocking join error")??;

                progress.add_upload_total_bytes_est(expected_size);

                // Feed the ZIP byte stream into S3 as it is constructed.
                let (reader, writer) = tokio::io::duplex(1024 * 1024);
                let writer_task = tokio::task::spawn_blocking({
                    move || -> Result<u64> {
                        let w = SyncIoBridge::new(writer);
                        let mut w = io::BufWriter::with_capacity(4 * 1024 * 1024, w);
                        let written = crate::part_zip::write_zip(&mut w, &files)?;
                        w.flush()?;
                        Ok(written)
                    }
                });

                let _ = etx.send(crate::tui::BackupEvent::PartUploading {
                    hash: hash.clone(),
                });

                let upload_hash = hash.clone();
                let upload_etx = etx.clone();
                let on_progress: Arc<dyn Fn(u64, u64) + Send + Sync> =
                    Arc::new(move |bytes_so_far, total| {
                        let _ = upload_etx.send(crate::tui::BackupEvent::PartUploadProgress {
                            hash: upload_hash.clone(),
                            bytes_uploaded: bytes_so_far,
                            total,
                        });
                    });

                let upload_result = storage
                    .put_object_multipart_reader(
                        &remote_key,
                        reader,
                        expected_size,
                        Some(on_progress),
                    )
                    .await
                    .with_context(|| format!("Failed to upload {remote_key}"));
                let writer_result = writer_task
                    .await
                    .context("ZIP writer task panicked")
                    .and_then(|result| result);
                let uploaded = match (upload_result, writer_result) {
                    (Err(upload_error), _) => return Err(upload_error),
                    (Ok(_), Err(writer_error)) => return Err(writer_error),
                    (Ok(uploaded), Ok(written)) => {
                        if uploaded != written {
                            bail!(
                                "ZIP upload size mismatch for {hash}: uploaded {uploaded} vs written {written}"
                            );
                        }
                        uploaded
                    }
                };

                progress.record_upload_done();
                cleanup_staged_part(dir)
                    .await
                    .with_context(|| format!("Failed to clean up staged part {hash}"))?;

                let _ = etx.send(crate::tui::BackupEvent::PartDone {
                    hash: hash.clone(),
                    zip_size: uploaded,
                });

                Ok((hash, uploaded))
            }
        })
        .buffer_unordered(part_concurrency)
        .collect()
        .await;

    let mut sizes = HashMap::new();
    for r in results {
        let (hash, size) = r?;
        let _ = sizes.insert(hash, size);
    }
    Ok(sizes)
}

/// Resolve shard/replica identity from CLI flags, env vars, or ClickHouse system.macros.
async fn resolve_shard_replica(cfg: &Config) -> Result<(String, String)> {
    let mut shard = cfg.shard.clone();
    let mut replica = cfg.replica.clone();

    if (shard.is_none() || replica.is_none()) && !cfg.use_clickhouse_local {
        match query_macros(cfg).await {
            Ok(macros) => {
                if shard.is_none() {
                    shard = macros.get("shard").cloned();
                }
                if replica.is_none() {
                    replica = macros.get("replica").cloned();
                }
            }
            Err(e) => {
                eprintln!("Warning: failed to query system.macros: {e}");
            }
        }
    }

    let shard = shard.ok_or_else(|| {
        anyhow::anyhow!(
            "Could not determine shard identity. Set --shard or define 'shard' in ClickHouse system.macros"
        )
    })?;
    let replica = replica.ok_or_else(|| {
        anyhow::anyhow!(
            "Could not determine replica identity. Set --replica or define 'replica' in ClickHouse system.macros"
        )
    })?;

    Ok((shard, replica))
}

/// Load known blob hashes + sizes from the latest manifest for this shard/replica.
///
/// Uses the lexicographic max (which matches timestamp order since auto names
/// embed a sortable timestamp). Returns empty sets if no prior manifest exists.
///
/// Tradeoff: we only consult the LATEST manifest rather than the union of all
/// prior manifests. For <512KB blobs that existed in older snapshots but not
/// the latest, this forces a re-upload (harmless — CAS PUT is idempotent). For
/// ≥512KB blobs, the downstream HEAD check catches them. Revisit if dedup
/// telemetry shows meaningful waste.
async fn load_known_from_latest_manifest(
    storage: &Storage,
    shard: &str,
    replica: &str,
) -> Result<(HashSet<String>, HashMap<String, u64>)> {
    let prefix = format!("live_{shard}_{replica}_");
    let names = manifest::list_manifest_names(storage).await?;
    let Some(latest) = names.into_iter().filter(|n| n.starts_with(&prefix)).max() else {
        return Ok((HashSet::new(), HashMap::new()));
    };
    println!("Using latest manifest for known-hash lookup: {latest}");
    let m = manifest::read_manifest(storage, &latest).await?;
    let mut hashes = HashSet::with_capacity(m.parts.len());
    let mut sizes = HashMap::with_capacity(m.parts.len());
    for p in m.parts {
        let _ = hashes.insert(p.blob_hash.clone());
        let _ = sizes.insert(p.blob_hash, p.blob_size);
    }
    Ok((hashes, sizes))
}

fn parts_to_upload(
    parts: &[PartInfo],
    known_hashes: &HashSet<String>,
) -> (HashMap<String, PartInfo>, usize) {
    let mut seen = HashSet::new();
    let uploads = parts
        .iter()
        .filter(|part| {
            seen.insert(part.hash_of_all_files.clone())
                && !known_hashes.contains(&part.hash_of_all_files)
        })
        .map(|part| (part.hash_of_all_files.clone(), part.clone()))
        .collect();
    (uploads, seen.len())
}

pub async fn create_snapshot(cfg: &Config, name: Option<&str>) -> Result<String> {
    let now = chrono::Utc::now();
    let timestamp_secs = now.timestamp();

    println!("Initializing storage...");
    let (storage, progress) = init_storage(cfg)?;
    let storage_ref = storage.as_ref();

    // Query active parts and compute table map.
    println!("Querying active parts from ClickHouse...");
    let parts = query_active_parts(cfg).await?;
    println!("Found {} active parts", parts.len());

    // Resolve shard/replica identity (from CLI, env, or system.macros).
    let (shard, replica) = resolve_shard_replica(cfg).await?;
    println!("Identity: shard={shard}, replica={replica}");

    let snapshot_name = if let Some(n) = name {
        n.to_string()
    } else {
        manifest::build_auto_name(&shard, &replica, now, &Uuid::new_v4().to_string())
    };

    // GC's grace period (on S3 LastModified) protects in-flight blobs. No
    // marker write needed — freshly-uploaded blobs are too young to delete.

    let ch_data_path = PathBuf::from(&cfg.ch_data_path);

    // Known hashes from the latest manifest for this shard/replica.
    let (known_hashes, known_sizes_from_manifest) =
        load_known_from_latest_manifest(storage_ref, &shard, &replica).await?;
    let all_part_hashes: HashSet<String> =
        parts.iter().map(|p| p.hash_of_all_files.clone()).collect();
    println!(
        "{} of {} part hashes known from latest manifest",
        known_hashes.intersection(&all_part_hashes).count(),
        all_part_hashes.len()
    );

    // Spawn progress consumer (TUI or plain logger).
    let (event_tx, tui_handle, is_tui) =
        crate::tui::spawn_progress(&snapshot_name, Some(progress.clone()));
    // Suppress UploadProgress println logging when TUI owns the terminal.
    if is_tui {
        progress.suppress_logging();
    }
    let _ = event_tx.send(crate::tui::BackupEvent::PartsDiscovered {
        parts: parts
            .iter()
            .map(|p| crate::tui::PartSummary {
                hash: p.hash_of_all_files.clone(),
                database: p.database.clone(),
                table: p.table.clone(),
                part_name: p.name.clone(),
                bytes_on_disk: p.bytes_on_disk,
                rows_count: p.rows_count,
            })
            .collect(),
        known_hashes: known_hashes.clone(),
    });

    // Track sizes for newly uploaded blobs (existing sizes fetched later when needed).
    let mut uploaded_sizes: HashMap<String, u64> = HashMap::new();

    // Phase 1: stage ALL needed part directories into a local CAS tree.
    const MAX_STAGING_ITERATIONS: usize = 5;
    let staging_root = cfg.output_dir.join(STAGING_DIR_NAME);
    cleanup_staging_dirs_sync(&cfg.output_dir)?;
    fs::create_dir_all(&staging_root)?;
    let mut staging_cleanup = StagingCleanup::new(staging_root.clone());

    let mut staged_dirs: HashMap<String, PathBuf> = HashMap::new();
    let mut current_parts = parts;

    // Kick off HEAD checks for parts we think need uploading.
    // These run concurrently with staging since they're independent (S3 vs local disk).
    let head_check_hashes: Vec<(String, u64)> = {
        let mut seen = HashSet::new();
        current_parts
            .iter()
            .filter(|p| {
                seen.insert(p.hash_of_all_files.clone())
                    && !known_hashes.contains(&p.hash_of_all_files)
            })
            .map(|p| (p.hash_of_all_files.clone(), p.bytes_on_disk))
            .collect()
    };
    let head_check_handle = tokio::spawn({
        let storage = storage.as_ref().clone();
        let etx = event_tx.clone();
        async move { head_check_existing(&storage, head_check_hashes, &etx).await }
    });

    for iter in 1..=MAX_STAGING_ITERATIONS {
        let (need_upload, unique_part_count) = parts_to_upload(&current_parts, &known_hashes);
        let need_upload_count = need_upload.len();
        let need_stage: HashMap<String, PartInfo> = need_upload
            .into_iter()
            .filter(|(h, _)| !staged_dirs.contains_key(h))
            .collect();

        log_println!(
            is_tui,
            "Part archives: {} total, {} need upload ({} need staging)",
            unique_part_count,
            need_upload_count,
            need_stage.len()
        );

        if need_stage.is_empty() {
            break;
        }

        log_println!(
            is_tui,
            "Phase 1: staging part directories (pass {}/{})...",
            iter,
            MAX_STAGING_ITERATIONS
        );
        let stage = tokio::task::spawn_blocking({
            let need_stage = need_stage.clone();
            let ch_data_path = ch_data_path.clone();
            let staging_root = staging_root.clone();
            let progress = Arc::clone(&progress);
            let etx = event_tx.clone();
            move || {
                stage_parts_cas_sync(
                    &need_stage,
                    &ch_data_path,
                    &staging_root,
                    &progress,
                    Some(&etx),
                )
            }
        })
        .await
        .context("staging spawn_blocking join error")?;

        staged_dirs.extend(stage.staged);

        if stage.missing_hashes.is_empty() {
            break;
        }

        if iter == MAX_STAGING_ITERATIONS {
            bail!(
                "Some parts disappeared during staging after {} passes ({} missing)",
                MAX_STAGING_ITERATIONS,
                stage.missing_hashes.len()
            );
        }

        log_println!(
            is_tui,
            "Info: {} parts disappeared during staging, retrying with fresh parts list...",
            stage.missing_hashes.len()
        );
        current_parts = query_active_parts(cfg).await?;
    }

    // Collect HEAD check results (should be done by now — ran during staging).
    let head_skipped = head_check_handle
        .await
        .context("HEAD check task panicked")?;

    // Collect metadata files (DDL) for the manifest.
    // Tables can be dropped between the parts query and reading metadata files,
    // so retry with a fresh parts list on failure (same pattern as staging).
    let mut snapshot_files = None;
    for attempt in 1..=MAX_STAGING_ITERATIONS {
        let current_tables = table_map_from_parts(&current_parts);
        log_println!(is_tui, "Capturing metadata...");
        match collect_metadata_files(&current_tables, &ch_data_path) {
            Ok(files) => {
                snapshot_files = Some(files);
                break;
            }
            Err(e) => {
                if attempt == MAX_STAGING_ITERATIONS {
                    return Err(e.context("metadata collection failed after retries"));
                }
                log_println!(
                    is_tui,
                    "Warning: metadata collection failed ({}), re-querying parts...",
                    e
                );
                current_parts = query_active_parts(cfg).await?;
            }
        }
    }
    let Some(snapshot_files) = snapshot_files else {
        bail!("metadata collection did not complete");
    };

    // Phase 2: upload staged directories as streamed ZIPs.
    // Only upload hashes that are still required for this snapshot.
    let (need_upload_final, _) = parts_to_upload(&current_parts, &known_hashes);

    let mut upload_dirs = HashMap::new();
    let mut unused_dirs = Vec::new();
    for (hash, dir) in staged_dirs {
        if need_upload_final.contains_key(&hash) && !head_skipped.contains_key(&hash) {
            let _ = upload_dirs.insert(hash, dir);
        } else {
            unused_dirs.push(dir);
        }
    }
    for hash in need_upload_final.keys() {
        if !head_skipped.contains_key(hash) && !upload_dirs.contains_key(hash) {
            bail!("Need upload for {hash} but not staged");
        }
    }
    cleanup_staged_parts(unused_dirs).await?;

    if !head_skipped.is_empty() {
        log_println!(
            is_tui,
            "HEAD check: {} parts already in S3, {} to upload",
            head_skipped.len(),
            upload_dirs.len()
        );
    }
    log_println!(
        is_tui,
        "Phase 2: uploading {} part archives...",
        upload_dirs.len()
    );
    let new_uploads = upload_part_archives(
        storage.clone(),
        upload_dirs,
        cfg.part_concurrency,
        progress.clone(),
        event_tx.clone(),
    )
    .await?;

    staging_cleanup.cleanup_now()?;

    // Track newly uploaded sizes + head-skipped sizes.
    for (hash, size) in new_uploads {
        let _ = uploaded_sizes.insert(hash, size);
    }
    for (hash, size) in head_skipped {
        let _ = uploaded_sizes.insert(hash, size);
    }

    // Resolve a size for every hash referenced by this snapshot. Sources
    // are disjoint: uploaded ⊕ head-skipped ⊕ known-from-latest-manifest.
    // If any hash is missing here we have a bug, not a user error.
    let mut known_sizes: HashMap<String, u64> = known_sizes_from_manifest;
    for (h, s) in &uploaded_sizes {
        if let Some(prev) = known_sizes.insert(h.clone(), *s)
            && prev != *s
        {
            bail!(
                "Blob size mismatch for {}: manifest had {} but uploaded {}",
                h,
                format_bytes(prev),
                format_bytes(*s)
            );
        }
    }

    let manifest_parts: Vec<ManifestPart> = current_parts
        .iter()
        .map(|p| {
            let hash = p.hash_of_all_files.clone();
            let size = *known_sizes
                .get(&hash)
                .ok_or_else(|| anyhow::anyhow!("Missing size for {hash}"))?;
            Ok(ManifestPart {
                database: p.database.clone(),
                table_name: p.table.clone(),
                part_name: p.name.clone(),
                blob_hash: hash,
                blob_size: size,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let hostname = gethostname::gethostname().to_string_lossy().to_string();
    let manifest_rec = Manifest {
        name: snapshot_name.clone(),
        timestamp: timestamp_secs,
        shard: shard.clone(),
        replica: replica.clone(),
        created_by: hostname,
        parts: manifest_parts,
        files: snapshot_files,
    };

    log_println!(
        is_tui,
        "Writing manifest ({} parts, {} files)...",
        manifest_rec.parts.len(),
        manifest_rec.files.len()
    );
    manifest::write_manifest(storage_ref, &manifest_rec).await?;
    log_println!(is_tui, "Wrote manifest: {snapshot_name}");

    let _ = event_tx.send(crate::tui::BackupEvent::BackupComplete {
        snapshot_name: snapshot_name.clone(),
    });
    drop(event_tx);
    let _ = tui_handle.await;

    Ok(snapshot_name)
}

/// Restore a snapshot from its manifest to a local ClickHouse data directory.
///
/// Downloads every referenced part ZIP blob and extracts it into
/// `data/{db}/{table}/{part}/`. Metadata (DDL) from the manifest is written
/// into `metadata/`. Validates optional shard/replica expectations against
/// the manifest header.
#[allow(clippy::too_many_arguments)]
pub async fn restore_snapshot(
    storage: &Storage,
    snapshot_name: &str,
    output_dir: &Path,
    download_concurrency: usize,
    table_filter: Option<(&str, &str)>,
    expected_shard: Option<&str>,
    expected_replica: Option<&str>,
    exclude_tables: Option<&HashSet<(String, String)>>,
    attach_cfg: Option<&Config>,
) -> Result<()> {
    let manifest_rec = manifest::read_manifest(storage, snapshot_name)
        .await
        .with_context(|| format!("Reading manifest for '{snapshot_name}'"))?;

    if let Some(expected) = expected_shard
        && manifest_rec.shard != expected
    {
        bail!(
            "Shard mismatch: snapshot '{snapshot_name}' has shard='{}', expected '{expected}'",
            manifest_rec.shard
        );
    }
    if let Some(expected) = expected_replica
        && manifest_rec.replica != expected
    {
        bail!(
            "Replica mismatch: snapshot '{snapshot_name}' has replica='{}', expected '{expected}'",
            manifest_rec.replica
        );
    }

    let FilteredRestoreItems {
        parts,
        files,
        skipped,
    } = filter_restore_items(
        manifest_rec.parts,
        manifest_rec.files,
        table_filter,
        exclude_tables,
    )?;

    for table in &skipped {
        println!("Skipping existing table: {}.{}", table.0, table.1);
    }

    println!(
        "Restoring snapshot '{snapshot_name}': {} metadata files, {} parts",
        files.len(),
        parts.len()
    );

    fs::create_dir_all(output_dir).with_context(|| format!("create {}", output_dir.display()))?;

    for file in &files {
        let content = file.decode_content()?;
        let path = output_dir.join(&file.path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, &content)?;
    }
    println!("  Wrote {} metadata files", files.len());

    if parts.is_empty() {
        println!("  No parts to restore");
        return Ok(());
    }

    let total_parts = parts.len() as u64;
    let total_bytes: u64 = parts.iter().map(|p| p.blob_size).sum();
    let attach_mode = attach_cfg.is_some();
    println!(
        "  Downloading {total_parts} part archives ({}){}",
        format_bytes(total_bytes),
        if attach_mode { " to detached/" } else { "" }
    );

    let downloaded = Arc::new(AtomicU64::new(0));
    let downloaded_bytes = Arc::new(AtomicU64::new(0));

    let results: Vec<Result<(String, String, String)>> = stream::iter(parts)
        .map(|part| {
            let storage = storage.clone();
            let output_dir = output_dir.to_path_buf();
            let downloaded = Arc::clone(&downloaded);
            let downloaded_bytes = Arc::clone(&downloaded_bytes);
            async move {
                let remote_key = blob_remote_key(&part.blob_hash);
                let dest_dir = if attach_mode {
                    output_dir
                        .join("data")
                        .join(&part.database)
                        .join(&part.table_name)
                        .join("detached")
                        .join(&part.part_name)
                } else {
                    output_dir
                        .join("data")
                        .join(&part.database)
                        .join(&part.table_name)
                        .join(&part.part_name)
                };
                fs::create_dir_all(&dest_dir)
                    .with_context(|| format!("create {}", dest_dir.display()))?;

                let body = storage
                    .get_object_stream(&remote_key)
                    .await
                    .with_context(|| format!("get {remote_key}"))?;
                let reader = body.into_async_read();

                tokio::task::spawn_blocking(move || -> Result<()> {
                    let mut r = SyncIoBridge::new(reader);
                    crate::part_zip::extract_zip(&mut r, &dest_dir)
                })
                .await
                .context("ZIP extract task panicked")??;

                let count = downloaded.fetch_add(1, Ordering::Relaxed) + 1;
                let bytes =
                    downloaded_bytes.fetch_add(part.blob_size, Ordering::Relaxed) + part.blob_size;
                if count.is_multiple_of(10) || count == total_parts {
                    println!(
                        "  Restored {count}/{total_parts} parts ({})",
                        format_bytes(bytes)
                    );
                }
                Ok((part.database, part.table_name, part.part_name))
            }
        })
        .buffer_unordered(download_concurrency)
        .collect()
        .await;

    let restored_parts: Vec<_> = results.into_iter().collect::<Result<Vec<_>>>()?;

    if let Some(cfg) = attach_cfg {
        println!(
            "  Attaching {} parts to ClickHouse...",
            restored_parts.len()
        );
        let attached = Arc::new(AtomicU64::new(0));

        let attach_results: Vec<Result<()>> = stream::iter(&restored_parts)
            .map(|(db, table, part_name)| {
                let attached = Arc::clone(&attached);
                async move {
                    crate::clickhouse::attach_part(cfg, db, table, part_name)
                        .await
                        .with_context(|| format!("ATTACH PART {db}.{table}/{part_name}"))?;
                    let count = attached.fetch_add(1, Ordering::Relaxed) + 1;
                    if count.is_multiple_of(10) || count == total_parts {
                        println!("  Attached {count}/{total_parts} parts");
                    }
                    Ok(())
                }
            })
            .buffer_unordered(download_concurrency)
            .collect()
            .await;

        for r in attach_results {
            r?;
        }
    }

    println!(
        "Restore complete: {} metadata files, {total_parts} parts",
        files.len()
    );
    Ok(())
}

#[derive(Debug)]
struct FilteredRestoreItems {
    parts: Vec<ManifestPart>,
    files: Vec<ManifestFile>,
    skipped: Vec<(String, String)>,
}

fn filter_restore_items(
    parts: Vec<ManifestPart>,
    files: Vec<ManifestFile>,
    table_filter: Option<(&str, &str)>,
    exclude_tables: Option<&HashSet<(String, String)>>,
) -> Result<FilteredRestoreItems> {
    let mut parts = parts;
    let mut files = files;
    let mut skipped = Vec::new();

    if let Some((db, tbl)) = table_filter {
        parts.retain(|p| p.database == db && p.table_name == tbl);
        if parts.is_empty() {
            bail!("No parts found for table '{db}.{tbl}' in snapshot");
        }
        let db_sql = format!("metadata/{db}.sql");
        let table_sql = format!("metadata/{db}/{tbl}.sql");
        files.retain(|f| f.path == db_sql || f.path == table_sql);
    }

    if let Some(exclude) = exclude_tables {
        let mut skipped_set: BTreeSet<(String, String)> = BTreeSet::new();
        parts.retain(|p| {
            let key = (p.database.clone(), p.table_name.clone());
            if exclude.contains(&key) {
                let _ = skipped_set.insert(key);
                false
            } else {
                true
            }
        });
        skipped = skipped_set.into_iter().collect();

        files.retain(|f| {
            if let Some(rest) = f.path.strip_prefix("metadata/")
                && let Some(inner) = rest.strip_suffix(".sql")
                && let Some((db, tbl)) = inner.split_once('/')
            {
                !exclude.contains(&(db.to_string(), tbl.to_string()))
            } else {
                true
            }
        });
    }

    Ok(FilteredRestoreItems {
        parts,
        files,
        skipped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn cleanup_staged_part_removes_links_and_marker() {
        let tmp = TempDir::new().expect("tempdir");
        let source = tmp.path().join("source.bin");
        fs::write(&source, b"part data").expect("write source");

        let staged = tmp.path().join("staging_parts/ab/abcdef");
        fs::create_dir_all(&staged).expect("create staged dir");
        fs::hard_link(&source, staged.join("data.bin")).expect("hardlink staged file");
        let marker = staged.with_extension("staged");
        fs::write(&marker, b"ok").expect("write marker");

        cleanup_staged_part_sync(&staged).expect("cleanup staged part");

        assert!(!staged.exists());
        assert!(!marker.exists());
        assert_eq!(fs::read(source).expect("read source"), b"part data");
    }

    #[test]
    fn cleanup_staged_part_is_idempotent() {
        let tmp = TempDir::new().expect("tempdir");
        let staged = tmp.path().join("staging_parts/ab/abcdef");

        cleanup_staged_part_sync(&staged).expect("cleanup absent staged part");
        cleanup_staged_part_sync(&staged).expect("cleanup absent staged part again");
    }

    #[test]
    fn cleanup_staging_dirs_removes_only_chbk_staging_dirs() {
        let tmp = TempDir::new().expect("tempdir");
        let current = tmp.path().join(STAGING_DIR_NAME);
        let legacy = tmp.path().join("staging_parts-old-1234");
        let unrelated = tmp.path().join("staging_parts_archive");
        fs::create_dir_all(&current).expect("create current staging dir");
        fs::create_dir_all(&legacy).expect("create legacy staging dir");
        fs::create_dir_all(&unrelated).expect("create unrelated dir");

        cleanup_staging_dirs_sync(tmp.path()).expect("cleanup staging dirs");

        assert!(!current.exists());
        assert!(!legacy.exists());
        assert!(unrelated.exists());
    }
}
