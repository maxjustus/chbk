//! Garbage collection for blob storage.
//!
//! Under the manifest design, the set of referenced blobs is the union of
//! `blob_hash` over every manifest. GC lists remote blobs, diffs against that
//! union, and deletes the remainder — protected by an age-based grace period
//! on `LastModified` that covers in-flight backups.
//!
//! Only GC needs serialization (two concurrent GCs would race deletes), so a
//! single S3 lock at `gc/.lock` gates the cleanup functions below. Backup
//! is lock-free.

use crate::Config;
use crate::blob_hash::{BlobHash, from_hex as hash_from_hex};
use crate::manifest::{self, Manifest};
use crate::storage::Storage;
use crate::util::{blob_remote_key, format_bytes};
use anyhow::{Context, Result};
use chrono::Utc;
use futures::{StreamExt, pin_mut};
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod lock;

/// Concurrency for downloading manifests during GC planning.
const MANIFEST_DOWNLOAD_CONCURRENCY: usize = 32;

const LOCK_LOST_MSG: &str =
    "GC lock lost mid-operation — aborted to prevent races with another GC instance";

/// Early-return if the GC lock was lost since the last check.
fn bail_if_lock_lost(lost: &CancellationToken) -> Result<()> {
    if lost.is_cancelled() {
        anyhow::bail!(LOCK_LOST_MSG);
    }
    Ok(())
}

/// Acquire the GC lock with heartbeat, run the given async closure, then release.
///
/// The closure receives a `CancellationToken` that fires if the heartbeat
/// loses the lock to another instance. Destructive operations (blob deletes,
/// manifest deletes) must check this token before proceeding — otherwise two
/// GC instances could race and double-delete.
async fn with_gc_lock<F, Fut>(storage: &Storage, f: F) -> Result<()>
where
    F: FnOnce(CancellationToken) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let instance_id = Uuid::new_v4().to_string();

    println!("Acquiring GC lock...");
    let etag = lock::acquire_with_retry(storage, &instance_id, 10)
        .await
        .context("Failed to acquire GC lock")?
        .ok_or_else(|| {
            anyhow::anyhow!("Could not acquire GC lock - another operation may be running")
        })?;
    println!("Acquired GC lock");

    let stop_heartbeat = CancellationToken::new();
    let lock_lost = CancellationToken::new();
    let heartbeat = lock::spawn_heartbeat(
        storage.clone(),
        instance_id.clone(),
        etag,
        stop_heartbeat.clone(),
        lock_lost.clone(),
    );

    // Race the caller's work against lock-loss. If the heartbeat detects loss,
    // `lock_lost` fires and we return an error without awaiting `f` further —
    // whatever `f` has not yet consumed from its own cancellation token is its
    // problem to handle. Callers that are about to delete must check the token.
    let result = tokio::select! {
        result = f(lock_lost.clone()) => result,
        () = lock_lost.cancelled() => Err(anyhow::anyhow!(LOCK_LOST_MSG)),
    };

    stop_heartbeat.cancel();
    let _ = heartbeat.await;

    // Only attempt release if we still own the lock. `release` also checks
    // ownership, but skipping the call when we've lost it avoids a spurious
    // warning.
    if !lock_lost.is_cancelled()
        && let Err(e) = lock::release(storage, &instance_id).await
    {
        eprintln!("Warning: failed to release GC lock: {e}");
    }

    result
}

/// Union of blob hashes across every manifest in the bucket.
async fn referenced_blob_hashes(storage: &Storage) -> Result<(HashSet<String>, usize)> {
    let manifests = manifest::read_all_manifests(storage, MANIFEST_DOWNLOAD_CONCURRENCY).await?;
    let count = manifests.len();
    let mut referenced = HashSet::new();
    for m in &manifests {
        referenced.extend(m.parts.iter().map(|part| part.blob_hash.clone()));
    }
    Ok((referenced, count))
}

/// Delete every blob in S3 that isn't referenced by any manifest, skipping
/// blobs younger than the grace period. The grace period replaces the legacy
/// `.active/` marker protocol — in-flight backups' blobs are protected simply
/// by being too new to delete.
pub async fn gc_all_blobs(
    cfg: &Config,
    dry_run: bool,
    shard_concurrency: usize,
    grace_period_hours: u64,
    storage: &Storage,
) -> Result<()> {
    with_gc_lock(storage, |lock_lost| async move {
        let (referenced, manifest_count) = referenced_blob_hashes(storage).await?;
        println!(
            "Referenced blobs from {manifest_count} manifests: {}",
            referenced.len()
        );

        let referenced_hashes: HashSet<BlobHash> =
            referenced.iter().filter_map(|h| hash_from_hex(h)).collect();

        remote_scan_and_delete(
            cfg,
            &referenced_hashes,
            dry_run,
            shard_concurrency.max(1),
            grace_period_hours,
            storage,
            &lock_lost,
        )
        .await
    })
    .await
}

/// Delete a snapshot and the blobs it exclusively held.
pub async fn gc_snapshot(
    cfg: &Config,
    snapshot_name: &str,
    dry_run: bool,
    storage: &Storage,
) -> Result<()> {
    with_gc_lock(storage, |lock_lost| async move {
        let target = manifest::read_manifest(storage, snapshot_name)
            .await
            .with_context(|| format!("Reading manifest for '{snapshot_name}'"))?;
        let target_hashes = manifest::blob_hash_set(&target);

        let all_names = manifest::list_manifest_names(storage).await?;
        let other_names: Vec<String> = all_names
            .into_iter()
            .filter(|n| n != snapshot_name)
            .collect();

        // NOTE: For rm-snapshot we must download ALL other manifests to
        // compute exclusive blobs. 365 snapshots ~= 36MB — acceptable for an
        // infrequent operation. Revisit with a summary index if this grows.
        let other_hashes = union_hashes_for_names(storage, &other_names).await?;

        let exclusive: Vec<String> = target_hashes.difference(&other_hashes).cloned().collect();
        println!(
            "Snapshot '{snapshot_name}' has {} exclusive blobs",
            exclusive.len()
        );

        if dry_run {
            println!("Would delete manifest: {snapshot_name}");
            for hash in &exclusive {
                println!("Would delete blob: {hash}");
            }
            println!("GC would delete {} blobs", exclusive.len());
            return Ok(());
        }

        bail_if_lock_lost(&lock_lost)?;
        manifest::delete_manifest(storage, snapshot_name).await?;
        println!("Deleted manifest: {snapshot_name}");

        if !exclusive.is_empty() {
            bail_if_lock_lost(&lock_lost)?;
            delete_exclusive_blobs(&exclusive, cfg.delete_concurrency, storage).await?;
        }

        Ok(())
    })
    .await
}

/// Prune old live snapshots (`live_*` auto-generated) with tiered retention.
pub async fn prune_old_live_snapshots(
    cfg: &Config,
    retain_all_minutes: u64,
    retain_daily_minutes: Option<u64>,
    current_timestamp: i64,
    dry_run: bool,
    storage: &Storage,
) -> Result<()> {
    with_gc_lock(storage, |lock_lost| async move {
        // Downloading every manifest is required to compute exclusive blobs
        // anyway, so we get `timestamp` from the manifest body rather than
        // parsing names.
        let all_manifests =
            manifest::read_all_manifests(storage, MANIFEST_DOWNLOAD_CONCURRENCY).await?;

        let mut live_by_name: Vec<(String, i64)> = all_manifests
            .iter()
            .filter(|m| m.name.starts_with("live_"))
            .map(|m| (m.name.clone(), m.timestamp))
            .collect();
        live_by_name.sort_by_key(|(_, ts)| *ts);

        let to_prune = compute_prune_set(
            live_by_name,
            retain_all_minutes,
            retain_daily_minutes,
            current_timestamp,
        );

        if to_prune.is_empty() {
            println!("No live snapshots to prune");
            return Ok(());
        }

        let action = if dry_run { "Would prune" } else { "Pruning" };
        match retain_daily_minutes {
            Some(daily) => println!(
                "{action} {} snapshots (keeping: all within {}min + daily for {}min)",
                to_prune.len(),
                retain_all_minutes,
                daily
            ),
            None => println!(
                "{action} {} old live snapshots (beyond {}min retention)",
                to_prune.len(),
                retain_all_minutes
            ),
        }

        let prune_set: HashSet<&str> = to_prune.iter().map(String::as_str).collect();
        let mut target_hashes = HashSet::new();
        let mut other_hashes = HashSet::new();
        for m in &all_manifests {
            if prune_set.contains(m.name.as_str()) {
                target_hashes.extend(m.parts.iter().map(|part| part.blob_hash.clone()));
            } else {
                other_hashes.extend(m.parts.iter().map(|part| part.blob_hash.clone()));
            }
        }

        let exclusive: Vec<String> = target_hashes.difference(&other_hashes).cloned().collect();
        println!("Found {} exclusive blobs to delete", exclusive.len());

        if dry_run {
            for name in &to_prune {
                println!("Would prune snapshot: {name}");
            }
            for hash in &exclusive {
                println!("Would delete blob: {hash}");
            }
            return Ok(());
        }

        for name in &to_prune {
            bail_if_lock_lost(&lock_lost)?;
            if let Err(e) = manifest::delete_manifest(storage, name).await {
                eprintln!("Warning: failed to delete manifest {name}: {e}");
            }
        }
        println!("Deleted {} manifests", to_prune.len());

        if !exclusive.is_empty() {
            bail_if_lock_lost(&lock_lost)?;
            delete_exclusive_blobs(&exclusive, cfg.delete_concurrency, storage).await?;
        }

        Ok(())
    })
    .await
}

/// Tiered-retention computation shared between real pruning and tests.
fn compute_prune_set(
    live_by_name: Vec<(String, i64)>,
    retain_all_minutes: u64,
    retain_daily_minutes: Option<u64>,
    current_timestamp: i64,
) -> Vec<String> {
    use chrono::DateTime;
    use std::collections::HashMap;

    let all_cutoff = current_timestamp - (retain_all_minutes as i64 * 60);

    let Some(daily_minutes) = retain_daily_minutes else {
        return live_by_name
            .into_iter()
            .filter(|(_, ts)| *ts < all_cutoff)
            .map(|(name, _)| name)
            .collect();
    };

    let daily_cutoff = current_timestamp - (daily_minutes as i64 * 60);
    let mut to_prune = Vec::new();
    let mut daily: HashMap<String, Vec<(String, i64)>> = HashMap::new();

    for (name, ts) in live_by_name {
        if ts >= all_cutoff {
            continue;
        }
        if ts >= daily_cutoff {
            let dt = DateTime::from_timestamp(ts, 0)
                .or_else(|| DateTime::from_timestamp(current_timestamp, 0))
                .unwrap_or_default();
            daily
                .entry(dt.format("%Y-%m-%d").to_string())
                .or_default()
                .push((name, ts));
        } else {
            to_prune.push(name);
        }
    }

    for (_day, mut snapshots) in daily {
        snapshots.sort_by_key(|(_, ts)| std::cmp::Reverse(*ts));
        for (name, _) in snapshots.into_iter().skip(1) {
            to_prune.push(name);
        }
    }

    to_prune
}

/// Download the given manifests and return the union of their blob hashes.
async fn union_hashes_for_names(storage: &Storage, names: &[String]) -> Result<HashSet<String>> {
    use futures::stream;

    let manifests: Vec<Result<Manifest>> = stream::iter(
        names
            .iter()
            .map(|n| async move { manifest::read_manifest(storage, n).await }),
    )
    .buffer_unordered(MANIFEST_DOWNLOAD_CONCURRENCY)
    .collect()
    .await;

    let mut union = HashSet::new();
    for m in manifests {
        union.extend(m?.parts.into_iter().map(|part| part.blob_hash));
    }
    Ok(union)
}

/// Progress tracking for remote scan operations.
struct ScanProgress {
    start: Instant,
    last_log: Instant,
    scanned: u64,
    orphans_found: u64,
    orphans_deleted: u64,
    bytes_found: u64,
    bytes_deleted: u64,
}

impl ScanProgress {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            start: now,
            last_log: now,
            scanned: 0,
            orphans_found: 0,
            orphans_deleted: 0,
            bytes_found: 0,
            bytes_deleted: 0,
        }
    }

    const fn record_orphan(&mut self, size: u64) {
        self.orphans_found += 1;
        self.bytes_found += size;
    }

    const fn record_deleted(&mut self, count: u64, bytes: u64) {
        self.orphans_deleted += count;
        self.bytes_deleted += bytes;
    }

    fn scan_rate(&self) -> f64 {
        let elapsed = self.start.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.scanned as f64 / elapsed
        } else {
            0.0
        }
    }

    fn should_log(&mut self) -> bool {
        let elapsed = self.last_log.elapsed();
        if elapsed.as_secs() >= 5 || self.scanned.is_multiple_of(10000) {
            self.last_log = Instant::now();
            true
        } else {
            false
        }
    }

    fn log_progress(&self) {
        println!(
            "Scan: {} objects ({:.0}/s) | Orphans: {} found ({}) | Deleted: {} ({})",
            self.scanned,
            self.scan_rate(),
            self.orphans_found,
            format_bytes(self.bytes_found),
            self.orphans_deleted,
            format_bytes(self.bytes_deleted),
        );
    }

    fn log_final(&self) {
        println!(
            "Remote scan finished: {} objects in {:.1}s ({:.0}/s) | {} orphans ({}) deleted",
            self.scanned,
            self.start.elapsed().as_secs_f64(),
            self.scan_rate(),
            self.orphans_deleted,
            format_bytes(self.bytes_deleted),
        );
    }
}

/// Delete blobs by hash and report results. Used by gc_snapshot and gc_live
/// after computing exclusive blob sets.
async fn delete_exclusive_blobs(
    exclusive: &[String],
    delete_concurrency: usize,
    storage: &Storage,
) -> Result<()> {
    let keys: Vec<String> = exclusive.iter().map(|h| blob_remote_key(h)).collect();
    let stats = storage
        .delete_objects_bulk(keys, delete_concurrency)
        .await?;
    println!("GC deleted {} blobs", stats.success);
    if stats.errors > 0 {
        eprintln!(
            "Warning: {} blobs failed to delete ({} throttled)",
            stats.errors, stats.throttled
        );
    }
    Ok(())
}

/// Delete (or dry-run report) a batch of orphan blobs.
async fn delete_or_report_batch(
    batch: &[(String, u64)],
    dry_run: bool,
    label: &str,
    delete_concurrency: usize,
    storage: &Storage,
    progress: &mut ScanProgress,
) -> Result<()> {
    let batch_bytes: u64 = batch.iter().map(|(_, s)| *s).sum();
    let batch_count = batch.len() as u64;

    if dry_run {
        println!(
            "Would delete {} of {} orphans ({})",
            label,
            batch_count,
            format_bytes(batch_bytes)
        );
        progress.record_deleted(batch_count, batch_bytes);
    } else {
        let keys: Vec<String> = batch.iter().map(|(h, _)| blob_remote_key(h)).collect();
        println!(
            "Deleting {} of {} orphans ({})...",
            label,
            keys.len(),
            format_bytes(batch_bytes)
        );
        let stats = storage
            .delete_objects_bulk(keys, delete_concurrency)
            .await?;
        progress.record_deleted(stats.success, batch_bytes);
        if stats.errors > 0 {
            eprintln!(
                "{} delete: {} failed ({} throttled)",
                label, stats.errors, stats.throttled
            );
        }
    }
    Ok(())
}

/// Parse an S3 key under `base/data/blobs/<shard>/<hash>` and return the hash
/// string if it's a valid blob not in `referenced`.
fn parse_unreferenced_blob(key: &str, referenced: &HashSet<BlobHash>) -> Option<String> {
    let parts: Vec<&str> = key.split('/').collect();
    let ["base", "data", "blobs", shard, hash_str, ..] = parts.as_slice() else {
        return None;
    };
    if shard.len() != 2 || hash_str.len() != 32 || hash_str.get(0..2) != Some(shard) {
        return None;
    }
    let hash = hash_from_hex(hash_str)?;
    if referenced.contains(&hash) {
        return None;
    }
    Some((*hash_str).to_string())
}

/// Scan every shard under `base/data/blobs/` and delete orphan blobs.
///
/// An orphan is a blob whose hash isn't in `referenced` AND whose
/// `LastModified` is older than the grace period (so in-flight backups can't
/// have their blobs deleted out from under them).
async fn remote_scan_and_delete(
    cfg: &Config,
    referenced: &HashSet<BlobHash>,
    dry_run: bool,
    shard_concurrency: usize,
    grace_period_hours: u64,
    storage: &Storage,
    lock_lost: &CancellationToken,
) -> Result<()> {
    let remote_prefix = "base/data/blobs/";
    let batch_size = 1000usize;
    let delete_concurrency = cfg.delete_concurrency.max(1);

    let cutoff = Utc::now() - chrono::Duration::hours(grace_period_hours as i64);
    println!(
        "Scanning '{remote_prefix}' (shard_concurrency={shard_concurrency}, batch_size={batch_size}, delete_concurrency={delete_concurrency}, grace_period={grace_period_hours}h)..."
    );
    println!(
        "Skipping blobs newer than {} (grace period protection)",
        cutoff.format("%Y-%m-%d %H:%M:%S UTC")
    );

    let referenced = Arc::new(referenced.clone());
    let scanned_count = Arc::new(AtomicU64::new(0));
    let skipped_young = Arc::new(AtomicU64::new(0));

    let orphan_stream = storage
        .list_shards_parallel(remote_prefix, shard_concurrency)
        .inspect({
            let scanned_count = Arc::clone(&scanned_count);
            move |_| {
                let _ = scanned_count.fetch_add(1, Ordering::Relaxed);
            }
        })
        .filter_map({
            let referenced = Arc::clone(&referenced);
            let skipped_young = Arc::clone(&skipped_young);
            move |res| {
                let referenced = Arc::clone(&referenced);
                let skipped_young = Arc::clone(&skipped_young);
                async move {
                    let (key, size, last_modified) = match res {
                        Ok(v) => v,
                        Err(err) => {
                            eprintln!("Remote list error: {err}");
                            return None;
                        }
                    };

                    if last_modified > cutoff {
                        let _ = skipped_young.fetch_add(1, Ordering::Relaxed);
                        return None;
                    }

                    parse_unreferenced_blob(&key, &referenced).map(|hash_str| (hash_str, size))
                }
            }
        });

    pin_mut!(orphan_stream);

    let mut progress = ScanProgress::new();
    let mut batch: Vec<(String, u64)> = Vec::with_capacity(batch_size);

    while let Some((hash, size)) = orphan_stream.next().await {
        progress.scanned = scanned_count.load(Ordering::Relaxed);
        progress.record_orphan(size);
        batch.push((hash, size));

        if progress.should_log() {
            progress.log_progress();
        }

        if batch.len() >= batch_size {
            bail_if_lock_lost(lock_lost)?;
            delete_or_report_batch(
                &batch,
                dry_run,
                "batch",
                delete_concurrency,
                storage,
                &mut progress,
            )
            .await?;
            batch.clear();
        }
    }

    if !batch.is_empty() {
        bail_if_lock_lost(lock_lost)?;
        delete_or_report_batch(
            &batch,
            dry_run,
            "final batch",
            delete_concurrency,
            storage,
            &mut progress,
        )
        .await?;
    }

    progress.scanned = scanned_count.load(Ordering::Relaxed);
    let skipped = skipped_young.load(Ordering::Relaxed);

    if progress.orphans_found == 0 {
        println!("Remote scan: no orphan blobs detected");
    }
    if skipped > 0 {
        println!("Skipped {skipped} blobs newer than grace period ({grace_period_hours}h)");
    }
    progress.log_final();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_progress_tracking() {
        let mut progress = ScanProgress::new();
        assert_eq!(progress.scanned, 0);
        progress.record_orphan(1000);
        progress.record_orphan(2000);
        assert_eq!(progress.orphans_found, 2);
        assert_eq!(progress.bytes_found, 3000);
        progress.record_deleted(2, 3000);
        assert_eq!(progress.orphans_deleted, 2);
        assert_eq!(progress.bytes_deleted, 3000);
        progress.scanned = 100;
        assert_eq!(progress.scanned, 100);
    }

    #[test]
    fn test_scan_progress_should_log_on_count_boundary() {
        let mut progress = ScanProgress::new();
        progress.scanned = 9999;
        assert!(!progress.should_log());
        progress.scanned = 10000;
        assert!(progress.should_log());
        progress.scanned = 10001;
        assert!(!progress.should_log());
        progress.scanned = 20000;
        assert!(progress.should_log());
    }

    #[test]
    fn prune_set_with_no_daily_retention_drops_everything_past_cutoff() {
        // retain_all = 60min → cutoff = now - 3600. ts >= cutoff is kept.
        let now: i64 = 1_000_000;
        let snaps = vec![
            ("very_old".into(), now - 7200),
            ("just_past".into(), now - 3601),
            ("fresh".into(), now - 100),
        ];
        let to_prune = compute_prune_set(snaps, 60, None, now);
        let set: HashSet<_> = to_prune.into_iter().collect();
        assert!(set.contains("very_old"));
        assert!(set.contains("just_past"));
        assert!(!set.contains("fresh"));
    }

    #[test]
    fn prune_set_with_daily_retention_keeps_one_per_day() {
        // 2026-04-20 at 12:00:00 UTC → 1_776_686_400
        let now: i64 = 1_776_686_400;
        let day = 86_400;
        let snaps = vec![
            ("old".into(), now - 5 * day),
            ("day1_early".into(), now - 3 * day),
            ("day1_late".into(), now - 3 * day + 300),
            ("fresh".into(), now - 60),
        ];
        // retain_all: 1 hour; retain_daily: 10 days
        let to_prune = compute_prune_set(snaps, 60, Some(10 * 24 * 60), now);
        let set: HashSet<_> = to_prune.into_iter().collect();
        // 'old' beyond daily window? now - 5*day vs daily cutoff (now - 10*day).
        // 5 days < 10 days → inside daily window, gets grouped by day; only
        // latest per day kept.
        assert!(!set.contains("old"));
        // day1_early older than day1_late → early is pruned, late kept.
        assert!(set.contains("day1_early"));
        assert!(!set.contains("day1_late"));
        // 'fresh' within retain_all → never considered.
        assert!(!set.contains("fresh"));
    }
}
