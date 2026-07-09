//! S3 distributed lock for GC serialization.
//!
//! Prevents two concurrent GC runs from racing deletes. Blobs are CAS and
//! manifests are write-once, so backup itself is lock-free — this lock is
//! GC-only.

use crate::storage::Storage;
use anyhow::{Context, Result};
use bytes::Bytes;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

const LOCK_KEY: &str = "gc/.lock";
const TTL_SECS: u64 = 60;
const HEARTBEAT_SECS: u64 = 30;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct LockInfo {
    instance_id: String,
    hostname: String,
    acquired_at: i64,
    ttl_secs: u64,
}

fn now_epoch_secs() -> i64 {
    // SystemTime clock is always after UNIX_EPOCH on any reasonable platform;
    // if it isn't, treat as epoch to avoid an unwrap().
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

impl LockInfo {
    fn new(instance_id: &str) -> Self {
        Self {
            instance_id: instance_id.to_string(),
            hostname: gethostname::gethostname().to_string_lossy().to_string(),
            acquired_at: now_epoch_secs(),
            ttl_secs: TTL_SECS,
        }
    }

    fn is_expired(&self) -> bool {
        now_epoch_secs() > self.acquired_at + self.ttl_secs as i64
    }
}

/// Attempt to PUT the lock object with If-None-Match. Returns the etag on
/// success, `None` if the lock was already present.
async fn try_put(storage: &Storage, instance_id: &str) -> Result<Option<String>> {
    let info = LockInfo::new(instance_id);
    let body = Bytes::from(serde_json::to_vec(&info)?);
    let (etag, conflict) = storage
        .put_object_conditional(LOCK_KEY, body, None, true)
        .await?;
    Ok((!conflict).then_some(etag))
}

/// Acquire the lock once. Returns `Some(etag)` if acquired, `None` if held by
/// another instance.
pub async fn acquire(storage: &Storage, instance_id: &str) -> Result<Option<String>> {
    if let Some(etag) = try_put(storage, instance_id).await? {
        return Ok(Some(etag));
    }

    // Lock exists — check if expired.
    let Ok(data) = storage.get_object(LOCK_KEY).await else {
        // Disappeared between conflict and fetch — try once more.
        return try_put(storage, instance_id).await;
    };
    let Ok(existing) = serde_json::from_slice::<LockInfo>(&data) else {
        // Corrupt payload — treat as stale.
        return break_stale(storage, instance_id).await;
    };
    if existing.is_expired() {
        return break_stale(storage, instance_id).await;
    }
    eprintln!(
        "GC lock held by {} ({}), acquired {}s ago",
        existing.instance_id,
        existing.hostname,
        now_epoch_secs() - existing.acquired_at
    );
    Ok(None)
}

async fn break_stale(storage: &Storage, instance_id: &str) -> Result<Option<String>> {
    eprintln!("Breaking stale GC lock...");
    let _ = storage.delete_object(LOCK_KEY).await;
    try_put(storage, instance_id).await
}

/// Release the lock, verifying ownership first.
pub async fn release(storage: &Storage, instance_id: &str) -> Result<()> {
    if let Ok(data) = storage.get_object(LOCK_KEY).await
        && let Ok(existing) = serde_json::from_slice::<LockInfo>(&data)
        && existing.instance_id != instance_id
    {
        return Ok(());
    }
    let _ = storage.delete_object(LOCK_KEY).await;
    Ok(())
}

/// Refresh an held lock (If-Match on previous etag). Returns the new etag, or
/// `None` if the lock was taken by another instance.
async fn refresh(
    storage: &Storage,
    instance_id: &str,
    expected_etag: &str,
) -> Result<Option<String>> {
    let info = LockInfo::new(instance_id);
    let body = Bytes::from(serde_json::to_vec(&info)?);
    let (new_etag, conflict) = storage
        .put_object_conditional(LOCK_KEY, body, Some(expected_etag), false)
        .await?;
    Ok((!conflict).then_some(new_etag))
}

/// Spawn a background task that refreshes the lock every `HEARTBEAT_SECS`.
/// Stops when `cancel` is triggered. If the lock is lost to another holder
/// (refresh returns `Ok(None)`), triggers `lost` so callers can abort their
/// work before making any more destructive operations.
pub fn spawn_heartbeat(
    storage: Storage,
    instance_id: String,
    initial_etag: String,
    cancel: CancellationToken,
    lost: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut etag = initial_etag;
        let interval = Duration::from_secs(HEARTBEAT_SECS);
        loop {
            tokio::select! {
                () = tokio::time::sleep(interval) => match refresh(&storage, &instance_id, &etag).await {
                    Ok(Some(new_etag)) => etag = new_etag,
                    Ok(None) => {
                        eprintln!("ERROR: lost GC lock (taken by another instance) — aborting");
                        lost.cancel();
                        break;
                    }
                    Err(e) => eprintln!("Warning: failed to refresh GC lock: {e}"),
                },
                () = cancel.cancelled() => break,
            }
        }
    })
}

/// Acquire with simple linear-exponential retry. Returns `Some(etag)` if
/// eventually acquired, `None` if every attempt lost the race.
pub async fn acquire_with_retry(
    storage: &Storage,
    instance_id: &str,
    max_attempts: u32,
) -> Result<Option<String>> {
    for attempt in 1..=max_attempts {
        match acquire(storage, instance_id).await {
            Ok(Some(etag)) => return Ok(Some(etag)),
            Ok(None) if attempt < max_attempts => {
                let delay = Duration::from_secs(2u64.pow(attempt.min(5)));
                eprintln!(
                    "Lock unavailable, retrying in {delay:?} (attempt {attempt}/{max_attempts})"
                );
                tokio::time::sleep(delay).await;
            }
            Ok(None) => return Ok(None),
            Err(e) if attempt < max_attempts => {
                eprintln!("Lock error: {e}, retrying...");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => return Err(e).context("acquire GC lock"),
        }
    }
    Ok(None)
}
