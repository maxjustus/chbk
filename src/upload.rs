use crate::storage::{Storage, StorageConfig, UploadProgress};
use anyhow::{Context, Result};
use std::sync::Arc;

pub fn init_storage(cfg: &crate::Config) -> Result<(Arc<Storage>, Arc<UploadProgress>)> {
    let progress = Arc::new(UploadProgress::new());

    let bucket = cfg.s3_bucket.clone().context("S3_BUCKET is required")?;
    let region = cfg.s3_region.clone().context("S3_REGION is required")?;
    let access_key = cfg
        .s3_access_key_id
        .clone()
        .context("S3_ACCESS_KEY_ID is required")?;
    let secret_key = cfg
        .s3_secret_access_key
        .clone()
        .context("S3_SECRET_ACCESS_KEY is required")?;

    let storage_config = StorageConfig {
        bucket,
        region,
        endpoint: cfg.s3_endpoint.clone(),
        access_key_id: access_key,
        secret_access_key: secret_key,
        prefix: cfg.s3_prefix.clone(),
    };

    let storage = Arc::new(Storage::new(
        storage_config,
        Some(progress.clone()),
        cfg.upload_min_chunk_size,
        cfg.upload_max_chunk_size,
        cfg.upload_target_parts,
        cfg.multipart_part_concurrency,
    ));

    Ok((storage, progress))
}
