use crate::retry::{RetryConfig, RetryResult, with_retry};
use anyhow::Error as AnyhowError;
use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::retry::RetryConfig as SdkRetryConfig;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier,
};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::stream::{BoxStream, StreamExt};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// Retry config for S3 API calls (multipart init, part upload, complete, etc.)
const fn s3_retry_config() -> RetryConfig {
    RetryConfig {
        max_attempts: 5,
        base_delay_ms: 1000,
    }
}

/// Retry classification of an S3 SDK error. Network-level failures are
/// transient; service errors are dispatched by code; everything else is fatal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum S3ErrorKind {
    Throttle,
    Transient,
    Fatal,
}

impl S3ErrorKind {
    fn from_sdk<E>(err: &SdkError<E>) -> Self
    where
        E: ProvideErrorMetadata,
    {
        match err {
            SdkError::TimeoutError(_)
            | SdkError::DispatchFailure(_)
            | SdkError::ResponseError(_) => Self::Transient,
            SdkError::ServiceError(svc) => {
                let by_code = Self::from_code(svc.err().code());
                if by_code == Self::Fatal && svc.raw().status().as_u16() >= 500 {
                    Self::Transient
                } else {
                    by_code
                }
            }
            _ => Self::Fatal,
        }
    }

    fn from_code(code: Option<&str>) -> Self {
        match code {
            Some("SlowDown" | "Throttling" | "ThrottlingException" | "RequestThrottled") => {
                Self::Throttle
            }
            Some("InternalError" | "ServiceUnavailable" | "RequestTimeout") => Self::Transient,
            _ => Self::Fatal,
        }
    }

    const fn is_retryable(self) -> bool {
        matches!(self, Self::Throttle | Self::Transient)
    }
}

/// Classify an S3 SDK result into a `RetryResult`, recording retry/throttle/error
/// metrics on the given progress tracker.
fn classify_s3<T, E>(
    result: Result<T, SdkError<E>>,
    progress: Option<&Arc<UploadProgress>>,
) -> RetryResult<T, AnyhowError>
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    match result {
        Ok(v) => RetryResult::Ok(v),
        Err(e) => {
            let kind = S3ErrorKind::from_sdk(&e);
            let err = AnyhowError::new(e);
            if let Some(p) = progress {
                match kind {
                    S3ErrorKind::Throttle => {
                        p.record_retry();
                        p.record_throttle();
                    }
                    S3ErrorKind::Transient => p.record_retry(),
                    S3ErrorKind::Fatal => p.record_error(),
                }
            }
            if kind.is_retryable() {
                RetryResult::Retry(err)
            } else {
                RetryResult::Fatal(err)
            }
        }
    }
}

/// S3 storage configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub prefix: Option<String>,
}

// Multipart upload constraints (AWS S3 and most S3-compatible stores)
pub const MULTIPART_MIN_CHUNK: u64 = 5 * 1024 * 1024; // 5 MiB (AWS minimum)
pub const MULTIPART_MAX_CHUNK: u64 = 5 * 1024 * 1024 * 1024; // 5 GiB (AWS maximum)

// Multipart upload tuning defaults
pub const DEFAULT_MIN_PART_SIZE: u64 = 16 * 1024 * 1024; // 16 MB minimum chunk
pub const DEFAULT_MAX_PART_SIZE: u64 = 512 * 1024 * 1024; // 512 MB max chunk
pub const DEFAULT_TARGET_PARTS: u64 = 128; // target part count for good parallelism without excessive API calls
pub const DEFAULT_MULTIPART_PART_CONCURRENCY: usize = 16; // max concurrent part uploads per file
const READ_AHEAD_CHUNKS: usize = 4; // number of chunks to read ahead while uploads are in flight

/// Storage abstraction over aws-sdk-s3
#[derive(Debug, Clone)]
pub struct Storage {
    client: Client,
    bucket: String,
    prefix: Option<String>,
    progress: Option<Arc<UploadProgress>>,
    min_chunk_size: u64,
    max_chunk_size: u64,
    target_parts: u64,
    multipart_part_concurrency: usize,
}

impl Storage {
    pub fn new(
        config: StorageConfig,
        progress: Option<Arc<UploadProgress>>,
        min_chunk_size: u64,
        max_chunk_size: u64,
        target_parts: u64,
        multipart_part_concurrency: usize,
    ) -> Self {
        let min_chunk_size = min_chunk_size.max(MULTIPART_MIN_CHUNK);
        let max_chunk_size = max_chunk_size.max(min_chunk_size).min(MULTIPART_MAX_CHUNK);
        let target_parts = target_parts.max(1);
        let multipart_part_concurrency = multipart_part_concurrency.max(1);

        let credentials = Credentials::new(
            &config.access_key_id,
            &config.secret_access_key,
            None,
            None,
            "chbk",
        );

        let mut s3_config_builder = aws_sdk_s3::Config::builder()
            .credentials_provider(credentials)
            .region(Region::new(config.region.clone()))
            .retry_config(SdkRetryConfig::disabled())
            .behavior_version_latest();

        if let Some(ref endpoint) = config.endpoint {
            s3_config_builder = s3_config_builder
                .endpoint_url(endpoint)
                .force_path_style(true);
        }

        let s3_config = s3_config_builder.build();
        let client = Client::from_conf(s3_config);

        let sanitized_prefix = sanitize_s3_prefix(config.prefix);

        Self {
            client,
            bucket: config.bucket,
            prefix: sanitized_prefix,
            progress,
            min_chunk_size,
            max_chunk_size,
            target_parts,
            multipart_part_concurrency,
        }
    }

    /// Apply prefix to key if configured
    fn prefixed_key(&self, key: &str) -> String {
        match &self.prefix {
            Some(prefix) => format!("{prefix}/{key}"),
            None => key.to_string(),
        }
    }

    /// Run an S3 SDK call under the standard retry/classify/progress policy.
    /// The closure is re-invoked on each retry and must produce a fresh future;
    /// clone captures inside it.
    async fn s3_retry<T, F, Fut, E>(&self, context: &str, mut f: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, SdkError<E>>>,
        E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    {
        let config = s3_retry_config();
        let progress = self.progress.clone();
        with_retry::<T, AnyhowError, _, _>(&config, context, move |_| {
            let fut = f();
            let progress = progress.clone();
            async move { classify_s3(fut.await, progress.as_ref()) }
        })
        .await
    }

    /// Create a multipart upload with retry for transient network failures.
    async fn create_multipart_upload_with_retry(
        &self,
        key: &str,
        prefixed_key: &str,
    ) -> Result<CreateMultipartUploadOutput> {
        let bucket = self.bucket.clone();
        let prefixed = prefixed_key.to_string();
        let client = self.client.clone();

        self.s3_retry(&format!("create_multipart_upload({key})"), || {
            let bucket = bucket.clone();
            let prefixed = prefixed.clone();
            let client = client.clone();
            async move {
                client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(&prefixed)
                    .checksum_algorithm(ChecksumAlgorithm::Crc32)
                    .send()
                    .await
            }
        })
        .await
        .with_context(|| format!("Failed to start multipart upload for {key}"))
    }

    /// Upload an AsyncRead stream using multipart upload.
    ///
    /// Returns the total number of bytes uploaded.
    /// Upload via multipart from an async reader.
    ///
    /// `on_progress` is called after each multipart chunk is confirmed uploaded
    /// with (bytes_uploaded_so_far, expected_total_bytes).
    pub async fn put_object_multipart_reader<R>(
        &self,
        key: &str,
        reader: R,
        expected_size: u64,
        on_progress: Option<Arc<dyn Fn(u64, u64) + Send + Sync>>,
    ) -> Result<u64>
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        let prefixed = self.prefixed_key(key);

        // Create multipart upload with retry for transient failures
        let create_resp = self
            .create_multipart_upload_with_retry(key, &prefixed)
            .await?;

        let upload_id = create_resp
            .upload_id()
            .ok_or_else(|| anyhow!("No upload_id returned for {key}"))?
            .to_string();

        match self
            .put_object_multipart_reader_inner(
                key,
                &prefixed,
                &upload_id,
                reader,
                expected_size,
                on_progress,
            )
            .await
        {
            Ok(total) => Ok(total),
            Err(e) => {
                if let Err(abort_err) = self
                    .client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&prefixed)
                    .upload_id(&upload_id)
                    .send()
                    .await
                {
                    eprintln!("Warning: failed to abort multipart upload for {key}: {abort_err}");
                }
                Err(e)
            }
        }
    }

    async fn put_object_multipart_reader_inner<R>(
        &self,
        key: &str,
        prefixed_key: &str,
        upload_id: &str,
        mut reader: R,
        expected_size: u64,
        on_progress: Option<Arc<dyn Fn(u64, u64) + Send + Sync>>,
    ) -> Result<u64>
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        let chunk_size = compute_chunk_size(
            expected_size.max(1),
            self.min_chunk_size,
            self.max_chunk_size,
            self.target_parts,
        );

        let (chunk_tx, mut chunk_rx) = mpsc::channel::<(i32, Bytes, String)>(READ_AHEAD_CHUNKS);

        let reader_handle = tokio::spawn(async move {
            let mut part_number: i32 = 0;
            let chunk_size_usize: usize = chunk_size
                .try_into()
                .with_context(|| format!("chunk_size too large: {chunk_size}"))?;

            loop {
                // Read up to chunk_size bytes into an owned buffer. This avoids an extra copy
                // (previously: Vec<u8> read buffer -> Bytes::copy_from_slice).
                let mut buf = BytesMut::with_capacity(chunk_size_usize);
                while buf.len() < chunk_size_usize {
                    let n = reader.read_buf(&mut buf).await?;
                    if n == 0 {
                        break;
                    }
                }

                if buf.is_empty() {
                    break;
                }

                part_number += 1;
                let crc32 = crc32fast::hash(buf.as_ref());
                let crc32_b64 = base64_encode_crc32(crc32);
                let chunk_bytes = buf.len() as u64;
                let data = buf.freeze();

                if chunk_tx.send((part_number, data, crc32_b64)).await.is_err() {
                    break;
                }

                if chunk_bytes < chunk_size {
                    break;
                }
            }

            Ok::<(), anyhow::Error>(())
        });

        let mut tasks: JoinSet<Result<(i32, u64, UploadPartOutput)>> = JoinSet::new();
        let mut completed_parts: Vec<CompletedPart> = Vec::new();
        let mut uploaded: u64 = 0;
        let progress = self.progress.clone();

        let handle_completed_part =
            |uploaded: &mut u64,
             pn: i32,
             chunk_bytes: u64,
             resp: UploadPartOutput,
             progress: &Option<Arc<UploadProgress>>,
             on_progress: &Option<Arc<dyn Fn(u64, u64) + Send + Sync>>,
             completed_parts: &mut Vec<CompletedPart>| {
                *uploaded += chunk_bytes;
                if let Some(p) = progress.as_ref() {
                    p.record_bytes(chunk_bytes);
                }
                if let Some(cb) = on_progress.as_ref() {
                    cb(*uploaded, expected_size);
                }
                completed_parts.push(completed_part_from_upload(pn, &resp));
            };

        loop {
            while tasks.len() >= self.multipart_part_concurrency {
                if let Some(result) = tasks.join_next().await {
                    let (pn, chunk_bytes, resp) = result
                        .with_context(|| format!("Part upload task panicked for {key}"))??;
                    handle_completed_part(
                        &mut uploaded,
                        pn,
                        chunk_bytes,
                        resp,
                        &progress,
                        &on_progress,
                        &mut completed_parts,
                    );
                }
            }

            match chunk_rx.recv().await {
                Some((part_number, data, crc32_b64)) => {
                    let chunk_bytes = data.len() as u64;
                    let client = self.client.clone();
                    let bucket = self.bucket.clone();
                    let key_owned = prefixed_key.to_string();
                    let upload_id_owned = upload_id.to_string();
                    let progress = progress.clone();

                    let _ = tasks.spawn(async move {
                        let config = s3_retry_config();
                        let resp = with_retry(
                            &config,
                            &format!("upload_part({key_owned} #{part_number})"),
                            |_| {
                                let client = client.clone();
                                let body = data.clone();
                                let bucket = bucket.clone();
                                let key = key_owned.clone();
                                let upload_id = upload_id_owned.clone();
                                let crc = crc32_b64.clone();
                                let progress = progress.clone();
                                async move {
                                    classify_s3(
                                        client
                                            .upload_part()
                                            .bucket(&bucket)
                                            .key(&key)
                                            .upload_id(&upload_id)
                                            .part_number(part_number)
                                            .body(ByteStream::from(body))
                                            .checksum_algorithm(ChecksumAlgorithm::Crc32)
                                            .checksum_crc32(&crc)
                                            .customize()
                                            .disable_payload_signing()
                                            .send()
                                            .await,
                                        progress.as_ref(),
                                    )
                                }
                            },
                        )
                        .await?;
                        Ok((part_number, chunk_bytes, resp))
                    });
                }
                None => break,
            }
        }

        reader_handle
            .await
            .with_context(|| format!("Reader task panicked for {key}"))?
            .with_context(|| format!("Reader task failed for {key}"))?;

        while let Some(result) = tasks.join_next().await {
            let (pn, chunk_bytes, resp) =
                result.with_context(|| format!("Part upload task panicked for {key}"))??;
            handle_completed_part(
                &mut uploaded,
                pn,
                chunk_bytes,
                resp,
                &progress,
                &on_progress,
                &mut completed_parts,
            );
        }

        completed_parts.sort_by_key(CompletedPart::part_number);

        let complete_key = prefixed_key.to_string();
        let complete_bucket = self.bucket.clone();
        let complete_upload_id = upload_id.to_string();
        let complete_client = self.client.clone();

        let _ = self
            .s3_retry(&format!("complete_multipart_upload({key})"), || {
                let client = complete_client.clone();
                let bucket = complete_bucket.clone();
                let key = complete_key.clone();
                let upload_id = complete_upload_id.clone();
                let parts = completed_parts.clone();
                async move {
                    client
                        .complete_multipart_upload()
                        .bucket(&bucket)
                        .key(&key)
                        .upload_id(&upload_id)
                        .multipart_upload(
                            CompletedMultipartUpload::builder()
                                .set_parts(Some(parts))
                                .build(),
                        )
                        .send()
                        .await
                }
            })
            .await
            .with_context(|| format!("Failed to complete multipart upload for {key}"))?;

        Ok(uploaded)
    }

    /// Check if an object exists via HEAD request.
    /// Returns `Some(content_length)` if it exists, `None` if not found.
    pub async fn object_exists(&self, key: &str) -> Result<Option<u64>> {
        let prefixed = self.prefixed_key(key);
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&prefixed)
            .send()
            .await
        {
            Ok(resp) => Ok(Some(resp.content_length().unwrap_or(0) as u64)),
            Err(e)
                if e.as_service_error().is_some_and(
                    aws_sdk_s3::operation::head_object::HeadObjectError::is_not_found,
                ) =>
            {
                Ok(None)
            }
            Err(e) => Err(e).with_context(|| format!("Failed to HEAD {key}")),
        }
    }

    /// Delete a single object.
    pub async fn delete_object(&self, key: &str) -> Result<()> {
        let prefixed = self.prefixed_key(key);
        let _ = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(&prefixed)
            .send()
            .await
            .with_context(|| format!("Failed to delete {key}"))?;
        Ok(())
    }

    /// Get object contents as bytes.
    pub async fn get_object(&self, key: &str) -> Result<Bytes> {
        let prefixed = self.prefixed_key(key);
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&prefixed)
            .send()
            .await
            .with_context(|| format!("Failed to get {key}"))?;

        let bytes = resp
            .body
            .collect()
            .await
            .with_context(|| format!("Failed to read bytes from {key}"))?
            .into_bytes();

        Ok(bytes)
    }

    /// Get an object as a streaming body.
    pub async fn get_object_stream(&self, key: &str) -> Result<ByteStream> {
        let prefixed = self.prefixed_key(key);
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&prefixed)
            .send()
            .await
            .with_context(|| format!("Failed to get {key}"))?;
        Ok(resp.body)
    }

    /// Put object with optional conditional headers.
    /// - if_match: ETag for update (returns precondition_failed=true on mismatch)
    /// - if_none_match: true = create only if not exists (returns precondition_failed=true if exists)
    ///
    /// Returns (etag, precondition_failed).
    pub async fn put_object_conditional(
        &self,
        key: &str,
        data: Bytes,
        if_match: Option<&str>,
        if_none_match: bool,
    ) -> Result<(String, bool)> {
        let prefixed = self.prefixed_key(key);

        let mut req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&prefixed)
            .body(ByteStream::from(data));

        if let Some(etag) = if_match {
            req = req.if_match(etag);
        }
        if if_none_match {
            req = req.if_none_match("*");
        }

        match req.send().await {
            Ok(resp) => {
                let etag = resp.e_tag().unwrap_or("").to_string();
                Ok((etag, false))
            }
            Err(e) => {
                if is_precondition_failed(&e) {
                    Ok((String::new(), true))
                } else {
                    Err(e).with_context(|| format!("Failed to put {key}"))
                }
            }
        }
    }

    /// List objects under prefix with metadata.
    /// Returns Vec of (key, size, last_modified) tuples.
    /// Keys are returned relative to the storage prefix.
    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<(String, u64, DateTime<Utc>)>> {
        let prefixed = self.prefixed_key(prefix);
        let mut results = Vec::new();
        let mut continuation: Option<String> = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefixed);

            if let Some(token) = continuation.take() {
                req = req.continuation_token(token);
            }

            let resp = req.send().await?;

            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    let size = obj.size().unwrap_or(0) as u64;
                    let last_modified = obj
                        .last_modified()
                        .and_then(|dt| DateTime::from_timestamp(dt.secs(), dt.subsec_nanos()))
                        .unwrap_or_else(Utc::now);

                    let relative_key = match &self.prefix {
                        Some(p) => {
                            let prefix_with_slash = format!("{p}/");
                            key.strip_prefix(&prefix_with_slash)
                                .unwrap_or(key)
                                .to_string()
                        }
                        None => key.to_string(),
                    };
                    results.push((relative_key, size, last_modified));
                }
            }

            match resp.next_continuation_token() {
                Some(token) => continuation = Some(token.to_string()),
                None => break,
            }
        }

        Ok(results)
    }

    /// Stream objects from multiple shard prefixes in parallel.
    /// Returns (key, size, last_modified) tuples.
    pub fn list_shards_parallel(
        &self,
        base_prefix: &str,
        shard_concurrency: usize,
    ) -> BoxStream<'static, Result<(String, u64, DateTime<Utc>)>> {
        let storage = self.clone();
        let base_prefix = base_prefix.to_owned();
        futures::stream::iter(0..=255u8)
            .map(move |shard| {
                let storage = storage.clone();
                let prefix = format!("{base_prefix}{shard:02x}/");
                async move {
                    match storage.list_objects(&prefix).await {
                        Ok(items) => items.into_iter().map(Ok).collect(),
                        Err(error) => vec![Err(error)],
                    }
                }
            })
            .buffer_unordered(shard_concurrency.max(1))
            .flat_map(futures::stream::iter)
            .boxed()
    }

    /// Bulk delete objects using S3 batch delete API.
    pub async fn delete_objects_bulk(
        &self,
        keys: Vec<String>,
        concurrency: usize,
    ) -> Result<DeleteStats> {
        if keys.is_empty() {
            return Ok(DeleteStats::default());
        }

        let total = keys.len();
        let concurrency = concurrency.max(1);

        // S3 batch delete supports up to 1000 objects per request
        const BATCH_SIZE: usize = 1000;

        let success_count = Arc::new(AtomicU64::new(0));
        let error_count = Arc::new(AtomicU64::new(0));
        let throttle_count = Arc::new(AtomicU64::new(0));
        let batches_done = Arc::new(AtomicU64::new(0));

        // Group keys by shard prefix so each batch only hits one S3 partition
        let batches = batch_keys_by_shard(keys, BATCH_SIZE);
        let num_batches = batches.len();

        println!(
            "Deleting {total} objects in {num_batches} batches with concurrency {concurrency}"
        );

        futures::stream::iter(batches)
            .map(|batch| {
                let client = self.client.clone();
                let bucket = self.bucket.clone();
                let prefix = self.prefix.clone();
                let success_count = success_count.clone();
                let error_count = error_count.clone();
                let throttle_count = throttle_count.clone();
                let batches_done = batches_done.clone();

                async move {
                    let objects: Vec<ObjectIdentifier> = batch
                        .iter()
                        .filter_map(|key| {
                            let prefixed = match &prefix {
                                Some(p) => format!("{p}/{key}"),
                                None => key.clone(),
                            };
                            ObjectIdentifier::builder().key(prefixed).build().ok()
                        })
                        .collect();

                    if objects.is_empty() {
                        return;
                    }

                    let delete = match Delete::builder()
                        .set_objects(Some(objects))
                        .quiet(true)
                        .build()
                    {
                        Ok(d) => d,
                        Err(e) => {
                            eprintln!("Failed to build delete: {e}");
                            let _ = error_count.fetch_add(batch.len() as u64, Ordering::Relaxed);
                            return;
                        }
                    };

                    let retry_cfg = RetryConfig {
                        max_attempts: 5,
                        base_delay_ms: 200,
                    };
                    let batch_result = with_retry(&retry_cfg, "delete_objects(batch)", |_| {
                        let client = client.clone();
                        let bucket = bucket.clone();
                        let delete = delete.clone();
                        async move {
                            match client
                                .delete_objects()
                                .bucket(&bucket)
                                .delete(delete)
                                .send()
                                .await
                            {
                                Ok(v) => RetryResult::Ok(v),
                                Err(e) if S3ErrorKind::from_sdk(&e).is_retryable() => {
                                    RetryResult::Retry(e)
                                }
                                Err(e) => RetryResult::Fatal(e),
                            }
                        }
                    })
                    .await;

                    let did_fallback = match batch_result {
                        Ok(resp) => {
                            let errors_len = resp.errors().len() as u64;
                            let deleted = batch.len() as u64 - errors_len;
                            let _ = success_count.fetch_add(deleted, Ordering::Relaxed);

                            for err in resp.errors() {
                                let _ = error_count.fetch_add(1, Ordering::Relaxed);
                                eprintln!(
                                    "Delete error for {}: {} - {}",
                                    err.key().unwrap_or("?"),
                                    err.code().unwrap_or("?"),
                                    err.message().unwrap_or("?")
                                );
                                if S3ErrorKind::from_code(err.code()) == S3ErrorKind::Throttle {
                                    let _ = throttle_count.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            false
                        }
                        Err(e) if !S3ErrorKind::from_sdk(&e).is_retryable() => {
                            let reason = if let Some(se) = e.as_service_error() {
                                format!(
                                    "code={}, msg={}",
                                    se.meta().code().unwrap_or("?"),
                                    se.meta().message().unwrap_or("?")
                                )
                            } else {
                                format!("{e:#}")
                            };
                            eprintln!(
                                "Batch delete not supported ({reason}). Falling back to individual deletes..."
                            );
                            true
                        }
                        Err(e) => {
                            let detail = if let Some(se) = e.as_service_error() {
                                format!(
                                    "code={}, msg={}",
                                    se.meta().code().unwrap_or("?"),
                                    se.meta().message().unwrap_or("?")
                                )
                            } else {
                                format!("{e:#}")
                            };
                            eprintln!(
                                "Batch delete failed after {} attempts: {detail}",
                                retry_cfg.max_attempts
                            );
                            let _ = error_count.fetch_add(batch.len() as u64, Ordering::Relaxed);
                            let _ = throttle_count.fetch_add(batch.len() as u64, Ordering::Relaxed);
                            false
                        }
                    };

                    if did_fallback {
                        // Within-batch parallel fallback so a provider that
                        // lacks DeleteObjects doesn't collapse throughput to 1
                        // request at a time per batch.
                        const FALLBACK_CONCURRENCY: usize = 16;
                        futures::stream::iter(batch)
                            .for_each_concurrent(FALLBACK_CONCURRENCY, |key| {
                                let client = client.clone();
                                let bucket = bucket.clone();
                                let prefix = prefix.clone();
                                let success_count = success_count.clone();
                                let error_count = error_count.clone();
                                async move {
                                    let prefixed = match &prefix {
                                        Some(p) => format!("{p}/{key}"),
                                        None => key.clone(),
                                    };
                                    match client
                                        .delete_object()
                                        .bucket(&bucket)
                                        .key(prefixed)
                                        .send()
                                        .await
                                    {
                                        Ok(_) => {
                                            let _ = success_count.fetch_add(1, Ordering::Relaxed);
                                        }
                                        Err(e2) => {
                                            let _ = error_count.fetch_add(1, Ordering::Relaxed);
                                            if let Some(se) = e2.as_service_error() {
                                                eprintln!(
                                                    "Delete failed for {}: code={} msg={}",
                                                    key,
                                                    se.code().unwrap_or("?"),
                                                    se.message().unwrap_or("?")
                                                );
                                            } else {
                                                eprintln!("Delete failed for {key}: {e2:#}");
                                            }
                                        }
                                    }
                                }
                            })
                            .await;
                    }

                    let done = batches_done.fetch_add(1, Ordering::Relaxed) + 1;
                    if done.is_multiple_of(10) || done == num_batches as u64 {
                        println!(
                            "Delete progress: {}/{} objects ({}/{} batches)",
                            success_count.load(Ordering::Relaxed),
                            total,
                            done,
                            num_batches
                        );
                    }
                }
            })
            .buffer_unordered(concurrency)
            .for_each(|()| async {})
            .await;

        let final_success = success_count.load(Ordering::Relaxed);
        let final_errors = error_count.load(Ordering::Relaxed);
        let final_throttles = throttle_count.load(Ordering::Relaxed);

        if final_errors > 0 {
            eprintln!("Delete completed: {final_success} succeeded, {final_errors} failed");
            if final_throttles > 0 {
                eprintln!(
                    "{final_throttles} failures were due to S3 throttling. Re-run gc-all to retry."
                );
            }
        } else {
            println!("Deleted {final_success} objects");
        }

        Ok(DeleteStats {
            success: final_success,
            errors: final_errors,
            throttled: final_throttles,
        })
    }
}

/// Statistics from a bulk delete operation
#[derive(Debug, Default)]
pub struct DeleteStats {
    pub success: u64,
    pub errors: u64,
    pub throttled: u64,
}

/// Check if an SDK error is a 412 Precondition Failed.
fn is_precondition_failed<E>(e: &SdkError<E>) -> bool {
    match e {
        SdkError::ServiceError(se) => se.raw().status().as_u16() == 412,
        _ => false,
    }
}

fn sanitize_s3_prefix(prefix: Option<String>) -> Option<String> {
    prefix.and_then(|p| {
        let trimmed = p.trim_matches('/');
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

/// Compute optimal chunk size for a file of known size.
fn compute_chunk_size(total_size: u64, min_chunk: u64, max_chunk: u64, target_parts: u64) -> u64 {
    (total_size / target_parts.max(1))
        .max(min_chunk)
        .min(max_chunk)
}

fn completed_part_from_upload(part_number: i32, resp: &UploadPartOutput) -> CompletedPart {
    let mut part_builder = CompletedPart::builder().part_number(part_number);
    if let Some(etag) = resp.e_tag() {
        part_builder = part_builder.e_tag(etag);
    }
    if let Some(crc) = resp.checksum_crc32() {
        part_builder = part_builder.checksum_crc32(crc);
    }
    part_builder.build()
}

/// Encode CRC32 as base64 (AWS S3 format)
fn base64_encode_crc32(crc: u32) -> String {
    aws_smithy_types::base64::encode(crc.to_be_bytes())
}

/// Batch keys by shard prefix for partition-aware bulk deletes.
///
/// Groups keys by their 2-character shard prefix (from `base/data/blobs/XX/...`)
/// and creates batches within each shard. This ensures each DeleteObjects call
/// only hits keys from one S3 partition, avoiding cross-partition throttling.
fn batch_keys_by_shard(keys: Vec<String>, batch_size: usize) -> Vec<Vec<String>> {
    let mut by_shard: HashMap<String, Vec<String>> = HashMap::new();
    for key in keys {
        let shard = key
            .strip_prefix("base/data/blobs/")
            .and_then(|s| s.get(..2))
            .unwrap_or("??")
            .to_string();
        by_shard.entry(shard).or_default().push(key);
    }

    by_shard
        .into_values()
        .flat_map(|shard_keys| {
            shard_keys
                .chunks(batch_size)
                .map(<[String]>::to_vec)
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Tracks staging and S3 upload progress.
#[derive(Debug)]
pub struct UploadProgress {
    start: Instant,
    // Staging progress (hardlink/copy into local CAS)
    staging_parts_total: AtomicU64,
    staging_parts_done: AtomicU64,
    // Upload progress (bytes uploaded to S3)
    upload_parts_total: AtomicU64,
    upload_parts_done: AtomicU64,
    upload_total_bytes_est: AtomicU64,
    uploaded_bytes: AtomicU64,
    // Logging state
    last_log_ms: AtomicU64, // millis since start, avoids mutex
    last_uploaded_bytes: AtomicU64,
    /// When true, suppress println logging (TUI owns the terminal).
    suppress_log: std::sync::atomic::AtomicBool,
    // S3 retry/throttle/error counters
    retries: AtomicU64,
    throttles: AtomicU64,
    errors: AtomicU64,
}

impl UploadProgress {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            staging_parts_total: AtomicU64::new(0),
            staging_parts_done: AtomicU64::new(0),
            upload_parts_total: AtomicU64::new(0),
            upload_parts_done: AtomicU64::new(0),
            upload_total_bytes_est: AtomicU64::new(0),
            uploaded_bytes: AtomicU64::new(0),
            last_log_ms: AtomicU64::new(0),
            last_uploaded_bytes: AtomicU64::new(0),
            suppress_log: std::sync::atomic::AtomicBool::new(false),
            retries: AtomicU64::new(0),
            throttles: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    /// Suppress println logging (when TUI owns the terminal).
    pub fn suppress_logging(&self) {
        self.suppress_log.store(true, Ordering::Relaxed);
    }

    pub fn set_staging_total(&self, parts: u64) {
        self.staging_parts_total.store(parts, Ordering::Relaxed);
        self.staging_parts_done.store(0, Ordering::Relaxed);
        self.maybe_log_progress();
    }

    pub fn record_part_staged(&self) {
        let _ = self.staging_parts_done.fetch_add(1, Ordering::Relaxed);
        self.maybe_log_progress();
    }

    pub fn set_upload_total(&self, parts: u64) {
        self.upload_parts_total.store(parts, Ordering::Relaxed);
        self.upload_parts_done.store(0, Ordering::Relaxed);
        self.upload_total_bytes_est.store(0, Ordering::Relaxed);
        self.uploaded_bytes.store(0, Ordering::Relaxed);
        self.last_uploaded_bytes.store(0, Ordering::Relaxed);
        self.maybe_log_progress();
    }

    pub fn add_upload_total_bytes_est(&self, bytes: u64) {
        let _ = self
            .upload_total_bytes_est
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_upload_done(&self) {
        let _ = self.upload_parts_done.fetch_add(1, Ordering::Relaxed);
        self.maybe_log_progress();
    }

    /// Current cumulative uploaded bytes (atomic, no event-queue lag).
    pub fn uploaded_bytes(&self) -> u64 {
        self.uploaded_bytes.load(Ordering::Relaxed)
    }

    pub fn record_bytes(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let _ = self.uploaded_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.maybe_log_progress();
    }

    pub fn record_retry(&self) {
        let _ = self.retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_throttle(&self) {
        let _ = self.throttles.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        let _ = self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn retries(&self) -> u64 {
        self.retries.load(Ordering::Relaxed)
    }

    pub fn throttles(&self) -> u64 {
        self.throttles.load(Ordering::Relaxed)
    }

    pub fn errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }

    fn maybe_log_progress(&self) {
        if self.suppress_log.load(Ordering::Relaxed) {
            return;
        }
        let elapsed = self.start.elapsed();
        let now_ms = elapsed.as_millis() as u64;
        let last_ms = self.last_log_ms.load(Ordering::Relaxed);
        if now_ms < last_ms + 1000 {
            return;
        }
        if self
            .last_log_ms
            .compare_exchange(last_ms, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let delta_ms = now_ms.saturating_sub(last_ms).max(1);
        let delta_secs = delta_ms as f64 / 1000.0;

        let staging_total = self.staging_parts_total.load(Ordering::Relaxed);
        let staging_done = self.staging_parts_done.load(Ordering::Relaxed);

        let upload_total = self.upload_parts_total.load(Ordering::Relaxed);
        let upload_done = self.upload_parts_done.load(Ordering::Relaxed);
        let upload_total_bytes = self.upload_total_bytes_est.load(Ordering::Relaxed);
        let uploaded = self.uploaded_bytes.load(Ordering::Relaxed);

        let prev_uploaded = self.last_uploaded_bytes.swap(uploaded, Ordering::Relaxed);

        let upload_rate = ((uploaded.saturating_sub(prev_uploaded)) as f64 / delta_secs) as u64;

        let mut parts = Vec::new();

        if staging_total > 0 {
            parts.push(format!("stage {staging_done}/{staging_total}"));
        }

        if upload_total > 0 || uploaded > 0 {
            let pct = uploaded
                .saturating_mul(100)
                .checked_div(upload_total_bytes)
                .unwrap_or(0)
                .min(100);
            parts.push(format!(
                "up {upload_done}/{upload_total} {} ({}%) @ {}/s",
                crate::util::format_bytes(uploaded),
                pct,
                crate::util::format_bytes(upload_rate),
            ));
        }

        if !parts.is_empty() {
            println!("Progress: {}", parts.join(" | "));
        }
    }
}

impl Default for UploadProgress {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_MIN: u64 = DEFAULT_MIN_PART_SIZE;
    const TEST_MAX: u64 = DEFAULT_MAX_PART_SIZE;
    const TEST_TARGET: u64 = DEFAULT_TARGET_PARTS;

    #[test]
    fn test_compute_chunk_size_small_file() {
        let size = 100 * 1024 * 1024;
        assert_eq!(
            compute_chunk_size(size, TEST_MIN, TEST_MAX, TEST_TARGET),
            TEST_MIN
        );
    }

    #[test]
    fn test_compute_chunk_size_medium_file() {
        let size = 10 * 1024 * 1024 * 1024;
        let chunk = compute_chunk_size(size, TEST_MIN, TEST_MAX, TEST_TARGET);
        assert_eq!(chunk, size / TEST_TARGET);
        assert!(chunk >= TEST_MIN);
        assert!(chunk <= TEST_MAX);
    }

    #[test]
    fn test_compute_chunk_size_large_file() {
        let size = 1024 * 1024 * 1024 * 1024u64;
        assert_eq!(
            compute_chunk_size(size, TEST_MIN, TEST_MAX, TEST_TARGET),
            TEST_MAX
        );
    }

    #[test]
    fn test_compute_chunk_size_target_parts() {
        let size = 40 * 1024 * 1024 * 1024u64;
        let chunk = compute_chunk_size(size, TEST_MIN, TEST_MAX, TEST_TARGET);
        let parts = size.div_ceil(chunk);
        assert!((TEST_TARGET..=TEST_TARGET + 1).contains(&parts));
    }

    #[test]
    fn test_compute_chunk_size_bounds() {
        assert!(compute_chunk_size(0, TEST_MIN, TEST_MAX, TEST_TARGET) >= TEST_MIN);
        assert!(compute_chunk_size(u64::MAX, TEST_MIN, TEST_MAX, TEST_TARGET) <= TEST_MAX);
    }

    #[test]
    fn test_compute_chunk_size_custom_bounds() {
        let custom_min = 32 * 1024 * 1024;
        let custom_max = 128 * 1024 * 1024;
        let size = 100 * 1024 * 1024;
        assert_eq!(
            compute_chunk_size(size, custom_min, custom_max, TEST_TARGET),
            custom_min
        );

        let size = 100 * 1024 * 1024 * 1024u64;
        assert_eq!(
            compute_chunk_size(size, custom_min, custom_max, TEST_TARGET),
            custom_max
        );
    }

    #[test]
    fn test_base64_encode_crc32() {
        let encoded = base64_encode_crc32(0);
        assert_eq!(encoded, "AAAAAA==");

        let encoded = base64_encode_crc32(0xDEAD_BEEF);
        assert_eq!(encoded, "3q2+7w==");
    }

    #[test]
    fn test_s3_error_kind_from_code_throttles() {
        for code in [
            "SlowDown",
            "Throttling",
            "ThrottlingException",
            "RequestThrottled",
        ] {
            assert_eq!(
                S3ErrorKind::from_code(Some(code)),
                S3ErrorKind::Throttle,
                "code {code} should be Throttle"
            );
        }
    }

    #[test]
    fn test_s3_error_kind_from_code_transient() {
        for code in ["InternalError", "ServiceUnavailable", "RequestTimeout"] {
            assert_eq!(
                S3ErrorKind::from_code(Some(code)),
                S3ErrorKind::Transient,
                "code {code} should be Transient"
            );
        }
    }

    #[test]
    fn test_s3_error_kind_from_code_fatal() {
        assert_eq!(S3ErrorKind::from_code(None), S3ErrorKind::Fatal);
        assert_eq!(
            S3ErrorKind::from_code(Some("NoSuchKey")),
            S3ErrorKind::Fatal
        );
        assert_eq!(
            S3ErrorKind::from_code(Some("AccessDenied")),
            S3ErrorKind::Fatal
        );
        assert_eq!(S3ErrorKind::from_code(Some("")), S3ErrorKind::Fatal);
    }

    #[test]
    fn test_s3_error_kind_is_retryable() {
        assert!(S3ErrorKind::Throttle.is_retryable());
        assert!(S3ErrorKind::Transient.is_retryable());
        assert!(!S3ErrorKind::Fatal.is_retryable());
    }

    #[test]
    fn test_batch_keys_by_shard_groups_correctly() {
        let keys = vec![
            "base/data/blobs/aa/hash1".to_string(),
            "base/data/blobs/aa/hash2".to_string(),
            "base/data/blobs/bb/hash3".to_string(),
            "base/data/blobs/aa/hash4".to_string(),
            "base/data/blobs/cc/hash5".to_string(),
        ];

        let batches = batch_keys_by_shard(keys, 1000);

        // Should produce 3 batches (one per shard)
        assert_eq!(batches.len(), 3);

        // Each batch should only contain keys from one shard
        for batch in &batches {
            let first_shard: String = batch[0]
                .strip_prefix("base/data/blobs/")
                .unwrap()
                .chars()
                .take(2)
                .collect();

            for key in batch {
                assert!(key.starts_with(&format!("base/data/blobs/{first_shard}/")));
            }
        }
    }
}
