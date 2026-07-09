//! S3-native snapshot manifests.
//!
//! Each snapshot is one self-contained record at `snapshots/{name}.json.zst`.
//! Contains blob references, DDL payloads, and identity.

use crate::storage::Storage;
use anyhow::{Context, Result, bail};
use aws_smithy_types::base64;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub const MANIFEST_PREFIX: &str = "snapshots/";
pub const MANIFEST_SUFFIX: &str = ".json.zst";

const ZSTD_LEVEL: i32 = 3;
/// Timestamp format for auto-generated names. Lexicographically sortable so
/// filename ordering matches chronological ordering — useful for "latest
/// manifest" lookups without downloading.
pub const AUTO_TIMESTAMP_FMT: &str = "y%Ym%md%d_h%Hm%Ms%S";
/// Length of the uuid suffix appended to auto-generated names.
const AUTO_UUID_LEN: usize = 8;

/// Self-contained snapshot record stored at `snapshots/{name}.json.zst`.
///
/// The S3 key is authoritative for `name`: `read_manifest` overwrites the
/// deserialized field with the key-derived name so that any drift between the
/// key and the body is invisible to callers. The field stays in the wire
/// format for operators doing `aws s3 cp ... - | zstd -d | jq`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub name: String,
    pub timestamp: i64,
    pub shard: String,
    pub replica: String,
    pub created_by: String,
    pub parts: Vec<ManifestPart>,
    pub files: Vec<ManifestFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestPart {
    pub database: String,
    pub table_name: String,
    pub part_name: String,
    pub blob_hash: String,
    pub blob_size: u64,
}

/// A metadata file (DDL, user-defined function, etc.) embedded in the manifest.
/// `content_b64` is always base64 — covers SQL text and binary user_scripts/user_defined files.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestFile {
    pub path: String,
    pub content_b64: String,
}

impl ManifestFile {
    pub fn from_bytes(path: impl Into<String>, content: &[u8]) -> Self {
        Self {
            path: path.into(),
            content_b64: base64::encode(content),
        }
    }

    pub fn decode_content(&self) -> Result<Vec<u8>> {
        base64::decode(&self.content_b64)
            .with_context(|| format!("invalid base64 for {}", self.path))
    }
}

pub fn manifest_key(name: &str) -> String {
    format!("{MANIFEST_PREFIX}{name}{MANIFEST_SUFFIX}")
}

/// Extract the manifest name from an S3 key. Returns `None` for keys that don't
/// match the expected `snapshots/{name}.json.zst` shape.
pub fn manifest_name_from_key(key: &str) -> Option<&str> {
    key.strip_prefix(MANIFEST_PREFIX)
        .and_then(|s| s.strip_suffix(MANIFEST_SUFFIX))
}

/// Build an auto-generated manifest name: `live_{shard}_{replica}_{ts}_{uuid8}`.
/// The UUID suffix guarantees uniqueness across concurrent backups on the same replica.
pub fn build_auto_name(shard: &str, replica: &str, ts: DateTime<Utc>, uuid: &str) -> String {
    let short_uuid: String = uuid
        .chars()
        .filter(char::is_ascii_hexdigit)
        .take(AUTO_UUID_LEN)
        .collect();
    format!(
        "live_{shard}_{replica}_{ts}_{short_uuid}",
        ts = ts.format(AUTO_TIMESTAMP_FMT),
    )
}

/// Serialize + zstd-compress a manifest.
pub fn encode(manifest: &Manifest) -> Result<Vec<u8>> {
    let json = serde_json::to_vec(manifest).context("serialize manifest")?;
    zstd::encode_all(json.as_slice(), ZSTD_LEVEL).context("compress manifest")
}

/// Zstd-decompress + deserialize a manifest.
pub fn decode(bytes: &[u8]) -> Result<Manifest> {
    let json = zstd::decode_all(bytes).context("decompress manifest")?;
    serde_json::from_slice(&json).context("deserialize manifest")
}

/// Write a manifest. Uses conditional PUT (If-None-Match: *) — manifests are
/// write-once. Errors with a distinct message on collision so callers can
/// distinguish "name taken" from generic I/O failures.
pub async fn write_manifest(storage: &Storage, manifest: &Manifest) -> Result<()> {
    let key = manifest_key(&manifest.name);
    let body = Bytes::from(encode(manifest)?);
    let (_etag, conflict) = storage
        .put_object_conditional(&key, body, None, true)
        .await
        .with_context(|| format!("write manifest {}", manifest.name))?;
    if conflict {
        bail!("manifest already exists: {}", manifest.name);
    }
    Ok(())
}

pub async fn read_manifest(storage: &Storage, name: &str) -> Result<Manifest> {
    let key = manifest_key(name);
    let bytes = storage
        .get_object(&key)
        .await
        .with_context(|| format!("read manifest {name}"))?;
    let mut m = decode(&bytes)?;
    // The S3 key is authoritative. If a body was written with a stale name
    // (e.g. an operator renamed the object), prefer the key.
    m.name = name.to_string();
    Ok(m)
}

/// List all manifest names (no downloads). Returns names only — not keys.
pub async fn list_manifest_names(storage: &Storage) -> Result<Vec<String>> {
    let entries = storage.list_objects(MANIFEST_PREFIX).await?;
    Ok(entries
        .into_iter()
        .filter_map(|(key, _size, _modified)| manifest_name_from_key(&key).map(str::to_owned))
        .collect())
}

/// Download every manifest. For GC paths that need referenced blob unions.
pub async fn read_all_manifests(storage: &Storage, concurrency: usize) -> Result<Vec<Manifest>> {
    let names = list_manifest_names(storage).await?;
    stream::iter(
        names
            .into_iter()
            .map(|name| async move { read_manifest(storage, &name).await }),
    )
    .buffer_unordered(concurrency.max(1))
    .try_collect()
    .await
}

/// Set of blob hashes referenced by a manifest's parts.
pub fn blob_hash_set(manifest: &Manifest) -> HashSet<String> {
    manifest.parts.iter().map(|p| p.blob_hash.clone()).collect()
}

pub async fn delete_manifest(storage: &Storage, name: &str) -> Result<()> {
    storage
        .delete_object(&manifest_key(name))
        .await
        .with_context(|| format!("delete manifest {name}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn sample_manifest() -> Manifest {
        Manifest {
            name: "live_01_r1_y2026m04d20_h12m30s45_abcdef12".to_string(),
            timestamp: 1_713_614_445,
            shard: "01".to_string(),
            replica: "r1".to_string(),
            created_by: "host.example".to_string(),
            parts: vec![
                ManifestPart {
                    database: "db".to_string(),
                    table_name: "t1".to_string(),
                    part_name: "all_1_1_0".to_string(),
                    blob_hash: "aa11bb22".to_string(),
                    blob_size: 1234,
                },
                ManifestPart {
                    database: "db".to_string(),
                    table_name: "t2".to_string(),
                    part_name: "all_1_1_0".to_string(),
                    blob_hash: "cc33dd44".to_string(),
                    blob_size: 5678,
                },
            ],
            files: vec![
                ManifestFile::from_bytes("metadata/db.sql", b"CREATE DATABASE db"),
                ManifestFile::from_bytes("user_scripts/bin.exe", &[0u8, 1, 2, 255, 128]),
            ],
        }
    }

    #[test]
    fn round_trip_preserves_manifest() {
        let m = sample_manifest();
        let bytes = encode(&m).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(m, decoded);
    }

    #[test]
    fn round_trip_empty_manifest() {
        let m = Manifest {
            name: "empty".to_string(),
            timestamp: 0,
            shard: String::new(),
            replica: String::new(),
            created_by: String::new(),
            parts: vec![],
            files: vec![],
        };
        let bytes = encode(&m).unwrap();
        assert_eq!(decode(&bytes).unwrap(), m);
    }

    #[test]
    fn deserialize_ignores_unknown_legacy_fields() {
        // Older manifests may have version/uuid/tables/partition; serde
        // silently drops them without #[serde(deny_unknown_fields)].
        let json = r#"{
            "version": 1,
            "name": "old",
            "timestamp": 0,
            "uuid": "ignored",
            "snapshot_type": "auto",
            "shard": "",
            "replica": "",
            "created_by": "",
            "tables": ["db.t"],
            "parts": [],
            "files": []
        }"#;
        let m: Manifest = serde_json::from_str(json).unwrap();
        assert_eq!(m.name, "old");
    }

    #[test]
    fn binary_content_round_trips() {
        let raw: Vec<u8> = (0u8..=255).collect();
        let f = ManifestFile::from_bytes("user_scripts/blob", &raw);
        assert_eq!(f.decode_content().unwrap(), raw);
    }

    #[test]
    fn blob_hash_set_is_unique_hashes() {
        let m = sample_manifest();
        let hashes = blob_hash_set(&m);
        assert_eq!(hashes.len(), 2);
        assert!(hashes.contains("aa11bb22"));
        assert!(hashes.contains("cc33dd44"));
    }

    #[test]
    fn manifest_key_and_name_round_trip() {
        let key = manifest_key("foo");
        assert_eq!(key, "snapshots/foo.json.zst");
        assert_eq!(manifest_name_from_key(&key), Some("foo"));
        assert_eq!(manifest_name_from_key("snapshots/"), None);
        assert_eq!(manifest_name_from_key("other/foo.json.zst"), None);
    }

    #[test]
    fn build_auto_name_has_expected_shape() {
        let ts = Utc.with_ymd_and_hms(2026, 4, 20, 12, 30, 45).unwrap();
        let name = build_auto_name("01", "r1", ts, "abcdef12-3456-7890-abcd-ef1234567890");
        assert_eq!(name, "live_01_r1_y2026m04d20_h12m30s45_abcdef12");
    }
}
