use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct PartInfo {
    pub database: String,
    pub table: String,
    pub name: String,
    pub path: String,
    /// Content hash of all files in this part (ClickHouse system.parts.hash_of_all_files), normalized to 32 hex chars.
    pub hash_of_all_files: String,
    /// Total bytes on disk for this part (from system.parts.bytes_on_disk).
    pub bytes_on_disk: u64,
    /// Number of rows in this part (from system.parts.rows).
    pub rows_count: u64,
}

pub fn table_map_from_parts(parts: &[PartInfo]) -> BTreeMap<String, BTreeSet<String>> {
    let mut map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for part in parts {
        let _ = map
            .entry(part.database.clone())
            .or_default()
            .insert(part.table.clone());
    }
    map
}
