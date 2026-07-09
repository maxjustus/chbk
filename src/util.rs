pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    if bytes == 0 {
        return "0 B".to_string();
    }
    let mut value = bytes as f64;
    let mut unit_index = 0;
    while value >= 1024.0 && unit_index < UNITS.len() - 1 {
        value /= 1024.0;
        unit_index += 1;
    }
    // unit_index is bounded by UNITS.len() - 1 from the loop guard above
    #[allow(clippy::indexing_slicing)]
    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.2} {}", value, UNITS[unit_index])
    }
}

/// Extract 2-char shard prefix from hash.
#[inline]
fn blob_shard(hash: &str) -> &str {
    // len >= 2 checked; str is ASCII hex so byte offset == char offset
    #[allow(clippy::indexing_slicing)]
    if hash.len() >= 2 { &hash[0..2] } else { "__" }
}

/// Return the remote object key for a blob hash.
pub fn blob_remote_key(hash: &str) -> String {
    format!("base/data/blobs/{}/{}", blob_shard(hash), hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1), "1 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(10240), "10.00 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 100), "100.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
        assert_eq!(format_bytes(1024u64 * 1024 * 1024 * 1024), "1.00 TB");
        assert_eq!(format_bytes(1024u64 * 1024 * 1024 * 1024 * 5), "5.00 TB");
    }

    // ============ blob_shard ============

    #[test]
    fn test_blob_shard() {
        assert_eq!(blob_shard("abcdef1234567890abcdef1234567890"), "ab");
        assert_eq!(blob_shard("ff00112233445566778899aabbccddeeff"), "ff");
        assert_eq!(blob_shard("a"), "__");
        assert_eq!(blob_shard(""), "__");
        assert_eq!(blob_shard("ab"), "ab");
    }
}
