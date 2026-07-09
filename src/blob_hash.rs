//! Blob hash utilities.
//!
//! Provides a compact 128-bit hash representation and conversions to/from hex strings.

/// 128-bit blob hash stored as raw bytes (16 bytes vs 32+ bytes for hex string).
/// Used for efficient set operations and comparisons.
pub type BlobHash = [u8; 16];

/// Convert hex string to BlobHash. Returns None if invalid.
#[inline]
// chunks(2) on 32-byte string guarantees 2-byte chunks; i bounded by 16
#[allow(clippy::indexing_slicing)]
pub fn from_hex(s: &str) -> Option<BlobHash> {
    if s.len() != 32 {
        return None;
    }
    let mut result = [0u8; 16];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let high = hex_digit(chunk[0])?;
        let low = hex_digit(chunk[1])?;
        result[i] = (high << 4) | low;
    }
    Some(result)
}

#[inline]
const fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn to_hex(h: &BlobHash) -> String {
        const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
        let mut result = String::with_capacity(32);
        for byte in h {
            result.push(HEX_CHARS[(byte >> 4) as usize] as char);
            result.push(HEX_CHARS[(byte & 0x0f) as usize] as char);
        }
        result
    }

    #[test]
    fn test_from_hex_valid() {
        let hash = from_hex("abcdef0123456789abcdef0123456789");
        assert!(hash.is_some());
        let h = hash.unwrap();
        assert_eq!(h[0], 0xab);
        assert_eq!(h[1], 0xcd);
    }

    #[test]
    fn test_from_hex_invalid_length() {
        assert!(from_hex("abc").is_none());
        assert!(from_hex("abcdef0123456789abcdef012345678").is_none()); // 31 chars
        assert!(from_hex("abcdef0123456789abcdef01234567890").is_none()); // 33 chars
    }

    #[test]
    fn test_from_hex_invalid_chars() {
        assert!(from_hex("ghijkl0123456789abcdef0123456789").is_none());
        assert!(from_hex("ABCDEF0123456789abcdef0123456789").is_some()); // uppercase valid
    }

    #[test]
    fn test_roundtrip() {
        let original = "deadbeef0123456789abcdef01234567";
        let hash = from_hex(original).unwrap();
        let back = to_hex(&hash);
        assert_eq!(back, original);
    }
}
