//! Shared MPEG/Annex-B start-code scanning helpers.
//!
//! H.264, HEVC, MPEG-2 and the MPEG-2 Program Stream demuxer all locate the
//! 3-byte `00 00 01` start-code prefix to delimit NAL units / PES units. A
//! single memchr-backed implementation lives here so every caller gets the
//! same SIMD-accelerated scan instead of a hand-rolled byte-by-byte loop.

/// Find the position of the next start code (`00 00 01`) at or after `from`.
///
/// Backed by `memchr::memmem::find` for SIMD-accelerated bytestring search. On
/// AVX2-capable x86_64 this runs several times faster than a byte-by-byte scan;
/// on a 200 KB UHD HEVC frame the saving is in the hundreds of microseconds per
/// call. The reported offset is the start of the `00 00 01` triple, so a 4-byte
/// `00 00 00 01` start code is reported at the second `00`.
pub fn find_start_code(data: &[u8], from: usize) -> Option<usize> {
    if data.len() < from + 3 {
        return None;
    }
    memchr::memmem::find(&data[from..], b"\x00\x00\x01").map(|rel| from + rel)
}

/// Skip past the start code at position `pos`, returning the first byte after
/// it. Handles both the 3-byte (`00 00 01`) and 4-byte (`00 00 00 01`) forms.
/// Returns `None` if `pos` does not begin a start code or the buffer is too
/// short to contain one.
pub fn skip_start_code(data: &[u8], pos: usize) -> Option<usize> {
    if pos + 2 >= data.len() {
        return None;
    }
    if data[pos] == 0x00 && data[pos + 1] == 0x00 {
        if pos + 3 < data.len() && data[pos + 2] == 0x00 && data[pos + 3] == 0x01 {
            return Some(pos + 4); // 4-byte start code
        }
        if data[pos + 2] == 0x01 {
            return Some(pos + 3); // 3-byte start code
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_start_code_3byte() {
        let data = [0x00, 0x00, 0x01, 0x65];
        assert_eq!(find_start_code(&data, 0), Some(0));
    }

    #[test]
    fn find_start_code_4byte() {
        let data = [0x00, 0x00, 0x00, 0x01, 0x65];
        // The 00 00 01 triple starts at offset 1 in a 4-byte start code.
        assert_eq!(find_start_code(&data, 0), Some(1));
    }

    #[test]
    fn find_start_code_offset() {
        let data = [0xFF, 0xFF, 0x00, 0x00, 0x01, 0x09];
        assert_eq!(find_start_code(&data, 0), Some(2));
    }

    #[test]
    fn find_start_code_none() {
        let data = [0x00, 0x00, 0x00, 0x00];
        assert_eq!(find_start_code(&data, 0), None);
    }

    #[test]
    fn find_start_code_too_short() {
        let data = [0x00, 0x00];
        assert_eq!(find_start_code(&data, 0), None);
    }

    #[test]
    fn skip_3byte() {
        let data = [0x00, 0x00, 0x01, 0x65];
        assert_eq!(skip_start_code(&data, 0), Some(3));
    }

    #[test]
    fn skip_4byte() {
        let data = [0x00, 0x00, 0x00, 0x01, 0x65];
        assert_eq!(skip_start_code(&data, 0), Some(4));
    }

    #[test]
    fn skip_not_a_start_code() {
        let data = [0xFF, 0x00, 0x01, 0x65];
        assert_eq!(skip_start_code(&data, 0), None);
    }
}
