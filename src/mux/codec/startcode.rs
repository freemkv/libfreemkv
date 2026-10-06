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

// MSB-first bit reader over an RBSP, for the leading fields of a coded slice header. Does NOT
// de-emulate `00 00 03`; safe only for the leading fields.
pub(crate) struct BitReader<'a> {
    data: &'a [u8],
    bit: usize,
}

impl<'a> BitReader<'a> {
    /// Reader positioned at the first bit of `data`.
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, bit: 0 }
    }

    /// Read a single bit, MSB-first. `None` once the buffer is exhausted.
    pub fn read_bit(&mut self) -> Option<u32> {
        let byte = self.bit / 8;
        if byte >= self.data.len() {
            return None;
        }
        let b = (self.data[byte] >> (7 - (self.bit & 7))) & 1;
        self.bit += 1;
        Some(b as u32)
    }

    /// Read `n` bits, MSB-first, into the low `n` bits of a `u32`. Returns
    /// `None` if fewer than `n` bits remain (the same exhaustion contract as
    /// [`read_bit`](Self::read_bit)), leaving the reader's position unspecified.
    /// `n` must be `<= 32`; a larger `n` shifts the earliest bits out of the
    /// `u32`, keeping only the last 32. `read_bits(0)` yields `Some(0)`.
    pub fn read_bits(&mut self, n: u32) -> Option<u32> {
        let mut v = 0u32;
        for _ in 0..n {
            v = (v << 1) | self.read_bit()?;
        }
        Some(v)
    }

    /// Skip `n` bits; `None` if that would run past the end.
    pub fn skip_bits(&mut self, n: u32) -> Option<()> {
        for _ in 0..n {
            self.read_bit()?;
        }
        Some(())
    }

    /// Read an unsigned Exp-Golomb code `ue(v)` (H.264 §9.1 / HEVC §9.2):
    /// count leading zeros, read the `1` stop bit, then that many info bits;
    /// `code_num = 2^leadingZeros - 1 + info`. `None` on truncation or an
    /// absurdly long code (>31 leading zeros — malformed input, not a real
    /// slice header).
    pub fn read_ue(&mut self) -> Option<u32> {
        let mut leading_zeros = 0u32;
        while self.read_bit()? == 0 {
            leading_zeros += 1;
            if leading_zeros > 31 {
                return None;
            }
        }
        let mut info = 0u32;
        for _ in 0..leading_zeros {
            info = (info << 1) | self.read_bit()?;
        }
        Some((1u32 << leading_zeros) - 1 + info)
    }
}

#[cfg(test)]
#[path = "startcode_tests.rs"]
mod tests;
