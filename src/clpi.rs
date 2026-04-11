//! CLPI clip info parser — maps clips to sector ranges on disc.
//!
//! Each .clpi file in BDMV/CLIPINF/ describes one M2TS clip.
//! The EP (Entry Point) map provides timestamp → SPN mapping.
//! SPN × 192 = byte offset in the m2ts file.
//!
//! Reference: https://github.com/lw/BluRay/wiki/CLPI

use crate::disc::Extent;
use crate::error::{Error, Result};

/// Parsed CLPI clip info.
#[derive(Debug)]
pub struct ClipInfo {
    pub version: String,
    /// Total source packets in the m2ts (each 192 bytes)
    pub source_packet_count: u32,
    /// Coarse EP entries for the primary video stream
    pub ep_coarse: Vec<EpCoarse>,
    /// Fine EP entries for the primary video stream
    pub ep_fine: Vec<EpFine>,
}

#[derive(Debug, Clone)]
pub struct EpCoarse {
    pub ref_to_fine_id: u32,
    pub pts_coarse: u32,
    pub spn_coarse: u32,
}

#[derive(Debug, Clone)]
pub struct EpFine {
    pub pts_fine: u32,
    pub spn_fine: u32,
}

impl ClipInfo {
    /// Reconstruct full PTS from coarse + fine entry.
    pub fn full_pts(coarse: &EpCoarse, fine: &EpFine) -> u32 {
        (coarse.pts_coarse << 19) + (fine.pts_fine << 8)
    }

    /// Reconstruct full SPN from coarse + fine entry.
    pub fn full_spn(coarse: &EpCoarse, fine: &EpFine) -> u32 {
        (coarse.spn_coarse & 0xFFFE0000) + fine.spn_fine
    }

    /// Get all EP entries as (PTS, SPN) pairs, fully resolved.
    pub fn resolved_ep_map(&self) -> Vec<(u32, u32)> {
        let mut entries = Vec::new();

        for (ci, coarse) in self.ep_coarse.iter().enumerate() {
            let fine_start = coarse.ref_to_fine_id as usize;
            let fine_end = if ci + 1 < self.ep_coarse.len() {
                self.ep_coarse[ci + 1].ref_to_fine_id as usize
            } else {
                self.ep_fine.len()
            };

            for fi in fine_start..fine_end.min(self.ep_fine.len()) {
                let fine = &self.ep_fine[fi];
                let pts = Self::full_pts(coarse, fine);
                let spn = Self::full_spn(coarse, fine);
                entries.push((pts, spn));
            }
        }

        entries
    }

    /// Get sector extents for a given in/out time range.
    ///
    /// Converts PTS timestamps to SPN ranges, then SPN to LBA
    /// using the file's starting LBA on disc.
    pub fn get_extents(&self, in_time: u32, out_time: u32) -> Vec<Extent> {
        let ep_map = self.resolved_ep_map();
        if ep_map.is_empty() {
            return Vec::new();
        }

        // Find SPN at or before in_time
        let start_spn = match ep_map.binary_search_by_key(&in_time, |(pts, _)| *pts) {
            Ok(i) => ep_map[i].1,
            Err(0) => ep_map[0].1,
            Err(i) => ep_map[i - 1].1,
        };

        // Find SPN at or after out_time
        let end_spn = match ep_map.binary_search_by_key(&out_time, |(pts, _)| *pts) {
            Ok(i) => ep_map[i].1,
            Err(i) if i < ep_map.len() => ep_map[i].1,
            _ => ep_map.last().unwrap().1 + 1,
        };

        if end_spn <= start_spn {
            return Vec::new();
        }

        // SPN → byte offset: spn × 192
        // Byte offset → sectors: offset / 2048
        // Note: the caller needs to add the file's starting LBA from UDF
        let start_byte = start_spn as u64 * 192;
        let end_byte = end_spn as u64 * 192;
        let start_sector = (start_byte / 2048) as u32;
        let end_sector = end_byte.div_ceil(2048) as u32;

        vec![Extent {
            start_lba: start_sector, // relative to m2ts file start
            sector_count: end_sector - start_sector,
        }]
    }
}

/// Parse a CLPI file from raw bytes.
pub fn parse(data: &[u8]) -> Result<ClipInfo> {
    if data.len() < 40 {
        return Err(Error::ClpiParse);
    }

    if &data[0..4] != b"HDMV" {
        return Err(Error::ClpiParse);
    }
    let version = String::from_utf8_lossy(&data[4..8]).to_string();

    // Header offsets
    let _seq_info_start = u32::from_be_bytes([data[8], data[9], data[10], data[11]]) as usize;
    let _prog_info_start = u32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;
    let cpi_start = u32::from_be_bytes([data[16], data[17], data[18], data[19]]) as usize;

    // ClipInfo section at offset 40
    // source_packet_count at offset 40 + 4(len) + 2(reserved) + 1(stream_type) + 1(app_type) + 4(reserved) + 4(ts_rate)
    let source_packet_count = if data.len() > 56 {
        u32::from_be_bytes([data[56], data[57], data[58], data[59]])
    } else {
        0
    };

    // Parse CPI / EP Map
    let (ep_coarse, ep_fine) = if cpi_start > 0 && cpi_start + 8 < data.len() {
        parse_cpi(&data[cpi_start..])?
    } else {
        (Vec::new(), Vec::new())
    };

    Ok(ClipInfo {
        version,
        source_packet_count,
        ep_coarse,
        ep_fine,
    })
}

/// Parse the CPI section containing the EP map.
fn parse_cpi(data: &[u8]) -> Result<(Vec<EpCoarse>, Vec<EpFine>)> {
    if data.len() < 8 {
        return Ok((Vec::new(), Vec::new()));
    }

    let cpi_length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if cpi_length < 4 {
        return Ok((Vec::new(), Vec::new()));
    }

    // CPI type at bits 44-47 (byte 5, lower 4 bits)
    // Skip to EP map: offset 4 (after length) + 2 (reserved/type)
    let ep_map = &data[6..];
    if ep_map.len() < 4 {
        return Ok((Vec::new(), Vec::new()));
    }

    // EP map header
    // [0] reserved
    // [1] number of stream PID entries
    let num_streams = ep_map[1] as usize;
    if num_streams == 0 {
        return Ok((Vec::new(), Vec::new()));
    }

    // Stream PID entry headers start at offset 2
    // Each: 2(PID) + 2(reserved+type) + 2(num_coarse) + 4(num_fine) + 4(ep_map_start) = 14 bytes
    // We only care about the first stream (primary video)
    if ep_map.len() < 16 {
        return Ok((Vec::new(), Vec::new()));
    }

    // Stream PID entry — bit-packed per BD spec (libbluray clpi_parse.c):
    //   stream_PID: 16 bits           → ep_map[2..4]
    //   reserved: 10 bits             ┐
    //   EP_stream_type: 4 bits        │ ep_map[4..14] = 80 bits
    //   num_EP_coarse: 16 bits        │ (10+4+16+18+32 = 80)
    //   num_EP_fine: 18 bits          │
    //   EP_map_start_address: 32 bits ┘
    if ep_map.len() < 16 {
        return Ok((Vec::new(), Vec::new()));
    }
    let _stream_pid = u16::from_be_bytes([ep_map[2], ep_map[3]]);

    // Read 10 bytes (80 bits) from ep_map[4..14] for bit extraction
    // Use two u64s since we need 80 bits
    let hi = u64::from_be_bytes([
        ep_map[4], ep_map[5], ep_map[6], ep_map[7], ep_map[8], ep_map[9], ep_map[10], ep_map[11],
    ]);
    let lo_bytes = [ep_map[12], ep_map[13]];

    // Bit 0-9: reserved (10)
    // Bit 10-13: EP_stream_type (4)
    // Bit 14-29: num_coarse (16)
    // Bit 30-47: num_fine (18)
    // Bit 48-79: EP_map_start (32) — bits 48-63 in hi, bits 64-79 in lo
    let num_coarse = ((hi >> 34) & 0xFFFF) as usize;
    let num_fine = ((hi >> 16) & 0x3FFFF) as usize;
    let ep_map_offset = (((hi & 0xFFFF) as u32) << 16) | (u16::from_be_bytes(lo_bytes) as u32);
    let ep_map_offset = ep_map_offset as usize;

    // EP map for this stream starts at ep_map_offset relative to ep_map start
    if ep_map_offset + 4 > ep_map.len() {
        return Ok((Vec::new(), Vec::new()));
    }

    let stream_ep = &ep_map[ep_map_offset..];
    if stream_ep.len() < 4 {
        return Ok((Vec::new(), Vec::new()));
    }

    // Fine table start address (relative to this stream EP map)
    let fine_start =
        u32::from_be_bytes([stream_ep[0], stream_ep[1], stream_ep[2], stream_ep[3]]) as usize;

    // Coarse entries start at offset 4, 8 bytes each
    let coarse_data = &stream_ep[4..];
    let mut ep_coarse = Vec::with_capacity(num_coarse);
    for i in 0..num_coarse {
        let off = i * 8;
        if off + 8 > coarse_data.len() {
            break;
        }

        let dword0 = u32::from_be_bytes([
            coarse_data[off],
            coarse_data[off + 1],
            coarse_data[off + 2],
            coarse_data[off + 3],
        ]);
        let ref_to_fine_id = dword0 >> 14;
        let pts_coarse = dword0 & 0x3FFF;
        let spn_coarse = u32::from_be_bytes([
            coarse_data[off + 4],
            coarse_data[off + 5],
            coarse_data[off + 6],
            coarse_data[off + 7],
        ]);

        ep_coarse.push(EpCoarse {
            ref_to_fine_id,
            pts_coarse,
            spn_coarse,
        });
    }

    // Fine entries at fine_start, 4 bytes each
    let mut ep_fine = Vec::with_capacity(num_fine);
    if fine_start < stream_ep.len() {
        let fine_data = &stream_ep[fine_start..];
        for i in 0..num_fine {
            let off = i * 4;
            if off + 4 > fine_data.len() {
                break;
            }

            let dword = u32::from_be_bytes([
                fine_data[off],
                fine_data[off + 1],
                fine_data[off + 2],
                fine_data[off + 3],
            ]);
            // Bits: is_angle(1) + i_end_offset(3) + pts_fine(11) + spn_fine(17)
            let pts_fine = (dword >> 17) & 0x7FF;
            let spn_fine = dword & 0x1FFFF;

            ep_fine.push(EpFine { pts_fine, spn_fine });
        }
    }

    Ok((ep_coarse, ep_fine))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal CLPI binary.
    /// `cpi_data` is the raw CPI section bytes (starting with the 4-byte CPI length).
    fn build_clpi(source_packet_count: u32, cpi_data: Option<&[u8]>) -> Vec<u8> {
        // We need at least 60 bytes for the header area.
        // Offsets:
        //   0..4:   "HDMV"
        //   4..8:   "0200"
        //   8..12:  seq_info_start (unused, set to 0)
        //   12..16: prog_info_start (unused, set to 0)
        //   16..20: cpi_start
        //   20..40: reserved/padding
        //   40..56: ClipInfo section area (length + stuff before source_packet_count)
        //   56..60: source_packet_count

        let cpi_start: u32 = if cpi_data.is_some() { 60 } else { 0 };

        let mut buf = vec![0u8; 60];
        // Magic + version
        buf[0..4].copy_from_slice(b"HDMV");
        buf[4..8].copy_from_slice(b"0200");
        // seq_info_start = 0
        // prog_info_start = 0
        // cpi_start
        buf[16..20].copy_from_slice(&cpi_start.to_be_bytes());
        // source_packet_count at offset 56
        buf[56..60].copy_from_slice(&source_packet_count.to_be_bytes());

        if let Some(cpi) = cpi_data {
            buf.extend_from_slice(cpi);
        }

        buf
    }

    /// Build a CPI section with one stream's EP map.
    /// coarse_entries: Vec<(ref_to_fine_id, pts_coarse, spn_coarse)>
    /// fine_entries: Vec<(pts_fine, spn_fine)>
    fn build_cpi(
        stream_pid: u16,
        coarse_entries: &[(u32, u32, u32)],
        fine_entries: &[(u32, u32)],
    ) -> Vec<u8> {
        // CPI section layout:
        //   [0..4]   cpi_length (u32 BE)
        //   [4..6]   reserved/type (2 bytes)
        //   [6..]    EP map
        //
        // EP map layout (relative to byte 6 of CPI):
        //   [0]      reserved
        //   [1]      num_streams (1)
        //   [2..4]   stream_PID (u16 BE)
        //   [4..14]  80 bits: reserved(10) + EP_stream_type(4) + num_coarse(16) + num_fine(18) + EP_map_start(32)
        //   [14..]   (next stream entry, if any)
        //
        // Stream EP map (at EP_map_start relative to EP map start):
        //   [0..4]   fine_start (relative to stream EP map start)
        //   [4..]    coarse entries, 8 bytes each
        //   [fine_start..] fine entries, 4 bytes each

        let num_coarse = coarse_entries.len() as u32;
        let num_fine = fine_entries.len() as u32;

        // EP_map_start: offset from ep_map start where the stream EP data begins.
        // ep_map has: reserved(1) + num_streams(1) + stream_header(12) = 14 bytes
        // So EP_map_start = 14 (first stream data right after the header)
        let ep_map_start: u32 = 14;

        // Build the 80-bit stream PID entry (10 bytes: ep_map[4..14])
        // Bits: reserved(10) + EP_stream_type(4) + num_coarse(16) + num_fine(18) + EP_map_start(32)
        // Total: 80 bits = 10 bytes
        //
        // Pack into a u128 for convenience then extract 10 bytes
        let ep_stream_type: u32 = 1; // video
        let packed: u128 = ((0u128) << 70)                         // reserved: 10 bits
            | ((ep_stream_type as u128) << 66)                      // EP_stream_type: 4 bits
            | ((num_coarse as u128) << 50)                          // num_coarse: 16 bits
            | ((num_fine as u128) << 32)                            // num_fine: 18 bits
            | (ep_map_start as u128); // EP_map_start: 32 bits
        let packed_bytes = packed.to_be_bytes(); // 16 bytes, we want the last 10
        let stream_header_bits = &packed_bytes[6..16];

        // Build stream EP data
        // fine_start = 4 (header) + num_coarse * 8
        let fine_start: u32 = 4 + num_coarse * 8;
        let mut stream_ep = Vec::new();
        stream_ep.extend_from_slice(&fine_start.to_be_bytes());

        // Coarse entries: 8 bytes each
        // dword0 = (ref_to_fine_id << 14) | (pts_coarse & 0x3FFF)
        // dword1 = spn_coarse
        for &(ref_id, pts_c, spn_c) in coarse_entries {
            let dword0 = (ref_id << 14) | (pts_c & 0x3FFF);
            stream_ep.extend_from_slice(&dword0.to_be_bytes());
            stream_ep.extend_from_slice(&spn_c.to_be_bytes());
        }

        // Fine entries: 4 bytes each
        // dword = (is_angle(1) + i_end_offset(3) + pts_fine(11) + spn_fine(17))
        for &(pts_f, spn_f) in fine_entries {
            let dword: u32 = ((pts_f & 0x7FF) << 17) | (spn_f & 0x1FFFF);
            stream_ep.extend_from_slice(&dword.to_be_bytes());
        }

        // Assemble EP map
        let mut ep_map = Vec::new();
        ep_map.push(0); // reserved
        ep_map.push(1); // num_streams = 1
        ep_map.extend_from_slice(&stream_pid.to_be_bytes());
        ep_map.extend_from_slice(stream_header_bits);
        ep_map.extend_from_slice(&stream_ep);

        // Assemble CPI section
        let mut cpi = Vec::new();
        let cpi_length = (2 + ep_map.len()) as u32; // reserved/type(2) + ep_map
        cpi.extend_from_slice(&cpi_length.to_be_bytes());
        cpi.extend_from_slice(&[0u8; 2]); // reserved/type
        cpi.extend_from_slice(&ep_map);

        cpi
    }

    #[test]
    fn parse_valid_clpi() {
        let cpi = build_cpi(
            0x1011,
            &[(0, 100, 0x00020000)], // 1 coarse
            &[(50, 1024)],           // 1 fine
        );
        let data = build_clpi(500_000, Some(&cpi));

        let clip = parse(&data).expect("should parse valid CLPI");
        assert_eq!(clip.version, "0200");
        assert_eq!(clip.source_packet_count, 500_000);
        assert_eq!(clip.ep_coarse.len(), 1);
        assert_eq!(clip.ep_fine.len(), 1);
    }

    #[test]
    fn parse_ep_map() {
        let cpi = build_cpi(
            0x1011,
            &[
                (0, 100, 0x00020000), // coarse 0: fine starts at 0, pts_coarse=100, spn_coarse=0x20000
                (2, 200, 0x00040000), // coarse 1: fine starts at 2, pts_coarse=200, spn_coarse=0x40000
            ],
            &[
                (50, 1024),  // fine 0
                (100, 2048), // fine 1
                (25, 512),   // fine 2
                (75, 1536),  // fine 3
            ],
        );
        let data = build_clpi(1_000_000, Some(&cpi));

        let clip = parse(&data).expect("should parse EP map");
        assert_eq!(clip.ep_coarse.len(), 2);
        assert_eq!(clip.ep_fine.len(), 4);

        // Verify coarse entries
        assert_eq!(clip.ep_coarse[0].ref_to_fine_id, 0);
        assert_eq!(clip.ep_coarse[0].pts_coarse, 100);
        assert_eq!(clip.ep_coarse[0].spn_coarse, 0x00020000);
        assert_eq!(clip.ep_coarse[1].ref_to_fine_id, 2);
        assert_eq!(clip.ep_coarse[1].pts_coarse, 200);
        assert_eq!(clip.ep_coarse[1].spn_coarse, 0x00040000);

        // Verify fine entries
        assert_eq!(clip.ep_fine[0].pts_fine, 50);
        assert_eq!(clip.ep_fine[0].spn_fine, 1024);
        assert_eq!(clip.ep_fine[1].pts_fine, 100);
        assert_eq!(clip.ep_fine[1].spn_fine, 2048);
        assert_eq!(clip.ep_fine[2].pts_fine, 25);
        assert_eq!(clip.ep_fine[2].spn_fine, 512);
        assert_eq!(clip.ep_fine[3].pts_fine, 75);
        assert_eq!(clip.ep_fine[3].spn_fine, 1536);

        // Verify resolved EP map assigns fine entries to coarse correctly
        let resolved = clip.resolved_ep_map();
        assert_eq!(resolved.len(), 4);
        // First two fines belong to coarse 0, last two to coarse 1
    }

    #[test]
    fn full_pts_calculation() {
        let coarse = EpCoarse {
            ref_to_fine_id: 0,
            pts_coarse: 100,
            spn_coarse: 0,
        };
        let fine = EpFine {
            pts_fine: 50,
            spn_fine: 0,
        };
        // full_pts = (100 << 19) + (50 << 8) = 52_428_800 + 12_800 = 52_441_600
        let pts = ClipInfo::full_pts(&coarse, &fine);
        assert_eq!(pts, (100 << 19) + (50 << 8));
        assert_eq!(pts, 52_441_600);
    }

    #[test]
    fn full_spn_calculation() {
        let coarse = EpCoarse {
            ref_to_fine_id: 0,
            pts_coarse: 0,
            spn_coarse: 0x00FE0000,
        };
        let fine = EpFine {
            pts_fine: 0,
            spn_fine: 0x1234,
        };
        // full_spn = (0x00FE0000 & 0xFFFE0000) + 0x1234 = 0x00FE0000 + 0x1234 = 0x00FE1234
        let spn = ClipInfo::full_spn(&coarse, &fine);
        assert_eq!(spn, 0x00FE0000 + 0x1234);
        assert_eq!(spn, 0x00FE1234);

        // Test that the low bit of spn_coarse is masked out
        let coarse2 = EpCoarse {
            ref_to_fine_id: 0,
            pts_coarse: 0,
            spn_coarse: 0x00FF0000,
        };
        let spn2 = ClipInfo::full_spn(&coarse2, &fine);
        // 0x00FF0000 & 0xFFFE0000 = 0x00FE0000, so low 17 bits of coarse are zeroed
        assert_eq!(spn2, 0x00FE0000 + 0x1234);
    }

    #[test]
    fn parse_invalid_magic() {
        let mut data = build_clpi(1000, None);
        data[0] = b'X';
        data[1] = b'X';
        data[2] = b'X';
        data[3] = b'X';
        assert!(parse(&data).is_err());
    }

    #[test]
    fn parse_empty_ep_map() {
        // cpi_start = 0 means no CPI section
        let data = build_clpi(100_000, None);
        let clip = parse(&data).expect("should parse with no EP map");
        assert_eq!(clip.source_packet_count, 100_000);
        assert!(clip.ep_coarse.is_empty());
        assert!(clip.ep_fine.is_empty());

        // Also test: CPI section present but with zero streams
        let mut cpi = Vec::new();
        let cpi_length: u32 = 6; // reserved/type(2) + ep_map(reserved(1) + num_streams=0(1) + 2 padding)
        cpi.extend_from_slice(&cpi_length.to_be_bytes());
        cpi.extend_from_slice(&[0u8; 2]); // reserved/type
        cpi.push(0); // reserved
        cpi.push(0); // num_streams = 0
        cpi.extend_from_slice(&[0u8; 4]); // padding

        let data2 = build_clpi(100_000, Some(&cpi));
        let clip2 = parse(&data2).expect("should parse with zero-stream EP map");
        assert!(clip2.ep_coarse.is_empty());
        assert!(clip2.ep_fine.is_empty());
    }
}
