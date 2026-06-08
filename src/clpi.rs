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
pub(crate) struct ClipInfo {
    /// CLPI version string. Parsed for completeness; not yet consumed.
    #[allow(dead_code)]
    pub version: String,
    /// Total source packets in the m2ts (each 192 bytes)
    pub source_packet_count: u32,
    /// Coarse EP entries for the primary video stream. Populated for the
    /// EP-map → sector-extent lookup (`get_extents`), which is exercised by
    /// tests and reserved for the timestamp-range read path.
    #[allow(dead_code)]
    pub ep_coarse: Vec<EpCoarse>,
    /// Fine EP entries for the primary video stream (see `ep_coarse`).
    #[allow(dead_code)]
    pub ep_fine: Vec<EpFine>,
    /// Per-stream metadata from the ProgramInfo section (BD spec).
    /// Cross-validates the MPLS STN view — see `labels/clpi_audit.rs`.
    /// Empty when program_info is missing or malformed.
    pub streams: Vec<ClpiStream>,
}

/// One stream descriptor from the CLPI ProgramInfo / stream_coding_info
/// table. Mirrors the same fields the MPLS STN table carries — see
/// `mpls::StreamEntry` for the playlist-side equivalent.
#[derive(Debug, Clone)]
pub(crate) struct ClpiStream {
    /// PID of the stream in the MPEG-TS (matches MPLS).
    pub pid: u16,
    /// BD stream coding type byte (0x80 LPCM, 0x83 TrueHD, 0x86 DTS-HD MA,
    /// 0x90 PG, etc.). See `labels::mpls_universal::coding_type_to_codec_hint`.
    pub coding_type: u8,
    /// ISO 639-2 3-char language code. Empty for video streams.
    pub language: String,
    // The CLPI cross-validation consumer (labels/clpi_audit.rs) reads only
    // pid/coding_type/language. The codec sub-fields below are parsed from
    // the BD stream_coding_info for completeness but have no reader yet.
    /// Audio format byte (1=mono, 3=stereo, 6=5.1, 12=7.1).
    /// Zero for non-audio streams.
    #[allow(dead_code)]
    pub audio_format: u8,
    /// Audio sample rate (1=48kHz, 4=96kHz, 5=192kHz). Zero for non-audio.
    #[allow(dead_code)]
    pub audio_rate: u8,
    /// Video format byte (1=480i, 4=1080i, 5=720p, 6=1080p, 8=2160p).
    /// Zero for non-video.
    #[allow(dead_code)]
    pub video_format: u8,
    /// Video rate (1=23.976, 2=24, 3=25, 4=29.97, 6=50, 7=59.94).
    #[allow(dead_code)]
    pub video_rate: u8,
}

/// Coarse EP-map entry. Fields feed the EP-map resolution used by
/// `get_extents` (test-exercised; reserved for the timestamp-range path).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct EpCoarse {
    pub ref_to_fine_id: u32,
    pub pts_coarse: u32,
    pub spn_coarse: u32,
}

/// Fine EP-map entry (see `EpCoarse`).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct EpFine {
    pub pts_fine: u32,
    pub spn_fine: u32,
}

// EP-map → sector-extent resolution. Exercised by the unit tests and
// reserved for the timestamp-range read path; no production caller yet.
#[allow(dead_code)]
impl ClipInfo {
    /// Reconstruct full PTS from coarse + fine entry.
    ///
    /// The BD spec PTS is 33-bit: `pts_coarse` is 14 bits (max 16383) and
    /// `16383 << 19` exceeds `u32::MAX`, so the result must be `u64` to
    /// avoid overflow (panic in debug, silent wrap in release).
    pub fn full_pts(coarse: &EpCoarse, fine: &EpFine) -> u64 {
        ((coarse.pts_coarse as u64) << 19) + ((fine.pts_fine as u64) << 8)
    }

    /// Reconstruct full SPN from coarse + fine entry.
    pub fn full_spn(coarse: &EpCoarse, fine: &EpFine) -> u32 {
        // The two operands occupy non-overlapping bit ranges (coarse holds
        // the high bits, fine the low 17), so OR expresses intent and is
        // robust to a hand-constructed EpFine.
        debug_assert!(fine.spn_fine <= 0x1_FFFF);
        (coarse.spn_coarse & 0xFFFE_0000) | fine.spn_fine
    }

    /// Get all EP entries as (PTS, SPN) pairs, fully resolved.
    ///
    /// PTS resets at each coarse-group boundary on disc, so the raw
    /// concatenation is not globally monotonic. The returned vector is
    /// sorted by PTS so callers (e.g. [`get_extents`]) can binary-search it.
    ///
    /// [`get_extents`]: ClipInfo::get_extents
    pub fn resolved_ep_map(&self) -> Vec<(u64, u32)> {
        let mut entries = Vec::with_capacity(self.ep_fine.len());

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

        // get_extents binary-searches by PTS, so the map must be ordered.
        // Real discs have globally increasing PTS in coarse order; sort by
        // (pts, spn) so a cross-group PTS collision can't leave the search
        // landing on the wrong group's SPN.
        entries.sort_by_key(|&(pts, spn)| (pts, spn));

        entries
    }

    /// Get sector extents for a given in/out time range.
    ///
    /// Converts PTS timestamps to SPN ranges, then SPN to LBA
    /// using the file's starting LBA on disc.
    pub fn get_extents(&self, in_time: u64, out_time: u64) -> Vec<Extent> {
        // resolved_ep_map() returns entries sorted by PTS, so binary search
        // is valid here.
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
            _ => ep_map.last().unwrap().1.saturating_add(1),
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
    let prog_info_start = u32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;
    let cpi_start = u32::from_be_bytes([data[16], data[17], data[18], data[19]]) as usize;

    // ClipInfo section at offset 40
    // source_packet_count at offset 40 + 4(len) + 2(reserved) + 1(stream_type) + 1(app_type) + 4(reserved) + 4(ts_rate)
    let source_packet_count = if data.len() >= 60 {
        u32::from_be_bytes([data[56], data[57], data[58], data[59]])
    } else {
        0
    };

    // Parse ProgramInfo (per-stream language + codec). Best-effort:
    // malformed program_info doesn't fail the parse, just gives an
    // empty streams list. EP map is unaffected — sector-range lookups
    // continue to work.
    let streams = if prog_info_start > 0 && prog_info_start + 6 < data.len() {
        parse_program_info(&data[prog_info_start..])
    } else {
        Vec::new()
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
        streams,
    })
}

/// Parse the ProgramInfo section: per-stream (pid, coding_type,
/// language, codec sub-fields). Layout per BD spec / libbluray
/// clpi_parse.c:
///
/// ```text
/// ProgramInfo:
///   length: 4 bytes
///   reserved: 1 byte
///   num_programs: 1 byte
///   for each program:
///     spn_program_sequence_start: 4 bytes
///     program_map_pid: 2 bytes
///     num_streams: 1 byte
///     num_groups: 1 byte
///     for each stream:
///       pid: 2 bytes
///       stream_coding_info_length: 1 byte
///       stream_coding_info: (varies by coding_type)
///         coding_type: 1 byte
///         per-type bytes (see match arms below)
/// ```
///
/// Returns `Vec::new()` on any structural mismatch — we don't propagate
/// errors because the EP map is the primary CLPI output, and a corrupt
/// program_info shouldn't break sector-range lookups.
fn parse_program_info(data: &[u8]) -> Vec<ClpiStream> {
    let mut out = Vec::new();
    if data.len() < 6 {
        return out;
    }
    // length: 4 bytes (skipped — we trust the section bounds in the
    // caller's slice and read the bytes that follow). Reserved 1 byte
    // at offset 4. num_programs at offset 5.
    let num_programs = data[5] as usize;
    let mut pos = 6usize;
    for _ in 0..num_programs {
        // Program header: 4 (spn) + 2 (pmt_pid) + 1 (num_streams) + 1 (num_groups) = 8 bytes
        if pos + 8 > data.len() {
            return out;
        }
        let num_streams = data[pos + 6] as usize;
        pos += 8;

        for _ in 0..num_streams {
            // Stream header: 2 (pid) + 1 (sci_length) + sci bytes
            if pos + 3 > data.len() {
                return out;
            }
            let pid = u16::from_be_bytes([data[pos], data[pos + 1]]);
            let sci_len = data[pos + 2] as usize;
            let sci_end = pos + 3 + sci_len;
            if sci_end > data.len() || sci_len < 1 {
                return out;
            }
            let sci = &data[pos + 3..sci_end];
            let coding_type = sci[0];

            let mut audio_format = 0u8;
            let mut audio_rate = 0u8;
            let mut video_format = 0u8;
            let mut video_rate = 0u8;
            let mut language = String::new();

            match coding_type {
                // Video — MPEG-2 (0x02), H.264 (0x1B), HEVC (0x24)
                0x02 | 0x1B | 0x24 => {
                    if sci.len() >= 2 {
                        video_format = (sci[1] >> 4) & 0x0F;
                        video_rate = sci[1] & 0x0F;
                    }
                }
                // Primary audio — LPCM(0x80), AC-3(0x81), DTS(0x82),
                // TrueHD(0x83), AC-3+(0x84), DTS-HD(0x85), DTS-HD MA(0x86)
                0x80..=0x86 => {
                    if sci.len() >= 2 {
                        audio_format = (sci[1] >> 4) & 0x0F;
                        audio_rate = sci[1] & 0x0F;
                    }
                    if sci.len() >= 5 {
                        language = String::from_utf8_lossy(&sci[2..5]).to_string();
                    }
                }
                // Secondary audio (0xA1 AC-3+, 0xA2 DTS-HD)
                0xA1 | 0xA2 => {
                    if sci.len() >= 2 {
                        audio_format = (sci[1] >> 4) & 0x0F;
                        audio_rate = sci[1] & 0x0F;
                    }
                    if sci.len() >= 5 {
                        language = String::from_utf8_lossy(&sci[2..5]).to_string();
                    }
                }
                // PG (0x90), IG (0x91): coding_type + 3-byte language [+ char_code for PG]
                0x90 | 0x91 => {
                    if sci.len() >= 4 {
                        language = String::from_utf8_lossy(&sci[1..4]).to_string();
                    }
                }
                _ => {}
            }

            out.push(ClpiStream {
                pid,
                coding_type,
                language,
                audio_format,
                audio_rate,
                video_format,
                video_rate,
            });

            pos = sci_end;
        }
    }
    out
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

    // Bound all EP-map reads to this CPI section. The length field counts
    // bytes after itself, so the section spans data[..cpi_length + 4]. A
    // bogus ep_map_offset within data.len() but past the CPI section would
    // otherwise read into an adjacent CLPI section; clamp first.
    let data = &data[..(cpi_length + 4).min(data.len())];

    // CPI type at bits 44-47 (byte 5, lower 4 bits)
    // Skip to EP map: offset 4 (after length) + 2 (reserved/type)
    if data.len() < 6 {
        return Ok((Vec::new(), Vec::new()));
    }
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
    // Cap the pre-reservation by what the slice can actually hold:
    // num_coarse is a 16-bit disc field, so a hostile value would
    // otherwise reserve up to ~0.5 MB for an entry table that doesn't exist.
    let mut ep_coarse = Vec::with_capacity(num_coarse.min(coarse_data.len() / 8));
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
    // Cap the pre-reservation: num_fine is an 18-bit disc field (max
    // 262143), so reserve only what the slice can actually hold.
    let mut ep_fine = if fine_start < stream_ep.len() {
        Vec::with_capacity(num_fine.min((stream_ep.len() - fine_start) / 4))
    } else {
        Vec::new()
    };
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
        let packed: u128 = ((ep_stream_type as u128) << 66)           // EP_stream_type: 4 bits
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
        assert_eq!(pts, (100u64 << 19) + (50u64 << 8));
        assert_eq!(pts, 52_441_600);
    }

    #[test]
    fn full_pts_no_u32_overflow() {
        // pts_coarse is a 14-bit field (max 0x3FFF = 16383); 16383 << 19
        // overflows u32, so full_pts must use u64.
        let coarse = EpCoarse {
            ref_to_fine_id: 0,
            pts_coarse: 0x3FFF,
            spn_coarse: 0,
        };
        let fine = EpFine {
            pts_fine: 0x7FF,
            spn_fine: 0,
        };
        let pts = ClipInfo::full_pts(&coarse, &fine);
        assert_eq!(pts, (0x3FFFu64 << 19) + (0x7FFu64 << 8));
        assert!(pts > u32::MAX as u64);
    }

    #[test]
    fn resolved_ep_map_sorted_for_binary_search() {
        // Two coarse groups whose fine PTS reset across the boundary
        // (50,100 then 25,75) produce a non-monotonic raw concatenation.
        // resolved_ep_map must sort so get_extents' binary search is valid.
        let cpi = build_cpi(
            0x1011,
            &[(0, 0, 0x00020000), (2, 0, 0x00040000)],
            &[(50, 1024), (100, 2048), (25, 512), (75, 1536)],
        );
        let data = build_clpi(1_000_000, Some(&cpi));
        let clip = parse(&data).expect("should parse");

        let resolved = clip.resolved_ep_map();
        assert_eq!(resolved.len(), 4);
        // Strictly sorted by PTS.
        for w in resolved.windows(2) {
            assert!(w[0].0 <= w[1].0, "ep_map not sorted: {resolved:?}");
        }
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
    fn parse_truncated_clipinfo_no_panic() {
        // 57/58/59-byte CLPI with valid magic: passes the data.len() < 40
        // guard but data[56..60] needs 60 bytes. Must not panic.
        for len in 40..60usize {
            let mut data = vec![0u8; len];
            data[0..4].copy_from_slice(b"HDMV");
            if len >= 8 {
                data[4..8].copy_from_slice(b"0200");
            }
            let clip = parse(&data).expect("short CLPI should parse, not panic");
            // source_packet_count is unreadable below 60 bytes → 0.
            assert_eq!(clip.source_packet_count, 0);
        }
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

    // ─────────────────────────────────────────────────────────────────────
    // Added hardening tests. Grounded in the BD-ROM CLPI spec
    // (https://github.com/lw/BluRay/wiki/CLPI) and libbluray clpi_parse.c.
    // ─────────────────────────────────────────────────────────────────────

    /// Build a ProgramInfo section. `streams` = Vec<(pid, sci_bytes)>.
    /// Layout per source doc: length(4)+reserved(1)+num_programs(1)+
    /// per program [spn(4)+pmt_pid(2)+num_streams(1)+num_groups(1)] then
    /// per stream [pid(2)+sci_len(1)+sci].
    fn build_program_info(streams: &[(u16, Vec<u8>)]) -> Vec<u8> {
        let mut body = Vec::new();
        body.push(0); // reserved (offset 4)
        body.push(1); // num_programs = 1 (offset 5)
        // program 0 header (8 bytes)
        body.extend_from_slice(&0u32.to_be_bytes()); // spn_program_sequence_start
        body.extend_from_slice(&0u16.to_be_bytes()); // program_map_pid
        body.push(streams.len() as u8); // num_streams
        body.push(0); // num_groups
        for (pid, sci) in streams {
            body.extend_from_slice(&pid.to_be_bytes());
            body.push(sci.len() as u8);
            body.extend_from_slice(sci);
        }
        // Prepend length(4) = bytes after the length field.
        let mut out = Vec::new();
        out.extend_from_slice(&(body.len() as u32).to_be_bytes());
        out.extend_from_slice(&body);
        out
    }

    /// Build a CLPI with a ProgramInfo section. prog_info_start is placed
    /// right after the 60-byte header; cpi (if any) follows program_info.
    fn build_clpi_with_proginfo(
        source_packet_count: u32,
        prog_info: &[u8],
        cpi_data: Option<&[u8]>,
    ) -> Vec<u8> {
        let mut buf = vec![0u8; 60];
        buf[0..4].copy_from_slice(b"HDMV");
        buf[4..8].copy_from_slice(b"0200");
        let prog_info_start: u32 = 60;
        buf[12..16].copy_from_slice(&prog_info_start.to_be_bytes());
        let cpi_start: u32 = if cpi_data.is_some() {
            (60 + prog_info.len()) as u32
        } else {
            0
        };
        buf[16..20].copy_from_slice(&cpi_start.to_be_bytes());
        buf[56..60].copy_from_slice(&source_packet_count.to_be_bytes());
        buf.extend_from_slice(prog_info);
        if let Some(cpi) = cpi_data {
            buf.extend_from_slice(cpi);
        }
        buf
    }

    /// source_packet_count is a big-endian u32 at offset [56..60]. Verify
    /// BE decode of a value with all four bytes distinct (not LE / wrong
    /// offset).
    #[test]
    fn source_packet_count_big_endian_offset_56() {
        let data = build_clpi(0x01020304, None);
        let clip = parse(&data).expect("should parse");
        assert_eq!(clip.source_packet_count, 0x01020304);
    }

    /// Magic must be exactly "HDMV" at [0..4]. Anything else → ClpiParse.
    /// Spec: CLPI files begin with the type_indicator "HDMV".
    #[test]
    fn wrong_magic_rejected() {
        let mut data = build_clpi(1000, None);
        data[0..4].copy_from_slice(b"INDX");
        assert!(parse(&data).is_err());
    }

    /// Under-40-byte input is rejected before any field read
    /// (`data.len() < 40` guard).
    #[test]
    fn under_40_bytes_rejected() {
        assert!(parse(&[0u8; 39]).is_err());
        assert!(parse(b"HDMV0200").is_err());
        assert!(parse(&[]).is_err());
    }

    /// ProgramInfo: a video stream (coding 0x1B = H.264) carries
    /// format/rate in sci[1] nibbles and NO language. Verify the video
    /// arm: format hi-nibble, rate lo-nibble, language stays empty.
    #[test]
    fn program_info_video_stream() {
        // sci = coding_type(0x1B) + format_rate(0x61 → fmt 6, rate 1)
        let sci = vec![0x1Bu8, 0x61];
        let pi = build_program_info(&[(0x1011, sci)]);
        let data = build_clpi_with_proginfo(100, &pi, None);
        let clip = parse(&data).expect("should parse");
        assert_eq!(clip.streams.len(), 1);
        assert_eq!(clip.streams[0].pid, 0x1011);
        assert_eq!(clip.streams[0].coding_type, 0x1B);
        assert_eq!(clip.streams[0].video_format, 6);
        assert_eq!(clip.streams[0].video_rate, 1);
        assert_eq!(clip.streams[0].language, "");
    }

    /// ProgramInfo primary-audio (coding 0x80..=0x86): sci[1] = format/rate
    /// nibbles, sci[2..5] = ISO 639 language. Verify TrueHD (0x83) at
    /// offset, 5.1 / 48kHz, language "eng".
    #[test]
    fn program_info_audio_stream_lang_offset() {
        // sci = 0x83 + 0x61 (fmt 6, rate 1) + "eng"
        let sci = vec![0x83u8, 0x61, b'e', b'n', b'g'];
        let pi = build_program_info(&[(0x1100, sci)]);
        let data = build_clpi_with_proginfo(100, &pi, None);
        let clip = parse(&data).expect("should parse");
        assert_eq!(clip.streams[0].coding_type, 0x83);
        assert_eq!(clip.streams[0].audio_format, 6);
        assert_eq!(clip.streams[0].audio_rate, 1);
        assert_eq!(clip.streams[0].language, "eng");
    }

    /// ProgramInfo PG (0x90)/IG (0x91): layout is coding_type(1)+lang(3),
    /// so language is at sci[1..4] (NOT sci[2..5] like audio). Verify the
    /// PG arm reads from the right offset.
    #[test]
    fn program_info_pg_lang_offset() {
        // sci = 0x90 + "fra" (lang directly after coding_type)
        let sci = vec![0x90u8, b'f', b'r', b'a'];
        let pi = build_program_info(&[(0x1200, sci)]);
        let data = build_clpi_with_proginfo(100, &pi, None);
        let clip = parse(&data).expect("should parse");
        assert_eq!(clip.streams[0].coding_type, 0x90);
        assert_eq!(clip.streams[0].language, "fra");
        // Audio nibbles must NOT be populated for a PG stream.
        assert_eq!(clip.streams[0].audio_format, 0);
    }

    /// ProgramInfo with multiple streams: PID and coding for each must be
    /// read from the correct per-stream offset (pid(2)+sci_len(1)+sci).
    /// Three mixed streams must all parse with distinct PIDs in order.
    #[test]
    fn program_info_multiple_streams_advance_correctly() {
        let v = (0x1011u16, vec![0x24u8, 0x81]); // HEVC video
        let a = (0x1100u16, vec![0x86u8, 0x61, b'e', b'n', b'g']); // DTS-HD MA
        let s = (0x1200u16, vec![0x90u8, b'j', b'p', b'n']); // PG
        let pi = build_program_info(&[v, a, s]);
        let data = build_clpi_with_proginfo(100, &pi, None);
        let clip = parse(&data).expect("should parse");
        assert_eq!(clip.streams.len(), 3);
        assert_eq!(clip.streams[0].pid, 0x1011);
        assert_eq!(clip.streams[0].coding_type, 0x24);
        assert_eq!(clip.streams[1].pid, 0x1100);
        assert_eq!(clip.streams[1].coding_type, 0x86);
        assert_eq!(clip.streams[1].language, "eng");
        assert_eq!(clip.streams[2].pid, 0x1200);
        assert_eq!(clip.streams[2].language, "jpn");
    }

    /// parse_program_info is best-effort: a stream whose declared sci_len
    /// runs past the section (`sci_end > data.len()`) makes it return the
    /// streams collected so far (here: none), never panic. Source returns
    /// `out` early on the overflow.
    #[test]
    fn program_info_truncated_sci_no_panic() {
        // One stream claiming sci_len = 200 but with no body.
        let mut body = Vec::new();
        body.push(0); // reserved
        body.push(1); // num_programs
        body.extend_from_slice(&0u32.to_be_bytes());
        body.extend_from_slice(&0u16.to_be_bytes());
        body.push(1); // num_streams
        body.push(0); // num_groups
        body.extend_from_slice(&0x1011u16.to_be_bytes()); // pid
        body.push(200); // sci_len = 200, no body follows
        let mut pi = Vec::new();
        pi.extend_from_slice(&(body.len() as u32).to_be_bytes());
        pi.extend_from_slice(&body);
        let data = build_clpi_with_proginfo(100, &pi, None);
        let clip = parse(&data).expect("should not panic");
        assert!(clip.streams.is_empty());
    }

    /// parse_program_info rejects sci_len == 0 (`sci_len < 1` → return).
    /// A zero-length stream_coding_info is unusable.
    #[test]
    fn program_info_zero_sci_len_yields_no_stream() {
        let mut body = Vec::new();
        body.push(0);
        body.push(1);
        body.extend_from_slice(&0u32.to_be_bytes());
        body.extend_from_slice(&0u16.to_be_bytes());
        body.push(1);
        body.push(0);
        body.extend_from_slice(&0x1011u16.to_be_bytes());
        body.push(0); // sci_len = 0
        let mut pi = Vec::new();
        pi.extend_from_slice(&(body.len() as u32).to_be_bytes());
        pi.extend_from_slice(&body);
        let data = build_clpi_with_proginfo(100, &pi, None);
        let clip = parse(&data).expect("should parse");
        assert!(clip.streams.is_empty());
    }

    /// pts_coarse field is 14 bits: dword0 = ref_to_fine_id<<14 | pts_coarse.
    /// A pts_coarse of 0x3FFF (max) with ref_to_fine_id 5 must decode both
    /// without bleed. Verify the >>14 and &0x3FFF split.
    #[test]
    fn coarse_pts_14bit_split() {
        let cpi = build_cpi(0x1011, &[(5, 0x3FFF, 0x12340000)], &[(0, 0)]);
        let data = build_clpi(1000, Some(&cpi));
        let clip = parse(&data).expect("should parse");
        assert_eq!(clip.ep_coarse[0].ref_to_fine_id, 5);
        assert_eq!(clip.ep_coarse[0].pts_coarse, 0x3FFF);
        assert_eq!(clip.ep_coarse[0].spn_coarse, 0x12340000);
    }

    /// Fine entry: dword = is_angle(1)+i_end_offset(3)+pts_fine(11)+
    /// spn_fine(17). pts_fine occupies bits 17..28 (>>17 & 0x7FF), spn_fine
    /// the low 17 bits (& 0x1FFFF). Set high bits (is_angle/i_end_offset)
    /// and verify they do NOT bleed into pts_fine.
    #[test]
    fn fine_entry_bit_layout_isolates_pts_and_spn() {
        // Construct a raw fine dword with is_angle=1, i_end_offset=0b111,
        // pts_fine=0x5AA, spn_fine=0x1AAAA, then verify decode.
        let is_angle: u32 = 1;
        let i_end: u32 = 0b111;
        let pts_f: u32 = 0x5AA; // 11-bit
        let spn_f: u32 = 0x1AAAA; // 17-bit
        let dword: u32 = (is_angle << 31) | (i_end << 28) | (pts_f << 17) | spn_f;

        // Build the CPI by hand with this raw fine dword.
        let mut stream_ep = Vec::new();
        let fine_start: u32 = 4; // no coarse entries → fine right after header
        stream_ep.extend_from_slice(&fine_start.to_be_bytes());
        stream_ep.extend_from_slice(&dword.to_be_bytes());

        let num_coarse: u32 = 0;
        let num_fine: u32 = 1;
        let ep_map_start: u32 = 14;
        let ep_stream_type: u32 = 1;
        let packed: u128 = ((ep_stream_type as u128) << 66)
            | ((num_coarse as u128) << 50)
            | ((num_fine as u128) << 32)
            | (ep_map_start as u128);
        let packed_bytes = packed.to_be_bytes();
        let stream_header_bits = &packed_bytes[6..16];

        let mut ep_map = Vec::new();
        ep_map.push(0);
        ep_map.push(1);
        ep_map.extend_from_slice(&0x1011u16.to_be_bytes());
        ep_map.extend_from_slice(stream_header_bits);
        ep_map.extend_from_slice(&stream_ep);

        let mut cpi = Vec::new();
        cpi.extend_from_slice(&((2 + ep_map.len()) as u32).to_be_bytes());
        cpi.extend_from_slice(&[0u8; 2]);
        cpi.extend_from_slice(&ep_map);

        let data = build_clpi(1000, Some(&cpi));
        let clip = parse(&data).expect("should parse");
        assert_eq!(clip.ep_fine.len(), 1);
        assert_eq!(clip.ep_fine[0].pts_fine, 0x5AA); // high bits stripped
        assert_eq!(clip.ep_fine[0].spn_fine, 0x1AAAA);
    }

    /// resolved_ep_map assigns fine entries to coarse groups via
    /// [ref_to_fine_id .. next coarse's ref_to_fine_id). full_pts combines
    /// coarse<<19 + fine<<8 and full_spn ORs masked coarse with fine.
    /// Verify the first resolved entry's (pts, spn) for a known fixture.
    #[test]
    fn resolved_ep_map_combines_coarse_and_fine() {
        // coarse 0: ref_to_fine_id=0, pts_coarse=10, spn_coarse=0x00020000
        // fine 0: pts_fine=3, spn_fine=0x100
        let cpi = build_cpi(0x1011, &[(0, 10, 0x00020000)], &[(3, 0x100)]);
        let data = build_clpi(1000, Some(&cpi));
        let clip = parse(&data).expect("should parse");
        let resolved = clip.resolved_ep_map();
        assert_eq!(resolved.len(), 1);
        let expected_pts = (10u64 << 19) + (3u64 << 8);
        let expected_spn = (0x00020000u32 & 0xFFFE_0000) | 0x100;
        assert_eq!(resolved[0].0, expected_pts);
        assert_eq!(resolved[0].1, expected_spn);
    }

    /// get_extents converts an in/out PTS range to a single sector Extent.
    /// SPN→byte = spn×192, byte→sector = /2048 (start floored, end ceiled),
    /// relative to m2ts file start. Verify the math for a known fixture.
    #[test]
    fn get_extents_spn_to_sector_math() {
        // Two EP points: PTS p0 → SPN 0, PTS p1 → SPN big_spn.
        // full_spn ORs (spn_coarse & 0xFFFE0000) with spn_fine, so the SPN
        // must be coarse-aligned (low 17 bits clear) to survive intact.
        // 0x20000 (131072) is the smallest non-zero coarse-aligned SPN.
        let big_spn: u32 = 0x20000;
        let cpi = build_cpi(0x1011, &[(0, 0, 0), (1, 100, big_spn)], &[(0, 0), (0, 0)]);
        let data = build_clpi(1000, Some(&cpi));
        let clip = parse(&data).expect("should parse");

        let p0 = (0u64 << 19) + (0u64 << 8); // PTS of first EP
        let p1 = (100u64 << 19) + (0u64 << 8); // PTS of second EP
        let extents = clip.get_extents(p0, p1);
        assert_eq!(extents.len(), 1);
        // start_spn = 0, end_spn = big_spn. SPN→byte ×192, byte→sector /2048.
        let start_byte = 0u64 * 192;
        let end_byte = big_spn as u64 * 192;
        let start_sector = (start_byte / 2048) as u32;
        let end_sector = end_byte.div_ceil(2048) as u32;
        assert_eq!(extents[0].start_lba, start_sector);
        assert_eq!(extents[0].sector_count, end_sector - start_sector);
        // Concretely: 0x20000 × 192 / 2048 = 12288 sectors.
        assert_eq!(extents[0].sector_count, 12288);
    }

    /// get_extents returns an empty Vec when the EP map is empty (no CPI),
    /// since there is no SPN to resolve. Documented early return.
    #[test]
    fn get_extents_empty_when_no_ep_map() {
        let data = build_clpi(1000, None);
        let clip = parse(&data).expect("should parse");
        assert!(clip.get_extents(0, 1_000_000).is_empty());
    }

    /// get_extents returns empty when end_spn <= start_spn (degenerate or
    /// inverted range). Source has an explicit `if end_spn <= start_spn`
    /// guard. Use in_time == out_time on a single-point map.
    #[test]
    fn get_extents_empty_on_degenerate_range() {
        let cpi = build_cpi(0x1011, &[(0, 50, 0x1000)], &[(0, 0)]);
        let data = build_clpi(1000, Some(&cpi));
        let clip = parse(&data).expect("should parse");
        let p = (50u64 << 19) + (0u64 << 8);
        // in == out → start_spn == end_spn → empty.
        assert!(clip.get_extents(p, p).is_empty());
    }

    /// full_spn masks the LOW 17 bits of spn_coarse (& 0xFFFE0000) before
    /// OR-ing fine. A spn_coarse with low bits set must have them cleared,
    /// then replaced by spn_fine. Independent of parse, exercises the
    /// reconstruction directly with a hostile low-bit pattern.
    #[test]
    fn full_spn_clears_coarse_low_17_bits() {
        let coarse = EpCoarse {
            ref_to_fine_id: 0,
            pts_coarse: 0,
            spn_coarse: 0x0006_FFFF, // low 17 bits all set
        };
        let fine = EpFine {
            pts_fine: 0,
            spn_fine: 0x5,
        };
        // 0x0006_FFFF & 0xFFFE_0000 = 0x0006_0000; | 0x5 = 0x0006_0005.
        assert_eq!(ClipInfo::full_spn(&coarse, &fine), 0x0006_0005);
    }

    /// CPI guard: cpi_length < 4 short-circuits to empty maps (the length
    /// field counts bytes after itself, and the EP map needs ≥4). A
    /// cpi_length of 0/1/2/3 must yield empty EP maps, not panic.
    #[test]
    fn cpi_length_below_4_yields_empty() {
        for bad_len in 0u32..4 {
            let mut cpi = Vec::new();
            cpi.extend_from_slice(&bad_len.to_be_bytes());
            cpi.extend_from_slice(&[0u8; 20]); // padding so the slice exists
            let data = build_clpi(1000, Some(&cpi));
            let clip = parse(&data).expect("should parse");
            assert!(clip.ep_coarse.is_empty(), "len={bad_len}");
            assert!(clip.ep_fine.is_empty(), "len={bad_len}");
        }
    }

    /// ep_map_offset that points past the EP map (`ep_map_offset + 4 >
    /// ep_map.len()`) → empty maps (bounds guard), not panic. Patch the
    /// EP_map_start field to a huge value.
    #[test]
    fn ep_map_offset_out_of_bounds_yields_empty() {
        let cpi = build_cpi(0x1011, &[(0, 10, 0x20000)], &[(5, 100)]);
        let mut data = build_clpi(1000, Some(&cpi));
        // EP_map_start is the low 32 bits of the 80-bit stream header at
        // ep_map[4..14]. In the file: header(60) + cpi_length(4) +
        // reserved(2) + ep_map reserved(1) + num_streams(1) + pid(2) = 70,
        // then 10 header bytes [70..80]; EP_map_start is the last 4 [76..80].
        let off = 60 + 4 + 2 + 1 + 1 + 2 + 6; // = 76
        data[off..off + 4].copy_from_slice(&0xFFFF_FFFFu32.to_be_bytes());
        let clip = parse(&data).expect("should not panic");
        assert!(clip.ep_coarse.is_empty());
        assert!(clip.ep_fine.is_empty());
    }

    /// num_coarse declares more entries than the CPI section holds. The
    /// loop must stop at `off + 8 > coarse_data.len()` (break), not read
    /// out of bounds. Patch num_coarse to a large value while supplying 1
    /// coarse entry's worth of bytes.
    #[test]
    fn coarse_count_overshoot_truncates_safely() {
        let cpi = build_cpi(0x1011, &[(0, 10, 0x20000)], &[(5, 100)]);
        let mut data = build_clpi(1000, Some(&cpi));
        // num_coarse is bits 14..30 of the 80-bit header. Rather than
        // bit-surgery, rebuild with a hand-set num_coarse=255 but only 1
        // coarse entry of bytes — done below directly.
        let _ = &mut data;

        let num_coarse_decl: u32 = 255;
        let num_fine: u32 = 1;
        let ep_map_start: u32 = 14;
        let ep_stream_type: u32 = 1;
        let packed: u128 = ((ep_stream_type as u128) << 66)
            | ((num_coarse_decl as u128) << 50)
            | ((num_fine as u128) << 32)
            | (ep_map_start as u128);
        let packed_bytes = packed.to_be_bytes();
        let stream_header_bits = &packed_bytes[6..16];

        // stream EP data: fine_start points past the 1 coarse entry.
        let fine_start: u32 = 4 + 1 * 8;
        let mut stream_ep = Vec::new();
        stream_ep.extend_from_slice(&fine_start.to_be_bytes());
        // exactly ONE coarse entry (8 bytes), though header claims 255.
        stream_ep.extend_from_slice(&((0u32 << 14) | 10).to_be_bytes());
        stream_ep.extend_from_slice(&0x20000u32.to_be_bytes());
        // one fine entry (4 bytes)
        stream_ep.extend_from_slice(&(((5u32 & 0x7FF) << 17) | 100).to_be_bytes());

        let mut ep_map = Vec::new();
        ep_map.push(0);
        ep_map.push(1);
        ep_map.extend_from_slice(&0x1011u16.to_be_bytes());
        ep_map.extend_from_slice(stream_header_bits);
        ep_map.extend_from_slice(&stream_ep);
        let mut cpi2 = Vec::new();
        cpi2.extend_from_slice(&((2 + ep_map.len()) as u32).to_be_bytes());
        cpi2.extend_from_slice(&[0u8; 2]);
        cpi2.extend_from_slice(&ep_map);
        let data2 = build_clpi(1000, Some(&cpi2));
        let clip = parse(&data2).expect("should not panic on coarse overshoot");
        // Only the 1 real coarse entry was readable.
        assert_eq!(clip.ep_coarse.len(), 1);
        assert_eq!(clip.ep_coarse[0].pts_coarse, 10);
    }

    /// resolved_ep_map: the LAST coarse group's fine range extends to
    /// ep_fine.len() (no "next coarse" bound). Verify all trailing fine
    /// entries are assigned to the final coarse group.
    #[test]
    fn resolved_ep_map_last_group_to_end() {
        // coarse 0 ref_to_fine_id=0, coarse 1 ref_to_fine_id=1.
        // 3 fine entries: fine 0 → coarse 0; fine 1,2 → coarse 1.
        let cpi = build_cpi(
            0x1011,
            &[(0, 0, 0), (1, 100, 0)],
            &[(0, 10), (0, 20), (0, 30)],
        );
        let data = build_clpi(1000, Some(&cpi));
        let clip = parse(&data).expect("should parse");
        let resolved = clip.resolved_ep_map();
        // All 3 fine entries resolved (last group picks up fine 1 and 2).
        assert_eq!(resolved.len(), 3);
    }
}
