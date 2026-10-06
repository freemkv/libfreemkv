//! CLPI clip info parser (BD-ROM Clip Information).
//!
//! Each .clpi file in BDMV/CLIPINF/ describes one M2TS clip. This parser reads
//! two things from it: `source_packet_count` (the clip's total 192-byte packet
//! count) and the ProgramInfo stream table (PID/coding-type/language), which
//! cross-validates the MPLS STN view (see `labels/clpi_audit.rs`). The CPI/EP
//! map is NOT parsed — reading is whole-clip via UDF and trimming is done at the
//! playlist/timeline (PTS) level, not by EP-map SPN→sector seeking.

use crate::error::{Error, Result};

/// Parsed CLPI clip info.
#[derive(Debug)]
pub(crate) struct ClipInfo {
    /// Total source packets in the m2ts (each 192 bytes)
    pub source_packet_count: u32,
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
}

/// Parse a CLPI file from raw bytes. A file too short to hold the packet count still
/// parses (count 0); only a bad magic or a head under 16 bytes errors.
pub fn parse(data: &[u8]) -> Result<ClipInfo> {
    // 16 bytes reach the section offsets; below that, nothing usable.
    if data.len() < 16 || &data[0..4] != b"HDMV" {
        return Err(Error::ClpiParse);
    }

    // Header offsets
    let prog_info_start = u32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;

    // ClipInfo section at offset 40
    // source_packet_count at offset 40 + 4(len) + 2(reserved) + 1(stream_type) + 1(app_type) + 4(reserved) + 4(ts_rate)
    // A file cut before [56..60] keeps its streams and reports 0 (unknown) packets.
    let source_packet_count = data
        .get(56..60)
        .map_or(0, |b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]));

    // Parse ProgramInfo (per-stream language + codec), best-effort: malformed
    // program_info doesn't fail the parse, just yields an empty streams list.
    let streams = if prog_info_start > 0 && prog_info_start + 6 < data.len() {
        parse_program_info(&data[prog_info_start..])
    } else {
        Vec::new()
    };

    Ok(ClipInfo {
        source_packet_count,
        streams,
    })
}

// Parse the ProgramInfo section: per-stream (pid, coding_type, language, codec sub-fields).
// On a structural mismatch, returns the fully validated entries parsed so far.
fn parse_program_info(data: &[u8]) -> Vec<ClpiStream> {
    use crate::consts::coding_type as c;
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

            let mut language = String::new();

            match coding_type {
                // Video — MPEG-2, H.264, HEVC
                c::MPEG2_VIDEO | c::H264 | c::HEVC => {}
                // Primary audio — LPCM, AC-3, DTS, TrueHD, AC-3+, DTS-HD HR, DTS-HD MA
                // and secondary audio (AC-3+ secondary, DTS-HD secondary)
                c::LPCM..=c::DTS_HD_MA | c::AC3_PLUS_SECONDARY | c::DTS_HD_SECONDARY => {
                    if sci.len() >= 5 {
                        language = String::from_utf8_lossy(&sci[2..5]).to_string();
                    }
                }
                // TextST: coding_type + character_code + 3-byte language
                c::TEXT_SUBTITLE if sci.len() >= 5 => {
                    language = String::from_utf8_lossy(&sci[2..5]).to_string();
                }
                // PG, IG: coding_type + 3-byte language [+ char_code for PG]
                c::PG | c::IG if sci.len() >= 4 => {
                    language = String::from_utf8_lossy(&sci[1..4]).to_string();
                }
                _ => {}
            }

            out.push(ClpiStream {
                pid,
                coding_type,
                language,
            });

            pos = sci_end;
        }
    }
    out
}

#[cfg(test)]
#[path = "clpi_tests.rs"]
mod tests;
