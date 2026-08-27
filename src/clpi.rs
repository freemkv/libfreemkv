//! CLPI clip info parser — maps clips to sector ranges on disc.
//!
//! Each .clpi file in BDMV/CLIPINF/ describes one M2TS clip.
//! The EP (Entry Point) map provides timestamp → SPN mapping.
//! SPN × 192 = byte offset in the m2ts file.
//!
//! Format is documented in the BD-ROM Clip Information (CLPI) specification.

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

/// Parse a CLPI file from raw bytes.
pub fn parse(data: &[u8]) -> Result<ClipInfo> {
    if data.len() < 40 {
        return Err(Error::ClpiParse);
    }

    if &data[0..4] != b"HDMV" {
        return Err(Error::ClpiParse);
    }

    // Header offsets
    let _seq_info_start = u32::from_be_bytes([data[8], data[9], data[10], data[11]]) as usize;
    let prog_info_start = u32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;

    // ClipInfo section at offset 40
    // source_packet_count at offset 40 + 4(len) + 2(reserved) + 1(stream_type) + 1(app_type) + 4(reserved) + 4(ts_rate)
    let source_packet_count = if data.len() >= 60 {
        u32::from_be_bytes([data[56], data[57], data[58], data[59]])
    } else {
        0
    };

    // Parse ProgramInfo (per-stream language + codec), best-effort: malformed
    // program_info doesn't fail the parse, just yields an empty streams list.
    // EP map is unaffected — sector-range lookups still work.
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

/// Parse the ProgramInfo section: per-stream (pid, coding_type,
/// language, codec sub-fields). Layout per the Blu-ray Disc Read-Only Format
/// Part 3 CLIPINF (CLPI) specification:
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
                c::LPCM..=c::DTS_HD_MA => {
                    if sci.len() >= 5 {
                        language = String::from_utf8_lossy(&sci[2..5]).to_string();
                    }
                }
                // Secondary audio (AC-3+ secondary, DTS-HD secondary)
                c::AC3_PLUS_SECONDARY | c::DTS_HD_SECONDARY => {
                    if sci.len() >= 5 {
                        language = String::from_utf8_lossy(&sci[2..5]).to_string();
                    }
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
mod tests {
    use super::*;

    /// Build a minimal CLPI binary.
    /// `cpi_data` is the raw CPI section bytes (starting with the 4-byte CPI length).
    fn build_clpi(source_packet_count: u32, cpi_data: Option<&[u8]>) -> Vec<u8> {
        // Need at least 60 bytes for the header area: "HDMV"/"0200" magic,
        // seq_info_start/prog_info_start (unused here), cpi_start, reserved
        // padding, then the ClipInfo section ending in source_packet_count.

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

    // ─────────────────────────────────────────────────────────────────────
    // Added hardening tests, grounded in the BD-ROM CLPI spec byte layout.
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

    // ─────────────────────────────────────────────────────────────────────
    // Section-offset gates in `parse`.
    // ─────────────────────────────────────────────────────────────────────

    /// A prog_info_start of 0 means "no ProgramInfo section" — the CLPI
    /// header bytes at offset 0 must NOT be reinterpreted as a ProgramInfo
    /// table. The fixture is crafted so that parsing from offset 0 WOULD
    /// yield a stream (num_programs at [5], a second program header whose
    /// num_streams byte at [20] is 1, then a well-formed stream record), so
    /// the empty result can only come from the `prog_info_start > 0` gate.
    #[test]
    fn prog_info_start_zero_does_not_parse_header_as_program_info() {
        let mut data = build_clpi(1000, None);
        data[5] = 2; // num_programs = 2 if read from offset 0
        // program 0 header = data[6..14]; its num_streams byte is data[12],
        // which is prog_info_start's first byte and must stay 0.
        data[20] = 1; // program 1 (header data[14..22]) declares 1 stream
        data[22..24].copy_from_slice(&0x1011u16.to_be_bytes()); // pid
        data[24] = 2; // sci_len
        data[25] = 0x1B; // coding_type H.264
        data[26] = 0x61; // video format 6 / rate 1
        let clip = parse(&data).expect("should parse");
        assert!(
            clip.streams.is_empty(),
            "prog_info_start == 0 must mean absent, got {:?}",
            clip.streams
        );
    }

    // ─────────────────────────────────────────────────────────────────────
    // parse_program_info
    // ─────────────────────────────────────────────────────────────────────

    /// Secondary audio (0xA1 AC-3+ secondary, 0xA2 DTS-HD secondary) has the
    /// same stream_coding_info layout as primary audio: sci[1] carries
    /// audio_presentation_type in the high nibble and sampling_frequency in
    /// the low nibble, sci[2..5] the ISO 639-2 language. Both sub-fields and
    /// the language must be populated.
    #[test]
    fn program_info_secondary_audio_fields() {
        for coding in [c_ac3_plus_secondary(), c_dts_hd_secondary()] {
            let sci = vec![coding, 0x61, b'd', b'e', b'u'];
            let pi = build_program_info(&[(0x1A00, sci)]);
            let data = build_clpi_with_proginfo(100, &pi, None);
            let clip = parse(&data).expect("should parse");
            assert_eq!(clip.streams.len(), 1, "coding {coding:#04x}");
            let s = &clip.streams[0];
            assert_eq!(s.coding_type, coding);
            // 0x61: high nibble 6, low nibble 1 — distinct values, so a
            // swapped/ORed/XORed nibble extraction cannot pass.
            assert_eq!(s.language, "deu", "coding {coding:#04x}");
        }
    }

    fn c_ac3_plus_secondary() -> u8 {
        crate::consts::coding_type::AC3_PLUS_SECONDARY
    }
    fn c_dts_hd_secondary() -> u8 {
        crate::consts::coding_type::DTS_HD_SECONDARY
    }

    /// A stream_coding_info of exactly 1 byte (coding_type only) is the
    /// minimum the parser accepts: the stream is recorded with its PID and
    /// coding_type, and every sub-field that needs more bytes stays empty.
    /// Notably a PG stream must NOT read sci[1..4] when only sci[0] exists.
    #[test]
    fn program_info_sci_len_one_yields_bare_stream() {
        let pi = build_program_info(&[(0x1200, vec![0x90u8])]);
        let data = build_clpi_with_proginfo(100, &pi, None);
        let clip = parse(&data).expect("should not panic");
        assert_eq!(clip.streams.len(), 1);
        assert_eq!(clip.streams[0].pid, 0x1200);
        assert_eq!(clip.streams[0].coding_type, 0x90);
        assert_eq!(clip.streams[0].language, "");
    }

    /// Below the 6-byte ProgramInfo header (length(4)+reserved(1)+
    /// num_programs(1)) there is nothing to read; the length guard must fire
    /// before `data[5]`.
    #[test]
    fn program_info_below_header_size_is_empty() {
        for len in 0..6usize {
            assert!(parse_program_info(&vec![0u8; len]).is_empty(), "len={len}");
        }
    }

    /// A declared program whose 8-byte header runs past the section end must
    /// stop before reading num_streams at `data[pos + 6]`.
    #[test]
    fn program_info_truncated_program_header_is_empty() {
        // length(4) + reserved(1) + num_programs=1 (1) + only 4 of the 8
        // program-header bytes.
        let mut data = vec![0u8; 6];
        data[5] = 1;
        data.extend_from_slice(&[0u8; 4]);
        assert!(parse_program_info(&data).is_empty());
    }

    /// A declared stream whose 3-byte header (pid(2)+sci_len(1)) runs past
    /// the section end must stop before reading the PID.
    #[test]
    fn program_info_truncated_stream_header_is_empty() {
        let mut data = vec![0u8; 6];
        data[5] = 1; // num_programs
        data.extend_from_slice(&[0u8; 8]); // program header
        data[6 + 6] = 1; // num_streams = 1
        data.extend_from_slice(&[0u8; 2]); // only 2 of the 3 stream bytes
        assert_eq!(data.len(), 16);
        assert!(parse_program_info(&data).is_empty());
    }

    // ─────────────────────────────────────────────────────────────────────
    // parse_cpi — low-level fixtures
    // ─────────────────────────────────────────────────────────────────────
}
