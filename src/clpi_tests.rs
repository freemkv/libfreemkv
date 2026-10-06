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
fn parse_truncated_clipinfo_parses_with_unknown_count() {
    let full = build_clpi(1000, None);
    for len in 16..60usize {
        let c = parse(&full[..len]).unwrap_or_else(|e| panic!("len {len}: {e:?}"));
        assert_eq!(c.source_packet_count, 0, "len {len}");
    }
    assert_eq!(parse(&full[..60]).expect("60").source_packet_count, 1000);
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

// Added hardening tests, grounded in the BD-ROM CLPI spec byte layout. Build a ProgramInfo
// section; `streams` = Vec<(pid, sci_bytes)>.
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

/// Input shorter than the 16-byte header is rejected before any field read.
#[test]
fn under_16_bytes_rejected() {
    assert!(parse(&[0u8; 15]).is_err());
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

/// An audio or TextST sci shorter than 5 bytes has no language field: the stream is
/// kept, the language stays empty, and nothing indexes past the sci.
#[test]
fn program_info_short_audio_and_textst_sci_keep_the_stream_without_a_language() {
    let pi = build_program_info(&[
        (0x1100, vec![0x83u8, 0x61, b'e', b'n']),
        (0x1200, vec![0x92u8, 0x01, b'e', b'n']),
    ]);
    let data = build_clpi_with_proginfo(100, &pi, None);
    let clip = parse(&data).expect("should parse");
    assert_eq!(clip.streams.len(), 2);
    assert!(clip.streams.iter().all(|s| s.language.is_empty()));
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

// Best-effort: a stream whose sci_len runs past the section end
// (`sci_end > data.len()`) returns streams collected so far (none
// here), never panics — source returns `out` early on overflow.
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

// Section-offset gates in `parse`: prog_info_start == 0 means "no ProgramInfo section" —
// offset-0 bytes must NOT be reinterpreted as a table.
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

// parse_program_info: secondary audio (0xA1/0xA2) shares primary
// audio's sci layout — sci[1] = presentation-type/rate nibbles,
// sci[2..5] = ISO 639-2 language. Both sub-fields must be populated.
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

// A 1-byte sci (coding_type only) is the accepted minimum: PID and
// coding_type get recorded, all sub-fields needing more bytes stay
// empty — a PG stream must NOT read sci[1..4] when only sci[0] exists.
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

/// TextST (0x92): coding_type + character_code + 3-byte language, so the
/// language sits at sci[2..5].
#[test]
fn program_info_text_subtitle_lang_offset() {
    let sci = vec![0x92u8, 0x01, b'd', b'e', b'u'];
    let pi = build_program_info(&[(0x1800, sci)]);
    let clip = parse(&build_clpi_with_proginfo(100, &pi, None)).expect("parse");
    assert_eq!(clip.streams[0].coding_type, 0x92);
    assert_eq!(clip.streams[0].language, "deu");
}

/// IG (0x91) shares the PG layout: language at sci[1..4].
#[test]
fn program_info_ig_lang_offset() {
    let sci = vec![0x91u8, b'e', b's', b'p'];
    let pi = build_program_info(&[(0x1400, sci)]);
    let clip = parse(&build_clpi_with_proginfo(100, &pi, None)).expect("parse");
    assert_eq!(clip.streams[0].language, "esp");
}

/// Streams from every program are collected, in order.
#[test]
fn program_info_multiple_programs_all_collected() {
    let mut body = vec![0u8, 2]; // reserved, num_programs = 2
    for (pid, lang) in [(0x1200u16, b"fra"), (0x1201u16, b"jpn")] {
        body.extend_from_slice(&[0u8; 6]); // spn + pmt_pid
        body.extend_from_slice(&[1, 0]); // num_streams, num_groups
        body.extend_from_slice(&pid.to_be_bytes());
        body.push(4);
        body.push(0x90);
        body.extend_from_slice(lang);
    }
    let mut pi = (body.len() as u32).to_be_bytes().to_vec();
    pi.extend_from_slice(&body);
    let clip = parse(&build_clpi_with_proginfo(100, &pi, None)).expect("parse");
    assert_eq!(clip.streams.len(), 2);
    assert_eq!(clip.streams[0].language, "fra");
    assert_eq!(clip.streams[1].pid, 0x1201);
    assert_eq!(clip.streams[1].language, "jpn");
}

/// A trailing CPI section after ProgramInfo does not disturb parsing.
#[test]
fn cpi_section_present_still_parses() {
    let pi = build_program_info(&[(0x1200, vec![0x90u8, b'f', b'r', b'a'])]);
    let cpi = [0u8, 0, 0, 2, 0, 0];
    let data = build_clpi_with_proginfo(777, &pi, Some(&cpi));
    let clip = parse(&data).expect("parse");
    assert_eq!(clip.source_packet_count, 777);
    assert_eq!(clip.streams.len(), 1);
    let clip = parse(&build_clpi(5, Some(&cpi))).expect("parse");
    assert_eq!(clip.source_packet_count, 5);
}

// ─────────────────────────────────────────────────────────────────────
// parse_cpi — low-level fixtures
// ─────────────────────────────────────────────────────────────────────
