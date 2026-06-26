//! End-to-end `fvi://` tests: drive a REAL MPEG-2 Program-Stream image through
//! the public highway (`build_iso_pipeline`, MpegPs → PS demux → `Mpeg2Parser`)
//! and into the `fvi://` sink built by `output()`, then parse the `.fvi` back
//! and assert the per-picture index is correct.
//!
//! These tests deliberately use only the public API and the real parser /
//! pipeline — no stubbed frames that bypass the demuxer or the codec parse.

use libfreemkv::disc::{
    Codec, ColorSpace, ContentFormat, DiscTitle, Extent, FrameRate, HdrFormat, Resolution, Stream,
    VideoStream,
};
use libfreemkv::pes::Stream as PesStream;
use libfreemkv::{DecryptKeys, SectorSource, build_iso_pipeline, output};
use std::path::PathBuf;

/// DVD video PES stream_id (0xE0).
const DVD_VIDEO_STREAM_ID: u8 = 0xE0;

// ── MPEG-2 elementary-stream fixture builders (mirror the in-crate ones) ──────

/// 720x480, 4:3, 29.97 (aspect_ratio_information=2, frame_rate_code=4).
fn m2_seq_header() -> Vec<u8> {
    let (w, h, aspect, fr): (u16, u16, u8, u8) = (720, 480, 2, 4);
    let mut hdr = vec![0x00, 0x00, 0x01, 0xB3u8];
    hdr.push((w >> 4) as u8);
    hdr.push((((w & 0x0F) as u8) << 4) | (((h >> 8) & 0x0F) as u8));
    hdr.push((h & 0xFF) as u8);
    hdr.push((aspect << 4) | (fr & 0x0F));
    hdr.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0x00]);
    hdr
}

/// GOP header (00 00 01 B8) with a zeroed time-code / flags.
fn m2_gop() -> Vec<u8> {
    vec![0x00, 0x00, 0x01, 0xB8u8, 0x00, 0x00, 0x00, 0x00]
}

/// One coded picture: picture header (coding_type, temporal_reference) + a
/// picture coding extension carrying tff=1 (00 00 01 B5, ext-id 1000), + slice
/// padding.
fn m2_pic(coding_type: u8, tr: u16) -> Vec<u8> {
    let b4 = ((tr >> 2) & 0xFF) as u8;
    let b5 = (((tr & 0x03) as u8) << 6) | ((coding_type & 0x07) << 3);
    let mut au = vec![0x00, 0x00, 0x01, 0x00u8, b4, b5, 0x00, 0x00];
    // Picture coding extension: e0=ext-id 1000, e2=0x03 (frame picture),
    // e3 bit7 = top_field_first = 1.
    au.extend_from_slice(&[0x00, 0x00, 0x01, 0xB5u8, 0x80, 0x00, 0x03, 0x80, 0x00]);
    au.extend_from_slice(&[0xAA; 32]);
    au
}

// ── Program-Stream packing ────────────────────────────────────────────────────

/// A 14-byte MPEG-2 PS pack header (00 00 01 BA …) with no stuffing.
fn ps_pack_header() -> Vec<u8> {
    let mut p = vec![0x00, 0x00, 0x01, 0xBAu8];
    // 9 bytes of SCR/mux-rate fields (content irrelevant to the demuxer's
    // framing) + a final byte whose low 3 bits are pack_stuffing_length = 0.
    p.extend_from_slice(&[0x44, 0x00, 0x04, 0x00, 0x04, 0x01, 0x01, 0x89, 0xC3]);
    p.push(0xF8); // stuffing_length = 0 (low 3 bits)
    p
}

/// A video PES (stream_id 0xE0) carrying `es`, with a 33-bit PTS in 90 kHz
/// ticks and a bounded PES_packet_length. PTS prefix nibble is 0b0010.
fn video_pes(es: &[u8], pts: u64) -> Vec<u8> {
    let mut pes = vec![0x00, 0x00, 0x01, DVD_VIDEO_STREAM_ID];
    // PES header: flags1=0x80, flags2=0x80 (PTS only), header_data_len=5.
    let mut body = vec![0x80u8, 0x80, 0x05];
    // 5-byte PTS ('0010' marker + 33-bit value with marker bits).
    let p = pts & 0x1_FFFF_FFFF;
    body.push(0x21 | (((p >> 30) & 0x07) << 1) as u8);
    body.push(((p >> 22) & 0xFF) as u8);
    body.push((0x01 | (((p >> 15) & 0x7F) << 1)) as u8);
    body.push(((p >> 7) & 0xFF) as u8);
    body.push((0x01 | ((p & 0x7F) << 1)) as u8);
    body.extend_from_slice(es);
    let len = body.len() as u16;
    pes.extend_from_slice(&len.to_be_bytes());
    pes.extend_from_slice(&body);
    pes
}

/// One GOP's worth of ES (seq header + GOP + I + P + B pictures).
fn gop_es() -> Vec<u8> {
    let mut es = m2_seq_header();
    es.extend_from_slice(&m2_gop());
    es.extend_from_slice(&m2_pic(1, 0)); // I (keyframe, GOP opener)
    es.extend_from_slice(&m2_pic(2, 2)); // P
    es.extend_from_slice(&m2_pic(3, 1)); // B
    es
}

/// In-memory sector source serving a fixed byte image.
struct MemSource {
    data: Vec<u8>,
}

impl SectorSource for MemSource {
    fn capacity_sectors(&self) -> u32 {
        (self.data.len() / 2048) as u32
    }
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> libfreemkv::error::Result<usize> {
        let start = lba as usize * 2048;
        let want = count as usize * 2048;
        for (i, b) in buf[..want].iter_mut().enumerate() {
            *b = self.data.get(start + i).copied().unwrap_or(0);
        }
        Ok(want)
    }
}

fn mpeg2_dvd_title(extent_sectors: u32) -> DiscTitle {
    let mut title = DiscTitle::empty();
    title.streams.push(Stream::Video(VideoStream {
        pid: 0xE0, // DVD_VIDEO_PID
        codec: Codec::Mpeg2,
        resolution: Resolution::R480i,
        frame_rate: FrameRate::F29_97,
        hdr: HdrFormat::Sdr,
        color_space: ColorSpace::Smpte170m,
        display_aspect: Some((4, 3)),
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    }));
    title.content_format = ContentFormat::MpegPs;
    title.extents = vec![Extent {
        start_lba: 0,
        sector_count: extent_sectors,
    }];
    title
}

/// Tiny unique temp dir helper.
fn tempdir() -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("fmkv_fvi_pipe_{}_{}", std::process::id(), n));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Build a 6-sector PS image: GOP A in sector 0, GOP B in sector 3 — each in
/// its own 3-sector AACS-aligned region so the prefetcher's unit alignment is
/// satisfied and each batch carries a distinct, ascending source offset.
fn two_gop_image() -> Vec<u8> {
    let mut data = vec![0u8; 6 * 2048];

    let mut a = ps_pack_header();
    a.extend_from_slice(&video_pes(&gop_es(), 0));
    data[..a.len()].copy_from_slice(&a);

    let mut b = ps_pack_header();
    b.extend_from_slice(&video_pes(&gop_es(), 3003)); // ~0.1s later
    let off = 3 * 2048;
    data[off..off + b.len()].copy_from_slice(&b);

    data
}

/// Drive the real highway and write every frame into the `fvi://` sink.
fn run_to_fvi(image: Vec<u8>, title: DiscTitle, path: &std::path::Path) {
    let mut input = build_iso_pipeline(
        MemSource { data: image },
        title.clone(),
        DecryptKeys::None,
        3, // 3-sector (one AACS unit) batches → one source stamp per GOP region
        ContentFormat::MpegPs,
        None,
        None,
    )
    .expect("pipeline builds");

    let url = format!("fvi://{}", path.display());
    let mut sink = output(&url, &title).expect("fvi sink opens");

    while let Some(frame) = input.read().expect("read ok") {
        sink.write(&frame).expect("sink write ok");
    }
    sink.finish().expect("sink finish ok");
}

#[test]
fn fvi_sink_indexes_real_mpeg2_pipeline_output() {
    let dir = tempdir();
    let path = dir.join("movie.fvi");
    run_to_fvi(two_gop_image(), mpeg2_dvd_title(6), &path);

    let text = std::fs::read_to_string(&path).unwrap();
    let mut lines = text.lines();

    // ── Header line (docs/FVI_FORMAT.md v1 schema) ─────────────────────────────
    let header: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    assert_eq!(header["format"], "freemkv/video-index");
    assert_eq!(header["fvi_version"], 1);
    assert_eq!(header["timescale"], 1_000_000_000u64);
    let stream = &header["stream"];
    assert_eq!(stream["codec"], "mpeg2video");
    assert_eq!(stream["width"], 720);
    assert_eq!(stream["height"], 480);
    assert_eq!(stream["dar"], serde_json::json!([4, 3])); // anamorphic DVD
    assert_eq!(stream["scan"], "interlaced"); // 480i
    assert_eq!(stream["frame_rate"], serde_json::json!([30000, 1001]));
    // SMPTE 170M → CICP (6,6,6), limited range.
    assert_eq!(stream["colour"]["primaries"], 6);
    assert_eq!(stream["colour"]["transfer"], 6);
    assert_eq!(stream["colour"]["matrix"], 6);
    assert_eq!(stream["colour"]["range"], "limited");
    // Provenance root: medium defaults to "file", sector_size present.
    assert_eq!(header["source"]["sector_size"], 2048);

    // ── Records ───────────────────────────────────────────────────────────────
    let records: Vec<serde_json::Value> = lines.map(|l| serde_json::from_str(l).unwrap()).collect();
    assert!(
        records.len() >= 4,
        "two GOPs of I/P/B → at least 4 pictures, got {}",
        records.len()
    );

    // `n` is 0-based and contiguous in coded order.
    for (i, r) in records.iter().enumerate() {
        assert_eq!(r["n"], i as u64, "record n must be contiguous coded order");
    }

    // Every picture carries the codec-agnostic coding members from the REAL
    // parser, derived through the `PictureInfo` accessors.
    for r in &records {
        assert!(
            ["I", "P", "B"].contains(&r["type"].as_str().unwrap()),
            "type must be a real coding type, got {}",
            r["type"]
        );
        // tff was set in the picture coding extension fixture, the frame is an
        // interlaced (non-progressive) frame picture → field_order "tff",
        // progressive false, 2 displayed fields.
        assert_eq!(
            r["field_order"], "tff",
            "top_field_first survives the parse as field_order"
        );
        assert_eq!(r["progressive"], false);
        assert_eq!(r["nb_fields"], 2);
        // No GOP-closure signal is carried by the codec-agnostic PictureInfo, so
        // the `gop` member is honestly omitted (not fabricated).
        assert!(r.get("gop").is_none(), "gop member omitted, never guessed");
    }

    // Exactly two I-pictures (one per GOP), each `type` I and a random-access
    // point (`key` true — the parser-flagged intra/decode-restart point). The
    // fixture's only intra pictures are the two GOP-opening I-frames.
    let key_pics: Vec<&serde_json::Value> = records
        .iter()
        .filter(|r| r["key"] == serde_json::Value::Bool(true))
        .collect();
    assert_eq!(key_pics.len(), 2, "two intra/random-access pictures");
    for opener in &key_pics {
        assert_eq!(opener["type"], "I", "a random-access point is an I-picture");
    }

    // The two stamped source sectors (sector 0 region and sector 3 region) reach
    // the index, in ascending order — provenance carried, never reconstructed.
    let src_sectors: Vec<u64> = records
        .iter()
        .filter_map(|r| r["src"]["sector"].as_u64())
        .collect();
    assert_eq!(
        src_sectors,
        vec![0, 3],
        "stamped src sectors must reach the .fvi in arrival order; got {src_sectors:?}"
    );
    // For each stamped record, sector == byte / 2048 (SourcePos::at_byte). The
    // byte offset is exact (here 14 into each region, just past the pack
    // header), so it is NOT sector-aligned — provenance is byte-exact.
    for r in &records {
        if let (Some(sector), Some(byte)) = (r["src"]["sector"].as_u64(), r["src"]["byte"].as_u64())
        {
            assert_eq!(sector, byte / 2048, "src.sector must equal src.byte / 2048");
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Codec-agnostic path: a non-MPEG2 stream (HEVC/H.264/VC-1) emits frames with
/// `coding == None` but real `keyframe` + `source` + `pts`. The `.fvi` records
/// must still be USEFUL — `key`/`type` from the frame's keyframe flag, `src`/
/// `pts` populated — NOT degraded to `type:"?"`/`src:null` just because
/// `PictureInfo` is MPEG-2-specific. The genuine null/"P"-fallback path only
/// fires when a field is truly absent (no provenance / non-key). No panic.
#[test]
fn fvi_sink_indexes_non_mpeg2_frames_codec_agnostically() {
    use libfreemkv::pes::{PesFrame, SourcePos};

    let dir = tempdir();
    let path = dir.join("nocoding.fvi");
    let title = mpeg2_dvd_title(0);

    let mk = |pts: i64, keyframe: bool, source: Option<SourcePos>| PesFrame {
        track: 0,
        pts,
        keyframe,
        data: vec![0u8; 8],
        duration_ns: None,
        source,
        coding: None, // non-MPEG2: no PictureInfo
    };

    let url = format!("fvi://{}", path.display());
    let mut sink = output(&url, &title).expect("fvi sink opens");
    // IDR (keyframe) with real provenance — must NOT be null/"?".
    sink.write(&mk(1234, true, Some(SourcePos::at_byte(8192))))
        .unwrap();
    // Non-key with provenance.
    sink.write(&mk(5678, false, Some(SourcePos::at_byte(16384))))
        .unwrap();
    // Keyframe with NO provenance — src genuinely null, but key/type still set.
    sink.write(&mk(9012, true, None)).unwrap();
    sink.finish().unwrap();

    let text = std::fs::read_to_string(&path).unwrap();
    let recs: Vec<serde_json::Value> = text
        .lines()
        .skip(1)
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(recs.len(), 3);

    // IDR: key true, type "I" (from keyframe), real src + pts, no mpeg2 fields.
    assert_eq!(recs[0]["key"], true, "HEVC IDR → key from frame.keyframe");
    assert_eq!(recs[0]["type"], "I");
    assert_eq!(recs[0]["pts"], 1234);
    assert_eq!(recs[0]["src"]["sector"], 4); // 8192 / 2048
    assert_eq!(recs[0]["src"]["byte"], 8192);
    assert!(
        recs[0].get("field_order").is_none() && recs[0].get("nb_fields").is_none(),
        "coding-absent record omits field_order/nb_fields"
    );
    assert!(recs[0].get("dts").is_none(), "no DTS on a frame → omitted");

    // Non-key with provenance: key false, type "P", src still present.
    assert_eq!(recs[1]["key"], false);
    assert_eq!(recs[1]["type"], "P");
    assert_eq!(recs[1]["src"]["sector"], 8); // 16384 / 2048

    // Keyframe without provenance: key/type still set, src genuinely null.
    assert_eq!(recs[2]["key"], true);
    assert_eq!(recs[2]["type"], "I");
    assert_eq!(recs[2]["src"], serde_json::Value::Null);

    let _ = std::fs::remove_dir_all(&dir);
}
