//! `demux://` sink — write each track of a title as a separate elementary
//! stream file (per-codec ES, PGS `.sup`, VobSub `.idx`/`.sub`, LPCM raw PCM),
//! plus a chapters file and per-audio-track delay metadata.
//!
//! This is a write-only [`crate::pes::Stream`]. Instead of muxing the per-track
//! `PesFrame` stream into a single container (as `MkvStream` does), it routes
//! each frame's payload to the file for `frame.track`, post-processing where the
//! internal codec `Frame` form differs from the standalone on-disk ES form:
//!
//! - **HEVC / H.264**: the codec parsers emit hvcC/avcC-style 4-byte
//!   length-prefixed NALs (start codes stripped) and carry the parameter sets
//!   out-of-band in `codec_private`. A standalone `.hevc`/`.h264` needs Annex-B
//!   framing with the parameter sets prepended — see [`AnnexBWriter`].
//! - **PGS**: the parser collapses a display/clear PCS pair into one
//!   duration-bearing `Frame` with the raw segment payload but no `PG` magic /
//!   timestamp header. A `.sup` needs the HDMV segment framing rebuilt — see
//!   [`PgsSupWriter`].
//! - **VobSub**: the `.sub` is the raw SPU stream; the `.idx` sidecar is
//!   synthesized from per-SPU PTS + byte offsets — see [`VobSubWriter`].
//!
//! Every other codec writes `frame.data` verbatim ([`PassthroughWriter`]).
//!
//! The sink does NOT touch the MKV mux path; it is purely additive.

use crate::disc::{Chapter, Codec, DiscTitle, Stream as DiscStream};
use crate::mux::hevc::{append_length_prefixed_as_annex_b, avcc_to_annex_b, hvcc_to_annex_b};
use crate::mux::timeline::TimelineContinuity;
use crate::pes::{PesFrame, Stream};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Filename-naming strategy for the per-track files.
// `allow(dead_code)`: the sink honours all variants, but only the `#[default]` is
// constructed today (`output()` builds `DemuxOptions::default()`). The alternates
// are a staged option surface awaiting the CLI `--naming` flag (not yet wired).
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Naming {
    /// `<base> <track> <lang> <codec> [DELAY <n>ms].<ext>` — human-readable.
    #[default]
    Friendly,
    /// `<base> <pid>.<ext>` — names by MPEG PID.
    Pid,
    /// `track<NN>.<ext>` — bare track index.
    Track,
}

/// How (and whether) to record audio sync delay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DelayMode {
    /// Embed `DELAY <n>ms` in each audio filename (mkvmerge-readable).
    #[default]
    Filename,
    /// Write a `<base> delays.txt` sidecar instead.
    Sidecar,
    /// Record no delay information.
    None,
}

/// Chapter export format.
// `allow(dead_code)`: only the `#[default]` XML variant is constructed today (via
// `DemuxOptions::default()`); OGM/Both await the CLI `--chapters` flag.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChaptersFmt {
    /// mkvmerge chapter XML.
    #[default]
    Xml,
    /// OGM/simple `CHAPTERnn=`/`CHAPTERnnNAME=` text.
    Ogm,
    /// Both files.
    Both,
}

/// Options controlling `demux://` output, assembled from CLI flags.
#[derive(Debug, Clone)]
pub struct DemuxOptions {
    /// Filename stem (e.g. the playlist or disc label).
    pub base: String,
    /// Naming strategy.
    pub naming: Naming,
    /// Delay-metadata mode.
    pub delay_mode: DelayMode,
    /// Chapter export format.
    pub chapters_fmt: ChaptersFmt,
    /// Also export chapters (the `chapters` selection keyword / default on).
    pub export_chapters: bool,
    /// Selected track indices. `None` = all tracks.
    pub selection: Option<Vec<usize>>,
}

impl Default for DemuxOptions {
    fn default() -> Self {
        Self {
            base: "title".to_string(),
            naming: Naming::default(),
            delay_mode: DelayMode::default(),
            chapters_fmt: ChaptersFmt::default(),
            export_chapters: true,
            selection: None,
        }
    }
}

/// Track class, used for delay attribution and naming.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrackKind {
    Video,
    Audio,
    Subtitle,
}

// ── Codec → on-disk extension ────────────────────────────────────────────────

/// File extension (without the dot) for a codec's standalone elementary stream.
/// Chosen to match what mkvmerge / x265 / ffmpeg / BDSup2Sub expect.
fn extension_for(codec: Codec) -> &'static str {
    match codec {
        Codec::Hevc => "hevc",
        Codec::H264 => "h264",
        Codec::Vc1 => "vc1",
        Codec::Mpeg2 => "m2v",
        Codec::Mpeg1 => "mpv",
        Codec::Av1 => "obu",
        Codec::TrueHd => "thd",
        Codec::DtsHdMa | Codec::DtsHdHr => "dtshd",
        Codec::Dts => "dts",
        Codec::Ac3 => "ac3",
        Codec::Ac3Plus => "eac3",
        Codec::Lpcm => "pcm",
        Codec::Aac => "aac",
        Codec::Mp2 => "mp2",
        Codec::Mp3 => "mp3",
        Codec::Flac => "flac",
        Codec::Opus => "opus",
        Codec::Pgs => "sup",
        Codec::DvdSub => "sub",
        Codec::Srt => "srt",
        Codec::Ssa => "ssa",
        Codec::Unknown(_) => "bin",
    }
}

/// Short codec label for friendly filenames.
fn codec_label(codec: Codec) -> &'static str {
    match codec {
        Codec::Hevc => "HEVC",
        Codec::H264 => "AVC",
        Codec::Vc1 => "VC1",
        Codec::Mpeg2 => "MPEG2",
        Codec::Mpeg1 => "MPEG1",
        Codec::Av1 => "AV1",
        Codec::TrueHd => "TrueHD",
        Codec::DtsHdMa => "DTS-HD-MA",
        Codec::DtsHdHr => "DTS-HD-HR",
        Codec::Dts => "DTS",
        Codec::Ac3 => "AC3",
        Codec::Ac3Plus => "EAC3",
        Codec::Lpcm => "LPCM",
        Codec::Aac => "AAC",
        Codec::Mp2 => "MP2",
        Codec::Mp3 => "MP3",
        Codec::Flac => "FLAC",
        Codec::Opus => "Opus",
        Codec::Pgs => "PGS",
        Codec::DvdSub => "VobSub",
        Codec::Srt => "SRT",
        Codec::Ssa => "SSA",
        Codec::Unknown(_) => "Unknown",
    }
}

// ── Per-codec ES writers ─────────────────────────────────────────────────────

/// Per-codec elementary-stream writer.
///
/// Most codecs are pass-through; HEVC/H.264 re-frame to Annex-B, PGS re-frames
/// to `.sup`, VobSub records `.idx` entries and emits the sidecar at finish.
trait EsWriter: Send {
    /// Write one frame's payload to `w`. Returns the number of bytes written to
    /// the main file (used by the VobSub writer for `.idx` filepos tracking).
    fn write_frame(&mut self, w: &mut dyn Write, f: &PesFrame, pts_ns: i64) -> io::Result<usize>;

    /// Finalize. Default: no-op. The VobSub writer serializes its `.idx` here.
    fn finish(&mut self, _w: &mut dyn Write) -> io::Result<()> {
        Ok(())
    }
}

/// Verbatim pass-through: `frame.data` is already a standalone ES.
struct PassthroughWriter;

impl EsWriter for PassthroughWriter {
    fn write_frame(&mut self, w: &mut dyn Write, f: &PesFrame, _pts: i64) -> io::Result<usize> {
        w.write_all(&f.data)?;
        Ok(f.data.len())
    }
}

/// HEVC/H.264 writer: reframes 4-byte-length-prefixed NALs (the hvcC/avcC form
/// the parsers emit) into Annex-B, prepending the parameter sets once.
struct AnnexBWriter {
    /// Annex-B-framed VPS/SPS/PPS (or SPS/PPS), parsed from the hvcC/avcC.
    params: Vec<u8>,
    wrote_params: bool,
}

impl AnnexBWriter {
    fn new(codec: Codec, codec_private: Option<&[u8]>) -> Self {
        let params = codec_private
            .map(|rec| annexb_param_sets(codec, rec))
            .unwrap_or_default();
        Self {
            params,
            wrote_params: false,
        }
    }
}

impl EsWriter for AnnexBWriter {
    fn write_frame(&mut self, w: &mut dyn Write, f: &PesFrame, _pts: i64) -> io::Result<usize> {
        let mut n = 0;
        if !self.wrote_params {
            // Prepend the parameter sets at the very start of the stream so a
            // raw decoder (which has no hvcC/avcC) sees them in-band.
            if !self.params.is_empty() {
                w.write_all(&self.params)?;
                n += self.params.len();
            }
            self.wrote_params = true;
        }
        // Reframe via the canonical length-prefixed→Annex-B converter (single
        // source of truth across all muxers — see `crate::mux::hevc`). It skips
        // zero-length NALs and drops a truncated trailing NAL without panicking,
        // rather than `break`ing on the first zero-length NAL.
        let mut scratch = Vec::with_capacity(f.data.len() + (f.data.len() / 32) + 4);
        append_length_prefixed_as_annex_b(&mut scratch, &f.data);
        w.write_all(&scratch)?;
        n += scratch.len();
        Ok(n)
    }
}

/// Extract the parameter-set NALs from an hvcC (HEVC) or avcC (H.264)
/// configuration record and return them as a single Annex-B blob
/// (`00 00 00 01 | NAL …`). Returns an empty Vec if the record can't be parsed.
/// Delegates to the canonical hvcC/avcC → Annex-B converters in
/// [`crate::mux::hevc`] — the single source of truth across all muxers.
fn annexb_param_sets(codec: Codec, record: &[u8]) -> Vec<u8> {
    let converted = match codec {
        Codec::Hevc => hvcc_to_annex_b(record),
        Codec::H264 => avcc_to_annex_b(record),
        _ => return Vec::new(),
    };
    converted.unwrap_or_else(|| {
        // A malformed hvcC/avcC record yields no parameter sets. Returning empty
        // means keyframes ship WITHOUT in-band SPS/PPS — playable from the first
        // keyframe but broken for seek-to-arbitrary-point and hardware decoders.
        // Surface it rather than silently degrading the output.
        tracing::warn!(
            target: "mux",
            ?codec,
            "codec-private (hvcC/avcC) parse failed; keyframes will lack in-band SPS/PPS"
        );
        Vec::new()
    })
}

/// PGS `.sup` writer: rebuilds the HDMV segment framing the parser stripped.
///
/// The parser hands us the concatenated PGS segments of a display set in
/// `frame.data` (segment_type + segment_size + payload, repeated), with no `PG`
/// magic and no PTS/DTS. A `.sup` prefixes each segment with a 13-byte header:
///   `0x50 0x47` ("PG") | PTS u32 BE | DTS u32 BE | (segment_type|size already
///   present in the payload).
/// When the parser folded a trailing clear (`duration_ns` set), we re-emit it as
/// an empty composition at `pts + duration` so players time the subtitle out.
struct PgsSupWriter;

// ── PGS / HDMV segment framing constants ─────────────────────────────────────
// HDMV Presentation Graphics Stream, as published in the Blu-ray Disc
// Read-Only Format (BD-ROM) Part 3 graphics-stream specification (and the
// public US 2009/0185789 A1 application that documents the segment layout).

/// `.sup` per-segment magic: ASCII "PG" (0x50 0x47) starting each segment's
/// 13-byte header (magic | PTS u32 BE | DTS u32 BE) in a PGStream `.sup` file.
const SUP_MAGIC: [u8; 2] = [0x50, 0x47];
/// Size in bytes of the `.sup` per-segment header (magic 2 + PTS 4 + DTS 4).
const SUP_HEADER_LEN: usize = SUP_MAGIC.len() + 4 + 4;
/// PGS segment type: Presentation Composition Segment (PCS).
const SEG_PCS: u8 = 0x16;
/// PGS segment type: END of display set.
const SEG_END: u8 = 0x80;
/// PCS `composition_state` value: Normal (an update to the current epoch).
const PCS_COMPOSITION_STATE_NORMAL: u8 = 0x00;
/// PGS segment header on the wire (inside `frame.data`): type(1) + size(2 BE).
const PGS_SEG_HEADER_LEN: usize = 3;
/// Byte offset of `width`/`height` within a PCS segment (after type+size).
const PCS_WIDTH_OFFSET: usize = PGS_SEG_HEADER_LEN; // 3

/// 90 kHz ticks from nanoseconds (saturating into u32 for the `.sup` header).
fn ns_to_90k(pts_ns: i64) -> u32 {
    if pts_ns <= 0 {
        return 0;
    }
    // 90_000 / 1e9 = 9 / 100_000
    let ticks = (pts_ns as i128 * 9) / 100_000;
    ticks.clamp(0, u32::MAX as i128) as u32
}

impl PgsSupWriter {
    /// Walk the concatenated segments in `data`, emitting each with a `PG`
    /// header carrying `pts90k`/`dts90k`. Returns bytes written.
    fn emit_segments(
        data: &[u8],
        pts90k: u32,
        dts90k: u32,
        w: &mut dyn Write,
    ) -> io::Result<usize> {
        let mut pos = 0;
        let mut written = 0;
        // Each PGS segment in the payload is: type(1) + size(2 BE) + size bytes.
        while pos + PGS_SEG_HEADER_LEN <= data.len() {
            let size = u16::from_be_bytes([data[pos + 1], data[pos + 2]]) as usize;
            let seg_end = pos + PGS_SEG_HEADER_LEN + size;
            if seg_end > data.len() {
                break;
            }
            w.write_all(&SUP_MAGIC)?;
            w.write_all(&pts90k.to_be_bytes())?;
            w.write_all(&dts90k.to_be_bytes())?;
            w.write_all(&data[pos..seg_end])?;
            written += SUP_HEADER_LEN + size;
            pos = seg_end;
        }
        Ok(written)
    }

    /// Build a synthetic "clear" display set: an empty PCS (0 composition
    /// objects) followed by an END segment. The parser folds the original
    /// clear/end PCS pair's wipe time into the display frame's `duration_ns`
    /// and drops the clear bytes, so a faithful `.sup` re-emits one here at
    /// `display_pts + duration`. Without it every subtitle lingers to EOF.
    ///
    /// `width`/`height` are carried from the display set's PCS so the clear PCS
    /// advertises the same video geometry; they don't affect the wipe but keep
    /// the segment well-formed.
    ///
    /// Returned bytes are concatenated `type(1)+size(2 BE)+payload` segments,
    /// the same shape [`emit_segments`] consumes.
    fn synthetic_clear_display_set(width: u16, height: u16) -> Vec<u8> {
        // Empty PCS payload (HDMV PGS, BD-ROM Part 3): width(2) height(2)
        // frame_rate(1) composition_number(2) composition_state(1)
        // palette_update_flag(1) palette_id(1) number_of_composition_objects(1).
        const PCS_FRAME_RATE: u8 = 0x10; // reserved high nibble | rate code
        const PCS_NO_OBJECTS: u8 = 0x00; // number_of_composition_objects = 0
        let [w_hi, w_lo] = width.to_be_bytes();
        let [h_hi, h_lo] = height.to_be_bytes();
        let pcs_payload = [
            w_hi,
            w_lo,
            h_hi,
            h_lo,
            PCS_FRAME_RATE,
            0x00,
            0x00, // composition_number
            PCS_COMPOSITION_STATE_NORMAL,
            0x00, // palette_update_flag
            0x00, // palette_id
            PCS_NO_OBJECTS,
        ];
        let mut out = Vec::with_capacity(PGS_SEG_HEADER_LEN * 2 + pcs_payload.len());
        out.push(SEG_PCS);
        out.extend_from_slice(&(pcs_payload.len() as u16).to_be_bytes());
        out.extend_from_slice(&pcs_payload);
        // END segment: type SEG_END, zero-length payload.
        out.push(SEG_END);
        out.extend_from_slice(&0u16.to_be_bytes());
        out
    }

    /// Read the (width, height) the display set's first PCS advertises, if the
    /// frame starts with a PCS carrying them; else `(0, 0)`.
    fn pcs_dimensions(data: &[u8]) -> (u16, u16) {
        // segment: type(1) size(2) payload; PCS payload begins width(2) height(2).
        if data.len() >= PCS_WIDTH_OFFSET + 4 && data[0] == SEG_PCS {
            let w = u16::from_be_bytes([data[PCS_WIDTH_OFFSET], data[PCS_WIDTH_OFFSET + 1]]);
            let h = u16::from_be_bytes([data[PCS_WIDTH_OFFSET + 2], data[PCS_WIDTH_OFFSET + 3]]);
            (w, h)
        } else {
            (0, 0)
        }
    }
}

impl EsWriter for PgsSupWriter {
    fn write_frame(&mut self, w: &mut dyn Write, f: &PesFrame, pts_ns: i64) -> io::Result<usize> {
        let pts90 = ns_to_90k(pts_ns);
        let mut written = Self::emit_segments(&f.data, pts90, pts90, w)?;
        // The parser folds the display/clear PCS pair's wipe time into
        // `duration_ns` and drops the clear bytes. Re-emit a synthetic clear
        // display set at `pts + duration` so the subtitle is timed out instead
        // of lingering to EOF.
        if let Some(dur) = f.duration_ns {
            let clear_pts = ns_to_90k(pts_ns.saturating_add(dur as i64));
            let (w_px, h_px) = Self::pcs_dimensions(&f.data);
            let clear = Self::synthetic_clear_display_set(w_px, h_px);
            written += Self::emit_segments(&clear, clear_pts, clear_pts, w)?;
        }
        Ok(written)
    }
}

/// VobSub writer: appends raw SPUs to the `.sub` and records `(pts, filepos)`
/// for the `.idx` sidecar emitted at finish.
struct VobSubWriter {
    idx_path: PathBuf,
    /// Pre-formatted `.idx` palette header line bytes, if available.
    palette_line: Option<String>,
    /// Two-letter language id for the `.idx` `id:` line (empty = omit).
    lang2: String,
    entries: Vec<(i64, u64)>,
    pos: u64,
}

impl VobSubWriter {
    fn new(idx_path: PathBuf, codec_private: Option<&[u8]>, lang: &str) -> Self {
        // codec_private for DvdSub is the pre-formatted VobSub `.idx` palette
        // header (UTF-8). Carry it through verbatim if present.
        let palette_line = codec_private
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(|s| s.trim_end().to_string());
        // VobSub `id:` lines use a 2-letter code; stream languages are ISO
        // 639-2 (3-letter). Take the leading two chars — the convention
        // mkvmerge reads to assign a track language.
        let lang2: String = lang.chars().take(2).collect();
        Self {
            idx_path,
            palette_line,
            lang2,
            entries: Vec::new(),
            pos: 0,
        }
    }
}

impl EsWriter for VobSubWriter {
    fn write_frame(&mut self, w: &mut dyn Write, f: &PesFrame, pts_ns: i64) -> io::Result<usize> {
        self.entries.push((pts_ns, self.pos));
        w.write_all(&f.data)?;
        self.pos += f.data.len() as u64;
        Ok(f.data.len())
    }

    fn finish(&mut self, _w: &mut dyn Write) -> io::Result<()> {
        let mut idx = String::new();
        idx.push_str("# VobSub index file, v7\n");
        if let Some(p) = &self.palette_line {
            idx.push_str(p);
            idx.push('\n');
        }
        idx.push_str("langidx: 0\n\n");
        // The conventional `id: <lang2>, index: 0` line mkvmerge reads to
        // assign the subtitle track's language. Omit the language token when
        // unknown but still emit the index so the entry list is well-formed.
        if self.lang2.is_empty() {
            idx.push_str("id: , index: 0\n");
        } else {
            idx.push_str(&format!("id: {}, index: 0\n", self.lang2));
        }
        for (pts_ns, filepos) in &self.entries {
            idx.push_str(&format!(
                "timestamp: {}, filepos: {:09x}\n",
                fmt_idx_timestamp(*pts_ns),
                filepos
            ));
        }
        std::fs::write(&self.idx_path, idx)
    }
}

/// VobSub `.idx` timestamp: `HH:MM:SS:mmm` (note colon before ms, per spec).
fn fmt_idx_timestamp(pts_ns: i64) -> String {
    let total_ms = (pts_ns.max(0) / 1_000_000) as u64;
    let ms = total_ms % 1000;
    let total_s = total_ms / 1000;
    let s = total_s % 60;
    let m = (total_s / 60) % 60;
    let h = total_s / 3600;
    format!("{h:02}:{m:02}:{s:02}:{ms:03}")
}

/// Pick the per-codec writer for a track.
fn es_writer_for(
    codec: Codec,
    codec_private: Option<&[u8]>,
    idx_path: Option<PathBuf>,
    lang: &str,
) -> Box<dyn EsWriter> {
    match codec {
        Codec::Hevc | Codec::H264 => Box::new(AnnexBWriter::new(codec, codec_private)),
        Codec::Pgs => Box::new(PgsSupWriter),
        Codec::DvdSub => Box::new(VobSubWriter::new(
            idx_path.unwrap_or_else(|| PathBuf::from("subtitle.idx")),
            codec_private,
            lang,
        )),
        _ => Box::new(PassthroughWriter),
    }
}

// ── Delay + chapter helpers ──────────────────────────────────────────────────

/// Delay in ms = round((audio_first_pts − ref_video_first_pts) / 1e6).
fn delay_ms(audio_first_pts_ns: i64, ref_video_first_pts_ns: i64) -> i64 {
    let diff = audio_first_pts_ns - ref_video_first_pts_ns;
    // Round to nearest ms (ties away from zero).
    if diff >= 0 {
        (diff + 500_000) / 1_000_000
    } else {
        (diff - 500_000) / 1_000_000
    }
}

/// `DELAY <signed-int>ms` — matches mkvmerge's case-insensitive
/// `delay\s+(-?\d+)` filename-delay parser.
fn delay_token(ms: i64) -> String {
    format!("DELAY {ms}ms")
}

/// Format a chapter time (seconds) as `HH:MM:SS.nnnnnnnnn` for chapter XML.
fn fmt_chapter_time_ns(time_secs: f64) -> String {
    let total_ns = (time_secs.max(0.0) * 1e9).round() as u64;
    let ns = total_ns % 1_000_000_000;
    let total_s = total_ns / 1_000_000_000;
    let s = total_s % 60;
    let m = (total_s / 60) % 60;
    let h = total_s / 3600;
    format!("{h:02}:{m:02}:{s:02}.{ns:09}")
}

/// Serialize chapters as mkvmerge chapter XML.
fn chapters_xml(chapters: &[Chapter]) -> String {
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    s.push_str("<!DOCTYPE Chapters SYSTEM \"matroskachapters.dtd\">\n");
    s.push_str("<Chapters>\n  <EditionEntry>\n");
    for (i, c) in chapters.iter().enumerate() {
        s.push_str("    <ChapterAtom>\n");
        s.push_str(&format!(
            "      <ChapterTimeStart>{}</ChapterTimeStart>\n",
            fmt_chapter_time_ns(c.time_secs)
        ));
        s.push_str("      <ChapterDisplay>\n");
        let name = if c.name.is_empty() {
            (i + 1).to_string()
        } else {
            c.name.clone()
        };
        s.push_str(&format!(
            "        <ChapterString>{}</ChapterString>\n",
            xml_escape(&name)
        ));
        s.push_str("        <ChapterLanguage>und</ChapterLanguage>\n");
        s.push_str("      </ChapterDisplay>\n");
        s.push_str("    </ChapterAtom>\n");
    }
    s.push_str("  </EditionEntry>\n</Chapters>\n");
    s
}

/// Serialize chapters as OGM/simple chapter text.
fn chapters_ogm(chapters: &[Chapter]) -> String {
    let mut s = String::new();
    for (i, c) in chapters.iter().enumerate() {
        let n = i + 1;
        // OGM uses HH:MM:SS.mmm (millisecond precision).
        let total_ms = (c.time_secs.max(0.0) * 1000.0).round() as u64;
        let ms = total_ms % 1000;
        let total_s = total_ms / 1000;
        let sec = total_s % 60;
        let m = (total_s / 60) % 60;
        let h = total_s / 3600;
        let name = if c.name.is_empty() {
            n.to_string()
        } else {
            c.name.clone()
        };
        s.push_str(&format!("CHAPTER{n:02}={h:02}:{m:02}:{sec:02}.{ms:03}\n"));
        s.push_str(&format!("CHAPTER{n:02}NAME={name}\n"));
    }
    s
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Replace path-hostile characters in a filename component.
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => c,
        })
        .collect()
}

// ── The sink ─────────────────────────────────────────────────────────────────

/// One open output track.
struct TrackOut {
    /// Current on-disk path (audio is renamed to embed the delay at finish).
    path: PathBuf,
    w: BufWriter<File>,
    kind: TrackKind,
    writer: Box<dyn EsWriter>,
    first_pts_ns: Option<i64>,
}

/// `demux://` sink: one file per selected track + chapters + delay metadata.
pub struct DemuxSink {
    dir: PathBuf,
    title: DiscTitle,
    opts: DemuxOptions,
    /// Index = track id; `None` for unselected tracks.
    tracks: Vec<Option<TrackOut>>,
    ref_video_track: Option<usize>,
    timeline: TimelineContinuity,
    finished: bool,
}

impl DemuxSink {
    /// Create the sink: make `dir`, and open one file per selected track.
    pub fn create(dir: &Path, title: &DiscTitle, opts: &DemuxOptions) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut tracks: Vec<Option<TrackOut>> = Vec::with_capacity(title.streams.len());
        let mut ref_video_track = None;

        for (idx, stream) in title.streams.iter().enumerate() {
            let selected = opts
                .selection
                .as_ref()
                .map(|sel| sel.contains(&idx))
                .unwrap_or(true);
            if !selected {
                tracks.push(None);
                continue;
            }

            let (kind, codec, pid, lang) = match stream {
                DiscStream::Video(v) => (TrackKind::Video, v.codec, v.pid, String::new()),
                DiscStream::Audio(a) => (TrackKind::Audio, a.codec, a.pid, a.language.clone()),
                DiscStream::Subtitle(s) => {
                    (TrackKind::Subtitle, s.codec, s.pid, s.language.clone())
                }
            };
            if kind == TrackKind::Video && ref_video_track.is_none() {
                ref_video_track = Some(idx);
            }

            let ext = extension_for(codec);
            let stem = Self::stem_for(opts, idx, pid, &lang, codec);
            let path = dir.join(format!("{stem}.{ext}"));
            let file = File::create(&path)?;

            // VobSub carries a sidecar `.idx`.
            let sidecar = if codec == Codec::DvdSub {
                Some(dir.join(format!("{stem}.idx")))
            } else {
                None
            };
            let codec_private = title.codec_privates.get(idx).and_then(|o| o.as_deref());
            let writer = es_writer_for(codec, codec_private, sidecar.clone(), &lang);

            let _ = sidecar; // sidecar path is owned by the VobSub writer
            tracks.push(Some(TrackOut {
                path,
                w: BufWriter::new(file),
                kind,
                writer,
                first_pts_ns: None,
            }));
        }

        Ok(Self {
            dir: dir.to_path_buf(),
            title: title.clone(),
            opts: opts.clone(),
            tracks,
            ref_video_track,
            timeline: TimelineContinuity::new(),
            finished: false,
        })
    }

    /// Filename stem (without extension) for a track.
    fn stem_for(opts: &DemuxOptions, idx: usize, pid: u16, lang: &str, codec: Codec) -> String {
        match opts.naming {
            Naming::Track => format!("track{idx:02}"),
            Naming::Pid => format!("{} {:04x}", sanitize(&opts.base), pid),
            Naming::Friendly => {
                let mut parts = vec![sanitize(&opts.base), format!("t{idx:02}")];
                if !lang.is_empty() {
                    parts.push(lang.to_string());
                }
                parts.push(codec_label(codec).to_string());
                parts.join(" ")
            }
        }
    }

    /// Apply audio delays (rename files) or write the `delays.txt` sidecar.
    fn apply_delays(&mut self) -> io::Result<()> {
        if self.opts.delay_mode == DelayMode::None {
            return Ok(());
        }
        let ref_pts = self
            .ref_video_track
            .and_then(|t| self.tracks.get(t).and_then(|o| o.as_ref()))
            .and_then(|t| t.first_pts_ns)
            .unwrap_or(0);

        let mut sidecar_lines = String::new();

        for slot in self.tracks.iter_mut() {
            let Some(t) = slot.as_mut() else { continue };
            if t.kind != TrackKind::Audio {
                continue;
            }
            let Some(first) = t.first_pts_ns else {
                continue;
            };
            let ms = delay_ms(first, ref_pts);

            match self.opts.delay_mode {
                DelayMode::Filename => {
                    // Insert the delay token before the extension.
                    let ext = t.path.extension().and_then(|e| e.to_str()).unwrap_or("");
                    let stem = t
                        .path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("audio");
                    let new_name = format!("{stem} {}.{ext}", delay_token(ms));
                    let new_path = self.dir.join(new_name);
                    std::fs::rename(&t.path, &new_path)?;
                    t.path = new_path;
                }
                DelayMode::Sidecar => {
                    let name = t
                        .path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("audio");
                    sidecar_lines.push_str(&format!("{name}\t{ms}\n"));
                }
                DelayMode::None => {}
            }
        }

        if self.opts.delay_mode == DelayMode::Sidecar && !sidecar_lines.is_empty() {
            let p = self
                .dir
                .join(format!("{} delays.txt", sanitize(&self.opts.base)));
            std::fs::write(p, sidecar_lines)?;
        }
        Ok(())
    }

    /// Write the chapter file(s).
    fn write_chapters(&self) -> io::Result<()> {
        if !self.opts.export_chapters || self.title.chapters.is_empty() {
            return Ok(());
        }
        let base = sanitize(&self.opts.base);
        if matches!(self.opts.chapters_fmt, ChaptersFmt::Xml | ChaptersFmt::Both) {
            std::fs::write(
                self.dir.join(format!("{base} chapters.xml")),
                chapters_xml(&self.title.chapters),
            )?;
        }
        if matches!(self.opts.chapters_fmt, ChaptersFmt::Ogm | ChaptersFmt::Both) {
            std::fs::write(
                self.dir.join(format!("{base} chapters.txt")),
                chapters_ogm(&self.title.chapters),
            )?;
        }
        Ok(())
    }
}

impl Stream for DemuxSink {
    fn read(&mut self) -> io::Result<Option<PesFrame>> {
        // Write-only sink, per the Stream trait contract.
        Err(crate::error::Error::StreamWriteOnly.into())
    }

    fn write(&mut self, frame: &PesFrame) -> io::Result<()> {
        // The PRIMARY VIDEO track (`ref_video_track`, the first DiscStream::Video)
        // drives epoch decisions; every other track is a passive rider on the same
        // global offset — see `TimelineContinuity`. Drive epochs off the same
        // dynamically-resolved video reference used for the audio-delay
        // computation, NOT the literal stream index 0 — an M2TS/PMT title can list
        // an audio ES before the video ES, in which case track 0 is audio and a
        // non-video epoch driver would ratchet the frontier on sparse/lagging PTS.
        let drives = Some(frame.track) == self.ref_video_track;
        let pts = self.timeline.adjust(frame.pts, drives);
        if let Some(Some(t)) = self.tracks.get_mut(frame.track) {
            t.first_pts_ns.get_or_insert(pts);
            t.writer.write_frame(&mut t.w, frame, pts)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        // Flush each track's codec writer, then the buffered file.
        for slot in self.tracks.iter_mut() {
            if let Some(t) = slot.as_mut() {
                t.writer.finish(&mut t.w)?;
                t.w.flush()?;
            }
        }
        self.apply_delays()?;
        self.write_chapters()?;
        Ok(())
    }

    fn info(&self) -> &DiscTitle {
        &self.title
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::{
        AudioChannels, AudioStream, ColorSpace, ContentFormat, FrameRate, HdrFormat, LabelPurpose,
        Resolution, SampleRate, VideoStream,
    };

    fn video_stream(codec: Codec) -> DiscStream {
        DiscStream::Video(VideoStream {
            pid: 0x1011,
            codec,
            resolution: Resolution::R1080p,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })
    }

    fn audio_stream(codec: Codec, lang: &str) -> DiscStream {
        DiscStream::Audio(AudioStream {
            pid: 0x1100,
            codec,
            channels: AudioChannels::Stereo,
            language: lang.to_string(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        })
    }

    fn title_with(streams: Vec<DiscStream>, privates: Vec<Option<Vec<u8>>>) -> DiscTitle {
        let mut t = DiscTitle::empty();
        t.streams = streams;
        t.codec_privates = privates;
        t.content_format = ContentFormat::BdTs;
        t
    }

    // ── Annex-B reframing ────────────────────────────────────────────────────
    //
    // The length-prefixed → Annex-B conversion and the hvcC/avcC param-set
    // extraction are exercised canonically in `crate::mux::hevc`; the sink
    // delegates to those helpers. Here we only assert the sink-level wiring:
    // param-set prepend and (crucially) that a zero-length NAL mid-frame no
    // longer truncates the rest of the access unit.

    #[test]
    fn zero_length_nal_midframe_does_not_truncate_access_unit() {
        // The OLD local reframer `break`d on a zero-length NAL, dropping every
        // NAL after it. The canonical `append_length_prefixed_as_annex_b` skips
        // just the empty NAL and keeps going. Frame: NAL(2) | NAL(0) | NAL(3).
        let mut w = AnnexBWriter::new(Codec::H264, None);
        let mut out = Vec::new();
        let f = PesFrame {
            coding: None,
            source: None,
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![
                0, 0, 0, 2, 0xAA, 0xBB, // NAL #1 (len 2)
                0, 0, 0, 0, // zero-length NAL — must be skipped, not fatal
                0, 0, 0, 3, 0x01, 0x02, 0x03, // NAL #3 (len 3) — must survive
            ],
            duration_ns: None,
        };
        w.write_frame(&mut out, &f, 0).unwrap();
        // Both real NALs present; the empty NAL emitted nothing.
        assert_eq!(
            out,
            vec![0, 0, 0, 1, 0xAA, 0xBB, 0, 0, 0, 1, 0x01, 0x02, 0x03],
            "trailing NAL after a zero-length NAL must NOT be dropped"
        );
    }

    #[test]
    fn annexb_writer_prepends_params_once() {
        let rec = [
            1, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0, 2, 0x67, 0x42, 1, 0, 1, 0x68,
        ];
        let mut w = AnnexBWriter::new(Codec::H264, Some(&rec));
        let mut out = Vec::new();
        let f1 = PesFrame {
            coding: None,
            source: None,
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![0, 0, 0, 2, 0xAA, 0xBB],
            duration_ns: None,
        };
        w.write_frame(&mut out, &f1, 0).unwrap();
        // params (SPS+PPS as annexb) then the frame NAL.
        assert_eq!(
            out,
            vec![
                0, 0, 0, 1, 0x67, 0x42, // SPS
                0, 0, 0, 1, 0x68, // PPS
                0, 0, 0, 1, 0xAA, 0xBB // frame NAL
            ]
        );
        // Second frame: NO param re-prepend.
        let mut out2 = Vec::new();
        let f2 = PesFrame {
            coding: None,
            source: None,
            track: 0,
            pts: 0,
            keyframe: false,
            data: vec![0, 0, 0, 1, 0xCC],
            duration_ns: None,
        };
        w.write_frame(&mut out2, &f2, 0).unwrap();
        assert_eq!(out2, vec![0, 0, 0, 1, 0xCC]);
    }

    // ── Delay ────────────────────────────────────────────────────────────────

    #[test]
    fn delay_ms_sign_and_rounding() {
        // Audio later than video → positive (must be delayed).
        assert_eq!(delay_ms(1_000_000_000, 0), 1000);
        // Audio earlier than video → negative (must be advanced).
        assert_eq!(delay_ms(0, 248_000_000), -248);
        // Rounding to nearest ms.
        assert_eq!(delay_ms(1_600_000, 0), 2);
        assert_eq!(delay_ms(1_400_000, 0), 1);
        assert_eq!(delay_ms(-1_600_000, 0), -2);
    }

    #[test]
    fn delay_token_matches_mkvmerge_regex() {
        // mkvmerge: case-insensitive /delay\s+(-?\d+)/.
        let re = regex_lite_delay;
        assert_eq!(re("Movie eng AC3 DELAY -248ms.ac3"), Some(-248));
        assert_eq!(re(&format!("x {}.dts", delay_token(1000))), Some(1000));
        assert_eq!(re(&format!("x {}.thd", delay_token(0))), Some(0));
        assert_eq!(re(&format!("x {}.eac3", delay_token(-5))), Some(-5));
    }

    /// Minimal stand-in for mkvmerge's `delay\s+(-?\d+)` (case-insensitive).
    fn regex_lite_delay(name: &str) -> Option<i64> {
        let lower = name.to_lowercase();
        let idx = lower.find("delay")?;
        let after = &name[idx + 5..];
        let after = after.trim_start();
        let mut chars = after.chars().peekable();
        let mut num = String::new();
        if chars.peek() == Some(&'-') {
            num.push('-');
            chars.next();
        }
        for c in chars {
            if c.is_ascii_digit() {
                num.push(c);
            } else {
                break;
            }
        }
        num.parse().ok()
    }

    // ── PGS .sup framing ─────────────────────────────────────────────────────

    #[test]
    fn pgs_sup_frames_each_segment_with_pg_header() {
        // One segment: type=0x16, size=2, payload=[0xDE,0xAD].
        let payload = [SEG_PCS, 0x00, 0x02, 0xDE, 0xAD];
        let mut out = Vec::new();
        let written = PgsSupWriter::emit_segments(&payload, 0x10, 0x10, &mut out).unwrap();
        assert_eq!(&out[0..2], &SUP_MAGIC);
        assert_eq!(&out[2..6], &0x10u32.to_be_bytes()); // PTS
        assert_eq!(&out[6..10], &0x10u32.to_be_bytes()); // DTS
        assert_eq!(&out[SUP_HEADER_LEN..], &payload); // segment body verbatim
        assert_eq!(written, SUP_HEADER_LEN + 2);
    }

    #[test]
    fn pgs_frame_with_duration_emits_clear_segment() {
        // A display set with a real PCS (type 0x16) carrying 1920x1080, and a
        // duration → the writer must append a synthetic clear display set
        // (empty PCS + END) timestamped at pts + duration.
        let mut pcs = vec![SEG_PCS, 0x00, 0x0B];
        pcs.extend_from_slice(&[0x07, 0x80, 0x04, 0x38]); // 1920x1080
        pcs.extend_from_slice(&[0x10, 0x00, 0x00, 0x80, 0x00, 0x00, 0x01]); // 1 object
        let f = PesFrame {
            coding: None,
            source: None,
            track: 0,
            pts: 1_000_000_000, // 1s
            keyframe: true,
            data: pcs,
            duration_ns: Some(2_000_000_000), // 2s display → clear at 3s
        };
        let mut out = Vec::new();
        let mut w = PgsSupWriter;
        w.write_frame(&mut out, &f, f.pts).unwrap();

        // Parse out every PG-framed segment: PG(2) PTS(4) DTS(4) type(1) size(2).
        let mut segs: Vec<(u8, u32)> = Vec::new();
        let mut pos = 0;
        while pos + SUP_HEADER_LEN <= out.len() {
            assert_eq!(
                &out[pos..pos + 2],
                &SUP_MAGIC,
                "each segment carries PG magic"
            );
            let pts = u32::from_be_bytes([out[pos + 2], out[pos + 3], out[pos + 4], out[pos + 5]]);
            let seg_type = out[pos + SUP_HEADER_LEN];
            let size =
                u16::from_be_bytes([out[pos + SUP_HEADER_LEN + 1], out[pos + SUP_HEADER_LEN + 2]])
                    as usize;
            segs.push((seg_type, pts));
            pos += SUP_HEADER_LEN + PGS_SEG_HEADER_LEN + size;
        }
        // Display PCS at 1s (90k), then a clear PCS + END at 3s.
        let clear90 = ns_to_90k(3_000_000_000);
        assert!(
            segs.iter().any(|&(t, p)| t == SEG_PCS && p == clear90),
            "a clear PCS must be emitted at pts+duration, got {segs:?}"
        );
        assert!(
            segs.iter().any(|&(t, p)| t == SEG_END && p == clear90),
            "an END segment must terminate the clear display set, got {segs:?}"
        );
    }

    #[test]
    fn pgs_frame_without_duration_emits_no_clear() {
        // No duration → no synthetic clear (the subtitle's wipe time is unknown).
        let f = PesFrame {
            coding: None,
            source: None,
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![SEG_PCS, 0x00, 0x02, 0xDE, 0xAD],
            duration_ns: None,
        };
        let mut out = Vec::new();
        let mut w = PgsSupWriter;
        w.write_frame(&mut out, &f, 0).unwrap();
        // Exactly one PG-framed segment (the display), no clear appended.
        // Output = `.sup` header (10) + the on-wire segment (type+size 3 + 2
        // payload = 5) → 15 bytes, with no trailing clear.
        assert_eq!(&out[0..2], &SUP_MAGIC);
        assert_eq!(
            out.len(),
            SUP_HEADER_LEN + PGS_SEG_HEADER_LEN + 2,
            "only the display segment, no clear"
        );
    }

    #[test]
    fn ns_to_90k_conversion() {
        assert_eq!(ns_to_90k(0), 0);
        // 1 second = 90000 ticks.
        assert_eq!(ns_to_90k(1_000_000_000), 90_000);
        assert_eq!(ns_to_90k(-5), 0);
    }

    // ── VobSub .idx ──────────────────────────────────────────────────────────

    #[test]
    fn vobsub_idx_synthesis() {
        let dir = tempdir();
        let idx = dir.join("sub.idx");
        let mut w = VobSubWriter::new(idx.clone(), Some(b"palette: 000000, ffffff"), "eng");
        let mut sub = Vec::new();
        let f1 = PesFrame {
            coding: None,
            source: None,
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![0xAA; 10],
            duration_ns: None,
        };
        let f2 = PesFrame {
            coding: None,
            source: None,
            track: 0,
            pts: 1_000_000_000,
            keyframe: true,
            data: vec![0xBB; 20],
            duration_ns: None,
        };
        w.write_frame(&mut sub, &f1, 0).unwrap();
        w.write_frame(&mut sub, &f2, 1_000_000_000).unwrap();
        w.finish(&mut sub).unwrap();
        let idx_text = std::fs::read_to_string(&idx).unwrap();
        assert!(idx_text.contains("palette: 000000, ffffff"));
        // The conventional `id:` line mkvmerge reads to assign the language.
        assert!(
            idx_text.contains("id: en, index: 0"),
            "missing id: line, got:\n{idx_text}"
        );
        assert!(idx_text.contains("timestamp: 00:00:00:000, filepos: 000000000"));
        // Second SPU at 1s, filepos = 10.
        assert!(idx_text.contains("timestamp: 00:00:01:000, filepos: 00000000a"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn idx_timestamp_format() {
        assert_eq!(fmt_idx_timestamp(0), "00:00:00:000");
        assert_eq!(fmt_idx_timestamp(3_661_500_000_000), "01:01:01:500");
    }

    // ── Chapters ─────────────────────────────────────────────────────────────

    #[test]
    fn chapter_xml_and_ogm() {
        let chaps = vec![
            Chapter {
                time_secs: 0.0,
                name: "1".to_string(),
            },
            Chapter {
                time_secs: 65.5,
                name: "2".to_string(),
            },
        ];
        let xml = chapters_xml(&chaps);
        assert!(xml.contains("<ChapterTimeStart>00:00:00.000000000</ChapterTimeStart>"));
        assert!(xml.contains("<ChapterTimeStart>00:01:05.500000000</ChapterTimeStart>"));
        let ogm = chapters_ogm(&chaps);
        assert!(ogm.contains("CHAPTER01=00:00:00.000"));
        assert!(ogm.contains("CHAPTER02=00:01:05.500"));
        assert!(ogm.contains("CHAPTER02NAME=2"));
    }

    // ── Timeline continuity ──────────────────────────────────────────────────
    //
    // The corrector itself is tested verbatim in `crate::mux::timeline`. Here we
    // only confirm the sink drives it with the right `drives_epoch`: track 0 is
    // the epoch driver, every other track is a passive rider on the same offset.

    #[test]
    fn timeline_track0_drives_epoch_others_ride() {
        let mut tl = TimelineContinuity::new();
        // Clip 1: video 0..10s (track 0 drives the epoch).
        assert_eq!(tl.adjust(0, true), 0);
        assert_eq!(tl.adjust(0, false), 0); // audio rides the same offset
        assert_eq!(tl.adjust(10_000_000_000, true), 10_000_000_000);
        // Clip 2 seam: video PTS jumps back to ~0 (> 3s back) → new epoch.
        let out = tl.adjust(0, true);
        assert!(out >= 10_000_000_000, "epoch must advance past prev high");
        // Audio in clip 2 (non-epoch) gets the SAME offset (A/V sync preserved).
        let a = tl.adjust(0, false);
        assert_eq!(a, out);
    }

    /// Regression for the hardcoded `frame.track == 0` epoch driver: on an
    /// M2TS/PMT title the PMT can list an AUDIO ES before the VIDEO ES, so the
    /// video lands at stream index 1. The sink already resolves the video
    /// reference dynamically (`ref_video_track`); the epoch driver must use that
    /// SAME reference, not the literal 0. Here track 0 is audio and track 1 is
    /// video. A video-only clip boundary (track 1) must open a new epoch (bump
    /// `offset_ns`). With the bug (only `frame.track == 0` drives epochs) the
    /// video back-jump would be treated as a passive rider and `offset_ns` would
    /// stay 0 — corrupting every track's rebased timeline.
    #[test]
    fn epoch_driver_follows_ref_video_not_track_zero() {
        let dir = tempdir();
        // Audio FIRST (index 0), video SECOND (index 1).
        let title = title_with(
            vec![audio_stream(Codec::Ac3, "eng"), video_stream(Codec::H264)],
            vec![None, None],
        );
        let mut sink = DemuxSink::create(&dir, &title, &DemuxOptions::default()).unwrap();
        assert_eq!(
            sink.ref_video_track,
            Some(1),
            "video reference must be the first VIDEO stream (index 1), not 0"
        );

        let vid = |pts: i64, data: u8| PesFrame {
            coding: None,
            source: None,
            track: 1, // VIDEO is track 1 here
            pts,
            keyframe: true,
            data: vec![0x00, 0x00, 0x00, 0x01, data],
            duration_ns: None,
        };
        // Clip 1 video: 0s then 10s — advances the frontier.
        sink.write(&vid(0, 0xAA)).unwrap();
        sink.write(&vid(10_000_000_000, 0xBB)).unwrap();
        // Clip 2 seam: video PTS jumps back to ~0 (> 3s back) → NEW epoch. The
        // video track (index 1) must drive this, bumping the offset.
        sink.write(&vid(0, 0xCC)).unwrap();

        assert!(
            sink.timeline.offset_ns >= 10_000_000_000,
            "video (track 1) must drive the epoch: offset_ns should have advanced \
             past the previous high, got {}",
            sink.timeline.offset_ns
        );
        sink.finish().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── End-to-end sink ──────────────────────────────────────────────────────

    #[test]
    fn sink_keys_files_by_track_and_writes_all() {
        let dir = tempdir();
        let title = title_with(
            vec![video_stream(Codec::Mpeg2), audio_stream(Codec::Ac3, "eng")],
            vec![None, None],
        );
        let opts = DemuxOptions {
            base: "Test".to_string(),
            ..Default::default()
        };
        let mut sink = DemuxSink::create(&dir, &title, &opts).unwrap();

        // Video frame (track 0) and audio frame (track 1).
        sink.write(&PesFrame {
            coding: None,
            source: None,
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![0x00, 0x00, 0x01, 0xB3, 0xDE],
            duration_ns: None,
        })
        .unwrap();
        sink.write(&PesFrame {
            coding: None,
            source: None,
            track: 1,
            pts: 100_000_000, // audio 100ms late
            keyframe: true,
            data: vec![0x0B, 0x77, 0x01, 0x02],
            duration_ns: None,
        })
        .unwrap();
        sink.finish().unwrap();

        // Video file written verbatim (passthrough).
        let v = std::fs::read(dir.join("Test t00 MPEG2.m2v")).unwrap();
        assert_eq!(v, vec![0x00, 0x00, 0x01, 0xB3, 0xDE]);
        // Audio file renamed with the delay token (100ms → DELAY 100ms).
        let a = std::fs::read(dir.join("Test t01 eng AC3 DELAY 100ms.ac3")).unwrap();
        assert_eq!(a, vec![0x0B, 0x77, 0x01, 0x02]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sink_respects_track_selection() {
        let dir = tempdir();
        let title = title_with(
            vec![video_stream(Codec::Mpeg2), audio_stream(Codec::Ac3, "eng")],
            vec![None, None],
        );
        let opts = DemuxOptions {
            base: "Sel".to_string(),
            selection: Some(vec![0]), // video only
            ..Default::default()
        };
        let mut sink = DemuxSink::create(&dir, &title, &opts).unwrap();
        sink.write(&PesFrame {
            coding: None,
            source: None,
            track: 1,
            pts: 0,
            keyframe: true,
            data: vec![0xFF],
            duration_ns: None,
        })
        .unwrap(); // dropped — track 1 not selected
        sink.finish().unwrap();
        assert!(dir.join("Sel t00 MPEG2.m2v").exists());
        // No audio file created.
        let entries: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "ac3").unwrap_or(false))
            .collect();
        assert!(entries.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sink_read_returns_write_only() {
        let dir = tempdir();
        let title = title_with(vec![video_stream(Codec::Mpeg2)], vec![None]);
        let mut sink = DemuxSink::create(&dir, &title, &DemuxOptions::default()).unwrap();
        let err = Stream::read(&mut sink).expect_err("sink read must error");
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Tiny unique temp dir helper (avoids a dev-dependency on `tempfile`).
    fn tempdir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("fmkv_demux_test_{}_{}", std::process::id(), n));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}
