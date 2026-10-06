//! `demux://` sink — write each track of a title as a separate elementary
//! stream file (per-codec ES, PGS `.sup`, VobSub `.idx`/`.sub`, LPCM raw PCM),
//! plus a chapters file and per-audio-track delay metadata.
//!
//! This is a write-only [`crate::pes::PesSink`] that routes each frame's payload to the file for
//! `frame.track`, post-processing where the codec's internal `Frame` form differs from the
//! on-disk ES form (HEVC/H.264 Annex-B, PGS `.sup`, VobSub `.idx`).

use crate::disc::{Chapter, Codec, DiscTitle, Stream as DiscStream};
use crate::mux::hevc::{
    append_length_prefixed_as_annex_b_sized, avcc_to_annex_b, hvcc_to_annex_b, nal_length_size,
};
use crate::mux::timeline::TimelineContinuity;
use crate::pes::{PesFrame, PesSink};
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
    /// Embed `DELAY <n>ms` in each audio filename (the filename-delay
    /// convention downstream muxers parse).
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
    /// Matroska chapter XML.
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
    /// Restrict output to one track class. `None` = every class (plain
    /// `demux://`). `Some(Audio)` is the `audio://` sink; `Some(Subtitle)` is
    /// `sub://`. Filtered tracks are skipped entirely (no file written).
    pub kind_filter: Option<TrackKind>,
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
            kind_filter: None,
        }
    }
}

/// Track class, used for delay attribution and naming — and, via
/// [`DemuxOptions::kind_filter`], to restrict a demux to one class (the
/// `audio://` / `sub://` sinks are a `demux://` filtered to Audio / Subtitle).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackKind {
    Video,
    Audio,
    Subtitle,
}

// ── Codec → on-disk extension ────────────────────────────────────────────────

/// File extension (without the dot) for a codec's standalone elementary stream.
/// Chosen to match the conventional elementary-stream extensions downstream
/// muxers and codec tools expect.
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
pub(crate) fn codec_label(codec: Codec) -> &'static str {
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

// Per-codec elementary-stream writer: most codecs pass through; HEVC/H.264
// re-frame to Annex-B, PGS re-frames to `.sup`, VobSub emits `.idx`.
trait EsWriter: Send {
    /// Write one frame's payload to `w`. Returns the number of bytes written to
    /// the main file.
    ///
    /// Nothing in production reads this count: the sole caller discards it with
    /// `?`, and `VobSubWriter` tracks `.idx` fileposes from its own `pos` field.
    /// This previously claimed the VobSub writer depended on the return value,
    /// which would have a maintainer believe a wrong count shows up as broken
    /// `.idx` output. It does not.
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

/// HEVC/H.264 writer: reframes length-prefixed NALs (the hvcC/avcC form the
/// parsers emit) into Annex-B, prepending the parameter sets once.
struct AnnexBWriter {
    /// Annex-B-framed VPS/SPS/PPS (or SPS/PPS), parsed from the hvcC/avcC.
    params: Vec<u8>,
    wrote_params: bool,
    /// Octets per NAL length prefix, from the configuration record's
    /// `lengthSizeMinusOne` (ISO/IEC 14496-15). NOT assumed to be 4: a legal
    /// avcC/hvcC may declare 1 or 2, and reading those as u32-BE parses no NALs
    /// at all, so the raw prefixed bytes would be emitted as if already Annex B.
    length_size: usize,
    /// Reused length-prefixed -> Annex-B buffer, cleared per frame (avoids a per-frame alloc).
    scratch: Vec<u8>,
}

impl AnnexBWriter {
    fn new(codec: Codec, codec_private: Option<&[u8]>) -> Self {
        let params = codec_private
            .map(|rec| annexb_param_sets(codec, rec))
            .unwrap_or_default();
        Self {
            params,
            wrote_params: false,
            length_size: nal_length_size(codec, codec_private),
            scratch: Vec::new(),
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
        // Reframe via the canonical length-prefixed->Annex-B converter (see
        // `crate::mux::hevc`), which skips zero-length NALs and drops a truncated
        // trailing NAL without panicking. Reuse `self.scratch` to avoid per-frame allocation.
        self.scratch.clear();
        self.scratch.reserve(f.data.len() + (f.data.len() / 32) + 4);
        let mut scratch = std::mem::take(&mut self.scratch);
        append_length_prefixed_as_annex_b_sized(&mut scratch, &f.data, self.length_size);
        w.write_all(&scratch)?;
        n += scratch.len();
        self.scratch = scratch;
        Ok(n)
    }
}

// Extract the parameter-set NALs from an hvcC (HEVC) or avcC (H.264) record
// as a single Annex-B blob; empty Vec if unparsable. Delegates to the
// canonical hvcC/avcC -> Annex-B converters in `crate::mux::hevc`.
fn annexb_param_sets(codec: Codec, record: &[u8]) -> Vec<u8> {
    let converted = match codec {
        Codec::Hevc => hvcc_to_annex_b(record),
        Codec::H264 => avcc_to_annex_b(record),
        _ => return Vec::new(),
    };
    converted.unwrap_or_else(|| {
        // A malformed hvcC/avcC record yields no parameter sets, so keyframes ship
        // without in-band SPS/PPS (breaks seek and hardware decoders); warn rather
        // than silently degrading.
        tracing::warn!(
            target: "mux",
            ?codec,
            "codec-private (hvcC/avcC) parse failed; keyframes will lack in-band SPS/PPS"
        );
        Vec::new()
    })
}

// PGS `.sup` writer: rebuilds the HDMV segment framing the parser stripped, prefixing each
// segment with a 13-byte `PG` header (PTS/DTS).
#[derive(Default)]
struct PgsSupWriter {
    // Delay a duration-derived clear until the next frame, so an original
    // clear (or replacement PCS) at that timestamp takes precedence.
    pending_clear: Option<(i64, u16, u16)>,
    // Bytes of truncated trailing segments not written (a partial segment would
    // desync the `.sup` framing); reported at finish.
    truncated_bytes: u64,
}

// ── PGS / HDMV segment framing constants ─────────────────────────────────────
// HDMV Presentation Graphics Stream, per BD-ROM Part 3 graphics-stream spec
// (and public US 2009/0185789 A1, which documents the segment layout).

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

/// 90 kHz ticks from nanoseconds via the shared round-to-nearest helper (design J16),
/// clamped to `0..=u32::MAX` for the `.sup` header (≤ 0 ns writes 0).
fn ns_to_90k(pts_ns: i64) -> u32 {
    if pts_ns <= 0 {
        return 0;
    }
    crate::mux::codec::ns_to_ticks(pts_ns).clamp(0, u32::MAX as i64) as u32
}

impl PgsSupWriter {
    /// Walk the concatenated segments in `data`, emitting each with a `PG`
    /// header carrying `pts90k`/`dts90k`. Returns `(bytes written, trailing
    /// bytes of a truncated segment left unwritten)`.
    fn emit_segments(
        data: &[u8],
        pts90k: u32,
        dts90k: u32,
        w: &mut dyn Write,
    ) -> io::Result<(usize, usize)> {
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
            written += SUP_HEADER_LEN + PGS_SEG_HEADER_LEN + size;
            pos = seg_end;
        }
        Ok((written, data.len() - pos))
    }

    // Synthetic "clear" display set (empty PCS + END) re-emitted at `display_pts + duration`;
    // without it every subtitle lingers to EOF.
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
        let is_pcs = f.data.first() == Some(&SEG_PCS) && f.data.len() > 13;
        let mut written = 0;
        // An authored clear states the real end, even past a duration the parser capped.
        let is_clear = is_pcs && f.data[13] == 0;
        if let Some((end, width, height)) = self.pending_clear {
            if (end < pts_ns && !is_clear) || (end == pts_ns && !is_pcs) {
                let clear = Self::synthetic_clear_display_set(width, height);
                let pts = ns_to_90k(end);
                written += Self::emit_segments(&clear, pts, pts, w)?.0;
                self.pending_clear = None;
            } else if is_pcs {
                // A real clear/replacement at or before the computed end wins.
                self.pending_clear = None;
            }
        }
        let (n, truncated) = Self::emit_segments(&f.data, pts90, pts90, w)?;
        written += n;
        self.truncated_bytes += truncated as u64;
        // Retain fallback support for old MKVs that only carry durations, and
        // for a final display without a clear. Never synthesize a clear OF a
        // clear (nor of a standalone WDS/END segment).
        if let Some(dur) = f.duration_ns.filter(|_| is_pcs && f.data[13] > 0) {
            let end = pts_ns.saturating_add(i64::try_from(dur).unwrap_or(i64::MAX));
            let (w_px, h_px) = Self::pcs_dimensions(&f.data);
            self.pending_clear = Some((end, w_px, h_px));
        }
        Ok(written)
    }

    fn finish(&mut self, w: &mut dyn Write) -> io::Result<()> {
        if let Some((end, width, height)) = self.pending_clear.take() {
            let clear = Self::synthetic_clear_display_set(width, height);
            let pts = ns_to_90k(end);
            Self::emit_segments(&clear, pts, pts, w)?;
        }
        if self.truncated_bytes > 0 {
            tracing::warn!(
                target: "mux",
                bytes = self.truncated_bytes,
                "PGS .sup: truncated trailing segments were not written"
            );
        }
        Ok(())
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
        // downstream muxers read to assign a track language.
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
        // The conventional `id: <lang2>, index: 0` line downstream muxers read
        // to assign the subtitle track's language. Omit the language token when
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
        Codec::Pgs => Box::new(PgsSupWriter::default()),
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

/// `DELAY <signed-int>ms` — matches the conventional case-insensitive
/// `delay\s+(-?\d+)` filename-delay convention downstream muxers parse.
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

/// Serialize chapters as Matroska chapter XML.
pub(crate) fn chapters_xml(chapters: &[Chapter]) -> String {
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
pub(crate) fn chapters_ogm(chapters: &[Chapter]) -> String {
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
            escape_controls(&c.name, &[])
        };
        s.push_str(&format!("CHAPTER{n:02}={h:02}:{m:02}:{sec:02}.{ms:03}\n"));
        s.push_str(&format!("CHAPTER{n:02}NAME={name}\n"));
    }
    s
}

fn xml_escape(s: &str) -> String {
    // XML 1.0 forbids C0 controls other than tab / LF / CR.
    escape_controls(s, &['\t', '\n', '\r'])
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

// Disc-derived text in a line-oriented or XML file: control chars (and U+FFFE / U+FFFF)
// outside `allow` are written in their `escape_debug()` form, as `Error::DirNameCollision`
// does, instead of raw.
fn escape_controls(s: &str, allow: &[char]) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if (c.is_control() && !allow.contains(&c)) || matches!(c, '\u{FFFE}' | '\u{FFFF}') {
            out.extend(c.escape_debug());
        } else {
            out.push(c);
        }
    }
    out
}

// Replace path-hostile characters (control chars incl. NUL) in a filename component;
// disc-derived text (e.g. STN language codes) is unvalidated.
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
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
    /// Every VIDEO track index, recorded before the kind filter drops slots.
    video_tracks: std::collections::HashSet<usize>,
    /// First PTS seen on `ref_video_track`, even when that track has no `TrackOut`.
    /// `None` = no reference seen, so no delay is emitted (see `apply_delays`).
    ref_first_pts_ns: Option<i64>,
    timeline: TimelineContinuity,
    finished: bool,
    /// Frames persisted to a track file (drop gate denominator); filtered-out tracks
    /// are not counted, matching `timeline.dropped_for(persisted_tracks)` in `finish()`.
    frames_mapped: u64,
    /// MPEG-2 multichannel extension tracks (no ES file form), reported once packets arrive.
    excluded: super::ps::UnstoredExtensions,
}

impl DemuxSink {
    /// Create the sink: make `dir`, and open one file per selected track.
    pub fn create(dir: &Path, title: &DiscTitle, opts: &DemuxOptions) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut tracks: Vec<Option<TrackOut>> = Vec::with_capacity(title.streams.len());
        let mut ref_video_track = None;
        let mut video_tracks: std::collections::HashSet<usize> = std::collections::HashSet::new();
        let excluded = super::ps::UnstoredExtensions::new(title, "elementary-stream file");

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
            // Before the kind filter: the video track drives multi-clip PTS-continuity
            // rebasing and the audio DELAY tag even on `audio://`/`sub://` outputs.
            if kind == TrackKind::Video && ref_video_track.is_none() {
                ref_video_track = Some(idx);
            }
            // Also before the kind filter: the seam-crossing rule differs for a
            // B-frame-reorder track; reading kind from the filtered slot would
            // misclassify a video track on an `audio://`/`sub://` export.
            if kind == TrackKind::Video {
                video_tracks.insert(idx);
            }
            // Kind filter: `audio://` / `sub://` keep only their class.
            if opts.kind_filter.is_some_and(|k| k != kind) {
                tracks.push(None);
                continue;
            }
            // A 13818-3 extension stream is not a decodable ES on its own: left out, reported.
            if excluded.contains(idx) {
                tracks.push(None);
                continue;
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
            video_tracks,
            ref_first_pts_ns: None,
            timeline: TimelineContinuity::with_clips(&title.clips, title.content_format),
            finished: false,
            frames_mapped: 0,
            excluded,
        })
    }

    /// Filename stem (without extension) for a track.
    fn stem_for(opts: &DemuxOptions, idx: usize, pid: u16, lang: &str, codec: Codec) -> String {
        match opts.naming {
            Naming::Track => format!("track{idx:02}"),
            Naming::Pid => format!("{} {:04x}", sanitize(&opts.base), pid),
            Naming::Friendly => {
                let mut parts = vec![sanitize(&opts.base), format!("t{idx:02}")];
                // The language is disc bytes too, and got none of the
                // treatment `opts.base` two lines up already had.
                let lang = sanitize(lang);
                if !lang.is_empty() {
                    parts.push(lang);
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
        // No reference PTS: omit the delay rather than default to zero. A filename
        // claiming `DELAY 600ms` when the offset is unknown is silently wrong; a
        // missing tag correctly says "no delay information".
        let Some(ref_pts) = self.ref_first_pts_ns else {
            tracing::warn!(
                target: "mux",
                "demux sink: no reference video PTS observed; omitting audio DELAY metadata"
            );
            return Ok(());
        };

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

impl PesSink for DemuxSink {
    fn undelivered_streams(&self) -> Vec<usize> {
        self.excluded.seen()
    }

    fn write(&mut self, frame: &PesFrame) -> io::Result<()> {
        if self.excluded.drop_frame(frame.track) {
            return Ok(());
        }
        // Use the dynamically-resolved video reference, not literal stream index 0:
        // an M2TS/PMT title can list audio before video, so a non-video epoch
        // driver would ratchet the frontier on sparse/lagging PTS.
        let drives = Some(frame.track) == self.ref_video_track;
        // Keyed on track kind, not on driving epochs: a Dolby Vision enhancement
        // layer doesn't drive epochs but carries B-frame reorder, which is what
        // the seam-crossing rule keys on.
        let is_video = self.video_tracks.contains(&frame.track);
        // See `MkvMuxer::write_frame`: `None` is material outside the
        // playlist's clip marks and is dropped rather than emitted.
        let Some(pts) = self.timeline.map_picture(
            frame.pts,
            drives,
            frame.track,
            is_video,
            frame.source.map(|s| s.byte),
            is_video.then_some(crate::mux::timeline::SeamPic {
                keyframe: frame.keyframe,
                coding: frame.coding,
            }),
        ) else {
            return Ok(());
        };
        if drives {
            // Delay reference: recorded here, not in the track's `TrackOut`, so
            // it survives the `audio://` / `sub://` kind filter dropping the
            // video output.
            self.ref_first_pts_ns.get_or_insert(pts);
        }
        if let Some(Some(t)) = self.tracks.get_mut(frame.track) {
            // Denominator = persisted frames only. A kind-filtered frame (video
            // in an `audio://` export) maps but writes no file; counting it masks
            // a seam plan that dropped every *persisted* frame → zero-byte exit 0.
            self.frames_mapped = self.frames_mapped.saturating_add(1);
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
        // Same as the MKV muxer's finish: surface unexpected drop volume. Count
        // drops ONLY for persisted tracks (matches the `frames_mapped` denominator;
        // see its doc for why filtered exports must not fold non-persisted drops in).
        let persisted_tracks: Vec<usize> = self
            .tracks
            .iter()
            .enumerate()
            .filter_map(|(i, slot)| slot.as_ref().map(|_| i))
            .collect();
        let seam_dropped = self.timeline.dropped_for(&persisted_tracks);
        // Frames arrived and all were dropped: previously finished cleanly as
        // zero-byte track files. Keyed on frames-offered so a legitimately empty
        // sink (chapters-only export, absent track class) still writes none.
        if self.frames_mapped == 0 && seam_dropped > 0 {
            return Err(crate::error::Error::SinkWroteNothing.into());
        }
        if seam_dropped > self.frames_mapped {
            return Err(crate::error::Error::SeamPlanDroppedMost {
                dropped: seam_dropped,
                written: self.frames_mapped,
            }
            .into());
        }
        if seam_dropped > 0 {
            tracing::info!(
                target: "mux",
                dropped = seam_dropped,
                "frames outside the playlist's clip marks were dropped at clip joins"
            );
        }
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
#[path = "demux_sink_tests.rs"]
mod tests;
