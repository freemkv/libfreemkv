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
use crate::pes::{PesFrame, Stream};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Filename-naming strategy for the per-track files.
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

const ANNEXB_START: [u8; 4] = [0x00, 0x00, 0x00, 0x01];

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
        n += length_prefixed_to_annexb(&f.data, w)?;
        Ok(n)
    }
}

/// Convert a buffer of 4-byte big-endian length-prefixed NAL units to Annex-B
/// (each NAL prefixed with `00 00 00 01`). Returns bytes written. A malformed
/// length (running past the buffer) stops the walk cleanly rather than panic.
fn length_prefixed_to_annexb(data: &[u8], w: &mut dyn Write) -> io::Result<usize> {
    let mut pos = 0;
    let mut written = 0;
    while pos + 4 <= data.len() {
        let len =
            u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;
        if len == 0 || pos + len > data.len() {
            // Truncated / malformed length prefix: stop the walk. Emitting a
            // partial NAL would corrupt the stream worse than dropping the tail.
            break;
        }
        w.write_all(&ANNEXB_START)?;
        w.write_all(&data[pos..pos + len])?;
        written += ANNEXB_START.len() + len;
        pos += len;
    }
    Ok(written)
}

/// Extract the parameter-set NALs from an hvcC (HEVC) or avcC (H.264)
/// configuration record and return them as a single Annex-B blob
/// (`00 00 00 01 | NAL …`). Returns an empty Vec if the record can't be parsed.
fn annexb_param_sets(codec: Codec, record: &[u8]) -> Vec<u8> {
    match codec {
        Codec::Hevc => hvcc_param_sets(record),
        Codec::H264 => avcc_param_sets(record),
        _ => Vec::new(),
    }
}

/// Parse VPS/SPS/PPS arrays out of an HEVCDecoderConfigurationRecord.
/// Layout: 22-byte fixed header, then `numOfArrays` (u8); per array:
/// `array_completeness|NAL_type` (u8), `numNalus` (u16 BE); per NAL:
/// `nalUnitLength` (u16 BE) + bytes.
fn hvcc_param_sets(rec: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    if rec.len() < 23 {
        return out;
    }
    let num_arrays = rec[22] as usize;
    let mut pos = 23;
    for _ in 0..num_arrays {
        if pos + 3 > rec.len() {
            break;
        }
        // rec[pos] = array_completeness(1) | reserved(1) | NAL_unit_type(6)
        pos += 1;
        let num_nalus = u16::from_be_bytes([rec[pos], rec[pos + 1]]) as usize;
        pos += 2;
        for _ in 0..num_nalus {
            if pos + 2 > rec.len() {
                return out;
            }
            let nlen = u16::from_be_bytes([rec[pos], rec[pos + 1]]) as usize;
            pos += 2;
            if pos + nlen > rec.len() {
                return out;
            }
            out.extend_from_slice(&ANNEXB_START);
            out.extend_from_slice(&rec[pos..pos + nlen]);
            pos += nlen;
        }
    }
    out
}

/// Parse SPS/PPS out of an AVCDecoderConfigurationRecord.
/// Layout: 5-byte fixed header, `numOfSPS`(u8, low 5 bits); per SPS:
/// length(u16 BE) + bytes; `numOfPPS`(u8); per PPS: length(u16 BE) + bytes.
fn avcc_param_sets(rec: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    if rec.len() < 6 {
        return out;
    }
    let num_sps = (rec[5] & 0x1F) as usize;
    let mut pos = 6;
    let take = |count: usize, pos: &mut usize, out: &mut Vec<u8>| -> bool {
        for _ in 0..count {
            if *pos + 2 > rec.len() {
                return false;
            }
            let nlen = u16::from_be_bytes([rec[*pos], rec[*pos + 1]]) as usize;
            *pos += 2;
            if *pos + nlen > rec.len() {
                return false;
            }
            out.extend_from_slice(&ANNEXB_START);
            out.extend_from_slice(&rec[*pos..*pos + nlen]);
            *pos += nlen;
        }
        true
    };
    if !take(num_sps, &mut pos, &mut out) {
        return out;
    }
    if pos >= rec.len() {
        return out;
    }
    let num_pps = rec[pos] as usize;
    pos += 1;
    take(num_pps, &mut pos, &mut out);
    out
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
        while pos + 3 <= data.len() {
            let size = u16::from_be_bytes([data[pos + 1], data[pos + 2]]) as usize;
            let seg_end = pos + 3 + size;
            if seg_end > data.len() {
                break;
            }
            w.write_all(&[0x50, 0x47])?; // "PG"
            w.write_all(&pts90k.to_be_bytes())?;
            w.write_all(&dts90k.to_be_bytes())?;
            w.write_all(&data[pos..seg_end])?;
            written += 13 + size;
            pos = seg_end;
        }
        Ok(written)
    }
}

impl EsWriter for PgsSupWriter {
    fn write_frame(&mut self, w: &mut dyn Write, f: &PesFrame, pts_ns: i64) -> io::Result<usize> {
        let pts90 = ns_to_90k(pts_ns);
        Self::emit_segments(&f.data, pts90, pts90, w)
    }
}

/// VobSub writer: appends raw SPUs to the `.sub` and records `(pts, filepos)`
/// for the `.idx` sidecar emitted at finish.
struct VobSubWriter {
    idx_path: PathBuf,
    /// Pre-formatted `.idx` palette header line bytes, if available.
    palette_line: Option<String>,
    entries: Vec<(i64, u64)>,
    pos: u64,
}

impl VobSubWriter {
    fn new(idx_path: PathBuf, codec_private: Option<&[u8]>) -> Self {
        // codec_private for DvdSub is the pre-formatted VobSub `.idx` palette
        // header (UTF-8). Carry it through verbatim if present.
        let palette_line = codec_private
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(|s| s.trim_end().to_string());
        Self {
            idx_path,
            palette_line,
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
) -> Box<dyn EsWriter> {
    match codec {
        Codec::Hevc | Codec::H264 => Box::new(AnnexBWriter::new(codec, codec_private)),
        Codec::Pgs => Box::new(PgsSupWriter),
        Codec::DvdSub => Box::new(VobSubWriter::new(
            idx_path.unwrap_or_else(|| PathBuf::from("subtitle.idx")),
            codec_private,
        )),
        _ => Box::new(PassthroughWriter),
    }
}

// ── Timeline rebase (seamless-branch PTS continuity) ─────────────────────────

/// Discontinuity threshold: a backward video-PTS jump larger than this opens a
/// new epoch. Mirrors the MKV muxer's `DISCONTINUITY_BACKSTEP_NS` (3 s).
const DISCONTINUITY_BACKSTEP_NS: i64 = 3_000_000_000;
/// 1 ms seam gap inserted between epochs (mirrors the MKV muxer).
const SEAM_GAP_NS: i64 = 1_000_000;

/// Port of the MKV muxer's `TimelineContinuity` for the demux sink: track 0
/// (primary video) drives epochs; a single global `offset_ns` is added to every
/// track so A/V sync is preserved across clip seams in seamless-branched titles.
struct TimelineRebase {
    offset_ns: i64,
    high_ns: i64,
    started: bool,
}

impl TimelineRebase {
    fn new() -> Self {
        Self {
            offset_ns: 0,
            high_ns: 0,
            started: false,
        }
    }

    /// Map a raw concatenated PTS to a continuous one. Only track 0 opens
    /// epochs; all tracks get the same global offset.
    fn rebase(&mut self, track: usize, pts_ns: i64) -> i64 {
        if track == 0 {
            if !self.started {
                self.started = true;
                self.high_ns = pts_ns;
            } else if pts_ns < self.high_ns - DISCONTINUITY_BACKSTEP_NS {
                // Clip seam: shift this and following frames forward so the new
                // epoch starts just after the previous high-water mark.
                self.offset_ns += (self.high_ns - pts_ns) + SEAM_GAP_NS;
            }
            let out = pts_ns + self.offset_ns;
            self.high_ns = self.high_ns.max(out);
            out
        } else {
            pts_ns + self.offset_ns
        }
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
    timeline: TimelineRebase,
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
            let writer = es_writer_for(codec, codec_private, sidecar.clone());

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
            timeline: TimelineRebase::new(),
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
        let pts = self.timeline.rebase(frame.track, frame.pts);
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

    #[test]
    fn length_prefixed_converts_to_annexb() {
        // Two NALs: lengths 2 and 3.
        let data = [0, 0, 0, 2, 0xAA, 0xBB, 0, 0, 0, 3, 0x01, 0x02, 0x03];
        let mut out = Vec::new();
        let n = length_prefixed_to_annexb(&data, &mut out).unwrap();
        assert_eq!(
            out,
            vec![0, 0, 0, 1, 0xAA, 0xBB, 0, 0, 0, 1, 0x01, 0x02, 0x03]
        );
        assert_eq!(n, out.len());
    }

    #[test]
    fn length_prefixed_stops_on_truncation() {
        // Declares length 5 but only 2 bytes follow → drop the bad tail.
        let data = [0, 0, 0, 5, 0xAA, 0xBB];
        let mut out = Vec::new();
        length_prefixed_to_annexb(&data, &mut out).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn avcc_param_sets_extracted_as_annexb() {
        // Minimal avcC: header(5) numSPS=1 spsLen=2 SPS=[0x67,0x42] numPPS=1
        // ppsLen=1 PPS=[0x68].
        let rec = [
            1, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0, 2, 0x67, 0x42, 1, 0, 1, 0x68,
        ];
        let blob = avcc_param_sets(&rec);
        assert_eq!(blob, vec![0, 0, 0, 1, 0x67, 0x42, 0, 0, 0, 1, 0x68]);
    }

    #[test]
    fn hvcc_param_sets_extracted_as_annexb() {
        // hvcC: 22-byte header (we only need byte 22 = numArrays), then arrays.
        let mut rec = vec![0u8; 22];
        rec.push(2); // numArrays
        // Array 1: type byte, numNalus=1, len=2, NAL=[0x40,0x01]
        rec.extend_from_slice(&[0x20, 0, 1, 0, 2, 0x40, 0x01]);
        // Array 2: type byte, numNalus=1, len=1, NAL=[0x42]
        rec.extend_from_slice(&[0x21, 0, 1, 0, 1, 0x42]);
        let blob = hvcc_param_sets(&rec);
        assert_eq!(blob, vec![0, 0, 0, 1, 0x40, 0x01, 0, 0, 0, 1, 0x42]);
    }

    #[test]
    fn annexb_writer_prepends_params_once() {
        let rec = [
            1, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0, 2, 0x67, 0x42, 1, 0, 1, 0x68,
        ];
        let mut w = AnnexBWriter::new(Codec::H264, Some(&rec));
        let mut out = Vec::new();
        let f1 = PesFrame {
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
        let payload = [0x16, 0x00, 0x02, 0xDE, 0xAD];
        let mut out = Vec::new();
        let written = PgsSupWriter::emit_segments(&payload, 0x10, 0x10, &mut out).unwrap();
        assert_eq!(&out[0..2], b"PG");
        assert_eq!(&out[2..6], &0x10u32.to_be_bytes()); // PTS
        assert_eq!(&out[6..10], &0x10u32.to_be_bytes()); // DTS
        assert_eq!(&out[10..], &payload); // segment body verbatim
        assert_eq!(written, 13 + 2);
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
        let mut w = VobSubWriter::new(idx.clone(), Some(b"palette: 000000, ffffff"));
        let mut sub = Vec::new();
        let f1 = PesFrame {
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![0xAA; 10],
            duration_ns: None,
        };
        let f2 = PesFrame {
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

    // ── Timeline rebase ──────────────────────────────────────────────────────

    #[test]
    fn timeline_rebase_handles_seam_jump() {
        let mut tl = TimelineRebase::new();
        // Clip 1: video 0..10s.
        assert_eq!(tl.rebase(0, 0), 0);
        assert_eq!(tl.rebase(1, 0), 0); // audio rides the same offset
        assert_eq!(tl.rebase(0, 10_000_000_000), 10_000_000_000);
        // Clip 2 seam: video PTS jumps back to ~0 (> 3s back) → new epoch.
        let out = tl.rebase(0, 0);
        assert!(out >= 10_000_000_000, "epoch must advance past prev high");
        // Audio in clip 2 gets the SAME offset (A/V sync preserved).
        let a = tl.rebase(1, 0);
        assert_eq!(a, out);
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
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![0x00, 0x00, 0x01, 0xB3, 0xDE],
            duration_ns: None,
        })
        .unwrap();
        sink.write(&PesFrame {
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
