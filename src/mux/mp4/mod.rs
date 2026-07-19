//! Progressive MP4 (ISO-BMFF) muxer — `mp4://`.
//!
//! Writes `ftyp` + `mdat` + `moov` (moov-at-end): sample data streams straight
//! into `mdat` as frames arrive, per-track sample tables accumulate in memory,
//! and the `moov` index is written at `finish()` after seeking back to patch the
//! `mdat` size. Unlike the fragmented `fmp4` sibling (DASH init+moof/mdat), this
//! is a single self-contained file — the shape people mean by "an mp4".
//!
//! ## Track model
//!
//! One video track (HEVC / H.264) plus every audio track whose codec has a clean
//! MP4 mapping (AC-3 → `ac-3`/`dac3`, E-AC-3 → `ec-3`/`dec3`). This is the fit
//! oracle: a codec MP4 can't carry (TrueHD, DTS, LPCM) or that has no sample
//! entry here is **excluded, never silently dropped** — [`fit_report`] lets the
//! CLI enumerate exactly what was left out and why. Video NALs pass through
//! unchanged (the demux hands us length-prefixed hvcC/avcC framing — already
//! MP4's form). Decode timestamps are derived (the pipeline carries presentation
//! PTS only): video is constant-frame-rate on disc, so a constant decode
//! duration + signed `ctts` reproduces the B-frame reorder exactly; audio has no
//! reorder, so per-sample durations come straight from the PTS deltas.
//!
//! Reference: ISO/IEC 14496-12 (ISO base media file format), 14496-15 (avcC/hvcC).

use crate::disc::{Codec, DiscTitle, Stream as DiscStream};
use crate::pes::{PesFrame, Stream};
use std::io::{self, Seek, SeekFrom, Write};

mod audio;
mod boxes;
mod read;
use boxes::{bx, fullbox};
pub use read::Mp4Reader;

/// Nanoseconds per second — PTS is carried in ns, media timescales are Hz.
const NS: i64 = 1_000_000_000;

/// One accumulated sample's bookkeeping (the mdat bytes are already on disk).
struct Sample {
    /// Absolute file offset of the sample's first byte.
    offset: u64,
    /// Sample size in bytes.
    size: u32,
    /// Presentation timestamp in nanoseconds (composition time).
    pts_ns: i64,
    /// True for a sync sample (IDR / keyframe). Always true for audio.
    keyframe: bool,
}

/// Which media class a track carries (drives handler / header-box choice).
#[derive(Clone, Copy, PartialEq)]
enum Media {
    Video,
    Audio,
}

/// One output track: its identity, the inputs its sample entry needs, and its
/// accumulated samples.
struct Track {
    media: Media,
    /// 1-based MP4 track_ID.
    track_id: u32,
    codec: Codec,
    /// Video: `hvcC`/`avcC`. Audio: unused (the sample entry is built from the
    /// first frame's bitstream and cached in `audio_entry`).
    codec_private: Vec<u8>,
    width: u32,
    height: u32,
    colr: Option<(u16, u16, u16, bool)>,
    language: [u8; 2],
    /// Audio sample entry (`ac-3`/`ec-3` + config), built from the first frame.
    audio_entry: Option<Vec<u8>>,
    /// Audio media timescale (Hz), captured with `audio_entry`.
    audio_timescale: u32,
    samples: Vec<Sample>,
}

/// Why a stream was excluded from an `mp4://` mux (for the never-silent report).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mp4SkipReason {
    /// A subtitle track — MP4 carries only text subs; disc subs are bitmap.
    BitmapSubtitle,
    /// An audio codec with no MP4 mapping here (TrueHD, DTS, LPCM, …).
    UnmappableAudio,
    /// A secondary/dependent video view (e.g. MVC 3D right eye).
    SecondaryVideo,
}

/// The plan for an `mp4://` mux of `title`: which streams are carried and which
/// are excluded (with the reason). The CLI prints the exclusions so a lossy
/// export is never silent; the sink applies the same predicate.
pub struct Mp4FitReport {
    /// `title.streams` indices that will be muxed.
    pub included: Vec<usize>,
    /// Excluded `(stream index, reason)`.
    pub skipped: Vec<(usize, Mp4SkipReason)>,
}

/// Compute the fit plan without opening a file. Video: the first primary
/// HEVC/H.264 track. Audio: every AC-3 / E-AC-3 track. Everything else is
/// skipped with a reason.
pub fn fit_report(title: &DiscTitle) -> Mp4FitReport {
    let mut included = Vec::new();
    let mut skipped = Vec::new();
    let mut have_video = false;
    for (i, s) in title.streams.iter().enumerate() {
        match s {
            DiscStream::Video(v) => {
                if v.is_mvc_dependent() {
                    skipped.push((i, Mp4SkipReason::SecondaryVideo));
                } else if !have_video && matches!(v.codec, Codec::Hevc | Codec::H264) {
                    included.push(i);
                    have_video = true;
                } else {
                    skipped.push((i, Mp4SkipReason::SecondaryVideo));
                }
            }
            DiscStream::Audio(a) => {
                if audio::audio_fits(a.codec) {
                    included.push(i);
                } else {
                    skipped.push((i, Mp4SkipReason::UnmappableAudio));
                }
            }
            DiscStream::Subtitle(_) => skipped.push((i, Mp4SkipReason::BitmapSubtitle)),
        }
    }
    Mp4FitReport { included, skipped }
}

/// Pack an ISO 639-2 language ("eng") into the 15-bit mdhd form (bit 15 = 0,
/// three 5-bit values of `char - 0x60`). Falls back to "und".
fn pack_language(lang: &str) -> [u8; 2] {
    let b = lang.as_bytes();
    if b.len() != 3 || !b.iter().all(|c| c.is_ascii_lowercase()) {
        return [0x55, 0xC4]; // 'und'
    }
    let v = (((b[0] - 0x60) as u16) << 10) | (((b[1] - 0x60) as u16) << 5) | ((b[2] - 0x60) as u16);
    v.to_be_bytes()
}

/// Progressive MP4 sink. Owns a seekable writer so it can seek back to patch the
/// `mdat` size once all samples are written. The CLI wraps the output file in a
/// bounded-cache `WritebackFile` (like the MKV muxer) so a UHD-scale mux to slow
/// / network staging doesn't hit the dirty-page burst pathology; the `mdat` patch
/// is an ordinary backpatch seek, which `WritebackFile` handles the same way it
/// handles MKV cluster backpatching.
pub struct Mp4Sink<W: Write + Seek> {
    writer: W,
    title: DiscTitle,
    tracks: Vec<Track>,
    /// `title.streams` index → position in `tracks`, or `None` if excluded.
    route: Vec<Option<usize>>,
    /// File offset of the `mdat` box header (for the 64-bit size patch).
    mdat_start: u64,
    /// Running `mdat` payload size in bytes.
    mdat_payload: u64,
    finished: bool,
}

impl<W: Write + Seek> Mp4Sink<W> {
    /// Create the sink over an already-opened seekable `writer`: build the track
    /// plan (fit oracle) and write `ftyp` plus the `mdat` header (64-bit size,
    /// patched at `finish()`).
    pub fn create(mut writer: W, title: &DiscTitle) -> io::Result<Self> {
        let report = fit_report(title);
        let has_video = report
            .included
            .iter()
            .any(|&i| matches!(title.streams[i], DiscStream::Video(_)));
        if !has_video {
            return Err(crate::error::Error::MuxNoVideoTrack.into());
        }

        let mut tracks = Vec::new();
        let mut route = vec![None; title.streams.len()];
        let mut next_id = 1u32;
        let mut video_codec = Codec::Hevc;
        for &i in &report.included {
            let track_id = next_id;
            next_id += 1;
            route[i] = Some(tracks.len());
            match &title.streams[i] {
                DiscStream::Video(v) => {
                    video_codec = v.codec;
                    let cp = title
                        .codec_privates
                        .get(i)
                        .and_then(|c| c.clone())
                        .ok_or(crate::error::Error::MuxMissingCodecPrivate)?;
                    let (w, h) = v.resolution.pixels();
                    tracks.push(Track {
                        media: Media::Video,
                        track_id,
                        codec: v.codec,
                        codec_private: cp,
                        width: w,
                        height: h,
                        colr: video_colr(&title.streams[i]),
                        language: [0x55, 0xC4],
                        audio_entry: None,
                        audio_timescale: 0,
                        samples: Vec::new(),
                    });
                }
                DiscStream::Audio(a) => {
                    tracks.push(Track {
                        media: Media::Audio,
                        track_id,
                        codec: a.codec,
                        codec_private: Vec::new(),
                        width: 0,
                        height: 0,
                        colr: None,
                        language: pack_language(&a.language),
                        audio_entry: None,
                        audio_timescale: a.sample_rate.hz() as u32,
                        samples: Vec::new(),
                    });
                }
                DiscStream::Subtitle(_) => unreachable!("fit_report never includes subtitles"),
            }
        }

        let ftyp = build_ftyp(video_codec);
        let mdat_start = ftyp.len() as u64;
        writer.write_all(&ftyp)?;
        // mdat with 64-bit largesize: size=1 signals "largesize follows"; the
        // 8-byte largesize placeholder is patched at finish() once known.
        writer.write_all(&1u32.to_be_bytes())?;
        writer.write_all(b"mdat")?;
        writer.write_all(&0u64.to_be_bytes())?;

        Ok(Self {
            writer,
            title: title.clone(),
            tracks,
            route,
            mdat_start,
            mdat_payload: 0,
            finished: false,
        })
    }

    /// Assemble and write the `moov` box from every track's sample tables.
    fn write_moov(&mut self) -> io::Result<()> {
        // Movie timescale = 90 kHz; movie duration = the longest track (converted).
        let movie_ts = 90_000u32;
        let mut movie_dur = 0u64;
        let mut traks: Vec<Vec<u8>> = Vec::new();
        for t in &self.tracks {
            let (trak, secs) = build_trak(t);
            traks.push(trak);
            movie_dur = movie_dur.max((secs * movie_ts as f64) as u64);
        }
        let next_id = self.tracks.len() as u32 + 1;

        let mut moov = build_mvhd(movie_ts, movie_dur, next_id);
        for trak in traks {
            moov.extend_from_slice(&trak);
        }
        let moov = bx(b"moov", &moov);
        self.writer.write_all(&moov)
    }
}

impl<W: Write + Seek + Send> Stream for Mp4Sink<W> {
    fn read(&mut self) -> io::Result<Option<PesFrame>> {
        Err(crate::error::Error::StreamWriteOnly.into())
    }

    fn write(&mut self, frame: &PesFrame) -> io::Result<()> {
        let Some(slot) = self.route.get(frame.track).copied().flatten() else {
            return Ok(()); // excluded track (or out of range)
        };
        // Build the audio sample entry from the first frame of an audio track.
        if self.tracks[slot].media == Media::Audio && self.tracks[slot].audio_entry.is_none() {
            if let Some(entry) = audio::dolby_sample_entry(self.tracks[slot].codec, &frame.data) {
                self.tracks[slot].audio_entry = Some(entry);
            } else {
                // Unparseable first frame — skip until one parses (avoids a
                // track with samples but no sample entry).
                return Ok(());
            }
        }
        let pts_ns = frame.pts;
        let offset = self.mdat_start + 16 + self.mdat_payload;
        self.writer.write_all(&frame.data)?;
        self.mdat_payload += frame.data.len() as u64;
        self.tracks[slot].samples.push(Sample {
            offset,
            size: frame.data.len() as u32,
            pts_ns,
            keyframe: frame.keyframe,
        });
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        // Drop tracks that never received a sample (e.g. an audio track whose
        // frames never parsed) so moov carries no empty trak.
        self.tracks.retain(|t| !t.samples.is_empty());
        if self.tracks.is_empty() {
            return Err(crate::error::Error::MuxEmpty.into());
        }
        // Patch the mdat 64-bit largesize: header (16) + payload.
        let mdat_total = 16 + self.mdat_payload;
        self.writer.seek(SeekFrom::Start(self.mdat_start + 8))?;
        self.writer.write_all(&mdat_total.to_be_bytes())?;
        self.writer.seek(SeekFrom::End(0))?;
        self.write_moov()?;
        self.writer.flush()
    }

    fn info(&self) -> &DiscTitle {
        &self.title
    }
}

// ── per-track box assembly ───────────────────────────────────────────────────

/// Build a track's `trak` box and return `(bytes, duration_seconds)`.
fn build_trak(t: &Track) -> (Vec<u8>, f64) {
    match t.media {
        Media::Video => build_video_trak_full(t),
        Media::Audio => build_audio_trak_full(t),
    }
}

fn build_video_trak_full(t: &Track) -> (Vec<u8>, f64) {
    let timing = VideoTiming::derive(&t.samples);
    let media_dur = timing.total_duration();
    let secs = media_dur as f64 / timing.timescale as f64;

    let stsd = build_visual_stsd(t.codec, &t.codec_private, t.width, t.height, t.colr);
    let stbl = build_video_stbl(stsd, &t.samples, &timing);
    let minf = build_minf(video_vmhd(), stbl);
    let mdia = build_mdia(
        t.language,
        timing.timescale,
        media_dur,
        b"vide",
        "VideoHandler",
        minf,
    );
    let tkhd = build_tkhd(t.track_id, t.width, t.height, media_dur, false);
    let mut body = tkhd;
    body.extend_from_slice(&mdia);
    (bx(b"trak", &body), secs)
}

fn build_audio_trak_full(t: &Track) -> (Vec<u8>, f64) {
    let ts = t.audio_timescale.max(1);
    let durs = audio_sample_durations(&t.samples, ts);
    let media_dur: u64 = durs.iter().map(|&d| d as u64).sum();
    let secs = media_dur as f64 / ts as f64;

    let entry = t.audio_entry.clone().unwrap_or_default();
    let stbl = build_audio_stbl(entry, &t.samples, &durs);
    let minf = build_minf(audio_smhd(), stbl);
    let mdia = build_mdia(t.language, ts, media_dur, b"soun", "SoundHandler", minf);
    let tkhd = build_tkhd(t.track_id, 0, 0, media_dur, true);
    let mut body = tkhd;
    body.extend_from_slice(&mdia);
    (bx(b"trak", &body), secs)
}

// ── timing ───────────────────────────────────────────────────────────────────

/// Video decode timing: constant decode duration (CFR) + per-sample composition
/// time, so `ctts[i] = CTS[i] − i·d` reproduces the B-frame reorder.
struct VideoTiming {
    timescale: u32,
    sample_dur: u32,
    cts: Vec<i64>,
}

impl VideoTiming {
    fn derive(samples: &[Sample]) -> Self {
        let (timescale, sample_dur) = detect_rate(samples);
        let min_pts = samples.iter().map(|s| s.pts_ns).min().unwrap_or(0);
        let cts = samples
            .iter()
            .map(|s| ((s.pts_ns - min_pts) as i128 * timescale as i128 / NS as i128) as i64)
            .collect();
        Self {
            timescale,
            sample_dur,
            cts,
        }
    }
    fn total_duration(&self) -> u64 {
        self.cts.len() as u64 * self.sample_dur as u64
    }
    fn ctts(&self) -> Vec<i32> {
        self.cts
            .iter()
            .enumerate()
            .map(|(i, &c)| (c - (i as i64 * self.sample_dur as i64)) as i32)
            .collect()
    }
}

/// Per-sample audio decode durations from PTS deltas (audio has no reorder, so
/// composition == decode). The last sample repeats the previous duration.
fn audio_sample_durations(samples: &[Sample], timescale: u32) -> Vec<u32> {
    let ticks = |ns: i64| (ns as i128 * timescale as i128 / NS as i128) as i64;
    let mut durs = Vec::with_capacity(samples.len());
    for w in samples.windows(2) {
        durs.push((ticks(w[1].pts_ns) - ticks(w[0].pts_ns)).max(0) as u32);
    }
    if let Some(&last) = durs.last() {
        durs.push(last);
    } else if !samples.is_empty() {
        durs.push(timescale / 30); // single-sample fallback
    }
    durs
}

/// Standard frame rates as `(timescale, sample_duration)` — exact integer ratios
/// so a CFR track has zero accumulated drift.
const STD_RATES: &[(u32, u32, f64)] = &[
    (24000, 1001, 23.976),
    (24, 1, 24.0),
    (25, 1, 25.0),
    (30000, 1001, 29.97),
    (30, 1, 30.0),
    (50, 1, 50.0),
    (60000, 1001, 59.94),
    (60, 1, 60.0),
];

/// Detect the constant frame rate from the median presentation delta, snapping
/// to the nearest standard rate. Falls back to a 90 kHz timescale with a rounded
/// duration when nothing matches (non-standard / too few samples).
fn detect_rate(samples: &[Sample]) -> (u32, u32) {
    if samples.len() < 2 {
        return (90_000, 3_003);
    }
    let mut pts: Vec<i64> = samples.iter().map(|s| s.pts_ns).collect();
    pts.sort_unstable();
    let mut deltas: Vec<i64> = pts
        .windows(2)
        .map(|w| w[1] - w[0])
        .filter(|&d| d > 0)
        .collect();
    if deltas.is_empty() {
        return (90_000, 3_003);
    }
    deltas.sort_unstable();
    let median = deltas[deltas.len() / 2];
    let fps = NS as f64 / median as f64;
    for &(ts, dur, rate) in STD_RATES {
        if (fps - rate).abs() < 0.5 {
            return (ts, dur);
        }
    }
    let dur = ((median as i128 * 90_000) / NS as i128).max(1) as u32;
    (90_000, dur)
}

// ── box builders ─────────────────────────────────────────────────────────────

/// `ftyp` — major brand `isom`, compatible brands incl. the codec brand.
fn build_ftyp(codec: Codec) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(b"isom");
    body.extend_from_slice(&0x200u32.to_be_bytes());
    body.extend_from_slice(b"isom");
    body.extend_from_slice(b"iso2");
    body.extend_from_slice(b"mp41");
    match codec {
        Codec::Hevc => body.extend_from_slice(b"hvc1"),
        Codec::H264 => body.extend_from_slice(b"avc1"),
        _ => {}
    }
    bx(b"ftyp", &body)
}

fn build_mvhd(timescale: u32, duration: u64, next_track_id: u32) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&0u64.to_be_bytes()); // creation_time
    body.extend_from_slice(&0u64.to_be_bytes()); // modification_time
    body.extend_from_slice(&timescale.to_be_bytes());
    body.extend_from_slice(&duration.to_be_bytes());
    body.extend_from_slice(&0x0001_0000u32.to_be_bytes()); // rate 1.0
    body.extend_from_slice(&0x0100u16.to_be_bytes()); // volume 1.0
    body.extend_from_slice(&[0u8; 2]);
    body.extend_from_slice(&[0u8; 8]);
    for v in [0x1_0000u32, 0, 0, 0, 0x1_0000, 0, 0, 0, 0x4000_0000] {
        body.extend_from_slice(&v.to_be_bytes());
    }
    body.extend_from_slice(&[0u8; 24]);
    body.extend_from_slice(&next_track_id.to_be_bytes());
    fullbox(b"mvhd", 1, 0, &body)
}

fn build_tkhd(track_id: u32, width: u32, height: u32, duration: u64, audio: bool) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&0u64.to_be_bytes()); // creation
    body.extend_from_slice(&0u64.to_be_bytes()); // modification
    body.extend_from_slice(&track_id.to_be_bytes());
    body.extend_from_slice(&[0u8; 4]); // reserved
    body.extend_from_slice(&duration.to_be_bytes());
    body.extend_from_slice(&[0u8; 8]); // reserved
    body.extend_from_slice(&0u16.to_be_bytes()); // layer
    body.extend_from_slice(&0u16.to_be_bytes()); // alternate_group
    body.extend_from_slice(&(if audio { 0x0100u16 } else { 0 }).to_be_bytes()); // volume
    body.extend_from_slice(&[0u8; 2]);
    for v in [0x1_0000u32, 0, 0, 0, 0x1_0000, 0, 0, 0, 0x4000_0000] {
        body.extend_from_slice(&v.to_be_bytes());
    }
    body.extend_from_slice(&(width << 16).to_be_bytes());
    body.extend_from_slice(&(height << 16).to_be_bytes());
    fullbox(b"tkhd", 1, 0x07, &body)
}

#[allow(clippy::too_many_arguments)]
fn build_mdia(
    language: [u8; 2],
    timescale: u32,
    duration: u64,
    handler: &[u8; 4],
    handler_name: &str,
    minf: Vec<u8>,
) -> Vec<u8> {
    let mut mdhd = Vec::new();
    mdhd.extend_from_slice(&0u64.to_be_bytes());
    mdhd.extend_from_slice(&0u64.to_be_bytes());
    mdhd.extend_from_slice(&timescale.to_be_bytes());
    mdhd.extend_from_slice(&duration.to_be_bytes());
    mdhd.extend_from_slice(&language);
    mdhd.extend_from_slice(&0u16.to_be_bytes());
    let mdhd = fullbox(b"mdhd", 1, 0, &mdhd);

    let hdlr = build_hdlr(handler, handler_name);

    let mut body = mdhd;
    body.extend_from_slice(&hdlr);
    body.extend_from_slice(&minf);
    bx(b"mdia", &body)
}

fn build_hdlr(handler: &[u8; 4], name: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&0u32.to_be_bytes());
    body.extend_from_slice(handler);
    body.extend_from_slice(&[0u8; 12]);
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    fullbox(b"hdlr", 0, 0, &body)
}

fn video_vmhd() -> Vec<u8> {
    let mut vmhd = Vec::new();
    vmhd.extend_from_slice(&0u16.to_be_bytes()); // graphicsmode
    vmhd.extend_from_slice(&[0u8; 6]); // opcolor
    fullbox(b"vmhd", 0, 1, &vmhd)
}

fn audio_smhd() -> Vec<u8> {
    let mut smhd = Vec::new();
    smhd.extend_from_slice(&0u16.to_be_bytes()); // balance
    smhd.extend_from_slice(&0u16.to_be_bytes()); // reserved
    fullbox(b"smhd", 0, 0, &smhd)
}

fn build_minf(header: Vec<u8>, stbl: Vec<u8>) -> Vec<u8> {
    let dinf = build_dinf();
    let mut body = header;
    body.extend_from_slice(&dinf);
    body.extend_from_slice(&stbl);
    bx(b"minf", &body)
}

fn build_dinf() -> Vec<u8> {
    let url = fullbox(b"url ", 0, 1, &[]);
    let mut dref = Vec::new();
    dref.extend_from_slice(&1u32.to_be_bytes());
    dref.extend_from_slice(&url);
    let dref = fullbox(b"dref", 0, 0, &dref);
    bx(b"dinf", &dref)
}

/// Colour signalling for the `colr` box (nclx): (primaries, transfer, matrix,
/// full_range). `None` when the stream carries no usable colour info.
fn video_colr(stream: &DiscStream) -> Option<(u16, u16, u16, bool)> {
    let DiscStream::Video(v) = stream else {
        return None;
    };
    if let Some(c) = v.measured_cicp {
        return Some((
            c.primaries as u16,
            c.transfer as u16,
            c.matrix as u16,
            c.range == 2,
        ));
    }
    use crate::disc::ColorSpace::*;
    let cicp = match v.color_space {
        Bt709 => (1, 1, 1),
        Bt2020 => (9, 16, 9),
        Bt470bg => (5, 6, 5),
        Smpte170m => (6, 6, 6),
        Unknown => return None,
    };
    Some((cicp.0, cicp.1, cicp.2, false))
}

/// Video `stbl`: sample entry + `stts`(constant) + `stss` + `ctts` + `stsc` +
/// `stsz` + `co64`.
fn build_video_stbl(stsd: Vec<u8>, samples: &[Sample], timing: &VideoTiming) -> Vec<u8> {
    let mut stts = Vec::new();
    stts.extend_from_slice(&1u32.to_be_bytes());
    stts.extend_from_slice(&(samples.len() as u32).to_be_bytes());
    stts.extend_from_slice(&timing.sample_dur.to_be_bytes());
    let stts = fullbox(b"stts", 0, 0, &stts);

    let sync: Vec<u32> = samples
        .iter()
        .enumerate()
        .filter(|(_, s)| s.keyframe)
        .map(|(i, _)| i as u32 + 1)
        .collect();
    let mut stss = Vec::new();
    stss.extend_from_slice(&(sync.len() as u32).to_be_bytes());
    for n in &sync {
        stss.extend_from_slice(&n.to_be_bytes());
    }
    let stss = fullbox(b"stss", 0, 0, &stss);

    let ctts = build_ctts(&timing.ctts());
    let stsc = build_stsc();
    let stsz = build_stsz(samples);
    let co64 = build_co64(samples);

    let mut body = stsd;
    body.extend_from_slice(&stts);
    body.extend_from_slice(&stss);
    body.extend_from_slice(&ctts);
    body.extend_from_slice(&stsc);
    body.extend_from_slice(&stsz);
    body.extend_from_slice(&co64);
    bx(b"stbl", &body)
}

/// Audio `stbl`: sample entry + run-length `stts` (per-sample durations) +
/// `stsc` + `stsz` + `co64`. No `stss` (every audio sample is a sync sample) and
/// no `ctts` (no reorder).
fn build_audio_stbl(sample_entry: Vec<u8>, samples: &[Sample], durs: &[u32]) -> Vec<u8> {
    let mut stsd = Vec::new();
    stsd.extend_from_slice(&1u32.to_be_bytes());
    stsd.extend_from_slice(&sample_entry);
    let stsd = fullbox(b"stsd", 0, 0, &stsd);

    // Run-length coalesce equal consecutive durations.
    let mut runs: Vec<(u32, u32)> = Vec::new();
    for &d in durs {
        match runs.last_mut() {
            Some((count, val)) if *val == d => *count += 1,
            _ => runs.push((1, d)),
        }
    }
    let mut stts = Vec::new();
    stts.extend_from_slice(&(runs.len() as u32).to_be_bytes());
    for (count, val) in &runs {
        stts.extend_from_slice(&count.to_be_bytes());
        stts.extend_from_slice(&val.to_be_bytes());
    }
    let stts = fullbox(b"stts", 0, 0, &stts);

    let stsc = build_stsc();
    let stsz = build_stsz(samples);
    let co64 = build_co64(samples);

    let mut body = stsd;
    body.extend_from_slice(&stts);
    body.extend_from_slice(&stsc);
    body.extend_from_slice(&stsz);
    body.extend_from_slice(&co64);
    bx(b"stbl", &body)
}

/// `stsc`: one sample per chunk (offsets listed one-per-sample in `co64`).
fn build_stsc() -> Vec<u8> {
    let mut stsc = Vec::new();
    stsc.extend_from_slice(&1u32.to_be_bytes()); // entry_count
    stsc.extend_from_slice(&1u32.to_be_bytes()); // first_chunk
    stsc.extend_from_slice(&1u32.to_be_bytes()); // samples_per_chunk
    stsc.extend_from_slice(&1u32.to_be_bytes()); // sample_description_index
    fullbox(b"stsc", 0, 0, &stsc)
}

fn build_stsz(samples: &[Sample]) -> Vec<u8> {
    let mut stsz = Vec::new();
    stsz.extend_from_slice(&0u32.to_be_bytes()); // per-sample sizes
    stsz.extend_from_slice(&(samples.len() as u32).to_be_bytes());
    for s in samples {
        stsz.extend_from_slice(&s.size.to_be_bytes());
    }
    fullbox(b"stsz", 0, 0, &stsz)
}

fn build_co64(samples: &[Sample]) -> Vec<u8> {
    let mut co64 = Vec::new();
    co64.extend_from_slice(&(samples.len() as u32).to_be_bytes());
    for s in samples {
        co64.extend_from_slice(&s.offset.to_be_bytes());
    }
    fullbox(b"co64", 0, 0, &co64)
}

/// `ctts` version 1 (signed composition offsets), run-length coalesced.
fn build_ctts(offsets: &[i32]) -> Vec<u8> {
    let mut runs: Vec<(u32, i32)> = Vec::new();
    for &o in offsets {
        match runs.last_mut() {
            Some((count, val)) if *val == o => *count += 1,
            _ => runs.push((1, o)),
        }
    }
    let mut body = Vec::new();
    body.extend_from_slice(&(runs.len() as u32).to_be_bytes());
    for (count, val) in &runs {
        body.extend_from_slice(&count.to_be_bytes());
        body.extend_from_slice(&val.to_be_bytes());
    }
    fullbox(b"ctts", 1, 0, &body)
}

/// Visual `stsd` with one `hvc1`/`avc1` sample entry carrying the config record
/// (`hvcC`/`avcC`) and, when present, a `colr` box.
fn build_visual_stsd(
    codec: Codec,
    codec_private: &[u8],
    width: u32,
    height: u32,
    colr: Option<(u16, u16, u16, bool)>,
) -> Vec<u8> {
    let (fourcc, cfg_type): (&[u8; 4], &[u8; 4]) = match codec {
        Codec::Hevc => (b"hvc1", b"hvcC"),
        _ => (b"avc1", b"avcC"),
    };

    let mut entry = Vec::new();
    entry.extend_from_slice(&[0u8; 6]);
    entry.extend_from_slice(&1u16.to_be_bytes()); // data_reference_index
    entry.extend_from_slice(&0u16.to_be_bytes()); // pre_defined
    entry.extend_from_slice(&0u16.to_be_bytes()); // reserved
    entry.extend_from_slice(&[0u8; 12]); // pre_defined[3]
    entry.extend_from_slice(&(width as u16).to_be_bytes());
    entry.extend_from_slice(&(height as u16).to_be_bytes());
    entry.extend_from_slice(&0x0048_0000u32.to_be_bytes());
    entry.extend_from_slice(&0x0048_0000u32.to_be_bytes());
    entry.extend_from_slice(&0u32.to_be_bytes());
    entry.extend_from_slice(&1u16.to_be_bytes()); // frame_count
    entry.extend_from_slice(&[0u8; 32]); // compressorname
    entry.extend_from_slice(&0x0018u16.to_be_bytes()); // depth
    entry.extend_from_slice(&0xFFFFu16.to_be_bytes());
    entry.extend_from_slice(&bx(cfg_type, codec_private));
    if let Some((p, t, m, full)) = colr {
        let mut c = Vec::new();
        c.extend_from_slice(b"nclx");
        c.extend_from_slice(&p.to_be_bytes());
        c.extend_from_slice(&t.to_be_bytes());
        c.extend_from_slice(&m.to_be_bytes());
        c.push(if full { 0x80 } else { 0x00 });
        entry.extend_from_slice(&bx(b"colr", &c));
    }
    let entry = bx(fourcc, &entry);

    let mut stsd = Vec::new();
    stsd.extend_from_slice(&1u32.to_be_bytes());
    stsd.extend_from_slice(&entry);
    fullbox(b"stsd", 0, 0, &stsd)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::{
        AudioChannels, AudioStream, Codec, ColorSpace, DiscTitle, FrameRate, HdrFormat,
        LabelPurpose, Resolution, SampleRate, Stream as DiscStream, SubtitleStream, VideoStream,
    };
    use crate::labels::LabelQualifier;

    fn hevc_video() -> DiscStream {
        DiscStream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Hevc,
            resolution: Resolution::R2160p,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Hdr10,
            color_space: ColorSpace::Bt2020,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })
    }

    fn audio(codec: Codec, lang: &str) -> DiscStream {
        DiscStream::Audio(AudioStream {
            pid: 0x1100,
            codec,
            channels: AudioChannels::Surround51,
            language: lang.into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        })
    }

    fn subtitle() -> DiscStream {
        DiscStream::Subtitle(SubtitleStream {
            pid: 0x1200,
            codec: Codec::Pgs,
            language: "eng".into(),
            forced: false,
            qualifier: LabelQualifier::None,
            codec_data: None,
        })
    }

    fn title(streams: Vec<DiscStream>, cps: Vec<Option<Vec<u8>>>) -> DiscTitle {
        let mut t = DiscTitle::empty();
        t.streams = streams;
        t.codec_privates = cps;
        t
    }

    #[test]
    fn fit_report_includes_video_and_dolby_only() {
        let t = title(
            vec![
                hevc_video(),
                audio(Codec::TrueHd, "eng"),
                audio(Codec::Ac3, "eng"),
                audio(Codec::Ac3Plus, "fra"),
                subtitle(),
            ],
            vec![Some(vec![1, 2, 3]), None, None, None, None],
        );
        let r = fit_report(&t);
        assert_eq!(r.included, vec![0, 2, 3], "video + AC3 + EAC3");
        // TrueHD (unmappable audio) and PGS (bitmap subtitle) are skipped.
        assert!(r.skipped.contains(&(1, Mp4SkipReason::UnmappableAudio)));
        assert!(r.skipped.contains(&(4, Mp4SkipReason::BitmapSubtitle)));
    }

    #[test]
    fn no_video_track_is_an_error() {
        let t = title(vec![audio(Codec::Ac3, "eng")], vec![None]);
        let err = match Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t) {
            Ok(_) => panic!("expected no-video-track error"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    fn frame(track: usize, pts_ns: i64, key: bool, data: Vec<u8>) -> PesFrame {
        PesFrame {
            track,
            pts: pts_ns,
            keyframe: key,
            data,
            duration_ns: None,
            source: None,
            coding: None,
        }
    }

    // A minimal AC-3 5.1 frame the audio parser accepts.
    fn ac3_frame() -> Vec<u8> {
        vec![
            0x0B,
            0x77,
            0x00,
            0x00,
            0b00_010110,
            0b01000_000,
            0b111_00_00_1,
            0x00,
            0xFF,
            0xFF,
        ]
    }

    fn walk(buf: &[u8]) -> Vec<([u8; 4], usize, usize)> {
        let mut out = Vec::new();
        let mut pos = 0;
        while pos + 8 <= buf.len() {
            let size = u32::from_be_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]);
            let size = if size == 1 {
                u64::from_be_bytes([
                    buf[pos + 8],
                    buf[pos + 9],
                    buf[pos + 10],
                    buf[pos + 11],
                    buf[pos + 12],
                    buf[pos + 13],
                    buf[pos + 14],
                    buf[pos + 15],
                ]) as usize
            } else {
                size as usize
            };
            let bt = [buf[pos + 4], buf[pos + 5], buf[pos + 6], buf[pos + 7]];
            assert!(size >= 8 && pos + size <= buf.len(), "box {bt:?} bad size");
            out.push((bt, pos, size));
            pos += size;
        }
        assert_eq!(pos, buf.len(), "top-level boxes tile exactly");
        out
    }

    #[test]
    fn av_mux_has_two_traks_and_tiles() {
        let t = title(
            vec![hevc_video(), audio(Codec::Ac3, "eng")],
            vec![Some(vec![1, 2, 3, 4]), None],
        );
        let d = 41_708_333;
        let mut s = Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t).unwrap();
        // Two video frames (track 0) + two AC-3 frames (track 1).
        s.write(&frame(0, 0, true, vec![0xAB; 800])).unwrap();
        s.write(&frame(1, 0, true, ac3_frame())).unwrap();
        s.write(&frame(0, d, false, vec![0xCD; 400])).unwrap();
        s.write(&frame(1, 32_000_000, true, ac3_frame())).unwrap();
        s.finish().unwrap();
        let buf = s.writer.into_inner();
        let boxes = walk(&buf);
        let types: Vec<[u8; 4]> = boxes.iter().map(|(t, _, _)| *t).collect();
        assert_eq!(types, vec![*b"ftyp", *b"mdat", *b"moov"]);
        // moov must contain exactly two trak boxes.
        let (_, ms, msz) = *boxes.iter().find(|(t, _, _)| t == b"moov").unwrap();
        let moov = &buf[ms + 8..ms + msz];
        let trak_count = {
            let mut n = 0;
            let mut pos = 0;
            while pos + 8 <= moov.len() {
                let size =
                    u32::from_be_bytes([moov[pos], moov[pos + 1], moov[pos + 2], moov[pos + 3]])
                        as usize;
                if &moov[pos + 4..pos + 8] == b"trak" {
                    n += 1;
                }
                if size < 8 {
                    break;
                }
                pos += size;
            }
            n
        };
        assert_eq!(trak_count, 2, "one video + one audio trak");
        // mdat = header + 800+400 video + two AC-3 frames.
        let (_, _, mdat_sz) = *boxes.iter().find(|(t, _, _)| t == b"mdat").unwrap();
        assert_eq!(mdat_sz, 16 + 800 + 400 + ac3_frame().len() * 2);
    }

    #[test]
    fn detect_rate_snaps_23_976() {
        let d = 41_708_333;
        let samples: Vec<Sample> = (0..10)
            .map(|i| Sample {
                offset: 0,
                size: 1,
                pts_ns: i as i64 * d,
                keyframe: i == 0,
            })
            .collect();
        assert_eq!(detect_rate(&samples), (24000, 1001));
    }
}
