//! Progressive MP4 (ISO-BMFF) muxer — `mp4://`.
//!
//! Writes `ftyp` + `moov` + `mdat` with **faststart on by default**: a
//! `moov`-sized hole is reserved between `ftyp` and `mdat` at the start (so
//! sample offsets are fixed and never rewritten), sample data streams into
//! `mdat`, and at `finish()` the `moov` index is written into the reserved hole
//! with a trailing `free` box for the slack. If the estimate is blown, it falls
//! back to moov-at-end. Unlike the fragmented `fmp4` sibling (DASH init+moof/
//! mdat), this is a single self-contained file — the shape people mean by "an
//! mp4" — and moov-first means it streams over HTTP without a pre-fetch.
//!
//! ## Track model
//!
//! One video track (HEVC / H.264) plus every audio track whose codec has a clean
//! MP4 mapping (AC-3 → `ac-3`/`dac3`, E-AC-3 → `ec-3`/`dec3`, DTS/DTS-HD →
//! `dtsc`/`dtsh`/`ddts`). This is the fit oracle: a codec MP4 can't carry
//! (TrueHD, LPCM) or that has no sample entry here is **excluded, never silently
//! dropped** — [`fit_report`] lets the
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

/// Movie (mvhd) timescale in Hz. `tkhd.duration` is expressed in THIS timescale
/// (ISO/IEC 14496-12 §8.3.2), not the track's own media timescale.
const MOVIE_TIMESCALE: u32 = 90_000;

// ── faststart reserve sizing ─────────────────────────────────────────────────
//
// Faststart is on by default: reserve a `moov`-sized hole between `ftyp` and
// `mdat`, then write `moov` into it at finish and pad the slack with a `free`
// box. Because the hole precedes `mdat`, sample offsets are fixed from the start
// — no rewrite, no offset patch. `moov` is dominated by the per-sample tables
// (`stsz` 4 B + `co64` 8 B + `ctts`/`stts`/`stss` ~4 B, one-sample-per-chunk), so
// its size scales with the total sample count.

/// Estimated `moov` bytes per sample (calibrated against real discs; bias high).
const BYTES_PER_SAMPLE: u64 = 16;
/// Safety buffer added on top of the rounded estimate.
const RESERVE_BUFFER: u64 = 4 << 20; // 4 MiB
/// Floor for the reserve (covers short/unknown-duration titles).
const RESERVE_FLOOR: u64 = 8 << 20; // 8 MiB
/// Rounding granularity for the reserve.
const RESERVE_GRAIN: u64 = 4 << 20; // 4 MiB

fn round_up_grain(x: u64) -> u64 {
    x.div_ceil(RESERVE_GRAIN) * RESERVE_GRAIN
}

/// Estimate the faststart hole: `round_up_4MB(bytes_per_sample × est_samples)`
/// plus a 4 MiB buffer, floored at 8 MiB. `est_samples` comes from the title
/// duration × each included track's frame rate.
fn estimate_reserve(title: &DiscTitle, included: &[usize]) -> u64 {
    let dur = title.duration_secs.max(1.0);
    let mut est_samples = 0f64;
    for &i in included {
        match &title.streams[i] {
            DiscStream::Video(v) => {
                let (n, d) = v.frame_rate.as_fraction();
                let fps = if n > 0 && d > 0 {
                    n as f64 / d as f64
                } else {
                    24.0
                };
                est_samples += dur * fps;
            }
            DiscStream::Audio(a) => {
                // Samples per frame differs sharply by codec, and 1.6.0 added DTS
                // to the carried set (audio_fits now admits Dts | DtsHdMa |
                // DtsHdHr). A DTS core AU is (nblks+1)*32 — commonly 512 samples,
                // a third of an (E-)AC-3 frame's 1536 — so modelling every audio
                // track as AC-3 under-reserved a DTS track's sample table 3x and
                // pushed the mux onto the moov-at-end fallback.
                let samples_per_frame = match a.codec {
                    Codec::Dts | Codec::DtsHdMa | Codec::DtsHdHr => 512.0,
                    _ => 1536.0,
                };
                est_samples += dur * (a.sample_rate.hz() / samples_per_frame);
            }
            DiscStream::Subtitle(_) => {}
        }
    }
    let est = (est_samples as u64).saturating_mul(BYTES_PER_SAMPLE);
    round_up_grain(est).max(RESERVE_FLOOR) + RESERVE_BUFFER
}

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
    /// `title.streams` index this track was built from — the identity
    /// `Mp4FitReport` speaks in, so a track dropped at `finish()` can be named
    /// in [`Mp4Sink::final_report`].
    stream_idx: usize,
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
///
/// Marked `#[non_exhaustive]`: new reasons appear as the writer learns to
/// distinguish more of them, so downstream must not match exhaustively.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Mp4SkipReason {
    /// A subtitle track — MP4 carries only text subs; disc subs are bitmap.
    BitmapSubtitle,
    /// An audio codec with no MP4 mapping here (TrueHD, LPCM, …). AC-3/E-AC-3 and
    /// DTS/DTS-HD ARE mapped and carried.
    UnmappableAudio,
    /// A secondary/dependent video view (e.g. MVC 3D right eye).
    SecondaryVideo,
    /// A primary video track whose codec this MP4 writer can't carry
    /// (only HEVC/H.264 are supported — e.g. VC-1, MPEG-2, AV1).
    UnmappableVideo,
    /// Planned as carried, but the stream delivered no sample at all, so
    /// `finish()` dropped the track rather than write an empty `trak`.
    /// A *post-mux* reason: [`fit_report`] cannot predict it, only
    /// [`Mp4Sink::final_report`] reports it.
    NoSamples,
    /// Planned as carried, and samples DID reach `mdat`, but no frame yielded a
    /// parseable audio sample entry, so the track could not be described in
    /// `stsd` and `finish()` dropped it (its bytes stay in `mdat`, unreferenced).
    /// A *post-mux* reason — see [`Mp4Sink::final_report`].
    UndescribableAudio,
}

/// The plan for an `mp4://` mux of `title`: which streams are carried and which
/// are excluded (with the reason). The CLI prints the exclusions so a lossy
/// export is never silent; the sink applies the same predicate.
///
/// [`fit_report`] returns the PRE-mux plan, which is a prediction: two of its
/// inclusions can still fail at `finish()` (a stream that delivers no sample, an
/// audio stream no frame of which yields a parseable sample entry). Ask
/// [`Mp4Sink::final_report`] after `finish()` for what the file actually
/// contains — the plan alone is not a statement about the output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mp4FitReport {
    /// `title.streams` indices that will be muxed.
    pub included: Vec<usize>,
    /// Excluded `(stream index, reason)`.
    pub skipped: Vec<(usize, Mp4SkipReason)>,
}

/// Compute the fit plan without opening a file. Video: the first primary
/// HEVC/H.264 track. Audio: every track `audio::audio_fits` carries — the Dolby
/// family (AC-3 / E-AC-3) and DTS (core / DTS-HD HRA / DTS-HD MA). Everything
/// else is skipped with a reason.
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
                } else if have_video {
                    // A second primary video (after one was already carried).
                    skipped.push((i, Mp4SkipReason::SecondaryVideo));
                } else {
                    // First primary video, but an unsupported codec (VC-1/MPEG-2/AV1).
                    skipped.push((i, Mp4SkipReason::UnmappableVideo));
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
    /// File offset of the `mdat` box header (for the 64-bit size patch). With
    /// faststart this is `ftyp_len + reserve` (the hole precedes `mdat`).
    mdat_start: u64,
    /// Running `mdat` payload size in bytes.
    mdat_payload: u64,
    /// File offset where the reserved faststart hole begins (right after `ftyp`).
    hole_start: u64,
    /// Reserved hole size in bytes (`moov` + trailing `free` padding go here).
    reserve: u64,
    finished: bool,
    /// The create-time (pre-mux) plan, kept so [`Self::final_report`] can hand
    /// back a report that matches the FILE rather than the prediction.
    plan: Mp4FitReport,
    /// Streams the plan promised that `finish()` actually dropped, with why.
    /// Empty until `finish()` runs.
    dropped: Vec<(usize, Mp4SkipReason)>,
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
            return Err(crate::error::Error::Mp4NoVideoTrack.into());
        }

        let mut tracks = Vec::new();
        let mut route = vec![None; title.streams.len()];
        let mut video_codec = Codec::Hevc;
        // Track ids are 1-based and assigned in inclusion order. `moov`'s
        // next_track_id is NOT derived from this counter — it is max(track_id) + 1
        // computed after the sample-less retain, since ids are handed out here
        // before any track is dropped.
        for (n, &i) in report.included.iter().enumerate() {
            let track_id = n as u32 + 1;
            route[i] = Some(tracks.len());
            match &title.streams[i] {
                DiscStream::Video(v) => {
                    video_codec = v.codec;
                    let cp = title
                        .codec_privates
                        .get(i)
                        .and_then(|c| c.clone())
                        .ok_or(crate::error::Error::Mp4MissingCodecPrivate)?;
                    let (w, h) = v.resolution.pixels();
                    tracks.push(Track {
                        media: Media::Video,
                        track_id,
                        stream_idx: i,
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
                        stream_idx: i,
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
        writer.write_all(&ftyp)?;
        let hole_start = ftyp.len() as u64;

        // Faststart: reserve a `moov`-sized hole (a `free` box) between `ftyp`
        // and `mdat`. Only the 8-byte `free` header is written now; the body is
        // left as a hole (sparse) and overwritten at finish() by moov + a smaller
        // `free`. `mdat` therefore starts at a fixed offset, so co64 offsets are
        // correct as written — no rewrite, no patch.
        let reserve = estimate_reserve(title, &report.included);
        writer.write_all(&(reserve as u32).to_be_bytes())?;
        writer.write_all(b"free")?;

        let mdat_start = hole_start + reserve;
        writer.seek(SeekFrom::Start(mdat_start))?;
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
            hole_start,
            reserve,
            finished: false,
            plan: report,
            dropped: Vec::new(),
        })
    }

    /// What the file ACTUALLY contains, in the same shape as the pre-mux
    /// [`fit_report`] plan. Before `finish()` it equals that plan; after
    /// `finish()` every track the writer had to drop has moved from `included`
    /// into `skipped` with a post-mux reason ([`Mp4SkipReason::NoSamples`],
    /// [`Mp4SkipReason::UndescribableAudio`]).
    ///
    /// This exists because the plan is a PREDICTION. `finish()` drops an audio
    /// track no frame of which yielded a parseable sample entry (it cannot be
    /// described in `stsd`) and returns `Ok` so an export whose video is fine
    /// still succeeds — but then the plan, which is the only structured report
    /// the crate publishes, still named that stream as carried. A caller
    /// believing it reported a successful export of a file with no audio. Ask
    /// this after `finish()` before telling anyone what was written.
    pub fn final_report(&self) -> Mp4FitReport {
        let mut included = self.plan.included.clone();
        included.retain(|i| !self.dropped.iter().any(|(d, _)| d == i));
        let mut skipped = self.plan.skipped.clone();
        skipped.extend(self.dropped.iter().copied());
        skipped.sort_by_key(|&(i, _)| i);
        Mp4FitReport { included, skipped }
    }

    /// Assemble the `moov` box from every track's sample tables.
    fn build_moov(&self) -> Vec<u8> {
        // Movie timescale = 90 kHz; movie duration = the longest track (converted).
        let movie_ts = MOVIE_TIMESCALE;
        let mut movie_dur = 0u64;
        let mut traks: Vec<Vec<u8>> = Vec::new();
        for t in &self.tracks {
            let (trak, secs) = build_trak(t);
            traks.push(trak);
            movie_dur = movie_dur.max((secs * movie_ts as f64) as u64);
        }
        // `next_track_id` must EXCEED every track_ID in use (ISO/IEC 14496-12
        // §8.2.2). Deriving it from the retained COUNT broke that whenever
        // `finish()` dropped a track: ids [1, 3] retained → count 2 → 3, which
        // names a live track, so a tool appending a track with it creates a
        // duplicate id. Take the real maximum.
        let next_id = self
            .tracks
            .iter()
            .map(|t| t.track_id)
            .max()
            .unwrap_or(0)
            .saturating_add(1);

        let mut moov = build_mvhd(movie_ts, movie_dur, next_id);
        for trak in traks {
            moov.extend_from_slice(&trak);
        }
        bx(b"moov", &moov)
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
        // Derive the audio sample entry opportunistically from whichever frame
        // parses first. The entry is only needed at finish(), when build_moov runs
        // — nothing on this path consumes it — so an unparseable frame must NOT
        // cost us the frame. Dropping leading frames here lost audio silently, and
        // a track whose frames never parsed vanished from the output entirely with
        // no report; finish() now decides that case loudly instead.
        if self.tracks[slot].media == Media::Audio
            && self.tracks[slot].audio_entry.is_none()
            && let Some(entry) = audio::dolby_sample_entry(self.tracks[slot].codec, &frame.data)
        {
            self.tracks[slot].audio_entry = Some(entry);
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
        // Every drop below is recorded in `self.dropped` so `final_report()` — the
        // structured answer to "what is in this file" — cannot keep claiming a
        // track the file does not have. A `tracing::warn` alone left the crate's
        // own public report lying about the output.
        let mut dropped = Vec::new();
        // Drop tracks that never received a sample so moov carries no empty trak.
        self.tracks.retain(|t| {
            let kept = !t.samples.is_empty();
            if !kept {
                tracing::warn!(
                    stream = t.stream_idx,
                    codec = ?t.codec,
                    "mp4: track received no samples, dropping it (see final_report)"
                );
                dropped.push((t.stream_idx, Mp4SkipReason::NoSamples));
            }
            kept
        });
        // An audio track with samples but no sample entry cannot be DESCRIBED: the
        // moov would carry an stsd declaring entry_count=1 around an empty entry,
        // i.e. a structurally invalid mp4 returned as success. Drop it instead, and
        // say so — this crate's policy is that a skipped track is never silent.
        // Its bytes stay in mdat unreferenced, which is harmless (wasted space in a
        // valid file) and preferable to failing an export whose video is fine.
        self.tracks.retain(|t| {
            let describable = t.media != Media::Audio || t.audio_entry.is_some();
            if !describable {
                tracing::warn!(
                    stream = t.stream_idx,
                    codec = ?t.codec,
                    samples = t.samples.len(),
                    "mp4: no audio frame yielded a parseable sample entry, dropping track \
                     (see final_report)"
                );
                dropped.push((t.stream_idx, Mp4SkipReason::UndescribableAudio));
            }
            describable
        });
        self.dropped.append(&mut dropped);
        if self.tracks.is_empty() {
            return Err(crate::error::Error::MuxEmpty.into());
        }
        // Patch the mdat 64-bit largesize: header (16) + payload.
        let mdat_total = 16 + self.mdat_payload;
        self.writer.seek(SeekFrom::Start(self.mdat_start + 8))?;
        self.writer.write_all(&mdat_total.to_be_bytes())?;

        let moov = self.build_moov();
        let moov_len = moov.len() as u64;
        let gap = self.reserve.checked_sub(moov_len);
        // Faststart when moov fits the reserved hole with either an exact fill or
        // room for a valid (≥8-byte) `free` box in the slack. Otherwise fall back
        // to moov-at-end (rare — the +4 MiB buffer makes this near-impossible).
        match gap {
            Some(g) if g == 0 || g >= 8 => {
                self.writer.seek(SeekFrom::Start(self.hole_start))?;
                self.writer.write_all(&moov)?;
                if g >= 8 {
                    // Fill the slack with a `free` box (header only; body is the
                    // existing hole, ignored by parsers).
                    self.writer.write_all(&(g as u32).to_be_bytes())?;
                    self.writer.write_all(b"free")?;
                }
            }
            _ => {
                // Fallback: moov-at-end. The reserved hole stays a `free` box.
                self.writer.seek(SeekFrom::End(0))?;
                self.writer.write_all(&moov)?;
            }
        }
        self.writer.seek(SeekFrom::End(0))?;
        self.writer.flush()
    }

    fn info(&self) -> &DiscTitle {
        &self.title
    }

    /// The streams `finish()` had to drop — see [`Self::final_report`] for the
    /// reasons. The driver surfaces this as `MuxOutcome::undelivered_streams` so
    /// the caller learns programmatically that the file is missing a stream the
    /// pre-mux plan promised, instead of only in a log line.
    fn undelivered_streams(&self) -> Vec<usize> {
        self.dropped.iter().map(|&(i, _)| i).collect()
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
    // tkhd.duration is in the MOVIE timescale, not `timing.timescale`.
    let tkhd_dur = (secs * MOVIE_TIMESCALE as f64) as u64;
    let tkhd = build_tkhd(t.track_id, t.width, t.height, tkhd_dur, false);
    let mut body = tkhd;
    body.extend_from_slice(&mdia);
    (bx(b"trak", &body), secs)
}

fn build_audio_trak_full(t: &Track) -> (Vec<u8>, f64) {
    let ts = t.audio_timescale.max(1);
    let durs = audio_sample_durations(&t.samples, ts);
    let media_dur: u64 = durs.iter().map(|&d| d as u64).sum();
    let secs = media_dur as f64 / ts as f64;

    // finish() drops any audio track without an entry, so this is Some for every
    // track that reaches here; default only guards a future caller of build_trak.
    let entry = t.audio_entry.clone().unwrap_or_default();
    let stbl = build_audio_stbl(entry, &t.samples, &durs);
    let minf = build_minf(audio_smhd(), stbl);
    let mdia = build_mdia(t.language, ts, media_dur, b"soun", "SoundHandler", minf);
    // tkhd.duration is in the MOVIE timescale, not the audio media timescale.
    let tkhd_dur = (secs * MOVIE_TIMESCALE as f64) as u64;
    let tkhd = build_tkhd(t.track_id, 0, 0, tkhd_dur, true);
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

/// Standard frame rates as `(timescale, sample_duration, fps)` — exact integer
/// ratios so a CFR track has zero accumulated drift.
///
/// The order of this table is NOT significant: [`detect_rate`] picks the entry
/// nearest the measured rate, so a new rate may be appended anywhere without
/// shadowing an existing one.
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

/// How far the measured rate may sit from a [`STD_RATES`] entry and still snap to
/// it. Half an fps separates every neighbouring pair in the table (23.976/24 are
/// 0.024 apart, so both fall inside one another's window — which is exactly why
/// the match must be nearest-wins, not first-wins).
const RATE_TOLERANCE_FPS: f64 = 0.5;

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
    // Snap to the NEAREST standard rate inside the tolerance window, not the
    // first one inside it. First-match made the answer depend on table order:
    // every 1000/1001 rate sits within 0.5 fps of its integer twin and precedes
    // it, so an exact 24.000 / 30.000 / 60.000 fps source was always declared
    // 24000/1001, 30000/1001, 60000/1001 — a 0.1% timing error over the whole
    // track. Nearest-match is order-independent, so the fix cannot be undone by
    // someone appending a rate to STD_RATES (which is why it is preferred over
    // simply reordering the table).
    let mut best: Option<(u32, u32, f64)> = None;
    for &(ts, dur, rate) in STD_RATES {
        let d = (fps - rate).abs();
        if d < RATE_TOLERANCE_FPS && best.is_none_or(|(_, _, best_d)| d < best_d) {
            best = Some((ts, dur, d));
        }
    }
    if let Some((ts, dur, _)) = best {
        return (ts, dur);
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

/// Colour signalling for the `colr` box (nclx, ISO/IEC 14496-12 §12.1.5):
/// (primaries, transfer, matrix, full_range) as ITU-T H.273 code points. `None`
/// when the stream carries no usable colour info.
///
/// The code points come from [`crate::mux::mkv::cicp_for_video`] — the single
/// resolver EVERY sink shares (measured bitstream CICP first, then the coarse
/// `ColorSpace` enum with the HDR-driven transfer override). This box must never
/// carry its own copy of that mapping: the copy that used to live here had drifted
/// to hardcode transfer 16 (SMPTE ST 2084 / PQ) for all BT.2020 — tagging an HLG
/// title, whose transfer is 18 (ARIB STD-B67), as PQ — and transfer 6 (BT.601) for
/// BT.470 System B/G, whose transfer is 5. Both disagreed with the MKV sink and
/// the FVI sidecar for the same disc.
fn video_colr(stream: &DiscStream) -> Option<(u16, u16, u16, bool)> {
    let DiscStream::Video(v) = stream else {
        return None;
    };
    // No measured CICP and no colorimetry from the playlist → nothing usable to
    // signal. The shared resolver returns the CICP "unspecified" triple (2/2/2)
    // for that case; an ABSENT `colr` box already means exactly that, so omit the
    // box rather than write it (unchanged behaviour for this sink).
    if v.measured_cicp.is_none() && v.color_space == crate::disc::ColorSpace::Unknown {
        return None;
    }
    let (matrix, transfer, primaries, range) = crate::mux::mkv::cicp_for_video(v);
    Some((
        primaries as u16,
        transfer as u16,
        matrix as u16,
        // MeasuredCicp/Matroska Range: 2 = full, 1 = limited (the disc norm).
        range == 2,
    ))
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
    fn fit_report_labels_unsupported_primary_video() {
        // A primary video whose codec the MP4 writer can't carry (VC-1) must be
        // skipped as UnmappableVideo, NOT SecondaryVideo (which means an MVC view).
        let mut vc1 = match hevc_video() {
            DiscStream::Video(v) => v,
            _ => unreachable!(),
        };
        vc1.codec = Codec::Vc1;
        let t = title(
            vec![DiscStream::Video(vc1), audio(Codec::Ac3, "eng")],
            vec![None, None],
        );
        let r = fit_report(&t);
        assert!(r.skipped.contains(&(0, Mp4SkipReason::UnmappableVideo)));
        assert_eq!(r.included, vec![1], "only the AC-3 audio is carried");
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

    /// An audio track whose frames never yield a parseable sample entry must be
    /// dropped from moov, NOT written as an stsd declaring entry_count=1 around an
    /// empty entry — that is a structurally invalid mp4 returned as success.
    ///
    /// The video track must survive, and no audio frame may be lost from mdat on
    /// the way: write() previously returned Ok(()) without recording the sample,
    /// so leading audio frames vanished silently.
    ///
    /// Mutation check: restore write()'s early `return Ok(())` and the unparseable
    /// bytes never reach mdat; drop finish()'s describability retain and moov gains
    /// a second trak carrying an empty sample entry.
    #[test]
    fn audio_track_with_no_parseable_sample_entry_is_dropped_not_emitted_empty() {
        let t = title(
            vec![hevc_video(), audio(Codec::Ac3, "eng")],
            vec![Some(vec![1, 2, 3, 4]), None],
        );
        let mut s = Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t).unwrap();
        s.write(&frame(0, 0, true, vec![0xAB; 800])).unwrap();
        // Not an AC-3 syncframe: dolby_sample_entry cannot parse it, ever.
        let junk = vec![0x5Au8; 64];
        s.write(&frame(1, 0, true, junk.clone())).unwrap();
        s.write(&frame(1, 32_000_000, true, junk.clone())).unwrap();
        s.finish().unwrap();
        let buf = s.writer.into_inner();

        let boxes = walk(&buf);
        let (_, ms, msz) = *boxes.iter().find(|(t, _, _)| t == b"moov").unwrap();
        let moov = &buf[ms + 8..ms + msz];
        let mut traks = 0;
        let mut pos = 0;
        while pos + 8 <= moov.len() {
            let size = u32::from_be_bytes([moov[pos], moov[pos + 1], moov[pos + 2], moov[pos + 3]])
                as usize;
            if &moov[pos + 4..pos + 8] == b"trak" {
                traks += 1;
            }
            if size < 8 {
                break;
            }
            pos += size;
        }
        assert_eq!(
            traks, 1,
            "only the video trak may be described; the undescribable audio track is dropped"
        );

        // The audio bytes were still WRITTEN (no silent frame loss) — they simply
        // end up unreferenced in mdat rather than being discarded at write time.
        assert!(
            buf.windows(junk.len()).any(|w| w == &junk[..]),
            "audio frames must reach mdat rather than being dropped by write()"
        );
    }

    /// Dropping the undescribable audio track keeps the export succeeding (its
    /// video is fine), but the crate must not then keep CLAIMING that stream:
    /// `mp4_fit_report` — the only structured report — still lists it as
    /// included, so a caller printing the plan reports a successful export of a
    /// file with no audio at all.
    ///
    /// `final_report()` must therefore describe the FILE (the stream moved to
    /// `skipped` with `UndescribableAudio`), and `undelivered_streams()` — which
    /// the driver folds into `MuxOutcome::undelivered_streams` — must name it so
    /// the loss is programmatic, not just a log line.
    ///
    /// Mutation check: stop recording the drop in `finish()` and the plan and the
    /// file disagree again with nothing but a `tracing::warn` between them.
    #[test]
    fn dropped_audio_track_is_reported_not_just_logged() {
        let t = title(
            vec![hevc_video(), audio(Codec::Ac3, "eng")],
            vec![Some(vec![1, 2, 3, 4]), None],
        );

        // The PRE-mux plan promises the audio stream. It cannot know better: the
        // codec fits, only the frames turn out to be unparseable.
        let plan = fit_report(&t);
        assert_eq!(plan.included, vec![0, 1], "the plan promises both streams");

        let mut s = Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t).unwrap();
        s.write(&frame(0, 0, true, vec![0xAB; 800])).unwrap();
        // Not an AC-3 syncframe — `dolby_sample_entry` can never parse it.
        s.write(&frame(1, 0, true, vec![0x5Au8; 64])).unwrap();
        assert!(
            s.undelivered_streams().is_empty(),
            "nothing is decided before finish()"
        );
        s.finish().unwrap();

        let actual = s.final_report();
        assert_eq!(
            actual.included,
            vec![0],
            "the post-mux report must list only the video the file actually carries"
        );
        assert!(
            actual
                .skipped
                .contains(&(1, Mp4SkipReason::UndescribableAudio)),
            "the dropped audio stream must appear as skipped with its reason: {:?}",
            actual.skipped
        );
        assert_eq!(
            s.undelivered_streams(),
            vec![1],
            "the driver's programmatic loss signal must name stream 1"
        );
    }

    /// `mvhd.next_track_id` must EXCEED every track_ID in the file (ISO/IEC
    /// 14496-12 §8.2.2). It was derived from the retained track COUNT, so a
    /// drop at `finish()` made it collide with a live id: ids [1, 3] retained →
    /// count 2 → next_track_id 3, which is track 3. A tool appending a track with
    /// that id creates a duplicate.
    #[test]
    fn mvhd_next_track_id_exceeds_every_retained_track_id() {
        let t = title(
            vec![
                hevc_video(),
                audio(Codec::Ac3, "eng"), // track_id 2 — gets no samples, dropped
                audio(Codec::Ac3, "fra"), // track_id 3 — survives
            ],
            vec![Some(vec![1, 2, 3, 4]), None, None],
        );
        let mut s = Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t).unwrap();
        s.write(&frame(0, 0, true, vec![0xAB; 800])).unwrap();
        // Nothing for stream 1; stream 2 gets real AC-3.
        s.write(&frame(2, 0, true, ac3_frame())).unwrap();
        s.write(&frame(2, 32_000_000, true, ac3_frame())).unwrap();
        s.finish().unwrap();

        // The middle track really was dropped (ids 1 and 3 retained).
        assert_eq!(s.undelivered_streams(), vec![1]);
        assert!(
            s.final_report()
                .skipped
                .contains(&(1, Mp4SkipReason::NoSamples))
        );
        let retained_ids: Vec<u32> = s.tracks.iter().map(|t| t.track_id).collect();
        assert_eq!(retained_ids, vec![1, 3]);

        let buf = s.writer.into_inner();
        let boxes = walk(&buf);
        let (_, ms, msz) = *boxes.iter().find(|(t, _, _)| t == b"moov").unwrap();
        let moov = &buf[ms + 8..ms + msz];
        // mvhd is moov's first child; next_track_id is its last 4 bytes.
        let mvhd_size = u32::from_be_bytes([moov[0], moov[1], moov[2], moov[3]]) as usize;
        assert_eq!(&moov[4..8], b"mvhd");
        let next_id = u32::from_be_bytes([
            moov[mvhd_size - 4],
            moov[mvhd_size - 3],
            moov[mvhd_size - 2],
            moov[mvhd_size - 1],
        ]);
        assert!(
            retained_ids.iter().all(|&id| next_id > id),
            "next_track_id {next_id} must exceed every used id {retained_ids:?}"
        );
        assert_eq!(next_id, 4);
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
        // Faststart layout: ftyp, moov, free (reserve slack), mdat — moov BEFORE mdat.
        assert_eq!(
            types,
            vec![*b"ftyp", *b"moov", *b"free", *b"mdat"],
            "faststart: moov precedes mdat"
        );
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
    fn reserve_rounds_to_4mb_plus_buffer() {
        // round_up_4MB(x) + 4 MiB, floored at 8 MiB.
        assert_eq!(round_up_grain(1), 4 << 20);
        assert_eq!(round_up_grain(4 << 20), 4 << 20);
        assert_eq!(round_up_grain((4 << 20) + 1), 8 << 20);
        // A 2 hr feature, 24 fps video + one AC-3 track: ~173k + ~225k samples
        // × 16 B ≈ 6.4 MB → round to 8 MB → +4 MB buffer = 12 MB (floor also 8+4).
        let mut t = title(vec![hevc_video(), audio(Codec::Ac3, "eng")], vec![]);
        t.duration_secs = 7200.0;
        let r = estimate_reserve(&t, &[0, 1]);
        assert!(
            r % (4 << 20) == 0,
            "reserve is 4 MiB-aligned + 4 MiB buffer"
        );
        assert!(
            r >= 12 << 20 && r <= 20 << 20,
            "≈12-16 MB for a 2h feature, got {r}"
        );
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

    // ── colr (ITU-T H.273 / CICP) ────────────────────────────────────────────

    /// Decode `(primaries, transfer, matrix, full_range)` back out of the `colr`
    /// nclx box of an emitted visual sample entry, so the assertion is on the
    /// bytes that reach the file. `None` when no `colr` box was written.
    fn colr_of(v: &VideoStream) -> Option<(u16, u16, u16, bool)> {
        // `codec_private` is a byte pattern that cannot itself contain "colr".
        let stsd = build_visual_stsd(
            Codec::Hevc,
            &[0u8; 8],
            1920,
            1080,
            video_colr(&DiscStream::Video(v.clone())),
        );
        let i = stsd.windows(4).position(|w| w == b"colr")?;
        let p = &stsd[i + 4..];
        assert_eq!(&p[..4], b"nclx", "only the nclx colour type is written");
        Some((
            u16::from_be_bytes([p[4], p[5]]),
            u16::from_be_bytes([p[6], p[7]]),
            u16::from_be_bytes([p[8], p[9]]),
            p[10] & 0x80 != 0,
        ))
    }

    fn video_stream() -> VideoStream {
        match hevc_video() {
            DiscStream::Video(v) => v,
            _ => unreachable!(),
        }
    }

    #[test]
    fn colr_transfer_is_hlg_for_an_hlg_title_not_pq() {
        // ITU-T H.273 Table 3: transfer 18 = ARIB STD-B67 (HLG), 16 = SMPTE
        // ST 2084 (PQ). `video_colr` hardcoded 16 for every BT.2020 stream, so an
        // HLG title got the PQ EOTF applied to it — while the MKV sink of the same
        // rip correctly wrote 18.
        let mut v = video_stream();
        v.hdr = HdrFormat::Hlg;
        v.color_space = ColorSpace::Bt2020;
        assert_eq!(
            colr_of(&v).expect("colr written"),
            (9, 18, 9, false),
            "BT.2020 primaries/matrix (9) with the HLG transfer (18)"
        );
    }

    #[test]
    fn colr_transfer_is_bt470bg_for_a_pal_dvd_not_bt601() {
        // ITU-T H.273: transfer 5 = ITU-R BT.470-6 System B/G, 6 = BT.601.
        // A PAL DVD is System B/G in all three code points.
        let mut v = video_stream();
        v.hdr = HdrFormat::Sdr;
        v.color_space = ColorSpace::Bt470bg;
        assert_eq!(colr_of(&v).expect("colr written"), (5, 5, 5, false));
    }

    #[test]
    fn colr_agrees_with_the_shared_cicp_resolver_for_every_color_space() {
        // One resolver, every sink: the `colr` box must carry exactly what
        // `mkv::cicp_for_video` returns for the same stream, so an mp4:// rip and
        // an mkv:// rip of one title can never describe different colour.
        for cs in [
            ColorSpace::Bt709,
            ColorSpace::Bt2020,
            ColorSpace::Bt470bg,
            ColorSpace::Smpte170m,
        ] {
            for hdr in [
                HdrFormat::Sdr,
                HdrFormat::Hdr10,
                HdrFormat::Hdr10Plus,
                HdrFormat::Hlg,
                HdrFormat::DolbyVision,
            ] {
                let mut v = video_stream();
                v.color_space = cs;
                v.hdr = hdr;
                let (m, t, p, r) = crate::mux::mkv::cicp_for_video(&v);
                assert_eq!(
                    colr_of(&v).expect("colr written"),
                    (p as u16, t as u16, m as u16, r == 2),
                    "colr disagrees with the shared resolver for {cs:?} / {hdr:?}"
                );
            }
        }
        // Unknown colorimetry: no usable colour info, so no `colr` box at all —
        // an absent box and an "unspecified" (2/2/2) box mean the same thing, and
        // writing nothing is what this sink has always done.
        let mut v = video_stream();
        v.color_space = ColorSpace::Unknown;
        assert!(colr_of(&v).is_none());
    }

    // ── detect_rate ──────────────────────────────────────────────────────────

    /// Mux a video-only MP4 whose samples are exactly `delta_ns` apart and return
    /// the `(mdhd.timescale, stts.sample_delta)` decoded out of the emitted file.
    fn muxed_video_timing(delta_ns: i64) -> (u32, u32) {
        let t = title(vec![hevc_video()], vec![Some(vec![1, 2, 3, 4])]);
        let mut s = Mp4Sink::create(std::io::Cursor::new(Vec::new()), &t).unwrap();
        for i in 0..10i64 {
            s.write(&frame(0, i * delta_ns, i == 0, vec![0xAB; 16]))
                .unwrap();
        }
        s.finish().unwrap();
        let buf = s.writer.into_inner();

        // One trak → exactly one `mdhd` and one `stts`.
        let i = buf.windows(4).position(|w| w == b"mdhd").expect("mdhd");
        // After the type: version+flags(4), creation(8), modification(8), timescale(4).
        let timescale = u32::from_be_bytes(buf[i + 24..i + 28].try_into().unwrap());
        let j = buf.windows(4).position(|w| w == b"stts").expect("stts");
        // After the type: version+flags(4), entry_count(4), sample_count(4), sample_delta(4).
        let delta = u32::from_be_bytes(buf[j + 16..j + 20].try_into().unwrap());
        (timescale, delta)
    }

    #[test]
    fn exact_integer_frame_rates_are_not_declared_as_their_fractional_twins() {
        // `detect_rate` returned the FIRST STD_RATES entry within 0.5 fps, and each
        // 1000/1001 rate precedes its integer twin, so 24.000 / 30.000 / 60.000
        // were always written as 24000/1001, 30000/1001 and 60000/1001. The
        // declared timescale/sample_delta is read back out of the muxed file.
        for (delta_ns, want) in [
            (41_666_667i64, (24u32, 1u32)), // 24.000
            (33_333_333, (30, 1)),          // 30.000
            (16_666_667, (60, 1)),          // 60.000
            (40_000_000, (25, 1)),          // 25.000
            (20_000_000, (50, 1)),          // 50.000
            (41_708_333, (24_000, 1001)),   // 23.976
            (33_366_667, (30_000, 1001)),   // 29.97
            (16_683_333, (60_000, 1001)),   // 59.94
        ] {
            assert_eq!(
                muxed_video_timing(delta_ns),
                want,
                "{delta_ns} ns/frame must be declared as {want:?}"
            );
        }
    }

    #[test]
    fn detect_rate_picks_the_nearest_std_rate_regardless_of_table_order() {
        // Order-independence is the property that keeps this fixed: every entry
        // must resolve to itself when its own exact rate is measured, no matter
        // where it sits in STD_RATES. A first-match rule can only satisfy this if
        // the table happens to be ordered, which is what broke.
        for &(ts, dur, rate) in STD_RATES {
            let d = (NS as f64 / rate).round() as i64;
            let samples: Vec<Sample> = (0..10)
                .map(|i| Sample {
                    offset: 0,
                    size: 1,
                    pts_ns: i as i64 * d,
                    keyframe: i == 0,
                })
                .collect();
            assert_eq!(
                detect_rate(&samples),
                (ts, dur),
                "{rate} fps must resolve to its own STD_RATES entry"
            );
        }
    }
}
