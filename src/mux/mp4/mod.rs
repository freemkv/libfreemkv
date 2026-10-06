//! Progressive MP4 (ISO-BMFF) muxer — `mp4://`.
//!
//! Writes `ftyp` + `moov` + `mdat` with **faststart on by default**: a
//! `moov`-sized hole is reserved between `ftyp` and `mdat` at the start, sample
//! data streams into `mdat`, and at `finish()` the `moov` index is written into
//! the reserved hole, falling back to moov-at-end if the estimate is blown.
//!
//! One video track plus every audio track with a clean MP4 mapping; codecs MP4
//! can't carry are **excluded, never silently dropped** — [`fit_report`] says why.

use crate::disc::{Codec, DiscTitle, Stream as DiscStream};
use crate::pes::{PesFrame, PesSink};
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

// Faststart reserve sizing: reserve a `moov`-sized hole between `ftyp` and `mdat`
// so sample offsets are fixed up front (no rewrite/offset patch at finish).
// `moov` size scales with sample count (stsz/co64/ctts/stts/stss per sample).

/// Estimated `moov` bytes per sample (calibrated against real discs; bias high).
const BYTES_PER_SAMPLE: u64 = 16;
/// Safety buffer added on top of the rounded estimate.
const RESERVE_BUFFER: u64 = 4 << 20; // 4 MiB
/// Floor for the reserve (covers short/unknown-duration titles).
const RESERVE_FLOOR: u64 = 8 << 20; // 8 MiB
/// Rounding granularity for the reserve.
const RESERVE_GRAIN: u64 = 4 << 20; // 4 MiB
/// Largest reserve expressible in a `free` box's 32-bit size field, rounded down
/// to a whole grain.
const RESERVE_CAP: u64 = (u32::MAX as u64 / RESERVE_GRAIN) * RESERVE_GRAIN;

/// Round up to `RESERVE_GRAIN`, saturating rather than wrapping. `div_ceil` then
/// multiply overflows for inputs within one grain of `u64::MAX`, which would turn
/// an enormous estimate into a tiny reserve — the opposite of the intent.
fn round_up_grain(x: u64) -> u64 {
    x.div_ceil(RESERVE_GRAIN).saturating_mul(RESERVE_GRAIN)
}

// Whether leftover reserved-hole slack can be closed: 0 needs no `free` box, 8+ bytes holds one
// (8-byte box header). 1-7 bytes fits no box, so finish() falls back to moov-at-end.
fn faststart_fits(gap: u64) -> bool {
    gap == 0 || gap >= 8
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
                // A DTS core AU is ~512 samples vs 1536 for (E-)AC-3; modelling every
                // audio track as AC-3 under-reserved DTS tracks 3x and forced fallback.
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
    let reserve = round_up_grain(est)
        .max(RESERVE_FLOOR)
        .saturating_add(RESERVE_BUFFER);
    // The hole is a `free` box with a 32-bit size field; a reserve >= u32::MAX
    // would truncate and leave mdat beyond a box claiming to be far shorter.
    // Clamp to the largest grain-aligned value the field can hold.
    reserve.min(RESERVE_CAP)
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
    /// Video: `hvcC`/`avcC`. Audio: AAC's ASC; other entries come from the first
    /// frame's bitstream (cached in `audio_entry`).
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
    /// A DVD MPEG-2 multichannel extension track
    /// ([`AudioStream::is_mp2_extension`](crate::disc::AudioStream::is_mp2_extension)): MP4
    /// has no mapping for ISO/IEC 13818-3 extension frames. A *post-mux* reason: reported
    /// only once the track's packets arrived.
    Mp2Extension,
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
/// family (AC-3 / E-AC-3), DTS (core / DTS-HD HRA / DTS-HD MA), AAC and MPEG audio. Everything
/// else is skipped with a reason, except a DVD MPEG-2 multichannel extension track, which
/// only [`Mp4Sink::final_report`] lists, and only once its packets arrived.
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
            // Not planned either way: IFO coding mode 3 only declares it. The sink reports it
            // (Mp2Extension, post-mux) once its packets actually arrive.
            DiscStream::Audio(a) if a.is_mp2_extension() => {}
            DiscStream::Audio(a) => {
                let cp = title.codec_privates.get(i).and_then(|c| c.as_deref());
                let described = a.codec != Codec::Aac || cp.and_then(audio::aac_config).is_some();
                if audio::audio_fits(a.codec) && described {
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
    /// Streams the plan promised that `finish()` actually dropped, with why, plus MPEG-2
    /// extension tracks whose packets arrived (the plan never lists those).
    dropped: Vec<(usize, Mp4SkipReason)>,
    /// MPEG-2 multichannel extension tracks (no MP4 mapping), reported once packets arrive.
    excluded: super::ps::UnstoredExtensions,
    /// Clip-join PTS correction, shared with the MKV muxer and the demux sink: a
    /// multi-clip playlist's source PTS resets or jumps at each join.
    timeline: crate::mux::timeline::TimelineContinuity,
    /// The video stream that drives the timeline's epochs.
    ref_video: Option<usize>,
    /// Frames the timeline placed (the denominator for its drop count).
    frames_mapped: u64,
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
        // Track ids are 1-based, assigned in inclusion order. `moov`'s next_track_id
        // is max(track_id) + 1 computed after the sample-less retain, not this counter.
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
                    // ISO/IEC 14496-12 §8.3.2/§12.1.3 make width/height mandatory;
                    // writing 0x0 yields a structurally valid file no player can
                    // render, with no error anywhere, so refuse instead.
                    let (w, h) = v
                        .resolution
                        .pixels()
                        .ok_or(crate::error::Error::Mp4UnknownResolution)?;
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
                        // AAC's AudioSpecificConfig; the other codecs describe in-band.
                        codec_private: title
                            .codec_privates
                            .get(i)
                            .cloned()
                            .flatten()
                            .unwrap_or_default(),
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

        // Faststart: write only the 8-byte `free` header now; the body is left as a
        // hole, overwritten at finish() by moov + a smaller `free`. mdat therefore
        // starts at a fixed offset, so co64 offsets are correct as written.
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

        let ref_video = tracks
            .iter()
            .find(|t| t.media == Media::Video)
            .map(|t| t.stream_idx);
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
            excluded: super::ps::UnstoredExtensions::new(title, "MP4"),
            timeline: crate::mux::timeline::TimelineContinuity::with_clips(
                &title.clips,
                title.content_format,
            ),
            ref_video,
            frames_mapped: 0,
            dropped: Vec::new(),
        })
    }

    /// What the file ACTUALLY contains, in the same shape as the pre-mux
    /// [`fit_report`] plan. Before `finish()` it equals that plan; after
    /// `finish()` every track the writer had to drop has moved from `included`
    /// into `skipped` with a post-mux reason ([`Mp4SkipReason::NoSamples`],
    /// [`Mp4SkipReason::UndescribableAudio`], [`Mp4SkipReason::Mp2Extension`]).
    ///
    /// Call this after `finish()`, not the pre-mux plan, before reporting what was written: the
    /// plan is only a prediction and can still list a stream `finish()` had to drop.
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
        let delays = start_delays_ns(&self.tracks);
        for (t, &delay) in self.tracks.iter().zip(&delays) {
            let (trak, secs) = build_trak_at(t, delay);
            traks.push(trak);
            movie_dur = movie_dur.max((secs * movie_ts as f64) as u64);
        }
        // `next_track_id` must EXCEED every track_ID in use (ISO/IEC 14496-12 §8.2.2).
        // Deriving it from the retained count breaks if finish() drops a track (e.g.
        // ids [1, 3] retained → count 2 → 3 collides), so take the real maximum.
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

impl<W: Write + Seek + Send> PesSink for Mp4Sink<W> {
    fn write(&mut self, frame: &PesFrame) -> io::Result<()> {
        if self.finished {
            return Err(crate::error::Error::StreamClosed.into());
        }
        if self.excluded.drop_frame(frame.track) {
            if !self.dropped.iter().any(|&(i, _)| i == frame.track) {
                self.dropped
                    .push((frame.track, Mp4SkipReason::Mp2Extension));
            }
            return Ok(());
        }
        let Some(slot) = self.route.get(frame.track).copied().flatten() else {
            return Ok(()); // excluded track (or out of range)
        };
        // Derive the audio sample entry opportunistically from whichever frame parses
        // first; it's only needed at finish(). Dropping unparseable leading frames
        // here used to lose audio silently — finish() now reports that case loudly.
        if self.tracks[slot].media == Media::Audio
            && self.tracks[slot].audio_entry.is_none()
            && let Some(entry) = audio::dolby_sample_entry(
                self.tracks[slot].codec,
                &frame.data,
                self.tracks[slot].audio_timescale,
            )
            .or_else(|| {
                let t = &self.tracks[slot];
                audio::mpeg_sample_entry(t.codec, &frame.data, &t.codec_private)
            })
        {
            // A rate the title could not name (e.g. 22.05 kHz MP3) comes from the entry.
            if self.tracks[slot].audio_timescale == 0 {
                self.tracks[slot].audio_timescale = audio::entry_sample_rate(&entry).unwrap_or(0);
            }
            self.tracks[slot].audio_entry = Some(entry);
        }
        // Onto the continuous timeline first; `None` is material outside the clip marks.
        let is_video = self.tracks[slot].media == Media::Video;
        let Some(pts_ns) = self.timeline.map_picture(
            frame.pts,
            Some(frame.track) == self.ref_video,
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
        self.frames_mapped += 1;
        // Nothing decodes before the first video keyframe: it is not stored.
        if self.tracks[slot].media == Media::Video
            && !frame.keyframe
            && self.tracks[slot].samples.is_empty()
        {
            return Ok(());
        }
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
        // As the MKV muxer and demux sink: a seam plan that dropped everything, or most
        // of the title, is a failed mux, not a short file at exit 0.
        let seam_dropped = self.timeline.dropped_total();
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
        // Every drop below is recorded in `self.dropped` so `final_report()` cannot
        // keep claiming a track the file doesn't have; a `tracing::warn` alone left
        // the crate's own public report lying about the output.
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
        // An audio track with samples but no sample entry can't be described: stsd
        // would declare entry_count=1 around an empty entry, a structurally invalid
        // mp4. Drop and report it; unreferenced mdat bytes are harmless waste.
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
            Some(g) if faststart_fits(g) => {
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

    // The streams finish() had to drop — see final_report() for reasons. Folded into
    // MuxOutcome::undelivered_streams so callers learn this programmatically.
    fn undelivered_streams(&self) -> Vec<usize> {
        self.dropped.iter().map(|&(i, _)| i).collect()
    }
}

// ── per-track box assembly ───────────────────────────────────────────────────

/// Per-track start delay: how far each track's first sample lies after the earliest
/// sample of all tracks (the shared t=0).
fn start_delays_ns(tracks: &[Track]) -> Vec<i64> {
    let first = |t: &Track| t.samples.iter().map(|s| s.pts_ns).min();
    let origin = tracks.iter().filter_map(first).min().unwrap_or(0);
    tracks
        .iter()
        .map(|t| first(t).map_or(0, |f| f.saturating_sub(origin)))
        .collect()
}

/// `edts` with an empty edit of `delay_ns` followed by the whole media; empty when no delay.
fn build_edts(delay_ns: i64, media_secs: f64) -> Vec<u8> {
    if delay_ns <= 0 {
        return Vec::new();
    }
    let ticks = |ns: f64| (ns * MOVIE_TIMESCALE as f64 / NS as f64) as u64;
    let mut body = Vec::new();
    body.extend_from_slice(&2u32.to_be_bytes());
    for (dur, media_time) in [
        (ticks(delay_ns as f64), -1i64),
        ((media_secs * MOVIE_TIMESCALE as f64) as u64, 0),
    ] {
        body.extend_from_slice(&dur.to_be_bytes());
        body.extend_from_slice(&media_time.to_be_bytes());
        body.extend_from_slice(&1u16.to_be_bytes()); // media_rate_integer
        body.extend_from_slice(&0u16.to_be_bytes()); // media_rate_fraction
    }
    bx(b"edts", &fullbox(b"elst", 1, 0, &body))
}

/// Build a track's `trak` box and return `(bytes, duration_seconds)`.
fn build_trak_at(t: &Track, delay_ns: i64) -> (Vec<u8>, f64) {
    match t.media {
        Media::Video => build_video_trak_full(t, delay_ns),
        Media::Audio => build_audio_trak_full(t, delay_ns),
    }
}

fn build_video_trak_full(t: &Track, delay_ns: i64) -> (Vec<u8>, f64) {
    let timing = VideoTiming::derive(&t.samples);
    let media_dur = timing.total_duration();
    // Presentation, not decode, extent: lost frames leave CTS beyond `media_dur`.
    let secs = timing.presentation_end() as f64 / timing.timescale as f64;

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
    let tkhd_dur = ((secs + delay_ns.max(0) as f64 / NS as f64) * MOVIE_TIMESCALE as f64) as u64;
    let tkhd = build_tkhd(t.track_id, t.width, t.height, tkhd_dur, false);
    let mut body = tkhd;
    body.extend_from_slice(&build_edts(delay_ns, secs));
    body.extend_from_slice(&mdia);
    (
        bx(b"trak", &body),
        secs + delay_ns.max(0) as f64 / NS as f64,
    )
}

fn build_audio_trak_full(t: &Track, delay_ns: i64) -> (Vec<u8>, f64) {
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
    let tkhd_dur = ((secs + delay_ns.max(0) as f64 / NS as f64) * MOVIE_TIMESCALE as f64) as u64;
    let tkhd = build_tkhd(t.track_id, 0, 0, tkhd_dur, true);
    let mut body = tkhd;
    body.extend_from_slice(&build_edts(delay_ns, secs));
    body.extend_from_slice(&mdia);
    (
        bx(b"trak", &body),
        secs + delay_ns.max(0) as f64 / NS as f64,
    )
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
            .map(|s| {
                // Round to nearest tick: truncation lands CFR PTS one tick low.
                let num = (s.pts_ns - min_pts) as i128 * timescale as i128;
                ((num + NS as i128 / 2) / NS as i128) as i64
            })
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
    // Where the last frame to present ends; past `total_duration` when frames were lost.
    fn presentation_end(&self) -> u64 {
        let last = self.cts.iter().copied().max().unwrap_or(0).max(0) as u64;
        self.total_duration()
            .max(last.saturating_add(self.sample_dur as u64))
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

// Standard frame rates as (timescale, sample_duration, fps) — exact integer
// ratios so a CFR track has zero accumulated drift. Table order is NOT
// significant: detect_rate() picks the nearest entry, not the first match.
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

// How far the measured rate may sit from a STD_RATES entry and still snap to it; nearest-wins,
// not first-wins (see STD_RATES).
const RATE_TOLERANCE_FPS: f64 = 0.5;

/// Nearest `STD_RATES` entry to `fps` inside the tolerance window, as (timescale, duration).
// Nearest, not first: first-match declared exact 24/30/60 fps as their 1000/1001 twin.
pub(super) fn nearest_std_rate(fps: f64) -> Option<(u32, u32)> {
    let mut best: Option<(u32, u32, f64)> = None;
    for &(ts, dur, rate) in STD_RATES {
        let d = (fps - rate).abs();
        if d < RATE_TOLERANCE_FPS && best.is_none_or(|(_, _, best_d)| d < best_d) {
            best = Some((ts, dur, d));
        }
    }
    best.map(|(ts, dur, _)| (ts, dur))
}

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
    if let Some(r) = nearest_std_rate(fps) {
        return r;
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

// Colour signalling for `colr` (nclx, ISO/IEC 14496-12 §12.1.5). Must use
// crate::mux::mkv::cicp_for_video, the one resolver every sink shares.
fn video_colr(stream: &DiscStream) -> Option<(u16, u16, u16, bool)> {
    let DiscStream::Video(v) = stream else {
        return None;
    };
    // Nothing usable to signal: the resolver returns CICP "unspecified" (2/2/2)
    // here, but an absent `colr` box already means that, so omit it instead.
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
#[path = "mod_tests.rs"]
mod tests;
