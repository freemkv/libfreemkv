//! Progressive MP4 (ISO-BMFF) muxer — `mp4://`.
//!
//! Writes `ftyp` + `mdat` + `moov` (moov-at-end): sample data streams straight
//! into `mdat` as frames arrive, per-track sample tables accumulate in memory,
//! and the `moov` index is written at `finish()` after seeking back to patch the
//! `mdat` size. Unlike the fragmented `fmp4` sibling (DASH init+moof/mdat), this
//! is a single self-contained file — the shape people mean by "an mp4".
//!
//! ## Milestone 1 (this file): video track only
//!
//! One video track (HEVC / H.264), passthrough NALs (the demux already hands us
//! length-prefixed hvcC/avcC-form NALs — exactly MP4's framing, no reframing),
//! full sample tables (`stts`/`stsz`/`stsc`/`co64`/`stss`/`ctts`), and a `colr`
//! box for HDR10 signalling. Decode timestamps are derived (the pipeline carries
//! presentation PTS only): the stream is constant-frame-rate on disc, so a
//! constant decode duration + signed `ctts` composition offsets reproduces the
//! B-frame reorder exactly. Audio tracks and the fit-oracle track selection land
//! in Milestone 2.
//!
//! Reference: ISO/IEC 14496-12 (ISO base media file format), 14496-15 (NAL-unit
//! structured video: `avcC`/`hvcC`).

use crate::disc::{Codec, DiscTitle, Stream as DiscStream};
use crate::pes::{PesFrame, Stream};
use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;

mod boxes;
use boxes::{bx, fullbox};

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
    /// True for a sync sample (IDR / keyframe).
    keyframe: bool,
}

/// Progressive MP4 sink. Owns the output `File` so it can seek back to patch the
/// `mdat` size once all samples are written.
pub struct Mp4Sink {
    file: File,
    title: DiscTitle,
    /// Index into `title.streams` of the muxed video track.
    video_track: usize,
    codec: Codec,
    /// `hvcC` / `avcC` decoder configuration record (from `codec_privates`).
    codec_private: Vec<u8>,
    width: u32,
    height: u32,
    /// File offset of the `mdat` box header (for the 64-bit size patch).
    mdat_start: u64,
    /// Running `mdat` payload size in bytes.
    mdat_payload: u64,
    samples: Vec<Sample>,
    finished: bool,
}

impl Mp4Sink {
    /// Create the sink: pick the video track, open the file, and write `ftyp`
    /// plus the `mdat` header (64-bit size, patched at `finish()`).
    pub fn create(path: &Path, title: &DiscTitle) -> io::Result<Self> {
        let (video_track, vs) = title
            .streams
            .iter()
            .enumerate()
            .find_map(|(i, s)| match s {
                DiscStream::Video(v) if !v.is_mvc_dependent() => Some((i, v)),
                _ => None,
            })
            .ok_or(crate::error::Error::MuxNoVideoTrack)?;

        let codec = vs.codec;
        if !matches!(codec, Codec::Hevc | Codec::H264) {
            // M1 carries only the NAL-structured codecs whose codec_private is a
            // ready hvcC/avcC. VC-1/MPEG-2 have no such record here (and VC-1 has
            // no MP4 mapping at all) — fail loud rather than emit a broken track.
            return Err(crate::error::Error::Mp4UnsupportedVideoCodec.into());
        }
        let codec_private = title
            .codec_privates
            .get(video_track)
            .and_then(|c| c.clone())
            .ok_or(crate::error::Error::MuxMissingCodecPrivate)?;

        let (width, height) = vs.resolution.pixels();

        let mut file = File::create(path)?;
        file.write_all(&build_ftyp(codec))?;
        let mdat_start = file.stream_position()?;
        // mdat with 64-bit largesize: size=1 signals "largesize follows"; the
        // 8-byte largesize placeholder is patched at finish() once known.
        file.write_all(&1u32.to_be_bytes())?;
        file.write_all(b"mdat")?;
        file.write_all(&0u64.to_be_bytes())?;

        Ok(Self {
            file,
            title: title.clone(),
            video_track,
            codec,
            codec_private,
            width,
            height,
            mdat_start,
            mdat_payload: 0,
            samples: Vec::new(),
            finished: false,
        })
    }

    /// Assemble and write the `moov` box from the accumulated sample tables.
    fn write_moov(&mut self) -> io::Result<()> {
        let timing = Timing::derive(&self.samples);
        let stbl = build_stbl(
            self.codec,
            &self.codec_private,
            self.width,
            self.height,
            video_colr(&self.title.streams[self.video_track]),
            &self.samples,
            &timing,
        );
        let dur = timing.total_duration();

        let mut moov = Vec::new();
        moov.extend_from_slice(&build_mvhd(timing.timescale, dur));
        moov.extend_from_slice(&build_video_trak(
            self.width,
            self.height,
            timing.timescale,
            dur,
            stbl,
        ));
        let moov = bx(b"moov", &moov);
        self.file.write_all(&moov)
    }
}

impl Stream for Mp4Sink {
    fn read(&mut self) -> io::Result<Option<PesFrame>> {
        Err(crate::error::Error::StreamWriteOnly.into())
    }

    fn write(&mut self, frame: &PesFrame) -> io::Result<()> {
        // M1 is video-only: drop every non-video-track frame. (Audio tracks and
        // the never-silent fit report arrive in M2.)
        if frame.track != self.video_track {
            return Ok(());
        }
        let offset = self.mdat_start + 16 + self.mdat_payload;
        self.file.write_all(&frame.data)?;
        self.mdat_payload += frame.data.len() as u64;
        self.samples.push(Sample {
            offset,
            size: frame.data.len() as u32,
            pts_ns: frame.pts,
            keyframe: frame.keyframe,
        });
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        // Patch the mdat 64-bit largesize: header (16) + payload.
        let mdat_total = 16 + self.mdat_payload;
        self.file.seek(SeekFrom::Start(self.mdat_start + 8))?;
        self.file.write_all(&mdat_total.to_be_bytes())?;
        self.file.seek(SeekFrom::End(0))?;
        self.write_moov()?;
        self.file.flush()
    }

    fn info(&self) -> &DiscTitle {
        &self.title
    }
}

// ── timing derivation ────────────────────────────────────────────────────────

/// Per-track decode timing derived from presentation PTS. The pipeline carries
/// presentation timestamps only; MP4 needs a monotonic decode timeline plus a
/// composition offset per sample. On disc the video is constant-frame-rate, so a
/// constant decode duration reproduces decode order and `ctts[i] = CTS[i] - i·d`
/// (signed) reproduces the B-frame reorder exactly.
struct Timing {
    /// Media timescale (Hz).
    timescale: u32,
    /// Constant per-sample decode duration in timescale ticks.
    sample_dur: u32,
    /// Composition time of each sample in timescale ticks (CTS, ≥ 0, min = 0).
    cts: Vec<i64>,
}

impl Timing {
    fn derive(samples: &[Sample]) -> Self {
        // Median presentation delta → nearest standard frame rate, so the
        // integer timescale divides evenly (zero long-run drift).
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

    /// Track duration in timescale ticks: sum of the constant decode durations.
    fn total_duration(&self) -> u64 {
        self.cts.len() as u64 * self.sample_dur as u64
    }

    /// Signed composition offset `ctts[i] = CTS[i] − DTS[i]`, `DTS[i] = i·d`.
    fn ctts(&self) -> Vec<i32> {
        self.cts
            .iter()
            .enumerate()
            .map(|(i, &c)| (c - (i as i64 * self.sample_dur as i64)) as i32)
            .collect()
    }
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
        return (90_000, 3_003); // ~29.97 placeholder; single-sample tracks are degenerate
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
    // Non-standard: 90 kHz with a rounded per-sample duration.
    let dur = ((median as i128 * 90_000) / NS as i128).max(1) as u32;
    (90_000, dur)
}

// ── box builders ─────────────────────────────────────────────────────────────

/// `ftyp` — major brand `isom`, compatible brands incl. the codec brand so
/// players recognise the video format (`hvc1` for HEVC, `avc1` for H.264).
fn build_ftyp(codec: Codec) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(b"isom");
    body.extend_from_slice(&0x200u32.to_be_bytes()); // minor_version
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

fn build_mvhd(timescale: u32, duration: u64) -> Vec<u8> {
    // Version 1 (64-bit times/duration).
    let mut body = Vec::new();
    body.extend_from_slice(&0u64.to_be_bytes()); // creation_time
    body.extend_from_slice(&0u64.to_be_bytes()); // modification_time
    body.extend_from_slice(&timescale.to_be_bytes());
    body.extend_from_slice(&duration.to_be_bytes());
    body.extend_from_slice(&0x0001_0000u32.to_be_bytes()); // rate 1.0
    body.extend_from_slice(&0x0100u16.to_be_bytes()); // volume 1.0
    body.extend_from_slice(&[0u8; 2]); // reserved
    body.extend_from_slice(&[0u8; 8]); // reserved
    for v in [0x1_0000u32, 0, 0, 0, 0x1_0000, 0, 0, 0, 0x4000_0000] {
        body.extend_from_slice(&v.to_be_bytes());
    }
    body.extend_from_slice(&[0u8; 24]); // pre_defined[6]
    body.extend_from_slice(&2u32.to_be_bytes()); // next_track_ID
    fullbox(b"mvhd", 1, 0, &body)
}

fn build_video_trak(
    width: u32,
    height: u32,
    timescale: u32,
    duration: u64,
    stbl: Vec<u8>,
) -> Vec<u8> {
    let tkhd = build_tkhd(width, height, duration);
    let mdia = build_mdia(timescale, duration, stbl);
    let mut body = Vec::new();
    body.extend_from_slice(&tkhd);
    body.extend_from_slice(&mdia);
    bx(b"trak", &body)
}

fn build_tkhd(width: u32, height: u32, duration: u64) -> Vec<u8> {
    // Version 1, flags 0x000007 (enabled | in_movie | in_preview).
    let mut body = Vec::new();
    body.extend_from_slice(&0u64.to_be_bytes()); // creation_time
    body.extend_from_slice(&0u64.to_be_bytes()); // modification_time
    body.extend_from_slice(&1u32.to_be_bytes()); // track_ID
    body.extend_from_slice(&[0u8; 4]); // reserved
    body.extend_from_slice(&duration.to_be_bytes());
    body.extend_from_slice(&[0u8; 8]); // reserved
    body.extend_from_slice(&0u16.to_be_bytes()); // layer
    body.extend_from_slice(&0u16.to_be_bytes()); // alternate_group
    body.extend_from_slice(&0u16.to_be_bytes()); // volume (video = 0)
    body.extend_from_slice(&[0u8; 2]); // reserved
    for v in [0x1_0000u32, 0, 0, 0, 0x1_0000, 0, 0, 0, 0x4000_0000] {
        body.extend_from_slice(&v.to_be_bytes());
    }
    body.extend_from_slice(&(width << 16).to_be_bytes());
    body.extend_from_slice(&(height << 16).to_be_bytes());
    fullbox(b"tkhd", 1, 0x07, &body)
}

fn build_mdia(timescale: u32, duration: u64, stbl: Vec<u8>) -> Vec<u8> {
    let mut mdhd = Vec::new();
    mdhd.extend_from_slice(&0u64.to_be_bytes()); // creation
    mdhd.extend_from_slice(&0u64.to_be_bytes()); // modification
    mdhd.extend_from_slice(&timescale.to_be_bytes());
    mdhd.extend_from_slice(&duration.to_be_bytes());
    mdhd.extend_from_slice(&[0x55, 0xC4]); // language 'und'
    mdhd.extend_from_slice(&0u16.to_be_bytes()); // pre_defined
    let mdhd = fullbox(b"mdhd", 1, 0, &mdhd);

    let hdlr = build_hdlr(b"vide", "VideoHandler");
    let minf = build_video_minf(stbl);

    let mut body = Vec::new();
    body.extend_from_slice(&mdhd);
    body.extend_from_slice(&hdlr);
    body.extend_from_slice(&minf);
    bx(b"mdia", &body)
}

fn build_hdlr(handler: &[u8; 4], name: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&0u32.to_be_bytes()); // pre_defined
    body.extend_from_slice(handler);
    body.extend_from_slice(&[0u8; 12]); // reserved
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    fullbox(b"hdlr", 0, 0, &body)
}

fn build_video_minf(stbl: Vec<u8>) -> Vec<u8> {
    // vmhd (version 0, flags 1).
    let mut vmhd = Vec::new();
    vmhd.extend_from_slice(&0u16.to_be_bytes()); // graphicsmode
    vmhd.extend_from_slice(&[0u8; 6]); // opcolor
    let vmhd = fullbox(b"vmhd", 0, 1, &vmhd);
    let dinf = build_dinf();

    let mut body = Vec::new();
    body.extend_from_slice(&vmhd);
    body.extend_from_slice(&dinf);
    body.extend_from_slice(&stbl);
    bx(b"minf", &body)
}

fn build_dinf() -> Vec<u8> {
    let url = fullbox(b"url ", 0, 1, &[]); // self-contained
    let mut dref = Vec::new();
    dref.extend_from_slice(&1u32.to_be_bytes()); // entry_count
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
    // Fall back to the coarse colour-space enum → CICP code points.
    use crate::disc::ColorSpace::*;
    let cicp = match v.color_space {
        Bt709 => (1, 1, 1),
        Bt2020 => (9, 16, 9), // BT.2020 NCL; transfer 16 = PQ (HDR10 default)
        Bt470bg => (5, 6, 5),
        Smpte170m => (6, 6, 6),
        Unknown => return None,
    };
    Some((cicp.0, cicp.1, cicp.2, false))
}

/// Build `stbl` — the sample table: sample entry (`hvc1`/`avc1` + config + colr),
/// `stts`, `stss`, `ctts`, `stsc`, `stsz`, `co64`.
#[allow(clippy::too_many_arguments)]
fn build_stbl(
    codec: Codec,
    codec_private: &[u8],
    width: u32,
    height: u32,
    colr: Option<(u16, u16, u16, bool)>,
    samples: &[Sample],
    timing: &Timing,
) -> Vec<u8> {
    let stsd = build_visual_stsd(codec, codec_private, width, height, colr);

    // stts: one run of `count` samples, each with the constant decode duration.
    let mut stts = Vec::new();
    stts.extend_from_slice(&1u32.to_be_bytes()); // entry_count
    stts.extend_from_slice(&(samples.len() as u32).to_be_bytes());
    stts.extend_from_slice(&timing.sample_dur.to_be_bytes());
    let stts = fullbox(b"stts", 0, 0, &stts);

    // stss: 1-based sample numbers of the sync samples.
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

    // ctts (version 1, signed offsets), coalesced into runs.
    let ctts = build_ctts(&timing.ctts());

    // stsc: one sample per chunk (offsets listed one-per-sample in co64).
    let mut stsc = Vec::new();
    stsc.extend_from_slice(&1u32.to_be_bytes()); // entry_count
    stsc.extend_from_slice(&1u32.to_be_bytes()); // first_chunk
    stsc.extend_from_slice(&1u32.to_be_bytes()); // samples_per_chunk
    stsc.extend_from_slice(&1u32.to_be_bytes()); // sample_description_index
    let stsc = fullbox(b"stsc", 0, 0, &stsc);

    // stsz: per-sample sizes.
    let mut stsz = Vec::new();
    stsz.extend_from_slice(&0u32.to_be_bytes()); // sample_size 0 = per-sample
    stsz.extend_from_slice(&(samples.len() as u32).to_be_bytes());
    for s in samples {
        stsz.extend_from_slice(&s.size.to_be_bytes());
    }
    let stsz = fullbox(b"stsz", 0, 0, &stsz);

    // co64: 64-bit chunk (= per-sample) offsets — movies exceed 4 GiB.
    let mut co64 = Vec::new();
    co64.extend_from_slice(&(samples.len() as u32).to_be_bytes());
    for s in samples {
        co64.extend_from_slice(&s.offset.to_be_bytes());
    }
    let co64 = fullbox(b"co64", 0, 0, &co64);

    let mut body = Vec::new();
    body.extend_from_slice(&stsd);
    body.extend_from_slice(&stts);
    body.extend_from_slice(&stss);
    body.extend_from_slice(&ctts);
    body.extend_from_slice(&stsc);
    body.extend_from_slice(&stsz);
    body.extend_from_slice(&co64);
    bx(b"stbl", &body)
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
        body.extend_from_slice(&val.to_be_bytes()); // i32 BE
    }
    fullbox(b"ctts", 1, 0, &body)
}

/// Visual `stsd` with one `hvc1`/`avc1` sample entry carrying the config record
/// (`hvcC`/`avcC`) and, when present, a `colr` box for colour/HDR signalling.
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
    entry.extend_from_slice(&[0u8; 6]); // reserved
    entry.extend_from_slice(&1u16.to_be_bytes()); // data_reference_index
    entry.extend_from_slice(&0u16.to_be_bytes()); // pre_defined
    entry.extend_from_slice(&0u16.to_be_bytes()); // reserved
    entry.extend_from_slice(&[0u8; 12]); // pre_defined[3]
    entry.extend_from_slice(&(width as u16).to_be_bytes());
    entry.extend_from_slice(&(height as u16).to_be_bytes());
    entry.extend_from_slice(&0x0048_0000u32.to_be_bytes()); // horizresolution 72dpi
    entry.extend_from_slice(&0x0048_0000u32.to_be_bytes()); // vertresolution 72dpi
    entry.extend_from_slice(&0u32.to_be_bytes()); // reserved
    entry.extend_from_slice(&1u16.to_be_bytes()); // frame_count
    entry.extend_from_slice(&[0u8; 32]); // compressorname
    entry.extend_from_slice(&0x0018u16.to_be_bytes()); // depth
    entry.extend_from_slice(&0xFFFFu16.to_be_bytes()); // pre_defined = -1
    // Config record (hvcC / avcC) is the codec_private verbatim.
    entry.extend_from_slice(&bx(cfg_type, codec_private));
    if let Some((p, t, m, full)) = colr {
        let mut colr_body = Vec::new();
        colr_body.extend_from_slice(b"nclx");
        colr_body.extend_from_slice(&p.to_be_bytes());
        colr_body.extend_from_slice(&t.to_be_bytes());
        colr_body.extend_from_slice(&m.to_be_bytes());
        colr_body.push(if full { 0x80 } else { 0x00 }); // full_range flag in MSB
        entry.extend_from_slice(&bx(b"colr", &colr_body));
    }
    let entry = bx(fourcc, &entry);

    let mut stsd = Vec::new();
    stsd.extend_from_slice(&1u32.to_be_bytes()); // entry_count
    stsd.extend_from_slice(&entry);
    fullbox(b"stsd", 0, 0, &stsd)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::{
        Codec, ColorSpace, DiscTitle, FrameRate, HdrFormat, Resolution, Stream as DiscStream,
        VideoStream,
    };

    fn video_title() -> DiscTitle {
        let mut t = DiscTitle::empty();
        t.streams = vec![DiscStream::Video(VideoStream {
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
        })];
        // A minimal but non-empty hvcC stand-in (opaque to the muxer — copied
        // verbatim into the sample entry).
        t.codec_privates = vec![Some(vec![0x01, 0x02, 0x03, 0x04])];
        t
    }

    fn frame(track: usize, pts_ns: i64, key: bool, len: usize) -> PesFrame {
        PesFrame {
            track,
            pts: pts_ns,
            keyframe: key,
            data: vec![0xAB; len],
            duration_ns: None,
            source: None,
            coding: None,
        }
    }

    /// Walk a flat box sequence, asserting each declared size tiles exactly.
    fn walk(buf: &[u8]) -> Vec<([u8; 4], usize, usize)> {
        let mut out = Vec::new();
        let mut pos = 0;
        while pos + 8 <= buf.len() {
            let size = u32::from_be_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]);
            let bt = [buf[pos + 4], buf[pos + 5], buf[pos + 6], buf[pos + 7]];
            let size = if size == 1 {
                // 64-bit largesize (mdat).
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
            assert!(size >= 8, "box {bt:?} size {size} < 8");
            assert!(pos + size <= buf.len(), "box {bt:?} overruns buffer");
            out.push((bt, pos, size));
            pos += size;
        }
        assert_eq!(pos, buf.len(), "top-level boxes did not tile exactly");
        out
    }

    fn child<'a>(payload: &'a [u8], want: &[u8; 4]) -> Option<&'a [u8]> {
        let mut pos = 0;
        while pos + 8 <= payload.len() {
            let size = u32::from_be_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]) as usize;
            let bt = [
                payload[pos + 4],
                payload[pos + 5],
                payload[pos + 6],
                payload[pos + 7],
            ];
            if size < 8 || pos + size > payload.len() {
                return None;
            }
            if &bt == want {
                return Some(&payload[pos..pos + size]);
            }
            pos += size;
        }
        None
    }

    fn mux_to_file(frames: &[PesFrame]) -> Vec<u8> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "freemkv_mp4_test_{}_{}.mp4",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        {
            let mut sink = Mp4Sink::create(&path, &video_title()).unwrap();
            for f in frames {
                sink.write(f).unwrap();
            }
            sink.finish().unwrap();
        }
        let buf = std::fs::read(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        buf
    }

    #[test]
    fn top_level_is_ftyp_mdat_moov_and_tiles_exactly() {
        // 3 frames: IDR, then two at increasing PTS (24000/1001 fps cadence).
        let d = 41_708_333; // ~1/23.976 s in ns
        let frames = [
            frame(0, 0, true, 1000),
            frame(0, d, false, 500),
            frame(0, 2 * d, false, 400),
        ];
        let buf = mux_to_file(&frames);
        let boxes = walk(&buf);
        let types: Vec<[u8; 4]> = boxes.iter().map(|(t, _, _)| *t).collect();
        assert_eq!(types, vec![*b"ftyp", *b"mdat", *b"moov"]);
    }

    #[test]
    fn mdat_carries_only_video_track_bytes() {
        // Interleave an audio-track frame (track 1) that M1 must drop.
        let d = 41_708_333;
        let frames = [
            frame(0, 0, true, 1000),
            frame(1, 0, true, 9999), // audio — dropped in M1
            frame(0, d, false, 500),
        ];
        let buf = mux_to_file(&frames);
        let boxes = walk(&buf);
        let (_, mdat_start, mdat_size) = *boxes.iter().find(|(t, _, _)| t == b"mdat").unwrap();
        // mdat payload = 16-byte header + only the two video samples (1000 + 500).
        assert_eq!(mdat_size, 16 + 1000 + 500);
        let _ = mdat_start;
    }

    #[test]
    fn stbl_tables_match_the_video_samples() {
        let d = 41_708_333;
        let frames = [
            frame(0, 0, true, 1000),
            frame(0, d, false, 500),
            frame(0, 2 * d, false, 400),
        ];
        let buf = mux_to_file(&frames);
        let boxes = walk(&buf);
        let (_, ms, msz) = *boxes.iter().find(|(t, _, _)| t == b"moov").unwrap();
        let moov = &buf[ms + 8..ms + msz];
        let trak = child(moov, b"trak").unwrap();
        let mdia = child(&trak[8..], b"mdia").unwrap();
        let minf = child(&mdia[8..], b"minf").unwrap();
        let stbl = child(&minf[8..], b"stbl").unwrap();

        // stsz sample_count == 3.
        let stsz = child(&stbl[8..], b"stsz").unwrap();
        let count = u32::from_be_bytes([stsz[16], stsz[17], stsz[18], stsz[19]]);
        assert_eq!(count, 3, "three video samples");
        // co64 has 3 offsets (per-sample chunks).
        let co64 = child(&stbl[8..], b"co64").unwrap();
        let co_count = u32::from_be_bytes([co64[12], co64[13], co64[14], co64[15]]);
        assert_eq!(co_count, 3);
        // stss: exactly one sync sample (the IDR at index 1).
        let stss = child(&stbl[8..], b"stss").unwrap();
        let sync_count = u32::from_be_bytes([stss[12], stss[13], stss[14], stss[15]]);
        assert_eq!(sync_count, 1);
        let first_sync = u32::from_be_bytes([stss[16], stss[17], stss[18], stss[19]]);
        assert_eq!(first_sync, 1, "sync sample numbers are 1-based");
        // hvc1 sample entry present under stsd.
        let stsd = child(&stbl[8..], b"stsd").unwrap();
        assert!(child(&stsd[16..], b"hvc1").is_some(), "hvc1 sample entry");
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
