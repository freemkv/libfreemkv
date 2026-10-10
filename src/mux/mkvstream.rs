//! MkvStream — Matroska container stream.
//!
//! Read: MKV container → demux EBML → PES frames out.
//! Write: PES frames in → MKV mux → Matroska container.

use super::mkv::{MkvMuxer, MkvTrack};
use super::{WriteSeek, ebml};

// Parsed Info + Tracks. `ts_scale_ns` feeds the frame read path; `tracks` maps Matroska
// TrackNumbers onto `DiscTitle::streams` indices; `probe` is the public header view.
struct MkvHeader {
    title: crate::disc::DiscTitle,
    codec_privates: Vec<(u16, Vec<u8>)>,
    ts_scale_ns: i64,
    tracks: TrackTable,
    probe: MkvProbe,
}

/// What a Matroska file declares about itself, read by [`probe_mkv`] from the
/// Info and Tracks elements alone (no cluster is read).
///
/// `muxing_app` / `writing_app` name the program that wrote the file; for
/// freemkv output they carry its version (see [`parse_freemkv_version`]).
/// `duration_secs` is the Segment's declared Duration (absent when the file
/// has none). `last_cue_secs` is only filled by [`probe_mkv_with_cues`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MkvProbe {
    pub muxing_app: Option<String>,
    pub writing_app: Option<String>,
    pub duration_secs: Option<f64>,
    pub title: Option<String>,
    pub tracks: Vec<MkvProbeTrack>,
    /// TimestampScale in nanoseconds per tick (Matroska default 1 000 000).
    pub timestamp_scale: u64,
    /// Timestamp of the last Cues entry, i.e. the start of the last indexed
    /// cluster — a lower bound on the muxed runtime.
    pub last_cue_secs: Option<f64>,
}

/// One TrackEntry as declared in the Tracks element.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MkvProbeTrack {
    pub number: u16,
    pub kind: MkvTrackKind,
    pub codec_id: String,
    pub language: String,
}

/// Matroska TrackType.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MkvTrackKind {
    Video,
    Audio,
    Subtitle,
    Other(u64),
}

impl MkvTrackKind {
    fn from_track_type(t: u64) -> Self {
        match t {
            1 => Self::Video,
            2 => Self::Audio,
            17 => Self::Subtitle,
            n => Self::Other(n),
        }
    }
}

/// Read the EBML header, Segment Info and Tracks of a Matroska file and stop:
/// no cluster, Chapters, Attachments or Tags is touched, so this is cheap
/// enough to run over a whole library.
pub fn probe_mkv(mut r: impl Read) -> io::Result<MkvProbe> {
    Ok(parse_mkv_header(&mut r, false)?.probe)
}

/// [`probe_mkv`] plus `last_cue_secs`: follows the SeekHead to the Cues element
/// (seeking over clusters, never reading one). `last_cue_secs` stays `None`
/// when the file has no Cues.
pub fn probe_mkv_with_cues(mut r: impl Read + io::Seek) -> io::Result<MkvProbe> {
    r.seek(io::SeekFrom::Start(0))?;
    let mut probe = parse_mkv_header(&mut r, false)?.probe;
    r.seek(io::SeekFrom::Start(0))?;
    let last_cue_ticks = last_cue_ticks(&mut r)?;
    probe.last_cue_secs =
        last_cue_ticks.map(|t| t as f64 * probe.timestamp_scale as f64 / 1_000_000_000.0);
    Ok(probe)
}

/// The `(major, minor, patch)` of a freemkv muxing-app string such as
/// `"freemkv 1.7.7 (gc8e67f1)"`. `None` for other writers and for the bare
/// `"freemkv"` older builds wrote. A pre-release suffix on the patch
/// (`"1.8.0-rc.1"`) is ignored.
pub fn parse_freemkv_version(app: &str) -> Option<(u32, u32, u32)> {
    let ver = app
        .trim()
        .strip_prefix("freemkv ")?
        .split_whitespace()
        .next()?;
    let mut parts = ver.splitn(3, '.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?;
    let digits = patch
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(patch.len());
    Some((major, minor, patch[..digits].parse().ok()?))
}

// Walk the Segment's top-level elements (seeking, never reading a cluster) to the
// Cues — directly or via a SeekHead entry — and return the largest CueTime.
fn last_cue_ticks(r: &mut (impl Read + io::Seek)) -> io::Result<Option<u64>> {
    let (id, size, _) = ebml::read_element_header(r)?;
    if id != ebml::EBML || size == u64::MAX {
        return Err(crate::error::Error::MkvSourceInvalid.into());
    }
    skip_bytes(r, size)?;
    let (id, seg_size, _) = ebml::read_element_header(r)?;
    if id != ebml::SEGMENT {
        return Err(crate::error::Error::MkvSourceInvalid.into());
    }
    let seg_start = r.stream_position()?;
    let seg_end = seg_start.saturating_add(seg_size);
    let mut cues_pos: Option<u64> = None;
    let mut pos = seg_start;
    while pos < seg_end {
        let (id, size, hlen) = match ebml::read_element_header(r) {
            Ok(h) => h,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        };
        if size == u64::MAX {
            break;
        }
        match id {
            ebml::CUES => return read_last_cue(r, size).map(Some),
            ebml::SEEK_HEAD if cues_pos.is_none() => {
                cues_pos = seekhead_target(r, size, ebml::CUES)?;
            }
            ebml::CLUSTER => break,
            _ => {
                r.seek(io::SeekFrom::Current(i64::try_from(size).map_err(
                    |_| io::Error::from(crate::error::Error::MkvSourceInvalid),
                )?))?;
            }
        }
        pos = pos.saturating_add(hlen as u64).saturating_add(size);
        r.seek(io::SeekFrom::Start(pos))?;
    }
    let Some(rel) = cues_pos else {
        return Ok(None);
    };
    r.seek(io::SeekFrom::Start(seg_start.saturating_add(rel)))?;
    let (id, size, _) = ebml::read_element_header(r)?;
    if id != ebml::CUES || size == u64::MAX {
        return Err(crate::error::Error::MkvSourceInvalid.into());
    }
    read_last_cue(r, size).map(Some)
}

// SeekPosition (relative to the Segment body) of `target` in a SeekHead body.
fn seekhead_target(r: &mut impl Read, size: u64, target: u32) -> io::Result<Option<u64>> {
    let mut found = None;
    for_each_child(r, size, |r, cid, cs| {
        if cid != ebml::SEEK {
            return skip_bytes(r, cs);
        }
        let (mut id, mut at) = (None, None);
        for_each_child(r, cs, |r, sid, ss| match sid {
            ebml::SEEK_ID => {
                let b = ebml::read_binary_val(r, checked_size(ss, 4)?)?;
                id = Some(b.iter().fold(0u32, |acc, &x| (acc << 8) | x as u32));
                Ok(())
            }
            ebml::SEEK_POSITION => {
                at = Some(read_uint_bounded(r, ss)?);
                Ok(())
            }
            _ => skip_bytes(r, ss),
        })?;
        if id == Some(target) && found.is_none() {
            found = at;
        }
        Ok(())
    })?;
    Ok(found)
}

// Largest CueTime (in TimestampScale ticks) over the CuePoints of a Cues body.
fn read_last_cue(r: &mut impl Read, size: u64) -> io::Result<u64> {
    let mut last = 0u64;
    for_each_child(r, size, |r, cid, cs| {
        if cid != ebml::CUE_POINT {
            return skip_bytes(r, cs);
        }
        for_each_child(r, cs, |r, pid, ps| {
            if pid == ebml::CUE_TIME {
                last = last.max(read_uint_bounded(r, ps)?);
                Ok(())
            } else {
                skip_bytes(r, ps)
            }
        })
    })?;
    Ok(last)
}

// Visit each child of a sized master body, rejecting unknown-size or overrunning children.
fn for_each_child<R: Read>(
    r: &mut R,
    size: u64,
    mut f: impl FnMut(&mut R, u32, u64) -> io::Result<()>,
) -> io::Result<()> {
    let mut remaining = size;
    while remaining > 0 {
        let (cid, cs, hlen) = ebml::read_element_header(r)?;
        let consumed = (hlen as u64).saturating_add(cs);
        if cs == u64::MAX || consumed > remaining {
            return Err(crate::error::Error::MkvSourceInvalid.into());
        }
        remaining -= consumed;
        f(r, cid, cs)?;
    }
    Ok(())
}

// Skip `n` bytes on a forward-only reader (no Seek required). A short skip is a TRUNCATED
// element, reported as `MkvSourceInvalid` (not a silent `Ok`).
fn skip_bytes(r: &mut impl Read, n: u64) -> io::Result<()> {
    let skipped = io::copy(&mut r.take(n), &mut io::sink())?;
    if skipped != n {
        return Err(crate::error::Error::MkvSourceInvalid.into());
    }
    Ok(())
}

// ── Sanity caps for untrusted EBML element sizes: cast to `usize` for alloc/read, so every
// size is checked against a cap first (OOM/panic guard).

const MAX_BLOCK_SIZE: u64 = 64 * 1024 * 1024; // largest accepted SIMPLE_BLOCK payload
/// Largest accepted CODEC_PRIVATE payload. hvcC/avcC/setup blobs are a
/// few KB in practice; 16 MiB is far above any legitimate value.
const MAX_CODEC_PRIVATE: u64 = 16 * 1024 * 1024;
/// Largest total of decoded CODEC_PRIVATE retained across all TrackEntries.
const MAX_CODEC_PRIVATE_TOTAL: usize = 2 * MAX_CODEC_PRIVATE as usize;
/// Largest accepted string element (TITLE/CODEC_ID/LANGUAGE/TRACK_NAME).
const MAX_STRING_LEN: u64 = 64 * 1024;
/// EBML unsigned-int elements are at most 8 bytes wide.
const MAX_UINT_LEN: u64 = 8;
/// EBML float elements are 4 or 8 bytes wide.
const MAX_FLOAT_LEN: u64 = 8;

/// Reject an untrusted element size that exceeds `cap` before it is used
/// to allocate or read. Returns the size as `usize` when within bounds.
fn checked_size(size: u64, cap: u64) -> io::Result<usize> {
    if size > cap {
        return Err(crate::error::Error::MkvSourceInvalid.into());
    }
    Ok(size as usize)
}

/// Read a bounded unsigned int. Guards against `size > 8` (which would
/// otherwise index out of the fixed 8-byte buffer in `read_uint_val`)
/// before delegating.
fn read_uint_bounded(r: &mut impl Read, size: u64) -> io::Result<u64> {
    ebml::read_uint_val(r, checked_size(size, MAX_UINT_LEN)?)
}

/// Read a bounded UTF-8 string element.
fn read_string_bounded(r: &mut impl Read, size: u64) -> io::Result<String> {
    ebml::read_string_val(r, checked_size(size, MAX_STRING_LEN)?)
}

/// Read a bounded EBML float element (4- or 8-byte), reusing the same
/// `checked_size` cap as the uint/string readers instead of a raw `as usize`
/// truncation of an untrusted element size.
fn read_float_bounded(r: &mut impl Read, size: u64) -> io::Result<f64> {
    ebml::read_float_val(r, checked_size(size, MAX_FLOAT_LEN)?)
}

use crate::disc::*;
use std::io::{self, Read};

struct ReadState {
    reader: Box<dyn Read + Send>,
    /// Current cluster timestamp in TimestampScale *ticks* (not ms). Combined
    /// with each block's relative tick offset and scaled to nanoseconds via
    /// `ts_scale_ns`.
    cluster_ts_ticks: i64,
    /// TimestampScale in nanoseconds per tick (Matroska INFO/TimestampScale,
    /// default 1_000_000 = 1 ms). Foreign MKVs may use a different scale; the
    /// frame PTS must honour it, not assume milliseconds.
    ts_scale_ns: i64,
    /// Codec private data per track (track_number, hvcC/avcC bytes).
    codec_privates: Vec<(u16, Vec<u8>)>,
    /// TrackNumber → stream-index map (and per-track DefaultDuration). MKV
    /// TrackNumbers are not required to be `1..=N` in TrackEntry order, so this
    /// is the only permitted translation between the two spaces.
    tracks: TrackTable,
    /// Frames decoded from a LACED Block that have not been handed out yet. One
    /// Block can carry many frames (RFC 9559 §10.3) while `Stream::read` yields
    /// one at a time, so the surplus waits here.
    pending: std::collections::VecDeque<crate::pes::PesFrame>,
    /// Number of `BlockAdditions` subtrees skipped on read-back (see
    /// `MkvStream`'s `Stream::lost_bytes`). Each one is a per-frame side payload — for a
    /// Blu-ray 3D rip written by this crate, one MVC dependent-view (right-eye)
    /// access unit — that the PES frame model cannot carry, so it is dropped.
    additions_dropped: u64,
    /// Cumulative `BlockAdditional` payload bytes dropped on read-back.
    additions_dropped_bytes: u64,
}

impl ReadState {
    // Count one dropped unit (a block of an undecodable track) in errors()/lost_bytes().
    fn count_lost(&mut self, bytes: u64) {
        self.additions_dropped = self.additions_dropped.saturating_add(1);
        self.additions_dropped_bytes = self.additions_dropped_bytes.saturating_add(bytes);
    }

    // Count frames whose ContentEncoding could not be undone (see `parse_block_counted`).
    fn count_undecoded(&mut self, lost: &[u64]) {
        if !lost.is_empty() && self.additions_dropped == 0 {
            tracing::warn!(
                target: "mux",
                code = crate::error::E_MKV_SOURCE_INVALID,
                "mkv read-back: a compressed frame could not be inflated; dropped and \
                 counted in lost_bytes/errors"
            );
        }
        for &bytes in lost {
            self.count_lost(bytes);
        }
    }
}

// Safety cap on frames buffered before the first video frame triggers muxer
// construction (backstop for a pathological audio-only-prefix stream); past
// it we build with no measured field order (logged) rather than buffer forever.
const MAX_PENDING_FRAMES: usize = 4096;
// Companion byte cap: frame count alone doesn't bound memory (a UHD frame is
// hundreds of KB, so 4096 of them exceeds a gigabyte). Far above any real
// audio-only prefix, and finite.
const MAX_PENDING_BYTES: usize = 64 << 20;
// open() read-ahead cap while inferring PCM depths (plus at most one block).
const PCM_PROBE_BYTES: usize = 64 << 20;

enum Mode {
    Write(WriteMode),
    Read(Box<ReadState>),
}

// MKV write state with DEFERRED muxer construction: the track header
// (carrying `FieldOrder`) is written only once the first coded picture is
// in hand, so field order is the parser's MEASURED value, never a guess.
enum WriteMode {
    /// Header not written yet: buffering frames until the first video frame.
    Pending(Box<PendingMux>),
    /// Header written; muxing live. Boxed (MkvMuxer is large) to keep the enum
    /// small (clippy::large_enum_variant).
    Active(Box<MkvMuxer<Box<dyn WriteSeek + Send>>>),
    /// Sentinel held while the muxer is being built (across the Pending → Active
    /// swap); also the terminal state after a successful `finish()`.
    Building,
    /// Header write or finalize failed: the file is truncated, so every later
    /// `write()` / `finish()` errors instead of reporting success.
    Failed,
}

// Error for writing into a muxer that is finished or whose output already failed.
fn muxer_unusable() -> io::Error {
    crate::error::Error::StreamClosed.into()
}

/// Everything needed to build the muxer, held until the first coded picture
/// lets the primary video track's field order be set from the source.
struct PendingMux {
    writer: Box<dyn WriteSeek + Send>,
    tracks: Vec<MkvTrack>,
    timings: Vec<crate::pes::TrackTiming>,
    /// Index of the primary (first) video track, if any — the track whose
    /// `FieldOrder` is set from the first coded picture's measured coding.
    video_track: Option<usize>,
    /// `--log-level 3` opening-capture side-file path (if any).
    opening_capture_path: Option<std::path::PathBuf>,
    /// Frames received before activation, replayed in order once built. Each
    /// carries an optional MVC dependent-view `BlockAdditional` (present only
    /// for a 3D base-view frame that was already paired before activation).
    buffered: Vec<(crate::pes::PesFrame, Option<Vec<u8>>)>,
    /// Running payload total of `buffered`, so the cap can bound bytes and not
    /// only frame count. Maintained on push; `buffered` is drained exactly once,
    /// at activation, after which neither field is consulted again.
    buffered_bytes: usize,
    /// How far before the first IDR the timeline origin (the earliest sample) sits.
    origin_lead_ns: i64,
    /// Frames dropped for lying before the Blu-ray clip IN.
    dropped_pre_origin: u64,
}

impl PendingMux {
    // Origin = earliest buffered frame of any selected track (and the IDR). Only for
    // Blu-ray, frames before the clip IN (`in_ns`) are outside the play item: they
    // are dropped (counted) and do not seed the origin.
    fn set_origin(&mut self, idr_pts: i64, in_ns: Option<i64>) {
        let origin = self
            .buffered
            .iter()
            .map(|(f, _)| f.pts)
            .filter(|&pts| in_ns.is_none_or(|i| pts >= i))
            .fold(idr_pts, i64::min);
        self.origin_lead_ns = idr_pts.saturating_sub(origin);
        let before = self.buffered.len();
        self.buffered.retain(|(f, _)| f.pts >= origin);
        self.dropped_pre_origin = (before - self.buffered.len()) as u64;
    }
}

/// Matroska container stream.
pub struct MkvStream {
    disc_title: DiscTitle,
    mode: Mode,
    /// Blu-ray 3D (MVC) merge state — present iff the title carries an MVC
    /// dependent (right-eye) view. Folds the dependent stream's frames into the
    /// base video track as per-frame `BlockAdditional`, paired by PTS, so the
    /// output is a single MVC track instead of two independent H.264 tracks.
    mvc: Option<MvcMerge>,
    /// `title.streams` index → muxer track index when a track is left out without an MVC fold
    /// (`None` = not written); `None` when every stream maps to its own index.
    remap: Option<Vec<Option<usize>>>,
    /// DVD MPEG-2 multichannel extension tracks Matroska cannot store (no registered codec or
    /// BlockAddIDType carries 13818-3 `ext_frame`s); reported once their packets arrive.
    excluded: super::ps::UnstoredExtensions,
}

// Base frames held awaiting their PTS-matching dependent AU before the oldest
// flushes unpaired. SSIF interleaves base/dependent per unit so pairing is
// normally 1-2 frames deep; this only bounds memory if pairing drifts.
const MVC_PAIR_WINDOW: usize = 32;

/// A base-view frame (track already remapped to the muxer's base track index)
/// awaiting — or already carrying — its dependent-view `BlockAdditional`.
struct PendingBase {
    frame: crate::pes::PesFrame,
    additional: Option<Vec<u8>>,
}

/// State for folding the MVC dependent (right-eye) view into the base track.
struct MvcMerge {
    /// `title.streams` index of the base (left-eye) video stream.
    base_stream_idx: usize,
    /// `title.streams` index of the dependent (right-eye) video stream.
    dep_stream_idx: usize,
    /// Muxer track index of the base view — where the dependent AU is attached
    /// as a `BlockAdditional` and where `mvc_params` (the `mvcC` mapping) lives.
    base_track_idx: usize,
    /// `title.streams` index → muxer track index. The dependent maps to `None`
    /// (it becomes a BlockAdditional, not a track); every other stream shifts
    /// down by one if it followed the dependent in stream order.
    stream_to_track: Vec<Option<usize>>,
    /// Base frames (decode order) awaiting their dependent or a window flush.
    pending_base: std::collections::VecDeque<PendingBase>,
    /// Dependent AU data keyed by PTS, waiting for the matching base.
    dep_by_pts: std::collections::HashMap<i64, Vec<u8>>,
    /// `(subset_sps, pps)` from the first dependent AU — builds the `mvcC`
    /// MVCDecoderConfigurationRecord for the base track's BlockAdditionMapping.
    captured_params: Option<(Vec<u8>, Vec<u8>)>,
    /// Count of dependent AUs dropped with no matching base (diagnostic).
    orphan_deps: u64,
}

impl MvcMerge {
    // Muxer track of a frame that is neither the base nor the dependent view
    // (`Some(None)` = unmapped, dropped); `None` when the frame needs `ingest`.
    fn passthrough_track(&self, track: usize) -> Option<Option<usize>> {
        if track == self.base_stream_idx || track == self.dep_stream_idx {
            return None;
        }
        Some(self.stream_to_track.get(track).copied().flatten())
    }

    // Ingest one frame; returns `(frame, additional)` pairs ready for the muxer,
    // in emit order. Base frames buffer to pair with their dependent by PTS; the
    // dependent produces no frame of its own (folds into `BlockAdditional`).
    fn ingest(
        &mut self,
        frame: &crate::pes::PesFrame,
    ) -> Vec<(crate::pes::PesFrame, Option<Vec<u8>>)> {
        let mut out = Vec::new();
        if frame.track == self.dep_stream_idx {
            if self.captured_params.is_none() {
                self.captured_params = extract_mvc_params(&frame.data);
            }
            // Attach to a waiting base of the same PTS, else stash by PTS.
            if let Some(pb) = self
                .pending_base
                .iter_mut()
                .find(|pb| pb.frame.pts == frame.pts && pb.additional.is_none())
            {
                pb.additional = Some(frame.data.clone());
            } else {
                // Bound the orphan map BEFORE inserting: drop a badly-drifted buffer
                // but keep THIS just-arrived dependent, whose base frame commonly
                // arrives next — clearing after insert would discard it too.
                if self.dep_by_pts.len() >= MVC_PAIR_WINDOW * 4 {
                    self.orphan_deps += self.dep_by_pts.len() as u64;
                    self.dep_by_pts.clear();
                }
                // A duplicate-PTS dependent (e.g. a stale repeat after a stream
                // discontinuity) displaces the prior one — count it as an orphan
                // rather than losing it silently.
                if self
                    .dep_by_pts
                    .insert(frame.pts, frame.data.clone())
                    .is_some()
                {
                    self.orphan_deps += 1;
                }
            }
        } else if frame.track == self.base_stream_idx {
            let additional = self.dep_by_pts.remove(&frame.pts);
            let mut remapped = frame.clone();
            remapped.track = self.base_track_idx;
            self.pending_base.push_back(PendingBase {
                frame: remapped,
                additional,
            });
        } else {
            // Audio / subtitle / other video: remap the track index and forward.
            let mut remapped = frame.clone();
            if let Some(Some(t)) = self.stream_to_track.get(frame.track) {
                remapped.track = *t;
                out.push((remapped, None));
            }
        }
        self.drain_ready(&mut out);
        out
    }

    /// Emit base frames from the FIFO front once each has its dependent attached,
    /// or flush the oldest unpaired base as a plain Block when the window is full.
    fn drain_ready(&mut self, out: &mut Vec<(crate::pes::PesFrame, Option<Vec<u8>>)>) {
        loop {
            let front_ready = self
                .pending_base
                .front()
                .map(|pb| pb.additional.is_some())
                .unwrap_or(false);
            if (front_ready || self.pending_base.len() > MVC_PAIR_WINDOW)
                && let Some(pb) = self.pending_base.pop_front()
            {
                out.push((pb.frame, pb.additional));
                continue;
            }
            break;
        }
    }

    /// Flush every remaining buffered base frame (unpaired → plain Block) at EOF.
    fn flush(&mut self) -> Vec<(crate::pes::PesFrame, Option<Vec<u8>>)> {
        let mut out = Vec::new();
        for pb in self.pending_base.drain(..) {
            out.push((pb.frame, pb.additional));
        }
        self.orphan_deps += self.dep_by_pts.len() as u64;
        self.dep_by_pts.clear();
        out
    }
}

/// Hand a frame to the muxer, attaching the MVC dependent view as a
/// `BlockAdditional` when `additional` is `Some` (a 3D base frame), else a
/// plain block.
fn emit_to_muxer(
    m: &mut MkvMuxer<Box<dyn WriteSeek + Send>>,
    track: usize,
    frame: &crate::pes::PesFrame,
    additional: Option<&[u8]>,
) -> io::Result<()> {
    m.write_frame_at_with_padding(
        track,
        frame.pts,
        frame.keyframe,
        &frame.data,
        frame.duration_ns,
        additional,
        // Provenance: which clip this frame came from is a lookup, not a guess.
        frame.source.map(|s| s.byte),
        // This picture's measured coding: its scan type and what the seam plan may trim.
        frame.coding,
        frame.discard_padding_ns,
    )
}

// Scan a length-prefixed H.264 NAL stream for the first subset SPS (type 15)
// and PPS (type 8) that populate the `mvcC` record. `Some` only when both are
// found; `None` otherwise (serializer then emits no mvcC mapping and logs it).
fn extract_mvc_params(data: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut subset_sps: Option<Vec<u8>> = None;
    let mut pps: Option<Vec<u8>> = None;
    let mut i = 0usize;
    while i + 4 <= data.len() {
        let len = u32::from_be_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]) as usize;
        i += 4;
        // A zero-length NAL (a stray length prefix) is skipped, not fatal — the
        // subset SPS / PPS may still follow. A length that runs past the buffer
        // end IS unrecoverable (the NAL can't be read), so stop there.
        if len == 0 {
            continue;
        }
        if i + len > data.len() {
            break;
        }
        let nal = &data[i..i + len];
        i += len;
        match nal[0] & 0x1F {
            15 if subset_sps.is_none() => subset_sps = Some(nal.to_vec()),
            8 if pps.is_none() => pps = Some(nal.to_vec()),
            _ => {}
        }
        if subset_sps.is_some() && pps.is_some() {
            break;
        }
    }
    Some((subset_sps?, pps?))
}

impl MkvStream {
    /// Create for writing PES frames → MKV container. Codec privates come from
    /// `title.codec_privates` (populated by the input stream).
    ///
    /// `output_path` (when known) enables the `--log-level 3` opening-frame
    /// capture to `<output>.opening.bin`; `None` (e.g. an in-memory / stdio sink)
    /// silently skips the side-file capture — the per-track TrackEntry dump still
    /// fires either way.
    pub fn create(
        writer: Box<dyn WriteSeek + Send>,
        title: &DiscTitle,
        output_path: Option<&std::path::Path>,
    ) -> io::Result<Self> {
        // Blu-ray 3D (MVC): a dependent (right-eye) view is NOT its own track — it's
        // folded into the base track as per-frame BlockAdditional. Detect it here so
        // we skip building a track for it and set up the merge.
        let dep_stream_idx = title
            .streams
            .iter()
            .position(|s| matches!(s, crate::disc::Stream::Video(v) if v.is_mvc_dependent()));
        // Base is the first NON-dependent video; excluding the dependent means a
        // malformed title whose only video IS the dependent yields `None` (muxed
        // as an ordinary track) instead of `base == dep` and a panic on the skipped slot.
        let base_stream_idx = title
            .streams
            .iter()
            .position(|s| matches!(s, crate::disc::Stream::Video(v) if !v.is_mvc_dependent()));
        // The merge is only active when BOTH a dependent and a distinct base exist;
        // only then is the dependent's track skipped/folded.
        let mvc_active = dep_stream_idx.is_some() && base_stream_idx.is_some();
        let skip_stream_idx = if mvc_active { dep_stream_idx } else { None };

        let mut tracks = Vec::new();
        let mut has_default_video = false;
        let mut has_default_audio = false;
        // `title.streams` index → muxer track index (`None` = the dependent view,
        // which has no track). Streams after the dependent shift down by one.
        let mut stream_to_track: Vec<Option<usize>> = Vec::with_capacity(title.streams.len());
        let excluded = super::ps::UnstoredExtensions::new(title, "Matroska");
        let mut unmapped = false;
        for (idx, s) in title.streams.iter().enumerate() {
            if Some(idx) == skip_stream_idx {
                stream_to_track.push(None);
                continue;
            }
            if excluded.contains(idx) {
                stream_to_track.push(None);
                continue;
            }
            // No Matroska CodecID: left out (fit_report plans it out), never mislabelled.
            let Some(mut track) = MkvTrack::from_stream(s) else {
                tracing::warn!(target: "mux", stream = idx, "codec has no Matroska mapping; left out");
                unmapped = true;
                stream_to_track.push(None);
                continue;
            };
            // Only first video and first audio are default
            if track.is_default {
                match track.track_type {
                    1 if !has_default_video => has_default_video = true,
                    2 if !has_default_audio => has_default_audio = true,
                    _ => track.is_default = false,
                }
            }
            if let Some(cp) = title.codec_privates.get(idx).and_then(|c| c.as_ref()) {
                track.codec_private = Some(cp.clone());
            }
            if matches!(s, crate::disc::Stream::Audio(a) if a.codec == Codec::Lpcm) {
                let cp = title.codec_privates.get(idx).and_then(|c| c.as_deref());
                track.bit_depth = super::codec::lpcm::output_depth(cp);
            }
            stream_to_track.push(Some(tracks.len()));
            tracks.push(track);
        }

        // The MVC merge needs a dependent, a distinct base and a built base track (an unmapped
        // base yields no merge). An unmapped stream forces the remap even under MVC.
        let remap =
            (unmapped || (!mvc_active && !excluded.is_empty())).then(|| stream_to_track.clone());
        let mvc = match (mvc_active, dep_stream_idx, base_stream_idx) {
            (true, Some(dep_stream_idx), Some(base_stream_idx)) => stream_to_track
                .get(base_stream_idx)
                .copied()
                .flatten()
                .map(|base_track_idx| MvcMerge {
                    base_stream_idx,
                    dep_stream_idx,
                    base_track_idx,
                    stream_to_track,
                    pending_base: std::collections::VecDeque::new(),
                    dep_by_pts: std::collections::HashMap::new(),
                    captured_params: None,
                    orphan_deps: 0,
                }),
            _ => None,
        };

        // Defer muxer construction until the first coded picture arrives, so the
        // primary video track's FieldOrder is set from the parser's MEASURED value
        // before the header is written — never a guess.
        let video_track = tracks.iter().position(|t| t.track_type == 1);

        Ok(Self {
            disc_title: title.clone(),
            mvc,
            remap,
            excluded,
            mode: Mode::Write(WriteMode::Pending(Box::new(PendingMux {
                timings: vec![crate::pes::TrackTiming::default(); tracks.len()],
                writer,
                tracks,
                video_track,
                opening_capture_path: output_path.map(|p| p.to_path_buf()),
                buffered: Vec::new(),
                buffered_bytes: 0,
                origin_lead_ns: 0,
                dropped_pre_origin: 0,
            }))),
        })
    }

    // Build the muxer from the pending state, setting the primary video track's
    // `FieldOrder` from the MEASURED `coding` of the first coded picture, then
    // write the header and replay buffered frames. No-op if not pending.
    fn activate(
        &mut self,
        coding: Option<crate::mux::codec::PictureInfo>,
        video_picture_seen: bool,
    ) -> io::Result<()> {
        let result = self.build_muxer(coding, video_picture_seen);
        if result.is_err() {
            self.mode = Mode::Write(WriteMode::Failed);
        }
        result
    }

    fn build_muxer(
        &mut self,
        coding: Option<crate::mux::codec::PictureInfo>,
        video_picture_seen: bool,
    ) -> io::Result<()> {
        let mut pending = match std::mem::replace(&mut self.mode, Mode::Write(WriteMode::Building))
        {
            Mode::Write(WriteMode::Pending(p)) => p,
            // Not pending (already active / read): restore and bail.
            other => {
                self.mode = other;
                return Ok(());
            }
        };
        if let Some(vt) = pending.video_track {
            apply_coding_to_track(&mut pending.tracks[vt], coding, video_picture_seen);
        }
        // Blu-ray 3D: set base track's `mvc_params` from the dependent view's captured
        // subset-SPS/PPS BEFORE the header is written (so TrackEntry carries `mvcC`);
        // captured from the first dependent AU, which arrives right after the base AU.
        if let Some(mvc) = &self.mvc {
            if let Some(params) = &mvc.captured_params {
                if let Some(t) = pending.tracks.get_mut(mvc.base_track_idx) {
                    t.mvc_params = Some(params.clone());
                }
            } else {
                tracing::warn!(
                    target: "mux",
                    "MVC: no dependent-view subset-SPS/PPS captured before activation; \
                     the base track will carry no mvcC mapping (3D not signalled)."
                );
            }
        }
        // --log-level 3: dump the FINAL TrackEntry metadata (field order set).
        for (i, track) in pending.tracks.iter().enumerate() {
            crate::diag::dump_mkv_track((i + 1) as u64, track);
        }
        let mut muxer = MkvMuxer::new_with_timing(
            pending.writer,
            &pending.tracks,
            &pending.timings,
            Some(&self.disc_title.playlist),
            self.disc_title.duration_secs,
            &self.disc_title.chapters,
        )?;
        // Seam correction from the playlist's marks where the title has them.
        muxer.set_clips(&self.disc_title.clips, self.disc_title.content_format);
        muxer.set_origin_lead_ns(pending.origin_lead_ns, pending.dropped_pre_origin);
        if let Some(path) = &pending.opening_capture_path {
            muxer.set_opening_capture(crate::diag::OpeningCapture::new(path, pending.tracks.len()));
        }
        for (f, additional) in pending.buffered.drain(..) {
            // Provenance must survive the replay: these pre-muxer frames still carry
            // the byte offset they were read from — dropping it here would fall back
            // to the timestamp heuristic this change set exists to stop relying on.
            let replayed = muxer.write_frame_at_with_padding(
                f.track,
                f.pts,
                f.keyframe,
                &f.data,
                f.duration_ns,
                additional.as_deref(),
                f.source.map(|s| s.byte),
                f.coding,
                f.discard_padding_ns,
            );
            // A track-range reject writes nothing and is not fatal (see
            // `fail_on_write_error`): drop that buffered frame, keep the file.
            match replayed {
                Err(e) if crate::error::error_code(&e) == Some(crate::error::E_MUX_TRACK_RANGE) => {
                    tracing::warn!(
                        target: "mux",
                        track = f.track,
                        "buffered frame for a track outside the muxer dropped"
                    );
                }
                other => other?,
            }
        }
        self.mode = Mode::Write(WriteMode::Active(Box::new(muxer)));
        Ok(())
    }

    // A frame write that failed mid-cluster leaves the file torn: later writes and
    // finish() must error. A track-range reject writes nothing, so it is not fatal.
    fn fail_on_write_error(&mut self, r: io::Result<()>) -> io::Result<()> {
        if let Err(e) = &r
            && crate::error::error_code(e) != Some(crate::error::E_MUX_TRACK_RANGE)
        {
            self.mode = Mode::Write(WriteMode::Failed);
        }
        r
    }

    // Emit one frame on muxer track `track` (overrides `frame.track`) with an optional
    // MVC dependent-view `BlockAdditional`: the first video frame triggers muxer
    // construction (its coding sets FieldOrder); earlier frames buffer.
    fn emit(
        &mut self,
        track: usize,
        frame: &crate::pes::PesFrame,
        additional: Option<&[u8]>,
    ) -> io::Result<()> {
        match &mut self.mode {
            Mode::Read(_) => return Err(crate::error::Error::StreamReadOnly.into()),
            Mode::Write(WriteMode::Active(m)) => {
                let r = emit_to_muxer(m, track, frame, additional);
                return self.fail_on_write_error(r);
            }
            Mode::Write(WriteMode::Building | WriteMode::Failed) => return Err(muxer_unusable()),
            Mode::Write(WriteMode::Pending(_)) => {}
        }
        // Pending: the first video frame (or the safety cap) triggers muxer
        // construction; that frame's coding sets the field order. Other frames
        // buffer until then.
        let (activate_now, use_coding) = match &self.mode {
            Mode::Write(WriteMode::Pending(p)) => {
                let is_video = match p.video_track {
                    Some(vt) => track == vt,
                    // No video track: nothing to wait for — build on frame one.
                    None => true,
                };
                let capped =
                    p.buffered.len() >= MAX_PENDING_FRAMES || p.buffered_bytes >= MAX_PENDING_BYTES;
                (is_video || capped, is_video)
            }
            _ => unreachable!("guarded above"),
        };
        if activate_now {
            // The first video keyframe must open a cluster BEFORE replaying
            // audio already buffered while its parser assembled the opening GOP.
            // Replaying the audio first made the muxer drop that entire prefix.
            if use_coding && frame.keyframe {
                // Only Blu-ray marks share the PES clock (see `TimelineContinuity::with_clips`).
                let in_ns = match self.disc_title.content_format {
                    crate::disc::ContentFormat::BdTs => self.disc_title.clips.first(),
                    crate::disc::ContentFormat::MpegPs | crate::disc::ContentFormat::DvdPs => None,
                }
                .map(|c| c.in_time as i64 * 1_000_000_000 / 45_000);
                if let Mode::Write(WriteMode::Pending(p)) = &mut self.mode {
                    p.set_origin(frame.pts, in_ns);
                    let mut f = frame.clone();
                    f.track = track;
                    p.buffered.insert(0, (f, additional.map(<[u8]>::to_vec)));
                }
                return self.activate(frame.coding, true);
            }
            // Pass the trigger frame's coding only when it IS the video frame; a
            // cap-triggered build never saw the video frame, so nothing measured
            // is passed (apply_coding_to_track then logs + leaves UNDETERMINED).
            self.activate(if use_coding { frame.coding } else { None }, use_coding)?;
            if let Mode::Write(WriteMode::Active(m)) = &mut self.mode {
                let r = emit_to_muxer(m, track, frame, additional);
                return self.fail_on_write_error(r);
            }
            Ok(())
        } else {
            if let Mode::Write(WriteMode::Pending(p)) = &mut self.mode {
                let add = additional.map(|a| a.to_vec());
                p.buffered_bytes = p
                    .buffered_bytes
                    .saturating_add(frame.data.len() + add.as_ref().map_or(0, |a| a.len()));
                let mut f = frame.clone();
                f.track = track;
                p.buffered.push((f, add));
            }
            Ok(())
        }
    }

    /// Open an MKV file for reading → PES frames.
    pub fn open(mut reader: impl Read + Send + 'static) -> io::Result<Self> {
        let MkvHeader {
            title: disc_title,
            codec_privates,
            ts_scale_ns,
            tracks,
            probe: _,
        } = parse_mkv_header(&mut reader, true)?;
        let mut stream = Self {
            disc_title,
            mvc: None,
            remap: None,
            excluded: Default::default(),
            mode: Mode::Read(Box::new(ReadState {
                reader: Box::new(reader),
                cluster_ts_ticks: 0,
                ts_scale_ns,
                codec_privates,
                tracks,
                pending: std::collections::VecDeque::new(),
                additions_dropped: 0,
                additions_dropped_bytes: 0,
            })),
        };
        stream.resolve_pcm_depths()?;
        Ok(stream)
    }

    // Decide the depth of each PCM track without BitDepth from its first blocks. Read-ahead
    // stops at PCM_PROBE_BYTES, PCM_PROBE_MAX_FRAMES in all, or PCM_PROBE_FRAMES of the track.
    fn resolve_pcm_depths(&mut self) -> io::Result<()> {
        let mut buffered: Vec<crate::pes::PesFrame> = Vec::new();
        // Indices into `buffered` per track, so a probe step never rescans every frame.
        let mut by_track: Vec<Vec<usize>> = Vec::new();
        // Track of the last frame read; `None` = consider every track.
        let mut focus: Option<usize> = None;
        let mut bytes = 0usize;
        loop {
            let (open, lace) = match &self.mode {
                Mode::Read(rs) => (
                    rs.tracks.pcm_infer.iter().any(Option::is_some),
                    rs.pending.iter().map(|f| f.data.len()).sum::<usize>(),
                ),
                Mode::Write(_) => (false, 0),
            };
            if !open {
                break;
            }
            let end = if bytes.saturating_add(lace) >= PCM_PROBE_BYTES
                || buffered.len() >= PCM_PROBE_MAX_FRAMES
            {
                ProbeEnd::Capped
            } else {
                ProbeEnd::Reading
            };
            let only = focus.filter(|_| end == ProbeEnd::Reading);
            let decided = self.try_infer_pcm(&buffered, &by_track, end, only);
            if !decided.is_empty() {
                self.apply_pcm_decisions(&decided, &mut buffered);
                continue;
            }
            match self.read_parsed()? {
                Some(f) => {
                    bytes = bytes.saturating_add(f.data.len());
                    if by_track.len() <= f.track {
                        by_track.resize_with(f.track + 1, Vec::new);
                    }
                    by_track[f.track].push(buffered.len());
                    focus = Some(f.track);
                    buffered.push(f);
                }
                None => {
                    let decided = self.try_infer_pcm(&buffered, &by_track, ProbeEnd::Eof, None);
                    self.apply_pcm_decisions(&decided, &mut buffered);
                }
            }
        }
        if let Mode::Read(rs) = &mut self.mode {
            for f in buffered.into_iter().rev() {
                rs.pending.push_front(f);
            }
        }
        Ok(())
    }

    // `(track, Some(depth) | None=undecidable)` for tracks decidable from `frames` (only
    // track `only` when set). Past the budget, an ambiguous span takes the common 16-bit default.
    fn try_infer_pcm(
        &self,
        frames: &[crate::pes::PesFrame],
        by_track: &[Vec<usize>],
        end: ProbeEnd,
        only: Option<usize>,
    ) -> Vec<(usize, Option<u64>)> {
        let Mode::Read(rs) = &self.mode else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for (idx, info) in rs.tracks.pcm_infer.iter().enumerate() {
            let Some(info) = info else { continue };
            if only.is_some_and(|t| t != idx) {
                continue;
            }
            let mine: Vec<_> = by_track
                .get(idx)
                .map_or(&[][..], Vec::as_slice)
                .iter()
                .filter_map(|&i| frames.get(i))
                .collect();
            let depth = match pcm_depth_fit(&mine, *info, rs.ts_scale_ns) {
                PcmFit::One(w) => Some(w * 8),
                PcmFit::Neither => None,
                PcmFit::Both | PcmFit::Unmeasured
                    if end == ProbeEnd::Reading && mine.len() < PCM_PROBE_FRAMES =>
                {
                    continue;
                }
                PcmFit::Unmeasured => None,
                PcmFit::Both => {
                    tracing::warn!(
                        target: "mux",
                        code = crate::error::E_MKV_SOURCE_INVALID,
                        track = idx,
                        "mkv read-back: PCM depth ambiguous, assuming 16-bit"
                    );
                    Some(16)
                }
            };
            out.push((idx, depth));
        }
        out
    }

    // Record inferred depths, convert already-read frames, or demote the track.
    fn apply_pcm_decisions(
        &mut self,
        decided: &[(usize, Option<u64>)],
        buffered: &mut [crate::pes::PesFrame],
    ) {
        let Mode::Read(rs) = &mut self.mode else {
            return;
        };
        for &(idx, depth) in decided {
            let Some(Some(info)) = rs.tracks.pcm_infer.get_mut(idx).map(Option::take) else {
                continue;
            };
            match depth.map(|d| PcmIn::for_track(info.le, d)) {
                Some(Ok(layout)) => {
                    if let Some(slot) = rs.tracks.pcm.get_mut(idx) {
                        *slot = layout;
                    }
                    if let Some(layout) = layout {
                        let pending = rs.pending.iter_mut();
                        for f in buffered
                            .iter_mut()
                            .chain(pending)
                            .filter(|f| f.track == idx)
                        {
                            f.data = layout.to_be24(&f.data);
                        }
                    }
                }
                _ => {
                    tracing::warn!(
                        target: "mux",
                        code = crate::error::E_MKV_SOURCE_INVALID,
                        track = idx,
                        "mkv read-back: PCM track has no BitDepth and none can be inferred"
                    );
                    if let Some(crate::disc::Stream::Audio(a)) =
                        self.disc_title.streams.get_mut(idx)
                    {
                        a.codec = Codec::Unknown(0);
                    }
                }
            }
        }
    }
}

// Set a video track's `FieldOrder` from the MEASURED coding of the first coded picture — the
// parser's value, never a guess.
fn apply_coding_to_track(
    track: &mut MkvTrack,
    coding: Option<crate::mux::codec::PictureInfo>,
    video_picture_seen: bool,
) {
    // HDR10 static metadata measured from the bitstream (HEVC SEI), applied for any
    // track type once the mastering-display SEI was seen. `None` (SDR/no-SEI) leaves
    // the track's `hdr10` untouched -> omitted.
    if let Some(h) = coding.and_then(|c| c.hdr10()) {
        track.hdr10 = Some(h);
    }
    if !track.interlaced {
        return;
    }
    use crate::mux::codec::FieldOrder;
    match coding.and_then(|c| c.field_order()) {
        Some(FieldOrder::Tff) => track.field_order = ebml::FIELD_ORDER_TFF,
        Some(FieldOrder::Bff) => track.field_order = ebml::FIELD_ORDER_BFF,
        // Measured progressive: no field order (UNDETERMINED, not a guess), and
        // the track is not interlaced — the DECLARED 480i/576i from the IFO is
        // superseded by the coded picture, exactly as TFF/BFF override it above.
        Some(FieldOrder::Progressive) => {
            track.field_order = ebml::FIELD_ORDER_UNDETERMINED;
            tracing::debug!(
                target: "mux",
                "video track declared interlaced by the source scan but the first \
                 coded picture measures PROGRESSIVE; writing FlagInterlaced=progressive"
            );
            track.interlaced = false;
        }
        None if video_picture_seen => {
            tracing::warn!(
                target: "mux",
                "interlaced video track had a video picture but NO usable field order \
                 (coding_present={}); writing FieldOrder=UNDETERMINED — NOT a guess. \
                 Debug why the source/parser did not set top_field_first.",
                coding.is_some(),
            );
            track.field_order = ebml::FIELD_ORDER_UNDETERMINED;
        }
        None => {
            // No video picture was ever measured (empty title finalized with no
            // frames, or a cap-triggered build before the first video frame).
            // Coding is legitimately absent, not a parser defect — log quietly.
            tracing::debug!(
                target: "mux",
                "interlaced video track activated with no video picture \
                 (empty/buffered-only title); writing FieldOrder=UNDETERMINED.",
            );
            track.field_order = ebml::FIELD_ORDER_UNDETERMINED;
        }
    }
}

impl MkvStream {
    // One parsed frame (PCM samples already converted where the layout is known).
    fn read_parsed(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        let rs = match self.mode {
            Mode::Read(ref mut rs) => rs,
            Mode::Write(_) => return Err(crate::error::Error::StreamWriteOnly.into()),
        };

        // Frames still owed from the last LACED Block come out before any new
        // element is read, so a lace is never truncated by the next Block.
        if let Some(frame) = rs.pending.pop_front() {
            return Ok(Some(frame));
        }

        loop {
            let (id, size, _) = match ebml::read_element_header(&mut rs.reader) {
                Ok(h) => h,
                // Only a genuine premature/clean EOF ends the stream; any other error
                // must propagate, or a mid-mux I/O failure (disc/sector/network)
                // would silently truncate the output with no error signal.
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e),
            };

            match id {
                ebml::CLUSTER => continue,
                ebml::CLUSTER_TIMESTAMP => {
                    let raw = read_uint_bounded(&mut rs.reader, size)?;
                    // Untrusted u64: a value above i64::MAX would cast to a large
                    // negative i64 and poison every block PTS in the cluster; reject
                    // it, mirroring the EBML-size guard in parse_mkv_header.
                    if raw > i64::MAX as u64 {
                        return Err(crate::error::Error::MkvSourceInvalid.into());
                    }
                    rs.cluster_ts_ticks = raw as i64;
                    continue;
                }
                ebml::SIMPLE_BLOCK => {
                    let block =
                        ebml::read_binary_val(&mut rs.reader, checked_size(size, MAX_BLOCK_SIZE)?)?;
                    if rs.tracks.is_undecodable(&block) || block_is_malformed(&block) {
                        rs.count_lost(size);
                        continue;
                    }
                    let (frames, lost) = parse_block_counted(
                        &block,
                        rs.cluster_ts_ticks,
                        rs.ts_scale_ns,
                        &rs.tracks,
                        None,
                    )?;
                    rs.count_undecoded(&lost);
                    rs.pending.extend(frames);
                    if let Some(frame) = rs.pending.pop_front() {
                        return Ok(Some(frame));
                    }
                    continue;
                }
                ebml::BLOCK_GROUP => {
                    // MkvMuxer emits a BlockGroup (BLOCK + BLOCK_DURATION) for every
                    // frame with a duration (AC3 audio, PGS subtitles); descend and
                    // read both children so a round-trip doesn't silently drop them.
                    if size == u64::MAX {
                        return Err(crate::error::Error::MkvSourceInvalid.into());
                    }
                    let mut remaining = size;
                    let mut block: Option<Vec<u8>> = None;
                    let mut duration_ms: Option<u64> = None;
                    // Keyframe-ness of a BlockGroup is carried ONLY by ReferenceBlock's
                    // presence — SimpleBlock's 0x80 bit is reserved (always 0) here,
                    // so reading it broke every MPEG-2 frame (always this path).
                    let mut has_reference = false;
                    let mut discard_padding_ns = 0i64;
                    // BlockAdditions (count, bytes), tallied only if the Block itself is kept,
                    // so a dropped group is counted once.
                    let mut additions = (0u64, 0u64);
                    while remaining > 0 {
                        let (cid, cs, hlen) = ebml::read_element_header(&mut rs.reader)?;
                        if cs == u64::MAX {
                            return Err(crate::error::Error::MkvSourceInvalid.into());
                        }
                        // A child whose header+body exceeds bytes left in the group
                        // is malformed — reject rather than saturating `remaining` to 0.
                        let consumed = (hlen as u64).saturating_add(cs);
                        if consumed > remaining {
                            return Err(crate::error::Error::MkvSourceInvalid.into());
                        }
                        remaining -= consumed;
                        match cid {
                            ebml::BLOCK => {
                                block = Some(ebml::read_binary_val(
                                    &mut rs.reader,
                                    checked_size(cs, MAX_BLOCK_SIZE)?,
                                )?);
                            }
                            ebml::DISCARD_PADDING => {
                                // Zero length is a valid EBML signed integer of value 0.
                                if cs > 8 {
                                    return Err(crate::error::Error::MkvSourceInvalid.into());
                                }
                                if cs > 0 {
                                    let raw = read_uint_bounded(&mut rs.reader, cs)?;
                                    let shift = 64 - cs * 8;
                                    discard_padding_ns = ((raw << shift) as i64) >> shift;
                                }
                            }
                            ebml::BLOCK_DURATION => {
                                duration_ms = Some(read_uint_bounded(&mut rs.reader, cs)?);
                            }
                            ebml::REFERENCE_BLOCK => {
                                // Presence alone is the signal — this Block
                                // references another, so it is not a keyframe.
                                // The offset value itself is not needed here.
                                has_reference = true;
                                skip_bytes(&mut rs.reader, cs)?;
                            }
                            ebml::BLOCK_ADDITIONS => {
                                additions.0 += 1;
                                additions.1 = additions.1.saturating_add(cs);
                                skip_bytes(&mut rs.reader, cs)?;
                            }
                            _ => skip_bytes(&mut rs.reader, cs)?,
                        }
                    }
                    if block.is_none() {
                        // Block is mandatory (RFC 9559); count the unit as lost, never silent.
                        if rs.additions_dropped == 0 {
                            tracing::warn!(
                                target: "mux",
                                bytes = size,
                                "mkv read-back: dropping a BlockGroup with no Block; \
                                 counted in lost_bytes/errors."
                            );
                        }
                        rs.additions_dropped = rs.additions_dropped.saturating_add(1);
                        rs.additions_dropped_bytes =
                            rs.additions_dropped_bytes.saturating_add(size);
                    }
                    if block
                        .as_ref()
                        .is_some_and(|b| rs.tracks.is_undecodable(b) || block_is_malformed(b))
                    {
                        rs.count_lost(size);
                        continue;
                    }
                    if block.is_some() && additions.0 > 0 {
                        // Carries the MVC dependent-view AU for 3D titles; `PesFrame` has no
                        // side-payload field, so a 3D re-mux becomes 2D. Never silent: counted.
                        if rs.additions_dropped == 0 {
                            tracing::warn!(
                                target: "mux",
                                bytes = additions.1,
                                "mkv read-back: dropping a BlockAdditions payload this \
                                 reader cannot carry (a Blu-ray 3D MVC dependent view is \
                                 the expected case); the output will be base-view only. \
                                 Counted in lost_bytes/errors."
                            );
                        }
                        rs.additions_dropped = rs.additions_dropped.saturating_add(additions.0);
                        rs.additions_dropped_bytes =
                            rs.additions_dropped_bytes.saturating_add(additions.1);
                    }
                    if let Some(block) = block {
                        // BLOCK_DURATION is TimestampScale ticks, not ms — scale by
                        // ts_scale_ns (1_000_000 for our own 1ms scale, non-default
                        // in foreign MKVs), same scaling PTS uses.
                        let dur_ns =
                            duration_ms.map(|ticks| ticks.saturating_mul(rs.ts_scale_ns as u64));
                        let (frames, lost) = parse_block_counted(
                            &block,
                            rs.cluster_ts_ticks,
                            rs.ts_scale_ns,
                            &rs.tracks,
                            dur_ns,
                        )?;
                        rs.count_undecoded(&lost);
                        // Override the flag-bit guess from `parse_block`
                        // (meaningful for SimpleBlock only) with the
                        // BlockGroup's authoritative signal.
                        let last = frames.len().saturating_sub(1);
                        rs.pending
                            .extend(frames.into_iter().enumerate().map(|(i, mut f)| {
                                if (discard_padding_ns > 0 && i == last)
                                    || (discard_padding_ns < 0 && i == 0)
                                {
                                    f.discard_padding_ns = discard_padding_ns;
                                }
                                f.keyframe = !has_reference;
                                f
                            }));
                        if let Some(frame) = rs.pending.pop_front() {
                            return Ok(Some(frame));
                        }
                    }
                    continue;
                }
                _ => {
                    // An unknown-size element here would drain the whole stream
                    // (take(u64::MAX)) and silently drop all later frames;
                    // reject it like the rest of the parser.
                    if size == u64::MAX {
                        return Err(crate::error::Error::MkvSourceInvalid.into());
                    }
                    skip_bytes(&mut rs.reader, size)?;
                    continue;
                }
            }
        }
    }
}

impl MkvStream {
    /// The title being read or written (both directions open this type).
    pub fn info(&self) -> &crate::disc::DiscTitle {
        &self.disc_title
    }
}

impl crate::pes::PesSource for MkvStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        self.read_parsed()
    }

    fn info(&self) -> &crate::disc::DiscTitle {
        &self.disc_title
    }

    fn track_timing(&self, track: usize) -> crate::pes::TrackTiming {
        match &self.mode {
            Mode::Read(rs) => rs.tracks.timings.get(track).copied().unwrap_or_default(),
            _ => Default::default(),
        }
    }

    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        if let Mode::Read(ref rs) = self.mode {
            // `track` is a stream index but `codec_privates` is keyed by Matroska
            // TrackNumber (RFC 9559 §5.1.4.1.1 requires only non-zero) — translate
            // through the real map instead of assuming `track + 1`.
            let track_num = rs.tracks.num_of(track)?;
            rs.codec_privates
                .iter()
                .find(|(tn, _)| *tn == track_num)
                .map(|(_, data)| data.clone())
        } else {
            None
        }
    }

    fn headers_ready(&self) -> bool {
        true // MKV has all headers upfront in the EBML header
    }

    // Units dropped on read-back: `BlockAdditions` (e.g. a 3D MVC dependent view), Block-less
    // BlockGroups, malformed blocks and blocks of undecodable tracks. Reported like a disc-read
    // skip: `0` for the write side / sources with none.
    fn errors(&self) -> u64 {
        match self.mode {
            Mode::Read(ref rs) => rs.additions_dropped,
            _ => 0,
        }
    }

    // Cumulative bytes of the units counted by `errors()`; counts the whole
    // skipped subtree (payload + EBML framing), an upper bound on the payload.
    fn lost_bytes(&self) -> u64 {
        match self.mode {
            Mode::Read(ref rs) => rs.additions_dropped_bytes,
            _ => 0,
        }
    }
}

impl crate::pes::PesSink for MkvStream {
    fn write(&mut self, frame: &crate::pes::PesFrame) -> io::Result<()> {
        if matches!(self.mode, Mode::Read(_)) {
            return Err(crate::error::Error::StreamReadOnly.into());
        }
        if self.excluded.drop_frame(frame.track) {
            return Ok(());
        }
        // Non-3D fast path: emit the frame directly, no clone, no buffering.
        let Some(mvc) = self.mvc.as_mut() else {
            return match self.remap.as_ref().map(|r| r.get(frame.track).copied()) {
                // Left out at create (already warned): nothing to write.
                Some(Some(None)) => Ok(()),
                Some(Some(Some(t))) => self.emit(t, frame, None),
                _ => self.emit(frame.track, frame, None),
            };
        };
        // 3D, but not a base/dependent video frame: remap and emit without a clone.
        if let Some(mapped) = mvc.passthrough_track(frame.track) {
            return match mapped {
                Some(t) => self.emit(t, frame, None),
                None => Ok(()),
            };
        }
        // Blu-ray 3D: fold the dependent view into the base as BlockAdditional
        // (paired by PTS) and yield 0+ frames; `ingest` returns owned pairs so the
        // `self.mvc` borrow is released before `emit`.
        let emits = mvc.ingest(frame);
        for (f, additional) in emits {
            self.emit(f.track, &f, additional.as_deref())?;
        }
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        // Blu-ray 3D: flush any base frames still awaiting a dependent (emitted
        // unpaired as plain Blocks) before finalizing.
        if let Some(mvc) = self.mvc.as_mut() {
            let tail = mvc.flush();
            let orphans = mvc.orphan_deps;
            if orphans > 0 {
                tracing::debug!(
                    target: "mux",
                    "MVC: {orphans} dependent-view access units had no matching base frame (dropped)"
                );
            }
            for (f, additional) in tail {
                self.emit(f.track, &f, additional.as_deref())?;
            }
        }
        // A title that produced no frames (or only buffered ones) is still
        // finalized into a valid MKV: activate now with no measured coding.
        if matches!(self.mode, Mode::Write(WriteMode::Pending(_))) {
            // No video picture was ever measured for this title (it produced no
            // frames, or only buffered non-video ones): coding is legitimately
            // absent, not a parser defect — `video_picture_seen=false`.
            self.activate(None, false)?;
        }
        match std::mem::replace(&mut self.mode, Mode::Write(WriteMode::Building)) {
            Mode::Write(WriteMode::Active(m)) => {
                if let Err(e) = m.finish() {
                    self.mode = Mode::Write(WriteMode::Failed);
                    return Err(e);
                }
                Ok(())
            }
            Mode::Write(WriteMode::Failed) => {
                self.mode = Mode::Write(WriteMode::Failed);
                Err(muxer_unusable())
            }
            other => {
                self.mode = other;
                Ok(())
            }
        }
    }

    fn info(&self) -> &crate::disc::DiscTitle {
        &self.disc_title
    }

    fn undelivered_streams(&self) -> Vec<usize> {
        // Tracks Matroska has no mapping for, once their packets arrived.
        self.excluded.seen()
    }

    fn set_track_timing(
        &mut self,
        track: usize,
        timing: crate::pes::TrackTiming,
    ) -> io::Result<()> {
        let p = match &mut self.mode {
            Mode::Write(WriteMode::Pending(p)) => p,
            Mode::Read(_) => return Err(crate::error::Error::StreamReadOnly.into()),
            // The TrackEntry is already written: the timing can no longer be encoded.
            Mode::Write(_) => return Err(crate::error::Error::StreamHeaderWritten.into()),
        };
        let range = || -> io::Error {
            crate::error::Error::MuxTrackRange {
                track,
                tracks: self.disc_title.streams.len(),
            }
            .into()
        };
        let map = self
            .mvc
            .as_ref()
            .map(|m| &m.stream_to_track)
            .or(self.remap.as_ref());
        let mapped = match map {
            Some(m) => match m.get(track) {
                // Folded into the base (MVC) or left out: nothing to set.
                Some(None) => return Ok(()),
                Some(Some(t)) => *t,
                None => return Err(range()),
            },
            None => track,
        };
        *p.timings.get_mut(mapped).ok_or_else(range)? = timing;
        Ok(())
    }

    fn set_codec_private(&mut self, track: usize, data: &[u8]) -> io::Result<bool> {
        let map = self
            .mvc
            .as_ref()
            .map(|m| &m.stream_to_track)
            .or(self.remap.as_ref());
        let track = match map {
            Some(m) => match m.get(track).copied().flatten() {
                Some(t) => t,
                None => return Ok(false),
            },
            None => track,
        };
        match &mut self.mode {
            Mode::Write(WriteMode::Pending(p)) => match p.tracks.get_mut(track) {
                Some(t) if t.codec_private.is_none() => {
                    t.codec_private = Some(data.to_vec());
                    Ok(true)
                }
                _ => Ok(false),
            },
            Mode::Write(WriteMode::Active(m)) => m.set_codec_private(track, data),
            _ => Ok(false),
        }
    }
}

// ── MKV header parsing (read side) ────────────────────────────

// Reads the EBML header, then the Segment's Info + Tracks. With `want_chapters` it runs on to
// the first Cluster to find Chapters (best effort); without, it stops once Info + Tracks are read.
fn parse_mkv_header(r: &mut impl Read, want_chapters: bool) -> io::Result<MkvHeader> {
    let mut title = String::new();
    // EBML `DURATION` is a float expressed in TimestampScale ticks, not
    // milliseconds (Matroska spec). Converted to seconds as ticks * ts_scale_ns / 1e9.
    let mut duration_ticks: Option<f64> = None;
    let (mut muxing_app, mut writing_app, mut title_seen) = (None, None, false);
    let mut probe_tracks: Vec<MkvProbeTrack> = Vec::new();
    let mut ts_scale: u64 = 1_000_000;
    let mut streams: Vec<crate::disc::Stream> = Vec::new();
    let mut codec_privates: Vec<(u16, Vec<u8>)> = Vec::new();
    let mut codec_private_bytes = 0usize;
    let mut tracks = TrackTable::default();

    let (id, size, _) = ebml::read_element_header(r)?;
    if id != ebml::EBML {
        return Err(crate::error::Error::MkvSourceInvalid.into());
    }
    if size > i64::MAX as u64 {
        return Err(crate::error::Error::MkvSourceInvalid.into());
    }
    skip_bytes(r, size)?;

    let (id, _, _) = ebml::read_element_header(r)?;
    if id != ebml::SEGMENT {
        return Err(crate::error::Error::MkvSourceInvalid.into());
    }

    let mut chapters: Vec<Chapter> = Vec::new();
    let (mut got_info, mut got_tracks) = (false, false);

    loop {
        if !want_chapters && got_info && got_tracks {
            break;
        }
        let (id, size, _) = match ebml::read_element_header(r) {
            Ok(h) => h,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        };

        match id {
            ebml::INFO => {
                // An unknown-size (u64::MAX) parent would drain children until
                // an EOF read error instead of a clean MkvSourceInvalid; reject it for
                // parity with the segment loop guard below.
                if size == u64::MAX {
                    return Err(crate::error::Error::MkvSourceInvalid.into());
                }
                let mut remaining = size;
                while remaining > 0 {
                    let (cid, cs, hlen) = ebml::read_element_header(r)?;
                    // An inner child declaring EBML unknown size (cs == u64::MAX)
                    // would overflow `hlen + cs` (debug panic) and is meaningless
                    // for a sized parent — reject it.
                    if cs == u64::MAX {
                        return Err(crate::error::Error::MkvSourceInvalid.into());
                    }
                    // A child whose header+body exceeds bytes left in the parent is
                    // malformed — reject rather than saturating `remaining` to 0
                    // (mirrors the BLOCK_GROUP child-loop guard).
                    let consumed = (hlen as u64).saturating_add(cs);
                    if consumed > remaining {
                        return Err(crate::error::Error::MkvSourceInvalid.into());
                    }
                    remaining -= consumed;
                    match cid {
                        ebml::TIMESTAMP_SCALE => ts_scale = read_uint_bounded(r, cs)?,
                        ebml::DURATION => duration_ticks = Some(read_float_bounded(r, cs)?),
                        ebml::TITLE => {
                            title = read_string_bounded(r, cs)?;
                            title_seen = true;
                        }
                        ebml::MUXING_APP => muxing_app = Some(read_string_bounded(r, cs)?),
                        ebml::WRITING_APP => writing_app = Some(read_string_bounded(r, cs)?),
                        _ => {
                            skip_bytes(r, cs)?;
                        }
                    }
                }
                got_info = true;
            }
            ebml::TRACKS => {
                if size == u64::MAX {
                    return Err(crate::error::Error::MkvSourceInvalid.into());
                }
                let mut remaining = size;
                while remaining > 0 {
                    let (cid, cs, hlen) = ebml::read_element_header(r)?;
                    if cs == u64::MAX {
                        return Err(crate::error::Error::MkvSourceInvalid.into());
                    }
                    // Reject a child that overruns the TRACKS body rather than
                    // saturating `remaining` to 0 (same guard as BLOCK_GROUP).
                    let consumed = (hlen as u64).saturating_add(cs);
                    if consumed > remaining {
                        return Err(crate::error::Error::MkvSourceInvalid.into());
                    }
                    remaining -= consumed;
                    if cid == ebml::TRACK_ENTRY {
                        let t = parse_track(r, cs)?;
                        // TrackNumber is unique (RFC 9559 5.1.4.1.1); the count bounds allocation.
                        if probe_tracks.len() >= MAX_TRACK_ENTRIES
                            || probe_tracks.iter().any(|p| p.number == t.number)
                        {
                            return Err(crate::error::Error::MkvSourceInvalid.into());
                        }
                        probe_tracks.push(t.probe);
                        if let Some(s) = t.stream {
                            // Record the TrackNumber alongside the stream it maps
                            // to, in the SAME order, so block routing never has to
                            // guess that TrackNumbers are 1..=N.
                            streams.push(s);
                            tracks.push(
                                t.number,
                                t.default_duration_ns,
                                t.timing,
                                t.pcm,
                                t.pcm_infer,
                                t.decode,
                            );
                        }
                        if t.undecodable {
                            tracks.undecodable.push(t.number);
                        }
                        if let Some(cp) = t.codec_private {
                            codec_private_bytes = codec_private_bytes.saturating_add(cp.len());
                            if codec_private_bytes > MAX_CODEC_PRIVATE_TOTAL {
                                return Err(crate::error::Error::MkvSourceInvalid.into());
                            }
                            codec_privates.push((t.number, cp));
                        }
                    } else {
                        skip_bytes(r, cs)?;
                    }
                }
                got_tracks = true;
            }
            ebml::CHAPTERS if want_chapters && size != u64::MAX => {
                // Chapters are optional metadata: a bad or truncated element is dropped.
                match read_chapters_buf(r, size) {
                    Ok(buf) => match parse_chapters(&buf) {
                        Ok(c) => chapters = c,
                        Err(e) => {
                            tracing::warn!(target: "mux", error = %e, "ignoring malformed MKV Chapters element")
                        }
                    },
                    Err(e) if is_truncation(&e) => break,
                    Err(e) => return Err(e),
                }
            }
            ebml::CLUSTER => break,
            _ if size != u64::MAX => match skip_bytes(r, size) {
                Ok(()) => {}
                // Truncated Attachments/Tags before the first Cluster: stop scanning.
                Err(e) if want_chapters && is_truncation(&e) => break,
                Err(e) => return Err(e),
            },
            _ => break,
        }
    }

    // Clamp the (untrusted) scale to a positive i64 for the tick→ns multiply on
    // the read path; default to 1 ms if absent or absurd. Duration and the probe use the
    // same clamped scale as the frames; a non-finite or negative Duration is absent.
    let ts_scale_ns = if ts_scale == 0 || ts_scale > i64::MAX as u64 {
        1_000_000
    } else {
        ts_scale as i64
    };
    let duration_secs = duration_ticks
        .map(|t| t * (ts_scale_ns as f64) / 1_000_000_000.0)
        .filter(|s| s.is_finite() && *s >= 0.0);
    let probe = MkvProbe {
        muxing_app,
        writing_app,
        duration_secs,
        title: title_seen.then(|| title.clone()),
        tracks: probe_tracks,
        timestamp_scale: ts_scale_ns as u64,
        last_cue_secs: None,
    };
    let disc_title = DiscTitle {
        selection_evidence: Default::default(),
        playlist: title,
        duration_secs: duration_secs.unwrap_or(0.0),
        streams,
        chapters,
        ..DiscTitle::empty()
    };
    Ok(MkvHeader {
        title: disc_title,
        codec_privates,
        ts_scale_ns,
        tracks,
        probe,
    })
}

/// Most chapter marks kept from an untrusted Chapters element.
const MAX_CHAPTERS: usize = 4096;
/// Largest Chapters body that is parsed; a bigger one is skipped.
const MAX_CHAPTERS_BYTES: u64 = 4 * 1024 * 1024;

// A short read / bad element: the data is truncated or malformed, not an I/O failure.
fn is_truncation(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::UnexpectedEof | io::ErrorKind::InvalidData
    )
}

// Read a Chapters body of `size` bytes into memory; over the cap it is skipped (empty).
fn read_chapters_buf(r: &mut impl Read, size: u64) -> io::Result<Vec<u8>> {
    if size > MAX_CHAPTERS_BYTES {
        skip_bytes(r, size)?;
        return Ok(Vec::new());
    }
    let mut buf = Vec::new();
    r.take(size).read_to_end(&mut buf)?;
    if buf.len() as u64 != size {
        return Err(crate::error::Error::MkvSourceInvalid.into());
    }
    Ok(buf)
}

/// Read the default EditionEntry's (else the first) top-level ChapterAtoms
/// (start time + first ChapString, else a 1-based index) from a Chapters
/// body, skipping hidden and disabled atoms.
fn parse_chapters(body: &[u8]) -> io::Result<Vec<Chapter>> {
    let mut editions: Vec<(bool, Vec<Chapter>)> = Vec::new();
    for_each_child(&mut &body[..], body.len() as u64, |r, cid, cs| {
        if cid != ebml::EDITION_ENTRY {
            return skip_bytes(r, cs);
        }
        let (mut is_default, mut out) = (false, Vec::<Chapter>::new());
        for_each_child(r, cs, |r, cid, cs| match cid {
            ebml::EDITION_FLAG_DEFAULT => {
                is_default = read_uint_bounded(r, cs)? != 0;
                Ok(())
            }
            ebml::CHAPTER_ATOM if out.len() < MAX_CHAPTERS => {
                let (mut start_ns, mut name) = (0u64, None);
                let (mut hidden, mut enabled) = (false, true);
                for_each_child(r, cs, |r, cid, cs| match cid {
                    ebml::CHAPTER_TIME_START => {
                        start_ns = read_uint_bounded(r, cs)?;
                        Ok(())
                    }
                    ebml::CHAPTER_FLAG_HIDDEN => {
                        hidden = read_uint_bounded(r, cs)? != 0;
                        Ok(())
                    }
                    ebml::CHAPTER_FLAG_ENABLED => {
                        enabled = read_uint_bounded(r, cs)? != 0;
                        Ok(())
                    }
                    ebml::CHAPTER_DISPLAY => for_each_child(r, cs, |r, cid, cs| {
                        if cid == ebml::CHAP_STRING && name.is_none() {
                            name = Some(read_string_bounded(r, cs)?);
                            Ok(())
                        } else {
                            skip_bytes(r, cs)
                        }
                    }),
                    _ => skip_bytes(r, cs),
                })?;
                if !hidden && enabled {
                    out.push(Chapter {
                        time_secs: start_ns as f64 / 1_000_000_000.0,
                        name: name.unwrap_or_else(|| (out.len() + 1).to_string()),
                    });
                }
                Ok(())
            }
            _ => skip_bytes(r, cs),
        })?;
        editions.push((is_default, out));
        Ok(())
    })?;
    let pick = editions.iter().position(|e| e.0).unwrap_or(0);
    Ok(editions
        .into_iter()
        .nth(pick)
        .map(|e| e.1)
        .unwrap_or_default())
}

/// Largest valid 13-bit MPEG-TS PID.
const MAX_TS_PID: u32 = 0x1FFF;

// Most TrackEntry elements accepted from one (untrusted) Tracks element.
const MAX_TRACK_ENTRIES: usize = 512;

// TrackEntry LanguageBCP47 (RFC 9559 5.1.4.1.21); when present, Language is ignored.
const LANGUAGE_BCP47: u32 = 0x22_B59D;

// Map an MKV track number to a synthetic BD-TS PID, rejecting overflow of the
// 13-bit PID space. Track 1 -> video PID (0x1011); others -> 0x1100+(tnum-2),
// computed in `u32` so the addition can never wrap.
fn ts_pid_for_track(tnum: u16) -> io::Result<u16> {
    // MKV track numbers are 1-based; 0 is invalid (and would underflow the
    // `tnum - 2` below).
    if tnum == 0 {
        return Err(crate::error::Error::MkvSourceInvalid.into());
    }
    let pid: u32 = if tnum == 1 {
        0x1011
    } else {
        0x1100u32 + (tnum as u32 - 2)
    };
    if pid > MAX_TS_PID {
        return Err(crate::error::Error::MkvSourceInvalid.into());
    }
    Ok(pid as u16)
}

// One decoded TrackEntry. `stream` is `None` for a TrackType this crate does not carry, in
// which case the TrackNumber gets no stream index at all.
struct ParsedTrack {
    stream: Option<crate::disc::Stream>,
    number: u16,
    codec_private: Option<Vec<u8>>,
    default_duration_ns: Option<u64>,
    timing: crate::pes::TrackTiming,
    pcm: Option<PcmIn>,
    pcm_infer: Option<PcmInfer>,
    probe: MkvProbeTrack,
    // ContentEncodings to undo on each frame.
    decode: Vec<Decode>,
    // Encrypted / unsupported compression: carried as no stream, its blocks counted.
    undecodable: bool,
}

// A PCM track that declares no BitDepth: the depth is inferred from the first
// blocks' byte counts over their duration (see `MkvStream::resolve_pcm_depths`).
#[derive(Clone, Copy, Debug)]
struct PcmInfer {
    le: bool,
    rate: f64,
    channels: u8,
}

// Frames of one PCM track a depth probe may buffer before settling on a default.
const PCM_PROBE_FRAMES: usize = 512;
// Frames of all tracks a depth probe may buffer (empty blocks cost no payload bytes).
const PCM_PROBE_MAX_FRAMES: usize = 16 * PCM_PROBE_FRAMES;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProbeEnd {
    Reading,
    Capped,
    Eof,
}

#[derive(Debug, PartialEq, Eq)]
enum PcmFit {
    One(u64),
    Both,
    // Both fit only because no span could be measured: no evidence.
    Unmeasured,
    Neither,
}

// Which of 2/3 bytes per sample fits every block's size and the bytes over the
// measured span, allowing two timestamp ticks of quantisation error.
fn pcm_depth_fit(frames: &[&crate::pes::PesFrame], info: PcmInfer, tick_ns: i64) -> PcmFit {
    let ch = u64::from(info.channels.max(1));
    let mut fits = [2u64, 3].map(|w| {
        frames
            .iter()
            .all(|f| (f.data.len() as u64).is_multiple_of(ch * w))
    });
    let err = tick_ns.max(1).saturating_mul(2);
    let span = pcm_span(frames);
    if let Some((bytes, span_ns)) = span {
        let per_ns = info.rate * ch as f64 / 1e9;
        for (fit, w) in fits.iter_mut().zip([2.0f64, 3.0]) {
            let lo = span_ns.saturating_sub(err).max(0) as f64 * per_ns * w * 0.99;
            let hi = span_ns.saturating_add(err) as f64 * per_ns * w * 1.01;
            *fit &= (lo..=hi).contains(&(bytes as f64));
        }
    }
    match fits {
        [true, false] => PcmFit::One(2),
        [false, true] => PcmFit::One(3),
        [true, true] if span.is_some_and(|(_, ns)| ns > err) => PcmFit::Both,
        [true, true] => PcmFit::Unmeasured,
        [false, false] => PcmFit::Neither,
    }
}

// `(bytes, ns)` of the longest run of frames whose end time is known: a later
// frame's timestamp, or the last frame's timestamp plus its duration.
fn pcm_span(frames: &[&crate::pes::PesFrame]) -> Option<(u64, i64)> {
    let first = frames.first()?.pts;
    let mut best = None;
    let mut bytes = 0u64;
    for (i, f) in frames.iter().enumerate() {
        if i > 0 && f.pts > frames[i - 1].pts {
            best = Some((bytes, f.pts.saturating_sub(first)));
        }
        bytes = bytes.saturating_add(f.data.len() as u64);
    }
    let last = frames.last()?;
    if let Some(d) = last.duration_ns.and_then(|d| i64::try_from(d).ok()) {
        best = Some((bytes, last.pts.saturating_add(d).saturating_sub(first)));
    }
    best.filter(|&(_, ns)| ns > 0)
}

// Source layout of a PCM track whose samples are rewritten to the 24-bit
// big-endian form every LPCM consumer expects. `None` = already 24-bit BE.
#[derive(Clone, Copy, Debug, PartialEq)]
enum PcmIn {
    Be16,
    Le16,
    Le24,
}

impl PcmIn {
    // Layout for a PCM codec ID + BitDepth: `Ok(None)` = already 24-bit BE,
    // `Err(())` = not representable as LPCM.
    fn for_track(le: bool, bit_depth: u64) -> Result<Option<Self>, ()> {
        match (le, bit_depth) {
            (false, 16) => Ok(Some(PcmIn::Be16)),
            (false, 24) => Ok(None),
            (true, 16) => Ok(Some(PcmIn::Le16)),
            (true, 24) => Ok(Some(PcmIn::Le24)),
            _ => Err(()),
        }
    }

    // Rewrite samples to 24-bit big-endian; a trailing partial sample is dropped.
    fn to_be24(self, data: &[u8]) -> Vec<u8> {
        let width = if self == PcmIn::Le24 { 3 } else { 2 };
        let mut out = Vec::with_capacity(data.len() / width * 3);
        for s in data.chunks_exact(width) {
            match self {
                PcmIn::Be16 => out.extend_from_slice(&[s[0], s[1], 0]),
                PcmIn::Le16 => out.extend_from_slice(&[s[1], s[0], 0]),
                PcmIn::Le24 => out.extend_from_slice(&[s[2], s[1], s[0]]),
            }
        }
        out
    }
}

// Matroska Video>DisplayUnit (RFC 9559 5.1.4.1.28.13); 3 = aspect ratio, 4 = unknown.
const DISPLAY_UNIT: u32 = 0x54B2;
const DISPLAY_UNIT_UNKNOWN: u64 = 4;
// Video>PixelCrop{Bottom,Top,Left,Right} (RFC 9559 5.1.4.1.28.8-11).
const PIXEL_CROP_BOTTOM: u32 = 0x54AA;
const PIXEL_CROP_TOP: u32 = 0x54BB;
const PIXEL_CROP_LEFT: u32 = 0x54CC;
const PIXEL_CROP_RIGHT: u32 = 0x54DD;

// The Video children a remux carries on: pixel and display size, and Colour.
#[derive(Default)]
struct VideoMeta {
    pixel_width: u32,
    pixel_height: u32,
    display_width: Option<u64>,
    display_height: Option<u64>,
    display_unit: u64,
    // PixelCrop (bottom, top, left, right).
    crop: [u64; 4],
    // (matrix, transfer, primaries, range) as declared; `None` = no Colour element.
    colour: Option<[Option<u64>; 4]>,
}

impl VideoMeta {
    // Display shape as a reduced `(w, h)` when it differs from the writer's pixel grid for `res`.
    fn display_aspect(&self, res: Resolution) -> Option<(u32, u32)> {
        // RFC 9559: the Display size defaults to the pixel size minus the PixelCrop edges.
        let [cb, ct, cl, cr] = self.crop;
        let pw = u64::from(self.pixel_width).saturating_sub(cl.saturating_add(cr));
        let ph = u64::from(self.pixel_height).saturating_sub(ct.saturating_add(cb));
        let (dw, dh) = if self.display_unit == DISPLAY_UNIT_UNKNOWN {
            (pw, ph)
        } else {
            (
                self.display_width.unwrap_or(pw),
                self.display_height.unwrap_or(ph),
            )
        };
        let (bw, bh) = res.pixels()?;
        if dw == 0 || dh == 0 || u128::from(dw) * u128::from(bh) == u128::from(bw) * u128::from(dh)
        {
            return None;
        }
        let (mut a, mut b) = (dw, dh);
        while b != 0 {
            (a, b) = (b, a % b);
        }
        Some((u32::try_from(dw / a).ok()?, u32::try_from(dh / a).ok()?))
    }

    // Declared CICP; absent members take their Matroska defaults (2 = unspecified, range 0).
    fn cicp(&self) -> Option<MeasuredCicp> {
        let [m, t, p, r] = self.colour?;
        let code =
            |v: Option<u64>, default: u8| v.map_or(default, |v| u8::try_from(v).unwrap_or(default));
        Some(MeasuredCicp {
            matrix: code(m, 2),
            transfer: code(t, 2),
            primaries: code(p, 2),
            range: code(r, 0),
        })
    }
}

// Parse a TrackEntry's Video body.
fn parse_video(r: &mut impl Read, size: u64) -> io::Result<VideoMeta> {
    let mut v = VideoMeta::default();
    // Untrusted u64: saturate rather than wrap onto a real size.
    let dim = |x: u64| u32::try_from(x).unwrap_or(u32::MAX);
    for_each_child(r, size, |r, id, cs| {
        match id {
            ebml::PIXEL_WIDTH => v.pixel_width = dim(read_uint_bounded(r, cs)?),
            ebml::PIXEL_HEIGHT => v.pixel_height = dim(read_uint_bounded(r, cs)?),
            ebml::DISPLAY_WIDTH => v.display_width = Some(read_uint_bounded(r, cs)?),
            ebml::DISPLAY_HEIGHT => v.display_height = Some(read_uint_bounded(r, cs)?),
            DISPLAY_UNIT => v.display_unit = read_uint_bounded(r, cs)?,
            PIXEL_CROP_BOTTOM => v.crop[0] = read_uint_bounded(r, cs)?,
            PIXEL_CROP_TOP => v.crop[1] = read_uint_bounded(r, cs)?,
            PIXEL_CROP_LEFT => v.crop[2] = read_uint_bounded(r, cs)?,
            PIXEL_CROP_RIGHT => v.crop[3] = read_uint_bounded(r, cs)?,
            ebml::COLOUR => {
                let mut c = [None; 4];
                for_each_child(r, cs, |r, cid, ccs| {
                    let slot = match cid {
                        ebml::MATRIX_COEFFICIENTS => 0,
                        ebml::TRANSFER_CHARACTERISTICS => 1,
                        ebml::PRIMARIES => 2,
                        ebml::RANGE => 3,
                        _ => return skip_bytes(r, ccs),
                    };
                    c[slot] = Some(read_uint_bounded(r, ccs)?);
                    Ok(())
                })?;
                v.colour = Some(c);
            }
            _ => skip_bytes(r, cs)?,
        }
        Ok(())
    })?;
    Ok(v)
}

// H.273 transfer 16 = PQ (HDR10), 18 = HLG; the container cannot tell HDR10+ or DV apart.
fn hdr_from_transfer(transfer: u8) -> HdrFormat {
    match transfer {
        16 => HdrFormat::Hdr10,
        18 => HdrFormat::Hlg,
        _ => HdrFormat::Sdr,
    }
}

fn color_space_from_primaries(primaries: u8) -> ColorSpace {
    match primaries {
        1 => ColorSpace::Bt709,
        5 => ColorSpace::Bt470bg,
        6 => ColorSpace::Smpte170m,
        9 => ColorSpace::Bt2020,
        _ => ColorSpace::Unknown,
    }
}

// Standard rate whose frame period is within 0.01% of DefaultDuration `ns`.
fn frame_rate_from_ns(ns: u64) -> FrameRate {
    use FrameRate::*;
    [F23_976, F24, F25, F29_97, F30, F50, F59_94, F60]
        .into_iter()
        .find(|r| {
            let (num, den) = r.as_fraction();
            let exact = 1e9 * f64::from(den) / f64::from(num);
            (ns as f64 - exact).abs() <= exact * 1e-4
        })
        .unwrap_or(Unknown)
}

// Matroska ContentEncodings (RFC 9559 5.1.4.1.31).
const CONTENT_ENCODINGS: u32 = 0x6D80;
const CONTENT_ENCODING: u32 = 0x6240;
const CONTENT_ENCODING_ORDER: u32 = 0x5031;
const CONTENT_ENCODING_SCOPE: u32 = 0x5032;
const CONTENT_ENCODING_TYPE: u32 = 0x5033;
const CONTENT_COMPRESSION: u32 = 0x5034;
const CONTENT_COMP_ALGO: u32 = 0x4254;
const CONTENT_COMP_SETTINGS: u32 = 0x4255;
const CONTENT_ENCRYPTION: u32 = 0x5035;
// Most ContentEncoding entries accepted on one track.
const MAX_CONTENT_ENCODINGS: usize = 8;

// One reversible content encoding.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Decode {
    // ContentCompAlgo 3 (header stripping): the bytes to put back in front.
    Prefix(Vec<u8>),
    // ContentCompAlgo 0.
    Zlib,
}

// A track's ContentEncodings, each list in decode order (highest ContentEncodingOrder first).
#[derive(Default)]
struct Encodings {
    frames: Vec<Decode>,
    private: Vec<Decode>,
    // Encrypted, or compressed with an algorithm this reader lacks (bzlib, lzo).
    undecodable: bool,
}

fn parse_content_encodings(r: &mut impl Read, size: u64) -> io::Result<Encodings> {
    // (order, scope, step); `None` = undecodable.
    let mut list: Vec<(u64, u64, Option<Decode>)> = Vec::new();
    for_each_child(r, size, |r, id, cs| {
        if id != CONTENT_ENCODING {
            return skip_bytes(r, cs);
        }
        if list.len() >= MAX_CONTENT_ENCODINGS {
            return Err(crate::error::Error::MkvSourceInvalid.into());
        }
        // RFC defaults: order 0, scope 1 (frames), type 0 (compression), algo 0 (zlib).
        let (mut order, mut scope, mut etype, mut algo) = (0, 1, 0, 0);
        let (mut settings, mut encrypted) = (Vec::new(), false);
        for_each_child(r, cs, |r, cid, ccs| {
            match cid {
                CONTENT_ENCODING_ORDER => order = read_uint_bounded(r, ccs)?,
                CONTENT_ENCODING_SCOPE => scope = read_uint_bounded(r, ccs)?,
                CONTENT_ENCODING_TYPE => etype = read_uint_bounded(r, ccs)?,
                CONTENT_COMPRESSION => for_each_child(r, ccs, |r, k, ks| {
                    match k {
                        CONTENT_COMP_ALGO => algo = read_uint_bounded(r, ks)?,
                        CONTENT_COMP_SETTINGS => {
                            settings = ebml::read_binary_val(r, checked_size(ks, MAX_STRING_LEN)?)?
                        }
                        _ => skip_bytes(r, ks)?,
                    }
                    Ok(())
                })?,
                CONTENT_ENCRYPTION => {
                    encrypted = true;
                    skip_bytes(r, ccs)?
                }
                _ => skip_bytes(r, ccs)?,
            }
            Ok(())
        })?;
        let step = match (etype, algo) {
            (0, 0) if !encrypted => Some(Decode::Zlib),
            (0, 3) if !encrypted => Some(Decode::Prefix(std::mem::take(&mut settings))),
            _ => None,
        };
        list.push((order, scope, step));
        Ok(())
    })?;
    list.sort_by_key(|a| std::cmp::Reverse(a.0));
    let mut enc = Encodings::default();
    for (_, scope, step) in list {
        let Some(step) = step else {
            enc.undecodable = true;
            continue;
        };
        if scope & 2 != 0 {
            enc.private.push(step.clone());
        }
        if scope & 1 != 0 {
            enc.frames.push(step);
        }
    }
    Ok(enc)
}

// `V_MS/VFW/FOURCC` carries any VFW codec; a BITMAPINFOHEADER whose `biCompression`
// names a non-VC-1 FourCC (DivX, XviD, ...) is not ours. A short or absent blob stays VC-1.
fn vfw_fourcc_is_foreign(codec_private: Option<&[u8]>) -> bool {
    match codec_private.and_then(|cp| cp.get(16..20)) {
        Some(fcc) => !matches!(
            fcc.to_ascii_uppercase().as_slice(),
            b"WVC1" | b"WMVA" | b"WMV3"
        ),
        None => false,
    }
}

// Undo `steps` on one payload; the result may not exceed `cap` bytes (zlib bomb guard).
fn decode_content(steps: &[Decode], mut data: Vec<u8>, cap: usize) -> io::Result<Vec<u8>> {
    for step in steps {
        data = match step {
            Decode::Prefix(p) => {
                if p.len().saturating_add(data.len()) > cap {
                    return Err(crate::error::Error::MkvSourceInvalid.into());
                }
                [p.as_slice(), &data].concat()
            }
            Decode::Zlib => inflate_capped(&data, cap)?,
        };
    }
    Ok(data)
}

// Inflate a zlib stream, refusing (not truncating) output past `cap` bytes.
fn inflate_capped(data: &[u8], cap: usize) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    let limit = u64::try_from(cap).unwrap_or(u64::MAX).saturating_add(1);
    flate2::read::ZlibDecoder::new(data)
        .take(limit)
        .read_to_end(&mut out)
        .map_err(|_| io::Error::from(crate::error::Error::MkvSourceInvalid))?;
    if out.len() > cap {
        return Err(crate::error::Error::MkvSourceInvalid.into());
    }
    Ok(out)
}

// ISO 639-2 code for a BCP 47 tag's primary language subtag ("und" when unmappable).
fn iso639_2_from_bcp47(tag: &str) -> String {
    let primary = tag.split('-').next().unwrap_or_default();
    match primary.len() {
        2 => crate::labels::vocab::iso639_1_to_iso639_2(primary).map(str::to_string),
        3 if primary.bytes().all(|b| b.is_ascii_alphabetic()) => Some(primary.to_ascii_lowercase()),
        _ => None,
    }
    .unwrap_or_else(|| "und".into())
}

// Decode one TrackEntry body.
fn parse_track(r: &mut impl Read, size: u64) -> io::Result<ParsedTrack> {
    let (mut ttype, mut tnum) = (0u64, 0u16);
    /// RFC 9559 §5.1.4.1.13 gives DefaultDuration as nanoseconds per frame with
    /// "range: not 0". A value this large is nonsense for a frame period and
    /// would only skew laced-frame spacing, so treat it as absent.
    const MAX_DEFAULT_DURATION_NS: u64 = 60 * 1_000_000_000;
    let mut default_dur: Option<u64> = None;
    let mut timing = crate::pes::TrackTiming::default();
    // RFC 9559 5.1.4.1.20: Language defaults to "eng".
    let (mut codec_id, mut lang, mut name) = (String::new(), String::from("eng"), String::new());
    let mut bcp47: Option<String> = None;
    let mut video = VideoMeta::default();
    // RFC 9559 5.1.4.1.28.3: Channels defaults to 1.
    let (mut sr, mut ch, mut forced) = (0.0f64, 1u8, false);
    let mut bit_depth = 0u64;
    let mut codec_priv: Option<Vec<u8>> = None;
    let mut enc = Encodings::default();

    let mut remaining = size;
    while remaining > 0 {
        let (cid, cs, hlen) = ebml::read_element_header(r)?;
        if cs == u64::MAX {
            return Err(crate::error::Error::MkvSourceInvalid.into());
        }
        // Reject a child that overruns the TrackEntry body rather than
        // saturating `remaining` to 0 (same guard as BLOCK_GROUP).
        let consumed = (hlen as u64).saturating_add(cs);
        if consumed > remaining {
            return Err(crate::error::Error::MkvSourceInvalid.into());
        }
        remaining -= consumed;
        match cid {
            ebml::TRACK_NUMBER => {
                // Reject a TRACK_NUMBER above u16::MAX rather than truncating
                // with `as u16` (which would alias 65536→0, 65537→1, … onto
                // existing small track numbers and corrupt PID/codec lookup).
                let n = read_uint_bounded(r, cs)?;
                if n > u16::MAX as u64 {
                    return Err(crate::error::Error::MkvSourceInvalid.into());
                }
                tnum = n as u16;
            }
            ebml::TRACK_TYPE => ttype = read_uint_bounded(r, cs)?,
            ebml::DEFAULT_DURATION => {
                let ns = read_uint_bounded(r, cs)?;
                default_dur = (ns > 0 && ns <= MAX_DEFAULT_DURATION_NS).then_some(ns);
            }
            ebml::CODEC_DELAY => timing.codec_delay_ns = read_uint_bounded(r, cs)?,
            ebml::SEEK_PRE_ROLL => timing.seek_preroll_ns = read_uint_bounded(r, cs)?,
            ebml::CODEC_ID => codec_id = read_string_bounded(r, cs)?,
            ebml::CODEC_PRIVATE => {
                codec_priv = Some(ebml::read_binary_val(
                    r,
                    checked_size(cs, MAX_CODEC_PRIVATE)?,
                )?)
            }
            ebml::LANGUAGE => lang = read_string_bounded(r, cs)?,
            LANGUAGE_BCP47 => bcp47 = Some(read_string_bounded(r, cs)?),
            ebml::TRACK_NAME => name = read_string_bounded(r, cs)?,
            ebml::FLAG_FORCED => forced = read_uint_bounded(r, cs)? != 0,
            ebml::VIDEO => video = parse_video(r, cs)?,
            CONTENT_ENCODINGS => enc = parse_content_encodings(r, cs)?,
            ebml::AUDIO => {
                let mut arem = cs;
                while arem > 0 {
                    let (aid, as_, ahlen) = ebml::read_element_header(r)?;
                    if as_ == u64::MAX {
                        return Err(crate::error::Error::MkvSourceInvalid.into());
                    }
                    // Reject a child overrunning the Audio body (same guard as
                    // BLOCK_GROUP) rather than saturating `arem` to 0.
                    let consumed = (ahlen as u64).saturating_add(as_);
                    if consumed > arem {
                        return Err(crate::error::Error::MkvSourceInvalid.into());
                    }
                    arem -= consumed;
                    match aid {
                        ebml::SAMPLING_FREQUENCY => sr = read_float_bounded(r, as_)?,
                        // Clamp instead of `as u8`: a CHANNELS value that's a multiple of
                        // 256 would truncate to 0 (invalid) on a bare cast; saturate to
                        // u8::MAX so an absurd count degrades to "many", never to 0.
                        ebml::CHANNELS => ch = read_uint_bounded(r, as_)?.min(u8::MAX as u64) as u8,
                        ebml::BIT_DEPTH => bit_depth = read_uint_bounded(r, as_)?,
                        _ => {
                            skip_bytes(r, as_)?;
                        }
                    }
                }
            }
            _ => {
                skip_bytes(r, cs)?;
            }
        }
    }

    if let Some(tag) = bcp47 {
        lang = iso639_2_from_bcp47(&tag);
    }
    if enc.undecodable {
        tracing::warn!(
            target: "mux",
            code = crate::error::E_MKV_SOURCE_INVALID,
            track_number = tnum,
            "mkv read-back: track is encrypted or uses an unsupported compression; \
             dropped, its blocks counted in lost_bytes/errors"
        );
        codec_priv = None;
    }
    // A CodecPrivate that cannot be undone makes the track undecodable, not the file.
    let codec_priv =
        match codec_priv.map(|cp| decode_content(&enc.private, cp, MAX_CODEC_PRIVATE as usize)) {
            Some(Ok(cp)) => Some(cp),
            Some(Err(_)) => {
                tracing::warn!(
                    target: "mux",
                    code = crate::error::E_MKV_SOURCE_INVALID,
                    track_number = tnum,
                    "mkv read-back: compressed CodecPrivate could not be inflated; track dropped"
                );
                enc.undecodable = true;
                None
            }
            None => None,
        };
    // The PCM arms bind layout state, so the ladder is chained `if`s; the CodecIDs
    // stay the `ebml::CODEC_*` consts shared with the muxer. Keep each ID in one arm.
    let cid = codec_id.as_str();
    let (mut pcm, mut infer) = (None, None);
    let is_pcm = cid == ebml::CODEC_PCM_BE || cid == ebml::CODEC_PCM_LE;
    let codec = if is_pcm && bit_depth == 0 {
        infer = Some(PcmInfer {
            le: cid == ebml::CODEC_PCM_LE,
            rate: sr,
            channels: ch,
        });
        Codec::Lpcm
    } else if is_pcm {
        match PcmIn::for_track(cid == ebml::CODEC_PCM_LE, bit_depth) {
            Ok(layout) => {
                pcm = layout;
                Codec::Lpcm
            }
            Err(()) => Codec::Unknown(0),
        }
    } else if cid == ebml::CODEC_HEVC {
        Codec::Hevc
    } else if cid == ebml::CODEC_H264 {
        Codec::H264
    } else if cid == ebml::CODEC_VC1 && !vfw_fourcc_is_foreign(codec_priv.as_deref()) {
        Codec::Vc1
    } else if cid == ebml::CODEC_MPEG2 {
        Codec::Mpeg2
    } else if cid == ebml::CODEC_MPEG1 {
        Codec::Mpeg1
    } else if cid == ebml::CODEC_AV1 {
        Codec::Av1
    } else if cid == ebml::CODEC_AC3 {
        Codec::Ac3
    } else if cid == ebml::CODEC_EAC3 {
        Codec::Ac3Plus
    } else if cid == ebml::CODEC_TRUEHD {
        Codec::TrueHd
    } else if cid == ebml::CODEC_DTS {
        Codec::Dts
    } else if cid == ebml::CODEC_AAC {
        Codec::Aac
    } else if cid == ebml::CODEC_MP2 {
        Codec::Mp2
    } else if cid == ebml::CODEC_MP3 {
        Codec::Mp3
    } else if cid == ebml::CODEC_FLAC {
        Codec::Flac
    } else if cid == ebml::CODEC_OPUS {
        Codec::Opus
    } else if cid == ebml::CODEC_PGS {
        Codec::Pgs
    } else if cid == ebml::CODEC_VOBSUB {
        Codec::DvdSub
    } else {
        Codec::Unknown(0)
    };
    let res = Resolution::from_height(video.pixel_height);
    let chs = AudioChannels::from_count(ch);
    // The standard rate within 0.1% of SamplingFrequency; any other rate is Unknown, never a
    // neighbouring bucket.
    let srs = [44100u32, 48000, 88200, 96000, 176400, 192000]
        .into_iter()
        .find(|&hz| (sr - f64::from(hz)).abs() <= f64::from(hz) * 1e-3)
        .map_or(SampleRate::Unknown, SampleRate::from_hz);

    // Map MKV track numbers to BD-TS PIDs, computed in u32 so `0x1100 + (tnum - 2)`
    // can't wrap u16 (a 13-bit PID tops out at 0x1FFF); out of range is rejected. Only
    // carried track types need one: a dropped type never fails the file.
    let ts_pid = match ttype {
        1 | 2 | 17 => ts_pid_for_track(tnum)?,
        _ => 0,
    };

    let probe = MkvProbeTrack {
        number: tnum,
        kind: MkvTrackKind::from_track_type(ttype),
        codec_id,
        language: lang.clone(),
    };
    let cicp = video.cicp();
    let stream = match ttype {
        1 => {
            let is_secondary = name.contains("Dolby Vision EL") || name.contains("DV EL");
            Some(crate::disc::Stream::Video(VideoStream {
                pid: ts_pid,
                codec,
                resolution: res,
                frame_rate: default_dur.map_or(FrameRate::Unknown, frame_rate_from_ns),
                hdr: cicp.map_or(HdrFormat::Sdr, |c| hdr_from_transfer(c.transfer)),
                color_space: cicp.map_or(ColorSpace::Bt709, |c| {
                    color_space_from_primaries(c.primaries)
                }),
                // The writer sizes pixels from the resolution bucket, so any other
                // shape (scope, anamorphic) travels as the display aspect.
                display_aspect: video.display_aspect(res),
                secondary: is_secondary,
                label: name,
                measured_cicp: cicp,
            }))
        }
        2 => Some(crate::disc::Stream::Audio(AudioStream {
            pid: ts_pid,
            codec,
            channels: chs,
            language: lang,
            sample_rate: srs,
            secondary: false,
            purpose: crate::disc::LabelPurpose::Normal,
            label: name,
        })),
        17 => Some(crate::disc::Stream::Subtitle(SubtitleStream {
            pid: ts_pid,
            codec,
            language: lang,
            forced,
            qualifier: crate::disc::LabelQualifier::None,
            codec_data: None,
        })),
        _ => None,
    };
    let stream = stream.filter(|_| !enc.undecodable);
    Ok(ParsedTrack {
        stream,
        number: tnum,
        codec_private: codec_priv,
        default_duration_ns: default_dur,
        timing,
        pcm,
        pcm_infer: infer,
        probe,
        decode: enc.frames,
        undecodable: enc.undecodable,
    })
}

// Read-side map from Matroska TrackNumber to the index of the corresponding entry in
// `DiscTitle::streams` (TrackNumber is NOT guaranteed `1..=N`).
#[derive(Default)]
struct TrackTable {
    /// Matroska TrackNumber of `DiscTitle::streams[i]`, indexed by `i`.
    nums: Vec<u16>,
    /// TrackEntry `DefaultDuration` (RFC 9559 §5.1.4.1.13 — nanoseconds per
    /// frame, already in Matroska Ticks = ns), per stream index, when declared.
    /// Used to space the frames of a LACED Block, whose second and later frames
    /// carry an "underdetermined" timestamp per RFC 9559 §10.3.5.
    default_durations: Vec<Option<u64>>,
    timings: Vec<crate::pes::TrackTiming>,
    /// PCM tracks whose samples are rewritten to 24-bit big-endian on read.
    pcm: Vec<Option<PcmIn>>,
    /// PCM tracks whose depth is still to be inferred from their blocks.
    pcm_infer: Vec<Option<PcmInfer>>,
    /// ContentEncodings undone on each frame, per stream index.
    decode: Vec<Vec<Decode>>,
    /// TrackNumbers dropped as undecodable; their blocks are counted as lost.
    undecodable: Vec<u16>,
}

impl TrackTable {
    fn push(
        &mut self,
        num: u16,
        default_duration_ns: Option<u64>,
        timing: crate::pes::TrackTiming,
        pcm: Option<PcmIn>,
        infer: Option<PcmInfer>,
        decode: Vec<Decode>,
    ) {
        self.decode.push(decode);
        self.nums.push(num);
        self.timings.push(timing);
        self.default_durations.push(default_duration_ns);
        self.pcm.push(pcm);
        self.pcm_infer.push(infer);
    }

    /// Stream index carrying blocks with this TrackNumber, or `None` when the
    /// file has no such (retained) track.
    fn index_of(&self, num: u64) -> Option<usize> {
        if num == 0 || num > u16::MAX as u64 {
            return None;
        }
        let num = num as u16;
        self.nums.iter().position(|&n| n == num)
    }

    /// Whether a (Simple)Block belongs to a track dropped as undecodable.
    fn is_undecodable(&self, block: &[u8]) -> bool {
        let (num, _) = block_vint(block);
        u16::try_from(num).is_ok_and(|n| n != 0 && self.undecodable.contains(&n))
    }

    /// TrackNumber of a stream index (the inverse of `index_of`).
    fn num_of(&self, idx: usize) -> Option<u16> {
        self.nums.get(idx).copied()
    }

    /// TrackNumbers `1..=n` in stream order — the layout this crate's own writer
    /// emits, and the shape the unit tests exercise.
    #[cfg(test)]
    fn contiguous(n: usize) -> Self {
        Self {
            nums: (1..=n as u16).collect(),
            default_durations: vec![None; n],
            timings: vec![Default::default(); n],
            pcm: vec![None; n],
            pcm_infer: vec![None; n],
            decode: vec![Vec::new(); n],
            undecodable: Vec::new(),
        }
    }
}

/// Lacing mode from the 2-bit LACING field of a (Simple)Block flags byte
/// (RFC 9559 §10.1/§10.2: `KEY | Rsvrd | INV | LACING(2) | DIS`, bit 0 = MSB,
/// so the field is `flags & 0x06` shifted right by 1).
const LACING_MASK: u8 = 0x06;
const LACING_NONE: u8 = 0b00;
const LACING_XIPH: u8 = 0b01;
const LACING_FIXED: u8 = 0b10;
const LACING_EBML: u8 = 0b11;

// Read one unsigned EBML VINT (RFC 8794 §4.4) from `d`'s head, returning
// `(value, octet width)`. `None` on truncation or width > 8 octets. Unlike
// `block_vint` (4-octet track-number decoder), lacing needs the full 1..=8.
fn lace_vint(d: &[u8]) -> Option<(u64, usize)> {
    let first = *d.first()?;
    if first == 0 {
        return None; // width > 8 octets — not representable here
    }
    let width = first.leading_zeros() as usize + 1; // 1..=8
    if d.len() < width {
        return None;
    }
    // Strip the VINT_MARKER bit, then fold in the remaining octets big-endian.
    let mut v = (first as u64) & (0xFFu64 >> width);
    for &b in &d[1..width] {
        v = (v << 8) | b as u64;
    }
    Some((v, width))
}

/// Read one SIGNED EBML lacing VINT. Per RFC 9559 §10.3.3 the signed value is
/// the unsigned VINT value minus `2^((7*n)-1) - 1`, where `n` is the octet width.
fn lace_svint(d: &[u8]) -> Option<(i64, usize)> {
    let (v, width) = lace_vint(d)?;
    // width <= 8 → 7*8-1 = 55, so both the bias and `v` (at most 2^56-1) are
    // exactly representable in i64; no overflow is possible here.
    let bias = (1i64 << (7 * width as u32 - 1)) - 1;
    Some(((v as i64) - bias, width))
}

// Split a LACED (Simple)Block's body into frame payloads per RFC 9559 §10.3. `None` on a
// malformed lacing header — caller MUST reject the block, not treat it as one frame.
pub(crate) fn split_lacing(lacing: u8, body: &[u8]) -> Option<Vec<&[u8]>> {
    let (&count_minus_one, rest) = body.split_first()?;
    let n = count_minus_one as usize + 1;

    // Sizes of the first n-1 frames; the last frame's size is deduced from what
    // remains in the Block (RFC 9559 §10.3.2/§10.3.3).
    let mut sizes: Vec<usize> = Vec::with_capacity(n);
    let mut pos = 0usize;
    match lacing {
        LACING_FIXED => {
            // §10.3.4: no sizes are stored; every frame MUST have the same size,
            // deduced from the Block's total size. A body that does not divide
            // evenly is malformed.
            if rest.len() % n != 0 {
                return None;
            }
            let each = rest.len() / n;
            // `each == 0` is malformed, not "n empty frames" — 0 % n == 0 passes the
            // divisibility check above, and `chunks` on an empty slice yields nothing,
            // so unrejected this silently dropped the whole lace with no error.
            if each == 0 {
                return None;
            }
            return Some(rest.chunks(each).take(n).collect());
        }
        LACING_XIPH => {
            // §10.3.2: each size is a run of 0xFF octets (255 each) terminated
            // by an octet below 255 (which may itself be 0).
            for _ in 0..n - 1 {
                let mut sz = 0usize;
                loop {
                    let b = *rest.get(pos)?;
                    pos += 1;
                    sz = sz.checked_add(b as usize)?;
                    if b != 0xFF {
                        break;
                    }
                }
                sizes.push(sz);
            }
        }
        LACING_EBML => {
            // §10.3.3: the first size is an unsigned VINT; each later size is a
            // SIGNED VINT holding the difference from the previous size.
            if n >= 2 {
                let (first, w) = lace_vint(rest.get(pos..)?)?;
                pos += w;
                let mut prev = i64::try_from(first).ok()?;
                sizes.push(usize::try_from(prev).ok()?);
                for _ in 0..n - 2 {
                    let (delta, w) = lace_svint(rest.get(pos..)?)?;
                    pos += w;
                    prev = prev.checked_add(delta)?;
                    sizes.push(usize::try_from(prev).ok()?);
                }
            }
        }
        _ => return None,
    }

    // Carve the frames out of the bytes after the size table. The declared sizes
    // must fit inside what remains, with the remainder going to the last frame.
    let payload = rest.get(pos..)?;
    let declared: usize = sizes.iter().try_fold(0usize, |a, &s| a.checked_add(s))?;
    let last = payload.len().checked_sub(declared)?;
    sizes.push(last);

    let mut out = Vec::with_capacity(n);
    let mut at = 0usize;
    for sz in sizes {
        let end = at.checked_add(sz)?;
        out.push(payload.get(at..end)?);
        at = end;
    }
    Some(out)
}

// Parse a (Simple)Block payload into zero or more PesFrames: zero means SKIPPED (too
// short/track 0/undeclared TrackNumber), >1 means LACED, `Err` means a malformed lacing header.
#[cfg(test)]
fn parse_block(
    block: &[u8],
    cluster_ts_ticks: i64,
    ts_scale_ns: i64,
    tracks: &TrackTable,
    duration_ns: Option<u64>,
) -> io::Result<Vec<crate::pes::PesFrame>> {
    parse_block_counted(block, cluster_ts_ticks, ts_scale_ns, tracks, duration_ns).map(|(f, _)| f)
}

// `parse_block` plus the encoded sizes of frames whose ContentEncoding could not be undone
// (corrupt zlib, past the size cap): those frames are dropped, never passed on raw.
fn parse_block_counted(
    block: &[u8],
    cluster_ts_ticks: i64,
    ts_scale_ns: i64,
    tracks: &TrackTable,
    duration_ns: Option<u64>,
) -> io::Result<(Vec<crate::pes::PesFrame>, Vec<u64>)> {
    let frames = parse_block_raw(block, cluster_ts_ticks, ts_scale_ns, tracks, duration_ns)?;
    let mut kept = Vec::with_capacity(frames.len());
    let mut lost = Vec::new();
    // One decoded-size budget for the whole block: a laced block's frames share it, so
    // 256 frames each inflating to the cap cannot hold 256x the cap at once.
    let mut budget = MAX_BLOCK_SIZE as usize;
    for mut f in frames {
        if let Some(steps) = tracks.decode.get(f.track).filter(|s| !s.is_empty()) {
            let len = f.data.len() as u64;
            match decode_content(steps, std::mem::take(&mut f.data), budget) {
                Ok(d) => {
                    budget -= d.len();
                    f.data = d;
                }
                Err(_) => {
                    lost.push(len);
                    continue;
                }
            }
        }
        if let Some(Some(layout)) = tracks.pcm.get(f.track) {
            f.data = layout.to_be24(&f.data);
        }
        kept.push(f);
    }
    Ok((kept, lost))
}

fn parse_block_raw(
    block: &[u8],
    cluster_ts_ticks: i64,
    ts_scale_ns: i64,
    tracks: &TrackTable,
    duration_ns: Option<u64>,
) -> io::Result<Vec<crate::pes::PesFrame>> {
    if block.len() < 4 {
        return Ok(Vec::new());
    }
    let (track, vl) = block_vint(block);
    if vl + 3 > block.len() {
        return Ok(Vec::new());
    }
    // Track 0 is invalid (RFC 9559 §5.1.4.1.1: "range: not 0"). block_vint also
    // returns 0 for an undecodable VINT, so a corrupt/zero-track block
    // must be skipped rather than attributed to the first stream.
    if track == 0 {
        return Ok(Vec::new());
    }

    let rel_ts = i16::from_be_bytes([block[vl], block[vl + 1]]);
    let flags = block[vl + 2];
    let keyframe = flags & 0x80 != 0;
    let body = &block[vl + 3..];
    // saturating_add: a hostile CLUSTER_TIMESTAMP near i64::MAX plus a positive
    // rel_ts would overflow (panic in debug, wrap in release) — done before the
    // saturating_mul below so the sum is fully bounded on adversarial input.
    let pts_ticks = cluster_ts_ticks.saturating_add(rel_ts as i64);
    // saturating_mul: a hostile CLUSTER_TIMESTAMP could push pts_ticks near
    // i64::MAX, where ticks→ns would overflow and panic in debug builds.
    let base_pts = pts_ticks.saturating_mul(ts_scale_ns);

    // Blocks for tracks this file does not declare (or whose TrackType this
    // reader dropped) are skipped. Resolved through the real TrackNumber→index
    // map, NOT `TrackNumber - 1`.
    let Some(track_idx) = tracks.index_of(track) else {
        return Ok(Vec::new());
    };

    let lacing = (flags & LACING_MASK) >> 1;
    if lacing == LACING_NONE {
        return Ok(vec![crate::pes::PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: track_idx,
            pts: base_pts,
            keyframe,
            data: body.to_vec(),
            duration_ns,
        }]);
    }

    let Some(laced) = split_lacing(lacing, body) else {
        tracing::warn!(
            target: "mux",
            track_number = track,
            lacing,
            body_len = body.len(),
            "mkv read-back: malformed lacing header in a (Simple)Block; the frame \
             boundaries are unknowable, so the block is rejected rather than passed \
             downstream as one mangled frame"
        );
        // Its own code, not `MkvSourceInvalid` (generic corruption) or `MkvInvalid`
        // (`error::is_skippable_title_stub` treats that as an empty nav/menu stub,
        // which would drop a real unseparable track while reporting success).
        return Err(crate::error::Error::MkvLacingInvalid.into());
    };

    // RFC 9559 §10.3.5: a Block's timestamp applies to the FIRST laced frame only;
    // later frames are "underdetermined" but contiguous. Recover spacing from the
    // track's DefaultDuration when declared, else BlockDuration divided across the lace.
    let count = laced.len().max(1) as u64;
    let per_frame_ns = tracks
        .default_durations
        .get(track_idx)
        .copied()
        .flatten()
        .or_else(|| duration_ns.map(|d| d / count));
    if per_frame_ns.is_none() && laced.len() > 1 {
        tracing::warn!(
            target: "mux",
            track_number = track,
            frames = laced.len(),
            "mkv read-back: laced Block on a track with neither DefaultDuration nor \
             BlockDuration; the laced frames share one timestamp because the source \
             declares nothing to derive their spacing from (RFC 9559 §10.3.5)"
        );
    }

    let mut out = Vec::with_capacity(laced.len());
    for (i, data) in laced.into_iter().enumerate() {
        let step = per_frame_ns
            .unwrap_or(0)
            .saturating_mul(i as u64)
            .min(i64::MAX as u64) as i64;
        out.push(crate::pes::PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: track_idx,
            pts: base_pts.saturating_add(step),
            keyframe,
            data: data.to_vec(),
            // A laced Block's BlockDuration covers the WHOLE lace, so the
            // per-frame duration is the derived spacing, not the block's.
            duration_ns: per_frame_ns,
        });
    }
    Ok(out)
}

// Block TrackNumber VINT (1..=8 octets); `(0, 1)` for an undecodable one, `(0, 0)` when empty.
fn block_vint(d: &[u8]) -> (u64, usize) {
    if d.is_empty() {
        return (0, 0);
    }
    lace_vint(d).unwrap_or((0, 1))
}

// A (Simple)Block too short for its header or naming TrackNumber 0: never a frame.
fn block_is_malformed(block: &[u8]) -> bool {
    let (track, vl) = block_vint(block);
    block.len() < 4 || track == 0 || vl + 3 > block.len()
}

#[cfg(test)]
#[path = "mkvstream_tests.rs"]
mod tests;

// Read-back of TrackEntry metadata that a remux must carry to its output.
#[cfg(test)]
#[path = "mkvstream_readback_tests.rs"]
mod readback_tests;
