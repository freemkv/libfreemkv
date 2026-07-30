//! MkvStream — Matroska container stream.
//!
//! Read: MKV container → demux EBML → PES frames out.
//! Write: PES frames in → MKV mux → Matroska container.

use super::mkv::{MkvMuxer, MkvTrack};
use super::{WriteSeek, ebml};

/// (title, codec_privates, ts_scale_ns, track_table) — `ts_scale_ns` is the
/// TimestampScale in nanoseconds per tick, threaded into the frame read path;
/// `track_table` maps Matroska TrackNumbers onto `DiscTitle::streams` indices.
type MkvHeaderResult = io::Result<(crate::disc::DiscTitle, Vec<(u16, Vec<u8>)>, i64, TrackTable)>;

/// Skip `n` bytes on a forward-only reader (no Seek required).
fn skip_bytes(r: &mut impl Read, n: u64) -> io::Result<()> {
    io::copy(&mut r.take(n), &mut io::sink())?;
    Ok(())
}

// ── Sanity caps for untrusted EBML element sizes ──────────────
//
// Sizes come straight from the EBML stream (file or network) and are
// otherwise cast to `usize` and used to allocate/read. An adversarial
// or corrupt container can claim a multi-GB element and trigger an OOM
// allocation, or claim an integer element wider than 8 bytes and panic
// the fixed 8-byte reader. Every untrusted size is validated against
// one of these caps before allocation.

/// Largest accepted SIMPLE_BLOCK payload. A block is a small vint track
/// header + 2-byte rel-ts + 1-byte flags + one frame of elementary data.
/// UHD HEVC keyframes run a few MB; 64 MiB is generously above any real
/// single-frame block while still bounding a hostile allocation.
const MAX_BLOCK_SIZE: u64 = 64 * 1024 * 1024;
/// Largest accepted CODEC_PRIVATE payload. hvcC/avcC/setup blobs are a
/// few KB in practice; 16 MiB is far above any legitimate value.
const MAX_CODEC_PRIVATE: u64 = 16 * 1024 * 1024;
/// Largest accepted string element (TITLE/CODEC_ID/LANGUAGE/TRACK_NAME).
const MAX_STRING_LEN: u64 = 64 * 1024;
/// EBML unsigned-int elements are at most 8 bytes wide.
const MAX_UINT_LEN: u64 = 8;

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

/// Safety cap on frames buffered before the first video frame triggers muxer
/// construction. The first video frame normally arrives within the first few
/// frames, so this is only a backstop for a pathological audio-only-prefix
/// stream — past it we build with no measured field order (logged) rather than
/// buffer unbounded.
const MAX_PENDING_FRAMES: usize = 4096;
/// Companion byte cap on the same pending buffer. The frame count alone does not
/// bound memory: frames are arbitrarily large, and a UHD video frame runs to a
/// few hundred KB, so 4096 of them is over a gigabyte. 64 MiB is far more than
/// the handful of frames a real audio-only prefix produces, and finite.
const MAX_PENDING_BYTES: usize = 64 << 20;

enum Mode {
    Write(WriteMode),
    Read(ReadState),
}

/// MKV write state with DEFERRED muxer construction. The track header (which
/// carries `FieldOrder`) is written only once the first coded picture is in
/// hand, so the primary video track's field order is set to the parser's
/// MEASURED value the first time — never a guessed default a later pass would
/// rewrite. The muxer still only ever muxes the track it is *given*; this stream
/// is the adapter that routes the parser's measured field order onto that track
/// before construction.
enum WriteMode {
    /// Header not written yet: buffering frames until the first video frame.
    Pending(Box<PendingMux>),
    /// Header written; muxing live. Boxed (MkvMuxer is large) to keep the enum
    /// small (clippy::large_enum_variant).
    Active(Box<MkvMuxer<Box<dyn WriteSeek + Send>>>),
    /// Sentinel held in `self.mode` while the muxer is being built (across the
    /// Pending → Active swap). It is also the terminal state left behind after
    /// `finish()` swaps the muxer out, and the degraded state left behind if
    /// `activate()` fails partway (the first error still surfaces via `?`). In
    /// that terminal state a subsequent `write()` no-ops (`Ok(())`) and `finish()`
    /// does not re-finalize.
    Building,
}

/// Everything needed to build the muxer, held until the first coded picture
/// lets the primary video track's field order be set from the source.
struct PendingMux {
    writer: Box<dyn WriteSeek + Send>,
    tracks: Vec<MkvTrack>,
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
}

/// Largest number of base frames held awaiting their PTS-matching dependent AU
/// before the oldest is flushed unpaired (a plain Block). The SSIF interleaves
/// base and dependent access units per unit, so a base's dependent normally
/// arrives within one or two frames; this window only bounds memory/latency for
/// a stream where the pairing drifts.
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
    /// Ingest one incoming frame; returns `(frame, additional)` pairs ready to
    /// hand to the muxer, in emit order. Base frames buffer briefly to pair with
    /// their dependent by PTS; the dependent stream produces no frames of its own
    /// (it becomes `BlockAdditional`); all other streams pass straight through
    /// with their track index remapped.
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
                // Bound the orphan map BEFORE inserting: if dependents pile up
                // unpaired (pairing badly drifted), drop the drifted buffer so it
                // stays bounded — but keep THIS just-arrived dependent, whose base
                // frame commonly arrives next. Clearing after the insert would
                // discard it and overcount orphans by one.
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
    frame: &crate::pes::PesFrame,
    additional: Option<&[u8]>,
) -> io::Result<()> {
    m.write_frame(
        frame.track,
        frame.pts,
        frame.keyframe,
        &frame.data,
        frame.duration_ns,
        additional,
    )
}

/// Scan a length-prefixed (4-byte big-endian) H.264 NAL stream for the first
/// subset SPS (NAL type 15) and first PPS (NAL type 8) — the two parameter sets
/// that populate the `mvcC` MVCDecoderConfigurationRecord. Returns
/// `Some((subset_sps, pps))` only when BOTH are found; `None` otherwise (the
/// serializer then emits no mvcC mapping and logs it).
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
        // Blu-ray 3D (MVC): a dependent (right-eye) view stream is NOT emitted as
        // its own track — it is folded into the base track as per-frame
        // BlockAdditional. Detect it so we skip building a track for it and set up
        // the merge. `base_stream_idx` is the first video stream.
        let dep_stream_idx = title
            .streams
            .iter()
            .position(|s| matches!(s, crate::disc::Stream::Video(v) if v.is_mvc_dependent()));
        // The base is the first NON-dependent video. Excluding the dependent here
        // means a (malformed / hand-built) title whose only video IS the dependent
        // yields `base_stream_idx == None` → no merge (the dependent is muxed as an
        // ordinary track) instead of `base == dep` and a panic on the skipped slot.
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
        for (idx, s) in title.streams.iter().enumerate() {
            if Some(idx) == skip_stream_idx {
                stream_to_track.push(None);
                continue;
            }
            let mut track = match s {
                crate::disc::Stream::Video(v) => MkvTrack::video(v),
                crate::disc::Stream::Audio(a) => MkvTrack::audio(a),
                crate::disc::Stream::Subtitle(s) => MkvTrack::subtitle(s),
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
            stream_to_track.push(Some(tracks.len()));
            tracks.push(track);
        }

        // Assemble the MVC merge only when active — i.e. a dependent AND a
        // distinct base video both exist (established above). `base_stream_idx`
        // then always has a built track, so its remap is `Some` (no panic path).
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

        // Defer muxer construction (and the TrackEntry dump) until the first
        // coded picture arrives, so the primary video track's FieldOrder is set
        // from the parser's MEASURED value before the header is written — never
        // a guess. The dump moves to activation so it reflects the final track.
        let video_track = tracks.iter().position(|t| t.track_type == 1);

        Ok(Self {
            disc_title: title.clone(),
            mvc,
            mode: Mode::Write(WriteMode::Pending(Box::new(PendingMux {
                writer,
                tracks,
                video_track,
                opening_capture_path: output_path.map(|p| p.to_path_buf()),
                buffered: Vec::new(),
                buffered_bytes: 0,
            }))),
        })
    }

    /// Build the muxer from the pending state, setting the primary video track's
    /// `FieldOrder` from the MEASURED `coding` of the first coded picture (when
    /// available), then write the header and replay buffered frames. A no-op if
    /// not pending. The muxer only ever muxes the track it is given — this routes
    /// the parser's measured value onto that track first.
    fn activate(
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
        // Blu-ray 3D: set the base video track's `mvc_params` from the dependent
        // view's captured subset-SPS/PPS BEFORE the header is written, so the
        // TrackEntry carries the `mvcC` BlockAdditionMapping. Captured from the
        // first dependent AU (which arrives right after the first base AU in the
        // SSIF), so it is available by the time the first base frame activates.
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
        let mut muxer = MkvMuxer::new(
            pending.writer,
            &pending.tracks,
            Some(&self.disc_title.playlist),
            self.disc_title.duration_secs,
            &self.disc_title.chapters,
        )?;
        if let Some(path) = &pending.opening_capture_path {
            muxer.set_opening_capture(crate::diag::OpeningCapture::new(path, pending.tracks.len()));
        }
        for (f, additional) in pending.buffered.drain(..) {
            muxer.write_frame(
                f.track,
                f.pts,
                f.keyframe,
                &f.data,
                f.duration_ns,
                additional.as_deref(),
            )?;
        }
        self.mode = Mode::Write(WriteMode::Active(Box::new(muxer)));
        Ok(())
    }

    /// Emit one frame (track index already muxer-relative) with an optional MVC
    /// dependent-view `BlockAdditional`, honouring the deferred-activation
    /// machinery: the first video frame triggers muxer construction (its coding
    /// sets FieldOrder); earlier frames buffer. `additional` is `None` for every
    /// non-3D frame and for the 3D base frames that had no paired dependent.
    fn emit(&mut self, frame: &crate::pes::PesFrame, additional: Option<&[u8]>) -> io::Result<()> {
        match &mut self.mode {
            Mode::Read(_) => return Err(crate::error::Error::StreamReadOnly.into()),
            Mode::Write(WriteMode::Active(m)) => {
                return emit_to_muxer(m, frame, additional);
            }
            Mode::Write(WriteMode::Building) => return Ok(()),
            Mode::Write(WriteMode::Pending(_)) => {}
        }
        // Pending: the first video frame (or the safety cap) triggers muxer
        // construction; that frame's coding sets the field order. Other frames
        // buffer until then.
        let (activate_now, use_coding) = match &self.mode {
            Mode::Write(WriteMode::Pending(p)) => {
                let is_video = match p.video_track {
                    Some(vt) => frame.track == vt,
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
            // Pass the trigger frame's coding only when it IS the video frame; a
            // cap-triggered build never saw the video frame, so nothing measured
            // is passed (apply_coding_to_track then logs + leaves UNDETERMINED).
            self.activate(if use_coding { frame.coding } else { None }, use_coding)?;
            if let Mode::Write(WriteMode::Active(m)) = &mut self.mode {
                return emit_to_muxer(m, frame, additional);
            }
            Ok(())
        } else {
            if let Mode::Write(WriteMode::Pending(p)) = &mut self.mode {
                let add = additional.map(|a| a.to_vec());
                p.buffered_bytes = p
                    .buffered_bytes
                    .saturating_add(frame.data.len() + add.as_ref().map_or(0, |a| a.len()));
                p.buffered.push((frame.clone(), add));
            }
            Ok(())
        }
    }

    /// Open an MKV file for reading → PES frames.
    pub fn open(mut reader: impl Read + Send + 'static) -> io::Result<Self> {
        let (disc_title, codec_privates, ts_scale_ns, tracks) = parse_mkv_header(&mut reader)?;
        Ok(Self {
            disc_title,
            mvc: None,
            mode: Mode::Read(ReadState {
                reader: Box::new(reader),
                cluster_ts_ticks: 0,
                ts_scale_ns,
                codec_privates,
                tracks,
                pending: std::collections::VecDeque::new(),
                additions_dropped: 0,
                additions_dropped_bytes: 0,
            }),
        })
    }
}

/// Set a video track's `FieldOrder` from the MEASURED coding of the first coded
/// picture — the parser's value, the first time, never a guess.
///
/// A progressive track — or a progressive picture on an interlaced-flagged track
/// — has no field order (left UNDETERMINED — expected). An INTERLACED track that
/// reaches here WITH a video picture but no measured field order is a
/// parser/source gap (MPEG-2 carries `top_field_first` on every interlaced
/// picture, so it should never be missing): LOG it loudly so the source can be
/// debugged, and leave UNDETERMINED — a muxer never fabricates a source fact.
/// `video_picture_seen == false` (an empty title finalized with no frames, or a
/// cap-triggered build that never saw the video frame) is NOT a defect — the
/// missing coding is expected there, so log it quietly.
fn apply_coding_to_track(
    track: &mut MkvTrack,
    coding: Option<crate::mux::codec::PictureInfo>,
    video_picture_seen: bool,
) {
    // HDR10 static metadata measured from the bitstream (HEVC SEI). Applied for
    // ANY track type that carries it (independent of interlace): the first coded
    // picture's PictureInfo holds it once both HDR10 SEI messages were seen.
    // `None` (SDR / no-SEI) leaves the track's `hdr10` untouched → omitted.
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
        // A progressive picture on an interlaced-flagged track carries no field
        // order. Leave UNDETERMINED (not a guess) — there is no parser gap here.
        Some(FieldOrder::Progressive) => {
            track.field_order = ebml::FIELD_ORDER_UNDETERMINED;
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

impl crate::pes::Stream for MkvStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
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
                // Only a genuine premature/clean EOF ends the stream. Any other
                // error (disc read failure, corrupt sector, network drop) must
                // propagate, or a mid-mux I/O failure would silently truncate
                // the output with no error signal.
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e),
            };

            match id {
                ebml::CLUSTER => continue,
                ebml::CLUSTER_TIMESTAMP => {
                    let raw = read_uint_bounded(&mut rs.reader, size)?;
                    // The cluster timestamp is an untrusted u64; a value above
                    // i64::MAX would cast to a large negative i64 and poison
                    // every block PTS in the cluster. Reject it, mirroring the
                    // EBML-size guard in parse_mkv_header.
                    if raw > i64::MAX as u64 {
                        return Err(crate::error::Error::MkvSourceInvalid.into());
                    }
                    rs.cluster_ts_ticks = raw as i64;
                    continue;
                }
                ebml::SIMPLE_BLOCK => {
                    let block =
                        ebml::read_binary_val(&mut rs.reader, checked_size(size, MAX_BLOCK_SIZE)?)?;
                    let frames = parse_block(
                        &block,
                        rs.cluster_ts_ticks,
                        rs.ts_scale_ns,
                        &rs.tracks,
                        None,
                    )?;
                    rs.pending.extend(frames);
                    if let Some(frame) = rs.pending.pop_front() {
                        return Ok(Some(frame));
                    }
                    continue;
                }
                ebml::BLOCK_GROUP => {
                    // MkvMuxer emits a BlockGroup (BLOCK + BLOCK_DURATION) for
                    // every frame carrying a duration — i.e. all AC3 audio and
                    // PGS subtitle frames. Descend into the group, read the
                    // inner BLOCK (0xA1) and BLOCK_DURATION (0x9B), and yield a
                    // frame so a round-trip through this muxer does not silently
                    // drop those tracks. A non-u64::MAX size bounds the children.
                    if size == u64::MAX {
                        return Err(crate::error::Error::MkvSourceInvalid.into());
                    }
                    let mut remaining = size;
                    let mut block: Option<Vec<u8>> = None;
                    let mut duration_ms: Option<u64> = None;
                    // Keyframe-ness of a BlockGroup is carried ONLY by the
                    // presence/absence of a ReferenceBlock child: inside a
                    // BlockGroup the SimpleBlock 0x80 keyframe bit is reserved
                    // and the writer always emits it as 0. Reading that bit
                    // (as this arm used to) makes EVERY BlockGroup frame look
                    // like a non-keyframe — which silently broke every re-mux
                    // of MPEG-2 video, whose parser stamps a per-frame duration
                    // so all of its frames take the BlockGroup path.
                    let mut has_reference = false;
                    while remaining > 0 {
                        let (cid, cs, hlen) = ebml::read_element_header(&mut rs.reader)?;
                        if cs == u64::MAX {
                            return Err(crate::error::Error::MkvSourceInvalid.into());
                        }
                        // A child whose header + body exceeds the bytes left in
                        // the BlockGroup is malformed — reject it rather than
                        // saturating `remaining` to 0 and reading past the group.
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
                                // A `BlockAdditions > BlockMore > BlockAdditional`
                                // subtree is a per-frame SIDE payload. For a
                                // Blu-ray 3D title written by this crate it is the
                                // MVC dependent-view (right-eye) access unit,
                                // BlockAddID=2, described by the track's `mvcC`
                                // BlockAdditionMapping.
                                //
                                // `PesFrame` has no side-payload field and the
                                // read-side stream table (built by
                                // `parse_mkv_header`, which does not parse
                                // BlockAdditionMapping) has no dependent-view
                                // track to hand it to, so this reader CANNOT
                                // reconstruct it — re-muxing a 3D MKV yields a
                                // base-view-only (2D) file. Reconstruction needs
                                // header-parse plumbing well beyond this arm.
                                //
                                // What it must NOT be is silent: this used to fall
                                // into the `_` skip arm below, so an
                                // `mkv://` → `mkv://` re-mux of a 3D rip dropped
                                // one whole eye with no error, no warning and
                                // `lost_bytes == 0`. Account for it so the loss
                                // reaches `MuxOutcome.lost_bytes` / `errors`.
                                if rs.additions_dropped == 0 {
                                    tracing::warn!(
                                        target: "mux",
                                        bytes = cs,
                                        "mkv read-back: dropping a BlockAdditions payload this \
                                         reader cannot carry (a Blu-ray 3D MVC dependent view is \
                                         the expected case); the output will be base-view only. \
                                         Counted in lost_bytes/errors."
                                    );
                                }
                                rs.additions_dropped = rs.additions_dropped.saturating_add(1);
                                rs.additions_dropped_bytes =
                                    rs.additions_dropped_bytes.saturating_add(cs);
                                skip_bytes(&mut rs.reader, cs)?;
                            }
                            _ => skip_bytes(&mut rs.reader, cs)?,
                        }
                    }
                    if let Some(block) = block {
                        // BLOCK_DURATION is expressed in TimestampScale ticks,
                        // not milliseconds. Scale by the segment's ts_scale_ns
                        // (1_000_000 for freemkv's own 1 ms scale; non-default
                        // in foreign MKVs) — same scaling PTS uses.
                        let dur_ns =
                            duration_ms.map(|ticks| ticks.saturating_mul(rs.ts_scale_ns as u64));
                        let frames = parse_block(
                            &block,
                            rs.cluster_ts_ticks,
                            rs.ts_scale_ns,
                            &rs.tracks,
                            dur_ns,
                        )?;
                        // Override the flag-bit guess from `parse_block`
                        // (meaningful for SimpleBlock only) with the
                        // BlockGroup's authoritative signal.
                        rs.pending.extend(frames.into_iter().map(|mut f| {
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

    fn write(&mut self, frame: &crate::pes::PesFrame) -> io::Result<()> {
        if matches!(self.mode, Mode::Read(_)) {
            return Err(crate::error::Error::StreamReadOnly.into());
        }
        // Non-3D fast path: emit the frame directly, no clone, no buffering.
        if self.mvc.is_none() {
            return self.emit(frame, None);
        }
        // Blu-ray 3D: run the frame through the MVC merge, which remaps track
        // indices, folds the dependent view into the base as BlockAdditional
        // (paired by PTS), and yields 0+ frames ready to emit. `ingest` returns
        // owned pairs so the `self.mvc` borrow is released before `emit`.
        let emits = self.mvc.as_mut().unwrap().ingest(frame);
        for (f, additional) in emits {
            self.emit(&f, additional.as_deref())?;
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
                self.emit(&f, additional.as_deref())?;
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
        if let Mode::Write(WriteMode::Active(m)) =
            std::mem::replace(&mut self.mode, Mode::Write(WriteMode::Building))
        {
            m.finish()?;
        }
        Ok(())
    }

    fn info(&self) -> &crate::disc::DiscTitle {
        &self.disc_title
    }

    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        if let Mode::Read(ref rs) = self.mode {
            // `track` is a stream index; `codec_privates` is keyed by Matroska
            // TrackNumber. Those are NOT the same space (RFC 9559 §5.1.4.1.1
            // requires only a non-zero TrackNumber), so translate through the
            // real map instead of assuming `track + 1`.
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

    /// Count of `BlockAdditions` subtrees dropped on read-back — each one a
    /// per-frame side payload (a Blu-ray 3D MVC dependent-view access unit for a
    /// 3D rip written by this crate) that the PES frame model cannot carry.
    ///
    /// Reported through the same channel as a disc-read skip event because it is
    /// the same kind of fact: input bytes that did not reach the output. A 3D
    /// re-mux losing an eye is a degraded outcome, and a degraded outcome is
    /// never silent. `0` for the write side and for any source with no
    /// `BlockAdditions`.
    fn errors(&self) -> u64 {
        match self.mode {
            Mode::Read(ref rs) => rs.additions_dropped,
            _ => 0,
        }
    }

    /// Cumulative `BlockAdditions` bytes dropped on read-back — see
    /// [`errors`](crate::pes::Stream::errors). Counts the whole skipped subtree (the
    /// `BlockAdditional` payload plus a handful of bytes of EBML framing above
    /// it), so it is an upper bound on the payload proper.
    fn lost_bytes(&self) -> u64 {
        match self.mode {
            Mode::Read(ref rs) => rs.additions_dropped_bytes,
            _ => 0,
        }
    }
}

// ── MKV header parsing (read side) ────────────────────────────

/// Returns (DiscTitle, codec_privates: Vec<(track_number, codec_private_bytes)>)
fn parse_mkv_header(r: &mut impl Read) -> MkvHeaderResult {
    let mut title = String::new();
    // EBML `DURATION` is a float expressed in TimestampScale ticks, not
    // milliseconds (Matroska spec). Named accordingly; converted to
    // seconds below as ticks * ts_scale_ns / 1e9.
    let mut duration_ticks = 0.0f64;
    let mut ts_scale: u64 = 1_000_000;
    let mut streams: Vec<crate::disc::Stream> = Vec::new();
    let mut codec_privates: Vec<(u16, Vec<u8>)> = Vec::new();
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

    let (mut got_info, mut got_tracks) = (false, false);

    loop {
        if got_info && got_tracks {
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
                    remaining = remaining.saturating_sub(hlen as u64 + cs);
                    match cid {
                        ebml::TIMESTAMP_SCALE => ts_scale = read_uint_bounded(r, cs)?,
                        ebml::DURATION => duration_ticks = ebml::read_float_val(r, cs as usize)?,
                        ebml::TITLE => title = read_string_bounded(r, cs)?,
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
                    remaining = remaining.saturating_sub(hlen as u64 + cs);
                    if cid == ebml::TRACK_ENTRY {
                        let (stream, tnum, cp, default_dur) = parse_track(r, cs)?;
                        if let Some(s) = stream {
                            // Record the TrackNumber alongside the stream it maps
                            // to, in the SAME order, so block routing never has to
                            // guess that TrackNumbers are 1..=N.
                            streams.push(s);
                            tracks.push(tnum, default_dur);
                        }
                        if let Some(cp) = cp {
                            codec_privates.push((tnum, cp));
                        }
                    } else {
                        skip_bytes(r, cs)?;
                    }
                }
                got_tracks = true;
            }
            ebml::CLUSTER => break,
            _ if size != u64::MAX => {
                skip_bytes(r, size)?;
            }
            _ => break,
        }
    }

    let disc_title = DiscTitle {
        playlist: title,
        duration_secs: duration_ticks * (ts_scale as f64) / 1_000_000_000.0,
        streams,
        ..DiscTitle::empty()
    };
    // Clamp the (untrusted) scale to a positive i64 for the tick→ns multiply on
    // the read path; default to 1 ms if absent or absurd.
    let ts_scale_ns = if ts_scale == 0 || ts_scale > i64::MAX as u64 {
        1_000_000
    } else {
        ts_scale as i64
    };
    Ok((disc_title, codec_privates, ts_scale_ns, tracks))
}

/// Largest valid 13-bit MPEG-TS PID.
const MAX_TS_PID: u32 = 0x1FFF;

/// Map an MKV track number to a synthetic BD-TS PID, rejecting any value that
/// would overflow the 13-bit PID space. Track 1 is the video PID (0x1011);
/// every other track maps to `0x1100 + (tnum - 2)`. Computed in `u32` so the
/// addition can never wrap, unlike the prior `u16` arithmetic.
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

/// (stream, track_number, codec_private_bytes, default_duration_ns) — one
/// decoded `TrackEntry`. `stream` is `None` for a TrackType this crate does not
/// carry, in which case the TrackNumber gets no stream index at all.
type ParsedTrack = (
    Option<crate::disc::Stream>,
    u16,
    Option<Vec<u8>>,
    Option<u64>,
);

/// Returns (stream, track_number, codec_private_bytes, default_duration_ns)
fn parse_track(r: &mut impl Read, size: u64) -> io::Result<ParsedTrack> {
    let (mut ttype, mut tnum) = (0u64, 0u16);
    /// RFC 9559 §5.1.4.1.13 gives DefaultDuration as nanoseconds per frame with
    /// "range: not 0". A value this large is nonsense for a frame period and
    /// would only skew laced-frame spacing, so treat it as absent.
    const MAX_DEFAULT_DURATION_NS: u64 = 60 * 1_000_000_000;
    let mut default_dur: Option<u64> = None;
    let (mut codec_id, mut lang, mut name) = (String::new(), String::from("und"), String::new());
    let (mut ph, mut sr, mut ch, mut forced) = (0u32, 0.0f64, 0u8, false);
    let mut codec_priv: Option<Vec<u8>> = None;

    let mut remaining = size;
    while remaining > 0 {
        let (cid, cs, hlen) = ebml::read_element_header(r)?;
        if cs == u64::MAX {
            return Err(crate::error::Error::MkvSourceInvalid.into());
        }
        remaining = remaining.saturating_sub(hlen as u64 + cs);
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
            ebml::CODEC_ID => codec_id = read_string_bounded(r, cs)?,
            ebml::CODEC_PRIVATE => {
                codec_priv = Some(ebml::read_binary_val(
                    r,
                    checked_size(cs, MAX_CODEC_PRIVATE)?,
                )?)
            }
            ebml::LANGUAGE => lang = read_string_bounded(r, cs)?,
            ebml::TRACK_NAME => name = read_string_bounded(r, cs)?,
            ebml::FLAG_FORCED => forced = read_uint_bounded(r, cs)? != 0,
            ebml::VIDEO => {
                let mut vrem = cs;
                while vrem > 0 {
                    let (vid, vs, vhlen) = ebml::read_element_header(r)?;
                    if vs == u64::MAX {
                        return Err(crate::error::Error::MkvSourceInvalid.into());
                    }
                    vrem = vrem.saturating_sub(vhlen as u64 + vs);
                    if vid == ebml::PIXEL_HEIGHT {
                        ph = read_uint_bounded(r, vs)? as u32;
                    } else {
                        skip_bytes(r, vs)?;
                    }
                }
            }
            ebml::AUDIO => {
                let mut arem = cs;
                while arem > 0 {
                    let (aid, as_, ahlen) = ebml::read_element_header(r)?;
                    if as_ == u64::MAX {
                        return Err(crate::error::Error::MkvSourceInvalid.into());
                    }
                    arem = arem.saturating_sub(ahlen as u64 + as_);
                    match aid {
                        ebml::SAMPLING_FREQUENCY => sr = ebml::read_float_val(r, as_ as usize)?,
                        // Clamp instead of `as u8`: a foreign/corrupt MKV with a
                        // CHANNELS value that is a multiple of 256 would truncate to
                        // 0 (an invalid channel count) on a bare cast. Saturate to
                        // u8::MAX so an absurd count degrades to "many", never to 0.
                        ebml::CHANNELS => ch = read_uint_bounded(r, as_)?.min(u8::MAX as u64) as u8,
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

    // &str consts can't be `match` patterns, so compare via guards — this keeps
    // the single source of truth in `ebml::CODEC_*` shared with the muxer.
    let cid = codec_id.as_str();
    let codec = if cid == ebml::CODEC_HEVC {
        Codec::Hevc
    } else if cid == ebml::CODEC_H264 {
        Codec::H264
    } else if cid == ebml::CODEC_VC1 {
        Codec::Vc1
    } else if cid == ebml::CODEC_MPEG2 {
        Codec::Mpeg2
    } else if cid == ebml::CODEC_AC3 {
        Codec::Ac3
    } else if cid == ebml::CODEC_EAC3 {
        Codec::Ac3Plus
    } else if cid == ebml::CODEC_TRUEHD {
        Codec::TrueHd
    } else if cid == ebml::CODEC_DTS {
        Codec::Dts
    } else if cid == ebml::CODEC_PCM_BE {
        Codec::Lpcm
    } else if cid == ebml::CODEC_PGS {
        Codec::Pgs
    } else if cid == ebml::CODEC_VOBSUB {
        Codec::DvdSub
    } else {
        Codec::Unknown(0)
    };
    let res = Resolution::from_height(ph);
    let chs = AudioChannels::from_count(ch);
    let srs = if sr >= 192000.0 {
        SampleRate::S192
    } else if sr >= 176400.0 {
        SampleRate::S176_4
    } else if sr >= 96000.0 {
        SampleRate::S96
    } else if sr >= 88200.0 {
        SampleRate::S88_2
    } else if (44100.0..48000.0).contains(&sr) {
        SampleRate::S44_1
    } else if sr >= 48000.0 {
        SampleRate::S48
    } else {
        // Anything below the lowest rate this enum maps is UNKNOWN, not 48 kHz.
        // The ladder's final `else` used to be S48, so a legal 32000 Hz AC-3 or
        // DTS track — common in broadcast-sourced content — was recorded as
        // 48 kHz, and the wrong rate then propagated into the reconstructed
        // AudioStream. `SampleRate::from_hz` in disc/mod.rs is the crate's
        // canonical mapping and returns Unknown here; this ladder exists only
        // because the MKV element is a float and needs tolerance rather than
        // exact equality.
        SampleRate::Unknown
    };

    // Map MKV track numbers to BD-TS PIDs. A 13-bit TS PID tops out at
    // 0x1FFF; compute in u32 so the `0x1100 + (tnum - 2)` arithmetic can't
    // wrap u16 for large track numbers, and reject anything that would land
    // outside the valid PID space.
    let ts_pid = ts_pid_for_track(tnum)?;

    let stream = match ttype {
        1 => {
            let is_secondary = name.contains("Dolby Vision EL") || name.contains("DV EL");
            Some(crate::disc::Stream::Video(VideoStream {
                pid: ts_pid,
                codec,
                resolution: res,
                frame_rate: FrameRate::Unknown,
                hdr: HdrFormat::Sdr,
                color_space: ColorSpace::Bt709,
                // Remux input: the source MKV's DisplayWidth/Height is preserved
                // by the writer separately; nothing anamorphic to reconstruct here.
                display_aspect: None,
                secondary: is_secondary,
                label: name,
                measured_cicp: None,
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
    Ok((stream, tnum, codec_priv, default_dur))
}

/// Read-side map from Matroska TrackNumber to the index of the corresponding
/// entry in `DiscTitle::streams`.
///
/// RFC 9559 §5.1.4.1.1 constrains TrackNumber only to be non-zero ("range: not
/// 0"); NOTHING in the specification requires the numbers to be `1..=N`, to be
/// contiguous, or to appear in ascending TrackEntry order. `parse_track` also
/// DROPS every TrackEntry whose TrackType this crate cannot carry (anything but
/// 1/2/17 — e.g. a TrackType 18 buttons track), so the TrackNumber space and the
/// stream vector diverge for perfectly legal inputs.
///
/// The reader used to derive the stream index as `TrackNumber - 1`, which routes
/// blocks to the WRONG stream (parsed by the wrong codec parser) or drops them
/// entirely. This table records the real TrackNumber for each retained stream,
/// in stream order, and is the only thing allowed to translate between the two.
#[derive(Default)]
struct TrackTable {
    /// Matroska TrackNumber of `DiscTitle::streams[i]`, indexed by `i`.
    nums: Vec<u16>,
    /// TrackEntry `DefaultDuration` (RFC 9559 §5.1.4.1.13 — nanoseconds per
    /// frame, already in Matroska Ticks = ns), per stream index, when declared.
    /// Used to space the frames of a LACED Block, whose second and later frames
    /// carry an "underdetermined" timestamp per RFC 9559 §10.3.5.
    default_durations: Vec<Option<u64>>,
}

impl TrackTable {
    fn push(&mut self, num: u16, default_duration_ns: Option<u64>) {
        self.nums.push(num);
        self.default_durations.push(default_duration_ns);
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

/// Read one unsigned EBML VINT (RFC 8794 §4.4) from the head of `d`, returning
/// `(value, octet width)`. `None` when the first octet has no VINT_MARKER (a
/// width above 8 octets, which Matroska lacing never uses) or the value is
/// truncated.
///
/// Distinct from `block_vint`: that one is the *track number* decoder and caps
/// at 4 octets with a "treat as track 0" fallback, whereas lacing sizes need the
/// full 1..=8 range and must be able to report malformedness.
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

/// Split the body of a LACED (Simple)Block — the bytes after the flags octet,
/// beginning with the Lacing Head — into its individual frame payloads, per
/// RFC 9559 §10.3.
///
/// `lacing` is the 2-bit LACING field value (`LACING_XIPH`, `LACING_EBML` or
/// `LACING_FIXED`). Returns `None` when the lacing header is malformed — the
/// frame boundaries are then unknown, and the caller MUST reject the block
/// rather than hand a concatenation of frames plus lacing header downstream as
/// though it were one frame (which is exactly the silent corruption this
/// function exists to end).
///
/// The Lacing Head is "number of frames in the lace minus 1" on one octet, so
/// the frame count is bounded by 256 and no allocation here is attacker-scaled.
fn split_lacing(lacing: u8, body: &[u8]) -> Option<Vec<&[u8]>> {
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
            // A zero-size frame carries no data and cannot be a valid frame, so
            // `each == 0` is malformed rather than "n empty frames". It has to be
            // rejected explicitly: 0 % n == 0 passes the divisibility check above,
            // and `chunks` on an empty slice yields nothing whatever its argument,
            // so clamping the chunk width instead returned Some(vec![]) — zero
            // frames where the Lacing Head declared n, dropping the whole lace
            // silently with no error raised.
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

/// Parse a (Simple)Block payload into zero or more PesFrames.
///
/// Zero frames means the block was SKIPPED — too short, track 0, or a
/// TrackNumber this file does not (retainedly) declare. More than one frame
/// means the Block was LACED (RFC 9559 §10.3): one Block legitimately carries
/// several frames, and handing the raw payload downstream as a single frame
/// feeds the codec parser a concatenation of frames plus lacing header. An
/// `Err` means the lacing header is malformed, so the frame boundaries are
/// unknowable — the block is rejected rather than mangled.
///
/// `cluster_ts_ticks` is the open cluster's timestamp in TimestampScale ticks
/// and `ts_scale_ns` is that scale (ns per tick); the block PTS is computed as
/// `(cluster_ts_ticks + rel_ts) * ts_scale_ns` so foreign MKVs whose scale
/// isn't 1 ms are honoured (freemkv's own output uses 1_000_000 and round-trips
/// unchanged). `tracks` resolves the TrackNumber to a stream index; `duration_ns`
/// is propagated for BlockGroup blocks (None for SimpleBlock).
fn parse_block(
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
    // returns 0 for an unsupported 5+ byte VINT, so a corrupt/zero-track block
    // must be skipped rather than attributed to the first stream.
    if track == 0 {
        return Ok(Vec::new());
    }

    let rel_ts = i16::from_be_bytes([block[vl], block[vl + 1]]);
    let flags = block[vl + 2];
    let keyframe = flags & 0x80 != 0;
    let body = &block[vl + 3..];
    // saturating_add: a hostile CLUSTER_TIMESTAMP near i64::MAX plus a positive
    // rel_ts would overflow this add (panic in debug/test, wrap to a large
    // negative PTS in release) — one operation BEFORE the saturating_mul below.
    // rel_ts as i64 is exact, so this fully bounds the sum on adversarial input.
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
        // Its own code, not the generic `MkvSourceInvalid`: a laced Block names a
        // specific RFC 9559 §10.3 feature whose header is self-inconsistent, which
        // is a distinct diagnosis from "the container is corrupt somewhere".
        // Neither is `MkvInvalid` — `error::is_skippable_title_stub` classifies
        // that code as an empty nav/menu stub, so a real track with unseparable
        // frames would be dropped by the caller while the run reported success.
        return Err(crate::error::Error::MkvLacingInvalid.into());
    };

    // RFC 9559 §10.3.5: a Block carries a single timestamp, which applies to the
    // FIRST frame of the lace; every later frame has an "underdetermined"
    // timestamp but MUST be contiguous with its predecessor. Recover the spacing
    // from the track's DefaultDuration (§5.1.4.1.13, already nanoseconds) when
    // declared, else by dividing this Block's BlockDuration across the lace.
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

fn block_vint(d: &[u8]) -> (u64, usize) {
    if d.is_empty() {
        return (0, 0);
    }
    if d[0] & 0x80 != 0 {
        return ((d[0] & 0x7F) as u64, 1);
    }
    if d[0] & 0x40 != 0 && d.len() >= 2 {
        return ((((d[0] & 0x3F) as u64) << 8) | d[1] as u64, 2);
    }
    if d[0] & 0x20 != 0 && d.len() >= 3 {
        return (
            (((d[0] & 0x1F) as u64) << 16) | ((d[1] as u64) << 8) | d[2] as u64,
            3,
        );
    }
    if d[0] & 0x10 != 0 && d.len() >= 4 {
        return (
            (((d[0] & 0x0F) as u64) << 24)
                | ((d[1] as u64) << 16)
                | ((d[2] as u64) << 8)
                | d[3] as u64,
            4,
        );
    }
    (0, 1) // Unsupported 5+ byte VINT — treat as track 0
}

#[cfg(test)]
mod tests {

    /// A fixed lace whose body is empty declares n frames and carries none. It is
    /// malformed, and must be rejected rather than silently yielding zero frames:
    /// 0 % n == 0 passes the divisibility check, and `chunks` on an empty slice
    /// yields nothing whatever width it is given, so the whole lace vanished with
    /// no error raised and the caller saw a clean short block.
    #[test]
    fn fixed_lacing_with_an_empty_body_is_malformed_not_zero_frames() {
        // Lacing Head only: count_minus_one = 2, i.e. three frames declared,
        // followed by no payload at all.
        assert_eq!(super::split_lacing(super::LACING_FIXED, &[2u8]), None);
        // Same shape for a single declared frame.
        assert_eq!(super::split_lacing(super::LACING_FIXED, &[0u8]), None);
    }

    /// The non-degenerate fixed lace still splits evenly, so the guard above did
    /// not tighten the valid case.
    #[test]
    fn fixed_lacing_splits_an_evenly_divisible_body() {
        let laced = super::split_lacing(super::LACING_FIXED, &[2u8, 1, 2, 3, 4, 5, 6])
            .expect("three 2-byte frames is a well-formed fixed lace");
        assert_eq!(laced, vec![&[1u8, 2][..], &[3, 4][..], &[5, 6][..]]);
    }
    use super::*;
    use crate::pes::Stream as _;
    use std::io::Cursor;

    /// Length-prefix (4-byte big-endian) each NAL, as the H.264 parser emits.
    fn lp(nals: &[&[u8]]) -> Vec<u8> {
        let mut v = Vec::new();
        for n in nals {
            v.extend_from_slice(&(n.len() as u32).to_be_bytes());
            v.extend_from_slice(n);
        }
        v
    }

    fn mvc_frame(track: usize, pts: i64, keyframe: bool, data: Vec<u8>) -> crate::pes::PesFrame {
        crate::pes::PesFrame {
            track,
            pts,
            keyframe,
            data,
            duration_ns: None,
            source: None,
            coding: None,
        }
    }

    // A subset SPS (NAL type 15), a PPS (type 8), and a coded-slice-extension
    // (type 20) — the shape of a dependent-view access unit.
    const SUBSET_SPS: [u8; 5] = [0x6F, 0x80, 0x00, 0x33, 0xAA]; // 0x6F & 0x1F = 15
    const DEP_PPS: [u8; 3] = [0x68, 0xEE, 0x3C]; // 0x68 & 0x1F = 8
    const DEP_SLICE: [u8; 3] = [0x74, 0x11, 0x22]; // 0x74 & 0x1F = 20

    #[test]
    fn extract_mvc_params_finds_subset_sps_and_pps() {
        let data = lp(&[&SUBSET_SPS, &DEP_PPS, &DEP_SLICE]);
        let (s, p) = extract_mvc_params(&data).expect("both param sets present");
        assert_eq!(s, SUBSET_SPS, "subset SPS (NAL 15) captured verbatim");
        assert_eq!(p, DEP_PPS, "PPS (NAL 8) captured verbatim");
        // Missing PPS → None (the serializer then emits no mvcC mapping).
        assert!(extract_mvc_params(&lp(&[&SUBSET_SPS, &DEP_SLICE])).is_none());
        // Missing subset SPS → None.
        assert!(extract_mvc_params(&lp(&[&DEP_PPS, &DEP_SLICE])).is_none());
    }

    fn empty_merge() -> MvcMerge {
        MvcMerge {
            base_stream_idx: 0,
            dep_stream_idx: 2,
            base_track_idx: 0,
            stream_to_track: vec![Some(0), Some(1), None],
            pending_base: std::collections::VecDeque::new(),
            dep_by_pts: std::collections::HashMap::new(),
            captured_params: None,
            orphan_deps: 0,
        }
    }

    #[test]
    fn mvc_merge_pairs_base_and_dependent_by_pts() {
        let mut m = empty_merge();
        let dep = lp(&[&SUBSET_SPS, &DEP_PPS, &DEP_SLICE]);

        // Base arrives first (SSIF order): buffered, nothing emitted yet.
        let e = m.ingest(&mvc_frame(0, 100, true, lp(&[&[0x65, 1, 2]])));
        assert!(e.is_empty(), "base held until its dependent arrives");

        // Dependent arrives → base is emitted, remapped to the base track, with
        // the dependent AU as its BlockAdditional; params are captured.
        let e = m.ingest(&mvc_frame(2, 100, false, dep.clone()));
        assert_eq!(e.len(), 1, "the paired base frame is emitted");
        assert_eq!(e[0].0.track, 0, "remapped to the base muxer track");
        assert_eq!(
            e[0].1.as_deref(),
            Some(dep.as_slice()),
            "dependent attached"
        );
        assert!(m.captured_params.is_some(), "mvcC params captured");

        // Audio passes straight through (remapped, no additional).
        let e = m.ingest(&mvc_frame(1, 100, true, vec![0xAA]));
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].0.track, 1);
        assert!(e[0].1.is_none());

        // Dependent-before-base (reordered) also pairs.
        let dep2 = lp(&[&DEP_SLICE]);
        assert!(m.ingest(&mvc_frame(2, 200, false, dep2.clone())).is_empty());
        let e = m.ingest(&mvc_frame(0, 200, false, lp(&[&[0x61, 3, 4]])));
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].1.as_deref(), Some(dep2.as_slice()));
    }

    #[test]
    fn mvc_merge_flushes_unpaired_base_at_eof() {
        let mut m = empty_merge();
        // Base with no dependent ever → held, then flushed unpaired at EOF.
        assert!(
            m.ingest(&mvc_frame(0, 10, true, vec![0, 0, 0, 1]))
                .is_empty()
        );
        let tail = m.flush();
        assert_eq!(tail.len(), 1, "unpaired base still emitted");
        assert!(tail[0].1.is_none(), "no BlockAdditional when unpaired");
    }

    #[test]
    fn extract_mvc_params_no_panic_on_truncated_or_empty() {
        // Empty, sub-header, zero-length NAL, and a length prefix claiming more
        // than is present must all return None without panicking (untrusted AU).
        assert!(extract_mvc_params(&[]).is_none());
        assert!(extract_mvc_params(&[0, 0, 0]).is_none());
        assert!(
            extract_mvc_params(&[0, 0, 0, 0]).is_none(),
            "lone zero-length NAL yields no params"
        );
        assert!(
            extract_mvc_params(&[0, 0, 0, 10, 0x6F]).is_none(),
            "length prefix past end breaks, no slice panic"
        );
        // A zero-length NAL is SKIPPED, not fatal: valid param sets that follow
        // are still found (a stray length prefix must not abandon the whole AU).
        let mut d = vec![0, 0, 0, 0];
        d.extend_from_slice(&lp(&[&SUBSET_SPS, &DEP_PPS]));
        let (s, p) = extract_mvc_params(&d).expect("params found past the zero-length NAL");
        assert_eq!(s, SUBSET_SPS);
        assert_eq!(p, DEP_PPS);
    }

    #[test]
    fn mvc_merge_flushes_oldest_base_once_past_window() {
        let mut m = empty_merge();
        // Push more unpaired base frames than the window; the excess flush as
        // plain (unpaired) blocks in FIFO order once len exceeds MVC_PAIR_WINDOW.
        let n = MVC_PAIR_WINDOW + 8;
        let mut emitted = 0usize;
        for pts in 0..n {
            emitted += m
                .ingest(&mvc_frame(0, pts as i64, false, vec![0, 0, 0, 1]))
                .len();
        }
        assert_eq!(emitted, 8, "the {n} bases beyond the window flush unpaired");
        assert_eq!(m.pending_base.len(), MVC_PAIR_WINDOW, "window still held");
        assert!(m.flush().iter().all(|(_, add)| add.is_none()));
    }

    #[test]
    fn mvc_merge_dep_overflow_drops_old_keeps_newest() {
        let mut m = empty_merge();
        // Fill dep_by_pts to the bound with unpaired dependents (unique PTS).
        for pts in 0..(MVC_PAIR_WINDOW * 4) {
            assert!(
                m.ingest(&mvc_frame(2, pts as i64, false, lp(&[&DEP_SLICE])))
                    .is_empty()
            );
        }
        assert_eq!(m.dep_by_pts.len(), MVC_PAIR_WINDOW * 4);
        // One more overflows: the drifted buffer is cleared BUT the newest survives
        // so its (soon-to-arrive) base can still pair.
        let dep_new = lp(&[&DEP_SLICE]);
        m.ingest(&mvc_frame(2, 9_999, false, dep_new.clone()));
        assert_eq!(m.dep_by_pts.len(), 1, "old cleared, newest kept");
        assert!(m.dep_by_pts.contains_key(&9_999));
        assert_eq!(
            m.orphan_deps,
            (MVC_PAIR_WINDOW * 4) as u64,
            "old buffer counted once"
        );
        // The surviving dependent pairs with its base.
        let e = m.ingest(&mvc_frame(0, 9_999, false, vec![0x61, 1]));
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].1.as_deref(), Some(dep_new.as_slice()));
    }

    #[test]
    fn create_does_not_panic_when_only_video_is_mvc_dependent() {
        // A (malformed / hand-built) title whose single video IS the dependent
        // must NOT panic: base_stream_idx is None, so no merge is set up and the
        // dependent is muxed as an ordinary track.
        use crate::disc::{
            Codec, ColorSpace, DiscTitle, FrameRate, HdrFormat, Resolution, Stream, VideoStream,
        };
        let dep = VideoStream {
            pid: 0x1012,
            codec: Codec::H264,
            resolution: Resolution::R1080p,
            frame_rate: FrameRate::F24,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            display_aspect: None,
            secondary: true,
            label: crate::disc::MVC_DEPENDENT_LABEL.to_string(),
            measured_cicp: None,
        };
        let title = DiscTitle {
            streams: vec![Stream::Video(dep)],
            ..DiscTitle::empty()
        };
        let s = MkvStream::create(Box::new(Cursor::new(Vec::new())), &title, None)
            .expect("create must succeed, not panic");
        assert!(
            s.mvc.is_none(),
            "no merge when there is no distinct base view"
        );
    }

    #[test]
    fn apply_coding_to_track_sets_measured_field_order_never_guesses() {
        use crate::disc::{Codec, ColorSpace, FrameRate, HdrFormat, Resolution, VideoStream};
        use crate::mux::codec::coding::{CodingType, Mpeg2Coding, PictureInfo};

        let interlaced_track = || {
            MkvTrack::video(&VideoStream {
                pid: 0xE0,
                codec: Codec::Mpeg2,
                resolution: Resolution::R576i, // interlaced
                frame_rate: FrameRate::F25,
                hdr: HdrFormat::Sdr,
                color_space: ColorSpace::Bt470bg,
                display_aspect: None,
                secondary: false,
                label: String::new(),
                measured_cicp: None,
            })
        };
        let pic = |tff: bool, pf: bool| {
            PictureInfo::mpeg2(
                CodingType::I,
                Mpeg2Coding {
                    top_field_first: tff,
                    repeat_first_field: false,
                    progressive_frame: pf,
                    progressive_sequence: false,
                    frame_picture: true,
                },
            )
        };

        // A freshly built interlaced track has no field order — UNDETERMINED,
        // never a scan-time guess.
        assert_eq!(
            interlaced_track().field_order,
            ebml::FIELD_ORDER_UNDETERMINED
        );

        // MEASURED bottom-field-first → BFF (6). The red-flag fix.
        let mut t = interlaced_track();
        apply_coding_to_track(&mut t, Some(pic(false, false)), true);
        assert_eq!(
            t.field_order,
            ebml::FIELD_ORDER_BFF,
            "measured BFF → FieldOrder=6"
        );

        // MEASURED top-field-first → TFF (1).
        let mut t = interlaced_track();
        apply_coding_to_track(&mut t, Some(pic(true, false)), true);
        assert_eq!(
            t.field_order,
            ebml::FIELD_ORDER_TFF,
            "measured TFF → FieldOrder=1"
        );

        // Interlaced track, a video picture but NO usable field order →
        // UNDETERMINED (logged loudly, never faked).
        let mut t = interlaced_track();
        apply_coding_to_track(&mut t, None, true);
        assert_eq!(
            t.field_order,
            ebml::FIELD_ORDER_UNDETERMINED,
            "no measured value → UNDETERMINED, never a guess"
        );

        // Interlaced track activated with NO video picture (empty/buffered-only
        // title) → UNDETERMINED, logged quietly (not a parser defect).
        let mut t = interlaced_track();
        apply_coding_to_track(&mut t, None, false);
        assert_eq!(
            t.field_order,
            ebml::FIELD_ORDER_UNDETERMINED,
            "empty title → UNDETERMINED, never a guess"
        );

        // Progressive picture on an interlaced-flagged track → UNDETERMINED (no
        // field order applies; not faked to TFF/BFF).
        let mut t = interlaced_track();
        apply_coding_to_track(&mut t, Some(pic(true, true)), true);
        assert_eq!(t.field_order, ebml::FIELD_ORDER_UNDETERMINED);

        // A PROGRESSIVE track is never touched — field order stays UNDETERMINED.
        let mut prog = MkvTrack::video(&VideoStream {
            pid: 0xE0,
            codec: Codec::H264,
            resolution: Resolution::R1080p, // progressive
            frame_rate: FrameRate::F24,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        });
        assert!(!prog.interlaced);
        apply_coding_to_track(&mut prog, Some(pic(false, false)), true);
        assert_eq!(prog.field_order, ebml::FIELD_ORDER_UNDETERMINED);
    }

    /// `apply_coding_to_track` routes MEASURED HDR10 static metadata from the
    /// first coded picture onto the track (independent of interlace), and leaves
    /// it `None` when the picture carried none — never fabricated.
    #[test]
    fn apply_coding_to_track_plumbs_measured_hdr10() {
        use crate::disc::{Codec, ColorSpace, FrameRate, HdrFormat, Resolution, VideoStream};
        use crate::mux::codec::Hdr10Metadata;
        use crate::mux::codec::coding::{CodingType, PictureInfo};

        let make = || {
            MkvTrack::video(&VideoStream {
                pid: 0xE0,
                codec: Codec::Hevc,
                resolution: Resolution::R2160p, // progressive UHD
                frame_rate: FrameRate::F24,
                hdr: HdrFormat::Hdr10,
                color_space: ColorSpace::Bt2020,
                display_aspect: None,
                secondary: false,
                label: String::new(),
                measured_cicp: None,
            })
        };
        let h = Hdr10Metadata {
            display_primaries_x: [8500, 6550, 35400],
            display_primaries_y: [39850, 2300, 14600],
            white_point_x: 15635,
            white_point_y: 16450,
            max_display_mastering_luminance: 10_000_000,
            min_display_mastering_luminance: 1,
            max_content_light_level: 1000,
            max_pic_average_light_level: 400,
        };

        // Picture carries HDR10 → plumbed onto the track.
        let mut t = make();
        assert!(t.hdr10.is_none(), "fresh track has no HDR10");
        let pic = PictureInfo::coding_type_only(CodingType::I).with_hdr10(Some(h));
        apply_coding_to_track(&mut t, Some(pic), true);
        assert_eq!(t.hdr10, Some(h), "measured HDR10 must reach the track");

        // Picture without HDR10 → track stays None (never fabricated).
        let mut t = make();
        let pic = PictureInfo::coding_type_only(CodingType::I);
        apply_coding_to_track(&mut t, Some(pic), true);
        assert!(t.hdr10.is_none(), "no measured HDR10 → track stays None");

        // No coding at all → None.
        let mut t = make();
        apply_coding_to_track(&mut t, None, true);
        assert!(t.hdr10.is_none());
    }

    // `From<Error> for io::Error` encodes the numeric code into the
    // Display string as "E{code}: ...". Check the prefix.
    /// Extract the error from a `MkvStream::open` result without requiring
    /// `MkvStream: Debug` (which `unwrap_err` would).
    fn open_err(r: io::Result<MkvStream>) -> io::Error {
        match r {
            Ok(_) => panic!("expected MkvStream::open to fail"),
            Err(e) => e,
        }
    }

    /// Whether the error is the read path's malformed-source rejection
    /// (`E_MKV_SOURCE_INVALID`). Asserted rather than the historical
    /// `E_MKV_INVALID` on purpose: `E_MKV_INVALID` is the no-muxable-frames stub
    /// code, and `error::is_skippable_title_stub` classifies it as skippable, so
    /// a corrupt source reported under it would be silently passed over by an
    /// all-titles rip that then exited successfully.
    fn is_mkv_source_invalid(e: &io::Error) -> bool {
        has_code(e, crate::error::E_MKV_SOURCE_INVALID) && !crate::error::is_skippable_title_stub(e)
    }

    /// Whether an error carries the given numeric code (the crate's errors
    /// render as `E<code>` with no English text).
    fn has_code(e: &io::Error, code: u16) -> bool {
        e.kind() == io::ErrorKind::InvalidData && e.to_string().starts_with(&format!("E{code}"))
    }

    #[test]
    fn ts_pid_for_track_maps_and_rejects_overflow() {
        // Track 1 → video PID; track 2 → first audio PID base.
        assert_eq!(ts_pid_for_track(1).unwrap(), 0x1011);
        assert_eq!(ts_pid_for_track(2).unwrap(), 0x1100);
        assert_eq!(ts_pid_for_track(3).unwrap(), 0x1101);
        // Highest track that still lands inside the 13-bit PID space.
        // 0x1100 + (tnum-2) <= 0x1FFF  ⇒  tnum <= 0xF01.
        assert_eq!(ts_pid_for_track(0xF01).unwrap(), 0x1FFF);
        // One past the edge must be rejected, not wrap u16.
        assert!(is_mkv_source_invalid(&ts_pid_for_track(0xF02).unwrap_err()));
        // Former overflow case (debug panic / release garbage PID) is rejected.
        assert!(is_mkv_source_invalid(
            &ts_pid_for_track(u16::MAX).unwrap_err()
        ));
        // Track 0 is invalid (1-based) and would underflow tnum-2.
        assert!(is_mkv_source_invalid(&ts_pid_for_track(0).unwrap_err()));
    }

    #[test]
    fn checked_size_rejects_over_cap() {
        // Within cap → Ok with usize value.
        assert_eq!(checked_size(100, 256).unwrap(), 100);
        assert_eq!(checked_size(256, 256).unwrap(), 256);
        // Over cap → MkvSourceInvalid, never a giant allocation.
        let e = checked_size(257, 256).unwrap_err();
        assert!(is_mkv_source_invalid(&e));
        // A hostile multi-GB block size is rejected as MkvSourceInvalid.
        let e = checked_size(4 * 1024 * 1024 * 1024, MAX_BLOCK_SIZE).unwrap_err();
        assert!(is_mkv_source_invalid(&e));
    }

    #[test]
    fn read_uint_bounded_rejects_oversized_int() {
        // size > 8 would index out of the fixed 8-byte buffer in
        // read_uint_val (panic / OOB). The guard turns it into a clean
        // MkvSourceInvalid error instead.
        let mut data = Cursor::new(vec![0u8; 16]);
        let e = read_uint_bounded(&mut data, 9).unwrap_err();
        assert!(is_mkv_source_invalid(&e));
    }

    #[test]
    fn read_uint_bounded_accepts_valid_width() {
        // 8 bytes is the max legal EBML uint width and must still work.
        let mut data = Cursor::new(vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02]);
        assert_eq!(read_uint_bounded(&mut data, 8).unwrap(), 0x0102);
    }

    #[test]
    fn read_string_bounded_rejects_huge_string() {
        // Claimed string length far above the cap must not allocate.
        let mut data = Cursor::new(vec![0u8; 16]);
        let e = read_string_bounded(&mut data, MAX_STRING_LEN + 1).unwrap_err();
        assert!(is_mkv_source_invalid(&e));
    }

    /// Build a minimal MKV (EBML header + Segment + Info + Tracks) so the
    /// reader is positioned in the cluster body, then append the given
    /// cluster bytes. Returns the full byte stream.
    fn minimal_mkv_with_cluster(cluster_body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        // EBML header (empty body).
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        // Segment (unknown size so the reader streams children).
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        // Empty Info.
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        // Empty Tracks.
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        out.extend_from_slice(cluster_body);
        out
    }

    #[test]
    fn simple_block_oversized_size_is_rejected() {
        // Cluster containing a SIMPLE_BLOCK that claims a 2 GiB payload.
        // The reader must reject it (MkvSourceInvalid) rather than attempt a
        // multi-GB allocation. Header parse stops at CLUSTER, so the
        // SIMPLE_BLOCK is hit on the first read().
        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
        ebml::write_size(&mut cluster, 2 * 1024 * 1024 * 1024).unwrap();
        // No payload follows — but we must fail on the size check, before
        // any read of the body.
        let bytes = minimal_mkv_with_cluster(&cluster);

        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let e = stream.read().unwrap_err();
        assert!(is_mkv_source_invalid(&e));
    }

    #[test]
    fn well_formed_simple_block_round_trips() {
        // A small, well-formed SIMPLE_BLOCK must still parse into a frame.
        // We need at least one stream so the track index is in range, so
        // give Tracks one video TRACK_ENTRY (track number 1).
        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();

        // Tracks → one TRACK_ENTRY (track number 1, type 1 = video).
        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
        let mut track_entry = Vec::new();
        ebml::write_id(&mut track_entry, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut track_entry, entry.len() as u64).unwrap();
        track_entry.extend_from_slice(&entry);
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, track_entry.len() as u64).unwrap();
        out.extend_from_slice(&track_entry);

        // Cluster with a SIMPLE_BLOCK: track vint=0x81 (track 1),
        // rel_ts=0x0000, flags=0x80 (keyframe), then 4 bytes of data.
        ebml::write_id(&mut out, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        let block = [0x81u8, 0x00, 0x00, 0x80, 0xAA, 0xBB, 0xCC, 0xDD];
        ebml::write_id(&mut out, ebml::SIMPLE_BLOCK).unwrap();
        ebml::write_size(&mut out, block.len() as u64).unwrap();
        out.extend_from_slice(&block);

        let mut stream = MkvStream::open(Cursor::new(out)).unwrap();
        let frame = stream.read().unwrap().expect("expected a frame");
        assert_eq!(frame.track, 0);
        assert!(frame.keyframe);
        assert_eq!(frame.data, vec![0xAA, 0xBB, 0xCC, 0xDD]);
    }
    #[test]
    fn truncated_simple_block_body_errors_not_panics() {
        // A SIMPLE_BLOCK that declares a 64-byte payload but supplies none.
        // read_exact_bounded must surface a clean typed MkvSourceInvalid error
        // (a truncated declared element is malformed input), never panic,
        // and never allocate the full declared size up front.
        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
        ebml::write_size(&mut cluster, 64).unwrap();
        // No body bytes follow → short read.
        let bytes = minimal_mkv_with_cluster(&cluster);

        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let e = stream.read().unwrap_err();
        assert!(is_mkv_source_invalid(&e));
    }

    /// Build a minimal MKV header + Segment + Info, then a Tracks element with a
    /// single TRACK_ENTRY of the given track number/type, then the cluster bytes.
    fn mkv_with_track_and_cluster(tnum: u64, ttype: u64, cluster_body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();

        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, tnum).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, ttype).unwrap();
        let mut track_entry = Vec::new();
        ebml::write_id(&mut track_entry, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut track_entry, entry.len() as u64).unwrap();
        track_entry.extend_from_slice(&entry);
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, track_entry.len() as u64).unwrap();
        out.extend_from_slice(&track_entry);

        out.extend_from_slice(cluster_body);
        out
    }

    #[test]
    fn oversized_codec_private_is_rejected() {
        // A TRACK_ENTRY whose CODEC_PRIVATE declares a payload above
        // MAX_CODEC_PRIVATE must be rejected (MkvSourceInvalid) before any
        // multi-MB allocation, while parsing the header.
        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
        // CODEC_PRIVATE header claiming a huge size (no body needed — the
        // size check fires first).
        ebml::write_id(&mut entry, ebml::CODEC_PRIVATE).unwrap();
        ebml::write_size(&mut entry, MAX_CODEC_PRIVATE + 1).unwrap();
        let mut track_entry = Vec::new();
        ebml::write_id(&mut track_entry, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut track_entry, entry.len() as u64).unwrap();
        track_entry.extend_from_slice(&entry);

        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, track_entry.len() as u64).unwrap();
        out.extend_from_slice(&track_entry);

        let e = match MkvStream::open(Cursor::new(out)) {
            Ok(_) => panic!("expected MkvSourceInvalid, got Ok"),
            Err(e) => e,
        };
        assert!(is_mkv_source_invalid(&e));
    }

    #[test]
    fn block_group_frame_round_trips_with_duration() {
        // MkvMuxer emits AC3/PGS frames — and every MPEG-2 video frame — as a
        // BlockGroup (BLOCK + BLOCK_DURATION [+ REFERENCE_BLOCK]). The reader
        // must descend into the group and yield the frame (with its duration)
        // rather than skipping it — otherwise every such frame this muxer writes
        // is lost on read-back.
        //
        // Keyframe-ness: inside a BlockGroup the SimpleBlock 0x80 bit is
        // RESERVED (always 0 here); a Block with NO ReferenceBlock child is a
        // keyframe. This group has none, so the frame is a keyframe — which is
        // also the truth for the AC-3/PGS frames this path was written for
        // (they are self-contained). The `!keyframe` this test used to assert
        // came from reading the reserved bit; see
        // `reference_block_marks_block_group_frame_as_non_keyframe`.
        let block = [0x82u8, 0x00, 0x05, 0x00, 0x11, 0x22, 0x33]; // track 2, rel 5, reserved bit 0, 3 data
        let mut bg_body = Vec::new();
        ebml::write_id(&mut bg_body, ebml::BLOCK).unwrap();
        ebml::write_size(&mut bg_body, block.len() as u64).unwrap();
        bg_body.extend_from_slice(&block);
        ebml::write_uint(&mut bg_body, ebml::BLOCK_DURATION, 40).unwrap(); // 40 ms

        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        // CLUSTER_TIMESTAMP = 100 ms so pts = (100 + 5) ms.
        ebml::write_uint(&mut cluster, ebml::CLUSTER_TIMESTAMP, 100).unwrap();
        ebml::write_id(&mut cluster, ebml::BLOCK_GROUP).unwrap();
        ebml::write_size(&mut cluster, bg_body.len() as u64).unwrap();
        cluster.extend_from_slice(&bg_body);

        // Track 2 (audio) so track_idx 1 needs two streams; give two TRACK_ENTRYs.
        // Reuse the helper for track 1, then a manual second entry would be
        // simpler — instead build directly with two entries.
        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        let mut tracks = Vec::new();
        for (n, t) in [(1u64, 1u64), (2u64, 2u64)] {
            let mut entry = Vec::new();
            ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, n).unwrap();
            ebml::write_uint(&mut entry, ebml::TRACK_TYPE, t).unwrap();
            ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
            ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
            tracks.extend_from_slice(&entry);
        }
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, tracks.len() as u64).unwrap();
        out.extend_from_slice(&tracks);
        out.extend_from_slice(&cluster);

        let mut stream = MkvStream::open(Cursor::new(out)).unwrap();
        let frame = stream
            .read()
            .unwrap()
            .expect("BlockGroup frame must be read");
        assert_eq!(frame.track, 1, "track 2 → index 1");
        assert!(
            frame.keyframe,
            "a BlockGroup with no ReferenceBlock is a keyframe (the 0x80 bit is reserved here)"
        );
        assert_eq!(frame.data, vec![0x11, 0x22, 0x33]);
        assert_eq!(frame.pts, 105 * 1_000_000, "pts = (cluster 100 + rel 5) ms");
        assert_eq!(frame.duration_ns, Some(40 * 1_000_000));
    }

    /// A BlockGroup's `BlockAdditions` subtree (BlockAddID=2 — the MVC
    /// dependent/right-eye access unit this crate's 3D writer emits, see
    /// `mkv.rs::build_block_group`) cannot be carried by `PesFrame`, so read-back
    /// drops it. That is a LOSSY outcome, and this crate's rule is that a lossy
    /// outcome is never silent.
    ///
    /// Regression: the arm did not exist, so the subtree fell into the `_ =>`
    /// skip arm — an `mkv://` → `mkv://` re-mux of a 3D rip lost one whole eye
    /// with no error, no warning and `lost_bytes == 0`, i.e. the mux reported a
    /// clean, complete, loss-free copy of a file it had halved. The base view
    /// must still read back intact, and the dropped payload must now be counted
    /// so it reaches `MuxOutcome.lost_bytes` / `.errors`.
    #[test]
    fn block_additions_dropped_on_read_back_is_counted_not_silent() {
        // The dependent-view payload: big enough that a byte count is unambiguous.
        let dependent_au = vec![0x5Au8; 512];

        // BlockAdditions > BlockMore > { BlockAddID = 2, BlockAdditional }.
        let mut more = Vec::new();
        ebml::write_uint(&mut more, ebml::BLOCK_ADD_ID, 2).unwrap();
        ebml::write_binary(&mut more, ebml::BLOCK_ADDITIONAL, &dependent_au).unwrap();
        let mut adds = Vec::new();
        ebml::write_id(&mut adds, ebml::BLOCK_MORE).unwrap();
        ebml::write_size(&mut adds, more.len() as u64).unwrap();
        adds.extend_from_slice(&more);

        // BlockGroup > { Block(base view), BlockAdditions }.
        let block = [0x81u8, 0x00, 0x00, 0x00, 0xAA, 0xBB, 0xCC];
        let mut bg_body = Vec::new();
        ebml::write_id(&mut bg_body, ebml::BLOCK).unwrap();
        ebml::write_size(&mut bg_body, block.len() as u64).unwrap();
        bg_body.extend_from_slice(&block);
        ebml::write_id(&mut bg_body, ebml::BLOCK_ADDITIONS).unwrap();
        ebml::write_size(&mut bg_body, adds.len() as u64).unwrap();
        bg_body.extend_from_slice(&adds);

        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        ebml::write_id(&mut cluster, ebml::BLOCK_GROUP).unwrap();
        ebml::write_size(&mut cluster, bg_body.len() as u64).unwrap();
        cluster.extend_from_slice(&bg_body);

        // One video TRACK_ENTRY (track number 1) so track index 0 is in range.
        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
        let mut tracks = Vec::new();
        ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
        tracks.extend_from_slice(&entry);

        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, tracks.len() as u64).unwrap();
        out.extend_from_slice(&tracks);
        out.extend_from_slice(&cluster);

        let mut stream = MkvStream::open(Cursor::new(out)).unwrap();
        assert_eq!(stream.errors(), 0, "no BlockAdditions seen before the read");
        assert_eq!(stream.lost_bytes(), 0);

        let frame = stream
            .read()
            .unwrap()
            .expect("the base-view BlockGroup frame must still be read");
        assert_eq!(frame.track, 0);
        assert_eq!(
            frame.data,
            vec![0xAA, 0xBB, 0xCC],
            "the frame carries the BASE view only — the dependent view is lost"
        );
        assert!(
            !frame.data.contains(&0x5A),
            "PesFrame has no side-payload field, so the dependent AU is NOT in the frame"
        );

        // The loss is now reported.
        assert_eq!(
            stream.errors(),
            1,
            "one dropped BlockAdditions subtree must be counted as a loss event"
        );
        assert!(
            stream.lost_bytes() >= dependent_au.len() as u64,
            "dropped bytes ({}) must cover the {}-byte dependent AU",
            stream.lost_bytes(),
            dependent_au.len()
        );

        // EOF, and the counters survive it (the driver samples them after the run).
        assert!(stream.read().unwrap().is_none());
        assert_eq!(stream.errors(), 1);
    }

    /// A BlockGroup carrying a ReferenceBlock is NOT a keyframe — that element's
    /// presence is the only non-keyframe signal a BlockGroup has (the
    /// SimpleBlock 0x80 flag bit is reserved and always 0 inside one).
    ///
    /// Regression: the reader used to `skip_bytes` past REFERENCE_BLOCK and read
    /// the reserved bit instead, so EVERY BlockGroup frame came back as a
    /// non-keyframe. Since the MPEG-2 parser stamps a per-frame duration, all
    /// MPEG-2 video takes the BlockGroup path — so no video frame ever looked
    /// like a keyframe on re-mux. That silently dropped all video on
    /// `mkv://`→`m2ts://` and failed `mkv://`→`mkv://` with E6008.
    #[test]
    fn reference_block_marks_block_group_frame_as_non_keyframe() {
        // Same construction as the test above, plus a ReferenceBlock child.
        let block = [0x82u8, 0x00, 0x05, 0x00, 0x11, 0x22, 0x33];
        let mut bg_body = Vec::new();
        ebml::write_id(&mut bg_body, ebml::BLOCK).unwrap();
        ebml::write_size(&mut bg_body, block.len() as u64).unwrap();
        bg_body.extend_from_slice(&block);
        ebml::write_uint(&mut bg_body, ebml::BLOCK_DURATION, 40).unwrap();
        // References a keyframe 40 ms earlier ⇒ this Block is not a seek point.
        ebml::write_int(&mut bg_body, ebml::REFERENCE_BLOCK, -40).unwrap();

        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        ebml::write_uint(&mut cluster, ebml::CLUSTER_TIMESTAMP, 100).unwrap();
        ebml::write_id(&mut cluster, ebml::BLOCK_GROUP).unwrap();
        ebml::write_size(&mut cluster, bg_body.len() as u64).unwrap();
        cluster.extend_from_slice(&bg_body);

        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        let mut tracks = Vec::new();
        for (n, t) in [(1u64, 1u64), (2u64, 2u64)] {
            let mut entry = Vec::new();
            ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, n).unwrap();
            ebml::write_uint(&mut entry, ebml::TRACK_TYPE, t).unwrap();
            ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
            ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
            tracks.extend_from_slice(&entry);
        }
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, tracks.len() as u64).unwrap();
        out.extend_from_slice(&tracks);
        out.extend_from_slice(&cluster);

        let mut stream = MkvStream::open(Cursor::new(out)).unwrap();
        let frame = stream
            .read()
            .unwrap()
            .expect("BlockGroup frame must be read");
        assert!(
            !frame.keyframe,
            "a BlockGroup WITH a ReferenceBlock must read back as a non-keyframe"
        );
        assert_eq!(frame.data, vec![0x11, 0x22, 0x33]);
        assert_eq!(frame.duration_ns, Some(40 * 1_000_000));
    }

    #[test]
    fn track_number_zero_is_rejected() {
        // A TRACK_ENTRY with TRACK_NUMBER 0 must be rejected (the ts_pid
        // computation would underflow `tnum - 2`).
        let bytes = mkv_with_track_and_cluster(0, 1, &[]);
        let e = open_err(MkvStream::open(Cursor::new(bytes)));
        assert!(is_mkv_source_invalid(&e));
    }

    #[test]
    fn track_number_above_u16_is_rejected() {
        // 65536 would truncate to 0 via `as u16` and then underflow.
        let bytes = mkv_with_track_and_cluster(65536, 1, &[]);
        let e = open_err(MkvStream::open(Cursor::new(bytes)));
        assert!(is_mkv_source_invalid(&e));
    }

    #[test]
    fn unknown_size_inner_child_in_tracks_is_rejected() {
        // A TRACK_ENTRY child declaring EBML unknown size (cs == u64::MAX) must
        // be rejected, not used in `hlen + cs` (which would overflow → debug
        // panic). Hand-build a TRACK_ENTRY whose first child carries the
        // unknown-size marker.
        let mut entry = Vec::new();
        ebml::write_id(&mut entry, ebml::TRACK_NUMBER).unwrap();
        ebml::write_unknown_size(&mut entry).unwrap(); // child size = unknown

        let mut tracks = Vec::new();
        ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
        tracks.extend_from_slice(&entry);

        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, tracks.len() as u64).unwrap();
        out.extend_from_slice(&tracks);

        let e = open_err(MkvStream::open(Cursor::new(out)));
        assert!(is_mkv_source_invalid(&e));
    }

    #[test]
    fn oversized_title_string_is_rejected() {
        // INFO/TITLE declaring a string above MAX_STRING_LEN must be
        // rejected during header parse, not allocated.
        let mut info = Vec::new();
        ebml::write_id(&mut info, ebml::TITLE).unwrap();
        ebml::write_size(&mut info, MAX_STRING_LEN + 1).unwrap();

        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, info.len() as u64).unwrap();
        out.extend_from_slice(&info);

        let e = match MkvStream::open(Cursor::new(out)) {
            Ok(_) => panic!("expected MkvSourceInvalid, got Ok"),
            Err(e) => e,
        };
        assert!(is_mkv_source_invalid(&e));
    }

    #[test]
    fn read_uint_val_len_nine_errors_not_panics() {
        // Direct helper test: an EBML uint cannot exceed 8 bytes. len=9
        // would index past the fixed 8-byte stack buffer and panic on
        // untrusted input; it must return MkvSourceInvalid instead.
        let mut data = Cursor::new(vec![0u8; 16]);
        let e = ebml::read_uint_val(&mut data, 9).unwrap_err();
        assert!(is_mkv_source_invalid(&e));
    }

    #[test]
    fn read_float_val_bad_width_errors() {
        // EBML floats are exactly 0, 4, or 8 bytes. Any other width is
        // malformed and must error rather than over- or under-read.
        let mut data = Cursor::new(vec![0u8; 16]);
        let e = ebml::read_float_val(&mut data, 5).unwrap_err();
        assert!(is_mkv_source_invalid(&e));
        // 0/4/8 remain valid widths.
        let mut z = Cursor::new(vec![0u8; 16]);
        assert_eq!(ebml::read_float_val(&mut z, 0).unwrap(), 0.0);
        let mut f4 = Cursor::new(vec![0u8; 16]);
        assert!(ebml::read_float_val(&mut f4, 4).is_ok());
        let mut f8 = Cursor::new(vec![0u8; 16]);
        assert!(ebml::read_float_val(&mut f8, 8).is_ok());
    }

    #[test]
    fn non_utf8_string_element_is_rejected() {
        // A string element with invalid UTF-8 bytes must surface a numeric
        // MkvSourceInvalid error, not an io::Error wrapping the FromUtf8Error
        // English message (library no-English rule).
        let mut data = Cursor::new(vec![0xFF, 0xFE, 0xFD, 0xFC]);
        let e = ebml::read_string_val(&mut data, 4).unwrap_err();
        assert!(is_mkv_source_invalid(&e));
    }

    #[test]
    fn simple_block_track_zero_is_skipped() {
        // A SimpleBlock with track vint 0 must be skipped, not attributed to
        // track 0. Build one track, then a cluster whose only block is track 0
        // followed by a valid track-1 block; read() must return the track-1 one.
        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        // track vint 0 is not directly encodable (0x80 is track 0 → block_vint
        // returns (0,1)); use 0x80 as the track byte.
        let bad = [0x80u8, 0x00, 0x00, 0x80, 0xEE];
        ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
        ebml::write_size(&mut cluster, bad.len() as u64).unwrap();
        cluster.extend_from_slice(&bad);
        let good = [0x81u8, 0x00, 0x00, 0x80, 0xAB, 0xCD];
        ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
        ebml::write_size(&mut cluster, good.len() as u64).unwrap();
        cluster.extend_from_slice(&good);

        let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let frame = stream.read().unwrap().expect("track-1 frame expected");
        assert_eq!(frame.track, 0);
        assert_eq!(frame.data, vec![0xAB, 0xCD]);
    }

    // ============================================================
    // block_vint — the (Simple)Block track-number VINT. Matroska §6.2:
    // the leading-1 bit position selects the width (1-4 bytes here), and
    // the value occupies the remaining bits. A width-selection bug would
    // mis-attribute every block to the wrong track.
    // ============================================================

    #[test]
    fn block_vint_width_selection_and_values() {
        // 1-byte: 0x81 → track 1 (high bit is the marker, low 7 = value).
        assert_eq!(block_vint(&[0x81]), (1, 1));
        assert_eq!(block_vint(&[0xFF]), (0x7F, 1)); // max 1-byte track
        // 2-byte: 0x40 marker, 14-bit value. 0x40 0x80 → 0x80.
        assert_eq!(block_vint(&[0x40, 0x80]), (0x80, 2));
        assert_eq!(block_vint(&[0x7F, 0xFF]), (0x3FFF, 2)); // max 2-byte
        // 3-byte: 0x20 marker, 21-bit value.
        assert_eq!(block_vint(&[0x20, 0x00, 0x01]), (1, 3));
        assert_eq!(block_vint(&[0x3F, 0xFF, 0xFF]), (0x1F_FFFF, 3));
        // 4-byte: 0x10 marker, 28-bit value.
        assert_eq!(block_vint(&[0x10, 0x00, 0x00, 0x01]), (1, 4));
        assert_eq!(block_vint(&[0x1F, 0xFF, 0xFF, 0xFF]), (0x0FFF_FFFF, 4));
    }

    #[test]
    fn block_vint_unsupported_and_truncated_forms() {
        // Empty input → (0, 0).
        assert_eq!(block_vint(&[]), (0, 0));
        // A 2-byte marker but only 1 byte available falls through to the
        // catch-all (0, 1) — treated as track 0 (skipped by parse_block).
        assert_eq!(block_vint(&[0x40]), (0, 1));
        // A 5+ byte VINT (0x08 marker) is unsupported → (0, 1), so the block
        // is skipped rather than mis-decoded.
        assert_eq!(block_vint(&[0x08, 0, 0, 0, 0]), (0, 1));
        // 0x00 first byte: no marker in bits 7..4 → unsupported → (0, 1).
        assert_eq!(block_vint(&[0x00, 0x11]), (0, 1));
    }

    // ============================================================
    // parse_block — turns a (Simple)Block payload into PesFrames.
    // Layout: [track VINT][rel_ts i16 BE][flags u8][data...].
    // Guards: len<4 → none; vl+3 > len → none; track 0 → none;
    // unknown TrackNumber → none.
    // ============================================================

    /// `parse_block` for an UNLACED block on a file whose TrackNumbers are
    /// `1..=streams_len` (the layout this crate's own writer emits): the single
    /// frame, or `None` when the block was skipped.
    fn parse_block_one(
        block: &[u8],
        cluster_ts_ticks: i64,
        ts_scale_ns: i64,
        streams_len: usize,
        duration_ns: Option<u64>,
    ) -> Option<crate::pes::PesFrame> {
        let frames = parse_block(
            block,
            cluster_ts_ticks,
            ts_scale_ns,
            &TrackTable::contiguous(streams_len),
            duration_ns,
        )
        .expect("unlaced block never errors");
        assert!(
            frames.len() <= 1,
            "an unlaced block yields at most one frame"
        );
        frames.into_iter().next()
    }

    #[test]
    fn parse_block_too_short_is_none() {
        // Fewer than 4 bytes can't hold vint(1)+ts(2)+flags(1); must be None.
        assert!(parse_block_one(&[0x81, 0x00, 0x00], 0, 1_000_000, 1, None).is_none());
        assert!(parse_block_one(&[], 0, 1_000_000, 1, None).is_none());
    }

    #[test]
    fn parse_block_header_longer_than_payload_is_none() {
        // A 2-byte track VINT (0x40 0x01) needs vl(2)+3 = 5 bytes minimum, but
        // only 4 are supplied → vl+3 > len → None (no OOB index of data slice).
        let block = [0x40u8, 0x01, 0x00, 0x00]; // len 4, vl 2 → 2+3=5 > 4
        assert!(parse_block_one(&block, 0, 1_000_000, 2, None).is_none());
    }

    #[test]
    fn parse_block_track_index_out_of_range_is_none() {
        // track 2 → index 1, but only 1 stream exists → must skip (None),
        // never index past the streams slice.
        let block = [0x82u8, 0x00, 0x00, 0x80, 0xAA]; // track 2
        assert!(parse_block_one(&block, 0, 1_000_000, 1, None).is_none());
        // With 2 streams it resolves to index 1.
        let f = parse_block_one(&block, 0, 1_000_000, 2, None).unwrap();
        assert_eq!(f.track, 1);
    }

    #[test]
    fn parse_block_pts_honours_timestamp_scale() {
        // PTS = (cluster_ts_ticks + rel_ts) * ts_scale_ns. With a non-1ms scale
        // the result must scale accordingly (foreign MKVs). rel_ts = 10 here.
        let block = [0x81u8, 0x00, 0x0A, 0x80, 0xAA]; // track 1, rel 10, kf
        // ts_scale 1_000_000 (1ms): cluster 100 + rel 10 = 110 ticks → 110ms.
        let f = parse_block_one(&block, 100, 1_000_000, 1, None).unwrap();
        assert_eq!(f.pts, 110 * 1_000_000);
        assert!(f.keyframe);
        // ts_scale 90_000 (90kHz): (100+10) * 90_000.
        let f = parse_block_one(&block, 100, 90_000, 1, None).unwrap();
        assert_eq!(f.pts, 110 * 90_000);
    }

    #[test]
    fn parse_block_negative_rel_ts_is_signed() {
        // rel_ts is a SIGNED 16-bit big-endian value. 0xFFFF = -1. The pts must
        // go DOWN from the cluster timestamp, not jump to +65535.
        let block = [0x81u8, 0xFF, 0xFF, 0x80, 0xAA]; // rel_ts = -1
        let f = parse_block_one(&block, 100, 1_000_000, 1, None).unwrap();
        assert_eq!(f.pts, 99 * 1_000_000, "rel_ts -1 must subtract one tick");
    }

    #[test]
    fn parse_block_keyframe_flag_and_duration_propagate() {
        // flags bit 0x80 = keyframe; a clear bit = delta frame. duration_ns is
        // passed through unchanged (BlockGroup path supplies it).
        let kf = [0x81u8, 0x00, 0x00, 0x80, 0xAA];
        let nkf = [0x81u8, 0x00, 0x00, 0x00, 0xAA];
        assert!(
            parse_block_one(&kf, 0, 1_000_000, 1, None)
                .unwrap()
                .keyframe
        );
        assert!(
            !parse_block_one(&nkf, 0, 1_000_000, 1, None)
                .unwrap()
                .keyframe
        );
        let f = parse_block_one(&kf, 0, 1_000_000, 1, Some(40_000_000)).unwrap();
        assert_eq!(f.duration_ns, Some(40_000_000));
    }

    #[test]
    fn parse_block_pts_saturates_no_overflow() {
        // A hostile cluster timestamp near i64::MAX must not panic on the
        // ticks→ns multiply; saturating_mul caps it. (Guards the debug-build
        // overflow the source comment calls out.)
        let block = [0x81u8, 0x00, 0x00, 0x80, 0xAA];
        let f = parse_block_one(&block, i64::MAX, 1_000_000, 1, None).unwrap();
        assert_eq!(f.pts, i64::MAX, "ticks→ns must saturate, not wrap/panic");
    }

    #[test]
    fn parse_block_cluster_ts_plus_rel_ts_saturates_no_overflow() {
        // Regression: a hostile CLUSTER_TIMESTAMP near i64::MAX plus a POSITIVE
        // rel_ts overflows the `cluster_ts + rel_ts` ADD — one step before the
        // saturating_mul. With a plain `+` this panics in debug/test (overflow
        // checks on) and silently wraps to a large negative PTS in release.
        // rel_ts = +0x7FFF = 32767 (max positive signed 16-bit).
        let block = [0x81u8, 0x7F, 0xFF, 0x80, 0xAA];
        let f = parse_block_one(&block, i64::MAX, 1_000_000, 1, None).unwrap();
        // The add saturates at i64::MAX, then the mul saturates too.
        assert_eq!(
            f.pts,
            i64::MAX,
            "cluster_ts + rel_ts must saturate, not panic/wrap"
        );
    }

    // ============================================================
    // ts_pid_for_track — mid-range mapping (the existing test covers the
    // edges; this fills in a representative middle value to lock the
    // 0x1100 + (tnum-2) formula).
    // ============================================================

    // ============================================================
    // CLUSTER_TIMESTAMP overflow guard — a value above i64::MAX would cast
    // to a large negative i64 and poison every block PTS in the cluster.
    // The reader must reject it.
    // ============================================================

    #[test]
    fn cluster_timestamp_above_i64_max_is_rejected() {
        // CLUSTER_TIMESTAMP encoded as an 8-byte uint with the top bit set
        // (> i64::MAX). The reader must surface MkvSourceInvalid on read().
        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        ebml::write_id(&mut cluster, ebml::CLUSTER_TIMESTAMP).unwrap();
        ebml::write_size(&mut cluster, 8).unwrap();
        cluster.extend_from_slice(&0xFFFF_FFFF_FFFF_FFFFu64.to_be_bytes());
        let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let e = stream.read().unwrap_err();
        assert!(is_mkv_source_invalid(&e));
    }

    // ============================================================
    // A malformed mkv:// SOURCE must never be classified as a skippable
    // title stub. The read path used to raise `Error::MkvInvalid` for every
    // malformed-input rejection, and `error::is_skippable_title_stub` reports
    // that code as an empty nav/menu stub — so an all-titles rip silently
    // passed over a corrupt input and exited reporting success.
    // ============================================================

    #[test]
    fn corrupt_source_is_not_classified_as_a_skippable_title_stub() {
        // Same corrupt fixture as above (CLUSTER_TIMESTAMP > i64::MAX) driven
        // through the real reader, asserted against the public classifier.
        // Mutation: raising `Error::MkvInvalid` instead of
        // `Error::MkvSourceInvalid` at that guard turns this red.
        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        ebml::write_id(&mut cluster, ebml::CLUSTER_TIMESTAMP).unwrap();
        ebml::write_size(&mut cluster, 8).unwrap();
        cluster.extend_from_slice(&0xFFFF_FFFF_FFFF_FFFFu64.to_be_bytes());
        let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let e = stream.read().unwrap_err();
        assert!(
            !crate::error::is_skippable_title_stub(&e),
            "a corrupt mkv:// source must be a failure, not a skippable stub: {e}"
        );
        assert_eq!(
            e.to_string(),
            format!("E{}", crate::error::E_MKV_SOURCE_INVALID)
        );
        // A truncated element body (the EBML read primitive) is the same verdict,
        // proving the classification is not specific to one guard.
        let short = ebml::read_binary_val(&mut Cursor::new(&[1u8, 2, 3, 4]), 100).unwrap_err();
        assert!(!crate::error::is_skippable_title_stub(&short));
        assert_eq!(
            short.to_string(),
            format!("E{}", crate::error::E_MKV_SOURCE_INVALID)
        );
    }

    // ============================================================
    // parse_mkv_header — TimestampScale threading and clamping. The frame
    // PTS path multiplies by ts_scale_ns; a zero or absurd scale must
    // clamp to the 1ms default rather than zero out / overflow PTS.
    // ============================================================

    #[test]
    fn zero_timestamp_scale_clamps_to_default() {
        // A foreign/corrupt INFO with TimestampScale 0 must clamp to 1_000_000
        // (1ms), so a rel_ts 5 block at cluster 100 still yields 105ms — not 0.
        let mut info = Vec::new();
        ebml::write_uint(&mut info, ebml::TIMESTAMP_SCALE, 0).unwrap();

        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
        let mut track_entry = Vec::new();
        ebml::write_id(&mut track_entry, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut track_entry, entry.len() as u64).unwrap();
        track_entry.extend_from_slice(&entry);

        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        ebml::write_uint(&mut cluster, ebml::CLUSTER_TIMESTAMP, 100).unwrap();
        let block = [0x81u8, 0x00, 0x05, 0x80, 0xAA];
        ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
        ebml::write_size(&mut cluster, block.len() as u64).unwrap();
        cluster.extend_from_slice(&block);

        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, info.len() as u64).unwrap();
        out.extend_from_slice(&info);
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, track_entry.len() as u64).unwrap();
        out.extend_from_slice(&track_entry);
        out.extend_from_slice(&cluster);

        let mut stream = MkvStream::open(Cursor::new(out)).unwrap();
        let f = stream.read().unwrap().expect("frame");
        assert_eq!(f.pts, 105 * 1_000_000, "zero scale must clamp to 1ms");
    }

    #[test]
    fn duration_uses_timestamp_scale_for_seconds() {
        // DURATION is a float in TimestampScale TICKS, not ms. With scale
        // 1_000_000 (1ms) and duration 5000 ticks → 5.0 s. The header parser
        // must convert via ticks * scale_ns / 1e9.
        let mut info = Vec::new();
        ebml::write_uint(&mut info, ebml::TIMESTAMP_SCALE, 1_000_000).unwrap();
        ebml::write_float(&mut info, ebml::DURATION, 5000.0).unwrap();

        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
        let mut track_entry = Vec::new();
        ebml::write_id(&mut track_entry, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut track_entry, entry.len() as u64).unwrap();
        track_entry.extend_from_slice(&entry);

        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, info.len() as u64).unwrap();
        out.extend_from_slice(&info);
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, track_entry.len() as u64).unwrap();
        out.extend_from_slice(&track_entry);

        let stream = MkvStream::open(Cursor::new(out)).unwrap();
        assert_eq!(stream.info().duration_secs, 5.0);
    }

    #[test]
    fn missing_ebml_header_is_rejected() {
        // A stream whose first element is not the EBML header (0x1A45DFA3) is
        // not a Matroska file and must be rejected.
        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap(); // wrong first element
        ebml::write_size(&mut out, 0).unwrap();
        let e = open_err(MkvStream::open(Cursor::new(out)));
        assert!(is_mkv_source_invalid(&e));
    }

    #[test]
    fn segment_must_follow_ebml_header() {
        // After a valid EBML header the next element must be the Segment; a
        // different element is malformed.
        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap(); // not SEGMENT
        ebml::write_size(&mut out, 0).unwrap();
        let e = open_err(MkvStream::open(Cursor::new(out)));
        assert!(is_mkv_source_invalid(&e));
    }

    #[test]
    fn track_type_to_codec_and_pid_mapping_round_trips() {
        // A video TRACK_ENTRY (type 1, codec HEVC) must map to a VideoStream
        // with the V_MPEGH/ISO/HEVC → Codec::Hevc translation and track 1 → PID
        // 0x1011. Confirms parse_track wiring end to end.
        let mut entry = Vec::new();
        ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, 1).unwrap();
        ebml::write_uint(&mut entry, ebml::TRACK_TYPE, 1).unwrap();
        ebml::write_string(&mut entry, ebml::CODEC_ID, ebml::CODEC_HEVC).unwrap();
        let mut track_entry = Vec::new();
        ebml::write_id(&mut track_entry, ebml::TRACK_ENTRY).unwrap();
        ebml::write_size(&mut track_entry, entry.len() as u64).unwrap();
        track_entry.extend_from_slice(&entry);

        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, track_entry.len() as u64).unwrap();
        out.extend_from_slice(&track_entry);

        let stream = MkvStream::open(Cursor::new(out)).unwrap();
        match &stream.info().streams[0] {
            crate::disc::Stream::Video(v) => {
                assert_eq!(v.codec, Codec::Hevc);
                assert_eq!(v.pid, 0x1011);
            }
            _ => panic!("expected video stream"),
        }
    }

    #[test]
    fn block_group_unknown_size_is_rejected() {
        // A BLOCK_GROUP declaring unknown size (u64::MAX) would loop draining
        // the stream; the reader must reject it as MkvSourceInvalid.
        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        ebml::write_id(&mut cluster, ebml::BLOCK_GROUP).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap(); // size = unknown
        let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let e = stream.read().unwrap_err();
        assert!(is_mkv_source_invalid(&e));
    }

    #[test]
    fn read_then_eof_returns_none() {
        // After the last block, a clean EOF on the next element header must
        // return Ok(None) (end of stream), not an error.
        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        let block = [0x81u8, 0x00, 0x00, 0x80, 0xAA];
        ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
        ebml::write_size(&mut cluster, block.len() as u64).unwrap();
        cluster.extend_from_slice(&block);
        let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        assert!(stream.read().unwrap().is_some(), "first frame");
        assert!(stream.read().unwrap().is_none(), "clean EOF → None");
    }

    // ============================================================
    // Block LACING (RFC 9559 §10.3) and TrackNumber→stream routing
    // (RFC 9559 §5.1.4.1.1).
    // ============================================================

    /// One TrackEntry description for `mkv_with_tracks_and_cluster`:
    /// (TrackNumber, TrackType, DefaultDuration ns, CodecPrivate).
    struct TrackSpec {
        tnum: u64,
        ttype: u64,
        default_duration_ns: Option<u64>,
        codec_private: Option<Vec<u8>>,
    }

    impl TrackSpec {
        fn new(tnum: u64, ttype: u64) -> Self {
            Self {
                tnum,
                ttype,
                default_duration_ns: None,
                codec_private: None,
            }
        }
        fn with_default_duration(mut self, ns: u64) -> Self {
            self.default_duration_ns = Some(ns);
            self
        }
        fn with_codec_private(mut self, cp: &[u8]) -> Self {
            self.codec_private = Some(cp.to_vec());
            self
        }
    }

    /// Build an MKV header with an arbitrary set of TrackEntries — arbitrary
    /// TrackNumbers, in arbitrary order — followed by `cluster_body`.
    fn mkv_with_tracks_and_cluster(specs: &[TrackSpec], cluster_body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        ebml::write_id(&mut out, ebml::EBML).unwrap();
        ebml::write_size(&mut out, 0).unwrap();
        ebml::write_id(&mut out, ebml::SEGMENT).unwrap();
        ebml::write_unknown_size(&mut out).unwrap();
        ebml::write_id(&mut out, ebml::INFO).unwrap();
        ebml::write_size(&mut out, 0).unwrap();

        let mut tracks = Vec::new();
        for s in specs {
            let mut entry = Vec::new();
            ebml::write_uint(&mut entry, ebml::TRACK_NUMBER, s.tnum).unwrap();
            ebml::write_uint(&mut entry, ebml::TRACK_TYPE, s.ttype).unwrap();
            if let Some(ns) = s.default_duration_ns {
                ebml::write_uint(&mut entry, ebml::DEFAULT_DURATION, ns).unwrap();
            }
            if let Some(cp) = &s.codec_private {
                ebml::write_binary(&mut entry, ebml::CODEC_PRIVATE, cp).unwrap();
            }
            ebml::write_id(&mut tracks, ebml::TRACK_ENTRY).unwrap();
            ebml::write_size(&mut tracks, entry.len() as u64).unwrap();
            tracks.extend_from_slice(&entry);
        }
        ebml::write_id(&mut out, ebml::TRACKS).unwrap();
        ebml::write_size(&mut out, tracks.len() as u64).unwrap();
        out.extend_from_slice(&tracks);
        out.extend_from_slice(cluster_body);
        out
    }

    /// Wrap one raw (Simple)Block payload in a Cluster with the given timestamp.
    fn cluster_with_simple_block(cluster_ts: u64, block: &[u8]) -> Vec<u8> {
        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        ebml::write_uint(&mut cluster, ebml::CLUSTER_TIMESTAMP, cluster_ts).unwrap();
        ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
        ebml::write_size(&mut cluster, block.len() as u64).unwrap();
        cluster.extend_from_slice(block);
        cluster
    }

    /// Drain every frame a reader will yield.
    fn drain(stream: &mut MkvStream) -> Vec<crate::pes::PesFrame> {
        let mut out = Vec::new();
        while let Some(f) = stream.read().expect("no read error") {
            out.push(f);
        }
        out
    }

    /// EBML lacing (RFC 9559 §10.3.3): the Lacing Head, the first frame's size as
    /// an unsigned VINT, then each later size as a SIGNED VINT difference from
    /// the previous one. Three frames of 3/4/5 octets must come out as THREE
    /// frames with byte-exact payloads.
    ///
    /// Regression (silent corruption): the reader took the Block payload verbatim
    /// and never looked at the LACING bits, so this Block became ONE 15-byte frame
    /// whose first three bytes are the lacing header — garbage handed to the codec
    /// parser with no error, and one timestamp for three frames.
    #[test]
    fn ebml_laced_block_yields_every_frame_with_exact_payloads() {
        // size 3 → 0x83 (VINT, value 3). size delta 4-3 = +1 → unsigned 1 + bias
        // (2^6-1 = 63) = 64 → 0xC0 with the VINT_MARKER.
        let mut block = vec![
            0x81, // TrackNumber 1
            0x00, 0x00, // rel_ts 0
            0x86, // KEY | LACING = 11b (EBML)
            0x02, // Lacing Head: 3 frames minus 1
            0x83, // first frame size = 3
            0xC0, // second frame size = previous + 1 = 4
        ];
        block.extend_from_slice(&[0xAA; 3]);
        block.extend_from_slice(&[0xBB; 4]);
        block.extend_from_slice(&[0xCC; 5]);

        // DefaultDuration 24 ms/frame is what §10.3.5 leaves the reader to space
        // the second and later frames by.
        let specs = [TrackSpec::new(1, 2).with_default_duration(24_000_000)];
        let bytes = mkv_with_tracks_and_cluster(&specs, &cluster_with_simple_block(100, &block));
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let frames = drain(&mut stream);

        assert_eq!(frames.len(), 3, "one laced Block carries three frames");
        assert_eq!(frames[0].data, vec![0xAA; 3], "frame 1 payload byte-exact");
        assert_eq!(frames[1].data, vec![0xBB; 4], "frame 2 payload byte-exact");
        assert_eq!(frames[2].data, vec![0xCC; 5], "frame 3 payload byte-exact");
        for f in &frames {
            assert_eq!(f.track, 0);
            assert!(f.keyframe, "the KEY flag covers the whole lace");
            assert_eq!(f.duration_ns, Some(24_000_000));
        }
        // The Block timestamp applies to the FIRST frame; the rest are spaced by
        // DefaultDuration (§10.3.5).
        assert_eq!(frames[0].pts, 100 * 1_000_000);
        assert_eq!(frames[1].pts, 100 * 1_000_000 + 24_000_000);
        assert_eq!(frames[2].pts, 100 * 1_000_000 + 48_000_000);
    }

    /// Xiph lacing (RFC 9559 §10.3.2): sizes are runs of 0xFF octets terminated
    /// by an octet below 255, and a size that is a multiple of 255 ends in a 0.
    #[test]
    fn xiph_laced_block_splits_on_255_coded_sizes() {
        let mut block = vec![
            0x81, // TrackNumber 1
            0x00, 0x00, // rel_ts 0
            0x82, // KEY | LACING = 01b (Xiph)
            0x01, // Lacing Head: 2 frames minus 1
            0xFF, 0x00, // first frame size = 255 (a multiple of 255 → trailing 0)
        ];
        block.extend_from_slice(&[0xAA; 255]);
        block.extend_from_slice(&[0xBB; 2]);

        let specs = [TrackSpec::new(1, 2)];
        let bytes = mkv_with_tracks_and_cluster(&specs, &cluster_with_simple_block(0, &block));
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let frames = drain(&mut stream);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].data, vec![0xAA; 255]);
        assert_eq!(
            frames[1].data,
            vec![0xBB; 2],
            "the last frame's size is the Block remainder"
        );
    }

    /// Fixed-size lacing (RFC 9559 §10.3.4): no sizes are stored; every frame is
    /// the Block remainder divided by the frame count.
    #[test]
    fn fixed_size_laced_block_splits_evenly() {
        let mut block = vec![
            0x81, // TrackNumber 1
            0x00, 0x00, // rel_ts 0
            0x84, // KEY | LACING = 10b (fixed-size)
            0x02, // Lacing Head: 3 frames minus 1
        ];
        block.extend_from_slice(&[0x11, 0x11, 0x22, 0x22, 0x33, 0x33]);

        let specs = [TrackSpec::new(1, 2)];
        let bytes = mkv_with_tracks_and_cluster(&specs, &cluster_with_simple_block(0, &block));
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let frames = drain(&mut stream);
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].data, vec![0x11, 0x11]);
        assert_eq!(frames[1].data, vec![0x22, 0x22]);
        assert_eq!(frames[2].data, vec![0x33, 0x33]);
    }

    /// A lacing header whose declared sizes do not fit in the Block leaves the
    /// frame boundaries unknowable. That MUST be an error, never a pass-through
    /// of the raw payload as one frame.
    #[test]
    fn malformed_lacing_header_is_rejected_not_passed_through() {
        // Xiph, 2 frames, first size declared as 200 but only 4 payload octets
        // follow → the remainder for the last frame underflows.
        let block = [
            0x81, 0x00, 0x00, 0x82, // KEY | Xiph lacing
            0x01, // 2 frames
            0xC8, // first frame size = 200
            0xAA, 0xBB, 0xCC, 0xDD,
        ];
        let specs = [TrackSpec::new(1, 2)];
        let bytes = mkv_with_tracks_and_cluster(&specs, &cluster_with_simple_block(0, &block));
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let e = stream.read().unwrap_err();
        assert!(
            has_code(&e, crate::error::E_MKV_LACING_INVALID),
            "malformed lacing must be rejected"
        );
        assert!(
            !crate::error::is_skippable_title_stub(&e),
            "a track whose frames cannot be separated is NOT an empty nav stub"
        );

        // Fixed-size lacing whose body does not divide evenly by the frame count.
        let block = [
            0x81, 0x00, 0x00, 0x84, // KEY | fixed-size lacing
            0x02, // 3 frames
            0xAA, 0xBB, 0xCC, 0xDD, // 4 octets — not divisible by 3
        ];
        let bytes = mkv_with_tracks_and_cluster(&specs, &cluster_with_simple_block(0, &block));
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        assert!(has_code(
            &stream.read().unwrap_err(),
            crate::error::E_MKV_LACING_INVALID
        ));
    }

    /// A laced Block on a track with no DefaultDuration falls back to spreading
    /// the BlockGroup's BlockDuration across the lace: the Block's duration covers
    /// the WHOLE lace (RFC 9559 §5.1.3.5), not each frame.
    #[test]
    fn laced_block_duration_is_divided_across_the_lace() {
        let mut block = vec![0x81u8, 0x00, 0x00, 0x04, 0x01]; // fixed-size, 2 frames, no KEY
        block.extend_from_slice(&[0x11, 0x22]);
        let mut bg_body = Vec::new();
        ebml::write_id(&mut bg_body, ebml::BLOCK).unwrap();
        ebml::write_size(&mut bg_body, block.len() as u64).unwrap();
        bg_body.extend_from_slice(&block);
        // 48 ms for the pair → 24 ms per frame.
        ebml::write_uint(&mut bg_body, ebml::BLOCK_DURATION, 48).unwrap();

        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        ebml::write_id(&mut cluster, ebml::BLOCK_GROUP).unwrap();
        ebml::write_size(&mut cluster, bg_body.len() as u64).unwrap();
        cluster.extend_from_slice(&bg_body);

        let specs = [TrackSpec::new(1, 2)];
        let bytes = mkv_with_tracks_and_cluster(&specs, &cluster);
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let frames = drain(&mut stream);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].data, vec![0x11]);
        assert_eq!(frames[1].data, vec![0x22]);
        assert_eq!(frames[0].duration_ns, Some(24_000_000));
        assert_eq!(frames[1].pts, 24_000_000, "spaced by the derived duration");
    }

    /// RFC 9559 §5.1.4.1.1 constrains TrackNumber only to be non-zero — nothing
    /// requires `1..=N` in TrackEntry order. A file with a TrackType this reader
    /// drops (18 = buttons) between two carried tracks makes the TrackNumber
    /// space and the stream vector diverge.
    ///
    /// Regression (silent corruption): the reader computed the stream index as
    /// `TrackNumber - 1`, so the audio blocks of TrackNumber 3 resolved to index
    /// 2 in a 2-stream title and were DISCARDED — a remux with no audio, reported
    /// as success.
    /// A legal SamplingFrequency below the lowest rate this enum maps must come
    /// back as Unknown, not silently as 48 kHz.
    ///
    /// The ladder's final `else` was `SampleRate::S48`, so a 32000 Hz AC-3 or DTS
    /// track — legal, and common in broadcast-sourced content — was recorded as
    /// 48 kHz and the wrong rate propagated into the reconstructed AudioStream.
    /// The crate's canonical mapping, `SampleRate::from_hz`, returns Unknown for
    /// 32000; this ladder disagreed with it.
    #[test]
    fn a_sub_44100_sampling_frequency_is_unknown_not_48k() {
        /// One TrackEntry body: an audio track with the given sampling frequency.
        fn audio_track_body(freq: f64) -> Vec<u8> {
            let mut audio = Vec::new();
            audio.push(super::ebml::SAMPLING_FREQUENCY as u8);
            audio.push(0x88); // 8-byte float payload
            audio.extend_from_slice(&freq.to_be_bytes());
            audio.push(super::ebml::CHANNELS as u8);
            audio.extend_from_slice(&[0x81, 0x02]);

            let mut body = Vec::new();
            body.push(super::ebml::TRACK_NUMBER as u8);
            body.extend_from_slice(&[0x81, 0x01]);
            body.push(super::ebml::TRACK_TYPE as u8);
            body.extend_from_slice(&[0x81, super::ebml::TRACK_TYPE_AUDIO as u8]);
            body.push(super::ebml::CODEC_ID as u8);
            let cid = b"A_AC3";
            body.push(0x80 | cid.len() as u8);
            body.extend_from_slice(cid);
            body.push(super::ebml::AUDIO as u8);
            body.push(0x80 | audio.len() as u8);
            body.extend_from_slice(&audio);
            body
        }

        for (freq, want) in [
            (32000.0f64, SampleRate::Unknown),
            (16000.0, SampleRate::Unknown),
            (44100.0, SampleRate::S44_1),
            (48000.0, SampleRate::S48),
            (96000.0, SampleRate::S96),
        ] {
            let body = audio_track_body(freq);
            let mut cur = std::io::Cursor::new(body.clone());
            let parsed = super::parse_track(&mut cur, body.len() as u64)
                .unwrap_or_else(|e| panic!("track with {freq} Hz must parse: {e}"));
            let got = match parsed.0.as_ref().expect("an audio track yields a stream") {
                Stream::Audio(a) => a.sample_rate,
                other => panic!("expected an audio stream, got {other:?}"),
            };
            assert_eq!(got, want, "{freq} Hz must map to {want:?}, got {got:?}");
        }
    }

    #[test]
    fn sparse_track_numbers_route_to_the_right_stream() {
        let video = [0x81u8, 0x00, 0x00, 0x80, 0x11]; // TrackNumber 1
        let audio = [0x83u8, 0x00, 0x0A, 0x80, 0x22]; // TrackNumber 3, rel_ts 10
        let buttons = [0x82u8, 0x00, 0x00, 0x80, 0x33]; // TrackNumber 2 — dropped track

        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        for b in [video.as_slice(), buttons.as_slice(), audio.as_slice()] {
            ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
            ebml::write_size(&mut cluster, b.len() as u64).unwrap();
            cluster.extend_from_slice(b);
        }

        let specs = [
            TrackSpec::new(1, 1),  // video   → stream 0
            TrackSpec::new(2, 18), // buttons → dropped, no stream
            TrackSpec::new(3, 2),  // audio   → stream 1
        ];
        let bytes = mkv_with_tracks_and_cluster(&specs, &cluster);
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        assert_eq!(
            stream.info().streams.len(),
            2,
            "the buttons track is dropped"
        );
        let frames = drain(&mut stream);
        assert_eq!(
            frames.len(),
            2,
            "the video and audio blocks both survive; only the dropped track's do not"
        );
        assert_eq!(frames[0].track, 0, "TrackNumber 1 → stream 0");
        assert_eq!(frames[0].data, vec![0x11]);
        assert_eq!(frames[1].track, 1, "TrackNumber 3 → stream 1, not dropped");
        assert_eq!(frames[1].data, vec![0x22]);
    }

    /// A descending TrackEntry order is legal too: the map is by number, not by
    /// position, and a block must never be attributed to the wrong codec parser.
    #[test]
    fn descending_track_numbers_route_by_number_not_position() {
        let first = [0x87u8, 0x00, 0x00, 0x80, 0xAA]; // TrackNumber 7
        let second = [0x84u8, 0x00, 0x00, 0x80, 0xBB]; // TrackNumber 4
        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        for b in [first.as_slice(), second.as_slice()] {
            ebml::write_id(&mut cluster, ebml::SIMPLE_BLOCK).unwrap();
            ebml::write_size(&mut cluster, b.len() as u64).unwrap();
            cluster.extend_from_slice(b);
        }
        let specs = [TrackSpec::new(7, 1), TrackSpec::new(4, 2)];
        let bytes = mkv_with_tracks_and_cluster(&specs, &cluster);
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let frames = drain(&mut stream);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].track, 0, "TrackNumber 7 is the FIRST TrackEntry");
        assert_eq!(frames[1].track, 1, "TrackNumber 4 is the second");
    }

    /// `codec_private(stream_idx)` is keyed by TrackNumber internally, so it must
    /// translate through the same map — not assume `stream_idx + 1`.
    #[test]
    fn codec_private_resolves_through_the_track_number_map() {
        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        let specs = [
            TrackSpec::new(1, 1).with_codec_private(&[0x01, 0x02]),
            TrackSpec::new(2, 18).with_codec_private(&[0xDE, 0xAD]),
            TrackSpec::new(3, 2).with_codec_private(&[0x03, 0x04]),
        ];
        let bytes = mkv_with_tracks_and_cluster(&specs, &cluster);
        let stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        assert_eq!(stream.codec_private(0), Some(vec![0x01, 0x02]));
        assert_eq!(
            stream.codec_private(1),
            Some(vec![0x03, 0x04]),
            "stream 1 is TrackNumber 3 — not TrackNumber 2 (the dropped track)"
        );
        assert_eq!(stream.codec_private(2), None, "no third stream");
    }

    /// The signed-VINT bias of RFC 9559 §10.3.3 exactly as the spec's own EBML
    /// lacing example encodes it (800 then 500 → a delta of -300).
    #[test]
    fn lace_vint_matches_the_spec_worked_example() {
        // 800 = 0x320, encoded as a 2-octet VINT: 0x43 0x20.
        assert_eq!(lace_vint(&[0x43, 0x20]), Some((800, 2)));
        // -300 as a 2-octet signed VINT: 0x5E 0xD3 (value 0x1ED3 minus bias 8191).
        assert_eq!(lace_svint(&[0x5E, 0xD3]), Some((-300, 2)));
        // 1-octet forms: 0x81 → 1; signed 0x80 → -(2^6-1) = -63.
        assert_eq!(lace_vint(&[0x81]), Some((1, 1)));
        assert_eq!(lace_svint(&[0x80]), Some((-63, 1)));
        // A first octet of 0 has no VINT_MARKER within 8 octets → unrepresentable.
        assert!(lace_vint(&[0x00, 0x01]).is_none());
        // Truncated: a 2-octet marker with only one octet available.
        assert!(lace_vint(&[0x43]).is_none());
    }
}
