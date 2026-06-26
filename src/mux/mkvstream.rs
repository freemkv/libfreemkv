//! MkvStream — Matroska container stream.
//!
//! Read: MKV container → demux EBML → PES frames out.
//! Write: PES frames in → MKV mux → Matroska container.

use super::mkv::{MkvMuxer, MkvTrack};
use super::{WriteSeek, ebml};

/// (title, codec_privates, ts_scale_ns) — `ts_scale_ns` is the
/// TimestampScale in nanoseconds per tick, threaded into the frame read path.
type MkvHeaderResult = io::Result<(crate::disc::DiscTitle, Vec<(u16, Vec<u8>)>, i64)>;

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
        return Err(crate::error::Error::MkvInvalid.into());
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
}

/// Safety cap on frames buffered before the first video frame triggers muxer
/// construction. The first video frame normally arrives within the first few
/// frames, so this is only a backstop for a pathological audio-only-prefix
/// stream — past it we build with no measured field order (logged) rather than
/// buffer unbounded.
const MAX_PENDING_FRAMES: usize = 4096;

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
    /// Frames received before activation, replayed in order once built.
    buffered: Vec<crate::pes::PesFrame>,
}

/// Matroska container stream.
pub struct MkvStream {
    disc_title: DiscTitle,
    mode: Mode,
}

impl MkvStream {
    /// Create for writing PES frames → MKV container.
    /// Codec privates come from title.codec_privates (populated by input stream).
    pub fn create(writer: Box<dyn WriteSeek + Send>, title: &DiscTitle) -> io::Result<Self> {
        Self::create_at(writer, title, None)
    }

    /// As [`create`](Self::create), but `output_path` (when known) enables the
    /// `--log-level 3` opening-frame capture to `<output>.opening.bin`. A `None`
    /// path (e.g. an in-memory / stdio sink) silently skips the side-file
    /// capture; the per-track TrackEntry dump still fires.
    pub fn create_at(
        writer: Box<dyn WriteSeek + Send>,
        title: &DiscTitle,
        output_path: Option<&std::path::Path>,
    ) -> io::Result<Self> {
        let mut tracks = Vec::new();
        let mut has_default_video = false;
        let mut has_default_audio = false;
        for (idx, s) in title.streams.iter().enumerate() {
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
            tracks.push(track);
        }

        // Defer muxer construction (and the TrackEntry dump) until the first
        // coded picture arrives, so the primary video track's FieldOrder is set
        // from the parser's MEASURED value before the header is written — never
        // a guess. The dump moves to activation so it reflects the final track.
        let video_track = tracks.iter().position(|t| t.track_type == 1);

        Ok(Self {
            disc_title: title.clone(),
            mode: Mode::Write(WriteMode::Pending(Box::new(PendingMux {
                writer,
                tracks,
                video_track,
                opening_capture_path: output_path.map(|p| p.to_path_buf()),
                buffered: Vec::new(),
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
        for f in pending.buffered.drain(..) {
            muxer.write_frame(f.track, f.pts, f.keyframe, &f.data, f.duration_ns)?;
        }
        self.mode = Mode::Write(WriteMode::Active(Box::new(muxer)));
        Ok(())
    }

    /// Open an MKV file for reading → PES frames.
    pub fn open(mut reader: impl Read + Send + 'static) -> io::Result<Self> {
        let (disc_title, codec_privates, ts_scale_ns) = parse_mkv_header(&mut reader)?;
        Ok(Self {
            disc_title,
            mode: Mode::Read(ReadState {
                reader: Box::new(reader),
                cluster_ts_ticks: 0,
                ts_scale_ns,
                codec_privates,
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
        let streams_len = self.disc_title.streams.len();
        let rs = match self.mode {
            Mode::Read(ref mut rs) => rs,
            Mode::Write(_) => return Err(crate::error::Error::StreamWriteOnly.into()),
        };

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
                        return Err(crate::error::Error::MkvInvalid.into());
                    }
                    rs.cluster_ts_ticks = raw as i64;
                    continue;
                }
                ebml::SIMPLE_BLOCK => {
                    let block =
                        ebml::read_binary_val(&mut rs.reader, checked_size(size, MAX_BLOCK_SIZE)?)?;
                    if let Some(frame) = parse_block(
                        &block,
                        rs.cluster_ts_ticks,
                        rs.ts_scale_ns,
                        streams_len,
                        None,
                    ) {
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
                        return Err(crate::error::Error::MkvInvalid.into());
                    }
                    let mut remaining = size;
                    let mut block: Option<Vec<u8>> = None;
                    let mut duration_ms: Option<u64> = None;
                    while remaining > 0 {
                        let (cid, cs, hlen) = ebml::read_element_header(&mut rs.reader)?;
                        if cs == u64::MAX {
                            return Err(crate::error::Error::MkvInvalid.into());
                        }
                        // A child whose header + body exceeds the bytes left in
                        // the BlockGroup is malformed — reject it rather than
                        // saturating `remaining` to 0 and reading past the group.
                        let consumed = (hlen as u64).saturating_add(cs);
                        if consumed > remaining {
                            return Err(crate::error::Error::MkvInvalid.into());
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
                        if let Some(frame) = parse_block(
                            &block,
                            rs.cluster_ts_ticks,
                            rs.ts_scale_ns,
                            streams_len,
                            dur_ns,
                        ) {
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
                        return Err(crate::error::Error::MkvInvalid.into());
                    }
                    skip_bytes(&mut rs.reader, size)?;
                    continue;
                }
            }
        }
    }

    fn write(&mut self, frame: &crate::pes::PesFrame) -> io::Result<()> {
        // Fast paths.
        match &mut self.mode {
            Mode::Read(_) => return Err(crate::error::Error::StreamReadOnly.into()),
            Mode::Write(WriteMode::Active(m)) => {
                return m.write_frame(
                    frame.track,
                    frame.pts,
                    frame.keyframe,
                    &frame.data,
                    frame.duration_ns,
                );
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
                (is_video || p.buffered.len() >= MAX_PENDING_FRAMES, is_video)
            }
            _ => unreachable!("guarded above"),
        };
        if activate_now {
            // Pass the trigger frame's coding only when it IS the video frame; a
            // cap-triggered build never saw the video frame, so nothing measured
            // is passed (apply_coding_to_track then logs + leaves UNDETERMINED).
            self.activate(if use_coding { frame.coding } else { None }, use_coding)?;
            if let Mode::Write(WriteMode::Active(m)) = &mut self.mode {
                return m.write_frame(
                    frame.track,
                    frame.pts,
                    frame.keyframe,
                    &frame.data,
                    frame.duration_ns,
                );
            }
            Ok(())
        } else {
            if let Mode::Write(WriteMode::Pending(p)) = &mut self.mode {
                p.buffered.push(frame.clone());
            }
            Ok(())
        }
    }

    fn finish(&mut self) -> io::Result<()> {
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
        let track_num = (track + 1) as u16; // MKV tracks are 1-based
        if let Mode::Read(ref rs) = self.mode {
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

    let (id, size, _) = ebml::read_element_header(r)?;
    if id != ebml::EBML {
        return Err(crate::error::Error::MkvInvalid.into());
    }
    if size > i64::MAX as u64 {
        return Err(crate::error::Error::MkvInvalid.into());
    }
    skip_bytes(r, size)?;

    let (id, _, _) = ebml::read_element_header(r)?;
    if id != ebml::SEGMENT {
        return Err(crate::error::Error::MkvInvalid.into());
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
                // an EOF read error instead of a clean MkvInvalid; reject it for
                // parity with the segment loop guard below.
                if size == u64::MAX {
                    return Err(crate::error::Error::MkvInvalid.into());
                }
                let mut remaining = size;
                while remaining > 0 {
                    let (cid, cs, hlen) = ebml::read_element_header(r)?;
                    // An inner child declaring EBML unknown size (cs == u64::MAX)
                    // would overflow `hlen + cs` (debug panic) and is meaningless
                    // for a sized parent — reject it.
                    if cs == u64::MAX {
                        return Err(crate::error::Error::MkvInvalid.into());
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
                    return Err(crate::error::Error::MkvInvalid.into());
                }
                let mut remaining = size;
                while remaining > 0 {
                    let (cid, cs, hlen) = ebml::read_element_header(r)?;
                    if cs == u64::MAX {
                        return Err(crate::error::Error::MkvInvalid.into());
                    }
                    remaining = remaining.saturating_sub(hlen as u64 + cs);
                    if cid == ebml::TRACK_ENTRY {
                        let (stream, tnum, cp) = parse_track(r, cs)?;
                        if let Some(s) = stream {
                            streams.push(s);
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
    Ok((disc_title, codec_privates, ts_scale_ns))
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
        return Err(crate::error::Error::MkvInvalid.into());
    }
    let pid: u32 = if tnum == 1 {
        0x1011
    } else {
        0x1100u32 + (tnum as u32 - 2)
    };
    if pid > MAX_TS_PID {
        return Err(crate::error::Error::MkvInvalid.into());
    }
    Ok(pid as u16)
}

/// Returns (stream, track_number, codec_private_bytes)
fn parse_track(
    r: &mut impl Read,
    size: u64,
) -> io::Result<(Option<crate::disc::Stream>, u16, Option<Vec<u8>>)> {
    let (mut ttype, mut tnum) = (0u64, 0u16);
    let (mut codec_id, mut lang, mut name) = (String::new(), String::from("und"), String::new());
    let (mut ph, mut sr, mut ch, mut forced) = (0u32, 0.0f64, 0u8, false);
    let mut codec_priv: Option<Vec<u8>> = None;

    let mut remaining = size;
    while remaining > 0 {
        let (cid, cs, hlen) = ebml::read_element_header(r)?;
        if cs == u64::MAX {
            return Err(crate::error::Error::MkvInvalid.into());
        }
        remaining = remaining.saturating_sub(hlen as u64 + cs);
        match cid {
            ebml::TRACK_NUMBER => {
                // Reject a TRACK_NUMBER above u16::MAX rather than truncating
                // with `as u16` (which would alias 65536→0, 65537→1, … onto
                // existing small track numbers and corrupt PID/codec lookup).
                let n = read_uint_bounded(r, cs)?;
                if n > u16::MAX as u64 {
                    return Err(crate::error::Error::MkvInvalid.into());
                }
                tnum = n as u16;
            }
            ebml::TRACK_TYPE => ttype = read_uint_bounded(r, cs)?,
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
                        return Err(crate::error::Error::MkvInvalid.into());
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
                        return Err(crate::error::Error::MkvInvalid.into());
                    }
                    arem = arem.saturating_sub(ahlen as u64 + as_);
                    match aid {
                        ebml::SAMPLING_FREQUENCY => sr = ebml::read_float_val(r, as_ as usize)?,
                        ebml::CHANNELS => ch = read_uint_bounded(r, as_)? as u8,
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
    } else {
        SampleRate::S48
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
    Ok((stream, tnum, codec_priv))
}

/// Parse a (Simple)Block payload into a PesFrame, or `None` if it should be
/// skipped (too short, track 0, or a track index out of range).
///
/// `cluster_ts_ticks` is the open cluster's timestamp in TimestampScale ticks
/// and `ts_scale_ns` is that scale (ns per tick); the block PTS is computed as
/// `(cluster_ts_ticks + rel_ts) * ts_scale_ns` so foreign MKVs whose scale
/// isn't 1 ms are honoured (freemkv's own output uses 1_000_000 and round-trips
/// unchanged). `streams_len` bounds the resolved track index; `duration_ns` is
/// propagated for BlockGroup blocks (None for SimpleBlock).
fn parse_block(
    block: &[u8],
    cluster_ts_ticks: i64,
    ts_scale_ns: i64,
    streams_len: usize,
    duration_ns: Option<u64>,
) -> Option<crate::pes::PesFrame> {
    if block.len() < 4 {
        return None;
    }
    let (track, vl) = block_vint(block);
    if vl + 3 > block.len() {
        return None;
    }
    // Track 0 is invalid (MKV track numbers are 1-based). block_vint also
    // returns 0 for an unsupported 5+ byte VINT, so a corrupt/zero-track block
    // must be skipped rather than attributed to the first stream.
    if track == 0 {
        return None;
    }

    let rel_ts = i16::from_be_bytes([block[vl], block[vl + 1]]);
    let keyframe = block[vl + 2] & 0x80 != 0;
    let data = block[vl + 3..].to_vec();
    // saturating_add: a hostile CLUSTER_TIMESTAMP near i64::MAX plus a positive
    // rel_ts would overflow this add (panic in debug/test, wrap to a large
    // negative PTS in release) — one operation BEFORE the saturating_mul below.
    // rel_ts as i64 is exact, so this fully bounds the sum on adversarial input.
    let pts_ticks = cluster_ts_ticks.saturating_add(rel_ts as i64);
    let track_idx = (track as usize) - 1; // track >= 1 checked above

    // Skip blocks for non-existent tracks.
    if track_idx >= streams_len {
        return None;
    }

    Some(crate::pes::PesFrame {
        coding: None,
        source: None,
        track: track_idx,
        // saturating_mul: a hostile CLUSTER_TIMESTAMP could push pts_ticks near
        // i64::MAX, where ticks→ns would overflow and panic in debug builds.
        pts: pts_ticks.saturating_mul(ts_scale_ns),
        keyframe,
        data,
        duration_ns,
    })
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
    use super::*;
    use crate::pes::Stream as _;
    use std::io::Cursor;

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

    fn is_mkv_invalid(e: &io::Error) -> bool {
        e.kind() == io::ErrorKind::InvalidData
            && e.to_string()
                .starts_with(&format!("E{}", crate::error::E_MKV_INVALID))
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
        assert!(is_mkv_invalid(&ts_pid_for_track(0xF02).unwrap_err()));
        // Former overflow case (debug panic / release garbage PID) is rejected.
        assert!(is_mkv_invalid(&ts_pid_for_track(u16::MAX).unwrap_err()));
        // Track 0 is invalid (1-based) and would underflow tnum-2.
        assert!(is_mkv_invalid(&ts_pid_for_track(0).unwrap_err()));
    }

    #[test]
    fn checked_size_rejects_over_cap() {
        // Within cap → Ok with usize value.
        assert_eq!(checked_size(100, 256).unwrap(), 100);
        assert_eq!(checked_size(256, 256).unwrap(), 256);
        // Over cap → MkvInvalid, never a giant allocation.
        let e = checked_size(257, 256).unwrap_err();
        assert!(is_mkv_invalid(&e));
        // A hostile multi-GB block size is rejected as MkvInvalid.
        let e = checked_size(4 * 1024 * 1024 * 1024, MAX_BLOCK_SIZE).unwrap_err();
        assert!(is_mkv_invalid(&e));
    }

    #[test]
    fn read_uint_bounded_rejects_oversized_int() {
        // size > 8 would index out of the fixed 8-byte buffer in
        // read_uint_val (panic / OOB). The guard turns it into a clean
        // MkvInvalid error instead.
        let mut data = Cursor::new(vec![0u8; 16]);
        let e = read_uint_bounded(&mut data, 9).unwrap_err();
        assert!(is_mkv_invalid(&e));
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
        assert!(is_mkv_invalid(&e));
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
        // The reader must reject it (MkvInvalid) rather than attempt a
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
        assert!(is_mkv_invalid(&e));
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
        // read_exact_bounded must surface a clean typed MkvInvalid error
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
        assert!(is_mkv_invalid(&e));
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
        // MAX_CODEC_PRIVATE must be rejected (MkvInvalid) before any
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
            Ok(_) => panic!("expected MkvInvalid, got Ok"),
            Err(e) => e,
        };
        assert!(is_mkv_invalid(&e));
    }

    #[test]
    fn block_group_frame_round_trips_with_duration() {
        // MkvMuxer emits AC3/PGS frames as a BlockGroup (BLOCK + BLOCK_DURATION).
        // The reader must descend into the group and yield the frame (with its
        // duration) rather than skipping it — otherwise every AC3/PGS frame this
        // muxer writes is lost on read-back.
        let block = [0x82u8, 0x00, 0x05, 0x00, 0x11, 0x22, 0x33]; // track 2, rel 5, not-kf, 3 data
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
        assert!(!frame.keyframe);
        assert_eq!(frame.data, vec![0x11, 0x22, 0x33]);
        assert_eq!(frame.pts, 105 * 1_000_000, "pts = (cluster 100 + rel 5) ms");
        assert_eq!(frame.duration_ns, Some(40 * 1_000_000));
    }

    #[test]
    fn track_number_zero_is_rejected() {
        // A TRACK_ENTRY with TRACK_NUMBER 0 must be rejected (the ts_pid
        // computation would underflow `tnum - 2`).
        let bytes = mkv_with_track_and_cluster(0, 1, &[]);
        let e = open_err(MkvStream::open(Cursor::new(bytes)));
        assert!(is_mkv_invalid(&e));
    }

    #[test]
    fn track_number_above_u16_is_rejected() {
        // 65536 would truncate to 0 via `as u16` and then underflow.
        let bytes = mkv_with_track_and_cluster(65536, 1, &[]);
        let e = open_err(MkvStream::open(Cursor::new(bytes)));
        assert!(is_mkv_invalid(&e));
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
        assert!(is_mkv_invalid(&e));
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
            Ok(_) => panic!("expected MkvInvalid, got Ok"),
            Err(e) => e,
        };
        assert!(is_mkv_invalid(&e));
    }

    #[test]
    fn read_uint_val_len_nine_errors_not_panics() {
        // Direct helper test: an EBML uint cannot exceed 8 bytes. len=9
        // would index past the fixed 8-byte stack buffer and panic on
        // untrusted input; it must return MkvInvalid instead.
        let mut data = Cursor::new(vec![0u8; 16]);
        let e = ebml::read_uint_val(&mut data, 9).unwrap_err();
        assert!(is_mkv_invalid(&e));
    }

    #[test]
    fn read_float_val_bad_width_errors() {
        // EBML floats are exactly 0, 4, or 8 bytes. Any other width is
        // malformed and must error rather than over- or under-read.
        let mut data = Cursor::new(vec![0u8; 16]);
        let e = ebml::read_float_val(&mut data, 5).unwrap_err();
        assert!(is_mkv_invalid(&e));
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
        // MkvInvalid error, not an io::Error wrapping the FromUtf8Error
        // English message (library no-English rule).
        let mut data = Cursor::new(vec![0xFF, 0xFE, 0xFD, 0xFC]);
        let e = ebml::read_string_val(&mut data, 4).unwrap_err();
        assert!(is_mkv_invalid(&e));
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
    // parse_block — turns a (Simple)Block payload into a PesFrame.
    // Layout: [track VINT][rel_ts i16 BE][flags u8][data...].
    // Guards: len<4 → None; vl+3 > len → None; track 0 → None;
    // track_idx >= streams_len → None.
    // ============================================================

    #[test]
    fn parse_block_too_short_is_none() {
        // Fewer than 4 bytes can't hold vint(1)+ts(2)+flags(1); must be None.
        assert!(parse_block(&[0x81, 0x00, 0x00], 0, 1_000_000, 1, None).is_none());
        assert!(parse_block(&[], 0, 1_000_000, 1, None).is_none());
    }

    #[test]
    fn parse_block_header_longer_than_payload_is_none() {
        // A 2-byte track VINT (0x40 0x01) needs vl(2)+3 = 5 bytes minimum, but
        // only 4 are supplied → vl+3 > len → None (no OOB index of data slice).
        let block = [0x40u8, 0x01, 0x00, 0x00]; // len 4, vl 2 → 2+3=5 > 4
        assert!(parse_block(&block, 0, 1_000_000, 2, None).is_none());
    }

    #[test]
    fn parse_block_track_index_out_of_range_is_none() {
        // track 2 → index 1, but only 1 stream exists → must skip (None),
        // never index past the streams slice.
        let block = [0x82u8, 0x00, 0x00, 0x80, 0xAA]; // track 2
        assert!(parse_block(&block, 0, 1_000_000, 1, None).is_none());
        // With 2 streams it resolves to index 1.
        let f = parse_block(&block, 0, 1_000_000, 2, None).unwrap();
        assert_eq!(f.track, 1);
    }

    #[test]
    fn parse_block_pts_honours_timestamp_scale() {
        // PTS = (cluster_ts_ticks + rel_ts) * ts_scale_ns. With a non-1ms scale
        // the result must scale accordingly (foreign MKVs). rel_ts = 10 here.
        let block = [0x81u8, 0x00, 0x0A, 0x80, 0xAA]; // track 1, rel 10, kf
        // ts_scale 1_000_000 (1ms): cluster 100 + rel 10 = 110 ticks → 110ms.
        let f = parse_block(&block, 100, 1_000_000, 1, None).unwrap();
        assert_eq!(f.pts, 110 * 1_000_000);
        assert!(f.keyframe);
        // ts_scale 90_000 (90kHz): (100+10) * 90_000.
        let f = parse_block(&block, 100, 90_000, 1, None).unwrap();
        assert_eq!(f.pts, 110 * 90_000);
    }

    #[test]
    fn parse_block_negative_rel_ts_is_signed() {
        // rel_ts is a SIGNED 16-bit big-endian value. 0xFFFF = -1. The pts must
        // go DOWN from the cluster timestamp, not jump to +65535.
        let block = [0x81u8, 0xFF, 0xFF, 0x80, 0xAA]; // rel_ts = -1
        let f = parse_block(&block, 100, 1_000_000, 1, None).unwrap();
        assert_eq!(f.pts, 99 * 1_000_000, "rel_ts -1 must subtract one tick");
    }

    #[test]
    fn parse_block_keyframe_flag_and_duration_propagate() {
        // flags bit 0x80 = keyframe; a clear bit = delta frame. duration_ns is
        // passed through unchanged (BlockGroup path supplies it).
        let kf = [0x81u8, 0x00, 0x00, 0x80, 0xAA];
        let nkf = [0x81u8, 0x00, 0x00, 0x00, 0xAA];
        assert!(parse_block(&kf, 0, 1_000_000, 1, None).unwrap().keyframe);
        assert!(!parse_block(&nkf, 0, 1_000_000, 1, None).unwrap().keyframe);
        let f = parse_block(&kf, 0, 1_000_000, 1, Some(40_000_000)).unwrap();
        assert_eq!(f.duration_ns, Some(40_000_000));
    }

    #[test]
    fn parse_block_pts_saturates_no_overflow() {
        // A hostile cluster timestamp near i64::MAX must not panic on the
        // ticks→ns multiply; saturating_mul caps it. (Guards the debug-build
        // overflow the source comment calls out.)
        let block = [0x81u8, 0x00, 0x00, 0x80, 0xAA];
        let f = parse_block(&block, i64::MAX, 1_000_000, 1, None).unwrap();
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
        let f = parse_block(&block, i64::MAX, 1_000_000, 1, None).unwrap();
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
        // (> i64::MAX). The reader must surface MkvInvalid on read().
        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        ebml::write_id(&mut cluster, ebml::CLUSTER_TIMESTAMP).unwrap();
        ebml::write_size(&mut cluster, 8).unwrap();
        cluster.extend_from_slice(&0xFFFF_FFFF_FFFF_FFFFu64.to_be_bytes());
        let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let e = stream.read().unwrap_err();
        assert!(is_mkv_invalid(&e));
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
        assert!(is_mkv_invalid(&e));
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
        assert!(is_mkv_invalid(&e));
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
        // the stream; the reader must reject it as MkvInvalid.
        let mut cluster = Vec::new();
        ebml::write_id(&mut cluster, ebml::CLUSTER).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap();
        ebml::write_id(&mut cluster, ebml::BLOCK_GROUP).unwrap();
        ebml::write_unknown_size(&mut cluster).unwrap(); // size = unknown
        let bytes = mkv_with_track_and_cluster(1, 1, &cluster);
        let mut stream = MkvStream::open(Cursor::new(bytes)).unwrap();
        let e = stream.read().unwrap_err();
        assert!(is_mkv_invalid(&e));
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
}
