//! PES frames — the unit that moves between an input and an output.
//!
//! A [`PesSource`] yields frames with `read()`; a [`PesSink`] takes them with `write()`.
//! Each handles its own format internally.
//!
//! source.read()     → PES frame (sectors → decrypt → demux internally)
//! sink.write(frame) → output file or wire (mux internally)

// Shared by `serialize`/`deserialize` so the wire format round-trips.
// Oversized frames are rejected on write rather than hard-erroring
// mid-stream on read.
const MAX_FRAME_SIZE: usize = 256 * 1024 * 1024; // 256 MiB

/// Where a unit's first byte came from in the SOURCE address space.
///
/// `byte` is the absolute byte offset of the unit's first byte within the
/// source stream the producer emits (the decrypted sector stream / ISO);
/// `sector` is that offset's 2048-byte logical sector (`byte / 2048`). One
/// value, **stamped once at the demux seam and propagated unchanged** through
/// every pipeline stage to the emitted frame — no stage recomputes it. This is
/// the load-bearing column for a frame-accurate source index (random-access
/// position) and is reusable for loss-to-timestamp mapping and seek indexing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SourcePos {
    /// Source logical sector (2048-byte) of the unit's first byte.
    pub sector: u64,
    /// Absolute source byte offset of the unit's first byte.
    pub byte: u64,
}

impl SourcePos {
    /// Build a `SourcePos` from an absolute source byte offset, deriving the
    /// 2048-byte sector. The single construction helper — callers stamp the
    /// byte offset they know and the sector follows, so the two never disagree.
    pub fn at_byte(byte: u64) -> Self {
        Self {
            sector: byte / 2048,
            byte,
        }
    }
}

/// One frame of elementary stream data.
#[derive(Debug, Clone)]
pub struct PesFrame {
    /// Track index (0-based, matches stream info track order).
    pub track: usize,
    /// Presentation timestamp in nanoseconds.
    pub pts: i64,
    /// True if this is a keyframe (IDR for video).
    pub keyframe: bool,
    /// Raw elementary stream data (NAL units, audio samples, etc).
    pub data: Vec<u8>,
    /// Optional duration in nanoseconds. Carried on the wire (8 bytes
    /// little-endian, `u64::MAX` as the sentinel for `None`). Set by
    /// the PGS parser so the MKV muxer can emit `BlockDuration`; also
    /// preserved across network:// and stdio:// hops.
    pub duration_ns: Option<u64>,
    /// Matroska DiscardPadding in nanoseconds: positive trims the end, negative
    /// trims the beginning. On the PES wire only for FMKV header v2 streams.
    pub discard_padding_ns: i64,
    /// Byte-exact source provenance of this frame's first byte, stamped at the
    /// demux seam. `None` for synthetic sources / the `skip_parse` path and for
    /// the `deserialize` (network/stdio) hop — NOT serialized on the wire.
    pub source: Option<SourcePos>,
    /// Codec-agnostic per-picture coding info (field order / type / pulldown),
    /// set by the video parser; `None` for audio/subtitle frames, codecs that
    /// do not yet fill it, and the deserialize hop. The muxer reads it through
    /// the [`crate::mux::codec::PictureInfo`] accessors to stamp `FieldOrder` /
    /// `DefaultDuration` — never re-deriving from the bitstream.
    pub coding: Option<crate::mux::codec::PictureInfo>,
}

/// Sentinel value for `duration_ns` on the wire: `u64::MAX` means `None`.
/// Valid durations are always much smaller (u64::MAX ns ≈ 584 years).
const DURATION_NONE_SENTINEL: u64 = u64::MAX;

impl PesFrame {
    /// Serialize to bytes:
    /// track(1) | pts(8 LE) | keyframe(1) | duration_ns(8 LE) | len(4 LE) | data
    ///
    /// `duration_ns` is encoded as `u64::MAX` when `None`, or the value when `Some`.
    pub fn serialize(&self, w: &mut dyn std::io::Write) -> std::io::Result<()> {
        self.serialize_inner(w, false)
    }

    fn serialize_inner(&self, w: &mut dyn std::io::Write, padded: bool) -> std::io::Result<()> {
        if self.track > 255 {
            return Err(crate::error::Error::PesTrackTooLarge { track: self.track }.into());
        }
        // Enforce the same ceiling the reader uses, so a frame that writes
        // can always be read back (round-trippable wire format).
        if self.data.len() > MAX_FRAME_SIZE {
            return Err(crate::error::Error::PesFrameTooLarge {
                size: self.data.len(),
            }
            .into());
        }
        let duration_wire = self.duration_ns.unwrap_or(DURATION_NONE_SENTINEL);
        w.write_all(&[self.track as u8])?;
        w.write_all(&self.pts.to_le_bytes())?;
        w.write_all(&[if self.keyframe { 1 } else { 0 }])?;
        w.write_all(&duration_wire.to_le_bytes())?;
        w.write_all(&(self.data.len() as u32).to_le_bytes())?;
        if padded {
            w.write_all(&self.discard_padding_ns.to_le_bytes())?;
        }
        w.write_all(&self.data)
    }

    /// [`serialize`](Self::serialize) plus, when `padded` (FMKV header v2), an
    /// 8-byte LE `discard_padding_ns` after the fixed header.
    pub(crate) fn serialize_ext(
        &self,
        w: &mut dyn std::io::Write,
        padded: bool,
    ) -> std::io::Result<()> {
        self.serialize_inner(w, padded)
    }

    /// [`deserialize`](Self::deserialize) for a stream whose FMKV header says
    /// frames carry the `discard_padding_ns` extension (`padded`).
    pub(crate) fn deserialize_ext(
        r: &mut dyn std::io::Read,
        padded: bool,
    ) -> std::io::Result<Option<Self>> {
        if !padded {
            return Self::deserialize(r);
        }
        Self::deserialize_inner(r, true)
    }

    /// Deserialize from bytes. Returns None at a clean end of stream.
    ///
    /// A clean EOF is exactly zero bytes available before the next frame.
    /// A partial header (1-21 bytes, e.g. a crash or short write) is a real
    /// error (`UnexpectedEof`), not silently treated as EOF — otherwise
    /// truncated `.pes` data would be accepted as a graceful end.
    pub fn deserialize(r: &mut dyn std::io::Read) -> std::io::Result<Option<Self>> {
        Self::deserialize_inner(r, false)
    }

    fn deserialize_inner(r: &mut dyn std::io::Read, padded: bool) -> std::io::Result<Option<Self>> {
        // Probe one byte first to distinguish clean EOF from a truncated header.
        // Loop on EINTR so a recoverable interrupted read doesn't fail — symmetric
        // with read_exact's internal retry for the rest of the header and data.
        let mut first = [0u8; 1];
        loop {
            match r.read(&mut first) {
                Ok(0) => return Ok(None), // clean EOF, no frame started
                Ok(_) => break,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }

        let mut header = [0u8; 22]; // 1 + 8 + 1 + 8 + 4
        header[0] = first[0];
        // The remaining 21 header bytes must be present; a short read here is
        // a truncated frame, propagated as UnexpectedEof.
        r.read_exact(&mut header[1..])?;
        let track = header[0] as usize;
        let pts = i64::from_le_bytes([
            header[1], header[2], header[3], header[4], header[5], header[6], header[7], header[8],
        ]);
        let keyframe = header[9] != 0;
        let duration_wire = u64::from_le_bytes([
            header[10], header[11], header[12], header[13], header[14], header[15], header[16],
            header[17],
        ]);
        let duration_ns = if duration_wire == DURATION_NONE_SENTINEL {
            None
        } else {
            Some(duration_wire)
        };
        let len = u32::from_le_bytes([header[18], header[19], header[20], header[21]]) as usize;
        let mut discard_padding_ns = 0;
        if padded {
            let mut ext = [0u8; 8];
            r.read_exact(&mut ext)?;
            discard_padding_ns = i64::from_le_bytes(ext);
        }
        if len > MAX_FRAME_SIZE {
            return Err(crate::error::Error::PesFrameTooLarge { size: len }.into());
        }
        // Grow INCREMENTALLY (not `vec![0u8; len]`): `len` is attacker-controlled to 256 MiB, so
        // a truncated stream claiming a huge frame must not pre-allocate the max. Read each chunk
        // straight into `data`'s own tail — no scratch buffer, no second copy; short reads EOF.
        const GROW_CHUNK: usize = 1024 * 1024; // 1 MiB growth step
        let mut data: Vec<u8> = Vec::with_capacity(len.min(GROW_CHUNK));
        let mut filled = 0usize;
        while filled < len {
            let want = (len - filled).min(GROW_CHUNK);
            data.resize(filled + want, 0);
            r.read_exact(&mut data[filled..filled + want])?;
            filled += want;
        }
        Ok(Some(Self {
            discard_padding_ns,
            track,
            pts,
            keyframe,
            data,
            duration_ns,
            // Provenance and coding are not carried on the wire — a frame read
            // back from a network:// / stdio:// / .pes hop has no source bytes
            // or parser context to attribute.
            source: None,
            coding: None,
        }))
    }

    // Create from a codec::Frame with a track index. `pub(crate)`: takes
    // the internal `mux::codec::Frame` type, so it can't be public API.
    pub(crate) fn from_codec_frame(track: usize, frame: crate::mux::codec::Frame) -> Self {
        Self {
            discard_padding_ns: 0,
            track,
            pts: frame.pts_ns,
            keyframe: frame.keyframe,
            data: frame.data,
            duration_ns: frame.duration_ns,
            source: frame.source,
            coding: frame.coding,
        }
    }
}

/// Decoder delay and seek preroll retained across container remuxing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TrackTiming {
    /// Nanoseconds of decoder delay, applied by the container's consumer.
    pub codec_delay_ns: u64,
    /// Nanoseconds of decoding required before a seek target.
    pub seek_preroll_ns: u64,
}

/// Where PES frames come from (pipeline design §2.2): an opened input's title (its
/// [`DiscTitle`](crate::disc::DiscTitle) track table) and its frames. Built by
/// [`crate::input`] / [`crate::open_source`] for every input scheme.
///
/// `Send` so a source can move to the mux's producer thread.
pub trait PesSource: Send {
    /// Read the next frame, or `Ok(None)` at end of stream.
    fn read(&mut self) -> std::io::Result<Option<PesFrame>>;

    /// The title being read: its tracks, codec setup, chapters. Stable across reads.
    fn info(&self) -> &crate::disc::DiscTitle;

    /// Container timing metadata, indexed in the same order as `info().streams`.
    fn track_timing(&self, _track: usize) -> TrackTiming {
        TrackTiming::default()
    }

    /// Codec initialization data for a track (SPS/PPS, AC-3 fscod, etc.).
    /// `None` for tracks that don't need codec_private (raw passthrough).
    fn codec_private(&self, _track: usize) -> Option<Vec<u8>> {
        None
    }

    /// True when `codec_private` is available for every primary video track
    /// and every AAC track (the latter bounded by a short wait and EOF) —
    /// callers buffer input frames until this flips, since some output
    /// formats (MKV) can't write frames without codec init data.
    fn headers_ready(&self) -> bool {
        true
    }

    /// `(track, frames)` for tracks whose in-band codec config changed after
    /// the track header was fixed (e.g. AAC stereo↔5.1); those frames keep the
    /// first config. Empty when none.
    fn config_changes(&self) -> Vec<(usize, u64)> {
        Vec::new()
    }

    /// Cumulative count of read errors the source skipped past (zero-filled bad sectors,
    /// blanked damaged units, frames the resync gate dropped).
    fn errors(&self) -> u64 {
        0
    }

    /// Cumulative bytes actually skipped (zero-filled) past read errors. Distinct from
    /// [`errors`](Self::errors), which counts skip *events*: one AACS skip covers a whole
    /// 6144-byte unit, so consumers estimating lost time scale by this byte count.
    fn lost_bytes(&self) -> u64 {
        0
    }
}

/// Where PES frames go (pipeline design §2.2): a container or wire writer, opened by
/// [`crate::output`] for every output scheme with the title it writes.
///
/// `Send` so a sink can move to the mux's write consumer thread.
pub trait PesSink: Send {
    /// Write a frame.
    fn write(&mut self, frame: &PesFrame) -> std::io::Result<()>;

    /// Finalize: flush buffered frames, write any container index (MKV `Cues`), close the
    /// underlying file/socket.
    fn finish(&mut self) -> std::io::Result<()>;

    /// End a sink whose producer failed or was stopped before the end of the title.
    /// Default: [`finish`](Self::finish) (a file keeps its partial output); a wire sink
    /// overrides it so its receiver sees a failure, not a clean end.
    fn finish_incomplete(&mut self) -> std::io::Result<()> {
        self.finish()
    }

    /// The title this sink was opened with.
    fn info(&self) -> &crate::disc::DiscTitle;

    /// Set timing before the first frame is written. Carried by `mkv://` and the
    /// FMKV `network://` / `stdio://` wire; `m2ts://` has no field for it and
    /// `mp4://` carries no Opus track, so those keep the no-op default.
    fn set_track_timing(&mut self, _track: usize, _timing: TrackTiming) -> std::io::Result<()> {
        Ok(())
    }

    /// Supply a codec_private that resolved after the header was written (late
    /// AAC config). `Ok(true)` when the sink recorded it; default `Ok(false)`.
    fn set_codec_private(&mut self, _track: usize, _data: &[u8]) -> std::io::Result<bool> {
        Ok(false)
    }

    /// `info().streams` indices this sink PLANNED to carry (and accepted frames for) but
    /// could not put in the finished container, valid after [`finish`](Self::finish).
    /// Empty for every sink that writes everything it accepted, except `mp4://` (audio
    /// with no parseable sample entry, known at finish) and `m2ts://` (LPCM BD LPCM can't
    /// carry, known from creation).
    fn undelivered_streams(&self) -> Vec<usize> {
        Vec::new()
    }
}

/// Wraps any output sink and counts bytes written.
///
/// Progress tracking is a CLI concern — streams don't know their size.
/// Wrap the output with `CountingStream`, then query `bytes_written()`.
///
/// ```text
/// let mut output = CountingStream::new(libfreemkv::output(dest, &title, None)?);
/// while let Ok(Some(frame)) = input.read() {
///     output.write(&frame)?;
///     let pct = output.bytes_written() as f64 / total as f64;
/// }
/// ```
pub struct CountingStream {
    inner: Box<dyn PesSink>,
    written: u64,
}

impl CountingStream {
    pub fn new(inner: Box<dyn PesSink>) -> Self {
        Self { inner, written: 0 }
    }

    /// Total bytes of PES frame data written through this stream.
    pub fn bytes_written(&self) -> u64 {
        self.written
    }
}

impl crate::pes::PesSink for CountingStream {
    fn write(&mut self, frame: &PesFrame) -> std::io::Result<()> {
        // Only count bytes that actually made it to the inner sink, so a
        // failed write doesn't permanently inflate bytes_written().
        self.inner.write(frame)?;
        self.written += frame.data.len() as u64;
        Ok(())
    }

    fn finish(&mut self) -> std::io::Result<()> {
        self.inner.finish()
    }

    fn finish_incomplete(&mut self) -> std::io::Result<()> {
        self.inner.finish_incomplete()
    }

    fn info(&self) -> &crate::disc::DiscTitle {
        self.inner.info()
    }

    fn set_track_timing(&mut self, track: usize, timing: TrackTiming) -> std::io::Result<()> {
        self.inner.set_track_timing(track, timing)
    }

    fn set_codec_private(&mut self, track: usize, data: &[u8]) -> std::io::Result<bool> {
        self.inner.set_codec_private(track, data)
    }

    fn undelivered_streams(&self) -> Vec<usize> {
        self.inner.undelivered_streams()
    }
}

#[cfg(test)]
#[path = "pes_tests.rs"]
mod tests;
